
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Persistence;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using Nixie;

namespace Kahuna.Server.KeyValues.Handlers;

/// <summary>
/// Stage-3 continuation for a persistent multi-page range scan (GetByRange).
///
/// The scan decomposes into a three-stage cycle that repeats once per disk page:
///   Stage 1 (actor thread, sync)  — snapshot in-memory range entries; dispatch the first
///            disk page via GetKeyValueByRange; defer the reply via the Promise.
///   Stage 2 (scheduler thread)    — pure disk read; mutates no actor-owned state.
///   Stage 3 (actor thread, here)  — K-way merge of the snapshotted memory entries and the
///            disk page; evaluate each key synchronously; accumulate results. When the
///            result limit is met, build and resolve the response. When more disk pages are
///            needed, dispatch the next page (stage 2) and defer resolution again, leaving
///            the actor mailbox free until the next ResumeRead message arrives.
///
/// A backend that serves reads from memory (<see cref="Persistence.Backend.IPersistenceBackend.ServesReadsFromMemory"/>)
/// takes a shorter path (<see cref="RunInline"/>): stage 2 runs on the actor thread inside the
/// stage-1 turn, and the merge follows at once, so the common page costs no scheduler hand-off,
/// no thread-pool continuation and no resume message. The merge is the same code either way; only
/// where the page is read differs.
///
/// The same continuation object is reused across all pages: it carries the full merge state
/// (memory snapshot cursor, accumulated results, disk cursor) from one Execute call to the
/// next, so no cross-page object graph is needed.
///
/// The in-memory entries are snapshotted as a sorted list at stage 1 rather than held as a
/// live BTree iterator. Holding a live iterator across actor messages would expose the scan
/// to BTree structural changes caused by writes that land between pages.
/// </summary>
internal sealed class RangeScanContinuation : ReadContinuation
{
    // ── Scan identity ─────────────────────────────────────────────────────────

    private readonly string prefix;
    private readonly int limit;
    private readonly KeyValueDurability durability;
    private readonly int partitionId;
    private readonly bool startInclusive;
    private readonly string? startKey;
    private readonly string? endKey;
    private readonly bool endInclusive;

    // ── Evaluation context (captured at stage 1) ──────────────────────────────

    private readonly HLCTimestamp transactionId;
    private readonly HLCTimestamp snapshotTs;
    private readonly HLCTimestamp currentTime;
    private readonly bool isSnapshotRead;

    /// <summary>
    /// True for a statement read: the transaction's MVCC entry on a key is served only when it holds a staged
    /// write, and a read pin is skipped as if the key were never read, so the page reads it at the snapshot.
    /// </summary>
    private readonly bool stagedWritesOnly;

    // Heads of rows this scan evaluated and left out of its page, kept across disk pages for the intent merge.
    // Allocated only while prepared intents linger (see DurableSnapshotSource.RecordExcludedHead).
    private Dictionary<string, ExcludedHead>? excludedHeads;

    /// <summary>Canonical decisions routed off-mailbox for the still-pending foreign intents this scan's window
    /// meets (keyed by intent identity). Null on the first attempt; populated when the manager re-issues the scan
    /// after resolving a committed-but-unsettled remote-anchor intent so the overlay serves it instead of retrying.</summary>
    private readonly IReadOnlyDictionary<(HLCTimestamp TransactionId, long Epoch), TransactionDecision>? routedDecisions;

    /// <summary>
    /// The prepared intents of the positions still ahead of the merge, captured on the actor thread before every
    /// disk page is read, keyed by key with the first capture of each key kept. A page is read off the actor and
    /// merged on it later, and a settlement can apply in between: it lands the intent's committed value in the base
    /// the pages are read from (the unflushed overlay at once, the resident entry through its coherence message)
    /// and removes the intent. A page read before that settlement, overlaid at the end with the live intent window
    /// read after it, saw neither the base nor the intent: a committed insert was missing from the scan, and a
    /// committed delete stayed in it. The window where this shows is a leader that never staged the transaction —
    /// a newly promoted one, whose predecessor's commits settle late through the handover — since the staging
    /// leader holds a write intent on every such key that makes the page wait instead. Capturing before the read
    /// keeps, for every position, an intent at least as old as the base that position was read from, so the final
    /// overlay still carries what the base lacked; the merge judges a captured intent against the row the page
    /// holds by revision, so a position consumed after the settlement stands on its own. Null until a capture
    /// finds an intent; a scan that meets none pays one empty window descent per page.
    /// </summary>
    private Dictionary<string, PreparedIntent>? capturedIntents;

    // ── K-way merge state (mutable, actor-thread-only) ───────────────────────

    /// <summary>
    /// The current bounded batch of in-memory entries (sorted by key). Stage 1 captures the first
    /// batch; when the merge consumes it and more resident keys may exist, <see cref="RefillMemory"/>
    /// re-queries the next batch on the actor thread. Within a batch the entries are not re-read.
    /// </summary>
    private List<(string Key, KeyValueEntry Entry)> memItems;

    /// <summary>Current position in the current memItems batch. Advances as keys are consumed.</summary>
    private int memIdx;

    /// <summary>Upper bound on the memory batch size, used when re-querying the next batch.</summary>
    private readonly int memBatch;

    /// <summary>True while the last memory batch was full, i.e. more resident keys may remain.</summary>
    private bool memMaybeMore;

    /// <summary>End bound of the resident range, reused when re-querying the next memory batch.</summary>
    private readonly string? memEnd;
    private readonly bool memEndInclusive;

    /// <summary>Disk cursor for the next GetKeyValueByRange call (key to start the next page at).</summary>
    private string diskCursor;

    /// <summary>
    /// True when the row at <see cref="diskCursor"/>, if the backend holds one, adds nothing to the merge:
    /// the exclusive start key of the request on the first page, or the previous page's sentinel row (already
    /// merged, or dropped by the as-of projection) on every later page. The backend seek is inclusive, so
    /// such a page is read one row longer to still carry <c>limit + 1</c> new rows; without that extra row a
    /// cursor page held exactly <c>limit</c> new rows and needed a second backend read to learn that more exist.
    /// </summary>
    private bool cursorRowExcluded;

    /// <summary>
    /// The largest disk key the merge consumed so far. A disk page carries its limit+1 sentinel row, and the
    /// next page starts at that row's key (an inclusive seek), so the rows at or below this key at the head
    /// of a new page were merged already and are skipped.
    /// </summary>
    private string? lastDiskKeyMerged;

    /// <summary>
    /// The combined stage-2 result: snapshot-projected entries plus raw pagination facts.
    /// Set by the stage-2 callback (off-actor) before the ResumeRead message is dispatched, or by
    /// <see cref="RunInline"/> on the actor thread; consumed and cleared by the merge (actor thread, stage 3).
    /// </summary>
    internal RangeDiskPage? RangeScanPage { get; set; }

    /// <summary>Items accumulated across all pages so far. Cleared only when the scan is resolved.</summary>
    private readonly List<(string, ReadOnlyKeyValueEntry)> accumulated;

    /// <summary>What the scan cost so far, summed over every disk page and merge step (actor thread only).</summary>
    private KeyValueScanTally tally;

    /// <summary>The page's observation as it stands; this path always merges with a disk read.</summary>
    private KeyValueScanObservation Observation => tally.ToObservation(diskFallback: true);

    // ─────────────────────────────────────────────────────────────────────────

    internal RangeScanContinuation(
        string prefix,
        int limit,
        KeyValueDurability durability,
        int partitionId,
        bool startInclusive,
        string? startKey,
        string? endKey,
        bool endInclusive,
        HLCTimestamp transactionId,
        HLCTimestamp snapshotTs,
        HLCTimestamp currentTime,
        bool isSnapshotRead,
        List<(string, KeyValueEntry)> memItems,
        string? memEnd,
        bool memEndInclusive,
        int memBatch,
        bool memMaybeMore,
        string diskCursor,
        KeyValueReplyRef promise,
        IReadOnlyDictionary<(HLCTimestamp TransactionId, long Epoch), TransactionDecision>? routedDecisions = null,
        bool stagedWritesOnly = false) : base(promise)
    {
        this.prefix = prefix;
        this.limit = limit;
        this.durability = durability;
        this.partitionId = partitionId;
        this.startInclusive = startInclusive;
        this.startKey = startKey;
        this.endKey = endKey;
        this.endInclusive = endInclusive;
        this.transactionId = transactionId;
        this.snapshotTs = snapshotTs;
        this.currentTime = currentTime;
        this.isSnapshotRead = isSnapshotRead;
        this.memItems = memItems;
        this.memEnd = memEnd;
        this.memEndInclusive = memEndInclusive;
        this.memBatch = memBatch;
        this.memMaybeMore = memMaybeMore;
        this.diskCursor = diskCursor;
        this.routedDecisions = routedDecisions;
        this.stagedWritesOnly = stagedWritesOnly;
        this.accumulated = [];

        // The first page seeks at the request's start key; when that start is exclusive, the row at the
        // seek position is skipped by the merge and counts for nothing.
        cursorRowExcluded = startKey is not null && !startInclusive;
    }

    /// <summary>
    /// Stage 3 on the scheduler path: merges the page the stage-2 callback stored in <see cref="RangeScanPage"/>,
    /// then either resolves every waiter or dispatches the next page and returns with the reply still deferred.
    /// </summary>
    internal override void Execute(KeyValueContext context)
    {
        KeyValueResponse? response = MergePage(context);

        if (response is null)
            DispatchNextPage(context);
        else
            Resolve(response);
    }

    /// <summary>
    /// The whole scan on the actor thread, for a backend that serves reads from memory: reads each page with
    /// <see cref="TryGetByRangeHandler.ReadDiskPage"/> on the calling thread and merges it at once, so the
    /// common page costs no scheduler hand-off, no thread-pool continuation and no resume message. Returns the
    /// response to reply with synchronously, or null when the scan was handed to the scheduler path (the
    /// reply is then deferred to the promise, exactly as on that path).
    /// <para>
    /// A page needs a further page only when its rows were dead, as-of dropped or already merged, so the common
    /// scan is one read. A range dense with dead rows could otherwise keep the mailbox for the whole walk, so
    /// the inline walk stops at the same inspection budget as the ephemeral scan and hands the rest to the
    /// scheduler, where the mailbox is free between pages.
    /// </para>
    /// </summary>
    internal KeyValueResponse? RunInline(KeyValueContext context)
    {
        long inspectionBudgetRaw = (long)limit + 1 + KeyValueScanLimits.MaxScanInspectionSlack;
        long inspectionBudget = inspectionBudgetRaw > int.MaxValue ? int.MaxValue : inspectionBudgetRaw;

        KeyValueResponse? response = RunInlinePages(context, inspectionBudget);

        if (response is null)
            DispatchNextPage(context);

        return response;
    }

    /// <summary>
    /// Reads and merges pages on the actor thread, as <see cref="RunInline"/> does, until the scan is complete or
    /// it has inspected <paramref name="inspectionBudget"/> entries. Returns the response, or null when the budget
    /// stopped the walk: <see cref="diskCursor"/> then points at the next page, and the caller reads it off the
    /// actor thread. A batched range probe shares one budget among its seeks this way.
    /// </summary>
    internal KeyValueResponse? RunInlinePages(KeyValueContext context, long inspectionBudget)
    {
        while (true)
        {
            CaptureIntentsAhead(context);

            RangeScanPage = ReadPage(context.PersistenceBackend, shouldAbort: null);
            KeyValueScanMetrics.PagesReadOnActor.Add(1);

            KeyValueResponse? response = MergePage(context);

            if (response is not null)
                return response;

            if (tally.EntriesInspected >= inspectionBudget)
                return null;
        }
    }

    /// <summary>The entries this scan inspected so far, over every page it merged (actor thread only).</summary>
    internal long EntriesInspected => tally.EntriesInspected;

    /// <summary>
    /// Stage 2: reads the page at <see cref="diskCursor"/> up to the scan's end bound and snapshot-projects it. Reads
    /// only the immutable scan identity and the cursor fields the last merge left, so it may run on a scheduler
    /// thread while the actor is free, as long as no merge of this scan runs at the same time.
    /// </summary>
    internal RangeDiskPage ReadPage(Persistence.Backend.IPersistenceBackend backend, Func<bool>? shouldAbort) =>
        TryGetByRangeHandler.ReadDiskPage(
            backend, prefix, diskCursor, cursorRowExcluded, endKey, endInclusive, limit,
            isSnapshotRead, snapshotTs, currentTime, shouldAbort);

    /// <summary>
    /// Merges <see cref="RangeScanPage"/> into the accumulated result (actor thread). Returns the page's
    /// response when the scan is complete or must stop, or null when the disk page ran out with more rows
    /// behind it before the result filled: <see cref="diskCursor"/> then points at the next page and the
    /// caller reads it, inline or through the scheduler.
    /// </summary>
    internal KeyValueResponse? MergePage(KeyValueContext context)
    {
        if (Faulted)
            return KeyValueStaticResponses.MustRetryResponse;

        // The rows of this page the projection could answer for only from below their queued revisions, each with
        // the ceiling it was projected at (see TryGetByRangeHandler.ProjectSnapshotPage).
        Dictionary<string, long>? fencedKeys = RangeScanPage?.HistoryFencedKeys;

        if (RangeScanPage is { } arrived)
        {
            tally.Add(arrived.Tally);

            // A fenced row is decided by its resident entry: the archive holds the queued revisions, so it either
            // serves the row or proves that the projection below them is its as-of image (EvaluateKeySync). One
            // that is not resident has no other source: the page would be short that row, or stale by the queued
            // revisions, so answer MustRetry, and the scan loop re-reads the page from the same cursor once the
            // flush lands.
            if (fencedKeys is not null)
            {
                foreach (string fencedKey in fencedKeys.Keys)
                {
                    if (context.Store.TryGetValue(fencedKey, out _))
                        continue;

                    RangeScanPage = null;
                    return new(KeyValueResponseType.MustRetry,
                        new KeyValueGetByRangeResult(KeyValueResponseType.MustRetry, [], null, false, Observation));
                }
            }
        }

        // RawHasMore and RawNextCursor come from the raw (unprojected) disk page; the
        // projected list may be smaller or empty if rows were filtered by snapshot projection.
        // Using projected.Count for diskHasMore would prematurely terminate the scan when
        // all limit+1 raw rows are post-snapshot and dropped by projection.
        List<(string, ReadOnlyKeyValueEntry)> diskPage = RangeScanPage?.Projected ?? [];
        bool diskHasMore = RangeScanPage?.RawHasMore ?? false;
        int diskAvailable = diskPage.Count;
        int diskIdx = 0;

        // A continuation page begins at the previous page's sentinel key, which the merge consumed.
        if (lastDiskKeyMerged is not null)
            while (diskIdx < diskAvailable && string.CompareOrdinal(diskPage[diskIdx].Item1, lastDiskKeyMerged) <= 0)
                diskIdx++;

        // For snapshot scans, build a key→projected-entry map from the current disk page so
        // EvaluateKeySync can fall back to it when the in-memory revision archive misses.
        // The disk page has already been snapshot-projected by the stage-2 task (off-actor),
        // so every entry in this map is already at-or-before snapshotTs.
        Dictionary<string, ReadOnlyKeyValueEntry>? diskProjections = null;
        if (isSnapshotRead && diskAvailable > 0)
        {
            diskProjections = new(diskAvailable);
            for (int i = 0; i < diskAvailable; i++)
                diskProjections[diskPage[i].Item1] = diskPage[i].Item2;
        }

        while (accumulated.Count <= limit)
        {
            // When the disk page is exhausted but more pages exist, stop the merge here:
            // there may be disk keys that sort before the remaining memory keys, so we must
            // fetch the next page before continuing. The remaining memItems are preserved
            // (memIdx is not reset) and will resume in the next Execute call.
            if (diskIdx >= diskAvailable && diskHasMore)
                break;

            // If the current memory batch is exhausted but more resident keys may remain, pull the
            // next bounded batch (actor thread) before deciding the merge order for this step.
            if (memIdx >= memItems.Count && memMaybeMore)
                RefillMemory(context);

            string? memKey = memIdx < memItems.Count ? memItems[memIdx].Key : null;
            string? diskKey = diskIdx < diskAvailable ? diskPage[diskIdx].Item1 : null;

            if (memKey is null && diskKey is null)
                break; // both sources exhausted

            string keyToProcess;
            KeyValueEntry? entry;

            int cmp;
            if (memKey is null) cmp = 1;
            else if (diskKey is null) cmp = -1;
            else cmp = string.CompareOrdinal(memKey, diskKey);

            if (cmp < 0)
            {
                keyToProcess = memKey!;
                entry = memItems[memIdx].Entry;
                memIdx++;
            }
            else if (cmp > 0)
            {
                keyToProcess = diskKey!;
                ReadOnlyKeyValueEntry de = diskPage[diskIdx].Item2;
                diskIdx++;
                lastDiskKeyMerged = keyToProcess;

                if (fencedKeys is not null && fencedKeys.ContainsKey(keyToProcess)
                    && context.Store.TryGetValue(keyToProcess, out KeyValueEntry? nowResident))
                {
                    // A fenced row is projected only from below its queued revisions, so it is served through its
                    // resident entry, whose archive holds those revisions. The entry is not in the memory batch:
                    // it was loaded after stage 1 copied the batch (a point read warmed it), and the fence check
                    // above found it resident.
                    entry = nowResident;
                }
                else
                {
                    // Build a transient entry for the disk-only key; not inserted into the store
                    // (populateCache:false semantics — range scans must not pollute the LRU).
                    entry = new KeyValueEntry
                    {
                        Bucket = GetBucket(keyToProcess),
                        Value = de.Value,
                        Revision = de.Revision,
                        FlushedRevision = de.Revision,
                        Expires = de.Expires,
                        LastUsed = de.LastUsed,
                        LastModified = de.LastModified,
                        State = de.State
                    };
                }
            }
            else
            {
                // Same key in both sources. The resident entry is usually the fresher committed base
                // and carries the MVCC / RYOW state, but a stale resident that missed its coherence
                // notification can shadow a newer durable row indefinitely — so the collision is
                // decided by (revision, commit HLC), not unconditionally by residency.
                keyToProcess = memKey!;
                KeyValueEntry memEntry = memItems[memIdx].Entry;
                ReadOnlyKeyValueEntry diskEntry = diskPage[diskIdx].Item2;
                memIdx++;
                diskIdx++;
                lastDiskKeyMerged = keyToProcess;

                // In snapshot mode the disk page is as-of projected, so this comparison is only a
                // staleness signal there, never a serving decision.
                bool diskIsNewer = diskEntry.Revision > memEntry.Revision
                    || (diskEntry.Revision == memEntry.Revision && diskEntry.LastModified > memEntry.LastModified);

                if (diskIsNewer && !isSnapshotRead)
                {
                    // Converge the resident entry off this scan through the coherence-reconcile apply,
                    // which adopts the durable row only when strictly newer and owns the intent/fence
                    // handling. The scan itself never waits on it and never installs into the store.
                    // Published reads of the key defer to the actor until the reconcile landed.
                    context.PublishedHeads.NotePendingApply(keyToProcess, diskEntry.Revision, diskEntry.LastModified);

                    context.ActorContext.Self.Send(
                        KeyValueRequestPool.RentInvalidateOrApply(
                            keyToProcess,
                            diskEntry.Revision,
                            diskEntry.Value,
                            diskEntry.Expires,
                            diskEntry.LastModified,
                            diskEntry.LastModified,
                            diskEntry.State,
                            forceResident: false,
                            transactionId: HLCTimestamp.Zero,
                            partitionId: partitionId,
                            noRevision: false,
                            isRollback: false,
                            returnToPoolOnReceive: true,
                            backendHydrated: true,
                            hydratedEntry: new ReadOnlyKeyValueEntry(
                                diskEntry.Value, diskEntry.Revision, diskEntry.Expires,
                                diskEntry.LastUsed, diskEntry.LastModified, diskEntry.State),
                            reconcile: true
                        )
                    );
                }

                if (diskIsNewer && !isSnapshotRead && transactionId == HLCTimestamp.Zero)
                {
                    // Plain current read: serve the newer committed durable row as a transient entry
                    // (populateCache:false semantics, like the disk-only branch). Transactional and
                    // snapshot reads keep the resident base — its MVCC / RYOW / intent state must not
                    // be bypassed — and converge through the reconcile sent above.
                    entry = new KeyValueEntry
                    {
                        Bucket = GetBucket(keyToProcess),
                        Value = diskEntry.Value,
                        Revision = diskEntry.Revision,
                        FlushedRevision = diskEntry.Revision,
                        Expires = diskEntry.Expires,
                        LastUsed = diskEntry.LastUsed,
                        LastModified = diskEntry.LastModified,
                        State = diskEntry.State
                    };
                }
                else
                    entry = memEntry;
            }

            // Exclusive-start enforcement: the BTree honours this natively; apply it to
            // disk-origin keys as well (and to any key that re-enters here as the cursor
            // when StartKey is also the first key of a continuation page).
            if (!startInclusive && startKey is not null &&
                string.CompareOrdinal(keyToProcess, startKey) == 0)
                continue;

            // End-key enforcement: the BTree applies the bound to memory keys via the bounds passed to
            // GetByRange, and the backend page stops at it for disk keys. This check is the guard for a
            // backend that reads past the bound (the interface default trims, a test double may not),
            // and it ends the merge as soon as either side reaches the bound.
            if (endKey is not null)
            {
                int cmpEnd = string.CompareOrdinal(keyToProcess, endKey);
                if (cmpEnd > 0 || (!endInclusive && cmpEnd == 0))
                    goto Done;
            }

            KeyValueResponse? response = EvaluateKeySync(context, keyToProcess, entry, diskProjections, fencedKeys);
            tally.EntriesInspected++;

            if (response is null || response.Type == KeyValueResponseType.DoesNotExist)
            {
                tally.DeadEntriesSkipped++;

                // A head the page leaves out (a delete, an expired value) still supersedes an older committed intent
                // on the key; remember it for the intent merge, which otherwise sees no row and would inject the
                // intent's older value.
                if (!isSnapshotRead && entry is not null)
                    DurableSnapshotSource.RecordExcludedHead(context, ref excludedHeads, keyToProcess, entry.Revision, entry.LastModified);
                continue;
            }

            if (response.Type != KeyValueResponseType.Get || response.Entry is null)
            {
                // Scan-aborting response (WaitingForReplication, Aborted, etc.). It still carries what the
                // attempt cost, so a page retried from the same cursor can report the sum over its attempts.
                return new(response.Type, new KeyValueGetByRangeResult(response.Type, [], null, false, Observation));
            }

            accumulated.Add((keyToProcess, response.Entry));
        }

        Done:
        // The current disk page ran out with more rows behind it before the result filled: the next page
        // starts at the last raw row, which this merge consumed (or the projection dropped), so that row
        // is excluded from the next page's count. The caller reads the page, inline or on the scheduler.
        if (diskIdx >= diskAvailable && diskHasMore && accumulated.Count <= limit)
        {
            // RawNextCursor is the last raw row of the page; guaranteed non-null when RawHasMore.
            diskCursor = RangeScanPage!.RawNextCursor!;
            cursorRowExcluded = true;
            RangeScanPage = null;
            return null;
        }

        // Durable-intent scan visibility: overlay prepared intents covering this page's window onto the accumulated
        // page. No-op when the intent store is empty (durable-intent path disabled). The merge owns the page
        // cap/cursor over the merged sequence, so an injected key counts toward the page and can become the cursor.
        if (context.PreparedIntentStore is { } intentStore)
        {
            // The K-way merge only reaches here after draining every disk page, so the sole "more" signal is the
            // limit+1 sentinel; the ceiling is the largest accumulated key.
            bool kvHasMore = accumulated.Count > limit;
            string? kvCeilingKey = accumulated.Count > 0 ? accumulated[^1].Item1 : null;

            // Clamp the intent window to the sentinel key when the page truncated there: an intent past the
            // ceiling cannot affect any returned row and belongs to the next page — without the clamp an
            // undecided intent ordinally far past the page makes the whole page retry, which starves bounded
            // probes of busy ranges. A page that exhausted the range keeps the request's own end bound, so
            // intent-only committed keys past the last base row still inject.
            string? intentEnd = kvHasMore ? kvCeilingKey : memEnd;
            bool intentEndIncl = kvHasMore || memEndInclusive;

            IReadOnlyList<PreparedIntent> ranged = CollectPageIntents(intentStore, intentEnd, intentEndIncl);
            if (ranged.Count > 0)
            {
                PreparedIntentScanMerge.ScanMergeResult merge = PreparedIntentScanMerge.Merge(
                    accumulated, ranged, snapshotTs, currentTime, limit, kvHasMore, kvCeilingKey,
                    i => DurableReadVisibility.ScanDecision(context, routedDecisions, i),
                    k => DurableSnapshotSource.ReaderHasOwnVersion(context, k, transactionId, stagedWritesOnly),
                    i => DurableSnapshotSource.HeadSupersedesIntent(context, i, excludedHeads, isSnapshotRead ? snapshotTs : HLCTimestamp.Zero));
                if (merge.MustRetry)
                    return new(KeyValueResponseType.MustRetry,
                        new KeyValueGetByRangeResult(KeyValueResponseType.MustRetry, [], null, false, Observation));

                tally.IntentsMerged += merge.IntentsMerged;
                return TryGetByRangeHandler.BuildMergedResponse(prefix, durability, merge, snapshotTs, Observation);
            }
        }

        return BuildResponse(accumulated, limit, snapshotTs, prefix, durability, Observation);
    }

    /// <summary>
    /// Stage 2 on the scheduler path: enqueues the read of the page at <see cref="diskCursor"/> on the backend
    /// read scheduler under the scan's data partition, and arranges for the result to come back to the actor
    /// as a <c>ResumeRead</c> message carrying this continuation. The reply stays deferred to the promise; a
    /// rejected enqueue (back-pressure, a stopping scheduler) resolves every waiter with a retryable result
    /// instead of leaving the scan half-assembled.
    /// </summary>
    internal void DispatchNextPage(KeyValueContext context)
    {
        CaptureIntentsAhead(context);

        Task<RangeDiskPage> nextTask;
        try
        {
            nextTask = context.BackendReadScheduler.EnqueueTask(
                partitionId,
                () =>
                {
                    // The scheduler can run this long after the deadline sweep expired this
                    // continuation; skip the disk work then — the stage-3 late-completion
                    // guard drops the result anyway.
                    if (Cancelled)
                    {
                        KeyValueScanMetrics.ScansAbandonedCancelled.Add(1);
                        return new RangeDiskPage([], false, null);
                    }

                    KeyValueScanMetrics.PagesReadOnScheduler.Add(1);
                    return ReadPage(context.PersistenceBackend, () => Cancelled);
                });
        }
        catch (Exception ex)
        {
            context.Logger.LogWarning(
                "KeyValueActor/RangeScan: read scheduler rejected page enqueue for prefix {Prefix} cursor {Cursor}: {Ex}",
                prefix, diskCursor, ex.Message);
            Resolve(KeyValueStaticResponses.MustRetryResponse);
            return;
        }

        IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> self = context.ActorContext.Self;
        _ = nextTask.ContinueWith(t =>
        {
            if (!t.IsCompletedSuccessfully)
            {
                // Surface the fault — a swallowed exception here becomes an undiagnosable
                // retry-exhaustion at the client.
                context.Logger.LogWarning(t.Exception?.GetBaseException(), "KeyValueActor/RangeScan: disk page read faulted for prefix {Prefix}", prefix);
                SetFaulted();
            }
            else RangeScanPage = t.Result;
            self.Send(new KeyValueRequest(KeyValueRequestType.ResumeRead) { Continuation = this });
        }, TaskScheduler.Default);
    }

    // ── Prepared-intent capture (actor thread) ──────────────────────────────────

    /// <summary>
    /// Captures into <see cref="capturedIntents"/> the prepared intents of every position the merge has not
    /// consumed yet: from the largest disk key merged so far (every key at or below it, from either source, is
    /// consumed; a page's merge stops right after its last disk key) to the scan's end bound. Runs on the actor
    /// thread before each disk page read, so a settlement that lands between this capture and the read is in
    /// both the capture and the page, and one that landed before it is in the page.
    /// </summary>
    private void CaptureIntentsAhead(KeyValueContext context)
    {
        if (context.PreparedIntentStore is not { } intentStore)
            return;

        string from = lastDiskKeyMerged ?? startKey ?? prefix;
        bool fromInclusive = lastDiskKeyMerged is null && (startKey is null || startInclusive);

        IReadOnlyList<PreparedIntent> ahead = intentStore.SnapshotScanWindow(from, fromInclusive, memEnd, memEndInclusive);
        if (ahead.Count == 0)
            return;

        capturedIntents ??= new(StringComparer.Ordinal);
        foreach (PreparedIntent intent in ahead)
            capturedIntents.TryAdd(intent.Key, intent);
    }

    /// <summary>
    /// The intents the final overlay reconciles the page with: the page's live window (which adds the intents
    /// prepared since the last capture) folded into the captured set, which then stands filtered to the page's
    /// ceiling. A captured intent that settled meanwhile is still here — that is the point — and its decision is
    /// read from the transaction record when its own resolution is still pending.
    /// </summary>
    private IReadOnlyList<PreparedIntent> CollectPageIntents(PreparedIntentStore intentStore, string? intentEnd, bool intentEndIncl)
    {
        IReadOnlyList<PreparedIntent> live =
            intentStore.SnapshotScanWindow(startKey ?? prefix, startInclusive || startKey is null, intentEnd, intentEndIncl);

        if (capturedIntents is null)
            return live;

        foreach (PreparedIntent intent in live)
            capturedIntents.TryAdd(intent.Key, intent);

        List<PreparedIntent> page = new(capturedIntents.Count);

        foreach (PreparedIntent intent in capturedIntents.Values)
        {
            if (intentEnd is not null)
            {
                int cmp = string.CompareOrdinal(intent.Key, intentEnd);
                if (cmp > 0 || (cmp == 0 && !intentEndIncl))
                    continue;
            }

            page.Add(intent);
        }

        return page;
    }

    // ── Per-key evaluation (sync; mirrors TryGetByRangeHandler.Get without disk I/O) ──────

    /// <summary>
    /// Evaluates a single key for inclusion in the range scan result.
    ///
    /// Returns null to skip (DoesNotExist in scan context), a Get response to include the
    /// entry, or a scan-aborting response (WaitingForReplication, Aborted) to terminate.
    /// All paths are synchronous — the entry is already in hand from the in-memory snapshot
    /// or from the disk page fetched in stage 2, so no further backend I/O is needed.
    ///
    /// Runs on the actor thread (stage 3); actor-owned state mutations (MVCC entries,
    /// TouchEntry) are safe.
    /// </summary>
    /// <summary>
    /// Re-queries the next bounded batch of resident keys, starting exclusively after the last key
    /// of the current batch, and resets the batch cursor. Runs on the actor thread (stage 3). This
    /// keeps each actor visit O(memBatch) instead of O(resident-range-size); the trade-off is that a
    /// resident range larger than one batch is read as several as-of-resume batches rather than a
    /// single stage-1 snapshot, so a write landing in a not-yet-read key region during the scan may
    /// be observed — within the scan's not-strictly-point-in-time contract.
    /// </summary>
    private void RefillMemory(KeyValueContext context)
    {
        if (memItems.Count == 0)
        {
            memMaybeMore = false;
            return;
        }

        string lastKey = memItems[^1].Key;
        List<(string Key, KeyValueEntry Entry)> next = [];
        foreach (KeyValuePair<string, KeyValueEntry> kv in
            context.Store.GetByRange(lastKey, false, memEnd, memEndInclusive, memBatch))
            next.Add((kv.Key, kv.Value));

        memItems = next;
        memIdx = 0;
        memMaybeMore = next.Count == memBatch;
    }

    private KeyValueResponse? EvaluateKeySync(
        KeyValueContext context,
        string key,
        KeyValueEntry? entry,
        Dictionary<string, ReadOnlyKeyValueEntry>? diskProjections,
        Dictionary<string, long>? fencedKeys = null)
    {
        // Replication intent: a pending replication on this key means the entry is not yet
        // fully committed. Clear expired intents as housekeeping; block if still live.
        if (entry?.ReplicationIntent is not null)
        {
            if (entry.ReplicationIntent.Expires - currentTime > TimeSpan.Zero)
                return KeyValueStaticResponses.WaitingForReplicationResponse;
            entry.ReplicationIntent = null;
        }

        // Write intent from another transaction: for snapshot scans, if the pending commit
        // could land at-or-before the snapshot timestamp we must wait (the shared safe-time
        // rule — see KeyValueWriteIntentSafeTime, which also proves a writer that began at or
        // after the snapshot can never commit inside it). For non-snapshot scans fall through
        // to the committed state (the intent does not block the read). Gated on the caller's
        // explicit timestamp, not on snapshotTs: the latter is normalised to "now" for the
        // cursor and is never null, so testing it made every non-snapshot page wait on any
        // live intent — the point-read handler tests message.ReadTimestamp, and so must this.
        if (entry?.WriteIntent != null && entry.WriteIntent.TransactionId != transactionId)
        {
            if (!KeyValueWriteIntentLease.IsLive(context, key, entry.WriteIntent, currentTime))
                entry.WriteIntent = null;
            else if (isSnapshotRead
                     && KeyValueWriteIntentSafeTime.MayCommitAtOrBefore(entry.WriteIntent, snapshotTs))
                return KeyValueStaticResponses.WaitingForReplicationResponse;
        }

        // Transactional MVCC: read-your-own-writes and OCC snapshot tracking. A statement read takes only a
        // staged write from here; its read pins read as committed state at the snapshot below, so a later
        // statement sees later commits and is never aborted for a pin an earlier statement left.
        if (transactionId != HLCTimestamp.Zero)
        {
            if (entry is not null)
            {
                entry.MvccEntries ??= new();

                if (entry.MvccEntries.TryGetValue(transactionId, out KeyValueMvccEntry? existing)
                    && (!stagedWritesOnly || existing.StagedUnder is not null))
                {
                    if (entry.Revision > existing.Revision)
                        return KeyValueStaticResponses.AbortedResponse;

                    if (existing.State is KeyValueState.Undefined or KeyValueState.Deleted ||
                        (existing.Expires != HLCTimestamp.Zero && existing.Expires - currentTime < TimeSpan.Zero))
                        return null;

                    return new(KeyValueResponseType.Get, new ReadOnlyKeyValueEntry(
                        existing.Value, existing.Revision, existing.Expires,
                        existing.LastUsed, existing.LastModified, entry.State));
                }
            }

            // No existing MVCC entry for this key and transaction.
            // For AS-OF snapshot scans: fall through — the snapshot-visibility path below
            // serves the committed revision at-or-before the snapshot, matching the read-only
            // contract (no OCC tracking for snapshot scans).
            //
            // OCC snapshot + byte accounting mutate actor-owned state, so they may run only on an
            // entry that is genuinely resident *now*. `entry` was captured from the stage-1 memory
            // snapshot: a Collect arriving between pages can have evicted it (StoreKey nulled, bytes
            // already reclaimed) or replaced it with a different object. Recording MVCC state on such
            // an orphan leaks approximateStoreBytes and loses OCC tracking (later ops re-load a
            // different object). Re-look up the key and proceed only when the captured object is the
            // one still in the store; otherwise (evicted, replaced, or disk-only) treat it as
            // non-resident — fall through to the committed-state read with no OCC tracking.
            if (!isSnapshotRead
                && entry is not null
                && context.Store.TryGetValue(key, out KeyValueEntry? liveEntry)
                && ReferenceEquals(liveEntry, entry))
            {
                // Snapshot the current committed state for OCC conflict detection. A committed-but-unsettled foreign
                // intent supersedes the resident base, so snapshot its committed value instead — otherwise this
                // transaction binds a stale base and its later reads of the key return DoesNotExist or OCC-Aborted.
                KeyValueMvccEntry newMvcc;
                switch (DurableSnapshotSource.Resolve(context, key, transactionId, entry, currentTime, out KeyValueMvccEntry intentSnapshot))
                {
                    case SnapshotDecision.Retry:
                        return KeyValueStaticResponses.WaitingForReplicationResponse;
                    case SnapshotDecision.UseIntent:
                        newMvcc = intentSnapshot;
                        break;
                    default:
                        newMvcc = new()
                        {
                            Value = entry.Value,
                            Revision = entry.Revision,
                            Expires = entry.Expires,
                            LastUsed = entry.LastUsed,
                            LastModified = entry.LastModified,
                            State = entry.State
                        };
                        break;
                }

                entry.MvccEntries!.Add(transactionId, newMvcc);
                context.AdjustEstimatedEntryBytes(
                    entry, KeyValueStoreAccounting.MvccEntryAddedBytes(entry.MvccEntries.Count == 1, newMvcc.Value));

                if (entry.Revision > newMvcc.Revision)
                    return KeyValueStaticResponses.AbortedResponse;

                if (newMvcc.State is KeyValueState.Undefined or KeyValueState.Deleted ||
                    (newMvcc.Expires != HLCTimestamp.Zero && newMvcc.Expires - currentTime < TimeSpan.Zero))
                    return null;

                return new(KeyValueResponseType.Get, new ReadOnlyKeyValueEntry(
                    newMvcc.Value, newMvcc.Revision, newMvcc.Expires,
                    newMvcc.LastUsed, newMvcc.LastModified, newMvcc.State));
            }
        }

        // Snapshot visibility: when the current revision was committed after the snapshot
        // timestamp, serve the most recent archived revision at-or-before the snapshot.
        // If no such revision exists (key was created after the snapshot, or the revision
        // was pruned), the key is invisible for this scan.
        //
        // Only for an explicit as-of read (isSnapshotRead). snapshotTs is never null — stage 1
        // normalises a missing read timestamp to "now" so the cursor can carry it — and the old
        // test on snapshotTs alone sent every non-snapshot page down this path for any resident
        // key written between stage 1 and this merge. A hot key trims its in-memory archive past
        // that instant (or a head jump leaves a gap), the lookup missed, there is no disk
        // projection on a non-snapshot page, and a key with a live committed head was silently
        // dropped from the result: a read-committed COUNT(*) over a table under write load
        // returned fewer rows than exist. A non-snapshot page must serve the committed head,
        // exactly as a point read without a timestamp does.
        if (isSnapshotRead && entry is not null && entry.LastModified > snapshotTs)
        {
            if (!entry.TryGetRevisionAtOrBefore(snapshotTs, out long snapRevision, out KeyValueRevisionEntry snapshot))
            {
                // A head jump skipped revisions this entry never archived, and their flush
                // requests may still be queued: neither the in-memory archive nor the disk
                // projection can answer for the skipped window yet. Silently falling through
                // here made the row vanish from (or read stale in) the scan; point reads
                // already fail closed on the same condition (see TryGetHandler). MustRetry
                // aborts the page, and the caller's page retry loop re-reads once the queued
                // flushes are acknowledged.
                if (entry.SnapshotAtRiskFromUnflushedGap(snapshotTs))
                    return KeyValueStaticResponses.MustRetryResponse;

                // The disk projection was read from the persisted history, which is missing any
                // revision still queued for the background writer; if one of those is not in the
                // archive either, the projection may be an older row than the committed answer.
                if (UnflushedHistoryFence.HistoryMayLag(context.UnflushedWrites, key, entry.Revisions, entry.Revision - 1))
                    return KeyValueStaticResponses.MustRetryResponse;

                // Every revision queued now is in the archive and past the snapshot, so the row's as-of image
                // is in the flushed history below them, which is what the projection of a fenced row was taken
                // from (TryGetByRangeHandler.ProjectSnapshotPage). That holds only while the queued revisions are
                // the ones the projection was taken below: a flush that landed since moved the ceiling up, and a
                // revision the projection never saw may have been trimmed from the archive meanwhile. Fail the
                // page closed then; the retry projects the row again at the new ceiling.
                if (fencedKeys is not null && fencedKeys.TryGetValue(key, out long projectedCeiling)
                    && UnflushedHistoryFence.FlushedHistoryCeiling(context.UnflushedWrites, key) != projectedCeiling)
                    return KeyValueStaticResponses.MustRetryResponse;

                // In-memory revision archive does not reach as far back as snapshotTs.
                // Fall back to the stage-2 disk projection carried in from the off-actor task.
                // Purely memory-only keys (never flushed) have no disk projection and are omitted.
                if (diskProjections is not null && diskProjections.TryGetValue(key, out ReadOnlyKeyValueEntry? diskSnap) && diskSnap is not null)
                {
                    if (diskSnap.State is KeyValueState.Deleted or KeyValueState.Undefined ||
                        (diskSnap.Expires != HLCTimestamp.Zero && diskSnap.Expires - currentTime < TimeSpan.Zero))
                        return null;
                    return new(KeyValueResponseType.Get, diskSnap);
                }
                return null;
            }

            if (snapshot.State is KeyValueState.Deleted or KeyValueState.Undefined ||
                (snapshot.Expires != HLCTimestamp.Zero && snapshot.Expires - currentTime < TimeSpan.Zero))
                return null;

            return new(KeyValueResponseType.Get, new ReadOnlyKeyValueEntry(
                snapshot.Value, snapRevision, snapshot.Expires,
                currentTime, snapshot.LastModified, snapshot.State));
        }

        // Non-transactional, non-snapshot path: serve the current committed state.
        if (entry is null || entry.State is KeyValueState.Deleted or KeyValueState.Undefined ||
            (entry.Expires != HLCTimestamp.Zero && entry.Expires - currentTime < TimeSpan.Zero))
            return null;

        context.TouchEntry(entry, currentTime);

        return new(KeyValueResponseType.Get, new ReadOnlyKeyValueEntry(
            entry.Value, entry.Revision, entry.Expires,
            entry.LastUsed, entry.LastModified, entry.State));
    }

    // ── Static helpers ────────────────────────────────────────────────────────

    private static string? GetBucket(string key)
    {
        int index = key.LastIndexOf('/');
        return index == -1 ? null : key[..index];
    }

    private static KeyValueResponse BuildResponse(
        List<(string, ReadOnlyKeyValueEntry)> items,
        int limit,
        HLCTimestamp snapshotTs,
        string prefix,
        KeyValueDurability durability,
        KeyValueScanObservation observation)
    {
        bool hasMore = items.Count > limit;
        if (hasMore)
            items.RemoveAt(items.Count - 1);

        string? nextCursor = hasMore && items.Count > 0
            ? KeyValueRangeCursor.Encode(items[^1].Item1, durability, prefix, snapshotTs)
            : null;

        return new(KeyValueResponseType.Get, new KeyValueGetByRangeResult(KeyValueResponseType.Get, items, nextCursor, hasMore, observation));
    }
}
