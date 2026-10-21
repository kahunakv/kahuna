
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Shared.KeyValue;
using Kahuna.Utils;
using Kommander;
using Kommander.Time;
using Nixie;

namespace Kahuna.Server.KeyValues.Handlers;

internal sealed class TryGetByRangeHandler : BaseHandler
{
    // Upper bound on how many resident rows stage 1 copies before dispatching the first disk page.
    // Keeps the pre-check actor visit O(batch) instead of O(resident-range-size); the continuation
    // re-queries the next batch on demand if the merge consumes past it. A batch this size means a
    // scan over a resident range no larger than it behaves exactly as a single frozen snapshot.
    private const int MemoryScanBatchSize = 512;

    public TryGetByRangeHandler(KeyValueContext context) : base(context) { }

    public ValueTask<KeyValueResponse> Execute(KeyValueRequest message)
    {
        if (message.Durability == KeyValueDurability.Ephemeral)
            return GetByRangeEphemeral(message);

        return ValueTask.FromResult(GetByRangePersistentStage1(message));
    }

    // ── Ephemeral (in-memory only) ────────────────────────────────────────────

    private ValueTask<KeyValueResponse> GetByRangeEphemeral(KeyValueRequest message) =>
        // currentTime: always fresh — used for expiry / intent checks.
        GetByRangeEphemeral(message, context.Raft.HybridLogicalClock.TrySendOrLocalEvent(context.Raft.GetLocalNodeId()));

    /// <summary>
    /// One ephemeral range page read at <paramref name="currentTime"/>, which may inspect at most
    /// <paramref name="inspectionSlack"/> dead entries beyond the live ones it needs. A batched range probe reads every
    /// seek at the probe's one instant this way, with what is left of one slack it shares among its seeks.
    /// </summary>
    internal async ValueTask<KeyValueResponse> GetByRangeEphemeral(
        KeyValueRequest message, HLCTimestamp currentTime, int inspectionSlack = KeyValueScanLimits.MaxScanInspectionSlack)
    {
        string prefix = message.Key;
        int limit = message.Limit;

        // snapshotTs: captured once at page 0 and carried unchanged through the cursor.
        // Zero ⟹ "latest" (legacy / non-paged behaviour, no snapshot filtering).
        HLCTimestamp snapshotTs = message.ReadTimestamp.IsNull() ? currentTime : message.ReadTimestamp;

        (string memStart, bool memStartIncl, string? memEnd, bool memEndIncl) = ComputeBounds(message);

        List<(string, ReadOnlyKeyValueEntry)> items = [];

        // The walk stops when we collect limit+1 live items (HasMore sentinel) or the range is
        // exhausted — a hard limit+1 window would produce a premature HasMore=false whenever
        // tombstones/expired entries fill the window. To keep an interleaved run of dead entries from
        // parking the mailbox thread on an O(resident-range) walk, bound the *inspected* count too:
        // once it is exceeded we stop and resume the next page from the last inspected key. Because the
        // page cursor is exclusive, resuming past a tombstone loses no live entry — the caller just
        // fetches another page. The budget carries limit+1 live capacity plus slack for dead rows, so a
        // normal (sparse-tombstone) page behaves exactly as before.
        // Compute in long and clamp: an unbounded scan arrives as limit == int.MaxValue, and
        // limit + 1 + slack would overflow to a negative int — making the "inspected >= budget"
        // guard fire on the very first key and truncate the page to a single item.
        long inspectionBudgetRaw = (long)limit + 1 + inspectionSlack;
        int inspectionBudget = inspectionBudgetRaw > int.MaxValue ? int.MaxValue : (int)inspectionBudgetRaw;
        int inspected = 0;
        KeyValueScanTally tally = default;
        string? resumeAfterKey = null;

        foreach ((string key, KeyValueEntry? _) in context.Store.GetByRange(memStart, memStartIncl, memEnd, memEndIncl, int.MaxValue))
        {
            inspected++;
            tally.EntriesInspected++;

            KeyValueResponse response = await Get(currentTime, message.TransactionId, key, message.Durability, snapshotTs, snapshotRead: !message.ReadTimestamp.IsNull());

            if (response.Type == KeyValueResponseType.DoesNotExist)
            {
                tally.DeadEntriesSkipped++;

                if (inspected >= inspectionBudget)
                {
                    resumeAfterKey = key;
                    break;
                }
                continue;
            }

            if (response.Type != KeyValueResponseType.Get || response.Entry is null)
                return new(response.Type, new KeyValueGetByRangeResult(response.Type, [], null, false, tally.ToObservation(diskFallback: false)));

            items.Add((key, response.Entry));

            if (items.Count == limit + 1)
                break;

            if (inspected >= inspectionBudget)
            {
                resumeAfterKey = key;
                break;
            }
        }

        // Durable-intent scan visibility: overlay prepared intents covering this page's window onto the page. No-op
        // when the intent store is empty (durable-intent path disabled), so ordinary scans are unchanged.
        if (context.PreparedIntentStore is { } intentStore)
        {
            // The KV side has more beyond this page when the limit+1 sentinel was fetched or the walk truncated on
            // its inspection budget; the ceiling is that sentinel/last-inspected key.
            bool kvHasMore = items.Count > limit || resumeAfterKey is not null;
            string? kvCeilingKey = resumeAfterKey ?? (items.Count > 0 ? items[^1].Item1 : null);

            // Bound the intent window exactly to the KV rows this page drew from: clamped to the truncation point
            // (the inspection-budget cursor, or the limit+1 sentinel key) whenever the walk stopped early, so an
            // intent past it is deferred to the next page rather than injected here. Without the sentinel clamp an
            // undecided intent ordinally far past the page — one that cannot affect any returned row — makes the
            // whole page retry, which starves bounded probes of busy ranges. Only a walk that exhausted the range
            // keeps the request's own end bound, so intent-only committed keys past the last base row still inject.
            string? intentEnd;
            bool intentEndIncl;

            if (resumeAfterKey is not null)
            {
                intentEnd = resumeAfterKey;
                intentEndIncl = true;
            }
            else if (items.Count > limit)
            {
                intentEnd = items[^1].Item1;
                intentEndIncl = true;
            }
            else
            {
                intentEnd = memEnd;
                intentEndIncl = memEndIncl;
            }

            IReadOnlyList<PreparedIntent> ranged =
                intentStore.SnapshotScanWindow(memStart, memStartIncl, intentEnd, intentEndIncl);
            if (ranged.Count > 0)
            {
                PreparedIntentScanMerge.ScanMergeResult merge = PreparedIntentScanMerge.Merge(
                    items, ranged, snapshotTs, currentTime, limit, kvHasMore, kvCeilingKey,
                    i => DurableReadVisibility.ScanDecision(context, message.ForeignScanDecisions, i),
                    k => DurableSnapshotSource.ReaderHasOwnVersion(context, k, message.TransactionId),
                    i => DurableSnapshotSource.HeadSupersedesIntent(
                        context, i, excludedHeads: null, message.ReadTimestamp.IsNull() ? HLCTimestamp.Zero : message.ReadTimestamp));
                if (merge.MustRetry)
                    return new(KeyValueResponseType.MustRetry,
                        new KeyValueGetByRangeResult(KeyValueResponseType.MustRetry, [], null, false, tally.ToObservation(diskFallback: false)));

                tally.IntentsMerged += merge.IntentsMerged;
                return BuildMergedResponse(prefix, message.Durability, merge, snapshotTs, tally.ToObservation(diskFallback: false));
            }
        }

        return BuildResponse(prefix, message.Durability, items, limit, snapshotTs, tally.ToObservation(diskFallback: false), resumeAfterKey);
    }

    // ── Persistent (K-way merge of memory + disk, detached off the actor mailbox) ──────────

    private KeyValueResponse GetByRangePersistentStage1(KeyValueRequest message)
    {
        HLCTimestamp currentTime = context.Raft.HybridLogicalClock.TrySendOrLocalEvent(context.Raft.GetLocalNodeId());

        IActorContext<KeyValueActor, KeyValueRequest, KeyValueResponse> actorContext = context.ActorContext;

        if (!actorContext.Reply.HasValue)
            return KeyValueStaticResponses.ErroredResponse;

        KeyValueReplyRef promise = KeyValueReplyRef.From(actorContext.Reply.Value);

        RangeScanContinuation cont = CreatePersistentScan(message, currentTime, promise);

        if (context.PersistenceBackend is { ServesReadsFromMemory: true })
        {
            // Every backend read is a memory lookup: read and merge the page in this turn, and reply
            // synchronously. The hand-off through the scheduler worker, the thread-pool continuation
            // and the resume message costs several times the read itself, and the mailbox is held for
            // microseconds either way. Null means the inline walk reached its inspection budget and
            // handed the rest to the scheduler path, which replies through the promise.
            KeyValueResponse? response = cont.RunInline(context);

            if (response is not null)
                return response;
        }
        else
            cont.DispatchNextPage(context);

        actorContext.ByPassReply = true;
        return KeyValueStaticResponses.WaitingForReplicationResponse;
    }

    /// <summary>
    /// Stage 1 of a persistent range page read at <paramref name="currentTime"/>: snapshots the first bounded batch of
    /// the resident entries in range and returns the continuation that merges them with the disk pages. Reads no disk
    /// page; the caller reads the first one, inline or through the scheduler. A batched range probe builds one per seek
    /// this way, with every seek at the probe's one instant, and passes a smaller <paramref name="memoryBatch"/> so the
    /// copies of all its seeks together stay within its bound on entries; the merge refills a batch it consumes.
    /// </summary>
    internal RangeScanContinuation CreatePersistentScan(
        KeyValueRequest message, HLCTimestamp currentTime, KeyValueReplyRef promise, int memoryBatch = MemoryScanBatchSize)
    {
        string prefix = message.Key;
        int limit = message.Limit;

        HLCTimestamp snapshotTs = message.ReadTimestamp.IsNull() ? currentTime : message.ReadTimestamp;

        (string memStart, bool memStartIncl, string? memEnd, bool memEndIncl) = ComputeBounds(message);

        // Stage 1: snapshot a bounded batch of the matching in-memory entries (actor thread,
        // synchronous). Materialising the BTree range as a list before dispatching stage 2 avoids
        // holding a live iterator across actor messages: concurrent writes between pages would
        // cause BTree structural changes that could invalidate a deferred iterator. The copy is
        // capped at MemoryScanBatchSize (or limit+1 if larger) so a large resident range does not
        // turn this pre-check into O(resident-count) work; if the merge consumes the whole batch,
        // the continuation re-queries the next batch on the actor thread.
        int memBatch = Math.Max(limit + 1, memoryBatch);
        List<(string, KeyValueEntry)> memItems = [];
        foreach (KeyValuePair<string, KeyValueEntry> kv in
            context.Store.GetByRange(memStart, memStartIncl, memEnd, memEndIncl, memBatch))
            memItems.Add((kv.Key, kv.Value));

        // Full batch ⟹ there may be more resident keys past the last one copied.
        bool memMaybeMore = memItems.Count == memBatch;

        string diskCursor = message.StartKey ?? prefix;

        // Route every disk page to the FairReadScheduler partition that owns this range, not
        // message.PartitionId (which the manager leaves at 0 for scans). Enqueuing scans under
        // partition 0 would collapse per-partition fairness/back-pressure and read-after-write
        // FIFO ordering onto one queue. The continuation reuses this for subsequent pages.
        int scanPartition = ResolvePartition(prefix);

        // The continuation carries the request's own end bound (not the prefix upper bound ComputeBounds
        // substitutes for the BTree) to the backend: the backend already stops at the prefix, and a null
        // bound means just that.
        return new(
            prefix, limit, message.Durability, scanPartition,
            message.StartInclusive, message.StartKey,
            message.EndKey, message.EndInclusive,
            message.TransactionId, snapshotTs, currentTime,
            isSnapshotRead: !message.ReadTimestamp.IsNull(),
            memItems, memEnd, memEndIncl, memBatch, memMaybeMore, diskCursor,
            promise, message.ForeignScanDecisions);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /// <summary>
    /// Stage 2 of the persistent range scan: reads one raw disk page from <paramref name="diskCursor"/> up to
    /// the scan's end bound and snapshot-projects it (<see cref="ProjectSnapshotPage"/>). The backend stops at
    /// the bound, so a short bounded seek reads and projects only the entries inside it rather than a full
    /// page; a raw page shorter than the sentinel size because the bound ended it carries no next cursor.
    /// The page carries what the read cost — the revision rows and seeks of the walk and the as-of lookups
    /// of the projection — so stage 3 can add it to the scan's observation.
    /// <para>
    /// The page holds <c>limit + 1</c> rows, the sentinel that tells the merge more rows exist, plus one more
    /// when <paramref name="cursorRowExcluded"/> says the row at the cursor adds nothing (an exclusive start
    /// key, or the previous page's sentinel): the seek is inclusive, so that row takes a slot, and without
    /// the extra row a cursor page carried exactly <c>limit</c> new rows and cost a second backend read.
    /// </para>
    /// Runs on a scheduler thread, or on the actor thread for a backend that serves reads from memory.
    /// </summary>
    internal static RangeDiskPage ReadDiskPage(
        IPersistenceBackend backend,
        string prefix,
        string diskCursor,
        bool cursorRowExcluded,
        string? endKey,
        bool endInclusive,
        int limit,
        bool isSnapshotRead,
        HLCTimestamp snapshotTs,
        HLCTimestamp currentTime,
        Func<bool>? shouldAbort)
    {
        KeyValueScanTally tally = default;

        int pageSize = SaturatingPageSize(limit, cursorRowExcluded ? 1 : 0);

        List<(string, ReadOnlyKeyValueEntry)> rawPage =
            backend.GetKeyValueByRange(prefix, diskCursor, endKey, endInclusive, pageSize, ref tally);

        // The production backend is the unflushed-writes overlay; its index says which keys still have a
        // revision queued for the writer, which the as-of projection must not read around.
        UnflushedKeyValueWritesIndex? unflushed = (backend as UnflushedOverlayPersistenceBackend)?.UnflushedWrites;

        return ProjectSnapshotPage(rawPage, pageSize, isSnapshotRead, snapshotTs, currentTime, backend, shouldAbort, tally, unflushed);
    }

    /// <summary>
    /// Builds a <see cref="RangeDiskPage"/> from a raw disk page: extracts raw pagination
    /// facts (has-more flag and next-cursor key) from the unfiltered <paramref name="rawPage"/>,
    /// then snapshot-projects every row, the sentinel included. The raw page has more behind it
    /// exactly when it is as long as the <paramref name="pageSize"/> the backend was asked for; the
    /// next page then starts at its last row.
    ///
    /// Separating pagination from projection fixes a silent correctness bug: when every raw row
    /// is post-snapshot and dropped by projection, the caller must still know the raw page was
    /// full (RawHasMore=true) and fetch the next page rather than concluding the disk is exhausted.
    ///
    /// The sentinel row is a real row of the range, so it takes part in the stage-3 merge like any
    /// other: a page whose merge ends on it needs no second disk read to learn that more rows exist,
    /// where a page without it had to fetch the next page to find one more row. The next page starts
    /// at the sentinel key, and the continuation skips the rows it already merged.
    ///
    /// Runs on a scheduler thread: the per-key GetKeyValueRevisionAtOrBefore calls do disk I/O and
    /// must not block the actor mailbox. The one exception is a backend that serves reads from
    /// memory, whose lookups cost microseconds; the scan then runs this on the actor thread.
    /// <para>
    /// A row whose head is newer than the snapshot is answered from the persisted history, and the
    /// history is complete only below the oldest revision of the key still queued for the background
    /// writer (<see cref="UnflushedHistoryFence"/>). A row this projection drops (a history miss) or
    /// replaces with an older revision never reaches the stage-3 fence of the resident path, so the
    /// projection applies the fence itself: a row with a queued revision below its head is left out of
    /// the page and named in <see cref="RangeDiskPage.HistoryFencedKeys"/>. The merge then serves it from
    /// its resident entry, whose archive holds the queued revisions, and answers MustRetry when it is
    /// not resident, instead of serving a page that is short a row, or stale by one. A newly promoted
    /// leader is where this shows: the rows it applied as a follower are not resident, so a snapshot
    /// scan reads every one of them through this projection, and the youngest of them still have
    /// their first revisions queued when a later commit moves the head past the snapshot.
    /// </para>
    /// </summary>
    internal static RangeDiskPage ProjectSnapshotPage(
        List<(string, ReadOnlyKeyValueEntry)> rawPage,
        int pageSize,
        bool isSnapshotRead,
        HLCTimestamp snapshotTs,
        HLCTimestamp currentTime,
        IPersistenceBackend backend,
        Func<bool>? shouldAbort = null,
        KeyValueScanTally tally = default,
        UnflushedKeyValueWritesIndex? unflushed = null)
    {
        // Derive raw pagination facts before any projection filtering.
        bool rawHasMore = rawPage.Count >= pageSize;
        string? rawNextCursor = rawHasMore ? rawPage[^1].Item1 : null;

        if (!isSnapshotRead)
            return new RangeDiskPage(rawPage, rawHasMore, rawNextCursor, tally);

        int usableCount = rawPage.Count;
        List<(string, ReadOnlyKeyValueEntry)> projected = new(usableCount);
        HashSet<string>? fencedKeys = null;
        for (int i = 0; i < usableCount; i++)
        {
            // Stop projecting once the continuation is expired: each stale key costs a
            // revision-history read, and the result of an expired page is dropped at stage 3.
            if (shouldAbort is not null && shouldAbort())
                break;

            (string k, ReadOnlyKeyValueEntry e) = rawPage[i];

            if (e.LastModified.CompareTo(snapshotTs) <= 0)
            {
                projected.Add((k, e));
                continue;
            }

            // The history below the head is the only source this projection has for the row's as-of
            // image, and it is incomplete while a revision below the head is still queued for the writer:
            // the read would answer with an older row, or with nothing for a key whose first revision is
            // the queued one. Leave the row to the merge, which serves it from a resident entry or fails
            // the page closed; the window is bounded by the flush cadence.
            if (UnflushedHistoryFence.HistoryMayLag(unflushed, k, archive: null, e.Revision - 1))
            {
                (fencedKeys ??= new(StringComparer.Ordinal)).Add(k);
                continue;
            }

            // A dropped row never reaches the stage-3 merge, so it is counted here: the merge counts only the
            // rows it receives.
            KeyValueEntry? snap = backend.GetKeyValueRevisionAtOrBefore(k, e.Revision - 1, snapshotTs, ref tally);
            if (snap is null || snap.State is KeyValueState.Deleted or KeyValueState.Undefined
                || (snap.Expires != HLCTimestamp.Zero && snap.Expires.CompareTo(currentTime) < 0))
            {
                tally.EntriesInspected++;
                tally.DeadEntriesSkipped++;
                continue;
            }

            projected.Add((k, new ReadOnlyKeyValueEntry(
                snap.Value, snap.Revision, snap.Expires, snap.LastUsed, snap.LastModified, snap.State)));
        }
        return new RangeDiskPage(projected, rawHasMore, rawNextCursor, tally, fencedKeys);
    }

    /// <summary>
    /// The limit+1 pagination sentinel plus <paramref name="extraRows"/>, saturated so an unbounded scan
    /// (limit == int.MaxValue) does not overflow negative — backends treat a negative limit as "no rows",
    /// silently emptying the disk page.
    /// </summary>
    internal static int SaturatingPageSize(int limit, int extraRows = 0)
    {
        long pageSize = (long)limit + 1 + extraRows;
        return pageSize > int.MaxValue ? int.MaxValue : (int)pageSize;
    }

    /// <summary>Computes the effective BTree bounds, substituting the prefix upper bound when EndKey is null.</summary>
    internal static (string start, bool startIncl, string? end, bool endIncl) ComputeBounds(KeyValueRequest message) =>
        ComputeBounds(message.Key, message.StartKey, message.StartInclusive, message.EndKey, message.EndInclusive);

    /// <summary>The effective BTree bounds of a page over <paramref name="prefix"/> between the given bounds.</summary>
    internal static (string start, bool startIncl, string? end, bool endIncl) ComputeBounds(
        string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive)
    {
        string start = startKey ?? prefix;
        bool startIncl = startKey is null || startInclusive;

        string? end = endKey ?? GetPrefixUpperBound(prefix);
        bool endIncl = endKey is not null && endInclusive;

        return (start, startIncl, end, endIncl);
    }

    /// <summary>
    /// Builds the paged response, encoding <paramref name="snapshotTs"/> into the cursor so
    /// subsequent pages read at the same snapshot.
    /// </summary>
    private static KeyValueResponse BuildResponse(
        string prefix,
        KeyValueDurability durability,
        List<(string, ReadOnlyKeyValueEntry)> items,
        int limit,
        HLCTimestamp snapshotTs,
        KeyValueScanObservation observation,
        string? resumeAfterKey = null)
    {
        // Two independent "more pages exist" signals: the limit+1 sentinel (a full page of live items)
        // and inspection truncation (the walk stopped on the inspection budget before filling the page).
        // The sentinel's cursor is the last returned item; truncation's cursor is the last inspected key
        // (possibly a tombstone past the last live item) so the next page resumes strictly beyond it.
        bool sentinelHasMore = items.Count > limit;
        if (sentinelHasMore)
            items.RemoveAt(items.Count - 1);

        string? cursorKey = sentinelHasMore
            ? (items.Count > 0 ? items[^1].Item1 : null)
            : resumeAfterKey;

        bool hasMore = cursorKey is not null;
        string? nextCursor = hasMore
            ? KeyValueRangeCursor.Encode(cursorKey!, durability, prefix, snapshotTs)
            : null;

        KeyValueGetByRangeResult result = new(KeyValueResponseType.Get, items, nextCursor, hasMore, observation);
        return new(KeyValueResponseType.Get, result);
    }

    /// <summary>
    /// Builds the paged response from a durable-intent scan-merge outcome. Pagination (item cap and cursor key) is
    /// already resolved by <see cref="PreparedIntentScanMerge"/> against the merged sequence; this only encodes the
    /// cursor (stamping <paramref name="snapshotTs"/> so subsequent pages read at the same snapshot). Shared with
    /// the persistent continuation so both scan paths encode the merged cursor identically.
    /// </summary>
    internal static KeyValueResponse BuildMergedResponse(
        string prefix,
        KeyValueDurability durability,
        PreparedIntentScanMerge.ScanMergeResult merge,
        HLCTimestamp snapshotTs,
        KeyValueScanObservation observation)
    {
        string? nextCursor = merge.HasMore && merge.NextCursorKey is not null
            ? KeyValueRangeCursor.Encode(merge.NextCursorKey, durability, prefix, snapshotTs)
            : null;

        KeyValueGetByRangeResult result = new(KeyValueResponseType.Get, merge.Items, nextCursor, nextCursor is not null, observation);
        return new(KeyValueResponseType.Get, result);
    }

    /// <summary>
    /// Returns the smallest string greater than every string with the given prefix.
    /// Null only when the prefix consists entirely of char.MaxValue (practically impossible).
    /// </summary>
    private static string? GetPrefixUpperBound(string prefix)
    {
        for (int i = prefix.Length - 1; i >= 0; i--)
        {
            if (prefix[i] < char.MaxValue)
                return string.Concat(prefix.AsSpan(0, i), ((char)(prefix[i] + 1)).ToString());
        }
        return null;
    }

    /// <summary>
    /// Resolves a single key for a range scan.
    ///
    /// <paramref name="currentTime"/> is the current HLC tick (expiry / intent checks).
    /// <paramref name="readTimestamp"/> is always the effective snapshot timestamp — callers
    /// normalise it to <c>currentTime</c> when no explicit AS-OF snapshot was requested, so it is
    /// never <c>Zero</c> inside this method.
    /// <paramref name="snapshotRead"/> is <c>true</c> when the caller supplied an explicit AS-OF
    /// timestamp (<c>message.ReadTimestamp != Zero</c>); when <c>false</c> an ordinary RYOW/MVCC
    /// scan is in progress and the transactional block runs normally.
    /// </summary>
    private async Task<KeyValueResponse> Get(
        HLCTimestamp currentTime,
        HLCTimestamp transactionId,
        string key,
        KeyValueDurability durability,
        HLCTimestamp readTimestamp,
        ReadOnlyKeyValueEntry? keyValueEntry = null,
        bool snapshotRead = false)
    {
        // populateCache: false — this is a read-only scan. Inserting disk entries into the
        // store here would mutate the BTree while GetByRangePersistent is lazily enumerating it.
        KeyValueEntry? entry = await GetKeyValueEntry(key, durability, keyValueEntry, populateCache: false);

        ReadOnlyKeyValueEntry readOnlyKeyValueEntry;

        if (entry?.ReplicationIntent is not null)
        {
            if (entry.ReplicationIntent.Expires - currentTime > TimeSpan.Zero)
                return KeyValueStaticResponses.WaitingForReplicationResponse;

            entry.ReplicationIntent = null;
        }

        // A live write intent from another transaction is not a blocking condition for reads
        // UNLESS the scan carries a snapshot timestamp and the pending commit ts could land
        // at-or-before that snapshot (safe-time wait). Clear expired intents as housekeeping;
        // otherwise apply the shared safe-time rule (see KeyValueWriteIntentSafeTime): a commit
        // provably outside the snapshot — a known commit ts above it, or a writer that began at
        // or after it — falls through to committed state instead of retrying the page.
        if (entry?.WriteIntent != null)
        {
            if (entry.WriteIntent.TransactionId != transactionId)
            {
                if (!KeyValueWriteIntentLease.IsLive(context, key, entry.WriteIntent, currentTime))
                    entry.WriteIntent = null;
                // Gated on the caller's explicit timestamp (snapshotRead), not on readTimestamp,
                // which is normalised to "now" and never null here — see the persistent merge.
                else if (snapshotRead
                         && KeyValueWriteIntentSafeTime.MayCommitAtOrBefore(entry.WriteIntent, readTimestamp))
                    return KeyValueStaticResponses.WaitingForReplicationResponse;
                // live write intent from another tx: fall through to committed state
            }
        }

        if (transactionId != HLCTimestamp.Zero)
        {
            // Check for an existing MVCC entry first. This covers RYOW (own uncommitted writes)
            // and prior read-snapshots for OCC conflict detection. Runs for both AS-OF and
            // non-AS-OF scans: an own write must be visible regardless of the snapshot timestamp.
            if (entry is not null)
            {
                entry.MvccEntries ??= new();

                if (entry.MvccEntries.TryGetValue(transactionId, out KeyValueMvccEntry? mvccEntry))
                {
                    if (entry.Revision > mvccEntry.Revision)
                        return KeyValueStaticResponses.AbortedResponse;

                    if (mvccEntry.State is KeyValueState.Undefined or KeyValueState.Deleted ||
                        mvccEntry.Expires != HLCTimestamp.Zero && mvccEntry.Expires - currentTime < TimeSpan.Zero)
                        return KeyValueStaticResponses.DoesNotExistContextResponse;

                    readOnlyKeyValueEntry = new(
                        mvccEntry.Value,
                        mvccEntry.Revision,
                        mvccEntry.Expires,
                        mvccEntry.LastUsed,
                        mvccEntry.LastModified,
                        entry.State);

                    return new(KeyValueResponseType.Get, readOnlyKeyValueEntry);
                }
            }

            // No prior read or write for this key by the transaction.
            // For AS-OF snapshot scans fall through to TryGetRevisionAtOrBefore below — that path
            // serves the revision at-or-before readTimestamp, matching TryGetHandler's behaviour.
            // For ordinary read-write scans, snapshot the current state for OCC conflict tracking.
            if (!snapshotRead)
            {
                if (entry is null)
                    return KeyValueStaticResponses.DoesNotExistContextResponse;

                // A committed-but-unsettled foreign intent supersedes the resident base: snapshot its committed
                // value so this transaction's later reads of the key stay consistent instead of binding a stale base.
                // entry.MvccEntries is already initialised (??= new() in the block above).
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

                if (entry.Revision > newMvcc.Revision)
                    return KeyValueStaticResponses.AbortedResponse;

                if (newMvcc.State is KeyValueState.Undefined or KeyValueState.Deleted ||
                    newMvcc.Expires != HLCTimestamp.Zero && newMvcc.Expires - currentTime < TimeSpan.Zero)
                    return KeyValueStaticResponses.DoesNotExistContextResponse;

                readOnlyKeyValueEntry = new(
                    newMvcc.Value,
                    newMvcc.Revision,
                    newMvcc.Expires,
                    newMvcc.LastUsed,
                    newMvcc.LastModified,
                    newMvcc.State);

                return new(KeyValueResponseType.Get, readOnlyKeyValueEntry);
            }
            // else (snapshotRead, no own write): fall through to snapshot-visibility path below.
        }

        // Non-transactional snapshot visibility check.
        // When the current revision was committed after the snapshot, serve the most recent
        // archived revision whose LastModified ≤ readTimestamp (snapshot isolation) instead of
        // dropping the key. If no such revision is retained (key didn't exist at the snapshot,
        // or the revision was trimmed / lives only on disk), the key is invisible for this page.
        // Only for an explicit as-of read: readTimestamp is normalised to "now" for the cursor
        // and is never null, and a non-snapshot scan must serve the committed head rather than
        // drop a live key whose archive does not reach the stamp (see RangeScanContinuation).
        if (snapshotRead && entry is not null && entry.LastModified > readTimestamp)
        {
            if (!entry.TryGetRevisionAtOrBefore(readTimestamp, out long snapRevision, out KeyValueRevisionEntry snapshot))
                return KeyValueStaticResponses.DoesNotExistContextResponse;

            if (snapshot.State is KeyValueState.Deleted or KeyValueState.Undefined ||
                (snapshot.Expires != HLCTimestamp.Zero && snapshot.Expires - currentTime < TimeSpan.Zero))
                return KeyValueStaticResponses.DoesNotExistContextResponse;

            return new(KeyValueResponseType.Get, new ReadOnlyKeyValueEntry(
                snapshot.Value,
                snapRevision,
                snapshot.Expires,
                currentTime,
                snapshot.LastModified,
                snapshot.State));
        }

        if (entry is null || entry.State is KeyValueState.Deleted or KeyValueState.Undefined ||
            entry.Expires != HLCTimestamp.Zero && entry.Expires - currentTime < TimeSpan.Zero)
            return KeyValueStaticResponses.DoesNotExistContextResponse;

        context.TouchEntry(entry, currentTime);

        readOnlyKeyValueEntry = new(
            entry.Value,
            entry.Revision,
            entry.Expires,
            entry.LastUsed,
            entry.LastModified,
            entry.State);

        return new(KeyValueResponseType.Get, readOnlyKeyValueEntry);
    }
}
