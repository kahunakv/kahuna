
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

    /// <summary>Items accumulated across all pages so far. Cleared only when the scan is resolved.</summary>
    private readonly List<(string, ReadOnlyKeyValueEntry)> accumulated;

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
        TaskCompletionSource<KeyValueResponse?> promise) : base(promise)
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
        this.accumulated = [];
    }

    internal override void Execute(KeyValueContext context)
    {
        if (Faulted)
        {
            Resolve(KeyValueStaticResponses.MustRetryResponse);
            return;
        }

        List<(string, ReadOnlyKeyValueEntry)> diskPage = ScanDiskResult ?? [];
        bool diskHasMore = diskPage.Count > limit;
        int diskAvailable = diskHasMore ? diskPage.Count - 1 : diskPage.Count;
        int diskIdx = 0;

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

            bool isResident;
            if (cmp < 0)
            {
                keyToProcess = memKey!;
                entry = memItems[memIdx].Entry;
                memIdx++;
                isResident = true;
            }
            else if (cmp > 0)
            {
                keyToProcess = diskKey!;
                ReadOnlyKeyValueEntry de = diskPage[diskIdx].Item2;
                diskIdx++;
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
                isResident = false;
            }
            else
            {
                // Same key: memory wins — it has the fresher value plus MVCC / RYOW state.
                keyToProcess = memKey!;
                entry = memItems[memIdx].Entry;
                memIdx++;
                diskIdx++;
                isResident = true;
            }

            // Exclusive-start enforcement: the BTree honours this natively; apply it to
            // disk-origin keys as well (and to any key that re-enters here as the cursor
            // when StartKey is also the first key of a continuation page).
            if (!startInclusive && startKey is not null &&
                string.CompareOrdinal(keyToProcess, startKey) == 0)
                continue;

            // End-key enforcement: GetKeyValueByRange has no endKey parameter, so the upper
            // bound must be enforced here for disk keys; the BTree already applies it for
            // memory keys via the bounds passed to GetByRange.
            if (endKey is not null)
            {
                int cmpEnd = string.CompareOrdinal(keyToProcess, endKey);
                if (cmpEnd > 0 || (!endInclusive && cmpEnd == 0))
                    goto Done;
            }

            KeyValueResponse? response = EvaluateKeySync(context, keyToProcess, entry, isResident);

            if (response is null || response.Type == KeyValueResponseType.DoesNotExist)
                continue;

            if (response.Type != KeyValueResponseType.Get || response.Entry is null)
            {
                // Scan-aborting response (WaitingForReplication, Aborted, etc.).
                Resolve(new(response.Type, new KeyValueGetByRangeResult(response.Type, [], null, false)));
                return;
            }

            accumulated.Add((keyToProcess, response.Entry));
        }

        Done:
        // If the current disk page ran out but more pages remain, and the result cap has not
        // been reached, dispatch the next disk page and defer resolution to the next Execute.
        if (diskIdx >= diskAvailable && diskHasMore && accumulated.Count <= limit)
        {
            diskCursor = diskPage[diskAvailable].Item1; // the (limit+1)-th item is the next start
            ScanDiskResult = null;

            Task<List<(string, ReadOnlyKeyValueEntry)>> nextTask;
            try
            {
                string capturedCursor = diskCursor;
                nextTask = context.Raft.ReadScheduler.EnqueueTask(
                    partitionId,
                    () => context.PersistenceBackend.GetKeyValueByRange(prefix, capturedCursor, limit + 1));
            }
            catch (Exception ex)
            {
                context.Logger.LogWarning(
                    "KeyValueActor/RangeScan: read scheduler rejected next-page enqueue for prefix {Prefix} cursor {Cursor}: {Ex}",
                    prefix, diskCursor, ex.Message);
                Resolve(KeyValueStaticResponses.MustRetryResponse);
                return;
            }

            IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> self = context.ActorContext.Self;
            _ = nextTask.ContinueWith(t =>
            {
                if (!t.IsCompletedSuccessfully) SetFaulted();
                else ScanDiskResult = t.Result;
                self.Send(new KeyValueRequest(KeyValueRequestType.ResumeRead) { Continuation = this });
            }, TaskScheduler.Default);

            return; // mailbox is free until the next ResumeRead arrives
        }

        Resolve(BuildResponse(accumulated, limit, snapshotTs, prefix, durability));
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

    private KeyValueResponse? EvaluateKeySync(KeyValueContext context, string key, KeyValueEntry? entry, bool isResident)
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
        // could land at-or-before the snapshot timestamp we must wait. For non-snapshot
        // scans fall through to the committed state (the intent does not block the read).
        if (entry?.WriteIntent != null && entry.WriteIntent.TransactionId != transactionId)
        {
            if (entry.WriteIntent.Expires - currentTime <= TimeSpan.Zero)
                entry.WriteIntent = null;
            else if (!snapshotTs.IsNull())
            {
                HLCTimestamp commitTs = entry.WriteIntent.CommitTimestamp;
                if (commitTs.IsNull() || commitTs.CompareTo(snapshotTs) <= 0)
                    return KeyValueStaticResponses.WaitingForReplicationResponse;
            }
        }

        // Transactional MVCC: read-your-own-writes and OCC snapshot tracking.
        if (transactionId != HLCTimestamp.Zero)
        {
            if (entry is not null)
            {
                entry.MvccEntries ??= new();

                if (entry.MvccEntries.TryGetValue(transactionId, out KeyValueMvccEntry? existing))
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
            // For disk-only keys (isResident=false): skip MVCC creation entirely. The transient
            // entry is discarded after this call, so the MVCC entry would never participate in
            // OCC conflict detection. Writing it + calling AdjustEstimatedEntryBytes on an entry
            // not in the store leaks approximateStoreBytes without bound; fall through to the
            // committed-state path instead (correct read, no OCC tracking for non-resident keys).
            //
            // Residual edge: isResident reflects membership in the stage-1 memory snapshot, not
            // the current store. A key evicted by a Collect arriving between pages still has
            // isResident=true here, so the accounting call below runs on an entry that's no
            // longer in the store. The window is narrow (only keys resident at scan start, only
            // during a Collect between pages), but to close it entirely, replace the isResident
            // guard with context.Store.ContainsKey(key) at the accounting call site.
            if (!isSnapshotRead && isResident)
            {
                if (entry is null)
                    return null; // key does not exist; nothing to snapshot for OCC

                // Snapshot the current committed state for OCC conflict detection.
                KeyValueMvccEntry newMvcc = new()
                {
                    Value = entry.Value,
                    Revision = entry.Revision,
                    Expires = entry.Expires,
                    LastUsed = entry.LastUsed,
                    LastModified = entry.LastModified,
                    State = entry.State
                };

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
                    newMvcc.LastUsed, newMvcc.LastModified, entry.State));
            }
        }

        // Snapshot visibility: when the current revision was committed after the snapshot
        // timestamp, serve the most recent archived revision at-or-before the snapshot.
        // If no such revision exists (key was created after the snapshot, or the revision
        // was pruned), the key is invisible for this scan.
        if (!snapshotTs.IsNull() && entry is not null && entry.LastModified > snapshotTs)
        {
            if (!entry.TryGetRevisionAtOrBefore(snapshotTs, out long snapRevision, out KeyValueRevisionEntry snapshot))
                return null;

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
        KeyValueDurability durability)
    {
        bool hasMore = items.Count > limit;
        if (hasMore)
            items.RemoveAt(items.Count - 1);

        string? nextCursor = hasMore && items.Count > 0
            ? KeyValueRangeCursor.Encode(items[^1].Item1, durability, prefix, snapshotTs)
            : null;

        return new(KeyValueResponseType.Get, new KeyValueGetByRangeResult(KeyValueResponseType.Get, items, nextCursor, hasMore));
    }
}
