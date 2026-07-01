
using Kahuna.Server.Configuration;
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

    public Task<KeyValueResponse> Execute(KeyValueRequest message)
    {
        if (message.Durability == KeyValueDurability.Ephemeral)
            return GetByRangeEphemeral(message);

        return Task.FromResult(GetByRangePersistentStage1(message));
    }

    // ── Ephemeral (in-memory only) ────────────────────────────────────────────

    private async Task<KeyValueResponse> GetByRangeEphemeral(KeyValueRequest message)
    {
        string prefix = message.Key;
        int limit = message.Limit;

        // currentTime: always fresh — used for expiry / intent checks.
        HLCTimestamp currentTime = context.Raft.HybridLogicalClock.TrySendOrLocalEvent(context.Raft.GetLocalNodeId());

        // snapshotTs: captured once at page 0 and carried unchanged through the cursor.
        // Zero ⟹ "latest" (legacy / non-paged behaviour, no snapshot filtering).
        HLCTimestamp snapshotTs = message.ReadTimestamp.IsNull() ? currentTime : message.ReadTimestamp;

        (string memStart, bool memStartIncl, string? memEnd, bool memEndIncl) = ComputeBounds(message);

        List<(string, ReadOnlyKeyValueEntry)> items = [];

        // No hard limit on the BTree walk — the loop stops when we collect limit+1 live
        // items (HasMore sentinel) or the range is exhausted. A hard limit+1 window would
        // produce a premature HasMore=false whenever tombstones/expired entries fill the window.
        foreach ((string key, KeyValueEntry? _) in context.Store.GetByRange(memStart, memStartIncl, memEnd, memEndIncl, int.MaxValue))
        {
            KeyValueResponse response = await Get(currentTime, message.TransactionId, key, message.Durability, snapshotTs, snapshotRead: !message.ReadTimestamp.IsNull());

            if (response.Type == KeyValueResponseType.DoesNotExist)
                continue;

            if (response.Type != KeyValueResponseType.Get || response.Entry is null)
                return new(response.Type, new KeyValueGetByRangeResult(response.Type, [], null, false));

            items.Add((key, response.Entry));

            if (items.Count == limit + 1)
                break;
        }

        return BuildResponse(prefix, message.Durability, items, limit, snapshotTs);
    }

    // ── Persistent (K-way merge of memory + disk, detached off the actor mailbox) ──────────

    private KeyValueResponse GetByRangePersistentStage1(KeyValueRequest message)
    {
        string prefix = message.Key;
        int limit = message.Limit;

        HLCTimestamp currentTime = context.Raft.HybridLogicalClock.TrySendOrLocalEvent(context.Raft.GetLocalNodeId());
        HLCTimestamp snapshotTs = message.ReadTimestamp.IsNull() ? currentTime : message.ReadTimestamp;

        (string memStart, bool memStartIncl, string? memEnd, bool memEndIncl) = ComputeBounds(message);

        // Stage 1: snapshot a bounded batch of the matching in-memory entries (actor thread,
        // synchronous). Materialising the BTree range as a list before dispatching stage 2 avoids
        // holding a live iterator across actor messages: concurrent writes between pages would
        // cause BTree structural changes that could invalidate a deferred iterator. The copy is
        // capped at MemoryScanBatchSize (or limit+1 if larger) so a large resident range does not
        // turn this pre-check into O(resident-count) work; if the merge consumes the whole batch,
        // the continuation re-queries the next batch on the actor thread.
        int memBatch = Math.Max(limit + 1, MemoryScanBatchSize);
        List<(string, KeyValueEntry)> memItems = [];
        foreach (KeyValuePair<string, KeyValueEntry> kv in
            context.Store.GetByRange(memStart, memStartIncl, memEnd, memEndIncl, memBatch))
            memItems.Add((kv.Key, kv.Value));

        // Full batch ⟹ there may be more resident keys past the last one copied.
        bool memMaybeMore = memItems.Count == memBatch;

        IActorContext<KeyValueActor, KeyValueRequest, KeyValueResponse> actorContext = context.ActorContext;

        if (!actorContext.Reply.HasValue)
            return KeyValueStaticResponses.ErroredResponse;

        TaskCompletionSource<KeyValueResponse?> promise = actorContext.Reply.Value.Promise;

        string diskCursor = message.StartKey ?? prefix;

        // Route every disk page to the FairReadScheduler partition that owns this range, not
        // message.PartitionId (which the manager leaves at 0 for scans). Enqueuing scans under
        // partition 0 would collapse per-partition fairness/back-pressure and read-after-write
        // FIFO ordering onto one queue. The continuation reuses this for subsequent pages.
        int scanPartition = ResolvePartition(prefix);

        RangeScanContinuation cont = new(
            prefix, limit, message.Durability, scanPartition,
            message.StartInclusive, message.StartKey,
            message.EndKey, message.EndInclusive,
            message.TransactionId, snapshotTs, currentTime,
            isSnapshotRead: !message.ReadTimestamp.IsNull(),
            memItems, memEnd, memEndIncl, memBatch, memMaybeMore, diskCursor, promise);

        Task<List<(string, ReadOnlyKeyValueEntry)>> readTask;
        try
        {
            readTask = context.Raft.ReadScheduler.EnqueueTask(
                scanPartition,
                () => context.PersistenceBackend.GetKeyValueByRange(prefix, diskCursor, limit + 1));
        }
        catch (Exception ex)
        {
            context.Logger.LogWarning(
                "KeyValueActor/RangeScan: read scheduler rejected enqueue for prefix {Prefix}: {Ex}",
                prefix, ex.Message);
            cont.Resolve(KeyValueStaticResponses.MustRetryResponse);
            actorContext.ByPassReply = true;
            return KeyValueStaticResponses.MustRetryResponse;
        }

        _ = readTask.ContinueWith(t =>
        {
            if (!t.IsCompletedSuccessfully) cont.SetFaulted();
            else cont.ScanDiskResult = t.Result;
            actorContext.Self.Send(
                new KeyValueRequest(KeyValueRequestType.ResumeRead) { Continuation = cont });
        }, TaskScheduler.Default);

        actorContext.ByPassReply = true;
        return KeyValueStaticResponses.WaitingForReplicationResponse;
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /// <summary>Computes the effective BTree bounds, substituting the prefix upper bound when EndKey is null.</summary>
    private static (string start, bool startIncl, string? end, bool endIncl) ComputeBounds(KeyValueRequest message)
    {
        string start = message.StartKey ?? message.Key;
        bool startIncl = message.StartKey is null || message.StartInclusive;

        string? end = message.EndKey ?? GetPrefixUpperBound(message.Key);
        bool endIncl = message.EndKey is not null && message.EndInclusive;

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
        HLCTimestamp snapshotTs)
    {
        bool hasMore = items.Count > limit;
        if (hasMore)
            items.RemoveAt(items.Count - 1);

        string? nextCursor = hasMore && items.Count > 0
            ? KeyValueRangeCursor.Encode(items[^1].Item1, durability, prefix, snapshotTs)
            : null;

        KeyValueGetByRangeResult result = new(KeyValueResponseType.Get, items, nextCursor, hasMore);
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
        // at-or-before that snapshot (safe-time wait).
        // Clear expired intents as housekeeping; otherwise apply the safe-time decision:
        //   CommitTimestamp == Zero  → undetermined — must wait (WaitingForReplication to retry the page).
        //   CommitTimestamp ≤ T     → might commit in our snapshot window — must wait.
        //   CommitTimestamp > T     → provably outside our snapshot — fall through to committed state.
        if (entry?.WriteIntent != null)
        {
            if (entry.WriteIntent.TransactionId != transactionId)
            {
                if (entry.WriteIntent.Expires - currentTime <= TimeSpan.Zero)
                    entry.WriteIntent = null;
                else if (!readTimestamp.IsNull())
                {
                    HLCTimestamp commitTs = entry.WriteIntent.CommitTimestamp;
                    if (commitTs.IsNull() || commitTs.CompareTo(readTimestamp) <= 0)
                        return KeyValueStaticResponses.WaitingForReplicationResponse;
                    // commitTs > readTimestamp: write won't land in our snapshot; fall through
                }
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

                // entry.MvccEntries is already initialised (??= new() in the block above).
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
                    entry.State);

                return new(KeyValueResponseType.Get, readOnlyKeyValueEntry);
            }
            // else (snapshotRead, no own write): fall through to snapshot-visibility path below.
        }

        // Non-transactional snapshot visibility check.
        // When the current revision was committed after the snapshot, serve the most recent
        // archived revision whose LastModified ≤ readTimestamp (snapshot isolation) instead of
        // dropping the key. If no such revision is retained (key didn't exist at the snapshot,
        // or the revision was trimmed / lives only on disk), the key is invisible for this page.
        if (!readTimestamp.IsNull() && entry is not null && entry.LastModified > readTimestamp)
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
