
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
    public TryGetByRangeHandler(KeyValueContext context) : base(context) { }

    public async Task<KeyValueResponse> Execute(KeyValueRequest message)
    {
        if (message.Durability == KeyValueDurability.Ephemeral)
            return await GetByRangeEphemeral(message);

        return await GetByRangePersistent(message);
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

    // ── Persistent (k-way merge of memory + disk) ────────────────────────────

    private async Task<KeyValueResponse> GetByRangePersistent(KeyValueRequest message)
    {
        string prefix = message.Key;
        int limit = message.Limit;

        HLCTimestamp currentTime = context.Raft.HybridLogicalClock.TrySendOrLocalEvent(context.Raft.GetLocalNodeId());
        HLCTimestamp snapshotTs = message.ReadTimestamp.IsNull() ? currentTime : message.ReadTimestamp;

        (string memStart, bool memStartIncl, string? memEnd, bool memEndIncl) = ComputeBounds(message);

        // Step 1 — memory stream: lazy, no hard limit. The loop stops at limit+1 live items
        // or exhaustion. A hard limit+1 window would produce premature HasMore=false when
        // tombstones fill the window (same issue as ephemeral path above).
        IEnumerable<KeyValuePair<string, KeyValueEntry>> memStream =
            context.Store.GetByRange(memStart, memStartIncl, memEnd, memEndIncl, int.MaxValue);

        // Step 2 — disk: initial page. We fetch limit+1 items; if all limit+1 arrive the
        // backend has more. We keep all limit+1 in the list but only process the first limit
        // (diskAvailable). The (limit+1)-th item is the "peek" — the exact start key for the
        // next refill, so no item is ever skipped or double-counted.
        string diskCursor = message.StartKey ?? prefix;
        List<(string, ReadOnlyKeyValueEntry)> diskPage = await context.Raft.ReadScheduler.EnqueueTask(
            message.PartitionId,
            () => context.PersistenceBackend.GetKeyValueByRange(prefix, diskCursor, limit + 1));

        bool diskHasMore  = diskPage.Count > limit;
        int  diskAvailable = diskHasMore ? diskPage.Count - 1 : diskPage.Count;
        int  diskIdx       = 0;

        // Step 3 — K-way merge
        List<(string, ReadOnlyKeyValueEntry)> items = [];

        using IEnumerator<KeyValuePair<string, KeyValueEntry>> memEnum = memStream.GetEnumerator();
        bool memHasNext = memEnum.MoveNext();

        while (items.Count <= limit)
        {
            // Refill disk when the processable window is exhausted but more pages exist.
            // Start from the peek item so no key is skipped or duplicated.
            if (diskIdx >= diskAvailable && diskHasMore)
            {
                diskCursor  = diskPage[diskAvailable].Item1;
                diskPage    = await context.Raft.ReadScheduler.EnqueueTask(
                    message.PartitionId,
                    () => context.PersistenceBackend.GetKeyValueByRange(prefix, diskCursor, limit + 1));
                diskHasMore  = diskPage.Count > limit;
                diskAvailable = diskHasMore ? diskPage.Count - 1 : diskPage.Count;
                diskIdx      = 0;
            }

            string? memKey  = memHasNext ? memEnum.Current.Key : null;
            string? diskKey = diskIdx < diskAvailable ? diskPage[diskIdx].Item1 : null;

            if (memKey is null && diskKey is null)
                break;

            string keyToProcess;
            ReadOnlyKeyValueEntry? diskEntry = null;

            int cmp;
            if (memKey is null)       cmp = 1;
            else if (diskKey is null) cmp = -1;
            else cmp = string.CompareOrdinal(memKey, diskKey);

            if (cmp < 0)
            {
                keyToProcess = memKey!;
                memHasNext = memEnum.MoveNext();
            }
            else if (cmp > 0)
            {
                keyToProcess = diskKey!;
                diskEntry = diskPage[diskIdx].Item2;
                diskIdx++;
            }
            else
            {
                // same key: memory wins (has MVCC / RYOW), advance both
                keyToProcess = memKey!;
                memHasNext = memEnum.MoveNext();
                diskIdx++;
            }

            // Exclusive start — BTree handles this natively; apply the same skip for disk.
            if (!message.StartInclusive && message.StartKey is not null &&
                string.CompareOrdinal(keyToProcess, message.StartKey) == 0)
                continue;

            // EndKey enforcement for disk path: ComputeBounds passes memEnd to the
            // BTree but GetKeyValueByRange has no endKey parameter; enforce it here for both.
            if (message.EndKey is not null)
            {
                int cmpEnd = string.CompareOrdinal(keyToProcess, message.EndKey);
                if (cmpEnd > 0 || (!message.EndInclusive && cmpEnd == 0))
                    break;
            }

            KeyValueResponse response = await Get(currentTime, message.TransactionId, keyToProcess, message.Durability, snapshotTs, diskEntry, snapshotRead: !message.ReadTimestamp.IsNull());

            if (response.Type == KeyValueResponseType.DoesNotExist)
                continue;

            if (response.Type != KeyValueResponseType.Get || response.Entry is null)
                return new(response.Type, new KeyValueGetByRangeResult(response.Type, [], null, false));

            items.Add((keyToProcess, response.Entry));
        }

        return BuildResponse(prefix, message.Durability, items, limit, snapshotTs);
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

        // AS-OF snapshot scans (snapshotRead = true) skip the MVCC/RYOW block and fall through
        // to the non-transactional snapshot-visibility path below, which serves the revision
        // at-or-before readTimestamp via TryGetRevisionAtOrBefore — matching TryGetHandler's
        // point-read behaviour for the same (transactionId != Zero, readTimestamp != Zero) case.
        if (transactionId != HLCTimestamp.Zero && !snapshotRead)
        {
            // Read-only scan: never insert a placeholder (would mutate the BTree mid-enumeration
            // and pollute the read-through cache). A key absent from both memory and disk simply
            // does not exist at this snapshot. RYOW still holds: keys the transaction itself wrote
            // already live in the store with an MvccEntry and are returned via the branch below.
            if (entry is null)
                return KeyValueStaticResponses.DoesNotExistContextResponse;

            entry.MvccEntries ??= new();

            if (!entry.MvccEntries.TryGetValue(transactionId, out KeyValueMvccEntry? mvccEntry))
            {
                mvccEntry = new()
                {
                    Value = entry.Value,
                    Revision = entry.Revision,
                    Expires = entry.Expires,
                    LastUsed = entry.LastUsed,
                    LastModified = entry.LastModified,
                    State = entry.State
                };

                entry.MvccEntries.Add(transactionId, mvccEntry);
            }
            // If the MVCC entry already exists, the transaction wrote it: RYOW — always visible.

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

        entry.LastUsed = currentTime;

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
