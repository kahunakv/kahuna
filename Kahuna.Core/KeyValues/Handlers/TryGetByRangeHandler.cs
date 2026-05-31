
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
            KeyValueResponse response = await Get(currentTime, message.TransactionId, key, message.Durability, snapshotTs);

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

            // EndKey enforcement for disk path (Bug 3): ComputeBounds passes memEnd to the
            // BTree but GetKeyValueByRange has no endKey parameter; enforce it here for both.
            if (message.EndKey is not null)
            {
                int cmpEnd = string.CompareOrdinal(keyToProcess, message.EndKey);
                if (cmpEnd > 0 || (!message.EndInclusive && cmpEnd == 0))
                    break;
            }

            KeyValueResponse response = await Get(currentTime, message.TransactionId, keyToProcess, message.Durability, snapshotTs, diskEntry);

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
    /// Resolves a single key with full MVCC/RYOW semantics, gated by a fixed snapshot timestamp.
    ///
    /// <paramref name="currentTime"/> is always the current HLC tick (used for expiry and intent
    /// timeout checks). <paramref name="readTimestamp"/> is the fixed scan snapshot:
    ///   • Zero  → "latest" — no snapshot filtering (legacy/non-paged behaviour).
    ///   • non-Zero → entries whose <see cref="KeyValueEntry.LastModified"/> is after the snapshot
    ///     are treated as invisible, *except* when the scanning transaction itself wrote them (RYOW).
    /// </summary>
    private async Task<KeyValueResponse> Get(
        HLCTimestamp currentTime,
        HLCTimestamp transactionId,
        string key,
        KeyValueDurability durability,
        HLCTimestamp readTimestamp,
        ReadOnlyKeyValueEntry? keyValueEntry = null)
    {
        KeyValueEntry? entry = await GetKeyValueEntry(key, durability, keyValueEntry);

        ReadOnlyKeyValueEntry readOnlyKeyValueEntry;

        if (entry?.ReplicationIntent is not null)
        {
            if (entry.ReplicationIntent.Expires - currentTime > TimeSpan.Zero)
                return KeyValueStaticResponses.WaitingForReplicationResponse;

            entry.ReplicationIntent = null;
        }

        if (entry?.WriteIntent != null)
        {
            if (entry.WriteIntent.TransactionId != transactionId)
            {
                if (entry.WriteIntent.Expires - currentTime > TimeSpan.Zero)
                    return KeyValueStaticResponses.MustRetryResponse;

                entry.WriteIntent = null;
            }
        }

        if (transactionId != HLCTimestamp.Zero)
        {
            if (entry is null)
            {
                entry = new() { Bucket = GetBucket(key), State = KeyValueState.Undefined, Revision = -1 };
                context.Store.Insert(key, entry);
            }

            entry.MvccEntries ??= new();

            if (!entry.MvccEntries.TryGetValue(transactionId, out KeyValueMvccEntry? mvccEntry))
            {
                // Snapshot check (MVCC path, no prior write by this tx).
                // entry.LastModified > readTimestamp means the *current* revision was committed
                // after the snapshot was captured.  The faithful behavior would be to return the
                // pre-snapshot value.  We cannot do that here because:
                //   • entry.Revisions (populated only for ephemeral keys by TrySetHandler) maps
                //     {revision_number → byte[]}, with NO per-revision timestamp.  Without a
                //     timestamp we cannot identify which revision was the last one ≤ readTimestamp.
                //   • For persistent keys entry.Revisions is null (TrySetHandler returns before
                //     the Revisions block for persistent durability).
                //   • There may have been multiple writes after the snapshot; the immediately
                //     previous revision might itself be post-snapshot.
                // Until a timestamp-versioned revision history (e.g. {revision, LastModified}[])
                // is added, we fall back to DoesNotExist — the key is treated as invisible rather
                // than returning a potentially misleading post-snapshot intermediate value.
                // TODO: add per-revision timestamps to entry.Revisions so the last revision
                //       whose LastModified ≤ readTimestamp can be served here.
                if (!readTimestamp.IsNull() && entry.LastModified > readTimestamp)
                    return KeyValueStaticResponses.DoesNotExistContextResponse;

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

        // Non-transactional snapshot visibility check (same limitation as the MVCC path above).
        // A key whose current revision was committed after the snapshot ideally returns the
        // pre-snapshot value, but entry.Revisions carries no per-revision timestamps so we
        // cannot recover it.  The key is treated as DoesNotExist for this page rather than
        // returning a post-snapshot or arbitrary historical value.
        // TODO: add per-revision timestamps to KeyValueEntry.Revisions and serve the last
        //       revision whose LastModified ≤ readTimestamp here.
        if (!readTimestamp.IsNull() && entry is not null && entry.LastModified > readTimestamp)
            return KeyValueStaticResponses.DoesNotExistContextResponse;

        if (entry is null || entry.State == KeyValueState.Deleted ||
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
