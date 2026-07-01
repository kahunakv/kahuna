
using Kahuna.Server.Persistence;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.KeyValues.Handlers;

/// <summary>
/// Stage-3 continuation for a persistent bucket (GetByBucket) scan.
///
/// Stage 1 collects all in-memory matches for the prefix and registers this continuation if
/// the disk store also needs to be consulted (i.e., the result cap was not reached from memory
/// alone). Stage 2 runs GetKeyValueByPrefix off-actor. Stage 3 (Execute) merges the disk page
/// against the current resident store — a write that landed during stage 2 is picked up by
/// re-checking the store for each disk-sourced key — then sorts and resolves all waiters.
///
/// Concurrent requests for the same prefix coalesce onto one disk read: later arrivals attach
/// their Promise via AddWaiter and receive the same merged result.
/// </summary>
internal sealed class BucketScanContinuation : ReadContinuation
{
    private readonly string prefix;
    private readonly HLCTimestamp transactionId;
    private readonly HLCTimestamp readTimestamp;
    private readonly List<(string, ReadOnlyKeyValueEntry)> inMemoryItems;
    private readonly HashSet<string> seenKeys;
    private readonly HLCTimestamp currentTime;

    internal BucketScanContinuation(
        string prefix,
        HLCTimestamp transactionId,
        HLCTimestamp readTimestamp,
        List<(string, ReadOnlyKeyValueEntry)> inMemoryItems,
        HashSet<string> seenKeys,
        HLCTimestamp currentTime,
        TaskCompletionSource<KeyValueResponse?> promise) : base(promise)
    {
        this.prefix = prefix;
        this.transactionId = transactionId;
        this.readTimestamp = readTimestamp;
        this.inMemoryItems = inMemoryItems;
        this.seenKeys = seenKeys;
        this.currentTime = currentTime;
    }

    internal override void Execute(KeyValueContext context)
    {
        context.PendingReads.Remove((prefix, -2L, false));

        if (Faulted)
        {
            Resolve(KeyValueStaticResponses.MustRetryResponse);
            return;
        }

        List<(string, ReadOnlyKeyValueEntry)> items = inMemoryItems;

        if (ScanDiskResult is not null)
        {
            foreach ((string key, ReadOnlyKeyValueEntry diskEntry) in ScanDiskResult)
            {
                if (items.Count >= KeyValueScanLimits.MaxPrefixScanResults)
                    break;

                if (seenKeys.Contains(key))
                    continue;

                // Reconcile: prefer the higher-revision entry between current store and disk.
                // A write may have committed while the disk read was in flight.
                KeyValueEntry entry;
                if (context.Store.TryGetValue(key, out KeyValueEntry? resident) &&
                    resident.Revision >= diskEntry.Revision)
                {
                    entry = resident;
                }
                else
                {
                    entry = BuildEntry(key, diskEntry);
                    context.InsertStoreEntry(key, entry);
                }

                KeyValueResponse? result = EvaluateEntry(
                    context, currentTime, transactionId, readTimestamp, key, entry);

                if (result is null || result.Type == KeyValueResponseType.DoesNotExist)
                    continue;

                if (result.Type != KeyValueResponseType.Get || result.Entry is null)
                {
                    // Abort the whole scan (WaitingForReplication, Aborted, etc.)
                    Resolve(new(result.Type, []));
                    return;
                }

                seenKeys.Add(key);
                items.Add((key, result.Entry));
            }
        }

        items.Sort(static (x, y) => string.Compare(x.Item1, y.Item1, StringComparison.Ordinal));
        Resolve(new(KeyValueResponseType.Get, items));
    }

    private static KeyValueEntry BuildEntry(string key, ReadOnlyKeyValueEntry disk) => new()
    {
        Bucket = GetBucket(key),
        Value = disk.Value,
        Revision = disk.Revision,
        FlushedRevision = disk.Revision,
        Expires = disk.Expires,
        LastUsed = disk.LastUsed,
        LastModified = disk.LastModified,
        State = disk.State
    };

    private static string? GetBucket(string key)
    {
        int index = key.LastIndexOf('/');
        return index == -1 ? null : key[..index];
    }

    /// <summary>
    /// Evaluates a resident KeyValueEntry against the MVCC/snapshot/state rules for a scan
    /// result row. Returns null to indicate the entry should be skipped (DoesNotExist in scan
    /// context), a scan-abort response (WaitingForReplication, Aborted) to terminate the whole
    /// scan, or a Get response to include the entry.
    ///
    /// Called from both stage 1 (in-memory rows) and stage 3 (reconciled disk rows), so it
    /// must run on the actor thread in both cases.
    /// </summary>
    internal static KeyValueResponse? EvaluateEntry(
        KeyValueContext context,
        HLCTimestamp currentTime,
        HLCTimestamp transactionId,
        HLCTimestamp readTimestamp,
        string key,
        KeyValueEntry entry)
    {
        if (entry.ReplicationIntent is not null)
        {
            if (entry.ReplicationIntent.Expires - currentTime > TimeSpan.Zero)
                return KeyValueStaticResponses.WaitingForReplicationResponse;
            entry.ReplicationIntent = null;
        }

        if (entry.WriteIntent != null && entry.WriteIntent.TransactionId != transactionId)
        {
            if (entry.WriteIntent.Expires - currentTime <= TimeSpan.Zero)
                entry.WriteIntent = null;
            else if (!readTimestamp.IsNull())
            {
                HLCTimestamp commitTs = entry.WriteIntent.CommitTimestamp;
                if (commitTs.IsNull() || commitTs.CompareTo(readTimestamp) <= 0)
                    return KeyValueStaticResponses.WaitingForReplicationResponse;
            }
        }

        if (transactionId != HLCTimestamp.Zero && readTimestamp.IsNull())
        {
            entry.MvccEntries ??= new();

            if (!entry.MvccEntries.TryGetValue(transactionId, out KeyValueMvccEntry? mvccEntry))
            {
                bool mvccDictJustCreated = entry.MvccEntries.Count == 0;
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
                context.AdjustEstimatedEntryBytes(
                    entry, KeyValueStoreAccounting.MvccEntryAddedBytes(mvccDictJustCreated, mvccEntry.Value));
            }

            if (entry.Revision > mvccEntry.Revision)
                return KeyValueStaticResponses.AbortedResponse;

            if (mvccEntry.State is KeyValueState.Undefined or KeyValueState.Deleted ||
                (mvccEntry.Expires != HLCTimestamp.Zero && mvccEntry.Expires - currentTime < TimeSpan.Zero))
                return null;

            return new(KeyValueResponseType.Get, new ReadOnlyKeyValueEntry(
                mvccEntry.Value, mvccEntry.Revision, mvccEntry.Expires,
                mvccEntry.LastUsed, mvccEntry.LastModified, entry.State));
        }

        if (!readTimestamp.IsNull() && entry.LastModified > readTimestamp)
        {
            if (!entry.TryGetRevisionAtOrBefore(
                    readTimestamp, out long snapRevision, out KeyValueRevisionEntry snapshot))
                return null;

            if (snapshot.State is KeyValueState.Deleted or KeyValueState.Undefined ||
                (snapshot.Expires != HLCTimestamp.Zero && snapshot.Expires - currentTime < TimeSpan.Zero))
                return null;

            return new(KeyValueResponseType.Get, new ReadOnlyKeyValueEntry(
                snapshot.Value, snapRevision, snapshot.Expires,
                currentTime, snapshot.LastModified, snapshot.State));
        }

        if (entry.State is KeyValueState.Undefined or KeyValueState.Deleted ||
            (entry.Expires != HLCTimestamp.Zero && entry.Expires - currentTime < TimeSpan.Zero))
            return null;

        context.TouchEntry(entry, currentTime);

        return new(KeyValueResponseType.Get, new ReadOnlyKeyValueEntry(
            entry.Value, entry.Revision, entry.Expires,
            entry.LastUsed, entry.LastModified, entry.State));
    }
}
