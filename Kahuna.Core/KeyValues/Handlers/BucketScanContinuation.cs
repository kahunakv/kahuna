
using Kahuna.Server.Persistence;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
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
    private readonly (string, long, bool)? scanKey;

    /// <summary>Canonical decisions routed off-mailbox for the still-pending foreign intents this bucket meets
    /// (keyed by intent identity). Null on the first attempt; populated when the manager re-issues the scan after
    /// resolving a committed-but-unsettled remote-anchor intent so the overlay serves it instead of retrying.</summary>
    private readonly IReadOnlyDictionary<(HLCTimestamp TransactionId, long Epoch), TransactionDecision>? routedDecisions;

    /// <summary>
    /// Snapshot-projected disk rows resolved in stage 2. Populated only for snapshot scans
    /// (readTimestamp non-null); null for latest and transactional scans.
    ///
    /// For each key from the disk prefix page whose current revision was written after the
    /// snapshot timestamp, this holds the highest revision at-or-before the snapshot — the
    /// same projection <c>TryScanByPrefixFromDiskHandler</c> and the range-scan path compute.
    /// Disk rows already at-or-before the snapshot are stored here too (as-is, without a
    /// separate lookup) so the fallback can serve them when a resident entry supersedes the
    /// disk version and the in-memory revision archive has been trimmed past the snapshot.
    ///
    /// Read on the actor thread (stage 3). Written in stage 2 (off-actor) before the
    /// ResumeRead message is sent, so the Send establishes the happens-before edge.
    /// </summary>
    internal Dictionary<string, ReadOnlyKeyValueEntry>? SnapshotProjections { get; set; }

    internal BucketScanContinuation(
        string prefix,
        HLCTimestamp transactionId,
        HLCTimestamp readTimestamp,
        List<(string, ReadOnlyKeyValueEntry)> inMemoryItems,
        HashSet<string> seenKeys,
        HLCTimestamp currentTime,
        TaskCompletionSource<KeyValueResponse?> promise,
        (string, long, bool)? scanKey,
        IReadOnlyDictionary<(HLCTimestamp TransactionId, long Epoch), TransactionDecision>? routedDecisions = null) : base(promise)
    {
        this.prefix = prefix;
        this.transactionId = transactionId;
        this.readTimestamp = readTimestamp;
        this.inMemoryItems = inMemoryItems;
        this.seenKeys = seenKeys;
        this.currentTime = currentTime;
        this.scanKey = scanKey;
        this.routedDecisions = routedDecisions;
    }

    internal override void RemovePendingKey(KeyValueContext context)
    {
        // Only remove from PendingReads if this continuation was registered there.
        // Private (transactional / snapshot) continuations must not evict a concurrent
        // plain scan's registered entry that happens to share the same prefix.
        if (scanKey.HasValue)
            context.PendingReads.Remove(scanKey.Value);
    }

    internal override void Execute(KeyValueContext context)
    {
        RemovePendingKey(context);

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
                    context, currentTime, transactionId, readTimestamp, key, entry, SnapshotProjections);

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

        // Durable-intent bucket visibility: overlay prepared intents belonging to this bucket, so a committed
        // insert/override/delete not yet materialized is reflected exactly as it would be in the equivalent range
        // scan. No-op off the durable path.
        (items, bool mustRetry) = OverlayBucketIntents(context, prefix, items, currentTime, readTimestamp, transactionId, routedDecisions);
        if (mustRetry)
        {
            Resolve(KeyValueStaticResponses.MustRetryResponse);
            return;
        }

        Resolve(new(KeyValueResponseType.Get, items));
    }

    /// <summary>
    /// Reconciles a bucket scan's assembled page with the durable prepared intents belonging to the bucket: a
    /// committed intent overrides/injects, a committed delete or expired committed value excludes, an undecided
    /// in-bucket intent makes the scan retry. A bucket scan has no pagination cursor, so the merge is invoked with
    /// the scan's result cap as the limit (no "more" signal) and its capped item list is returned. No-op (returns
    /// the page unchanged) when the intent store is empty or no intent covers the bucket.
    /// </summary>
    internal static (List<(string, ReadOnlyKeyValueEntry)> Items, bool MustRetry) OverlayBucketIntents(
        KeyValueContext context,
        string? bucket,
        List<(string, ReadOnlyKeyValueEntry)> items,
        HLCTimestamp currentTime,
        HLCTimestamp readTimestamp,
        HLCTimestamp transactionId,
        IReadOnlyDictionary<(HLCTimestamp TransactionId, long Epoch), TransactionDecision>? routedDecisions = null)
    {
        if (context.PreparedIntentStore is not { } intentStore)
            return (items, false);

        IReadOnlyList<PreparedIntent> bucketIntents = intentStore.SnapshotBucket(bucket);
        if (bucketIntents.Count == 0)
            return (items, false);

        HLCTimestamp snapshotTs = readTimestamp.IsNull() ? currentTime : readTimestamp;

        PreparedIntentScanMerge.ScanMergeResult merge = PreparedIntentScanMerge.Merge(
            items, bucketIntents, snapshotTs, currentTime,
            limit: KeyValueScanLimits.MaxPrefixScanResults, kvHasMore: false, kvCeilingKey: null,
            i => DurableReadVisibility.ScanDecision(context, routedDecisions, i),
            k => DurableSnapshotSource.ReaderHasOwnVersion(context, k, transactionId));

        return (merge.Items, merge.MustRetry);
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
    /// Called from both stage 1 (in-memory rows, <paramref name="diskProjections"/> = null) and
    /// stage 3 (reconciled disk rows, <paramref name="diskProjections"/> carries the stage-2
    /// snapshot projections). Must run on the actor thread in both cases.
    ///
    /// For snapshot scans: when the in-memory revision archive misses
    /// (<c>TryGetRevisionAtOrBefore</c> returns false), the method falls back to the stage-2
    /// disk projection for the key. Purely memory-only keys with no disk history remain omitted.
    /// </summary>
    internal static KeyValueResponse? EvaluateEntry(
        KeyValueContext context,
        HLCTimestamp currentTime,
        HLCTimestamp transactionId,
        HLCTimestamp readTimestamp,
        string key,
        KeyValueEntry entry,
        Dictionary<string, ReadOnlyKeyValueEntry>? diskProjections = null)
    {
        if (entry.ReplicationIntent is not null)
        {
            if (entry.ReplicationIntent.Expires - currentTime > TimeSpan.Zero)
                return KeyValueStaticResponses.WaitingForReplicationResponse;
            entry.ReplicationIntent = null;
        }

        if (entry.WriteIntent != null && entry.WriteIntent.TransactionId != transactionId)
        {
            if (!KeyValueWriteIntentLease.IsLive(entry.WriteIntent, currentTime))
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
                // A committed-but-unsettled foreign intent supersedes the resident base: snapshot its committed
                // value so this transaction's later reads of the key stay consistent instead of binding a stale base.
                switch (DurableSnapshotSource.Resolve(context, key, transactionId, currentTime, out KeyValueMvccEntry intentSnapshot))
                {
                    case SnapshotDecision.Retry:
                        return KeyValueStaticResponses.WaitingForReplicationResponse;
                    case SnapshotDecision.UseIntent:
                        mvccEntry = intentSnapshot;
                        break;
                    default:
                        mvccEntry = new()
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

                bool mvccDictJustCreated = entry.MvccEntries.Count == 0;
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
                mvccEntry.LastUsed, mvccEntry.LastModified, mvccEntry.State));
        }

        if (!readTimestamp.IsNull() && entry.LastModified > readTimestamp)
        {
            if (!entry.TryGetRevisionAtOrBefore(
                    readTimestamp, out long snapRevision, out KeyValueRevisionEntry snapshot))
            {
                // In-memory revision archive does not reach as far back as readTimestamp.
                // Fall back to the stage-2 disk projection when available.
                // Purely memory-only keys (never flushed to disk) have no projection and remain omitted.
                if (diskProjections is not null && diskProjections.TryGetValue(key, out ReadOnlyKeyValueEntry? diskSnap))
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

        if (entry.State is KeyValueState.Undefined or KeyValueState.Deleted ||
            (entry.Expires != HLCTimestamp.Zero && entry.Expires - currentTime < TimeSpan.Zero))
            return null;

        context.TouchEntry(entry, currentTime);

        return new(KeyValueResponseType.Get, new ReadOnlyKeyValueEntry(
            entry.Value, entry.Revision, entry.Expires,
            entry.LastUsed, entry.LastModified, entry.State));
    }
}
