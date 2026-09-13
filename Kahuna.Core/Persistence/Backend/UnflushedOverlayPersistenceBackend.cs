
using Kahuna.Server.KeyValues;
using Kahuna.Server.Locks.Data;
using Kahuna.Server.Persistence.Pitr;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.Persistence.Backend;

/// <summary>
/// Decorates a persistence backend with the node's <see cref="UnflushedKeyValueWritesIndex"/> so
/// every key-value read — point, batched, by-revision, prefix and range scans — observes committed
/// writes that are queued for the background writer but not yet flushed. Without the overlay the
/// inner backend is behind the node's commit frontier between apply and flush, and a cache-missing
/// read (most visibly on a freshly promoted partition leader) would answer <c>DoesNotExist</c> for
/// a durably committed key — a terminal absence callers have no reason to retry.
///
/// <para>
/// The overlay always holds the newest queued head per key, so merging is newest-wins by
/// (revision, commit HLC). Deleted heads are surfaced with their <see cref="KeyValueState.Deleted"/>
/// state exactly as the inner backends surface persisted tombstones — callers already filter.
/// A confirmed <see cref="StoreKeyValues"/> prunes the flushed heads from the overlay; a failed
/// flush leaves them covered.
/// </para>
/// </summary>
internal sealed class UnflushedOverlayPersistenceBackend : IPersistenceBackend, IDisposable
{
    private readonly IPersistenceBackend inner;

    private readonly UnflushedKeyValueWritesIndex unflushedWrites;

    private readonly UnflushedLockWritesIndex unflushedLockWrites;

    public UnflushedOverlayPersistenceBackend(
        IPersistenceBackend inner,
        UnflushedKeyValueWritesIndex unflushedWrites,
        UnflushedLockWritesIndex unflushedLockWrites)
    {
        this.inner = inner;
        this.unflushedWrites = unflushedWrites;
        this.unflushedLockWrites = unflushedLockWrites;
    }

    /// <summary>The key-value overlay index producers record queued writes into.</summary>
    internal UnflushedKeyValueWritesIndex UnflushedWrites => unflushedWrites;

    /// <summary>The lock overlay index producers record queued lock mutations into.</summary>
    internal UnflushedLockWritesIndex UnflushedLockWrites => unflushedLockWrites;

    // Floors pass straight through: the overlay tracks unflushed rows, and a floor write happens
    // only after the rows it certifies flushed. Explicit forwarding is required — the interface's
    // default implementations would otherwise silently discard them.
    public bool StoreDurabilityFloors(IReadOnlyList<(int PartitionId, long Floor)> floors) =>
        inner.StoreDurabilityFloors(floors);

    public long GetDurabilityFloor(int partitionId) => inner.GetDurabilityFloor(partitionId);

    public bool RemoveDurabilityFloor(int partitionId) => inner.RemoveDurabilityFloor(partitionId);

    // Explicit forwarding is required — the interface's default implementation would otherwise
    // answer "no reset performed" without ever reaching the real engine.
    public bool TryRecoverFromStorageFailure() => inner.TryRecoverFromStorageFailure();

    public bool StoreLocks(List<PersistenceRequestItem> items)
    {
        bool stored = inner.StoreLocks(items);

        if (stored)
        {
            foreach (PersistenceRequestItem item in items)
                unflushedLockWrites.RemoveFlushed(
                    item.Key,
                    item.Revision,
                    new HLCTimestamp(item.LastModifiedNode, item.LastModifiedPhysical, item.LastModifiedCounter));
        }

        return stored;
    }

    public bool StoreKeyValues(List<PersistenceRequestItem> items)
    {
        bool stored = inner.StoreKeyValues(items);

        // Only a confirmed flush prunes the overlay; on failure the writer retains the items for
        // retry and the overlay keeps covering them.
        if (stored)
        {
            foreach (PersistenceRequestItem item in items)
                unflushedWrites.RemoveFlushed(
                    item.Key,
                    item.Revision,
                    new HLCTimestamp(item.LastModifiedNode, item.LastModifiedPhysical, item.LastModifiedCounter));
        }

        return stored;
    }

    public LockEntry? GetLock(string resource)
    {
        LockEntry? entry = inner.GetLock(resource);

        if (unflushedLockWrites.TryGet(resource, out UnflushedLockWrite queued)
            && (entry is null
                || !(entry.FencingToken > queued.FencingToken
                     || (entry.FencingToken == queued.FencingToken && entry.LastModified > queued.LastModified))))
            return new()
            {
                Owner = queued.Owner,
                FencingToken = queued.FencingToken,
                Expires = queued.Expires,
                LastUsed = queued.LastUsed,
                LastModified = queued.LastModified,
                State = queued.State
            };

        return entry;
    }

    public KeyValueEntry? GetKeyValue(string keyName)
    {
        KeyValueEntry? entry = inner.GetKeyValue(keyName);

        if (unflushedWrites.TryGet(keyName, out UnflushedKeyValueWrite queued)
            && (entry is null || !IsInnerNewer(entry, queued)))
            return Materialize(queued);

        return entry;
    }

    public KeyValueEntry?[] GetKeyValues(string[] keyNames)
    {
        KeyValueEntry?[] results = inner.GetKeyValues(keyNames);

        if (unflushedWrites.IsEmpty)
            return results;

        for (int i = 0; i < keyNames.Length; i++)
        {
            if (unflushedWrites.TryGet(keyNames[i], out UnflushedKeyValueWrite queued)
                && (results[i] is null || !IsInnerNewer(results[i]!, queued)))
                results[i] = Materialize(queued);
        }

        return results;
    }

    public KeyValueEntry? GetKeyValueRevision(string keyName, long revision)
    {
        // The overlay holds only the newest queued head; serve it on an exact revision match. A
        // no-revision write retains no history in the inner backend either, so it is excluded to
        // mirror the inner by-revision contract.
        if (unflushedWrites.TryGet(keyName, out UnflushedKeyValueWrite queued)
            && !queued.NoRevision && queued.Revision == revision)
            return Materialize(queued);

        return inner.GetKeyValueRevision(keyName, revision);
    }

    public KeyValueHydration GetKeyValueWithRecentRevisions(string keyName, int recentRevisions)
    {
        KeyValueHydration hydration = inner.GetKeyValueWithRecentRevisions(keyName, recentRevisions);

        if (!unflushedWrites.TryGet(keyName, out UnflushedKeyValueWrite queued)
            || (hydration.Head is not null && IsInnerNewer(hydration.Head, queued)))
            return hydration;

        // The queued write is the newest head. The inner head, if it holds a revision, becomes the
        // newest archived revision — the same shape a flush would leave behind. A no-revision queued
        // write retains no history, so the inner head is dropped rather than archived.
        KeyValueEntry queuedHead = Materialize(queued);
        List<KeyValueEntry> recent = new(hydration.RecentRevisions.Count + 1);
        if (hydration.Head is not null && !queued.NoRevision && hydration.Head.Revision < queuedHead.Revision)
            recent.Add(hydration.Head);
        recent.AddRange(hydration.RecentRevisions);
        if (recent.Count > recentRevisions)
            recent.RemoveRange(recentRevisions, recent.Count - recentRevisions);

        return new KeyValueHydration(queuedHead, recent);
    }

    public KeyValueEntry? GetKeyValueRevisionAtOrBefore(string keyName, long maxRevision, HLCTimestamp readTimestamp)
    {
        KeyValueEntry? best = inner.GetKeyValueRevisionAtOrBefore(keyName, maxRevision, readTimestamp);

        if (unflushedWrites.TryGet(keyName, out UnflushedKeyValueWrite queued)
            && !queued.NoRevision
            && queued.Revision <= maxRevision
            && queued.LastModified.CompareTo(readTimestamp) <= 0
            && (best is null || queued.Revision > best.Revision))
            return Materialize(queued);

        return best;
    }

    public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByPrefix(string prefixKeyName)
    {
        List<(string, ReadOnlyKeyValueEntry)> items = inner.GetKeyValueByPrefix(prefixKeyName);

        if (unflushedWrites.IsEmpty)
            return items;

        return MergeScan(
            items,
            unflushedWrites.Collect(prefixKeyName, null, KeyValueScanLimits.MaxPrefixScanResults, PageCeiling(items, KeyValueScanLimits.MaxPrefixScanResults)),
            KeyValueScanLimits.MaxPrefixScanResults);
    }

    public List<(string Key, ReadOnlyKeyValueEntry Current, ReadOnlyKeyValueEntry? Snapshot)> GetKeyValueByPrefixAtOrBefore(
        string prefixKeyName, HLCTimestamp readTimestamp, Func<bool>? shouldAbort = null)
    {
        List<(string Key, ReadOnlyKeyValueEntry Current, ReadOnlyKeyValueEntry? Snapshot)> items =
            inner.GetKeyValueByPrefixAtOrBefore(prefixKeyName, readTimestamp, shouldAbort);

        if (unflushedWrites.IsEmpty)
            return items;

        // The candidate selection is bounded exactly as for the latest-read scans: the page is the
        // first MaxPrefixScanResults keys of the ordinal union, whichever version each key resolves
        // to below. Only the selection is bounded; the per-key as-of merge is unchanged.
        string? ceiling = items.Count >= KeyValueScanLimits.MaxPrefixScanResults && items.Count > 0
            ? items[^1].Key
            : null;

        List<KeyValuePair<string, UnflushedKeyValueWrite>> queuedItems =
            unflushedWrites.Collect(prefixKeyName, null, KeyValueScanLimits.MaxPrefixScanResults, ceiling);
        if (queuedItems.Count == 0)
            return items;

        Dictionary<string, (ReadOnlyKeyValueEntry Current, ReadOnlyKeyValueEntry? Snapshot)> merged =
            new(items.Count + queuedItems.Count, StringComparer.Ordinal);

        foreach ((string key, ReadOnlyKeyValueEntry current, ReadOnlyKeyValueEntry? snapshot) in items)
            merged[key] = (current, snapshot);

        foreach ((string key, UnflushedKeyValueWrite queued) in queuedItems)
        {
            ReadOnlyKeyValueEntry queuedEntry = new(
                queued.Value, queued.Revision, queued.Expires, queued.LastUsed, queued.LastModified, queued.State);

            if (!merged.TryGetValue(key, out (ReadOnlyKeyValueEntry Current, ReadOnlyKeyValueEntry? Snapshot) existing))
            {
                // Key absent from the inner backend: the queued head is the only committed
                // version, and it is the as-of image exactly when it is at-or-before the snapshot.
                merged[key] = (queuedEntry,
                    queued.LastModified.CompareTo(readTimestamp) <= 0 ? queuedEntry : null);
                continue;
            }

            // Head merge is newest-wins by (revision, commit HLC), matching MergeScan.
            ReadOnlyKeyValueEntry current = existing.Current.Revision > queued.Revision
                || (existing.Current.Revision == queued.Revision && existing.Current.LastModified > queued.LastModified)
                ? existing.Current
                : queuedEntry;

            ReadOnlyKeyValueEntry? snapshot;
            if (current.LastModified.CompareTo(readTimestamp) <= 0)
            {
                // The merged head is itself at-or-before the snapshot, so it is the as-of image.
                snapshot = current;
            }
            else
            {
                // Head is newer than the snapshot: the queued head can improve the inner as-of
                // pick only when it qualifies as retained history — it retains a revision row
                // (not a no-revision write), sits at-or-before the snapshot, below the merged
                // head's revision, and above the inner backend's pick.
                snapshot = existing.Snapshot;
                if (!queued.NoRevision
                    && queued.LastModified.CompareTo(readTimestamp) <= 0
                    && queued.Revision < current.Revision
                    && (snapshot is null || queued.Revision > snapshot.Revision))
                    snapshot = queuedEntry;
            }

            merged[key] = (current, snapshot);
        }

        List<(string, ReadOnlyKeyValueEntry, ReadOnlyKeyValueEntry?)> result = new(
            Math.Min(merged.Count, KeyValueScanLimits.MaxPrefixScanResults));

        foreach (string key in merged.Keys.OrderBy(static k => k, StringComparer.Ordinal))
        {
            if (result.Count >= KeyValueScanLimits.MaxPrefixScanResults)
                break;
            (ReadOnlyKeyValueEntry current, ReadOnlyKeyValueEntry? snapshot) = merged[key];
            result.Add((key, current, snapshot));
        }

        return result;
    }

    public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByRange(string prefix, string? startKey, int limit)
    {
        List<(string, ReadOnlyKeyValueEntry)> items = inner.GetKeyValueByRange(prefix, startKey, limit);

        if (unflushedWrites.IsEmpty)
            return items;

        // Mirror the inner seek: start at the greater of prefix and startKey, inclusive.
        string? effectiveStart = startKey is not null && string.CompareOrdinal(startKey, prefix) > 0 ? startKey : null;

        return MergeScan(items, unflushedWrites.Collect(prefix, effectiveStart, limit, PageCeiling(items, limit)), limit);
    }

    /// <summary>
    /// The last key of a full inner page: no overlay key above it can enter a page of
    /// <paramref name="limit"/> rows, because the inner rows at or below it already fill the page.
    /// Null when the inner page is short (the inner side is exhausted, so every larger overlay key
    /// is still a candidate) or when the page size is unbounded.
    /// </summary>
    private static string? PageCeiling(List<(string, ReadOnlyKeyValueEntry)> items, int limit) =>
        limit >= 0 && limit < int.MaxValue && items.Count >= limit && items.Count > 0
            ? items[^1].Item1
            : null;

    // Whole-family scans are a physical-family primitive (replica seeding / un-host purging), not a
    // read-your-writes path: the opaque, stateless cursor cannot window the unflushed set without
    // double- or never-emitting its keys across pages, so the overlay is NOT merged here. Callers
    // that need completeness against the commit frontier drain the background writer first — the
    // scan contract on the interface states this.
    public KeyValueScanPage ScanKeyValues(string? cursor, int limit) => inner.ScanKeyValues(cursor, limit);

    public LockScanPage ScanLocks(string? cursor, int limit) => inner.ScanLocks(cursor, limit);

    // Physical removals pass through: the install/purge callers guarantee no writes for the
    // affected keys are queued (a seeding partition has no local applies; an un-hosted partition's
    // delivery has stopped), so there is no overlay state to reconcile.
    public bool DeleteKeyValues(IReadOnlyList<string> keys) => inner.DeleteKeyValues(keys);

    public bool DeleteLocks(IReadOnlyList<string> resources) => inner.DeleteLocks(resources);

    public bool PruneKeyValueRevisions(
        IReadOnlyCollection<string>? keys,
        int retentionCount,
        TimeSpan retentionAge,
        int batchSize,
        HLCTimestamp floorTimestamp,
        out RevisionPruneResult result) =>
        inner.PruneKeyValueRevisions(keys, retentionCount, retentionAge, batchSize, floorTimestamp, out result);

    // Forwarded explicitly: the interface's default implementation would drop the budget on the
    // floor and hand the inner backend an unbounded pass.
    public bool PruneKeyValueRevisions(
        IReadOnlyCollection<string>? keys,
        int retentionCount,
        TimeSpan retentionAge,
        int batchSize,
        HLCTimestamp floorTimestamp,
        TimeSpan timeBudget,
        out RevisionPruneResult result) =>
        inner.PruneKeyValueRevisions(keys, retentionCount, retentionAge, batchSize, floorTimestamp, timeBudget, out result);

    public CheckpointResult CreateCheckpoint(string destinationPath, long appliedIndex, HLCTimestamp appliedTime) =>
        inner.CreateCheckpoint(destinationPath, appliedIndex, appliedTime);

    public CheckpointResult CreateCheckpointAsOf(string destinationPath, long appliedIndex, HLCTimestamp cut, CancellationToken ct = default) =>
        inner.CreateCheckpointAsOf(destinationPath, appliedIndex, cut, ct);

    public bool SupportsExactAsOfCheckpoint => inner.SupportsExactAsOfCheckpoint;

    public HLCTimestamp GetPrunedHistoryFloor() => inner.GetPrunedHistoryFloor();

    public void Dispose()
    {
        if (inner is IDisposable disposable)
            disposable.Dispose();
    }

    /// <summary>
    /// Overlays queued heads onto a scan page: newest wins per key, results stay in ordinal key
    /// order, and the page size cap is preserved. Deleted heads are kept in the result with their
    /// state, matching how the inner backends surface persisted tombstones.
    ///
    /// <para>
    /// Both inputs are ordinal-ordered — the inner backends seek and iterate in key order, and
    /// <see cref="UnflushedKeyValueWritesIndex.Collect"/> sorts its bounded selection — so the union
    /// is a single two-way merge that allocates only the result and one entry per selected overlay
    /// row. An inner page that is not strictly ascending (a contract violation) is normalised first
    /// rather than merged wrongly.
    /// </para>
    /// </summary>
    private static List<(string, ReadOnlyKeyValueEntry)> MergeScan(
        List<(string, ReadOnlyKeyValueEntry)> diskItems,
        List<KeyValuePair<string, UnflushedKeyValueWrite>> queuedItems,
        int limit)
    {
        if (queuedItems.Count == 0)
            return diskItems;

        // Defensive: a caller-side unbounded page size must never become a negative capacity or an
        // instantly-exhausted page here.
        if (limit < 0)
            limit = int.MaxValue;

        if (!IsStrictlyAscending(diskItems))
        {
            KeyValueScanMetrics.MergePagesReordered.Add(1);
            diskItems = NormalizeDiskPage(diskItems);
        }

        int diskCount = diskItems.Count;
        int queuedCount = queuedItems.Count;
        long unionBound = (long)diskCount + queuedCount;
        List<(string, ReadOnlyKeyValueEntry)> result = new((int)Math.Min(unionBound, limit));

        int i = 0, j = 0;
        while (result.Count < limit && (i < diskCount || j < queuedCount))
        {
            if (j >= queuedCount)
            {
                result.Add(diskItems[i++]);
                continue;
            }

            if (i >= diskCount)
            {
                result.Add(MaterializeRow(queuedItems[j++]));
                continue;
            }

            int cmp = string.CompareOrdinal(diskItems[i].Item1, queuedItems[j].Key);
            if (cmp < 0)
            {
                result.Add(diskItems[i++]);
            }
            else if (cmp > 0)
            {
                result.Add(MaterializeRow(queuedItems[j++]));
            }
            else
            {
                (string key, ReadOnlyKeyValueEntry existing) = diskItems[i];
                UnflushedKeyValueWrite queued = queuedItems[j].Value;

                bool innerWins = existing.Revision > queued.Revision
                    || (existing.Revision == queued.Revision && existing.LastModified > queued.LastModified);

                result.Add(innerWins ? (key, existing) : MaterializeRow(queuedItems[j]));
                i++;
                j++;
            }
        }

        return result;
    }

    private static (string, ReadOnlyKeyValueEntry) MaterializeRow(in KeyValuePair<string, UnflushedKeyValueWrite> queued) =>
        (queued.Key, new ReadOnlyKeyValueEntry(
            queued.Value.Value, queued.Value.Revision, queued.Value.Expires, queued.Value.LastUsed,
            queued.Value.LastModified, queued.Value.State));

    private static bool IsStrictlyAscending(List<(string, ReadOnlyKeyValueEntry)> items)
    {
        for (int i = 1; i < items.Count; i++)
        {
            if (string.CompareOrdinal(items[i - 1].Item1, items[i].Item1) >= 0)
                return false;
        }

        return true;
    }

    /// <summary>Sorts an out-of-order inner page and collapses duplicate keys to their last row —
    /// the same result the previous dictionary-based union produced for such input.</summary>
    private static List<(string, ReadOnlyKeyValueEntry)> NormalizeDiskPage(List<(string, ReadOnlyKeyValueEntry)> items)
    {
        Dictionary<string, ReadOnlyKeyValueEntry> byKey = new(items.Count, StringComparer.Ordinal);
        foreach ((string key, ReadOnlyKeyValueEntry entry) in items)
            byKey[key] = entry;

        List<(string, ReadOnlyKeyValueEntry)> normalized = new(byKey.Count);
        foreach (KeyValuePair<string, ReadOnlyKeyValueEntry> kv in byKey)
            normalized.Add((kv.Key, kv.Value));

        normalized.Sort(static (a, b) => string.CompareOrdinal(a.Item1, b.Item1));
        return normalized;
    }

    /// <summary>Materialises a fresh entry per read — callers mutate returned entries and insert them into actor stores.</summary>
    private static KeyValueEntry Materialize(in UnflushedKeyValueWrite queued) => new()
    {
        Value = queued.Value,
        Revision = queued.Revision,
        Expires = queued.Expires,
        LastUsed = queued.LastUsed,
        LastModified = queued.LastModified,
        State = queued.State
    };

    private static bool IsInnerNewer(KeyValueEntry entry, in UnflushedKeyValueWrite queued) =>
        entry.Revision > queued.Revision
        || (entry.Revision == queued.Revision && entry.LastModified > queued.LastModified);
}
