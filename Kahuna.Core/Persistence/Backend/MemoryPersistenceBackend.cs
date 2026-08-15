
using System.Text.Json;
using Kahuna.Server.KeyValues;
using Kahuna.Server.Locks;
using Kahuna.Server.Locks.Data;
using Kahuna.Server.Persistence.Pitr;
using Kommander.Time;
using System.Collections.Concurrent;
using System.Runtime.InteropServices;

namespace Kahuna.Server.Persistence.Backend;

/// <summary>
/// Provides an in-memory implementation of the <see cref="IPersistenceBackend"/> interface
/// to store locks and key-value pairs without the use of persistent storage.
/// </summary>
/// <remarks>
/// This backend is used for tests and in-memory deployments. keyValues is backed by a
/// SortedList so GetKeyValueByPrefix and GetKeyValueByRange can binary-search to the
/// start position and iterate O(log N + k) instead of sorting the whole dictionary on
/// every call.
/// </remarks>
internal sealed class MemoryPersistenceBackend : IPersistenceBackend, IDisposable
{
    private readonly ConcurrentDictionary<string, LockEntry> locks = new();

    // SortedList gives array-backed ordered keys — binary-search for range start, then
    // iterate forward. Not thread-safe; guarded by kvLock.
    // Trade-offs vs. the old ConcurrentDictionary:
    //   Inserts of new keys: O(N) array shift (was O(1)). Low write volume in tests makes
    //     this acceptable; switch to BTree<string,...> if sustained writes become a concern.
    //   Point reads (GetKeyValue): now take kvLock (were lock-free). In practice reads are
    //     serialized through ReadScheduler.EnqueueTask anyway, so contention is bounded; in
    //     a true in-memory production deployment this would be a read-scalability narrowing.
    private readonly SortedList<string, KeyValueEntry> keyValues = new(StringComparer.Ordinal);
    
    private readonly Lock kvLock = new();

    private readonly ConcurrentDictionary<string, ConcurrentDictionary<long, KeyValueEntry>> keyValueRevisions = new();

    /// <summary>
    /// Per-key provenance for no-revision writes: the earliest and latest HLC at which the key was
    /// written with SetNoRevision. A no-revision write leaves no retained history row, so an as-of
    /// read whose boundary falls on an overwritten no-revision value cannot reconstruct it — this
    /// lets <see cref="CreateCheckpointAsOf"/> fail closed instead of silently returning a stale
    /// revision or omitting the key. Guarded by <see cref="kvLock"/>. Rebuilt naturally on WAL replay
    /// since every write flows through <see cref="StoreKeyValues"/>.
    /// </summary>
    private readonly Dictionary<string, (HLCTimestamp Earliest, HLCTimestamp Latest)> noRevisionWrites =
        new(StringComparer.Ordinal);

    /// <summary>
    /// Stores locks in the persistence backend. Updates existing locks or adds new ones
    /// based on the provided list of persistence request items.
    /// </summary>
    /// <param name="items">A list of <see cref="PersistenceRequestItem"/> containing the lock data to be stored or updated.</param>
    /// <returns>Returns <c>true</c> if the locks were successfully stored or updated.</returns>
    /// <summary>
    /// Per-partition application-durability floors. The memory backend has no real durability, but
    /// mirroring the contract keeps embedded/test topologies on the same code path as disk backends.
    /// </summary>
    private readonly ConcurrentDictionary<int, long> durabilityFloors = new();

    public bool StoreDurabilityFloors(IReadOnlyList<(int PartitionId, long Floor)> floors)
    {
        foreach ((int partitionId, long floor) in floors)
            durabilityFloors.AddOrUpdate(partitionId, floor, (_, existing) => Math.Max(existing, floor));

        return true;
    }

    public long GetDurabilityFloor(int partitionId) =>
        durabilityFloors.TryGetValue(partitionId, out long floor) ? floor : -1;

    public bool RemoveDurabilityFloor(int partitionId)
    {
        durabilityFloors.TryRemove(partitionId, out _);
        return true;
    }

    public bool StoreLocks(List<PersistenceRequestItem> items)
    {
        foreach (ref readonly PersistenceRequestItem item in CollectionsMarshal.AsSpan(items))
        {
            if (locks.TryGetValue(item.Key, out LockEntry? lockContext))
            {
                lockContext.Owner = item.Value;
                lockContext.Expires = new(item.ExpiresNode, item.ExpiresPhysical, item.ExpiresCounter);
                lockContext.FencingToken = item.Revision;
                lockContext.LastUsed = new(item.LastUsedNode, item.LastUsedPhysical, item.LastUsedCounter);
                lockContext.LastModified = new(item.LastModifiedNode, item.LastModifiedPhysical, item.LastModifiedCounter);
                lockContext.State = (LockState)item.State;
            }
            else
            {
                locks.TryAdd(item.Key, new()
                {
                    Owner = item.Value,
                    FencingToken = item.Revision,
                    Expires = new(item.ExpiresNode, item.ExpiresPhysical, item.ExpiresCounter),
                    LastUsed = new(item.LastUsedNode, item.LastUsedPhysical, item.LastUsedCounter),
                    LastModified = new(item.LastModifiedNode, item.LastModifiedPhysical, item.LastModifiedCounter),
                    State = (LockState)item.State
                });
            }
        }

        return true;
    }

    /// <summary>
    /// Stores key-value pairs in the memory persistence backend. Updates existing entries or adds new ones
    /// based on the provided list of persistence request items.
    /// </summary>
    /// <param name="items">A list of <see cref="PersistenceRequestItem"/> containing the key-value data to be stored or updated.</param>
    /// <returns>Returns <c>true</c> if the key-value pairs were successfully stored or updated.</returns>
    public bool StoreKeyValues(List<PersistenceRequestItem> items)
    {
        lock (kvLock)
        {
            foreach (ref readonly PersistenceRequestItem item in CollectionsMarshal.AsSpan(items))
            {
                if (keyValues.TryGetValue(item.Key, out KeyValueEntry? existing))
                {
                    existing.Value = item.Value;
                    existing.Expires = new(item.ExpiresNode, item.ExpiresPhysical, item.ExpiresCounter);
                    existing.Revision = item.Revision;
                    existing.LastUsed = new(item.LastUsedNode, item.LastUsedPhysical, item.LastUsedCounter);
                    existing.LastModified = new(item.LastModifiedNode, item.LastModifiedPhysical, item.LastModifiedCounter);
                    existing.State = (KeyValueState)item.State;
                }
                else
                {
                    keyValues.Add(item.Key, new()
                    {
                        Value = item.Value,
                        Revision = item.Revision,
                        Expires = new(item.ExpiresNode, item.ExpiresPhysical, item.ExpiresCounter),
                        LastUsed = new(item.LastUsedNode, item.LastUsedPhysical, item.LastUsedCounter),
                        LastModified = new(item.LastModifiedNode, item.LastModifiedPhysical, item.LastModifiedCounter),
                        State = (KeyValueState)item.State
                    });
                }

                // Record no-revision provenance: the earliest/latest HLC this key was written without
                // retaining history. Used by the as-of checkpoint to fail closed when a cut's boundary
                // could be an overwritten no-revision value.
                if (item.NoRevision)
                {
                    HLCTimestamp writeHlc = new(item.LastModifiedNode, item.LastModifiedPhysical, item.LastModifiedCounter);
                    if (noRevisionWrites.TryGetValue(item.Key, out (HLCTimestamp Earliest, HLCTimestamp Latest) span))
                        noRevisionWrites[item.Key] = (
                            span.Earliest == HLCTimestamp.Zero || writeHlc.CompareTo(span.Earliest) < 0 ? writeHlc : span.Earliest,
                            writeHlc.CompareTo(span.Latest) > 0 ? writeHlc : span.Latest);
                    else
                        noRevisionWrites[item.Key] = (writeHlc, writeHlc);
                }

                // Store an independent snapshot per revision — NOT a reference to the shared current
                // entry, which is mutated in place on every later write (that aliasing would make all
                // revisions report the latest values, breaking historical/snapshot reads).
                // Skipped for no-revision writes; only keyValues (the current-value store) is updated.
                if (!item.NoRevision)
                {
                    ConcurrentDictionary<long, KeyValueEntry> revisions = keyValueRevisions.GetOrAdd(item.Key, _ => new());
                    revisions[item.Revision] = new()
                    {
                        Value = item.Value,
                        Revision = item.Revision,
                        Expires = new(item.ExpiresNode, item.ExpiresPhysical, item.ExpiresCounter),
                        LastUsed = new(item.LastUsedNode, item.LastUsedPhysical, item.LastUsedCounter),
                        LastModified = new(item.LastModifiedNode, item.LastModifiedPhysical, item.LastModifiedCounter),
                        State = (KeyValueState)item.State
                    };
                }
            }
        }

        return true;
    }

    public LockEntry? GetLock(string resource)
    {
        return locks.GetValueOrDefault(resource);
    }

    public KeyValueEntry? GetKeyValue(string keyName)
    {
        lock (kvLock)
        {
            keyValues.TryGetValue(keyName, out KeyValueEntry? entry);
            return entry;
        }
    }

    public KeyValueEntry? GetKeyValueRevision(string keyName, long revision)
    {
        if (keyValueRevisions.TryGetValue(keyName, out ConcurrentDictionary<long, KeyValueEntry>? revisions) &&
            revisions.TryGetValue(revision, out KeyValueEntry? entry))
            return entry;

        return null;
    }

    public KeyValueEntry? GetKeyValueRevisionAtOrBefore(string keyName, long maxRevision, HLCTimestamp readTimestamp)
    {
        if (!keyValueRevisions.TryGetValue(keyName, out ConcurrentDictionary<long, KeyValueEntry>? revisions))
            return null;

        KeyValueEntry? best = null;
        long bestRevision = -1;

        foreach (KeyValuePair<long, KeyValueEntry> kv in revisions)
        {
            long rev = kv.Key;
            KeyValueEntry entry = kv.Value;

            if (rev <= maxRevision && entry.LastModified.CompareTo(readTimestamp) <= 0 && rev > bestRevision)
            {
                best = entry;
                bestRevision = rev;
            }
        }

        return best;
    }

    /// <summary>
    /// Retrieves a list of key-value pairs where the key starts with the specified prefix.
    /// The values are returned in a read-only context.
    /// </summary>
    /// <param name="prefixKeyName">The prefix used to filter keys in the key-value store.</param>
    /// <returns>Returns a list of tuples where each tuple contains a key and its associated <see cref="ReadOnlyKeyValueEntry"/>.</returns>
    public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByPrefix(string prefixKeyName)
    {
        List<(string, ReadOnlyKeyValueEntry)> items = [];

        lock (kvLock)
        {
            IList<string> keys = keyValues.Keys;
            int start = LowerBound(keys, prefixKeyName);

            for (int i = start; i < keys.Count && items.Count < KeyValueScanLimits.MaxPrefixScanResults; i++)
            {
                string key = keys[i];
                if (!key.StartsWith(prefixKeyName, StringComparison.Ordinal))
                    break;

                KeyValueEntry value = keyValues.Values[i];
                items.Add((key, new(
                    value.Value,
                    value.Revision,
                    value.Expires,
                    value.LastUsed,
                    value.LastModified,
                    value.State
                )));
            }
        }

        return items;
    }

    /// <summary>
    /// Retrieves a bounded, ordered page of key-value pairs whose keys start with <paramref name="prefix"/>,
    /// beginning at <paramref name="startKey"/> (or the prefix start if null), up to <paramref name="limit"/> entries.
    /// </summary>
    public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByRange(string prefix, string? startKey, int limit)
    {
        List<(string, ReadOnlyKeyValueEntry)> items = [];

        lock (kvLock)
        {
            IList<string> keys = keyValues.Keys;
            // Seek to the greater of prefix and startKey so a startKey that sorts before
            // the prefix block doesn't land outside it and return an empty result.
            string seekTarget = startKey is not null && string.CompareOrdinal(startKey, prefix) > 0
                ? startKey : prefix;
            int start = LowerBound(keys, seekTarget);

            for (int i = start; i < keys.Count && items.Count < limit; i++)
            {
                string key = keys[i];
                if (!key.StartsWith(prefix, StringComparison.Ordinal))
                    break;

                KeyValueEntry value = keyValues.Values[i];
                items.Add((key, new(
                    value.Value,
                    value.Revision,
                    value.Expires,
                    value.LastUsed,
                    value.LastModified,
                    value.State
                )));
            }
        }

        return items;
    }

    /// <summary>
    /// Whole-family paged scan of the current key-value rows: keys strictly greater than the
    /// cursor (which is simply the last key of the previous page), in ordinal order.
    /// </summary>
    public KeyValueScanPage ScanKeyValues(string? cursor, int limit)
    {
        List<(string, ReadOnlyKeyValueEntry)> items = [];

        lock (kvLock)
        {
            IList<string> keys = keyValues.Keys;

            int start = 0;
            if (cursor is not null)
            {
                start = LowerBound(keys, cursor);
                if (start < keys.Count && string.CompareOrdinal(keys[start], cursor) == 0)
                    start++;
            }

            for (int i = start; i < keys.Count && items.Count < limit; i++)
            {
                KeyValueEntry value = keyValues.Values[i];
                items.Add((keys[i], new(
                    value.Value,
                    value.Revision,
                    value.Expires,
                    value.LastUsed,
                    value.LastModified,
                    value.State
                )));
            }
        }

        return new(items, items.Count == limit ? items[^1].Item1 : null);
    }

    /// <summary>
    /// Whole-family paged scan of the lock rows. The lock table is unordered, so each page
    /// snapshots and sorts the resource set — O(N log N) per page, acceptable for this
    /// test/embedded backend.
    /// </summary>
    public LockScanPage ScanLocks(string? cursor, int limit)
    {
        List<string> resources = [.. locks.Keys];
        resources.Sort(StringComparer.Ordinal);

        List<(string, LockEntry)> items = [];

        foreach (string resource in resources)
        {
            if (items.Count >= limit)
                break;

            if (cursor is not null && string.CompareOrdinal(resource, cursor) <= 0)
                continue;

            // A concurrently removed resource is simply skipped.
            if (locks.TryGetValue(resource, out LockEntry? entry))
                items.Add((resource, entry));
        }

        return new(items, items.Count == limit ? items[^1].Item1 : null);
    }

    /// <summary>
    /// Physically removes each key's current row, revision history and no-revision provenance.
    /// </summary>
    public bool DeleteKeyValues(IReadOnlyList<string> keys)
    {
        lock (kvLock)
        {
            foreach (string key in keys)
            {
                keyValues.Remove(key);
                noRevisionWrites.Remove(key);
                keyValueRevisions.TryRemove(key, out _);
            }
        }

        return true;
    }

    /// <summary>Physically removes each lock resource's row.</summary>
    public bool DeleteLocks(IReadOnlyList<string> resources)
    {
        foreach (string resource in resources)
            locks.TryRemove(resource, out _);

        return true;
    }

    /// <summary>
    /// Returns the index of the first key >= <paramref name="target"/> in the sorted key list,
    /// or keys.Count if all keys are smaller.
    /// </summary>
    private static int LowerBound(IList<string> keys, string target)
    {
        int lo = 0, hi = keys.Count;
        while (lo < hi)
        {
            int mid = (lo + hi) >>> 1;
            if (string.CompareOrdinal(keys[mid], target) < 0)
                lo = mid + 1;
            else
                hi = mid;
        }
        return lo;
    }

    public bool PruneKeyValueRevisions(
        IReadOnlyCollection<string>? keys,
        int retentionCount,
        TimeSpan retentionAge,
        int batchSize,
        HLCTimestamp floorTimestamp,
        out RevisionPruneResult result)
    {
        result = new(0, 0, BatchLimitReached: false);
        return true;
    }

    public bool SupportsExactAsOfCheckpoint => true;

    public CheckpointResult CreateCheckpoint(string destinationPath, long appliedIndex, HLCTimestamp appliedTime)
    {
        List<MemoryCheckpointEntry> kvEntries;
        lock (kvLock)
        {
            kvEntries = new(keyValues.Count);
            foreach (KeyValuePair<string, KeyValueEntry> kv in keyValues)
                kvEntries.Add(ToCheckpointEntry(kv.Key, kv.Value));
        }

        return WriteCheckpoint(destinationPath, appliedIndex, appliedTime, kvEntries, includeLocks: true);
    }

    /// <summary>
    /// Writes an exact as-of-<paramref name="cut"/> checkpoint: for each key the newest revision with
    /// <c>LastModified ≤ cut</c> (resolved from retained revision history), omitting keys whose entire
    /// history is newer than the cut. Keys that carry no revision history (NoRevision writes) are
    /// included only if their latest value is itself ≤ the cut.
    /// </summary>
    public CheckpointResult CreateCheckpointAsOf(
        string destinationPath, long appliedIndex, HLCTimestamp cut, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();

        List<MemoryCheckpointEntry> kvEntries;
        lock (kvLock)
        {
            kvEntries = new(keyValues.Count);
            foreach (KeyValuePair<string, KeyValueEntry> kv in keyValues)
            {
                // The current value is the latest write. If it is at or before the cut, it is the
                // exact as-of state regardless of how it was written (revisioned or SetNoRevision) —
                // nothing newer exists. This is what makes a revisioned→no-revision key correct: a cut
                // after the no-revision write returns that value, not a stale older revision.
                if (kv.Value.LastModified.CompareTo(cut) <= 0)
                {
                    kvEntries.Add(ToCheckpointEntry(kv.Key, kv.Value));
                    continue;
                }

                // The current value is after the cut. The boundary is the newest write at or before
                // the cut — a retained revision if one exists, but possibly an overwritten
                // no-revision value that left no history row.
                KeyValueEntry? boundary = GetKeyValueRevisionAtOrBefore(kv.Key, long.MaxValue, cut);
                HLCTimestamp boundaryHlc = boundary?.LastModified ?? HLCTimestamp.Zero;

                // Fail closed when a no-revision write at or before the cut is newer than the newest
                // retained revision at/before the cut: its value was overwritten and cannot be
                // reconstructed, so it — not the older revision — may be the true as-of state.
                if (noRevisionWrites.TryGetValue(kv.Key, out (HLCTimestamp Earliest, HLCTimestamp Latest) span)
                    && span.Earliest.CompareTo(cut) <= 0
                    && span.Latest.CompareTo(boundaryHlc) > 0)
                    throw new ExactCheckpointUnavailableException(
                        $"Key '{kv.Key}' has a SetNoRevision write in its as-of-{cut} boundary window whose " +
                        "value was overwritten and cannot be reconstructed; the cut cannot be produced exactly.");

                // Otherwise the newest retained revision at/before the cut is the exact state; a key
                // whose entire history is newer than the cut is correctly omitted.
                if (boundary is not null)
                    kvEntries.Add(ToCheckpointEntry(kv.Key, boundary));
            }
        }

        // Locks are excluded from as-of images: they are volatile lease/coordination state with no
        // history, so a physical snapshot would leak stale/post-cut locks. A restored node re-derives
        // lock state at runtime (from the cluster / re-acquisition).
        return WriteCheckpoint(destinationPath, appliedIndex, cut, kvEntries, includeLocks: false);
    }

    private static MemoryCheckpointEntry ToCheckpointEntry(string key, KeyValueEntry e) => new()
    {
        Key = key,
        Value = e.Value,
        Revision = e.Revision,
        ExpiresNode = e.Expires.N,
        ExpiresPhysical = e.Expires.L,
        ExpiresCounter = e.Expires.C,
        LastUsedNode = e.LastUsed.N,
        LastUsedPhysical = e.LastUsed.L,
        LastUsedCounter = e.LastUsed.C,
        LastModifiedNode = e.LastModified.N,
        LastModifiedPhysical = e.LastModified.L,
        LastModifiedCounter = e.LastModified.C,
        State = (int)e.State
    };

    private CheckpointResult WriteCheckpoint(
        string destinationPath, long appliedIndex, HLCTimestamp appliedTime,
        List<MemoryCheckpointEntry> kvEntries, bool includeLocks)
    {
        string tmpPath = destinationPath + ".tmp_" + Guid.NewGuid().ToString("N")[..8];
        Directory.CreateDirectory(tmpPath);

        try
        {
            List<MemoryCheckpointLockEntry> lockEntries = [];
            foreach (KeyValuePair<string, LockEntry> kv in locks)
            {
                if (!includeLocks)
                    break;
                LockEntry l = kv.Value;
                lockEntries.Add(new()
                {
                    Resource = kv.Key,
                    Owner = l.Owner,
                    FencingToken = l.FencingToken,
                    ExpiresNode = l.Expires.N,
                    ExpiresPhysical = l.Expires.L,
                    ExpiresCounter = l.Expires.C,
                    LastUsedNode = l.LastUsed.N,
                    LastUsedPhysical = l.LastUsed.L,
                    LastUsedCounter = l.LastUsed.C,
                    LastModifiedNode = l.LastModified.N,
                    LastModifiedPhysical = l.LastModified.L,
                    LastModifiedCounter = l.LastModified.C,
                    State = (int)l.State
                });
            }

            File.WriteAllText(Path.Combine(tmpPath, "store.json"), JsonSerializer.Serialize(kvEntries));
            File.WriteAllText(Path.Combine(tmpPath, "locks.json"), JsonSerializer.Serialize(lockEntries));

            CheckpointManifest manifest = CheckpointManifest.From(appliedIndex, appliedTime);
            manifest.WriteTo(tmpPath);

            Directory.Move(tmpPath, destinationPath);

            return new(destinationPath, manifest);
        }
        catch
        {
            if (Directory.Exists(tmpPath))
                Directory.Delete(tmpPath, recursive: true);
            throw;
        }
    }

    /// <summary>
    /// Opens an existing memory-backend checkpoint written by <see cref="CreateCheckpoint"/>
    /// as a read-only (non-writable) in-memory store.
    /// </summary>
    public static MemoryPersistenceBackend OpenCheckpoint(string checkpointPath)
    {
        MemoryPersistenceBackend backend = new();

        List<MemoryCheckpointEntry>? kvEntries = JsonSerializer.Deserialize<List<MemoryCheckpointEntry>>(
            File.ReadAllText(Path.Combine(checkpointPath, "store.json")));

        if (kvEntries is { Count: > 0 })
        {
            List<PersistenceRequestItem> items = new(kvEntries.Count);
            foreach (MemoryCheckpointEntry e in kvEntries)
            {
                items.Add(new(
                    e.Key, e.Value, e.Revision,
                    e.ExpiresNode, e.ExpiresPhysical, e.ExpiresCounter,
                    e.LastUsedNode, e.LastUsedPhysical, e.LastUsedCounter,
                    e.LastModifiedNode, e.LastModifiedPhysical, e.LastModifiedCounter,
                    e.State
                ));
            }
            backend.StoreKeyValues(items);
        }

        string locksFile = Path.Combine(checkpointPath, "locks.json");
        if (File.Exists(locksFile))
        {
            List<MemoryCheckpointLockEntry>? lockEntries =
                JsonSerializer.Deserialize<List<MemoryCheckpointLockEntry>>(File.ReadAllText(locksFile));

            if (lockEntries is { Count: > 0 })
            {
                List<PersistenceRequestItem> lockItems = new(lockEntries.Count);
                foreach (MemoryCheckpointLockEntry l in lockEntries)
                {
                    lockItems.Add(new(
                        l.Resource, l.Owner, l.FencingToken,
                        l.ExpiresNode, l.ExpiresPhysical, l.ExpiresCounter,
                        l.LastUsedNode, l.LastUsedPhysical, l.LastUsedCounter,
                        l.LastModifiedNode, l.LastModifiedPhysical, l.LastModifiedCounter,
                        l.State
                    ));
                }
                backend.StoreLocks(lockItems);
            }
        }

        return backend;
    }

    public void Dispose()
    {
        GC.SuppressFinalize(this);
    }

    // DTOs used exclusively by CreateCheckpoint / OpenCheckpoint serialization.

    private sealed class MemoryCheckpointEntry
    {
        public string Key { get; set; } = "";
        public byte[]? Value { get; set; }
        public long Revision { get; set; }
        public int ExpiresNode { get; set; }
        public long ExpiresPhysical { get; set; }
        public uint ExpiresCounter { get; set; }
        public int LastUsedNode { get; set; }
        public long LastUsedPhysical { get; set; }
        public uint LastUsedCounter { get; set; }
        public int LastModifiedNode { get; set; }
        public long LastModifiedPhysical { get; set; }
        public uint LastModifiedCounter { get; set; }
        public int State { get; set; }
    }

    private sealed class MemoryCheckpointLockEntry
    {
        public string Resource { get; set; } = "";
        public byte[]? Owner { get; set; }
        public long FencingToken { get; set; }
        public int ExpiresNode { get; set; }
        public long ExpiresPhysical { get; set; }
        public uint ExpiresCounter { get; set; }
        public int LastUsedNode { get; set; }
        public long LastUsedPhysical { get; set; }
        public uint LastUsedCounter { get; set; }
        public int LastModifiedNode { get; set; }
        public long LastModifiedPhysical { get; set; }
        public uint LastModifiedCounter { get; set; }
        public int State { get; set; }
    }
}
