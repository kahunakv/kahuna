
using System.Text.Json;
using Kahuna.Server.KeyValues;
using Kahuna.Server.Locks;
using Kahuna.Server.Locks.Data;
using Kahuna.Server.Persistence.Pitr;
using Kommander.Time;
using System.Collections.Concurrent;

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
    private readonly object kvLock = new();

    private readonly ConcurrentDictionary<string, ConcurrentDictionary<long, KeyValueEntry>> keyValueRevisions = new();

    /// <summary>
    /// Stores locks in the persistence backend. Updates existing locks or adds new ones
    /// based on the provided list of persistence request items.
    /// </summary>
    /// <param name="items">A list of <see cref="PersistenceRequestItem"/> containing the lock data to be stored or updated.</param>
    /// <returns>Returns <c>true</c> if the locks were successfully stored or updated.</returns>
    public bool StoreLocks(List<PersistenceRequestItem> items)
    {
        foreach (PersistenceRequestItem item in items)
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
            foreach (PersistenceRequestItem item in items)
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

                ConcurrentDictionary<long, KeyValueEntry> revisions = keyValueRevisions.GetOrAdd(item.Key, _ => new());
                revisions[item.Revision] = keyValues[item.Key];
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
        out RevisionPruneResult result)
    {
        result = new(0, 0, BatchLimitReached: false);
        return true;
    }

    public CheckpointResult CreateCheckpoint(string destinationPath, long appliedIndex, HLCTimestamp appliedTime)
    {
        string tmpPath = destinationPath + ".tmp_" + Guid.NewGuid().ToString("N")[..8];
        Directory.CreateDirectory(tmpPath);

        try
        {
            List<MemoryCheckpointEntry> kvEntries;
            lock (kvLock)
            {
                kvEntries = new(keyValues.Count);
                foreach (KeyValuePair<string, KeyValueEntry> kv in keyValues)
                {
                    KeyValueEntry e = kv.Value;
                    kvEntries.Add(new()
                    {
                        Key = kv.Key,
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
                    });
                }
            }

            List<MemoryCheckpointLockEntry> lockEntries = new(locks.Count);
            foreach (KeyValuePair<string, LockEntry> kv in locks)
            {
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
