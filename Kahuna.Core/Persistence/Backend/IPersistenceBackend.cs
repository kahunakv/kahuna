
using Kahuna.Server.Locks;
using Kahuna.Server.KeyValues;
using Kahuna.Server.Locks.Data;
using Kahuna.Server.Persistence.Pitr;
using Kommander.Time;

namespace Kahuna.Server.Persistence.Backend;

/// <summary>
/// Defines an interface for persistence backend implementations, providing methods
/// for storing and retrieving locks and key-value pairs from a persistent storage system.
/// </summary>
internal interface IPersistenceBackend
{
    public bool StoreLocks(List<PersistenceRequestItem> items);

    public bool StoreKeyValues(List<PersistenceRequestItem> items);

    public LockEntry? GetLock(string resource);
    
    public KeyValueEntry? GetKeyValue(string keyName);
    
    public KeyValueEntry? GetKeyValueRevision(string keyName, long revision);

    public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByPrefix(string prefixKeyName);

    public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByRange(string prefix, string? startKey, int limit);

/// <summary>
    /// Prunes persisted key/value revision history according to retention policy.
    /// </summary>
    /// <param name="keys">Target keys to prune, or <c>null</c> for a backend-wide sweep.</param>
    /// <param name="retentionCount">Maximum revisions to keep per key; <c>0</c> disables count-based pruning.</param>
    /// <param name="retentionAge">Maximum revision age; <see cref="TimeSpan.Zero"/> disables age-based pruning.</param>
    /// <param name="batchSize">Maximum revision records to delete in this pass.</param>
    /// <param name="result">Statistics for the prune pass.</param>
    /// <returns><c>true</c> when the pass completed without error.</returns>
    public bool PruneKeyValueRevisions(
        IReadOnlyCollection<string>? keys,
        int retentionCount,
        TimeSpan retentionAge,
        int batchSize,
        out RevisionPruneResult result
    );

    /// <summary>
    /// Produces a crash-consistent base-image snapshot of the storage engine at
    /// <paramref name="destinationPath"/>.  The image is accompanied by a
    /// <see cref="CheckpointManifest"/> sidecar that records the WAL index and HLC
    /// at which the snapshot was taken so restore can locate the correct replay start.
    /// <para>
    /// <b>Content policy:</b> all implementations snapshot both KV and lock state.
    /// RocksDB and SQLite do so implicitly (full-DB copy); Memory serialises both tables
    /// to JSON. The restore path decides which tables to apply.
    /// </para>
    /// <para>
    /// All implementations write into a temp sibling directory first and rename it into
    /// place atomically so a failure mid-copy never leaves a partial checkpoint at
    /// <paramref name="destinationPath"/>.
    /// </para>
    /// <para>
    /// RocksDB creates hard-links to live SST files — fast, does not stall foreground writes.
    /// SQLite runs <c>VACUUM INTO</c> per shard under a per-shard exclusive lock — each shard
    /// copy stalls writes to that shard for its duration; callers should schedule off the
    /// hot write path. Memory serialises both tables to JSON.
    /// </para>
    /// </summary>
    public CheckpointResult CreateCheckpoint(string destinationPath, long appliedIndex, HLCTimestamp appliedTime);
}