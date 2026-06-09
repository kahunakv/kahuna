
using Kahuna.Server.Locks;
using Kahuna.Server.KeyValues;
using Kahuna.Server.Locks.Data;

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
}