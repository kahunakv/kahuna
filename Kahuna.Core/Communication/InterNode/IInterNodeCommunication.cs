
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Locks;
using Kahuna.Server.Locks.Data;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Locks;
using Kommander.Time;

namespace Kahuna.Server.Communication.Internode;

/// <summary>
/// Provides methods for inter-node communication within a distributed system.
/// </summary>
public interface IInterNodeCommunication
{
    public Task<(LockResponseType, long)> TryLock(string node, string resource, byte[] owner, int expiresMs, LockDurability durability, CancellationToken cancellationToken);
    
    public Task<(LockResponseType, long)> TryExtendLock(string node, string resource, byte[] owner, int expiresMs, LockDurability durability, CancellationToken cancellationToken);
    
    public Task<LockResponseType> TryUnlock(string node, string resource, byte[] owner, LockDurability durability, CancellationToken cancellationToken);

    public Task<(LockResponseType, ReadOnlyLockEntry?)> GetLock(string node, string resource, LockDurability durability, CancellationToken cancellationToken);

    public Task<(KeyValueResponseType, long, HLCTimestamp)> TrySetKeyValue(string node, HLCTimestamp transactionId, string key, byte[]? value, byte[]? compareValue, long compareRevision, KeyValueFlags flags, int expiresMs, KeyValueDurability durability, long routedGeneration, CancellationToken cancellationToken);
    
    public Task TrySetManyNodeKeyValue(string node, List<KahunaSetKeyValueRequestItem> items, Lock lockSync, List<KahunaSetKeyValueResponseItem> responses, CancellationToken cancellationToken);

    public Task TryDeleteManyNodeKeyValue(string node, List<KahunaDeleteKeyValueRequestItem> items, Lock lockSync, List<KahunaDeleteKeyValueResponseItem> responses, CancellationToken cancellationToken);

    public Task<(KeyValueResponseType, long, HLCTimestamp)> TryDeleteKeyValue(string node, HLCTimestamp transactionId, string key, KeyValueDurability durability, CancellationToken cancellationToken);

    public Task<(KeyValueResponseType, long, HLCTimestamp)> TryExtendKeyValue(string node, HLCTimestamp transactionId, string key, int expiresMs, KeyValueDurability durability, CancellationToken cancellationToken);

    public Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> TryGetValue(string node, HLCTimestamp transactionId, string key, long revision, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken);

    public Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> TryExistsValue(string node, HLCTimestamp transactionId, string key, long revision, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken);

    public Task TryGetManyNodeValues(string node, HLCTimestamp transactionId, HLCTimestamp readTimestamp, List<(string key, long revision, KeyValueDurability durability)> keys, Lock lockSync, List<(KeyValueResponseType type, string key, KeyValueDurability durability, ReadOnlyKeyValueEntry? entry)> responses, CancellationToken cancellationToken);

    public Task TryExistsManyNodeValues(string node, HLCTimestamp transactionId, HLCTimestamp readTimestamp, List<(string key, long revision, KeyValueDurability durability)> keys, Lock lockSync, List<(KeyValueResponseType type, string key, KeyValueDurability durability, ReadOnlyKeyValueEntry? entry)> responses, CancellationToken cancellationToken);

    public Task<KeyValueResponseType> TryCheckWriteIntentValue(string node, HLCTimestamp transactionId, string key, KeyValueDurability durability, CancellationToken cancellationToken);

    public Task<(KeyValueResponseType, string, KeyValueDurability, HLCTimestamp HolderTransactionId)> TryAcquireExclusiveLock(string node, HLCTimestamp transactionId, string key, int expiresMs, KeyValueDurability durability, CancellationToken cancellationToken);

    public Task<KeyValueResponseType> TryAcquireExclusivePrefixLock(string node, HLCTimestamp transactionId, string prefixKey, int expiresMs, KeyValueDurability durability, CancellationToken cancellationToken);

    public Task<(KeyValueResponseType, HLCTimestamp HolderTransactionId)> TryAcquireRangeLock(string node, HLCTimestamp transactionId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive, int expiresMs, KeyValueDurability durability, RangeLockMode mode, CancellationToken cancellationToken);

    public Task<(KeyValueResponseType, HLCTimestamp HolderTransactionId)> TryAcquireExclusiveRangeLock(string node, HLCTimestamp transactionId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive, int expiresMs, KeyValueDurability durability, CancellationToken cancellationToken);

    public Task TryAcquireNodeExclusiveLocks(string node, HLCTimestamp transactionId, List<(string key, int expiresMs, KeyValueDurability durability)> xkeys, Lock lockSync, List<(KeyValueResponseType type, string key, KeyValueDurability durability, HLCTimestamp holder)> responses, CancellationToken cancellationToken);

    public Task<(KeyValueResponseType, string)> TryReleaseExclusiveLock(string node, HLCTimestamp transactionId, string key, KeyValueDurability durability, CancellationToken cancellationToken);
    
    public Task<KeyValueResponseType> TryReleaseExclusivePrefixLock(string node, HLCTimestamp transactionId, string prefixKey, KeyValueDurability durability, CancellationToken cancellationToken);

    public Task<KeyValueResponseType> TryReleaseExclusiveRangeLock(string node, HLCTimestamp transactionId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive, KeyValueDurability durability, CancellationToken cancellationToken);

    public Task TryReleaseNodeExclusiveLocks(string node, HLCTimestamp transactionId, List<(string key, KeyValueDurability durability)> xkeys, Lock lockSync, List<(KeyValueResponseType type, string key, KeyValueDurability durability)> responses, CancellationToken cancellationToken);

    public Task<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)> TryPrepareMutations(string node, HLCTimestamp transactionId, HLCTimestamp commitId, string key, KeyValueDurability durability, long routedGeneration, CancellationToken cancellationToken);

    public Task TryPrepareNodeMutations(string node, HLCTimestamp transactionId, HLCTimestamp commitId, List<(string key, KeyValueDurability durability)> xkeys, Lock lockSync, List<(KeyValueResponseType type, HLCTimestamp, string key, KeyValueDurability durability)> responses, CancellationToken cancellationToken);

    public Task<(KeyValueResponseType, long)> TryCommitMutations(string node, HLCTimestamp transactionId, string key, HLCTimestamp ticketId, KeyValueDurability durability, CancellationToken cancellationToken);

    public Task TryCommitNodeMutations(string node, HLCTimestamp transactionId, List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)> xkeys, Lock lockSync, List<(KeyValueResponseType type, string key, long, KeyValueDurability durability)> responses, CancellationToken cancellationToken);
    
    public Task<(KeyValueResponseType, long)> TryRollbackMutations(string node, HLCTimestamp transactionId, string key, HLCTimestamp ticketId, KeyValueDurability durability, CancellationToken cancellationToken);
    
    public Task TryRollbackNodeMutations(string node, HLCTimestamp transactionId, List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)> xkeys, Lock lockSync, List<(KeyValueResponseType type, string key, long, KeyValueDurability durability)> responses, CancellationToken cancellationToken);
    
    public Task<KeyValueGetByBucketResult> GetByBucket(string node, HLCTimestamp transactionId, string prefixedKey, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken);

    public Task<KeyValueGetByRangeResult> GetByRange(string node, HLCTimestamp transactionId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive, int limit, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken);
    
    public Task<KeyValueGetByBucketResult> ScanByPrefix(string node, string prefixedKey, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken);
    
    public Task<(KeyValueResponseType, HLCTimestamp)> StartTransaction(string node, KeyValueTransactionOptions options, CancellationToken cancellationToken);
    
    public Task<KeyValueResponseType> CommitTransaction(string node, string uniqueId, HLCTimestamp timestamp, List<KeyValueTransactionModifiedKey> acquiredLocks, List<KeyValueTransactionModifiedKey> modifiedKeys, List<KeyValueTransactionReadKey> readKeys, CancellationToken cancellationToken);
    
    public Task<KeyValueResponseType> RollbackTransaction(string node, string uniqueId, HLCTimestamp timestamp, List<KeyValueTransactionModifiedKey> acquiredLocks, List<KeyValueTransactionModifiedKey> modifiedKeys, CancellationToken cancellationToken);

    public Task<bool> EnsureKeyRangeSeeded(string node, string keySpace, CancellationToken cancellationToken);

    public Task<bool> EnsureKeyRangeRemoved(string node, string keySpace, CancellationToken cancellationToken);

    /// <summary>Reads live range locks for <paramref name="keySpace"/> from the actor pool on <paramref name="node"/>.</summary>
    public Task<List<KeyValueRangeLock>> GetRangeLocks(string node, string keySpace, CancellationToken cancellationToken);

    /// <summary>Injects clamped lock entries into the actor pool for <paramref name="keySpace"/> on <paramref name="node"/>.</summary>
    public Task ImportRangeLocks(string node, string keySpace, List<KeyValueRangeLock> locks, CancellationToken cancellationToken);

    /// <summary>Forwards a snapshot-hold acquire to the meta-partition leader on <paramref name="node"/>.</summary>
    public Task<(KeyValueResponseType Type, string HoldId, HLCTimestamp LeaseExpiry)> AcquireSnapshotHold(string node, string holderId, HLCTimestamp timestamp, int leaseMs, CancellationToken cancellationToken);

    /// <summary>Forwards a snapshot-hold renew to the meta-partition leader on <paramref name="node"/>.</summary>
    public Task<(KeyValueResponseType Type, HLCTimestamp LeaseExpiry)> RenewSnapshotHold(string node, string holdId, int leaseMs, CancellationToken cancellationToken);

    /// <summary>Forwards a snapshot-hold release to the meta-partition leader on <paramref name="node"/>.</summary>
    public Task<KeyValueResponseType> ReleaseSnapshotHold(string node, string holdId, CancellationToken cancellationToken);

    /// <summary>Reads the authoritative snapshot floor from the meta-partition leader on <paramref name="node"/>.</summary>
    public Task<(HLCTimestamp Floor, int LiveHolds)> GetSnapshotFloor(string node, CancellationToken cancellationToken);
}
