
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

    public Task<(KeyValueResponseType, long, HLCTimestamp)> TrySetKeyValue(string node, HLCTimestamp transactionId, string key, byte[]? value, byte[]? compareValue, long compareRevision, KeyValueFlags flags, int expiresMs, KeyValueDurability durability, CancellationToken cancellationToken);
    
    public Task TrySetManyNodeKeyValue(string node, List<KahunaSetKeyValueRequestItem> items, Lock lockSync, List<KahunaSetKeyValueResponseItem> responses, CancellationToken cancellationToken);

    public Task<(KeyValueResponseType, long, HLCTimestamp)> TryDeleteKeyValue(string node, HLCTimestamp transactionId, string key, KeyValueDurability durability, CancellationToken cancellationToken);

    public Task<(KeyValueResponseType, long, HLCTimestamp)> TryExtendKeyValue(string node, HLCTimestamp transactionId, string key, int expiresMs, KeyValueDurability durability, CancellationToken cancellationToken);

    public Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> TryGetValue(string node, HLCTimestamp transactionId, string key, long revision, KeyValueDurability durability, CancellationToken cancellationToken);

    public Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> TryExistsValue(string node, HLCTimestamp transactionId, string key, long revision, KeyValueDurability durability, CancellationToken cancellationToken);

    public Task<(KeyValueResponseType, string, KeyValueDurability)> TryAcquireExclusiveLock(string node, HLCTimestamp transactionId, string key, int expiresMs, KeyValueDurability durability, CancellationToken cancellationToken);
    
    public Task<KeyValueResponseType> TryAcquireExclusivePrefixLock(string node, HLCTimestamp transactionId, string prefixKey, int expiresMs, KeyValueDurability durability, CancellationToken cancellationToken);

    public Task TryAcquireNodeExclusiveLocks(string node, HLCTimestamp transactionId, List<(string key, int expiresMs, KeyValueDurability durability)> xkeys, Lock lockSync, List<(KeyValueResponseType type, string key, KeyValueDurability durability)> responses, CancellationToken cancellationToken);

    public Task<(KeyValueResponseType, string)> TryReleaseExclusiveLock(string node, HLCTimestamp transactionId, string key, KeyValueDurability durability, CancellationToken cancellationToken);
    
    public Task<KeyValueResponseType> TryReleaseExclusivePrefixLock(string node, HLCTimestamp transactionId, string prefixKey, KeyValueDurability durability, CancellationToken cancellationToken);

    public Task TryReleaseNodeExclusiveLocks(string node, HLCTimestamp transactionId, List<(string key, KeyValueDurability durability)> xkeys, Lock lockSync, List<(KeyValueResponseType type, string key, KeyValueDurability durability)> responses, CancellationToken cancellationToken);

    public Task<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)> TryPrepareMutations(string node, HLCTimestamp transactionId, HLCTimestamp commitId, string key, KeyValueDurability durability, CancellationToken cancellationToken);

    public Task TryPrepareNodeMutations(string node, HLCTimestamp transactionId, HLCTimestamp commitId, List<(string key, KeyValueDurability durability)> xkeys, Lock lockSync, List<(KeyValueResponseType type, HLCTimestamp, string key, KeyValueDurability durability)> responses, CancellationToken cancellationToken);

    public Task<(KeyValueResponseType, long)> TryCommitMutations(string node, HLCTimestamp transactionId, string key, HLCTimestamp ticketId, KeyValueDurability durability, CancellationToken cancellationToken);

    public Task TryCommitNodeMutations(string node, HLCTimestamp transactionId, List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)> xkeys, Lock lockSync, List<(KeyValueResponseType type, string key, long, KeyValueDurability durability)> responses, CancellationToken cancellationToken);
    
    public Task<(KeyValueResponseType, long)> TryRollbackMutations(string node, HLCTimestamp transactionId, string key, HLCTimestamp ticketId, KeyValueDurability durability, CancellationToken cancellationToken);
    
    public Task TryRollbackNodeMutations(string node, HLCTimestamp transactionId, List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)> xkeys, Lock lockSync, List<(KeyValueResponseType type, string key, long, KeyValueDurability durability)> responses, CancellationToken cancellationToken);
    
    public Task<KeyValueGetByBucketResult> GetByBucket(string node, HLCTimestamp transactionId, string prefixedKey, KeyValueDurability durability, CancellationToken cancellationToken);
    
    public Task<KeyValueGetByBucketResult> ScanByPrefix(string node, string prefixedKey, KeyValueDurability durability, CancellationToken cancellationToken);
    
    public Task<(KeyValueResponseType, HLCTimestamp)> StartTransaction(string node, KeyValueTransactionOptions options, CancellationToken cancellationToken);
    
    public Task<KeyValueResponseType> CommitTransaction(string node, string uniqueId, HLCTimestamp timestamp, List<KeyValueTransactionModifiedKey> acquiredLocks, List<KeyValueTransactionModifiedKey> modifiedKeys, CancellationToken cancellationToken);
    
    public Task<KeyValueResponseType> RollbackTransaction(string node, string uniqueId, HLCTimestamp timestamp, List<KeyValueTransactionModifiedKey> acquiredLocks, List<KeyValueTransactionModifiedKey> modifiedKeys, CancellationToken cancellationToken);    
}