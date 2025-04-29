
using Kommander.Data;
using Kommander.Time;

using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Locks;
using Kahuna.Shared.Communication.Rest;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Locks;

namespace Kahuna;

public interface IKahuna
{
    public Task<(LockResponseType, long)> LocateAndTryLock(string resource, byte[] owner, int expiresMs, LockDurability durability, CancellationToken cancellationToken);

    public Task<(LockResponseType, long)> LocateAndTryExtendLock(string resource, byte[] owner, int expiresMs, LockDurability durability, CancellationToken cancellationToken);

    public Task<LockResponseType> LocateAndTryUnlock(string resource, byte[] owner, LockDurability durability,CancellationToken cancellationToken);

    public Task<(LockResponseType, ReadOnlyLockContext?)> LocateAndGetLock(string resource, LockDurability durability, CancellationToken cancellationToken);
    
    public Task<(LockResponseType, long)> TryLock(string resource, byte[] owner, int expiresMs, LockDurability durability);

    public Task<(LockResponseType, long)> TryExtendLock(string resource, byte[] owner, int expiresMs, LockDurability durability);

    public Task<LockResponseType> TryUnlock(string resource, byte[] owner, LockDurability durability);
    
    public Task<(LockResponseType, ReadOnlyLockContext?)> GetLock(string resource, LockDurability durability);

    public Task<(KeyValueResponseType, long, HLCTimestamp)> LocateAndTrySetKeyValue(HLCTimestamp transactionId, string key, byte[]? value, byte[]? compareValue, long compareRevision, KeyValueFlags flags, int expiresMs, KeyValueDurability durability, CancellationToken cancellationToken);
    
    public Task<List<KahunaSetKeyValueResponseItem>> LocateAndTrySetManyKeyValue(IEnumerable<KahunaSetKeyValueRequestItem> setManyItems, CancellationToken cancellationToken);

    public Task<(KeyValueResponseType, ReadOnlyKeyValueContext?)> LocateAndTryExistsValue(HLCTimestamp transactionId, string key, long revision, KeyValueDurability durability, CancellationToken cancelationToken);

    public Task<(KeyValueResponseType, ReadOnlyKeyValueContext?)> LocateAndTryGetValue(HLCTimestamp transactionId, string key, long revision, KeyValueDurability durability, CancellationToken cancelationToken);

    public Task<(KeyValueResponseType, long, HLCTimestamp)> LocateAndTryDeleteKeyValue(HLCTimestamp transactionId, string key, KeyValueDurability durability, CancellationToken cancellationToken);
    
    public Task<(KeyValueResponseType, long, HLCTimestamp)> LocateAndTryExtendKeyValue(HLCTimestamp transactionId, string key, int expiresMs, KeyValueDurability durability, CancellationToken cancellationToken);

    public Task<KeyValueGetByPrefixResult> LocateAndGetByPrefix(HLCTimestamp transactionId, string prefixedKey, KeyValueDurability durability, CancellationToken cancelationToken);

    public Task<(KeyValueResponseType, long, HLCTimestamp)> TrySetKeyValue(HLCTimestamp transactionId, string key, byte[]? value, byte[]? compareValue, long compareRevision, KeyValueFlags flags, int expiresMs, KeyValueDurability durability);

    public Task<(KeyValueResponseType, long, HLCTimestamp)> TryExtendKeyValue(HLCTimestamp transactionId, string key, int expiresMs, KeyValueDurability durability);

    public Task<(KeyValueResponseType, long, HLCTimestamp)> TryDeleteKeyValue(HLCTimestamp transactionId, string key, KeyValueDurability durability);

    public Task<(KeyValueResponseType, ReadOnlyKeyValueContext?)> TryGetValue(HLCTimestamp transactionId, string key, long revision, KeyValueDurability durability);
    
    public Task<(KeyValueResponseType, ReadOnlyKeyValueContext?)> TryExistsValue(HLCTimestamp transactionId, string key, long revision, KeyValueDurability durability);

    public Task<(KeyValueResponseType, string, KeyValueDurability)> LocateAndTryAcquireExclusiveLock(HLCTimestamp transactionId, string key, int expiresMs, KeyValueDurability durability, CancellationToken cancelationToken);

    public Task<List<(KeyValueResponseType, string, KeyValueDurability)>> LocateAndTryAcquireManyExclusiveLocks(HLCTimestamp transactionId, List<(string key, int expiresMs, KeyValueDurability durability)> keys, CancellationToken cancelationToken);
    
    public Task<(KeyValueResponseType, string)> LocateAndTryReleaseExclusiveLock(HLCTimestamp transactionId, string key, KeyValueDurability durability, CancellationToken cancelationToken);
    
    public Task<List<(KeyValueResponseType, string, KeyValueDurability)>> LocateAndTryReleaseManyExclusiveLocks(HLCTimestamp transactionId, List<(string key, KeyValueDurability durability)> keys, CancellationToken cancelationToken);
    
    public Task<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)> LocateAndTryPrepareMutations(HLCTimestamp transactionId, HLCTimestamp commitId, string key, KeyValueDurability durability, CancellationToken cancelationToken);

    public Task<List<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)>> LocateAndTryPrepareManyMutations(HLCTimestamp transactionId, HLCTimestamp commitId, List<(string key, KeyValueDurability durability)> keys, CancellationToken cancelationToken);
    
    public Task<(KeyValueResponseType, long)> LocateAndTryCommitMutations(HLCTimestamp transactionId, string key, HLCTimestamp ticketId, KeyValueDurability durability, CancellationToken cancelationToken);

    public Task<List<(KeyValueResponseType, string, long, KeyValueDurability)>> LocateAndTryCommitManyMutations(HLCTimestamp transactionId, List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)> keys, CancellationToken cancelationToken);
    
    public Task<(KeyValueResponseType, long)> LocateAndTryRollbackMutations(HLCTimestamp transactionId, string key, HLCTimestamp ticketId, KeyValueDurability durability, CancellationToken cancelationToken);
    
    public Task<List<(KeyValueResponseType, string, long, KeyValueDurability)>> LocateAndTryRollbackManyMutations(HLCTimestamp transactionId, List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)> keys, CancellationToken cancelationToken);

    public Task<(KeyValueResponseType, HLCTimestamp)> LocateAndStartTransaction(KeyValueTransactionOptions options, CancellationToken cancelationToken);       
    
    public Task<KeyValueResponseType> LocateAndCommitTransaction(string uniqueId, HLCTimestamp timestamp, List<KeyValueTransactionModifiedKey> acquiredLocks, List<KeyValueTransactionModifiedKey> modifiedKeys, CancellationToken cancelationToken);
    
    public Task<KeyValueResponseType> LocateAndRollbackTransaction(string uniqueId, HLCTimestamp timestamp, List<KeyValueTransactionModifiedKey> acquiredLocks, List<KeyValueTransactionModifiedKey> modifiedKeys, CancellationToken cancelationToken);

    public Task<(KeyValueResponseType, string, KeyValueDurability)> TryAcquireExclusiveLock(HLCTimestamp transactionId, string key, int expiresMs, KeyValueDurability durability);
    
    public Task<(KeyValueResponseType, string)> TryReleaseExclusiveLock(HLCTimestamp transactionId, string key, KeyValueDurability durability);

    public Task<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)> TryPrepareMutations(HLCTimestamp transactionId, HLCTimestamp commitId, string key, KeyValueDurability durability);

    public Task<(KeyValueResponseType, long)> TryCommitMutations(HLCTimestamp transactionId, string key, HLCTimestamp proposalTicketId, KeyValueDurability durability);
    
    public Task<(KeyValueResponseType, long)> TryRollbackMutations(HLCTimestamp transactionId, string key, HLCTimestamp proposalTicketId, KeyValueDurability durability);
    
    public Task<KeyValueTransactionResult> TryExecuteTransactionScript(byte[] script, string? hash, List<KeyValueParameter>? parameters);
    
    public Task<KeyValueGetByPrefixResult> GetByPrefix(HLCTimestamp transactionId, string prefixKeyName, KeyValueDurability durability);

    public Task<KeyValueGetByPrefixResult> ScanByPrefix(string prefixKeyName, KeyValueDurability durability);

    public Task<KeyValueGetByPrefixResult> ScanAllByPrefix(string prefixKeyName, KeyValueDurability durability);

    public Task<(KeyValueResponseType, HLCTimestamp)> StartTransaction(KeyValueTransactionOptions options);       
    
    public Task<KeyValueResponseType> CommitTransaction(HLCTimestamp timestamp, List<KeyValueTransactionModifiedKey> acquiredLocks, List<KeyValueTransactionModifiedKey> modifiedKeys);
    
    public Task<KeyValueResponseType> RollbackTransaction(HLCTimestamp timestamp, List<KeyValueTransactionModifiedKey> acquiredLocks, List<KeyValueTransactionModifiedKey> modifiedKeys);
    
    public Task<bool> OnLogRestored(int partitionId, RaftLog log);

    public Task<bool> OnReplicationReceived(int partitionId, RaftLog log);

    public void OnReplicationError(int partitionId, RaftLog log);    
}