
using Kommander.Data;
using Kommander.Time;

using Kahuna.Server.KeyValues;
using Kahuna.Server.Locks;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Locks;

namespace Kahuna;

public interface IKahuna
{
    public Task<(LockResponseType, long)> TryLock(string resource, byte[] owner, int expiresMs, LockDurability durability);

    public Task<(LockResponseType, long)> TryExtendLock(string resource, byte[] owner, int expiresMs, LockDurability durability);

    public Task<LockResponseType> TryUnlock(string resource, byte[] owner, LockDurability durability);
    
    public Task<(LockResponseType, ReadOnlyLockContext?)> GetLock(string resource, LockDurability durability);

    public Task<(KeyValueResponseType, long)> LocateAndTrySetKeyValue(HLCTimestamp transactionId, string key, byte[]? value, byte[]? compareValue, long compareRevision, KeyValueFlags flags, int expiresMs, KeyValueDurability durability, CancellationToken cancellationToken);

    public Task<(KeyValueResponseType, ReadOnlyKeyValueContext?)> LocateAndTryGetValue(HLCTimestamp transactionId, string key, long revision, KeyValueDurability durability, CancellationToken cancelationToken);

    public Task<(KeyValueResponseType, long)> LocateAndTryDeleteKeyValue(HLCTimestamp transactionId, string key, KeyValueDurability durability, CancellationToken cancellationToken);
    
    public Task<(KeyValueResponseType, long)> LocateAndTryExtendKeyValue(HLCTimestamp transactionId, string key, int expiresMs, KeyValueDurability durability, CancellationToken cancellationToken);

    public Task<(KeyValueResponseType, long)> TrySetKeyValue(HLCTimestamp transactionId, string key, byte[]? value, byte[]? compareValue, long compareRevision, KeyValueFlags flags, int expiresMs, KeyValueDurability durability);

    public Task<(KeyValueResponseType, long)> TryExtendKeyValue(HLCTimestamp transactionId, string key, int expiresMs, KeyValueDurability durability);

    public Task<(KeyValueResponseType, long)> TryDeleteKeyValue(HLCTimestamp transactionId, string key, KeyValueDurability durability);

    public Task<(KeyValueResponseType, ReadOnlyKeyValueContext?)> TryGetValue(HLCTimestamp transactionId, string key, long revision, KeyValueDurability durability);

    public Task<(KeyValueResponseType, string, KeyValueDurability)> LocateAndTryAcquireExclusiveLock(HLCTimestamp transactionId, string key, int expiresMs, KeyValueDurability durability, CancellationToken cancelationToken);
    
    public Task<(KeyValueResponseType, string)> LocateAndTryReleaseExclusiveLock(HLCTimestamp transactionId, string key, KeyValueDurability durability, CancellationToken cancelationToken);
    
    public Task<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)> LocateAndTryPrepareMutations(HLCTimestamp transactionId, string key, KeyValueDurability durability, CancellationToken cancelationToken);
    
    public Task<(KeyValueResponseType, long)> LocateAndTryCommitMutations(HLCTimestamp transactionId, string key, HLCTimestamp ticketId, KeyValueDurability durability, CancellationToken cancelationToken);
    
    public Task<(KeyValueResponseType, long)> LocateAndTryRollbackMutations(HLCTimestamp transactionId, string key, HLCTimestamp ticketId, KeyValueDurability durability, CancellationToken cancelationToken);
    
    public Task<KeyValueTransactionResult> TryExecuteTx(byte[] script, string? hash, List<KeyValueParameter>? parameters);

    public Task<bool> OnReplicationReceived(RaftLog log);

    public void OnReplicationError(RaftLog log);
}