
using Kahuna.Server.KeyValues;
using Kahuna.Locks;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Locks;
using Kommander.Data;
using Kommander.Time;

namespace Kahuna;

public interface IKahuna
{
    public Task<(LockResponseType, long)> TryLock(string resource, byte[] owner, int expiresMs, LockConsistency consistency);

    public Task<(LockResponseType, long)> TryExtendLock(string resource, byte[] owner, int expiresMs, LockConsistency consistency);

    public Task<LockResponseType> TryUnlock(string resource, byte[] owner, LockConsistency consistency);
    
    public Task<(LockResponseType, ReadOnlyLockContext?)> GetLock(string resource, LockConsistency consistency);

    public Task<(KeyValueResponseType, long)> LocateAndTrySetKeyValue(HLCTimestamp transactionId, string key, byte[]? value, byte[]? compareValue, long compareRevision, KeyValueFlags flags, int expiresMs, KeyValueConsistency consistency, CancellationToken cancellationToken);

    public Task<(KeyValueResponseType, ReadOnlyKeyValueContext?)> LocateAndTryGetValue(HLCTimestamp transactionId, string key, KeyValueConsistency consistency, CancellationToken cancelationToken);

    public Task<(KeyValueResponseType, long)> TrySetKeyValue(HLCTimestamp transactionId, string key, byte[]? value, byte[]? compareValue, long compareRevision, KeyValueFlags flags, int expiresMs, KeyValueConsistency consistency);

    public Task<(KeyValueResponseType, long)> TryExtendKeyValue(string key, int expiresMs, KeyValueConsistency consistency);

    public Task<(KeyValueResponseType, long)> TryDeleteKeyValue(string key, KeyValueConsistency consistency);

    public Task<(KeyValueResponseType, ReadOnlyKeyValueContext?)> TryGetValue(HLCTimestamp transactionId,  string key, KeyValueConsistency consistency);

    public Task<(KeyValueResponseType, string)> LocateAndTryAcquireExclusiveLock(HLCTimestamp transactionId, string key, int expiresMs, KeyValueConsistency consistency, CancellationToken cancelationToken);
    
    public Task<(KeyValueResponseType, string)> LocateAndTryReleaseExclusiveLock(HLCTimestamp transactionId, string key, KeyValueConsistency consistency, CancellationToken cancelationToken);
    
    public Task<(KeyValueResponseType, HLCTimestamp, string)> LocateAndTryPrepareMutations(HLCTimestamp transactionId, string key, KeyValueConsistency consistency, CancellationToken cancelationToken);
    
    public Task<(KeyValueResponseType, long)> LocateAndTryCommitMutations(HLCTimestamp transactionId, string key, HLCTimestamp ticketId, KeyValueConsistency consistency, CancellationToken cancelationToken);
    
    public Task<KeyValueTransactionResult> TryExecuteTx(string script);

    public Task<bool> OnReplicationReceived(RaftLog log);

    public void OnReplicationError(RaftLog log);
}