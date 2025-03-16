
using Kahuna.KeyValues;
using Kahuna.Locks;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Locks;
using Kommander.Data;

namespace Kahuna;

public interface IKahuna
{
    public Task<(LockResponseType, long)> TryLock(string resource, byte[] owner, int expiresMs, LockConsistency consistency);

    public Task<(LockResponseType, long)> TryExtendLock(string resource, byte[] owner, int expiresMs, LockConsistency consistency);

    public Task<LockResponseType> TryUnlock(string resource, byte[] owner, LockConsistency consistency);
    
    public Task<(LockResponseType, ReadOnlyLockContext?)> GetLock(string resource, LockConsistency consistency);

    public Task<(KeyValueResponseType, long)> LocateAndTrySetKeyValue(string key, byte[]? value, byte[]? compareValue, long compareRevision, KeyValueFlags flags, int expiresMs, KeyValueConsistency consistency, CancellationToken cancellationToken);

    public Task<(KeyValueResponseType, ReadOnlyKeyValueContext?)> LocateAndTryGetValue(string key, KeyValueConsistency consistency, CancellationToken cancelationToken);

    public Task<(KeyValueResponseType, long)> TrySetKeyValue(string key, byte[]? value, byte[]? compareValue, long compareRevision, KeyValueFlags flags, int expiresMs, KeyValueConsistency consistency);

    public Task<(KeyValueResponseType, long)> TryExtendKeyValue(string key, int expiresMs, KeyValueConsistency consistency);

    public Task<(KeyValueResponseType, long)> TryDeleteKeyValue(string key, KeyValueConsistency consistency);

    public Task<(KeyValueResponseType, ReadOnlyKeyValueContext?)> TryGetValue(string key, KeyValueConsistency consistency);
    
    public Task<KeyValueTransactionResult> TryExecuteTx(string script);

    public Task<bool> OnReplicationReceived(RaftLog log);

    public void OnReplicationError(RaftLog log);
}