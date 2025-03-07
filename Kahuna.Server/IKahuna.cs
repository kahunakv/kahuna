
using Kahuna.KeyValues;
using Kahuna.Locks;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Locks;
using Kommander.Data;

namespace Kahuna;

public interface IKahuna
{
    public Task<(LockResponseType, long)> TryLock(string lockName, string lockId, int expiresMs, LockConsistency consistency);

    public Task<LockResponseType> TryExtendLock(string lockName, string lockId, int expiresMs, LockConsistency consistency);

    public Task<LockResponseType> TryUnlock(string lockName, string lockId, LockConsistency consistency);
    
    public Task<(LockResponseType, ReadOnlyLockContext?)> GetLock(string lockName, LockConsistency consistency);

    public Task<KeyValueResponseType> TrySetKeyValue(string key, string? value, int expiresMs, KeyValueConsistency consistency);

    public Task<KeyValueResponseType> TryExtendKeyValue(string key, int expiresMs, KeyValueConsistency consistency);

    public Task<KeyValueResponseType> TryDeleteKeyValue(string key, KeyValueConsistency consistency);

    public Task<(KeyValueResponseType, ReadOnlyKeyValueContext?)> TryGetValue(string key, KeyValueConsistency consistency);

    public Task<bool> OnReplicationReceived(RaftLog log);

    public void OnReplicationError(RaftLog log);
}