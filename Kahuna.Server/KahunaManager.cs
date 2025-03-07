using Kahuna.KeyValues;
using Kahuna.Locks;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Locks;
using Kommander.Data;

namespace Kahuna;

/// <summary>
/// Façade to the internal systems of Kahuna.
/// </summary>
public sealed class KahunaManager : IKahuna
{
    private readonly LockManager locks;

    private readonly KeyValuesManager keyValues;
    
    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="locks"></param>
    /// <param name="keyValues"></param>
    public KahunaManager(LockManager locks, KeyValuesManager keyValues)
    {
        this.locks = locks;
        this.keyValues = keyValues;
    }
    
    /// <summary>
    /// Passes a TryLock request to the locker actor for the given lock name. 
    /// </summary>
    /// <param name="lockName"></param>
    /// <param name="lockId"></param>
    /// <param name="expiresMs"></param>
    /// <param name="consistency"></param>
    /// <returns></returns>
    public Task<(LockResponseType, long)> TryLock(string lockName, string lockId, int expiresMs, LockConsistency consistency)
    {
        return locks.TryLock(lockName, lockId, expiresMs, consistency);
    }

    /// <summary>
    /// Passes a TryExtendLock request to the locker actor for the given lock name. 
    /// </summary>
    /// <param name="lockName"></param>
    /// <param name="lockId"></param>
    /// <param name="expiresMs"></param>
    /// <param name="consistency"></param>
    /// <returns></returns>
    public Task<LockResponseType> TryExtendLock(string lockName, string lockId, int expiresMs, LockConsistency consistency)
    {
        return locks.TryExtendLock(lockName, lockId, expiresMs, consistency);
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="lockName"></param>
    /// <param name="lockId"></param>
    /// <param name="consistency"></param>
    /// <returns></returns>
    public Task<LockResponseType> TryUnlock(string lockName, string lockId, LockConsistency consistency)
    {
        return locks.TryUnlock(lockName, lockId, consistency);
    }

    /// <summary>
    /// Passes a TryUnlock request to the locker actor for the given lock name. 
    /// </summary>
    /// <param name="lockName"></param>
    /// <param name="consistency"></param>
    /// <returns></returns>
    public Task<(LockResponseType, ReadOnlyLockContext?)> GetLock(string lockName, LockConsistency consistency)
    {
        return locks.GetLock(lockName, consistency);
    }

    public Task<KeyValueResponseType> TrySetKeyValue(string key, string? value, int expiresMs, KeyValueConsistency consistency)
    {
        return keyValues.TrySetKeyValue(key, value, expiresMs, consistency);
    }

    public Task<KeyValueResponseType> TryExtendKeyValue(string key, int expiresMs, KeyValueConsistency consistency)
    {
        return keyValues.TryExtendKeyValue(key, expiresMs, consistency);
    }

    public Task<KeyValueResponseType> TryDeleteKeyValue(string key, KeyValueConsistency consistency)
    {
        return keyValues.TryDeleteKeyValue(key, consistency);
    }

    public Task<(KeyValueResponseType, ReadOnlyKeyValueContext?)> TryGetValue(string keyValueName, KeyValueConsistency consistency)
    {
        return keyValues.TryGetValue(keyValueName, consistency);
    }

    public Task<bool> OnReplicationReceived(RaftLog log)
    {
        return locks.OnReplicationReceived(log);
    }

    public void OnReplicationError(RaftLog log)
    {
        locks.OnReplicationError(log);
    }
}