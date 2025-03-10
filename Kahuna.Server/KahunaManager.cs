using Kahuna.Configuration;
using Kahuna.KeyValues;
using Kahuna.Locks;
using Kahuna.Persistence;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Locks;
using Kommander;
using Kommander.Data;
using Nixie;

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
    public KahunaManager(ActorSystem actorSystem, IRaft raft, KahunaConfiguration configuration, ILogger<IKahuna> logger)
    {
        IPersistence persistence = GetPersistence(configuration);
        
        this.locks = new LockManager(actorSystem, raft, persistence, configuration, logger);
        this.keyValues = new KeyValuesManager(actorSystem, raft, persistence, configuration, logger);
        
    }
    
    /// <summary>
    /// Creates the persistence instance
    /// </summary>
    /// <param name="configuration"></param>
    /// <returns></returns>
    /// <exception cref="KahunaServerException"></exception>
    private static IPersistence GetPersistence(KahunaConfiguration configuration)
    {
        return configuration.Storage switch
        {
            "rocksdb" => new RocksDbPersistence(configuration.StoragePath, configuration.StorageRevision),
            "sqlite" => new SqlitePersistence(configuration.StoragePath, configuration.StorageRevision),
            _ => throw new KahunaServerException("Invalid storage type")
        };
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
    public Task<(LockResponseType, long)> TryExtendLock(string lockName, string lockId, int expiresMs, LockConsistency consistency)
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
    /// Tries Unlocking the lock with the given name and id.
    /// </summary>
    /// <param name="lockName"></param>
    /// <param name="consistency"></param>
    /// <returns></returns>
    public Task<(LockResponseType, ReadOnlyLockContext?)> GetLock(string lockName, LockConsistency consistency)
    {
        return locks.GetLock(lockName, consistency);
    }

    /// <summary>
    /// Set key to hold the string value. If key already holds a value, it is overwritten
    /// </summary>
    /// <param name="key"></param>
    /// <param name="value"></param>
    /// <param name="expiresMs"></param>
    /// <param name="consistency"></param>
    /// <returns></returns>
    public Task<(KeyValueResponseType, long)> TrySetKeyValue(string key, string? value, int expiresMs, KeyValueConsistency consistency)
    {
        return keyValues.TrySetKeyValue(key, value, expiresMs, consistency);
    }

    /// <summary>
    /// Set a timeout on key. After the timeout has expired, the key will automatically be deleted
    /// </summary>
    /// <param name="key"></param>
    /// <param name="expiresMs"></param>
    /// <param name="consistency"></param>
    /// <returns></returns>
    public Task<(KeyValueResponseType, long)> TryExtendKeyValue(string key, int expiresMs, KeyValueConsistency consistency)
    {
        return keyValues.TryExtendKeyValue(key, expiresMs, consistency);
    }

    /// <summary>
    /// Removes the specified key. A key is ignored if it does not exist.
    /// </summary>
    /// <param name="key"></param>
    /// <param name="consistency"></param>
    /// <returns></returns>
    public Task<KeyValueResponseType> TryDeleteKeyValue(string key, KeyValueConsistency consistency)
    {
        return keyValues.TryDeleteKeyValue(key, consistency);
    }

    public Task<(KeyValueResponseType, ReadOnlyKeyValueContext?)> TryGetValue(string keyValueName, KeyValueConsistency consistency)
    {
        return keyValues.TryGetValue(keyValueName, consistency);
    }

    public async Task<bool> OnReplicationReceived(RaftLog log)
    {
        await Task.WhenAll(locks.OnReplicationReceived(log), keyValues.OnReplicationReceived(log));
        return true;
    }

    public void OnReplicationError(RaftLog log)
    {
        locks.OnReplicationError(log);
        keyValues.OnReplicationError(log);
    }
}