
using Nixie;
using Nixie.Routers;

using Kommander;
using Kommander.Data;

using Kahuna.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Locks;
using Kahuna.Server.Persistence;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Locks;

namespace Kahuna;

/// <summary>
/// Façade to the internal systems of Kahuna.
/// </summary>
public sealed class KahunaManager : IKahuna
{
    private readonly ActorSystem actorSystem;
    
    private readonly LockManager locks;

    private readonly KeyValuesManager keyValues;
    
    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="actorSystem"></param>
    /// <param name="raft"></param>
    /// <param name="configuration"></param>
    /// <param name="logger"></param>
    public KahunaManager(ActorSystem actorSystem, IRaft raft, KahunaConfiguration configuration, ILogger<IKahuna> logger)
    {
        this.actorSystem = actorSystem;
        
        IPersistence persistence = GetPersistence(configuration);
        
        IActorRef<ConsistentHashActor<PersistenceActor, PersistenceRequest, PersistenceResponse>, PersistenceRequest, PersistenceResponse> persistenceActorRouter = GetPersistenceRouter(persistence, configuration, logger);
        
        IActorRef<BackgroundWriterActor, BackgroundWriteRequest> backgroundWriter = actorSystem.Spawn<BackgroundWriterActor, BackgroundWriteRequest>(
            "background-writer", 
            raft, 
            persistenceActorRouter, 
            logger
        );
        
        this.locks = new(actorSystem, raft, persistence, persistenceActorRouter, backgroundWriter, configuration, logger);
        this.keyValues = new(actorSystem, raft, persistence, persistenceActorRouter, backgroundWriter, configuration, logger);
    }
    
    /// <summary>
    /// Creates the persistence router
    /// </summary>
    /// <param name="persistence"></param>
    /// <param name="configuration"></param>
    /// <param name="logger"></param>
    /// <returns></returns>
    private IActorRef<ConsistentHashActor<PersistenceActor, PersistenceRequest, PersistenceResponse>, PersistenceRequest, PersistenceResponse> GetPersistenceRouter(
        IPersistence persistence, 
        KahunaConfiguration configuration,
        ILogger logger
    )
    {
        List<IActorRef<PersistenceActor, PersistenceRequest, PersistenceResponse>> persistenceInstances = new(configuration.PersistenceWorkers);

        for (int i = 0; i < configuration.PersistenceWorkers; i++)
            persistenceInstances.Add(actorSystem.Spawn<PersistenceActor, PersistenceRequest, PersistenceResponse>("persistence-" + i, persistence, logger));

        return actorSystem.CreateConsistentHashRouter(persistenceInstances);
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
            _ => throw new KahunaServerException("Invalid storage type: " + configuration.Storage)
        };
    }
    
    /// <summary>
    /// Passes a TryLock request to the locker actor for the given lock name. 
    /// </summary>
    /// <param name="resource"></param>
    /// <param name="owner"></param>
    /// <param name="expiresMs"></param>
    /// <param name="consistency"></param>
    /// <returns></returns>
    public Task<(LockResponseType, long)> TryLock(string resource, byte[] owner, int expiresMs, LockConsistency consistency)
    {
        return locks.TryLock(resource, owner, expiresMs, consistency);
    }

    /// <summary>
    /// Passes a TryExtendLock request to the locker actor for the given lock name. 
    /// </summary>
    /// <param name="resource"></param>
    /// <param name="owner"></param>
    /// <param name="expiresMs"></param>
    /// <param name="consistency"></param>
    /// <returns></returns>
    public Task<(LockResponseType, long)> TryExtendLock(string resource, byte[] owner, int expiresMs, LockConsistency consistency)
    {
        return locks.TryExtendLock(resource, owner, expiresMs, consistency);
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="resource"></param>
    /// <param name="owner"></param>
    /// <param name="consistency"></param>
    /// <returns></returns>
    public Task<LockResponseType> TryUnlock(string resource, byte[] owner, LockConsistency consistency)
    {
        return locks.TryUnlock(resource, owner, consistency);
    }

    /// <summary>
    /// Tries Unlocking the lock with the given name and id.
    /// </summary>
    /// <param name="resource"></param>
    /// <param name="consistency"></param>
    /// <returns></returns>
    public Task<(LockResponseType, ReadOnlyLockContext?)> GetLock(string resource, LockConsistency consistency)
    {
        return locks.GetLock(resource, consistency);
    }

    /// <summary>
    /// Locates the leader node for the given key and executes the TrySet request.
    /// </summary>
    /// <param name="key"></param>
    /// <param name="value"></param>
    /// <param name="compareValue"></param>
    /// <param name="compareRevision"></param>
    /// <param name="flags"></param>
    /// <param name="expiresMs"></param>
    /// <param name="consistency"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, long)> LocateAndTrySetKeyValue(
        string key,
        byte[]? value,
        byte[]? compareValue,
        long compareRevision,
        KeyValueFlags flags,
        int expiresMs,
        KeyValueConsistency consistency,
        CancellationToken cancellationToken
    )
    {
        return await keyValues.LocateAndTrySetKeyValue(key, value, compareValue, compareRevision, flags, expiresMs, consistency, cancellationToken);
    }

    /// <summary>
    /// Locates the leader node for the given key and executes the TryGetValue request.
    /// </summary>
    /// <param name="key"></param>
    /// <param name="consistency"></param>
    /// <param name="cancelationToken"></param>
    /// <returns></returns>
    public Task<(KeyValueResponseType, ReadOnlyKeyValueContext?)> LocateAndTryGetValue(string key, KeyValueConsistency consistency, CancellationToken cancelationToken)
    {
        return keyValues.LocateAndTryGetValue(key, consistency, cancelationToken);
    }

    /// <summary>
    /// Set key to hold the string value. If key already holds a value, it is overwritten
    /// </summary>
    /// <param name="key"></param>
    /// <param name="value"></param>
    /// <param name="compareValue"></param>
    /// <param name="compareRevision"></param>
    /// <param name="flags"></param>
    /// <param name="expiresMs"></param>
    /// <param name="consistency"></param>
    /// <returns></returns>
    public Task<(KeyValueResponseType, long)> TrySetKeyValue(
        string key, 
        byte[]? value,
        byte[]? compareValue,
        long compareRevision,
        KeyValueFlags flags,
        int expiresMs, 
        KeyValueConsistency consistency
    )
    {
        return keyValues.TrySetKeyValue(key, value, compareValue, compareRevision, flags, expiresMs, consistency);
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
    public Task<(KeyValueResponseType, long)> TryDeleteKeyValue(string key, KeyValueConsistency consistency)
    {
        return keyValues.TryDeleteKeyValue(key, consistency);
    }

    /// <summary>
    /// Returns a value and its context by the specified key
    /// </summary>
    /// <param name="keyValueName"></param>
    /// <param name="consistency"></param>
    /// <returns></returns>
    public Task<(KeyValueResponseType, ReadOnlyKeyValueContext?)> TryGetValue(string keyValueName, KeyValueConsistency consistency)
    {
        return keyValues.TryGetValue(keyValueName, consistency);
    }
    
    /// <summary>
    /// Executes a key/value transaction
    /// </summary>
    /// <param name="script"></param>
    /// <returns></returns>
    public Task<KeyValueTransactionResult> TryExecuteTx(string script)
    {
        return keyValues.TryExecuteTx(script);
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