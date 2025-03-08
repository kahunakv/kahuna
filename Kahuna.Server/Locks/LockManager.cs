
using Kahuna.Configuration;
using Kahuna.Persistence;
using Kahuna.Replication;
using Kahuna.Replication.Protos;
using Kahuna.Shared.Locks;
using Kommander;
using Kommander.Data;
using Kommander.Time;
using Nixie;
using Nixie.Routers;

namespace Kahuna.Locks;

/// <summary>
/// LockManager is a singleton class that manages lock actors.
/// </summary>
public sealed class LockManager
{
    private readonly ActorSystem actorSystem;

    private readonly IRaft raft;

    private readonly ILogger<IKahuna> logger;
    
    private readonly IActorRef<ConsistentHashActor<PersistenceActor, PersistenceRequest, PersistenceResponse>, PersistenceRequest, PersistenceResponse> persistenceActorRouter;

    private readonly IActorRefStruct<ConsistentHashActorStruct<LockActor, LockRequest, LockResponse>, LockRequest, LockResponse> ephemeralLocksRouter;
    
    private readonly IActorRefStruct<ConsistentHashActorStruct<LockActor, LockRequest, LockResponse>, LockRequest, LockResponse> consistentLocksRouter;
    
    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="actorSystem"></param>
    /// <param name="raft"></param>
    /// <param name="persistence"></param>
    /// <param name="configuration"></param>
    /// <param name="logger"></param>
    public LockManager(ActorSystem actorSystem, IRaft raft, IPersistence persistence, KahunaConfiguration configuration, ILogger<IKahuna> logger)
    {
        this.actorSystem = actorSystem;
        this.raft = raft;
        this.logger = logger;
        
        persistenceActorRouter = GetPersistenceRouter(persistence, configuration);
        
        IActorRef<LockBackgroundWriterActor, LockBackgroundWriteRequest> backgroundWriter = actorSystem.Spawn<LockBackgroundWriterActor, LockBackgroundWriteRequest>(
            "locks-background-writer", 
            raft, 
            persistenceActorRouter, 
            logger
        );
        
        ephemeralLocksRouter = GetEphemeralRouter(backgroundWriter, persistence, configuration);
        consistentLocksRouter = GetConsistentRouter(backgroundWriter, persistence, configuration);
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
    /// Creates the persistence router
    /// </summary>
    /// <param name="persistence"></param>
    /// <param name="configuration"></param>
    /// <returns></returns>
    private IActorRef<ConsistentHashActor<PersistenceActor, PersistenceRequest, PersistenceResponse>, PersistenceRequest, PersistenceResponse> GetPersistenceRouter(
        IPersistence persistence, 
        KahunaConfiguration configuration
    )
    {
        List<IActorRef<PersistenceActor, PersistenceRequest, PersistenceResponse>> persistenceInstances = new(configuration.PersistenceWorkers);

        for (int i = 0; i < configuration.PersistenceWorkers; i++)
            persistenceInstances.Add(actorSystem.Spawn<PersistenceActor, PersistenceRequest, PersistenceResponse>("locks-persistence-" + i, persistence, logger));

        return actorSystem.CreateConsistentHashRouter(persistenceInstances);
    }

    /// <summary>
    /// Creates the ephemeral locks router
    /// </summary>
    /// <param name="backgroundWriter"></param>
    /// <param name="persistence"></param>
    /// <param name="workers"></param>
    /// <returns></returns>
    private IActorRefStruct<ConsistentHashActorStruct<LockActor, LockRequest, LockResponse>, LockRequest, LockResponse> GetEphemeralRouter(
        IActorRef<LockBackgroundWriterActor, LockBackgroundWriteRequest> backgroundWriter, 
        IPersistence persistence, 
        KahunaConfiguration configuration
    )
    {
        List<IActorRefStruct<LockActor, LockRequest, LockResponse>> ephemeralInstances = new(configuration.LocksWorkers);

        for (int i = 0; i < configuration.LocksWorkers; i++)
            ephemeralInstances.Add(actorSystem.SpawnStruct<LockActor, LockRequest, LockResponse>("ephemeral-lock-" + i, backgroundWriter, persistence, logger));

        return actorSystem.CreateConsistentHashRouterStruct(ephemeralInstances);
    }

    /// <summary>
    /// Creates the consistent locks router
    /// </summary>
    /// <param name="backgroundWriter"></param>
    /// <param name="persistence"></param>
    /// <param name="workers"></param>
    /// <returns></returns>
    private IActorRefStruct<ConsistentHashActorStruct<LockActor, LockRequest, LockResponse>, LockRequest, LockResponse> GetConsistentRouter(
        IActorRef<LockBackgroundWriterActor, LockBackgroundWriteRequest> backgroundWriter, 
        IPersistence persistence, 
        KahunaConfiguration configuration
    )
    {
        List<IActorRefStruct<LockActor, LockRequest, LockResponse>> consistentInstances = new(configuration.LocksWorkers);

        for (int i = 0; i < configuration.LocksWorkers; i++)
            consistentInstances.Add(actorSystem.SpawnStruct<LockActor, LockRequest, LockResponse>("consistent-lock-" + i, backgroundWriter, persistence, logger));
        
        return actorSystem.CreateConsistentHashRouterStruct(consistentInstances);
    }

    /// <summary>
    /// Receives replication messages once they're committed to the Raft log.
    /// </summary>
    /// <param name="log"></param>
    /// <returns></returns>
    public async Task<bool> OnReplicationReceived(RaftLog log)
    {
        if (log.LogData is null || log.LogData.Length == 0)
            return true;
        
        if (log.LogType != ReplicationTypes.KeyValues)
            return true;
        
        try
        {
            LockMessage lockMessage = ReplicationSerializer.UnserializeLockMessage(log.LogData);

            HLCTimestamp eventTime = new(lockMessage.TimeLogical, lockMessage.TimeCounter);

            await raft.HybridLogicalClock.ReceiveEvent(eventTime);

            switch ((LockRequestType)lockMessage.Type)
            {
                case LockRequestType.TryLock:
                {
                    PersistenceResponse? response = await persistenceActorRouter.Ask(new(
                        PersistenceRequestType.StoreLock,
                        lockMessage.Resource,
                        lockMessage.Owner,
                        lockMessage.FencingToken,
                        lockMessage.ExpireLogical,
                        lockMessage.ExpireCounter,
                        lockMessage.Consistency,
                        (int)LockState.Locked
                    ));
                    
                    if (response is null)
                        return false;

                    return response.Type == PersistenceResponseType.Success;
                }

                case LockRequestType.TryUnlock:
                {
                    PersistenceResponse? response = await persistenceActorRouter.Ask(new(
                        PersistenceRequestType.StoreLock,
                        lockMessage.Resource,
                        lockMessage.Owner,
                        lockMessage.FencingToken,
                        lockMessage.ExpireLogical,
                        lockMessage.ExpireCounter,
                        lockMessage.Consistency,
                        (int)LockState.Unlocked
                    ));
                    
                    if (response is null)
                        return false;

                    return response.Type == PersistenceResponseType.Success;
                }

                case LockRequestType.TryExtendLock:
                {
                    PersistenceResponse? response = await persistenceActorRouter.Ask(new(
                        PersistenceRequestType.StoreLock,
                        lockMessage.Resource,
                        lockMessage.Owner,
                        lockMessage.FencingToken,
                        lockMessage.ExpireLogical,
                        lockMessage.ExpireCounter,
                        lockMessage.Consistency,
                        (int)LockState.Locked
                    ));

                    if (response is null)
                        return false;

                    return response.Type == PersistenceResponseType.Success;
                }

                case LockRequestType.Get:
                    break;

                default:
                    logger.LogError("Unknown replication message type: {Type}", lockMessage.Type);
                    break;
            }
        } 
        catch (Exception ex)
        {
            logger.LogError(ex, "Error processing replication message");
            return false;
        }

        return true;
    }

    /// <summary>
    /// Invoken when a replication error occurs.
    /// </summary>
    /// <param name="log"></param>
    public void OnReplicationError(RaftLog log)
    {
        logger.LogError("Replication error: #{Id} {Type}", log.Id, log.LogType);
    }

    /// <summary>
    /// Passes a TryLock request to the locker actor for the given lock name.
    /// </summary>
    /// <param name="lockName"></param>
    /// <param name="lockId"></param>
    /// <param name="expiresMs"></param>
    /// <param name="consistency"></param>
    /// <returns></returns>
    public async Task<(LockResponseType, long)> TryLock(string lockName, string lockId, int expiresMs, LockConsistency consistency)
    {
        LockRequest request = new(
            LockRequestType.TryLock, 
            lockName, 
            lockId, 
            expiresMs, 
            consistency
        );

        LockResponse response;
        
        if (consistency == LockConsistency.Ephemeral)
            response = await ephemeralLocksRouter.Ask(request);
        else
            response = await consistentLocksRouter.Ask(request);
        
        return (response.Type, response.FencingToken);
    }
    
    /// <summary>
    /// Passes a TryExtendLock request to the locker actor for the given lock name.
    /// </summary>
    /// <param name="lockName"></param>
    /// <param name="lockId"></param>
    /// <param name="expiresMs"></param>
    /// <param name="consistency"></param>
    /// <returns></returns>
    public async Task<LockResponseType> TryExtendLock(string lockName, string lockId, int expiresMs, LockConsistency consistency)
    {
        LockRequest request = new(
            LockRequestType.TryExtendLock, 
            lockName, 
            lockId, 
            expiresMs, 
            consistency
        );

        LockResponse response;
        
        if (consistency == LockConsistency.Ephemeral)
            response = await ephemeralLocksRouter.Ask(request);
        else
            response = await consistentLocksRouter.Ask(request);
        
        return response.Type;
    }

    /// <summary>
    /// Passes a TryUnlock request to the locker actor for the given lock name.
    /// </summary>
    /// <param name="lockName"></param>
    /// <param name="lockId"></param>
    /// <param name="consistency"></param>
    /// <returns></returns>
    public async Task<LockResponseType> TryUnlock(string lockName, string lockId, LockConsistency consistency)
    {
        LockRequest request = new(
            LockRequestType.TryUnlock, 
            lockName, 
            lockId, 
            0, 
            consistency
        );

        LockResponse response;
        
        if (consistency == LockConsistency.Ephemeral)
            response = await ephemeralLocksRouter.Ask(request);
        else
            response = await consistentLocksRouter.Ask(request);
        
        return response.Type;
    }
    
    /// <summary>
    /// Passes a Get request to the locker actor for the given lock name.
    /// </summary>
    /// <param name="lockName"></param>
    /// <param name="consistency"></param>
    /// <returns></returns>
    public async Task<(LockResponseType, ReadOnlyLockContext?)> GetLock(string lockName, LockConsistency consistency)
    {
        LockRequest request = new(
            LockRequestType.Get, 
            lockName, 
            "", 
            0, 
            consistency
        );

        LockResponse response;
        
        if (consistency == LockConsistency.Ephemeral)
            response = await ephemeralLocksRouter.Ask(request);
        else
            response = await consistentLocksRouter.Ask(request);
        
        return (response.Type, response.Context);
    }
}