
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
public sealed class LockManager : IKahuna
{
    private readonly ActorSystem actorSystem;

    private readonly IRaft raft;

    private readonly ILogger<IKahuna> logger;
    
    private readonly IActorRef<PersistenceActor, PersistenceRequest, PersistenceResponse> persistenceActor;

    private readonly IActorRefStruct<ConsistentHashActorStruct<LockActor, LockRequest, LockResponse>, LockRequest, LockResponse> ephemeralLocksRouter;
    
    private readonly IActorRefStruct<ConsistentHashActorStruct<LockActor, LockRequest, LockResponse>, LockRequest, LockResponse> consistentLocksRouter;
    
    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="actorSystem"></param>
    /// <param name="raft"></param>
    /// <param name="logger"></param>
    public LockManager(ActorSystem actorSystem, IRaft raft, ILogger<IKahuna> logger)
    {
        this.actorSystem = actorSystem;
        this.raft = raft;
        this.logger = logger;

        SqlitePersistence persistence = new("/app/data", "v1");
        persistenceActor = actorSystem.Spawn<PersistenceActor, PersistenceRequest, PersistenceResponse>("locks-persistence", persistence, logger);
        
        IActorRef<LockBackgroundWriterActor, LockBackgroundWriteRequest> backgroundWriter = actorSystem.Spawn<LockBackgroundWriterActor, LockBackgroundWriteRequest>("locks-background-writer", raft, persistenceActor, logger);

        int workers = Math.Max(32, Environment.ProcessorCount * 4);
        
        List<IActorRefStruct<LockActor, LockRequest, LockResponse>> ephemeralInstances = new(workers);

        for (int i = 0; i < workers; i++)
            ephemeralInstances.Add(actorSystem.SpawnStruct<LockActor, LockRequest, LockResponse>("ephemeral-lock-" + i, backgroundWriter, persistence, logger));
        
        List<IActorRefStruct<LockActor, LockRequest, LockResponse>> consistentInstances = new(workers);

        for (int i = 0; i < workers; i++)
            consistentInstances.Add(actorSystem.SpawnStruct<LockActor, LockRequest, LockResponse>("consistent-lock-" + i, backgroundWriter, persistence, logger));

        ephemeralLocksRouter = actorSystem.CreateConsistentHashRouterStruct(ephemeralInstances);
        consistentLocksRouter = actorSystem.CreateConsistentHashRouterStruct(consistentInstances);
    }

    public async Task<bool> OnReplicationReceived(string type, byte[] data)
    {
        try
        {
            LockMessage lockMessage = ReplicationSerializer.Unserializer(data);

            HLCTimestamp eventTime = new(lockMessage.TimeLogical, lockMessage.TimeCounter);

            HLCTimestamp currentTime = await raft.HybridLogicalClock.ReceiveEvent(eventTime);
            
            //Console.WriteLine("Expires {0} {1} {2}:{3}", (LockRequestType)lockMessage.Type, lockMessage.Resource, lockMessage.ExpireLogical, lockMessage.ExpireCounter);

            //HLCTimestamp expires = new(lockMessage.ExpireLogical, lockMessage.ExpireCounter);
            //TimeSpan timeSpan = expires - currentTime;

            switch ((LockRequestType)lockMessage.Type)
            {
                case LockRequestType.TryLock:
                {
                    PersistenceResponse? response = await persistenceActor.Ask(new(
                        PersistenceRequestType.Store,
                        lockMessage.Resource,
                        lockMessage.Owner,
                        lockMessage.FencingToken,
                        lockMessage.ExpireLogical,
                        lockMessage.ExpireCounter,
                        (LockConsistency)lockMessage.Consistency,
                        LockState.Locked
                    ));
                    
                    if (response is null)
                        return false;

                    return response.Type == PersistenceResponseType.Success;
                }

                case LockRequestType.TryUnlock:
                {
                    PersistenceResponse? response = await persistenceActor.Ask(new(
                        PersistenceRequestType.Store,
                        lockMessage.Resource,
                        lockMessage.Owner,
                        lockMessage.FencingToken,
                        lockMessage.ExpireLogical,
                        lockMessage.ExpireCounter,
                        (LockConsistency)lockMessage.Consistency,
                        LockState.Unlocked
                    ));
                    
                    if (response is null)
                        return false;

                    return response.Type == PersistenceResponseType.Success;
                }

                case LockRequestType.TryExtendLock:
                {
                    PersistenceResponse? response = await persistenceActor.Ask(new(
                        PersistenceRequestType.Store,
                        lockMessage.Resource,
                        lockMessage.Owner,
                        lockMessage.FencingToken,
                        lockMessage.ExpireLogical,
                        lockMessage.ExpireCounter,
                        (LockConsistency)lockMessage.Consistency,
                        LockState.Locked
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