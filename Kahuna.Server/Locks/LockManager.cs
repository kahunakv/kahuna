
using Kahuna.Persistence;
using Kahuna.Replication;
using Kahuna.Replication.Protos;
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
    
    private readonly SqlitePersistence sqlitePersistence;
    
    private readonly IActorRefStruct<ConsistentHashActorStruct<LockActor, LockRequest, LockResponse>, LockRequest, LockResponse> ephemeralLocksRouter;
    
    private readonly IActorRefStruct<ConsistentHashActorStruct<LockActor, LockRequest, LockResponse>, LockRequest, LockResponse> persistentLocksRouter;
    
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

        this.sqlitePersistence = new SqlitePersistence("/app/data", "v1");

        int workers = Math.Max(32, Environment.ProcessorCount * 4);
        
        List<IActorRefStruct<LockActor, LockRequest, LockResponse>> ephemeralInstances = new(workers);

        for (int i = 0; i < workers; i++)
            ephemeralInstances.Add(actorSystem.SpawnStruct<LockActor, LockRequest, LockResponse>(null, logger));
        
        List<IActorRefStruct<LockActor, LockRequest, LockResponse>> persistentInstances = new(workers);

        for (int i = 0; i < workers; i++)
            persistentInstances.Add(actorSystem.SpawnStruct<LockActor, LockRequest, LockResponse>(null, logger));

        ephemeralLocksRouter = actorSystem.CreateConsistentHashRouterStruct(ephemeralInstances);
        persistentLocksRouter = actorSystem.CreateConsistentHashRouterStruct(persistentInstances);
    }

    public async Task<bool> OnReplicationReceived(string type, byte[] data)
    {
        try
        {
            LockMessage lockMessage = ReplicationSerializer.Unserializer(data);

            HLCTimestamp eventTime = new(lockMessage.TimeLogical, lockMessage.TimeCounter);

            HLCTimestamp currentTime = await raft.HybridLogicalClock.ReceiveEvent(eventTime);
            
            Console.WriteLine("Expires {0} {1} {2}:{3}", (LockRequestType)lockMessage.Type, lockMessage.Resource, lockMessage.ExpireLogical, lockMessage.ExpireCounter);

            //HLCTimestamp expires = new(lockMessage.ExpireLogical, lockMessage.ExpireCounter);
            //TimeSpan timeSpan = expires - currentTime;

            switch ((LockRequestType)lockMessage.Type)
            {
                case LockRequestType.TryLock:
                    await sqlitePersistence.UpdateLock(
                        lockMessage.Resource,
                        lockMessage.Owner,
                        lockMessage.ExpireLogical,
                        lockMessage.ExpireCounter,
                        lockMessage.FencingToken,
                        lockMessage.Consistency,
                        LockState.Locked
                    );
                    break;

                case LockRequestType.TryUnlock:
                    await sqlitePersistence.UpdateLock(
                        lockMessage.Resource,
                        lockMessage.Owner,
                        lockMessage.ExpireLogical,
                        lockMessage.ExpireCounter,
                        lockMessage.FencingToken,
                        lockMessage.Consistency,
                        LockState.Unlocked
                    );
                    break;

                case LockRequestType.TryExtendLock:
                    await sqlitePersistence.UpdateLock(
                        lockMessage.Resource,
                        lockMessage.Owner,
                        lockMessage.ExpireLogical,
                        lockMessage.ExpireCounter,
                        lockMessage.FencingToken,
                        lockMessage.Consistency,
                        LockState.Locked
                    );
                    break;

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
            response = await persistentLocksRouter.Ask(request);
        
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
            response = await persistentLocksRouter.Ask(request);
        
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
            response = await persistentLocksRouter.Ask(request);
        
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
            response = await persistentLocksRouter.Ask(request);
        
        return (response.Type, response.Context);
    }
}