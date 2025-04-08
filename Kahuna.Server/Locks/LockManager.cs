
using Nixie;
using Nixie.Routers;

using Kommander;
using Kommander.Data;
using Kommander.Time;

using Kahuna.Shared.Locks;
using Kahuna.Server.Configuration;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Server.Replication;
using Kahuna.Server.Replication.Protos;
using Kahuna.Server.Communication.Internode;

namespace Kahuna.Server.Locks;

/// <summary>
/// LockManager is a singleton class that manages lock actors.
/// </summary>
public sealed class LockManager
{
    private readonly ActorSystem actorSystem;

    private readonly IRaft raft;

    private readonly ILogger<IKahuna> logger;

    private readonly IActorRef<BackgroundWriterActor, BackgroundWriteRequest> backgroundWriter;

    private readonly IActorRefStruct<ConsistentHashActorStruct<LockActor, LockRequest, LockResponse>, LockRequest, LockResponse> ephemeralLocksRouter;
    
    private readonly IActorRefStruct<ConsistentHashActorStruct<LockActor, LockRequest, LockResponse>, LockRequest, LockResponse> persistentLocksRouter;

    private readonly LockLocator locator;
    
    private readonly LockRestorer restorer;

    private readonly LockReplicator replicator;
    
    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="actorSystem"></param>
    /// <param name="raft"></param>
    /// <param name="persistenceBackend"></param>
    /// <param name="backgroundWriter"></param>
    /// <param name="configuration"></param>
    /// <param name="logger"></param>
    public LockManager(
        ActorSystem actorSystem, 
        IRaft raft, 
        IInterNodeCommunication interNodeCommunication,
        IPersistenceBackend persistenceBackend, 
        IActorRef<BackgroundWriterActor, BackgroundWriteRequest> backgroundWriter,
        KahunaConfiguration configuration, 
        ILogger<IKahuna> logger
    )
    {
        this.actorSystem = actorSystem;
        this.raft = raft;
        this.backgroundWriter = backgroundWriter;
        this.logger = logger;
        
        locator = new(this, configuration, raft, interNodeCommunication, logger);
        
        ephemeralLocksRouter = GetEphemeralRouter(persistenceBackend, configuration);
        persistentLocksRouter = GetPersistentRouter(persistenceBackend, configuration);
        
        restorer = new(backgroundWriter, raft, logger);
        replicator = new(backgroundWriter, raft, logger);
    }

    /// <summary>
    /// Creates the ephemeral locks router
    /// </summary>
    /// <param name="backgroundWriter"></param>
    /// <param name="persistenceBackend"></param>
    /// <param name="workers"></param>
    /// <returns></returns>
    private IActorRefStruct<ConsistentHashActorStruct<LockActor, LockRequest, LockResponse>, LockRequest, LockResponse> GetEphemeralRouter(
        IPersistenceBackend persistenceBackend, 
        KahunaConfiguration configuration
    )
    {
        List<IActorRefStruct<LockActor, LockRequest, LockResponse>> ephemeralInstances = new(configuration.LocksWorkers);

        for (int i = 0; i < configuration.LocksWorkers; i++)
            ephemeralInstances.Add(actorSystem.SpawnStruct<LockActor, LockRequest, LockResponse>("ephemeral-lock-" + i, backgroundWriter, persistenceBackend, raft, logger));

        return actorSystem.CreateConsistentHashRouterStruct(ephemeralInstances);
    }

    /// <summary>
    /// Creates the consistent locks router
    /// </summary>
    /// <param name="backgroundWriter"></param>
    /// <param name="persistenceBackend"></param>
    /// <param name="workers"></param>
    /// <returns></returns>
    private IActorRefStruct<ConsistentHashActorStruct<LockActor, LockRequest, LockResponse>, LockRequest, LockResponse> GetPersistentRouter(
        IPersistenceBackend persistenceBackend, 
        KahunaConfiguration configuration
    )
    {
        List<IActorRefStruct<LockActor, LockRequest, LockResponse>> persistentInstances = new(configuration.LocksWorkers);

        for (int i = 0; i < configuration.LocksWorkers; i++)
            persistentInstances.Add(actorSystem.SpawnStruct<LockActor, LockRequest, LockResponse>("persistent-lock-" + i, backgroundWriter, persistenceBackend, raft, logger));
        
        return actorSystem.CreateConsistentHashRouterStruct(persistentInstances);
    }
    
    /// <summary>
    /// Receives restore messages that haven't been checkpointed yet.
    /// </summary>
    /// <param name="log"></param>
    /// <returns></returns>
    public Task<bool> OnLogRestored(RaftLog log)
    {
        return Task.FromResult(log.LogType != ReplicationTypes.Locks || restorer.Restore(log));
    }

    /// <summary>
    /// Receives replication messages once they're committed to the Raft log.
    /// </summary>
    /// <param name="log"></param>
    /// <returns></returns>
    public Task<bool> OnReplicationReceived(RaftLog log)
    {
        return Task.FromResult(log.LogType != ReplicationTypes.Locks || replicator.Replicate(log));
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
    /// Locates the leader node for the given key and passes a TryLock request to the locker actor for the given lock name.
    /// </summary>
    /// <param name="resource"></param>
    /// <param name="owner"></param>
    /// <param name="expiresMs"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public Task<(LockResponseType, long)> LocateAndTryLock(string resource, byte[] owner, int expiresMs, LockDurability durability, CancellationToken cancellationToken)
    {
        return locator.LocateAndTryLock(resource, owner, expiresMs, durability, cancellationToken);
    }
    
    /// <summary>
    /// Locates the leader node for the given key and passes a TryExtend request to the locker actor for the given lock name.
    /// </summary>
    /// <param name="resource"></param>
    /// <param name="owner"></param>
    /// <param name="expiresMs"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public Task<(LockResponseType, long)> LocateAndTryExtendLock(string resource, byte[] owner, int expiresMs, LockDurability durability, CancellationToken cancellationToken)
    {
        return locator.LocateAndTryExtendLock(resource, owner, expiresMs, durability, cancellationToken);
    }

    /// <summary>
    /// Locates the leader node for the given key and passes a TryUnlock request to the locker actor for the given lock name.
    /// </summary>
    /// <param name="resource"></param>
    /// <param name="owner"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public Task<LockResponseType> LocateAndTryUnlock(string resource, byte[] owner, LockDurability durability, CancellationToken cancellationToken)
    {
        return locator.LocateAndTryUnlock(resource, owner, durability, cancellationToken);
    }

    /// <summary>
    /// Locates the leader node for the given key and passes a TryGet request to the locker actor for the given lock name.
    /// </summary>
    /// <param name="resource"></param>
    /// <param name="owner"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public Task<(LockResponseType, ReadOnlyLockContext?)> LocateAndGetLock(string resource, LockDurability durability, CancellationToken cancellationToken)
    {
        return locator.LocateAndGetLock(resource, durability, cancellationToken);
    }

    /// <summary>
    /// Passes a TryLock request to the locker actor for the given lock name.
    /// </summary>
    /// <param name="resource"></param>
    /// <param name="owner"></param>
    /// <param name="expiresMs"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public async Task<(LockResponseType, long)> TryLock(string resource, byte[] owner, int expiresMs, LockDurability durability)
    {
        LockRequest request = new(
            LockRequestType.TryLock, 
            resource, 
            owner, 
            expiresMs, 
            durability
        );

        LockResponse response;
        
        if (durability == LockDurability.Ephemeral)
            response = await ephemeralLocksRouter.Ask(request);
        else
            response = await persistentLocksRouter.Ask(request);
        
        return (response.Type, response.FencingToken);
    }
    
    /// <summary>
    /// Passes a TryExtendLock request to the locker actor for the given lock name.
    /// </summary>
    /// <param name="resource"></param>
    /// <param name="owners"></param>
    /// <param name="expiresMs"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public async Task<(LockResponseType, long)> TryExtendLock(string resource, byte[] owner, int expiresMs, LockDurability durability)
    {
        LockRequest request = new(
            LockRequestType.TryExtendLock, 
            resource, 
            owner, 
            expiresMs, 
            durability
        );

        LockResponse response;
        
        if (durability == LockDurability.Ephemeral)
            response = await ephemeralLocksRouter.Ask(request);
        else
            response = await persistentLocksRouter.Ask(request);
        
        return (response.Type, response.FencingToken);
    }

    /// <summary>
    /// Passes a TryUnlock request to the locker actor for the given lock name.
    /// </summary>
    /// <param name="resource"></param>
    /// <param name="owner"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public async Task<LockResponseType> TryUnlock(string resource, byte[] owner, LockDurability durability)
    {
        LockRequest request = new(
            LockRequestType.TryUnlock, 
            resource, 
            owner, 
            0, 
            durability
        );

        LockResponse response;
        
        if (durability == LockDurability.Ephemeral)
            response = await ephemeralLocksRouter.Ask(request);
        else
            response = await persistentLocksRouter.Ask(request);
        
        return response.Type;
    }
    
    /// <summary>
    /// Passes a Get request to the locker actor for the given lock name.
    /// </summary>
    /// <param name="resource"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public async Task<(LockResponseType, ReadOnlyLockContext?)> GetLock(string resource, LockDurability durability)
    {
        LockRequest request = new(
            LockRequestType.Get, 
            resource, 
            null, 
            0, 
            durability
        );

        LockResponse response;
        
        if (durability == LockDurability.Ephemeral)
            response = await ephemeralLocksRouter.Ask(request);
        else
            response = await persistentLocksRouter.Ask(request);
        
        return (response.Type, response.Context);
    }
}