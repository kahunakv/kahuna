
using Nixie;
using Nixie.Routers;

using Kommander;
using Kommander.Data;
using Kommander.WAL.IO;
using Polly.Contrib.WaitAndRetry;
using Kahuna.Utils;

using Kahuna.Shared.Locks;
using Kahuna.Server.Configuration;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Server.Replication;
using Kahuna.Server.Communication.Internode;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.Locks.Data;

namespace Kahuna.Server.Locks;

/// <summary>
/// Provides functionality for managing resource locks within the system.
/// The LockManager class is responsible for coordinating lock-related operations
/// including acquiring, extending, releasing, and querying locks. Additionally, it
/// handles replication and log restoration events to maintain consistency across nodes.
/// </summary>
internal sealed class LockManager
{
    private const int MaxRetries = 3;
    
    private readonly ActorSystem actorSystem;

    private readonly IRaft raft;

    private readonly IRaftReadScheduler backendReadScheduler;

    private readonly ILogger<IKahuna> logger;

    /// <summary>
    /// A reference to the background writer actor responsible for processing
    /// background write requests in a concurrent and non-blocking manner.
    /// This actor handles operations related to persistence
    /// and other background tasks required by the LockManager.
    /// </summary>
    private readonly IActorRef<BackgroundWriterActor, BackgroundWriteRequest> backgroundWriter;

    /// <summary>
    /// 
    /// </summary>
    private readonly IActorRef<BalancingActor<LockProposalActor, LockProposalRequest>, LockProposalRequest> proposalRouter;

    /// <summary>
    /// A router responsible for managing and dispatching ephemeral lock-related requests
    /// to a dynamic pool of <see cref="LockActor"/> instances. This router handles the
    /// consistency and routing of <see cref="LockRequest"/> messages that pertain to locks
    /// with ephemeral durability, ensuring non-persistent, transient lock operations
    /// are processed efficiently.
    /// </summary>
    private readonly IActorRef<ConsistentHashActor<LockActor, LockRequest, LockResponse>, LockRequest, LockResponse> ephemeralLocksRouter;

    /// <summary>
    /// A router used to manage persistent lock actors for handling lock requests
    /// and responses for locks with persistent durability. This router ensures
    /// that lock operations are consistently hashed, enabling efficient and
    /// deterministic routing of lock requests to the appropriate lock actor
    /// instances.
    /// </summary>
    private readonly IActorRef<ConsistentHashActor<LockActor, LockRequest, LockResponse>, LockRequest, LockResponse> persistentLocksRouter;

    /// <summary>
    /// Routes a resource to its data partition so lock operations can validate leadership and
    /// carry the partition id for read-scheduler routing.
    /// </summary>
    private readonly DataPartitionRouter dataPartitionRouter;

    /// <summary>
    ///
    /// </summary>
    private readonly LockLocator locator;
    
    /// <summary>
    /// 
    /// </summary>
    private readonly LockRestorer restorer;

    /// <summary>
    /// 
    /// </summary>
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
    /// <summary>Per-partition application-durability floor tracker, shared node-wide.</summary>
    private readonly PartitionDurabilityTracker? durabilityTracker;

    /// <summary>Retained lock-actor shard lists (also addressed through the routers), so
    /// partition-scoped maintenance can broadcast to every shard.</summary>
    private readonly List<IActorRef<LockActor, LockRequest, LockResponse>> ephemeralLockInstances = [];

    private readonly List<IActorRef<LockActor, LockRequest, LockResponse>> persistentLockInstances = [];

    public LockManager(
        ActorSystem actorSystem,
        IRaft raft,
        IRaftReadScheduler backendReadScheduler,
        IInterNodeCommunication interNodeCommunication,
        IPersistenceBackend persistenceBackend,
        IActorRef<BackgroundWriterActor, BackgroundWriteRequest> backgroundWriter,
        KahunaConfiguration configuration,
        ILogger<IKahuna> logger,
        PartitionDurabilityTracker? durabilityTracker = null
    )
    {
        this.actorSystem = actorSystem;
        this.raft = raft;
        this.backendReadScheduler = backendReadScheduler;
        this.backgroundWriter = backgroundWriter;
        this.durabilityTracker = durabilityTracker;
        this.logger = logger;

        dataPartitionRouter = new(raft);
        locator = new(this, configuration, raft, interNodeCommunication, logger);

        proposalRouter = GetProposalRouter(persistenceBackend, configuration);
        ephemeralLocksRouter = GetEphemeralRouter(persistenceBackend, configuration);
        persistentLocksRouter = GetPersistentRouter(persistenceBackend, configuration);

        // The unflushed-lock-writes overlay travels with the decorated backend; the replicator and
        // restorer record queued mutations into it synchronously so lock reads observe them before
        // the flush lands. A raw (undecorated) backend yields null and recording becomes a no-op.
        UnflushedLockWritesIndex? unflushedLockWrites = (persistenceBackend as UnflushedOverlayPersistenceBackend)?.UnflushedLockWrites;

        // The persistent router lets the restorer/replicator send cache-coherence applies to the
        // owning lock actors, so a resident entry on a node that lost leadership tracks committed
        // mutations instead of staying frozen at that node's last tenure.
        restorer = new(backgroundWriter, raft, logger, unflushedLockWrites, durabilityTracker, persistentLocksRouter);
        replicator = new(backgroundWriter, raft, logger, unflushedLockWrites, durabilityTracker, persistentLocksRouter);
    }

    /// <summary>
    /// Broadcasts a partition eviction to every lock actor shard (ephemeral and persistent) so no
    /// shard retains a resident lease for the partition. Invoked when this node stops being one of
    /// the partition's replicas, and after a whole-partition snapshot install replaced the
    /// partition's backend rows — in both cases a retained resident lease could later be served
    /// (and minted from) ahead of newer committed state. Completes when every shard has processed
    /// the eviction.
    /// </summary>
    internal async Task EvictPartitionLocksAsync(int partitionId)
    {
        LockRequest request = new(LockRequestType.EvictPartition, string.Empty, null, 0, LockDurability.Persistent, 0, partitionId, null);

        List<Task<LockResponse?>> tasks = new(ephemeralLockInstances.Count + persistentLockInstances.Count);

        foreach (IActorRef<LockActor, LockRequest, LockResponse> actor in ephemeralLockInstances)
            tasks.Add(actor.Ask(request)!);

        foreach (IActorRef<LockActor, LockRequest, LockResponse> actor in persistentLockInstances)
            tasks.Add(actor.Ask(request)!);

        await Task.WhenAll(tasks);
    }

    /// <summary>
    /// Creates the ephemeral locks router
    /// </summary>
    /// <param name="backgroundWriter"></param>
    /// <param name="persistenceBackend"></param>
    /// <param name="workers"></param>
    /// <returns></returns>
    private IActorRef<ConsistentHashActor<LockActor, LockRequest, LockResponse>, LockRequest, LockResponse> GetEphemeralRouter(
        IPersistenceBackend persistenceBackend, 
        KahunaConfiguration configuration
    )
    {
        for (int i = 0; i < configuration.LocksWorkers; i++)
            ephemeralLockInstances.Add(actorSystem.Spawn<LockActor, LockRequest, LockResponse>(
                "ephemeral-lock-" + i,
                backgroundWriter,
                proposalRouter,
                persistenceBackend,
                raft,
                backendReadScheduler,
                configuration,
                logger
            ));

        return actorSystem.CreateConsistentHashRouter(ephemeralLockInstances);
    }

    /// <summary>
    /// Creates the consistent locks router
    /// </summary>
    /// <param name="backgroundWriter"></param>
    /// <param name="persistenceBackend"></param>
    /// <param name="workers"></param>
    /// <returns></returns>
    private IActorRef<ConsistentHashActor<LockActor, LockRequest, LockResponse>, LockRequest, LockResponse> GetPersistentRouter(
        IPersistenceBackend persistenceBackend, 
        KahunaConfiguration configuration
    )
    {
        for (int i = 0; i < configuration.LocksWorkers; i++)
            persistentLockInstances.Add(actorSystem.Spawn<LockActor, LockRequest, LockResponse>(
                "persistent-lock-" + i,
                backgroundWriter,
                proposalRouter,
                persistenceBackend,
                raft,
                backendReadScheduler,
                configuration,
                logger
            ));

        return actorSystem.CreateConsistentHashRouter(persistentLockInstances);
    }

    private IActorRef<BalancingActor<LockProposalActor, LockProposalRequest>, LockProposalRequest> GetProposalRouter(
        IPersistenceBackend persistenceBackend, 
        KahunaConfiguration configuration
    )
    {
        List<IActorRef<LockProposalActor, LockProposalRequest>> proposalInstances = new(configuration.LocksWorkers);

        for (int i = 0; i < configuration.LocksWorkers; i++)
            proposalInstances.Add(actorSystem.Spawn<LockProposalActor, LockProposalRequest>(
                "proposal-lock-" + i,
                raft,
                persistenceBackend,
                configuration,
                logger
            ));
        
        return actorSystem.Spawn<BalancingActor<LockProposalActor, LockProposalRequest>, LockProposalRequest>(null, proposalInstances);
    }
    
    /// <summary>
    /// Receives restore messages that haven't been checkpointed yet.
    /// </summary>
    /// <param name="partitionId"></param>
    /// <param name="log"></param>
    /// <returns></returns>
    public Task<bool> OnLogRestored(int partitionId, RaftLog log)
    {
        return Task.FromResult(log.LogType != ReplicationTypes.Locks || restorer.Restore(partitionId, log));
    }

    /// <summary>
    /// Receives replication messages once they're committed to the Raft log.
    /// </summary>
    /// <param name="partitionId"></param>
    /// <param name="log"></param>
    /// <returns></returns>
    public Task<bool> OnReplicationReceived(int partitionId, RaftLog log)
    {
        return Task.FromResult(log.LogType != ReplicationTypes.Locks || replicator.Replicate(partitionId, log));
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
    public Task<(LockResponseType, long)> LocateAndTryLock(
        string resource,
        byte[] owner, 
        int expiresMs, 
        LockDurability durability, 
        CancellationToken cancellationToken
    )
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
    public Task<(LockResponseType, long)> LocateAndTryExtendLock(
        string resource, 
        byte[] owner, 
        int expiresMs, 
        LockDurability durability, 
        CancellationToken cancellationToken
    )
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
    public Task<(LockResponseType, ReadOnlyLockEntry?)> LocateAndGetLock(string resource, LockDurability durability, CancellationToken cancellationToken)
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
        int partitionId = dataPartitionRouter.Locate(resource);

        // Forwarded requests arrive here without any leadership validation: the sender resolved
        // the leader from its cached view, which can name a node that is still mid-promotion (its
        // lock projection behind the committed log) or already deposed. Answering from that state
        // is how a released lock resurfaces as Busy. A non-leader must refuse retryably so the
        // caller re-resolves and lands on the published, fully caught-up leader.
        if (!await raft.AmILeaderIfHosted(partitionId, CancellationToken.None))
            return (LockResponseType.MustRetry, 0);

        LockRequest request = new(
            LockRequestType.TryLock,
            resource,
            owner,
            expiresMs,
            durability,
            0,
            partitionId,
            null
        );

        LazyRetryDelays retryDelays = new(TimeSpan.FromMilliseconds(1), MaxRetries);
        for (int retryAttempt = 0; retryAttempt < MaxRetries; retryAttempt++)
        {
            LockResponse? response;

            if (durability == LockDurability.Ephemeral)
                response = await ephemeralLocksRouter.Ask(request);
            else
                response = await persistentLocksRouter.Ask(request);

            if (response is null)
                return (LockResponseType.Errored, 0);

            if (response.Type != LockResponseType.WaitingForReplication)
                return (response.Type, response.FencingToken);

            if (retryDelays.TryNext(out TimeSpan delay)) await Task.Delay(delay);
        }

        return (LockResponseType.MustRetry, 0);
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
        int partitionId = dataPartitionRouter.Locate(resource);

        // See TryLock: forwarded requests must not be answered from a non-leader's stale state.
        if (!await raft.AmILeaderIfHosted(partitionId, CancellationToken.None))
            return (LockResponseType.MustRetry, 0);

        LockRequest request = new(
            LockRequestType.TryExtendLock,
            resource,
            owner,
            expiresMs,
            durability,
            0,
            partitionId,
            null
        );

        LazyRetryDelays retryDelays = new(TimeSpan.FromMilliseconds(1), MaxRetries);
        for (int retryAttempt = 0; retryAttempt < MaxRetries; retryAttempt++)
        {
            LockResponse? response;

            if (durability == LockDurability.Ephemeral)
                response = await ephemeralLocksRouter.Ask(request);
            else
                response = await persistentLocksRouter.Ask(request);

            if (response is null)
                return (LockResponseType.Errored, 0);

            if (response.Type != LockResponseType.WaitingForReplication)
                return (response.Type, response.FencingToken);

            if (retryDelays.TryNext(out TimeSpan delay)) await Task.Delay(delay);
        }

        return (LockResponseType.MustRetry, 0);
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
        int partitionId = dataPartitionRouter.Locate(resource);

        // See TryLock: forwarded requests must not be answered from a non-leader's stale state.
        if (!await raft.AmILeaderIfHosted(partitionId, CancellationToken.None))
            return LockResponseType.MustRetry;

        LockRequest request = new(
            LockRequestType.TryUnlock,
            resource,
            owner,
            0,
            durability,
            0,
            partitionId,
            null
        );

        LazyRetryDelays retryDelays = new(TimeSpan.FromMilliseconds(1), MaxRetries);
        for (int retryAttempt = 0; retryAttempt < MaxRetries; retryAttempt++)
        {
            LockResponse? response;

            if (durability == LockDurability.Ephemeral)
                response = await ephemeralLocksRouter.Ask(request);
            else
                response = await persistentLocksRouter.Ask(request);

            if (response is null)
                return LockResponseType.Errored;

            if (response.Type != LockResponseType.WaitingForReplication)
                return response.Type;

            if (retryDelays.TryNext(out TimeSpan delay)) await Task.Delay(delay);
        }

        return LockResponseType.MustRetry;
    }
    
    /// <summary>
    /// Passes a Get request to the locker actor for the given lock name.
    /// </summary>
    /// <param name="resource"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public async Task<(LockResponseType, ReadOnlyLockEntry?)> GetLock(string resource, LockDurability durability)
    {
        int partitionId = dataPartitionRouter.Locate(resource);

        // Reads mirror the locator's quorum-confirmed gate (read-index): a forwarded Get answered
        // by a minority-partitioned or mid-promotion node would report a stale holder/token.
        if (!await raft.ConfirmLeadershipIfHosted(partitionId, CancellationToken.None))
            return (LockResponseType.MustRetry, null);

        LockRequest request = new(
            LockRequestType.Get,
            resource,
            null,
            0,
            durability,
            0,
            partitionId,
            null
        );

        LockResponse? response;
        
        if (durability == LockDurability.Ephemeral)
            response = await ephemeralLocksRouter.Ask(request);
        else
            response = await persistentLocksRouter.Ask(request);
        
        if (response is null)
            return (LockResponseType.Errored, null);
        
        return (response.Type, response.Context);
    }
}