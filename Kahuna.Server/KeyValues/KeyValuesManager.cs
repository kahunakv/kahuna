
using Nixie;
using Nixie.Routers;

using Kommander;
using Kommander.Data;
using Kommander.Time;

using Kahuna.Server.Configuration;
using Kahuna.Server.Persistence;
using Kahuna.Server.Replication;
using Kahuna.Server.Replication.Protos;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues;

public sealed class KeyValuesManager
{
    private readonly ActorSystem actorSystem;

    private readonly IRaft raft;

    private readonly IPersistence persistence;

    private readonly ILogger<IKahuna> logger;

    private readonly KeyValueLocator locator;
    
    private readonly KeyValueTransactionCoordinator txCoordinator;

    private readonly IActorRef<BackgroundWriterActor, BackgroundWriteRequest> backgroundWriter;

    private readonly IActorRef<ConsistentHashActor<KeyValueActor, KeyValueRequest, KeyValueResponse>, KeyValueRequest, KeyValueResponse> ephemeralKeyValuesRouter;
    
    private readonly IActorRef<ConsistentHashActor<KeyValueActor, KeyValueRequest, KeyValueResponse>, KeyValueRequest, KeyValueResponse> persistentKeyValuesRouter;

    private readonly List<IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse>> ephemeralInstances = [];
    
    private readonly List<IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse>> persistentInstances = [];
    
    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="actorSystem"></param>
    /// <param name="raft"></param>
    /// <param name="persistence"></param>
    /// <param name="backgroundWriter"></param>
    /// <param name="configuration"></param>
    /// <param name="logger"></param>
    public KeyValuesManager(
        ActorSystem actorSystem, 
        IRaft raft, 
        IPersistence persistence, 
        IActorRef<BackgroundWriterActor, BackgroundWriteRequest> backgroundWriter,
        KahunaConfiguration configuration, 
        ILogger<IKahuna> logger
    )
    {
        this.actorSystem = actorSystem;
        this.raft = raft;
        this.backgroundWriter = backgroundWriter;
        this.logger = logger;

        this.persistence = persistence;
        
        ephemeralKeyValuesRouter = GetEphemeralRouter(configuration);
        persistentKeyValuesRouter = GetConsistentRouter(configuration);

        txCoordinator = new(this, configuration, raft, logger);
        locator = new(this, configuration, raft, logger);
    }

    /// <summary>
    /// Creates the ephemeral key/values router
    /// </summary>
    /// <param name="backgroundWriter"></param>
    /// <param name="persistence"></param>
    /// <param name="workers"></param>
    /// <returns></returns>
    private IActorRef<ConsistentHashActor<KeyValueActor, KeyValueRequest, KeyValueResponse>, KeyValueRequest, KeyValueResponse> GetEphemeralRouter(
        KahunaConfiguration configuration
    )
    {
        logger.LogDebug("Starting {Workers} ephemeral key/value workers", configuration.KeyValuesWorkers);

        for (int i = 0; i < configuration.KeyValuesWorkers; i++)
            ephemeralInstances.Add(actorSystem.Spawn<KeyValueActor, KeyValueRequest, KeyValueResponse>("ephemeral-keyvalue-" + i, backgroundWriter, persistence, logger));

        return actorSystem.CreateConsistentHashRouter(ephemeralInstances);
    }

    /// <summary>
    /// Creates the consistent key/values router
    /// </summary>
    /// <param name="backgroundWriter"></param>
    /// <param name="persistence"></param>
    /// <param name="workers"></param>
    /// <returns></returns>
    private IActorRef<ConsistentHashActor<KeyValueActor, KeyValueRequest, KeyValueResponse>, KeyValueRequest, KeyValueResponse> GetConsistentRouter(
        KahunaConfiguration configuration
    )
    {
        logger.LogDebug("Starting {Workers} consistent key/value workers", configuration.KeyValuesWorkers);

        for (int i = 0; i < configuration.KeyValuesWorkers; i++)
            persistentInstances.Add(actorSystem.Spawn<KeyValueActor, KeyValueRequest, KeyValueResponse>("consistent-keyvalue-" + i, backgroundWriter, persistence, logger));
        
        return actorSystem.CreateConsistentHashRouter(persistentInstances);
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
            KeyValueMessage keyValueMessage = ReplicationSerializer.UnserializeKeyValueMessage(log.LogData);

            HLCTimestamp eventTime = new(keyValueMessage.TimeLogical, keyValueMessage.TimeCounter);

            await raft.HybridLogicalClock.ReceiveEvent(eventTime);

            switch ((KeyValueRequestType)keyValueMessage.Type)
            {
                case KeyValueRequestType.TrySet:
                {
                    /*PersistenceResponse? response = await persistenceActorRouter.Ask(new(
                        PersistenceRequestType.StoreKeyValue,
                        [new(
                            keyValueMessage.Key,
                            keyValueMessage.Value?.ToByteArray(),
                            keyValueMessage.Revision,
                            keyValueMessage.ExpireLogical,
                            keyValueMessage.ExpireCounter,
                            (int)KeyValueState.Set
                        )]
                    ));
                    
                    if (response is null)
                        return false;

                    if (response.Type == PersistenceResponseType.Success)
                        logger.LogDebug("Replicated key/value set {Key} {Revision} to {Node}", keyValueMessage.Key, keyValueMessage.Revision, raft.GetLocalNodeId());*/

                    backgroundWriter.Send(new(
                        BackgroundWriteType.QueueStoreKeyValue,
                        -1,
                        keyValueMessage.Key,
                        keyValueMessage.Value?.ToByteArray(),
                        keyValueMessage.Revision,
                        new(keyValueMessage.ExpireLogical, keyValueMessage.ExpireCounter),
                        (int)KeyValueState.Set
                    ));

                    return true;
                }

                case KeyValueRequestType.TryDelete:
                {
                    /*PersistenceResponse? response = await persistenceActorRouter.Ask(new(
                        PersistenceRequestType.StoreKeyValue,
                        [new(
                            keyValueMessage.Key,
                            keyValueMessage.Value?.ToByteArray(),
                            keyValueMessage.Revision,
                            keyValueMessage.ExpireLogical,
                            keyValueMessage.ExpireCounter,
                            (int)KeyValueState.Deleted
                        )]
                    ));
                    
                    if (response is null)
                        return false;

                    if (response.Type == PersistenceResponseType.Success)
                        logger.LogDebug("Replicated key/value delete {Key} {Revision} to {Node}", keyValueMessage.Key, keyValueMessage.Revision, raft.GetLocalNodeId());*/
                    
                    backgroundWriter.Send(new(
                        BackgroundWriteType.QueueStoreKeyValue,
                        -1,
                        keyValueMessage.Key,
                        keyValueMessage.Value?.ToByteArray(),
                        keyValueMessage.Revision,
                        new(keyValueMessage.ExpireLogical, keyValueMessage.ExpireCounter),
                        (int)KeyValueState.Deleted
                    ));

                    return true;
                }

                case KeyValueRequestType.TryExtend:
                {
                    /*PersistenceResponse? response = await persistenceActorRouter.Ask(new(
                        PersistenceRequestType.StoreKeyValue,
                        [new(
                            keyValueMessage.Key,
                            keyValueMessage.Value?.ToByteArray(),
                            keyValueMessage.Revision,
                            keyValueMessage.ExpireLogical,
                            keyValueMessage.ExpireCounter,
                            (int)KeyValueState.Set
                        )]
                    ));

                    if (response is null)
                        return false;

                    if (response.Type == PersistenceResponseType.Success)
                        logger.LogDebug("Replicated key/value extend {Key} {Revision} to {Node}", keyValueMessage.Key, keyValueMessage.Revision, raft.GetLocalNodeId());*/
                    
                    backgroundWriter.Send(new(
                        BackgroundWriteType.QueueStoreKeyValue,
                        -1,
                        keyValueMessage.Key,
                        keyValueMessage.Value?.ToByteArray(),
                        keyValueMessage.Revision,
                        new(keyValueMessage.ExpireLogical, keyValueMessage.ExpireCounter),
                        (int)KeyValueState.Set
                    ));

                    return true;
                }

                case KeyValueRequestType.TryGet:
                case KeyValueRequestType.TryExists:
                    break;

                default:
                    logger.LogError("Unknown replication message type: {Type}", keyValueMessage.Type);
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
    /// Locates the leader node for the given key and executes the TrySet request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="value"></param>
    /// <param name="compareValue"></param>
    /// <param name="compareRevision"></param>
    /// <param name="flags"></param>
    /// <param name="expiresMs"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, long)> LocateAndTrySetKeyValue(
        HLCTimestamp transactionId,
        string key,
        byte[]? value,
        byte[]? compareValue,
        long compareRevision,
        KeyValueFlags flags,
        int expiresMs,
        KeyValueDurability durability,
        CancellationToken cancellationToken
    )
    {
        return await locator.LocateAndTrySetKeyValue(transactionId, key, value, compareValue, compareRevision, flags, expiresMs, durability, cancellationToken);
    }

    /// <summary>
    /// Locates the leader node for the given key and executes the TryGetValue request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="revision"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, ReadOnlyKeyValueContext?)> LocateAndTryGetValue(
        HLCTimestamp transactionId, 
        string key,
        long revision, 
        KeyValueDurability durability, 
        CancellationToken cancellationToken
    )
    {
        return await locator.LocateAndTryGetValue(transactionId, key, revision, durability, cancellationToken);
    }
    
    /// <summary>
    /// Locates the leader node for the given key and executes the TryExistsValue request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="revision"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, ReadOnlyKeyValueContext?)> LocateAndTryExistsValue(
        HLCTimestamp transactionId, 
        string key,
        long revision, 
        KeyValueDurability durability, 
        CancellationToken cancellationToken
    )
    {
        return await locator.LocateAndTryExistsValue(transactionId, key, revision, durability, cancellationToken);
    }
    
    /// <summary>
    /// Locates the leader node for the given key and executes the TryDelete request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, long)> LocateAndTryDeleteKeyValue(HLCTimestamp transactionId, string key, KeyValueDurability durability, CancellationToken cancellationToken)
    {
        return await locator.LocateAndTryDeleteKeyValue(transactionId, key, durability, cancellationToken);
    }
    
    /// <summary>
    /// Locates the leader node for the given key and executes the TryExtend request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="expiresMs"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, long)> LocateAndTryExtendKeyValue(HLCTimestamp transactionId, string key, int expiresMs, KeyValueDurability durability, CancellationToken cancellationToken)
    {
        return await locator.LocateAndTryExtendKeyValue(transactionId, key, expiresMs, durability, cancellationToken);
    }
    
    /// <summary>
    /// Locates the leader node for the given key and executes the TryAcquireExclusiveLock request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="expiresMs"></param>
    /// <param name="durability"></param>
    /// <param name="cancelationToken"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, string, KeyValueDurability)> LocateAndTryAcquireExclusiveLock(HLCTimestamp transactionId, string key, int expiresMs, KeyValueDurability durability, CancellationToken cancelationToken)
    {
        return await locator.LocateAndTryAcquireExclusiveLock(transactionId, key, expiresMs, durability, cancelationToken);
    }
    
    /// <summary>
    /// Locates the leader node for the given key and executes the TryReleaseExclusiveLock request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="expiresMs"></param>
    /// <param name="durability"></param>
    /// <param name="cancelationToken"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, string)> LocateAndTryReleaseExclusiveLock(HLCTimestamp transactionId, string key, KeyValueDurability durability, CancellationToken cancelationToken)
    {
        return await locator.LocateAndTryReleaseExclusiveLock(transactionId, key, durability, cancelationToken);
    }
    
    /// <summary>
    /// Locates the leader node for the given key and executes the TryPrepareMutations request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="expiresMs"></param>
    /// <param name="durability"></param>
    /// <param name="cancelationToken"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)> LocateAndTryPrepareMutations(HLCTimestamp transactionId, string key, KeyValueDurability durability, CancellationToken cancelationToken)
    {
        return await locator.LocateAndTryPrepareMutations(transactionId, key, durability, cancelationToken);
    }
    
    /// <summary>
    /// Locates the leader node for the given key and executes the TryCommitMutations request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="ticketId"></param>
    /// <param name="durability"></param>
    /// <param name="cancelationToken"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, long)> LocateAndTryCommitMutations(HLCTimestamp transactionId, string key, HLCTimestamp ticketId, KeyValueDurability durability, CancellationToken cancelationToken)
    {
        return await locator.LocateAndTryCommitMutations(transactionId, key, ticketId, durability, cancelationToken);
    }
    
    /// <summary>
    /// Locates the leader node for the given key and executes the TryRollbackMutations request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="ticketId"></param>
    /// <param name="durability"></param>
    /// <param name="cancelationToken"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, long)> LocateAndTryRollbackMutations(HLCTimestamp transactionId, string key, HLCTimestamp ticketId, KeyValueDurability durability, CancellationToken cancelationToken)
    {
        return await locator.LocateAndTryRollbackMutations(transactionId, key, ticketId, durability, cancelationToken);
    }

    /// <summary>
    /// Passes a TrySet request to the keyValueer actor for the given keyValue name.
    /// </summary>
    /// <param name="key"></param>
    /// <param name="value"></param>
    /// <param name="compareValue"></param>
    /// <param name="compareRevision"></param>
    /// <param name="flags"></param>
    /// <param name="expiresMs"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, long)> TrySetKeyValue(
        HLCTimestamp transactionId,
        string key, 
        byte[]? value, 
        byte[]? compareValue,
        long compareRevision,
        KeyValueFlags flags,
        int expiresMs, 
        KeyValueDurability durability
    )
    {
        KeyValueRequest request = new(
            KeyValueRequestType.TrySet, 
            transactionId,
            key, 
            value, 
            compareValue,
            compareRevision,
            flags,
            expiresMs, 
            HLCTimestamp.Zero,
            durability
        );

        KeyValueResponse? response;
        
        if (durability == KeyValueDurability.Ephemeral)
            response = await ephemeralKeyValuesRouter.Ask(request);
        else
            response = await persistentKeyValuesRouter.Ask(request);
        
        if (response is null)
            return (KeyValueResponseType.Errored, -1);
        
        return (response.Type, response.Revision);
    }
    
    /// <summary>
    /// Set a timeout on key. After the timeout has expired, the key will automatically be deleted
    /// </summary>
    /// <param name="key"></param>
    /// <param name="expiresMs"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, long)> TryExtendKeyValue(
        HLCTimestamp transactionId,
        string key, 
        int expiresMs, 
        KeyValueDurability durability
    )
    {
        KeyValueRequest request = new(
            KeyValueRequestType.TryExtend,
            transactionId,
            key, 
            null, 
            null,
            -1,
            KeyValueFlags.None,
            expiresMs, 
            HLCTimestamp.Zero,
            durability
        );

        KeyValueResponse? response;
        
        if (durability == KeyValueDurability.Ephemeral)
            response = await ephemeralKeyValuesRouter.Ask(request);
        else
            response = await persistentKeyValuesRouter.Ask(request);
        
        if (response is null)
            return (KeyValueResponseType.Errored, -1);
        
        return (response.Type, response.Revision);
    }

    /// <summary>
    /// Removes the specified key. A key is ignored if it does not exist.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, long)> TryDeleteKeyValue(HLCTimestamp transactionId, string key, KeyValueDurability durability)
    {
        KeyValueRequest request = new(
            KeyValueRequestType.TryDelete, 
            transactionId,
            key, 
            null, 
            null,
            -1,
            KeyValueFlags.None,
            0, 
            HLCTimestamp.Zero,
            durability
        );

        KeyValueResponse? response;
        
        if (durability == KeyValueDurability.Ephemeral)
            response = await ephemeralKeyValuesRouter.Ask(request);
        else
            response = await persistentKeyValuesRouter.Ask(request);
        
        if (response is null)
            return (KeyValueResponseType.Errored, -1);
        
        return (response.Type, response.Revision);
    }
    
    /// <summary>
    /// Passes a Get request to the key/value actor for the given keyValue name.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, ReadOnlyKeyValueContext?)> TryGetValue(
        HLCTimestamp transactionId, 
        string key,
        long revision,
        KeyValueDurability durability
    )
    {
        KeyValueRequest request = new(
            KeyValueRequestType.TryGet, 
            transactionId, 
            key, 
            null, 
            null,
            revision,
            KeyValueFlags.None,
            0, 
            HLCTimestamp.Zero,
            durability
        );

        KeyValueResponse? response;
        
        if (durability == KeyValueDurability.Ephemeral)
            response = await ephemeralKeyValuesRouter.Ask(request);
        else
            response = await persistentKeyValuesRouter.Ask(request);
        
        if (response is null)
            return (KeyValueResponseType.Errored, null);
        
        return (response.Type, response.Context);
    }
    
    /// <summary>
    /// Passes a Exists request to the key/value actor for the given keyValue name.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, ReadOnlyKeyValueContext?)> TryExistsValue(
        HLCTimestamp transactionId, 
        string key,
        long revision,
        KeyValueDurability durability
    )
    {
        KeyValueRequest request = new(
            KeyValueRequestType.TryExists, 
            transactionId, 
            key, 
            null, 
            null,
            revision,
            KeyValueFlags.None,
            0, 
            HLCTimestamp.Zero,
            durability
        );

        KeyValueResponse? response;
        
        if (durability == KeyValueDurability.Ephemeral)
            response = await ephemeralKeyValuesRouter.Ask(request);
        else
            response = await persistentKeyValuesRouter.Ask(request);
        
        if (response is null)
            return (KeyValueResponseType.Errored, null);
        
        return (response.Type, response.Context);
    }
    
    /// <summary>
    /// Passes a TryAcquireExclusiveLock request to the key/value actor for the given keyValue name.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="expiresMs"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, string, KeyValueDurability)> TryAcquireExclusiveLock(HLCTimestamp transactionId, string key, int expiresMs, KeyValueDurability durability)
    {
        KeyValueRequest request = new(
            KeyValueRequestType.TryAcquireExclusiveLock, 
            transactionId, 
            key, 
            null, 
            null,
            -1,
            KeyValueFlags.None,
            expiresMs, 
            HLCTimestamp.Zero,
            durability
        );

        KeyValueResponse? response;
        
        if (durability == KeyValueDurability.Ephemeral)
            response = await ephemeralKeyValuesRouter.Ask(request);
        else
            response = await persistentKeyValuesRouter.Ask(request);
        
        if (response is null)
            return (KeyValueResponseType.Errored, key, durability);
        
        return (response.Type, key, durability);
    }
    
    /// <summary>
    /// Passes a TryAcquireExclusiveLock request to the key/value actor for the given keyValue name.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, string)> TryReleaseExclusiveLock(HLCTimestamp transactionId, string key, KeyValueDurability durability)
    {
        KeyValueRequest request = new(
            KeyValueRequestType.TryReleaseExclusiveLock, 
            transactionId, 
            key, 
            null, 
            null,
            -1,
            KeyValueFlags.None,
            0, 
            HLCTimestamp.Zero,
            durability
        );

        KeyValueResponse? response;
        
        if (durability == KeyValueDurability.Ephemeral)
            response = await ephemeralKeyValuesRouter.Ask(request);
        else
            response = await persistentKeyValuesRouter.Ask(request);
        
        if (response is null)
            return (KeyValueResponseType.Errored, key);
        
        return (response.Type, key);
    }
    
    /// <summary>
    /// Passes a TryPrepare request to the key/value actor for the given keyValue name.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)> TryPrepareMutations(HLCTimestamp transactionId, string key, KeyValueDurability durability)
    {
        KeyValueRequest request = new(
            KeyValueRequestType.TryPrepareMutations, 
            transactionId, 
            key, 
            null, 
            null,
            -1,
            KeyValueFlags.None,
            0, 
            HLCTimestamp.Zero,
            durability
        );

        KeyValueResponse? response;
        
        if (durability == KeyValueDurability.Ephemeral)
            response = await ephemeralKeyValuesRouter.Ask(request);
        else
            response = await persistentKeyValuesRouter.Ask(request);
        
        if (response is null)
            return (KeyValueResponseType.Errored, HLCTimestamp.Zero, key, durability);
        
        return (response.Type, response.Ticket, key, durability);
    }
    
    /// <summary>
    /// Passes a TryCommit request to the key/value actor for the given keyValue name.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="proposalTicketId"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, long)> TryCommitMutations(HLCTimestamp transactionId, string key, HLCTimestamp proposalTicketId, KeyValueDurability durability)
    {
        KeyValueRequest request = new(
            KeyValueRequestType.TryCommitMutations, 
            transactionId, 
            key, 
            null, 
            null,
            -1,
            KeyValueFlags.None,
            0, 
            proposalTicketId,
            durability
        );

        KeyValueResponse? response;
        
        if (durability == KeyValueDurability.Ephemeral)
            response = await ephemeralKeyValuesRouter.Ask(request);
        else
            response = await persistentKeyValuesRouter.Ask(request);
        
        if (response is null)
            return (KeyValueResponseType.Errored, -1);
        
        return (response.Type, response.Revision);
    }
    
    /// <summary>
    /// Passes a TryRollback request to the key/value actor for the given keyValue name.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="proposalTicketId"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, long)> TryRollbackMutations(HLCTimestamp transactionId, string key, HLCTimestamp proposalTicketId, KeyValueDurability durability)
    {
        KeyValueRequest request = new(
            KeyValueRequestType.TryRollbackMutations, 
            transactionId, 
            key, 
            null, 
            null,
            -1,
            KeyValueFlags.None,
            0, 
            proposalTicketId,
            durability
        );

        KeyValueResponse? response;
        
        if (durability == KeyValueDurability.Ephemeral)
            response = await ephemeralKeyValuesRouter.Ask(request);
        else
            response = await persistentKeyValuesRouter.Ask(request);
        
        if (response is null)
            return (KeyValueResponseType.Errored, -1);
        
        return (response.Type, response.Revision);
    }

    /// <summary>
    /// Schedule a key/value transaction to be executed
    /// </summary>
    /// <param name="script"></param>
    /// <param name="hash"></param>
    /// <param name="parameters"></param>
    /// <returns></returns>
    public async Task<KeyValueTransactionResult> TryExecuteTx(byte[] script, string? hash, List<KeyValueParameter>? parameters)
    {
        return await txCoordinator.TryExecuteTx(script, hash, parameters);
    }

    /// <summary>
    /// Scans all nodes in the cluster and returns key/value pairs by prefix 
    /// </summary>
    /// <param name="prefixKeyName"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public async Task<KeyValueGetByPrefixResult> ScanAllByPrefix(string prefixKeyName, KeyValueDurability durability)
    {
        return await locator.ScanAllByPrefix(prefixKeyName, durability);
    }

    /// <summary>
    /// Scans the current node and returns key/value pairs by prefix 
    /// </summary>
    /// <param name="prefixKeyName"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    /// <exception cref="KahunaServerException"></exception>
    public async Task<KeyValueGetByPrefixResult> ScanByPrefix(string prefixKeyName, KeyValueDurability durability)
    {
        HLCTimestamp currentTime = await raft.HybridLogicalClock.TrySendOrLocalEvent();
        
        KeyValueRequest request = new(
            KeyValueRequestType.ScanByPrefix,
            currentTime,
            prefixKeyName,
            null,
            null,
            -1,
            KeyValueFlags.None,
            0,
            HLCTimestamp.Zero,
            durability
        );
        
        List<(string, ReadOnlyKeyValueContext)> items = [];
        
        if (durability == KeyValueDurability.Ephemeral)
        {
            List<Task<KeyValueResponse?>> tasks = new(ephemeralInstances.Count);
            
            // Ephemeral GetByPrefix does a brute force search on every ephemeral actor
            foreach (IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actor in ephemeralInstances)
                tasks.Add(actor.Ask(request));
            
            KeyValueResponse?[] responses = await Task.WhenAll(tasks);

            foreach (KeyValueResponse? response in responses)
            {
                if (response is { Type: KeyValueResponseType.Get, Items: not null })
                    items.AddRange(response.Items);    
            }
            
            return new(items);
        }

        if (durability == KeyValueDurability.Persistent)
        {
            List<Task<KeyValueResponse?>> tasks = new(persistentInstances.Count);
            
            // Persistent GetByPrefix does a brute force search on every persistent actor
            foreach (IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actor in persistentInstances)
                tasks.Add(actor.Ask(request));
            
            KeyValueResponse?[] responses = await Task.WhenAll(tasks);

            foreach (KeyValueResponse? response in responses)
            {
                if (response is { Type: KeyValueResponseType.Get, Items: not null })
                    items.AddRange(response.Items);    
            }
            
            return new(items);
        }

        throw new KahunaServerException("Unknown durability");
    }
}