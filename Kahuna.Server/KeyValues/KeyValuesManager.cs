
using Nixie;
using Nixie.Routers;

using Kommander;
using Kommander.Data;
using Kommander.Time;

using Kahuna.Configuration;
using Kahuna.Server.Persistence;
using Kahuna.Replication;
using Kahuna.Replication.Protos;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues;

public sealed class KeyValuesManager
{
    private readonly ActorSystem actorSystem;

    private readonly IRaft raft;

    private readonly ILogger<IKahuna> logger;

    private readonly KeyValueLocator locator;
    
    private readonly KeyValueTransactionCoordinator txCoordinator;

    private readonly IActorRef<ConsistentHashActor<PersistenceActor, PersistenceRequest, PersistenceResponse>, PersistenceRequest, PersistenceResponse> persistenceActorRouter;

    private readonly IActorRefStruct<ConsistentHashActorStruct<KeyValueActor, KeyValueRequest, KeyValueResponse>, KeyValueRequest, KeyValueResponse> ephemeralKeyValuesRouter;
    
    private readonly IActorRefStruct<ConsistentHashActorStruct<KeyValueActor, KeyValueRequest, KeyValueResponse>, KeyValueRequest, KeyValueResponse> consistentKeyValuesRouter;
    
    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="actorSystem"></param>
    /// <param name="raft"></param>
    /// <param name="persistence"></param>
    /// <param name="configuration"></param>
    /// <param name="logger"></param>
    public KeyValuesManager(
        ActorSystem actorSystem, 
        IRaft raft, 
        IPersistence persistence, 
        IActorRef<ConsistentHashActor<PersistenceActor, PersistenceRequest, PersistenceResponse>, PersistenceRequest, PersistenceResponse> persistenceActorRouter,
        IActorRef<BackgroundWriterActor, BackgroundWriteRequest> backgroundWriter,
        KahunaConfiguration configuration, 
        ILogger<IKahuna> logger
    )
    {
        this.actorSystem = actorSystem;
        this.raft = raft;
        this.logger = logger;
        
        this.persistenceActorRouter = persistenceActorRouter;
        
        ephemeralKeyValuesRouter = GetEphemeralRouter(backgroundWriter, persistence, configuration);
        consistentKeyValuesRouter = GetConsistentRouter(backgroundWriter, persistence, configuration);

        txCoordinator = new(this, configuration, raft, logger);
        locator = new(this, configuration, raft, logger);
    }

    /// <summary>
    /// Creates the ephemeral keyValues router
    /// </summary>
    /// <param name="backgroundWriter"></param>
    /// <param name="persistence"></param>
    /// <param name="workers"></param>
    /// <returns></returns>
    private IActorRefStruct<ConsistentHashActorStruct<KeyValueActor, KeyValueRequest, KeyValueResponse>, KeyValueRequest, KeyValueResponse> GetEphemeralRouter(
        IActorRef<BackgroundWriterActor, BackgroundWriteRequest> backgroundWriter, 
        IPersistence persistence, 
        KahunaConfiguration configuration
    )
    {
        List<IActorRefStruct<KeyValueActor, KeyValueRequest, KeyValueResponse>> ephemeralInstances = new(configuration.KeyValuesWorkers);

        for (int i = 0; i < configuration.KeyValuesWorkers; i++)
            ephemeralInstances.Add(actorSystem.SpawnStruct<KeyValueActor, KeyValueRequest, KeyValueResponse>("ephemeral-keyvalue-" + i, backgroundWriter, persistence, logger));

        return actorSystem.CreateConsistentHashRouterStruct(ephemeralInstances);
    }

    /// <summary>
    /// Creates the consistent keyValues router
    /// </summary>
    /// <param name="backgroundWriter"></param>
    /// <param name="persistence"></param>
    /// <param name="workers"></param>
    /// <returns></returns>
    private IActorRefStruct<ConsistentHashActorStruct<KeyValueActor, KeyValueRequest, KeyValueResponse>, KeyValueRequest, KeyValueResponse> GetConsistentRouter(
        IActorRef<BackgroundWriterActor, BackgroundWriteRequest> backgroundWriter, 
        IPersistence persistence, 
        KahunaConfiguration configuration
    )
    {
        List<IActorRefStruct<KeyValueActor, KeyValueRequest, KeyValueResponse>> consistentInstances = new(configuration.KeyValuesWorkers);

        for (int i = 0; i < configuration.KeyValuesWorkers; i++)
            consistentInstances.Add(actorSystem.SpawnStruct<KeyValueActor, KeyValueRequest, KeyValueResponse>("consistent-keyvalue-" + i, backgroundWriter, persistence, logger));
        
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
            KeyValueMessage keyValueMessage = ReplicationSerializer.UnserializeKeyValueMessage(log.LogData);

            HLCTimestamp eventTime = new(keyValueMessage.TimeLogical, keyValueMessage.TimeCounter);

            await raft.HybridLogicalClock.ReceiveEvent(eventTime);

            switch ((KeyValueRequestType)keyValueMessage.Type)
            {
                case KeyValueRequestType.TrySet:
                {
                    PersistenceResponse? response = await persistenceActorRouter.Ask(new(
                        PersistenceRequestType.StoreKeyValue,
                        keyValueMessage.Key,
                        keyValueMessage.Value?.ToByteArray(),
                        keyValueMessage.Revision,
                        keyValueMessage.ExpireLogical,
                        keyValueMessage.ExpireCounter,
                        keyValueMessage.Consistency,
                        (int)KeyValueState.Set
                    ));
                    
                    if (response is null)
                        return false;

                    return response.Type == PersistenceResponseType.Success;
                }

                case KeyValueRequestType.TryDelete:
                {
                    PersistenceResponse? response = await persistenceActorRouter.Ask(new(
                        PersistenceRequestType.StoreKeyValue,
                        keyValueMessage.Key,
                        keyValueMessage.Value?.ToByteArray(),
                        keyValueMessage.Revision,
                        keyValueMessage.ExpireLogical,
                        keyValueMessage.ExpireCounter,
                        keyValueMessage.Consistency,
                        (int)KeyValueState.Deleted
                    ));
                    
                    if (response is null)
                        return false;

                    return response.Type == PersistenceResponseType.Success;
                }

                case KeyValueRequestType.TryExtend:
                {
                    PersistenceResponse? response = await persistenceActorRouter.Ask(new(
                        PersistenceRequestType.StoreKeyValue,
                        keyValueMessage.Key,
                        keyValueMessage.Value?.ToByteArray(),
                        keyValueMessage.Revision,
                        keyValueMessage.ExpireLogical,
                        keyValueMessage.ExpireCounter,
                        keyValueMessage.Consistency,
                        (int)KeyValueState.Set
                    ));

                    if (response is null)
                        return false;

                    return response.Type == PersistenceResponseType.Success;
                }

                case KeyValueRequestType.TryGet:
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
    /// <param name="consistency"></param>
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
        KeyValueConsistency consistency,
        CancellationToken cancellationToken
    )
    {
        return await locator.LocateAndTrySetKeyValue(transactionId, key, value, compareValue, compareRevision, flags, expiresMs, consistency, cancellationToken);
    }

    /// <summary>
    /// Locates the leader node for the given key and executes the TryGetValue request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="consistency"></param>
    /// <param name="cancelationToken"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, ReadOnlyKeyValueContext?)> LocateAndTryGetValue(HLCTimestamp transactionId, string key, KeyValueConsistency consistency, CancellationToken cancelationToken)
    {
        return await locator.LocateAndTryGetValue(transactionId, key, consistency, cancelationToken);
    }
    
    /// <summary>
    /// Locates the leader node for the given key and executes the TryDelete request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="consistency"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, long)> LocateAndTryDeleteKeyValue(HLCTimestamp transactionId, string key, KeyValueConsistency consistency, CancellationToken cancellationToken)
    {
        return await locator.LocateAndTryDeleteKeyValue(transactionId, key, consistency, cancellationToken);
    }
    
    /// <summary>
    /// Locates the leader node for the given key and executes the TryExtend request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="expiresMs"></param>
    /// <param name="consistency"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, long)> LocateAndTryExtendKeyValue(HLCTimestamp transactionId, string key, int expiresMs, KeyValueConsistency consistency, CancellationToken cancellationToken)
    {
        return await locator.LocateAndTryExtendKeyValue(transactionId, key, expiresMs, consistency, cancellationToken);
    }
    
    /// <summary>
    /// Locates the leader node for the given key and executes the TryAcquireExclusiveLock request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="expiresMs"></param>
    /// <param name="consistency"></param>
    /// <param name="cancelationToken"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, string)> LocateAndTryAcquireExclusiveLock(HLCTimestamp transactionId, string key, int expiresMs, KeyValueConsistency consistency, CancellationToken cancelationToken)
    {
        return await locator.LocateAndTryAcquireExclusiveLock(transactionId, key, expiresMs, consistency, cancelationToken);
    }
    
    /// <summary>
    /// Locates the leader node for the given key and executes the TryReleaseExclusiveLock request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="expiresMs"></param>
    /// <param name="consistency"></param>
    /// <param name="cancelationToken"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, string)> LocateAndTryReleaseExclusiveLock(HLCTimestamp transactionId, string key, KeyValueConsistency consistency, CancellationToken cancelationToken)
    {
        return await locator.LocateAndTryReleaseExclusiveLock(transactionId, key, consistency, cancelationToken);
    }
    
    /// <summary>
    /// Locates the leader node for the given key and executes the TryPrepareMutations request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="expiresMs"></param>
    /// <param name="consistency"></param>
    /// <param name="cancelationToken"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, HLCTimestamp, string)> LocateAndTryPrepareMutations(HLCTimestamp transactionId, string key, KeyValueConsistency consistency, CancellationToken cancelationToken)
    {
        return await locator.LocateAndTryPrepareMutations(transactionId, key, consistency, cancelationToken);
    }
    
    /// <summary>
    /// Locates the leader node for the given key and executes the TryCommitMutations request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="ticketId"></param>
    /// <param name="consistency"></param>
    /// <param name="cancelationToken"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, long)> LocateAndTryCommitMutations(HLCTimestamp transactionId, string key, HLCTimestamp ticketId, KeyValueConsistency consistency, CancellationToken cancelationToken)
    {
        return await locator.LocateAndTryCommitMutations(transactionId, key, ticketId, consistency, cancelationToken);
    }
    
    /// <summary>
    /// Locates the leader node for the given key and executes the TryRollbackMutations request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="ticketId"></param>
    /// <param name="consistency"></param>
    /// <param name="cancelationToken"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, long)> LocateAndTryRollbackMutations(HLCTimestamp transactionId, string key, HLCTimestamp ticketId, KeyValueConsistency consistency, CancellationToken cancelationToken)
    {
        return await locator.LocateAndTryRollbackMutations(transactionId, key, ticketId, consistency, cancelationToken);
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
    /// <param name="consistency"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, long)> TrySetKeyValue(
        HLCTimestamp transactionId,
        string key, 
        byte[]? value, 
        byte[]? compareValue,
        long compareRevision,
        KeyValueFlags flags,
        int expiresMs, 
        KeyValueConsistency consistency
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
            consistency
        );

        KeyValueResponse response;
        
        if (consistency == KeyValueConsistency.Ephemeral)
            response = await ephemeralKeyValuesRouter.Ask(request);
        else
            response = await consistentKeyValuesRouter.Ask(request);
        
        return (response.Type, response.Revision);
    }
    
    /// <summary>
    /// Set a timeout on key. After the timeout has expired, the key will automatically be deleted
    /// </summary>
    /// <param name="key"></param>
    /// <param name="expiresMs"></param>
    /// <param name="consistency"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, long)> TryExtendKeyValue(
        HLCTimestamp transactionId,
        string key, 
        int expiresMs, 
        KeyValueConsistency consistency
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
            consistency
        );

        KeyValueResponse response;
        
        if (consistency == KeyValueConsistency.Ephemeral)
            response = await ephemeralKeyValuesRouter.Ask(request);
        else
            response = await consistentKeyValuesRouter.Ask(request);
        
        return (response.Type, response.Revision);
    }

    /// <summary>
    /// Removes the specified key. A key is ignored if it does not exist.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="consistency"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, long)> TryDeleteKeyValue(HLCTimestamp transactionId, string key, KeyValueConsistency consistency)
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
            consistency
        );

        KeyValueResponse response;
        
        if (consistency == KeyValueConsistency.Ephemeral)
            response = await ephemeralKeyValuesRouter.Ask(request);
        else
            response = await consistentKeyValuesRouter.Ask(request);
        
        return (response.Type, response.Revision);
    }
    
    /// <summary>
    /// Passes a Get request to the key/value actor for the given keyValue name.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="consistency"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, ReadOnlyKeyValueContext?)> TryGetValue(HLCTimestamp transactionId, string key, KeyValueConsistency consistency)
    {
        KeyValueRequest request = new(
            KeyValueRequestType.TryGet, 
            transactionId, 
            key, 
            null, 
            null,
            -1,
            KeyValueFlags.None,
            0, 
            HLCTimestamp.Zero,
            consistency
        );

        KeyValueResponse response;
        
        if (consistency == KeyValueConsistency.Ephemeral)
            response = await ephemeralKeyValuesRouter.Ask(request);
        else
            response = await consistentKeyValuesRouter.Ask(request);
        
        return (response.Type, response.Context);
    }
    
    /// <summary>
    /// Passes a TryAcquireExclusiveLock request to the key/value actor for the given keyValue name.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="expiresMs"></param>
    /// <param name="consistency"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, string)> TryAcquireExclusiveLock(HLCTimestamp transactionId, string key, int expiresMs, KeyValueConsistency consistency)
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
            consistency
        );

        KeyValueResponse response;
        
        if (consistency == KeyValueConsistency.Ephemeral)
            response = await ephemeralKeyValuesRouter.Ask(request);
        else
            response = await consistentKeyValuesRouter.Ask(request);
        
        return (response.Type, key);
    }
    
    /// <summary>
    /// Passes a TryAcquireExclusiveLock request to the key/value actor for the given keyValue name.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="consistency"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, string)> TryReleaseExclusiveLock(HLCTimestamp transactionId, string key, KeyValueConsistency consistency)
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
            consistency
        );

        KeyValueResponse response;
        
        if (consistency == KeyValueConsistency.Ephemeral)
            response = await ephemeralKeyValuesRouter.Ask(request);
        else
            response = await consistentKeyValuesRouter.Ask(request);
        
        return (response.Type, key);
    }
    
    /// <summary>
    /// Passes a TryPrepare request to the key/value actor for the given keyValue name.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="consistency"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, HLCTimestamp, string)> TryPrepareMutations(HLCTimestamp transactionId, string key, KeyValueConsistency consistency)
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
            consistency
        );

        KeyValueResponse response;
        
        if (consistency == KeyValueConsistency.Ephemeral)
            response = await ephemeralKeyValuesRouter.Ask(request);
        else
            response = await consistentKeyValuesRouter.Ask(request);
        
        return (response.Type, response.Ticket, key);
    }
    
    /// <summary>
    /// Passes a TryCommit request to the key/value actor for the given keyValue name.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="proposalTicketId"></param>
    /// <param name="consistency"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, long)> TryCommitMutations(HLCTimestamp transactionId, string key, HLCTimestamp proposalTicketId, KeyValueConsistency consistency)
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
            consistency
        );

        KeyValueResponse response;
        
        if (consistency == KeyValueConsistency.Ephemeral)
            response = await ephemeralKeyValuesRouter.Ask(request);
        else
            response = await consistentKeyValuesRouter.Ask(request);
        
        return (response.Type, response.Revision);
    }
    
    /// <summary>
    /// Passes a TryRollback request to the key/value actor for the given keyValue name.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="proposalTicketId"></param>
    /// <param name="consistency"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, long)> TryRollbackMutations(HLCTimestamp transactionId, string key, HLCTimestamp proposalTicketId, KeyValueConsistency consistency)
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
            consistency
        );

        KeyValueResponse response;
        
        if (consistency == KeyValueConsistency.Ephemeral)
            response = await ephemeralKeyValuesRouter.Ask(request);
        else
            response = await consistentKeyValuesRouter.Ask(request);
        
        return (response.Type, response.Revision);
    }

    /// <summary>
    /// Schedule a key/value transaction to be executed
    /// </summary>
    /// <param name="script"></param>
    /// <param name="hash"></param>
    /// <returns></returns>
    public async Task<KeyValueTransactionResult> TryExecuteTx(byte[] script, string? hash)
    {
        return await txCoordinator.TryExecuteTx(script, hash);
    }
}