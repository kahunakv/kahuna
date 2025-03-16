
using Nixie;
using Nixie.Routers;

using Kommander;
using Kommander.Data;
using Kommander.Time;

using Kahuna.Configuration;
using Kahuna.Persistence;
using Kahuna.Replication;
using Kahuna.Replication.Protos;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;

namespace Kahuna.KeyValues;

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
        return await locator.LocateAndTrySetKeyValue(key, value, compareValue, compareRevision, flags, expiresMs, consistency, cancellationToken);
    }

    /// <summary>
    /// Locates the leader node for the given key and executes the TryGetValue request.
    /// </summary>
    /// <param name="key"></param>
    /// <param name="consistency"></param>
    /// <param name="cancelationToken"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, ReadOnlyKeyValueContext?)> LocateAndTryGetValue(string key, KeyValueConsistency consistency, CancellationToken cancelationToken)
    {
        return await locator.LocateAndTryGetValue(key, consistency, cancelationToken);
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
            key, 
            value, 
            compareValue,
            compareRevision,
            flags,
            expiresMs, 
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
        string key, 
        int expiresMs, 
        KeyValueConsistency consistency
    )
    {
        KeyValueRequest request = new(
            KeyValueRequestType.TryExtend, 
            key, 
            null, 
            null,
            -1,
            KeyValueFlags.None,
            expiresMs, 
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
    /// Removes the specified keys. A key is ignored if it does not exist.
    /// </summary>
    /// <param name="key"></param>
    /// <param name="consistency"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, long)> TryDeleteKeyValue(string key, KeyValueConsistency consistency)
    {
        KeyValueRequest request = new(
            KeyValueRequestType.TryDelete, 
            key, 
            null, 
            null,
            -1,
            KeyValueFlags.None,
            0, 
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
    /// Passes a Get request to the keyValueer actor for the given keyValue name.
    /// </summary>
    /// <param name="key"></param>
    /// <param name="consistency"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, ReadOnlyKeyValueContext?)> TryGetValue(string key, KeyValueConsistency consistency)
    {
        KeyValueRequest request = new(
            KeyValueRequestType.TryGet, 
            key, 
            null, 
            null,
            -1,
            KeyValueFlags.None,
            0, 
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
    /// Schedule a key/value transaction to be executed
    /// </summary>
    /// <param name="script"></param>
    /// <returns></returns>
    public async Task<KeyValueTransactionResult> TryExecuteTx(string script)
    {
        return await txCoordinator.TryExecuteTx(script);
    }
}