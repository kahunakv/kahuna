
using Kahuna.Configuration;
using Kahuna.Persistence;
using Kahuna.Replication;
using Kahuna.Replication.Protos;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Data;
using Kommander.Time;
using Nixie;
using Nixie.Routers;

namespace Kahuna.KeyValues;

public class KeyValuesManager
{
    private readonly ActorSystem actorSystem;

    private readonly IRaft raft;

    private readonly ILogger<IKahuna> logger;
    
    private readonly IActorRef<ConsistentHashActor<PersistenceActor, PersistenceRequest, PersistenceResponse>, PersistenceRequest, PersistenceResponse> persistenceActorRouter;

    private readonly IActorRefStruct<ConsistentHashActorStruct<KeyValueActor, KeyValueRequest, KeyValueResponse>, KeyValueRequest, KeyValueResponse> ephemeralKeyValuesRouter;
    
    private readonly IActorRefStruct<ConsistentHashActorStruct<KeyValueActor, KeyValueRequest, KeyValueResponse>, KeyValueRequest, KeyValueResponse> consistentKeyValuesRouter;
    
    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="actorSystem"></param>
    /// <param name="raft"></param>
    /// <param name="configuration"></param>
    /// <param name="logger"></param>
    public KeyValuesManager(ActorSystem actorSystem, IRaft raft, KahunaConfiguration configuration, ILogger<IKahuna> logger)
    {
        this.actorSystem = actorSystem;
        this.raft = raft;
        this.logger = logger;

        IPersistence persistence = GetPersistence(configuration);
        
        persistenceActorRouter = GetPersistenceRouter(persistence, configuration);
        
        IActorRef<KeyValueBackgroundWriterActor, KeyValueBackgroundWriteRequest> backgroundWriter = actorSystem.Spawn<KeyValueBackgroundWriterActor, KeyValueBackgroundWriteRequest>(
            "keyvalues-background-writer", 
            raft, 
            persistenceActorRouter, 
            logger
        );
        
        ephemeralKeyValuesRouter = GetEphemeralRouter(backgroundWriter, persistence, configuration);
        consistentKeyValuesRouter = GetConsistentRouter(backgroundWriter, persistence, configuration);
    }

    /// <summary>
    /// Creates the persistence instance
    /// </summary>
    /// <param name="configuration"></param>
    /// <returns></returns>
    /// <exception cref="KahunaServerException"></exception>
    private static IPersistence GetPersistence(KahunaConfiguration configuration)
    {
        return new SqlitePersistence(configuration.StoragePath, configuration.StorageRevision);

        /*return configuration.Storage switch
        {
            "rocksdb" => new RocksDbPersistence(configuration.StoragePath, configuration.StorageRevision),
            "sqlite" => new SqlitePersistence(configuration.StoragePath, configuration.StorageRevision),
            _ => throw new KahunaServerException("Invalid storage type")
        };*/
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
            persistenceInstances.Add(actorSystem.Spawn<PersistenceActor, PersistenceRequest, PersistenceResponse>("keyValues-persistence-" + i, persistence, logger));

        return actorSystem.CreateConsistentHashRouter(persistenceInstances);
    }

    /// <summary>
    /// Creates the ephemeral keyValues router
    /// </summary>
    /// <param name="backgroundWriter"></param>
    /// <param name="persistence"></param>
    /// <param name="workers"></param>
    /// <returns></returns>
    private IActorRefStruct<ConsistentHashActorStruct<KeyValueActor, KeyValueRequest, KeyValueResponse>, KeyValueRequest, KeyValueResponse> GetEphemeralRouter(
        IActorRef<KeyValueBackgroundWriterActor, KeyValueBackgroundWriteRequest> backgroundWriter, 
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
        IActorRef<KeyValueBackgroundWriterActor, KeyValueBackgroundWriteRequest> backgroundWriter, 
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
                        PersistenceRequestType.Store,
                        keyValueMessage.Key,
                        keyValueMessage.Value,
                        keyValueMessage.ExpireLogical,
                        keyValueMessage.ExpireCounter,
                        (KeyValueConsistency)keyValueMessage.Consistency,
                        KeyValueState.Set
                    ));
                    
                    if (response is null)
                        return false;

                    return response.Type == PersistenceResponseType.Success;*/
                    return false;
                }

                case KeyValueRequestType.TryDelete:
                {
                    /*PersistenceResponse? response = await persistenceActorRouter.Ask(new(
                        PersistenceRequestType.Store,
                        keyValueMessage.Resource,
                        keyValueMessage.Owner,
                        keyValueMessage.FencingToken,
                        keyValueMessage.ExpireLogical,
                        keyValueMessage.ExpireCounter,
                        (KeyValueConsistency)keyValueMessage.Consistency,
                        KeyValueState.UnkeyValueed
                    ));
                    
                    if (response is null)
                        return false;

                    return response.Type == PersistenceResponseType.Success;*/
                    return false;
                }

                case KeyValueRequestType.TryExtend:
                {
                    /*PersistenceResponse? response = await persistenceActorRouter.Ask(new(
                        PersistenceRequestType.Store,
                        keyValueMessage.Resource,
                        keyValueMessage.Owner,
                        keyValueMessage.FencingToken,
                        keyValueMessage.ExpireLogical,
                        keyValueMessage.ExpireCounter,
                        (KeyValueConsistency)keyValueMessage.Consistency,
                        KeyValueState.KeyValueed
                    ));

                    if (response is null)
                        return false;

                    return response.Type == PersistenceResponseType.Success;*/
                    return false;
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
    /// Passes a TrySet request to the keyValueer actor for the given keyValue name.
    /// </summary>
    /// <param name="key"></param>
    /// <param name="value"></param>
    /// <param name="expiresMs"></param>
    /// <param name="consistency"></param>
    /// <returns></returns>
    public async Task<KeyValueResponseType> TrySetKeyValue(string key, string? value, int expiresMs, KeyValueConsistency consistency)
    {
        KeyValueRequest request = new(
            KeyValueRequestType.TrySet, 
            key, 
            value, 
            expiresMs, 
            consistency
        );

        KeyValueResponse response;
        
        if (consistency == KeyValueConsistency.Ephemeral)
            response = await ephemeralKeyValuesRouter.Ask(request);
        else
            response = await consistentKeyValuesRouter.Ask(request);
        
        return response.Type;
    }
    
    /// <summary>
    /// Passes a TryExtendKeyValue request to the keyValueer actor for the given keyValue name.
    /// </summary>
    /// <param name="key"></param>
    /// <param name="expiresMs"></param>
    /// <param name="consistency"></param>
    /// <returns></returns>
    public async Task<KeyValueResponseType> TryExtendKeyValue(string key, int expiresMs, KeyValueConsistency consistency)
    {
        KeyValueRequest request = new(
            KeyValueRequestType.TryExtend, 
            key, 
            null, 
            expiresMs, 
            consistency
        );

        KeyValueResponse response;
        
        if (consistency == KeyValueConsistency.Ephemeral)
            response = await ephemeralKeyValuesRouter.Ask(request);
        else
            response = await consistentKeyValuesRouter.Ask(request);
        
        return response.Type;
    }

    /// <summary>
    /// Passes a TryDeleteKeyValue request to the keyValueer actor for the given keyValue name.
    /// </summary>
    /// <param name="key"></param>
    /// <param name="consistency"></param>
    /// <returns></returns>
    public async Task<KeyValueResponseType> TryDeleteKeyValue(string key, KeyValueConsistency consistency)
    {
        KeyValueRequest request = new(
            KeyValueRequestType.TryDelete, 
            key, 
            null, 
            0, 
            consistency
        );

        KeyValueResponse response;
        
        if (consistency == KeyValueConsistency.Ephemeral)
            response = await ephemeralKeyValuesRouter.Ask(request);
        else
            response = await consistentKeyValuesRouter.Ask(request);
        
        return response.Type;
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
}