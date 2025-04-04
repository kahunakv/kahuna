
using Nixie;
using Nixie.Routers;

using Kommander;
using Kommander.Data;
using Kommander.Time;

using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Persistence;
using Kahuna.Server.Replication;
using Kahuna.Server.Replication.Protos;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues;

internal sealed class KeyValuesManager
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

    private readonly KeyValueRestorer restorer;

    private readonly KeyValueReplicator replicator;
    
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

        restorer = new(backgroundWriter, raft, logger);
        replicator = new(backgroundWriter, raft, logger);
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
        logger.LogDebug("Starting {Workers} persistent key/value workers", configuration.KeyValuesWorkers);

        for (int i = 0; i < configuration.KeyValuesWorkers; i++)
            persistentInstances.Add(actorSystem.Spawn<KeyValueActor, KeyValueRequest, KeyValueResponse>("persistent-keyvalue-" + i, backgroundWriter, persistence, logger));
        
        return actorSystem.CreateConsistentHashRouter(persistentInstances);
    }
    
    /// <summary>
    /// Receives replication messages once they're committed to the Raft log.
    /// </summary>
    /// <param name="log"></param>
    /// <returns></returns>
    public Task<bool> OnLogRestored(RaftLog log)
    {
        return Task.FromResult(log.LogType != ReplicationTypes.KeyValues || restorer.Restore(log));
    }

    /// <summary>
    /// Receives replication messages once they're committed to the Raft log.
    /// </summary>
    /// <param name="log"></param>
    /// <returns></returns>
    public Task<bool> OnReplicationReceived(RaftLog log)
    {
        return Task.FromResult(log.LogType != ReplicationTypes.KeyValues || replicator.Replicate(log));
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
    public Task<(KeyValueResponseType, long)> LocateAndTrySetKeyValue(
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
        return locator.LocateAndTrySetKeyValue(transactionId, key, value, compareValue, compareRevision, flags, expiresMs, durability, cancellationToken);
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
    public Task<(KeyValueResponseType, ReadOnlyKeyValueContext?)> LocateAndTryGetValue(
        HLCTimestamp transactionId, 
        string key,
        long revision, 
        KeyValueDurability durability, 
        CancellationToken cancellationToken
    )
    {
        return locator.LocateAndTryGetValue(transactionId, key, revision, durability, cancellationToken);
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
    public Task<(KeyValueResponseType, ReadOnlyKeyValueContext?)> LocateAndTryExistsValue(
        HLCTimestamp transactionId, 
        string key,
        long revision, 
        KeyValueDurability durability, 
        CancellationToken cancellationToken
    )
    {
        return locator.LocateAndTryExistsValue(transactionId, key, revision, durability, cancellationToken);
    }
    
    /// <summary>
    /// Locates the leader node for the given key and executes the TryDelete request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public Task<(KeyValueResponseType, long)> LocateAndTryDeleteKeyValue(HLCTimestamp transactionId, string key, KeyValueDurability durability, CancellationToken cancellationToken)
    {
        return locator.LocateAndTryDeleteKeyValue(transactionId, key, durability, cancellationToken);
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
    public Task<(KeyValueResponseType, long)> LocateAndTryExtendKeyValue(HLCTimestamp transactionId, string key, int expiresMs, KeyValueDurability durability, CancellationToken cancellationToken)
    {
        return locator.LocateAndTryExtendKeyValue(transactionId, key, expiresMs, durability, cancellationToken);
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
    public Task<(KeyValueResponseType, string, KeyValueDurability)> LocateAndTryAcquireExclusiveLock(HLCTimestamp transactionId, string key, int expiresMs, KeyValueDurability durability, CancellationToken cancelationToken)
    {
        return locator.LocateAndTryAcquireExclusiveLock(transactionId, key, expiresMs, durability, cancelationToken);
    }
    
    /// <summary>
    /// Locates the leader node for the given keys and executes the TryAcquireManyExclusiveLocks request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="keys"></param>
    /// <param name="cancelationToken"></param>
    /// <returns></returns>
    public Task<List<(KeyValueResponseType, string, KeyValueDurability)>> LocateAndTryAcquireManyExclusiveLocks(HLCTimestamp transactionId, List<(string key, int expiresMs, KeyValueDurability durability)> keys, CancellationToken cancelationToken)
    {
        return locator.LocateAndTryAcquireManyExclusiveLocks(transactionId, keys, cancelationToken);
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
    public Task<(KeyValueResponseType, string)> LocateAndTryReleaseExclusiveLock(HLCTimestamp transactionId, string key, KeyValueDurability durability, CancellationToken cancelationToken)
    {
        return locator.LocateAndTryReleaseExclusiveLock(transactionId, key, durability, cancelationToken);
    }
    
    /// <summary>
    /// Locates the leader node for the given keys and executes the TryReleaseManyExclusiveLocks requests
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="keys"></param>
    /// <param name="cancelationToken"></param>
    /// <returns></returns>
    public Task<List<(KeyValueResponseType, string, KeyValueDurability)>> LocateAndTryReleaseManyExclusiveLocks(HLCTimestamp transactionId, List<(string key, KeyValueDurability durability)> keys, CancellationToken cancelationToken)
    {
        return locator.LocateAndTryReleaseManyExclusiveLocks(transactionId, keys, cancelationToken);
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
    public Task<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)> LocateAndTryPrepareMutations(HLCTimestamp transactionId, string key, KeyValueDurability durability, CancellationToken cancelationToken)
    {
        return locator.LocateAndTryPrepareMutations(transactionId, key, durability, cancelationToken);
    }
    
    /// <summary>
    /// Locates the leader node for the given keys and executes many TryPrepareMutations requests.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="keys"></param>
    /// <param name="cancelationToken"></param>
    /// <returns></returns>
    public Task<List<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)>> LocateAndTryPrepareManyMutations(HLCTimestamp transactionId, List<(string key, KeyValueDurability durability)> keys, CancellationToken cancelationToken)
    {
        return locator.LocateAndTryPrepareManyMutations(transactionId, keys, cancelationToken);
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
    public Task<(KeyValueResponseType, long)> LocateAndTryCommitMutations(HLCTimestamp transactionId, string key, HLCTimestamp ticketId, KeyValueDurability durability, CancellationToken cancelationToken)
    {
        return locator.LocateAndTryCommitMutations(transactionId, key, ticketId, durability, cancelationToken);
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
    public Task<(KeyValueResponseType, long)> LocateAndTryRollbackMutations(HLCTimestamp transactionId, string key, HLCTimestamp ticketId, KeyValueDurability durability, CancellationToken cancelationToken)
    {
        return locator.LocateAndTryRollbackMutations(transactionId, key, ticketId, durability, cancelationToken);
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
    /// Passes a TryAcquireExclusiveLock request to the key/value actor for the given keys.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="keys"></param>
    /// <returns></returns>
    public async Task<List<(KeyValueResponseType, string, KeyValueDurability)>> TryAcquireManyExclusiveLocks(HLCTimestamp transactionId, List<(string key, int expiresMs, KeyValueDurability durability)> keys)
    {
        List<(KeyValueResponseType, string, KeyValueDurability)> responses = new(keys.Count);
        
        foreach ((string key, int expiresMs, KeyValueDurability durability) key in keys)
        {
            KeyValueRequest request = new(
                KeyValueRequestType.TryAcquireExclusiveLock,
                transactionId,
                key.key,
                null,
                null,
                -1,
                KeyValueFlags.None,
                key.expiresMs,
                HLCTimestamp.Zero,
                key.durability
            );

            KeyValueResponse? response;

            if (key.durability == KeyValueDurability.Ephemeral)
                response = await ephemeralKeyValuesRouter.Ask(request);
            else
                response = await persistentKeyValuesRouter.Ask(request);

            if (response is null)
            {
                responses.Add((KeyValueResponseType.Errored, key.key, key.durability));
                continue;
            }

            responses.Add((response.Type, key.key, key.durability));

            if (response.Type != KeyValueResponseType.Locked)
                break;
        }

        return responses;
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
    /// Passes a TryAcquireExclusiveLock request to the key/value actor for the given keys.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="keys"></param>
    /// <returns></returns>
    public async Task<List<(KeyValueResponseType, string, KeyValueDurability)>> TryReleaseManyExclusiveLocks(HLCTimestamp transactionId, List<(string key, KeyValueDurability durability)> keys)
    {
        List<(KeyValueResponseType, string, KeyValueDurability)> responses = new(keys.Count);
        
        foreach ((string key, KeyValueDurability durability) key in keys)
        {
            KeyValueRequest request = new(
                KeyValueRequestType.TryReleaseExclusiveLock,
                transactionId,
                key.key,
                null,
                null,
                -1,
                KeyValueFlags.None,
                0,
                HLCTimestamp.Zero,
                key.durability
            );

            KeyValueResponse? response;

            if (key.durability == KeyValueDurability.Ephemeral)
                response = await ephemeralKeyValuesRouter.Ask(request);
            else
                response = await persistentKeyValuesRouter.Ask(request);

            if (response is null)
            {
                responses.Add((KeyValueResponseType.Errored, key.key, key.durability));
                continue;
            }

            responses.Add((response.Type, key.key, key.durability));
        }

        return responses;
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
    /// Passes a TryPrepare request to the key/value actor for the given keys.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public async Task<List<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)>> TryPrepareManyMutations(HLCTimestamp transactionId, List<(string key, KeyValueDurability durability)> keys)
    {
        List<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)> responses = new(keys.Count);
        
        foreach ((string key, KeyValueDurability durability) key in keys)
        {
            KeyValueRequest request = new(
                KeyValueRequestType.TryPrepareMutations,
                transactionId,
                key.key,
                null,
                null,
                -1,
                KeyValueFlags.None,
                0,
                HLCTimestamp.Zero,
                key.durability
            );

            KeyValueResponse? response;

            if (key.durability == KeyValueDurability.Ephemeral)
                response = await ephemeralKeyValuesRouter.Ask(request);
            else
                response = await persistentKeyValuesRouter.Ask(request);

            if (response is null)
                return [(KeyValueResponseType.Errored, HLCTimestamp.Zero, key.key, key.durability)];

            responses.Add((response.Type, response.Ticket, key.key, key.durability));

            if (response.Type != KeyValueResponseType.Prepared) // Early abort if a key wasn't successfully prepared for commits
                return responses;
        }

        return responses;
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
    public Task<KeyValueTransactionResult> TryExecuteTx(byte[] script, string? hash, List<KeyValueParameter>? parameters)
    {
        return txCoordinator.TryExecuteTx(script, hash, parameters);
    }

    /// <summary>
    /// Scans all nodes in the cluster and returns key/value pairs by prefix 
    /// </summary>
    /// <param name="prefixKeyName"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public Task<KeyValueGetByPrefixResult> ScanAllByPrefix(string prefixKeyName, KeyValueDurability durability)
    {
        return locator.ScanAllByPrefix(prefixKeyName, durability);
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
        HLCTimestamp currentTime = raft.HybridLogicalClock.TrySendOrLocalEvent();
        
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

    /// <summary>
    /// Scans the current node and returns key/value pairs by prefix 
    /// </summary>
    /// <param name="prefixKeyName"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    /// <exception cref="KahunaServerException"></exception>
    public async Task<KeyValueGetByPrefixResult> GetByPrefix(string prefixKeyName, KeyValueDurability durability)
    {
        HLCTimestamp currentTime = raft.HybridLogicalClock.TrySendOrLocalEvent();

        KeyValueRequest request = new(
            KeyValueRequestType.GetByPrefix,
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
        
        KeyValueResponse? response;
        
        if (durability == KeyValueDurability.Ephemeral)
            response = await ephemeralKeyValuesRouter.Ask(request);
        else
            response = await persistentKeyValuesRouter.Ask(request);

        if (response is null)
            return new([]);
        
        if (response is { Type: KeyValueResponseType.Get, Items: not null })
            return new(response.Items); 
        
        return new([]);
    }
}