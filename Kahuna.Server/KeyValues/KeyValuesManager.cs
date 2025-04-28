
using Kahuna.Server.Communication.Internode;
using Nixie;
using Nixie.Routers;

using Kommander;
using Kommander.Data;
using Kommander.Time;
using Kommander.Support.Parallelization;

using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Server.Replication;
using Kahuna.Shared.Communication.Rest;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues;

internal sealed class KeyValuesManager
{
    private readonly ActorSystem actorSystem;

    private readonly IRaft raft;

    private readonly IPersistenceBackend persistenceBackend;

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
    /// <param name="persistenceBackend"></param>
    /// <param name="backgroundWriter"></param>
    /// <param name="configuration"></param>
    /// <param name="logger"></param>
    public KeyValuesManager(
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

        this.persistenceBackend = persistenceBackend;
        
        ephemeralKeyValuesRouter = GetEphemeralRouter(configuration);
        persistentKeyValuesRouter = GetConsistentRouter(configuration);

        txCoordinator = new(this, configuration, raft, logger);
        locator = new(this, configuration, raft, interNodeCommunication, logger);

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
        logger.LogDebug("Starting {Workers} ephemeral key/value workers", configuration.KeyValueWorkers);

        for (int i = 0; i < configuration.KeyValueWorkers; i++)
            ephemeralInstances.Add(actorSystem.Spawn<KeyValueActor, KeyValueRequest, KeyValueResponse>("ephemeral-keyvalue-" + i, backgroundWriter, persistenceBackend, raft, logger));

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
        logger.LogDebug("Starting {Workers} persistent key/value workers", configuration.KeyValueWorkers);

        for (int i = 0; i < configuration.KeyValueWorkers; i++)
            persistentInstances.Add(actorSystem.Spawn<KeyValueActor, KeyValueRequest, KeyValueResponse>("persistent-keyvalue-" + i, backgroundWriter, persistenceBackend, raft, logger));
        
        return actorSystem.CreateConsistentHashRouter(persistentInstances);
    }
    
    /// <summary>
    /// Receives restore messages that haven't been checkpointed yet.
    /// </summary>
    /// <param name="partitionId"></param>
    /// <param name="log"></param>
    /// <returns></returns>
    public Task<bool> OnLogRestored(int partitionId, RaftLog log)
    {
        return Task.FromResult(log.LogType != ReplicationTypes.KeyValues || restorer.Restore(partitionId, log));
    }

    /// <summary>
    /// Receives replication messages once they're committed to the Raft log.
    /// </summary>
    /// <param name="partitionId"></param>
    /// <param name="log"></param>
    /// <returns></returns>
    public Task<bool> OnReplicationReceived(int partitionId, RaftLog log)
    {
        return Task.FromResult(log.LogType != ReplicationTypes.KeyValues || replicator.Replicate(partitionId, log));
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
    public Task<(KeyValueResponseType, long, HLCTimestamp)> LocateAndTrySetKeyValue(
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
    /// Attempts to locate and set multiple key-value pairs in the system.
    /// </summary>
    /// <param name="setManyItems">A collection of key-value set requests to be processed.</param>
    /// <param name="cancellationToken">Token to signal cancellation of the operation.</param>
    /// <returns>A task that represents the asynchronous operation, containing a list of responses for the key-value set requests.</returns>
    public Task<List<KahunaSetKeyValueResponse>> LocateAndTrySetManyKeyValue(IEnumerable<KahunaSetKeyValueRequest> setManyItems, CancellationToken cancellationToken)
    {
        return locator.LocateAndTrySetManyKeyValue(setManyItems, cancellationToken);
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
    public Task<(KeyValueResponseType, long, HLCTimestamp)> LocateAndTryDeleteKeyValue(HLCTimestamp transactionId, string key, KeyValueDurability durability, CancellationToken cancellationToken)
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
    public Task<(KeyValueResponseType, long, HLCTimestamp)> LocateAndTryExtendKeyValue(HLCTimestamp transactionId, string key, int expiresMs, KeyValueDurability durability, CancellationToken cancellationToken)
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
    /// <param name="commitId"></param>
    /// <param name="key"></param>
    /// <param name="durability"></param>
    /// <param name="cancelationToken"></param>
    /// <returns></returns>
    public Task<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)> LocateAndTryPrepareMutations(
        HLCTimestamp transactionId, 
        HLCTimestamp commitId, 
        string key, 
        KeyValueDurability durability, 
        CancellationToken cancelationToken
    )
    {
        return locator.LocateAndTryPrepareMutations(transactionId, commitId, key, durability, cancelationToken);
    }
    
    /// <summary>
    /// Locates the leader node for the given keys and executes many TryPrepareMutations requests.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="commitId"></param> 
    /// <param name="keys"></param>
    /// <param name="cancelationToken"></param>
    /// <returns></returns>
    public Task<List<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)>> LocateAndTryPrepareManyMutations(
        HLCTimestamp transactionId,
        HLCTimestamp commitId,
        List<(string key, KeyValueDurability durability)> keys, 
        CancellationToken cancelationToken
    )
    {
        return locator.LocateAndTryPrepareManyMutations(transactionId, commitId, keys, cancelationToken);
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
    /// Locates the leader node for the given keys and executes the TryCommitMutations request. 
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="keys"></param>
    /// <param name="cancelationToken"></param>
    /// <returns></returns>
    public Task<List<(KeyValueResponseType, string, long, KeyValueDurability)>> LocateAndTryCommitManyMutations(HLCTimestamp transactionId, List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)> keys, CancellationToken cancelationToken)
    {
        return locator.LocateAndTryCommitManyMutations(transactionId, keys, cancelationToken);
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
    /// Locates the leader node for the given keys and executes the TryRollbackMutations request. 
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="keys"></param>
    /// <param name="cancelationToken"></param>
    /// <returns></returns>
    public Task<List<(KeyValueResponseType, string, long, KeyValueDurability)>> LocateAndTryRollbackManyMutations(
        HLCTimestamp transactionId, 
        List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)> keys, 
        CancellationToken cancellationToken
    )
    {
        return locator.LocateAndTryRollbackManyMutations(transactionId, keys, cancellationToken);
    }

    /// <summary>
    /// Locates the leader node for the given prefix and executes the GetByPrefix request.
    /// </summary>
    /// <param name="prefixedKey"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public Task<KeyValueGetByPrefixResult> LocateAndGetByPrefix(HLCTimestamp transactionId, string prefixedKey, KeyValueDurability durability, CancellationToken cancellationToken)
    {
        return locator.LocateAndGetByPrefix(transactionId, prefixedKey, durability, cancellationToken);
    }

    /// <summary>
    /// Locates the appropriate key-value partition and starts a transaction.
    /// </summary>
    /// <param name="options">The options for the key-value transaction.</param>
    /// <param name="cancellationToken">The cancellation token for the operation.</param>
    /// <returns>A task representing the asynchronous operation, containing the result of the transaction initiation
    /// as a tuple consisting of the response type and the associated HLC timestamp.</returns>
    public Task<(KeyValueResponseType, HLCTimestamp)> LocateAndStartTransaction(KeyValueTransactionOptions options, CancellationToken cancellationToken)
    {
        return locator.LocateAndStartTransaction(options, cancellationToken);        
    }

    /// <summary>
    /// Attempts to locate and commit a transaction with the specified unique identifier, timestamp,
    /// acquired locks, and modified keys.
    /// </summary>
    /// <param name="uniqueId">The unique identifier of the transaction.</param>
    /// <param name="timestamp">The timestamp associated with the transaction.</param>
    /// <param name="acquiredLocks">A list of keys that have been locked during the transaction.</param>
    /// <param name="modifiedKeys">A list of keys that were modified as part of the transaction.</param>
    /// <param name="cancellationToken">A token used to monitor for cancellation requests.</param>
    /// <returns>A task that represents the asynchronous operation, containing the result of the transaction operation.</returns>
    public Task<KeyValueResponseType> LocateAndCommitTransaction(
        string uniqueId,
        HLCTimestamp timestamp,
        List<KeyValueTransactionModifiedKey> acquiredLocks,
        List<KeyValueTransactionModifiedKey> modifiedKeys,
        CancellationToken cancellationToken
    )
    {
        return locator.LocateAndCommitTransaction(uniqueId, timestamp, acquiredLocks, modifiedKeys, cancellationToken);
    }

    /// <summary>
    /// Locates and rolls back a transaction identified by a unique ID and timestamp.
    /// This operation resolves any locks and reverts modifications associated with the transaction.
    /// </summary>
    /// <param name="uniqueId">The unique identifier of the transaction to be rolled back.</param>
    /// <param name="timestamp">The timestamp associated with the transaction.</param>
    /// <param name="acquiredLocks">A list of keys that were locked during the transaction.</param>
    /// <param name="modifiedKeys">A list of keys that were modified during the transaction.</param>
    /// <param name="cancellationToken">A cancellation token to observe while waiting for the task to complete.</param>
    /// <returns>A task that represents the asynchronous operation, containing the result of the rollback operation as a <see cref="KeyValueResponseType"/>.</returns>
    public Task<KeyValueResponseType> LocateAndRollbackTransaction(
        string uniqueId,
        HLCTimestamp timestamp,
        List<KeyValueTransactionModifiedKey> acquiredLocks,
        List<KeyValueTransactionModifiedKey> modifiedKeys,
        CancellationToken cancellationToken
    )
    {
        return locator.LocateAndRollbackTransaction(uniqueId, timestamp, acquiredLocks, modifiedKeys, cancellationToken);
    }

    /// <summary>
    /// Passes a TrySet request to the key/value actor for the given key/value key.
    /// </summary>
    /// <param name="key"></param>
    /// <param name="value"></param>
    /// <param name="compareValue"></param>
    /// <param name="compareRevision"></param>
    /// <param name="flags"></param>
    /// <param name="expiresMs"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, long, HLCTimestamp)> TrySetKeyValue(
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
            HLCTimestamp.Zero,
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
            return (KeyValueResponseType.Errored, -1, HLCTimestamp.Zero);
        
        return (response.Type, response.Revision, response.Ticket);
    }

    /// <summary>
    /// Attempts to set multiple key-value pairs on the node.
    /// </summary>
    /// <param name="items">A list of key-value set requests to be processed.</param>
    /// <returns>A task that represents the asynchronous operation. The task result contains a list of responses for each set request, indicating the outcome of the operation.</returns>
    public async Task<List<KahunaSetKeyValueResponse>> SetManyNodeKeyValue(List<KahunaSetKeyValueRequest> items)
    {
        //throw new NotImplementedException();
        
        Lock sync = new();
        List<KahunaSetKeyValueResponse> responses = new(items.Count);

        await items.ForEachAsync(5, async (KahunaSetKeyValueRequest item) =>
        {
            KeyValueRequest request = new(
                KeyValueRequestType.TrySet, 
                item.TransactionId,
                HLCTimestamp.Zero,
                item.Key ?? "", 
                item.Value, 
                item.CompareValue,
                item.CompareRevision,
                item.Flags,
                item.ExpiresMs, 
                HLCTimestamp.Zero,
                item.Durability
            );

            KeyValueResponse? response;
            
            if (item.Durability == KeyValueDurability.Ephemeral)
                response = await ephemeralKeyValuesRouter.Ask(request);
            else
                response = await persistentKeyValuesRouter.Ask(request);

            if (response is null)
            {
                responses.Add(new() { Type = KeyValueResponseType.Errored });
                return;
            }

            lock (sync)
                responses.Add(new() { Type = response.Type, Revision = response.Revision, LastModified = response.Ticket });
            
            //await Task.CompletedTask;
        });

        return responses;
    }
    
    /// <summary>
    /// Set a timeout on key. After the timeout has expired, the key will automatically be deleted
    /// </summary>
    /// <param name="key"></param>
    /// <param name="expiresMs"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, long, HLCTimestamp)> TryExtendKeyValue(
        HLCTimestamp transactionId,
        string key, 
        int expiresMs, 
        KeyValueDurability durability
    )
    {
        KeyValueRequest request = new(
            KeyValueRequestType.TryExtend,
            transactionId,
            HLCTimestamp.Zero,
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
            return (KeyValueResponseType.Errored, -1, HLCTimestamp.Zero);
        
        return (response.Type, response.Revision, response.Ticket);
    }

    /// <summary>
    /// Removes the specified key. A key is ignored if it does not exist.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, long, HLCTimestamp)> TryDeleteKeyValue(HLCTimestamp transactionId, string key, KeyValueDurability durability)
    {
        KeyValueRequest request = new(
            KeyValueRequestType.TryDelete, 
            transactionId,
            HLCTimestamp.Zero,
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
            return (KeyValueResponseType.Errored, -1, HLCTimestamp.Zero);
        
        return (response.Type, response.Revision, response.Ticket);
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
            HLCTimestamp.Zero,
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
            HLCTimestamp.Zero,
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
            HLCTimestamp.Zero,
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
    public async Task<List<(KeyValueResponseType, string, KeyValueDurability)>> TryAcquireManyExclusiveLocks(
        HLCTimestamp transactionId, 
        List<(string key, int expiresMs, KeyValueDurability durability)> keys
    )
    {
        List<(KeyValueResponseType, string, KeyValueDurability)> responses = new(keys.Count);
        
        foreach ((string key, int expiresMs, KeyValueDurability durability) key in keys)
        {
            KeyValueRequest request = new(
                KeyValueRequestType.TryAcquireExclusiveLock,
                transactionId,
                HLCTimestamp.Zero,
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
            HLCTimestamp.Zero,
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
    public async Task<List<(KeyValueResponseType, string, KeyValueDurability)>> TryReleaseManyExclusiveLocks(
        HLCTimestamp transactionId, 
        List<(string key, KeyValueDurability durability)> keys
    )
    {
        List<(KeyValueResponseType, string, KeyValueDurability)> responses = new(keys.Count);
        
        foreach ((string key, KeyValueDurability durability) key in keys)
        {
            KeyValueRequest request = new(
                KeyValueRequestType.TryReleaseExclusiveLock,
                transactionId,
                HLCTimestamp.Zero,
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
    /// <param name="commitId"></param>
    /// <param name="key"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)> TryPrepareMutations(HLCTimestamp transactionId, HLCTimestamp commitId, string key, KeyValueDurability durability)
    {
        KeyValueRequest request = new(
            KeyValueRequestType.TryPrepareMutations, 
            transactionId, 
            commitId,
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
    public async Task<List<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)>> TryPrepareManyMutations(
        HLCTimestamp transactionId,
        HLCTimestamp commitId,
        List<(string key, KeyValueDurability durability)> keys
    )
    {
        Lock sync = new();
        List<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)> responses = new(keys.Count);

        await keys.ForEachAsync(5, async ((string key, KeyValueDurability durability) key) =>
        {
            KeyValueRequest request = new(
                KeyValueRequestType.TryPrepareMutations,
                transactionId,
                commitId,
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
                return;

            lock (sync)
                responses.Add((response.Type, response.Ticket, key.key, key.durability));
            
            //(string key, KeyValueDurability durability)
        });

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
    public async Task<(KeyValueResponseType, long)> TryCommitMutations(
        HLCTimestamp transactionId, 
        string key, 
        HLCTimestamp proposalTicketId, 
        KeyValueDurability durability
    )
    {
        KeyValueRequest request = new(
            KeyValueRequestType.TryCommitMutations, 
            transactionId, 
            HLCTimestamp.Zero,
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
    /// Passes many TryCommit requests to the key/value actor for the given keyValue name.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="proposalTicketId"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public async Task<List<(KeyValueResponseType type, string key, long proposalIndex, KeyValueDurability durability)>> TryCommitManyMutations(
        HLCTimestamp transactionId, 
        List<(string key, HLCTimestamp proposalTicketId, KeyValueDurability durability)> keys
    )
    {
        Lock sync = new();
        List<(KeyValueResponseType type, string key, long proposalIndex, KeyValueDurability durability)> responses = new(keys.Count);

        await keys.ForEachAsync(5, async ((string key, HLCTimestamp proposalTicketId, KeyValueDurability durability) key) =>
        {
            KeyValueRequest request = new(
                KeyValueRequestType.TryCommitMutations,
                transactionId,
                HLCTimestamp.Zero,
                key.key,
                null,
                null,
                -1,
                KeyValueFlags.None,
                0,
                key.proposalTicketId,
                key.durability
            );

            KeyValueResponse? response;

            if (key.durability == KeyValueDurability.Ephemeral)
                response = await ephemeralKeyValuesRouter.Ask(request);
            else
                response = await persistentKeyValuesRouter.Ask(request);

            if (response is null)
                return;

            lock (sync)
                responses.Add((response.Type, key.key, response.Revision, key.durability));
        });

        return responses;
    }
    
    /// <summary>
    /// Passes a TryRollback request to the key/value actor for the given keyValue name.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="proposalTicketId"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, long)> TryRollbackMutations(
        HLCTimestamp transactionId, 
        string key, 
        HLCTimestamp proposalTicketId, 
        KeyValueDurability durability
    )
    {
        KeyValueRequest request = new(
            KeyValueRequestType.TryRollbackMutations, 
            transactionId, 
            HLCTimestamp.Zero,
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
    /// Passes many TryRollback requests to the key/value actor for the given keyValue name.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="proposalTicketId"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public async Task<List<(KeyValueResponseType type, string key, long proposalIndex, KeyValueDurability durability)>> TryRollbackManyMutations(
        HLCTimestamp transactionId, 
        List<(string key, HLCTimestamp proposalTicketId, KeyValueDurability durability)> keys
    )
    {
        Lock sync = new();
        List<(KeyValueResponseType type, string key, long proposalIndex, KeyValueDurability durability)> responses = new(keys.Count);

        await keys.ForEachAsync(5, async ((string key, HLCTimestamp proposalTicketId, KeyValueDurability durability) key) =>
        {
            KeyValueRequest request = new(
                KeyValueRequestType.TryRollbackMutations,
                transactionId,
                HLCTimestamp.Zero,
                key.key,
                null,
                null,
                -1,
                KeyValueFlags.None,
                0,
                key.proposalTicketId,
                key.durability
            );

            KeyValueResponse? response;

            if (key.durability == KeyValueDurability.Ephemeral)
                response = await ephemeralKeyValuesRouter.Ask(request);
            else
                response = await persistentKeyValuesRouter.Ask(request);

            if (response is null)
                return;

            lock (sync)
                responses.Add((response.Type, key.key, response.Revision, key.durability));
        });

        return responses;
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
    /// The returned values aren't consistent, they can contain stale data
    /// </summary>
    /// <param name="prefixKeyName"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    /// <exception cref="KahunaServerException"></exception>
    public async Task<KeyValueGetByPrefixResult> ScanByPrefix(string prefixKeyName, KeyValueDurability durability)
    {
        HLCTimestamp currentTime = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());
        
        KeyValueRequest request = new(
            KeyValueRequestType.ScanByPrefix,
            currentTime,
            HLCTimestamp.Zero,
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
            
            return new(KeyValueResponseType.Get, items);
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
            
            return new(KeyValueResponseType.Get, items);
        }

        throw new KahunaServerException("Unknown durability");
    }

    /// <summary>
    /// Returns a consistent snapshot of key/value pairs that matches the specified prefix
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="prefixKeyName"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public async Task<KeyValueGetByPrefixResult> GetByPrefix(HLCTimestamp transactionId, string prefixKeyName, KeyValueDurability durability)
    {
        KeyValueRequest request = new(
            KeyValueRequestType.GetByPrefix,
            transactionId,
            HLCTimestamp.Zero,
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
            return new(KeyValueResponseType.Errored, []);
        
        if (response is { Type: KeyValueResponseType.Get, Items: not null })
            return new(response.Type, response.Items); 
        
        return new(response.Type, []);
    }

    /// <summary>
    /// Starts a new transaction with the specified options.
    /// </summary>
    /// <param name="options">The options for configuring the transaction.</param>
    /// <returns>Returns an <c>HLCTimestamp</c> representing the timestamp of the started transaction.</returns>
    public Task<(KeyValueResponseType, HLCTimestamp)> StartTransaction(KeyValueTransactionOptions options)
    {
        return txCoordinator.StartTransaction(options);
    }

    /// <summary>
    /// Commits a transaction identified by the given timestamp.
    /// </summary>
    /// <param name="timestamp">The timestamp associated with the transaction to be committed.</param>
    /// <param name="acquiredLocks">List of acquired locks</param> 
    /// <param name="modifiedKeys">List of modified keys</param> 
    /// <returns>A task that represents the asynchronous operation, containing a boolean value that indicates whether the transaction was successfully committed.</returns>
    public Task<KeyValueResponseType> CommitTransaction(
        HLCTimestamp timestamp, 
        List<KeyValueTransactionModifiedKey> acquiredLocks, 
        List<KeyValueTransactionModifiedKey> modifiedKeys
    )
    {
        return txCoordinator.CommitTransaction(timestamp, acquiredLocks, modifiedKeys);
    }

    /// <summary>
    /// Rollbacks a transaction identified by the given timestamp.
    /// </summary>
    /// <param name="timestamp"></param>
    /// <param name="acquiredLocks">List of acquired locks</param> 
    /// <param name="modifiedKeys">List of modified keys</param> 
    /// <returns></returns>
    public Task<KeyValueResponseType> RollbackTransaction(
        HLCTimestamp timestamp, 
        List<KeyValueTransactionModifiedKey> acquiredLocks, 
        List<KeyValueTransactionModifiedKey> modifiedKeys
    )
    {
        return txCoordinator.RollbackTransaction(timestamp, acquiredLocks, modifiedKeys);
    }    
}