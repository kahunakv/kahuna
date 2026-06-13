
using Nixie;

using Kommander;
using Kommander.Data;
using Kommander.Time;

using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Locks;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Server.Replication;
using Kahuna.Server.ScriptParser;
using Kahuna.Server.Sequencer;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Locks;
using Kahuna.Shared.Sequences;
using Kahuna.Server.Communication.Internode;
using Kahuna.Server.Locks.Data;
using Kahuna.Shared.Communication.Rest;

namespace Kahuna;

/// <summary>
/// Façade to the internal systems of Kahuna.
/// </summary>
public sealed class KahunaManager : IKahuna, IDisposable
{
    /// <summary>
    /// Represents the core actor system used for managing actor-based concurrency within the KahunaManager.
    /// Manages the lifecycle and communication of actor instances used throughout the system.
    /// Provides the infrastructure for creating and handling actors like BackgroundWriterActor and ScriptParserEvicterActor.
    /// </summary>
    private readonly ActorSystem actorSystem;

    /// <summary>
    /// Manages distributed locking mechanisms within the KahunaManager.
    /// Provides functionality for acquiring, extending, and releasing locks
    /// in a distributed system, ensuring proper synchronization and consistency.
    /// Acts as the primary interface for lock-related operations, delegating the
    /// underlying execution to the LockManager class.
    /// </summary>
    private readonly LockManager locks;

    /// <summary>
    /// Manages key-value storage operations in KahunaManager, including the setting,
    /// retrieval, and existence checks of key-value pairs. Provides methods to handle
    /// concurrency, durability, and transactional requirements for key-value interactions.
    /// Acts as a centralized component for key-value operations within the Kahuna system.
    /// </summary>
    private readonly KeyValuesManager keyValues;

    private readonly SequencerManager sequencer;

    private readonly IActorRef<BackgroundWriterActor, BackgroundWriteRequest> backgroundWriter;

    private readonly IPersistenceBackend persistenceBackend;

    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="actorSystem"></param>
    /// <param name="raft"></param>
    /// <param name="configuration"></param>
    /// <param name="logger"></param>
    public KahunaManager(ActorSystem actorSystem, IRaft raft, KahunaConfiguration configuration, IInterNodeCommunication interNodeCommunication, ILogger<IKahuna> logger)
    {
        this.actorSystem = actorSystem;

        persistenceBackend = GetPersistence(configuration);

        backgroundWriter = actorSystem.Spawn<BackgroundWriterActor, BackgroundWriteRequest>(
            "background-writer",
            raft,
            persistenceBackend,
            configuration,
            logger
        );

        this.locks = new(actorSystem, raft, interNodeCommunication, persistenceBackend, backgroundWriter, configuration, logger);
        this.keyValues = new(actorSystem, raft, interNodeCommunication, persistenceBackend, backgroundWriter, configuration, logger);
        this.sequencer = new(keyValues, logger);

        // Register the key-range data-movement hook once, here, so every host (embedded,
        // server, tests) gets it uniformly without reaching across the internal API boundary.
        raft.RegisterStateMachineTransfer(keyValues.KvStateMachineTransfer);
    }

    public void Dispose()
    {
        GC.SuppressFinalize(this);

        keyValues.Dispose();

        if (persistenceBackend is IDisposable disposable)
            disposable.Dispose();
    }
    
    /// <summary>
    /// Flushes all pending dirty objects to the persistence backend and waits for completion.
    /// Use this before closing/disposing to ensure all queued writes land in storage.
    /// </summary>
    public Task FlushPersistenceAsync()
    {
        TaskCompletionSource<bool> tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
        backgroundWriter.Send(new(BackgroundWriteType.FlushAndNotify, tcs));
        return tcs.Task;
    }

    /// <summary>
    /// Creates the persistence instance
    /// </summary>
    /// <param name="configuration"></param>
    /// <returns></returns>
    /// <exception cref="KahunaServerException"></exception>
    private static IPersistenceBackend GetPersistence(KahunaConfiguration configuration)
    {
        return configuration.Storage switch
        {
            "rocksdb" => new RocksDbPersistenceBackend(configuration.StoragePath, configuration.StorageRevision),
            "sqlite" => new SqlitePersistenceBackend(configuration.StoragePath, configuration.StorageRevision),
            "memory" => new MemoryPersistenceBackend(),
            _ => throw new KahunaServerException("Invalid storage type: " + configuration.Storage)
        };
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
        return locks.LocateAndTryLock(resource, owner, expiresMs, durability, cancellationToken);
    }

    /// <summary>
    /// Locates the leader node for the given key and passes a TryExtendLock request to the locker actor for the given lock name.
    /// </summary>
    /// <param name="resource"></param>
    /// <param name="owner"></param>
    /// <param name="expiresMs"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public Task<(LockResponseType, long)> LocateAndTryExtendLock(string resource, byte[] owner, int expiresMs, LockDurability durability, CancellationToken cancellationToken)
    {
        return locks.LocateAndTryExtendLock(resource, owner, expiresMs, durability, cancellationToken);
    }

    /// <summary>
    /// Locates the leader node for the given key and passes a TryUnlock request to the locker actor for the given lock name.
    /// </summary>
    /// <param name="resource"></param>
    /// <param name="owner"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public Task<LockResponseType> LocateAndTryUnlock(string resource, byte[] owner, LockDurability durability,CancellationToken cancellationToken)
    {
        return locks.LocateAndTryUnlock(resource, owner, durability, cancellationToken);
    }

    /// <summary>
    /// Locates the leader node for the given key and passes a TryGetLock request to the locker actor for the given lock name.
    /// </summary>
    /// <param name="resource"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public Task<(LockResponseType, ReadOnlyLockEntry?)> LocateAndGetLock(string resource, LockDurability durability, CancellationToken cancellationToken)
    {
        return locks.LocateAndGetLock(resource, durability, cancellationToken);
    }
    
    /// <summary>
    /// Passes a TryLock request to the locker actor for the given lock name. 
    /// </summary>
    /// <param name="resource"></param>
    /// <param name="owner"></param>
    /// <param name="expiresMs"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public Task<(LockResponseType, long)> TryLock(string resource, byte[] owner, int expiresMs, LockDurability durability)
    {
        return locks.TryLock(resource, owner, expiresMs, durability);
    }

    /// <summary>
    /// Passes a TryExtendLock request to the locker actor for the given lock name. 
    /// </summary>
    /// <param name="resource"></param>
    /// <param name="owner"></param>
    /// <param name="expiresMs"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public Task<(LockResponseType, long)> TryExtendLock(string resource, byte[] owner, int expiresMs, LockDurability durability)
    {
        return locks.TryExtendLock(resource, owner, expiresMs, durability);
    }

    /// <summary>
    /// Passes a TryUnlock request to the locker actor for the given lock name.
    /// </summary>
    /// <param name="resource"></param>
    /// <param name="owner"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public Task<LockResponseType> TryUnlock(string resource, byte[] owner, LockDurability durability)
    {
        return locks.TryUnlock(resource, owner, durability);
    }

    /// <summary>
    /// Returns information about the lock for the given resource.
    /// </summary>
    /// <param name="resource"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public Task<(LockResponseType, ReadOnlyLockEntry?)> GetLock(string resource, LockDurability durability)
    {
        return locks.GetLock(resource, durability);
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
    /// <param name="durability"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, long, HLCTimestamp)> LocateAndTrySetKeyValue(
        HLCTimestamp transactionId,
        string key,
        byte[]? value,
        byte[]? compareValue,
        long compareRevision,
        KeyValueFlags flags,
        int expiresMs,
        KeyValueDurability durability,
        CancellationToken cancellationToken,
        long routedGeneration = 0
    )
    {
        return await keyValues.LocateAndTrySetKeyValue(
            transactionId,
            key,
            value,
            compareValue,
            compareRevision,
            flags,
            expiresMs,
            durability,
            cancellationToken,
            routedGeneration
        );
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="setManyItems"></param>
    /// <param name="contextCancellationToken"></param>
    /// <returns></returns>
    public Task<List<KahunaSetKeyValueResponseItem>> LocateAndTrySetManyKeyValue(List<KahunaSetKeyValueRequestItem> setManyItems, CancellationToken cancellationToken)
    {
        return keyValues.LocateAndTrySetManyKeyValue(
            setManyItems, 
            cancellationToken
        );
    }

    public Task<List<KahunaDeleteKeyValueResponseItem>> LocateAndTryDeleteManyKeyValue(List<KahunaDeleteKeyValueRequestItem> deleteManyItems, CancellationToken cancellationToken)
    {
        return keyValues.LocateAndTryDeleteManyKeyValue(deleteManyItems, cancellationToken);
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
    public Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> LocateAndTryGetValue(
        HLCTimestamp transactionId,
        string key,
        long revision,
        HLCTimestamp readTimestamp,
        KeyValueDurability durability,
        CancellationToken cancellationToken
    )
    {
        return keyValues.LocateAndTryGetValue(transactionId, key, revision, readTimestamp, durability, cancellationToken);
    }

    public Task<List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)>> LocateAndTryGetManyValues(
        HLCTimestamp transactionId,
        List<(string key, long revision, KeyValueDurability durability)> keys,
        CancellationToken cancellationToken
    )
    {
        return keyValues.LocateAndTryGetManyValues(transactionId, keys, cancellationToken);
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
    public Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> LocateAndTryExistsValue(
        HLCTimestamp transactionId,
        string key,
        long revision,
        HLCTimestamp readTimestamp,
        KeyValueDurability durability,
        CancellationToken cancellationToken
    )
    {
        return keyValues.LocateAndTryExistsValue(transactionId, key, revision, readTimestamp, durability, cancellationToken);
    }

    public Task<List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)>> LocateAndTryExistsManyValues(
        HLCTimestamp transactionId,
        List<(string key, long revision, KeyValueDurability durability)> keys,
        CancellationToken cancellationToken
    )
    {
        return keyValues.LocateAndTryExistsManyValues(transactionId, keys, cancellationToken);
    }

    public Task<KeyValueResponseType> LocateAndTryCheckWriteIntent(
        HLCTimestamp transactionId,
        string key,
        KeyValueDurability durability,
        CancellationToken cancellationToken
    )
    {
        return keyValues.LocateAndTryCheckWriteIntent(transactionId, key, durability, cancellationToken);
    }

    /// <summary>
    /// Locates the leader node for the given key and executes the TryDeleteValue request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public Task<(KeyValueResponseType, long, HLCTimestamp)> LocateAndTryDeleteKeyValue(
        HLCTimestamp transactionId, 
        string key, 
        KeyValueDurability durability, 
        CancellationToken cancellationToken
    )
    {
        return keyValues.LocateAndTryDeleteKeyValue(transactionId, key, durability, cancellationToken);
    }
    
    /// <summary>
    /// Locates the leader node for the given key and executes the TryExtendValue request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="expiresMs"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public Task<(KeyValueResponseType, long, HLCTimestamp)> LocateAndTryExtendKeyValue(
        HLCTimestamp transactionId, 
        string key, 
        int expiresMs, 
        KeyValueDurability durability, 
        CancellationToken cancellationToken
    )
    {
        return keyValues.LocateAndTryExtendKeyValue(transactionId, key, expiresMs, durability, cancellationToken);
    }
    
    /// <summary>
    /// Locates the leader node for the given key and executes the TryAcquireExclusiveLock request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="expiresMs"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public Task<(KeyValueResponseType, string, KeyValueDurability)> LocateAndTryAcquireExclusiveLock(
        HLCTimestamp transactionId, 
        string key, 
        int expiresMs, 
        KeyValueDurability durability, 
        CancellationToken cancellationToken
    )
    {
        return keyValues.LocateAndTryAcquireExclusiveLock(transactionId, key, expiresMs, durability, cancellationToken);
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="prefixKey"></param>
    /// <param name="expiresMs"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public Task<KeyValueResponseType> LocateAndTryAcquireExclusivePrefixLock(
        HLCTimestamp transactionId,
        string prefixKey,
        int expiresMs,
        KeyValueDurability durability,
        CancellationToken cancellationToken
    )
    {
        return keyValues.LocateAndTryAcquireExclusivePrefixLock(transactionId, prefixKey, expiresMs, durability, cancellationToken);
    }

    /// <summary>
    /// Locates the leader node for the given key and executes the TryAcquireExclusiveLock request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="keys"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public Task<List<(KeyValueResponseType, string, KeyValueDurability)>> LocateAndTryAcquireManyExclusiveLocks(
        HLCTimestamp transactionId,
        List<(string key, int expiresMs, KeyValueDurability durability)> keys, 
        CancellationToken cancellationToken
    )
    {
        return keyValues.LocateAndTryAcquireManyExclusiveLocks(transactionId, keys, cancellationToken);
    }

    /// <summary>
    /// Locates the leader node for the given key and executes the TryReleaseExclusiveLock request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public Task<(KeyValueResponseType, string)> LocateAndTryReleaseExclusiveLock(
        HLCTimestamp transactionId, 
        string key, 
        KeyValueDurability durability, 
        CancellationToken cancellationToken
    )
    {
        return keyValues.LocateAndTryReleaseExclusiveLock(transactionId, key, durability, cancellationToken);
    }
    
    /// <summary>
    /// 
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="prefixKey"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public Task<KeyValueResponseType> LocateAndTryReleaseExclusivePrefixLock(
        HLCTimestamp transactionId,
        string prefixKey,
        KeyValueDurability durability,
        CancellationToken cancellationToken
    )
    {
        return keyValues.LocateAndTryReleaseExclusivePrefixLock(transactionId, prefixKey, durability, cancellationToken);
    }

    public Task<KeyValueResponseType> LocateAndTryAcquireRangeLock(
        HLCTimestamp transactionId,
        string prefix,
        string? startKey, bool startInclusive,
        string? endKey,   bool endInclusive,
        int expiresMs,
        KeyValueDurability durability,
        RangeLockMode mode,
        CancellationToken cancellationToken
    ) => keyValues.LocateAndTryAcquireRangeLock(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, expiresMs, durability, mode, cancellationToken);

    public Task<KeyValueResponseType> LocateAndTryAcquireExclusiveRangeLock(
        HLCTimestamp transactionId,
        string prefix,
        string? startKey, bool startInclusive,
        string? endKey,   bool endInclusive,
        int expiresMs,
        KeyValueDurability durability,
        CancellationToken cancellationToken
    ) => keyValues.LocateAndTryAcquireRangeLock(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, expiresMs, durability, RangeLockMode.Exclusive, cancellationToken);

    public Task<KeyValueResponseType> LocateAndTryReleaseExclusiveRangeLock(
        HLCTimestamp transactionId,
        string prefix,
        string? startKey, bool startInclusive,
        string? endKey,   bool endInclusive,
        KeyValueDurability durability,
        CancellationToken cancellationToken
    )
    {
        return keyValues.LocateAndTryReleaseExclusiveRangeLock(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, durability, cancellationToken);
    }

    /// <summary>
    /// Locates the leader node for the given keysx and executes the TryReleaseExclusiveLock request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="keys"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public Task<List<(KeyValueResponseType, string, KeyValueDurability)>> LocateAndTryReleaseManyExclusiveLocks(
        HLCTimestamp transactionId, 
        List<(string key, KeyValueDurability durability)> keys, 
        CancellationToken cancellationToken
    )
    {
        return keyValues.LocateAndTryReleaseManyExclusiveLocks(transactionId, keys, cancellationToken);
    }

    /// <summary>
    /// Locates the leader node for the given key and executes the TryPrepareMutations request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="commitId"></param> 
    /// <param name="key"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public Task<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)> LocateAndTryPrepareMutations(
        HLCTimestamp transactionId,
        HLCTimestamp commitId,
        string key,
        KeyValueDurability durability,
        CancellationToken cancellationToken,
        long routedGeneration = 0
    )
    {
        return keyValues.LocateAndTryPrepareMutations(transactionId, commitId, key, durability, cancellationToken, routedGeneration);
    }

    /// <summary>
    /// Locates the leader node for the given key and executes the TryPrepareManyMutations request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="commitId"></param> 
    /// <param name="keys"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public Task<List<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)>> LocateAndTryPrepareManyMutations(
        HLCTimestamp transactionId,
        HLCTimestamp commitId,
        List<(string key, KeyValueDurability durability)> keys, 
        CancellationToken cancellationToken
    )
    {
        return keyValues.LocateAndTryPrepareManyMutations(transactionId, commitId, keys, cancellationToken);
    }

    /// <summary>
    /// Locates the leader node for the given key and executes the TryCommitMutations request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public Task<(KeyValueResponseType, long)> LocateAndTryCommitMutations(
        HLCTimestamp transactionId, 
        string key, 
        HLCTimestamp ticketId, 
        KeyValueDurability durability, 
        CancellationToken cancellationToken
    )
    {
        return keyValues.LocateAndTryCommitMutations(transactionId, key, ticketId, durability, cancellationToken);
    }

    /// <summary>
    /// Locates the leader node for the given keys and executes the TryCommitMutations request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="keys"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public Task<List<(KeyValueResponseType, string, long, KeyValueDurability)>> LocateAndTryCommitManyMutations(
        HLCTimestamp transactionId, 
        List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)> keys, 
        CancellationToken cancellationToken
    )
    {
        return keyValues.LocateAndTryCommitManyMutations(transactionId, keys, cancellationToken);
    }
    
    /// <summary>
    /// Locates the leader node for the given key and executes the TryRollbackMutations request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public Task<(KeyValueResponseType, long)> LocateAndTryRollbackMutations(
        HLCTimestamp transactionId, 
        string key, 
        HLCTimestamp ticketId, 
        KeyValueDurability durability, 
        CancellationToken cancellationToken
    )
    {
        return keyValues.LocateAndTryRollbackMutations(transactionId, key, ticketId, durability, cancellationToken);
    }
    
    /// <summary>
    /// Locates the leader node for the given keys and executes the TryRollbackMutations request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="keys"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public Task<List<(KeyValueResponseType, string, long, KeyValueDurability)>> LocateAndTryRollbackManyMutations(
        HLCTimestamp transactionId, 
        List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)> keys, 
        CancellationToken cancellationToken
    )
    {
        return keyValues.LocateAndTryRollbackManyMutations(transactionId, keys, cancellationToken);
    }

    /// <summary>
    /// Locates the leader node for the given prefix and executes the GetByBucket request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="prefixedKey"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public Task<KeyValueGetByBucketResult> LocateAndGetByBucket(
        HLCTimestamp transactionId,
        string prefixedKey,
        HLCTimestamp readTimestamp,
        KeyValueDurability durability,
        CancellationToken cancellationToken
    )
    {
        return keyValues.LocateAndGetByBucket(transactionId, prefixedKey, readTimestamp, durability, cancellationToken);
    }

    public Task<KeyValueGetByRangeResult> LocateAndGetByRange(
        HLCTimestamp transactionId,
        string prefix,
        string? startKey,
        bool startInclusive,
        string? endKey,
        bool endInclusive,
        int limit,
        HLCTimestamp readTimestamp,
        KeyValueDurability durability,
        CancellationToken cancellationToken)
    {
        return keyValues.LocateAndGetByRange(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, limit, readTimestamp, durability, cancellationToken);
    }

    public IAsyncEnumerable<(string Key, ReadOnlyKeyValueEntry Entry)> LocateAndScanRange(
        HLCTimestamp txId,
        string prefix,
        string? startKey,
        bool startInclusive,
        string? endKey,
        bool endInclusive,
        int pageSize,
        HLCTimestamp readTimestamp,
        KeyValueDurability durability,
        CancellationToken ct)
    {
        return keyValues.LocateAndScanRange(txId, prefix, startKey, startInclusive, endKey, endInclusive, pageSize, readTimestamp, durability, ct);
    }

    /// <summary>
    /// Starts a transaction for a key-value operation by locating the appropriate node and setting up the transaction.
    /// </summary>
    /// <param name="options">The options specifying the parameters of the transaction.</param>
    /// <param name="cancellationToken">The token to monitor for cancellation requests.</param>
    /// <returns>A task that represents the asynchronous operation. The task result contains the response type and the timestamp of the initiated transaction.</returns>
    public Task<(KeyValueResponseType, HLCTimestamp)> LocateAndStartTransaction(KeyValueTransactionOptions options, CancellationToken cancellationToken)
    {
        return keyValues.LocateAndStartTransaction(options, cancellationToken);        
    }

    /// <summary>
    /// Commits a transaction by locating required resources and applying all necessary modifications.
    /// </summary>
    /// <param name="uniqueId">The unique identifier for the transaction to be committed.</param>
    /// <param name="timestamp">The timestamp associated with the transaction.</param>
    /// <param name="acquiredLocks">A list of locks acquired as part of the transaction.</param>
    /// <param name="modifiedKeys">A list of keys modified as part of the transaction.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task representing the asynchronous operation, with a result of <see cref="KeyValueResponseType"/> indicating the outcome of the transaction commit.</returns>
    public Task<KeyValueResponseType> LocateAndCommitTransaction(
        string uniqueId, 
        HLCTimestamp timestamp, 
        List<KeyValueTransactionModifiedKey> acquiredLocks, 
        List<KeyValueTransactionModifiedKey> modifiedKeys,
        List<KeyValueTransactionReadKey> readKeys,
        CancellationToken cancellationToken
    )
    {
        return keyValues.LocateAndCommitTransaction(uniqueId, timestamp, acquiredLocks, modifiedKeys, readKeys, cancellationToken);
    }

    /// <summary>
    /// Rolls back a transaction and restores the state of the system to the point
    /// before the transaction was executed.
    /// </summary>
    /// <param name="uniqueId">The unique identifier for the transaction to be rolled back.</param>
    /// <param name="timestamp">The timestamp associated with the transaction.</param>
    /// <param name="acquiredLocks">A list of locks acquired during the transaction.</param>
    /// <param name="modifiedKeys">A list of keys modified during the transaction.</param>
    /// <param name="cancellationToken">A token to observe for cancellation requests.</param>
    /// <returns>A <see cref="KeyValueResponseType"/> indicating the result of the rollback operation.</returns>
    public Task<KeyValueResponseType> LocateAndRollbackTransaction(
        string uniqueId, 
        HLCTimestamp timestamp, 
        List<KeyValueTransactionModifiedKey> acquiredLocks, 
        List<KeyValueTransactionModifiedKey> modifiedKeys,
        CancellationToken cancellationToken
    )
    {
        return keyValues.LocateAndRollbackTransaction(uniqueId, timestamp, acquiredLocks, modifiedKeys, cancellationToken);
    }

    /// <summary>
    /// Set key to hold the string value. If key already holds a value, it is overwritten
    /// </summary>
    /// <param name="key"></param>
    /// <param name="value"></param>
    /// <param name="compareValue"></param>
    /// <param name="compareRevision"></param>
    /// <param name="flags"></param>
    /// <param name="expiresMs"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public Task<(KeyValueResponseType, long, HLCTimestamp)> TrySetKeyValue(
        HLCTimestamp transactionId,
        string key,
        byte[]? value,
        byte[]? compareValue,
        long compareRevision,
        KeyValueFlags flags,
        int expiresMs,
        KeyValueDurability durability,
        long routedGeneration = 0
    )
    {
        return keyValues.TrySetKeyValue(
            transactionId,
            key,
            value,
            compareValue,
            compareRevision,
            flags,
            expiresMs,
            durability,
            routedGeneration
        );
    }

    /// <summary>
    /// Set a timeout on key. After the timeout has expired, the key will automatically be deleted
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="expiresMs"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public Task<(KeyValueResponseType, long, HLCTimestamp)> TryExtendKeyValue(
        HLCTimestamp transactionId, 
        string key, 
        int expiresMs, 
        KeyValueDurability durability
    )
    {
        return keyValues.TryExtendKeyValue(transactionId, key, expiresMs, durability);
    }

    /// <summary>
    /// Removes the specified key. A key is ignored if it does not exist.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public Task<(KeyValueResponseType, long, HLCTimestamp)> TryDeleteKeyValue(
        HLCTimestamp transactionId, 
        string key, 
        KeyValueDurability durability
    )
    {
        return keyValues.TryDeleteKeyValue(transactionId, key, durability);
    }

    public Task<List<KahunaDeleteKeyValueResponseItem>> DeleteManyNodeKeyValue(List<KahunaDeleteKeyValueRequestItem> items)
    {
        return keyValues.DeleteManyNodeKeyValue(items);
    }

    /// <summary>
    /// Returns a value and its context by the specified key
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="revision"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> TryGetValue(
        HLCTimestamp transactionId,
        string key,
        long revision,
        HLCTimestamp readTimestamp,
        KeyValueDurability durability
    )
    {
        return keyValues.TryGetValue(transactionId, key, revision, readTimestamp, durability);
    }

    public Task<List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)>> TryGetManyValues(
        HLCTimestamp transactionId,
        List<(string key, long revision, KeyValueDurability durability)> keys
    )
    {
        return keyValues.TryGetManyValues(transactionId, keys);
    }
    
    /// <summary>
    /// Checks if a key exists and its context (without the value)
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="revision"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> TryExistsValue(
        HLCTimestamp transactionId,
        string key,
        long revision,
        HLCTimestamp readTimestamp,
        KeyValueDurability durability
    )
    {
        return keyValues.TryExistsValue(transactionId, key, revision, readTimestamp, durability);
    }

    public Task<List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)>> TryExistsManyValues(
        HLCTimestamp transactionId,
        List<(string key, long revision, KeyValueDurability durability)> keys
    )
    {
        return keyValues.TryExistsManyValues(transactionId, keys);
    }

    public Task<KeyValueResponseType> TryCheckWriteIntentValue(
        HLCTimestamp transactionId,
        string key,
        KeyValueDurability durability
    )
    {
        return keyValues.TryCheckWriteIntentValue(transactionId, key, durability);
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="expiresMs"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public Task<(KeyValueResponseType, string, KeyValueDurability)> TryAcquireExclusiveLock(
        HLCTimestamp transactionId, 
        string key, 
        int expiresMs, 
        KeyValueDurability durability
    )
    {
        return keyValues.TryAcquireExclusiveLock(transactionId, key, expiresMs, durability);
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="prefixKey"></param>
    /// <param name="expiresMs"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public Task<KeyValueResponseType> TryAcquireExclusivePrefixLock(HLCTimestamp transactionId, string prefixKey, int expiresMs, KeyValueDurability durability)
    {
        return keyValues.TryAcquireExclusivePrefixLock(transactionId, prefixKey, expiresMs, durability);
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public Task<(KeyValueResponseType, string)> TryReleaseExclusiveLock(
        HLCTimestamp transactionId, 
        string key, 
        KeyValueDurability durability
    )
    {
        return keyValues.TryReleaseExclusiveLock(transactionId, key, durability);
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="prefixKey"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public Task<KeyValueResponseType> TryReleaseExclusivePrefixLock(HLCTimestamp transactionId, string prefixKey, KeyValueDurability durability)
    {
        return keyValues.TryReleaseExclusivePrefixLock(transactionId, prefixKey, durability);
    }

    public Task<KeyValueResponseType> TryAcquireExclusiveRangeLock(HLCTimestamp transactionId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive, int expiresMs, KeyValueDurability durability)
        => keyValues.TryAcquireRangeLock(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, expiresMs, durability, RangeLockMode.Exclusive);

    public Task<KeyValueResponseType> TryAcquireRangeLock(HLCTimestamp transactionId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive, int expiresMs, KeyValueDurability durability, RangeLockMode mode)
    {
        return keyValues.TryAcquireRangeLock(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, expiresMs, durability, mode);
    }

    public Task<KeyValueResponseType> TryReleaseExclusiveRangeLock(HLCTimestamp transactionId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive, KeyValueDurability durability)
    {
        return keyValues.TryReleaseExclusiveRangeLock(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, durability);
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="commitId"></param>
    /// <param name="key"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public Task<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)> TryPrepareMutations(
        HLCTimestamp transactionId,
        HLCTimestamp commitId,
        string key,
        KeyValueDurability durability,
        long routedGeneration = 0
    )
    {
        return keyValues.TryPrepareMutations(transactionId, commitId, key, durability, routedGeneration);
    }

    /// <summary>
    /// Attempts to commit mutations for a specified transaction and key with the given durability.
    /// </summary>
    /// <param name="transactionId">The timestamp representing the transaction ID.</param>
    /// <param name="key">The key for which the mutations are being committed.</param>
    /// <param name="proposalTicketId">The timestamp representing the ID of the proposal ticket.</param>
    /// <param name="durability">The durability level of the transaction, indicating whether it is ephemeral or persistent.</param>
    /// <returns>A task representing the asynchronous operation, containing a tuple where the first element is the result of the operation as a <see cref="KeyValueResponseType"/>, and the second element is a long value associated with the operation.</returns>
    public Task<(KeyValueResponseType, long)> TryCommitMutations(
        HLCTimestamp transactionId,
        string key,
        HLCTimestamp proposalTicketId,
        KeyValueDurability durability
    )
    {
        return keyValues.TryCommitMutations(transactionId, key, proposalTicketId, durability);
    }

    /// <summary>
    /// Attempts to rollback mutations for a given key, transaction, and proposal ticket.
    /// </summary>
    /// <param name="transactionId">The unique identifier of the transaction to rollback.</param>
    /// <param name="key">The key associated with the mutations.</param>
    /// <param name="proposalTicketId">The identifier of the proposal ticket related to the mutation.</param>
    /// <param name="durability">The durability level for the operation.</param>
    /// <returns>A task containing a tuple with the response type and the associated transaction version.</returns>
    public Task<(KeyValueResponseType, long)> TryRollbackMutations(
        HLCTimestamp transactionId,
        string key,
        HLCTimestamp proposalTicketId,
        KeyValueDurability durability
    )
    {
        return keyValues.TryRollbackMutations(transactionId, key, proposalTicketId, durability);
    }

    /// <summary>
    /// Attempts to execute a transaction script with the provided parameters and returns the result.
    /// </summary>
    /// <param name="script">The transaction script to execute, represented as a byte array.</param>
    /// <param name="hash">An optional hash representing the script for validation or identification purposes.</param>
    /// <param name="parameters">An optional list of parameters to be passed into the script during execution.</param>
    /// <returns>A task that represents the asynchronous operation and resolves to the result of the transaction execution.</returns>
    public Task<KeyValueTransactionResult> TryExecuteTransactionScript(byte[] script, string? hash, List<KeyValueParameter>? parameters)
    {
        return keyValues.TryExecuteTx(script, hash, parameters);
    }

    /// <summary>
    /// Scans the current node in the cluster and returns key/value pairs by prefix
    /// </summary>
    /// <param name="prefixKeyName"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public Task<KeyValueGetByBucketResult> ScanByPrefix(string prefixKeyName, HLCTimestamp readTimestamp, KeyValueDurability durability)
    {
        return keyValues.ScanByPrefix(prefixKeyName, readTimestamp, durability);
    }
    
    /// <summary>
    /// Gets the key/value pairs by prefix from the current node in the cluster
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="prefixKeyName"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public Task<KeyValueGetByBucketResult> GetByBucket(HLCTimestamp transactionId, string prefixKeyName, HLCTimestamp readTimestamp, KeyValueDurability durability)
    {
        return keyValues.GetByBucket(transactionId, prefixKeyName, readTimestamp, durability);
    }
    
    /// <summary>
    /// Scans all nodes in the cluster and returns key/value pairs by prefix
    /// </summary>
    /// <param name="prefixKeyName"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public Task<KeyValueGetByBucketResult> ScanAllByPrefix(string prefixKeyName, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken)
    {
        return keyValues.ScanAllByPrefix(prefixKeyName, readTimestamp, durability, cancellationToken);
    }

    /// <summary>
    /// Starts a new interactive transaction with the specified options.
    /// </summary>
    /// <param name="options">The options to configure the transaction.</param>
    /// <returns>Returns the timestamp of the started transaction.</returns>
    public Task<(KeyValueResponseType, HLCTimestamp)> StartTransaction(KeyValueTransactionOptions options)
    {
        return keyValues.StartTransaction(options);
    }

    /// <summary>
    /// Commits a transaction with the specified timestamp and list of modified keys.
    /// </summary>
    /// <param name="timestamp">The timestamp associated with the transaction.</param>
    /// <param name="modifiedKeys">The list of keys that were modified as part of the transaction.</param>
    /// <returns>A task that represents the asynchronous operation, containing a boolean indicating whether the transaction commitment was successful.</returns>
    public Task<KeyValueResponseType> CommitTransaction(
        HLCTimestamp timestamp, 
        List<KeyValueTransactionModifiedKey> acquiredLocks, 
        List<KeyValueTransactionModifiedKey> modifiedKeys,
        List<KeyValueTransactionReadKey> readKeys
    )
    {
        return keyValues.CommitTransaction(timestamp, acquiredLocks, modifiedKeys, readKeys);
    }

    /// <summary>
    /// Rolls back a transaction, reverting changes associated with the specified keys.
    /// </summary>
    /// <param name="timestamp">The timestamp of the transaction to be rolled back.</param>
    /// <param name="modifiedKeys">The list of modified keys associated with the transaction.</param>
    /// <returns>A task that represents the asynchronous operation. The task result contains a boolean indicating whether the rollback succeeded.</returns>
    public Task<KeyValueResponseType> RollbackTransaction(
        HLCTimestamp timestamp, 
        List<KeyValueTransactionModifiedKey> acquiredLocks, 
        List<KeyValueTransactionModifiedKey> modifiedKeys
    )
    {
        return keyValues.RollbackTransaction(timestamp, acquiredLocks, modifiedKeys);
    }

    public Task<(SequenceResponseType, ReadOnlySequenceEntry?)> LocateAndGetSequence(
        string name,
        SequenceDurability durability,
        CancellationToken cancellationToken
    )
    {
        return sequencer.LocateAndGetSequence(name, durability, cancellationToken);
    }

    public Task<(SequenceResponseType, long)> LocateAndCreateSequence(
        string name,
        long initialValue,
        long increment,
        long? maxValue,
        SequenceDurability durability,
        CancellationToken cancellationToken
    )
    {
        return sequencer.LocateAndCreateSequence(name, initialValue, increment, maxValue, durability, cancellationToken);
    }

    public Task<(SequenceResponseType, SequenceAllocation)> LocateAndNextSequenceValue(
        string name,
        string? idempotencyKey,
        SequenceDurability durability,
        CancellationToken cancellationToken
    )
    {
        return sequencer.LocateAndNextSequenceValue(name, idempotencyKey, durability, cancellationToken);
    }

    public Task<(SequenceResponseType, SequenceAllocation)> LocateAndReserveSequenceRange(
        string name,
        int count,
        string? idempotencyKey,
        SequenceDurability durability,
        CancellationToken cancellationToken
    )
    {
        return sequencer.LocateAndReserveSequenceRange(name, count, idempotencyKey, durability, cancellationToken);
    }

    public Task<SequenceResponseType> LocateAndDeleteSequence(
        string name,
        SequenceDurability durability,
        CancellationToken cancellationToken
    )
    {
        return sequencer.LocateAndDeleteSequence(name, durability, cancellationToken);
    }
    
    public async Task<bool> OnLogRestored(int partitionId, RaftLog log)
    {
        return log.LogType switch
        {
            ReplicationTypes.KeyValues => await keyValues.OnLogRestored(partitionId, log),
            ReplicationTypes.RangeMap => await keyValues.OnLogRestored(partitionId, log),
            ReplicationTypes.Locks => await locks.OnLogRestored(partitionId, log),
            _ => true
        };
    }

    public async Task<bool> OnReplicationReceived(int partitionId, RaftLog log)
    {
        return log.LogType switch
        {
            ReplicationTypes.KeyValues => await keyValues.OnReplicationReceived(partitionId, log),
            ReplicationTypes.RangeMap => await keyValues.OnReplicationReceived(partitionId, log),
            ReplicationTypes.Locks => await locks.OnReplicationReceived(partitionId, log),
            _ => true
        };
    }

    public void OnReplicationError(int partitionId, RaftLog log)
    {
        locks.OnReplicationError(log);
        keyValues.OnReplicationError(log);
    }

    internal Task RunCollectOnAllInstancesAsync() => keyValues.RunCollectOnAllInstancesAsync();

    /// <summary>The replicated range-descriptor map.</summary>
    internal RangeMapStore RangeMapStore => keyValues.RangeMapStore;

    /// <summary>The per-node key-space routing registry.</summary>
    internal KeySpaceRegistry KeySpaceRegistry => keyValues.KeySpaceRegistry;

    /// <inheritdoc/>
    public void RegisterKeyRange(string keySpace) => keyValues.KeySpaceRegistry.RegisterKeyRange(keySpace);

    /// <inheritdoc/>
    public Task<bool> RegisterKeyRangeAsync(string keySpace, CancellationToken cancellationToken = default) =>
        keyValues.RegisterKeyRangeAsync(keySpace, cancellationToken);

    /// <summary>The key-range data-movement primitive; register with <c>IRaft.RegisterStateMachineTransfer</c>.</summary>
    internal KvStateMachineTransfer KvStateMachineTransfer => keyValues.KvStateMachineTransfer;

    /// <summary>Returns live range locks held on <paramref name="keySpace"/> in the local actor (export helper).</summary>
    internal Task<List<KeyValueRangeLock>> GetRangeLocksAsync(string keySpace) =>
        keyValues.GetRangeLocksAsync(keySpace);

    /// <summary>Injects clamped lock entries into the local actor for <paramref name="keySpace"/> (import helper).</summary>
    internal Task ImportRangeLocksAsync(string keySpace, List<KeyValueRangeLock> locks) =>
        keyValues.ImportRangeLocksAsync(keySpace, locks);

    // IKahuna surface for inter-node routing.
    public Task<List<KeyValueRangeLock>> GetRangeLocks(string keySpace) =>
        keyValues.GetRangeLocksAsync(keySpace);

    public Task ImportRangeLocks(string keySpace, List<KeyValueRangeLock> locks) =>
        keyValues.ImportRangeLocksAsync(keySpace, locks);

    /// <summary>Resolves a key to its owning <c>(partitionId, generation)</c> (key-order router).</summary>
    internal (int PartitionId, long Generation) LocateRange(string key) => keyValues.LocateRange(key);

    /// <summary>The split-transaction executor.</summary>
    internal RangeSplitter RangeSplitter => keyValues.RangeSplitter;

    /// <summary>The merge-transaction executor.</summary>
    internal RangeMerger RangeMerger => keyValues.RangeMerger;

    /// <summary>
    /// Returns the data partition id that <paramref name="key"/> routes to under Kahuna's own
    /// consistent-hash assignment. Matches the routing used by <c>LocateAndTrySetKeyValue</c> and
    /// all other locating operations, so callers can find the right leader without guessing.
    /// </summary>
    public int GetDataPartitionForKey(string key) => keyValues.LocateRange(key).PartitionId;

    /// <summary>
    /// Checks every KeyRange descriptor and splits any that exceed the configured size threshold.
    /// Returns the number of splits performed. Only executes on the node that holds leadership
    /// of both the system partition (0) and meta partition (1).
    /// </summary>
    public Task<int> TriggerAutoSplitAsync(CancellationToken ct = default) =>
        keyValues.TriggerAutoSplitAsync(ct);

    /// <summary>
    /// Test-seam overload: runs the auto-split trigger with an explicit <paramref name="threshold"/>
    /// and <paramref name="minRangeSize"/> instead of the production config values.
    /// </summary>
    internal Task<int> TriggerAutoSplitAsync(int threshold, int minRangeSize, CancellationToken ct = default) =>
        keyValues.TriggerAutoSplitAsync(threshold, minRangeSize, ct);

    /// <summary>
    /// Scans all KeyRange spaces for adjacent under-min descriptor pairs and merges them.
    /// Returns the number of merges performed. Only executes on the dual-leader node.
    /// </summary>
    public Task<int> TriggerAutoMergeAsync(CancellationToken ct = default) =>
        keyValues.TriggerAutoMergeAsync(ct);

    /// <summary>
    /// Test-seam overload: runs the auto-merge trigger with an explicit <paramref name="minMergeSize"/>
    /// instead of the production config value.
    /// </summary>
    internal Task<int> TriggerAutoMergeAsync(int minMergeSize, CancellationToken ct = default) =>
        keyValues.TriggerAutoMergeAsync(minMergeSize, ct);

    /// <summary>
    /// Issues a persistent key-range write on the <b>local</b> node carrying an explicit routed
    /// generation (descriptor fence). Must be called on the descriptor partition's leader. Lets tests
    /// inject a stale generation; production routes through the locator which captures the live one.
    /// </summary>
    internal Task<(KeyValueResponseType, long, HLCTimestamp)> TrySetKeyValueRanged(
        HLCTimestamp transactionId, string key, byte[]? value, long routedGeneration) =>
        keyValues.TrySetKeyValue(transactionId, key, value, null, -1, KeyValueFlags.Set, 0,
            KeyValueDurability.Persistent, routedGeneration);

    /// <summary>
    /// Test seam: executes a split with a callback invoked inside the quiesce window (after catch-up
    /// import, before cutover). Lets F3 tests race a direct write into the window to verify
    /// <c>MustRetry</c> is returned.
    /// </summary>
    internal Task<SplitOutcome> SplitAsyncWithHook(
        string keySpace,
        string splitKey,
        int newPartitionId,
        Func<Task> duringQuiesce,
        CancellationToken ct = default) =>
        keyValues.RangeSplitter.SplitAsync(keySpace, splitKey, newPartitionId, duringQuiesce, ct);

    /// <summary>
    /// Test seam (F5): <paramref name="beforeQuery"/> is called before each descriptor's paged query
    /// starts; <paramref name="afterDescriptor"/> is called after each descriptor's pages are fully
    /// collected. Used by <c>Bucket_FanOut_IsParallelAndLeaderCoalesced</c> (gate-based concurrency
    /// proof) and <c>Bucket_SplitMidScan_NoDupNoMissing</c> (mid-fan-out split injection).
    /// </summary>
    internal Task<KeyValueGetByBucketResult> LocateAndGetByBucketWithHooks(
        HLCTimestamp transactionId, string prefixedKey, KeyValueDurability durability,
        Func<int, Task>? beforeQuery, Func<int, Task>? afterDescriptor,
        CancellationToken cancellationToken) =>
        keyValues.LocateAndGetByBucketWithHooks(
            transactionId, prefixedKey, durability, beforeQuery, afterDescriptor, cancellationToken);

    /// <summary>
    /// Test seam: acquires a range lock with a callback invoked after the initial
    /// <c>FindIntersecting</c> snapshot but before any sub-lock RPC. Lets tests inject a split
    /// into that window to drive the generation fence deterministically.
    /// </summary>
    internal Task<KeyValueResponseType> AcquireExclusiveRangeLockWithHook(
        HLCTimestamp transactionId,
        string prefix,
        string? startKey, bool startInclusive,
        string? endKey,   bool endInclusive,
        int expiresMs,
        KeyValueDurability durability,
        Func<Task> afterSnapshot,
        CancellationToken cancellationToken
    ) => keyValues.LocateAndTryAcquireExclusiveRangeLockWithHook(
            transactionId, prefix, startKey, startInclusive, endKey, endInclusive,
            expiresMs, durability, afterSnapshot, cancellationToken);
}
