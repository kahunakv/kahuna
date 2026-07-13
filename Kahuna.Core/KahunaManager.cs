
using Nixie;

using Kommander;
using Kommander.Data;
using Kommander.Time;
using Kommander.WAL;

using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Locks;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Server.Persistence.Pitr;
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

    private readonly BackupService? backupService;

    /// <summary>
    /// Exposes the persistence backend for PITR bootstrap tests that need to extract a
    /// checkpoint from an already-running node before seeding a joining peer.
    /// </summary>
    internal IPersistenceBackend PersistenceBackend => persistenceBackend;

    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="actorSystem"></param>
    /// <param name="raft"></param>
    /// <param name="configuration"></param>
    /// <param name="logger"></param>
    public KahunaManager(ActorSystem actorSystem, IRaft raft, KahunaConfiguration configuration, IInterNodeCommunication interNodeCommunication, ILogger<IKahuna> logger)
        : this(actorSystem, raft, configuration, interNodeCommunication, GetPersistence(configuration, logger, null), logger)
    {
    }

    /// <summary>
    /// Constructor variant that shares a RocksDB memory bundle (block cache + WriteBufferManager) with the
    /// Raft WAL. The composition root creates one <paramref name="sharedResources"/> and injects the same
    /// instance here and into the WAL so both RocksDB databases draw from a single unified budget. The
    /// bundle is <b>borrowed</b>: this manager (and the backend it creates) never dispose it — the
    /// composition root disposes it after both databases are closed. A null bundle behaves exactly as the
    /// primary constructor.
    /// </summary>
    public KahunaManager(ActorSystem actorSystem, IRaft raft, KahunaConfiguration configuration, IInterNodeCommunication interNodeCommunication, RocksDbSharedResources? sharedResources, ILogger<IKahuna> logger)
        : this(actorSystem, raft, configuration, interNodeCommunication, GetPersistence(configuration, logger, sharedResources), logger)
    {
    }

    /// <summary>
    /// Constructor variant used by PITR bootstrap: accepts a pre-seeded <paramref name="preSeededBackend"/>
    /// instead of creating a fresh one from configuration.  The WAL for the Raft layer is also
    /// pre-seeded externally; only the persistence-backend injection is handled here.
    /// </summary>
    internal KahunaManager(ActorSystem actorSystem, IRaft raft, KahunaConfiguration configuration, IInterNodeCommunication interNodeCommunication, IPersistenceBackend preSeededBackend, ILogger<IKahuna> logger)
    {
        this.actorSystem = actorSystem;

        persistenceBackend = preSeededBackend;

        SnapshotFloorStore snapshotFloorStore = new(raft, configuration.StoragePath, configuration.StorageRevision, logger);

        // Late-bound bridge so the background writer can acknowledge flushes back to the key-value
        // layer; the writer is spawned before the KeyValuesManager exists, so it is wired below.
        FlushNotificationSink flushNotificationSink = new();

        backgroundWriter = actorSystem.Spawn<BackgroundWriterActor, BackgroundWriteRequest>(
            "background-writer",
            raft,
            persistenceBackend,
            snapshotFloorStore,
            configuration,
            logger,
            flushNotificationSink
        );

        this.locks = new(actorSystem, raft, interNodeCommunication, persistenceBackend, backgroundWriter, configuration, logger);
        this.keyValues = new(actorSystem, raft, interNodeCommunication, persistenceBackend, backgroundWriter, configuration, logger, snapshotFloorStore);

        // Now that the key-value router exists, route flush acknowledgements to the owning actor so
        // it can advance FlushedRevision (making committed-but-unflushed entries eligible for eviction).
        flushNotificationSink.OnKeyValueFlushed = keyValues.NotifyFlushed;
        this.sequencer = new(keyValues, logger);

        // Register the key-range data-movement hook once, here, so every host (embedded,
        // server, tests) gets it uniformly without reaching across the internal API boundary.
        raft.RegisterStateMachineTransfer(keyValues.KvStateMachineTransfer);

        // Register the meta-partition (id 0) whole-state hook so a node that falls below the meta
        // WAL compaction floor is repaired with both the range map and the snapshot-floor holds.
        // Required now that the hold registry replicates deltas, which cannot be reconstructed from
        // surviving log entries once compacted.
        raft.RegisterSystemStateTransfer(keyValues.MetaSystemStateTransfer);

        if (!string.IsNullOrWhiteSpace(configuration.BackupDir))
        {
            backupService = new BackupService(
                raft,
                persistenceBackend,
                configuration.BackupDir,
                configuration.Storage,
                configuration.StorageRevision,
                FlushPersistenceAsync,
                keyValues.GetSafeTimestampAsync);
        }
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

    /// <inheritdoc/>
    public async Task BootstrapFromPitrBackupAsync(
        string backupDir,
        Guid leafBackupId,
        HLCTimestamp targetTime,
        Kommander.WAL.IWAL walAdapter,
        TimeSpan pitrWindow,
        TimeSpan baseSnapshotInterval)
    {
        BackupCatalog catalog = new(new LocalDirectoryStorageTarget(backupDir));
        IReadOnlyList<BackupManifest> chain = catalog.ResolveAndValidate(leafBackupId);

        if (targetTime == HLCTimestamp.Zero)
        {
            targetTime = chain
                .SelectMany(m => m.PartitionRanges)
                .Select(r => r.ToHlc)
                .Aggregate(HLCTimestamp.Zero, (acc, hlc) => hlc.CompareTo(acc) > 0 ? hlc : acc);
        }

        await FlushPersistenceAsync();
        BootstrapHelper.BootstrapNode(chain, backupDir, targetTime, persistenceBackend, walAdapter, pitrWindow, DateTime.UtcNow, baseSnapshotInterval);
    }

    /// <summary>
    /// Creates the persistence instance
    /// </summary>
    /// <param name="configuration"></param>
    /// <returns></returns>
    /// <exception cref="KahunaServerException"></exception>
    private static IPersistenceBackend GetPersistence(KahunaConfiguration configuration, ILogger<IKahuna> logger, RocksDbSharedResources? sharedResources)
    {
        return configuration.Storage switch
        {
            "rocksdb" => new RocksDbPersistenceBackend(configuration.StoragePath, configuration.StorageRevision, sharedResources),
            "sqlite" => new SqlitePersistenceBackend(configuration.StoragePath, configuration.StorageRevision, logger),
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
        long routedGeneration = 0,
        string coordinatorKey = "",
        TransactionOperationId operationId = default
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
            routedGeneration,
            coordinatorKey,
            operationId
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

    public Task<List<KahunaDeleteKeyValueResponseItem>> LocateAndTryDeleteManyKeyValue(List<KahunaDeleteKeyValueRequestItem> deleteManyItems, CancellationToken cancellationToken, string coordinatorKey = "", TransactionOperationId operationId = default)
    {
        return keyValues.LocateAndTryDeleteManyKeyValue(deleteManyItems, cancellationToken, coordinatorKey, operationId);
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
        CancellationToken cancellationToken,
        string coordinatorKey = "",
        TransactionOperationId operationId = default
    )
    {
        return keyValues.LocateAndTryGetValue(transactionId, key, revision, readTimestamp, durability, cancellationToken, coordinatorKey, operationId);
    }

    public Task<List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)>> LocateAndTryGetManyValues(
        HLCTimestamp transactionId,
        HLCTimestamp readTimestamp,
        List<(string key, long revision, KeyValueDurability durability)> keys,
        CancellationToken cancellationToken
    )
    {
        return keyValues.LocateAndTryGetManyValues(transactionId, readTimestamp, keys, cancellationToken);
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
        CancellationToken cancellationToken,
        string coordinatorKey = "",
        TransactionOperationId operationId = default
    )
    {
        return keyValues.LocateAndTryExistsValue(transactionId, key, revision, readTimestamp, durability, cancellationToken, coordinatorKey, operationId);
    }

    public Task<List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)>> LocateAndTryExistsManyValues(
        HLCTimestamp transactionId,
        HLCTimestamp readTimestamp,
        List<(string key, long revision, KeyValueDurability durability)> keys,
        CancellationToken cancellationToken
    )
    {
        return keyValues.LocateAndTryExistsManyValues(transactionId, readTimestamp, keys, cancellationToken);
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
        CancellationToken cancellationToken,
        string coordinatorKey = "",
        TransactionOperationId operationId = default
    )
    {
        return keyValues.LocateAndTryDeleteKeyValue(transactionId, key, durability, cancellationToken, coordinatorKey, operationId);
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
        CancellationToken cancellationToken,
        string coordinatorKey = "",
        TransactionOperationId operationId = default
    )
    {
        return keyValues.LocateAndTryExtendKeyValue(transactionId, key, expiresMs, durability, cancellationToken, coordinatorKey, operationId);
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
    public Task<(KeyValueResponseType, string, KeyValueDurability, HLCTimestamp HolderTransactionId)> LocateAndTryAcquireExclusiveLock(
        HLCTimestamp transactionId,
        string key,
        int expiresMs,
        KeyValueDurability durability,
        CancellationToken cancellationToken,
        string coordinatorKey = "",
        TransactionOperationId operationId = default
    )
    {
        return keyValues.LocateAndTryAcquireExclusiveLock(transactionId, key, expiresMs, durability, cancellationToken, coordinatorKey, operationId);
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
        CancellationToken cancellationToken,
        string coordinatorKey = "",
        TransactionOperationId operationId = default
    )
    {
        return keyValues.LocateAndTryAcquireExclusivePrefixLock(transactionId, prefixKey, expiresMs, durability, cancellationToken, coordinatorKey, operationId);
    }

    /// <summary>
    /// Locates the leader node for the given key and executes the TryAcquireExclusiveLock request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="keys"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public Task<List<(KeyValueResponseType, string, KeyValueDurability, HLCTimestamp HolderTransactionId)>> LocateAndTryAcquireManyExclusiveLocks(
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
        CancellationToken cancellationToken,
        string coordinatorKey = "",
        TransactionOperationId operationId = default
    )
    {
        return keyValues.LocateAndTryReleaseExclusiveLock(transactionId, key, durability, cancellationToken, coordinatorKey, operationId);
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
        CancellationToken cancellationToken,
        string coordinatorKey = "",
        TransactionOperationId operationId = default
    )
    {
        return keyValues.LocateAndTryReleaseExclusivePrefixLock(transactionId, prefixKey, durability, cancellationToken, coordinatorKey, operationId);
    }

    public Task<(KeyValueResponseType, HLCTimestamp HolderTransactionId)> LocateAndTryAcquireRangeLock(
        HLCTimestamp transactionId,
        string prefix,
        string? startKey, bool startInclusive,
        string? endKey,   bool endInclusive,
        int expiresMs,
        KeyValueDurability durability,
        RangeLockMode mode,
        CancellationToken cancellationToken,
        string coordinatorKey = "",
        TransactionOperationId operationId = default
    ) => keyValues.LocateAndTryAcquireRangeLock(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, expiresMs, durability, mode, cancellationToken, coordinatorKey, operationId);

    public Task<(KeyValueResponseType, HLCTimestamp HolderTransactionId)> LocateAndTryAcquireExclusiveRangeLock(
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
        CancellationToken cancellationToken,
        string coordinatorKey = "",
        TransactionOperationId operationId = default
    )
    {
        return keyValues.LocateAndTryReleaseExclusiveRangeLock(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, durability, cancellationToken, coordinatorKey, operationId);
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
        long routedGeneration = 0,
        string? recordAnchorKey = null,
        CoordinatorDecisionRecord? embeddedDecision = null
    )
    {
        return keyValues.LocateAndTryPrepareMutations(transactionId, commitId, key, durability, cancellationToken, routedGeneration, recordAnchorKey, embeddedDecision);
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
        CancellationToken cancellationToken,
        string? recordAnchorKey = null
    )
    {
        return keyValues.LocateAndTryPrepareManyMutations(transactionId, commitId, keys, cancellationToken, recordAnchorKey);
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
        CancellationToken cancellationToken,
        string coordinatorKey = "",
        TransactionOperationId operationId = default
    )
    {
        return keyValues.LocateAndGetByBucket(transactionId, prefixedKey, readTimestamp, durability, cancellationToken, coordinatorKey, operationId);
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
    public Task<(KeyValueResponseType, TransactionHandle)> LocateAndStartTransaction(KeyValueTransactionOptions options, CancellationToken cancellationToken)
    {
        return keyValues.LocateAndStartTransaction(options, cancellationToken);
    }

    /// <summary>
    /// Commits the transaction identified by <paramref name="handle"/>.
    /// </summary>
    /// <param name="handle">The handle returned by <see cref="LocateAndStartTransaction"/>.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task representing the asynchronous operation with the commit outcome.</returns>
    public Task<(KeyValueResponseType, string?)> LocateAndCommitTransaction(TransactionHandle handle, CancellationToken cancellationToken)
    {
        return keyValues.LocateAndCommitTransaction(handle, cancellationToken);
    }

    /// <summary>
    /// Rolls back the transaction identified by <paramref name="handle"/>.
    /// </summary>
    /// <param name="handle">The handle returned by <see cref="LocateAndStartTransaction"/>.</param>
    /// <param name="cancellationToken">A token to observe for cancellation requests.</param>
    /// <returns>A <see cref="KeyValueResponseType"/> indicating the result of the rollback operation.</returns>
    public Task<KeyValueResponseType> LocateAndRollbackTransaction(TransactionHandle handle, CancellationToken cancellationToken)
    {
        return keyValues.LocateAndRollbackTransaction(handle, cancellationToken);
    }

    public Task<(OperationRegistrationOutcome outcome, KeyValueResponseType cachedType, long cachedRevision, HLCTimestamp cachedTimestamp, string? recordAnchorKey)> LocateAndBeginOperation(string coordinatorKey, HLCTimestamp transactionId, TransactionOperationId operationId, OperationKind kind, byte[]? payloadDigest, CancellationToken cancellationToken)
    {
        return keyValues.LocateAndBeginOperation(coordinatorKey, transactionId, operationId, kind, payloadDigest, cancellationToken);
    }

    public Task<string?> LocateAndCompleteOperation(string coordinatorKey, HLCTimestamp transactionId, TransactionOperationId operationId, OperationCompletionPayload payload, CancellationToken cancellationToken)
    {
        return keyValues.LocateAndCompleteOperation(coordinatorKey, transactionId, operationId, payload, cancellationToken);
    }

    public (OperationRegistrationOutcome outcome, KeyValueResponseType cachedType, long cachedRevision, HLCTimestamp cachedTimestamp, string? recordAnchorKey) BeginOperation(HLCTimestamp transactionId, TransactionOperationId operationId, OperationKind kind, byte[]? payloadDigest)
    {
        return keyValues.BeginOperation(transactionId, operationId, kind, payloadDigest);
    }

    public string? CompleteOperation(HLCTimestamp transactionId, TransactionOperationId operationId, OperationCompletionPayload payload)
    {
        return keyValues.CompleteOperation(transactionId, operationId, payload);
    }

    public Task<TransactionWorkingSet?> LocateAndGetTransactionWorkingSet(string coordinatorKey, HLCTimestamp transactionId, CancellationToken cancellationToken)
    {
        return keyValues.LocateAndGetTransactionWorkingSet(coordinatorKey, transactionId, cancellationToken);
    }

    public Task<(KeyValueResponseType, TransactionWorkingSet?)> LocateAndCloseTransaction(string coordinatorKey, HLCTimestamp transactionId, CancellationToken cancellationToken)
    {
        return keyValues.LocateAndCloseTransaction(coordinatorKey, transactionId, cancellationToken);
    }

    public TransactionWorkingSet? GetTransactionWorkingSet(HLCTimestamp transactionId)
    {
        return keyValues.GetTransactionWorkingSet(transactionId);
    }

    public Task<(KeyValueResponseType, TransactionWorkingSet?)> CloseTransaction(HLCTimestamp transactionId, CancellationToken cancellationToken)
    {
        return keyValues.CloseTransaction(transactionId, cancellationToken);
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
        HLCTimestamp readTimestamp,
        List<(string key, long revision, KeyValueDurability durability)> keys
    )
    {
        return keyValues.TryGetManyValues(transactionId, readTimestamp, keys);
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
        HLCTimestamp readTimestamp,
        List<(string key, long revision, KeyValueDurability durability)> keys
    )
    {
        return keyValues.TryExistsManyValues(transactionId, readTimestamp, keys);
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
    public Task<(KeyValueResponseType, string, KeyValueDurability, HLCTimestamp HolderTransactionId)> TryAcquireExclusiveLock(
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

    public Task<(KeyValueResponseType, HLCTimestamp HolderTransactionId)> TryAcquireExclusiveRangeLock(HLCTimestamp transactionId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive, int expiresMs, KeyValueDurability durability)
        => keyValues.TryAcquireRangeLock(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, expiresMs, durability, RangeLockMode.Exclusive);

    public Task<(KeyValueResponseType, HLCTimestamp HolderTransactionId)> TryAcquireRangeLock(HLCTimestamp transactionId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive, int expiresMs, KeyValueDurability durability, RangeLockMode mode)
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
        long routedGeneration = 0,
        string? recordAnchorKey = null,
        CoordinatorDecisionRecord? embeddedDecision = null
    )
    {
        return keyValues.TryPrepareMutations(transactionId, commitId, key, durability, routedGeneration, recordAnchorKey, embeddedDecision);
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
    public Task<(KeyValueResponseType, TransactionHandle)> StartTransaction(KeyValueTransactionOptions options)
    {
        return keyValues.StartTransaction(options);
    }

    /// <summary>
    /// Reads the decision-durability policy recorded for an active interactive session, or null when no
    /// active session with that id exists. Reflects exactly what Begin captured from the caller's options.
    /// </summary>
    internal DecisionDurability? GetRecordedDecisionDurability(HLCTimestamp transactionId)
    {
        return keyValues.GetRecordedDecisionDurability(transactionId);
    }

    /// <summary>Node-local persistent-participant completion receipts. Diagnostic/test access.</summary>
    internal CompletionReceiptStore CompletionReceiptStore => keyValues.CompletionReceiptStore;

    /// <summary>Partition-scoped durable coordinator decision records. Diagnostic/test access.</summary>
    internal CoordinatorDecisionStore CoordinatorDecisionStore => keyValues.CoordinatorDecisionStore;

    /// <summary>
    /// Commits the transaction identified by <paramref name="handle"/>.
    /// </summary>
    /// <param name="handle">The handle returned by <see cref="StartTransaction"/>.</param>
    /// <returns>A task containing the commit outcome.</returns>
    public Task<(KeyValueResponseType, string?)> CommitTransaction(TransactionHandle handle)
    {
        return keyValues.CommitTransaction(handle);
    }

    /// <summary>
    /// Rolls back the transaction identified by <paramref name="handle"/>.
    /// </summary>
    /// <param name="handle">The handle returned by <see cref="StartTransaction"/>.</param>
    /// <returns>A task containing the rollback outcome.</returns>
    public Task<KeyValueResponseType> RollbackTransaction(TransactionHandle handle)
    {
        return keyValues.RollbackTransaction(handle);
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
            ReplicationTypes.SnapshotFloor => await keyValues.OnLogRestored(partitionId, log),
            ReplicationTypes.CoordinatorDecision => await keyValues.OnLogRestored(partitionId, log),
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
            ReplicationTypes.SnapshotFloor => await keyValues.OnReplicationReceived(partitionId, log),
            ReplicationTypes.CoordinatorDecision => await keyValues.OnReplicationReceived(partitionId, log),
            ReplicationTypes.Locks => await locks.OnReplicationReceived(partitionId, log),
            _ => true
        };
    }

    public void OnReplicationError(int partitionId, RaftLog log)
    {
        locks.OnReplicationError(log);
        keyValues.OnReplicationError(log);
    }

    public Task<bool> OnLeaderChanged(int partitionId, string node) =>
        keyValues.OnLeaderChanged(partitionId, node);

    internal Task RunCollectOnAllInstancesAsync() => keyValues.RunCollectOnAllInstancesAsync();

    /// <summary>
    /// Removes all snapshot holds whose lease has expired. Exposed for tests so they can trigger
    /// a purge cycle without waiting for the periodic <see cref="SnapshotFloorReaperActor"/> timer.
    /// </summary>
    internal Task<int> PurgeExpiredSnapshotHoldsAsync(CancellationToken ct = default) =>
        keyValues.PurgeExpiredSnapshotHoldsAsync(ct);

    /// <summary>The replicated range-descriptor map.</summary>
    internal RangeMapStore RangeMapStore => keyValues.RangeMapStore;

    /// <summary>The replicated, refcounted, leased MVCC snapshot-floor registry.</summary>
    internal SnapshotFloorStore SnapshotFloorStore => keyValues.SnapshotFloorStore;

    /// <summary>
    /// Direct access to the <see cref="BackgroundWriterActor"/> instance for test injection
    /// (e.g., setting <see cref="BackgroundWriterActor.BeforePruneSampleHook"/>). Null until
    /// the first message is processed by the actor.
    /// </summary>
    internal BackgroundWriterActor? BackgroundWriterActor =>
        backgroundWriter.Runner.Actor as BackgroundWriterActor;

    /// <summary>The per-node key-space routing registry.</summary>
    internal KeySpaceRegistry KeySpaceRegistry => keyValues.KeySpaceRegistry;

    /// <summary>The quiesce store — for test inspection only.</summary>
    internal RangeQuiesceStore RangeQuiesceStore => keyValues.RangeQuiesceStore;

    /// <summary>
    /// Exposes the KeyValuesManager for in-process test inspection of accounting state.
    /// Not part of the production API surface.
    /// </summary>
    internal KeyValuesManager KeyValues => keyValues;

    /// <inheritdoc/>
    public void RegisterKeyRange(string keySpace) => keyValues.KeySpaceRegistry.RegisterKeyRange(keySpace);

    /// <inheritdoc/>
    public Task<bool> RegisterKeyRangeAsync(string keySpace, CancellationToken cancellationToken = default) =>
        keyValues.RegisterKeyRangeAsync(keySpace, cancellationToken);

    /// <inheritdoc/>
    public Task<bool> RemoveKeyRangeAsync(string keySpace, CancellationToken cancellationToken = default) =>
        keyValues.RemoveKeyRangeAsync(keySpace, cancellationToken);

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

    public Task ImportCompletionReceipts(IReadOnlyCollection<CompletionReceiptRecord> receipts)
    {
        keyValues.ImportCompletionReceipts(receipts);
        return Task.CompletedTask;
    }

    public Task ImportCoordinatorDecisions(IReadOnlyCollection<CoordinatorDecisionRecord> records)
    {
        keyValues.ImportCoordinatorDecisions(records);
        return Task.CompletedTask;
    }

    /// <summary>Resolves a key to its owning <c>(partitionId, generation)</c> (key-order router).</summary>
    internal (int PartitionId, long Generation) LocateRange(string key) => keyValues.LocateRange(key);

    /// <summary>The split-transaction executor.</summary>
    internal RangeSplitter RangeSplitter => keyValues.RangeSplitter;

    /// <summary>The auto-split trigger (exposed for regression tests of <c>ExecuteSplitAsync</c>).</summary>
    internal RangeSplitTrigger RangeSplitTrigger => keyValues.RangeSplitTrigger;

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
    /// Test seam: forces a split of the descriptor covering <paramref name="splitKey"/> at that
    /// exact key without requiring a pre-computed partition ID or threshold-sized data.
    /// Handles <c>ComputeNextPartitionId → CreatePartitionAsync → SplitAsync</c> internally.
    /// Pass <paramref name="duringQuiesce"/> to race an operation into the quiesce window.
    /// </summary>
    internal Task<SplitOutcome> ForceSplitAtKeyAsync(
        string keySpace,
        string splitKey,
        Func<Task>? duringQuiesce = null,
        CancellationToken ct = default) =>
        keyValues.ForceSplitAtKeyAsync(keySpace, splitKey, duringQuiesce, ct);

    /// <summary>
    /// Test seam for the multi-range bucket fan-out: <paramref name="beforeQuery"/> is called before each descriptor's paged query
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
    internal Task<(KeyValueResponseType, HLCTimestamp)> AcquireExclusiveRangeLockWithHook(
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

    /// <summary>
    /// Test seam: the register-remote range-lock acquire with a split-injection hook, so a test can
    /// assert how the generation fence interacts with the coordinator-owned working set (a fenced acquire
    /// must record no range descriptor).
    /// </summary>
    internal Task<(KeyValueResponseType, HLCTimestamp)> RegisterAndAcquireRangeLockWithHook(
        HLCTimestamp transactionId, string coordinatorKey, TransactionOperationId operationId, string prefix,
        string? startKey, bool startInclusive, string? endKey, bool endInclusive, int expiresMs,
        KeyValueDurability durability, RangeLockMode mode, Func<Task> afterSnapshot, CancellationToken cancellationToken
    ) => keyValues.RegisterAndAcquireRangeLockWithHook(
            transactionId, coordinatorKey, operationId, prefix, startKey, startInclusive, endKey, endInclusive,
            expiresMs, durability, mode, afterSnapshot, cancellationToken);

    // ── Backup / PITR ──────────────────────────────────────────────────────────────────────

    public bool IsBackupConfigured => backupService is not null;

    public Task<KahunaBackupInfo> TakeFullBackupAsync(CancellationToken ct = default) =>
        RequireBackupService().TakeFullAsync();

    public Task<KahunaBackupInfo> TakeIncrementalBackupAsync(Guid parentBackupId, CancellationToken ct = default) =>
        RequireBackupService().TakeIncrementalAsync(parentBackupId);

    public Task<KahunaBackupInfo> TakeCoordinatedBackupAsync(CancellationToken ct = default) =>
        RequireBackupService().TakeCoordinatedBackupAsync();

    public Task<IReadOnlyList<KahunaBackupInfo>> ListBackupsAsync(CancellationToken ct = default) =>
        Task.FromResult(RequireBackupService().ListBackups());

    public Task<IReadOnlyList<KahunaBackupInfo>> GetBackupChainAsync(Guid leafBackupId, CancellationToken ct = default) =>
        Task.FromResult(RequireBackupService().ResolveAndValidate(leafBackupId));

    public Task<KahunaRestoreResponse> RestoreToAsync(
        Guid leafBackupId,
        string targetDir,
        long targetTimeMs,
        CancellationToken ct = default)
    {
        HLCTimestamp targetTime = targetTimeMs > 0
            ? new HLCTimestamp(0, targetTimeMs, 0)
            : HLCTimestamp.Zero;
        KahunaRestoreResponse result = RequireBackupService().RestoreTo(leafBackupId, targetDir, targetTime);
        return Task.FromResult(result);
    }

    private BackupService RequireBackupService() =>
        backupService ?? throw new InvalidOperationException(
            "Backup is not configured on this node. Set BackupDir in configuration or --pitr-backup-dir.");

    // ── MVCC snapshot floor ─────────────────────────────────────────────────────────────────

    public Task<(KeyValueResponseType Type, string HoldId, HLCTimestamp LeaseExpiry)>
        LocateAndAcquireSnapshotHold(string holderId, HLCTimestamp timestamp, int leaseMs, CancellationToken ct) =>
        keyValues.AcquireSnapshotHold(holderId, timestamp, leaseMs, ct);

    public Task<(KeyValueResponseType Type, HLCTimestamp LeaseExpiry)>
        LocateAndRenewSnapshotHold(string holdId, int leaseMs, CancellationToken ct) =>
        keyValues.RenewSnapshotHold(holdId, leaseMs, ct);

    public Task<KeyValueResponseType>
        LocateAndReleaseSnapshotHold(string holdId, CancellationToken ct) =>
        keyValues.ReleaseSnapshotHold(holdId, ct);

    public Task<(HLCTimestamp EffectiveFloor, int LiveHolds)>
        GetSnapshotFloor(CancellationToken ct) =>
        keyValues.GetSnapshotFloor(ct);
}
