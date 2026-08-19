
using Nixie;
using Kommander;
using Kommander.WAL;
using Kommander.WAL.IO;
using Microsoft.Extensions.Logging.Abstractions;
using Kahuna.Server.Communication.Internode;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.Locks;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Server.Sequencer;
using Writes = Kahuna.Server.KeyValues.Writes;

namespace Kahuna.Server.Composition;

/// <summary>
/// Builds the object graph of a single Kahuna node: the persistence backend and its
/// committed-but-unflushed overlay, the backend I/O schedulers, the stores shared between the
/// background writer and the key-value layer, the subsystem managers, and the Raft state-transfer
/// registrations.
///
/// <para>
/// <b>What this deliberately does not do:</b> it never starts the I/O schedulers. They allocate
/// worker threads on <c>Start</c>, so starting them is the very last thing the manager's constructor
/// does — a failure anywhere in construction must not leak threads.
/// </para>
/// </summary>
internal static class KahunaNodeComposer
{
    /// <summary>
    /// Creates the raw persistence backend named by configuration. The result is <b>unwrapped</b>:
    /// <see cref="Build"/> applies the committed-but-unflushed overlay, and it is the single place
    /// that does so — wrapping twice would give a node two independent unflushed-write indexes, and
    /// a read that missed the live one would answer DoesNotExist for a durably committed key.
    /// </summary>
    internal static IPersistenceBackend CreateBackend(KahunaConfiguration configuration, ILogger<IKahuna> logger, RocksDbSharedResources? sharedResources)
    {
        return configuration.Storage switch
        {
            "rocksdb" => new RocksDbPersistenceBackend(configuration.StoragePath, configuration.StorageRevision, sharedResources, configuration.RocksDbDirectReads, configuration.RocksDbStatistics, logger),
            "sqlite" => new SqlitePersistenceBackend(configuration.StoragePath, configuration.StorageRevision, logger),
            "memory" => new MemoryPersistenceBackend(),
            _ => throw new KahunaServerException("Invalid storage type: " + configuration.Storage)
        };
    }

    /// <summary>
    /// Builds the node's subsystems over <paramref name="backend"/> and registers this node's
    /// state-transfer hooks with Raft.
    /// </summary>
    internal static KahunaNodeComponents Build(
        ActorSystem actorSystem,
        IRaft raft,
        KahunaConfiguration configuration,
        IInterNodeCommunication interNodeCommunication,
        IPersistenceBackend backend,
        ILogger<IKahuna> logger,
        ILogger<IRaft>? raftLogger,
        Func<Writes.IPartitionBatchExecutor, Writes.IPartitionBatchExecutor>? writeBatchExecutorDecorator)
    {
        // Overlay of committed-but-unflushed key-value writes: between a Raft apply and the periodic
        // background flush the raw backend is behind the node's commit frontier, so a cache-missing
        // read (most visibly on a freshly promoted partition leader) would answer DoesNotExist for a
        // durably committed key. Wrapping here puts every consumer — actors, scans, the background
        // writer, PITR — behind the overlay uniformly.
        IPersistenceBackend persistenceBackend = new UnflushedOverlayPersistenceBackend(
            backend, new UnflushedKeyValueWritesIndex(), new UnflushedLockWritesIndex());

        // The scheduler only logs its own defensive errors through this logger; production hosts pass a real
        // ILogger<IRaft>, while test harnesses that omit it get a silent sink.
        raftLogger ??= NullLogger<IRaft>.Instance;

        // Reject scheduler configs that would silently break the data plane rather than discovering it at
        // runtime: a non-positive queue depth makes every enqueue fail back-pressure (all backend I/O
        // disabled), and a non-positive thread count would auto-expand the pool to the processor count —
        // fine for the read pool but a surprise for the writer pool, which must stay small and explicit.
        if (configuration.BackendReadQueueDepth <= 0)
            throw new KahunaServerException(
                $"BackendReadQueueDepth must be positive; {configuration.BackendReadQueueDepth} would reject all backend I/O.");
        if (configuration.BackendReadIOThreads <= 0)
            throw new KahunaServerException(
                $"BackendReadIOThreads must be positive; got {configuration.BackendReadIOThreads}.");
        if (configuration.BackendWriteIOThreads <= 0)
            throw new KahunaServerException(
                $"BackendWriteIOThreads must be positive; got {configuration.BackendWriteIOThreads} (0/negative would auto-expand the writer pool to the processor count).");

        // Kahuna-owned backend I/O schedulers, kept off Kommander's WAL read pool. Created here but Started
        // only at the very end of the manager's construction: if any later step throws, the schedulers were
        // never started, so no worker threads leak (the FairReadScheduler allocates threads on Start, not
        // construction). They are stopped/disposed in the manager's Dispose — which runs after the actor
        // system drains, so in-flight backend I/O completes rather than faulting on a scheduler that Raft
        // teardown already stopped.
        // Concurrent per-partition dispatch: KV persistence reads are mutually independent (a dirty
        // in-memory entry is never served from disk, so disk is only read when it is authoritative),
        // and with the standalone default of one Raft partition the scheduler's single-flight
        // invariant otherwise serializes every backend read in the node onto one thread at a time.
        FairReadScheduler backendReadScheduler = new(raftLogger, configuration.BackendReadIOThreads, configuration.BackendReadQueueDepth, concurrentPerPartition: true);
        FairReadScheduler backendWriteScheduler = new(raftLogger, configuration.BackendWriteIOThreads, configuration.BackendReadQueueDepth);

        SnapshotFloorStore snapshotFloorStore = new(raft, configuration.StoragePath, configuration.StorageRevision, logger);

        // One completion-receipt store shared between the background writer (which snapshots it durably at
        // checkpoint time) and the key-value layer (which records and consults receipts), mirroring how the
        // snapshot-floor store is shared. The writer is spawned before the KeyValuesManager exists, so the
        // single instance is created here and injected into both.
        CompletionReceiptStore completionReceiptStore = new(configuration.StoragePath, configuration.StorageRevision, logger);

        // Durable-intent 2PC stores are likewise shared between the background writer (which snapshots them durably
        // at checkpoint time, before the WAL retention floor advances past their delta log entries) and the
        // key-value layer (which applies and reads them). Created here for the same reason as the receipt store:
        // the writer is spawned before the KeyValuesManager exists.
        TransactionRecordStore transactionRecordStore = new(configuration.StoragePath, configuration.StorageRevision, logger);
        PreparedIntentStore preparedIntentStore = new(configuration.StoragePath, configuration.StorageRevision, logger);

        // Late-bound bridge so the background writer can acknowledge flushes back to the key-value
        // layer; the writer is spawned before the KeyValuesManager exists, so it is wired below.
        FlushNotificationSink flushNotificationSink = new();

        // Per-partition application-durability floor: every committed WAL entry registers here at
        // its ordered consumer apply and resolves when its durable artifact (flushed row or store
        // snapshot) lands. The provider answers Kommander's replay/compaction floor queries; hosts
        // wire it into RaftConfiguration.ApplicationDurabilityProvider before the node joins.
        PartitionDurabilityTracker durabilityTracker = new();
        KahunaDurabilityProvider durabilityProvider = new(durabilityTracker, persistenceBackend);

        IActorRef<BackgroundWriterActor, BackgroundWriteRequest> backgroundWriter = actorSystem.Spawn<BackgroundWriterActor, BackgroundWriteRequest>(
            "background-writer",
            raft,
            backendWriteScheduler,
            persistenceBackend,
            snapshotFloorStore,
            completionReceiptStore,
            transactionRecordStore,
            preparedIntentStore,
            configuration,
            logger,
            flushNotificationSink,
            durabilityTracker
        );

        LockManager locks = new(actorSystem, raft, backendReadScheduler, interNodeCommunication, persistenceBackend, backgroundWriter, configuration, logger, durabilityTracker);
        KeyValuesManager keyValues = new(actorSystem, raft, backendReadScheduler, interNodeCommunication, persistenceBackend, backgroundWriter, configuration, logger, snapshotFloorStore, completionReceiptStore, transactionRecordStore, preparedIntentStore, writeBatchExecutorDecorator, durabilityTracker);

        // Now that the key-value router exists, route flush acknowledgements to the owning actor so
        // it can advance FlushedRevision (making committed-but-unflushed entries eligible for eviction).
        flushNotificationSink.OnKeyValueFlushed = keyValues.NotifyFlushed;

        SequencerManager sequencer = new(actorSystem, raft, interNodeCommunication, keyValues, configuration, logger);

        // Register the key-range data-movement hook once, here, so every host (embedded,
        // server, tests) gets it uniformly without reaching across the internal API boundary.
        raft.RegisterStateMachineTransfer(keyValues.KvStateMachineTransfer);

        // Register the meta-partition (id 0) whole-state hook so a node that falls below the meta
        // WAL compaction floor is repaired with both the range map and the snapshot-floor holds.
        // Required now that the hold registry replicates deltas, which cannot be reconstructed from
        // surviving log entries once compacted.
        raft.RegisterSystemStateTransfer(keyValues.MetaSystemStateTransfer);

        // Register the whole-partition state transfer so a replica added by the placement planner
        // can be seeded even after the partition's WAL has been compacted below what it needs.
        // The leader-side catch-up path prefers this over the range transfer's boundless-plan
        // export (which stays unsupported by design).
        raft.RegisterPartitionStateTransfer(keyValues.PartitionStateTransfer);

        // A snapshot install must also drop the lock actors' resident leases for the installed
        // partition (the key-value residents are registered inside the manager). A resident lease
        // that predates the install is trusted unconditionally by the grant path — it would mint
        // fencing tokens below the installed high-water mark on the next leader promotion,
        // regressing and reusing tokens already granted by other replicas.
        keyValues.PartitionStateTransfer.AddResidentStateInvalidationHook(locks.EvictPartitionLocksAsync);

        return new KahunaNodeComponents(
            persistenceBackend,
            backendReadScheduler,
            backendWriteScheduler,
            durabilityProvider,
            backgroundWriter,
            locks,
            keyValues,
            sequencer);
    }
}
