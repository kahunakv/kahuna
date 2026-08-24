using System.Diagnostics.CodeAnalysis;

using Nixie;

using Kommander;
using Kommander.WAL.IO;

using Kahuna.Server.Communication.Internode;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues.Logging;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.Persistence;
using Kahuna.Server.ScriptParser;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// Builds and wires every part of the key-value subsystem for one node: the replicated stores, the actor
/// rings, the write aggregator, the transaction coordinator and its admission gates, the resident-state
/// gauges, the collaborators, and the periodic checker actors.
///
/// <b>The statement order below is load-bearing.</b> The range map is built before anything that carries it
/// for the generation fence; the routers populate their instance lists as a side effect of spawning, so the
/// collector that reads those lists spawns after them; the durable-intent stores get their key/anchor
/// resolvers only once the locator exists; and the split/merge checker actors take their trigger as a
/// constructor argument, so they spawn only after the triggers are assigned — each under its own
/// configuration predicate, because spawning one unconditionally would start background work on a node that
/// opted out. Do not reorder.
/// </summary>
[SuppressMessage(
    "Design",
    "CA1001:Types that own disposable fields should be disposable",
    Justification = "This is a transient constructor-scoped helper. Every disposable field it builds is copied " +
                    "out into KeyValuesManager immediately after construction, and KeyValuesManager.Dispose() " +
                    "owns their teardown from that point on.")]
internal sealed class KeyValuesManagerBuilder
{
    // Wiring results, handed to the manager. Named as the manager names them so the sequence below is
    // exactly the sequence that used to run inside its constructor.
    internal readonly ActorSystem actorSystem;
    internal readonly IRaft raft;
    internal readonly IRaftReadScheduler backendReadScheduler;
    internal readonly IInterNodeCommunication interNodeCommunication;
    internal readonly IPersistenceBackend persistenceBackend;
    internal readonly IActorRef<BackgroundWriterActor, BackgroundWriteRequest> backgroundWriter;
    internal readonly ILogger<IKahuna> logger;
    internal readonly PartitionDurabilityTracker? durabilityTracker;

    internal readonly KeySpaceRegistry keySpaceRegistry = new();
    internal readonly KeyWriteFrequencyRegistry writeFrequencyRegistry = new();
    internal readonly DurableApplyResultLedger durableApplyResults = new();

    internal KeyValuesRuntime runtime = null!;
    internal IActorRef<ScriptParserEvicterActor, ScriptParserEvicterRequest> scriptParserEvicter = null!;
    internal RangeMapStore rangeMapStore = null!;
    internal PartitionDataEnumerator partitionDataEnumerator = null!;
    internal SnapshotFloorStore snapshotFloorStore = null!;
    internal CompletionReceiptStore completionReceiptStore = null!;
    internal TransactionRecordStore transactionRecordStore = null!;
    internal PreparedIntentStore preparedIntentStore = null!;
    internal PartitionStateTransfer partitionStateTransfer = null!;
    internal Writes.PartitionWriteAggregator writeAggregator = null!;
    internal KeyValueActorRouters routers = null!;
    internal SnapshotHoldService snapshotHolds = null!;
    internal IActorRef<KeyValueCollectorActor, KeyValueCollectorRequest> keyValueCollector = null!;
    internal TransactionPriorityOrderer sessionOrderer = null!;
    internal TransactionPriorityOrderer scriptOrderer = null!;
    internal TransactionCoordinator txCoordinator = null!;
    internal ScriptTransactionExecutor scriptExecutor = null!;
    internal System.Diagnostics.Metrics.Meter durableGaugeMeter = null!;
    internal System.Diagnostics.Metrics.Meter admissionGaugeMeter = null!;
    internal KeyValueLocator locator = null!;
    internal IActorRef<PreparedIntentRecoveryActor, PreparedIntentRecoveryRequest> preparedIntentRecovery = null!;
    internal KeyValueRestorer restorer = null!;
    internal KeyValueReplicator replicator = null!;
    internal KeyValueReplicationDispatcher replicationDispatcher = null!;
    internal Writes.DurableReplicationGateway durableReplication = null!;
    internal RangeStateTransferService rangeStateTransfer = null!;
    internal LocalKeyValueOperations localKeyValues = null!;
    internal LocalKeyValueReadOperations localKeyValueReads = null!;
    internal LocalLockOperations localLocks = null!;
    internal LocalMutationTicketOperations localMutationTickets = null!;
    internal LocalScanOperations localScans = null!;
    internal DurableMaintenanceService durableMaintenance = null!;
    internal OperationRegistrar operationRegistrar = null!;
    internal RoutedWriteOperations routedWrites = null!;
    internal RoutedReadOperations routedReads = null!;
    internal RoutedLockOperations routedLocks = null!;
    internal RoutedScanOperations routedScans = null!;
    internal TransactionSessionFacade transactionSessions = null!;
    internal NodeMaintenanceService nodeMaintenance = null!;
    internal KvStateMachineTransfer kvStateMachineTransfer = null!;
    internal MetaSystemStateTransfer metaSystemStateTransfer = null!;
    internal RangeSplitter rangeSplitter = null!;
    internal RangeSplitTrigger rangeSplitTrigger = null!;
    internal RangeMerger rangeMerger = null!;
    internal RangeMergeTrigger rangeMergeTrigger = null!;
    internal KeySpaceAdminService keySpaceAdmin = null!;

    internal KeyValuesManagerBuilder(
        KeyValuesManager manager,
        ActorSystem actorSystem,
        IRaft raft,
        IRaftReadScheduler backendReadScheduler,
        IInterNodeCommunication interNodeCommunication,
        IPersistenceBackend persistenceBackend,
        IActorRef<BackgroundWriterActor, BackgroundWriteRequest> backgroundWriter,
        KahunaConfiguration configuration,
        ILogger<IKahuna> logger,
        SnapshotFloorStore? externalFloorStore,
        CompletionReceiptStore? externalReceiptStore,
        TransactionRecordStore? externalRecordStore,
        PreparedIntentStore? externalIntentStore,
        Func<Writes.IPartitionBatchExecutor, Writes.IPartitionBatchExecutor>? writeBatchExecutorDecorator,
        PartitionDurabilityTracker? durabilityTracker)
    {
        this.actorSystem = actorSystem;
        this.raft = raft;
        this.backendReadScheduler = backendReadScheduler;
        this.interNodeCommunication = interNodeCommunication;
        this.persistenceBackend = persistenceBackend;
        this.backgroundWriter = backgroundWriter;
        this.logger = logger;
        this.durabilityTracker = durabilityTracker;



        scriptParserEvicter = actorSystem.Spawn<ScriptParserEvicterActor, ScriptParserEvicterRequest>("script-parser-evicter", logger);

        // Construct the range-descriptor map before the proposal router — the proposal actors carry it
        // for the generation fence.
        rangeMapStore = new(raft, configuration.StoragePath, configuration.StorageRevision, logger);

        // Per-partition data ownership over the node-global backend, for whole-partition snapshot
        // export and un-host purging. Classifies against the committed range map and the hash pool.
        partitionDataEnumerator = new(persistenceBackend, () => rangeMapStore.Current, raft.Configuration.InitialPartitions);

        // Snapshot-floor registry: replicated on the same meta partition as the range map.
        // When an external store is provided (shared with BackgroundWriterActor) use it directly;
        // otherwise create a new instance owned by this manager.
        snapshotFloorStore = externalFloorStore ?? new(raft, configuration.StoragePath, configuration.StorageRevision, logger);

        // Completion-receipt store: shared with the BackgroundWriterActor when provided (so the writer
        // snapshots the receipts this manager records), otherwise owned locally. The durable overload
        // reloads any persisted receipts so a re-commit after a cold restart still answers Committed.
        completionReceiptStore = externalReceiptStore ?? new CompletionReceiptStore(configuration.StoragePath, configuration.StorageRevision, logger);

        // Sync the per-node key-space registry with any descriptors loaded from the durable snapshot.
        // RangeMapStore.LoadFromDisk (called above) restores the range map but does not touch the
        // registry — they are separate objects. Without this call, a node that restarts with a
        // non-empty snapshot treats all key spaces as hash-routed (the default) until the first
        // live RegisterKeyRangeAsync call, causing routing mismatches in the 2PC prepare path.
        KeyValueReplicationDispatcher.SyncKeySpaceRegistryFromRangeMap(rangeMapStore, keySpaceRegistry);

        // Durable-intent 2PC stores share the same per-partition snapshot lifecycle; their key/anchor → partition
        // resolvers are attached below once the locator exists. Shared with the BackgroundWriterActor when provided
        // (so the writer gates each partition's WAL checkpoint on their durable snapshots), otherwise owned locally.
        transactionRecordStore = externalRecordStore ?? new TransactionRecordStore(configuration.StoragePath, configuration.StorageRevision, logger);
        preparedIntentStore = externalIntentStore ?? new PreparedIntentStore(configuration.StoragePath, configuration.StorageRevision, logger);

        // The staged-base fence's memory horizon comes from configuration; idempotent, so re-applying it to a
        // shared external store is harmless.
        preparedIntentStore.ConfigureStagedBaseFence(configuration.StagedBaseFenceRetentionMs);

        // A one-phase bundled commit is legal only if its own prepare — earlier in the same atomic batch — took
        // ownership of every written key; the record store validates that against the live intent set at apply
        // time. Both stores apply on the same ordered per-partition path, so the lookup is deterministic at the
        // commit transition's log position on every replica.
        transactionRecordStore.AttachBundledPrepareProbe((intentKey, transactionId, epoch) =>
            preparedIntentStore.Get(intentKey) is { } liveIntent &&
            liveIntent.TransactionId == transactionId && liveIntent.Epoch == epoch);

        // Whole-partition state transfer: seeds a replica whose needed log entries were compacted.
        // Drains the background writer before exporting so the physical-family scan reflects every
        // applied entry (the export contract requires at least everything applied at the boundary).
        partitionStateTransfer = new(
            partitionDataEnumerator,
            persistenceBackend,
            completionReceiptStore,
            transactionRecordStore,
            preparedIntentStore,
            () => rangeMapStore.Current,
            raft.Configuration.InitialPartitions,
            () =>
            {
                TaskCompletionSource<bool> flushed = new(TaskCreationOptions.RunContinuationsAsynchronously);
                backgroundWriter.Send(new(BackgroundWriteType.FlushAndNotify, flushed));
                return flushed.Task;
            },
            configuration.StoragePath,
            configuration.StorageRevision,
            logger);

        // Mutations below a snapshot boundary reach this node only through the whole-partition
        // install — the replicators never see them, so no cache-coherence apply advances a resident
        // actor entry. Registering the eviction here makes every install drop the key-value
        // residents of the installed partition; a stale resident entry would otherwise be served
        // with precedence over the freshly installed backend rows. (The lock residents are
        // registered by the composition root, which owns both managers.)
        partitionStateTransfer.AddResidentStateInvalidationHook(manager.EvictPartitionEntriesAsync);

        Writes.IPartitionBatchExecutor realBatchExecutor = new Writes.RaftPartitionBatchExecutor(raft);

        writeAggregator = new Writes.PartitionWriteAggregator(
            actorSystem,
            writeBatchExecutorDecorator?.Invoke(realBatchExecutor) ?? realBatchExecutor,
            new Writes.PartitionWriteAggregatorOptions
            {
                LingerMs = configuration.KeyValueWriteLingerMs,
                MaxBatchItems = configuration.KeyValueWriteMaxBatchItems,
                MaxBatchBytes = configuration.KeyValueWriteMaxBatchBytes,
                MaxQueuedItemsPerPartition = configuration.KeyValueWriteMaxQueuedItemsPerPartition,
                MaxQueuedBytesPerPartition = configuration.KeyValueWriteMaxQueuedBytesPerPartition,
                TerminalReserveItemsPerPartition = configuration.KeyValueWriteTerminalReserveItemsPerPartition,
                TerminalReserveBytesPerPartition = configuration.KeyValueWriteTerminalReserveBytesPerPartition,
                MaxQueuedItemsGlobal = configuration.KeyValueWriteMaxQueuedItemsGlobal,
                MaxQueuedBytesGlobal = configuration.KeyValueWriteMaxQueuedBytesGlobal,
                TerminalReserveItemsGlobal = configuration.KeyValueWriteTerminalReserveItemsGlobal,
                TerminalReserveBytesGlobal = configuration.KeyValueWriteTerminalReserveBytesGlobal,
                MaxOperationBytes = configuration.KeyValueWriteMaxOperationBytes,
                BatchExecutionTimeoutMs = configuration.KeyValueWriteBatchExecutionTimeoutMs,
                MaxQueueDelayMs = configuration.KeyValueWriteMaxQueueDelayMs,
                AggregatorInboxSize = configuration.MaxKeyValueWriteAggregatorInboxSize,
                LaneCount = Math.Max(1, configuration.KeyValueWorkers)
            },
            new Writes.RangeMapWriteFence(keySpaceRegistry, rangeMapStore),
            logger
        );

        // Everything the collaborators share, gathered once. The locator slot is filled below, after the
        // locator itself is built.
        runtime = new()
        {
            ActorSystem = actorSystem,
            Raft = raft,
            BackendReadScheduler = backendReadScheduler,
            InterNodeCommunication = interNodeCommunication,
            PersistenceBackend = persistenceBackend,
            BackgroundWriter = backgroundWriter,
            Configuration = configuration,
            Logger = logger,
            DurabilityTracker = durabilityTracker,
            KeySpaceRegistry = keySpaceRegistry,
            WriteFrequencyRegistry = writeFrequencyRegistry,
            DurableApplyResults = durableApplyResults,
            RangeMapStore = rangeMapStore,
            PartitionDataEnumerator = partitionDataEnumerator,
            PartitionStateTransfer = partitionStateTransfer,
            SnapshotFloorStore = snapshotFloorStore,
            CompletionReceiptStore = completionReceiptStore,
            TransactionRecordStore = transactionRecordStore,
            PreparedIntentStore = preparedIntentStore,
            WriteAggregator = writeAggregator
        };

        routers = new(runtime);
        runtime.Routers = routers;
        snapshotHolds = new(runtime);

        keyValueCollector = actorSystem.Spawn<KeyValueCollectorActor, KeyValueCollectorRequest>(
            "keyvalue-collector",
            routers.EphemeralInstances,
            routers.PersistentInstances,
            configuration,
            logger
        );

        // Two independent admission gates, because the two transaction shapes hold a slot for very different
        // durations: a script transaction for its own bounded execution, an interactive session for as long as
        // its client stays connected. Both are in-memory and per-node — a restart legitimately drops waiters
        // that had not started yet.
        sessionOrderer = new(
            configuration.MaxConcurrentSessions,
            configuration.TransactionPriorityReservedSlots,
            configuration.TransactionPriorityAgingThreshold,
            configuration.TransactionPriorityMaxQueued);

        scriptOrderer = new(
            configuration.MaxConcurrentTransactions,
            configuration.TransactionPriorityReservedSlots,
            configuration.TransactionPriorityAgingThreshold,
            configuration.TransactionPriorityMaxQueued);

        txCoordinator = new(manager, configuration, raft, logger, sessionOrderer);
        scriptExecutor = new(manager, configuration, raft, logger, txCoordinator, scriptOrderer);

        // Resident-state gauges for the durable-2PC stores + the admission counter, on an instance-owned meter
        // disposed on teardown so a disposed node's stores are not kept reachable by gauge callbacks.
        durableGaugeMeter = Transactions.DurableTransactionMetrics.RegisterGauges(
            () => transactionRecordStore.Count,
            () => completionReceiptStore.Count,
            () => preparedIntentStore.Count,
            () => preparedIntentStore.TotalBytes,
            () => txCoordinator.OutstandingDurableCount);

        // Admission-gate gauges on their own instance-owned meter, for the same reason: a disposed node's
        // orderers must not stay reachable through gauge callbacks.
        admissionGaugeMeter = Transactions.TransactionPriorityMetrics.RegisterGauges(scriptOrderer, sessionOrderer);

        // Periodic reaper for interactive transaction sessions abandoned without commit/rollback.
        actorSystem.Spawn<TransactionReaperActor, TransactionReaperRequest>(
            "transaction-reaper",
            txCoordinator,
            configuration,
            logger
        );

        // Periodic reaper for snapshot holds whose lease expired without an explicit release.
        actorSystem.Spawn<SnapshotFloorReaperActor, SnapshotFloorReaperRequest>(
            "snapshot-floor-reaper",
            snapshotFloorStore,
            configuration,
            logger
        );

        locator = new(manager, configuration, raft, interNodeCommunication, keySpaceRegistry, logger);
        runtime.Locator = locator;

        // Now that the locator exists, wire the anchor/key → data-partition resolvers the durable-intent stores
        // fence their coordinator-driven mutations on.
        transactionRecordStore.AttachAnchorResolver(locator.LocateRange);
        preparedIntentStore.AttachPartitionResolver(key => locator.LocateRange(key).PartitionId);

        // The receipt store's per-partition checkpoint snapshot routes each receipt by its key the same way.
        completionReceiptStore.AttachPartitionResolver(key => locator.LocateRange(key).PartitionId);

        preparedIntentRecovery = actorSystem.Spawn<PreparedIntentRecoveryActor, PreparedIntentRecoveryRequest>(
            "prepared-intent-recovery",
            manager,
            configuration,
            logger
        );

        // The unflushed-writes overlay travels with the decorated backend; the replicator and restorer
        // record queued writes into it synchronously so reads observe them before the flush lands. A
        // raw (undecorated) backend — some direct-construction tests — yields null and the recording
        // becomes a no-op, restoring pre-overlay behavior.
        UnflushedKeyValueWritesIndex? unflushedWrites = (persistenceBackend as UnflushedOverlayPersistenceBackend)?.UnflushedWrites;

        restorer = new(backgroundWriter, raft, completionReceiptStore, logger, unflushedWrites, durabilityTracker);
        replicator = new(backgroundWriter, routers.Persistent, raft, writeFrequencyRegistry, keySpaceRegistry, completionReceiptStore, logger, unflushedWrites, durabilityTracker);
        replicationDispatcher = new(runtime, restorer, replicator);
        durableReplication = new(runtime, replicator);
        runtime.DurableReplication = durableReplication;
        rangeStateTransfer = new(runtime, manager);
        localKeyValues = new(runtime);
        localKeyValueReads = new(runtime, localKeyValues);
        localLocks = new(runtime, localKeyValues);
        localMutationTickets = new(runtime);
        localScans = new(runtime, localKeyValues);
        durableMaintenance = new(runtime, manager, txCoordinator, rangeStateTransfer, localLocks);
        operationRegistrar = new(runtime, txCoordinator);
        routedWrites = new(runtime, operationRegistrar);
        routedReads = new(runtime, operationRegistrar);
        routedLocks = new(runtime, operationRegistrar);
        routedScans = new(runtime, operationRegistrar);
        transactionSessions = new(runtime, txCoordinator, scriptExecutor);
        nodeMaintenance = new(runtime);
        kvStateMachineTransfer = new(manager, persistenceBackend, logger);

        // Whole-partition state transfer for the meta partition (id 0). Repairs a node below the
        // meta WAL compaction floor by shipping the range map + snapshot-floor holds together —
        // required because the hold registry now replicates deltas that cannot be rebuilt from
        // surviving log entries once compacted.
        metaSystemStateTransfer = new(rangeMapStore, snapshotFloorStore);

        // RangeSplitter and RangeSplitTrigger both depend on this (KeyValuesManager) being fully
        // constructed, so we assign after all other fields are set.
        rangeSplitter     = new(raft, rangeMapStore, manager, logger);
        rangeSplitTrigger = new(raft, rangeMapStore, rangeSplitter, manager, writeFrequencyRegistry, configuration, logger);
        rangeMerger       = new(raft, rangeMapStore, manager, logger);
        rangeMergeTrigger = new(raft, rangeMapStore, rangeMerger, writeFrequencyRegistry, configuration, logger);

        keySpaceAdmin = new(runtime, manager, rangeSplitter, rangeSplitTrigger, rangeMerger, rangeMergeTrigger);

        // Periodic split checker: fires on every CollectionInterval and calls TriggerAsync.
        // Only executes the actual split work when this node is the dual-leader (guard is inside
        // RangeSplitTrigger.TriggerAsync); on all other nodes it returns 0 immediately.
        // Spawns when either the count-based or the load-based trigger is enabled.
        if (configuration.RangeSplitThreshold > 0 || configuration.RangeSplitLoadThreshold > 0)
        {
            actorSystem.Spawn<RangeSplitCheckerActor, RangeSplitCheckerRequest>(
                "range-split-checker",
                rangeSplitTrigger,
                configuration,
                logger
            );
        }

        // Fast load-poll: fires at RangeSplitLoadPollInterval to evaluate the load predicate
        // and debounce. Decoupled from the slow count-check cadence so sub-CollectionInterval
        // windows can be measured. Spawned only when the load branch is enabled.
        if (configuration.RangeSplitLoadThreshold > 0)
        {
            actorSystem.Spawn<RangeSplitLoadCheckerActor, RangeSplitLoadCheckerRequest>(
                "range-split-load-checker",
                rangeSplitTrigger,
                configuration,
                logger
            );
        }

        // Periodic merge checker: same dual-leader guard inside RangeMergeTrigger.TriggerAsync.
        if (configuration.RangeMergeMinSize > 0)
        {
            actorSystem.Spawn<RangeMergeCheckerActor, RangeMergeCheckerRequest>(
                "range-merge-checker",
                rangeMergeTrigger,
                configuration,
                logger
            );
        }
    }
}
