
using System.Collections.Concurrent;

using Nixie;
using Nixie.Routers;

using Polly.Contrib.WaitAndRetry;
using Kahuna.Utils;

using Kommander;
using Kommander.Data;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL.IO;
using Kommander.Support.Parallelization;

using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Server.Replication;
using Kahuna.Server.ScriptParser;
using Kahuna.Server.Communication.Internode;
using Kahuna.Server.KeyValues.Logging;
using System.Runtime.CompilerServices;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// Manages key-value operations with support for distributed systems, replication, consistency,
/// and durability. This class interacts with various components such as Raft for consensus,
/// persistence backends, and inter-node communication in a distributed environment.
/// </summary>
internal sealed partial class KeyValuesManager : IDisposable
{
    private const int MaxRetries = 3;

    private readonly ActorSystem actorSystem;

    private readonly IRaft raft;

    private readonly IRaftReadScheduler backendReadScheduler;

    private readonly IInterNodeCommunication interNodeCommunication;

    private readonly IPersistenceBackend persistenceBackend;

    private readonly ILogger<IKahuna> logger;

    private readonly KeyValueLocator locator;
    
    private readonly TransactionCoordinator txCoordinator;

    private readonly ScriptTransactionExecutor scriptExecutor;

    /// <summary>Admission gate governing how many interactive sessions may be open at once.</summary>
    internal readonly TransactionPriorityOrderer sessionOrderer;

    /// <summary>Admission gate governing how many script transactions may execute at once.</summary>
    internal readonly TransactionPriorityOrderer scriptOrderer;
    
    private readonly IActorRef<ScriptParserEvicterActor, ScriptParserEvicterRequest> scriptParserEvicter;

    private readonly IActorRef<KeyValueCollectorActor, KeyValueCollectorRequest> keyValueCollector;

    private readonly IActorRef<BackgroundWriterActor, BackgroundWriteRequest> backgroundWriter;

    /// <summary>Per-partition application-durability floor tracker, shared node-wide.</summary>
    private readonly PartitionDurabilityTracker? durabilityTracker;
    
    private readonly Writes.PartitionWriteAggregator writeAggregator;

    /// <summary>The two consistent-hash rings of key-value actors and the send path into them.</summary>
    private readonly KeyValueActorRouters routers;

    /// <summary>MVCC snapshot holds and the floor they imply.</summary>
    private readonly SnapshotHoldService snapshotHolds;

    /// <summary>Key-space registration, range lookup, and the manual split/merge entry points.</summary>
    private readonly KeySpaceAdminService keySpaceAdmin;

    /// <summary>Restore replay, committed-entry apply, replication errors and leader changes.</summary>
    private readonly KeyValueReplicationDispatcher replicationDispatcher;

    /// <summary>The durable-2PC replication path: routing, fencing, bundling and record lookup.</summary>
    private readonly Writes.DurableReplicationGateway durableReplication;

    /// <summary>Moves range locks, durable-2PC metadata and key-value pages between partitions.</summary>
    private readonly RangeStateTransferService rangeStateTransfer;

    /// <summary>Node-local execution of key-value reads and writes against the actor rings.</summary>
    private readonly LocalKeyValueOperations localKeyValues;

    /// <summary>Node-local execution of key-value reads against the actor rings.</summary>
    private readonly LocalKeyValueReadOperations localKeyValueReads;

    /// <summary>Node-local point, prefix and range lock acquire/release.</summary>
    private readonly LocalLockOperations localLocks;

    /// <summary>The manual two-phase-commit ticket path over the actors.</summary>
    private readonly LocalMutationTicketOperations localMutationTickets;

    /// <summary>Node-local prefix, bucket and range scans.</summary>
    private readonly LocalScanOperations localScans;

    /// <summary>Recovery and retention GC of the durable-2PC records, intents and receipts.</summary>
    private readonly DurableMaintenanceService durableMaintenance;

    /// <summary>Register-remote machinery for transaction-scoped operations.</summary>
    private readonly OperationRegistrar operationRegistrar;

    /// <summary>Routing façade for key-value writes.</summary>
    private readonly RoutedWriteOperations routedWrites;

    /// <summary>Routing façade for key-value reads.</summary>
    private readonly RoutedReadOperations routedReads;

    /// <summary>Routing façade for lock operations.</summary>
    private readonly RoutedLockOperations routedLocks;

    /// <summary>Routing façade for scans and the routed two-phase-commit ticket calls.</summary>
    private readonly RoutedScanOperations routedScans;

    /// <summary>Transaction session lifecycle and working-set mapping.</summary>
    private readonly TransactionSessionFacade transactionSessions;

    /// <summary>Periodic node upkeep: collection, safe timestamp, purge, evict and write drain.</summary>
    private readonly NodeMaintenanceService nodeMaintenance;

    // Aliases so the routed-operation wrappers below keep reading exactly as they did before the
    // registration machinery moved out.
    private ParticipantOperationCache participantOperationCache => operationRegistrar.ParticipantOperationCache;

    private static RegistrationRouting ClassifyRegistration(HLCTimestamp transactionId, string coordinatorKey, TransactionOperationId operationId) =>
        OperationRegistrar.ClassifyRegistration(transactionId, coordinatorKey, operationId);

    private Task<bool> CompleteRegisteredOperation(
        string coordinatorKey, HLCTimestamp transactionId, TransactionOperationId operationId,
        object response, OperationCompletionPayload payload) =>
        operationRegistrar.CompleteRegisteredOperation(coordinatorKey, transactionId, operationId, response, payload);

    private Task<object?> TryRecoverRegisteredOperation(string coordinatorKey, HLCTimestamp transactionId, TransactionOperationId operationId) =>
        operationRegistrar.TryRecoverRegisteredOperation(coordinatorKey, transactionId, operationId);

    /// <summary>Registers a transaction-scoped operation on its coordinator before it is applied.</summary>
    public ValueTask<(OperationRegistrationOutcome outcome, KeyValueResponseType cachedType, long cachedRevision, HLCTimestamp cachedTimestamp, string? recordAnchorKey)> LocateAndBeginOperation(string coordinatorKey, HLCTimestamp transactionId, TransactionOperationId operationId, OperationKind kind, byte[]? payloadDigest, CancellationToken cancellationToken) =>
        operationRegistrar.LocateAndBeginOperation(coordinatorKey, transactionId, operationId, kind, payloadDigest, cancellationToken);

    /// <summary>Records a transaction-scoped operation's confirmed effect on its coordinator.</summary>
    public ValueTask<(KeyValueResponseType outcome, string? anchor)> LocateAndCompleteOperation(string coordinatorKey, HLCTimestamp transactionId, TransactionOperationId operationId, OperationCompletionPayload payload, CancellationToken cancellationToken) =>
        operationRegistrar.LocateAndCompleteOperation(coordinatorKey, transactionId, operationId, payload, cancellationToken);

    /// <summary>Coordinator-side registration of an operation.</summary>
    public (OperationRegistrationOutcome outcome, KeyValueResponseType cachedType, long cachedRevision, HLCTimestamp cachedTimestamp, string? recordAnchorKey) BeginOperation(HLCTimestamp transactionId, TransactionOperationId operationId, OperationKind kind, byte[]? payloadDigest) =>
        operationRegistrar.BeginOperation(transactionId, operationId, kind, payloadDigest);

    /// <summary>Coordinator-side recording of an operation's confirmed effect.</summary>
    public string? CompleteOperation(HLCTimestamp transactionId, TransactionOperationId operationId, OperationCompletionPayload payload) =>
        operationRegistrar.CompleteOperation(transactionId, operationId, payload);

    /// <summary>The inbound leg of a routed completion, gated on this node still leading the coordinator.</summary>
    public Task<(KeyValueResponseType outcome, string? anchor)> CompleteOperationInbound(string coordinatorKey, HLCTimestamp transactionId, TransactionOperationId operationId, OperationCompletionPayload payload) =>
        operationRegistrar.CompleteOperationInbound(coordinatorKey, transactionId, operationId, payload);

    // Foreign-decision resolution belongs to the local execution path; the lock and scan paths still
    // reach it here until they move too.
    private Task<bool> TryRouteForeignDecision(KeyValueRequest request, string key, HLCTimestamp transactionId, KeyValueDurability durability, bool alreadyAttempted) =>
        localKeyValues.TryRouteForeignDecision(request, key, transactionId, durability, alreadyAttempted);

    private Task<IReadOnlyDictionary<(HLCTimestamp TransactionId, long Epoch), TransactionDecision>?> TryRouteForeignScanDecisions(
        IReadOnlyList<Transactions.Data.PreparedIntent> windowIntents,
        HLCTimestamp scanTransactionId,
        CancellationToken cancellationToken) =>
        localKeyValues.TryRouteForeignScanDecisions(windowIntents, scanTransactionId, cancellationToken);

    private KeyValueActorRing ephemeralKeyValuesRouter => routers.Ephemeral;

    private KeyValueActorRing persistentKeyValuesRouter => routers.Persistent;

    private IReadOnlyList<IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse>> ephemeralInstances => routers.EphemeralInstances;

    private IReadOnlyList<IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse>> persistentInstances => routers.PersistentInstances;

    internal IReadOnlyList<IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse>> EphemeralInstances => routers.EphemeralInstances;

    internal IReadOnlyList<IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse>> PersistentInstances => routers.PersistentInstances;

    /// <summary>
    /// Whether a request is a control message, exempt from the actor inbox bound and delivered ahead of
    /// the ordinary backlog.
    /// </summary>
    internal static bool IsControlRequest(KeyValueRequest request) => KeyValueActorRouters.IsControlRequest(request);

    /// <summary>
    /// Sends a request to a bounded key-value actor router, mapping inbox-full backpressure to a
    /// retryable <c>MustRetry</c> response.
    /// </summary>
    private static ValueTask<KeyValueResponse?> AskKeyValueActor(
        KeyValueActorRing router,
        KeyValueRequest request) => KeyValueActorRouters.AskKeyValueActor(router, request);

    private readonly KeyValueRestorer restorer;

    private readonly KeyValueReplicator replicator;

    private readonly RangeMapStore rangeMapStore;

    private readonly PartitionDataEnumerator partitionDataEnumerator;

    private readonly PartitionStateTransfer partitionStateTransfer;

    private readonly SnapshotFloorStore snapshotFloorStore;

    private readonly CompletionReceiptStore completionReceiptStore;

    /// <summary>
    /// Number of persistent keys that have been settled through the manual two-phase-commit ticket path
    /// on this node. Zero across an all-persistent or mixed transaction proves that path was never taken —
    /// the persistent subset went through the durable-intent path instead.
    /// </summary>
    public long ManualTicketPersistentSettlementCount => localMutationTickets.ManualTicketPersistentSettlementCount;

    /// <summary>
    /// Test-only injection point: when set and it returns true for a destination partition,
    /// <see cref="ImportCompletionReceiptsReplicated"/> reports failure without replicating, simulating a
    /// split/merge receipt handoff that could not be made durable so cutover must abort. Never wired in
    /// production paths.
    /// </summary>
    internal Func<int, bool>? ReplicateReceiptImportFault
    {
        get => rangeStateTransfer.ReplicateReceiptImportFault;
        set => rangeStateTransfer.ReplicateReceiptImportFault = value;
    }

    /// <summary>
    /// Test-only injection point: when set and it returns true for a participant partition,
    /// <see cref="ForgetCompletionReceiptsReplicated"/> reports failure without replicating, simulating a receipt
    /// forget that could not be made durable so the decision must keep the participant unreleased. Invoked exactly
    /// once per replicated forget, so a hook that always returns false also serves as the observation point for how
    /// many forget replications a GC pass issues. Never wired in production paths.
    /// </summary>
    internal Func<int, bool>? ReplicateReceiptForgetFault
    {
        get => rangeStateTransfer.ReplicateReceiptForgetFault;
        set => rangeStateTransfer.ReplicateReceiptForgetFault = value;
    }

    /// <summary>Test-only access to the transaction coordinator for driving renewal and reap directly.</summary>
    internal TransactionCoordinator Coordinator => txCoordinator;

    /// <summary>Test-only access to the Raft instance for HLC timestamp generation in tests.</summary>
    internal IRaft Raft => raft;

    /// <summary>Test-only access to the inter-node communication for fault/latency injection.</summary>
    internal IInterNodeCommunication InterNodeCommunication => interNodeCommunication;

    // Durable-intent 2PC model: canonical transaction records plus per-key prepared intents. Replication,
    // restore, and per-partition snapshot are wired here so followers and restore reconstruct them; the finalize
    // path that produces these transitions is not yet wired, so in steady state today nothing writes these log
    // types.
    private readonly TransactionRecordStore transactionRecordStore;

    private readonly PreparedIntentStore preparedIntentStore;

    // Carries each durable entry's apply result from the consumer apply to the write scheduler's completion for the
    // same log entry, so the completion reuses it instead of deserializing and re-applying an identical delta.
    private readonly Transactions.DurableApplyResultLedger durableApplyResults;

    // Instance-owned meter for the durable-2PC resident-state gauges; disposed on teardown so a disposed node's
    // stores are not kept reachable by the gauge callbacks.
    private readonly System.Diagnostics.Metrics.Meter durableGaugeMeter;

    private readonly System.Diagnostics.Metrics.Meter admissionGaugeMeter;

    /// <summary>The durable-intent 2PC stores and key→partition routing, exposed to the transaction coordinator's
    /// durable finalize path (the sole durable-persistent finalize path).</summary>
    internal TransactionRecordStore DurableTransactionRecordStore => transactionRecordStore;

    internal PreparedIntentStore DurablePreparedIntentStore => preparedIntentStore;

    /// <summary>Resolves a key to the data partition its durable records are anchored on.</summary>
    internal (int PartitionId, long Generation) LocateDurablePartition(string key) => durableReplication.LocateDurablePartition(key);

    /// <summary>Replicates a durable entry through the partition write scheduler.</summary>
    internal Task<bool> ReplicateDurableThroughScheduler(int partitionId, string logType, byte[] data, Writes.WriteAdmissionClass admissionClass, CancellationToken cancellationToken) =>
        durableReplication.ReplicateDurableThroughScheduler(partitionId, logType, data, admissionClass, cancellationToken);

    /// <summary>Replicates a durable entry fenced on a range lock's key and generation.</summary>
    internal Task<bool> ReplicateDurableThroughSchedulerFenced(int partitionId, string logType, byte[] data, string fenceKey, long fenceGeneration, Writes.WriteAdmissionClass admissionClass, CancellationToken cancellationToken, bool projectRecordLocally = true) =>
        durableReplication.ReplicateDurableThroughSchedulerFenced(partitionId, logType, data, fenceKey, fenceGeneration, admissionClass, cancellationToken, projectRecordLocally);

    /// <summary>Replicates an anchor's init+prepare as one atomic batch. Reports batch commit and prepare
    /// acknowledgement independently.</summary>
    internal Task<(bool BatchCommitted, bool PrepareAcknowledged)> ReplicateDurableBundleThroughSchedulerFenced(
        int partitionId, byte[] recordInitDelta, byte[] anchorPrepareDelta, string fenceKey, long fenceGeneration, CancellationToken cancellationToken) =>
        durableReplication.ReplicateDurableBundleThroughSchedulerFenced(partitionId, recordInitDelta, anchorPrepareDelta, fenceKey, fenceGeneration, cancellationToken);

    /// <summary>One-phase bundled commit. Null when the bundle could not be attempted at all.</summary>
    internal Task<(bool BatchCommitted, bool PrepareAcknowledged)?> ReplicateDurableOnePhaseBundleThroughSchedulerFenced(
        int partitionId, byte[] recordInitDelta, byte[] anchorPrepareDelta, byte[] decisionDelta, string fenceKey, long fenceGeneration, CancellationToken cancellationToken) =>
        durableReplication.ReplicateDurableOnePhaseBundleThroughSchedulerFenced(partitionId, recordInitDelta, anchorPrepareDelta, decisionDelta, fenceKey, fenceGeneration, cancellationToken);

    /// <summary>Executes a durable operation on this node (the inbound leg of a routed durable call).</summary>
    internal Task<bool> DurableOperationLocal(int partitionId, int kind, string logType, byte[] payload, CancellationToken cancellationToken) =>
        durableReplication.DurableOperationLocal(partitionId, kind, logType, payload, cancellationToken);

    /// <summary>Looks a transaction record up on this node.</summary>
    internal Task<byte[]?> LookupTransactionRecordLocal(int partitionId, HLCTimestamp transactionId, long epoch, string anchorKey, CancellationToken cancellationToken) =>
        durableReplication.LookupTransactionRecordLocal(partitionId, transactionId, epoch, anchorKey, cancellationToken);

    /// <summary>Looks a transaction record up on the partition that anchors it.</summary>
    internal Task<TransactionRecord?> LookupDurableRecordRouted(HLCTimestamp transactionId, long epoch, string anchorKey, CancellationToken cancellationToken) =>
        durableReplication.LookupDurableRecordRouted(transactionId, epoch, anchorKey, cancellationToken);

    /// <summary>Applies a prepared intent's commit on its partition.</summary>
    internal Task<bool> ApplyDurableCommit(int partitionId, Transactions.Data.PreparedIntent intent, CancellationToken cancellationToken) =>
        durableReplication.ApplyDurableCommit(partitionId, intent, cancellationToken);

    /// <summary>Applies a prepared intent's rollback on its partition.</summary>
    internal Task<bool> ApplyDurableRollback(int partitionId, Transactions.Data.PreparedIntent intent, CancellationToken cancellationToken) =>
        durableReplication.ApplyDurableRollback(partitionId, intent, cancellationToken);



    private readonly IActorRef<PreparedIntentRecoveryActor, PreparedIntentRecoveryRequest> preparedIntentRecovery;


    private readonly KvStateMachineTransfer kvStateMachineTransfer;

    private readonly MetaSystemStateTransfer metaSystemStateTransfer;

    private readonly KeySpaceRegistry keySpaceRegistry;


    private readonly RangeSplitter rangeSplitter;

    private readonly RangeSplitTrigger rangeSplitTrigger;

    private readonly RangeMerger rangeMerger;

    private readonly RangeMergeTrigger rangeMergeTrigger;

    private readonly KeyWriteFrequencyRegistry writeFrequencyRegistry;

    /// <summary>Infrastructure and shared state handed to every key-value collaborator.</summary>
    private readonly KeyValuesRuntime runtime;

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
        IRaftReadScheduler backendReadScheduler,
        IInterNodeCommunication interNodeCommunication,
        IPersistenceBackend persistenceBackend,
        IActorRef<BackgroundWriterActor, BackgroundWriteRequest> backgroundWriter,
        KahunaConfiguration configuration,
        ILogger<IKahuna> logger,
        SnapshotFloorStore? externalFloorStore = null,
        CompletionReceiptStore? externalReceiptStore = null,
        TransactionRecordStore? externalRecordStore = null,
        PreparedIntentStore? externalIntentStore = null,
        Func<Writes.IPartitionBatchExecutor, Writes.IPartitionBatchExecutor>? writeBatchExecutorDecorator = null,
        PartitionDurabilityTracker? durabilityTracker = null
    )
    {
        // Every part of the subsystem is built and wired by the builder, in an order that is itself
        // load-bearing (see its remarks). The manager only takes delivery of the results.
        KeyValuesManagerBuilder built = new(
            this,
            actorSystem,
            raft,
            backendReadScheduler,
            interNodeCommunication,
            persistenceBackend,
            backgroundWriter,
            configuration,
            logger,
            externalFloorStore,
            externalReceiptStore,
            externalRecordStore,
            externalIntentStore,
            writeBatchExecutorDecorator,
            durabilityTracker);

        this.runtime = built.runtime;
        this.scriptParserEvicter = built.scriptParserEvicter;
        this.rangeMapStore = built.rangeMapStore;
        this.partitionDataEnumerator = built.partitionDataEnumerator;
        this.snapshotFloorStore = built.snapshotFloorStore;
        this.completionReceiptStore = built.completionReceiptStore;
        this.transactionRecordStore = built.transactionRecordStore;
        this.preparedIntentStore = built.preparedIntentStore;
        this.partitionStateTransfer = built.partitionStateTransfer;
        this.writeAggregator = built.writeAggregator;
        this.routers = built.routers;
        this.snapshotHolds = built.snapshotHolds;
        this.keyValueCollector = built.keyValueCollector;
        this.sessionOrderer = built.sessionOrderer;
        this.scriptOrderer = built.scriptOrderer;
        this.txCoordinator = built.txCoordinator;
        this.scriptExecutor = built.scriptExecutor;
        this.durableGaugeMeter = built.durableGaugeMeter;
        this.admissionGaugeMeter = built.admissionGaugeMeter;
        this.locator = built.locator;
        this.preparedIntentRecovery = built.preparedIntentRecovery;
        this.restorer = built.restorer;
        this.replicator = built.replicator;
        this.replicationDispatcher = built.replicationDispatcher;
        this.durableReplication = built.durableReplication;
        this.rangeStateTransfer = built.rangeStateTransfer;
        this.localKeyValues = built.localKeyValues;
        this.localKeyValueReads = built.localKeyValueReads;
        this.localLocks = built.localLocks;
        this.localMutationTickets = built.localMutationTickets;
        this.localScans = built.localScans;
        this.durableMaintenance = built.durableMaintenance;
        this.operationRegistrar = built.operationRegistrar;
        this.routedWrites = built.routedWrites;
        this.routedReads = built.routedReads;
        this.routedLocks = built.routedLocks;
        this.routedScans = built.routedScans;
        this.transactionSessions = built.transactionSessions;
        this.nodeMaintenance = built.nodeMaintenance;
        this.kvStateMachineTransfer = built.kvStateMachineTransfer;
        this.metaSystemStateTransfer = built.metaSystemStateTransfer;
        this.rangeSplitter = built.rangeSplitter;
        this.rangeSplitTrigger = built.rangeSplitTrigger;
        this.rangeMerger = built.rangeMerger;
        this.rangeMergeTrigger = built.rangeMergeTrigger;
        this.keySpaceAdmin = built.keySpaceAdmin;
        this.keySpaceRegistry = built.keySpaceRegistry;
        this.writeFrequencyRegistry = built.writeFrequencyRegistry;
        this.durableApplyResults = built.durableApplyResults;
        this.actorSystem = built.actorSystem;
        this.raft = built.raft;
        this.backendReadScheduler = built.backendReadScheduler;
        this.interNodeCommunication = built.interNodeCommunication;
        this.persistenceBackend = built.persistenceBackend;
        this.backgroundWriter = built.backgroundWriter;
        this.logger = built.logger;
        this.durabilityTracker = built.durabilityTracker;
    }

    /// <summary>
    /// The replicated range-descriptor map. The single writer is
    /// <see cref="RangeMapStore.MutateAsync"/>; routing (Tasks 3+) reads <see cref="RangeMapStore.Current"/>.
    /// </summary>
    internal RangeMapStore RangeMapStore => rangeMapStore;

    /// <summary>Enumerates the backend data owned by one partition (snapshot export / un-host purge).</summary>
    internal PartitionDataEnumerator PartitionDataEnumerator => partitionDataEnumerator;

    /// <summary>Whole-partition state transfer, registered with Kommander for replica seeding.</summary>
    internal PartitionStateTransfer PartitionStateTransfer => partitionStateTransfer;

    /// <summary>
    /// The replicated, refcounted, leased MVCC snapshot-floor registry.
    /// </summary>
    internal SnapshotFloorStore SnapshotFloorStore => snapshotFloorStore;

    /// <summary>Node-local persistent-participant completion receipts. Diagnostic/test access.</summary>
    internal CompletionReceiptStore CompletionReceiptStore => completionReceiptStore;

    public void Dispose()
    {
        // Reject new writes and release any queued-but-not-dispatched ones retryably before tearing down.
        writeAggregator.Stop();
        txCoordinator.Dispose();
        rangeMapStore.Dispose();
        snapshotFloorStore.Dispose();
        rangeSplitTrigger?.Dispose();
        durableGaugeMeter.Dispose();
        admissionGaugeMeter.Dispose();

        // Fail anything still waiting for a slot rather than leaving callers awaiting an admission that a
        // torn-down node will never grant.
        scriptOrderer.Dispose();
        sessionOrderer.Dispose();
    }
}
