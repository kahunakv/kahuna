
using System.Collections.Concurrent;

using Nixie;
using Nixie.Routers;

using Polly.Contrib.WaitAndRetry;

using Kommander;
using Kommander.Data;
using Kommander.System;
using Kommander.Time;
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
internal sealed class KeyValuesManager : IDisposable
{
    private const int MaxRetries = 3;

    /// <summary>
    /// Test seam (null in production): forces the batched commit outcome for a participant group, short-
    /// circuiting its <c>CommitLogs</c> so the ticket genuinely does not commit. Lets a test drive a real
    /// cross-partition partial commit — one partition commits for real while another is forced to fail — and
    /// assert the coordinator never reports Aborted once any participant has committed. Keyed by the group's
    /// routing key; return null to leave the group's real commit untouched.
    /// </summary>
    internal Func<string, KeyValueResponseType?>? ForceCommitOutcomeForKey;

    /// <summary>
    /// Test seam (null in production): overrides the commit-time partition a key resolves to, simulating a
    /// range split/move between prepare and commit so a test can drive the split-cohesion guard in
    /// <c>CommitTicketGroup</c> without running a real split. Return null to use real routing.
    /// </summary>
    internal Func<string, int?>? PartitionOverrideForCommit;

    /// <summary>
    /// Test seam (null in production): forces a partition group's batched prepare outcome without staging or
    /// proposing it. Lets a test make one partition fail prepare while another prepares for real, so the
    /// coordinator aborts and rolls back the prepared partition's shared ticket — exercising the batched
    /// rollback. Keyed by the group's routing key; return null to leave the group's real staging untouched.
    /// </summary>
    internal Func<string, KeyValueResponseType?>? ForcePrepareOutcomeForKey;
    
    private readonly ActorSystem actorSystem;

    private readonly IRaft raft;

    private readonly IInterNodeCommunication interNodeCommunication;

    private readonly IPersistenceBackend persistenceBackend;

    private readonly ILogger<IKahuna> logger;

    private readonly KeyValueLocator locator;
    
    private readonly TransactionCoordinator txCoordinator;

    private readonly ScriptTransactionExecutor scriptExecutor;
    
    private readonly IActorRef<ScriptParserEvicterActor, ScriptParserEvicterRequest> scriptParserEvicter;

    private readonly IActorRef<KeyValueCollectorActor, KeyValueCollectorRequest> keyValueCollector;

    private readonly IActorRef<BackgroundWriterActor, BackgroundWriteRequest> backgroundWriter;
    
    private readonly Writes.PartitionWriteAggregator writeAggregator;

    private readonly IActorRef<BalancingActor<KeyValuePhaseTwoActor, KeyValuePhaseTwoRequest>, KeyValuePhaseTwoRequest> phaseTwoRouter;

    private readonly List<IActorRef<KeyValuePhaseTwoActor, KeyValuePhaseTwoRequest>> phaseTwoWorkers = [];

    private readonly IActorRef<ConsistentHashActor<KeyValueActor, KeyValueRequest, KeyValueResponse>, KeyValueRequest, KeyValueResponse> ephemeralKeyValuesRouter;
    
    private readonly IActorRef<ConsistentHashActor<KeyValueActor, KeyValueRequest, KeyValueResponse>, KeyValueRequest, KeyValueResponse> persistentKeyValuesRouter;

    private readonly List<IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse>> ephemeralInstances = [];
    
    private readonly List<IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse>> persistentInstances = [];

    internal IReadOnlyList<IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse>> EphemeralInstances => ephemeralInstances;
    internal IReadOnlyList<IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse>> PersistentInstances => persistentInstances;

    private readonly KeyValueRestorer restorer;

    private readonly KeyValueReplicator replicator;

    private readonly RangeMapStore rangeMapStore;

    private readonly SnapshotFloorStore snapshotFloorStore;

    private readonly CompletionReceiptStore completionReceiptStore;

    /// <summary>
    /// Test-only injection point: when set and it returns true for a destination partition,
    /// <see cref="ImportCompletionReceiptsReplicated"/> reports failure without replicating, simulating a
    /// split/merge receipt handoff that could not be made durable so cutover must abort. Never wired in
    /// production paths.
    /// </summary>
    internal Func<int, bool>? ReplicateReceiptImportFault { get; set; }

    /// <summary>
    /// Test-only injection point: when set and it returns true for a participant partition,
    /// <see cref="ForgetCompletionReceiptsReplicated"/> reports failure without replicating, simulating a receipt
    /// forget that could not be made durable so the decision must keep the participant unreleased. Never wired in
    /// production paths.
    /// </summary>
    internal Func<int, bool>? ReplicateReceiptForgetFault { get; set; }

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

    /// <summary>The durable-intent 2PC stores and key→partition routing, exposed to the transaction coordinator's
    /// durable finalize path (the sole durable-persistent finalize path).</summary>
    internal TransactionRecordStore DurableTransactionRecordStore => transactionRecordStore;

    internal PreparedIntentStore DurablePreparedIntentStore => preparedIntentStore;

    internal (int PartitionId, long Generation) LocateDurablePartition(string key) => locator.LocateRange(key);

    /// <summary>
    /// Replicates a durable-intent 2PC delta through the shared partition write scheduler so it coalesces with
    /// concurrent transactions' records to the same partition into one <c>ReplicateEntries</c> proposal. Returns
    /// true once the batch carrying it committed; false on scheduler backpressure (the finalizer maps that to a
    /// retryable outcome). The caller applies the committed delta to its local store, matching the direct-write
    /// path where the producer applies on completion.
    /// </summary>
    // Durable-operation kinds carried across the inter-node forwarding to the partition leader.
    private const int DurableOpReplicate = 0;
    private const int DurableOpCommit = 1;
    private const int DurableOpRollback = 2;

    /// <summary>
    /// Resolves whether a durable operation for <paramref name="partitionId"/> must run on a remote leader.
    /// Returns null when this node is the partition leader (or standalone) — run locally; otherwise the remote
    /// leader endpoint to forward to. The durable path proposes via the local scheduler and clears the staged write
    /// intent locally, both of which require the local node to be the partition leader, so on a cluster a coordinator
    /// that is not the leader forwards the operation to the leader (where records also coalesce across coordinators).
    /// </summary>
    private async Task<string?> ResolveDurableLeader(int partitionId, CancellationToken cancellationToken)
    {
        if (!raft.Joined || await raft.AmILeader(partitionId, cancellationToken).ConfigureAwait(false))
            return null;

        string leader = await raft.WaitForLeader(partitionId, cancellationToken).ConfigureAwait(false);
        return leader == raft.GetLocalEndpoint() ? null : leader;
    }

    internal async Task<bool> ReplicateDurableThroughScheduler(int partitionId, string logType, byte[] data, CancellationToken cancellationToken)
    {
        string? leader = await ResolveDurableLeader(partitionId, cancellationToken).ConfigureAwait(false);
        if (leader is not null)
        {
            bool ok = await interNodeCommunication.DurableOperation(leader, partitionId, DurableOpReplicate, logType, data, cancellationToken).ConfigureAwait(false);

            // The authoritative apply happened on the remote leader (its scheduler is the single ordered owner).
            // Keep a local projection of the canonical record only, so this node's own decision read-back and
            // lost-session consult resolve locally until the anchor-routed record lookup replaces it. The record
            // identity is this transaction's own, so the projection never misrepresents another transaction;
            // prepared intents are not projected (a remote conflict is already reflected in a false result here).
            if (ok && logType == ReplicationTypes.TransactionRecord)
                transactionRecordStore.Replicate(partitionId, new RaftLog { LogType = logType, LogData = data });

            return ok;
        }

        return await ReplicateDurableLocal(partitionId, logType, data, fenceKey: null, fenceGeneration: 0, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Like <see cref="ReplicateDurableThroughScheduler"/> but re-fences the local submission at dispatch against
    /// the range descriptor <paramref name="fenceKey"/> resolved to at freeze (<paramref name="fenceGeneration"/>):
    /// a split/merge between freeze and dispatch releases it retryably instead of appending to a retired partition.
    /// The re-fence applies on the local aggregator path; a forward to a remote leader carries no fence (the
    /// remote's freeze-to-dispatch race is a follow-up).
    /// </summary>
    internal async Task<bool> ReplicateDurableThroughSchedulerFenced(int partitionId, string logType, byte[] data, string fenceKey, long fenceGeneration, CancellationToken cancellationToken)
    {
        string? leader = await ResolveDurableLeader(partitionId, cancellationToken).ConfigureAwait(false);
        if (leader is not null)
        {
            bool ok = await interNodeCommunication.DurableOperation(leader, partitionId, DurableOpReplicate, logType, data, cancellationToken).ConfigureAwait(false);
            if (ok && logType == ReplicationTypes.TransactionRecord)
                transactionRecordStore.Replicate(partitionId, new RaftLog { LogType = logType, LogData = data });
            return ok;
        }

        return await ReplicateDurableLocal(partitionId, logType, data, fenceKey, fenceGeneration, cancellationToken).ConfigureAwait(false);
    }

    private async Task<bool> ReplicateDurableLocal(int partitionId, string logType, byte[] data, string? fenceKey, long fenceGeneration, CancellationToken cancellationToken)
    {
        TaskCompletionSource<bool> completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
        Writes.DurableProposalSubmission submission = new(
            partitionId,
            [new RaftProposalEntry(logType, data, AutoCommit: true, ExpectedGeneration: 0)],
            completion,
            ApplyDurableEntriesOnCommit,
            fenceKey,
            fenceGeneration);

        if (!writeAggregator.TryEnqueue(submission))
            return false;

        using CancellationTokenRegistration _ = cancellationToken.Register(static state => ((TaskCompletionSource<bool>)state!).TrySetResult(false), completion);
        return await submission.Committed.ConfigureAwait(false);
    }

    // The scheduler's ordered per-partition completion applies each durable record/intent delta to its store, in
    // Raft-commit order — the single authoritative apply owner on the leader. Key/value materialization records are
    // applied by their own leader path (ApplyDurableCommit / the replicator), not here. Returns whether every
    // PREPARE in the bundle took ownership of its key so a rejected prepare fails the producer's replicate.
    private bool ApplyDurableEntriesOnCommit(IReadOnlyList<RaftProposalEntry> entries)
    {
        bool preparesAcknowledged = true;

        foreach (RaftProposalEntry entry in entries)
        {
            RaftLog log = new() { LogType = entry.Type, LogData = entry.Data };

            // The store applies by record/intent identity; the partition argument is unused on the apply path.
            if (entry.Type == ReplicationTypes.TransactionRecord)
                transactionRecordStore.Replicate(0, log);
            else if (entry.Type == ReplicationTypes.PreparedIntent)
                preparesAcknowledged &= preparedIntentStore.ApplyDeltaAckPrepares(log);
        }

        return preparesAcknowledged;
    }

    /// <summary>
    /// Runs a durable operation that this node received via inter-node forwarding because it is the partition
    /// leader: replicate a delta through the local scheduler, or apply a committed/aborted intent's resolution on
    /// the local (leader) KV state. The intent crosses the wire serialized.
    /// </summary>
    internal async Task<bool> DurableOperationLocal(int partitionId, int kind, string logType, byte[] payload, CancellationToken cancellationToken)
    {
        switch (kind)
        {
            case DurableOpReplicate:
                // A forwarded durable op is applied on the leader it was routed to; the fence ran on the origin.
                return await ReplicateDurableLocal(partitionId, logType, payload, fenceKey: null, fenceGeneration: 0, cancellationToken).ConfigureAwait(false);

            case DurableOpCommit:
                replicator.ApplyDurableCommit(partitionId, PreparedIntentStore.DeserializeIntents(payload)[0]);
                return true;

            case DurableOpRollback:
                replicator.ApplyDurableRollback(partitionId, PreparedIntentStore.DeserializeIntents(payload)[0]);
                return true;

            default:
                return false;
        }
    }

    /// <summary>The soft cap on the positive-terminal record-lookup cache; a terminal (Commit/Abort) record is
    /// immutable so a hit is always valid, but the cache stops growing past this to bound memory.</summary>
    private const int DurableRecordLookupCacheMax = 8192;

    /// <summary>Positive-terminal cache for routed record lookups: only immutable terminal records are cached (an
    /// Undecided/absent outcome can still transition and must never be cached across a later decision).</summary>
    private readonly ConcurrentDictionary<(HLCTimestamp, long), TransactionRecord> durableRecordLookupCache = new();

    /// <summary>
    /// Serves a canonical transaction-record lookup that was routed to this node because it is the record's anchor
    /// partition leader: the local record store is authoritative here. Returns the serialized record, or null when
    /// none exists. The record crosses the wire serialized so the public transport contract never exposes the
    /// internal record type.
    /// </summary>
    internal Task<byte[]?> LookupTransactionRecordLocal(int partitionId, HLCTimestamp transactionId, long epoch, string anchorKey, CancellationToken cancellationToken)
    {
        TransactionRecord? record = transactionRecordStore.Get(transactionId, epoch);
        return Task.FromResult(record is null ? null : TransactionRecordStore.SerializeRecords([record]));
    }

    /// <summary>
    /// Linearizable canonical-record lookup routed by the record's anchor key: resolves the anchor partition, and
    /// if this node does not lead it, forwards to the leader (whose store is authoritative) instead of consulting a
    /// node-local projection that would otherwise force a retry-until-settlement. Terminal outcomes are positively
    /// cached (immutable); Undecided/absent outcomes are never cached.
    /// </summary>
    internal async Task<TransactionRecord?> LookupDurableRecordRouted(HLCTimestamp transactionId, long epoch, string anchorKey, CancellationToken cancellationToken)
    {
        if (durableRecordLookupCache.TryGetValue((transactionId, epoch), out TransactionRecord? cached))
            return cached;

        int partitionId = locator.LocateRange(anchorKey).PartitionId;
        string? leader = await ResolveDurableLeader(partitionId, cancellationToken).ConfigureAwait(false);

        TransactionRecord? record;
        if (leader is null)
        {
            record = transactionRecordStore.Get(transactionId, epoch);
        }
        else
        {
            byte[]? serialized = await interNodeCommunication
                .LookupTransactionRecord(leader, partitionId, transactionId, epoch, anchorKey, cancellationToken)
                .ConfigureAwait(false);

            record = serialized is null ? null : TransactionRecordStore.DeserializeRecords(serialized) is [TransactionRecord r, ..] ? r : null;
        }

        if (record is { IsTerminal: true } && durableRecordLookupCache.Count < DurableRecordLookupCacheMax)
            durableRecordLookupCache[(transactionId, epoch)] = record;

        return record;
    }

    /// <summary>
    /// Applies a durable-intent resolution's committed value on the leader (clears the committing transaction's
    /// staged write intent + MVCC snapshot and applies the value to the base entry — the durable analog of
    /// CompletePhaseTwo). The leader applies direct writes through CompleteProposal, not the replication callback,
    /// so a durable-committed value would never land in the leader's store without this. Idempotent.
    /// </summary>
    internal async Task ApplyDurableCommit(int partitionId, Transactions.Data.PreparedIntent intent, CancellationToken cancellationToken)
    {
        string? leader = await ResolveDurableLeader(partitionId, cancellationToken).ConfigureAwait(false);
        if (leader is not null)
            await interNodeCommunication.DurableOperation(leader, partitionId, DurableOpCommit, "", PreparedIntentStore.SerializeIntents([intent]), cancellationToken).ConfigureAwait(false);
        else
            replicator.ApplyDurableCommit(partitionId, intent);
    }

    /// <summary>
    /// Clears an aborted durable transaction's staged write intent + MVCC snapshot on the owning actor (the durable
    /// analog of ApplyConfirmedRollback), so the key is not blocked until the intent lease expires. Routed to the
    /// partition leader on a cluster (that is where the staged write intent lives).
    /// </summary>
    internal async Task ApplyDurableRollback(int partitionId, Transactions.Data.PreparedIntent intent, CancellationToken cancellationToken)
    {
        string? leader = await ResolveDurableLeader(partitionId, cancellationToken).ConfigureAwait(false);
        if (leader is not null)
            await interNodeCommunication.DurableOperation(leader, partitionId, DurableOpRollback, "", PreparedIntentStore.SerializeIntents([intent]), cancellationToken).ConfigureAwait(false);
        else
            replicator.ApplyDurableRollback(partitionId, intent);
    }


    private readonly IActorRef<PreparedIntentRecoveryActor, PreparedIntentRecoveryRequest> preparedIntentRecovery;

    private readonly RangeQuiesceStore rangeQuiesceStore = new();

    private readonly KvStateMachineTransfer kvStateMachineTransfer;

    private readonly MetaSystemStateTransfer metaSystemStateTransfer;

    private readonly KeySpaceRegistry keySpaceRegistry = new();

    /// <summary>
    /// Participant-side results of transaction operations whose actor executed but whose coordinator
    /// completion is not yet acknowledged, so a retry can recover the confirmed effect without reapplying.
    /// </summary>
    private readonly ParticipantOperationCache participantOperationCache = new();

    private RangeSplitter? rangeSplitter;

    private RangeSplitTrigger? rangeSplitTrigger;

    private RangeMerger? rangeMerger;

    private RangeMergeTrigger? rangeMergeTrigger;

    private readonly KeyWriteFrequencyRegistry writeFrequencyRegistry = new();

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
        ILogger<IKahuna> logger,
        SnapshotFloorStore? externalFloorStore = null,
        CompletionReceiptStore? externalReceiptStore = null,
        TransactionRecordStore? externalRecordStore = null,
        PreparedIntentStore? externalIntentStore = null,
        Func<Writes.IPartitionBatchExecutor, Writes.IPartitionBatchExecutor>? writeBatchExecutorDecorator = null
    )
    {
        this.actorSystem = actorSystem;
        this.raft = raft;
        this.interNodeCommunication = interNodeCommunication;
        this.backgroundWriter = backgroundWriter;
        this.logger = logger;

        this.persistenceBackend = persistenceBackend;

        scriptParserEvicter = actorSystem.Spawn<ScriptParserEvicterActor, ScriptParserEvicterRequest>("script-parser-evicter", logger);

        // Construct the range-descriptor map before the proposal router — the proposal actors carry it
        // for the generation fence.
        rangeMapStore = new(raft, configuration.StoragePath, configuration.StorageRevision, logger);

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
        SyncKeySpaceRegistryFromRangeMap();

        // Durable-intent 2PC stores share the same per-partition snapshot lifecycle; their key/anchor → partition
        // resolvers are attached below once the locator exists. Shared with the BackgroundWriterActor when provided
        // (so the writer gates each partition's WAL checkpoint on their durable snapshots), otherwise owned locally.
        transactionRecordStore = externalRecordStore ?? new TransactionRecordStore(configuration.StoragePath, configuration.StorageRevision, logger);
        preparedIntentStore = externalIntentStore ?? new PreparedIntentStore(configuration.StoragePath, configuration.StorageRevision, logger);

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
                MaxQueueDelayMs = configuration.KeyValueWriteMaxQueueDelayMs,
                AggregatorInboxSize = configuration.MaxKeyValueWriteAggregatorInboxSize,
                LaneCount = Math.Max(1, configuration.KeyValueWorkers)
            },
            new Writes.RangeMapWriteFence(keySpaceRegistry, rangeMapStore),
            logger);
        phaseTwoRouter = GetPhaseTwoRouter(configuration);
        ephemeralKeyValuesRouter = GetEphemeralRouter(configuration);
        persistentKeyValuesRouter = GetConsistentRouter(configuration);

        keyValueCollector = actorSystem.Spawn<KeyValueCollectorActor, KeyValueCollectorRequest>(
            "keyvalue-collector",
            ephemeralInstances,
            persistentInstances,
            configuration,
            logger
        );

        txCoordinator = new(this, configuration, raft, logger);
        scriptExecutor = new(this, configuration, raft, logger, txCoordinator);

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

        locator = new(this, configuration, raft, interNodeCommunication, keySpaceRegistry, rangeQuiesceStore, logger);

        // Now that the locator exists, wire the anchor/key → data-partition resolvers the durable-intent stores
        // fence their coordinator-driven mutations on.
        transactionRecordStore.AttachAnchorResolver(locator.LocateRange);
        preparedIntentStore.AttachPartitionResolver(key => locator.LocateRange(key).PartitionId);

        // The receipt store's per-partition checkpoint snapshot routes each receipt by its key the same way.
        completionReceiptStore.AttachPartitionResolver(key => locator.LocateRange(key).PartitionId);

        preparedIntentRecovery = actorSystem.Spawn<PreparedIntentRecoveryActor, PreparedIntentRecoveryRequest>(
            "prepared-intent-recovery",
            this,
            configuration,
            logger
        );

        restorer = new(backgroundWriter, raft, completionReceiptStore, logger);
        replicator = new(backgroundWriter, persistentKeyValuesRouter, raft, writeFrequencyRegistry, keySpaceRegistry, completionReceiptStore, logger);
        kvStateMachineTransfer = new(this, persistenceBackend, logger);

        // Whole-partition state transfer for the meta partition (id 0). Repairs a node below the
        // meta WAL compaction floor by shipping the range map + snapshot-floor holds together —
        // required because the hold registry now replicates deltas that cannot be rebuilt from
        // surviving log entries once compacted.
        metaSystemStateTransfer = new(rangeMapStore, snapshotFloorStore);

        // RangeSplitter and RangeSplitTrigger both depend on this (KeyValuesManager) being fully
        // constructed, so we assign after all other fields are set.
        rangeSplitter     = new(raft, rangeMapStore, kvStateMachineTransfer, rangeQuiesceStore, this, logger);
        rangeSplitTrigger = new(raft, rangeMapStore, rangeSplitter, this, writeFrequencyRegistry, configuration, logger);
        rangeMerger       = new(raft, rangeMapStore, kvStateMachineTransfer, this, logger);
        rangeMergeTrigger = new(raft, rangeMapStore, rangeMerger, writeFrequencyRegistry, configuration, logger);

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

    /// <summary>
    /// The replicated range-descriptor map. The single writer is
    /// <see cref="RangeMapStore.MutateAsync"/>; routing (Tasks 3+) reads <see cref="RangeMapStore.Current"/>.
    /// </summary>
    internal RangeMapStore RangeMapStore => rangeMapStore;

    /// <summary>
    /// The replicated, refcounted, leased MVCC snapshot-floor registry.
    /// </summary>
    internal SnapshotFloorStore SnapshotFloorStore => snapshotFloorStore;

    /// <summary>Node-local persistent-participant completion receipts. Diagnostic/test access.</summary>
    internal CompletionReceiptStore CompletionReceiptStore => completionReceiptStore;

    /// <summary>
    /// Acquires or renews a refcounted hold protecting all revisions at/after
    /// <paramref name="timestamp"/>. Idempotent by (holderId, timestamp).
    /// </summary>
    public async Task<(KeyValueResponseType Type, string HoldId, HLCTimestamp LeaseExpiry)> AcquireSnapshotHold(
        string holderId, HLCTimestamp timestamp, int leaseMs, CancellationToken ct)
    {
        if (!raft.Joined)
            return (KeyValueResponseType.MustRetry, string.Empty, HLCTimestamp.Zero);

        if (await raft.AmILeader(RangeMapStore.MetaPartitionId, ct).ConfigureAwait(false))
            return await snapshotFloorStore.AcquireAsync(holderId, timestamp, leaseMs, ct).ConfigureAwait(false);

        string leader = await raft.WaitForLeader(RangeMapStore.MetaPartitionId, ct).ConfigureAwait(false);
        if (leader == raft.GetLocalEndpoint())
            return await snapshotFloorStore.AcquireAsync(holderId, timestamp, leaseMs, ct).ConfigureAwait(false);

        return await interNodeCommunication.AcquireSnapshotHold(leader, holderId, timestamp, leaseMs, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Renews the lease on an existing hold. Fails when the hold has already expired or was never
    /// registered.
    /// </summary>
    public async Task<(KeyValueResponseType Type, HLCTimestamp LeaseExpiry)> RenewSnapshotHold(
        string holdId, int leaseMs, CancellationToken ct)
    {
        if (!raft.Joined)
            return (KeyValueResponseType.MustRetry, HLCTimestamp.Zero);

        if (await raft.AmILeader(RangeMapStore.MetaPartitionId, ct).ConfigureAwait(false))
            return await snapshotFloorStore.RenewAsync(holdId, leaseMs, ct).ConfigureAwait(false);

        string leader = await raft.WaitForLeader(RangeMapStore.MetaPartitionId, ct).ConfigureAwait(false);
        if (leader == raft.GetLocalEndpoint())
            return await snapshotFloorStore.RenewAsync(holdId, leaseMs, ct).ConfigureAwait(false);

        return await interNodeCommunication.RenewSnapshotHold(leader, holdId, leaseMs, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Releases a hold. The effective floor rises when the lowest hold is released.
    /// </summary>
    public async Task<KeyValueResponseType> ReleaseSnapshotHold(string holdId, CancellationToken ct)
    {
        if (!raft.Joined)
            return KeyValueResponseType.MustRetry;

        if (await raft.AmILeader(RangeMapStore.MetaPartitionId, ct).ConfigureAwait(false))
            return await snapshotFloorStore.ReleaseAsync(holdId, ct).ConfigureAwait(false);

        string leader = await raft.WaitForLeader(RangeMapStore.MetaPartitionId, ct).ConfigureAwait(false);
        if (leader == raft.GetLocalEndpoint())
            return await snapshotFloorStore.ReleaseAsync(holdId, ct).ConfigureAwait(false);

        return await interNodeCommunication.ReleaseSnapshotHold(leader, holdId, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Removes all holds whose lease has expired. Normally called by the background reaper;
    /// exposed here so tests can trigger a purge cycle without waiting for the timer.
    /// </summary>
    internal Task<int> PurgeExpiredSnapshotHoldsAsync(CancellationToken ct = default) =>
        snapshotFloorStore.PurgeExpiredHoldsAsync(ct);

    /// <summary>
    /// Returns the current effective floor (minimum live held timestamp, or
    /// <see cref="HLCTimestamp.Zero"/> when no hold is live) and the count of live holds.
    /// </summary>
    public async Task<(HLCTimestamp EffectiveFloor, int LiveHolds)> GetSnapshotFloor(CancellationToken ct)
    {
        if (await raft.AmILeader(RangeMapStore.MetaPartitionId, ct).ConfigureAwait(false))
            return ReadLocalSnapshotFloor();

        string leader = await raft.WaitForLeader(RangeMapStore.MetaPartitionId, ct).ConfigureAwait(false);
        if (leader == raft.GetLocalEndpoint())
            return ReadLocalSnapshotFloor();

        return await interNodeCommunication.GetSnapshotFloor(leader, ct).ConfigureAwait(false);
    }

    private (HLCTimestamp EffectiveFloor, int LiveHolds) ReadLocalSnapshotFloor()
    {
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());
        return snapshotFloorStore.GetEffectiveFloorAndCount(now);
    }

    /// <summary>
    /// The key-range data-movement primitive. Registered with Kommander via
    /// <c>RegisterStateMachineTransfer</c>; the split transaction calls its native export/import directly.
    /// </summary>
    internal KvStateMachineTransfer KvStateMachineTransfer => kvStateMachineTransfer;

    /// <summary>
    /// The meta-partition whole-state transfer. Registered with Kommander via
    /// <c>RegisterSystemStateTransfer</c> so a node below the meta WAL compaction floor is repaired
    /// with both the range map and the snapshot-floor holds.
    /// </summary>
    internal MetaSystemStateTransfer MetaSystemStateTransfer => metaSystemStateTransfer;

    /// <summary>
    /// The per-node key-space routing registry. Row/index spaces are registered as
    /// key-range here; <c>{db}/meta</c> and system spaces stay hash-routed (the default).
    /// </summary>
    internal KeySpaceRegistry KeySpaceRegistry => keySpaceRegistry;

    internal RangeQuiesceStore RangeQuiesceStore => rangeQuiesceStore;

    /// <summary>
    /// Generation stamped on an auto-seeded initial descriptor. Must be &gt;= 1 so it acts as a real
    /// routing fence (0 is the hash-mode "no fence" sentinel); splits/merges bump it from here.
    /// </summary>
    private const long InitialSeedGeneration = 1;

    /// <summary>
    /// Flips <paramref name="keySpace"/> to key-range routing on this node and, on the meta-partition
    /// leader, auto-seeds its initial whole-space descriptor (<c>[-inf, +inf)</c>) if none exists yet.
    /// <para>
    /// <see cref="KeySpaceRegistry.RegisterKeyRange"/> alone only changes the routing <i>mode</i>; until
    /// a covering descriptor exists, routing a key throws "no range descriptor covers key". This makes
    /// registration self-sufficient: callers no longer reach into the internal <see cref="RangeMapStore"/>
    /// to bootstrap the first range. The seed is the trivial whole-space range — no key-distribution or
    /// PK-type knowledge is needed; real boundaries are discovered later by the auto-splitter from live
    /// data.
    /// </para>
    /// <para>
    /// The mode flip is node-local (every node must call this). The seed is a single replicated meta-log
    /// write that only the meta-partition (<see cref="RangeMapStore.MetaPartitionId"/>) leader can commit;
    /// on any other node the seed step is a no-op and the descriptor arrives by replication. Idempotent:
    /// safe to call repeatedly and from every node. <b>Multi-node note:</b> if no node that leads the meta
    /// partition ever calls this for the space, it stays unseeded — same dual-leader coordination caveat
    /// the auto-splitter carries. For single-node / colocated-leader deployments it just works.
    /// </para>
    /// </summary>
    /// <returns><c>true</c> iff this call committed the seed descriptor.</returns>
    internal async Task<bool> RegisterKeyRangeAsync(string keySpace, CancellationToken cancellationToken = default)
    {
        // Key-range sharding needs at least one data partition (>= FirstDataPartitionId = 1). Since
        // the meta map shares the system partition (P0), even a single user partition (P1) can host
        // ranged data, so InitialPartitions = 1 is fully supported. The guard only trips on a
        // degenerate InitialPartitions = 0 misconfiguration, where it degrades to a no-op instead of
        // seeding onto a non-existent partition.
        if (raft.Configuration.InitialPartitions < RangeMapStore.FirstDataPartitionId)
            return false;

        keySpaceRegistry.RegisterKeyRange(keySpace);
        bool seeded = await EnsureKeyRangeSeededAsync(keySpace, cancellationToken).ConfigureAwait(false);
        // Re-affirm after the descriptor is committed and locally visible. A ReconcileTo triggered
        // by an unrelated replication event between the pre-flip above and the seed commit can prune
        // this space (descriptor not yet in the map on this node). Re-asserting here closes that
        // window: the descriptor is now visible, so any subsequent ReconcileTo will find it in the
        // live set and keep the mode.
        keySpaceRegistry.RegisterKeyRange(keySpace);
        return seeded;
    }

    private async Task<bool> EnsureKeyRangeSeededAsync(string keySpace, CancellationToken cancellationToken)
    {
        // Fast path: a covering descriptor already exists (seeded earlier, or replicated from the leader).
        if (rangeMapStore.Current.FindAll(keySpace).Count > 0)
            return false;

        // Only the meta-partition leader can commit the seed. If this node is not the leader,
        // resolve the current leader and forward the seed request to it. Then wait for the descriptor
        // to replicate to this node so follow-on routes on this node see it immediately.
        if (!await raft.AmILeader(RangeMapStore.MetaPartitionId, cancellationToken).ConfigureAwait(false))
        {
            string leader = await raft.WaitForLeader(RangeMapStore.MetaPartitionId, cancellationToken).ConfigureAwait(false);
            if (leader == raft.GetLocalEndpoint())
            {
                // We just became leader between the AmILeader check and WaitForLeader — fall through to seed locally.
            }
            else
            {
                bool forwarded = await interNodeCommunication.EnsureKeyRangeSeeded(leader, keySpace, cancellationToken).ConfigureAwait(false);
                // Wait for the descriptor to replicate to this node (the calling node registered the space
                // mode-flip but doesn't have the descriptor yet).
                if (rangeMapStore.Current.FindAll(keySpace).Count == 0)
                {
                    long deadline = Environment.TickCount64 + 10_000;
                    while (Environment.TickCount64 < deadline && rangeMapStore.Current.FindAll(keySpace).Count == 0)
                        await Task.Delay(25, cancellationToken).ConfigureAwait(false);
                }
                return forwarded;
            }
        }

        int seedPartition = SeedPartitionFor(keySpace);

        // Tracks whether this call actually appended the seed. A concurrent seed of the same space
        // can land between the fast-path check and the mutate gate; in that race the transform returns
        // the map unchanged and MutateAsync still re-replicates it (returning true), so we cannot infer
        // "I committed the seed" from MutateAsync's result alone.
        bool committed = false;

        bool ok = await rangeMapStore.MutateAsync(current =>
        {
            // Re-check under the mutate gate: a concurrent seed of the same space may have landed.
            foreach (RangeDescriptor existing in current)
                if (string.Equals(existing.KeySpace, keySpace, StringComparison.Ordinal))
                    return current; // already seeded — leave the map untouched (committed stays false)

            RangeDescriptor[] next = new RangeDescriptor[current.Count + 1];
            for (int i = 0; i < current.Count; i++)
                next[i] = current[i];

            next[current.Count] = new RangeDescriptor
            {
                KeySpace = keySpace,
                StartKey = null,
                EndKey = null,
                PartitionId = seedPartition,
                Generation = InitialSeedGeneration
            };

            committed = true;
            return next;
        }, cancellationToken).ConfigureAwait(false);

        return ok && committed;
    }

    // Return-value contract (mirrors IKahuna.RemoveKeyRangeAsync doc):
    //   true  — committed or idempotent no-op (MutateAsync re-replicates even an unchanged map).
    //   false (transient)  — quiesce window active; caller should retry after a short delay.
    //   false (permanent)  — InitialPartitions < 1 or /meta suffix; space was never registered.
    internal async Task<bool> RemoveKeyRangeAsync(string keySpace, CancellationToken cancellationToken = default)
    {
        // Permanent no-ops: key-range sharding is disabled or the space is a schema-log space.
        if (raft.Configuration.InitialPartitions < RangeMapStore.FirstDataPartitionId)
            return false;

        if (keySpace.EndsWith("/meta", StringComparison.Ordinal))
            return false;

        // Transient rejection: a split quiesce window is open. The window is short; the caller
        // should wait and retry. We prefer a coarse IsEmpty check over a per-key lookup because
        // the quiesce store has no space-scoped query — only per-key IsQuiesced and IsEmpty.
        if (!rangeQuiesceStore.IsEmpty)
            return false;

        if (!await raft.AmILeader(RangeMapStore.MetaPartitionId, cancellationToken).ConfigureAwait(false))
        {
            string leader = await raft.WaitForLeader(RangeMapStore.MetaPartitionId, cancellationToken).ConfigureAwait(false);
            if (leader != raft.GetLocalEndpoint())
            {
                // Forward to the meta-partition leader and wait for the removal to replicate here.
                bool forwarded = await interNodeCommunication.EnsureKeyRangeRemoved(leader, keySpace, cancellationToken).ConfigureAwait(false);
                long deadline = Environment.TickCount64 + 10_000;
                while (Environment.TickCount64 < deadline && rangeMapStore.Current.FindAll(keySpace).Count > 0)
                    await Task.Delay(25, cancellationToken).ConfigureAwait(false);
                return forwarded;
            }
        }

        // Leader path: filter out all descriptors for the space in one atomic map commit.
        // MutateAsync re-replicates even an unchanged map and returns true, so both the
        // "removed" and "was already absent" cases return true — matching the contract above.
        bool ok = await rangeMapStore.MutateAsync(current =>
        {
            int before = current.Count;
            IReadOnlyList<RangeDescriptor> next = current
                .Where(d => !string.Equals(d.KeySpace, keySpace, StringComparison.Ordinal))
                .ToArray();

            return next.Count == before ? current : next;
        }, cancellationToken).ConfigureAwait(false);

        return ok;
    }

    /// <summary>
    /// Picks the data partition for a key space's initial descriptor: a deterministic hash over the
    /// data-partition pool <c>[<see cref="RangeMapStore.FirstDataPartitionId"/>, InitialPartitions]</c>
    /// (key-range data never lives on the reserved system/meta partition 0). Spreads different spaces'
    /// initial ranges across partitions instead of piling them all on partition 1.
    /// </summary>
    private int SeedPartitionFor(string keySpace)
    {
        int poolSize = raft.Configuration.InitialPartitions;
        int dataPartitions = poolSize - (RangeMapStore.FirstDataPartitionId - 1); // count of [1 .. poolSize]
        if (dataPartitions < 1)
            throw new KahunaServerException(
                $"Key-range space '{keySpace}' requires at least {RangeMapStore.FirstDataPartitionId} partition(s) " +
                $"(InitialPartitions={poolSize}); key-range data cannot live on the reserved partition 0.");

        int offset = (int)(HashUtils.SimpleHash(keySpace) % (ulong)dataPartitions);
        return RangeMapStore.FirstDataPartitionId + offset;
    }

    /// <summary>
    /// Resolves a key to its owning <c>(partitionId, generation)</c> via the key-order router
    /// Reads the live <see cref="RangeMapStore.Current"/> snapshot.
    /// </summary>
    internal (int PartitionId, long Generation) LocateRange(string key) => locator.LocateRange(key);

    /// <summary>
    /// Routes a flush acknowledgement from the background writer to the persistent actor that owns
    /// <paramref name="key"/> (same consistent-hash routing as a normal persistent op), so the actor
    /// can advance the entry's FlushedRevision. Fire-and-forget: a lost ack only delays eviction of
    /// an already-durable entry, never risks correctness.
    /// </summary>
    internal void NotifyFlushed(string key, long revision)
        => persistentKeyValuesRouter.Send(KeyValueRequest.ForFlushAck(key, revision));

    /// <summary>The split-transaction executor. Splits a key range at a given split key.</summary>
    internal RangeSplitter RangeSplitter => rangeSplitter!;

    /// <summary>The auto-split trigger (exposed for regression tests of <c>ExecuteSplitAsync</c>).</summary>
    internal RangeSplitTrigger RangeSplitTrigger => rangeSplitTrigger!;

    /// <summary>The merge-transaction executor. Merges adjacent under-min ranges.</summary>
    internal RangeMerger RangeMerger => rangeMerger!;

    /// <summary>
    /// Scans all KeyRange spaces for under-min adjacent descriptor pairs and merges them.
    /// Delegates to <see cref="RangeMergeTrigger.TriggerAsync"/>; returns the number of merges performed.
    /// </summary>
    internal Task<int> TriggerAutoMergeAsync(CancellationToken ct = default) =>
        rangeMergeTrigger!.TriggerAsync(ct);

    /// <summary>
    /// Test-seam overload: runs the auto-merge trigger with an explicit <paramref name="minMergeSize"/>
    /// instead of the configured value.
    /// </summary>
    internal Task<int> TriggerAutoMergeAsync(int minMergeSize, CancellationToken ct = default)
    {
        var cfg = new Configuration.KahunaConfiguration { RangeMergeMinSize = minMergeSize };
        var trigger = new RangeMergeTrigger(raft, rangeMapStore, rangeMerger!, writeFrequencyRegistry, cfg, logger);
        return trigger.TriggerAsync(ct);
    }

    /// <summary>
    /// Checks every KeyRange descriptor and splits any that exceed the configured threshold.
    /// Delegates to <see cref="RangeSplitTrigger.TriggerAsync"/>; returns the number of splits performed.
    /// </summary>
    internal Task<int> TriggerAutoSplitAsync(CancellationToken ct = default) =>
        rangeSplitTrigger!.TriggerAsync(ct);

    /// <summary>
    /// Test-seam overload: runs the auto-split trigger with an explicit <paramref name="threshold"/>
    /// and <paramref name="minRangeSize"/> instead of the configured values.
    /// </summary>
    internal Task<int> TriggerAutoSplitAsync(int threshold, int minRangeSize, CancellationToken ct = default)
    {
        var cfg = new Configuration.KahunaConfiguration
        {
            RangeSplitThreshold    = threshold,
            RangeSplitMinRangeSize = minRangeSize
        };
        var trigger = new RangeSplitTrigger(raft, rangeMapStore, rangeSplitter!, this, writeFrequencyRegistry, cfg, logger);
        return trigger.TriggerAsync(ct);
    }

    /// <summary>
    /// Test seam: forces a split of the descriptor covering <paramref name="splitKey"/> at that
    /// exact key, bypassing any key-count threshold. Handles the full
    /// <c>ComputeNextPartitionId → CreatePartitionAsync → SplitAsync</c> sequence internally, so
    /// callers do not need to manage partition IDs. Optionally invokes
    /// <paramref name="duringQuiesce"/> inside the quiesce window (after catch-up import, before
    /// cutover) for F3-style race tests.
    /// </summary>
    internal async Task<SplitOutcome> ForceSplitAtKeyAsync(
        string keySpace,
        string splitKey,
        Func<Task>? duringQuiesce = null,
        CancellationToken ct = default)
    {
        int newId = RangeSplitter.ComputeNextPartitionId(rangeMapStore.Current);

        RaftPartitionLifecycleResult createResult;
        try
        {
            createResult = await raft.CreatePartitionAsync(newId, RaftRoutingMode.Unrouted, null, ct);
        }
        catch (RaftException ex)
        {
            // CreatePartitionAsync throws when this node is not the system-partition leader.
            // Return a clean failure so the caller can re-resolve the leader and retry.
            logger.LogWarning(ex, "ForceSplitAtKeyAsync: CreatePartitionAsync({Id}) threw for {Space}/{Key}", newId, keySpace, splitKey);
            return SplitOutcome.PartitionCreationFailed;
        }

        if (!createResult.Success)
        {
            logger.LogWarning("ForceSplitAtKeyAsync: CreatePartitionAsync({Id}) failed for {Space}/{Key}", newId, keySpace, splitKey);
            return SplitOutcome.PartitionCreationFailed;
        }

        return await rangeSplitter!.SplitAsync(keySpace, splitKey, newId, duringQuiesce, ct);
    }

    private IActorRef<BalancingActor<KeyValuePhaseTwoActor, KeyValuePhaseTwoRequest>, KeyValuePhaseTwoRequest> GetPhaseTwoRouter(
        KahunaConfiguration configuration
    )
    {
        for (int i = 0; i < configuration.KeyValueWorkers; i++)
            phaseTwoWorkers.Add(actorSystem.Spawn<KeyValuePhaseTwoActor, KeyValuePhaseTwoRequest>(
                "phasetwo-keyvalue-" + i,
                raft,
                logger
            ));

        return actorSystem.Spawn<BalancingActor<KeyValuePhaseTwoActor, KeyValuePhaseTwoRequest>, KeyValuePhaseTwoRequest>(null, phaseTwoWorkers);
    }

    /// <summary>
    /// The first phase-two worker instance, for test injection (e.g. setting
    /// <see cref="KeyValuePhaseTwoActor.BeforeRaftCallHook"/>). With <c>KeyValueWorkers = 1</c> it is the
    /// only worker, so a gated commit is deterministic. Null before the router is built.
    /// </summary>
    internal KeyValuePhaseTwoActor? FirstPhaseTwoWorker =>
        phaseTwoWorkers.Count > 0 ? phaseTwoWorkers[0].Runner.Actor as KeyValuePhaseTwoActor : null;

    /// <summary>
    /// Groups participants by their resolved Raft partition <b>and</b> routed fence generation (via the same
    /// <see cref="RangeRouting.Locate"/> used by per-key routing, so grouping cannot drift). This is the seam
    /// for partition-batched 2PC: every key in one group shares both a partition and a generation, so the
    /// group can be proposed with a single ticket and one <c>expectedGeneration</c> that is correct for all its
    /// keys — never applying one participant's generation to another. The common bulk write (one key space)
    /// forms a single group; the rare case where distinct generations map to one physical partition (a hash
    /// key at generation 0 alongside a key-range key, or two descriptors) splits into one group per generation
    /// so each is fenced correctly, at the cost of one extra proposal for that partition. Grouping is by
    /// partition, not leader: one node can lead several partitions, and a batched <c>ReplicateLogs</c> takes a
    /// single partition id.
    /// </summary>
    internal static Dictionary<(int PartitionId, long Generation), List<T>> GroupByPartition<T>(
        KeySpaceRegistry registry,
        RangeMap rangeMap,
        DataPartitionRouter dataPartitionRouter,
        IEnumerable<T> items,
        Func<T, string> keyOf)
    {
        Dictionary<(int PartitionId, long Generation), List<T>> groups = [];

        foreach (T item in items)
        {
            (int partitionId, long generation) = RangeRouting.Locate(registry, rangeMap, dataPartitionRouter, keyOf(item));
            (int PartitionId, long Generation) groupKey = (partitionId, generation);

            if (groups.TryGetValue(groupKey, out List<T>? group))
                group.Add(item);
            else
                groups[groupKey] = [item];
        }

        return groups;
    }

    /// <summary>
    /// Creates the ephemeral key/values router
    /// </summary>
    /// <param name="backgroundWriter"></param>
    /// <param name="persistence"></param>
    /// <param name="workers"></param>
    /// <returns></returns>
    /// <summary>
    /// Builds the bounded-inbox + priority-control options for a key-value actor. The bound applies
    /// only to ordinary user requests; control messages (<see cref="IsControlRequest"/>) are exempt and
    /// delivered ahead of the backlog so a hot-key flood never strands an in-flight completion.
    /// </summary>
    private static ActorRunnerOptions<KeyValueRequest> BuildActorInboxOptions(KahunaConfiguration configuration) => new()
    {
        MaxInboxSize = configuration.MaxKeyValueActorInboxSize > 0 ? configuration.MaxKeyValueActorInboxSize : null,
        IsControlMessage = IsControlRequest
    };

    /// <summary>
    /// Classifies a request as a priority control message, exempt from the inbox bound. Beyond the
    /// caller-promise-carrying completions (<c>ResumeRead</c>, <c>CompleteProposal</c>,
    /// <c>ReleaseProposal</c>) — which would strand in-flight work if rejected — this also exempts the
    /// internal fire-and-forget/maintenance messages that must never be silently dropped under
    /// backpressure: <c>InvalidateOrApply</c> (follower cache coherence — dropping it yields a stale
    /// read), <c>FlushAck</c> (durability marker), and the periodic <c>Collect</c> / snapshot
    /// <c>GetSafeTimestamp</c>. Everything else is an external user op and is subject to the bound.
    /// </summary>
    internal static bool IsControlRequest(KeyValueRequest request) =>
        request.Type is KeyValueRequestType.ResumeRead
            or KeyValueRequestType.CompleteProposal
            or KeyValueRequestType.ReleaseProposal
            or KeyValueRequestType.CompletePhaseTwo
            or KeyValueRequestType.InvalidateOrApply
            or KeyValueRequestType.FlushAck
            or KeyValueRequestType.Collect
            or KeyValueRequestType.GetSafeTimestamp;

    /// <summary>
    /// Sends a request to a bounded key-value actor router, mapping the actor's inbox-full backpressure
    /// (<see cref="ActorBusyException"/>, thrown when a hot actor is saturated with ordinary requests)
    /// to a retryable <c>MustRetry</c> response. Control messages are exempt from the bound and never
    /// reach this path. The message was never enqueued, so retrying is unconditionally safe.
    /// </summary>
    private static async Task<KeyValueResponse?> AskKeyValueActor(
        IActorRef<ConsistentHashActor<KeyValueActor, KeyValueRequest, KeyValueResponse>, KeyValueRequest, KeyValueResponse> router,
        KeyValueRequest request)
    {
        try
        {
            return await router.Ask(request);
        }
        catch (ActorBusyException)
        {
            return new KeyValueResponse(KeyValueResponseType.MustRetry);
        }
    }

    private IActorRef<ConsistentHashActor<KeyValueActor, KeyValueRequest, KeyValueResponse>, KeyValueRequest, KeyValueResponse> GetEphemeralRouter(
        KahunaConfiguration configuration
    )
    {
        logger.LogStartingEphemeralWorkers(configuration.KeyValueWorkers);

        ActorRunnerOptions<KeyValueRequest> options = BuildActorInboxOptions(configuration);

        for (int i = 0; i < configuration.KeyValueWorkers; i++)
            ephemeralInstances.Add(actorSystem.SpawnWithOptions<KeyValueActor, KeyValueRequest, KeyValueResponse>(
                "ephemeral-keyvalue-" + i,
                options,
                backgroundWriter,
                writeAggregator,
                phaseTwoRouter,
                persistenceBackend,
                raft,
                keySpaceRegistry,
                rangeMapStore,
                configuration,
                logger,
                snapshotFloorStore,
                completionReceiptStore,
                preparedIntentStore,
                transactionRecordStore
            ));

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
        logger.LogStartingPersistentWorkers(configuration.KeyValueWorkers);

        ActorRunnerOptions<KeyValueRequest> options = BuildActorInboxOptions(configuration);

        for (int i = 0; i < configuration.KeyValueWorkers; i++)
            persistentInstances.Add(actorSystem.SpawnWithOptions<KeyValueActor, KeyValueRequest, KeyValueResponse>(
                "persistent-keyvalue-" + i,
                options,
                backgroundWriter,
                writeAggregator,
                phaseTwoRouter,
                persistenceBackend,
                raft,
                keySpaceRegistry,
                rangeMapStore,
                configuration,
                logger,
                snapshotFloorStore,
                completionReceiptStore,
                preparedIntentStore,
                transactionRecordStore
            ));

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
        if (log.LogType == ReplicationTypes.RangeMap)
        {
            bool result = rangeMapStore.Restore(partitionId, log);
            // A replayed range-descriptor entry may introduce key spaces that are not yet in the
            // per-node KeySpaceRegistry. Sync so that routing on this node matches the restored map.
            if (result)
                SyncKeySpaceRegistryFromRangeMap();
            return Task.FromResult(result);
        }

        if (log.LogType == ReplicationTypes.SnapshotFloor)
            return Task.FromResult(snapshotFloorStore.Restore(partitionId, log));

        if (log.LogType == ReplicationTypes.TransactionRecord)
            return Task.FromResult(transactionRecordStore.Restore(partitionId, log));

        if (log.LogType == ReplicationTypes.PreparedIntent)
            return Task.FromResult(preparedIntentStore.Restore(partitionId, log));

        if (log.LogType == ReplicationTypes.CompletionReceipt)
            return Task.FromResult(completionReceiptStore.Restore(partitionId, log));

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
        if (log.LogType == ReplicationTypes.RangeMap)
        {
            bool result = rangeMapStore.Replicate(partitionId, log);
            // Keep the per-node KeySpaceRegistry in sync with every replicated range-descriptor
            // update. Without this, follower nodes that receive a new key-range descriptor via Raft
            // still route the corresponding key space via hash (the default), causing 2PC prepare
            // to be sent to the wrong partition and the transaction to be aborted.
            if (result)
                SyncKeySpaceRegistryFromRangeMap();
            return Task.FromResult(result);
        }

        if (log.LogType == ReplicationTypes.SnapshotFloor)
            return Task.FromResult(snapshotFloorStore.Replicate(partitionId, log));

        if (log.LogType == ReplicationTypes.TransactionRecord)
            return Task.FromResult(transactionRecordStore.Replicate(partitionId, log));

        if (log.LogType == ReplicationTypes.PreparedIntent)
            return Task.FromResult(preparedIntentStore.Replicate(partitionId, log));

        if (log.LogType == ReplicationTypes.CompletionReceipt)
            return Task.FromResult(completionReceiptStore.Replicate(partitionId, log));

        return Task.FromResult(log.LogType != ReplicationTypes.KeyValues || replicator.Replicate(partitionId, log));
    }

    /// <summary>
    /// Registers every key space that has a descriptor in the current range map into the local
    /// <see cref="KeySpaceRegistry"/>. This is the missing link between the replicated/restored
    /// range-descriptor map and the per-node routing-mode table: the map is shared across the
    /// cluster via Raft, but the registry is node-local and must be kept in sync explicitly.
    /// <para>
    /// Any key space present in the range map was placed there through <see cref="MutateAsync"/>,
    /// which in turn requires passing through <see cref="KeySpaceRegistry.RegisterKeyRange"/> on
    /// the seeding node. That path already enforces all validity rules (e.g. non-empty, not a
    /// reserved space). Re-applying those rules here would duplicate consumer-specific knowledge
    /// inside Kahuna; instead we call <c>RegisterKeyRange</c> unconditionally and let it enforce
    /// its own invariants if a descriptor ever arrives with an invalid key space.
    /// </para>
    /// </summary>
    private void SyncKeySpaceRegistryFromRangeMap()
    {
        HashSet<string> live = [];

        foreach (RangeDescriptor descriptor in rangeMapStore.Current.Descriptors)
        {
            if (!string.IsNullOrEmpty(descriptor.KeySpace))
                live.Add(descriptor.KeySpace);
        }

        keySpaceRegistry.ReconcileTo(live);
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
    /// Called by Kommander when partition leadership changes.
    ///
    /// <para>Intentionally a no-op. The per-key <c>InvalidateOrApply</c> messages from
    /// <see cref="KeyValueReplicator"/> keep every resident cache entry coherent as committed logs
    /// are applied on followers. Correctness at promotion depends on a node having applied all
    /// committed entries up to its commit frontier <em>before</em> it serves as leader — Kommander
    /// now guarantees exactly that (it drains pending committed applies, and applies entries it
    /// commits via the leader path, before advertising the node as leader). With that guarantee a
    /// newly promoted leader's cache is already current, so no additional sweep or serving gate is
    /// needed here. The promotion-races-apply invariant is exercised by
    /// <c>PromotedLeader_RacingCommit_ServesLatestRevision</c>.</para>
    /// </summary>
    public Task<bool> OnLeaderChanged(int partitionId, string node)
    {
        if (logger.IsEnabled(LogLevel.Debug))
            logger.LogDebug("KeyValues: leader for partition {PartitionId} is now {Node}", partitionId, node);

        return Task.FromResult(true);
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
        CancellationToken cancellationToken,
        long routedGeneration = 0,
        string coordinatorKey = "",
        TransactionOperationId operationId = default
    )
    {
        RegistrationRouting routing = ClassifyRegistration(transactionId, coordinatorKey, operationId);
        if (routing is RegistrationRouting.Legacy)
            return locator.LocateAndTrySetKeyValue(transactionId, key, value, compareValue, compareRevision, flags, expiresMs, durability, cancellationToken, routedGeneration);
        if (routing is RegistrationRouting.Malformed)
            return Task.FromResult((KeyValueResponseType.InvalidInput, 0L, HLCTimestamp.Zero));

        byte[] digest = OperationDigest.ForSet(key, value, compareValue, compareRevision, flags, expiresMs, durability);

        return RegisterAndTrySetKeyValue(transactionId, coordinatorKey, operationId, key, value, compareValue, compareRevision, flags, expiresMs, durability, digest, routedGeneration, cancellationToken);
    }

    /// <summary>
    /// Register-remote wrapper for a transaction-scoped write: registers the operation on the
    /// coordinator (dedup guard), applies the write on the key's leader only when the registration is
    /// new, then records the confirmed effect and caches the response. A retry carrying the same
    /// operation id replays the cached response instead of applying the write twice.
    /// </summary>
    private async Task<(KeyValueResponseType, long, HLCTimestamp)> RegisterAndTrySetKeyValue(
        HLCTimestamp transactionId, string coordinatorKey, TransactionOperationId operationId, string key,
        byte[]? value, byte[]? compareValue, long compareRevision, KeyValueFlags flags, int expiresMs,
        KeyValueDurability durability, byte[] digest, long routedGeneration, CancellationToken cancellationToken)
    {
        (OperationRegistrationOutcome outcome, KeyValueResponseType cachedType, long cachedRevision, HLCTimestamp cachedTimestamp, _) =
            await LocateAndBeginOperation(coordinatorKey, transactionId, operationId, OperationKind.Set, digest, cancellationToken);

        switch (outcome)
        {
            case OperationRegistrationOutcome.AlreadyCompleted:
                participantOperationCache.Remove(transactionId, operationId);
                return (cachedType, cachedRevision, cachedTimestamp);
            case OperationRegistrationOutcome.AlreadyPending:
                if (await TryRecoverRegisteredOperation(coordinatorKey, transactionId, operationId) is { } recovered)
                    return ((KeyValueResponseType, long, HLCTimestamp))recovered;
                return (KeyValueResponseType.MustRetry, 0, HLCTimestamp.Zero);
            case OperationRegistrationOutcome.RejectedCapacity:
                return (KeyValueResponseType.MustRetry, 0, HLCTimestamp.Zero);
            case OperationRegistrationOutcome.RejectedSessionBudget:
                return (KeyValueResponseType.Aborted, 0, HLCTimestamp.Zero);
            case OperationRegistrationOutcome.RejectedSessionClosed:
                return (KeyValueResponseType.Aborted, 0, HLCTimestamp.Zero);
            case OperationRegistrationOutcome.RejectedDuplicate:
                return (KeyValueResponseType.Errored, 0, HLCTimestamp.Zero);
        }

        (KeyValueResponseType type, long revision, HLCTimestamp lastModified) =
            await locator.LocateAndTrySetKeyValue(transactionId, key, value, compareValue, compareRevision, flags, expiresMs, durability, cancellationToken, routedGeneration);

        // Only a confirmed set records a modified key + its implicit point lock into the working set.
        bool applied = type == KeyValueResponseType.Set;

        (KeyValueResponseType, long, HLCTimestamp) response = (type, revision, lastModified);

        // Cache the result and drive the completion; if it is not acknowledged, surface MustRetry so a
        // same-id retry recovers the confirmed effect without applying the set a second time.
        if (!await CompleteRegisteredOperation(
                coordinatorKey, transactionId, operationId, response,
                new OperationCompletionPayload
                {
                    ModifiedKey = applied ? key : null,
                    AcquiredPointLock = applied ? key : null,
                    // Stage the confirmed value for a persistent write so the coordinator finalizes durably,
                    // carrying the NoRevision flag so a registered SET NOREV materializes revision-free.
                    StagedMutations = applied && durability == KeyValueDurability.Persistent
                        ? [new StagedMutationEffect(key, value, revision, expiresMs, (flags & KeyValueFlags.SetNoRevision) != 0)]
                        : null,
                    Durability = durability,
                    CachedType = type,
                    CachedRevision = revision,
                    CachedTimestamp = lastModified
                }))
            return (KeyValueResponseType.MustRetry, 0, HLCTimestamp.Zero);

        return response;
    }

    /// <summary>
    /// Attempts to locate and set multiple key-value pairs in the system.
    /// </summary>
    /// <param name="setManyItems">A collection of key-value set requests to be processed.</param>
    /// <param name="cancellationToken">Token to signal cancellation of the operation.</param>
    /// <returns>A task that represents the asynchronous operation, containing a list of responses for the key-value set requests.</returns>
    public Task<List<KahunaSetKeyValueResponseItem>> LocateAndTrySetManyKeyValue(
        List<KahunaSetKeyValueRequestItem> setManyItems,
        CancellationToken cancellationToken,
        string coordinatorKey = "",
        TransactionOperationId operationId = default
    )
    {
        // The whole batch registers as a single coordinator operation so its confirmed persistent keys fold
        // in canonical request order and anchor the transaction record deterministically. Fall back to the
        // unregistered fan-out when the caller supplied no operation identity (non-transactional path).
        HLCTimestamp transactionId = setManyItems.Count > 0 ? setManyItems[0].TransactionId : HLCTimestamp.Zero;

        RegistrationRouting routing = ClassifyRegistration(transactionId, coordinatorKey, operationId);
        if (routing is RegistrationRouting.Legacy)
            return locator.LocateAndTrySetManyKeyValue(setManyItems, cancellationToken);
        if (routing is RegistrationRouting.Malformed)
            return Task.FromResult(AllSetItemsResponse(setManyItems, KeyValueResponseType.InvalidInput));

        return RegisterAndTrySetManyKeyValue(transactionId, coordinatorKey, operationId, setManyItems, cancellationToken);
    }

    private async Task<List<KahunaSetKeyValueResponseItem>> RegisterAndTrySetManyKeyValue(
        HLCTimestamp transactionId, string coordinatorKey, TransactionOperationId operationId,
        List<KahunaSetKeyValueRequestItem> setManyItems, CancellationToken cancellationToken)
    {
        (OperationRegistrationOutcome outcome, _, _, _, _) =
            await LocateAndBeginOperation(coordinatorKey, transactionId, operationId, OperationKind.SetMany, OperationDigest.ForSetMany(setManyItems), cancellationToken);


        switch (outcome)
        {
            case OperationRegistrationOutcome.AlreadyPending:
                // A batch stuck pending from a lost completion cannot re-register as New, so re-driving is
                // impossible here; recover the confirmed effect from the participant cache instead.
                if (await TryRecoverRegisteredOperation(coordinatorKey, transactionId, operationId) is { } recovered)
                    return (List<KahunaSetKeyValueResponseItem>)recovered;
                return AllSetItemsResponse(setManyItems, KeyValueResponseType.MustRetry);
            case OperationRegistrationOutcome.RejectedCapacity:
                return AllSetItemsResponse(setManyItems, KeyValueResponseType.MustRetry);
            case OperationRegistrationOutcome.RejectedSessionBudget:
                return AllSetItemsResponse(setManyItems, KeyValueResponseType.Aborted);
            case OperationRegistrationOutcome.RejectedSessionClosed:
                return AllSetItemsResponse(setManyItems, KeyValueResponseType.Aborted);
            case OperationRegistrationOutcome.RejectedDuplicate:
                return AllSetItemsResponse(setManyItems, KeyValueResponseType.Errored);
            // New and AlreadyCompleted both execute: a batch is re-driven on retry rather than replaying a
            // cached list, and CompleteOperation is a no-op if it already completed. Re-driving is safe here
            // because CamusDB reissues the identical batch under the same operation id; the coordinator folds
            // the confirmed keys only once via the completion below.
        }

        List<KahunaSetKeyValueResponseItem> responses =
            await locator.LocateAndTrySetManyKeyValue(setManyItems, cancellationToken);

        // Fold the confirmed writes in canonical request order (fan-out returns them unordered), so the first
        // persistent key deterministically anchors the transaction record. Only a genuine Set is a confirmed
        // write; a failed SetIfNotExists (NotSet) or a transient (MustRetry) item folds nothing.
        //
        // Unlike delete-many, this does NOT cancel the registration when some items come back transient. The
        // caller (a mass writer) resends only the transient keys on retry — never the already-written ones,
        // whose intents now exist — so a "cancel then re-drive the whole batch under the same id" contract
        // would re-issue a SetIfNotExists for an already-set unique key and get a false NotSet. Instead the
        // confirmed keys fold now, the operation completes, and the caller retries the transient keys as a
        // fresh operation. A confirmed write is idempotent under participant-cache replay of the same id.
        Dictionary<string, (KeyValueResponseType Type, long Revision)> byKey = new(responses.Count, StringComparer.Ordinal);
        foreach (KahunaSetKeyValueResponseItem response in responses)
            byKey[response.Key ?? ""] = (response.Type, response.Revision);

        List<(string, KeyValueDurability)> modifiedKeys = [];
        List<StagedMutationEffect> stagedMutations = [];
        foreach (KahunaSetKeyValueRequestItem item in setManyItems)
        {
            if (byKey.TryGetValue(item.Key ?? "", out (KeyValueResponseType Type, long Revision) result) && result.Type == KeyValueResponseType.Set)
            {
                modifiedKeys.Add((item.Key ?? "", item.Durability));

                // Stage the confirmed value + new revision for each persistent write so the coordinator finalizes
                // the batch through the durable-intent path instead of the manual ticket path.
                if (item.Durability == KeyValueDurability.Persistent)
                    stagedMutations.Add(new StagedMutationEffect(item.Key ?? "", item.Value, result.Revision, item.ExpiresMs, (item.Flags & KeyValueFlags.SetNoRevision) != 0));
            }
        }

        // Cache the batch result and drive the completion off the caller token; if it is not acknowledged,
        // surface MustRetry so a same-id retry recovers the confirmed effect without re-writing.

        if (!await CompleteRegisteredOperation(
                coordinatorKey, transactionId, operationId, responses,
                new OperationCompletionPayload
                {
                    ModifiedKeys = modifiedKeys.Count > 0 ? modifiedKeys : null,
                    StagedMutations = stagedMutations.Count > 0 ? stagedMutations : null,
                    // A confirmed write folds its implicit point lock, exactly as the single-key path does. An
                    // optimistic transaction takes no explicit locks, so this is its only source of the held-lock
                    // set that PrepareMutations requires; for a pessimistic caller the explicit lock already
                    // recorded these keys, so the HashSet fold is idempotent.
                    AcquiredPointLocks = modifiedKeys.Count > 0 ? modifiedKeys : null,
                    // A batch that confirmed at least one write completes terminally so its effect folds. A
                    // batch that confirmed nothing must NOT be cached as a terminal success — that would let a
                    // same-id retry replay the false success forever instead of re-registering. Mark it
                    // transient so the completion cancels the registration and a same-id retry re-executes.
                    CachedType = modifiedKeys.Count > 0 ? KeyValueResponseType.Set : KeyValueResponseType.MustRetry
                }))
            return AllSetItemsResponse(setManyItems, KeyValueResponseType.MustRetry);

        return responses;
    }

    private static List<KahunaSetKeyValueResponseItem> AllSetItemsResponse(
        List<KahunaSetKeyValueRequestItem> items, KeyValueResponseType type)
    {
        List<KahunaSetKeyValueResponseItem> responses = new(items.Count);
        foreach (KahunaSetKeyValueRequestItem item in items)
            responses.Add(new()
            {
                Key = item.Key ?? "",
                Durability = item.Durability,
                Type = type
            });
        return responses;
    }

    public Task<List<KahunaDeleteKeyValueResponseItem>> LocateAndTryDeleteManyKeyValue(
        List<KahunaDeleteKeyValueRequestItem> deleteManyItems,
        CancellationToken cancellationToken,
        string coordinatorKey = "",
        TransactionOperationId operationId = default
    )
    {
        // The whole batch registers as a single coordinator operation so its confirmed persistent keys
        // fold in canonical request order and anchor the transaction record deterministically. Fall back to
        // the unregistered fan-out when the caller supplied no operation identity (non-transactional path).
        HLCTimestamp transactionId = deleteManyItems.Count > 0 ? deleteManyItems[0].TransactionId : HLCTimestamp.Zero;

        RegistrationRouting routing = ClassifyRegistration(transactionId, coordinatorKey, operationId);
        if (routing is RegistrationRouting.Legacy)
            return locator.LocateAndTryDeleteManyKeyValue(deleteManyItems, cancellationToken);
        if (routing is RegistrationRouting.Malformed)
            return Task.FromResult(AllItemsResponse(deleteManyItems, KeyValueResponseType.InvalidInput));

        return RegisterAndTryDeleteManyKeyValue(transactionId, coordinatorKey, operationId, deleteManyItems, cancellationToken);
    }

    private async Task<List<KahunaDeleteKeyValueResponseItem>> RegisterAndTryDeleteManyKeyValue(
        HLCTimestamp transactionId, string coordinatorKey, TransactionOperationId operationId,
        List<KahunaDeleteKeyValueRequestItem> deleteManyItems, CancellationToken cancellationToken)
    {
        List<(string Key, KeyValueDurability Durability)> canonicalItems = new(deleteManyItems.Count);
        foreach (KahunaDeleteKeyValueRequestItem item in deleteManyItems)
            canonicalItems.Add((item.Key ?? "", item.Durability));

        (OperationRegistrationOutcome outcome, _, _, _, _) =
            await LocateAndBeginOperation(coordinatorKey, transactionId, operationId, OperationKind.DeleteMany, OperationDigest.ForDeleteMany(canonicalItems), cancellationToken);

        switch (outcome)
        {
            case OperationRegistrationOutcome.AlreadyPending:
                // A batch stuck pending from a lost completion cannot re-register as New, so re-driving
                // is impossible here; recover the confirmed effect from the participant cache instead.
                if (await TryRecoverRegisteredOperation(coordinatorKey, transactionId, operationId) is { } recovered)
                    return (List<KahunaDeleteKeyValueResponseItem>)recovered;
                return AllItemsResponse(deleteManyItems, KeyValueResponseType.MustRetry);
            case OperationRegistrationOutcome.RejectedCapacity:
                return AllItemsResponse(deleteManyItems, KeyValueResponseType.MustRetry);
            case OperationRegistrationOutcome.RejectedSessionBudget:
                return AllItemsResponse(deleteManyItems, KeyValueResponseType.Aborted);
            case OperationRegistrationOutcome.RejectedSessionClosed:
                return AllItemsResponse(deleteManyItems, KeyValueResponseType.Aborted);
            case OperationRegistrationOutcome.RejectedDuplicate:
                return AllItemsResponse(deleteManyItems, KeyValueResponseType.Errored);
            // New and AlreadyCompleted both execute: a batch is re-driven on retry (deletes are idempotent)
            // rather than replaying a cached list, and CompleteOperation is a no-op if it already completed.
        }

        List<KahunaDeleteKeyValueResponseItem> responses =
            await locator.LocateAndTryDeleteManyKeyValue(deleteManyItems, cancellationToken);

        // Fold the confirmed deletes in canonical request order (fan-out returns them unordered), so the
        // first persistent key deterministically anchors the transaction record. A transient (MustRetry) item
        // folds nothing. The registration is NOT cancelled on a partial-transient batch: the caller (a mass
        // deleter) resends only the transient keys on retry as a fresh operation, so the confirmed keys must
        // fold now rather than be discarded for a full re-drive that never comes.
        Dictionary<(string, KeyValueDurability), (KeyValueResponseType Type, long Revision)> byKey = new(responses.Count);
        foreach (KahunaDeleteKeyValueResponseItem response in responses)
            byKey[(response.Key ?? "", response.Durability)] = (response.Type, response.Revision);

        List<(string, KeyValueDurability)> modifiedKeys = [];
        List<StagedMutationEffect> stagedMutations = [];
        foreach ((string Key, KeyValueDurability Durability) item in canonicalItems)
        {
            if (byKey.TryGetValue(item, out (KeyValueResponseType Type, long Revision) result) && result.Type == KeyValueResponseType.Deleted)
            {
                modifiedKeys.Add(item);

                // Stage the tombstone (null value) + new revision for each persistent delete so the coordinator
                // finalizes the batch through the durable-intent path.
                if (item.Durability == KeyValueDurability.Persistent)
                    stagedMutations.Add(new StagedMutationEffect(item.Key, null, result.Revision, 0, NoRevision: false));
            }
        }

        // Cache the batch result and drive the completion off the caller token; if it is not acknowledged,
        // surface MustRetry so a same-id retry recovers the confirmed effect without re-deleting.
        if (!await CompleteRegisteredOperation(
                coordinatorKey, transactionId, operationId, responses,
                new OperationCompletionPayload
                {
                    ModifiedKeys = modifiedKeys.Count > 0 ? modifiedKeys : null,
                    StagedMutations = stagedMutations.Count > 0 ? stagedMutations : null,
                    // A confirmed delete folds its implicit point lock, exactly as the single-key path does. An
                    // optimistic transaction takes no explicit locks, so this is its only source of the held-lock
                    // set that PrepareMutations requires; for a pessimistic caller the explicit lock already
                    // recorded these keys, so the HashSet fold is idempotent.
                    AcquiredPointLocks = modifiedKeys.Count > 0 ? modifiedKeys : null,
                    // A batch that confirmed at least one delete completes terminally so its effect folds. A
                    // batch that confirmed nothing must NOT be cached as a terminal success — that would let a
                    // same-id retry replay the false success forever instead of re-registering. Mark it
                    // transient so the completion cancels the registration and a same-id retry re-executes.
                    CachedType = modifiedKeys.Count > 0 ? KeyValueResponseType.Deleted : KeyValueResponseType.MustRetry
                }))
            return AllItemsResponse(deleteManyItems, KeyValueResponseType.MustRetry);

        return responses;
    }

    private static List<KahunaDeleteKeyValueResponseItem> AllItemsResponse(
        List<KahunaDeleteKeyValueRequestItem> items, KeyValueResponseType type)
    {
        List<KahunaDeleteKeyValueResponseItem> responses = new(items.Count);
        foreach (KahunaDeleteKeyValueRequestItem item in items)
            responses.Add(new()
            {
                Key = item.Key ?? "",
                Type = type,
                Revision = -1,
                LastModified = HLCTimestamp.Zero,
                Durability = item.Durability
            });
        return responses;
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
        RegistrationRouting routing = ClassifyRegistration(transactionId, coordinatorKey, operationId);
        if (routing is RegistrationRouting.Legacy)
            return locator.LocateAndTryGetValue(transactionId, key, revision, readTimestamp, durability, cancellationToken);
        if (routing is RegistrationRouting.Malformed)
            return Task.FromResult((KeyValueResponseType.InvalidInput, (ReadOnlyKeyValueEntry?)null));

        return RegisterAndTryReadValue(OperationKind.Get, transactionId, coordinatorKey, operationId, key, revision, readTimestamp, durability, cancellationToken);
    }

    /// <summary>
    /// Register-remote wrapper for a transaction-scoped read: registers the read for finalize fencing
    /// and records its <c>{exists, revision}</c> observation into the coordinator-owned read set. On a
    /// duplicate operation id the read is re-executed without re-folding: the first-recorded observation
    /// is authoritative for commit validation even if this re-execution returns a newer value.
    /// </summary>
    private async Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> RegisterAndTryReadValue(
        OperationKind kind, HLCTimestamp transactionId, string coordinatorKey, TransactionOperationId operationId, string key,
        long revision, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken)
    {
        byte[] digest = OperationDigest.ForRead(kind, key, revision, readTimestamp, durability);

        (OperationRegistrationOutcome outcome, _, _, _, _) =
            await LocateAndBeginOperation(coordinatorKey, transactionId, operationId, kind, digest, cancellationToken);

        switch (outcome)
        {
            case OperationRegistrationOutcome.AlreadyPending:
            case OperationRegistrationOutcome.RejectedCapacity:
                return (KeyValueResponseType.MustRetry, null);
            case OperationRegistrationOutcome.RejectedSessionBudget:
                return (KeyValueResponseType.Aborted, null);
            case OperationRegistrationOutcome.RejectedSessionClosed:
                return (KeyValueResponseType.Aborted, null);
            case OperationRegistrationOutcome.RejectedDuplicate:
                return (KeyValueResponseType.Errored, null);
        }

        (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = kind == OperationKind.Exists
            ? await locator.LocateAndTryExistsValue(transactionId, key, revision, readTimestamp, durability, cancellationToken)
            : await locator.LocateAndTryGetValue(transactionId, key, revision, readTimestamp, durability, cancellationToken);

        // A re-read under an already-completed id re-executes without re-folding:
        // the first-recorded observation is authoritative for commit validation.
        if (outcome != OperationRegistrationOutcome.New)
            return (type, entry);

        bool exists = entry is not null && type is KeyValueResponseType.Get or KeyValueResponseType.Exists;

        // A snapshot read is pinned to a past timestamp: it owns no live transactional MVCC entry and so
        // contributes no read dependency to validate at commit. It still completes for finalize fencing and
        // idempotent replay, but records no observation into the read set.
        bool snapshotRead = !readTimestamp.IsNull();

        await LocateAndCompleteOperation(
            coordinatorKey, transactionId, operationId,
            new OperationCompletionPayload
            {
                Read = snapshotRead
                    ? null
                    : new KeyValueTransactionReadKey { Key = key, Durability = durability, Exists = exists, Revision = exists ? entry!.Revision : -1 },
                Durability = durability,
                CachedType = type,
                CachedRevision = exists ? entry!.Revision : 0,
                CachedTimestamp = exists ? entry!.LastModified : HLCTimestamp.Zero
            },
            cancellationToken);

        return (type, entry);
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
        RegistrationRouting routing = ClassifyRegistration(transactionId, coordinatorKey, operationId);
        if (routing is RegistrationRouting.Legacy)
            return locator.LocateAndTryExistsValue(transactionId, key, revision, readTimestamp, durability, cancellationToken);
        if (routing is RegistrationRouting.Malformed)
            return Task.FromResult((KeyValueResponseType.InvalidInput, (ReadOnlyKeyValueEntry?)null));

        return RegisterAndTryReadValue(OperationKind.Exists, transactionId, coordinatorKey, operationId, key, revision, readTimestamp, durability, cancellationToken);
    }

    public Task<List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)>> LocateAndTryExistsManyValues(
        HLCTimestamp transactionId,
        HLCTimestamp readTimestamp,
        List<(string key, long revision, KeyValueDurability durability)> keys,
        CancellationToken cancellationToken,
        string coordinatorKey = "",
        TransactionOperationId operationId = default
    )
    {
        RegistrationRouting routing = ClassifyRegistration(transactionId, coordinatorKey, operationId);
        if (routing is RegistrationRouting.Legacy)
            return locator.LocateAndTryExistsManyValues(transactionId, readTimestamp, keys, cancellationToken);
        if (routing is RegistrationRouting.Malformed)
            return Task.FromResult(BuildManyReadRejection(keys, KeyValueResponseType.InvalidInput));

        return RegisterAndTryReadManyValues(OperationKind.ExistsMany, transactionId, coordinatorKey, operationId, readTimestamp, keys, cancellationToken);
    }

    public Task<List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)>> LocateAndTryGetManyValues(
        HLCTimestamp transactionId,
        HLCTimestamp readTimestamp,
        List<(string key, long revision, KeyValueDurability durability)> keys,
        CancellationToken cancellationToken,
        string coordinatorKey = "",
        TransactionOperationId operationId = default
    )
    {
        RegistrationRouting routing = ClassifyRegistration(transactionId, coordinatorKey, operationId);
        if (routing is RegistrationRouting.Legacy)
            return locator.LocateAndTryGetManyValues(transactionId, readTimestamp, keys, cancellationToken);
        if (routing is RegistrationRouting.Malformed)
            return Task.FromResult(BuildManyReadRejection(keys, KeyValueResponseType.InvalidInput));

        return RegisterAndTryReadManyValues(OperationKind.GetMany, transactionId, coordinatorKey, operationId, readTimestamp, keys, cancellationToken);
    }

    /// <summary>
    /// Builds a uniform per-key result list for a batch read that never reached the leaders (malformed
    /// registration, capacity/session rejection). One tuple per requested key carries the shared
    /// <paramref name="type"/> and the key's declared durability so callers see the same shape a real fan-out
    /// would return.
    /// </summary>
    private static List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)> BuildManyReadRejection(
        List<(string key, long revision, KeyValueDurability durability)> keys, KeyValueResponseType type)
    {
        List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)> rejected = new(keys.Count);
        foreach ((string key, _, KeyValueDurability durability) in keys)
            rejected.Add((type, key, durability, null));
        return rejected;
    }

    /// <summary>
    /// Registers a batch point read (GetMany/ExistsMany) as a single coordinator operation so every key it
    /// observes becomes a read dependency of the transaction: an optimistic commit validates them and aborts if
    /// any changed after the read. Mirrors <see cref="RegisterAndTryReadValue"/> but folds one observation per
    /// returned key into <see cref="OperationCompletionPayload.ReadObservations"/>. A snapshot read (pinned read
    /// timestamp) owns no live transactional MVCC and so records no observations, but still registers for
    /// finalize fencing and idempotent replay.
    /// </summary>
    private async Task<List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)>> RegisterAndTryReadManyValues(
        OperationKind kind, HLCTimestamp transactionId, string coordinatorKey, TransactionOperationId operationId,
        HLCTimestamp readTimestamp, List<(string key, long revision, KeyValueDurability durability)> keys,
        CancellationToken cancellationToken)
    {
        byte[] digest = kind == OperationKind.ExistsMany
            ? OperationDigest.ForExistsMany(keys, readTimestamp)
            : OperationDigest.ForGetMany(keys, readTimestamp);

        (OperationRegistrationOutcome outcome, _, _, _, _) =
            await LocateAndBeginOperation(coordinatorKey, transactionId, operationId, kind, digest, cancellationToken);

        switch (outcome)
        {
            case OperationRegistrationOutcome.AlreadyPending:
            case OperationRegistrationOutcome.RejectedCapacity:
                return BuildManyReadRejection(keys, KeyValueResponseType.MustRetry);
            case OperationRegistrationOutcome.RejectedSessionBudget:
                return BuildManyReadRejection(keys, KeyValueResponseType.Aborted);
            case OperationRegistrationOutcome.RejectedSessionClosed:
                return BuildManyReadRejection(keys, KeyValueResponseType.Aborted);
            case OperationRegistrationOutcome.RejectedDuplicate:
                return BuildManyReadRejection(keys, KeyValueResponseType.Errored);
        }

        List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)> result = kind == OperationKind.ExistsMany
            ? await locator.LocateAndTryExistsManyValues(transactionId, readTimestamp, keys, cancellationToken)
            : await locator.LocateAndTryGetManyValues(transactionId, readTimestamp, keys, cancellationToken);

        // A re-read under an already-completed id re-executes without re-folding:
        // the first-recorded observations are authoritative for commit validation.
        if (outcome != OperationRegistrationOutcome.New)
            return result;

        // A snapshot read is pinned to a past timestamp: it owns no live transactional MVCC entry and so
        // contributes no read dependency to validate at commit. It still completes for finalize fencing and
        // idempotent replay, but records no observation into the read set.
        List<KeyValueTransactionReadKey>? observations = null;
        KeyValueResponseType batchCachedType = kind == OperationKind.ExistsMany ? KeyValueResponseType.Exists : KeyValueResponseType.Get;

        if (readTimestamp.IsNull())
        {
            // Fold only confirmed per-key responses: Get (present), Exists (present, exists-check), and
            // DoesNotExist (confirmed absent). Every other type — transients (MustRetry/WaitingForReplication),
            // Errored (dropped actor response), InvalidInput (malformed key) — means the key's state is
            // unknown, not "absent". Folding a non-confirmed result as absent manufactures a false "missing"
            // observation that conflicts with a retry confirming the key present, aborting an otherwise
            // conflict-free optimistic commit. An allowlist (not a transient denylist) stays correct if a
            // future read path introduces another non-confirmed response type.
            observations = new(result.Count);
            bool anyConfirmed = false;

            foreach ((KeyValueResponseType type, string key, KeyValueDurability durability, ReadOnlyKeyValueEntry? entry) in result)
            {
                if (type is not (KeyValueResponseType.Get or KeyValueResponseType.Exists or KeyValueResponseType.DoesNotExist))
                    continue;  // state unknown (transient/errored/invalid), not "absent" — exclude from read set

                anyConfirmed = true;
                bool exists = entry is not null && type is KeyValueResponseType.Get or KeyValueResponseType.Exists;
                observations.Add(new KeyValueTransactionReadKey
                {
                    Key = key,
                    Durability = durability,
                    Exists = exists,
                    Revision = exists ? entry!.Revision : -1
                });
            }

            // An all-transient latest batch has no confirmed observations: cancel the registration so a
            // same-id retry re-registers as New (mirrors the point/write transient-cancel path).
            if (!anyConfirmed)
            {
                observations = null;
                batchCachedType = KeyValueResponseType.MustRetry;
            }
        }

        await LocateAndCompleteOperation(
            coordinatorKey, transactionId, operationId,
            new OperationCompletionPayload
            {
                ReadObservations = observations,
                Durability = keys.Count > 0 ? keys[0].durability : KeyValueDurability.Persistent,
                CachedType = batchCachedType
            },
            cancellationToken);

        return result;
    }
    
    /// <summary>
    /// Locates the leader node for the given key and checks whether a live write intent from another
    /// transaction exists. Used at commit time by optimistic transactions as a write-skew guard.
    /// </summary>
    public Task<KeyValueResponseType> LocateAndTryCheckWriteIntent(
        HLCTimestamp transactionId,
        string key,
        KeyValueDurability durability,
        CancellationToken cancellationToken
    )
    {
        return locator.LocateAndTryCheckWriteIntent(transactionId, key, durability, cancellationToken);
    }

    /// <summary>
    /// Locates the leader node for the given key and executes the TryDelete request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public Task<(KeyValueResponseType, long, HLCTimestamp)> LocateAndTryDeleteKeyValue(HLCTimestamp transactionId, string key, KeyValueDurability durability, CancellationToken cancellationToken, string coordinatorKey = "", TransactionOperationId operationId = default)
    {
        RegistrationRouting routing = ClassifyRegistration(transactionId, coordinatorKey, operationId);
        if (routing is RegistrationRouting.Legacy)
            return locator.LocateAndTryDeleteKeyValue(transactionId, key, durability, cancellationToken);
        if (routing is RegistrationRouting.Malformed)
            return Task.FromResult((KeyValueResponseType.InvalidInput, 0L, HLCTimestamp.Zero));

        return RegisterAndTryDeleteKeyValue(transactionId, coordinatorKey, operationId, key, durability, OperationDigest.ForDelete(key, durability), cancellationToken);
    }

    private async Task<(KeyValueResponseType, long, HLCTimestamp)> RegisterAndTryDeleteKeyValue(
        HLCTimestamp transactionId, string coordinatorKey, TransactionOperationId operationId, string key,
        KeyValueDurability durability, byte[] digest, CancellationToken cancellationToken)
    {
        (OperationRegistrationOutcome outcome, KeyValueResponseType cachedType, long cachedRevision, HLCTimestamp cachedTimestamp, _) =
            await LocateAndBeginOperation(coordinatorKey, transactionId, operationId, OperationKind.Delete, digest, cancellationToken);

        switch (outcome)
        {
            case OperationRegistrationOutcome.AlreadyCompleted:
                participantOperationCache.Remove(transactionId, operationId);
                return (cachedType, cachedRevision, cachedTimestamp);
            case OperationRegistrationOutcome.AlreadyPending:
                if (await TryRecoverRegisteredOperation(coordinatorKey, transactionId, operationId) is { } recovered)
                    return ((KeyValueResponseType, long, HLCTimestamp))recovered;
                return (KeyValueResponseType.MustRetry, 0, HLCTimestamp.Zero);
            case OperationRegistrationOutcome.RejectedCapacity:
                return (KeyValueResponseType.MustRetry, 0, HLCTimestamp.Zero);
            case OperationRegistrationOutcome.RejectedSessionBudget:
                return (KeyValueResponseType.Aborted, 0, HLCTimestamp.Zero);
            case OperationRegistrationOutcome.RejectedSessionClosed:
                return (KeyValueResponseType.Aborted, 0, HLCTimestamp.Zero);
            case OperationRegistrationOutcome.RejectedDuplicate:
                return (KeyValueResponseType.Errored, 0, HLCTimestamp.Zero);
        }

        (KeyValueResponseType type, long revision, HLCTimestamp lastModified) =
            await locator.LocateAndTryDeleteKeyValue(transactionId, key, durability, cancellationToken);

        bool applied = type == KeyValueResponseType.Deleted;

        (KeyValueResponseType, long, HLCTimestamp) response = (type, revision, lastModified);

        if (!await CompleteRegisteredOperation(
                coordinatorKey, transactionId, operationId, response,
                new OperationCompletionPayload
                {
                    ModifiedKey = applied ? key : null,
                    AcquiredPointLock = applied ? key : null,
                    // Stage the tombstone (null value) for a persistent delete so the coordinator finalizes durably.
                    StagedMutations = applied && durability == KeyValueDurability.Persistent
                        ? [new StagedMutationEffect(key, null, revision, 0, NoRevision: false)]
                        : null,
                    Durability = durability,
                    CachedType = type,
                    CachedRevision = revision,
                    CachedTimestamp = lastModified
                }))
            return (KeyValueResponseType.MustRetry, 0, HLCTimestamp.Zero);

        return response;
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
    public Task<(KeyValueResponseType, long, HLCTimestamp)> LocateAndTryExtendKeyValue(HLCTimestamp transactionId, string key, int expiresMs, KeyValueDurability durability, CancellationToken cancellationToken, string coordinatorKey = "", TransactionOperationId operationId = default)
    {
        RegistrationRouting routing = ClassifyRegistration(transactionId, coordinatorKey, operationId);
        if (routing is RegistrationRouting.Legacy)
            return locator.LocateAndTryExtendKeyValue(transactionId, key, expiresMs, durability, cancellationToken);
        if (routing is RegistrationRouting.Malformed)
            return Task.FromResult((KeyValueResponseType.InvalidInput, 0L, HLCTimestamp.Zero));

        return RegisterAndTryExtendKeyValue(transactionId, coordinatorKey, operationId, key, expiresMs, durability, OperationDigest.ForExtend(key, expiresMs, durability), cancellationToken);
    }

    private async Task<(KeyValueResponseType, long, HLCTimestamp)> RegisterAndTryExtendKeyValue(
        HLCTimestamp transactionId, string coordinatorKey, TransactionOperationId operationId, string key,
        int expiresMs, KeyValueDurability durability, byte[] digest, CancellationToken cancellationToken)
    {
        (OperationRegistrationOutcome outcome, KeyValueResponseType cachedType, long cachedRevision, HLCTimestamp cachedTimestamp, _) =
            await LocateAndBeginOperation(coordinatorKey, transactionId, operationId, OperationKind.Extend, digest, cancellationToken);

        switch (outcome)
        {
            case OperationRegistrationOutcome.AlreadyCompleted:
                participantOperationCache.Remove(transactionId, operationId);
                return (cachedType, cachedRevision, cachedTimestamp);
            case OperationRegistrationOutcome.AlreadyPending:
                if (await TryRecoverRegisteredOperation(coordinatorKey, transactionId, operationId) is { } recovered)
                    return ((KeyValueResponseType, long, HLCTimestamp))recovered;
                return (KeyValueResponseType.MustRetry, 0, HLCTimestamp.Zero);
            case OperationRegistrationOutcome.RejectedCapacity:
                return (KeyValueResponseType.MustRetry, 0, HLCTimestamp.Zero);
            case OperationRegistrationOutcome.RejectedSessionBudget:
                return (KeyValueResponseType.Aborted, 0, HLCTimestamp.Zero);
            case OperationRegistrationOutcome.RejectedSessionClosed:
                return (KeyValueResponseType.Aborted, 0, HLCTimestamp.Zero);
            case OperationRegistrationOutcome.RejectedDuplicate:
                return (KeyValueResponseType.Errored, 0, HLCTimestamp.Zero);
        }

        (KeyValueResponseType type, long revision, HLCTimestamp lastModified) =
            await locator.LocateAndTryExtendKeyValue(transactionId, key, expiresMs, durability, cancellationToken);

        bool applied = type == KeyValueResponseType.Extended;

        (KeyValueResponseType, long, HLCTimestamp) response = (type, revision, lastModified);

        if (!await CompleteRegisteredOperation(
                coordinatorKey, transactionId, operationId, response,
                new OperationCompletionPayload
                {
                    ModifiedKey = applied ? key : null,
                    AcquiredPointLock = applied ? key : null,
                    Durability = durability,
                    CachedType = type,
                    CachedRevision = revision,
                    CachedTimestamp = lastModified
                }))
            return (KeyValueResponseType.MustRetry, 0, HLCTimestamp.Zero);

        return response;
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
    public Task<(KeyValueResponseType, string, KeyValueDurability, HLCTimestamp HolderTransactionId)> LocateAndTryAcquireExclusiveLock(
        HLCTimestamp transactionId,
        string key,
        int expiresMs,
        KeyValueDurability durability,
        CancellationToken cancelationToken,
        string coordinatorKey = "",
        TransactionOperationId operationId = default
    )
    {
        RegistrationRouting routing = ClassifyRegistration(transactionId, coordinatorKey, operationId);
        if (routing is RegistrationRouting.Legacy)
            return locator.LocateAndTryAcquireExclusiveLock(transactionId, key, expiresMs, durability, cancelationToken);
        if (routing is RegistrationRouting.Malformed)
            return Task.FromResult((KeyValueResponseType.InvalidInput, key, durability, HLCTimestamp.Zero));

        return RegisterAndAcquireExclusiveLock(transactionId, coordinatorKey, operationId, key, expiresMs, durability, cancelationToken);
    }

    /// <summary>
    /// Register-remote wrapper for a transaction-scoped exclusive point-lock acquire: registers the
    /// operation and, on a confirmed lock, records the held point lock into the coordinator-owned lock
    /// set. A retry under the same operation id reports the lock as already held by this transaction.
    /// </summary>
    private async Task<(KeyValueResponseType, string, KeyValueDurability, HLCTimestamp)> RegisterAndAcquireExclusiveLock(
        HLCTimestamp transactionId, string coordinatorKey, TransactionOperationId operationId, string key,
        int expiresMs, KeyValueDurability durability, CancellationToken cancellationToken)
    {
        (OperationRegistrationOutcome outcome, KeyValueResponseType cachedType, _, _, _) =
            await LocateAndBeginOperation(coordinatorKey, transactionId, operationId, OperationKind.PointLock, OperationDigest.ForPointLockAcquire(key, expiresMs, durability), cancellationToken);

        switch (outcome)
        {
            case OperationRegistrationOutcome.AlreadyCompleted:
                // Replay the exact cached outcome: a first attempt that failed (e.g. AlreadyLocked/Aborted)
                // must not resurface as a successful acquire. The holder is only meaningful on a self-held
                // lock, so it is the transaction id on success and unknown (Zero) on a cached failure.
                participantOperationCache.Remove(transactionId, operationId);
                return (cachedType, key, durability, cachedType == KeyValueResponseType.Locked ? transactionId : HLCTimestamp.Zero);
            case OperationRegistrationOutcome.AlreadyPending:
                if (await TryRecoverRegisteredOperation(coordinatorKey, transactionId, operationId) is { } recovered)
                    return ((KeyValueResponseType, string, KeyValueDurability, HLCTimestamp))recovered;
                return (KeyValueResponseType.MustRetry, key, durability, HLCTimestamp.Zero);
            case OperationRegistrationOutcome.RejectedCapacity:
                return (KeyValueResponseType.MustRetry, key, durability, HLCTimestamp.Zero);
            case OperationRegistrationOutcome.RejectedSessionBudget:
                return (KeyValueResponseType.Aborted, key, durability, HLCTimestamp.Zero);
            case OperationRegistrationOutcome.RejectedSessionClosed:
                return (KeyValueResponseType.Aborted, key, durability, HLCTimestamp.Zero);
            case OperationRegistrationOutcome.RejectedDuplicate:
                return (KeyValueResponseType.Errored, key, durability, HLCTimestamp.Zero);
        }

        (KeyValueResponseType type, string resultKey, KeyValueDurability resultDurability, HLCTimestamp holder) =
            await locator.LocateAndTryAcquireExclusiveLock(transactionId, key, expiresMs, durability, cancellationToken);

        bool acquired = type == KeyValueResponseType.Locked;

        (KeyValueResponseType, string, KeyValueDurability, HLCTimestamp) response = (type, resultKey, resultDurability, holder);

        if (!await CompleteRegisteredOperation(
                coordinatorKey, transactionId, operationId, response,
                new OperationCompletionPayload
                {
                    AcquiredPointLock = acquired ? key : null,
                    Durability = durability,
                    CachedType = type
                }))
            return (KeyValueResponseType.MustRetry, key, durability, HLCTimestamp.Zero);

        return response;
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
        RegistrationRouting routing = ClassifyRegistration(transactionId, coordinatorKey, operationId);
        if (routing is RegistrationRouting.Legacy)
            return locator.LocateAndTryAcquireExclusivePrefixLock(transactionId, prefixKey, expiresMs, durability, cancellationToken);
        if (routing is RegistrationRouting.Malformed)
            return Task.FromResult(KeyValueResponseType.InvalidInput);

        return RegisterAndAcquireExclusivePrefixLock(transactionId, coordinatorKey, operationId, prefixKey, expiresMs, durability, cancellationToken);
    }

    /// <summary>
    /// Register-remote wrapper for a transaction-scoped exclusive prefix-lock acquire: registers the
    /// operation and, on a confirmed lock, records the held prefix lock into the coordinator-owned lock
    /// set. A retry under the same operation id reports the lock as already held by this transaction.
    /// </summary>
    private async Task<KeyValueResponseType> RegisterAndAcquireExclusivePrefixLock(
        HLCTimestamp transactionId, string coordinatorKey, TransactionOperationId operationId, string prefixKey,
        int expiresMs, KeyValueDurability durability, CancellationToken cancellationToken)
    {
        (OperationRegistrationOutcome outcome, KeyValueResponseType cachedType, _, _, _) =
            await LocateAndBeginOperation(coordinatorKey, transactionId, operationId, OperationKind.PrefixLock, OperationDigest.ForPrefixLockAcquire(prefixKey, expiresMs, durability), cancellationToken);

        switch (outcome)
        {
            case OperationRegistrationOutcome.AlreadyCompleted:
                // Replay the exact cached outcome so a first-attempt failure does not resurface as success.
                participantOperationCache.Remove(transactionId, operationId);
                return cachedType;
            case OperationRegistrationOutcome.AlreadyPending:
                if (await TryRecoverRegisteredOperation(coordinatorKey, transactionId, operationId) is { } recovered)
                    return (KeyValueResponseType)recovered;
                return KeyValueResponseType.MustRetry;
            case OperationRegistrationOutcome.RejectedCapacity:
                return KeyValueResponseType.MustRetry;
            case OperationRegistrationOutcome.RejectedSessionBudget:
                return KeyValueResponseType.Aborted;
            case OperationRegistrationOutcome.RejectedSessionClosed:
                return KeyValueResponseType.Aborted;
            case OperationRegistrationOutcome.RejectedDuplicate:
                return KeyValueResponseType.Errored;
        }

        KeyValueResponseType type =
            await locator.LocateAndTryAcquireExclusivePrefixLock(transactionId, prefixKey, expiresMs, durability, cancellationToken);

        bool acquired = type == KeyValueResponseType.Locked;

        if (!await CompleteRegisteredOperation(
                coordinatorKey, transactionId, operationId, type,
                new OperationCompletionPayload
                {
                    AcquiredPrefixLock = acquired ? prefixKey : null,
                    Durability = durability,
                    CachedType = type
                }))
            return KeyValueResponseType.MustRetry;

        return type;
    }

    /// <summary>
    /// Locates the leader node for the given keys and executes the TryAcquireManyExclusiveLocks request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="keys"></param>
    /// <param name="cancelationToken"></param>
    /// <returns></returns>
    public Task<List<(KeyValueResponseType, string, KeyValueDurability, HLCTimestamp HolderTransactionId)>> LocateAndTryAcquireManyExclusiveLocks(
        HLCTimestamp transactionId,
        List<(string key, int expiresMs, KeyValueDurability durability)> keys,
        CancellationToken cancelationToken,
        string coordinatorKey = "",
        TransactionOperationId operationId = default
    )
    {
        // The whole batch registers as one coordinator operation so every acquired point lock folds into the
        // server-owned working set and is released on commit/rollback. Without this an interactive
        // transaction's batch locks are foreign to the coordinator, and a committed batch write reads back as
        // if never written. Fall back to the unregistered fan-out when no operation identity is supplied.
        RegistrationRouting routing = ClassifyRegistration(transactionId, coordinatorKey, operationId);
        if (routing is RegistrationRouting.Legacy)
            return locator.LocateAndTryAcquireManyExclusiveLocks(transactionId, keys, cancelationToken);
        if (routing is RegistrationRouting.Malformed)
            return Task.FromResult(keys.Select(k => (KeyValueResponseType.InvalidInput, k.key, k.durability, HLCTimestamp.Zero)).ToList());

        return RegisterAndTryAcquireManyExclusiveLocks(transactionId, coordinatorKey, operationId, keys, cancelationToken);
    }

    private async Task<List<(KeyValueResponseType, string, KeyValueDurability, HLCTimestamp HolderTransactionId)>> RegisterAndTryAcquireManyExclusiveLocks(
        HLCTimestamp transactionId, string coordinatorKey, TransactionOperationId operationId,
        List<(string key, int expiresMs, KeyValueDurability durability)> keys, CancellationToken cancellationToken)
    {
        (OperationRegistrationOutcome outcome, _, _, _, _) =
            await LocateAndBeginOperation(coordinatorKey, transactionId, operationId, OperationKind.ManyPointLock, OperationDigest.ForManyPointLockAcquire(keys), cancellationToken);

        switch (outcome)
        {
            case OperationRegistrationOutcome.AlreadyPending:
                if (await TryRecoverRegisteredOperation(coordinatorKey, transactionId, operationId) is { } recovered)
                    return (List<(KeyValueResponseType, string, KeyValueDurability, HLCTimestamp)>)recovered;
                return keys.Select(k => (KeyValueResponseType.MustRetry, k.key, k.durability, HLCTimestamp.Zero)).ToList();
            case OperationRegistrationOutcome.RejectedCapacity:
                return keys.Select(k => (KeyValueResponseType.MustRetry, k.key, k.durability, HLCTimestamp.Zero)).ToList();
            case OperationRegistrationOutcome.RejectedSessionBudget:
                return keys.Select(k => (KeyValueResponseType.Aborted, k.key, k.durability, HLCTimestamp.Zero)).ToList();
            case OperationRegistrationOutcome.RejectedSessionClosed:
                return keys.Select(k => (KeyValueResponseType.Aborted, k.key, k.durability, HLCTimestamp.Zero)).ToList();
            case OperationRegistrationOutcome.RejectedDuplicate:
                return keys.Select(k => (KeyValueResponseType.Errored, k.key, k.durability, HLCTimestamp.Zero)).ToList();
        }

        List<(KeyValueResponseType, string, KeyValueDurability, HLCTimestamp HolderTransactionId)> responses =
            await locator.LocateAndTryAcquireManyExclusiveLocks(transactionId, keys, cancellationToken);

        // Fold every confirmed Locked key as a held point lock so commit/rollback release it. A transient
        // (MustRetry) key folds nothing; the caller resends only the transient subset as a fresh operation.
        List<(string, KeyValueDurability)> acquired = [];
        foreach ((KeyValueResponseType type, string key, KeyValueDurability durability, HLCTimestamp _) in responses)
        {
            if (type == KeyValueResponseType.Locked)
                acquired.Add((key, durability));
        }

        if (!await CompleteRegisteredOperation(
                coordinatorKey, transactionId, operationId, responses,
                new OperationCompletionPayload
                {
                    AcquiredPointLocks = acquired.Count > 0 ? acquired : null,
                    // A batch that acquired at least one lock completes terminally so its held locks fold. A
                    // batch that acquired nothing must NOT be cached as a terminal success — that would let a
                    // same-id retry replay the false success forever instead of re-registering. Mark it
                    // transient so the completion cancels the registration and a same-id retry re-executes.
                    CachedType = acquired.Count > 0 ? KeyValueResponseType.Locked : KeyValueResponseType.MustRetry
                }))
            return keys.Select(k => (KeyValueResponseType.MustRetry, k.key, k.durability, HLCTimestamp.Zero)).ToList();

        return responses;
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
    public Task<(KeyValueResponseType, string)> LocateAndTryReleaseExclusiveLock(HLCTimestamp transactionId, string key, KeyValueDurability durability, CancellationToken cancelationToken, string coordinatorKey = "", TransactionOperationId operationId = default)
    {
        RegistrationRouting routing = ClassifyRegistration(transactionId, coordinatorKey, operationId);
        if (routing is RegistrationRouting.Legacy)
            return locator.LocateAndTryReleaseExclusiveLock(transactionId, key, durability, cancelationToken);
        if (routing is RegistrationRouting.Malformed)
            return Task.FromResult((KeyValueResponseType.InvalidInput, key));

        return RegisterAndReleaseExclusiveLock(transactionId, coordinatorKey, operationId, key, durability, cancelationToken);
    }

    /// <summary>
    /// Register-remote wrapper for a transaction-scoped exclusive point-lock release: on a confirmed
    /// release, drops the held point lock from the coordinator-owned lock set so it is not released
    /// again at finalize.
    /// </summary>
    private async Task<(KeyValueResponseType, string)> RegisterAndReleaseExclusiveLock(
        HLCTimestamp transactionId, string coordinatorKey, TransactionOperationId operationId, string key,
        KeyValueDurability durability, CancellationToken cancellationToken)
    {
        (OperationRegistrationOutcome outcome, KeyValueResponseType cachedType, _, _, _) =
            await LocateAndBeginOperation(coordinatorKey, transactionId, operationId, OperationKind.PointLock, OperationDigest.ForPointLockRelease(key, durability), cancellationToken);

        switch (outcome)
        {
            case OperationRegistrationOutcome.AlreadyCompleted:
                // Replay the exact cached outcome so a first-attempt failure does not resurface as success.
                participantOperationCache.Remove(transactionId, operationId);
                return (cachedType, key);
            case OperationRegistrationOutcome.AlreadyPending:
                if (await TryRecoverRegisteredOperation(coordinatorKey, transactionId, operationId) is { } recovered)
                    return ((KeyValueResponseType, string))recovered;
                return (KeyValueResponseType.MustRetry, key);
            case OperationRegistrationOutcome.RejectedCapacity:
                return (KeyValueResponseType.MustRetry, key);
            case OperationRegistrationOutcome.RejectedSessionBudget:
                return (KeyValueResponseType.Aborted, key);
            case OperationRegistrationOutcome.RejectedSessionClosed:
                return (KeyValueResponseType.Aborted, key);
            case OperationRegistrationOutcome.RejectedDuplicate:
                return (KeyValueResponseType.Errored, key);
        }

        (KeyValueResponseType type, string resultKey) =
            await locator.LocateAndTryReleaseExclusiveLock(transactionId, key, durability, cancellationToken);

        bool released = type == KeyValueResponseType.Unlocked;

        (KeyValueResponseType, string) response = (type, resultKey);

        if (!await CompleteRegisteredOperation(
                coordinatorKey, transactionId, operationId, response,
                new OperationCompletionPayload
                {
                    ReleasedPointLock = released ? key : null,
                    Durability = durability,
                    CachedType = type
                }))
            return (KeyValueResponseType.MustRetry, key);

        return response;
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
    public Task<KeyValueResponseType> LocateAndTryReleaseExclusivePrefixLock(
        HLCTimestamp transactionId,
        string prefixKey,
        KeyValueDurability durability,
        CancellationToken cancellationToken,
        string coordinatorKey = "",
        TransactionOperationId operationId = default
    )
    {
        RegistrationRouting routing = ClassifyRegistration(transactionId, coordinatorKey, operationId);
        if (routing is RegistrationRouting.Legacy)
            return locator.LocateAndTryReleaseExclusivePrefixLock(transactionId, prefixKey, durability, cancellationToken);
        if (routing is RegistrationRouting.Malformed)
            return Task.FromResult(KeyValueResponseType.InvalidInput);

        return RegisterAndReleaseExclusivePrefixLock(transactionId, coordinatorKey, operationId, prefixKey, durability, cancellationToken);
    }

    /// <summary>
    /// Register-remote wrapper for a transaction-scoped exclusive prefix-lock release: on a confirmed
    /// release, drops the held prefix lock from the coordinator-owned lock set so it is not released
    /// again at finalize.
    /// </summary>
    private async Task<KeyValueResponseType> RegisterAndReleaseExclusivePrefixLock(
        HLCTimestamp transactionId, string coordinatorKey, TransactionOperationId operationId, string prefixKey,
        KeyValueDurability durability, CancellationToken cancellationToken)
    {
        (OperationRegistrationOutcome outcome, KeyValueResponseType cachedType, _, _, _) =
            await LocateAndBeginOperation(coordinatorKey, transactionId, operationId, OperationKind.PrefixLock, OperationDigest.ForPrefixLockRelease(prefixKey, durability), cancellationToken);

        switch (outcome)
        {
            case OperationRegistrationOutcome.AlreadyCompleted:
                // Replay the exact cached outcome so a first-attempt failure does not resurface as success.
                participantOperationCache.Remove(transactionId, operationId);
                return cachedType;
            case OperationRegistrationOutcome.AlreadyPending:
                if (await TryRecoverRegisteredOperation(coordinatorKey, transactionId, operationId) is { } recovered)
                    return (KeyValueResponseType)recovered;
                return KeyValueResponseType.MustRetry;
            case OperationRegistrationOutcome.RejectedCapacity:
                return KeyValueResponseType.MustRetry;
            case OperationRegistrationOutcome.RejectedSessionBudget:
                return KeyValueResponseType.Aborted;
            case OperationRegistrationOutcome.RejectedSessionClosed:
                return KeyValueResponseType.Aborted;
            case OperationRegistrationOutcome.RejectedDuplicate:
                return KeyValueResponseType.Errored;
        }

        KeyValueResponseType type =
            await locator.LocateAndTryReleaseExclusivePrefixLock(transactionId, prefixKey, durability, cancellationToken);

        bool released = type == KeyValueResponseType.Unlocked;

        if (!await CompleteRegisteredOperation(
                coordinatorKey, transactionId, operationId, type,
                new OperationCompletionPayload
                {
                    ReleasedPrefixLock = released ? prefixKey : null,
                    Durability = durability,
                    CachedType = type
                }))
            return KeyValueResponseType.MustRetry;

        return type;
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
    )
    {
        RegistrationRouting routing = ClassifyRegistration(transactionId, coordinatorKey, operationId);
        if (routing is RegistrationRouting.Legacy)
            return locator.LocateAndTryAcquireRangeLock(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, expiresMs, durability, mode, cancellationToken);
        if (routing is RegistrationRouting.Malformed)
            return Task.FromResult((KeyValueResponseType.InvalidInput, HLCTimestamp.Zero));

        return RegisterAndAcquireRangeLock(transactionId, coordinatorKey, operationId, prefix, startKey, startInclusive, endKey, endInclusive, expiresMs, durability, mode, cancellationToken);
    }

    /// <summary>
    /// Register-remote wrapper for a transaction-scoped range-lock acquire, upgrade, or renewal: registers
    /// the operation and, on a confirmed lock, records the range descriptor (bounds + mode) into the
    /// coordinator-owned lock set. A confirmed re-acquire at a different mode replaces the held mode, so a
    /// shared→exclusive upgrade or heartbeat renewal does not leave a duplicate descriptor.
    /// </summary>
    private async Task<(KeyValueResponseType, HLCTimestamp)> RegisterAndAcquireRangeLock(
        HLCTimestamp transactionId, string coordinatorKey, TransactionOperationId operationId, string prefix,
        string? startKey, bool startInclusive, string? endKey, bool endInclusive, int expiresMs,
        KeyValueDurability durability, RangeLockMode mode, CancellationToken cancellationToken,
        Func<Task>? afterSnapshot = null)
    {
        (OperationRegistrationOutcome outcome, KeyValueResponseType cachedType, _, _, _) =
            await LocateAndBeginOperation(coordinatorKey, transactionId, operationId, OperationKind.RangeLock,
                OperationDigest.ForRangeLockAcquire(prefix, startKey, startInclusive, endKey, endInclusive, mode, expiresMs, durability), cancellationToken);

        switch (outcome)
        {
            case OperationRegistrationOutcome.AlreadyCompleted:
                // Replay the exact cached outcome: a first attempt that failed must not resurface as a
                // successful acquire. The holder is the transaction id on success, unknown (Zero) otherwise.
                participantOperationCache.Remove(transactionId, operationId);
                return (cachedType, cachedType == KeyValueResponseType.Locked ? transactionId : HLCTimestamp.Zero);
            case OperationRegistrationOutcome.AlreadyPending:
                if (await TryRecoverRegisteredOperation(coordinatorKey, transactionId, operationId) is { } recovered)
                    return ((KeyValueResponseType, HLCTimestamp))recovered;
                return (KeyValueResponseType.MustRetry, HLCTimestamp.Zero);
            case OperationRegistrationOutcome.RejectedCapacity:
                return (KeyValueResponseType.MustRetry, HLCTimestamp.Zero);
            case OperationRegistrationOutcome.RejectedSessionBudget:
                return (KeyValueResponseType.Aborted, HLCTimestamp.Zero);
            case OperationRegistrationOutcome.RejectedSessionClosed:
                return (KeyValueResponseType.Aborted, HLCTimestamp.Zero);
            case OperationRegistrationOutcome.RejectedDuplicate:
                return (KeyValueResponseType.Errored, HLCTimestamp.Zero);
        }

        (KeyValueResponseType type, HLCTimestamp holder) =
            await locator.LocateAndTryAcquireRangeLock(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, expiresMs, durability, mode, afterSnapshot, cancellationToken);

        bool acquired = type == KeyValueResponseType.Locked;
        RangeLockKey range = new(prefix, startKey, startInclusive, endKey, endInclusive, durability);

        (KeyValueResponseType, HLCTimestamp) response = (type, holder);

        if (!await CompleteRegisteredOperation(
                coordinatorKey, transactionId, operationId, response,
                new OperationCompletionPayload
                {
                    AcquiredRangeLock = acquired ? (range, mode) : null,
                    Durability = durability,
                    CachedType = type
                }))
            return (KeyValueResponseType.MustRetry, HLCTimestamp.Zero);

        return response;
    }

    /// <summary>
    /// Test seam: runs the registered range-lock acquire with a hook fired after the range-map snapshot,
    /// so a split can be injected into the acquire window to exercise the generation fence and its
    /// effect on the coordinator-owned working set.
    /// </summary>
    internal Task<(KeyValueResponseType, HLCTimestamp)> RegisterAndAcquireRangeLockWithHook(
        HLCTimestamp transactionId, string coordinatorKey, TransactionOperationId operationId, string prefix,
        string? startKey, bool startInclusive, string? endKey, bool endInclusive, int expiresMs,
        KeyValueDurability durability, RangeLockMode mode, Func<Task> afterSnapshot, CancellationToken cancellationToken)
        => RegisterAndAcquireRangeLock(transactionId, coordinatorKey, operationId, prefix, startKey, startInclusive, endKey, endInclusive, expiresMs, durability, mode, cancellationToken, afterSnapshot);

    public Task<(KeyValueResponseType, HLCTimestamp HolderTransactionId)> LocateAndTryAcquireExclusiveRangeLock(
        HLCTimestamp transactionId,
        string prefix,
        string? startKey, bool startInclusive,
        string? endKey,   bool endInclusive,
        int expiresMs,
        KeyValueDurability durability,
        CancellationToken cancellationToken
    ) => locator.LocateAndTryAcquireRangeLock(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, expiresMs, durability, RangeLockMode.Exclusive, cancellationToken);

    internal Task<(KeyValueResponseType, HLCTimestamp)> LocateAndTryAcquireExclusiveRangeLockWithHook(
        HLCTimestamp transactionId,
        string prefix,
        string? startKey, bool startInclusive,
        string? endKey,   bool endInclusive,
        int expiresMs,
        KeyValueDurability durability,
        Func<Task> afterSnapshot,
        CancellationToken cancellationToken
    ) => locator.LocateAndTryAcquireExclusiveRangeLock(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, expiresMs, durability, afterSnapshot, cancellationToken);

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
        RegistrationRouting routing = ClassifyRegistration(transactionId, coordinatorKey, operationId);
        if (routing is RegistrationRouting.Legacy)
            return locator.LocateAndTryReleaseExclusiveRangeLock(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, durability, cancellationToken);
        if (routing is RegistrationRouting.Malformed)
            return Task.FromResult(KeyValueResponseType.InvalidInput);

        return RegisterAndReleaseRangeLock(transactionId, coordinatorKey, operationId, prefix, startKey, startInclusive, endKey, endInclusive, durability, cancellationToken);
    }

    /// <summary>
    /// Register-remote wrapper for a transaction-scoped range-lock release: on a confirmed release, drops
    /// the range descriptor from the coordinator-owned lock set so it is not released again at finalize.
    /// </summary>
    private async Task<KeyValueResponseType> RegisterAndReleaseRangeLock(
        HLCTimestamp transactionId, string coordinatorKey, TransactionOperationId operationId, string prefix,
        string? startKey, bool startInclusive, string? endKey, bool endInclusive,
        KeyValueDurability durability, CancellationToken cancellationToken)
    {
        (OperationRegistrationOutcome outcome, KeyValueResponseType cachedType, _, _, _) =
            await LocateAndBeginOperation(coordinatorKey, transactionId, operationId, OperationKind.RangeLock,
                OperationDigest.ForRangeLockRelease(prefix, startKey, startInclusive, endKey, endInclusive, durability), cancellationToken);

        switch (outcome)
        {
            case OperationRegistrationOutcome.AlreadyCompleted:
                // Replay the exact cached outcome so a first-attempt failure does not resurface as success.
                participantOperationCache.Remove(transactionId, operationId);
                return cachedType;
            case OperationRegistrationOutcome.AlreadyPending:
                if (await TryRecoverRegisteredOperation(coordinatorKey, transactionId, operationId) is { } recovered)
                    return (KeyValueResponseType)recovered;
                return KeyValueResponseType.MustRetry;
            case OperationRegistrationOutcome.RejectedCapacity:
                return KeyValueResponseType.MustRetry;
            case OperationRegistrationOutcome.RejectedSessionBudget:
                return KeyValueResponseType.Aborted;
            case OperationRegistrationOutcome.RejectedSessionClosed:
                return KeyValueResponseType.Aborted;
            case OperationRegistrationOutcome.RejectedDuplicate:
                return KeyValueResponseType.Errored;
        }

        KeyValueResponseType type =
            await locator.LocateAndTryReleaseExclusiveRangeLock(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, durability, cancellationToken);

        bool released = type == KeyValueResponseType.Unlocked;
        RangeLockKey range = new(prefix, startKey, startInclusive, endKey, endInclusive, durability);

        if (!await CompleteRegisteredOperation(
                coordinatorKey, transactionId, operationId, type,
                new OperationCompletionPayload
                {
                    ReleasedRangeLock = released ? range : null,
                    Durability = durability,
                    CachedType = type
                }))
            return KeyValueResponseType.MustRetry;

        return type;
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
        CancellationToken cancelationToken,
        long routedGeneration = 0,
        string? recordAnchorKey = null
    )
    {
        return locator.LocateAndTryPrepareMutations(transactionId, commitId, key, durability, cancelationToken, routedGeneration, recordAnchorKey);
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
        CancellationToken cancelationToken,
        string? recordAnchorKey = null
    )
    {
        return locator.LocateAndTryPrepareManyMutations(transactionId, commitId, keys, cancelationToken, recordAnchorKey);
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
    /// Locates the leader node for the given prefix and executes the GetByBucket request.
    /// </summary>
    /// <param name="prefixedKey"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public Task<KeyValueGetByBucketResult> LocateAndGetByBucket(HLCTimestamp transactionId, string prefixedKey, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken, string coordinatorKey = "", TransactionOperationId operationId = default)
    {
        RegistrationRouting routing = ClassifyRegistration(transactionId, coordinatorKey, operationId);
        if (routing is RegistrationRouting.Legacy)
            return locator.LocateAndGetByBucket(transactionId, prefixedKey, readTimestamp, durability, cancellationToken);
        if (routing is RegistrationRouting.Malformed)
            return Task.FromResult(new KeyValueGetByBucketResult(KeyValueResponseType.InvalidInput, []));

        return RegisterAndGetByBucket(transactionId, coordinatorKey, operationId, prefixedKey, readTimestamp, durability, cancellationToken);
    }

    /// <summary>
    /// Register-remote wrapper for a transaction-scoped bucket scan: registers the operation so it is
    /// fenced against finalize, then records every returned item as a read observation with point-read-set
    /// semantics. The scan asserts only that these exact items were observed at these revisions — not that
    /// no other key matches the bucket predicate.
    /// </summary>
    private async Task<KeyValueGetByBucketResult> RegisterAndGetByBucket(
        HLCTimestamp transactionId, string coordinatorKey, TransactionOperationId operationId, string prefixedKey,
        HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken)
    {
        (OperationRegistrationOutcome outcome, _, _, _, _) =
            await LocateAndBeginOperation(coordinatorKey, transactionId, operationId, OperationKind.Scan,
                OperationDigest.ForScan(prefixedKey, readTimestamp, durability), cancellationToken);

        switch (outcome)
        {
            case OperationRegistrationOutcome.AlreadyCompleted:
            case OperationRegistrationOutcome.AlreadyPending:
            case OperationRegistrationOutcome.RejectedCapacity:
                // A pending or already-completed scan re-executes the read without re-folding observations:
                // the first-recorded observations are authoritative for commit validation even if this
                // re-execution returns a newer value.
                if (outcome == OperationRegistrationOutcome.AlreadyCompleted)
                    return await locator.LocateAndGetByBucket(transactionId, prefixedKey, readTimestamp, durability, cancellationToken);
                return new KeyValueGetByBucketResult(KeyValueResponseType.MustRetry, []);
            case OperationRegistrationOutcome.RejectedSessionBudget:
                return new KeyValueGetByBucketResult(KeyValueResponseType.Aborted, []);
            case OperationRegistrationOutcome.RejectedSessionClosed:
                return new KeyValueGetByBucketResult(KeyValueResponseType.Aborted, []);
            case OperationRegistrationOutcome.RejectedDuplicate:
                return new KeyValueGetByBucketResult(KeyValueResponseType.Errored, []);
        }

        KeyValueGetByBucketResult result =
            await locator.LocateAndGetByBucket(transactionId, prefixedKey, readTimestamp, durability, cancellationToken);

        // A snapshot scan is pinned to a past timestamp and owns no live transactional MVCC, so its items
        // contribute no read dependencies; it still completes for finalize fencing and idempotent replay.
        List<KeyValueTransactionReadKey>? observations = null;
        if (readTimestamp.IsNull() && result.Type == KeyValueResponseType.Get && result.Items.Count > 0)
        {
            observations = new(result.Items.Count);
            foreach ((string itemKey, ReadOnlyKeyValueEntry entry) in result.Items)
                observations.Add(new KeyValueTransactionReadKey
                {
                    Key = itemKey,
                    Durability = durability,
                    Exists = entry.State == KeyValueState.Set,
                    Revision = entry.Revision
                });
        }

        await LocateAndCompleteOperation(
            coordinatorKey, transactionId, operationId,
            new OperationCompletionPayload
            {
                ReadObservations = observations,
                Durability = durability,
                CachedType = result.Type
            },
            cancellationToken);

        return result;
    }

    internal Task<KeyValueGetByBucketResult> LocateAndGetByBucketWithHooks(
        HLCTimestamp transactionId, string prefixedKey, KeyValueDurability durability,
        Func<int, Task>? beforeQuery, Func<int, Task>? afterDescriptor,
        CancellationToken cancellationToken) =>
        locator.LocateAndGetByBucket(transactionId, prefixedKey, HLCTimestamp.Zero, durability, beforeQuery, afterDescriptor, cancellationToken);

    public Task<KeyValueGetByRangeResult> LocateAndGetByRange(HLCTimestamp transactionId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive, int limit, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken, string coordinatorKey = "", TransactionOperationId operationId = default)
    {
        if (string.IsNullOrEmpty(coordinatorKey))
            return locator.LocateAndGetByRange(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, limit, readTimestamp, durability, cancellationToken);

        // A single-shot registered scan folds observations exactly when it is a latest read.
        return RegisterAndGetByRange(transactionId, coordinatorKey, operationId, prefix, startKey, startInclusive, endKey, endInclusive, limit, readTimestamp, durability, readTimestamp.IsNull(), cancellationToken);
    }

    /// <summary>
    /// Registers a paged range scan as a coordinator operation so every key it observes becomes a read
    /// dependency of the transaction: an optimistic commit validates them and aborts if any changed after the
    /// scan. Mirrors <see cref="RegisterAndGetByBucket"/>; still registers for finalize fencing and idempotent
    /// replay even when it folds nothing. Paging issues one registered operation per page (distinct bounds →
    /// distinct digest).
    ///
    /// <paramref name="recordObservations"/> is decoupled from <paramref name="readTimestamp"/>: a streaming
    /// latest scan latches a consistent snapshot on its first page and reads pages 1+ <em>as of</em> that pinned
    /// timestamp, but still folds every page's rows as read dependencies. A genuine snapshot scan (the caller
    /// pinned the read timestamp) owns no live transactional MVCC and folds nothing.
    /// </summary>
    private async Task<KeyValueGetByRangeResult> RegisterAndGetByRange(
        HLCTimestamp transactionId, string coordinatorKey, TransactionOperationId operationId, string prefix,
        string? startKey, bool startInclusive, string? endKey, bool endInclusive, int limit,
        HLCTimestamp readTimestamp, KeyValueDurability durability, bool recordObservations, CancellationToken cancellationToken)
    {
        (OperationRegistrationOutcome outcome, _, _, _, _) =
            await LocateAndBeginOperation(coordinatorKey, transactionId, operationId, OperationKind.Scan,
                OperationDigest.ForRangeScan(prefix, startKey, startInclusive, endKey, endInclusive, limit, readTimestamp, durability), cancellationToken);

        switch (outcome)
        {
            case OperationRegistrationOutcome.AlreadyCompleted:
            case OperationRegistrationOutcome.AlreadyPending:
            case OperationRegistrationOutcome.RejectedCapacity:
                // A pending or already-completed scan re-executes the read without re-folding observations:
                // the first-recorded observations are authoritative for commit validation even if this
                // re-execution returns a newer value.
                if (outcome == OperationRegistrationOutcome.AlreadyCompleted)
                    return await locator.LocateAndGetByRange(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, limit, readTimestamp, durability, cancellationToken);
                return new KeyValueGetByRangeResult(KeyValueResponseType.MustRetry, [], null, false);
            case OperationRegistrationOutcome.RejectedSessionBudget:
                return new KeyValueGetByRangeResult(KeyValueResponseType.Aborted, [], null, false);
            case OperationRegistrationOutcome.RejectedSessionClosed:
                return new KeyValueGetByRangeResult(KeyValueResponseType.Aborted, [], null, false);
            case OperationRegistrationOutcome.RejectedDuplicate:
                return new KeyValueGetByRangeResult(KeyValueResponseType.Errored, [], null, false);
        }

        KeyValueGetByRangeResult result =
            await locator.LocateAndGetByRange(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, limit, readTimestamp, durability, cancellationToken);

        List<KeyValueTransactionReadKey>? observations = null;
        if (recordObservations && result.Type == KeyValueResponseType.Get && result.Items.Count > 0)
        {
            observations = new(result.Items.Count);
            foreach ((string itemKey, ReadOnlyKeyValueEntry entry) in result.Items)
                observations.Add(new KeyValueTransactionReadKey
                {
                    Key = itemKey,
                    Durability = durability,
                    Exists = entry.State == KeyValueState.Set,
                    Revision = entry.Revision
                });
        }

        await LocateAndCompleteOperation(
            coordinatorKey, transactionId, operationId,
            new OperationCompletionPayload
            {
                ReadObservations = observations,
                Durability = durability,
                CachedType = result.Type
            },
            cancellationToken);

        return result;
    }

    /// <summary>
    /// Streams all key-value entries under <paramref name="prefix"/> as an <see cref="IAsyncEnumerable{T}"/>.
    /// Pages are fetched via <see cref="LocateAndGetByRange"/>; the snapshot timestamp captured on page 0
    /// is carried in every cursor and reused on each subsequent page for consistent reads.
    /// Transient <see cref="KeyValueResponseType.MustRetry"/> / <see cref="KeyValueResponseType.WaitingForReplication"/>
    /// responses cause the current page to be retried from the same cursor with exponential back-off.
    /// </summary>
    public async IAsyncEnumerable<(string Key, ReadOnlyKeyValueEntry Entry)> LocateAndScanRange(
        HLCTimestamp txId,
        string prefix,
        string? startKey,
        bool startInclusive,
        string? endKey,
        bool endInclusive,
        int pageSize,
        HLCTimestamp readTimestamp,
        KeyValueDurability durability,
        [EnumeratorCancellation] CancellationToken ct,
        string coordinatorKey = "",
        TransactionOperationId operationId = default)
    {
        string? cursorKey       = startKey;
        bool    cursorInclusive = startInclusive;
        // Seed from caller's T when supplied; Zero means "capture on first successful page".
        HLCTimestamp snapshotTs = readTimestamp;
        int backoffMs           = 1;
        // Each streamed page registers as its own coordinator operation (distinct bounds → distinct digest),
        // so it needs a distinct, deterministic operationId derived from the caller's base id and the page
        // number. An empty coordinatorKey means "legacy raw paging" and never registers.
        bool registered         = !string.IsNullOrEmpty(coordinatorKey);
        // Fold every page's rows as read dependencies exactly when the caller asked for a latest scan.
        // A latest scan latches a snapshot on page 0 for a consistent view, so pages 1+ read as of that
        // pinned timestamp — but they are still latest-scan observations and must fold. A genuine snapshot
        // scan (caller pinned readTimestamp) folds nothing.
        bool foldObservations   = readTimestamp.IsNull();
        int pageIndex           = 0;

        while (true)
        {
            ct.ThrowIfCancellationRequested();

            KeyValueGetByRangeResult page = registered
                ? await RegisterAndGetByRange(
                    txId, coordinatorKey, operationId.Derive(pageIndex), prefix,
                    cursorKey, cursorInclusive,
                    endKey, endInclusive,
                    pageSize, snapshotTs, durability, foldObservations, ct)
                : await locator.LocateAndGetByRange(
                    txId, prefix,
                    cursorKey, cursorInclusive,
                    endKey, endInclusive,
                    pageSize, snapshotTs, durability, ct);

            if (page.Type is KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication)
            {
                // On a transient failure at page 0, snapshotTs is still Zero, so the handler
                // will capture a fresh HLC tick on the next attempt.  This means the snapshot
                // instant is "first successful page 0" rather than "scan start."  That is
                // acceptable: MustRetry/WaitingForReplication means the leader wasn't ready yet,
                // so there is no meaningful earlier snapshot to preserve.  Pages 1+ always carry
                // the snapshotTs latched from the first successful page-0 cursor.
                await Task.Delay(backoffMs, ct);
                backoffMs = Math.Min(backoffMs * 2, 1000);
                continue;
            }

            backoffMs = 1;

            if (page.Type != KeyValueResponseType.Get)
                yield break;

            foreach ((string key, ReadOnlyKeyValueEntry entry) in page.Items)
                yield return (key, entry);

            if (!page.HasMore || page.NextCursor is null)
                yield break;

            // Decode cursor: advance past last key and latch the snapshot timestamp.
            if (!KeyValueRangeCursor.TryDecode(page.NextCursor, out string lastKey, out _, out _, out HLCTimestamp cursorTs))
                yield break;

            // If the caller supplied a readTimestamp it's already non-Null, so this
            // no-ops and preserves the caller's T across all pages.
            if (snapshotTs.IsNull())
                snapshotTs = cursorTs;

            cursorKey       = lastKey;
            cursorInclusive = false;
            pageIndex++;
        }
    }

    /// <summary>
    /// Locates the appropriate key-value partition and starts a transaction.
    /// </summary>
    /// <param name="options">The options for the key-value transaction.</param>
    /// <param name="cancellationToken">The cancellation token for the operation.</param>
    /// <returns>A task representing the asynchronous operation, containing the result of the transaction initiation
    /// as a tuple consisting of the response type and the associated HLC timestamp.</returns>
    public Task<(KeyValueResponseType, TransactionHandle)> LocateAndStartTransaction(KeyValueTransactionOptions options, CancellationToken cancellationToken)
    {
        return locator.LocateAndStartTransaction(options, cancellationToken);
    }

    /// <summary>
    /// Attempts to locate and commit the transaction identified by <paramref name="handle"/>.
    /// </summary>
    /// <param name="handle">The handle returned by <see cref="LocateAndStartTransaction"/>.</param>
    /// <param name="acquiredLocks">A list of keys that have been locked during the transaction.</param>
    /// <param name="modifiedKeys">A list of keys that were modified as part of the transaction.</param>
    /// <param name="readKeys">A list of keys read during the transaction.</param>
    /// <param name="cancellationToken">A token used to monitor for cancellation requests.</param>
    /// <returns>A task that represents the asynchronous operation, containing the result of the transaction operation.</returns>
    public Task<(KeyValueResponseType, string?)> LocateAndCommitTransaction(TransactionHandle handle, CancellationToken cancellationToken)
    {
        return locator.LocateAndCommitTransaction(handle, cancellationToken);
    }

    /// <summary>How a transaction-scoped request routes based on the register-remote identity it carries.</summary>
    private enum RegistrationRouting
    {
        /// <summary>Non-transactional, or a script/REST transaction that owns its 2PC: take the legacy locator path.</summary>
        Legacy,

        /// <summary>An interactive request carrying only one of {coordinator key, operation id}: reject as malformed.</summary>
        Malformed,

        /// <summary>An interactive request carrying both identity components: register on the coordinator.</summary>
        Registered
    }

    /// <summary>
    /// Classifies a transaction-scoped point/lock request by the register-remote identity it carries. A
    /// non-transactional request, or a script/REST transaction that manages its own 2PC, carries neither a
    /// coordinator key nor an operation id and takes the legacy locator path. An interactive request must
    /// carry <b>both</b>; carrying exactly one is malformed — applying it would mutate a participant outside
    /// the finalize fence, so it is rejected rather than silently degraded to the unregistered path.
    /// </summary>
    private static RegistrationRouting ClassifyRegistration(HLCTimestamp transactionId, string coordinatorKey, TransactionOperationId operationId)
    {
        bool hasCoordinator = !string.IsNullOrEmpty(coordinatorKey);
        bool hasOperation = !operationId.IsEmpty;

        if (transactionId.IsNull() || (!hasCoordinator && !hasOperation))
            return RegistrationRouting.Legacy;

        return hasCoordinator && hasOperation ? RegistrationRouting.Registered : RegistrationRouting.Malformed;
    }

    /// <summary>
    /// Drives the coordinator completion for an operation whose actor just executed, first caching the
    /// confirmed response and effect on this participant so a lost or cancelled completion is recoverable.
    /// The completion runs detached from the caller's cancellation token: once the actor has mutated,
    /// abandoning the completion because the caller went away would strand the effect with the coordinator
    /// record stuck pending. Returns true when the coordinator acknowledged the fold; false means the
    /// caller must surface <see cref="KeyValueResponseType.MustRetry"/>, and a same-id retry recovers the
    /// result through <see cref="TryRecoverRegisteredOperation"/> without reapplying the operation.
    /// </summary>
    private async Task<bool> CompleteRegisteredOperation(
        string coordinatorKey, HLCTimestamp transactionId, TransactionOperationId operationId,
        object response, OperationCompletionPayload payload)
    {
        participantOperationCache.Store(transactionId, operationId, response, payload);

        try
        {
            (KeyValueResponseType outcome, _) = await LocateAndCompleteOperation(coordinatorKey, transactionId, operationId, payload, CancellationToken.None);

            if (outcome == KeyValueResponseType.Set)
            {
                participantOperationCache.Remove(transactionId, operationId);
                return true;
            }

            // The routing resolved without an exception but the completion did not reach the coordinator
            // (e.g. a leadership flip between routing and landing). Retain the cache entry so a same-id
            // retry re-drives the idempotent completion through TryRecoverRegisteredOperation.
            logger.LogWarning("Completion of transaction operation {OperationId} was not acknowledged by coordinator; retaining participant result for retry", operationId);
            return false;
        }
        catch (Exception ex)
        {
            // RPC loss or transient fault — retain the cache entry for same-id retry recovery.
            logger.LogWarning(ex, "Completion of transaction operation {OperationId} was not acknowledged; retaining participant result for retry", operationId);
            return false;
        }
    }

    /// <summary>
    /// On an <see cref="OperationRegistrationOutcome.AlreadyPending"/> outcome, attempts to finish an
    /// operation whose actor already executed on this participant but whose completion was lost. Re-drives
    /// the coordinator completion from the cached effect (the coordinator fold is idempotent) instead of
    /// reapplying the operation. Returns the original boxed response when recovery succeeds; null means the
    /// caller should surface <see cref="KeyValueResponseType.MustRetry"/> (no local record, or the
    /// completion is still unreachable).
    /// </summary>
    private async Task<object?> TryRecoverRegisteredOperation(
        string coordinatorKey, HLCTimestamp transactionId, TransactionOperationId operationId)
    {
        if (!participantOperationCache.TryGet(transactionId, operationId, out object? response, out OperationCompletionPayload? payload))
            return null;

        try
        {
            (KeyValueResponseType outcome, _) = await LocateAndCompleteOperation(coordinatorKey, transactionId, operationId, payload!, CancellationToken.None);

            if (outcome == KeyValueResponseType.Set)
            {
                participantOperationCache.Remove(transactionId, operationId);
                return response;
            }

            // Coordinator did not acknowledge — retain cache entry so the next same-id retry re-drives.
            logger.LogWarning("Retry completion of transaction operation {OperationId} was not acknowledged by coordinator; will retry again", operationId);
            return null;
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Retry completion of transaction operation {OperationId} was not acknowledged; will retry again", operationId);
            return null;
        }
    }

    /// <summary>Routes an operation registration to the coordinator node identified by <paramref name="coordinatorKey"/>.</summary>
    public Task<(OperationRegistrationOutcome outcome, KeyValueResponseType cachedType, long cachedRevision, HLCTimestamp cachedTimestamp, string? recordAnchorKey)> LocateAndBeginOperation(string coordinatorKey, HLCTimestamp transactionId, TransactionOperationId operationId, OperationKind kind, byte[]? payloadDigest, CancellationToken cancellationToken)
    {
        return locator.LocateAndBeginOperation(coordinatorKey, transactionId, operationId, kind, payloadDigest, cancellationToken);
    }

    /// <summary>Routes an operation completion to the coordinator node identified by <paramref name="coordinatorKey"/>. Returns Set+anchor on acknowledgement, MustRetry when routing did not deliver.</summary>
    public Task<(KeyValueResponseType outcome, string? anchor)> LocateAndCompleteOperation(string coordinatorKey, HLCTimestamp transactionId, TransactionOperationId operationId, OperationCompletionPayload payload, CancellationToken cancellationToken)
    {
        return locator.LocateAndCompleteOperation(coordinatorKey, transactionId, operationId, payload, cancellationToken);
    }

    /// <summary>Node-local registration: the session lives here (this node is the coordinator for the key).</summary>
    public (OperationRegistrationOutcome outcome, KeyValueResponseType cachedType, long cachedRevision, HLCTimestamp cachedTimestamp, string? recordAnchorKey) BeginOperation(HLCTimestamp transactionId, TransactionOperationId operationId, OperationKind kind, byte[]? payloadDigest)
    {
        OperationRegistrationResult result = txCoordinator.BeginOperation(transactionId, operationId, kind, payloadDigest);

        CachedOperationResponse cached = result.CachedResponse is CachedOperationResponse c
            ? c
            : new(KeyValueResponseType.Errored, 0, HLCTimestamp.Zero);

        return (result.Outcome, cached.Type, cached.Revision, cached.CommitTimestamp, result.RecordAnchorKey);
    }

    /// <summary>
    /// Node-local completion: folds the confirmed effect into the coordinator-owned working set. Returns
    /// the transaction's record anchor after the fold, or null when nothing durable-anchoring exists yet.
    /// </summary>
    public string? CompleteOperation(HLCTimestamp transactionId, TransactionOperationId operationId, OperationCompletionPayload payload)
    {
        OperationEffect? effect = BuildEffect(payload);

        // A transient outcome, or a WaitingForReplication registration that produced no effect, must not
        // be cached as terminal: cancel so a same-id retry re-registers as new and actually applies the
        // mutation. A WaitingForReplication that DID produce an effect keeps the cached-response replay
        // path (do not cancel) — a retry would see the cached response and skip reapplication.
        if (payload.CachedType == KeyValueResponseType.MustRetry ||
            (payload.CachedType == KeyValueResponseType.WaitingForReplication && effect is null))
        {
            txCoordinator.CancelOperation(transactionId, operationId);
            return null;
        }

        return txCoordinator.CompleteOperation(transactionId, operationId, effect, new CachedOperationResponse(payload.CachedType, payload.CachedRevision, payload.CachedTimestamp));
    }

    /// <summary>
    /// Inbound landing point for a remote completion: re-checks local leadership before folding, so a
    /// node that lost the coordinator partition between the caller's route decision and the RPC landing
    /// does not silently swallow the completion as a false acknowledgement. Returns <c>Set</c>+anchor
    /// when this node is still the leader and the fold succeeded; <c>MustRetry</c> when leadership was
    /// lost. Does not re-forward — the caller's retry re-routes through a fresh
    /// <see cref="LocateAndCompleteOperation"/>.
    /// </summary>
    public async Task<(KeyValueResponseType outcome, string? anchor)> CompleteOperationInbound(string coordinatorKey, HLCTimestamp transactionId, TransactionOperationId operationId, OperationCompletionPayload payload)
    {
        if (string.IsNullOrEmpty(coordinatorKey))
            return (KeyValueResponseType.MustRetry, null);

        int partitionId = locator.LocatePartition(coordinatorKey);

        if (raft.Joined && !await raft.AmILeader(partitionId, CancellationToken.None))
            return (KeyValueResponseType.MustRetry, null);

        string? anchor = CompleteOperation(transactionId, operationId, payload);
        return (KeyValueResponseType.Set, anchor);
    }

    /// <summary>Maps a completion payload into the coordinator-owned working-set effect, or null when it records nothing.</summary>
    private static OperationEffect? BuildEffect(OperationCompletionPayload payload)
    {
        KeyValueDurability durability = payload.Durability;

        bool hasEffect =
            !string.IsNullOrEmpty(payload.ModifiedKey) ||
            (payload.ModifiedKeys is { Count: > 0 }) ||
            (payload.AcquiredPointLocks is { Count: > 0 }) ||
            !string.IsNullOrEmpty(payload.AcquiredPointLock) || !string.IsNullOrEmpty(payload.ReleasedPointLock) ||
            !string.IsNullOrEmpty(payload.AcquiredPrefixLock) || !string.IsNullOrEmpty(payload.ReleasedPrefixLock) ||
            payload.AcquiredRangeLock is not null || payload.ReleasedRangeLock is not null ||
            payload.Read is not null ||
            (payload.ReadObservations is { Count: > 0 }) ||
            (payload.StagedMutations is { Count: > 0 });

        if (!hasEffect)
            return null;

        return new()
        {
            ModifiedKey = string.IsNullOrEmpty(payload.ModifiedKey) ? null : (payload.ModifiedKey, durability),
            ModifiedKeys = payload.ModifiedKeys is { Count: > 0 } ? payload.ModifiedKeys : null,
            StagedMutations = payload.StagedMutations is { Count: > 0 } ? payload.StagedMutations : null,
            PointLock = string.IsNullOrEmpty(payload.AcquiredPointLock) ? null : (payload.AcquiredPointLock, durability),
            AcquiredPointLocks = payload.AcquiredPointLocks is { Count: > 0 } ? payload.AcquiredPointLocks : null,
            RemovePointLock = string.IsNullOrEmpty(payload.ReleasedPointLock) ? null : (payload.ReleasedPointLock, durability),
            PrefixLock = string.IsNullOrEmpty(payload.AcquiredPrefixLock) ? null : (payload.AcquiredPrefixLock, durability),
            RemovePrefixLock = string.IsNullOrEmpty(payload.ReleasedPrefixLock) ? null : (payload.ReleasedPrefixLock, durability),
            RangeLock = payload.AcquiredRangeLock,
            RemoveRangeLock = payload.ReleasedRangeLock,
            ReadObservation = payload.Read,
            ReadObservations = payload.ReadObservations
        };
    }

    /// <summary>Routes a working-set query to the coordinator node identified by <paramref name="coordinatorKey"/>.</summary>
    public Task<TransactionWorkingSet?> LocateAndGetTransactionWorkingSet(string coordinatorKey, HLCTimestamp transactionId, CancellationToken cancellationToken)
    {
        return locator.LocateAndGetTransactionWorkingSet(coordinatorKey, transactionId, cancellationToken);
    }

    /// <summary>Routes a close-and-snapshot to the coordinator node identified by <paramref name="coordinatorKey"/>.</summary>
    public Task<(KeyValueResponseType, TransactionWorkingSet?)> LocateAndCloseTransaction(string coordinatorKey, HLCTimestamp transactionId, CancellationToken cancellationToken)
    {
        return locator.LocateAndCloseTransaction(coordinatorKey, transactionId, cancellationToken);
    }

    /// <summary>Node-local working-set query: the session lives here (this node leads the coordinator partition).</summary>
    public TransactionWorkingSet? GetTransactionWorkingSet(HLCTimestamp transactionId)
    {
        WorkingSetSnapshot? snapshot = txCoordinator.GetTransactionWorkingSet(transactionId);
        return snapshot is null ? null : MapWorkingSet(snapshot);
    }

    /// <summary>Node-local close-and-snapshot: freezes the session and returns its frozen working set.</summary>
    public async Task<(KeyValueResponseType, TransactionWorkingSet?)> CloseTransaction(HLCTimestamp transactionId, CancellationToken cancellationToken)
    {
        (KeyValueResponseType type, WorkingSetSnapshot? snapshot) = await txCoordinator.CloseTransaction(transactionId, cancellationToken);
        return (type, snapshot is null ? null : MapWorkingSet(snapshot));
    }

    private static TransactionWorkingSet MapWorkingSet(WorkingSetSnapshot snapshot) => new()
    {
        ModifiedKeys = ToModifiedList(snapshot.ModifiedKeys),
        AcquiredLocks = ToModifiedList(snapshot.LocksAcquired),
        AcquiredPrefixLocks = ToModifiedList(snapshot.PrefixLocksAcquired),
        AcquiredRangeLocks = ToRangeLockList(snapshot.RangeLocksAcquired),
        ReadKeys = snapshot.ReadKeys is null
            ? []
            : snapshot.ReadKeys.Values.OrderBy(r => r.Key, StringComparer.Ordinal).ToList(),
        RecordAnchorKey = snapshot.RecordAnchorKey,
        PendingOperationCount = snapshot.PendingOperationCount
    };

    private static List<KeyValueTransactionRangeLock> ToRangeLockList(IReadOnlyDictionary<RangeLockKey, RangeLockMode>? ranges)
    {
        if (ranges is null)
            return [];

        return ranges
            .OrderBy(x => x.Key.Prefix, StringComparer.Ordinal)
            .ThenBy(x => x.Key.StartKey, StringComparer.Ordinal)
            .ThenBy(x => x.Key.EndKey, StringComparer.Ordinal)
            .Select(x => new KeyValueTransactionRangeLock
            {
                Prefix = x.Key.Prefix,
                StartKey = x.Key.StartKey,
                StartInclusive = x.Key.StartInclusive,
                EndKey = x.Key.EndKey,
                EndInclusive = x.Key.EndInclusive,
                Durability = x.Key.Durability,
                Mode = x.Value
            })
            .ToList();
    }

    private static List<KeyValueTransactionModifiedKey> ToModifiedList(IReadOnlySet<(string Key, KeyValueDurability Durability)>? set)
    {
        if (set is null)
            return [];

        return set
            .OrderBy(x => x.Key, StringComparer.Ordinal)
            .Select(x => new KeyValueTransactionModifiedKey { Key = x.Key, Durability = x.Durability })
            .ToList();
    }

    /// <summary>
    /// Locates and rolls back the transaction identified by <paramref name="handle"/>.
    /// </summary>
    /// <param name="handle">The handle returned by <see cref="LocateAndStartTransaction"/>.</param>
    /// <param name="cancellationToken">A cancellation token to observe while waiting for the task to complete.</param>
    /// <returns>A task that represents the asynchronous operation, containing the result of the rollback operation as a <see cref="KeyValueResponseType"/>.</returns>
    public Task<KeyValueResponseType> LocateAndRollbackTransaction(TransactionHandle handle, CancellationToken cancellationToken)
    {
        return locator.LocateAndRollbackTransaction(handle, cancellationToken);
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
        KeyValueDurability durability,
        long routedGeneration = 0
    )
    {
        KeyValueRequest request = KeyValueRequestPool.Rent(
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
            durability,
            0,
            0,
            null
        );

        request.RoutedGeneration = routedGeneration;

        try
        {
            foreach (TimeSpan delay in Backoff.DecorrelatedJitterBackoffV2(medianFirstRetryDelay: TimeSpan.FromMilliseconds(1), retryCount: MaxRetries))
            {
                KeyValueResponse? response;

                if (durability == KeyValueDurability.Ephemeral)
                    response = await AskKeyValueActor(ephemeralKeyValuesRouter, request);
                else
                    response = await AskKeyValueActor(persistentKeyValuesRouter, request);

                if (response is null)
                    return (KeyValueResponseType.Errored, -1, HLCTimestamp.Zero);

                if (response.Type == KeyValueResponseType.WaitingForReplication)
                {
                    await Task.Delay(delay);
                    continue;
                }

                return (response.Type, response.Revision, response.Ticket);
            }

            return (KeyValueResponseType.MustRetry, -1, HLCTimestamp.Zero);
        }
        finally
        {
            KeyValueRequestPool.Return(request);
        }
    }

    /// <summary>
    /// Attempts to set multiple key-value pairs on the node.
    /// </summary>
    /// <param name="items">A list of key-value set requests to be processed.</param>
    /// <returns>A task that represents the asynchronous operation. The task result contains a list of responses for each set request, indicating the outcome of the operation.</returns>
    public async Task<List<KahunaSetKeyValueResponseItem>> SetManyNodeKeyValue(List<KahunaSetKeyValueRequestItem> items)
    {
        // Fan every item out through the ordinary TrySet path, launched together. Persistent items meet in the
        // shared partition write aggregator — coalescing across this call, other concurrent many-key calls, and
        // single writes to the same partition — while ephemeral and transactional items keep their existing
        // per-key behavior. tasks[i] corresponds to items[i], so response order matches input order.
        Task<KahunaSetKeyValueResponseItem>[] tasks = new Task<KahunaSetKeyValueResponseItem>[items.Count];
        for (int i = 0; i < items.Count; i++)
            tasks[i] = SetOneNodeKeyValue(items[i]);

        return [.. await Task.WhenAll(tasks)];

        async Task<KahunaSetKeyValueResponseItem> SetOneNodeKeyValue(KahunaSetKeyValueRequestItem item)
        {
            KeyValueRequest request = KeyValueRequestPool.Rent(
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
                item.Durability,
                0,
                0,
                null
            );

            request.RoutedGeneration = item.RoutedGeneration;

            try
            {
                KeyValueResponse? response;

                if (item.Durability == KeyValueDurability.Ephemeral)
                    response = await AskKeyValueActor(ephemeralKeyValuesRouter, request);
                else
                    response = await AskKeyValueActor(persistentKeyValuesRouter, request);

                if (response is null)
                    return new() { Key = item.Key ?? "", Type = KeyValueResponseType.Errored, Durability = item.Durability };

                // A residual WaitingForReplication (a live replication intent the caller should retry against)
                // is retryable, not a terminal error.
                if (response.Type == KeyValueResponseType.WaitingForReplication)
                    return new() { Key = item.Key ?? "", Type = KeyValueResponseType.MustRetry, Durability = item.Durability };

                return new()
                {
                    Key = item.Key ?? "",
                    Type = response.Type,
                    Revision = response.Revision,
                    LastModified = response.Ticket,
                    Durability = item.Durability
                };
            }
            finally
            {
                KeyValueRequestPool.Return(request);
            }
        }
    }


    public async Task<List<KahunaDeleteKeyValueResponseItem>> DeleteManyNodeKeyValue(List<KahunaDeleteKeyValueRequestItem> items)
    {
        // Fan every item out through the ordinary TryDelete path, launched together. Persistent items meet in
        // the shared partition write aggregator (coalescing across this and other concurrent calls), while
        // ephemeral and transactional items keep their existing per-key behavior. Order matches input order.
        Task<KahunaDeleteKeyValueResponseItem>[] tasks = new Task<KahunaDeleteKeyValueResponseItem>[items.Count];
        for (int i = 0; i < items.Count; i++)
            tasks[i] = DeleteOneNodeKeyValue(items[i]);

        return [.. await Task.WhenAll(tasks)];

        async Task<KahunaDeleteKeyValueResponseItem> DeleteOneNodeKeyValue(KahunaDeleteKeyValueRequestItem item)
        {
            KeyValueRequest request = KeyValueRequestPool.Rent(
                KeyValueRequestType.TryDelete,
                item.TransactionId,
                HLCTimestamp.Zero,
                item.Key ?? "",
                null,
                null,
                -1,
                KeyValueFlags.None,
                0,
                HLCTimestamp.Zero,
                item.Durability,
                0,
                0,
                null
            );

            try
            {
                foreach (TimeSpan delay in Backoff.DecorrelatedJitterBackoffV2(medianFirstRetryDelay: TimeSpan.FromMilliseconds(1), retryCount: MaxRetries))
                {
                    KeyValueResponse? response;

                    if (item.Durability == KeyValueDurability.Ephemeral)
                        response = await AskKeyValueActor(ephemeralKeyValuesRouter, request);
                    else
                        response = await AskKeyValueActor(persistentKeyValuesRouter, request);

                    if (response is null)
                        return new()
                        {
                            Key = item.Key ?? "",
                            Type = KeyValueResponseType.Errored,
                            Revision = -1,
                            LastModified = HLCTimestamp.Zero,
                            Durability = item.Durability
                        };

                    if (response.Type == KeyValueResponseType.WaitingForReplication)
                    {
                        await Task.Delay(delay);
                        continue;
                    }

                    return new()
                    {
                        Key = item.Key ?? "",
                        Type = response.Type,
                        Revision = response.Revision,
                        LastModified = response.Ticket,
                        Durability = item.Durability
                    };
                }

                return new()
                {
                    Key = item.Key ?? "",
                    Type = KeyValueResponseType.MustRetry,
                    Revision = -1,
                    LastModified = HLCTimestamp.Zero,
                    Durability = item.Durability
                };
            }
            finally
            {
                KeyValueRequestPool.Return(request);
            }
        }
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
        KeyValueRequest request = KeyValueRequestPool.Rent(
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
            durability,
            0,
            0,
            null
        );

        try
        {
            foreach (TimeSpan delay in Backoff.DecorrelatedJitterBackoffV2(medianFirstRetryDelay: TimeSpan.FromMilliseconds(1), retryCount: MaxRetries))
            {
                KeyValueResponse? response;

                if (durability == KeyValueDurability.Ephemeral)
                    response = await AskKeyValueActor(ephemeralKeyValuesRouter, request);
                else
                    response = await AskKeyValueActor(persistentKeyValuesRouter, request);

                if (response is null)
                    return (KeyValueResponseType.Errored, -1, HLCTimestamp.Zero);

                if (response.Type == KeyValueResponseType.WaitingForReplication)
                {
                    await Task.Delay(delay);
                    continue;
                }

                return (response.Type, response.Revision, response.Ticket);
            }

            return (KeyValueResponseType.MustRetry, -1, HLCTimestamp.Zero);
        }
        finally
        {
            KeyValueRequestPool.Return(request);
        }
    }

    /// <summary>
    /// Removes the specified key. A key is ignored if it does not exist.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, long, HLCTimestamp)> TryDeleteKeyValue(
        HLCTimestamp transactionId, 
        string key, 
        KeyValueDurability durability
    )
    {
        KeyValueRequest request = KeyValueRequestPool.Rent(
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
            durability,
            0,
            0,
            null
        );

        try
        {
            foreach (TimeSpan delay in Backoff.DecorrelatedJitterBackoffV2(medianFirstRetryDelay: TimeSpan.FromMilliseconds(1), retryCount: MaxRetries))
            {
                KeyValueResponse? response;

                if (durability == KeyValueDurability.Ephemeral)
                    response = await AskKeyValueActor(ephemeralKeyValuesRouter, request);
                else
                    response = await AskKeyValueActor(persistentKeyValuesRouter, request);

                if (response is null)
                    return (KeyValueResponseType.Errored, -1, HLCTimestamp.Zero);

                if (response.Type == KeyValueResponseType.WaitingForReplication)
                {
                    await Task.Delay(delay);
                    continue;
                }

                return (response.Type, response.Revision, response.Ticket);
            }

            return (KeyValueResponseType.MustRetry, -1, HLCTimestamp.Zero);
        }
        finally
        {
            KeyValueRequestPool.Return(request);
        }
    }
    
    /// <summary>
    /// Passes a Get request to the key/value actor for the given keyValue name.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> TryGetValue(
        HLCTimestamp transactionId,
        string key,
        long revision,
        HLCTimestamp readTimestamp,
        KeyValueDurability durability
    )
    {
        KeyValueRequest request = KeyValueRequestPool.Rent(
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
            durability,
            0,
            0,
            null
        );

        request.ReadTimestamp = readTimestamp;

        try
        {
            // Exponential back-off loop, matching the LocateAndScanRange page-retry contract.
            // WaitingForReplication covers two cases:
            //   • Short replication lag (ReplicationIntent): typically resolves in < 10 ms.
            //   • Safe-time wait (pending write intent whose commit ts ≤ readTimestamp): can
            //     last up to the intent TTL (DefaultTxCompleteTimeout ≈ 15 s). The exponential
            //     back-off naturally amortises both: a replication lag resolves in the first 1-2
            //     iterations; a safe-time wait resolves once the in-flight write commits or the
            //     intent expires and the actor clears it.
            // The deadline is DefaultTxCompleteTimeout + 1 500 ms buffer.
            int backoffMs = 1;
            long deadline = Environment.TickCount64 + 16_500;

            while (true)
            {
                KeyValueResponse? response;

                if (durability == KeyValueDurability.Ephemeral)
                    response = await AskKeyValueActor(ephemeralKeyValuesRouter, request);
                else
                    response = await AskKeyValueActor(persistentKeyValuesRouter, request);

                if (response is null)
                    return (KeyValueResponseType.Errored, null);

                if (response.Type != KeyValueResponseType.WaitingForReplication)
                    return (response.Type, response.Entry);

                if (Environment.TickCount64 >= deadline)
                    return (KeyValueResponseType.MustRetry, null);

                await Task.Delay(backoffMs);
                backoffMs = Math.Min(backoffMs * 2, 1000);
            }
        }
        finally
        {
            KeyValueRequestPool.Return(request);
        }
    }
    
    /// <summary>
    /// Passes a Exists request to the key/value actor for the given keyValue name.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> TryExistsValue(
        HLCTimestamp transactionId,
        string key,
        long revision,
        HLCTimestamp readTimestamp,
        KeyValueDurability durability
    )
    {
        KeyValueRequest request = KeyValueRequestPool.Rent(
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
            durability,
            0,
            0,
            null
        );

        request.ReadTimestamp = readTimestamp;

        try
        {
            int backoffMs = 1;
            long deadline = Environment.TickCount64 + 16_500;

            while (true)
            {
                KeyValueResponse? response;

                if (durability == KeyValueDurability.Ephemeral)
                    response = await AskKeyValueActor(ephemeralKeyValuesRouter, request);
                else
                    response = await AskKeyValueActor(persistentKeyValuesRouter, request);

                if (response is null)
                    return (KeyValueResponseType.Errored, null);

                if (response.Type != KeyValueResponseType.WaitingForReplication)
                    return (response.Type, response.Entry);

                if (Environment.TickCount64 >= deadline)
                    return (KeyValueResponseType.MustRetry, null);

                await Task.Delay(backoffMs);
                backoffMs = Math.Min(backoffMs * 2, 1000);
            }
        }
        finally
        {
            KeyValueRequestPool.Return(request);
        }
    }

    public async Task<List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)>> TryGetManyValues(
        HLCTimestamp transactionId,
        HLCTimestamp readTimestamp,
        List<(string key, long revision, KeyValueDurability durability)> keys
    )
    {
        Task<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)>[] tasks = new Task<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)>[keys.Count];

        for (int i = 0; i < keys.Count; i++)
        {
            (string key, long revision, KeyValueDurability durability) item = keys[i];
            tasks[i] = TryGetManyValue(item);
        }

        return [.. await Task.WhenAll(tasks)];

        async Task<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)> TryGetManyValue(
            (string key, long revision, KeyValueDurability durability) item
        )
        {
            (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await TryGetValue(
                transactionId,
                item.key,
                item.revision,
                readTimestamp,
                item.durability
            );

            return (type, item.key, item.durability, entry);
        }
    }

    public async Task<List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)>> TryExistsManyValues(
        HLCTimestamp transactionId,
        HLCTimestamp readTimestamp,
        List<(string key, long revision, KeyValueDurability durability)> keys
    )
    {
        Task<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)>[] tasks = new Task<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)>[keys.Count];

        for (int i = 0; i < keys.Count; i++)
        {
            (string key, long revision, KeyValueDurability durability) item = keys[i];
            tasks[i] = TryExistsManyValue(item);
        }

        return [.. await Task.WhenAll(tasks)];

        async Task<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)> TryExistsManyValue(
            (string key, long revision, KeyValueDurability durability) item
        )
        {
            (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await TryExistsValue(
                transactionId,
                item.key,
                item.revision,
                readTimestamp,
                item.durability
            );

            return (type, item.key, item.durability, entry);
        }
    }

    /// <summary>
    /// Checks whether the given key has a live write intent from a transaction other than the caller.
    /// Used at commit time by optimistic transactions to detect concurrent writers (write-skew guard).
    /// Returns Aborted when a conflicting write intent is found; DoesNotExist otherwise.
    /// </summary>
    public async Task<KeyValueResponseType> TryCheckWriteIntentValue(
        HLCTimestamp transactionId,
        string key,
        KeyValueDurability durability
    )
    {
        KeyValueRequest request = KeyValueRequestPool.Rent(
            KeyValueRequestType.TryCheckWriteIntent,
            transactionId,
            HLCTimestamp.Zero,
            key,
            null,
            null,
            -1,
            KeyValueFlags.None,
            0,
            HLCTimestamp.Zero,
            durability,
            0,
            0,
            null
        );

        try
        {
            KeyValueResponse? response = durability == KeyValueDurability.Ephemeral
                ? await AskKeyValueActor(ephemeralKeyValuesRouter, request)
                : await AskKeyValueActor(persistentKeyValuesRouter, request);

            return response?.Type ?? KeyValueResponseType.Errored;
        }
        finally
        {
            KeyValueRequestPool.Return(request);
        }
    }

    /// <summary>
    /// Passes a TryAcquireExclusiveLock request to the key/value actor for the given keyValue name.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="expiresMs"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, string, KeyValueDurability, HLCTimestamp HolderTransactionId)> TryAcquireExclusiveLock(HLCTimestamp transactionId, string key, int expiresMs, KeyValueDurability durability)
    {
        KeyValueRequest request = KeyValueRequestPool.Rent(
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
            durability,
            0,
            0,
            null
        );

        try
        {
            foreach (TimeSpan delay in Backoff.DecorrelatedJitterBackoffV2(medianFirstRetryDelay: TimeSpan.FromMilliseconds(1), retryCount: MaxRetries))
            {
                KeyValueResponse? response;

                if (durability == KeyValueDurability.Ephemeral)
                    response = await AskKeyValueActor(ephemeralKeyValuesRouter, request);
                else
                    response = await AskKeyValueActor(persistentKeyValuesRouter, request);

                if (response is null)
                    return (KeyValueResponseType.Errored, key, durability, HLCTimestamp.Zero);

                if (response.Type == KeyValueResponseType.WaitingForReplication)
                {
                    await Task.Delay(delay);
                    continue;
                }

                return (response.Type, key, durability, response.HolderTransactionId);
            }

            return (KeyValueResponseType.MustRetry, key, durability, HLCTimestamp.Zero);
        }
        finally
        {
            KeyValueRequestPool.Return(request);
        }
    }

    /// <summary>
    /// Passes a TryAcquireExclusivePrefixLock request to the key/value actor to lock a range of keys by the specified prefix
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="expiresMs"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public async Task<KeyValueResponseType> TryAcquireExclusivePrefixLock(
        HLCTimestamp transactionId, 
        string prefixKey, 
        int expiresMs, 
        KeyValueDurability durability
    )
    {
        KeyValueRequest request = KeyValueRequestPool.Rent(
            KeyValueRequestType.TryAcquireExclusivePrefixLock, 
            transactionId,
            HLCTimestamp.Zero,
            prefixKey,
            null, 
            null,
            -1,
            KeyValueFlags.None,
            expiresMs, 
            HLCTimestamp.Zero,
            durability,
            0,
            0,
            null
        );
        
        try
        {
            foreach (TimeSpan delay in Backoff.DecorrelatedJitterBackoffV2(medianFirstRetryDelay: TimeSpan.FromMilliseconds(1), retryCount: MaxRetries))
            {
                KeyValueResponse? response;

                if (durability == KeyValueDurability.Ephemeral)
                    response = await AskKeyValueActor(ephemeralKeyValuesRouter, request);
                else
                    response = await AskKeyValueActor(persistentKeyValuesRouter, request);

                if (response is null)
                    return KeyValueResponseType.Errored;

                if (response.Type == KeyValueResponseType.WaitingForReplication)
                {
                    await Task.Delay(delay);
                    continue;
                }

                return response.Type;
            }

            return KeyValueResponseType.MustRetry;
        }
        finally
        {
            KeyValueRequestPool.Return(request);
        }
    }

    /// <summary>
    /// Passes a TryAcquireExclusiveLock request to the key/value actor for the given keys.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="keys"></param>
    /// <returns></returns>
    public async Task<List<(KeyValueResponseType, string, KeyValueDurability, HLCTimestamp HolderTransactionId)>> TryAcquireManyExclusiveLocks(
        HLCTimestamp transactionId,
        List<(string key, int expiresMs, KeyValueDurability durability)> keys
    )
    {
        List<(KeyValueResponseType, string, KeyValueDurability, HLCTimestamp)> responses = new(keys.Count);

        foreach ((string key, int expiresMs, KeyValueDurability durability) key in keys)
        {
            KeyValueRequest request = KeyValueRequestPool.Rent(
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
                key.durability,
                0,
                0,
                null
            );

            try
            {
                KeyValueResponse? response;

                if (key.durability == KeyValueDurability.Ephemeral)
                    response = await AskKeyValueActor(ephemeralKeyValuesRouter, request);
                else
                    response = await AskKeyValueActor(persistentKeyValuesRouter, request);

                if (response is null || response.Type == KeyValueResponseType.WaitingForReplication)
                {
                    responses.Add((KeyValueResponseType.Errored, key.key, key.durability, HLCTimestamp.Zero));
                    continue;
                }

                responses.Add((response.Type, key.key, key.durability, response.HolderTransactionId));

                if (response.Type != KeyValueResponseType.Locked)
                    break;
            }
            finally
            {
                KeyValueRequestPool.Return(request);
            }
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
        KeyValueRequest request = KeyValueRequestPool.Rent(
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
            durability,
            0,
            0,
            null
        );

        try
        {
            foreach (TimeSpan delay in Backoff.DecorrelatedJitterBackoffV2(medianFirstRetryDelay: TimeSpan.FromMilliseconds(1), retryCount: MaxRetries))
            {
                KeyValueResponse? response;

                if (durability == KeyValueDurability.Ephemeral)
                    response = await AskKeyValueActor(ephemeralKeyValuesRouter, request);
                else
                    response = await AskKeyValueActor(persistentKeyValuesRouter, request);

                if (response is null)
                    return (KeyValueResponseType.Errored, key);

                if (response.Type == KeyValueResponseType.WaitingForReplication)
                {
                    await Task.Delay(delay);
                    continue;
                }

                return (response.Type, key);
            }

            return (KeyValueResponseType.MustRetry, key);
        }
        finally
        {
            KeyValueRequestPool.Return(request);
        }
    }

    /// <summary>
    /// Passes a TryReleaseExclusivePrefixLock request to the key/value actor to lock a range of keys by the specified prefix
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public async Task<KeyValueResponseType> TryReleaseExclusivePrefixLock(HLCTimestamp transactionId, string prefixKey, KeyValueDurability durability)
    {
        KeyValueRequest request = KeyValueRequestPool.Rent(
            KeyValueRequestType.TryReleaseExclusivePrefixLock, 
            transactionId, 
            HLCTimestamp.Zero,
            prefixKey, 
            null, 
            null,
            -1,
            KeyValueFlags.None,
            0, 
            HLCTimestamp.Zero,
            durability,
            0,
            0,
            null
        );

        try
        {
            foreach (TimeSpan delay in Backoff.DecorrelatedJitterBackoffV2(medianFirstRetryDelay: TimeSpan.FromMilliseconds(1), retryCount: MaxRetries))
            {
                KeyValueResponse? response;

                if (durability == KeyValueDurability.Ephemeral)
                    response = await AskKeyValueActor(ephemeralKeyValuesRouter, request);
                else
                    response = await AskKeyValueActor(persistentKeyValuesRouter, request);

                if (response is null)
                    return KeyValueResponseType.Errored;

                if (response.Type == KeyValueResponseType.WaitingForReplication)
                {
                    await Task.Delay(delay);
                    continue;
                }

                return response.Type;
            }

            return KeyValueResponseType.MustRetry;
        }
        finally
        {
            KeyValueRequestPool.Return(request);
        }
    }

    public Task<(KeyValueResponseType, HLCTimestamp HolderTransactionId)> TryAcquireExclusiveRangeLock(
        HLCTimestamp transactionId,
        string prefix,
        string? startKey, bool startInclusive,
        string? endKey,   bool endInclusive,
        int expiresMs,
        KeyValueDurability durability
    ) => TryAcquireRangeLock(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, expiresMs, durability, RangeLockMode.Exclusive);

    public async Task<(KeyValueResponseType, HLCTimestamp HolderTransactionId)> TryAcquireRangeLock(
        HLCTimestamp transactionId,
        string prefix,
        string? startKey, bool startInclusive,
        string? endKey,   bool endInclusive,
        int expiresMs,
        KeyValueDurability durability,
        RangeLockMode mode
    )
    {
        KeyValueRequest request = KeyValueRequestPool.Rent(
            KeyValueRequestType.TryAcquireExclusiveRangeLock,
            transactionId,
            HLCTimestamp.Zero,
            prefix,
            null,
            null,
            -1,
            KeyValueFlags.None,
            expiresMs,
            HLCTimestamp.Zero,
            durability,
            0,
            0,
            null
        );

        request.StartKey       = startKey;
        request.StartInclusive = startInclusive;
        request.EndKey         = endKey;
        request.EndInclusive   = endInclusive;
        request.RangeLockMode  = mode;

        try
        {
            foreach (TimeSpan delay in Backoff.DecorrelatedJitterBackoffV2(medianFirstRetryDelay: TimeSpan.FromMilliseconds(1), retryCount: MaxRetries))
            {
                KeyValueResponse? response;

                if (durability == KeyValueDurability.Ephemeral)
                    response = await AskKeyValueActor(ephemeralKeyValuesRouter, request);
                else
                    response = await AskKeyValueActor(persistentKeyValuesRouter, request);

                if (response is null)
                    return (KeyValueResponseType.Errored, HLCTimestamp.Zero);

                if (response.Type == KeyValueResponseType.WaitingForReplication)
                {
                    await Task.Delay(delay);
                    continue;
                }

                return (response.Type, response.HolderTransactionId);
            }

            return (KeyValueResponseType.MustRetry, HLCTimestamp.Zero);
        }
        finally
        {
            KeyValueRequestPool.Return(request);
        }
    }

    public async Task<KeyValueResponseType> TryReleaseExclusiveRangeLock(
        HLCTimestamp transactionId,
        string prefix,
        string? startKey, bool startInclusive,
        string? endKey,   bool endInclusive,
        KeyValueDurability durability
    )
    {
        KeyValueRequest request = KeyValueRequestPool.Rent(
            KeyValueRequestType.TryReleaseExclusiveRangeLock,
            transactionId,
            HLCTimestamp.Zero,
            prefix,
            null,
            null,
            -1,
            KeyValueFlags.None,
            0,
            HLCTimestamp.Zero,
            durability,
            0,
            0,
            null
        );

        request.StartKey       = startKey;
        request.StartInclusive = startInclusive;
        request.EndKey         = endKey;
        request.EndInclusive   = endInclusive;

        try
        {
            foreach (TimeSpan delay in Backoff.DecorrelatedJitterBackoffV2(medianFirstRetryDelay: TimeSpan.FromMilliseconds(1), retryCount: MaxRetries))
            {
                KeyValueResponse? response;

                if (durability == KeyValueDurability.Ephemeral)
                    response = await AskKeyValueActor(ephemeralKeyValuesRouter, request);
                else
                    response = await AskKeyValueActor(persistentKeyValuesRouter, request);

                if (response is null)
                    return KeyValueResponseType.Errored;

                if (response.Type == KeyValueResponseType.WaitingForReplication)
                {
                    await Task.Delay(delay);
                    continue;
                }

                return response.Type;
            }

            return KeyValueResponseType.MustRetry;
        }
        finally
        {
            KeyValueRequestPool.Return(request);
        }
    }

    /// <summary>
    /// Returns a snapshot of all live range-lock entries stored in the local actor for
    /// <paramref name="keySpace"/>. Used by <c>KvStateMachineTransfer</c> to read lock
    /// state before serializing it into a range-snapshot stream.
    /// <para>
    /// <b>Persistent-only assumption.</b> Range locks for key-range spaces are always acquired
    /// through the persistent router (the split/merge path only applies to persistent spaces —
    /// ephemeral data is not transferable). Ephemeral range locks, if any exist, are held in
    /// the ephemeral router's actor pool and are not returned here. Split/merge callers must not
    /// rely on this method for ephemeral key spaces.
    /// </para>
    /// </summary>
    internal async Task<List<KeyValueRangeLock>> GetRangeLocksAsync(string keySpace)
    {
        KeyValueRequest request = KeyValueRequestPool.Rent(
            KeyValueRequestType.GetRangeLocks,
            HLCTimestamp.Zero,
            HLCTimestamp.Zero,
            keySpace,
            null, null, -1, KeyValueFlags.None, 0, HLCTimestamp.Zero,
            KeyValueDurability.Persistent, 0, 0, null);

        try
        {
            KeyValueResponse? response = await AskKeyValueActor(persistentKeyValuesRouter, request);
            return response?.RangeLockList ?? [];
        }
        finally
        {
            KeyValueRequestPool.Return(request);
        }
    }

    /// <summary>
    /// Injects <paramref name="locks"/> directly into the local actor's <c>LocksByRange</c>
    /// for <paramref name="keySpace"/> — no conflict checks, no acquire logic. Entries that
    /// duplicate an already-held lock (same tx + overlapping bounds) are silently skipped.
    /// Used by <c>KvStateMachineTransfer</c> to restore clamped locks into a destination
    /// partition after a split or merge.
    /// <para>
    /// <b>Persistent-only assumption.</b> Routes to the persistent actor pool for the same
    /// reason as <see cref="GetRangeLocksAsync"/> — applies only to persistent key-range
    /// spaces. Ephemeral range locks are not injected and do not need to be transferred.
    /// </para>
    /// </summary>
    internal async Task ImportRangeLocksAsync(string keySpace, List<KeyValueRangeLock> locks)
    {
        if (locks.Count == 0)
            return;

        KeyValueRequest request = KeyValueRequestPool.Rent(
            KeyValueRequestType.ImportRangeLocks,
            HLCTimestamp.Zero,
            HLCTimestamp.Zero,
            keySpace,
            null, null, -1, KeyValueFlags.None, 0, HLCTimestamp.Zero,
            KeyValueDurability.Persistent, 0, 0, null);

        request.RangeLockImportList = locks;

        try
        {
            await AskKeyValueActor(persistentKeyValuesRouter, request);
        }
        finally
        {
            KeyValueRequestPool.Return(request);
        }
    }

    /// <summary>
    /// Returns the live range locks held in the actor pool for <paramref name="keySpace"/>
    /// on the leader of <paramref name="partitionId"/>. Forwards via IPC when this node is
    /// not the leader.
    /// </summary>
    internal async Task<List<KeyValueRangeLock>> GetRangeLocksFromPartitionLeaderAsync(
        string keySpace,
        int partitionId,
        CancellationToken cancellationToken)
    {
        if (!raft.Joined || await raft.AmILeader(partitionId, cancellationToken).ConfigureAwait(false))
            return await GetRangeLocksAsync(keySpace).ConfigureAwait(false);

        string leader = await raft.WaitForLeader(partitionId, cancellationToken).ConfigureAwait(false);
        if (leader == raft.GetLocalEndpoint())
            return await GetRangeLocksAsync(keySpace).ConfigureAwait(false);

        return await interNodeCommunication.GetRangeLocks(leader, keySpace, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Injects <paramref name="locks"/> into the actor pool for <paramref name="keySpace"/>
    /// on the leader of <paramref name="partitionId"/>. Forwards via IPC when this node is
    /// not the leader.
    /// </summary>
    internal async Task ImportRangeLocksToPartitionLeaderAsync(
        string keySpace,
        int partitionId,
        List<KeyValueRangeLock> locks,
        CancellationToken cancellationToken)
    {
        if (locks.Count == 0)
            return;

        if (!raft.Joined || await raft.AmILeader(partitionId, cancellationToken).ConfigureAwait(false))
        {
            await ImportRangeLocksAsync(keySpace, locks).ConfigureAwait(false);
            return;
        }

        string leader = await raft.WaitForLeader(partitionId, cancellationToken).ConfigureAwait(false);
        if (leader == raft.GetLocalEndpoint())
        {
            await ImportRangeLocksAsync(keySpace, locks).ConfigureAwait(false);
            return;
        }

        await interNodeCommunication.ImportRangeLocks(leader, keySpace, locks, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Completion receipts whose key falls in <c>[startKey, endKey)</c> from this node's local store.
    /// Read locally (the receipt store is node-global, like the backend the range export reads).
    /// </summary>
    internal IReadOnlyCollection<CompletionReceiptRecord> GetLocalCompletionReceiptsForRange(string? startKey, string? endKey)
        => completionReceiptStore.SnapshotRange(startKey, endKey);

    /// <summary>Canonical transaction records whose anchor moves into <c>[startKey, endKey)</c> — the durable
    /// outcomes a split/merge must hand to the destination partition.</summary>
    internal IReadOnlyList<Transactions.Data.TransactionRecord> GetLocalTransactionRecordsForRange(string? startKey, string? endKey)
        => transactionRecordStore.SnapshotRange(startKey, endKey);

    /// <summary>Prepared intents whose key moves into <c>[startKey, endKey)</c> — the unresolved 2PC state a
    /// split/merge must hand to the destination partition.</summary>
    internal IReadOnlyList<Transactions.Data.PreparedIntent> GetLocalPreparedIntentsForRange(string? startKey, string? endKey)
        => preparedIntentStore.SnapshotRange(startKey, endKey);

    /// <summary>
    /// Replicates moved canonical records and prepared intents onto the destination partition through the ordered
    /// durable seam (which forwards to a remote destination leader), reconstructing them via the ordinary
    /// deterministic apply so every replica of the destination range holds them after cutover. Returns whether the
    /// whole handoff was durable; a false return must abort the split/merge cutover, so unresolved 2PC state is
    /// never stranded on a retired partition. Empty inputs are a no-op success.
    /// </summary>
    internal async Task<bool> ImportDurableTransactionStateToPartitionLeaderAsync(
        int partitionId,
        IReadOnlyList<Transactions.Data.TransactionRecord> records,
        IReadOnlyList<Transactions.Data.PreparedIntent> intents,
        CancellationToken cancellationToken)
    {
        if (records.Count > 0)
        {
            byte[] recordDelta = TransactionRecordStore.SerializeReconstructionDelta(records);
            if (!await ReplicateDurableThroughScheduler(partitionId, ReplicationTypes.TransactionRecord, recordDelta, cancellationToken).ConfigureAwait(false))
                return false;
        }

        if (intents.Count > 0)
        {
            byte[] intentDelta = PreparedIntentStore.SerializeDelta(
                intents.Select(i => (Transactions.Data.PreparedIntentCommand)new Transactions.Data.PrepareIntentCommand(i)));
            if (!await ReplicateDurableThroughScheduler(partitionId, ReplicationTypes.PreparedIntent, intentDelta, cancellationToken).ConfigureAwait(false))
                return false;
        }

        return true;
    }

    /// <summary>Records transferred completion receipts into this node's local store (state-transfer seeding).</summary>
    internal void ImportCompletionReceipts(IReadOnlyCollection<CompletionReceiptRecord> receiptsToImport)
        => completionReceiptStore.ImportRange(receiptsToImport);

    /// <summary>
    /// Replicates moved completion receipts onto the destination partition's Raft log, so every replica of
    /// the destination range holds them and a destination-leader change right after cutover still resolves a
    /// re-commit as <c>Committed</c>. Records them locally on success for immediate read-your-writes (the
    /// committed entry re-records idempotently on this node's own apply and on every follower). Returns whether
    /// the handoff was durable; a false return must abort the split/merge cutover.
    /// </summary>
    internal async Task<bool> ImportCompletionReceiptsReplicated(
        int partitionId,
        IReadOnlyCollection<CompletionReceiptRecord> receiptsToImport,
        CancellationToken cancellationToken)
    {
        if (receiptsToImport.Count == 0)
            return true;

        if (ReplicateReceiptImportFault is not null && ReplicateReceiptImportFault(partitionId))
            return false;

        byte[] data = CompletionReceiptStore.SerializeImport(receiptsToImport, partitionId);

        RaftReplicationResult result = await raft.ReplicateLogs(
            partitionId, ReplicationTypes.CompletionReceipt, data, cancellationToken: cancellationToken).ConfigureAwait(false);

        if (!result.Success)
        {
            logger.LogWarning(
                "Failed to replicate completion-receipt handoff Partition={Partition} Status={Status}",
                partitionId, result.Status);
            return false;
        }

        completionReceiptStore.ImportRange(receiptsToImport);
        return true;
    }

    /// <summary>
    /// Routes moved completion receipts to the leader of <paramref name="partitionId"/> for a replicated
    /// handoff. Forwards via IPC when this node is not the leader. Returns whether the handoff was durable on
    /// the destination partition; used by split/merge to gate cutover.
    /// </summary>
    internal async Task<bool> ImportCompletionReceiptsToPartitionLeaderAsync(
        int partitionId,
        IReadOnlyCollection<CompletionReceiptRecord> receiptsToImport,
        CancellationToken cancellationToken)
    {
        if (receiptsToImport.Count == 0)
            return true;

        if (!raft.Joined || await raft.AmILeader(partitionId, cancellationToken).ConfigureAwait(false))
            return await ImportCompletionReceiptsReplicated(partitionId, receiptsToImport, cancellationToken).ConfigureAwait(false);

        string leader = await raft.WaitForLeader(partitionId, cancellationToken).ConfigureAwait(false);
        if (leader == raft.GetLocalEndpoint())
            return await ImportCompletionReceiptsReplicated(partitionId, receiptsToImport, cancellationToken).ConfigureAwait(false);

        return await interNodeCommunication.ImportCompletionReceipts(leader, partitionId, receiptsToImport, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Replicates a receipt <b>forget</b> onto a participant partition's Raft log, so every replica of that
    /// partition drops the proof — not just the node that ran the coordinator drive. Forgets locally on success.
    /// Returns whether the forget was durable; only then may the decision record persist <c>ReceiptReleased</c>.
    /// </summary>
    internal async Task<bool> ForgetCompletionReceiptsReplicated(
        int partitionId,
        IReadOnlyCollection<CompletionReceiptRecord> receiptsToForget,
        CancellationToken cancellationToken)
    {
        if (receiptsToForget.Count == 0)
            return true;

        if (ReplicateReceiptForgetFault is not null && ReplicateReceiptForgetFault(partitionId))
            return false;

        byte[] data = CompletionReceiptStore.SerializeImport(receiptsToForget, partitionId, forget: true);

        RaftReplicationResult result = await raft.ReplicateLogs(
            partitionId, ReplicationTypes.CompletionReceipt, data, cancellationToken: cancellationToken).ConfigureAwait(false);

        if (!result.Success)
        {
            logger.LogWarning(
                "Failed to replicate completion-receipt forget Partition={Partition} Status={Status}",
                partitionId, result.Status);
            return false;
        }

        foreach (CompletionReceiptRecord receipt in receiptsToForget)
            completionReceiptStore.Forget(receipt.TransactionId, receipt.Key);

        return true;
    }

    /// <summary>
    /// Routes a receipt forget to the leader of <paramref name="partitionId"/> for a replicated forget, forwarding
    /// via IPC when this node is not the leader. Returns whether the forget was durable on that partition.
    /// </summary>
    internal async Task<bool> ForgetCompletionReceiptsToPartitionLeaderAsync(
        int partitionId,
        IReadOnlyCollection<CompletionReceiptRecord> receiptsToForget,
        CancellationToken cancellationToken)
    {
        if (receiptsToForget.Count == 0)
            return true;

        if (!raft.Joined || await raft.AmILeader(partitionId, cancellationToken).ConfigureAwait(false))
            return await ForgetCompletionReceiptsReplicated(partitionId, receiptsToForget, cancellationToken).ConfigureAwait(false);

        string leader = await raft.WaitForLeader(partitionId, cancellationToken).ConfigureAwait(false);
        if (leader == raft.GetLocalEndpoint())
            return await ForgetCompletionReceiptsReplicated(partitionId, receiptsToForget, cancellationToken).ConfigureAwait(false);

        return await interNodeCommunication.ImportCompletionReceipts(leader, partitionId, receiptsToForget, cancellationToken, forget: true).ConfigureAwait(false);
    }

    /// <summary>
    /// Participant-side recovery for the durable-intent path: on each partition this node leads, resolves due
    /// unresolved prepared intents to their canonical decision (presuming abort only past the decision deadline,
    /// for anchors this node also leads). No-op unless the durable-intent path is enabled. Runs off the request
    /// path; idempotent with a concurrent finalize.
    /// </summary>
    internal async Task RecoverPreparedIntents(CancellationToken cancellationToken)
    {
        if (preparedIntentStore.Count == 0)
            return;

        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        IReadOnlyList<PreparedIntent> due = preparedIntentStore.DueForRecovery(now);
        if (due.Count == 0)
            return;

        HashSet<int> partitions = [];
        foreach (PreparedIntent intent in due)
        {
            if (cancellationToken.IsCancellationRequested)
                return;

            int partitionId = locator.LocateRange(intent.Key).PartitionId;
            if (partitions.Contains(partitionId))
                continue;
            if (raft.Joined && !await raft.AmILeader(partitionId, cancellationToken).ConfigureAwait(false))
                continue;

            partitions.Add(partitionId);
        }

        if (partitions.Count == 0)
            return;

        DurableTransactionRecovery recovery = BuildPreparedIntentRecovery();
        foreach (int partitionId in partitions)
        {
            try
            {
                await recovery.SweepAsync(partitionId, now, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Prepared-intent recovery sweep failed for partition {Partition}", partitionId);
            }
        }
    }

    private DurableTransactionRecovery BuildPreparedIntentRecovery() => new(
        preparedIntentStore,
        // The scheduler seam is the single ordered apply owner: recovery's settle/materialize deltas apply in Raft
        // order alongside any concurrent finalizer decision for the same record, so the two cannot diverge.
        ReplicateDurableThroughScheduler,
        // Anchor record lookup routed to the anchor partition leader: a participant recovering an orphan intent
        // whose anchor lives on another node now reads the authoritative decision there instead of missing locally
        // and leaving it for the anchor's own sweep. A remote-anchor commit/abort is resolved directly; only a
        // genuinely absent/undecided record falls through to the leadership-gated drive-abort path.
        (transactionId, epoch, anchorKey, cancellationToken) => LookupDurableRecordRouted(transactionId, epoch, anchorKey, cancellationToken),
        DriveDurableAbortAsync);

    private async Task<TransactionRecord?> DriveDurableAbortAsync(AbortTransactionCommand abort, string anchorKey, CancellationToken cancellationToken)
    {
        int anchorPartition = locator.LocateRange(anchorKey).PartitionId;

        // Only drive/read the decision when this node leads the anchor partition; otherwise the local record store
        // is not authoritative and applying an abort locally could diverge from the real remote decision.
        if (raft.Joined && !await raft.AmILeader(anchorPartition, cancellationToken).ConfigureAwait(false))
            return null;

        // Drive the abort through the ordered scheduler seam (this node leads the anchor partition), which applies
        // it in Raft order — never overwriting a commit that won earlier in the log. Then read back the winner.
        byte[] delta = TransactionRecordStore.SerializeDelta([abort]);
        await ReplicateDurableThroughScheduler(anchorPartition, ReplicationTypes.TransactionRecord, delta, cancellationToken).ConfigureAwait(false);

        return transactionRecordStore.Get(abort.TransactionId, abort.Epoch);
    }

    /// <summary>
    /// Releases an exclusive range lock on the leader of <paramref name="partitionId"/>, forwarding
    /// via IPC if this node is not the leader. Used by <see cref="RangeSplitter"/> to release the
    /// quiesce lock on the <em>original</em> partition after cutover, bypassing the locator which
    /// would otherwise route to the newly-created partition.
    /// </summary>
    internal async Task<KeyValueResponseType> ReleaseExclusiveRangeLockOnPartitionLeaderAsync(
        int partitionId,
        HLCTimestamp transactionId,
        string keySpace,
        string? startKey, bool startInclusive,
        string? endKey, bool endInclusive,
        KeyValueDurability durability,
        CancellationToken cancellationToken)
    {
        if (!raft.Joined || await raft.AmILeader(partitionId, cancellationToken).ConfigureAwait(false))
            return await TryReleaseExclusiveRangeLock(transactionId, keySpace, startKey, startInclusive, endKey, endInclusive, durability).ConfigureAwait(false);

        string leader = await raft.WaitForLeader(partitionId, cancellationToken).ConfigureAwait(false);
        if (leader == raft.GetLocalEndpoint())
            return await TryReleaseExclusiveRangeLock(transactionId, keySpace, startKey, startInclusive, endKey, endInclusive, durability).ConfigureAwait(false);

        return await interNodeCommunication.TryReleaseExclusiveRangeLock(leader, transactionId, keySpace, startKey, startInclusive, endKey, endInclusive, durability, cancellationToken).ConfigureAwait(false);
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
            KeyValueRequest request = KeyValueRequestPool.Rent(
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
                key.durability,
                0,
                0,
                null
            );

            try
            {
                KeyValueResponse? response;

                if (key.durability == KeyValueDurability.Ephemeral)
                    response = await AskKeyValueActor(ephemeralKeyValuesRouter, request);
                else
                    response = await AskKeyValueActor(persistentKeyValuesRouter, request);

                if (response is null || response.Type == KeyValueResponseType.WaitingForReplication)
                {
                    responses.Add((KeyValueResponseType.Errored, key.key, key.durability));
                    continue;
                }

                responses.Add((response.Type, key.key, key.durability));
            }
            finally
            {
                KeyValueRequestPool.Return(request);
            }
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
    public async Task<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)> TryPrepareMutations(
        HLCTimestamp transactionId,
        HLCTimestamp commitId,
        string key,
        KeyValueDurability durability,
        long routedGeneration = 0,
        string? recordAnchorKey = null
    )
    {
        KeyValueRequest request = KeyValueRequestPool.Rent(
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
            durability,
            0,
            0,
            null
        );

        request.RoutedGeneration = routedGeneration;
        request.RecordAnchorKey = recordAnchorKey;

        try
        {
            foreach (TimeSpan delay in Backoff.DecorrelatedJitterBackoffV2(medianFirstRetryDelay: TimeSpan.FromMilliseconds(1), retryCount: MaxRetries))
            {
                KeyValueResponse? response;

                if (durability == KeyValueDurability.Ephemeral)
                    response = await AskKeyValueActor(ephemeralKeyValuesRouter, request);
                else
                    response = await AskKeyValueActor(persistentKeyValuesRouter, request);

                if (response is null)
                    return (KeyValueResponseType.Errored, HLCTimestamp.Zero, key, durability);

                if (response.Type == KeyValueResponseType.WaitingForReplication)
                {
                    await Task.Delay(delay);
                    continue;
                }

                return (response.Type, response.Ticket, key, durability);
            }

            return (KeyValueResponseType.MustRetry, HLCTimestamp.Zero, key, durability);
        }
        finally
        {
            KeyValueRequestPool.Return(request);
        }
    }

    /// <summary>
    /// Passes many TryPrepare requests to the key/value actor for the given keys.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="commitId"></param>
    /// <param name="keys"></param>
    /// <returns></returns>
    public async Task<List<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)>> TryPrepareManyMutations(
        HLCTimestamp transactionId,
        HLCTimestamp commitId,
        List<(string key, KeyValueDurability durability)> keys,
        string? recordAnchorKey = null
    )
    {
        // Persistent participants that share a Raft partition are proposed as ONE batched ReplicateLogs —
        // one WAL proposal, one fsync — instead of one propose per key, which is where the fsync
        // amplification on a bulk write (a table's rows all land in one key space ⇒ one partition) comes
        // from. Ephemeral participants have no Raft proposal, and a not-joined single node has nothing to
        // batch to; both keep the per-key prepare.
        List<(string key, KeyValueDurability durability)> batchable = [];
        List<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)> results = new(keys.Count);

        foreach ((string key, KeyValueDurability durability) key in keys)
        {
            if (key.durability == KeyValueDurability.Persistent && raft.Joined)
                batchable.Add(key);
            else
                results.Add(await PrepareOneMutation(transactionId, commitId, key, recordAnchorKey));
        }

        if (batchable.Count == 0)
            return results;

        Dictionary<(int PartitionId, long Generation), List<(string key, KeyValueDurability durability)>> groups =
            GroupByPartition(keySpaceRegistry, rangeMapStore.Current, new DataPartitionRouter(raft), batchable, k => k.key);

        foreach (((int partitionId, long generation), List<(string key, KeyValueDurability durability)> items) in groups)
            results.AddRange(await StageAndProposePartition(transactionId, commitId, partitionId, generation, items, recordAnchorKey));

        return results;
    }

    /// <summary>
    /// Prepares one mutation the per-key way: dispatches a <c>TryPrepareMutations</c> to the owning actor,
    /// which proposes (or, for a persistent key, dispatches the propose off its mailbox) and returns the
    /// per-key proposal ticket. Used for ephemeral participants and the not-joined single-node case, which
    /// are not batched.
    /// </summary>
    private async Task<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)> PrepareOneMutation(
        HLCTimestamp transactionId,
        HLCTimestamp commitId,
        (string key, KeyValueDurability durability) key,
        string? recordAnchorKey
    )
    {
        KeyValueRequest request = KeyValueRequestPool.Rent(
            KeyValueRequestType.TryPrepareMutations,
            transactionId, commitId, key.key,
            null, null, -1, KeyValueFlags.None, 0, HLCTimestamp.Zero, key.durability, 0, 0, null);

        request.RecordAnchorKey = recordAnchorKey;

        try
        {
            KeyValueResponse? response = key.durability == KeyValueDurability.Ephemeral
                ? await AskKeyValueActor(ephemeralKeyValuesRouter, request)
                : await AskKeyValueActor(persistentKeyValuesRouter, request);

            if (response is null || response.Type == KeyValueResponseType.WaitingForReplication)
                return (KeyValueResponseType.Errored, HLCTimestamp.Zero, key.key, key.durability);

            return (response.Type, response.Ticket, key.key, key.durability);
        }
        finally
        {
            KeyValueRequestPool.Return(request);
        }
    }

    /// <summary>
    /// Stages every participant of one partition (validate + pin write intent + build proposal, no Raft),
    /// then proposes the whole set in a single <c>ReplicateLogs</c> so the group shares one WAL proposal and
    /// one ticket. Keys that contributed a proposal share the returned ticket; a key that prepared with
    /// nothing to propose (an Undefined read) keeps a Zero ticket, exactly as the per-key path returns.
    ///
    /// <para>Staging installs each write intent before the batch proposes. If any key fails staging — or the
    /// batch propose itself fails — the batch cannot commit a partial set, so it unwinds the intents it did
    /// stage (they hold no ticket the coordinator would roll back) and reports every key of the partition as
    /// failed, aborting the transaction cleanly with no leaked intent.</para>
    /// </summary>
    private async Task<List<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)>> StageAndProposePartition(
        HLCTimestamp transactionId,
        HLCTimestamp commitId,
        int partitionId,
        long generation,
        List<(string key, KeyValueDurability durability)> items,
        string? recordAnchorKey
    )
    {
        // Test seam: force this group's prepare outcome without staging or proposing, so a test can fail one
        // partition's prepare while another prepares for real and drive the coordinator's rollback of the
        // prepared partition.
        if (ForcePrepareOutcomeForKey?.Invoke(items[0].key) is KeyValueResponseType forcedPrepare)
            return items.Select(i => (forcedPrepare, HLCTimestamp.Zero, i.key, i.durability)).ToList();

        // Exception-safe wrapper: any throw during staging or propose unwinds every intent staged so far, so a
        // partition batch that faults mid-flight cannot leak an entry-pinning intent. The staged list is owned
        // here so the catch can unwind whatever the core managed to stage.
        List<(string key, KeyValueDurability durability, KeyValueResponseType type, byte[]? proposal)> staged = new(items.Count);

        try
        {
            return await StageAndProposePartitionCore(transactionId, commitId, partitionId, generation, items, recordAnchorKey, staged);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Batched stage/propose threw Partition={Partition}; unwinding {Count} staged intent(s)", partitionId, staged.Count);
            await UnwindStagedIntents(transactionId, staged);
            return items.Select(i => (KeyValueResponseType.MustRetry, HLCTimestamp.Zero, i.key, i.durability)).ToList();
        }
    }

    private async Task<List<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)>> StageAndProposePartitionCore(
        HLCTimestamp transactionId,
        HLCTimestamp commitId,
        int partitionId,
        long generation,
        List<(string key, KeyValueDurability durability)> items,
        string? recordAnchorKey,
        List<(string key, KeyValueDurability durability, KeyValueResponseType type, byte[]? proposal)> staged
    )
    {
        foreach ((string key, KeyValueDurability durability) item in items)
        {
            KeyValueRequest request = KeyValueRequestPool.Rent(
                KeyValueRequestType.StagePrepareMutations,
                transactionId, commitId, item.key,
                null, null, -1, KeyValueFlags.None, 0, HLCTimestamp.Zero, item.durability, 0, 0, null);

            request.RecordAnchorKey = recordAnchorKey;
            request.RoutedGeneration = generation;

            try
            {
                KeyValueResponse? response = await AskKeyValueActor(persistentKeyValuesRouter, request);
                staged.Add(response is null
                    ? (item.key, item.durability, KeyValueResponseType.Errored, null)
                    : (item.key, item.durability, response.Type, response.StagedProposal));
            }
            finally
            {
                KeyValueRequestPool.Return(request);
            }
        }

        if (staged.Any(s => s.type != KeyValueResponseType.Prepared))
        {
            await UnwindStagedIntents(transactionId, staged);

            // Report the staged-and-unwound keys as failed too — the whole partition batch aborted.
            return staged.Select(s => (
                s.type == KeyValueResponseType.Prepared ? KeyValueResponseType.Errored : s.type,
                HLCTimestamp.Zero, s.key, s.durability)).ToList();
        }

        List<byte[]> batch = [];
        foreach ((string _, KeyValueDurability _, KeyValueResponseType _, byte[]? proposal) in staged)
            if (proposal is not null)
                batch.Add(proposal);

        // Every key prepared with nothing to propose (all Undefined reads): nothing to replicate, all
        // Prepared with a Zero ticket.
        if (batch.Count == 0)
            return staged.Select(s => (KeyValueResponseType.Prepared, HLCTimestamp.Zero, s.key, s.durability)).ToList();

        // The propose is deliberately NOT deadline-bounded. Prepare does not retry — the coordinator aborts on
        // any non-Prepared participant — so cancelling ReplicateLogs mid-quorum would abort valid transactions
        // and can leave a proposal half-registered. A stalled propose is already bounded by the write-intent
        // lease (DefaultTxCompleteTimeout), which pins the staged entries; the deadline that matters for the
        // lease-outliving race is on the commit, which retries idempotently. (Verified: a propose deadline
        // breaks the deadline-bounded recovery path.)
        // expectedGeneration is 0 (fence disabled) on purpose: `generation` here is the Kahuna range-descriptor
        // generation used to group and route the batch, NOT the physical Raft partition generation that
        // Kommander's expectedGeneration fences against. The two are independent — a range split bumps the
        // descriptor generation in place without touching the physical partition generation — so fencing this
        // proposal by the descriptor generation would reject every post-split key-range prepare with
        // PartitionMoved. The descriptor-move fence already ran per key on the actor (TryFenceKeyRange against
        // the current range map) during staging, before this propose.
        RaftReplicationResult result = await raft.ReplicateLogs(
            partitionId, ReplicationTypes.KeyValues, batch, autoCommit: false, expectedGeneration: 0);

        if (!result.Success)
        {
            logger.LogWarning("Batched propose failed Partition={Partition} Count={Count} Status={Status}", partitionId, batch.Count, result.Status);

            await UnwindStagedIntents(transactionId, staged);

            // A propose that never landed leaves nothing committed, so the transaction can safely abort. Leader
            // loss / a moved partition are retryable rather than definite; surface those as MustRetry.
            KeyValueResponseType outcome = IsTransientRaftStatus(result.Status)
                ? KeyValueResponseType.MustRetry
                : KeyValueResponseType.Errored;

            return staged.Select(s => (outcome, HLCTimestamp.Zero, s.key, s.durability)).ToList();
        }

        HLCTimestamp ticket = result.TicketId;

        return staged.Select(s => (
            KeyValueResponseType.Prepared,
            s.proposal is not null ? ticket : HLCTimestamp.Zero,
            s.key, s.durability)).ToList();
    }

    /// <summary>
    /// Unwinds the write intents of the keys that staged successfully (a failed batch never proposed them,
    /// so they hold no ticket the coordinator would roll back) via a no-Raft local rollback on each. The clear
    /// is <b>positively acknowledged</b>: a dropped or actor-busy clear (mapped to MustRetry) is retried rather
    /// than silently treated as done, because a swallowed clear leaves an entry-pinning intent the coordinator
    /// never sees. <see cref="ApplyRolledBackMutationsHandler"/> is idempotent, so re-driving is safe.
    /// </summary>
    private async Task UnwindStagedIntents(
        HLCTimestamp transactionId,
        List<(string key, KeyValueDurability durability, KeyValueResponseType type, byte[]? proposal)> staged)
    {
        foreach ((string key, KeyValueDurability durability, KeyValueResponseType type, byte[]? _) in staged)
        {
            if (type != KeyValueResponseType.Prepared)
                continue;

            for (int attempt = 0; attempt < MaxRetries; attempt++)
            {
                KeyValueRequest request = KeyValueRequestPool.Rent(
                    KeyValueRequestType.ApplyRolledBackMutations,
                    transactionId, HLCTimestamp.Zero, key,
                    null, null, -1, KeyValueFlags.None, 0, HLCTimestamp.Zero, durability, 0, 0, null);

                try
                {
                    KeyValueResponse? response = await AskKeyValueActor(persistentKeyValuesRouter, request);
                    if (response?.Type == KeyValueResponseType.RolledBack)
                        break;

                    logger.LogWarning("Unwind of staged intent {Key} returned {Type}; retrying", key, response?.Type);
                }
                finally
                {
                    KeyValueRequestPool.Return(request);
                }
            }
        }
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
        KeyValueRequest request = KeyValueRequestPool.Rent(
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
            durability,
            0,
            0,
            null
        );
        
        try
        {
            foreach (TimeSpan delay in Backoff.DecorrelatedJitterBackoffV2(medianFirstRetryDelay: TimeSpan.FromMilliseconds(1), retryCount: MaxRetries))
            {
                KeyValueResponse? response;

                if (durability == KeyValueDurability.Ephemeral)
                    response = await AskKeyValueActor(ephemeralKeyValuesRouter, request);
                else
                    response = await AskKeyValueActor(persistentKeyValuesRouter, request);

                if (response is null)
                    return (KeyValueResponseType.Errored, -1);

                if (response.Type == KeyValueResponseType.WaitingForReplication)
                {
                    await Task.Delay(delay);
                    continue;
                }

                return (response.Type, response.Revision);
            }

            return (KeyValueResponseType.Errored, -1);
        }
        finally
        {
            KeyValueRequestPool.Return(request);
        }
    }

    /// <summary>
    /// Passes many TryCommit requests to the key/value actor for the given keyValue name.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="keys"></param>
    /// <returns></returns>
    public async Task<List<(KeyValueResponseType type, string key, long proposalIndex, KeyValueDurability durability)>> TryCommitManyMutations(
        HLCTimestamp transactionId,
        List<(string key, HLCTimestamp proposalTicketId, KeyValueDurability durability)> keys
    )
    {
        // Persistent participants that share a Raft partition proposal ticket commit once: a single
        // CommitLogs settles the whole partition (idempotent on repeat), then each key applies its confirmed
        // mutation with no further Raft — the commit half of collapsing N per-key round trips into one.
        // Ephemeral keys, and persistent keys with no ticket (an Undefined read, or a not-joined single
        // node), keep the per-key commit.
        Dictionary<HLCTimestamp, List<(string key, KeyValueDurability durability)>> byTicket = [];
        List<(string key, HLCTimestamp proposalTicketId, KeyValueDurability durability)> perKey = [];

        foreach ((string key, HLCTimestamp proposalTicketId, KeyValueDurability durability) key in keys)
        {
            if (key.durability == KeyValueDurability.Persistent && key.proposalTicketId != HLCTimestamp.Zero && raft.Joined)
            {
                if (byTicket.TryGetValue(key.proposalTicketId, out List<(string key, KeyValueDurability durability)>? group))
                    group.Add((key.key, key.durability));
                else
                    byTicket[key.proposalTicketId] = [(key.key, key.durability)];
            }
            else
                perKey.Add(key);
        }

        List<(KeyValueResponseType type, string key, long proposalIndex, KeyValueDurability durability)> results = new(keys.Count);

        foreach ((string key, HLCTimestamp proposalTicketId, KeyValueDurability durability) key in perKey)
            results.Add(await CommitOneMutation(transactionId, key));

        foreach ((HLCTimestamp ticket, List<(string key, KeyValueDurability durability)> group) in byTicket)
            results.AddRange(await CommitTicketGroup(transactionId, ticket, group));

        return results;
    }

    /// <summary>
    /// Commits one mutation the per-key way: dispatches a <c>TryCommitMutations</c> to the owning actor,
    /// which commits its ticket (or, for a persistent key, off its mailbox) and applies. Used for ephemeral
    /// participants and persistent keys with no shared ticket, which are not batched.
    /// </summary>
    private async Task<(KeyValueResponseType type, string key, long proposalIndex, KeyValueDurability durability)> CommitOneMutation(
        HLCTimestamp transactionId,
        (string key, HLCTimestamp proposalTicketId, KeyValueDurability durability) key)
    {
        KeyValueRequest request = KeyValueRequestPool.Rent(
            KeyValueRequestType.TryCommitMutations, transactionId, HLCTimestamp.Zero, key.key,
            null, null, -1, KeyValueFlags.None, 0, key.proposalTicketId, key.durability, 0, 0, null);

        try
        {
            KeyValueResponse? response = key.durability == KeyValueDurability.Ephemeral
                ? await AskKeyValueActor(ephemeralKeyValuesRouter, request)
                : await AskKeyValueActor(persistentKeyValuesRouter, request);

            if (response is null || response.Type == KeyValueResponseType.WaitingForReplication)
                return (KeyValueResponseType.Errored, key.key, -1, key.durability);

            return (response.Type, key.key, response.Revision, key.durability);
        }
        finally
        {
            KeyValueRequestPool.Return(request);
        }
    }

    /// <summary>
    /// Commits every participant of one partition that shares a proposal ticket: one <c>CommitLogs</c>
    /// settles the ticket durably (idempotent if an earlier retry already committed it), then each key applies
    /// its confirmed mutation on its owning actor with no further Raft.
    ///
    /// <para>The commit is bounded by the phase-two deadline so a stalled leader cannot let the shared ticket
    /// outlive the participants' write-intent lease. A commit that does <b>not</b> take effect (leadership
    /// loss, a missing proposal after failover, queue/timeout, or cancellation) is retryable — the whole
    /// group returns MustRetry so the coordinator re-drives or re-routes; only a genuine hard error, where the
    /// commit provably never applied, is Errored. Once <c>CommitLogs</c> succeeds every log in the group is
    /// durable, so from that point <b>no participant may report a definite failure</b>: an apply that cannot
    /// archive locally is in-doubt (Committed on proof, else MustRetry), never Errored/Aborted.</para>
    /// </summary>
    private async Task<List<(KeyValueResponseType type, string key, long proposalIndex, KeyValueDurability durability)>> CommitTicketGroup(
        HLCTimestamp transactionId,
        HLCTimestamp ticket,
        List<(string key, KeyValueDurability durability)> group)
    {
        // Test seam: force this group's outcome without committing, so a cross-partition partial-commit test
        // can fail one partition while another commits for real.
        if (ForceCommitOutcomeForKey?.Invoke(group[0].key) is KeyValueResponseType forced)
            return group.Select(g => (forced, g.key, -1L, g.durability)).ToList();

        // Every key of a ticket shared one partition when the batch was proposed. Re-resolve them now: if a
        // range split or move has divided them across partitions since prepare, the shared ticket can no
        // longer be committed on a single partition without stranding the moved keys' logs on a partition that
        // reads no longer route to. Committing the ticket on the original partition would silently lose those
        // writes; committing on a new partition finds no ticket. So do NOT commit — return in-doubt MustRetry
        // so the transaction re-drives and re-prepares against the current partition. Nothing is stranded and
        // nothing is falsely aborted. (Fully committing across a split would require the range splitter to
        // drain or hand off in-flight prepared tickets before cutover — a separate range-machinery change.)
        if (!TryResolveCohesiveTicketPartition(group, out int partitionId))
        {
            logger.LogWarning("Batched commit: prepared ticket group split across partitions since prepare; MustRetry");
            return group.Select(g => (KeyValueResponseType.MustRetry, g.key, -1L, g.durability)).ToList();
        }

        // NOTE: the batched commit is intentionally NOT deadline-bounded yet. A Phase2CommitTimeout token
        // makes CommitLogs return OperationCancelled while the queued commit still applies async (Kommander
        // contract); the coordinator then re-drives, and the retry must re-apply the now-committed ticket
        // whose replicator apply deferred to the still-live write intent. Getting that trip → retry → apply
        // sequence correct in the batched path needs the deferred-apply-on-retry semantics resolved; until
        // then the write-intent lease (DefaultTxCompleteTimeout) bounds the intent pinning. Tracked as a
        // deferred remediation item.
        (bool success, RaftOperationStatus status, long commitIndex) = await raft.CommitLogs(partitionId, ticket, CancellationToken.None);

        if (!success)
        {
            logger.LogWarning("Batched commit failed Partition={Partition} Count={Count} Status={Status}", partitionId, group.Count, status);

            // The ticket did not commit here, so nothing in this group is durable yet. Everything retryable —
            // leadership loss, a missing proposal (failover truncated the uncommitted entry, or the ticket is
            // on another leader), queue/timeout, cancellation — returns MustRetry so the coordinator re-drives
            // or re-routes; only a genuine hard error aborts, and only because the commit provably never took.
            KeyValueResponseType outcome = IsTransientRaftStatus(status)
                ? KeyValueResponseType.MustRetry
                : KeyValueResponseType.Errored;

            return group.Select(g => (outcome, g.key, -1L, g.durability)).ToList();
        }

        // Durable from here. The shared commit index is the per-key proposal index. A non-Committed apply
        // (in-doubt, actor busy, intent replaced by a later owner) becomes MustRetry — never Errored — so a
        // committed group can never surface an abort.
        List<(KeyValueResponseType type, string key, long proposalIndex, KeyValueDurability durability)> results = new(group.Count);

        foreach ((string key, KeyValueDurability durability) g in group)
        {
            KeyValueRequest request = KeyValueRequestPool.Rent(
                KeyValueRequestType.ApplyCommittedMutations, transactionId, HLCTimestamp.Zero, g.key,
                null, null, -1, KeyValueFlags.None, 0, ticket, g.durability, 0, 0, null);

            try
            {
                KeyValueResponse? response = await AskKeyValueActor(persistentKeyValuesRouter, request);
                KeyValueResponseType applied = response?.Type == KeyValueResponseType.Committed
                    ? KeyValueResponseType.Committed
                    : KeyValueResponseType.MustRetry;
                results.Add((applied, g.key, commitIndex, g.durability));
            }
            finally
            {
                KeyValueRequestPool.Return(request);
            }
        }

        return results;
    }

    /// <summary>
    /// Resolves the partition a ticket group's keys currently share. Returns false when a range split/move has
    /// divided them across partitions since prepare — the shared ticket can no longer be settled on one
    /// partition, so the batched commit/rollback must defer to in-doubt MustRetry rather than risk stranding.
    /// Honours the commit-partition test seam.
    /// </summary>
    private bool TryResolveCohesiveTicketPartition(List<(string key, KeyValueDurability durability)> group, out int partitionId)
    {
        DataPartitionRouter router = new(raft);
        RangeMap rangeMap = rangeMapStore.Current;

        int Resolve(string key) => PartitionOverrideForCommit?.Invoke(key) ?? RangeRouting.Locate(keySpaceRegistry, rangeMap, router, key).PartitionId;

        partitionId = Resolve(group[0].key);
        for (int i = 1; i < group.Count; i++)
            if (Resolve(group[i].key) != partitionId)
                return false;

        return true;
    }

    private static bool IsTransientRaftStatus(RaftOperationStatus status) => status is
        RaftOperationStatus.NodeIsNotLeader or
        RaftOperationStatus.ProposalQueueFull or
        RaftOperationStatus.RestoreInProgress or
        RaftOperationStatus.ProposalTimeout or
        RaftOperationStatus.ReplicationFailed or
        RaftOperationStatus.OperationCancelled or
        RaftOperationStatus.ProposalNotFound;
    
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
            durability,
            0,
            0,
            null
        );

        KeyValueResponse? response;
        
        if (durability == KeyValueDurability.Ephemeral)
            response = await AskKeyValueActor(ephemeralKeyValuesRouter, request);
        else
            response = await AskKeyValueActor(persistentKeyValuesRouter, request);
        
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
        // Participants that share a Raft partition proposal ticket roll back once: a single RollbackLogs
        // settles the ticket (idempotent on repeat), then each key clears its local prepare state with no
        // further Raft — so an abort no longer issues N concurrent RollbackLogs for one ticket. Ephemeral keys,
        // and persistent keys with no ticket, keep the per-key rollback.
        Dictionary<HLCTimestamp, List<(string key, KeyValueDurability durability)>> byTicket = [];
        List<(string key, HLCTimestamp proposalTicketId, KeyValueDurability durability)> perKey = [];

        foreach ((string key, HLCTimestamp proposalTicketId, KeyValueDurability durability) key in keys)
        {
            if (key.durability == KeyValueDurability.Persistent && key.proposalTicketId != HLCTimestamp.Zero && raft.Joined)
            {
                if (byTicket.TryGetValue(key.proposalTicketId, out List<(string key, KeyValueDurability durability)>? group))
                    group.Add((key.key, key.durability));
                else
                    byTicket[key.proposalTicketId] = [(key.key, key.durability)];
            }
            else
                perKey.Add(key);
        }

        List<(KeyValueResponseType type, string key, long proposalIndex, KeyValueDurability durability)> results = new(keys.Count);

        foreach ((string key, HLCTimestamp proposalTicketId, KeyValueDurability durability) key in perKey)
            results.Add(await RollbackOneMutation(transactionId, key));

        foreach ((HLCTimestamp ticket, List<(string key, KeyValueDurability durability)> group) in byTicket)
            results.AddRange(await RollbackTicketGroup(transactionId, ticket, group));

        return results;
    }

    /// <summary>
    /// Rolls back one mutation the per-key way: dispatches a <c>TryRollbackMutations</c> to the owning actor,
    /// which rolls back its ticket and clears the local prepare state. Used for ephemeral participants and
    /// persistent keys with no shared ticket, which are not batched.
    /// </summary>
    private async Task<(KeyValueResponseType type, string key, long proposalIndex, KeyValueDurability durability)> RollbackOneMutation(
        HLCTimestamp transactionId,
        (string key, HLCTimestamp proposalTicketId, KeyValueDurability durability) key)
    {
        KeyValueRequest request = KeyValueRequestPool.Rent(
            KeyValueRequestType.TryRollbackMutations, transactionId, HLCTimestamp.Zero, key.key,
            null, null, -1, KeyValueFlags.None, 0, key.proposalTicketId, key.durability, 0, 0, null);

        try
        {
            KeyValueResponse? response = key.durability == KeyValueDurability.Ephemeral
                ? await AskKeyValueActor(ephemeralKeyValuesRouter, request)
                : await AskKeyValueActor(persistentKeyValuesRouter, request);

            if (response is null)
                return (KeyValueResponseType.Errored, key.key, -1, key.durability);

            return (response.Type, key.key, response.Revision, key.durability);
        }
        finally
        {
            KeyValueRequestPool.Return(request);
        }
    }

    /// <summary>
    /// Rolls back every participant of one partition that shares a proposal ticket: one <c>RollbackLogs</c>
    /// settles the ticket (idempotent if an earlier retry already rolled it back), then each key clears its
    /// local prepare state (write intent + MVCC) with no further Raft. A ticket group split across partitions
    /// since prepare, or a rollback that could not be settled, returns MustRetry so the participant is retained
    /// and re-driven rather than abandoned — only a settled rollback is positive proof the effect is gone.
    /// </summary>
    private async Task<List<(KeyValueResponseType type, string key, long proposalIndex, KeyValueDurability durability)>> RollbackTicketGroup(
        HLCTimestamp transactionId,
        HLCTimestamp ticket,
        List<(string key, KeyValueDurability durability)> group)
    {
        if (!TryResolveCohesiveTicketPartition(group, out int partitionId))
        {
            logger.LogWarning("Batched rollback: prepared ticket group split across partitions since prepare; MustRetry");
            return group.Select(g => (KeyValueResponseType.MustRetry, g.key, -1L, g.durability)).ToList();
        }

        (bool success, RaftOperationStatus status, long _) = await raft.RollbackLogs(partitionId, ticket, CancellationToken.None);

        if (!success)
        {
            logger.LogWarning("Batched rollback failed Partition={Partition} Count={Count} Status={Status}", partitionId, group.Count, status);

            // Anything short of a settled rollback is retryable — a rollback that did not take effect leaves
            // the prepare state possibly intact, so the participants are retained (MustRetry), never abandoned.
            // Only a proposal that already committed (rollback impossible) is a hard error the coordinator must
            // see.
            KeyValueResponseType outcome = status == RaftOperationStatus.Errored
                ? KeyValueResponseType.Errored
                : KeyValueResponseType.MustRetry;

            return group.Select(g => (outcome, g.key, -1L, g.durability)).ToList();
        }

        // The ticket is rolled back in Raft. Clear each key's local prepare state; a dropped/actor-busy clear
        // is retryable (MustRetry) so the intent is not left dangling.
        List<(KeyValueResponseType type, string key, long proposalIndex, KeyValueDurability durability)> results = new(group.Count);

        foreach ((string key, KeyValueDurability durability) g in group)
        {
            KeyValueRequest request = KeyValueRequestPool.Rent(
                KeyValueRequestType.ApplyRolledBackMutations, transactionId, HLCTimestamp.Zero, g.key,
                null, null, -1, KeyValueFlags.None, 0, HLCTimestamp.Zero, g.durability, 0, 0, null);

            try
            {
                KeyValueResponse? response = await AskKeyValueActor(persistentKeyValuesRouter, request);
                KeyValueResponseType applied = response?.Type == KeyValueResponseType.RolledBack
                    ? KeyValueResponseType.RolledBack
                    : KeyValueResponseType.MustRetry;
                results.Add((applied, g.key, -1L, g.durability));
            }
            finally
            {
                KeyValueRequestPool.Return(request);
            }
        }

        return results;
    }

    /// <summary>
    /// Schedule a key/value transaction to be executed
    /// </summary>
    /// <param name="script"></param>
    /// <param name="hash"></param>
    /// <param name="parameters"></param>
    /// <returns></returns>
    public Task<KeyValueTransactionResult> TryExecuteTx(ReadOnlyMemory<byte> script, string? hash, List<KeyValueParameter>? parameters)
    {
        return scriptExecutor.TryExecuteTx(script, hash, parameters);
    }

    /// <summary>
    /// Scans all nodes in the cluster and returns key/value pairs by prefix 
    /// </summary>
    /// <param name="prefixKeyName"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public Task<KeyValueGetByBucketResult> ScanAllByPrefix(string prefixKeyName, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken)
    {
        return locator.ScanAllByPrefix(prefixKeyName, readTimestamp, durability, cancellationToken);
    }

    /// <summary>
    /// Scans the current node and returns key/value pairs by prefix
    /// The returned values aren't consistent, they can contain stale data
    /// </summary>
    /// <param name="prefixKeyName"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    /// <exception cref="KahunaServerException"></exception>
    public async Task<KeyValueGetByBucketResult> ScanByPrefix(string prefixKeyName, HLCTimestamp readTimestamp, KeyValueDurability durability)
    {
        KeyValueRequest request = new(
            KeyValueRequestType.ScanByPrefix,
            HLCTimestamp.Zero,
            HLCTimestamp.Zero,
            prefixKeyName,
            null,
            null,
            -1,
            KeyValueFlags.None,
            0,
            HLCTimestamp.Zero,
            durability,
            0,
            0,
            null
        );

        request.ReadTimestamp = readTimestamp;

        List<(string, ReadOnlyKeyValueEntry)> items = [];
        
        if (durability == KeyValueDurability.Ephemeral)
        {
            List<Task<KeyValueResponse?>> tasks = new(ephemeralInstances.Count);
            
            // Ephemeral GetByBucket does a brute force search on every ephemeral actor
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
            
            // Persistent GetByBucket does a brute force search on every persistent actor
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
    /// Scans the current node and returns key/value pairs by prefix
    /// The returned values aren't consistent, they can contain stale data
    /// </summary>
    /// <param name="prefixKeyName"></param>    
    /// <returns></returns>
    /// <exception cref="KahunaServerException"></exception>
    public async Task<KeyValueGetByBucketResult> ScanByPrefixFromDisk(string prefixKeyName, HLCTimestamp readTimestamp)
    {
        KeyValueRequest request = new(
            KeyValueRequestType.ScanByPrefixFromDisk,
            HLCTimestamp.Zero,
            HLCTimestamp.Zero,
            prefixKeyName,
            null,
            null,
            -1,
            KeyValueFlags.None,
            0,
            HLCTimestamp.Zero,
            KeyValueDurability.Persistent,
            0,
            0,
            null
        );

        request.ReadTimestamp = readTimestamp;

        KeyValueResponse? response = await AskKeyValueActor(persistentKeyValuesRouter, request);

        if (response is null)
            return new(KeyValueResponseType.Errored, []);
        
        if (response is { Type: KeyValueResponseType.Get, Items: not null })
            return new(response.Type, response.Items); 
        
        return new(response.Type, []);
    }

    /// <summary>
    /// Returns a consistent snapshot of key/value pairs that matches the specified prefix
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="prefixKeyName"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public async Task<KeyValueGetByBucketResult> GetByBucket(HLCTimestamp transactionId, string prefixKeyName, HLCTimestamp readTimestamp, KeyValueDurability durability)
    {
        KeyValueRequest request = KeyValueRequestPool.Rent(
            KeyValueRequestType.GetByBucket,
            transactionId,
            HLCTimestamp.Zero,
            prefixKeyName,
            null,
            null,
            -1,
            KeyValueFlags.None,
            0,
            HLCTimestamp.Zero,
            durability,
            0,
            0,
            null
        );

        request.ReadTimestamp = readTimestamp;

        try
        {
            int backoffMs = 1;
            long deadline = Environment.TickCount64 + 16_500;

            while (true)
            {
                KeyValueResponse? response;

                if (durability == KeyValueDurability.Ephemeral)
                    response = await AskKeyValueActor(ephemeralKeyValuesRouter, request);
                else
                    response = await AskKeyValueActor(persistentKeyValuesRouter, request);

                if (response is null)
                    return new(KeyValueResponseType.Errored, []);

                if (response.Type != KeyValueResponseType.WaitingForReplication)
                {
                    if (response is { Type: KeyValueResponseType.Get, Items: not null })
                        return new(response.Type, response.Items);
                    return new(response.Type, []);
                }

                if (Environment.TickCount64 >= deadline)
                    return new(KeyValueResponseType.MustRetry, []);

                await Task.Delay(backoffMs);
                backoffMs = Math.Min(backoffMs * 2, 1000);
            }
        }
        finally
        {
            KeyValueRequestPool.Return(request);
        }
    }

    /// <summary>
    /// Executes a bounded, cursor-paged range scan over keys starting with <paramref name="prefix"/>.
    /// </summary>
    public async Task<KeyValueGetByRangeResult> GetByRange(
        HLCTimestamp transactionId,
        string prefix,
        string? startKey,
        bool startInclusive,
        string? endKey,
        bool endInclusive,
        int limit,
        HLCTimestamp readTimestamp,
        KeyValueDurability durability)
    {
        KeyValueRequest request = KeyValueRequestPool.RentRange(
            transactionId,
            prefix,
            startKey,
            startInclusive,
            endKey,
            endInclusive,
            limit,
            readTimestamp,
            durability,
            null
        );

        try
        {
            foreach (TimeSpan delay in Backoff.DecorrelatedJitterBackoffV2(medianFirstRetryDelay: TimeSpan.FromMilliseconds(1), retryCount: MaxRetries))
            {
                KeyValueResponse? response;

                if (durability == KeyValueDurability.Ephemeral)
                    response = await AskKeyValueActor(ephemeralKeyValuesRouter, request);
                else
                    response = await AskKeyValueActor(persistentKeyValuesRouter, request);

                if (response is null)
                    return new(KeyValueResponseType.Errored, [], null, false);

                if (response.Type == KeyValueResponseType.WaitingForReplication)
                {
                    await Task.Delay(delay);
                    continue;
                }

                if (response.RangeResult is not null)
                    return response.RangeResult;

                return new(response.Type, [], null, false);
            }

            return new(KeyValueResponseType.MustRetry, [], null, false);
        }
        finally
        {
            KeyValueRequestPool.Return(request);
        }
    }

    /// <summary>
    /// Starts a new transaction with the specified options.
    /// </summary>
    /// <param name="options">The options for configuring the transaction.</param>
    /// <returns>Returns an <c>HLCTimestamp</c> representing the timestamp of the started transaction.</returns>
    public Task<(KeyValueResponseType, TransactionHandle)> StartTransaction(KeyValueTransactionOptions options)
    {
        return txCoordinator.StartTransaction(options);
    }

    /// <summary>
    /// Reads the decision-durability policy recorded for an active interactive session, or null when no
    /// active session with that id exists. Reflects exactly what Begin captured from the caller's options.
    /// </summary>
    internal DecisionDurability? GetRecordedDecisionDurability(HLCTimestamp transactionId)
    {
        return txCoordinator.GetRecordedDecisionDurability(transactionId);
    }

    /// <summary>
    /// Reads the clamped session timeout recorded for an active interactive session, or null when no active
    /// session with that id exists. Reflects the value after the MaxTransactionTimeout clamp applied in Begin.
    /// </summary>
    internal int? GetRecordedSessionTimeout(HLCTimestamp transactionId)
    {
        return txCoordinator.GetRecordedSessionTimeout(transactionId);
    }

    /// <summary>
    /// Commits the transaction identified by <paramref name="handle"/>.
    /// </summary>
    /// <param name="handle">The handle returned by <see cref="StartTransaction"/>.</param>
    /// <returns>A task that represents the asynchronous operation containing the commit result.</returns>
    public Task<(KeyValueResponseType, string?)> CommitTransaction(TransactionHandle handle)
    {
        return txCoordinator.CommitTransaction(handle);
    }

    /// <summary>
    /// Rolls back the transaction identified by <paramref name="handle"/>.
    /// </summary>
    /// <param name="handle">The handle returned by <see cref="StartTransaction"/>.</param>
    /// <returns></returns>
    public Task<KeyValueResponseType> RollbackTransaction(TransactionHandle handle)
    {
        return txCoordinator.RollbackTransaction(handle);
    }

    /// <summary>
    /// Renews the range locks of every live interactive session so they outlive their original acquire TTL
    /// without a client heartbeat. Driven periodically by the transaction reaper; exposed directly so a caller
    /// (or a test) can trigger the sweep deterministically.
    /// </summary>
    internal Task RenewSessionRangeLocks()
    {
        return txCoordinator.RenewSessionRangeLocks();
    }

    /// <summary>
    /// Reclaims interactive sessions abandoned without commit or rollback, releasing their held locks and read
    /// snapshots. Driven periodically by the transaction reaper; exposed directly so a caller (or a test) can
    /// trigger the sweep deterministically.
    /// </summary>
    internal Task ReapAbandonedSessions()
    {
        return txCoordinator.ReapAbandonedSessions();
    }

    internal async Task RunCollectOnAllInstancesAsync()
    {
        KeyValueRequest collect = new(KeyValueRequestType.Collect);
        List<Task<KeyValueResponse?>> tasks = new(ephemeralInstances.Count + persistentInstances.Count);

        foreach (IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actor in ephemeralInstances)
            tasks.Add(actor.Ask(collect)!);

        foreach (IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actor in persistentInstances)
            tasks.Add(actor.Ask(collect)!);

        await Task.WhenAll(tasks);
    }

    /// <summary>
    /// Fans out a <c>GetSafeTimestamp</c> query to every key-value actor shard and returns
    /// the minimum prepared <c>CommitTimestamp</c> across all live write intents in the cluster.
    /// Returns <see cref="HLCTimestamp.Zero"/> when no shard has an in-flight prepared transaction.
    /// </summary>
    internal async Task<HLCTimestamp> GetSafeTimestampAsync()
    {
        KeyValueRequest request = new(KeyValueRequestType.GetSafeTimestamp);
        List<Task<KeyValueResponse?>> tasks = new(ephemeralInstances.Count + persistentInstances.Count);

        foreach (IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actor in ephemeralInstances)
            tasks.Add(actor.Ask(request)!);

        foreach (IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actor in persistentInstances)
            tasks.Add(actor.Ask(request)!);

        KeyValueResponse?[] results = await Task.WhenAll(tasks);

        HLCTimestamp min = HLCTimestamp.Zero;
        foreach (KeyValueResponse? r in results)
        {
            if (r is null || r.Ticket == HLCTimestamp.Zero)
                continue;
            if (min == HLCTimestamp.Zero || r.Ticket.CompareTo(min) < 0)
                min = r.Ticket;
        }

        return min;
    }

    /// <summary>Observable async drain of the direct-write aggregator: rejects new writes, releases queued
    /// ones retryably, and awaits in-flight batch settlement. Must run while the actor system and Raft are
    /// still alive (before their disposal), so in-flight batches can report their outcome.</summary>
    public Task DrainWritesAsync(TimeSpan timeout) => writeAggregator.StopAsync(timeout);

    public void Dispose()
    {
        // Reject new writes and release any queued-but-not-dispatched ones retryably before tearing down.
        writeAggregator.Stop();
        rangeMapStore.Dispose();
        snapshotFloorStore.Dispose();
        rangeSplitTrigger?.Dispose();
    }
}
