
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
    
    private readonly IActorRef<BalancingActor<KeyValueProposalActor, KeyValueProposalRequest>, KeyValueProposalRequest> proposalRouter;

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

    private readonly RangeQuiesceStore rangeQuiesceStore = new();

    private readonly KvStateMachineTransfer kvStateMachineTransfer;

    private readonly MetaSystemStateTransfer metaSystemStateTransfer;

    private readonly KeySpaceRegistry keySpaceRegistry = new();

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
        SnapshotFloorStore? externalFloorStore = null
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

        // Sync the per-node key-space registry with any descriptors loaded from the durable snapshot.
        // RangeMapStore.LoadFromDisk (called above) restores the range map but does not touch the
        // registry — they are separate objects. Without this call, a node that restarts with a
        // non-empty snapshot treats all key spaces as hash-routed (the default) until the first
        // live RegisterKeyRangeAsync call, causing routing mismatches in the 2PC prepare path.
        SyncKeySpaceRegistryFromRangeMap();

        proposalRouter = GetProposalRouter(configuration);
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

        restorer = new(backgroundWriter, raft, logger);
        replicator = new(backgroundWriter, persistentKeyValuesRouter, raft, writeFrequencyRegistry, keySpaceRegistry, logger);
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

    private IActorRef<BalancingActor<KeyValueProposalActor, KeyValueProposalRequest>, KeyValueProposalRequest> GetProposalRouter(
        KahunaConfiguration configuration
    )
    {
        List<IActorRef<KeyValueProposalActor, KeyValueProposalRequest>> proposalInstances = new(configuration.LocksWorkers);

        for (int i = 0; i < configuration.KeyValueWorkers; i++)
            proposalInstances.Add(actorSystem.Spawn<KeyValueProposalActor, KeyValueProposalRequest>(
                "proposal-keyvalue-" + i,
                raft,
                persistenceBackend,
                configuration,
                keySpaceRegistry,
                rangeMapStore,
                logger
            ));
        
        return actorSystem.Spawn<BalancingActor<KeyValueProposalActor, KeyValueProposalRequest>, KeyValueProposalRequest>(null, proposalInstances);
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
                proposalRouter,
                persistenceBackend,
                raft,
                keySpaceRegistry,
                rangeMapStore,
                configuration,
                logger,
                snapshotFloorStore
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
                proposalRouter,
                persistenceBackend,
                raft,
                keySpaceRegistry,
                rangeMapStore,
                configuration,
                logger,
                snapshotFloorStore
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
        if (transactionId.IsNull() || operationId.IsEmpty || string.IsNullOrEmpty(coordinatorKey))
            return locator.LocateAndTrySetKeyValue(transactionId, key, value, compareValue, compareRevision, flags, expiresMs, durability, cancellationToken, routedGeneration);

        byte[] digest = OperationDigest.ForSet(key, value, compareValue, compareRevision, flags, expiresMs, durability);

        return RegisterAndTrySetKeyValue(transactionId, coordinatorKey, operationId, key, value, compareValue, compareRevision, flags, expiresMs, durability, digest, cancellationToken, routedGeneration);
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
        KeyValueDurability durability, byte[] digest, CancellationToken cancellationToken, long routedGeneration)
    {
        (OperationRegistrationOutcome outcome, KeyValueResponseType cachedType, long cachedRevision, HLCTimestamp cachedTimestamp) =
            await LocateAndBeginOperation(coordinatorKey, transactionId, operationId, OperationKind.Set, digest, cancellationToken);

        switch (outcome)
        {
            case OperationRegistrationOutcome.AlreadyCompleted:
                return (cachedType, cachedRevision, cachedTimestamp);
            case OperationRegistrationOutcome.AlreadyPending:
            case OperationRegistrationOutcome.RejectedCapacity:
                return (KeyValueResponseType.MustRetry, 0, HLCTimestamp.Zero);
            case OperationRegistrationOutcome.RejectedSessionClosed:
                return (KeyValueResponseType.Aborted, 0, HLCTimestamp.Zero);
            case OperationRegistrationOutcome.RejectedDuplicate:
                return (KeyValueResponseType.Errored, 0, HLCTimestamp.Zero);
        }

        (KeyValueResponseType type, long revision, HLCTimestamp lastModified) =
            await locator.LocateAndTrySetKeyValue(transactionId, key, value, compareValue, compareRevision, flags, expiresMs, durability, cancellationToken, routedGeneration);

        // Only a confirmed set records a modified key + its implicit point lock into the working set.
        bool applied = type == KeyValueResponseType.Set;

        await LocateAndCompleteOperation(
            coordinatorKey, transactionId, operationId,
            new OperationCompletionPayload
            {
                ModifiedKey = applied ? key : null,
                AcquiredPointLock = applied ? key : null,
                Durability = durability,
                CachedType = type,
                CachedRevision = revision,
                CachedTimestamp = lastModified
            },
            cancellationToken);

        return (type, revision, lastModified);
    }

    /// <summary>
    /// Attempts to locate and set multiple key-value pairs in the system.
    /// </summary>
    /// <param name="setManyItems">A collection of key-value set requests to be processed.</param>
    /// <param name="cancellationToken">Token to signal cancellation of the operation.</param>
    /// <returns>A task that represents the asynchronous operation, containing a list of responses for the key-value set requests.</returns>
    public Task<List<KahunaSetKeyValueResponseItem>> LocateAndTrySetManyKeyValue(
        List<KahunaSetKeyValueRequestItem> setManyItems, 
        CancellationToken cancellationToken
    )
    {
        return locator.LocateAndTrySetManyKeyValue(setManyItems, cancellationToken);
    }

    public Task<List<KahunaDeleteKeyValueResponseItem>> LocateAndTryDeleteManyKeyValue(
        List<KahunaDeleteKeyValueRequestItem> deleteManyItems,
        CancellationToken cancellationToken
    )
    {
        return locator.LocateAndTryDeleteManyKeyValue(deleteManyItems, cancellationToken);
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
        if (transactionId.IsNull() || operationId.IsEmpty || string.IsNullOrEmpty(coordinatorKey))
            return locator.LocateAndTryGetValue(transactionId, key, revision, readTimestamp, durability, cancellationToken);

        return RegisterAndTryReadValue(OperationKind.Get, transactionId, coordinatorKey, operationId, key, revision, readTimestamp, durability, cancellationToken);
    }

    /// <summary>
    /// Register-remote wrapper for a transaction-scoped read: registers the read for finalize fencing
    /// and records its <c>{exists, revision}</c> observation into the coordinator-owned read set. Reads
    /// are idempotent, so a repeat under the same operation id simply re-reads (latest / read-your-writes)
    /// without re-recording the original observation.
    /// </summary>
    private async Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> RegisterAndTryReadValue(
        OperationKind kind, HLCTimestamp transactionId, string coordinatorKey, TransactionOperationId operationId, string key,
        long revision, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken)
    {
        byte[] digest = OperationDigest.ForRead(kind, key, revision, durability);

        (OperationRegistrationOutcome outcome, _, _, _) =
            await LocateAndBeginOperation(coordinatorKey, transactionId, operationId, kind, digest, cancellationToken);

        switch (outcome)
        {
            case OperationRegistrationOutcome.AlreadyPending:
            case OperationRegistrationOutcome.RejectedCapacity:
                return (KeyValueResponseType.MustRetry, null);
            case OperationRegistrationOutcome.RejectedSessionClosed:
                return (KeyValueResponseType.Aborted, null);
            case OperationRegistrationOutcome.RejectedDuplicate:
                return (KeyValueResponseType.Errored, null);
        }

        (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = kind == OperationKind.Exists
            ? await locator.LocateAndTryExistsValue(transactionId, key, revision, readTimestamp, durability, cancellationToken)
            : await locator.LocateAndTryGetValue(transactionId, key, revision, readTimestamp, durability, cancellationToken);

        // A re-read under an already-completed id keeps the first recorded observation.
        if (outcome != OperationRegistrationOutcome.New)
            return (type, entry);

        bool exists = entry is not null && type is KeyValueResponseType.Get or KeyValueResponseType.Exists;

        await LocateAndCompleteOperation(
            coordinatorKey, transactionId, operationId,
            new OperationCompletionPayload
            {
                Read = new KeyValueTransactionReadKey { Key = key, Durability = durability, Exists = exists, Revision = exists ? entry!.Revision : -1 },
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
        if (transactionId.IsNull() || operationId.IsEmpty || string.IsNullOrEmpty(coordinatorKey))
            return locator.LocateAndTryExistsValue(transactionId, key, revision, readTimestamp, durability, cancellationToken);

        return RegisterAndTryReadValue(OperationKind.Exists, transactionId, coordinatorKey, operationId, key, revision, readTimestamp, durability, cancellationToken);
    }

    public async Task<List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)>> LocateAndTryExistsManyValues(
        HLCTimestamp transactionId,
        HLCTimestamp readTimestamp,
        List<(string key, long revision, KeyValueDurability durability)> keys,
        CancellationToken cancellationToken
    )
    {
        return await locator.LocateAndTryExistsManyValues(transactionId, readTimestamp, keys, cancellationToken);
    }

    public async Task<List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)>> LocateAndTryGetManyValues(
        HLCTimestamp transactionId,
        HLCTimestamp readTimestamp,
        List<(string key, long revision, KeyValueDurability durability)> keys,
        CancellationToken cancellationToken
    )
    {
        return await locator.LocateAndTryGetManyValues(transactionId, readTimestamp, keys, cancellationToken);
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
        if (transactionId.IsNull() || operationId.IsEmpty || string.IsNullOrEmpty(coordinatorKey))
            return locator.LocateAndTryDeleteKeyValue(transactionId, key, durability, cancellationToken);

        return RegisterAndTryDeleteKeyValue(transactionId, coordinatorKey, operationId, key, durability, OperationDigest.ForDelete(key, durability), cancellationToken);
    }

    private async Task<(KeyValueResponseType, long, HLCTimestamp)> RegisterAndTryDeleteKeyValue(
        HLCTimestamp transactionId, string coordinatorKey, TransactionOperationId operationId, string key,
        KeyValueDurability durability, byte[] digest, CancellationToken cancellationToken)
    {
        (OperationRegistrationOutcome outcome, KeyValueResponseType cachedType, long cachedRevision, HLCTimestamp cachedTimestamp) =
            await LocateAndBeginOperation(coordinatorKey, transactionId, operationId, OperationKind.Delete, digest, cancellationToken);

        switch (outcome)
        {
            case OperationRegistrationOutcome.AlreadyCompleted:
                return (cachedType, cachedRevision, cachedTimestamp);
            case OperationRegistrationOutcome.AlreadyPending:
            case OperationRegistrationOutcome.RejectedCapacity:
                return (KeyValueResponseType.MustRetry, 0, HLCTimestamp.Zero);
            case OperationRegistrationOutcome.RejectedSessionClosed:
                return (KeyValueResponseType.Aborted, 0, HLCTimestamp.Zero);
            case OperationRegistrationOutcome.RejectedDuplicate:
                return (KeyValueResponseType.Errored, 0, HLCTimestamp.Zero);
        }

        (KeyValueResponseType type, long revision, HLCTimestamp lastModified) =
            await locator.LocateAndTryDeleteKeyValue(transactionId, key, durability, cancellationToken);

        bool applied = type == KeyValueResponseType.Deleted;

        await LocateAndCompleteOperation(
            coordinatorKey, transactionId, operationId,
            new OperationCompletionPayload
            {
                ModifiedKey = applied ? key : null,
                AcquiredPointLock = applied ? key : null,
                Durability = durability,
                CachedType = type,
                CachedRevision = revision,
                CachedTimestamp = lastModified
            },
            cancellationToken);

        return (type, revision, lastModified);
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
        if (transactionId.IsNull() || operationId.IsEmpty || string.IsNullOrEmpty(coordinatorKey))
            return locator.LocateAndTryExtendKeyValue(transactionId, key, expiresMs, durability, cancellationToken);

        return RegisterAndTryExtendKeyValue(transactionId, coordinatorKey, operationId, key, expiresMs, durability, OperationDigest.ForExtend(key, expiresMs, durability), cancellationToken);
    }

    private async Task<(KeyValueResponseType, long, HLCTimestamp)> RegisterAndTryExtendKeyValue(
        HLCTimestamp transactionId, string coordinatorKey, TransactionOperationId operationId, string key,
        int expiresMs, KeyValueDurability durability, byte[] digest, CancellationToken cancellationToken)
    {
        (OperationRegistrationOutcome outcome, KeyValueResponseType cachedType, long cachedRevision, HLCTimestamp cachedTimestamp) =
            await LocateAndBeginOperation(coordinatorKey, transactionId, operationId, OperationKind.Extend, digest, cancellationToken);

        switch (outcome)
        {
            case OperationRegistrationOutcome.AlreadyCompleted:
                return (cachedType, cachedRevision, cachedTimestamp);
            case OperationRegistrationOutcome.AlreadyPending:
            case OperationRegistrationOutcome.RejectedCapacity:
                return (KeyValueResponseType.MustRetry, 0, HLCTimestamp.Zero);
            case OperationRegistrationOutcome.RejectedSessionClosed:
                return (KeyValueResponseType.Aborted, 0, HLCTimestamp.Zero);
            case OperationRegistrationOutcome.RejectedDuplicate:
                return (KeyValueResponseType.Errored, 0, HLCTimestamp.Zero);
        }

        (KeyValueResponseType type, long revision, HLCTimestamp lastModified) =
            await locator.LocateAndTryExtendKeyValue(transactionId, key, expiresMs, durability, cancellationToken);

        bool applied = type == KeyValueResponseType.Extended;

        await LocateAndCompleteOperation(
            coordinatorKey, transactionId, operationId,
            new OperationCompletionPayload
            {
                ModifiedKey = applied ? key : null,
                AcquiredPointLock = applied ? key : null,
                Durability = durability,
                CachedType = type,
                CachedRevision = revision,
                CachedTimestamp = lastModified
            },
            cancellationToken);

        return (type, revision, lastModified);
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
        if (transactionId.IsNull() || operationId.IsEmpty || string.IsNullOrEmpty(coordinatorKey))
            return locator.LocateAndTryAcquireExclusiveLock(transactionId, key, expiresMs, durability, cancelationToken);

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
        (OperationRegistrationOutcome outcome, _, _, _) =
            await LocateAndBeginOperation(coordinatorKey, transactionId, operationId, OperationKind.PointLock, OperationDigest.ForPointLockAcquire(key, expiresMs, durability), cancellationToken);

        switch (outcome)
        {
            case OperationRegistrationOutcome.AlreadyCompleted:
                return (KeyValueResponseType.Locked, key, durability, transactionId);
            case OperationRegistrationOutcome.AlreadyPending:
            case OperationRegistrationOutcome.RejectedCapacity:
                return (KeyValueResponseType.MustRetry, key, durability, HLCTimestamp.Zero);
            case OperationRegistrationOutcome.RejectedSessionClosed:
                return (KeyValueResponseType.Aborted, key, durability, HLCTimestamp.Zero);
            case OperationRegistrationOutcome.RejectedDuplicate:
                return (KeyValueResponseType.Errored, key, durability, HLCTimestamp.Zero);
        }

        (KeyValueResponseType type, string resultKey, KeyValueDurability resultDurability, HLCTimestamp holder) =
            await locator.LocateAndTryAcquireExclusiveLock(transactionId, key, expiresMs, durability, cancellationToken);

        bool acquired = type == KeyValueResponseType.Locked;

        await LocateAndCompleteOperation(
            coordinatorKey, transactionId, operationId,
            new OperationCompletionPayload
            {
                AcquiredPointLock = acquired ? key : null,
                Durability = durability,
                CachedType = type
            },
            cancellationToken);

        return (type, resultKey, resultDurability, holder);
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
        if (transactionId.IsNull() || operationId.IsEmpty || string.IsNullOrEmpty(coordinatorKey))
            return locator.LocateAndTryAcquireExclusivePrefixLock(transactionId, prefixKey, expiresMs, durability, cancellationToken);

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
        (OperationRegistrationOutcome outcome, _, _, _) =
            await LocateAndBeginOperation(coordinatorKey, transactionId, operationId, OperationKind.PrefixLock, OperationDigest.ForPrefixLockAcquire(prefixKey, expiresMs, durability), cancellationToken);

        switch (outcome)
        {
            case OperationRegistrationOutcome.AlreadyCompleted:
                return KeyValueResponseType.Locked;
            case OperationRegistrationOutcome.AlreadyPending:
            case OperationRegistrationOutcome.RejectedCapacity:
                return KeyValueResponseType.MustRetry;
            case OperationRegistrationOutcome.RejectedSessionClosed:
                return KeyValueResponseType.Aborted;
            case OperationRegistrationOutcome.RejectedDuplicate:
                return KeyValueResponseType.Errored;
        }

        KeyValueResponseType type =
            await locator.LocateAndTryAcquireExclusivePrefixLock(transactionId, prefixKey, expiresMs, durability, cancellationToken);

        bool acquired = type == KeyValueResponseType.Locked;

        await LocateAndCompleteOperation(
            coordinatorKey, transactionId, operationId,
            new OperationCompletionPayload
            {
                AcquiredPrefixLock = acquired ? prefixKey : null,
                Durability = durability,
                CachedType = type
            },
            cancellationToken);

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
        CancellationToken cancelationToken
    )
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
    public Task<(KeyValueResponseType, string)> LocateAndTryReleaseExclusiveLock(HLCTimestamp transactionId, string key, KeyValueDurability durability, CancellationToken cancelationToken, string coordinatorKey = "", TransactionOperationId operationId = default)
    {
        if (transactionId.IsNull() || operationId.IsEmpty || string.IsNullOrEmpty(coordinatorKey))
            return locator.LocateAndTryReleaseExclusiveLock(transactionId, key, durability, cancelationToken);

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
        (OperationRegistrationOutcome outcome, _, _, _) =
            await LocateAndBeginOperation(coordinatorKey, transactionId, operationId, OperationKind.PointLock, OperationDigest.ForPointLockRelease(key, durability), cancellationToken);

        switch (outcome)
        {
            case OperationRegistrationOutcome.AlreadyCompleted:
                return (KeyValueResponseType.Unlocked, key);
            case OperationRegistrationOutcome.AlreadyPending:
            case OperationRegistrationOutcome.RejectedCapacity:
                return (KeyValueResponseType.MustRetry, key);
            case OperationRegistrationOutcome.RejectedSessionClosed:
                return (KeyValueResponseType.Aborted, key);
            case OperationRegistrationOutcome.RejectedDuplicate:
                return (KeyValueResponseType.Errored, key);
        }

        (KeyValueResponseType type, string resultKey) =
            await locator.LocateAndTryReleaseExclusiveLock(transactionId, key, durability, cancellationToken);

        bool released = type == KeyValueResponseType.Unlocked;

        await LocateAndCompleteOperation(
            coordinatorKey, transactionId, operationId,
            new OperationCompletionPayload
            {
                ReleasedPointLock = released ? key : null,
                Durability = durability,
                CachedType = type
            },
            cancellationToken);

        return (type, resultKey);
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
        if (transactionId.IsNull() || operationId.IsEmpty || string.IsNullOrEmpty(coordinatorKey))
            return locator.LocateAndTryReleaseExclusivePrefixLock(transactionId, prefixKey, durability, cancellationToken);

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
        (OperationRegistrationOutcome outcome, _, _, _) =
            await LocateAndBeginOperation(coordinatorKey, transactionId, operationId, OperationKind.PrefixLock, OperationDigest.ForPrefixLockRelease(prefixKey, durability), cancellationToken);

        switch (outcome)
        {
            case OperationRegistrationOutcome.AlreadyCompleted:
                return KeyValueResponseType.Unlocked;
            case OperationRegistrationOutcome.AlreadyPending:
            case OperationRegistrationOutcome.RejectedCapacity:
                return KeyValueResponseType.MustRetry;
            case OperationRegistrationOutcome.RejectedSessionClosed:
                return KeyValueResponseType.Aborted;
            case OperationRegistrationOutcome.RejectedDuplicate:
                return KeyValueResponseType.Errored;
        }

        KeyValueResponseType type =
            await locator.LocateAndTryReleaseExclusivePrefixLock(transactionId, prefixKey, durability, cancellationToken);

        bool released = type == KeyValueResponseType.Unlocked;

        await LocateAndCompleteOperation(
            coordinatorKey, transactionId, operationId,
            new OperationCompletionPayload
            {
                ReleasedPrefixLock = released ? prefixKey : null,
                Durability = durability,
                CachedType = type
            },
            cancellationToken);

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
        if (transactionId.IsNull() || operationId.IsEmpty || string.IsNullOrEmpty(coordinatorKey))
            return locator.LocateAndTryAcquireRangeLock(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, expiresMs, durability, mode, cancellationToken);

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
        (OperationRegistrationOutcome outcome, _, _, _) =
            await LocateAndBeginOperation(coordinatorKey, transactionId, operationId, OperationKind.RangeLock,
                OperationDigest.ForRangeLockAcquire(prefix, startKey, startInclusive, endKey, endInclusive, mode, expiresMs, durability), cancellationToken);

        switch (outcome)
        {
            case OperationRegistrationOutcome.AlreadyCompleted:
                return (KeyValueResponseType.Locked, transactionId);
            case OperationRegistrationOutcome.AlreadyPending:
            case OperationRegistrationOutcome.RejectedCapacity:
                return (KeyValueResponseType.MustRetry, HLCTimestamp.Zero);
            case OperationRegistrationOutcome.RejectedSessionClosed:
                return (KeyValueResponseType.Aborted, HLCTimestamp.Zero);
            case OperationRegistrationOutcome.RejectedDuplicate:
                return (KeyValueResponseType.Errored, HLCTimestamp.Zero);
        }

        (KeyValueResponseType type, HLCTimestamp holder) =
            await locator.LocateAndTryAcquireRangeLock(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, expiresMs, durability, mode, afterSnapshot, cancellationToken);

        bool acquired = type == KeyValueResponseType.Locked;
        RangeLockKey range = new(prefix, startKey, startInclusive, endKey, endInclusive, durability);

        await LocateAndCompleteOperation(
            coordinatorKey, transactionId, operationId,
            new OperationCompletionPayload
            {
                AcquiredRangeLock = acquired ? (range, mode) : null,
                Durability = durability,
                CachedType = type
            },
            cancellationToken);

        return (type, holder);
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
        if (transactionId.IsNull() || operationId.IsEmpty || string.IsNullOrEmpty(coordinatorKey))
            return locator.LocateAndTryReleaseExclusiveRangeLock(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, durability, cancellationToken);

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
        (OperationRegistrationOutcome outcome, _, _, _) =
            await LocateAndBeginOperation(coordinatorKey, transactionId, operationId, OperationKind.RangeLock,
                OperationDigest.ForRangeLockRelease(prefix, startKey, startInclusive, endKey, endInclusive, durability), cancellationToken);

        switch (outcome)
        {
            case OperationRegistrationOutcome.AlreadyCompleted:
                return KeyValueResponseType.Unlocked;
            case OperationRegistrationOutcome.AlreadyPending:
            case OperationRegistrationOutcome.RejectedCapacity:
                return KeyValueResponseType.MustRetry;
            case OperationRegistrationOutcome.RejectedSessionClosed:
                return KeyValueResponseType.Aborted;
            case OperationRegistrationOutcome.RejectedDuplicate:
                return KeyValueResponseType.Errored;
        }

        KeyValueResponseType type =
            await locator.LocateAndTryReleaseExclusiveRangeLock(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, durability, cancellationToken);

        bool released = type == KeyValueResponseType.Unlocked;
        RangeLockKey range = new(prefix, startKey, startInclusive, endKey, endInclusive, durability);

        await LocateAndCompleteOperation(
            coordinatorKey, transactionId, operationId,
            new OperationCompletionPayload
            {
                ReleasedRangeLock = released ? range : null,
                Durability = durability,
                CachedType = type
            },
            cancellationToken);

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
        long routedGeneration = 0
    )
    {
        return locator.LocateAndTryPrepareMutations(transactionId, commitId, key, durability, cancelationToken, routedGeneration);
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
    /// Locates the leader node for the given prefix and executes the GetByBucket request.
    /// </summary>
    /// <param name="prefixedKey"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public Task<KeyValueGetByBucketResult> LocateAndGetByBucket(HLCTimestamp transactionId, string prefixedKey, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken, string coordinatorKey = "", TransactionOperationId operationId = default)
    {
        if (transactionId.IsNull() || operationId.IsEmpty || string.IsNullOrEmpty(coordinatorKey))
            return locator.LocateAndGetByBucket(transactionId, prefixedKey, readTimestamp, durability, cancellationToken);

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
        (OperationRegistrationOutcome outcome, _, _, _) =
            await LocateAndBeginOperation(coordinatorKey, transactionId, operationId, OperationKind.Scan,
                OperationDigest.ForScan(prefixedKey, readTimestamp.L, readTimestamp.C, durability), cancellationToken);

        switch (outcome)
        {
            case OperationRegistrationOutcome.AlreadyCompleted:
            case OperationRegistrationOutcome.AlreadyPending:
            case OperationRegistrationOutcome.RejectedCapacity:
                // A pending or already-completed scan re-runs the read without re-recording observations;
                // the read-set already holds the first observation and reads are idempotent.
                if (outcome == OperationRegistrationOutcome.AlreadyCompleted)
                    return await locator.LocateAndGetByBucket(transactionId, prefixedKey, readTimestamp, durability, cancellationToken);
                return new KeyValueGetByBucketResult(KeyValueResponseType.MustRetry, []);
            case OperationRegistrationOutcome.RejectedSessionClosed:
                return new KeyValueGetByBucketResult(KeyValueResponseType.Aborted, []);
            case OperationRegistrationOutcome.RejectedDuplicate:
                return new KeyValueGetByBucketResult(KeyValueResponseType.Errored, []);
        }

        KeyValueGetByBucketResult result =
            await locator.LocateAndGetByBucket(transactionId, prefixedKey, readTimestamp, durability, cancellationToken);

        List<KeyValueTransactionReadKey>? observations = null;
        if (result.Type == KeyValueResponseType.Get && result.Items.Count > 0)
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

    public Task<KeyValueGetByRangeResult> LocateAndGetByRange(HLCTimestamp transactionId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive, int limit, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken)
    {
        return locator.LocateAndGetByRange(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, limit, readTimestamp, durability, cancellationToken);
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
        [EnumeratorCancellation] CancellationToken ct)
    {
        string? cursorKey       = startKey;
        bool    cursorInclusive = startInclusive;
        // Seed from caller's T when supplied; Zero means "capture on first successful page".
        HLCTimestamp snapshotTs = readTimestamp;
        int backoffMs           = 1;

        while (true)
        {
            ct.ThrowIfCancellationRequested();

            KeyValueGetByRangeResult page = await locator.LocateAndGetByRange(
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
    public Task<KeyValueResponseType> LocateAndCommitTransaction(
        TransactionHandle handle,
        List<KeyValueTransactionModifiedKey> acquiredLocks,
        List<KeyValueTransactionModifiedKey> modifiedKeys,
        List<KeyValueTransactionReadKey> readKeys,
        CancellationToken cancellationToken
    )
    {
        return locator.LocateAndCommitTransaction(handle, acquiredLocks, modifiedKeys, readKeys, cancellationToken);
    }

    /// <summary>Routes an operation registration to the coordinator node identified by <paramref name="coordinatorKey"/>.</summary>
    public Task<(OperationRegistrationOutcome outcome, KeyValueResponseType cachedType, long cachedRevision, HLCTimestamp cachedTimestamp)> LocateAndBeginOperation(string coordinatorKey, HLCTimestamp transactionId, TransactionOperationId operationId, OperationKind kind, byte[]? payloadDigest, CancellationToken cancellationToken)
    {
        return locator.LocateAndBeginOperation(coordinatorKey, transactionId, operationId, kind, payloadDigest, cancellationToken);
    }

    /// <summary>Routes an operation completion to the coordinator node identified by <paramref name="coordinatorKey"/>.</summary>
    public Task LocateAndCompleteOperation(string coordinatorKey, HLCTimestamp transactionId, TransactionOperationId operationId, OperationCompletionPayload payload, CancellationToken cancellationToken)
    {
        return locator.LocateAndCompleteOperation(coordinatorKey, transactionId, operationId, payload, cancellationToken);
    }

    /// <summary>Node-local registration: the session lives here (this node is the coordinator for the key).</summary>
    public (OperationRegistrationOutcome outcome, KeyValueResponseType cachedType, long cachedRevision, HLCTimestamp cachedTimestamp) BeginOperation(HLCTimestamp transactionId, TransactionOperationId operationId, OperationKind kind, byte[]? payloadDigest)
    {
        OperationRegistrationResult result = txCoordinator.BeginOperation(transactionId, operationId, kind, payloadDigest);

        CachedOperationResponse cached = result.CachedResponse is CachedOperationResponse c
            ? c
            : new(KeyValueResponseType.Errored, 0, HLCTimestamp.Zero);

        return (result.Outcome, cached.Type, cached.Revision, cached.CommitTimestamp);
    }

    /// <summary>Node-local completion: folds the confirmed effect into the coordinator-owned working set.</summary>
    public void CompleteOperation(HLCTimestamp transactionId, TransactionOperationId operationId, OperationCompletionPayload payload)
    {
        // A transient outcome must never be cached as terminal — cancel the registration so a retry
        // with the same operation id re-registers as new and actually applies the mutation.
        if (payload.CachedType == KeyValueResponseType.MustRetry)
        {
            txCoordinator.CancelOperation(transactionId, operationId);
            return;
        }

        OperationEffect? effect = BuildEffect(payload);
        txCoordinator.CompleteOperation(transactionId, operationId, effect, new CachedOperationResponse(payload.CachedType, payload.CachedRevision, payload.CachedTimestamp));
    }

    /// <summary>Maps a completion payload into the coordinator-owned working-set effect, or null when it records nothing.</summary>
    private static OperationEffect? BuildEffect(OperationCompletionPayload payload)
    {
        KeyValueDurability durability = payload.Durability;

        bool hasEffect =
            !string.IsNullOrEmpty(payload.ModifiedKey) ||
            !string.IsNullOrEmpty(payload.AcquiredPointLock) || !string.IsNullOrEmpty(payload.ReleasedPointLock) ||
            !string.IsNullOrEmpty(payload.AcquiredPrefixLock) || !string.IsNullOrEmpty(payload.ReleasedPrefixLock) ||
            payload.AcquiredRangeLock is not null || payload.ReleasedRangeLock is not null ||
            payload.Read is not null ||
            (payload.ReadObservations is { Count: > 0 });

        if (!hasEffect)
            return null;

        return new()
        {
            ModifiedKey = string.IsNullOrEmpty(payload.ModifiedKey) ? null : (payload.ModifiedKey, durability),
            PointLock = string.IsNullOrEmpty(payload.AcquiredPointLock) ? null : (payload.AcquiredPointLock, durability),
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
    /// <param name="acquiredLocks">A list of keys that were locked during the transaction.</param>
    /// <param name="modifiedKeys">A list of keys that were modified during the transaction.</param>
    /// <param name="cancellationToken">A cancellation token to observe while waiting for the task to complete.</param>
    /// <returns>A task that represents the asynchronous operation, containing the result of the rollback operation as a <see cref="KeyValueResponseType"/>.</returns>
    public Task<KeyValueResponseType> LocateAndRollbackTransaction(
        TransactionHandle handle,
        List<KeyValueTransactionModifiedKey> acquiredLocks,
        List<KeyValueTransactionModifiedKey> modifiedKeys,
        CancellationToken cancellationToken
    )
    {
        return locator.LocateAndRollbackTransaction(handle, acquiredLocks, modifiedKeys, cancellationToken);
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
        Task<KahunaSetKeyValueResponseItem>[] tasks = new Task<KahunaSetKeyValueResponseItem>[items.Count];

        for (int i = 0; i < items.Count; i++)
        {
            KahunaSetKeyValueRequestItem item = items[i];
            tasks[i] = SetOneNodeKeyValue(item);
        }

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

                if (response is null || response.Type == KeyValueResponseType.WaitingForReplication)
                    return new() { Type = KeyValueResponseType.Errored };

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
        Task<KahunaDeleteKeyValueResponseItem>[] tasks = new Task<KahunaDeleteKeyValueResponseItem>[items.Count];

        for (int i = 0; i < items.Count; i++)
        {
            KahunaDeleteKeyValueRequestItem item = items[i];
            tasks[i] = DeleteOneNodeKeyValue(item);
        }

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
        long routedGeneration = 0
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
        List<(string key, KeyValueDurability durability)> keys
    )
    {
        Task<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)>[] tasks =
            new Task<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)>[keys.Count];

        for (int i = 0; i < keys.Count; i++)
        {
            (string key, KeyValueDurability durability) key = keys[i];
            tasks[i] = PrepareOneMutation(key);
        }

        return [.. await Task.WhenAll(tasks)];

        async Task<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)> PrepareOneMutation(
            (string key, KeyValueDurability durability) key)
        {
            KeyValueRequest request = KeyValueRequestPool.Rent(
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
                    return (KeyValueResponseType.Errored, HLCTimestamp.Zero, key.key, key.durability);

                return (response.Type, response.Ticket, key.key, key.durability);
            }
            finally
            {
                KeyValueRequestPool.Return(request);
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
        Task<(KeyValueResponseType type, string key, long proposalIndex, KeyValueDurability durability)>[] tasks =
            new Task<(KeyValueResponseType type, string key, long proposalIndex, KeyValueDurability durability)>[keys.Count];

        for (int i = 0; i < keys.Count; i++)
        {
            (string key, HLCTimestamp proposalTicketId, KeyValueDurability durability) key = keys[i];
            tasks[i] = CommitOneMutation(key);
        }

        return [.. await Task.WhenAll(tasks)];

        async Task<(KeyValueResponseType type, string key, long proposalIndex, KeyValueDurability durability)> CommitOneMutation(
            (string key, HLCTimestamp proposalTicketId, KeyValueDurability durability) key)
        {
            KeyValueRequest request = KeyValueRequestPool.Rent(
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
                    return (KeyValueResponseType.Errored, key.key, -1, key.durability);

                return (response.Type, key.key, response.Revision, key.durability);
            }
            finally
            {
                KeyValueRequestPool.Return(request);
            }
        }
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
        Task<(KeyValueResponseType type, string key, long proposalIndex, KeyValueDurability durability)>[] tasks =
            new Task<(KeyValueResponseType type, string key, long proposalIndex, KeyValueDurability durability)>[keys.Count];

        for (int i = 0; i < keys.Count; i++)
        {
            (string key, HLCTimestamp proposalTicketId, KeyValueDurability durability) key = keys[i];
            tasks[i] = RollbackOneMutation(key);
        }

        return [.. await Task.WhenAll(tasks)];

        async Task<(KeyValueResponseType type, string key, long proposalIndex, KeyValueDurability durability)> RollbackOneMutation(
            (string key, HLCTimestamp proposalTicketId, KeyValueDurability durability) key)
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
                key.durability,
                0,
                0,
                null
            );

            KeyValueResponse? response;

            if (key.durability == KeyValueDurability.Ephemeral)
                response = await AskKeyValueActor(ephemeralKeyValuesRouter, request);
            else
                response = await AskKeyValueActor(persistentKeyValuesRouter, request);

            if (response is null)
                return (KeyValueResponseType.Errored, key.key, -1, key.durability);

            return (response.Type, key.key, response.Revision, key.durability);
        }
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
    /// Commits the transaction identified by <paramref name="handle"/>.
    /// </summary>
    /// <param name="handle">The handle returned by <see cref="StartTransaction"/>.</param>
    /// <param name="acquiredLocks">List of acquired locks.</param>
    /// <param name="modifiedKeys">List of modified keys.</param>
    /// <param name="readKeys">List of keys read during the transaction.</param>
    /// <returns>A task that represents the asynchronous operation containing the commit result.</returns>
    public Task<KeyValueResponseType> CommitTransaction(
        TransactionHandle handle,
        List<KeyValueTransactionModifiedKey> acquiredLocks,
        List<KeyValueTransactionModifiedKey> modifiedKeys,
        List<KeyValueTransactionReadKey> readKeys
    )
    {
        return txCoordinator.CommitTransaction(handle, acquiredLocks, modifiedKeys, readKeys);
    }

    /// <summary>
    /// Rolls back the transaction identified by <paramref name="handle"/>.
    /// </summary>
    /// <param name="handle">The handle returned by <see cref="StartTransaction"/>.</param>
    /// <param name="acquiredLocks">List of acquired locks.</param>
    /// <param name="modifiedKeys">List of modified keys.</param>
    /// <returns></returns>
    public Task<KeyValueResponseType> RollbackTransaction(
        TransactionHandle handle,
        List<KeyValueTransactionModifiedKey> acquiredLocks,
        List<KeyValueTransactionModifiedKey> modifiedKeys
    )
    {
        return txCoordinator.RollbackTransaction(handle, acquiredLocks, modifiedKeys);
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

    public void Dispose()
    {
        rangeMapStore.Dispose();
        snapshotFloorStore.Dispose();
        rangeSplitTrigger?.Dispose();
    }
}
