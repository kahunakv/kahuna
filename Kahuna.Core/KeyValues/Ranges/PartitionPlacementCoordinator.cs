
using Nixie;
using Kommander;
using Kahuna.Server.KeyValues.Logging;
using Kahuna.Server.Locks;
using Kahuna.Server.Persistence;

namespace Kahuna.Server.KeyValues.Ranges;

/// <summary>
/// Reacts to the committed placement map on behalf of one node: it owns the
/// <see cref="PartitionPlacementView"/>, tears down per-partition background state when this node
/// stops being a replica of a range, and re-derives the startup purge on the first committed map
/// application.
///
/// <para>
/// Both subscriptions (the view's hosted-set transitions and Raft's map-application event) are made
/// and released by this instance, so unsubscription always targets the delegates that were
/// registered. With the replication factor off every range is hosted everywhere, no transition ever
/// fires, and all of this is inert.
/// </para>
/// </summary>
internal sealed class PartitionPlacementCoordinator : IDisposable
{
    private readonly IRaft raft;

    private readonly KeyValuesManager keyValues;

    private readonly LockManager locks;

    /// <summary>Kept so a lost partition's cached persisted durability floor can be evicted.</summary>
    private readonly KahunaDurabilityProvider durabilityProvider;

    private readonly IActorRef<BackgroundWriterActor, BackgroundWriteRequest> backgroundWriter;

    private readonly ILogger<IKahuna> logger;

    /// <summary>One-shot latch for the startup purge re-derivation (first committed map application).</summary>
    private int startupPurgeSweepStarted;

    /// <summary>
    /// Per-node projection of the committed partition map. Its hosted-set transitions drive the
    /// teardown of per-partition background state when this node stops being a replica of a range.
    /// </summary>
    internal PartitionPlacementView View { get; }

    /// <summary>
    /// Builds the placement view and starts watching the committed map. Subscribing during
    /// construction keeps the "created but not yet watching" window closed: the view seeds itself
    /// from the committed map in its own constructor, so a map applied between the two subscriptions
    /// is still reflected.
    /// </summary>
    public PartitionPlacementCoordinator(
        IRaft raft,
        KeyValuesManager keyValues,
        LockManager locks,
        KahunaDurabilityProvider durabilityProvider,
        IActorRef<BackgroundWriterActor, BackgroundWriteRequest> backgroundWriter,
        ILogger<IKahuna> logger)
    {
        this.raft = raft;
        this.keyValues = keyValues;
        this.locks = locks;
        this.durabilityProvider = durabilityProvider;
        this.backgroundWriter = backgroundWriter;
        this.logger = logger;

        // Watch the committed placement map: when this node stops being a replica of a partition,
        // its per-partition background state (durability floors, checkpoint tracking, enqueued-HLC
        // watermarks) must be dropped — retained floors become WAL retention leaks (or worse, stale
        // vouchers) if the range is ever hosted here again.
        View = new PartitionPlacementView(raft);
        View.HostedPartitionsChanged += OnHostedPartitionsChanged;

        // Startup purge re-derivation: a crash mid-purge leaves partial local data for a partition
        // the committed map does not host here. Rather than a durable purge-intent record, the
        // intent is re-derived from the map itself on its first application — "the map does not
        // list me as a replica of P, so P's local data must go" — which is idempotent and covers
        // both a crashed purge and a replica removed while this node was down. Inert under full
        // replication (every range is hosted here).
        raft.OnPartitionMapChanged += OnFirstPartitionMapApplied;
    }

    /// <summary>
    /// Reacts to hosted-set transitions of the committed placement map. Runs on the map-application
    /// thread, so it only evicts caches, signals the background writer and schedules the heavier
    /// purge work — the writer and the purge both re-validate each partition against the committed
    /// map on their own threads before dropping anything.
    /// </summary>
    private void OnHostedPartitionsChanged(IReadOnlySet<int> gained, IReadOnlySet<int> lost)
    {
        // Replica movement is the placement signal operators watch during a rebalance; surface
        // every transition, in both directions, as a metric and an Information log.
        if (gained.Count > 0)
        {
            PlacementMetrics.ReplicasGained.Add(gained.Count);
            if (logger.IsEnabled(Microsoft.Extensions.Logging.LogLevel.Information))
                logger.LogHostedPartitionsGained(gained.Count, string.Join(", ", gained.Order()));
        }

        if (lost.Count == 0)
            return;

        PlacementMetrics.ReplicasLost.Add(lost.Count);
        if (logger.IsEnabled(Microsoft.Extensions.Logging.LogLevel.Information))
            logger.LogHostedPartitionsLost(lost.Count, string.Join(", ", lost.Order()));

        foreach (int partitionId in lost)
            durabilityProvider.Forget(partitionId);

        backgroundWriter.Send(new(BackgroundWriteType.ForgetUnhostedPartitions));

        foreach (int partitionId in lost)
        {
            int lostPartitionId = partitionId;
            _ = Task.Run(() => PurgeUnhostedPartitionSafelyAsync(lostPartitionId));
        }
    }

    /// <summary>
    /// One-shot startup work fired on the first committed map application. Logs the placement
    /// banner — the mode the node is running in (effective replication factor, rebalancer state,
    /// hosted partition count) — then re-derives the startup purge: any partition the map lists
    /// with a replica set that excludes this node gets its local leftovers purged, repairing a
    /// crash mid-purge and a replica removed while this node was down.
    /// </summary>
    private void OnFirstPartitionMapApplied(IReadOnlyList<Kommander.System.RaftPartitionRange> appliedRanges)
    {
        if (Interlocked.Exchange(ref startupPurgeSweepStarted, 1) != 0)
            return;

        raft.OnPartitionMapChanged -= OnFirstPartitionMapApplied;

        LogPlacementBanner(appliedRanges);

        _ = Task.Run(async () =>
        {
            foreach (Kommander.System.RaftPartitionRange range in raft.GetPartitionMap())
            {
                if (IsPartitionCommittedAbsent(range.PartitionId))
                    await PurgeUnhostedPartitionSafelyAsync(range.PartitionId);
            }
        });
    }

    /// <summary>
    /// Logs, once at Information, the placement mode this node runs in: the effective replication
    /// factor, whether the placement rebalancer moves replicas automatically, and how many of the
    /// cluster's partitions are hosted locally — so an operator can tell full replication apart
    /// from per-partition placement without digging through configuration.
    /// </summary>
    private void LogPlacementBanner(IReadOnlyList<Kommander.System.RaftPartitionRange> appliedRanges)
    {
        int totalCount = 0;
        int hostedCount = 0;

        foreach (Kommander.System.RaftPartitionRange range in appliedRanges)
        {
            if (range.State == Kommander.System.RaftPartitionState.Removed)
                continue;

            totalCount++;
            if (raft.HostsPartition(range.PartitionId))
                hostedCount++;
        }

        int replicationFactor = raft.Configuration.ReplicationFactor;

        logger.LogPlacementStartupBanner(
            replicationFactor,
            replicationFactor > 0 ? "per-partition placement" : "full replication",
            raft.Configuration.EnablePlacementRebalancer ? "enabled" : "disabled",
            hostedCount,
            totalCount);
    }

    /// <summary>
    /// Whether the committed map lists <paramref name="partitionId"/> with a non-empty replica set
    /// that excludes this node — the only condition that authorizes purging its local data, for the
    /// same reason Kommander's WAL reclaim is safe: leaving a replica set happens only through the
    /// final committed replica removal. A legacy range (empty replica set), an unknown partition,
    /// or a range already merged away all answer false — never purge on anything but a committed
    /// absence.
    /// </summary>
    private bool IsPartitionCommittedAbsent(int partitionId)
    {
        if (!raft.IsInitialized)
            return false;

        string localEndpoint = raft.GetLocalEndpoint();

        foreach (Kommander.System.RaftPartitionRange range in raft.GetPartitionMap())
        {
            if (range.PartitionId != partitionId)
                continue;

            if (range.State == Kommander.System.RaftPartitionState.Removed || range.Replicas.Count == 0)
                return false;

            foreach (Kommander.System.RaftReplica replica in range.Replicas)
            {
                if (string.Equals(replica.Endpoint, localEndpoint, StringComparison.Ordinal))
                    return false;
            }

            return true;
        }

        return false;
    }

    /// <summary>
    /// Background purge of one un-hosted partition: durable state through the key-value manager
    /// (serialized against a concurrent seeding install, re-checking committed absence throughout),
    /// then the resident lock leases. A failed or aborted attempt is converged by the startup
    /// re-derivation — never retried in a loop here, because the common abort cause is the
    /// partition being re-gained, where retrying would be wrong.
    /// </summary>
    private async Task PurgeUnhostedPartitionSafelyAsync(int partitionId)
    {
        try
        {
            bool StillUnhosted() => !raft.HostsPartition(partitionId) && IsPartitionCommittedAbsent(partitionId);

            if (!StillUnhosted())
                return;

            if (!await keyValues.PurgeUnhostedPartitionDataAsync(partitionId, StillUnhosted, CancellationToken.None))
                return;

            await locks.EvictUnhostedPartitionLocksAsync(partitionId);
        }
        catch (Exception ex)
        {
            // Retention converges at the next startup re-derivation; a purge failure must never
            // take down the node.
            logger.LogWarning(ex, "Purge of un-hosted partition #{PartitionId} did not complete; it will be re-derived from the committed map at the next startup", partitionId);
        }
    }

    /// <summary>
    /// Stops watching the committed map. Both handlers are released here — the map-application
    /// subscription against this same instance, so the unsubscription actually matches the
    /// registration — before the view drops its own Raft subscription.
    /// </summary>
    public void Dispose()
    {
        View.Dispose();
        raft.OnPartitionMapChanged -= OnFirstPartitionMapApplied;
    }
}
