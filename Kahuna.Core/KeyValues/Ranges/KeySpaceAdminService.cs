using Nixie;
using Nixie.Routers;

using Kommander;
using Kommander.Time;

using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues.Logging;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Ranges;

/// <summary>
/// The key-space administration surface: flipping a key space to key-range routing and seeding its first
/// whole-space descriptor, tearing that registration back down, resolving a key to its range, recording
/// flush notifications, and the manual entry points that drive an auto-split or auto-merge pass.
///
/// Registration is deliberately self-sufficient — flipping the routing mode without a covering descriptor
/// leaves routing unable to place a key at all — so the seed and the mode flip belong together here rather
/// than at each call site.
/// </summary>
internal sealed class KeySpaceAdminService
{
    private readonly KeyValuesRuntime runtime;

    private readonly KeyValuesManager manager;

    private readonly RangeSplitter rangeSplitter;

    private readonly RangeSplitTrigger rangeSplitTrigger;

    private readonly RangeMerger rangeMerger;

    private readonly RangeMergeTrigger rangeMergeTrigger;

    internal KeySpaceAdminService(
        KeyValuesRuntime runtime,
        KeyValuesManager manager,
        RangeSplitter rangeSplitter,
        RangeSplitTrigger rangeSplitTrigger,
        RangeMerger rangeMerger,
        RangeMergeTrigger rangeMergeTrigger)
    {
        this.runtime = runtime;
        this.manager = manager;
        this.rangeSplitter = rangeSplitter;
        this.rangeSplitTrigger = rangeSplitTrigger;
        this.rangeMerger = rangeMerger;
        this.rangeMergeTrigger = rangeMergeTrigger;
    }

    // Aliases matching the field names the moved bodies use, so those bodies stay byte-for-byte as they were.
    private IRaft raft => runtime.Raft;

    private ILogger<IKahuna> logger => runtime.Logger;

    private KahunaConfiguration configuration => runtime.Configuration;

    private RangeMapStore rangeMapStore => runtime.RangeMapStore;

    private KeySpaceRegistry keySpaceRegistry => runtime.KeySpaceRegistry;

    private KeyWriteFrequencyRegistry writeFrequencyRegistry => runtime.WriteFrequencyRegistry;

    private KeyValueLocator locator => runtime.Locator;

    private Kahuna.Server.Communication.Internode.IInterNodeCommunication interNodeCommunication => runtime.InterNodeCommunication;

    private KeyValueActorRing persistentKeyValuesRouter => runtime.Routers.Persistent;

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
        if (!await raft.AmILeaderIfHosted(RangeMapStore.MetaPartitionId, cancellationToken).ConfigureAwait(false))
        {
            // A null target means the seed cannot be routed from here: the meta partition is not
            // materialized locally, or this call is already serving a forwarded request that has
            // spent its forward-hop budget. Surface the typed retryable condition — answering
            // "false" would be indistinguishable from "already seeded", which is a silent lie.
            string? leader = await raft.TryResolveLeader(RangeMapStore.MetaPartitionId, cancellationToken).ConfigureAwait(false);
            if (leader is null)
                throw new PartitionNotHostedException(RangeMapStore.MetaPartitionId);

            if (leader == raft.GetLocalEndpoint())
            {
                // We just became leader between the leadership check and the resolution — fall through to seed locally.
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
                        Transactions.DurableTransactionMetrics.AddKvRetryWait("EnsureKeyRangeSeededAsync_1005");
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

        // Transient rejection: a range of this space is mid-move and quiesced, so its descriptors
        // are about to be rewritten by the cutover. Deleting them now would race that rewrite; the
        // window is short, so the caller waits and retries.
        HLCTimestamp quiesceProbeTime = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        foreach (RangeDescriptor descriptor in rangeMapStore.Current.FindAll(keySpace))
            if (descriptor.IsQuiescedAt(quiesceProbeTime))
                return false;

        if (!await raft.AmILeaderIfHosted(RangeMapStore.MetaPartitionId, cancellationToken).ConfigureAwait(false))
        {
            // See RegisterKeyRangeAsync: a null target is a retryable routing condition, not a no-op.
            string? leader = await raft.TryResolveLeader(RangeMapStore.MetaPartitionId, cancellationToken).ConfigureAwait(false);
            if (leader is null)
                throw new PartitionNotHostedException(RangeMapStore.MetaPartitionId);

            if (leader != raft.GetLocalEndpoint())
            {
                // Forward to the meta-partition leader and wait for the removal to replicate here.
                bool forwarded = await interNodeCommunication.EnsureKeyRangeRemoved(leader, keySpace, cancellationToken).ConfigureAwait(false);
                long deadline = Environment.TickCount64 + 10_000;
                while (Environment.TickCount64 < deadline && rangeMapStore.Current.FindAll(keySpace).Count > 0)
                    Transactions.DurableTransactionMetrics.AddKvRetryWait("RemoveKeyRangeAsync_1074");
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
    /// an already-durable entry, never risks correctness. The request is pooled: ownership transfers
    /// with the send, and the receiving actor returns it after handling — do not touch it after this
    /// call.
    /// </summary>
    internal void NotifyFlushed(string key, long revision)
        => persistentKeyValuesRouter.Send(KeyValueRequestPool.RentFlushAck(key, revision));

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
        // Shares the long-lived trigger's gate so a threshold-override run still serializes its
        // partition-ID allocation against the live cadences and the manual split.
        var trigger = new RangeSplitTrigger(
            raft, rangeMapStore, rangeSplitter!, manager, writeFrequencyRegistry, cfg, logger,
            rangeSplitTrigger!.SplitGate);

        return trigger.TriggerAsync(ct);
    }

    /// <summary>
    /// Forces a split of the descriptor covering <paramref name="splitKey"/> at that exact key,
    /// bypassing any key-count threshold — the operator-driven split, and the seam tests drive.
    /// Handles the full <c>allocate → CreatePartitionAsync → SplitAsync</c> sequence internally, so
    /// callers do not need to manage partition IDs. Optionally invokes
    /// <paramref name="duringQuiesce"/> inside the quiesce window (after catch-up import, before
    /// cutover) so a test can race an operation into it.
    /// <para>
    /// Runs through <see cref="RangeSplitTrigger"/> rather than allocating here, so it shares the
    /// automatic cadences' serialization: two branches allocating a partition ID concurrently could
    /// otherwise pick the same one, and the loser's orphan cleanup would retire the winner's live
    /// partition.
    /// </para>
    /// </summary>
    internal Task<SplitOutcome> ForceSplitAtKeyAsync(
        string keySpace,
        string splitKey,
        Func<Task>? duringQuiesce = null,
        CancellationToken ct = default) =>
        rangeSplitTrigger!.ExecuteSplitAtKeyAsync(keySpace, splitKey, duringQuiesce, ct);
}
