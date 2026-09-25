
using Kommander;
using Kommander.Data;
using Kommander.System;

using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues.Logging;

namespace Kahuna.Server.KeyValues.Ranges;

/// <summary>
/// Scans all KeyRange spaces, finds adjacent descriptor pairs where both are under the configured
/// minimum size, and merges each candidate pair.
///
/// <para>
/// <b>Leader requirement.</b> Must be invoked on the node that is simultaneously the
/// <b>system-partition (0) leader</b> (needed for <see cref="IRaft.RemovePartitionAsync"/>)
/// AND the <b>meta-partition (1) leader</b> (needed for <see cref="RangeMerger.MergeAsync"/>
/// which calls <see cref="RangeMapStore.MutateAsync"/>). Returns 0 immediately on nodes that
/// do not hold both roles. The periodic caller (<see cref="RangeMergeCheckerActor"/>) handles
/// graceful skipping.
/// </para>
///
/// <para>
/// <b>Local-node sampling.</b> <c>RangeMerger.CountRangeKeysAsync</c> reads the local node's
/// actor state via <c>manager.GetByRange</c>. This is correct today because the persistence
/// backend is node-global (every replica holds all keys). If storage is ever partitioned so that
/// only the data-partition leader holds a range's rows, sampling must be redirected to that
/// leader instead.
/// </para>
///
/// <para>
/// <b>Size dimension.</b> "Size" is measured in key count, not bytes or request load.
/// Byte-size and load-based merging are deferred.
/// </para>
/// </summary>
internal sealed class RangeMergeTrigger
{
    private readonly IRaft raft;
    private readonly RangeMapStore rangeMapStore;
    private readonly RangeMerger merger;
    private readonly KeyWriteFrequencyRegistry writeFrequencyRegistry;
    private readonly int minMergeSize;

    // When > 0, a partition whose log-ops/sec meets or exceeds this value is considered warm
    // and is ineligible for merging. Merging a warm partition would cause the merged result to
    // immediately re-trigger a load split, oscillating between merge and split indefinitely.
    // Mirrors RangeSplitLoadThreshold so the two triggers share the same rate boundary.
    private readonly double loadThreshold;

    private readonly ILogger<IKahuna> logger;

    public RangeMergeTrigger(
        IRaft raft,
        RangeMapStore rangeMapStore,
        RangeMerger merger,
        KeyWriteFrequencyRegistry writeFrequencyRegistry,
        KahunaConfiguration configuration,
        ILogger<IKahuna> logger)
    {
        this.raft                   = raft;
        this.rangeMapStore          = rangeMapStore;
        this.merger                 = merger;
        this.writeFrequencyRegistry = writeFrequencyRegistry;
        this.minMergeSize           = configuration.RangeMergeMinSize;
        this.loadThreshold          = configuration.RangeSplitLoadThreshold;
        this.logger                 = logger;
    }

    /// <summary>
    /// Scans all KeyRange spaces for under-min adjacent descriptor pairs and merges them.
    /// Returns the number of merges successfully performed.
    /// </summary>
    public async Task<int> TriggerAsync(CancellationToken ct = default)
    {
        // Guard: only run on the system-partition (P0) leader. Since Kommander 0.11.0 the meta map
        // shares P0, so RemovePartitionAsync (system-leader) and MergeAsync/MutateAsync (meta-leader)
        // require the same node — the old P0+P1 colocation requirement is gone.
        if (!await raft.AmILeaderIfHosted(RangeMapStore.MetaPartitionId, ct))
            return 0;

        // Finish removals earlier merges recorded in the replicated map but could not complete —
        // on this node or on any node that led before it. The list is part of the committed map,
        // so it survives leadership changes, restarts, and this trigger being a fresh instance.
        await RemoveRetiredPartitionsAsync(ct);

        RangeMap map = rangeMapStore.Current;

        IEnumerable<IGrouping<string, RangeDescriptor>> groups =
            map.Descriptors.GroupBy(d => d.KeySpace);

        int mergesDone = 0;

        foreach (IGrouping<string, RangeDescriptor> group in groups)
        {
            ct.ThrowIfCancellationRequested();

            string keySpace = group.Key;

            List<(RangeDescriptor Left, RangeDescriptor Right)> candidates =
                await merger.FindMergeCandidatesAsync(keySpace, minMergeSize, ct);

            foreach ((RangeDescriptor left, RangeDescriptor right) in candidates)
            {
                ct.ThrowIfCancellationRequested();

                // Load-warm guard: refuse to merge when either partition is actively serving
                // load at or above the split rate threshold. Merging a warm partition would
                // produce a combined range that immediately re-triggers a load split, bouncing
                // between merge and split indefinitely. Skip — the pair will be re-evaluated
                // on the next checker tick once both sides have cooled.
                if (IsWarm(left.PartitionId) || IsWarm(right.PartitionId))
                {
                    logger.LogRangeMergeTriggerWarmSkipped(keySpace, left.PartitionId, right.PartitionId);
                    RangeSplitMetrics.MergeWarmSkips.Add(1, new KeyValuePair<string, object?>("keyspace", keySpace));
                    continue;
                }

                logger.LogRangeMergeTriggerMerging(keySpace, left.StartKey ?? "-inf", left.EndKey, left.PartitionId, right.StartKey, right.EndKey ?? "+inf", right.PartitionId);

                MergeOutcome outcome = await merger.MergeAsync(keySpace, left, right, ct);

                if (!outcome.IsSuccess)
                {
                    logger.LogRangeMergeTriggerFailed(keySpace, outcome.Status.ToString());
                    continue;
                }

                // Retire the vacated partition now that the cutover listed it as retired. A removal
                // that does not land here (lost system leadership, a proposal without a verdict) is
                // not rolled back and not forgotten: the id stays in the committed map and the next
                // pass on whichever node leads the system partition finishes it.
                if (await RemoveRetiredPartitionAsync(outcome.RetiredPartitionId, ct))
                    logger.LogRangeMergeTriggerRetired(outcome.RetiredPartitionId, keySpace);
                else
                    logger.LogRangeMergeTriggerRemoveDeferred(outcome.RetiredPartitionId, keySpace);

                mergesDone++;

                // Re-read the map after each merge: a successful merge changes descriptor
                // counts and the cached candidate list is stale. Break out of the inner loop;
                // the outer periodic tick will pick up any remaining candidates next time.
                break;
            }
        }

        return mergesDone;
    }

    /// <summary>
    /// Removes every partition the committed map lists as retired, on the system-partition leader.
    /// Each removal that lands is followed by a map commit that drops the id, so a pass interrupted
    /// by a leadership change leaves at most an id whose partition is already removed, and the next
    /// leader's pass re-runs the removal (idempotent in Kommander) and then drops the id.
    /// </summary>
    private async Task RemoveRetiredPartitionsAsync(CancellationToken ct)
    {
        IReadOnlyList<int> retired = rangeMapStore.Current.RetiredPartitionIds;

        for (int i = 0; i < retired.Count; i++)
        {
            ct.ThrowIfCancellationRequested();

            int partitionId = retired[i];

            if (await RemoveRetiredPartitionAsync(partitionId, ct))
                logger.LogRangeMergeTriggerRetryRetired(partitionId);
        }
    }

    /// <summary>
    /// Removes one retired partition's Raft group and, once that landed, commits a map without its
    /// id. Returns false when either step did not land; the id then stays in the committed map for
    /// a later pass. Never removes a partition a descriptor still routes to: the map commit refuses
    /// to list such an id, and this re-checks the current map in case the two ever disagree.
    /// </summary>
    private async Task<bool> RemoveRetiredPartitionAsync(int partitionId, CancellationToken ct)
    {
        RangeMap map = rangeMapStore.Current;

        foreach (RangeDescriptor descriptor in map.Descriptors)
        {
            if (descriptor.PartitionId == partitionId)
            {
                logger.LogRangeMergeTriggerRetiredStillRouted(partitionId, descriptor.ToString());
                return false;
            }
        }

        RaftPartitionLifecycleResult removeResult;

        try
        {
            removeResult = await raft.RemovePartitionAsync(partitionId, ct);
        }
        catch (RaftException ex)
        {
            // Lost the system-partition leadership between the guard and the call, or the cluster is
            // not initialized: the removal belongs to whoever leads next.
            logger.LogRangeMergeTriggerRetryFailed(partitionId, ex.Message);
            return false;
        }

        if (!removeResult.Success)
        {
            logger.LogRangeMergeTriggerRetryFailed(partitionId, removeResult.Status.ToString());
            return false;
        }

        writeFrequencyRegistry.Remove(partitionId);

        // The Raft group is gone; drop the id from the map. A commit that does not land keeps the id,
        // and the next pass's removal answers Success at once (already removed) before retrying this.
        bool dropped = await rangeMapStore.MutateMapAsync(existing => existing.WithoutRetired(partitionId), ct);

        if (!dropped)
            logger.LogRangeMergeTriggerRetiredUnlisted(partitionId);

        return dropped;
    }

    /// <summary>
    /// Returns <c>true</c> when <paramref name="partitionId"/> is currently serving load at or
    /// above the split rate threshold, making it ineligible for merging.
    /// Always <c>false</c> when <see cref="loadThreshold"/> is zero (load-based splitting disabled).
    /// </summary>
    /// <remarks>
    /// Intentionally rate-only (not the full rate-AND-saturation predicate used by the split
    /// trigger). A partition busy enough to approach the split threshold should not be merged
    /// regardless of queue depth or commit-wait, so the simpler check is both correct and safely
    /// over-conservative: it refuses merges on partitions that are busy-but-not-yet-splitting,
    /// which is the right direction to err in for oscillation prevention.
    /// </remarks>
    private bool IsWarm(int partitionId)
    {
        if (loadThreshold <= 0)
            return false;

        return raft.GetPartitionLogOpsPerSecond(partitionId) >= loadThreshold;
    }
}
