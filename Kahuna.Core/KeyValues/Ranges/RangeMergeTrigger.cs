
using Kommander;
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
    private readonly int minMergeSize;
    private readonly ILogger<IKahuna> logger;

    // Partition IDs whose RemovePartitionAsync failed on a prior tick; retried each invocation.
    private readonly HashSet<int> pendingRemovals = [];

    public RangeMergeTrigger(
        IRaft raft,
        RangeMapStore rangeMapStore,
        RangeMerger merger,
        KahunaConfiguration configuration,
        ILogger<IKahuna> logger)
    {
        this.raft         = raft;
        this.rangeMapStore = rangeMapStore;
        this.merger        = merger;
        this.minMergeSize  = configuration.RangeMergeMinSize;
        this.logger        = logger;
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
        if (!await raft.AmILeader(RangeMapStore.MetaPartitionId, ct))
            return 0;

        // Retry any partition removals that failed on a previous tick.
        if (pendingRemovals.Count > 0)
        {
            var retried = new List<int>(pendingRemovals);
            foreach (int partId in retried)
            {
                ct.ThrowIfCancellationRequested();
                var retryResult = await raft.RemovePartitionAsync(partId, ct);
                if (retryResult.Success)
                {
                    pendingRemovals.Remove(partId);
                    logger.LogRangeMergeTriggerRetryRetired(partId);
                }
                else
                {
                    logger.LogRangeMergeTriggerRetryFailed(partId, retryResult.Status.ToString());
                }
            }
        }

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

                logger.LogRangeMergeTriggerMerging(keySpace, left.StartKey ?? "-inf", left.EndKey, left.PartitionId, right.StartKey, right.EndKey ?? "+inf", right.PartitionId);

                MergeOutcome outcome = await merger.MergeAsync(keySpace, left, right, ct);

                if (!outcome.IsSuccess)
                {
                    logger.LogRangeMergeTriggerFailed(keySpace, outcome.Status.ToString());
                    continue;
                }

                // Retire the vacated partition. We are the system-partition (0) leader so this
                // call is permitted. Best-effort: a failed removal is logged but does not roll
                // back the descriptor cutover (the orphan partition is simply never served).
                var removeResult = await raft.RemovePartitionAsync(outcome.RetiredPartitionId, ct);

                if (!removeResult.Success)
                {
                    pendingRemovals.Add(outcome.RetiredPartitionId);
                    logger.LogRangeMergeTriggerRemoveFailed(outcome.RetiredPartitionId, keySpace, removeResult.Status.ToString());
                }
                else
                {
                    logger.LogRangeMergeTriggerRetired(outcome.RetiredPartitionId, keySpace);
                }

                mergesDone++;

                // Re-read the map after each merge: a successful merge changes descriptor
                // counts and the cached candidate list is stale. Break out of the inner loop;
                // the outer periodic tick will pick up any remaining candidates next time.
                break;
            }
        }

        return mergesDone;
    }
}
