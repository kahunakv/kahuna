
using Kommander;
using Kommander.Data;
using Kommander.System;
using Kommander.Time;

using Kahuna.Server.Configuration;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Ranges;

/// <summary>
/// Checks every registered KeyRange descriptor and splits those that exceed
/// <see cref="KahunaConfiguration.RangeSplitThreshold"/> keys.
///
/// <para>
/// <b>Leader requirement.</b> Must be invoked on the node that is simultaneously the
/// <b>system-partition (0) leader</b> (needed for <see cref="IRaft.CreatePartitionAsync"/>)
/// AND the <b>meta-partition (1) leader</b> (needed for <see cref="RangeMapStore.MutateAsync"/>).
/// In practice the auto-split is triggered from a periodic background task that checks both.
/// </para>
///
/// <para>
/// <b>Sampling.</b> The trigger samples each range by reading keys from
/// <c>manager.GetByRange</c> in pages of <see cref="SamplePageSize"/> keys,
/// accumulating up to <see cref="MaxSampleKeys"/> keys. The sample is representative
/// enough for the policy; it is not a full count.
/// </para>
/// </summary>
internal sealed class RangeSplitTrigger
{
    /// <summary>Keys fetched per sample page.</summary>
    private const int SamplePageSize = 512;

    /// <summary>Maximum keys accumulated for the split-key computation. Bounding memory usage.</summary>
    private const int MaxSampleKeys = 4096;

    private readonly IRaft raft;
    private readonly RangeMapStore rangeMapStore;
    private readonly RangeSplitter splitter;
    private readonly KeyValuesManager manager;
    private readonly int threshold;
    private readonly int minRangeSize;
    private readonly ILogger<IKahuna> logger;

    public RangeSplitTrigger(
        IRaft raft,
        RangeMapStore rangeMapStore,
        RangeSplitter splitter,
        KeyValuesManager manager,
        KahunaConfiguration configuration,
        ILogger<IKahuna> logger)
    {
        this.raft         = raft;
        this.rangeMapStore = rangeMapStore;
        this.splitter      = splitter;
        this.manager       = manager;
        this.threshold     = configuration.RangeSplitThreshold;
        this.minRangeSize  = configuration.RangeSplitMinRangeSize;
        this.logger        = logger;
    }

    /// <summary>
    /// Scans all KeyRange descriptors and splits any that exceed the threshold.
    /// Returns the number of splits successfully performed.
    /// </summary>
    public async Task<int> TriggerAsync(CancellationToken ct = default)
    {
        // Guard: only run on the node that is leader of both system (0) and meta (1) partitions.
        // In practice these are often different nodes, so we require both. The periodic caller
        // handles skipping gracefully when this node does not hold both.
        if (!await raft.AmILeader(0, ct) || !await raft.AmILeader(RangeMapStore.MetaPartitionId, ct))
            return 0;

        RangeMap map = rangeMapStore.Current;

        // Collect all unique KeyRange spaces from the descriptor map.
        // We use the map rather than KeySpaceRegistry.GetMode to avoid a cross-assembly enum import
        // and because the map is the authoritative record of what exists.
        IEnumerable<IGrouping<string, RangeDescriptor>> groups =
            map.Descriptors
               .GroupBy(d => d.KeySpace);

        int splitsDone = 0;

        foreach (IGrouping<string, RangeDescriptor> group in groups)
        {
            foreach (RangeDescriptor descriptor in group)
            {
                ct.ThrowIfCancellationRequested();

                string? splitKey = await TryComputeSplitKeyAsync(descriptor, ct);
                if (splitKey is null)
                    continue;

                logger.LogInformation(
                    "RangeSplitTrigger: splitting {Space} [{Start},{End}) at {Key}",
                    descriptor.KeySpace,
                    descriptor.StartKey ?? "−∞",
                    descriptor.EndKey   ?? "+∞",
                    splitKey);

                int newId = RangeSplitter.ComputeNextPartitionId(rangeMapStore.Current);

                RaftPartitionLifecycleResult createResult =
                    await raft.CreatePartitionAsync(newId, RaftRoutingMode.Unrouted, null, ct);

                if (!createResult.Success)
                {
                    logger.LogWarning(
                        "RangeSplitTrigger: CreatePartitionAsync({Id}) failed for {Space}", newId, descriptor.KeySpace);
                    continue;
                }

                SplitOutcome outcome =
                    await splitter.SplitAsync(descriptor.KeySpace, splitKey, newId, ct);

                if (outcome.IsSuccess)
                {
                    splitsDone++;
                    logger.LogInformation(
                        "RangeSplitTrigger: split {Space} at {Key} → P{Id}", descriptor.KeySpace, splitKey, newId);
                }
                else
                {
                    logger.LogWarning(
                        "RangeSplitTrigger: SplitAsync failed for {Space} at {Key}: {Status}",
                        descriptor.KeySpace, splitKey, outcome.Status);
                }
            }
        }

        return splitsDone;
    }

    // ── private helpers ──────────────────────────────────────────────────────

    /// <summary>
    /// Samples <paramref name="descriptor"/>'s key range and returns a split key if the
    /// range exceeds the threshold, or <c>null</c> if no split is warranted.
    /// </summary>
    private async Task<string?> TryComputeSplitKeyAsync(RangeDescriptor descriptor, CancellationToken ct)
    {
        var sample = new List<(string Key, HLCTimestamp LastModified)>(Math.Min(threshold + 64, MaxSampleKeys));

        string prefix = descriptor.KeySpace;
        string? cursor = null;
        bool hasMore   = true;

        while (hasMore && sample.Count < MaxSampleKeys)
        {
            ct.ThrowIfCancellationRequested();

            string? pageStart;
            bool    pageStartInclusive;

            if (cursor is null)
            {
                pageStart          = descriptor.StartKey;
                pageStartInclusive = true;
            }
            else
            {
                pageStart          = cursor;
                pageStartInclusive = false; // cursor is the last key returned (exclusive next)
            }

            KeyValueGetByRangeResult page = await manager.GetByRange(
                HLCTimestamp.Zero,
                prefix,
                pageStart,
                pageStartInclusive,
                descriptor.EndKey,
                false,
                SamplePageSize,
                HLCTimestamp.Zero,
                KeyValueDurability.Persistent);

            if (page.Type != KeyValueResponseType.Get || page.Items.Count == 0)
                break;

            foreach ((string key, ReadOnlyKeyValueEntry entry) in page.Items)
                sample.Add((key, entry.LastModified));

            cursor  = page.Items[^1].Item1;
            hasMore = page.HasMore;

            // Early exit: we have enough to exceed the threshold + minRangeSize cushion.
            if (sample.Count >= threshold + minRangeSize)
                break;
        }

        return RangeSplitPolicy.ComputeSplitKey(sample, threshold, minRangeSize);
    }
}
