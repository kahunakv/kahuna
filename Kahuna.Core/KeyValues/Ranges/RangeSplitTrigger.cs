
using System.Collections.Concurrent;
using System.Diagnostics;

using Kommander;
using Kommander.Data;
using Kommander.System;
using Kommander.Time;

using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues.Logging;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Ranges;

/// <summary>
/// Checks every registered KeyRange descriptor and splits those that exceed
/// <see cref="KahunaConfiguration.RangeSplitThreshold"/> keys (count branch) or that have
/// been hot and backlogged for the full <see cref="KahunaConfiguration.RangeSplitLoadWindow"/>
/// (load branch, K2.2).
///
/// <para>
/// <b>Leader requirement.</b> Both branches require this node to be the meta-partition (P0)
/// leader. <see cref="TriggerAsync"/> (count branch, slow cadence) and
/// <see cref="LoadCheckAsync"/> (load branch, fast cadence) guard this independently; the
/// periodic callers skip gracefully on non-leader nodes.
/// </para>
///
/// <para>
/// <b>Sampling.</b> The trigger samples each range by reading keys from
/// <c>manager.GetByRange</c> in pages of <see cref="SamplePageSize"/> keys,
/// accumulating up to <see cref="MaxSampleKeys"/> keys. The sample is representative
/// enough for the policy; it is not a full count.
/// </para>
///
/// <para>
/// <b>Local-node sampling.</b> <c>manager.GetByRange</c> reads the local node's actor state.
/// This is correct today because the persistence backend is node-global (every replica holds all
/// keys). If storage is ever partitioned so that only the data-partition leader holds a range's
/// rows, sampling must be redirected to that leader instead.
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
    private readonly KeyWriteFrequencyRegistry writeFrequencyRegistry;

    // Count-branch config
    private readonly int threshold;
    private readonly int minRangeSize;

    // Load-branch config (K2.1 / K2.2)
    private readonly double loadThreshold;
    private readonly int    loadMinQueueDepth;
    private readonly double loadMaxCommitWaitMs;
    private readonly TimeSpan loadWindow;
    private readonly double loadImbalanceMax;

    // Per-descriptor debounce state for the load branch (K2.2):
    //   partitionId → Stopwatch.GetTimestamp() when the AND-predicate first held.
    // Cleared on predicate failure, on split (success or refusal), and on leadership loss.
    private readonly ConcurrentDictionary<int, long> hotSince = new();

    // Per-descriptor K2.3 refusal cooldown: partitionId → Stopwatch timestamp of last refusal.
    // After K2.3 refuses a split, the count branch skips sampling for IndivisibleCooldown so
    // a persistently-skewed large range is not re-sampled+re-logged every CollectionInterval.
    // The load branch is already rate-limited by the hotSince reset on refusal (one re-attempt
    // per loadWindow); this dict adds the same protection for the count branch.
    // Cleared when a split succeeds (histogram transferred, situation changed).
    private readonly ConcurrentDictionary<int, long> indivisibleAt = new();

    // How long to suppress count-branch re-sampling after a K2.3 refusal.
    // ~5× the default CollectionInterval (60 s); long enough to avoid spam while short enough
    // that a genuine load shift (hot-key traffic stops, histogram decays) is re-evaluated.
    private static readonly TimeSpan IndivisibleCooldown = TimeSpan.FromMinutes(5);

    // Serializes the create-partition + split-async region across both checker cadences.
    // RangeSplitCheckerActor (count, ~60s) and RangeSplitLoadCheckerActor (load, ~5s) are
    // separate Nixie actors with separate mailboxes, so they can enter TriggerAsync and
    // LoadCheckAsync concurrently. ComputeNextPartitionId reads the current map snapshot, so
    // two concurrent branches can compute the same newId and both call CreatePartitionAsync
    // with it. MutateAsync's generation guard makes this safe at the data layer (the second
    // call becomes a no-op), but we still pay a wasted CreatePartitionAsync RPC with a
    // partition-id collision. The semaphore eliminates that.
    private readonly SemaphoreSlim splitLock = new(1, 1);

    private readonly ILogger<IKahuna> logger;

    public RangeSplitTrigger(
        IRaft raft,
        RangeMapStore rangeMapStore,
        RangeSplitter splitter,
        KeyValuesManager manager,
        KeyWriteFrequencyRegistry writeFrequencyRegistry,
        KahunaConfiguration configuration,
        ILogger<IKahuna> logger)
    {
        this.raft                   = raft;
        this.rangeMapStore          = rangeMapStore;
        this.splitter               = splitter;
        this.manager                = manager;
        this.writeFrequencyRegistry = writeFrequencyRegistry;
        this.threshold              = configuration.RangeSplitThreshold;
        this.minRangeSize           = configuration.RangeSplitMinRangeSize;
        this.loadThreshold          = configuration.RangeSplitLoadThreshold;
        this.loadMinQueueDepth      = configuration.RangeSplitLoadMinQueueDepth;
        this.loadMaxCommitWaitMs    = configuration.RangeSplitLoadMaxCommitWaitMs;
        this.loadWindow             = configuration.RangeSplitLoadWindow;
        this.loadImbalanceMax       = configuration.RangeSplitLoadImbalanceMax;
        this.logger                 = logger;
    }

    /// <summary>
    /// Scans all KeyRange descriptors and splits any whose sampled key count exceeds
    /// <see cref="KahunaConfiguration.RangeSplitThreshold"/> (count branch).
    /// Returns the number of splits successfully performed.
    /// </summary>
    public async Task<int> TriggerAsync(CancellationToken ct = default)
    {
        // Guard: only run on the meta-partition (P0) leader. Since Kommander 0.11.0 the meta map
        // shares P0, so CreatePartitionAsync (system-leader) and the descriptor cutover (meta-leader)
        // require the same node — no P0+P1 colocation to coordinate. The periodic caller skips
        // gracefully when this node is not the P0 leader.
        if (!await raft.AmILeader(RangeMapStore.MetaPartitionId, ct))
            return 0;

        // Decay all write-frequency histograms once per checker pass so counts reflect recent
        // load rather than lifetime totals. This runs only on the meta-leader node (inside the
        // AmILeader gate above); follower replicas accumulate un-decayed lifetime counts. On a
        // meta-leader failover the new trigger node reads those lifetime-weighted counts until a
        // few passes erode them — the centroid lags recency briefly after failover, which is
        // acceptable given Decay()'s half-life convergence rate.
        foreach (KeyValuePair<int, KeyWriteFrequencyTracker> kv in writeFrequencyRegistry.All)
            kv.Value.Decay();

        // Count branch is disabled when threshold == 0. Return early but AFTER Decay() so the
        // load branch (LoadCheckAsync) always reads decayed histogram counts regardless of whether
        // the count branch is enabled.
        if (threshold <= 0)
            return 0;

        RangeMap map = rangeMapStore.Current;

        // Collect all unique KeyRange spaces from the descriptor map.
        // We use the map rather than KeySpaceRegistry.GetMode to avoid a cross-assembly enum import
        // and because the map is the authoritative record of what exists.
        IEnumerable<IGrouping<string, RangeDescriptor>> groups =
            map.Descriptors.GroupBy(d => d.KeySpace);

        int splitsDone = 0;

        foreach (IGrouping<string, RangeDescriptor> group in groups)
        {
            foreach (RangeDescriptor descriptor in group)
            {
                ct.ThrowIfCancellationRequested();

                // Skip if K2.3 refused this descriptor recently — avoids re-sampling 4096 keys
                // every CollectionInterval for a persistently-skewed range.
                if (IsInIndivisibleCooldown(descriptor.PartitionId))
                    continue;

                // Count branch: split when sampled key count >= threshold.
                string? splitKey = await TryComputeSplitKeyAsync(descriptor, threshold, ct);
                if (splitKey is null)
                    continue;

                if (await ExecuteSplitAsync(descriptor, splitKey, ct))
                    splitsDone++;
            }
        }

        return splitsDone;
    }

    /// <summary>
    /// Fast-path load poll (K2.2). Evaluates the load predicate for every KeyRange descriptor
    /// and fires a split when the predicate has held continuously for
    /// <see cref="KahunaConfiguration.RangeSplitLoadWindow"/>. Returns the number of splits done.
    /// </summary>
    /// <remarks>
    /// Called at <see cref="KahunaConfiguration.RangeSplitLoadPollInterval"/> (~5 s) by
    /// <see cref="RangeSplitLoadCheckerActor"/>, decoupled from the slow count-check cadence
    /// (<see cref="TriggerAsync"/> at <see cref="KahunaConfiguration.CollectionInterval"/> ~60 s).
    /// The typical poll is O(descriptors) with only three cheap <see cref="IRaft"/> accessor
    /// reads per descriptor; the expensive key-sampling step fires only when the debounce window
    /// elapses, which for normal workloads is rare.
    /// </remarks>
    public async Task<int> LoadCheckAsync(CancellationToken ct = default)
    {
        if (loadThreshold <= 0)
            return 0;

        if (!await raft.AmILeader(RangeMapStore.MetaPartitionId, ct))
        {
            // Lost meta-leadership — reset all debounce state so a future promotion starts clean
            // and doesn't inherit a hotSince timestamp from the previous leadership tenure.
            hotSince.Clear();
            return 0;
        }

        RangeMap map = rangeMapStore.Current;
        long now = Stopwatch.GetTimestamp();
        int splitsDone = 0;

        foreach (RangeDescriptor descriptor in map.Descriptors)
        {
            ct.ThrowIfCancellationRequested();

            int partitionId = descriptor.PartitionId;

            if (!EvaluateLoadPredicate(partitionId, out double ops, out int depth, out double commitWait))
            {
                // Predicate no longer holds — clear debounce so the next hot window starts fresh.
                hotSince.TryRemove(partitionId, out _);
                continue;
            }

            // Record when this descriptor first went hot; no-op if already tracked.
            long since = hotSince.GetOrAdd(partitionId, now);
            double elapsedMs = (now - since) * 1000.0 / Stopwatch.Frequency;

            if (elapsedMs < loadWindow.TotalMilliseconds)
                continue; // debounce window not yet elapsed

            // Debounce window satisfied — log using the values already read by EvaluateLoadPredicate.
            logger.LogRangeSplitTriggerLoadHot(descriptor.KeySpace, partitionId, elapsedMs, ops, depth, commitWait);

            // Load branch uses 2*minRangeSize as the effective threshold — a small-but-hot range
            // can split even if it is far below the count threshold.
            string? splitKey = await TryComputeSplitKeyAsync(descriptor, 2 * minRangeSize, ct);

            // Whether the split is indivisible, too small, or succeeds, reset the debounce so
            // the load branch doesn't retry on every poll interval.
            hotSince.TryRemove(partitionId, out _);

            if (splitKey is null)
                continue; // indivisible or too small — TryComputeSplitKeyAsync already logged

            if (await ExecuteSplitAsync(descriptor, splitKey, ct))
                splitsDone++;
        }

        return splitsDone;
    }

    // ── private helpers ──────────────────────────────────────────────────────

    /// <summary>
    /// Executes the create-partition + split-async step for <paramref name="descriptor"/> at
    /// <paramref name="splitKey"/>, serialized via <see cref="splitLock"/> so the count and
    /// load checker cadences cannot race on the same newId computation.
    /// </summary>
    /// <returns><c>true</c> if the split succeeded.</returns>
    private async Task<bool> ExecuteSplitAsync(RangeDescriptor descriptor, string splitKey, CancellationToken ct)
    {
        logger.LogRangeSplitTriggerSplitting(descriptor.KeySpace, descriptor.StartKey ?? "−∞", descriptor.EndKey ?? "+∞", splitKey);

        await splitLock.WaitAsync(ct);
        try
        {
            int newId = RangeSplitter.ComputeNextPartitionId(rangeMapStore.Current);

            RaftPartitionLifecycleResult createResult =
                await raft.CreatePartitionAsync(newId, RaftRoutingMode.Unrouted, null, ct);

            if (!createResult.Success)
            {
                logger.LogRangeSplitTriggerCreateFailed(newId, descriptor.KeySpace);
                return false;
            }

            SplitOutcome outcome = await splitter.SplitAsync(descriptor.KeySpace, splitKey, newId, ct);

            if (!outcome.IsSuccess)
            {
                logger.LogRangeSplitTriggerSplitFailed(descriptor.KeySpace, splitKey, outcome.Status.ToString());
                return false;
            }

            logger.LogRangeSplitTriggerSplit(descriptor.KeySpace, splitKey, newId);

            // Transfer write-frequency histogram to the two child ranges (K1b).
            TransferTrackerOnSplit(descriptor, splitKey, newId);

            // Reset load-branch debounce and indivisibility cooldown for the left child
            // (which inherits the parent partition ID) — the split changed the situation.
            hotSince.TryRemove(descriptor.PartitionId, out _);
            indivisibleAt.TryRemove(descriptor.PartitionId, out _);

            return true;
        }
        finally
        {
            splitLock.Release();
        }
    }

    /// <summary>
    /// Returns <c>true</c> when the descriptor's last K2.3 refusal is still within the
    /// <see cref="IndivisibleCooldown"/> window, meaning the count branch should skip sampling.
    /// </summary>
    private bool IsInIndivisibleCooldown(int partitionId)
    {
        if (!indivisibleAt.TryGetValue(partitionId, out long refusedTick))
            return false;

        double elapsedMs = (Stopwatch.GetTimestamp() - refusedTick) * 1000.0 / Stopwatch.Frequency;
        return elapsedMs < IndivisibleCooldown.TotalMilliseconds;
    }

    /// <summary>
    /// Returns <c>true</c> when partition <paramref name="partitionId"/> satisfies the K2.2
    /// load AND-predicate: rate ≥ threshold AND WAL queue depth ≥ min AND (commit-wait gate
    /// disabled OR commit-wait ≥ configured max). All values come from the K1a gossiped load
    /// report, so the check is valid even when the partition is led on another node.
    /// </summary>
    private bool EvaluateLoadPredicate(int partitionId, out double ops, out int depth, out double commitWait)
    {
        ops        = raft.GetPartitionLogOpsPerSecond(partitionId);
        depth      = raft.GetPartitionWalQueueDepth(partitionId);
        commitWait = raft.GetPartitionCommitWaitMs(partitionId);

        if (ops < loadThreshold)
            return false;

        if (depth < loadMinQueueDepth)
            return false;

        // Optional secondary saturation gate. AND-combined so it can never fire on its own.
        if (loadMaxCommitWaitMs > 0 && commitWait < loadMaxCommitWaitMs)
            return false;

        return true;
    }

    /// <summary>
    /// Samples <paramref name="descriptor"/>'s key range and returns a split key when the
    /// sample size meets <paramref name="effectiveThreshold"/>, or <c>null</c> if no split
    /// is warranted. Applies the K2.3 indivisibility guard when a write-frequency histogram
    /// is available and <see cref="loadImbalanceMax"/> is configured.
    /// </summary>
    /// <param name="effectiveThreshold">
    /// Minimum sample size required before a split is attempted.
    /// Count branch passes <see cref="threshold"/>; load branch passes <c>2 * minRangeSize</c>.
    /// </param>
    private async Task<string?> TryComputeSplitKeyAsync(RangeDescriptor descriptor, int effectiveThreshold, CancellationToken ct)
    {
        var sample = new List<(string Key, HLCTimestamp LastModified)>(Math.Min(effectiveThreshold + 64, MaxSampleKeys));

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

            // Early exit: we have enough to exceed the effective threshold + minRangeSize cushion.
            if (sample.Count >= effectiveThreshold + minRangeSize)
                break;
        }

        // Look up the write-frequency snapshot for this partition (K1b).
        // The snapshot may be empty (cold histogram — post-failover blind window or first run);
        // ComputeSplitKey falls back to the count-based median/percentile path transparently.
        IReadOnlyDictionary<string, long>? writeFreq =
            writeFrequencyRegistry.TryGet(descriptor.PartitionId)?.GetSnapshot();

        string? splitKey = RangeSplitPolicy.ComputeSplitKey(sample, effectiveThreshold, minRangeSize, writeFreq, out double achievableImbalance);

        // K2.3 indivisibility guard: refuse the split when the best achievable write-centroid
        // still concentrates too many writes on one child (e.g. all traffic on a single hot key).
        // Applies to both count and load branches whenever loadImbalanceMax is configured.
        // achievableImbalance == 0 means the histogram was cold and the count path was used —
        // in that case the guard does not apply.
        if (splitKey is not null && loadImbalanceMax > 0 && achievableImbalance > 0 && achievableImbalance >= loadImbalanceMax)
        {
            logger.LogRangeSplitTriggerIndivisible(descriptor.KeySpace, descriptor.PartitionId, achievableImbalance, loadImbalanceMax);
            indivisibleAt[descriptor.PartitionId] = Stopwatch.GetTimestamp();
            return null;
        }

        return splitKey;
    }

    // ── split-tracker transfer (K1b) ─────────────────────────────────────────

    /// <summary>
    /// After a successful split of <paramref name="parentDescriptor"/> at <paramref name="splitKey"/>,
    /// partitions the parent's write-frequency tracker into two child trackers keyed by the new
    /// partition IDs so each child starts with a warm histogram rather than rebuilding from zero.
    ///
    /// <para>
    /// The parent tracker is removed from the registry; the two children (parent-range and
    /// child-range) are installed under their respective partition IDs. If the parent tracker is
    /// absent (no writes recorded, or leadership was lost), this is a no-op — both children start
    /// cold and fall back to the count-based median until their histograms warm up.
    /// </para>
    /// </summary>
    internal void TransferTrackerOnSplit(
        RangeDescriptor parentDescriptor,
        string splitKey,
        int childPartitionId)
    {
        KeyWriteFrequencyTracker? parent =
            writeFrequencyRegistry.TryGet(parentDescriptor.PartitionId);

        if (parent is null)
            return;

        // Left child: [parentStart, splitKey) stays on the original partition.
        KeyWriteFrequencyTracker leftTracker =
            parent.FilterForChild(parentDescriptor.StartKey, splitKey);
        writeFrequencyRegistry.Replace(parentDescriptor.PartitionId, leftTracker);

        // Right child: [splitKey, parentEnd) moves to the new partition.
        KeyWriteFrequencyTracker rightTracker =
            parent.FilterForChild(splitKey, parentDescriptor.EndKey);
        writeFrequencyRegistry.Replace(childPartitionId, rightTracker);

        // Note: this transfer only updates the trigger node's (meta-leader's) registry. Other
        // replicas keep the parent-id tracker carrying both halves' keys until their out-of-range
        // entries decay away. If meta leadership migrates before decay completes, the new trigger
        // reads the un-split parent tracker for one or more passes — harmless since Decay() heals
        // it within a few half-lives and the count-based fallback remains correct throughout.
    }
}
