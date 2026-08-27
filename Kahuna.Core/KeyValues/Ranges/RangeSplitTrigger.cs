
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
/// (load branch).
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
internal sealed class RangeSplitTrigger : IDisposable
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

    // Load-branch config
    private readonly double loadThreshold;
    private readonly int    loadMinQueueDepth;
    private readonly double loadMinCommitWaitMs;
    private readonly TimeSpan loadWindow;
    private readonly double loadImbalanceMax;

    // Post-split settle window
    private readonly TimeSpan settleWindow;

    // Per-descriptor post-split cooldown: partitionId → Stopwatch timestamp of the split.
    // Both the left child (which inherits the parent partition ID) and the right child (newId) are
    // recorded so neither can re-split until settleWindow elapses. Cleared on leadership loss.
    private readonly ConcurrentDictionary<int, long> settledAt = new();

    // Per-descriptor debounce state for the load branch:
    //   partitionId → Stopwatch.GetTimestamp() when the AND-predicate first held.
    // Cleared on predicate failure, on split (success or refusal), and on leadership loss.
    private readonly ConcurrentDictionary<int, long> hotSince = new();

    // Per-descriptor indivisibility refusal cooldown: partitionId → Stopwatch timestamp of last refusal.
    // After a split is refused as indivisible, the count branch skips sampling for indivisibleCooldown
    // so a persistently-skewed large range is not re-sampled+re-logged every CollectionInterval.
    // The load branch is already rate-limited by the hotSince reset on refusal (one re-attempt
    // per loadWindow); this dict adds the same protection for the count branch.
    // Cleared when a split succeeds (histogram transferred, situation changed).
    private readonly ConcurrentDictionary<int, long> indivisibleAt = new();

    private readonly TimeSpan indivisibleCooldown;

    // Per-descriptor backoff after a move was refused because the moving range still held unsettled
    // durable intents. Unlike the indivisibility guard, which refuses a range whose shape cannot be
    // improved, this refusal says "not right now": the range is being written and its coordinators
    // have not all decided. Without a backoff the checker re-attempts on the very next pass, and each
    // attempt takes the quiesce and refuses writes into the moving half for the whole drain window —
    // so a range that is busy enough to be worth splitting pays that cost every pass while making no
    // progress. The delay doubles per consecutive refusal so a range that stays busy is retried
    // rarely, and it is cleared the moment a split succeeds.
    private readonly ConcurrentDictionary<int, DrainRefusal> drainRefusedAt = new();

    /// <summary>When a descriptor's drain was last refused, and how many times in a row.</summary>
    private readonly record struct DrainRefusal(long Tick, int Consecutive);

    /// <summary>Delay after the first drain refusal; doubles per consecutive refusal.</summary>
    private readonly TimeSpan drainRefusalBackoff;

    /// <summary>Ceiling for the doubled delay.</summary>
    private readonly TimeSpan drainRefusalBackoffMax;

    // Serializes the allocate + create-partition + split-async region across every split entry
    // point. RangeSplitCheckerActor (count, ~60s), RangeSplitLoadCheckerActor (load, ~5s) and the
    // manual admin split all reach it, the first two as separate Nixie actors with separate
    // mailboxes. Nothing reserves a partition ID until CreatePartitionAsync commits, so two
    // concurrent branches can allocate the same ID; the loser then fails its split and its orphan
    // cleanup would retire the winner's live partition. The semaphore eliminates that.
    private readonly SemaphoreSlim splitLock;

    /// <summary>True when this instance created <see cref="splitLock"/> and must dispose it.</summary>
    private readonly bool ownsSplitLock;

    private readonly ILogger<IKahuna> logger;

    /// <param name="splitGate">Gate to serialize splits on. Pass the long-lived trigger's
    /// <see cref="SplitGate"/> when constructing a short-lived trigger against the same node, so
    /// both allocate under one lock; null gives this instance its own.</param>
    public RangeSplitTrigger(
        IRaft raft,
        RangeMapStore rangeMapStore,
        RangeSplitter splitter,
        KeyValuesManager manager,
        KeyWriteFrequencyRegistry writeFrequencyRegistry,
        KahunaConfiguration configuration,
        ILogger<IKahuna> logger,
        SemaphoreSlim? splitGate = null)
    {
        this.splitLock              = splitGate ?? new SemaphoreSlim(1, 1);
        this.ownsSplitLock          = splitGate is null;
        this.raft                   = raft;
        this.rangeMapStore          = rangeMapStore;
        this.splitter               = splitter;
        this.manager                = manager;
        this.writeFrequencyRegistry = writeFrequencyRegistry;
        this.threshold              = configuration.RangeSplitThreshold;
        this.minRangeSize           = configuration.RangeSplitMinRangeSize;
        this.loadThreshold          = configuration.RangeSplitLoadThreshold;
        this.loadMinQueueDepth      = configuration.RangeSplitLoadMinQueueDepth;
        this.loadMinCommitWaitMs    = configuration.RangeSplitLoadMinCommitWaitMs;
        this.loadWindow             = configuration.RangeSplitLoadWindow;
        this.loadImbalanceMax       = configuration.RangeSplitLoadImbalanceMax;
        this.indivisibleCooldown    = configuration.RangeSplitIndivisibleCooldown;
        // One checker pass is the natural unit: the first refusal costs the range its next pass, and
        // the ceiling matches the indivisibility cooldown so no cooldown here outlives that one.
        // Both are derived, so a drain refusal needs no configuration of its own.
        this.drainRefusalBackoff    = configuration.CollectionInterval;
        this.drainRefusalBackoffMax = configuration.RangeSplitIndivisibleCooldown;
        this.settleWindow           = configuration.RangeSplitSettleWindow;
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
        if (!await raft.AmILeaderIfHosted(RangeMapStore.MetaPartitionId, ct))
        {
            // Clear all trigger-local state symmetrically with LoadCheckAsync, so a re-promotion
            // starts clean. In count-only mode (loadThreshold == 0) the load checker may never run,
            // making this the only path that resets these maps on leadership loss.
            hotSince.Clear();
            settledAt.Clear();
            indivisibleAt.Clear();
            drainRefusedAt.Clear();
            return 0;
        }

        // Decay all write-frequency histograms once per checker pass so counts reflect recent
        // load rather than lifetime totals. This runs only on the meta-leader node (inside the
        // AmILeader gate above); follower replicas accumulate un-decayed lifetime counts. On a
        // meta-leader failover the new trigger node reads those lifetime-weighted counts until a
        // few passes erode them — the centroid lags recency briefly after failover, which is
        // acceptable given Decay()'s half-life convergence rate.
        foreach (KeyValuePair<int, KeyWriteFrequencyTracker> kv in writeFrequencyRegistry.All)
            kv.Value.Decay();

        RangeMap map = rangeMapStore.Current;

        // Prune expired and orphaned entries from the cooldown dictionaries before the threshold
        // gate — in load-only mode (threshold == 0) TriggerAsync returns early below and pruning
        // would never run otherwise, leaving settledAt/indivisibleAt growing unbounded per split.
        PruneExpiredCooldowns(map);

        // Count branch is disabled when threshold == 0. Return early but AFTER Decay() and
        // PruneExpiredCooldowns() so both run regardless of which branch is enabled.
        if (threshold <= 0)
            return 0;

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

                try
                {
                    // Skip descriptors that just split (settle window): a still-hot child must not
                    // re-split while its predecessor's leadership transfer is in flight.
                    if (IsInSettleWindow(descriptor.PartitionId))
                    {
                        RangeSplitMetrics.SettleSkips.Add(1, new KeyValuePair<string, object?>("keyspace", descriptor.KeySpace));
                        continue;
                    }

                    // Skip if the indivisibility guard refused this descriptor recently — avoids
                    // re-sampling 4096 keys every CollectionInterval for a persistently-skewed range.
                    if (IsInIndivisibleCooldown(descriptor.PartitionId))
                        continue;

                    // Skip if the moving half could not be drained recently. The range is simply
                    // busy, so re-attempting now would re-quiesce it and refuse its writes again for
                    // nothing.
                    if (IsInDrainRefusalBackoff(descriptor.PartitionId))
                        continue;

                    // Count branch: split when sampled key count >= threshold.
                    string? splitKey = await TryComputeSplitKeyAsync(descriptor, threshold, ct);
                    if (splitKey is null)
                        continue;

                    if (await ExecuteSplitAsync(descriptor, splitKey, ct))
                        splitsDone++;
                }
                catch (OperationCanceledException)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    // One descriptor's failure (a dropped or oversized gather, a leadership change
                    // mid-split) must not end the pass: every other range still gets evaluated.
                    logger.LogError(ex,
                        "RangeSplitTrigger: split evaluation failed for {Space} P{Partition}; continuing with the remaining ranges",
                        descriptor.KeySpace, descriptor.PartitionId);
                }
            }
        }

        return splitsDone;
    }

    /// <summary>
    /// Fast-path load poll. Evaluates the load predicate for every KeyRange descriptor
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

        if (!await raft.AmILeaderIfHosted(RangeMapStore.MetaPartitionId, ct))
        {
            // Lost meta-leadership — reset all debounce and cooldown state so a future promotion
            // starts clean and doesn't inherit timestamps from the previous leadership tenure.
            hotSince.Clear();
            settledAt.Clear();
            return 0;
        }

        RangeMap map = rangeMapStore.Current;
        long now = Stopwatch.GetTimestamp();
        int splitsDone = 0;

        foreach (RangeDescriptor descriptor in map.Descriptors)
        {
            ct.ThrowIfCancellationRequested();

            int partitionId = descriptor.PartitionId;

            // Skip descriptors still within the post-split settle window.
            if (IsInSettleWindow(partitionId))
            {
                RangeSplitMetrics.SettleSkips.Add(1, new KeyValuePair<string, object?>("keyspace", descriptor.KeySpace));
                continue;
            }

            // Same backoff the count branch honours: a range whose drain was just refused is busy,
            // and the load branch polls far more often, so without this it would carry the whole
            // re-attempt cost on its own.
            if (IsInDrainRefusalBackoff(partitionId))
                continue;

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

            // Relief guard: a load split only redistributes pressure when the new child's leader
            // can land on a different node. Without a viable relocation target the split adds
            // Raft consensus overhead with zero throughput benefit — the parent and child both run
            // on the same saturated node. Skip when no peer has been heard from within the
            // debounce window (single-node cluster, or balancer disabled and all peers silent).
            // Reset the debounce so the check repeats after a full loadWindow rather than every
            // poll tick.
            //
            // Liveness proxy, not placement guarantee: GetActiveNodes > 0 confirms at least one
            // peer is alive, but does not guarantee the balancer will actually move the child
            // there (e.g. EnableLeaderBalancer = false with live peers — heartbeats pass the
            // guard, yet the split lands on the same node). That residual falls to the operator
            // enabling the leader balancer alongside RangeSplitLoadThreshold; this guard closes
            // only the clearest case (no live peers at all).
            if (raft.GetActiveNodes(loadWindow).Count == 0)
            {
                logger.LogRangeSplitTriggerLoadNoReliefTarget(descriptor.KeySpace, partitionId);
                RangeSplitMetrics.NoReliefSkips.Add(1, new KeyValuePair<string, object?>("keyspace", descriptor.KeySpace));
                hotSince.TryRemove(partitionId, out _);
                continue;
            }

            // Whether the attempt is indivisible, too small, fails, or succeeds, reset the debounce
            // so the load branch re-arms a full window instead of retrying on every poll interval.
            hotSince.TryRemove(partitionId, out _);

            try
            {
                // Load branch uses 2*minRangeSize as the effective threshold — a small-but-hot range
                // can split even if it is far below the count threshold.
                string? splitKey = await TryComputeSplitKeyAsync(descriptor, 2 * minRangeSize, ct);

                if (splitKey is null)
                    continue; // indivisible or too small — TryComputeSplitKeyAsync already logged

                if (await ExecuteSplitAsync(descriptor, splitKey, ct))
                    splitsDone++;
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                // One descriptor's failure must not end the poll: every other hot range still gets
                // evaluated, and the debounce reset above rate-limits the retry to one per window.
                logger.LogError(ex,
                    "RangeSplitTrigger: load split evaluation failed for {Space} P{Partition}; continuing with the remaining ranges",
                    descriptor.KeySpace, partitionId);
            }
        }

        return splitsDone;
    }

    // ── private helpers ──────────────────────────────────────────────────────

    /// <summary>
    /// Executes the allocate + create-partition + split-async step for <paramref name="descriptor"/>
    /// at <paramref name="splitKey"/>, serialized via <see cref="splitLock"/> so the count cadence,
    /// the load cadence and the manual admin split cannot race on the same allocation.
    /// </summary>
    /// <returns><c>true</c> if the split succeeded.</returns>
    /// <remarks>
    /// Internal (not private) so regression tests can drive the create-split-cleanup path directly
    /// with a crafted descriptor — exercising the stale-descriptor guard and the orphan-cleanup
    /// branch, which the normal trigger cadences only reach under a hard-to-reproduce race.
    /// </remarks>
    internal async Task<bool> ExecuteSplitAsync(RangeDescriptor descriptor, string splitKey, CancellationToken ct)
    {
        logger.LogRangeSplitTriggerSplitting(descriptor.KeySpace, descriptor.StartKey ?? "−∞", descriptor.EndKey ?? "+∞", splitKey);

        SplitOutcome outcome = await ExecuteSplitCoreAsync(
            descriptor.KeySpace,
            splitKey,
            // Stale-descriptor guard: if another branch already split this range its generation
            // has advanced in the live map. Bail out before issuing CreatePartitionAsync.
            freshMap =>
            {
                RangeDescriptor? live = freshMap.Descriptors.FirstOrDefault(d => d.PartitionId == descriptor.PartitionId);
                if (live is not null && live.Generation == descriptor.Generation)
                    return true;

                logger.LogRangeSplitTriggerDescriptorStale(descriptor.KeySpace, descriptor.PartitionId);
                return false;
            },
            duringQuiesce: null,
            ct);

        if (!outcome.IsSuccess)
        {
            // A refused drain is the one failure that is expected to repeat: the range is being
            // written and its coordinators have not all decided. Back off before re-attempting, so
            // the next pass does not re-quiesce it and refuse its writes again for nothing.
            if (outcome.Status == SplitStatus.UnsettledMovingIntents)
                RecordDrainRefusal(descriptor);

            return false;
        }

        int newId = outcome.NewPartitionId;

        // Bookkeeping runs after the split gate is released. A cadence that grabs the gate in that
        // window and re-picks this descriptor is still stopped by the stale-descriptor preflight —
        // the cutover bumped the parent's generation — so the settle window does not have to be
        // recorded under the lock to be effective.

        // Transfer write-frequency histogram to the two child ranges.
        TransferTrackerOnSplit(descriptor, splitKey, newId);

        // Record settle-window timestamps for both children: the left child
        // inherits the parent partition ID; the right child gets newId. Neither
        // will be re-evaluated until settleWindow elapses.
        long splitTick = Stopwatch.GetTimestamp();
        settledAt[descriptor.PartitionId] = splitTick;
        settledAt[newId]                  = splitTick;

        // Reset load-branch debounce and indivisibility cooldown for the left child
        // (which inherits the parent partition ID) — the split changed the situation.
        hotSince.TryRemove(descriptor.PartitionId, out _);
        indivisibleAt.TryRemove(descriptor.PartitionId, out _);
        drainRefusedAt.TryRemove(descriptor.PartitionId, out _);

        RangeSplitMetrics.Splits.Add(1, new KeyValuePair<string, object?>("keyspace", descriptor.KeySpace));

        return true;
    }

    /// <summary>
    /// The gate serializing this node's splits. Hand it to a short-lived trigger built against the
    /// same node so both allocate partition IDs under one lock.
    /// </summary>
    internal SemaphoreSlim SplitGate => splitLock;

    /// <summary>
    /// Splits the range covering <paramref name="splitKey"/> at that exact key, bypassing every
    /// threshold — the operator-driven split. Shares <see cref="splitLock"/> with the automatic
    /// cadences: both allocate a partition ID and create it, and nothing reserves an ID until
    /// <c>CreatePartitionAsync</c> commits, so running them concurrently would let two branches
    /// allocate the same ID and let the losing branch's cleanup retire the winner's live partition.
    /// </summary>
    /// <param name="duringQuiesce">Invoked inside the quiesce window (after catch-up import, before
    /// cutover) so tests can drive races against a split in progress.</param>
    internal Task<SplitOutcome> ExecuteSplitAtKeyAsync(
        string keySpace,
        string splitKey,
        Func<Task>? duringQuiesce,
        CancellationToken ct) =>
        ExecuteSplitCoreAsync(keySpace, splitKey, preflight: null, duringQuiesce, ct);

    /// <summary>
    /// The serialized allocate → create → split → cleanup region shared by every split entry point.
    /// <paramref name="preflight"/> runs under the lock against the freshly read map and aborts the
    /// split when it returns <c>false</c>.
    /// </summary>
    private async Task<SplitOutcome> ExecuteSplitCoreAsync(
        string keySpace,
        string splitKey,
        Func<RangeMap, bool>? preflight,
        Func<Task>? duringQuiesce,
        CancellationToken ct)
    {
        await splitLock.WaitAsync(ct);
        try
        {
            // Re-read the map snapshot inside the lock so the allocation sees any descriptor
            // committed by a split that beat us here.
            RangeMap freshMap = rangeMapStore.Current;

            if (preflight is not null && !preflight(freshMap))
                return SplitOutcome.ConcurrentSplit;

            int newId = RangeSplitter.ComputeNextPartitionId(raft, freshMap);

            RaftPartitionLifecycleResult createResult;
            try
            {
                createResult = await raft.CreatePartitionAsync(newId, RaftRoutingMode.Unrouted, null, ct);
            }
            catch (RaftException ex)
            {
                // CreatePartitionAsync throws when this node lost system-partition leadership in
                // the window between the AmILeader(0) check and here. Treat as a clean skip —
                // the next checker tick will re-evaluate with a fresh AmILeader check.
                logger.LogRangeSplitTriggerCreateFailed(newId, keySpace);
                logger.LogRangeSplitTriggerCreateThrew(ex, newId);
                return SplitOutcome.PartitionCreationFailed;
            }

            if (!createResult.Success)
            {
                logger.LogRangeSplitTriggerCreateFailed(newId, keySpace);
                return SplitOutcome.PartitionCreationFailed;
            }

            SplitOutcome outcome = await splitter.SplitAsync(keySpace, splitKey, newId, duringQuiesce, ct);

            if (!outcome.IsSuccess)
            {
                logger.LogRangeSplitTriggerSplitFailed(keySpace, splitKey, outcome.Status.ToString());
                await RemoveOrphanedPartitionAsync(newId, ct);
                return outcome;
            }

            logger.LogRangeSplitTriggerSplit(keySpace, splitKey, newId);

            return outcome;
        }
        finally
        {
            splitLock.Release();
        }
    }

    /// <summary>
    /// Best-effort removal of the partition created for a split that then failed, so it is not left
    /// permanently orphaned (unreferenced by routing, and its ID retired for good).
    /// <para>
    /// Skipped when the committed map does carry a descriptor on that partition: a cutover can
    /// commit and still report failure (a replication timeout on an entry that lands anyway), and
    /// removing the partition then would retire a range that is serving live data.
    /// </para>
    /// <para>
    /// Failures are logged, not thrown — the removal is retried inside Kommander, and a rejection
    /// that still surfaces here (lost system leadership, terminal error) cannot be repaired from
    /// this node, so it must at least be visible.
    /// </para>
    /// </summary>
    private async Task RemoveOrphanedPartitionAsync(int partitionId, CancellationToken ct)
    {
        foreach (RangeDescriptor descriptor in rangeMapStore.Current.Descriptors)
        {
            if (descriptor.PartitionId != partitionId)
                continue;

            logger.LogRangeSplitTriggerOrphanRemoveSkipped(
                partitionId, descriptor.KeySpace, descriptor.StartKey ?? "−∞", descriptor.EndKey ?? "+∞");

            return;
        }

        try
        {
            RaftPartitionLifecycleResult removeResult = await raft.RemovePartitionAsync(partitionId, ct);

            if (!removeResult.Success)
                logger.LogRangeSplitTriggerOrphanRemoveRejected(partitionId, removeResult.Status.ToString());
        }
        catch (Exception ex)
        {
            logger.LogRangeSplitTriggerOrphanRemoveFailed(partitionId, ex);
        }
    }

    /// <summary>
    /// Returns <c>true</c> when <paramref name="partitionId"/> is still within the post-split
    /// settle window, meaning neither branch should re-evaluate it yet.
    /// </summary>
    private bool IsInSettleWindow(int partitionId)
    {
        if (settleWindow <= TimeSpan.Zero)
            return false;

        if (!settledAt.TryGetValue(partitionId, out long splitTick))
            return false;

        double elapsedMs = (Stopwatch.GetTimestamp() - splitTick) * 1000.0 / Stopwatch.Frequency;
        return elapsedMs < settleWindow.TotalMilliseconds;
    }

    /// <summary>
    /// Removes expired and partition-no-longer-exists entries from <see cref="settledAt"/>,
    /// <see cref="indivisibleAt"/>, and <see cref="hotSince"/>. Called once per slow-cadence
    /// <see cref="TriggerAsync"/> pass to bound dict growth within a leadership tenure.
    /// </summary>
    private void PruneExpiredCooldowns(RangeMap map)
    {
        HashSet<int> activeIds = map.Descriptors.Select(d => d.PartitionId).ToHashSet();
        long now = Stopwatch.GetTimestamp();
        double settleMs      = settleWindow.TotalMilliseconds;
        double cooldownMs    = indivisibleCooldown.TotalMilliseconds;

        foreach (int id in settledAt.Keys)
        {
            if (!activeIds.Contains(id) ||
                (settledAt.TryGetValue(id, out long tick) &&
                 (now - tick) * 1000.0 / Stopwatch.Frequency >= settleMs))
                settledAt.TryRemove(id, out _);
        }

        foreach (int id in indivisibleAt.Keys)
        {
            if (!activeIds.Contains(id) ||
                (indivisibleAt.TryGetValue(id, out long tick) &&
                 (now - tick) * 1000.0 / Stopwatch.Frequency >= cooldownMs))
                indivisibleAt.TryRemove(id, out _);
        }

        foreach (int id in hotSince.Keys)
        {
            if (!activeIds.Contains(id))
                hotSince.TryRemove(id, out _);
        }

        foreach (int id in drainRefusedAt.Keys)
        {
            // Only a descriptor that no longer exists is dropped here. Removing an entry once its
            // delay expired would discard the consecutive count with it, so the next refusal would
            // start again at one delay and the doubling could never get past the second step — the
            // range would be re-attempted every other pass forever, which is most of what the
            // backoff exists to prevent. The count is cleared where it should be: on a split that
            // succeeds, and on leadership loss. Growth stays bounded by the descriptor count.
            if (!activeIds.Contains(id))
                drainRefusedAt.TryRemove(id, out _);
        }
    }

    /// <summary>
    /// Returns <c>true</c> when the descriptor's last indivisibility refusal is still within the
    /// <see cref="indivisibleCooldown"/> window, meaning the count branch should skip sampling.
    /// </summary>
    private bool IsInIndivisibleCooldown(int partitionId)
    {
        if (!indivisibleAt.TryGetValue(partitionId, out long refusedTick))
            return false;

        double elapsedMs = (Stopwatch.GetTimestamp() - refusedTick) * 1000.0 / Stopwatch.Frequency;
        return elapsedMs < indivisibleCooldown.TotalMilliseconds;
    }

    /// <summary>
    /// Records that a move of <paramref name="descriptor"/> was refused because its moving half still
    /// held unsettled durable intents, and lengthens the backoff for a range that keeps refusing.
    /// </summary>
    private void RecordDrainRefusal(RangeDescriptor descriptor)
    {
        long nowTick = Stopwatch.GetTimestamp();
        DrainRefusal refusal = drainRefusedAt.AddOrUpdate(
            descriptor.PartitionId,
            _ => new DrainRefusal(nowTick, 1),
            (_, previous) => new DrainRefusal(
                nowTick,
                NextConsecutive(
                    previous.Consecutive,
                    (nowTick - previous.Tick) * 1000.0 / Stopwatch.Frequency,
                    drainRefusalBackoffMax.TotalMilliseconds)));

        RangeSplitMetrics.DrainRefusals.Add(1, new KeyValuePair<string, object?>("keyspace", descriptor.KeySpace));

        double backoffMs = DrainBackoffMsFor(refusal.Consecutive);
        logger.LogRangeSplitTriggerDrainRefused(
            descriptor.KeySpace, descriptor.PartitionId, refusal.Consecutive, backoffMs);
    }

    /// <summary>
    /// The backoff after <paramref name="consecutive"/> refusals in a row: the base delay doubled
    /// once per extra refusal, capped. The shift is bounded before it is applied — a range that has
    /// refused thirty times would otherwise overflow the multiplier rather than saturate it.
    /// </summary>
    private double DrainBackoffMsFor(int consecutive) => ComputeDrainBackoffMs(
        drainRefusalBackoff.TotalMilliseconds, drainRefusalBackoffMax.TotalMilliseconds, consecutive);

    /// <summary>
    /// The streak value for a fresh refusal on a descriptor that has refused before. A refusal that
    /// follows the previous one closely continues the streak, so a range that keeps failing to drain
    /// is retried ever more rarely. One that arrives long after the last — more than twice the
    /// maximum delay, meaning the range drained or was left alone for that whole time — starts over,
    /// so a range is not punished for an episode that has since passed.
    /// </summary>
    internal static int NextConsecutive(int previousConsecutive, double elapsedSinceLastMs, double maxBackoffMs)
        => elapsedSinceLastMs > 2 * maxBackoffMs ? 1 : previousConsecutive + 1;

    /// <summary>
    /// The doubling itself, separated from the trigger's state so it can be checked directly.
    /// <paramref name="consecutive"/> counts refusals in a row and is 1 for the first one.
    /// </summary>
    internal static double ComputeDrainBackoffMs(double baseMs, double maxMs, int consecutive)
    {
        double ceiling = Math.Max(baseMs, maxMs);
        int doublings = Math.Clamp(consecutive - 1, 0, 20);
        return Math.Min(baseMs * (1L << doublings), ceiling);
    }

    /// <summary>
    /// Returns <c>true</c> while a descriptor whose drain was refused is still inside its backoff,
    /// meaning neither branch should attempt it again yet.
    /// </summary>
    private bool IsInDrainRefusalBackoff(int partitionId)
    {
        if (!drainRefusedAt.TryGetValue(partitionId, out DrainRefusal refusal))
            return false;

        double elapsedMs = (Stopwatch.GetTimestamp() - refusal.Tick) * 1000.0 / Stopwatch.Frequency;
        return elapsedMs < DrainBackoffMsFor(refusal.Consecutive);
    }

    /// <summary>
    /// Returns <c>true</c> when partition <paramref name="partitionId"/> satisfies the
    /// load AND-predicate: rate ≥ threshold AND WAL queue depth ≥ min AND (commit-wait gate
    /// disabled OR commit-wait ≥ configured max). All values come from the gossiped load
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
        if (loadMinCommitWaitMs > 0 && commitWait < loadMinCommitWaitMs)
            return false;

        return true;
    }

    /// <summary>
    /// Samples <paramref name="descriptor"/>'s key range and returns a split key when the
    /// sample size meets <paramref name="effectiveThreshold"/>, or <c>null</c> if no split
    /// is warranted. Applies the indivisibility guard when a write-frequency histogram
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

        // Look up the write-frequency snapshot for this partition.
        // The snapshot may be empty (cold histogram — post-failover blind window or first run);
        // ComputeSplitKey falls back to the count-based median/percentile path transparently.
        IReadOnlyDictionary<string, long>? writeFreq =
            writeFrequencyRegistry.TryGet(descriptor.PartitionId)?.GetSnapshot();

        string? splitKey = RangeSplitPolicy.ComputeSplitKey(sample, effectiveThreshold, minRangeSize, writeFreq, out double achievableImbalance);

        // Indivisibility guard: refuse the split when the best achievable write-centroid
        // still concentrates too many writes on one child (e.g. all traffic on a single hot key).
        // Applies to both count and load branches whenever loadImbalanceMax is configured.
        // achievableImbalance == 0 means the histogram was cold and the count path was used —
        // in that case the guard does not apply.
        if (splitKey is not null && loadImbalanceMax > 0 && achievableImbalance > 0 && achievableImbalance >= loadImbalanceMax)
        {
            logger.LogRangeSplitTriggerIndivisible(descriptor.KeySpace, descriptor.PartitionId, achievableImbalance, loadImbalanceMax);
            indivisibleAt[descriptor.PartitionId] = Stopwatch.GetTimestamp();
            RangeSplitMetrics.IndivisibleRefusals.Add(1, new KeyValuePair<string, object?>("keyspace", descriptor.KeySpace));
            return null;
        }

        return splitKey;
    }

    // ── split-tracker transfer ───────────────────────────────────────────────

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

    public void Dispose()
    {
        if (ownsSplitLock)
            splitLock.Dispose();
    }
}
