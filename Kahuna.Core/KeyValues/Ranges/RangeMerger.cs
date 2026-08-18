
using Kommander;
using Kommander.Time;

using Kahuna.Server.KeyValues.Logging;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Ranges;

/// <summary>
/// Executes the key-range merge transaction:
/// <c>[A,B)@P1 + [B,C)@P2</c> → <c>[A,C)@P1</c>, retiring P2.
///
/// <para>
/// <b>Step sequence:</b>
/// <list type="number">
///   <item>Validate adjacency: <c>left.EndKey == right.StartKey</c>.</item>
///   <item>Bulk transfer: export <c>[B,C)</c> at a fixed MVCC snapshot, import into the
///       survivor (P1). Because the persistence backend is node-global the import is a no-op for
///       correctness today, but the export/import path keeps the merge symmetric with the split and
///       prepares for future partition-scoped storage.</item>
///   <item>Atomic cutover: <see cref="RangeMapStore.MutateAsync"/> replaces <c>{left, right}</c>
///       with <c>[A,C)@P1 gen+1</c>.</item>
///   <item>Return <see cref="MergeOutcome"/> carrying the retired partition ID. The caller must
///       call <see cref="IRaft.RemovePartitionAsync"/> from the system-partition (0) leader.</item>
/// </list>
/// </para>
///
/// <para>
/// <b>No quiesce lock.</b> The split uses a range lock to quiesce 2PC commits during the
/// catch-up window. The merge does not, because
/// <c>LocateAndTryAcquireExclusiveRangeLock</c> rejects prefix ops over split keyspaces (the
/// partial-result safety guard). With today's node-global persistence
/// backend, a write to <c>[B,C)</c> that commits after the snapshot and before the cutover is
/// still visible on P1 (P1 and P2 share the same store; the export/import is a no-op as noted
/// above). The correctness gap only materialises under future partition-scoped storage.
/// Accepting the current gap is reasonable for under-min ranges (very few keys, low load); full
/// per-descriptor lock support is deferred.
/// </para>
///
/// <para>
/// <b>Caller constraint.</b> <see cref="MergeAsync"/> must be called on the node that is the
/// <b>meta-partition (1) leader</b>, because <see cref="RangeMapStore.MutateAsync"/> requires it.
/// After a successful merge the caller must call
/// <see cref="IRaft.RemovePartitionAsync(int, CancellationToken)"/> on the
/// <b>system-partition (0) leader</b> to retire P2.
/// </para>
/// </summary>
internal sealed class RangeMerger
{
    /// <summary>Page size for key-count sampling (also used for export paging).</summary>
    private const int CountPageSize = 512;

    private readonly IRaft raft;
    private readonly RangeMapStore rangeMapStore;
    private readonly KeyValuesManager manager;
    private readonly ILogger<IKahuna> logger;

    public RangeMerger(
        IRaft raft,
        RangeMapStore rangeMapStore,
        KeyValuesManager manager,
        ILogger<IKahuna> logger)
    {
        this.raft          = raft;
        this.rangeMapStore = rangeMapStore;
        this.manager       = manager;
        this.logger        = logger;
    }

    /// <summary>
    /// Merges two adjacent under-min ranges into the left (survivor) partition.
    /// Must be called on the <b>meta-partition (1) leader</b>.
    ///
    /// <para>
    /// On success, <see cref="MergeOutcome.RetiredPartitionId"/> is the ID of the retired right
    /// partition. The caller must call <see cref="IRaft.RemovePartitionAsync"/> for that ID from
    /// the system-partition (0) leader.
    /// </para>
    /// </summary>
    public async Task<MergeOutcome> MergeAsync(
        string keySpace,
        RangeDescriptor left,
        RangeDescriptor right,
        CancellationToken ct = default)
    {
        // -- 1. Validate adjacency -----------------------------------------------
        if (left.EndKey is null || right.StartKey is null ||
            string.CompareOrdinal(left.EndKey, right.StartKey) != 0)
        {
            logger.LogWarning(
                "RangeMerger: non-adjacent descriptors [{LS},{LE}) + [{RS},{RE})",
                left.StartKey ?? "-inf", left.EndKey ?? "+inf",
                right.StartKey ?? "-inf", right.EndKey ?? "+inf");
            return MergeOutcome.NotAdjacent;
        }

        logger.LogRangeMergerMerging(keySpace, left.StartKey ?? "-inf", left.EndKey, left.PartitionId, right.StartKey, right.EndKey ?? "+inf", right.PartitionId);

        // -- 2. Bulk copy [B,C) at snapshotTs -> survivor -------------------------
        // Routed through partition leaders: the right range is paged via the locator and every
        // page is replicated onto the survivor's Raft log, so the copy is correct even when this
        // node hosts neither side. Under legacy full replication the copy degenerates to the
        // historical local export/import (a physical no-op on a node-global backend).
        HLCTimestamp snapshotTs = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        bool placedRight = raft.GetPartitionReplicas(right.PartitionId).Count > 0;

        try
        {
            if (!await manager.CopyRangeToPartitionAsync(
                    keySpace, right.StartKey, right.EndKey, snapshotTs, right.PartitionId, left.PartitionId,
                    HLCTimestamp.Zero, ct))
            {
                logger.LogError(
                    "RangeMerger: bulk copy failed for {Space} [{Start},{End})",
                    keySpace, right.StartKey, right.EndKey ?? "+inf");
                return MergeOutcome.TransferFailed;
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex,
                "RangeMerger: bulk copy failed for {Space} [{Start},{End})",
                keySpace, right.StartKey, right.EndKey ?? "+inf");
            return MergeOutcome.TransferFailed;
        }

        // -- 2b. Transfer range locks: clamp right's live locks, inject into left leader --
        // Locks are actor-local (not Raft-replicated). Read from the right partition leader and
        // inject into the left (survivor) leader before cutover so writes to [B,C) routed to
        // the survivor after the merge are still blocked by any live range locks.
        // The pre-cutover import is followed by a post-cutover confirm-and-reimport loop
        // (EnsureLocksOnDestinationLeaderAsync) to handle a left-leadership change during the window.
        // Range locks are actor-local (not Raft-replicated); their transfer stays best-effort and is hardened
        // after cutover by the confirm-and-reimport loop. Correctness metadata (receipts, decisions) is not
        // best-effort — see below.
        List<KeyValueRangeLock> clampedLocks = [];

        try
        {
            HLCTimestamp lockNow = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

            List<KeyValueRangeLock> rightLocks = await manager.GetRangeLocksFromPartitionLeaderAsync(
                keySpace, right.PartitionId, ct);

            clampedLocks = KvStateMachineTransfer.FilterAndClamp(
                rightLocks, right.StartKey, right.EndKey, lockNow);

            if (clampedLocks.Count > 0)
                await manager.ImportRangeLocksToPartitionLeaderAsync(keySpace, left.PartitionId, clampedLocks, ct);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex,
                "RangeMerger: range-lock transfer from P{Right} to P{Left} failed (best-effort; merge continues)",
                right.PartitionId, left.PartitionId);
            clampedLocks = [];
        }

        // Replicate the moved range's completion receipts, canonical transaction records, and prepared intents
        // onto the survivor's Raft log so every replica of P1 holds them — a re-commit, re-drive, recovery, or
        // finalize routed to the survivor after cutover resolves correctly even if P1's leader changes. Unlike the
        // range-lock transfer this is NOT best-effort: the survivor becomes the sole route for [B,C) at cutover and
        // P2 is retired, so a lost receipt, decision, or unresolved intent would be lost for good. A non-durable
        // handoff aborts the merge before cutover.
        try
        {
            // With a committed replica set on the right range the gather must read its leader's
            // stores (this node may hold none of them); under legacy full replication it reads
            // this node's stores, which every replica of the right group populated.
            IReadOnlyCollection<CompletionReceiptRecord> movedReceipts;
            IReadOnlyList<Kahuna.Server.KeyValues.Transactions.Data.TransactionRecord> movedRecords;
            IReadOnlyList<Kahuna.Server.KeyValues.Transactions.Data.PreparedIntent> movedIntents;

            if (placedRight)
            {
                bool gathered;
                (gathered, movedReceipts, movedRecords, movedIntents) =
                    await manager.GetRangeTransactionStateFromPartitionLeaderAsync(
                        right.PartitionId, right.StartKey, right.EndKey, ct);

                if (!gathered)
                {
                    logger.LogError(
                        "RangeMerger: could not gather the moving range's transaction state from P{Right}'s leader — aborting merge before cutover",
                        right.PartitionId);
                    return MergeOutcome.TransferFailed;
                }
            }
            else
            {
                movedReceipts = manager.GetLocalCompletionReceiptsForRange(right.StartKey, right.EndKey);
                movedRecords = manager.GetLocalTransactionRecordsForRange(right.StartKey, right.EndKey);
                movedIntents = manager.GetLocalPreparedIntentsForRange(right.StartKey, right.EndKey);
            }

            if (!await manager.ImportCompletionReceiptsToPartitionLeaderAsync(left.PartitionId, movedReceipts, ct))
            {
                logger.LogError(
                    "RangeMerger: completion-receipt handoff to P{Left} not durable — aborting merge before cutover", left.PartitionId);
                return MergeOutcome.TransferFailed;
            }

            if (!await manager.ImportDurableTransactionStateToPartitionLeaderAsync(left.PartitionId, movedRecords, movedIntents, ct))
            {
                logger.LogError(
                    "RangeMerger: durable transaction-state handoff to P{Left} not durable — aborting merge before cutover", left.PartitionId);
                return MergeOutcome.TransferFailed;
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex,
                "RangeMerger: correctness-metadata handoff from P{Right} to P{Left} threw — aborting merge before cutover",
                right.PartitionId, left.PartitionId);
            return MergeOutcome.TransferFailed;
        }

        // -- 3. Atomic cutover ----------------------------------------------------
        // Replace {left, right} with [A,C)@P1 gen+1.
        long newGeneration = Math.Max(left.Generation, right.Generation) + 1;

        bool raceDetected = false;
        bool cutoverOk;

        try
        {
            cutoverOk = await rangeMapStore.MutateAsync(existing =>
            {
                // Race guard: both descriptors must still be at their expected generations.
                RangeDescriptor? liveLeft  = existing.FirstOrDefault(d =>
                    d.KeySpace == keySpace && d.PartitionId == left.PartitionId  && d.Generation == left.Generation);
                RangeDescriptor? liveRight = existing.FirstOrDefault(d =>
                    d.KeySpace == keySpace && d.PartitionId == right.PartitionId && d.Generation == right.Generation);

                if (liveLeft is null || liveRight is null)
                {
                    raceDetected = true;
                    return existing;
                }

                List<RangeDescriptor> next = existing
                    .Where(d => d != liveLeft && d != liveRight)
                    .ToList();

                // Merged range: [A,C) on the survivor (P1) with bumped generation.
                next.Add(new RangeDescriptor
                {
                    KeySpace    = keySpace,
                    StartKey    = left.StartKey,
                    EndKey      = right.EndKey,
                    PartitionId = left.PartitionId,
                    Generation  = newGeneration
                });

                return next;
            }, ct);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "RangeMerger: MutateAsync threw during cutover");
            return MergeOutcome.CutoverFailed;
        }

        if (raceDetected)
        {
            logger.LogWarning("RangeMerger: concurrent descriptor change detected on {Space}", keySpace);
            return MergeOutcome.ConcurrentChange;
        }

        if (!cutoverOk)
        {
            logger.LogError("RangeMerger: MutateAsync cutover failed (not leader or validation rejected)");
            return MergeOutcome.CutoverFailed;
        }

        // -- 3b. Post-cutover confirm-and-reimport (best-effort hardening) ----------------------
        // After cutover the left partition is the authoritative route for [B,C). A left-leadership
        // change during the pre-cutover window can strand the imported locks on a former leader.
        // Re-read the current left leader's LocksByRange and re-import any missing entries.
        if (clampedLocks.Count > 0)
            await KvStateMachineTransfer.EnsureLocksOnDestinationLeaderAsync(
                manager, keySpace, left.PartitionId, clampedLocks, logger, "RangeMerger", ct);

        logger.LogRangeMergerMerged(keySpace, left.StartKey ?? "-inf", right.EndKey ?? "+inf", left.PartitionId, newGeneration, right.PartitionId);

        return new MergeOutcome(MergeStatus.Succeeded, right.PartitionId, newGeneration);
    }

    // -- helpers ------------------------------------------------------------------

    /// <summary>
    /// Counts keys in the given descriptor's range by paging range reads, stopping early once
    /// <paramref name="maxCount"/> keys have been found. Used to decide whether a range is an
    /// under-min merge candidate. With a committed replica set the pages read through the locator
    /// (the range's leader) — this node may not host the range, and a local read would answer an
    /// empty count that wrongly qualifies a populated range for merging.
    ///
    /// <para>Returns whether the count is <c>Complete</c>. A page that could not be served ends the walk
    /// with whatever was counted so far, and that partial total says nothing about the range's size — a
    /// busy range whose first page is refused (a live transactional write in the window makes the whole
    /// page retryable) would otherwise count as under-min and be merged. Merging moves data, so an
    /// incomplete count must never decide it.</para>
    /// </summary>
    internal async Task<(int Count, bool Complete)> CountRangeKeysAsync(
        RangeDescriptor descriptor,
        int maxCount,
        CancellationToken ct = default)
    {
        int count      = 0;
        string? cursor = null;
        bool hasMore   = true;

        bool routeThroughLeader = raft.GetPartitionReplicas(descriptor.PartitionId).Count > 0;

        while (hasMore && count < maxCount)
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
                pageStartInclusive = false;
            }

            KeyValueGetByRangeResult page = routeThroughLeader
                ? await manager.LocateAndGetByRange(
                    HLCTimestamp.Zero,
                    descriptor.KeySpace,
                    pageStart,
                    pageStartInclusive,
                    descriptor.EndKey,
                    false,
                    Math.Min(CountPageSize, maxCount - count),
                    HLCTimestamp.Zero,
                    KeyValueDurability.Persistent,
                    ct)
                : await manager.GetByRange(
                    HLCTimestamp.Zero,
                    descriptor.KeySpace,
                    pageStart,
                    pageStartInclusive,
                    descriptor.EndKey,
                    false,
                    Math.Min(CountPageSize, maxCount - count),
                    HLCTimestamp.Zero,
                    KeyValueDurability.Persistent);

            // A refused page (anything but Get) leaves the count unfinished; an empty Get page is the
            // genuine end of the range.
            if (page.Type != KeyValueResponseType.Get)
                return (count, false);

            if (page.Items.Count == 0)
                break;

            count  += page.Items.Count;
            cursor  = page.Items[^1].Item1;
            hasMore = page.HasMore;
        }

        return (count, true);
    }

    /// <summary>
    /// Returns non-overlapping adjacent descriptor pairs within <paramref name="keySpace"/>
    /// where both descriptors have fewer than <paramref name="minMergeSize"/> keys.
    ///
    /// <para>
    /// <b>Non-overlapping guarantee.</b> For three consecutive under-min ranges A, B, C a naive
    /// scan would return both (A,B) and (B,C). Merging (A,B) retires B; the subsequent
    /// <see cref="MergeAsync"/> for (B,C) would then hit <see cref="MergeStatus.ConcurrentChange"/>
    /// because B no longer exists in the descriptor map. To avoid this wasted work the selection
    /// is greedy: once a descriptor is chosen as the <c>right</c> of a pair, the scan advances
    /// past it so it cannot also be the <c>left</c> of the next pair. In the A-B-C example only
    /// (A,B) is returned; C is re-evaluated on the next periodic tick.
    /// </para>
    /// </summary>
    internal async Task<List<(RangeDescriptor Left, RangeDescriptor Right)>> FindMergeCandidatesAsync(
        string keySpace,
        int minMergeSize,
        CancellationToken ct = default)
    {
        IReadOnlyList<RangeDescriptor> all = rangeMapStore.Current.FindAll(keySpace);

        if (all.Count < 2)
            return [];

        // Count keys for each descriptor; cache to avoid double-counting adjacent pairs. A range whose
        // count could not be completed is not a candidate: an incomplete count is systematically low, so
        // treating it as a size would merge exactly the busy ranges that refused to be counted.
        var undersized = new bool[all.Count];
        for (int i = 0; i < all.Count; i++)
        {
            (int count, bool complete) = await CountRangeKeysAsync(all[i], minMergeSize, ct);
            undersized[i] = complete && count < minMergeSize;
        }

        var result = new List<(RangeDescriptor, RangeDescriptor)>();
        for (int i = 0; i + 1 < all.Count; i++)
        {
            if (undersized[i] && undersized[i + 1])
            {
                result.Add((all[i], all[i + 1]));
                i++; // skip i+1: it was consumed as "right"; re-evaluate it next tick
            }
        }

        return result;
    }
}

/// <summary>Terminal status for a <see cref="RangeMerger.MergeAsync"/> call.</summary>
internal enum MergeStatus
{
    Succeeded,
    NotAdjacent,
    TransferFailed,
    CutoverFailed,
    ConcurrentChange,
}

/// <summary>Result of <see cref="RangeMerger.MergeAsync"/>.</summary>
internal readonly struct MergeOutcome
{
    public MergeStatus Status             { get; }
    public int         RetiredPartitionId { get; }
    public long        NewGeneration      { get; }

    public MergeOutcome(MergeStatus status, int retiredPartitionId = 0, long newGeneration = 0)
    {
        Status             = status;
        RetiredPartitionId = retiredPartitionId;
        NewGeneration      = newGeneration;
    }

    public bool IsSuccess => Status == MergeStatus.Succeeded;

    public static MergeOutcome NotAdjacent    => new(MergeStatus.NotAdjacent);
    public static MergeOutcome TransferFailed => new(MergeStatus.TransferFailed);
    public static MergeOutcome CutoverFailed  => new(MergeStatus.CutoverFailed);
    public static MergeOutcome ConcurrentChange => new(MergeStatus.ConcurrentChange);
}
