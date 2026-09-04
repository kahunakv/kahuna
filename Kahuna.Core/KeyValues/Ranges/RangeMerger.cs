
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
///   <item>Quiesce window: acquire an exclusive range lock on <c>[B,C)</c> at P2's leader, then
///       publish the same window on the replicated descriptor.</item>
///   <item>Settle the moving range's decided intents, then copy <c>[B,C)</c> at a fixed MVCC
///       snapshot into the survivor (P1).</item>
///   <item>Atomic cutover: <see cref="RangeMapStore.MutateAsync"/> replaces <c>{left, right}</c>
///       with <c>[A,C)@P1 gen+1</c>, which also clears the quiesce.</item>
///   <item>Release the range lock on P2 by partition id, and return <see cref="MergeOutcome"/>
///       carrying the retired partition ID. The caller must call
///       <see cref="IRaft.RemovePartitionAsync"/> from the system-partition (0) leader.</item>
/// </list>
/// </para>
///
/// <para>
/// <b>Quiesce scope.</b> <c>[B,C)</c> must accept no write between the copy and the cutover: one
/// that commits on P2 in that window is absent from the copy and unreachable once <c>[B,C)</c>
/// routes to P1 and P2 retires — acknowledged to the client, then gone. Each data partition has its
/// own replica set and its own backend, so P1 cannot see it. Two guards cover the window, and they
/// are complementary rather than redundant. The <b>exclusive range lock</b> on <c>[B,C)</c> is
/// installed by a message on P2's leader's key-value actor — the same single-threaded mailbox every
/// write for that key space passes through — so it is exactly ordered against in-flight writes: one
/// admitted before the lock is already proposed, one after is refused. The <b>descriptor quiesce</b>
/// (<see cref="RangeDescriptor.QuiescedUntil"/>, published through
/// <see cref="RangeMapStore.QuiesceRangeAsync"/>) rides the replicated range map, so it reaches
/// every node and, unlike the lock, survives a leadership change on P2: a promoted leader holds no
/// in-memory lock but still refuses writes into <c>[B,C)</c>. A refused client retries after cutover
/// and is then routed to P1.
/// </para>
///
/// <para>
/// The window closes on three independent terms. A successful cutover replaces both descriptors,
/// which clears the quiesce atomically with the routing change; the <c>finally</c> releases it by
/// owner on every other path; and the deadline stamped on the descriptor lapses on its own, so a
/// merge executor that dies mid-window leaves a range that reopens instead of one that refuses
/// writes forever.
/// </para>
///
/// <para>
/// <b>One copy, not two.</b> The split copies in bulk first and catches up inside its window,
/// because it splits ranges that are large or hot by definition. A merge candidate is an under-min
/// range, so a single copy inside the window is enough and the window stays short.
/// </para>
///
/// <para>
/// <b>Orphan rows.</b> The <c>[B,C)</c> rows stay physically present on P2's replicas — they are not
/// deleted. Nothing routes to P2 after cutover and the partition is retired, so they affect nothing.
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

    /// <summary>
    /// How long the quiesce window may stay open (ms). Long enough to cover the settle and the copy
    /// of an under-min range, and short enough that a merge executor which dies mid-window costs the
    /// range a bounded stretch of refused writes. Applied to both guards — the range lock's TTL and
    /// the replicated descriptor deadline — so neither outlives the other and reopens the window
    /// while the merge still runs.
    /// </summary>
    private const int QuiesceTtlMs = 30_000;

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
    public Task<MergeOutcome> MergeAsync(
        string keySpace,
        RangeDescriptor left,
        RangeDescriptor right,
        CancellationToken ct = default) =>
        MergeAsync(keySpace, left, right, null, ct);

    /// <summary>
    /// Internal overload for tests: <paramref name="duringQuiesce"/> is invoked between the durable
    /// transaction-state handoff and the cutover commit, while the range is quiesced. It exists so a
    /// test can race an operation into that window, which is otherwise too short to hit deliberately.
    /// </summary>
    internal async Task<MergeOutcome> MergeAsync(
        string keySpace,
        RangeDescriptor left,
        RangeDescriptor right,
        Func<Task>? duringQuiesce,
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

        HLCTimestamp mergeTxId = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        // -- 1b. Refuse a pair another move is already moving ---------------------
        // Checked over the whole merged span, not just [B,C): the cutover destroys both descriptors,
        // so a split part-way through moving a piece of the left range would lose its destination.
        // Refusing costs nothing — the pair is re-evaluated on the next tick, by which time the other
        // move has finished or its deadline has lapsed.
        if (rangeMapStore.IsQuiescedByAnotherMove(keySpace, left.StartKey, right.EndKey, mergeTxId))
        {
            logger.LogWarning(
                "RangeMerger: [{Start},{End}) is being moved by another range move; refusing this merge attempt",
                left.StartKey ?? "-inf", right.EndKey ?? "+inf");
            return MergeOutcome.ConcurrentMove;
        }

        // -- 1c. Quiesce: write-fence range lock on [B,C) -------------------------
        // Route to the DATA partition leader (not the local actor) so the lock is recorded where the
        // writes and 2PC handlers for [B,C) run. Uses the merge's own HLC as the lock's transaction
        // id, which is also the quiesce owner below.
        //
        // WriteFence, not Exclusive — same rationale as the splitter's quiesce: the fence blocks
        // every writer through the write-path check, tolerates a reader's Shared range lock (which
        // is carried across the cutover), and plants no per-key write intents that would wedge
        // snapshot scans of [B,C) for the copy window. A foreign Exclusive holder still refuses it.
        (KeyValueResponseType lockResult, _) = await manager.LocateAndTryAcquireRangeLock(
            mergeTxId,
            keySpace,
            right.StartKey, true,
            right.EndKey, false,
            QuiesceTtlMs,
            KeyValueDurability.Persistent,
            RangeLockMode.WriteFence,
            ct);

        // Only Locked will do. AlreadyLocked names a foreign holder — a re-entrant acquire under the
        // same transaction id answers Locked — so treating it as success would run the merge while
        // another transaction holds a conflicting lock over part of [B,C), which is what this lock
        // exists to prevent. A merge is opportunistic housekeeping: refusing and retrying on the next
        // tick costs nothing.
        if (lockResult != KeyValueResponseType.Locked)
        {
            logger.LogError("RangeMerger: failed to acquire quiesce lock — {Result}", lockResult);
            return MergeOutcome.QuiesceFailed;
        }

        // Publish the quiesce on the descriptor itself so it is enforced wherever a write lands and
        // wherever P2's leadership ends up, not only in this node's router. Fail the merge if it does
        // not commit: proceeding would copy the range while it is still accepting writes.
        HLCTimestamp quiescedUntil =
            raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId()) + QuiesceTtlMs;

        bool quiescePublished = await rangeMapStore.QuiesceRangeAsync(
            keySpace, right.StartKey, right.EndKey, mergeTxId, quiescedUntil, ct);

        if (!quiescePublished)
        {
            logger.LogError("RangeMerger: could not publish the quiesce for {Space} [{Start},{End})",
                keySpace, right.StartKey, right.EndKey ?? "+inf");

            // Undo the lock on an uncancellable token: a cancelled merge must not leave the range
            // locked for the lock's whole TTL.
            await manager.ReleaseExclusiveRangeLockOnPartitionLeaderAsync(
                right.PartitionId, mergeTxId, keySpace, right.StartKey, true, right.EndKey, false,
                KeyValueDurability.Persistent, CancellationToken.None);

            return MergeOutcome.QuiesceFailed;
        }

        try
        {
            return await MergeUnderQuiesceAsync(keySpace, left, right, mergeTxId, duringQuiesce, ct);
        }
        finally
        {
            // Reopen the range before releasing the lock. Scoped to this merge's own id, so it is a
            // no-op after a successful cutover (the descriptor it stamped no longer exists) and can
            // never reopen a window a later move opened. If it cannot commit — this node lost the meta
            // leadership — the deadline stamped above ends the window instead.
            await rangeMapStore.ReleaseQuiesceAsync(mergeTxId, CancellationToken.None);

            // Release the lock on the RETIRING partition by id. After cutover [B,C) routes to the
            // survivor, so a located release would be sent there and leave the lock stranded on P2.
            await manager.ReleaseExclusiveRangeLockOnPartitionLeaderAsync(
                right.PartitionId,
                mergeTxId,
                keySpace,
                right.StartKey, true,
                right.EndKey, false,
                KeyValueDurability.Persistent,
                CancellationToken.None);
        }
    }

    /// <summary>
    /// The part of the merge that runs with <c>[B,C)</c> quiesced: settle, copy, hand off the
    /// range's transaction state, and cut over. Split out so the caller's <c>finally</c> owns
    /// closing the window on every path out of it.
    /// </summary>
    private async Task<MergeOutcome> MergeUnderQuiesceAsync(
        string keySpace,
        RangeDescriptor left,
        RangeDescriptor right,
        HLCTimestamp mergeTxId,
        Func<Task>? duringQuiesce,
        CancellationToken ct)
    {
        // The transaction-state reads below (settle barrier, receipts/records/intents handoff) run
        // over node-global stores ordered by raw key, where a null end bound means "+infinity"
        // rather than this range's "end of the key space". Bound them to the key space: an
        // unbounded read of a tail range gathers every other key space's live intents that sort
        // above the start key, so under sustained writes the settle barrier never observes an
        // empty range — and a completed cutover would hand foreign key spaces' records and intents
        // to the survivor.
        string? movingEndKey = KeySpaceBounds.MovingEndKey(keySpace, right.EndKey);

        // -- 2. Settle the moving range's durable intents before the copy ---------
        // Under deferred settlement a committed value can exist only as a decided-but-unsettled
        // prepared intent, and the copy below captures base rows: cutting over now would move the
        // range while its newest committed values still ride intents. The quiesce blocks new
        // prepares, so settling here is stable — decided intents materialize into the moving range
        // and the copy carries the rows. An intent still undecided inside its window refuses this
        // attempt; the trigger retries once its coordinator has decided.
        if (!await manager.SettleMovingRangeIntentsAsync(right.PartitionId, right.StartKey, movingEndKey, ct))
        {
            logger.LogWarning(
                "RangeMerger: moving range [{Start},{End}) holds unsettled durable intents; refusing this merge attempt",
                right.StartKey, movingEndKey);
            return MergeOutcome.UnsettledMovingIntents;
        }

        // -- 3. Copy [B,C) at snapshotTs -> survivor ------------------------------
        // Routed through partition leaders: the right range is paged via the locator and every
        // page is replicated onto the survivor's Raft log, so the copy is correct even when this
        // node hosts neither side. Read as the quiesce lock's owner: the exclusive range lock
        // stamped a write intent on every resident key of [B,C), and a foreign snapshot read meeting
        // those live intents answers MustRetry forever.
        HLCTimestamp snapshotTs = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        bool placedRight = raft.GetPartitionReplicas(right.PartitionId).Count > 0;

        try
        {
            // The quiesced deadline: this copy runs inside the bounded quiesce window, and the
            // survivor partition is long-lived, so no first election needs to be waited out.
            if (!await manager.CopyRangeToPartitionAsync(
                    keySpace, right.StartKey, right.EndKey, snapshotTs, right.PartitionId, left.PartitionId,
                    mergeTxId, ct, RangeStateTransferService.RangeCopyQuiescedDeadlineMs))
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

        // -- 3b. Transfer range locks: clamp right's live locks, inject into left leader --
        // Locks are actor-local (not Raft-replicated). Read from the right partition leader and
        // inject into the left (survivor) leader before cutover so writes to [B,C) routed to
        // the survivor after the merge are still blocked by any live range locks. The pre-cutover
        // import is followed by a post-cutover confirm-and-reimport loop
        // (EnsureLocksOnDestinationLeaderAsync) to handle a left-leadership change during the window,
        // so the transfer stays best-effort. Correctness metadata (receipts, decisions) is not
        // best-effort — see below.
        //
        // This set is usually empty, and that is by design rather than an oversight: the quiesce lock
        // above is exclusive over the same interval, so a foreign lock overlapping [B,C) would have
        // made that acquire fail and this merge refuse. What survives is what can still appear after
        // it — a lock imported onto the retiring partition by another range move in the same window.
        // Keep the transfer for those; do not read an empty set as proof it is unreachable.
        List<KeyValueRangeLock> clampedLocks = [];

        try
        {
            HLCTimestamp lockNow = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

            List<KeyValueRangeLock> rightLocks = await manager.GetRangeLocksFromPartitionLeaderAsync(
                keySpace, right.PartitionId, ct);

            // mergeTxId (the quiesce lock) is excluded — it is released independently in the caller's
            // finally. Transferring it would move this merge's own lock onto the survivor, where it
            // would block writes to [B,C) after cutover until its TTL lapsed.
            clampedLocks = KvStateMachineTransfer.FilterAndClamp(
                rightLocks, right.StartKey, right.EndKey, lockNow, mergeTxId);

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
                        right.PartitionId, right.StartKey, movingEndKey, ct);

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
                movedReceipts = manager.GetLocalCompletionReceiptsForRange(right.StartKey, movingEndKey);
                movedRecords = manager.GetLocalTransactionRecordsForRange(right.StartKey, movingEndKey);
                movedIntents = manager.GetLocalPreparedIntentsForRange(right.StartKey, movingEndKey);
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

        // Test seam: let the caller race an operation into the quiesce window.
        if (duringQuiesce is not null)
            await duringQuiesce();

        // -- 4. Atomic cutover ----------------------------------------------------
        // Replace {left, right} with [A,C)@P1 gen+1. The merged descriptor carries no quiesce, so the
        // cutover ends the window atomically with the routing change it was protecting.
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

                // Drop the two ranges by identity, not by value: the descriptors read by the caller
                // predate the quiesce stamped onto the right one since, and a value comparison over a
                // record's every field is one field's drift away from matching the wrong element or
                // none at all. Both come out of `existing`, so reference identity is exact.
                List<RangeDescriptor> next = existing
                    .Where(d => !ReferenceEquals(d, liveLeft) && !ReferenceEquals(d, liveRight))
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
    /// under-min merge candidate. The pages always read through the locator (the range's partition
    /// leader), never this node's local store. Under a committed replica set this node may not host
    /// the range, and a local read would answer an empty count that wrongly qualifies a populated
    /// range for merging. Even on a hosting node, a live write intent — the signal that makes a
    /// busy page refuse and this count end incomplete — is in-memory state on the leader's actor
    /// only, invisible to a follower-local scan, and a follower's replicated state can also lag.
    /// When this node is the range's confirmed leader the locator degenerates to the local read.
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

            KeyValueGetByRangeResult page = await manager.LocateAndGetByRange(
                HLCTimestamp.Zero,
                descriptor.KeySpace,
                pageStart,
                pageStartInclusive,
                descriptor.EndKey,
                false,
                Math.Min(CountPageSize, maxCount - count),
                HLCTimestamp.Zero,
                KeyValueDurability.Persistent,
                ct);

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
    QuiesceFailed,
    CutoverFailed,
    ConcurrentChange,
    ConcurrentMove,
    UnsettledMovingIntents,
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
    public static MergeOutcome QuiesceFailed  => new(MergeStatus.QuiesceFailed);
    public static MergeOutcome CutoverFailed  => new(MergeStatus.CutoverFailed);
    public static MergeOutcome ConcurrentChange => new(MergeStatus.ConcurrentChange);
    public static MergeOutcome ConcurrentMove => new(MergeStatus.ConcurrentMove);
    public static MergeOutcome UnsettledMovingIntents => new(MergeStatus.UnsettledMovingIntents);
}
