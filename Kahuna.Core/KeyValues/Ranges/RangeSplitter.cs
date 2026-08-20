using Kommander;
using Kommander.Time;

using Kahuna.Server.KeyValues.Logging;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Ranges;

/// <summary>
/// Executes the key-range split transaction:
/// <c>R = [S,E)@P</c> → <c>[S,K)@P</c> + <c>[K,E)@P'</c>.
///
/// <para>
/// <b>Step sequence:</b>
/// <list type="number">
///   <item>Validate <c>S &lt; K &lt; E</c> ordinal and both halves non-empty (no thrash).</item>
///   <item><c>P' = CreatePartitionAsync(newId, Unrouted)</c> — fresh empty Raft group.</item>
///   <item>Initial bulk transfer: export <c>[K,E)</c> at <c>snapshotTs</c> (MVCC), import to P'.</item>
///   <item>Quiesce window: acquire an exclusive range lock on <c>[K,E)</c> to block
///       concurrent 2PC commits on P; do a final catch-up export at the quiesce timestamp;
///       import the catch-up to P'.</item>
///   <item>Atomic cutover: <see cref="RangeMapStore.MutateAsync"/> replaces <c>R</c> with
///       <c>[S,K)@P gen+1</c> and <c>[K,E)@P' gen+1</c> in one replicated meta entry.</item>
///   <item>Release range lock (fence now protects P'). <c>[K,E)</c> rows are left orphaned on P,
///       not deleted — see the <b>Orphan rows</b> note below.</item>
/// </list>
/// </para>
///
/// <para>
/// <b>Quiesce scope.</b> Two guards cover the window between the catch-up export and the cutover,
/// and they are complementary rather than redundant. The <b>exclusive range lock</b> on
/// <c>[K,E)</c> is installed by a message on P's leader's key-value actor — the same
/// single-threaded mailbox every write for that key space passes through — so it is exactly
/// ordered against in-flight writes: one admitted before the lock is already proposed, one after is
/// refused. The <b>descriptor quiesce</b> (<see cref="RangeDescriptor.QuiescedUntil"/>, published
/// through <see cref="RangeMapStore.QuiesceRangeAsync"/>) rides the replicated range map, so it
/// reaches every node and, unlike the lock, survives a leadership change on P: a promoted leader
/// holds no in-memory lock but still refuses writes into <c>[K,E)</c>. A refused client retries
/// after cutover and is then routed to P'.
/// </para>
///
/// <para>
/// The descriptor quiesce carries a deadline rather than relying on this method's <c>finally</c> to
/// end it, so a split executor that dies mid-window leaves a range that reopens on its own instead
/// of one that refuses writes forever. A successful cutover replaces the descriptor outright, which
/// clears the quiesce atomically with the routing change.
/// </para>
///
/// <para>
/// <b>Caller constraint.</b> <see cref="SplitAsync"/> must be called on the node that is the
/// <b>system-partition (partition 0) leader</b>, because <see cref="IRaft.CreatePartitionAsync"/>
/// enforces this. The auto-split trigger will run on the system-partition leader.
/// The rest of the work (export, import, meta-cutover) routes to the appropriate leaders via
/// the normal request path.
/// </para>
///
/// <para>
/// <b>New partition ID.</b> Allocated by <see cref="ComputeNextPartitionId"/> from Kommander's
/// partition map — one past every ID ever used, so a merged-away or rolled-back range's ID is never
/// handed out again (recreating a retired partition is refused outright). Concurrent splits are
/// serialised by the meta-partition Raft log, so the cutover MutateAsync rejects any case where a
/// concurrent split already used the same ID. On rejection the split can be retried with a freshly
/// computed ID.
/// </para>
///
/// <para>
/// <b>Orphan rows.</b> After cutover, the rows for <c>[K,E)</c> remain physically present on P's
/// replicas — they are not deleted. Because the persistence backend is node-global (keyed by full
/// key string, not partition-scoped), a local delete on the split executor would either destroy data
/// that P' shares on the same node or leave stale rows on remote replicas. Orphans are unreachable
/// via routing (the descriptor no longer points to P for that sub-range) and do not affect
/// correctness. Reclamation can be done later as a compaction pass scoped to each replica.
/// </para>
/// </summary>
internal sealed class RangeSplitter
{
    /// <summary>Minimum keys a range must have to be splittable (both halves must be non-empty).</summary>
    public const int MinRangeKeys = 2;

    /// <summary>
    /// How long the quiesce window may stay open (ms). Long enough to cover the catch-up export, and
    /// short enough that a split executor which dies mid-window costs the range a bounded stretch of
    /// refused writes. Applied to both guards — the range lock's TTL and the replicated descriptor
    /// deadline — so neither outlives the other and reopens the window while the split still runs.
    /// </summary>
    private const int QuiesceTtlMs = 30_000;

    private readonly IRaft raft;
    private readonly RangeMapStore rangeMapStore;
    private readonly KeyValuesManager manager;
    private readonly ILogger<IKahuna> logger;

    public RangeSplitter(
        IRaft raft,
        RangeMapStore rangeMapStore,
        KeyValuesManager manager,
        ILogger<IKahuna> logger)
    {
        this.raft = raft;
        this.rangeMapStore = rangeMapStore;
        this.manager = manager;
        this.logger = logger;
    }

    /// <summary>
    /// Executes the full split transaction for <paramref name="keySpace"/> at <paramref name="splitKey"/>,
    /// moving <c>[K,E)</c> to the pre-created partition <paramref name="newPartitionId"/>.
    ///
    /// <para>
    /// <b>Why the caller creates P' first.</b> <see cref="IRaft.CreatePartitionAsync"/> requires the
    /// caller to be the system-partition (0) leader, while <see cref="RangeMapStore.MutateAsync"/>
    /// (the cutover) requires the caller to be the meta-partition (1) leader. In a 3-node cluster
    /// these leaders are often on different nodes, so a single <c>SplitAsync</c> call cannot
    /// satisfy both constraints at once. Callers (tests, the Task-7 auto-splitter) are responsible
    /// for creating the target partition from the system-partition leader and passing the resulting
    /// ID here; this method then drives the transfer and cutover from the meta-partition leader.
    /// </para>
    ///
    /// <para>
    /// This method must be called on the <b>meta-partition (1) leader</b> to allow the cutover
    /// <c>MutateAsync</c> to succeed.
    /// </para>
    /// </summary>
    public Task<SplitOutcome> SplitAsync(
        string keySpace,
        string splitKey,
        int newPartitionId,
        CancellationToken ct = default) =>
        SplitAsync(keySpace, splitKey, newPartitionId, null, ct);

    /// <summary>
    /// Internal overload for tests: <paramref name="duringQuiesce"/> is invoked between the
    /// catch-up import and the cutover commit, while the range is quiesced. It exists so a test can
    /// race an operation into that window, which is otherwise too short to hit deliberately.
    /// </summary>
    internal async Task<SplitOutcome> SplitAsync(
        string keySpace,
        string splitKey,
        int newPartitionId,
        Func<Task>? duringQuiesce,
        CancellationToken ct = default)
    {
        // ── 1. Locate the covering range R = [S,E)@P ────────────────────────────
        RangeDescriptor? descriptor = rangeMapStore.Current.Find(keySpace, splitKey);
        if (descriptor is null)
        {
            logger.LogWarning("RangeSplitter: no range covers {Space}/{Key}", keySpace, splitKey);
            return SplitOutcome.NoRange;
        }

        // ── 2. Validate S < K < E (ordinal) ─────────────────────────────────────
        if (!ValidateSplitKey(descriptor, splitKey, out string? validationError))
        {
            logger.LogWarning("RangeSplitter: invalid split key — {Error}", validationError);
            return SplitOutcome.InvalidSplitKey;
        }

        // ── 3. Check both halves are non-empty (min-range-size guard) ────────────
        // We probe by exporting exactly one key from each half at "now". An empty export means
        // that half is empty — splitting would produce a gap or a vacuous range.
        // placedSource: whether the source partition has a committed replica set. It decides how
        // step 7c gathers the moving range's transaction state (leader read vs local stores).
        bool placedSource = raft.GetPartitionReplicas(descriptor.PartitionId).Count > 0;

        HLCTimestamp probeTs = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        HalfProbe leftProbe = await ProbeHalfAsync(keySpace, descriptor.StartKey, splitKey, probeTs, ct);
        HalfProbe rightProbe = await ProbeHalfAsync(keySpace, splitKey, descriptor.EndKey, probeTs, ct);

        // A probe that could not be answered is not evidence of an empty half. Report it as its own
        // retryable outcome so the next cadence (or the operator) tries again, instead of telling the
        // caller this range is too small to split — a permanent-sounding property of the data.
        if (leftProbe == HalfProbe.Indeterminate || rightProbe == HalfProbe.Indeterminate)
        {
            logger.LogWarning(
                "RangeSplitter: split probe indeterminate — left: {L}, right: {R}; retrying later", leftProbe, rightProbe);
            return SplitOutcome.ProbeIndeterminate;
        }

        if (leftProbe == HalfProbe.Empty || rightProbe == HalfProbe.Empty)
        {
            logger.LogWarning(
                "RangeSplitter: refusing split — left has keys: {L}, right has keys: {R}",
                leftProbe == HalfProbe.HasKeys, rightProbe == HalfProbe.HasKeys);
            return SplitOutcome.BelowMinRangeSize;
        }

        // ── 4. (Partition already created by caller) ─────────────────────────────

        // ── 5. Bulk copy [K,E) at snapshotTs → P' ───────────────────────────────
        // Routed through partition leaders: the source is paged via the locator and every page is
        // replicated onto P''s Raft log, so the copy is correct even when this node hosts neither
        // side. Under legacy full replication the copy degenerates to the local export/import.
        HLCTimestamp snapshotTs = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        try
        {
            if (!await manager.CopyRangeToPartitionAsync(
                    keySpace, splitKey, descriptor.EndKey, snapshotTs, descriptor.PartitionId, newPartitionId,
                    HLCTimestamp.Zero, ct))
            {
                logger.LogError("RangeSplitter: bulk copy failed for {Space} [{Key},{End})",
                    keySpace, splitKey, descriptor.EndKey ?? "+inf");
                return SplitOutcome.TransferFailed;
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "RangeSplitter: bulk copy failed for {Space} [{Key},{End})",
                keySpace, splitKey, descriptor.EndKey ?? "+inf");
            return SplitOutcome.TransferFailed;
        }

        // ── 6. Quiesce: exclusive range lock on [K,E) ────────────────────────────
        // Uses the internal split HLC as the transaction id for the range lock.
        HLCTimestamp splitTxId = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        // Route to the DATA partition leader (not the local actor) so the lock is recorded where
        // the 2PC handlers for [K,E) run.
        (KeyValueResponseType lockResult, _) = await manager.LocateAndTryAcquireExclusiveRangeLock(
            splitTxId,
            keySpace,
            splitKey, true,
            descriptor.EndKey, false,
            QuiesceTtlMs,
            KeyValueDurability.Persistent,
            ct);

        if (lockResult is not (KeyValueResponseType.Locked or KeyValueResponseType.AlreadyLocked))
        {
            logger.LogError(
                "RangeSplitter: failed to acquire quiesce lock — {Result}", lockResult);
            return SplitOutcome.QuiesceFailed;
        }

        // Publish the quiesce on the descriptor itself so it is enforced wherever a write lands and
        // wherever P's leadership ends up, not only in this node's router. Fail the split if it does
        // not commit: proceeding would copy the range while it is still accepting writes.
        HLCTimestamp quiescedUntil =
            raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId()) + QuiesceTtlMs;

        bool quiescePublished = await rangeMapStore.QuiesceRangeAsync(
            keySpace, splitKey, descriptor.EndKey, splitTxId, quiescedUntil, ct);

        if (!quiescePublished)
        {
            logger.LogError("RangeSplitter: could not publish the quiesce for {Space} [{Key},{End})",
                keySpace, splitKey, descriptor.EndKey ?? "+inf");

            // Undo the lock on an uncancellable token: a cancelled split must not leave the range
            // locked for the lock's whole TTL.
            await manager.ReleaseExclusiveRangeLockOnPartitionLeaderAsync(
                descriptor.PartitionId, splitTxId, keySpace, splitKey, true, descriptor.EndKey, false,
                KeyValueDurability.Persistent, CancellationToken.None);

            return SplitOutcome.QuiesceFailed;
        }

        try
        {
            // ── 6b. Settle the moving range's durable intents before the catch-up copy ──────────
            // Under deferred settlement a committed value can exist only as a decided-but-unsettled
            // prepared intent: the copies capture base rows, so cutting over now would move the range
            // while its newest committed values still ride intents — any leg of the intent handoff or
            // overlay that fails then serves the prior revision from the child (observed by Jepsen as
            // read skew with no faults injected). The quiesce above blocks new prepares, so settling
            // here is stable: decided intents materialize into the source range and the catch-up copy
            // carries the rows. An intent still undecided inside its window refuses this attempt —
            // the trigger retries once its coordinator has decided.
            if (!await manager.SettleMovingRangeIntentsAsync(descriptor.PartitionId, splitKey, descriptor.EndKey, ct))
            {
                logger.LogWarning(
                    "RangeSplitter: moving range [{Key},{End}) holds unsettled durable intents; refusing this split attempt",
                    splitKey, descriptor.EndKey ?? "+inf");
                return SplitOutcome.UnsettledMovingIntents;
            }

            // ── 7. Final catch-up copy: capture writes since snapshotTs ──────────
            // Read as the quiesce lock's owner: the exclusive range lock stamped a write intent
            // on every resident key of [K,E), and a foreign snapshot read meeting those live
            // intents answers MustRetry forever.
            HLCTimestamp catchupTs = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

            try
            {
                if (!await manager.CopyRangeToPartitionAsync(
                        keySpace, splitKey, descriptor.EndKey, catchupTs, descriptor.PartitionId, newPartitionId,
                        splitTxId, ct))
                {
                    logger.LogError("RangeSplitter: catch-up copy failed for {Space} [{Key},{End})",
                        keySpace, splitKey, descriptor.EndKey ?? "+inf");
                    return SplitOutcome.TransferFailed;
                }
            }
            catch (Exception ex)
            {
                // A refused export page throws rather than truncating the snapshot; the split is
                // retryable, exactly as a failed bulk copy.
                logger.LogError(ex, "RangeSplitter: catch-up copy failed for {Space} [{Key},{End})",
                    keySpace, splitKey, descriptor.EndKey ?? "+inf");
                return SplitOutcome.TransferFailed;
            }

            // ── 7b. Transfer range locks: clamp P's live locks to [K,E), inject into P' ──
            // Locks are actor-local (not Raft-replicated), so they must be read from the
            // source partition leader and injected into the destination partition leader via
            // the locator routing wrappers, which forward via IPC when the leader is remote.
            // splitTxId (the quiesce lock) is excluded — it is released independently at step 9.
            HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

            List<KeyValueRangeLock> sourceLocks = await manager.GetRangeLocksFromPartitionLeaderAsync(
                keySpace, descriptor.PartitionId, ct);

            List<KeyValueRangeLock> clampedLocks = KvStateMachineTransfer.FilterAndClamp(
                sourceLocks, splitKey, descriptor.EndKey, now, splitTxId);

            if (clampedLocks.Count > 0)
                await manager.ImportRangeLocksToPartitionLeaderAsync(keySpace, newPartitionId, clampedLocks, ct);

            // ── 7c/7d. Gather the moving range's transaction state, then hand it to P' ───
            // Completion receipts, canonical transaction records and prepared intents whose
            // key/anchor moves to [K,E) are replicated onto the destination partition's Raft log
            // so every replica of P' holds them — a re-commit, re-drive, recovery or finalize
            // routed to P' after cutover still resolves even if P''s leader changes. With a
            // committed replica set on the source the gather must read the source partition
            // leader's stores (this node may hold none of them); under legacy full replication it
            // reads this node's stores, which every replica of P's group populated. Neither
            // handoff is best-effort: a lost receipt, decision or unresolved intent would strand
            // its transaction, so a non-durable step aborts the split before cutover.
            IReadOnlyCollection<CompletionReceiptRecord> movedReceipts;
            IReadOnlyList<Kahuna.Server.KeyValues.Transactions.Data.TransactionRecord> movedRecords;
            IReadOnlyList<Kahuna.Server.KeyValues.Transactions.Data.PreparedIntent> movedIntents;

            if (placedSource)
            {
                bool gathered;
                (gathered, movedReceipts, movedRecords, movedIntents) =
                    await manager.GetRangeTransactionStateFromPartitionLeaderAsync(
                        descriptor.PartitionId, splitKey, descriptor.EndKey, ct);

                if (!gathered)
                {
                    logger.LogError(
                        "RangeSplitter: could not gather the moving range's transaction state from P{Source}'s leader — aborting split before cutover",
                        descriptor.PartitionId);
                    return SplitOutcome.TransferFailed;
                }
            }
            else
            {
                movedReceipts = manager.GetLocalCompletionReceiptsForRange(splitKey, descriptor.EndKey);
                movedRecords = manager.GetLocalTransactionRecordsForRange(splitKey, descriptor.EndKey);
                movedIntents = manager.GetLocalPreparedIntentsForRange(splitKey, descriptor.EndKey);
            }

            if (!await manager.ImportCompletionReceiptsToPartitionLeaderAsync(newPartitionId, movedReceipts, ct))
            {
                logger.LogError(
                    "RangeSplitter: completion-receipt handoff to P{New} not durable — aborting split before cutover", newPartitionId);
                return SplitOutcome.TransferFailed;
            }

            if (!await manager.ImportDurableTransactionStateToPartitionLeaderAsync(newPartitionId, movedRecords, movedIntents, ct))
            {
                logger.LogError(
                    "RangeSplitter: durable transaction-state handoff to P{New} not durable — aborting split before cutover", newPartitionId);
                return SplitOutcome.TransferFailed;
            }

            // Test seam: let the caller race an operation into the quiesce window.
            if (duringQuiesce is not null)
                await duringQuiesce();

            // ── 8. Atomic cutover ────────────────────────────────────────────────
            // Replace R with [S,K)@P and [K,E)@P' — both get generation+1 to invalidate any
            // stale routed-generation on either new range.
            long newGeneration = descriptor.Generation + 1;

            bool raceDetected = false;
            bool cutoverOk;

            try
            {
                cutoverOk = await rangeMapStore.MutateAsync(existing =>
                {
                    // Race guard: verify R still exists at the same generation.
                    RangeDescriptor? live = new RangeMap(existing).Find(keySpace, splitKey);
                    if (live is null || live.PartitionId != descriptor.PartitionId ||
                        live.Generation != descriptor.Generation)
                    {
                        raceDetected = true;
                        // Return unchanged — MutateAsync will commit a no-op. We detect this via
                        // raceDetected and return the appropriate outcome below.
                        return existing;
                    }

                    // Drop the range being split by identity, not by value: the descriptor read at
                    // step 1 predates the quiesce stamped onto it since, so a value comparison would
                    // no longer recognise it and the map would end up holding both the old range and
                    // its two halves.
                    List<RangeDescriptor> next = existing
                        .Where(d => !ReferenceEquals(d, live))
                        .ToList();

                    // Left half: [S, K) stays on P with bumped generation, and reopens — the cutover
                    // is the end of the window the quiesce was protecting.
                    next.Add(live with
                    {
                        EndKey = splitKey,
                        Generation = newGeneration,
                        QuiescedUntil = HLCTimestamp.Zero,
                        QuiesceOwner = HLCTimestamp.Zero,
                        QuiesceStartKey = null,
                        QuiesceEndKey = null
                    });

                    // Right half: [K, E) moves to P' with bumped generation.
                    next.Add(new RangeDescriptor
                    {
                        KeySpace = keySpace,
                        StartKey = splitKey,
                        EndKey = descriptor.EndKey,
                        PartitionId = newPartitionId,
                        Generation = newGeneration
                    });

                    return next;
                }, ct);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "RangeSplitter: MutateAsync threw during cutover");
                return SplitOutcome.CutoverFailed;
            }

            if (raceDetected)
            {
                logger.LogWarning("RangeSplitter: concurrent split detected on {Space} — descriptor moved", keySpace);
                return SplitOutcome.ConcurrentSplit;
            }

            if (!cutoverOk)
            {
                logger.LogError("RangeSplitter: MutateAsync cutover failed (not leader or validation rejected)");
                return SplitOutcome.CutoverFailed;
            }

            // ── 8b. Confirm the transferred locks landed on the CURRENT P' leader, re-importing if
            // a leadership change on the freshly-created partition stranded them on a node that is
            // no longer the leader (the 7b import targets the leader-at-import-time). Best-effort,
            // bounded — direct writes to [K,E) stay blocked by the quiesce (released in the finally
            // below) for the duration of this loop.
            //
            // NOTE (future hardening): the robust fix is to replicate range-lock acquire/release
            // through P''s Raft log so locks reconstruct on whichever node becomes leader. This loop
            // only narrows the window — a leadership change after the final confirm can still strand
            // a lock, because locks are in-memory, leader-local, non-replicated.
            if (clampedLocks.Count > 0)
                await KvStateMachineTransfer.EnsureLocksOnDestinationLeaderAsync(
                    manager, keySpace, newPartitionId, clampedLocks, logger, "RangeSplitter", ct);

            logger.LogRangeSplitterSplit(keySpace, splitKey, descriptor.StartKey ?? "-inf", descriptor.EndKey ?? "+inf", descriptor.PartitionId, newPartitionId, newGeneration);

            return new SplitOutcome(SplitStatus.Succeeded, newPartitionId, newGeneration);
        }
        finally
        {
            // Reopen the range before releasing the lock. Scoped to this split's own id, so it is a
            // no-op after a successful cutover (the descriptor it stamped no longer exists) and can
            // never reopen a window a later split opened over the same bounds. If it cannot commit —
            // this node lost the meta leadership — the deadline stamped above ends the window instead.
            await rangeMapStore.ReleaseQuiesceAsync(splitTxId, CancellationToken.None);

            // ── 9. Release quiesce lock on the ORIGINAL partition by ID ──────────
            // After cutover the locator routes [K,E) to P' — using LocateAndTryRelease
            // would send the release to P' and leave the quiesce lock stranded on P.
            // Target descriptor.PartitionId directly so the lock is released where it
            // was acquired, regardless of what the descriptor map says now.
            await manager.ReleaseExclusiveRangeLockOnPartitionLeaderAsync(
                descriptor.PartitionId,
                splitTxId,
                keySpace,
                splitKey, true,
                descriptor.EndKey, false,
                KeyValueDurability.Persistent,
                CancellationToken.None);
            // [K,E) rows on P are left as orphans — see class doc for rationale.
        }
    }

    // ── helpers ──────────────────────────────────────────────────────────────────

    /// <summary>
    /// Probes whether the half-open interval [start,end) within keySpace has at least one key.
    /// The probe always reads through the locator (the range's partition leader), never this
    /// node's local store. Under a committed replica set this node may not host the range at all;
    /// even when it does, a live write intent — the signal that makes a busy page refuse and this
    /// probe answer <see cref="HalfProbe.Indeterminate"/> — is in-memory state on the leader's
    /// actor only, invisible to a follower-local scan, and a follower's replicated state can also
    /// lag the leader. When this node is the range's confirmed leader the locator degenerates to
    /// the local read, so the collocated case keeps its cost.
    /// </summary>
    private async Task<HalfProbe> ProbeHalfAsync(
        string keySpace, string? startKey, string? endKey, HLCTimestamp ts, CancellationToken ct)
    {
        KeyValueGetByRangeResult result = await manager.LocateAndGetByRange(
            HLCTimestamp.Zero, keySpace, startKey, true, endKey, false, 1, ts,
            KeyValueDurability.Persistent, ct).ConfigureAwait(false);

        if (result.Items.Count > 0)
            return HalfProbe.HasKeys;

        // An empty answer only means "no keys" when the scan actually ran: a successful page is Get with
        // zero items. Every other type is a scan that could not be served — a live foreign write intent in
        // the window makes the whole page retryable, and so do a leadership change or a partition still
        // restoring. Reading those as "this half is empty" refuses the split for a structural reason that
        // is not true, and the halves most likely to carry an in-flight write are the busy ones this exists
        // to split.
        return result.Type == KeyValueResponseType.Get ? HalfProbe.Empty : HalfProbe.Indeterminate;
    }

    private static bool ValidateSplitKey(RangeDescriptor descriptor, string splitKey, out string? error)
    {
        if (descriptor.StartKey is not null &&
            string.CompareOrdinal(splitKey, descriptor.StartKey) <= 0)
        {
            error = $"split key '{splitKey}' must be strictly after StartKey '{descriptor.StartKey}'";
            return false;
        }

        if (descriptor.EndKey is not null &&
            string.CompareOrdinal(splitKey, descriptor.EndKey) >= 0)
        {
            error = $"split key '{splitKey}' must be strictly before EndKey '{descriptor.EndKey}'";
            return false;
        }

        error = null;
        return true;
    }

    /// <summary>
    /// The ID to pass to <see cref="IRaft.CreatePartitionAsync"/> on the system-partition leader
    /// before calling <see cref="SplitAsync"/>: the first partition ID nobody has ever used,
    /// lower-bounded by <see cref="RangeMapStore.FirstDataPartitionId"/>.
    /// <para>
    /// The authority is Kommander's partition map, not the descriptor set. A descriptor disappears
    /// when its range is merged away or when a split is rolled back, but the partition itself keeps
    /// a tombstone entry that can never be recreated — so "no descriptor references this ID" is not
    /// the same question as "this ID was never used", and answering the second one with the first
    /// hands out an ID whose creation is refused forever. The descriptor scan stays as a cheap guard
    /// for a range map that somehow ran ahead of the partition map.
    /// </para>
    /// <para>
    /// Advisory, not a reservation: nothing is claimed until <c>CreatePartitionAsync</c> commits, so
    /// concurrent allocators must be serialized by their caller.
    /// </para>
    /// </summary>
    internal static int ComputeNextPartitionId(IRaft raft, RangeMap map)
    {
        int nextId = raft.GetNextAvailablePartitionId();

        if (nextId < RangeMapStore.FirstDataPartitionId)
            nextId = RangeMapStore.FirstDataPartitionId;

        foreach (RangeDescriptor d in map.Descriptors)
            if (d.PartitionId >= nextId) nextId = d.PartitionId + 1;

        return nextId;
    }
}

/// <summary>
/// Whether one half of a range being split holds any key. <see cref="Indeterminate"/> is the answer a
/// scan gives when it could not be served at all, which must not be confused with an empty half.
/// </summary>
internal enum HalfProbe
{
    HasKeys,
    Empty,
    Indeterminate
}

/// <summary>Terminal status for a <see cref="RangeSplitter.SplitAsync"/> call.</summary>
internal enum SplitStatus
{
    Succeeded,
    NoRange,
    InvalidSplitKey,
    BelowMinRangeSize,
    ProbeIndeterminate,
    PartitionCreationFailed,
    TransferFailed,
    QuiesceFailed,
    CutoverFailed,
    ConcurrentSplit,
    UnsettledMovingIntents,
}

/// <summary>Result of <see cref="RangeSplitter.SplitAsync"/>.</summary>
internal readonly struct SplitOutcome
{
    public SplitStatus Status { get; }
    public int NewPartitionId { get; }
    public long NewGeneration { get; }

    public SplitOutcome(SplitStatus status, int newPartitionId = 0, long newGeneration = 0)
    {
        Status = status;
        NewPartitionId = newPartitionId;
        NewGeneration = newGeneration;
    }

    public bool IsSuccess => Status == SplitStatus.Succeeded;

    public static SplitOutcome NoRange => new(SplitStatus.NoRange);
    public static SplitOutcome InvalidSplitKey => new(SplitStatus.InvalidSplitKey);
    public static SplitOutcome BelowMinRangeSize => new(SplitStatus.BelowMinRangeSize);
    public static SplitOutcome ProbeIndeterminate => new(SplitStatus.ProbeIndeterminate);
    public static SplitOutcome PartitionCreationFailed => new(SplitStatus.PartitionCreationFailed);
    public static SplitOutcome TransferFailed => new(SplitStatus.TransferFailed);
    public static SplitOutcome QuiesceFailed => new(SplitStatus.QuiesceFailed);
    public static SplitOutcome CutoverFailed => new(SplitStatus.CutoverFailed);
    public static SplitOutcome ConcurrentSplit => new(SplitStatus.ConcurrentSplit);
    public static SplitOutcome UnsettledMovingIntents => new(SplitStatus.UnsettledMovingIntents);
}
