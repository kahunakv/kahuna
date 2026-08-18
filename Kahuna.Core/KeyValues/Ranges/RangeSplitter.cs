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
/// <b>Quiesce scope (F3).</b> The exclusive range lock blocks concurrent 2PC commits on
/// <c>[K,E)</c> during the catch-up window. F3 adds a best-effort quiesce for direct
/// (non-2PC) writes via <see cref="RangeQuiesceStore"/>: between the lock acquisition and its
/// release, the locator pre-route check on the split-executor node returns <c>MustRetry</c> for
/// any direct write that falls in <c>[K,E)</c>. The client retries after cutover and is then
/// routed to P'.
/// </para>
///
/// <para>
/// <b>Remaining cross-node gap.</b> The quiesce check is performed in the locator on the node
/// running the split. A direct write that arrives on a <em>different</em> node during the same
/// window bypasses the check: it routes to P (generation fence passes), commits on P, is absent
/// from the catch-up snapshot, and after cutover routes to P' — where it never arrives. That
/// write is silently lost. Fully closing the window requires replicating the quiesce state to the
/// data-partition proposal actor so the check can be enforced on every replica. <b>Deferred to
/// a future partition-scoped storage design.</b>
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

    /// <summary>TTL for the quiesce range lock (ms). Long enough to cover the catch-up export.</summary>
    private const int QuiesceLockTtlMs = 30_000;

    private readonly IRaft raft;
    private readonly RangeMapStore rangeMapStore;
    private readonly RangeQuiesceStore quiesceStore;
    private readonly KeyValuesManager manager;
    private readonly ILogger<IKahuna> logger;

    public RangeSplitter(
        IRaft raft,
        RangeMapStore rangeMapStore,
        RangeQuiesceStore quiesceStore,
        KeyValuesManager manager,
        ILogger<IKahuna> logger)
    {
        this.raft = raft;
        this.rangeMapStore = rangeMapStore;
        this.quiesceStore = quiesceStore;
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
    /// catch-up import and the cutover commit, while the range is quiesced. Used by
    /// <c>Split_DirectWriteDuringQuiesce_MustRetry</c> (F3) to race a direct write into
    /// the quiesce window.
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
        // With a committed replica set on the source, the probe must read through the locator
        // (the source partition's leader): this node may not host the range, and a local read
        // would answer empty for a populated half.
        bool placedSource = raft.GetPartitionReplicas(descriptor.PartitionId).Count > 0;

        HLCTimestamp probeTs = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        HalfProbe leftProbe = await ProbeHalfAsync(keySpace, descriptor.StartKey, splitKey, probeTs, placedSource, ct);
        HalfProbe rightProbe = await ProbeHalfAsync(keySpace, splitKey, descriptor.EndKey, probeTs, placedSource, ct);

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
            QuiesceLockTtlMs,
            KeyValueDurability.Persistent,
            ct);

        if (lockResult is not (KeyValueResponseType.Locked or KeyValueResponseType.AlreadyLocked))
        {
            logger.LogError(
                "RangeSplitter: failed to acquire quiesce lock — {Result}", lockResult);
            return SplitOutcome.QuiesceFailed;
        }

        // F3: quiesce direct (non-2PC) writes to [K,E) for the duration of the split window.
        quiesceStore.Quiesce(keySpace, splitKey, descriptor.EndKey);

        try
        {
            // ── 7. Final catch-up copy: capture writes since snapshotTs ──────────
            // Read as the quiesce lock's owner: the exclusive range lock stamped a write intent
            // on every resident key of [K,E), and a foreign snapshot read meeting those live
            // intents answers MustRetry forever.
            HLCTimestamp catchupTs = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

            if (!await manager.CopyRangeToPartitionAsync(
                    keySpace, splitKey, descriptor.EndKey, catchupTs, descriptor.PartitionId, newPartitionId,
                    splitTxId, ct))
            {
                logger.LogError("RangeSplitter: catch-up copy failed for {Space} [{Key},{End})",
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

            // F3 test seam: allow the caller to race a direct write while quiesced.
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

                    List<RangeDescriptor> next = existing
                        .Where(d => d != descriptor)
                        .ToList();

                    // Left half: [S, K) stays on P with bumped generation.
                    next.Add(descriptor with { EndKey = splitKey, Generation = newGeneration });

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
            // F3: release the direct-write quiesce before releasing the range lock.
            quiesceStore.Release(keySpace, splitKey, descriptor.EndKey);

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
    /// With a committed replica set the probe reads through the locator (the range's leader); a
    /// retryable answer counts as empty, which safely refuses the split for this round — the
    /// trigger retries on its next tick.
    /// </summary>
    private async Task<HalfProbe> ProbeHalfAsync(
        string keySpace, string? startKey, string? endKey, HLCTimestamp ts, bool routeThroughLeader, CancellationToken ct)
    {
        KeyValueGetByRangeResult result = routeThroughLeader
            ? await manager.LocateAndGetByRange(
                HLCTimestamp.Zero, keySpace, startKey, true, endKey, false, 1, ts,
                KeyValueDurability.Persistent, ct).ConfigureAwait(false)
            : await manager.GetByRange(
                HLCTimestamp.Zero, keySpace, startKey, true, endKey, false, 1, ts,
                KeyValueDurability.Persistent).ConfigureAwait(false);

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
}
