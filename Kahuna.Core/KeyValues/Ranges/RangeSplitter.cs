using Kommander;
using Kommander.Time;

using Kahuna.Server.KeyValues.Logging;
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
/// <b>New partition ID.</b> Computed as <c>max(partitionId in current map) + 1</c> before
/// <see cref="RangeMapStore.MutateAsync"/>. Concurrent splits are serialised by the meta-partition
/// Raft log, so the cutover MutateAsync rejects any case where a concurrent split already used the
/// same ID. On rejection the split can be retried with a freshly computed ID.
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
    private readonly KvStateMachineTransfer transfer;
    private readonly RangeQuiesceStore quiesceStore;
    private readonly KeyValuesManager manager;
    private readonly ILogger<IKahuna> logger;

    public RangeSplitter(
        IRaft raft,
        RangeMapStore rangeMapStore,
        KvStateMachineTransfer transfer,
        RangeQuiesceStore quiesceStore,
        KeyValuesManager manager,
        ILogger<IKahuna> logger)
    {
        this.raft = raft;
        this.rangeMapStore = rangeMapStore;
        this.transfer = transfer;
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
        HLCTimestamp probeTs = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        bool leftHasKeys = await HalfHasKeysAsync(keySpace, descriptor.StartKey, splitKey, probeTs, ct);
        bool rightHasKeys = await HalfHasKeysAsync(keySpace, splitKey, descriptor.EndKey, probeTs, ct);

        if (!leftHasKeys || !rightHasKeys)
        {
            logger.LogWarning(
                "RangeSplitter: refusing split — left has keys: {L}, right has keys: {R}", leftHasKeys, rightHasKeys);
            return SplitOutcome.BelowMinRangeSize;
        }

        // ── 4. (Partition already created by caller) ─────────────────────────────

        // ── 5. Bulk export [K,E) at snapshotTs → import to P' ───────────────────
        HLCTimestamp snapshotTs = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        try
        {
            Stream bulkSnapshot = await transfer.ExportRangeAsync(
                keySpace, splitKey, descriptor.EndKey, snapshotTs, KeyValueDurability.Persistent, ct);

            await transfer.ImportRangeAsync(bulkSnapshot, ct);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "RangeSplitter: bulk export/import failed for {Space} [{Key},{End})",
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
            // ── 7. Final catch-up export: capture writes since snapshotTs ────────
            HLCTimestamp catchupTs = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

            Stream catchupSnapshot = await transfer.ExportRangeAsync(
                keySpace, splitKey, descriptor.EndKey, catchupTs, KeyValueDurability.Persistent, ct);

            await transfer.ImportRangeAsync(catchupSnapshot, ct);

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

            // ── 7c. Transfer completion receipts whose key moves to [K,E) into P' ────────
            // Receipts are node-local (like locks, not Raft-replicated). Read the moving range's
            // receipts from this node's store — which holds them because this node participated in
            // P's group — and inject them into the destination partition leader so a re-commit routed
            // to P' after cutover resolves Committed rather than MustRetry.
            IReadOnlyCollection<CompletionReceiptRecord> movedReceipts =
                manager.GetLocalCompletionReceiptsForRange(splitKey, descriptor.EndKey);

            if (movedReceipts.Count > 0)
                await manager.ImportCompletionReceiptsToPartitionLeaderAsync(newPartitionId, movedReceipts, ct);

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

    /// <summary>Probes whether the half-open interval [start,end) within keySpace has at least one key.</summary>
    private async Task<bool> HalfHasKeysAsync(
        string keySpace, string? startKey, string? endKey, HLCTimestamp ts, CancellationToken ct)
    {
        KeyValueGetByRangeResult result = await manager.GetByRange(
            HLCTimestamp.Zero, keySpace, startKey, true, endKey, false, 1, ts,
            KeyValueDurability.Persistent).ConfigureAwait(false);

        return result.Items.Count > 0;
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

    /// <summary>Returns <c>max(PartitionId in current map) + 1</c>, lower-bounded by
    /// <see cref="RangeMapStore.FirstDataPartitionId"/>. Used by the auto-splitter to
    /// compute the ID to pass to <see cref="IRaft.CreatePartitionAsync"/> on the system-partition
    /// leader before calling <see cref="SplitAsync"/>.</summary>
    internal static int ComputeNextPartitionId(RangeMap map)
    {
        int maxId = RangeMapStore.FirstDataPartitionId - 1;

        foreach (RangeDescriptor d in map.Descriptors)
            if (d.PartitionId > maxId) maxId = d.PartitionId;

        return maxId + 1;
    }
}

/// <summary>Terminal status for a <see cref="RangeSplitter.SplitAsync"/> call.</summary>
internal enum SplitStatus
{
    Succeeded,
    NoRange,
    InvalidSplitKey,
    BelowMinRangeSize,
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
    public static SplitOutcome PartitionCreationFailed => new(SplitStatus.PartitionCreationFailed);
    public static SplitOutcome TransferFailed => new(SplitStatus.TransferFailed);
    public static SplitOutcome QuiesceFailed => new(SplitStatus.QuiesceFailed);
    public static SplitOutcome CutoverFailed => new(SplitStatus.CutoverFailed);
    public static SplitOutcome ConcurrentSplit => new(SplitStatus.ConcurrentSplit);
}
