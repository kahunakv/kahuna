
using System.Diagnostics.Metrics;
using Kommander;
using Kommander.Data;
using Kommander.Time;

using Kahuna.Server.Replication;
using Kahuna.Server.Replication.Protos;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Ranges;

/// <summary>
/// The bounds of the snapshot hold registry. Zero or less means no bound. Each live or lapsed registered hold
/// pins MVCC reclamation at its timestamp, so these bound how long and how widely a caller can pin it.
/// </summary>
internal readonly record struct SnapshotHoldLimits(int MaxLeaseMs, int MaxHoldsPerHolder, int MaxHolds);

/// <summary>
/// Replicated, refcounted, leased registry of MVCC snapshot holds. While any hold is live the
/// effective floor — the minimum held <see cref="SnapshotHold.Timestamp"/> among all live holds —
/// constrains revision reclamation so that the revision current at any held timestamp remains
/// readable via every read path.
///
/// <para><b>Replication model.</b> Each mutation replicates a <see cref="SnapshotFloorDeltaMessage"/>
/// (one keyed upsert/remove batch) on the <see cref="RangeMapStore.MetaPartitionId"/> system
/// partition (id 0), under <see cref="ReplicationTypes.SnapshotFloor"/> — not a full-registry
/// snapshot. Deltas are idempotent by holdId, so Raft's in-order re-delivery and replay of the log
/// tail above an installed snapshot both converge on the same set. Because P0's WAL is
/// checkpoint-compacted, a node that falls below the compaction floor cannot rebuild the registry
/// from surviving deltas alone; it is repaired by the whole-partition state transfer registered on
/// P0 (see <c>MetaSystemStateTransfer</c>), which ships the full hold set via
/// <see cref="SerializeState"/> / <see cref="InstallState"/>. The durable on-disk snapshot remains a
/// complete copy written on every mutation, so a cold restart reconstructs the full state locally.</para>
///
/// <para><b>Lease semantics.</b> A hold's <see cref="SnapshotHold.LeaseExpiry"/> is an HLC
/// timestamp; it is live iff <c>leaseExpiry &gt; currentHlc</c>. Liveness governs the <i>reported</i>
/// effective floor (<see cref="GetEffectiveFloor"/>) and purge eligibility — never reclamation.
/// Reclamation (<see cref="BeginPrune"/>, <see cref="GetProtectiveFloor"/>) honors every
/// <i>registered</i> hold, expired or not: a hold's protection ends only when a replicated removal
/// (an explicit release, or the reaper's purge) commits. Registry presence is therefore the
/// protection invariant — while a hold is registered, no node prunes past its timestamp — which is
/// what makes reviving a lapsed-but-registered hold sound (<see cref="RenewAsync"/>): presence
/// proves the pinned history is still intact. Holds loaded from the durable snapshot are exempt
/// from purge for a startup grace window, so a holder can renew after full-cluster downtime longer
/// than its lease. Lease comparisons always use the cluster HLC, never wall-clock time.</para>
///
/// <para><b>Single writer.</b> The ordered apply (<see cref="Replicate"/> / <see cref="Restore"/>, and
/// the meta state transfer) is the only writer of the registry. <see cref="AcquireAsync"/>,
/// <see cref="RenewAsync"/>, <see cref="ReleaseAsync"/> and <see cref="PurgeExpiredHoldsAsync"/> only
/// propose: each confirms meta-partition leadership first, so the applied registry it decides on
/// covers every committed entry, proposes a delta, and waits until this node's apply reaches the
/// proposed entry. They never install a registry of their own, which could replace entries the
/// apply installed while the proposal was in flight. They are serialized by <see cref="mutateGate"/>
/// locally and by the meta Raft log globally.</para>
/// </summary>
internal sealed class SnapshotFloorStore : IDisposable
{
    private readonly IRaft raft;

    private readonly ILogger<IKahuna> logger;

    private readonly SemaphoreSlim mutateGate = new(1, 1);

    private readonly string? snapshotPath;

    private readonly object fileLock = new();

    private readonly SnapshotHoldLimits limits;

    // Per-instance Meter owns the observable gauges. Disposing it removes the gauge callbacks
    // and breaks the strong reference from the meter to the capturing lambdas, so a disposed
    // store can be garbage-collected even though SnapshotFloorMetrics.Meter is static.
    private readonly Meter _instanceMeter;
    private readonly ObservableGauge<int>  liveHoldsGauge;
    private readonly ObservableGauge<long> effectiveFloorMsGauge;

    /// <summary>
    /// The committed hold registry. Written only by <see cref="Apply"/> and the meta state
    /// transfer (<see cref="CommitState"/>); read lock-free by <see cref="GetEffectiveFloor"/> and
    /// the hold introspection path.
    /// </summary>
    private volatile IReadOnlyDictionary<string, SnapshotHold> holds =
        new Dictionary<string, SnapshotHold>(StringComparer.Ordinal);

    /// <summary>
    /// Grace window after load during which holds present in the durable snapshot stay exempt
    /// from the expired-hold purge, so a holder that was down together with the cluster can
    /// renew (revive) its hold before the reaper ends its protection. Zero disables the grace.
    /// </summary>
    private readonly TimeSpan startupGraceWindow;

    /// <summary>
    /// Ids of the holds present in the durable snapshot at load time; these are the holds the
    /// startup grace protects from purge. Read and cleared only under <see cref="mutateGate"/>
    /// (the purge path). Null when nothing was loaded, no grace is configured, or the window
    /// has closed.
    /// </summary>
    private HashSet<string>? startupGraceHoldIds;

    /// <summary>HLC deadline of the startup grace window; Zero when no window was armed.</summary>
    private HLCTimestamp startupGraceDeadline;

    // The highest meta-log index this store applied, and the signal its waiters park on (replaced on every apply).
    private long appliedIndex;

    private TaskCompletionSource appliedSignal = new(TaskCreationOptions.RunContinuationsAsynchronously);

    // How long a mutation waits for its own entry to apply here before it answers anyway: the entry is committed, so
    // the answer is right, only this node's registry is behind. An acquire that cannot see its hold applied fails
    // closed instead, because it cannot prove a local prune honors the hold.
    private static readonly TimeSpan ApplyWaitLimit = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Serializes a hold mutation's in-memory commit against the open/close of a prune-delete
    /// window (<see cref="BeginPrune"/> / <see cref="EndPrune"/>). Sampling the floor and mutating
    /// the hold set under the same monitor is what makes acquisition and pruning agree on a single
    /// order: either the hold is committed before the floor sample (and is therefore reflected in
    /// it), or the acquire observes the prune window and fails closed.
    /// </summary>
    private readonly object pruneCommitLock = new();

    /// <summary>
    /// Prune-delete window generation. Bumped under <see cref="pruneCommitLock"/> when a backend
    /// revision delete opens (odd) and closes (even). An acquire that sees this value change — or
    /// find it odd — across its own commit knows a prune delete overlapped it and cannot prove its
    /// boundary survived, so it fails closed. Only ever mutated under the lock; read lock-free by
    /// an acquire capturing the value at entry.
    /// </summary>
    private long pruneDeleteGen;

    /// <summary>
    /// Cached result of the last O(N) floor scan. Built when the registry loads from disk and
    /// rebuilt on every mutation; valid until <see cref="FloorCacheState.NextExpiry"/> is reached
    /// (at which point a hold may have expired and the slow scan is needed again). Between
    /// mutations the cache is conservative: it may report a floor lower than the true floor (if a
    /// hold expired but was not yet purged), which is safe for reclamation decisions (keeps more
    /// revisions, never fewer). An empty cache is never authoritative while the registry is
    /// non-empty — the read path re-scans instead (see <see cref="GetEffectiveFloorAndCount"/>).
    /// </summary>
    private volatile FloorCacheState _floorCache = FloorCacheState.Empty;

    private sealed class FloorCacheState
    {
        public static readonly FloorCacheState Empty = new(HLCTimestamp.Zero, 0, HLCTimestamp.Zero, HLCTimestamp.Zero);

        public readonly HLCTimestamp Floor;
        public readonly int LiveCount;

        /// <summary>
        /// The minimum <see cref="SnapshotHold.LeaseExpiry"/> of all live holds at cache-fill
        /// time. The cache is definitely valid while <c>currentTime &lt; NextExpiry</c>. Zero
        /// means no live holds were present (cache always valid in that case).
        /// </summary>
        public readonly HLCTimestamp NextExpiry;

        /// <summary>
        /// The minimum <see cref="SnapshotHold.Timestamp"/> over <b>all</b> registered holds,
        /// live or expired. This is the reclamation bound: a hold protects its timestamp until
        /// it is removed from the registry (release or replicated purge), not merely until its
        /// lease lapses, so lease expiry and pruning cannot race. Never time-dependent, so this
        /// value is exact between mutations. Zero when the registry is empty.
        /// </summary>
        public readonly HLCTimestamp ProtectiveFloor;

        public FloorCacheState(HLCTimestamp floor, int liveCount, HLCTimestamp nextExpiry, HLCTimestamp protectiveFloor)
        {
            Floor = floor;
            LiveCount = liveCount;
            NextExpiry = nextExpiry;
            ProtectiveFloor = protectiveFloor;
        }
    }

    public SnapshotFloorStore(
        IRaft raft,
        string? storagePath,
        string? storageRevision,
        ILogger<IKahuna> logger,
        TimeSpan startupGraceWindow = default,
        SnapshotHoldLimits limits = default)
    {
        this.raft = raft;
        this.logger = logger;
        this.startupGraceWindow = startupGraceWindow;
        this.limits = limits;

        snapshotPath = string.IsNullOrEmpty(storagePath)
            ? null
            : Path.Combine(storagePath, $"snapshotfloor_{storageRevision}.snapshot");

        LoadFromDisk();

        // Per-instance Meter: disposing the store disposes this meter, which removes the
        // observable instrument callbacks and releases the closures capturing `this`.
        // Named "Kahuna" (same scope as the static counter meter) so the instrumentation scope
        // is unchanged — multiple Meters may share a name, and a consumer subscribing to the
        // "Kahuna" scope still collects these gauges; only per-instance disposal differs.
        _instanceMeter = new Meter("Kahuna", "1.0");

        liveHoldsGauge = _instanceMeter.CreateObservableGauge(
            "kahuna.snapshot_floor.live_holds",
            () => CountLiveHolds(),
            description: "Number of currently live (non-expired) snapshot holds.");

        effectiveFloorMsGauge = _instanceMeter.CreateObservableGauge(
            "kahuna.snapshot_floor.effective_floor_ms",
            () => ComputeEffectiveFloorMs(),
            description: "Physical (millisecond) component of the effective snapshot floor, or 0 when no hold is active.");
    }

    /// <summary>The committed hold set. Lock-free read.</summary>
    public IReadOnlyDictionary<string, SnapshotHold> Holds => holds;

    /// <summary>
    /// Returns the effective floor (minimum held timestamp among live holds) and the live hold
    /// count. O(1) fast path when no hold has expired since the last mutation; O(N) slow path
    /// otherwise. Returns <see cref="HLCTimestamp.Zero"/> / 0 when no hold is live.
    /// </summary>
    public (HLCTimestamp Floor, int LiveCount) GetEffectiveFloorAndCount(HLCTimestamp currentTime)
    {
        FloorCacheState cache = _floorCache;
        IReadOnlyDictionary<string, SnapshotHold> snapshot = holds;
        if (cache.LiveCount == 0)
        {
            // An empty cache proves nothing while the registry holds entries: a registry swap that
            // did not rebuild the cache would otherwise read as "no live holds" — the value that
            // licenses reclaiming everything. Answer an authoritative zero only for an empty
            // registry; otherwise pay the O(N) scan (N is the hold count, tens at most).
            return snapshot.Count == 0
                ? (HLCTimestamp.Zero, 0)
                : ScanFloorAndCount(snapshot, currentTime);
        }
        // Fast path: no hold has expired since the cache was filled.
        if (currentTime.CompareTo(cache.NextExpiry) < 0)
            return (cache.Floor, cache.LiveCount);
        // Slow path: at least one hold may have expired; recompute without updating the cache
        // (the cache is authoritatively updated only by mutations so we avoid the race).
        return ScanFloorAndCount(snapshot, currentTime);
    }

    /// <summary>
    /// Returns the effective floor: the minimum <see cref="SnapshotHold.Timestamp"/> among all
    /// currently live holds. O(1) fast path when no hold has expired since the last mutation.
    /// Returns <see cref="HLCTimestamp.Zero"/> when no hold is live.
    /// </summary>
    public HLCTimestamp GetEffectiveFloor(HLCTimestamp currentTime) =>
        GetEffectiveFloorAndCount(currentTime).Floor;

    private static (HLCTimestamp Floor, int LiveCount) ScanFloorAndCount(
        IReadOnlyDictionary<string, SnapshotHold> snapshot, HLCTimestamp currentTime)
    {
        HLCTimestamp floor = HLCTimestamp.Zero;
        int liveCount = 0;
        foreach (SnapshotHold hold in snapshot.Values)
        {
            if (!hold.IsLive(currentTime))
                continue;
            liveCount++;
            if (floor == HLCTimestamp.Zero || hold.Timestamp.CompareTo(floor) < 0)
                floor = hold.Timestamp;
        }
        return (floor, liveCount);
    }

    private static FloorCacheState BuildCache(
        IReadOnlyDictionary<string, SnapshotHold> snapshot, HLCTimestamp currentTime)
    {
        HLCTimestamp floor = HLCTimestamp.Zero;
        HLCTimestamp nextExpiry = HLCTimestamp.Zero;
        HLCTimestamp protectiveFloor = HLCTimestamp.Zero;
        int liveCount = 0;
        foreach (SnapshotHold hold in snapshot.Values)
        {
            // The protective (reclamation) floor spans every registered hold, expired or not.
            if (protectiveFloor == HLCTimestamp.Zero || hold.Timestamp.CompareTo(protectiveFloor) < 0)
                protectiveFloor = hold.Timestamp;
            if (!hold.IsLive(currentTime))
                continue;
            liveCount++;
            if (floor == HLCTimestamp.Zero || hold.Timestamp.CompareTo(floor) < 0)
                floor = hold.Timestamp;
            if (nextExpiry == HLCTimestamp.Zero || hold.LeaseExpiry.CompareTo(nextExpiry) < 0)
                nextExpiry = hold.LeaseExpiry;
        }
        return liveCount == 0 && protectiveFloor == HLCTimestamp.Zero
            ? FloorCacheState.Empty
            : new(floor, liveCount, nextExpiry, protectiveFloor);
    }

    /// <summary>
    /// Returns the reclamation floor: the minimum held timestamp over every registered hold,
    /// live or expired. Reclamation honors registry presence, not lease liveness — an expired
    /// hold keeps protecting its timestamp until an explicit release or the reaper's replicated
    /// purge removes it. That gap is what makes reviving a lapsed-but-registered hold sound:
    /// presence proves no prune anywhere has passed the held timestamp. Returns
    /// <see cref="HLCTimestamp.Zero"/> when the registry is empty. Lock-free; exact between
    /// mutations because it does not depend on the clock.
    /// </summary>
    public HLCTimestamp GetProtectiveFloor()
    {
        IReadOnlyDictionary<string, SnapshotHold> snapshot = holds;
        if (snapshot.Count == 0)
            return HLCTimestamp.Zero;

        FloorCacheState cache = _floorCache;
        if (cache.ProtectiveFloor != HLCTimestamp.Zero)
            return cache.ProtectiveFloor;

        // Defensive slow path: a non-empty registry must never read as "nothing protected" —
        // that is the value that licenses reclaiming everything — even if the cache was not
        // rebuilt for the current registry.
        HLCTimestamp floor = HLCTimestamp.Zero;
        foreach (SnapshotHold hold in snapshot.Values)
            if (floor == HLCTimestamp.Zero || hold.Timestamp.CompareTo(floor) < 0)
                floor = hold.Timestamp;
        return floor;
    }

    /// <summary>
    /// Samples the reclamation floor for read-only introspection by off-actor maintenance code.
    ///
    /// <para>This is the raw sample only; it does not open a prune-delete window. Callers that are
    /// about to delete revisions must use <see cref="BeginPrune"/>/<see cref="EndPrune"/> instead,
    /// which sample under <see cref="pruneCommitLock"/> and let a concurrent acquire fail closed.</para>
    ///
    /// <para>May be called from the scheduler thread — only volatile registry state is read.</para>
    /// </summary>
    public HLCTimestamp GetFloorForPrune() => GetProtectiveFloor();

    /// <summary>
    /// Opens a prune-delete window and returns the floor the delete must honor: the reclamation
    /// floor over every registered hold, live or expired (<see cref="GetProtectiveFloor"/>). The
    /// floor is sampled under <see cref="pruneCommitLock"/>, the same monitor a hold commit takes,
    /// so the sample reflects every hold committed before this call — including one acquired
    /// during a pre-sample pause. A hold that commits <em>after</em> this call instead observes
    /// the open window (an odd <see cref="pruneDeleteGen"/>) and fails closed, because the delete
    /// about to run was computed without it.
    ///
    /// <para>The returned <c>Token</c> must be passed to <see cref="EndPrune"/> once the backend
    /// delete finishes — on every path, success or exception — to close the window. The delete
    /// itself runs <b>outside</b> the lock so a slow backend call never blocks acquisition or the
    /// meta clock.</para>
    /// </summary>
    public (HLCTimestamp Floor, long Token) BeginPrune()
    {
        lock (pruneCommitLock)
        {
            HLCTimestamp floor = GetProtectiveFloor();
            long token = ++pruneDeleteGen; // odd → a delete is now in flight
            return (floor, token);
        }
    }

    /// <summary>
    /// Closes the prune-delete window opened by <see cref="BeginPrune"/>. Idempotent and tolerant
    /// of a stale token (only the matching in-flight token advances the generation), so a
    /// double-close or an out-of-order call cannot spuriously mark a window as closed.
    /// </summary>
    public void EndPrune(long token)
    {
        lock (pruneCommitLock)
        {
            if (pruneDeleteGen == token) // still the active (odd) window
                pruneDeleteGen++;        // even → idle
        }
    }

    /// <summary>
    /// Acquires or renews a hold. Idempotent by (holderId, timestamp): a repeat returns the same
    /// holdId and renews the lease — including for a lapsed-but-still-registered hold, whose
    /// revival carries the same continuity guarantee as <see cref="RenewAsync"/>. A fresh hold
    /// (no registered match) protects from its commit forward only; it does not prove the
    /// revision current at the requested timestamp survived earlier reclamation. Only the
    /// meta-partition leader can commit holds; followers return
    /// <see cref="KeyValueResponseType.MustRetry"/>. A lease above the configured maximum is
    /// <see cref="KeyValueResponseType.InvalidInput"/>, and a fresh hold past the per-holder or the
    /// registry bound is <see cref="KeyValueResponseType.SnapshotHoldLimitReached"/>; a repeat of a
    /// registered hold is never refused by a bound, because it adds nothing to the registry.
    /// </summary>
    public async Task<(KeyValueResponseType Type, string HoldId, HLCTimestamp LeaseExpiry)> AcquireAsync(
        string holderId,
        HLCTimestamp timestamp,
        int leaseMs,
        CancellationToken ct)
    {
        if (string.IsNullOrEmpty(holderId))
            return (KeyValueResponseType.Errored, string.Empty, HLCTimestamp.Zero);

        if (!LeaseAllowed(leaseMs))
            return (KeyValueResponseType.InvalidInput, string.Empty, HLCTimestamp.Zero);

        // Capture the prune-delete generation before replicating. If any delete window opens,
        // closes, or is already open across this acquire, the sampled floor that drove that delete
        // was computed without this hold — so we cannot prove the hold's boundary survived and must
        // fail closed. Read before the replication round-trip so the whole acquire is covered.
        long pruneGenAtStart = Volatile.Read(ref pruneDeleteGen);

        await mutateGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            // Confirmed leadership waits for the applied frontier to cover the commit frontier, so
            // the registry read below includes every committed purge, release and acquire.
            // Answering with the SAME holdId for a lapsed hold is a revival: it asserts the hold
            // was registered continuously, i.e. no prune ever passed its timestamp, which only a
            // fully-applied registry proves. Anything unconfirmed fails closed.
            if (!await raft.ConfirmLeadershipIfHosted(RangeMapStore.MetaPartitionId, ct).ConfigureAwait(false))
                return (KeyValueResponseType.MustRetry, string.Empty, HLCTimestamp.Zero);

            HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

            // Check for an existing hold with the same (holderId, timestamp) — idempotent acquire.
            SnapshotHold? existing = FindExistingHold(holderId, timestamp);

            if (existing is null && LimitReached(holderId) is { } bound)
            {
                SnapshotFloorMetrics.HoldsRefusedByLimit.Add(1, new KeyValuePair<string, object?>("bound", bound));
                return (KeyValueResponseType.SnapshotHoldLimitReached, string.Empty, HLCTimestamp.Zero);
            }

            HLCTimestamp expiry = AddMs(now, leaseMs);

            string holdId = existing?.HoldId ?? Guid.NewGuid().ToString("N");

            SnapshotHold hold = new(holdId, holderId, timestamp, expiry);

            if (!await ReplicateAsync(UpsertDelta(hold), ct).ConfigureAwait(false))
                return (KeyValueResponseType.MustRetry, string.Empty, HLCTimestamp.Zero);

            // The hold is committed and durable, but either this node's registry does not show it
            // yet or a prune delete overlapped this acquire. The boundary it was meant to protect
            // may already be gone, so the acquire must not report success. The committed hold
            // protects the timestamp for every future prune once applied, and a retry (idempotent
            // by holderId+timestamp) succeeds once no delete overlaps it.
            if (!IsAppliedAndPruneSafe(holdId, pruneGenAtStart))
                return (KeyValueResponseType.MustRetry, string.Empty, HLCTimestamp.Zero);

            return (KeyValueResponseType.Set, holdId, expiry);
        }
        finally
        {
            mutateGate.Release();
        }
    }

    /// <summary>
    /// Renews the lease on a registered hold. A lapsed hold that is still registered is revived:
    /// registration is the protection invariant (reclamation honors every registered hold, live
    /// or expired), so presence proves the pinned history is intact and success proves the
    /// protection never lapsed. Returns <see cref="KeyValueResponseType.DoesNotExist"/> once the
    /// hold has been released or purged — then the pinned history must be presumed reclaimed.
    /// </summary>
    public async Task<(KeyValueResponseType Type, HLCTimestamp LeaseExpiry)> RenewAsync(
        string holdId,
        int leaseMs,
        CancellationToken ct)
    {
        if (string.IsNullOrEmpty(holdId))
            return (KeyValueResponseType.Errored, HLCTimestamp.Zero);

        if (!LeaseAllowed(leaseMs))
            return (KeyValueResponseType.InvalidInput, HLCTimestamp.Zero);

        await mutateGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            // "Still registered" must be proven against a fully-applied registry: a fresh leader
            // that has not applied an inherited purge would see a ghost of a hold whose protection
            // pruning already ended, and reviving it would report a guarantee the data no longer
            // meets. Confirmed leadership waits for the applied frontier to cover the commit
            // frontier, so the read below is authoritative: still present ⇒ never purged ⇒ no
            // prune anywhere passed the held timestamp.
            if (!await raft.ConfirmLeadershipIfHosted(RangeMapStore.MetaPartitionId, ct).ConfigureAwait(false))
                return (KeyValueResponseType.MustRetry, HLCTimestamp.Zero);

            if (!holds.TryGetValue(holdId, out SnapshotHold? hold))
                return (KeyValueResponseType.DoesNotExist, HLCTimestamp.Zero);

            HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());
            HLCTimestamp expiry = AddMs(now, leaseMs);

            SnapshotHold renewed = hold with { LeaseExpiry = expiry };

            if (!await ReplicateAsync(UpsertDelta(renewed), ct).ConfigureAwait(false))
                return (KeyValueResponseType.MustRetry, HLCTimestamp.Zero);

            return (KeyValueResponseType.Set, expiry);
        }
        finally
        {
            mutateGate.Release();
        }
    }

    /// <summary>
    /// Releases a hold. The effective floor rises when the lowest hold is released.
    /// Returns <see cref="KeyValueResponseType.DoesNotExist"/> when the holdId does not exist.
    /// </summary>
    public async Task<KeyValueResponseType> ReleaseAsync(string holdId, CancellationToken ct)
    {
        if (string.IsNullOrEmpty(holdId))
            return KeyValueResponseType.Errored;

        await mutateGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            // A stale registry could answer DoesNotExist for a hold an inherited entry registered.
            if (!await raft.ConfirmLeadershipIfHosted(RangeMapStore.MetaPartitionId, ct).ConfigureAwait(false))
                return KeyValueResponseType.MustRetry;

            if (!holds.ContainsKey(holdId))
                return KeyValueResponseType.DoesNotExist;

            bool ok = await ReplicateAsync(RemoveDelta(holdId), ct).ConfigureAwait(false);
            return ok ? KeyValueResponseType.Deleted : KeyValueResponseType.MustRetry;
        }
        finally
        {
            mutateGate.Release();
        }
    }

    /// <summary>Finds the registered hold matching (holderId, timestamp), or null.</summary>
    private bool LeaseAllowed(int leaseMs) => leaseMs > 0 && (limits.MaxLeaseMs <= 0 || leaseMs <= limits.MaxLeaseMs);

    /// <summary>
    /// Names the bound one more hold of <paramref name="holderId"/> would pass ("holder" or "total"), or null when
    /// it fits. Lapsed holds count: they stay registered, and pin reclamation, until a release or a purge commits.
    /// </summary>
    private string? LimitReached(string holderId)
    {
        IReadOnlyDictionary<string, SnapshotHold> snapshot = holds;

        if (limits.MaxHolds > 0 && snapshot.Count >= limits.MaxHolds)
            return "total";

        if (limits.MaxHoldsPerHolder <= 0)
            return null;

        int ofHolder = 0;
        foreach (SnapshotHold h in snapshot.Values)
        {
            if (string.Equals(h.HolderId, holderId, StringComparison.Ordinal) && ++ofHolder >= limits.MaxHoldsPerHolder)
                return "holder";
        }

        return null;
    }

    private SnapshotHold? FindExistingHold(string holderId, HLCTimestamp timestamp)
    {
        foreach (SnapshotHold h in holds.Values)
            if (h.HolderId == holderId && h.Timestamp == timestamp)
                return h;
        return null;
    }

    /// <summary>Rebuilds the hold registry from a meta-log entry replayed during WAL restore.</summary>
    public bool Restore(int partitionId, RaftLog log) => Apply(partitionId, log);

    /// <summary>Applies a committed meta-log entry received via replication (follower / leader echo).</summary>
    public bool Replicate(int partitionId, RaftLog log) => Apply(partitionId, log);

    private bool Apply(int partitionId, RaftLog log)
    {
        if (partitionId != RangeMapStore.MetaPartitionId || log.LogType != ReplicationTypes.SnapshotFloor)
            return true;

        if (log.LogData is null)
            return true;

        try
        {
            SnapshotFloorDeltaMessage delta = ReplicationSerializer.UnserializeSnapshotFloorDeltaMessage(log.LogData);
            // Layer the delta onto the current registry (never a wholesale replace). Idempotent by
            // holdId, so re-delivery of the same entry — or replay of the tail above an installed
            // snapshot — converges. Raft applies committed entries in order, so an upsert cannot
            // resurrect a hold a later remove already deleted.
            // The read of the current registry and the swap share the lock, so a concurrent state
            // transfer cannot slip between them.
            lock (pruneCommitLock)
            {
                Dictionary<string, SnapshotHold> next = new(holds, StringComparer.Ordinal);
                ApplyDeltaEntries(next, delta);
                CommitInMemoryLocked(next);
            }

            NoteApplied(log.Id);
            return true;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to apply snapshot-floor log on partition {Partition}", partitionId);
            return false;
        }
    }

    private static void ApplyDeltaEntries(Dictionary<string, SnapshotHold> target, SnapshotFloorDeltaMessage delta)
    {
        foreach (SnapshotFloorDeltaEntry entry in delta.Entries)
        {
            if (entry.Remove)
                target.Remove(entry.Hold.HoldId);
            else
                target[entry.Hold.HoldId] = FromHoldMessage(entry.Hold);
        }
    }

    private static SnapshotFloorDeltaMessage UpsertDelta(SnapshotHold hold)
    {
        SnapshotFloorDeltaMessage delta = new();
        delta.Entries.Add(new SnapshotFloorDeltaEntry { Remove = false, Hold = ToHoldMessage(hold) });
        return delta;
    }

    private static SnapshotFloorDeltaMessage RemoveDelta(string holdId)
    {
        SnapshotFloorDeltaMessage delta = new();
        delta.Entries.Add(new SnapshotFloorDeltaEntry { Remove = true, Hold = new SnapshotHoldMessage { HoldId = holdId } });
        return delta;
    }

    private static SnapshotFloorDeltaMessage RemoveDelta(IEnumerable<string> holdIds)
    {
        SnapshotFloorDeltaMessage delta = new();
        foreach (string id in holdIds)
            delta.Entries.Add(new SnapshotFloorDeltaEntry { Remove = true, Hold = new SnapshotHoldMessage { HoldId = id } });
        return delta;
    }

    // Caller holds mutateGate. Proposes one delta and waits until this node's apply reaches it, so
    // the caller's answer and this node's registry agree when it returns.
    private async Task<bool> ReplicateAsync(SnapshotFloorDeltaMessage delta, CancellationToken ct)
    {
        byte[] data = ReplicationSerializer.Serialize(delta);

        RaftReplicationResult result = await raft.ReplicateLogs(
            RangeMapStore.MetaPartitionId,
            ReplicationTypes.SnapshotFloor,
            data,
            cancellationToken: ct
        ).ConfigureAwait(false);

        if (!result.Success)
        {
            logger.LogWarning(
                "Failed to replicate snapshot-floor mutation Status={Status} Ticket={Ticket}",
                result.Status, result.TicketId);
            return false;
        }

        await WaitForApplyAsync(result.LogIndex, ct).ConfigureAwait(false);
        return true;
    }

    private void NoteApplied(long logIndex)
    {
        long current = Volatile.Read(ref appliedIndex);
        while (logIndex > current)
        {
            long seen = Interlocked.CompareExchange(ref appliedIndex, logIndex, current);
            if (seen == current)
                break;

            current = seen;
        }

        Interlocked.Exchange(ref appliedSignal, new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)).TrySetResult();
    }

    private async Task WaitForApplyAsync(long logIndex, CancellationToken ct)
    {
        long deadline = Environment.TickCount64 + (long)ApplyWaitLimit.TotalMilliseconds;

        while (Volatile.Read(ref appliedIndex) < logIndex)
        {
            Task signal = Volatile.Read(ref appliedSignal).Task;
            if (Volatile.Read(ref appliedIndex) >= logIndex)
                return;

            long left = deadline - Environment.TickCount64;
            if (left <= 0)
            {
                logger.LogWarning("Snapshot-floor entry {LogIndex} committed but did not apply on this node within {Limit}", logIndex, ApplyWaitLimit);
                return;
            }

            try
            {
                await signal.WaitAsync(TimeSpan.FromMilliseconds(left), ct).ConfigureAwait(false);
            }
            catch (TimeoutException)
            {
            }
        }
    }

    // Whether the applied registry holds holdId and no prune-delete window opened, closed, or was
    // open since pruneGenAtStart. Checked under the same lock as the apply's swap and
    // BeginPrune's floor sample, so the ordering an acquire observes is exactly the ordering
    // pruning observes: every prune that opens after this check samples a floor that includes
    // the hold.
    private bool IsAppliedAndPruneSafe(string holdId, long pruneGenAtStart)
    {
        lock (pruneCommitLock)
        {
            long genNow = pruneDeleteGen;
            return holds.ContainsKey(holdId) && genNow == pruneGenAtStart && (genNow & 1L) == 0L;
        }
    }

    // Installs the resulting registry: swaps the volatile map, rebuilds the floor cache, and
    // persists the full set to disk. Called by the ordered apply and the meta state transfer only.
    // Runs under pruneCommitLock so the swap is ordered against BeginPrune's floor sample.
    private void CommitInMemory(Dictionary<string, SnapshotHold> next)
    {
        lock (pruneCommitLock)
            CommitInMemoryLocked(next);
    }

    private void CommitInMemoryLocked(Dictionary<string, SnapshotHold> next)
    {
        holds = next;
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());
        _floorCache = BuildCache(next, now);
        PersistToDisk(ToMessage(next));
    }

    /// <summary>
    /// Serializes the complete current hold set for the P0 whole-partition state transfer that
    /// repairs a node below the WAL compaction floor. Lock-free read of the volatile registry.
    /// </summary>
    public byte[] SerializeState() => ReplicationSerializer.Serialize(ToMessage(holds));

    /// <summary>
    /// Parses (does not install) a hold set from a transfer blob. Any decode failure throws here,
    /// before <see cref="CommitState"/> mutates anything, so the unified P0 transfer can validate
    /// both meta state machines before swapping either.
    /// </summary>
    public Dictionary<string, SnapshotHold> ParseState(ReadOnlySpan<byte> data) =>
        FromMessage(ReplicationSerializer.UnserializeSnapshotFloorMessage(data));

    /// <summary>
    /// Atomically installs a parsed hold set (from <see cref="ParseState"/>) and persists it to
    /// disk. Called on a follower being repaired below the compaction floor, where no local
    /// mutation is in flight (only the meta-partition leader mutates); the volatile swap is atomic.
    /// </summary>
    public void CommitState(Dictionary<string, SnapshotHold> parsed) => CommitInMemory(parsed);

    private void PersistToDisk(SnapshotFloorMessage message)
    {
        if (snapshotPath is null)
            return;

        try
        {
            byte[] data = ReplicationSerializer.Serialize(message);
            lock (fileLock)
            {
                string tmp = snapshotPath + ".tmp";
                File.WriteAllBytes(tmp, data);
                File.Move(tmp, snapshotPath, overwrite: true);
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to persist snapshot-floor snapshot to {Path}", snapshotPath);
        }
    }

    private void LoadFromDisk()
    {
        if (snapshotPath is null || !File.Exists(snapshotPath))
            return;

        try
        {
            byte[] data;
            lock (fileLock)
                data = File.ReadAllBytes(snapshotPath);

            SnapshotFloorMessage message = ReplicationSerializer.UnserializeSnapshotFloorMessage(data);
            Dictionary<string, SnapshotHold> loaded = FromMessage(message);
            holds = loaded;
            // Prime the floor cache from the loaded registry. Reads answer from the cache, and an
            // unprimed (empty) cache reads as "no live holds" — the value that licenses reclaiming
            // everything. A restarted node whose WAL replay delivers no floor delta (everything
            // sits at or below the checkpoint) would otherwise keep the empty cache until the
            // first post-restart mutation commits, and a node elected meta leader inside that
            // window would serve a false zero floor as an authoritative, read-index-confirmed
            // answer.
            HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());
            _floorCache = BuildCache(loaded, now);

            // Arm the startup grace: loaded holds stay exempt from the expired-hold purge until
            // the deadline, so a holder whose lease lapsed during full-cluster downtime — when
            // no reclamation could run and its pinned history is therefore intact — gets this
            // window to renew (revive) the hold before the reaper ends its protection.
            if (loaded.Count > 0 && startupGraceWindow > TimeSpan.Zero)
            {
                startupGraceHoldIds = new HashSet<string>(loaded.Keys, StringComparer.Ordinal);
                startupGraceDeadline = AddMs(now, (int)Math.Min(startupGraceWindow.TotalMilliseconds, int.MaxValue));
            }

            if (logger.IsEnabled(LogLevel.Information))
                logger.LogInformation("Loaded {Count} snapshot hold(s) from {Path}", loaded.Count, snapshotPath);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to load snapshot-floor snapshot from {Path}", snapshotPath);
        }
    }

    private static SnapshotHoldMessage ToHoldMessage(SnapshotHold hold) =>
        new()
        {
            HoldId = hold.HoldId,
            HolderId = hold.HolderId,
            TimestampNode     = hold.Timestamp.N,
            TimestampPhysical = hold.Timestamp.L,
            TimestampCounter  = hold.Timestamp.C,
            LeaseExpiryNode     = hold.LeaseExpiry.N,
            LeaseExpiryPhysical = hold.LeaseExpiry.L,
            LeaseExpiryCounter  = hold.LeaseExpiry.C,
        };

    private static SnapshotHold FromHoldMessage(SnapshotHoldMessage m)
    {
        HLCTimestamp ts = new(m.TimestampNode, m.TimestampPhysical, m.TimestampCounter);
        HLCTimestamp ex = new(m.LeaseExpiryNode, m.LeaseExpiryPhysical, m.LeaseExpiryCounter);
        return new SnapshotHold(m.HoldId, m.HolderId, ts, ex);
    }

    private static SnapshotFloorMessage ToMessage(IReadOnlyDictionary<string, SnapshotHold> holdMap)
    {
        SnapshotFloorMessage message = new();
        foreach (SnapshotHold hold in holdMap.Values)
            message.Holds.Add(ToHoldMessage(hold));
        return message;
    }

    private static Dictionary<string, SnapshotHold> FromMessage(SnapshotFloorMessage message)
    {
        Dictionary<string, SnapshotHold> result = new(message.Holds.Count, StringComparer.Ordinal);
        foreach (SnapshotHoldMessage m in message.Holds)
            result[m.HoldId] = FromHoldMessage(m);
        return result;
    }

    /// <summary>
    /// Removes all purge-eligible holds whose lease has expired. Called periodically by the
    /// background reaper so that a crashed holder cannot pin MVCC history indefinitely. A purge
    /// permanently ends a hold's protection — after it commits, prunes may pass the held
    /// timestamp and a later renew fails closed — so it only runs under confirmed leadership
    /// (a stale registry could purge a hold whose holder just renewed it), and holds loaded from
    /// the durable snapshot are exempt while the startup grace window is open. Returns the number
    /// of holds purged; 0 when the registry is clean, this node is not the confirmed
    /// meta-partition leader, or every expired hold is inside the grace window.
    /// </summary>
    public async Task<int> PurgeExpiredHoldsAsync(CancellationToken ct = default)
    {
        // Fast path: nothing to purge.
        if (holds.Count == 0)
            return 0;

        await mutateGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            // Confirm before scanning so expiry is judged against a fully-applied registry: an
            // inherited renew this node has not applied yet would otherwise read as expired and
            // be purged, silently ending a protection its holder believes it extended. On a
            // follower or an unconfirmable leader the cycle is skipped and retried next tick.
            if (!await raft.ConfirmLeadershipIfHosted(RangeMapStore.MetaPartitionId, ct).ConfigureAwait(false))
                return 0;

            HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

            bool graceOpen = startupGraceHoldIds is not null && now.CompareTo(startupGraceDeadline) < 0;
            if (!graceOpen)
                startupGraceHoldIds = null;

            List<string>? expired = null;
            foreach ((string holdId, SnapshotHold hold) in holds)
            {
                if (hold.IsLive(now))
                    continue;
                if (graceOpen && startupGraceHoldIds!.Contains(holdId))
                    continue;
                expired ??= [];
                expired.Add(holdId);
            }

            if (expired is null)
                return 0;

            bool ok = await ReplicateAsync(RemoveDelta(expired), ct).ConfigureAwait(false);
            if (!ok)
            {
                logger.LogWarning(
                    "Failed to replicate snapshot-floor purge of {Count} expired hold(s)",
                    expired.Count);
                return 0;
            }

            int purgedCount = expired.Count;
            if (logger.IsEnabled(LogLevel.Information))
                logger.LogInformation("Purged {Count} expired snapshot hold(s)", purgedCount);
            return purgedCount;
        }
        finally
        {
            mutateGate.Release();
        }
    }

    private int CountLiveHolds()
    {
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());
        return GetEffectiveFloorAndCount(now).LiveCount;
    }

    private long ComputeEffectiveFloorMs()
    {
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());
        return GetEffectiveFloor(now).L;
    }

    /// <summary>Returns an HLCTimestamp that is <paramref name="ms"/> milliseconds after <paramref name="origin"/>.</summary>
    private static HLCTimestamp AddMs(HLCTimestamp origin, int ms)
    {
        // HLC physical component is in milliseconds (Kommander convention).
        return new HLCTimestamp(origin.N, origin.L + ms, origin.C);
    }

    public void Dispose()
    {
        // Disposing the per-instance Meter removes the observable gauge callbacks and releases
        // the closures capturing `this`, allowing the store to be garbage-collected.
        _instanceMeter.Dispose();
        mutateGate.Dispose();
    }
}
