
using System.Diagnostics.Metrics;
using Kommander;
using Kommander.Data;
using Kommander.Time;

using Kahuna.Server.Replication;
using Kahuna.Server.Replication.Protos;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Ranges;

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
/// timestamp; it is live iff <c>leaseExpiry &gt; currentHlc</c>. Expired holds are excluded from
/// the effective floor but remain in the registry until explicitly released or overwritten by a
/// subsequent acquire on the same (holderId, timestamp). Automatic reclamation of expired entries
/// is deferred to a later task. Lease comparisons
/// always use the cluster HLC, never wall-clock time.</para>
///
/// <para><b>Single writer.</b> <see cref="AcquireAsync"/>, <see cref="RenewAsync"/>, and
/// <see cref="ReleaseAsync"/> are the only mutators, each serialized by
/// <see cref="mutateGate"/> locally and by the meta Raft log globally. Followers and restore
/// replays call <see cref="Replicate"/> / <see cref="Restore"/>.</para>
/// </summary>
internal sealed class SnapshotFloorStore : IDisposable
{
    private readonly IRaft raft;

    private readonly ILogger<IKahuna> logger;

    private readonly SemaphoreSlim mutateGate = new(1, 1);

    private readonly string? snapshotPath;

    private readonly object fileLock = new();

    // Per-instance Meter owns the observable gauges. Disposing it removes the gauge callbacks
    // and breaks the strong reference from the meter to the capturing lambdas, so a disposed
    // store can be garbage-collected even though SnapshotFloorMetrics.Meter is static.
    private readonly Meter _instanceMeter;
    private readonly ObservableGauge<int>  liveHoldsGauge;
    private readonly ObservableGauge<long> effectiveFloorMsGauge;

    /// <summary>
    /// The committed hold registry. Written only inside <see cref="AcquireAsync"/>,
    /// <see cref="RenewAsync"/>, <see cref="ReleaseAsync"/>, or <see cref="Apply"/>;
    /// read lock-free by <see cref="GetEffectiveFloor"/> and the hold introspection path.
    /// </summary>
    private volatile IReadOnlyDictionary<string, SnapshotHold> holds =
        new Dictionary<string, SnapshotHold>(StringComparer.Ordinal);

    /// <summary>
    /// Incremented on every hold mutation (acquire, renew, release, purge, restore/replicate).
    /// Used by <see cref="GetFloorForPrune"/> to detect that a mutation landed between an
    /// epoch read and a floor sample, so the floor can be re-sampled.
    /// </summary>
    private int mutationEpoch;

    /// <summary>
    /// Cached result of the last O(N) floor scan. Updated on every mutation and valid until
    /// <see cref="FloorCacheState.NextExpiry"/> is reached (at which point a hold may have
    /// expired and the slow scan is needed again). Between mutations the cache is conservative:
    /// it may report a floor lower than the true floor (if a hold expired but was not yet purged),
    /// which is safe for reclamation decisions (keeps more revisions, never fewer).
    /// </summary>
    private volatile FloorCacheState _floorCache = FloorCacheState.Empty;

    private sealed class FloorCacheState
    {
        public static readonly FloorCacheState Empty = new(HLCTimestamp.Zero, 0, HLCTimestamp.Zero);

        public readonly HLCTimestamp Floor;
        public readonly int LiveCount;

        /// <summary>
        /// The minimum <see cref="SnapshotHold.LeaseExpiry"/> of all live holds at cache-fill
        /// time. The cache is definitely valid while <c>currentTime &lt; NextExpiry</c>. Zero
        /// means no live holds were present (cache always valid in that case).
        /// </summary>
        public readonly HLCTimestamp NextExpiry;

        public FloorCacheState(HLCTimestamp floor, int liveCount, HLCTimestamp nextExpiry)
        {
            Floor = floor;
            LiveCount = liveCount;
            NextExpiry = nextExpiry;
        }
    }

    public SnapshotFloorStore(
        IRaft raft,
        string? storagePath,
        string? storageRevision,
        ILogger<IKahuna> logger)
    {
        this.raft = raft;
        this.logger = logger;

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
        if (cache.LiveCount == 0)
            return (HLCTimestamp.Zero, 0);
        // Fast path: no hold has expired since the cache was filled.
        if (currentTime.CompareTo(cache.NextExpiry) < 0)
            return (cache.Floor, cache.LiveCount);
        // Slow path: at least one hold may have expired; recompute without updating the cache
        // (the cache is authoritatively updated only by mutations so we avoid the race).
        return ScanFloorAndCount(holds, currentTime);
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
        int liveCount = 0;
        foreach (SnapshotHold hold in snapshot.Values)
        {
            if (!hold.IsLive(currentTime))
                continue;
            liveCount++;
            if (floor == HLCTimestamp.Zero || hold.Timestamp.CompareTo(floor) < 0)
                floor = hold.Timestamp;
            if (nextExpiry == HLCTimestamp.Zero || hold.LeaseExpiry.CompareTo(nextExpiry) < 0)
                nextExpiry = hold.LeaseExpiry;
        }
        return liveCount == 0 ? FloorCacheState.Empty : new(floor, liveCount, nextExpiry);
    }

    /// <summary>
    /// Samples the effective floor for use by an off-actor prune task.
    ///
    /// <para><b>What this closes.</b> Moving the sample inside the prune callback (rather than
    /// reading it on the actor thread before <c>EnqueueTask</c>) eliminates the
    /// scheduler-queue-latency window: a hold acquired while the task is queued but before it
    /// starts executing is now always observed. The epoch-retry loop further tightens the sample
    /// itself: if a <see cref="mutationEpoch"/> change is detected between the two epoch reads
    /// that bracket the floor scan, the scan is repeated so the returned value reflects the
    /// mutation.</para>
    ///
    /// <para><b>Residual window.</b> A hold acquired in the interval
    /// [<c>GetFloorForPrune</c> returns → <c>PruneKeyValueRevisions</c> completes] is still
    /// invisible to the running prune batch — the floor was sampled before the hold arrived and
    /// the delete cannot be un-done. This window is now micro- to low-millisecond (sample and
    /// delete are adjacent, no await between) versus the old scheduler-queue-latency window.
    /// Full closure would require mutual exclusion between acquire and the prune delete (e.g.
    /// holding <see cref="mutateGate"/> across the backend call), which was deferred as the
    /// residual probability is very low and CamusDB fork-points are typically well above the
    /// prune horizon.</para>
    ///
    /// <para>May be called from the scheduler thread — <see cref="holds"/> is a volatile
    /// copy-on-write dict and <see cref="IRaft.HybridLogicalClock"/> is thread-safe.</para>
    /// </summary>
    public HLCTimestamp GetFloorForPrune(IRaft raftClock)
    {
        int epoch1, epoch2;
        HLCTimestamp floor;
        do
        {
            epoch1 = Volatile.Read(ref mutationEpoch);
            if (holds.Count == 0)
                return HLCTimestamp.Zero;
            HLCTimestamp now = raftClock.HybridLogicalClock.TrySendOrLocalEvent(raftClock.GetLocalNodeId());
            floor = GetEffectiveFloor(now);
            epoch2 = Volatile.Read(ref mutationEpoch);
        }
        while (epoch1 != epoch2);
        return floor;
    }

    /// <summary>
    /// Acquires or renews a hold. Idempotent by (holderId, timestamp): a repeat returns the same
    /// holdId and renews the lease. Only the meta-partition leader can commit holds; followers
    /// return <see cref="KeyValueResponseType.MustRetry"/>.
    /// </summary>
    public async Task<(KeyValueResponseType Type, string HoldId, HLCTimestamp LeaseExpiry)> AcquireAsync(
        string holderId,
        HLCTimestamp timestamp,
        int leaseMs,
        CancellationToken ct)
    {
        if (string.IsNullOrEmpty(holderId))
            return (KeyValueResponseType.Errored, string.Empty, HLCTimestamp.Zero);

        if (leaseMs <= 0)
            return (KeyValueResponseType.InvalidInput, string.Empty, HLCTimestamp.Zero);

        await mutateGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());
            HLCTimestamp expiry = AddMs(now, leaseMs);

            // Check for an existing hold with the same (holderId, timestamp) — idempotent acquire.
            SnapshotHold? existing = null;
            foreach (SnapshotHold h in holds.Values)
            {
                if (h.HolderId == holderId && h.Timestamp == timestamp)
                {
                    existing = h;
                    break;
                }
            }

            string holdId = existing?.HoldId ?? Guid.NewGuid().ToString("N");

            SnapshotHold hold = new(holdId, holderId, timestamp, expiry);
            Dictionary<string, SnapshotHold> next = new(holds, StringComparer.Ordinal);
            next[holdId] = hold;

            bool ok = await ReplicateDeltaAsync(UpsertDelta(hold), next, ct).ConfigureAwait(false);
            if (!ok)
                return (KeyValueResponseType.MustRetry, string.Empty, HLCTimestamp.Zero);

            return (KeyValueResponseType.Set, holdId, expiry);
        }
        finally
        {
            mutateGate.Release();
        }
    }

    /// <summary>
    /// Renews the lease on an existing hold. Returns <see cref="KeyValueResponseType.DoesNotExist"/>
    /// when the holdId does not exist or has already expired.
    /// </summary>
    public async Task<(KeyValueResponseType Type, HLCTimestamp LeaseExpiry)> RenewAsync(
        string holdId,
        int leaseMs,
        CancellationToken ct)
    {
        if (string.IsNullOrEmpty(holdId))
            return (KeyValueResponseType.Errored, HLCTimestamp.Zero);

        if (leaseMs <= 0)
            return (KeyValueResponseType.InvalidInput, HLCTimestamp.Zero);

        await mutateGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            if (!holds.TryGetValue(holdId, out SnapshotHold? hold))
                return (KeyValueResponseType.DoesNotExist, HLCTimestamp.Zero);

            HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

            if (!hold.IsLive(now))
                return (KeyValueResponseType.DoesNotExist, HLCTimestamp.Zero);

            HLCTimestamp expiry = AddMs(now, leaseMs);

            SnapshotHold renewed = hold with { LeaseExpiry = expiry };
            Dictionary<string, SnapshotHold> next = new(holds, StringComparer.Ordinal);
            next[holdId] = renewed;

            bool ok = await ReplicateDeltaAsync(UpsertDelta(renewed), next, ct).ConfigureAwait(false);
            if (!ok)
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
            if (!holds.ContainsKey(holdId))
                return KeyValueResponseType.DoesNotExist;

            Dictionary<string, SnapshotHold> next = new(holds, StringComparer.Ordinal);
            next.Remove(holdId);

            bool ok = await ReplicateDeltaAsync(RemoveDelta(holdId), next, ct).ConfigureAwait(false);
            return ok ? KeyValueResponseType.Deleted : KeyValueResponseType.MustRetry;
        }
        finally
        {
            mutateGate.Release();
        }
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
            Dictionary<string, SnapshotHold> next = new(holds, StringComparer.Ordinal);
            ApplyDeltaEntries(next, delta);
            CommitInMemory(next);
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

    // Caller holds mutateGate. Replicates the delta, then eagerly commits the already-computed
    // resulting registry so the leader reads its own writes without waiting for the commit echo.
    private async Task<bool> ReplicateDeltaAsync(
        SnapshotFloorDeltaMessage delta, Dictionary<string, SnapshotHold> next, CancellationToken ct)
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

        CommitInMemory(next);
        return true;
    }

    // Installs the resulting registry: swaps the volatile map, rebuilds the floor cache before
    // bumping the epoch (so a concurrent GetFloorForPrune re-samples), and persists the full set
    // to disk. Idempotent — safe to run for both the eager leader commit and the ordered echo.
    private void CommitInMemory(Dictionary<string, SnapshotHold> next)
    {
        holds = next;
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());
        _floorCache = BuildCache(next, now);
        Interlocked.Increment(ref mutationEpoch);
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
            holds = FromMessage(message);
            if (logger.IsEnabled(LogLevel.Information))
                logger.LogInformation("Loaded {Count} snapshot hold(s) from {Path}", holds.Count, snapshotPath);
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
    /// Removes all holds whose lease has expired. Called periodically by the background reaper
    /// so that a crashed holder cannot pin MVCC history indefinitely. Returns the number of
    /// holds purged; 0 when the registry is clean or this node is not the meta-partition leader.
    /// </summary>
    public async Task<int> PurgeExpiredHoldsAsync(CancellationToken ct = default)
    {
        // Fast path: nothing to purge.
        if (holds.Count == 0)
            return 0;

        await mutateGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

            List<string>? expired = null;
            foreach ((string holdId, SnapshotHold hold) in holds)
            {
                if (!hold.IsLive(now))
                {
                    expired ??= [];
                    expired.Add(holdId);
                }
            }

            if (expired is null)
                return 0;

            Dictionary<string, SnapshotHold> next = new(holds, StringComparer.Ordinal);
            foreach (string id in expired)
                next.Remove(id);

            bool ok = await ReplicateDeltaAsync(RemoveDelta(expired), next, ct).ConfigureAwait(false);
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
