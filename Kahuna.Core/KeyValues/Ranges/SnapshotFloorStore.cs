
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
/// <para><b>Replication model.</b> Holds are stored as a full-snapshot Raft log on the
/// <see cref="RangeMapStore.MetaPartitionId"/> system partition (id 0), using
/// <see cref="ReplicationTypes.SnapshotFloor"/> as the log type — the same partition and mechanism
/// used by <see cref="RangeMapStore"/>. Each committed entry is a complete replacement of the hold
/// registry, so replaying the meta log on restore/failover converges on the latest set and
/// re-applying the same entry is idempotent.</para>
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

    // ObservableGauge instruments — registered once per instance, captured via lambda.
    // The Meter holds a strong reference to the callbacks so they live as long as the meter.
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

        // Register per-instance observable gauges so live state is always reflected.
        liveHoldsGauge = SnapshotFloorMetrics.Meter.CreateObservableGauge(
            "kahuna.snapshot_floor.live_holds",
            () => CountLiveHolds(),
            description: "Number of currently live (non-expired) snapshot holds.");

        effectiveFloorMsGauge = SnapshotFloorMetrics.Meter.CreateObservableGauge(
            "kahuna.snapshot_floor.effective_floor_ms",
            () => ComputeEffectiveFloorMs(),
            description: "Physical (millisecond) component of the effective snapshot floor, or 0 when no hold is active.");
    }

    /// <summary>The committed hold set. Lock-free read.</summary>
    public IReadOnlyDictionary<string, SnapshotHold> Holds => holds;

    /// <summary>
    /// Computes the effective floor: the minimum <see cref="SnapshotHold.Timestamp"/> among all
    /// currently live holds. Returns <see cref="HLCTimestamp.Zero"/> when no hold is live.
    /// </summary>
    public HLCTimestamp GetEffectiveFloor(HLCTimestamp currentTime)
    {
        HLCTimestamp floor = HLCTimestamp.Zero;
        foreach (SnapshotHold hold in holds.Values)
        {
            if (!hold.IsLive(currentTime))
                continue;
            if (floor == HLCTimestamp.Zero || hold.Timestamp.CompareTo(floor) < 0)
                floor = hold.Timestamp;
        }
        return floor;
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

            Dictionary<string, SnapshotHold> next = new(holds, StringComparer.Ordinal);
            next[holdId] = new SnapshotHold(holdId, holderId, timestamp, expiry);

            bool ok = await ReplicateAsync(next, ct).ConfigureAwait(false);
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

        await mutateGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            if (!holds.TryGetValue(holdId, out SnapshotHold? hold))
                return (KeyValueResponseType.DoesNotExist, HLCTimestamp.Zero);

            HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

            if (!hold.IsLive(now))
                return (KeyValueResponseType.DoesNotExist, HLCTimestamp.Zero);

            HLCTimestamp expiry = AddMs(now, leaseMs);

            Dictionary<string, SnapshotHold> next = new(holds, StringComparer.Ordinal);
            next[holdId] = hold with { LeaseExpiry = expiry };

            bool ok = await ReplicateAsync(next, ct).ConfigureAwait(false);
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

            bool ok = await ReplicateAsync(next, ct).ConfigureAwait(false);
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
            SnapshotFloorMessage message = ReplicationSerializer.UnserializeSnapshotFloorMessage(log.LogData);
            Dictionary<string, SnapshotHold> rebuilt = FromMessage(message);
            holds = rebuilt;
            Interlocked.Increment(ref mutationEpoch);
            PersistToDisk(message);
            return true;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to apply snapshot-floor log on partition {Partition}", partitionId);
            return false;
        }
    }

    // Caller holds mutateGate.
    private async Task<bool> ReplicateAsync(Dictionary<string, SnapshotHold> next, CancellationToken ct)
    {
        SnapshotFloorMessage message = ToMessage(next);
        byte[] data = ReplicationSerializer.Serialize(message);

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

        holds = next;
        Interlocked.Increment(ref mutationEpoch);
        PersistToDisk(message);
        return true;
    }

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

    private static SnapshotFloorMessage ToMessage(IReadOnlyDictionary<string, SnapshotHold> holdMap)
    {
        SnapshotFloorMessage message = new();
        foreach (SnapshotHold hold in holdMap.Values)
        {
            message.Holds.Add(new SnapshotHoldMessage
            {
                HoldId = hold.HoldId,
                HolderId = hold.HolderId,
                TimestampNode     = hold.Timestamp.N,
                TimestampPhysical = hold.Timestamp.L,
                TimestampCounter  = hold.Timestamp.C,
                LeaseExpiryNode     = hold.LeaseExpiry.N,
                LeaseExpiryPhysical = hold.LeaseExpiry.L,
                LeaseExpiryCounter  = hold.LeaseExpiry.C,
            });
        }
        return message;
    }

    private static Dictionary<string, SnapshotHold> FromMessage(SnapshotFloorMessage message)
    {
        Dictionary<string, SnapshotHold> result = new(message.Holds.Count, StringComparer.Ordinal);
        foreach (SnapshotHoldMessage m in message.Holds)
        {
            HLCTimestamp ts = new(m.TimestampNode, m.TimestampPhysical, m.TimestampCounter);
            HLCTimestamp ex = new(m.LeaseExpiryNode, m.LeaseExpiryPhysical, m.LeaseExpiryCounter);
            result[m.HoldId] = new SnapshotHold(m.HoldId, m.HolderId, ts, ex);
        }
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

            bool ok = await ReplicateAsync(next, ct).ConfigureAwait(false);
            if (!ok)
            {
                logger.LogWarning(
                    "Failed to replicate snapshot-floor purge of {Count} expired hold(s)",
                    expired.Count);
                return 0;
            }

            logger.LogInformation("Purged {Count} expired snapshot hold(s)", expired.Count);
            return expired.Count;
        }
        finally
        {
            mutateGate.Release();
        }
    }

    private int CountLiveHolds()
    {
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());
        int count = 0;
        foreach (SnapshotHold hold in holds.Values)
        {
            if (hold.IsLive(now))
                count++;
        }
        return count;
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
        // The observable gauges are owned by their Meter and do not implement IDisposable.
        mutateGate.Dispose();
    }
}
