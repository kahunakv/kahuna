using System.Diagnostics.Metrics;

using Kommander;
using Kommander.Data;
using Kommander.Time;

using Kahuna.Server.Replication;
using Kahuna.Server.Replication.Protos;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Ranges;

/// <summary>The bounds of the history hold registry. Zero or less means no bound.</summary>
internal readonly record struct HistoryHoldLimits(int MaxLeaseMs, int MaxHoldsPerHolder, int MaxHolds);

/// <summary>
/// Replicated, leased registry of history holds. A history hold keeps the WAL of one data partition retained from a
/// timestamp onward on every replica, so a watch can resume from that timestamp on any of them, also after a restart
/// or a replica move. Each replica pins its own WAL from this registry (see <see cref="HistoryHoldPinner"/>).
///
/// <para><b>Replication model.</b> The same as <see cref="SnapshotFloorStore"/>: each mutation replicates an idempotent
/// delta keyed by hold id on the meta partition (<see cref="RangeMapStore.MetaPartitionId"/>) under
/// <see cref="ReplicationTypes.HistoryHold"/>; a node below the meta partition's compaction floor is repaired by the
/// meta state transfer, which carries the whole registry; and a durable on-disk copy written on every mutation lets a
/// cold restart rebuild the registry, and pin, before the node applies anything.</para>
///
/// <para><b>Lease semantics.</b> A hold is live while its lease expiry (an HLC time) is later than the current HLC.
/// Liveness decides only what the reaper may purge; the pin honors every registered hold, live or expired, until a
/// replicated release or purge commits. A lapsed hold that is still registered can be renewed, but only after the
/// node confirms its meta-partition leadership, so a fresh leader that has not applied an inherited purge cannot revive
/// a hold whose pin already ended. Holds loaded from disk are exempt from the purge for a startup grace window.</para>
///
/// <para><b>Availability, not safety.</b> A replica can compact its WAL before it applies a new hold, so a hold makes a
/// resume likely, not certain. Safety comes from the replay, which answers <c>HistoryUnavailable</c> and never a silent
/// gap. That is why an acquire needs no prune-window check, unlike a snapshot hold.</para>
///
/// <para><b>Partition identity.</b> A partition id is never handed out again (the range splitter allocates past every
/// id ever used, and Kommander keeps a floor of the highest id ever created), so the id alone identifies one
/// incarnation. The reaper purges the holds of a partition that is retired or no longer exists.</para>
///
/// <para><b>Single writer.</b> <see cref="AcquireAsync"/>, <see cref="RenewAsync"/>, <see cref="ReleaseAsync"/> and
/// <see cref="PurgeAsync"/> are the only mutators, serialized by <see cref="mutateGate"/> locally and by the meta Raft
/// log globally. Followers and restore replays call <see cref="Replicate"/> / <see cref="Restore"/>.</para>
/// </summary>
internal sealed class HistoryHoldStore : IDisposable
{
    private readonly IRaft raft;

    private readonly ILogger<IKahuna> logger;

    private readonly HistoryHoldLimits limits;

    private readonly SemaphoreSlim mutateGate = new(1, 1);

    private readonly string? snapshotPath;

    private readonly object fileLock = new();

    private readonly TimeSpan startupGraceWindow;

    // The gauges read this store, so they live on a meter owned by it and disposed with it.
    private readonly Meter gaugeMeter = new("Kahuna", "1.0");

    private static readonly Counter<long> RefusedByLimit =
        Watch.KeyValueWatchMetrics.Meter.CreateCounter<long>(
            "kahuna.kv.history_holds.refused_total",
            description: "History hold acquires refused because a registry bound was reached, by bound (holder, total).");

    private static readonly Counter<long> Purged =
        Watch.KeyValueWatchMetrics.Meter.CreateCounter<long>(
            "kahuna.kv.history_holds.purged_total",
            description: "History holds the reaper purged, by reason (expired, partition_gone).");

    /// <summary>The committed registry. Replaced whole on every commit; read lock-free.</summary>
    private volatile IReadOnlyDictionary<string, HistoryHold> holds = new Dictionary<string, HistoryHold>(StringComparer.Ordinal);

    // Ids of the holds the durable copy held at load, exempt from the purge until the deadline. Read and cleared only
    // under mutateGate.
    private HashSet<string>? startupGraceHoldIds;

    private HLCTimestamp startupGraceDeadline;

    // Whether a partition still exists and is not retired, for the purge. Attached once the range map exists.
    private Func<int, bool>? partitionLive;

    public HistoryHoldStore(IRaft raft, string? storagePath, string? storageRevision, ILogger<IKahuna> logger, HistoryHoldLimits limits, TimeSpan startupGraceWindow = default)
    {
        this.raft = raft;
        this.logger = logger;
        this.limits = limits;
        this.startupGraceWindow = startupGraceWindow;

        snapshotPath = string.IsNullOrEmpty(storagePath)
            ? null
            : Path.Combine(storagePath, $"historyholds_{storageRevision}.snapshot");

        LoadFromDisk();

        gaugeMeter.CreateObservableGauge(
            "kahuna.kv.history_holds.live",
            CountLiveHolds,
            description: "Live (unexpired) history holds in the registry this node holds.");

        gaugeMeter.CreateObservableGauge(
            "kahuna.kv.history_holds.lowest_timestamp_age_ms",
            ObserveLowestTimestampAges,
            unit: "ms",
            description: "Age of the lowest held timestamp on each data partition, by partition. A hold that is never advanced keeps this growing.");
    }

    /// <summary>Raised after every committed change of the registry, on the committing thread. Handlers must not
    /// block: the apply path raises it.</summary>
    public event Action? Changed;

    /// <summary>The committed registry. Lock-free read.</summary>
    public IReadOnlyDictionary<string, HistoryHold> Holds => holds;

    /// <summary>Wires the partition liveness check the purge uses (manager construction).</summary>
    public void AttachPartitionLiveness(Func<int, bool> isLive) => partitionLive = isLive;

    /// <summary>The lowest timestamp over every registered hold of each partition, expired or not: what each replica
    /// must keep its WAL retained from.</summary>
    public Dictionary<int, HLCTimestamp> LowestTimestamps()
    {
        Dictionary<int, HLCTimestamp> lowest = [];

        foreach (HistoryHold hold in holds.Values)
        {
            if (!lowest.TryGetValue(hold.PartitionId, out HLCTimestamp current) || hold.Timestamp < current)
                lowest[hold.PartitionId] = hold.Timestamp;
        }

        return lowest;
    }

    /// <summary>Whether any hold is registered on <paramref name="partitionId"/>.</summary>
    public bool HasHolds(int partitionId)
    {
        foreach (HistoryHold hold in holds.Values)
        {
            if (hold.PartitionId == partitionId)
                return true;
        }

        return false;
    }

    /// <summary>The registered holds, of <paramref name="holderId"/> when given.</summary>
    public List<HistoryHoldInfo> List(string? holderId)
    {
        List<HistoryHoldInfo> found = [];

        foreach (HistoryHold hold in holds.Values)
        {
            if (holderId is null || string.Equals(hold.HolderId, holderId, StringComparison.Ordinal))
                found.Add(ToInfo(hold));
        }

        found.Sort(static (a, b) => a.PartitionId != b.PartitionId ? a.PartitionId.CompareTo(b.PartitionId) : string.CompareOrdinal(a.HoldId, b.HoldId));
        return found;
    }

    /// <summary>
    /// Registers one hold per partition for <paramref name="holderId"/>, from <paramref name="timestamp"/>. Idempotent
    /// by (holder, partition): an existing hold is renewed and moved to <paramref name="timestamp"/>, which must not be
    /// below its current timestamp. A lapsed existing hold is revived only under confirmed meta-partition leadership.
    /// Only the meta-partition leader commits; the caller routes there.
    /// </summary>
    public async Task<HistoryHoldAnswer> AcquireAsync(string holderId, IReadOnlyList<int> partitions, HLCTimestamp timestamp, int leaseMs, CancellationToken ct)
    {
        if (string.IsNullOrEmpty(holderId) || partitions.Count == 0 || timestamp == HLCTimestamp.Zero || !LeaseAllowed(leaseMs))
            return HistoryHoldAnswer.Refused(KeyValueResponseType.InvalidInput);

        Func<int, bool>? isLive = partitionLive;
        foreach (int partitionId in partitions)
        {
            // Only a data partition that exists can be held: the reaper would purge any other hold at once.
            if (partitionId == RangeMapStore.MetaPartitionId || partitionId < 0 || (isLive is not null && !isLive(partitionId)))
                return HistoryHoldAnswer.Refused(KeyValueResponseType.InvalidInput);
        }

        await mutateGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            HLCTimestamp now = Now();

            List<HistoryHold?> existing = new(partitions.Count);
            bool revives = false;

            foreach (int partitionId in partitions.Distinct())
            {
                HistoryHold? match = Find(holderId, partitionId);
                if (match is not null && timestamp < match.Timestamp)
                    return HistoryHoldAnswer.Refused(KeyValueResponseType.InvalidInput);

                revives |= match is not null && !match.IsLive(now);
                existing.Add(match);
            }

            if (revives)
            {
                if (!await ConfirmLeadership(ct).ConfigureAwait(false))
                    return HistoryHoldAnswer.Refused(KeyValueResponseType.MustRetry);

                // The confirmation may have applied a committed purge or release: re-read.
                now = Now();
                existing.Clear();
                foreach (int partitionId in partitions.Distinct())
                    existing.Add(Find(holderId, partitionId));
            }

            int fresh = existing.Count(e => e is null);
            if (fresh > 0)
            {
                int ofHolder = holds.Values.Count(h => string.Equals(h.HolderId, holderId, StringComparison.Ordinal));

                if (limits.MaxHoldsPerHolder > 0 && ofHolder + fresh > limits.MaxHoldsPerHolder)
                {
                    RefusedByLimit.Add(1, new KeyValuePair<string, object?>("bound", "holder"));
                    return HistoryHoldAnswer.Refused(KeyValueResponseType.HistoryHoldLimitReached);
                }

                if (limits.MaxHolds > 0 && holds.Count + fresh > limits.MaxHolds)
                {
                    RefusedByLimit.Add(1, new KeyValuePair<string, object?>("bound", "total"));
                    return HistoryHoldAnswer.Refused(KeyValueResponseType.HistoryHoldLimitReached);
                }
            }

            HLCTimestamp expiry = AddMs(now, leaseMs);
            Dictionary<string, HistoryHold> next = new(holds, StringComparer.Ordinal);
            HistoryHoldDeltaMessage delta = new();
            List<HistoryHoldInfo> touched = new(existing.Count);

            int index = 0;
            foreach (int partitionId in partitions.Distinct())
            {
                HistoryHold? match = existing[index++];
                HistoryHold hold = new(match?.HoldId ?? Guid.NewGuid().ToString("N"), holderId, partitionId, timestamp, expiry);

                next[hold.HoldId] = hold;
                delta.Entries.Add(new HistoryHoldDeltaEntry { Remove = false, Hold = ToMessage(hold) });
                touched.Add(ToInfo(hold));
            }

            if (!await ReplicateAsync(delta, ct).ConfigureAwait(false))
                return HistoryHoldAnswer.Refused(KeyValueResponseType.MustRetry);

            Commit(next);
            return new(KeyValueResponseType.Set, touched);
        }
        finally
        {
            mutateGate.Release();
        }
    }

    /// <summary>
    /// Renews the lease of every named hold and, when <paramref name="timestamp"/> is not zero, moves each forward to
    /// it. All or nothing: a missing hold answers <see cref="KeyValueResponseType.DoesNotExist"/> (its history must be
    /// presumed reclaimed), and a timestamp below a hold's current one answers <see cref="KeyValueResponseType.InvalidInput"/>.
    /// </summary>
    public async Task<HistoryHoldAnswer> RenewAsync(IReadOnlyList<string> holdIds, int leaseMs, HLCTimestamp timestamp, CancellationToken ct)
    {
        if (holdIds.Count == 0 || !LeaseAllowed(leaseMs))
            return HistoryHoldAnswer.Refused(KeyValueResponseType.InvalidInput);

        await mutateGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            HLCTimestamp now = Now();
            bool revives = false;

            foreach (string holdId in holdIds)
            {
                if (!holds.TryGetValue(holdId, out HistoryHold? hold))
                    return HistoryHoldAnswer.Refused(KeyValueResponseType.DoesNotExist);

                if (timestamp != HLCTimestamp.Zero && timestamp < hold.Timestamp)
                    return HistoryHoldAnswer.Refused(KeyValueResponseType.InvalidInput);

                revives |= !hold.IsLive(now);
            }

            if (revives)
            {
                // Still registered must be proven against a fully applied registry: confirmed leadership waits for the
                // applied frontier to reach the commit frontier, so the re-read below is authoritative.
                if (!await ConfirmLeadership(ct).ConfigureAwait(false))
                    return HistoryHoldAnswer.Refused(KeyValueResponseType.MustRetry);

                foreach (string holdId in holdIds)
                {
                    if (!holds.ContainsKey(holdId))
                        return HistoryHoldAnswer.Refused(KeyValueResponseType.DoesNotExist);
                }

                now = Now();
            }

            HLCTimestamp expiry = AddMs(now, leaseMs);
            Dictionary<string, HistoryHold> next = new(holds, StringComparer.Ordinal);
            HistoryHoldDeltaMessage delta = new();
            List<HistoryHoldInfo> touched = new(holdIds.Count);

            foreach (string holdId in holdIds.Distinct(StringComparer.Ordinal))
            {
                HistoryHold renewed = holds[holdId] with
                {
                    LeaseExpiry = expiry,
                    Timestamp = timestamp == HLCTimestamp.Zero ? holds[holdId].Timestamp : timestamp
                };

                next[holdId] = renewed;
                delta.Entries.Add(new HistoryHoldDeltaEntry { Remove = false, Hold = ToMessage(renewed) });
                touched.Add(ToInfo(renewed));
            }

            if (!await ReplicateAsync(delta, ct).ConfigureAwait(false))
                return HistoryHoldAnswer.Refused(KeyValueResponseType.MustRetry);

            Commit(next);
            return new(KeyValueResponseType.Set, touched);
        }
        finally
        {
            mutateGate.Release();
        }
    }

    /// <summary>Releases every named hold that is registered. <see cref="KeyValueResponseType.DoesNotExist"/> when none
    /// of them is.</summary>
    public async Task<HistoryHoldAnswer> ReleaseAsync(IReadOnlyList<string> holdIds, CancellationToken ct)
    {
        if (holdIds.Count == 0)
            return HistoryHoldAnswer.Refused(KeyValueResponseType.InvalidInput);

        await mutateGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            List<HistoryHold> found = [];
            foreach (string holdId in holdIds.Distinct(StringComparer.Ordinal))
            {
                if (holds.TryGetValue(holdId, out HistoryHold? hold))
                    found.Add(hold);
            }

            if (found.Count == 0)
                return HistoryHoldAnswer.Refused(KeyValueResponseType.DoesNotExist);

            Dictionary<string, HistoryHold> next = new(holds, StringComparer.Ordinal);
            HistoryHoldDeltaMessage delta = new();

            foreach (HistoryHold hold in found)
            {
                next.Remove(hold.HoldId);
                delta.Entries.Add(new HistoryHoldDeltaEntry { Remove = true, Hold = new HistoryHoldMessage { HoldId = hold.HoldId } });
            }

            if (!await ReplicateAsync(delta, ct).ConfigureAwait(false))
                return HistoryHoldAnswer.Refused(KeyValueResponseType.MustRetry);

            Commit(next);
            return new(KeyValueResponseType.Deleted, found.ConvertAll(ToInfo));
        }
        finally
        {
            mutateGate.Release();
        }
    }

    /// <summary>
    /// Removes the holds whose lease expired (outside the startup grace) and the holds of partitions that are retired
    /// or no longer exist. A purge ends a hold's pin for good, so it runs only under confirmed meta-partition
    /// leadership. Returns the number of holds purged.
    /// </summary>
    public async Task<int> PurgeAsync(CancellationToken ct = default)
    {
        if (holds.Count == 0)
            return 0;

        await mutateGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            if (!await ConfirmLeadership(ct).ConfigureAwait(false))
                return 0;

            HLCTimestamp now = Now();

            bool graceOpen = startupGraceHoldIds is not null && now < startupGraceDeadline;
            if (!graceOpen)
                startupGraceHoldIds = null;

            Func<int, bool>? isLive = partitionLive;
            List<HistoryHold> expired = [];
            List<HistoryHold> gone = [];

            foreach (HistoryHold hold in holds.Values)
            {
                if (isLive is not null && !isLive(hold.PartitionId))
                {
                    gone.Add(hold);
                    continue;
                }

                if (hold.IsLive(now) || (graceOpen && startupGraceHoldIds!.Contains(hold.HoldId)))
                    continue;

                expired.Add(hold);
            }

            if (expired.Count == 0 && gone.Count == 0)
                return 0;

            Dictionary<string, HistoryHold> next = new(holds, StringComparer.Ordinal);
            HistoryHoldDeltaMessage delta = new();

            foreach (HistoryHold hold in expired.Concat(gone))
            {
                next.Remove(hold.HoldId);
                delta.Entries.Add(new HistoryHoldDeltaEntry { Remove = true, Hold = new HistoryHoldMessage { HoldId = hold.HoldId } });
            }

            if (!await ReplicateAsync(delta, ct).ConfigureAwait(false))
            {
                logger.LogWarning("Failed to replicate the purge of {Count} history hold(s)", delta.Entries.Count);
                return 0;
            }

            Commit(next);

            if (expired.Count > 0)
                Purged.Add(expired.Count, new KeyValuePair<string, object?>("reason", "expired"));

            if (gone.Count > 0)
                Purged.Add(gone.Count, new KeyValuePair<string, object?>("reason", "partition_gone"));

            if (logger.IsEnabled(LogLevel.Information))
                logger.LogInformation("Purged {Expired} expired history hold(s) and {Gone} of partitions that no longer exist", expired.Count, gone.Count);

            return delta.Entries.Count;
        }
        finally
        {
            mutateGate.Release();
        }
    }

    /// <summary>Rebuilds the registry from a meta-log entry replayed during WAL restore.</summary>
    public bool Restore(int partitionId, RaftLog log) => Apply(partitionId, log);

    /// <summary>Applies a committed meta-log entry received through replication.</summary>
    public bool Replicate(int partitionId, RaftLog log) => Apply(partitionId, log);

    private bool Apply(int partitionId, RaftLog log)
    {
        if (partitionId != RangeMapStore.MetaPartitionId || log.LogType != ReplicationTypes.HistoryHold || log.LogData is null)
            return true;

        try
        {
            HistoryHoldDeltaMessage delta = ReplicationSerializer.UnserializeHistoryHoldDeltaMessage(log.LogData);

            // Layered onto the current registry, never a wholesale replace; idempotent by hold id, and Raft applies in
            // order, so an upsert cannot resurrect a hold a later remove deleted.
            Dictionary<string, HistoryHold> next = new(holds, StringComparer.Ordinal);
            foreach (HistoryHoldDeltaEntry entry in delta.Entries)
            {
                if (entry.Remove)
                    next.Remove(entry.Hold.HoldId);
                else
                    next[entry.Hold.HoldId] = FromMessage(entry.Hold);
            }

            Commit(next);
            return true;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to apply a history hold entry on partition {Partition}", partitionId);
            return false;
        }
    }

    /// <summary>Serializes the whole registry for the meta-partition state transfer. Lock-free read.</summary>
    public byte[] SerializeState() => ReplicationSerializer.Serialize(ToStateMessage(holds));

    /// <summary>Parses (does not install) a registry from a transfer blob; an empty blob (an exporter that predates
    /// the registry) is an empty registry. Throws on a decode failure, before anything is installed.</summary>
    public Dictionary<string, HistoryHold> ParseState(ReadOnlySpan<byte> data) =>
        FromStateMessage(ReplicationSerializer.UnserializeHistoryHoldStateMessage(data));

    /// <summary>Installs a parsed registry (from <see cref="ParseState"/>) and persists it.</summary>
    public void CommitState(Dictionary<string, HistoryHold> parsed) => Commit(parsed);

    private void Commit(Dictionary<string, HistoryHold> next)
    {
        holds = next;
        PersistToDisk(ToStateMessage(next));

        try
        {
            Changed?.Invoke();
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "A history hold change handler failed");
        }
    }

    private async Task<bool> ReplicateAsync(HistoryHoldDeltaMessage delta, CancellationToken ct)
    {
        RaftReplicationResult result = await raft.ReplicateLogs(
            RangeMapStore.MetaPartitionId,
            ReplicationTypes.HistoryHold,
            ReplicationSerializer.Serialize(delta),
            cancellationToken: ct).ConfigureAwait(false);

        if (!result.Success)
        {
            logger.LogWarning("Failed to replicate a history hold mutation Status={Status}", result.Status);
            return false;
        }

        return true;
    }

    private Task<bool> ConfirmLeadership(CancellationToken ct) =>
        raft.ConfirmLeadershipIfHosted(RangeMapStore.MetaPartitionId, ct).AsTask();

    private bool LeaseAllowed(int leaseMs) => leaseMs > 0 && (limits.MaxLeaseMs <= 0 || leaseMs <= limits.MaxLeaseMs);

    private HistoryHold? Find(string holderId, int partitionId)
    {
        foreach (HistoryHold hold in holds.Values)
        {
            if (hold.PartitionId == partitionId && string.Equals(hold.HolderId, holderId, StringComparison.Ordinal))
                return hold;
        }

        return null;
    }

    private HLCTimestamp Now() => raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

    private static HLCTimestamp AddMs(HLCTimestamp origin, int ms) => new(origin.N, origin.L + ms, origin.C);

    private void PersistToDisk(HistoryHoldStateMessage message)
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
            logger.LogError(ex, "Failed to persist the history hold registry to {Path}", snapshotPath);
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

            Dictionary<string, HistoryHold> loaded = FromStateMessage(ReplicationSerializer.UnserializeHistoryHoldStateMessage(data));
            holds = loaded;

            // Holders whose lease lapsed while the whole cluster was down get the grace window to renew.
            if (loaded.Count > 0 && startupGraceWindow > TimeSpan.Zero)
            {
                startupGraceHoldIds = new HashSet<string>(loaded.Keys, StringComparer.Ordinal);
                startupGraceDeadline = AddMs(Now(), (int)Math.Min(startupGraceWindow.TotalMilliseconds, int.MaxValue));
            }

            if (logger.IsEnabled(LogLevel.Information))
                logger.LogInformation("Loaded {Count} history hold(s) from {Path}", loaded.Count, snapshotPath);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to load the history hold registry from {Path}", snapshotPath);
        }
    }

    private int CountLiveHolds()
    {
        HLCTimestamp now = Now();
        int live = 0;

        foreach (HistoryHold hold in holds.Values)
        {
            if (hold.IsLive(now))
                live++;
        }

        return live;
    }

    private IEnumerable<Measurement<long>> ObserveLowestTimestampAges()
    {
        long nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        foreach (KeyValuePair<int, HLCTimestamp> entry in LowestTimestamps())
            yield return new Measurement<long>(Math.Max(0, nowMs - entry.Value.L), new KeyValuePair<string, object?>("partition", entry.Key));
    }

    internal static HistoryHoldInfo ToInfo(HistoryHold hold) =>
        new(hold.HoldId, hold.HolderId, hold.PartitionId, hold.Timestamp, hold.LeaseExpiry);

    private static HistoryHoldMessage ToMessage(HistoryHold hold) => new()
    {
        HoldId = hold.HoldId,
        HolderId = hold.HolderId,
        PartitionId = hold.PartitionId,
        TimestampNode = hold.Timestamp.N,
        TimestampPhysical = hold.Timestamp.L,
        TimestampCounter = hold.Timestamp.C,
        LeaseExpiryNode = hold.LeaseExpiry.N,
        LeaseExpiryPhysical = hold.LeaseExpiry.L,
        LeaseExpiryCounter = hold.LeaseExpiry.C
    };

    private static HistoryHold FromMessage(HistoryHoldMessage m) => new(
        m.HoldId,
        m.HolderId,
        m.PartitionId,
        new HLCTimestamp(m.TimestampNode, m.TimestampPhysical, m.TimestampCounter),
        new HLCTimestamp(m.LeaseExpiryNode, m.LeaseExpiryPhysical, m.LeaseExpiryCounter));

    private static HistoryHoldStateMessage ToStateMessage(IReadOnlyDictionary<string, HistoryHold> registry)
    {
        HistoryHoldStateMessage message = new();
        foreach (HistoryHold hold in registry.Values)
            message.Holds.Add(ToMessage(hold));
        return message;
    }

    private static Dictionary<string, HistoryHold> FromStateMessage(HistoryHoldStateMessage message)
    {
        Dictionary<string, HistoryHold> result = new(message.Holds.Count, StringComparer.Ordinal);
        foreach (HistoryHoldMessage m in message.Holds)
            result[m.HoldId] = FromMessage(m);
        return result;
    }

    public void Dispose()
    {
        gaugeMeter.Dispose();
        mutateGate.Dispose();
    }
}
