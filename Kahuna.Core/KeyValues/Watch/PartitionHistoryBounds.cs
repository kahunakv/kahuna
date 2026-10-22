using System.Globalization;
using System.Text;

using Kommander.Time;

namespace Kahuna.Server.KeyValues.Watch;

/// <summary>
/// What a watch resume must know about each partition's history on this node beyond the WAL rows themselves.
/// Persisted, so a restart keeps it.
///
/// <para><b>Install boundary.</b> The highest log index a whole-partition snapshot installed here. The entries at or
/// below it never applied here one by one: the install replaced the partition's state, so this node has no per-entry
/// history there even when WAL rows below it remain. A replay that would start at or below it answers
/// <c>HistoryUnavailable</c>. An install records its boundary durably before it reports success
/// (<see cref="RecordInstall"/> throws when it cannot), so no restart can lose one.</para>
///
/// <para><b>Unbounded commit ceiling.</b> A replay from a cursor W starts at the first entry at or after
/// <c>W − MaxClockLeadMs</c>, which is sound only for commits the admission window bounded. An entry admitted with no
/// bound (written before the window was turned on, or by a node with it off) can carry a commit far ahead of its
/// entry time; when that commit is at or above the entry time plus the bound
/// (<see cref="CommitAdmission.IsUnboundedLead"/>), a replay from below it could start after the entry and skip its
/// event. The ceiling is the highest such commit applied on the partition: no watermark below it is sent, and a replay
/// from a cursor below it answers <c>HistoryUnavailable</c>. It is fed by every apply and restore of a value write or a
/// prepare, carried by a partition snapshot, and persisted at most every <see cref="PersistIntervalMs"/> (a restart
/// also rebuilds it from the retained log).</para>
///
/// <para><b>Fails closed.</b> An existing file that cannot be read or parsed fences the node: no line of it is
/// trusted, and each partition takes at least its current log end as its boundary the first time a replay asks, so
/// no resume can lean on history from before the failure. The file keeps the fence and the partitions it already
/// fenced, so a restart does not fence them again. The ceilings of entries compacted before the failure are not
/// recoverable; the restore rebuilds them from the retained log.</para>
/// </summary>
internal sealed class PartitionHistoryBounds
{
    /// <summary>The longest a raised ceiling waits in memory before it is written.</summary>
    internal const int PersistIntervalMs = 500;

    private const string FencedMarker = "fenced";

    private readonly record struct Bounds(long InstallBoundary, HLCTimestamp UnboundedCeiling);

    private readonly Dictionary<int, Bounds> bounds = new();

    // Guards bounds, fenced, dirty and every file write: the file is always serialized from the state it writes.
    private readonly object stateLock = new();

    private readonly string? path;

    private readonly ILogger<IKahuna> logger;

    private bool fenced;

    // While fenced: the partitions whose boundary was already raised to their log end.
    private readonly HashSet<int> fencedPartitions = new();

    private volatile bool dirty;

    private long lastPersistTicks;

    private Func<int, long>? logEnd;

    public PartitionHistoryBounds(string? storagePath, string? storageRevision, ILogger<IKahuna> logger)
    {
        this.logger = logger;
        path = string.IsNullOrEmpty(storagePath) ? null : Path.Combine(storagePath, $"installboundaries_{storageRevision}.txt");
        Load();
    }

    /// <summary>Wires the last log index of a partition, which a fenced node takes as the boundary of a partition it
    /// has no record of.</summary>
    public void AttachLogEnd(Func<int, long> lastLogIndex) => logEnd = lastLogIndex;

    /// <summary>The install boundary of <paramref name="partitionId"/>, or zero when no snapshot was installed.</summary>
    public long InstallBoundary(int partitionId)
    {
        lock (stateLock)
        {
            bounds.TryGetValue(partitionId, out Bounds known);
            if (!fenced || fencedPartitions.Contains(partitionId))
                return known.InstallBoundary;
        }

        // Fenced and unrecorded: everything up to the current log end is history this node cannot vouch for.
        long fence = logEnd is { } end ? end(partitionId) : long.MaxValue;

        lock (stateLock)
        {
            Bounds current = bounds.GetValueOrDefault(partitionId);
            if (current.InstallBoundary < fence)
                bounds[partitionId] = current with { InstallBoundary = fence };

            fencedPartitions.Add(partitionId);
            TryPersistLocked();
            return Math.Max(current.InstallBoundary, fence);
        }
    }

    /// <summary>The unbounded commit ceiling of <paramref name="partitionId"/>, or zero when none was applied.</summary>
    public HLCTimestamp UnboundedCeiling(int partitionId)
    {
        lock (stateLock)
            return bounds.TryGetValue(partitionId, out Bounds known) ? known.UnboundedCeiling : HLCTimestamp.Zero;
    }

    /// <summary>
    /// Records that a snapshot installed <paramref name="partitionId"/> through <paramref name="upToIndex"/>, with the
    /// exporter's unbounded commit ceiling. Durable when it returns; throws when the record cannot be persisted, so
    /// the install fails and is driven again.
    /// </summary>
    public void RecordInstall(int partitionId, long upToIndex, HLCTimestamp unboundedCeiling)
    {
        lock (stateLock)
        {
            Bounds current = bounds.GetValueOrDefault(partitionId);
            bounds[partitionId] = new(
                Math.Max(current.InstallBoundary, upToIndex),
                unboundedCeiling > current.UnboundedCeiling ? unboundedCeiling : current.UnboundedCeiling);

            PersistLocked();
        }
    }

    /// <summary>Records an install boundary with no ceiling; see <see cref="RecordInstall"/>.</summary>
    public void Record(int partitionId, long upToIndex) => RecordInstall(partitionId, upToIndex, HLCTimestamp.Zero);

    /// <summary>
    /// Feeds one applied (or restored) entry that creates a commit: raises the partition's ceiling when the entry was
    /// admitted with no bound and its commit leads its time by the bound or more. Called from the apply paths; costs
    /// one comparison for every other entry.
    /// </summary>
    public void ObserveCommit(int partitionId, HLCTimestamp entryTime, HLCTimestamp commitTimestamp, long admissionLagMs)
    {
        if (!CommitAdmission.IsUnboundedLead(entryTime, commitTimestamp, admissionLagMs))
            return;

        lock (stateLock)
        {
            Bounds current = bounds.GetValueOrDefault(partitionId);
            if (commitTimestamp <= current.UnboundedCeiling)
                return;

            bounds[partitionId] = current with { UnboundedCeiling = commitTimestamp };
            dirty = true;

            if (Environment.TickCount64 - lastPersistTicks >= PersistIntervalMs)
                TryPersistLocked();
        }
    }

    /// <summary>Writes a raised ceiling that waited for the persist interval. One volatile read when nothing waits;
    /// called from the apply path.</summary>
    public void FlushIfDue()
    {
        if (!dirty || Environment.TickCount64 - Volatile.Read(ref lastPersistTicks) < PersistIntervalMs)
            return;

        lock (stateLock)
        {
            if (dirty)
                TryPersistLocked();
        }
    }

    /// <summary>Writes anything that waits, whatever the interval (node shutdown).</summary>
    public void Flush()
    {
        lock (stateLock)
        {
            if (dirty)
                TryPersistLocked();
        }
    }

    // A best-effort write: a failure keeps the state dirty for the next attempt. The in-memory state is already the
    // conservative one.
    private void TryPersistLocked()
    {
        try
        {
            PersistLocked();
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to persist the partition history bounds to {Path}", path);
        }
    }

    private void PersistLocked()
    {
        if (path is null)
        {
            dirty = false;
            return;
        }

        StringBuilder text = new();
        if (fenced)
        {
            text.Append(FencedMarker);
            foreach (int partitionId in fencedPartitions)
                text.Append(CultureInfo.InvariantCulture, $" {partitionId}");

            text.Append('\n');
        }

        foreach ((int partitionId, Bounds value) in bounds)
        {
            text.Append(CultureInfo.InvariantCulture, $"{partitionId} {value.InstallBoundary}");
            if (value.UnboundedCeiling != HLCTimestamp.Zero)
                text.Append(CultureInfo.InvariantCulture, $" {value.UnboundedCeiling.N} {value.UnboundedCeiling.L} {value.UnboundedCeiling.C}");

            text.Append('\n');
        }

        string tmp = path + ".tmp";
        File.WriteAllText(tmp, text.ToString());
        File.Move(tmp, path, overwrite: true);

        dirty = false;
        Volatile.Write(ref lastPersistTicks, Environment.TickCount64);
    }

    private void Load()
    {
        if (path is null || !File.Exists(path))
            return;

        try
        {
            foreach (string line in File.ReadAllLines(path))
            {
                if (line.Length == 0)
                    continue;

                if (line.StartsWith(FencedMarker, StringComparison.Ordinal))
                {
                    fenced = true;
                    foreach (string part in line.Split(' ', StringSplitOptions.RemoveEmptyEntries).Skip(1))
                    {
                        if (!int.TryParse(part, NumberStyles.Integer, CultureInfo.InvariantCulture, out int fencedPartition))
                            throw new FormatException($"malformed line '{line}'");

                        fencedPartitions.Add(fencedPartition);
                    }

                    continue;
                }

                if (!TryParse(line, out int partitionId, out Bounds value))
                    throw new FormatException($"malformed line '{line}'");

                bounds[partitionId] = value;
            }
        }
        catch (Exception ex)
        {
            // Nothing the file holds can be trusted now: every partition is fenced again at first use.
            fenced = true;
            fencedPartitions.Clear();
            logger.LogError(ex, "Failed to load the partition history bounds from {Path}; watch resumes are refused below each partition's current log end", path);
        }
    }

    // "<partition> <boundary>" or "<partition> <boundary> <node> <physical> <counter>".
    private static bool TryParse(string line, out int partitionId, out Bounds value)
    {
        value = default;
        string[] parts = line.Split(' ', StringSplitOptions.RemoveEmptyEntries);

        if (parts.Length is not (2 or 5)
            || !int.TryParse(parts[0], NumberStyles.Integer, CultureInfo.InvariantCulture, out partitionId)
            || !long.TryParse(parts[1], NumberStyles.Integer, CultureInfo.InvariantCulture, out long boundary))
        {
            partitionId = 0;
            return false;
        }

        HLCTimestamp ceiling = HLCTimestamp.Zero;

        if (parts.Length == 5)
        {
            if (!int.TryParse(parts[2], NumberStyles.Integer, CultureInfo.InvariantCulture, out int node)
                || !long.TryParse(parts[3], NumberStyles.Integer, CultureInfo.InvariantCulture, out long physical)
                || !uint.TryParse(parts[4], NumberStyles.Integer, CultureInfo.InvariantCulture, out uint counter))
                return false;

            ceiling = new HLCTimestamp(node, physical, counter);
        }

        value = new Bounds(boundary, ceiling);
        return true;
    }
}
