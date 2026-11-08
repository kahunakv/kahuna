using System.Globalization;
using System.Text;

using Kommander.Time;

namespace Kahuna.Server.KeyValues.Ranges;

/// <summary>
/// Which snapshot timestamps this node can answer for each partition it received through a whole-partition install.
///
/// <para>An install replaces a partition's rows, and the snapshot carries only part of the partition's revision
/// history: on-disk history is kept without bound by default and the export is held in memory on the sender, so it
/// carries each key's current row, its revision at each registered snapshot hold, and its revisions from just before
/// the export onward (see <see cref="PartitionStateTransfer"/>). A read at any other timestamp would find an older
/// revision than the true one, or none, and an as-of read reports a missing revision as "did not exist". This fence
/// records what the snapshot carried so such a read is refused instead of answered.</para>
///
/// <para>A timestamp is servable when it is at or above the partition's complete-from timestamp, or is exactly one of
/// its pinned hold timestamps. A pin covers that exact timestamp only: the next pin above it may be many revisions
/// newer, and the revisions between them were not carried.</para>
///
/// <para><b>Durable before it counts.</b> <see cref="Record"/> persists before it returns and throws when it cannot,
/// and the install runs it before clearing its incomplete marker, so no install can report success without its
/// bound. A later install of the partition replaces the bound, because it also replaces the rows.</para>
///
/// <para><b>Fails closed.</b> A file that cannot be read trusts none of its lines: at startup every partition is fenced
/// from the current time (<see cref="FenceAtStartup"/>), and that bound is persisted so the next restart does not
/// lose it. The same call fences a partition with a recorded install and no bound, which an older build installed
/// without carrying any history.</para>
///
/// <para>Reads check this on every snapshot request, so <see cref="MayRefuse"/> answers without looking at a partition:
/// a read at or above the highest bound on the node costs one volatile read and one comparison.</para>
/// </summary>
internal sealed class InstalledHistoryFence
{
    private const string AllPartitionsMarker = "all";

    /// <summary>What one install left on this node for its partition.</summary>
    /// <param name="CompleteFrom">The lowest timestamp at or above which every revision is present.</param>
    /// <param name="Pinned">The hold timestamps below <paramref name="CompleteFrom"/> that the snapshot carried, ascending.</param>
    internal readonly record struct Bound(HLCTimestamp CompleteFrom, HLCTimestamp[] Pinned);

    private sealed record State(Dictionary<int, Bound> Partitions, HLCTimestamp AllPartitionsFrom, HLCTimestamp Highest)
    {
        public static readonly State Empty = new([], HLCTimestamp.Zero, HLCTimestamp.Zero);
    }

    // Replaced whole under stateLock and read lock-free: a reader sees one install's bound or the next, never a mixture.
    private volatile State state = State.Empty;

    private readonly object stateLock = new();

    private readonly string? path;

    private readonly ILogger<IKahuna> logger;

    private bool loadFailed;

    public InstalledHistoryFence(string? storagePath, string? storageRevision, ILogger<IKahuna> logger)
    {
        this.logger = logger;
        path = string.IsNullOrEmpty(storagePath) ? null : Path.Combine(storagePath, $"installedhistory_{storageRevision}.txt");
        Load();
    }

    /// <summary>Whether a read at <paramref name="timestamp"/> could be refused on some partition of this node. False
    /// means it is servable everywhere, with no partition lookup.</summary>
    public bool MayRefuse(HLCTimestamp timestamp) => timestamp.CompareTo(state.Highest) < 0;

    /// <summary>Whether this node can answer a read of <paramref name="partitionId"/> at <paramref name="timestamp"/>.</summary>
    public bool IsServable(int partitionId, HLCTimestamp timestamp)
    {
        State current = state;

        if (current.AllPartitionsFrom != HLCTimestamp.Zero && timestamp.CompareTo(current.AllPartitionsFrom) < 0
            && !IsPinned(current, partitionId, timestamp))
            return false;

        if (!current.Partitions.TryGetValue(partitionId, out Bound bound))
            return true;

        return timestamp.CompareTo(bound.CompleteFrom) >= 0 || IsPinned(current, partitionId, timestamp);
    }

    private static bool IsPinned(State current, int partitionId, HLCTimestamp timestamp) =>
        current.Partitions.TryGetValue(partitionId, out Bound bound)
        && Array.BinarySearch(bound.Pinned, timestamp) >= 0;

    /// <summary>The partition's complete-from timestamp, or zero when no install bounds it: an exporter cannot carry
    /// a complete run of revisions below its own.</summary>
    public HLCTimestamp CompleteFromOf(int partitionId)
    {
        State current = state;

        HLCTimestamp own = current.Partitions.TryGetValue(partitionId, out Bound bound) ? bound.CompleteFrom : HLCTimestamp.Zero;
        return current.AllPartitionsFrom.CompareTo(own) > 0 ? current.AllPartitionsFrom : own;
    }

    /// <summary>The bound of <paramref name="partitionId"/>, or null when no install recorded one.</summary>
    public Bound? BoundOf(int partitionId) =>
        state.Partitions.TryGetValue(partitionId, out Bound bound) ? bound : null;

    /// <summary>
    /// Records what an install of <paramref name="partitionId"/> carried, replacing the bound of any earlier install.
    /// Durable when it returns; throws when it cannot be persisted, so the install fails and is driven again.
    /// </summary>
    public void Record(int partitionId, HLCTimestamp completeFrom, IEnumerable<HLCTimestamp> pinned)
    {
        HLCTimestamp[] ordered = [.. pinned.Where(p => p.CompareTo(completeFrom) < 0).Distinct().Order()];

        lock (stateLock)
        {
            Dictionary<int, Bound> partitions = new(state.Partitions) { [partitionId] = new(completeFrom, ordered) };
            Publish(partitions, state.AllPartitionsFrom);
            PersistLocked();
        }
    }

    /// <summary>
    /// Fences what a restart cannot vouch for, at <paramref name="now"/>: every partition when the file could not be
    /// read, and otherwise each of <paramref name="installedPartitions"/> with no bound, which an install from a build
    /// that carried no history left behind. A read below <paramref name="now"/> of such a partition is refused from
    /// here on. That includes a timestamp minted after the old install, which this node could answer, because nothing
    /// recorded says when the install ran.
    /// </summary>
    public void FenceAtStartup(IEnumerable<int> installedPartitions, HLCTimestamp now)
    {
        lock (stateLock)
        {
            Dictionary<int, Bound> partitions = new(state.Partitions);
            bool changed = false;

            foreach (int partitionId in installedPartitions)
            {
                if (partitions.TryAdd(partitionId, new(now, [])))
                    changed = true;
            }

            HLCTimestamp allPartitionsFrom = state.AllPartitionsFrom;
            if (loadFailed)
            {
                allPartitionsFrom = now;
                loadFailed = false;
                changed = true;
            }

            if (!changed)
                return;

            Publish(partitions, allPartitionsFrom);
            PersistLocked();
        }
    }

    private void Publish(Dictionary<int, Bound> partitions, HLCTimestamp allPartitionsFrom)
    {
        HLCTimestamp highest = allPartitionsFrom;

        foreach (Bound bound in partitions.Values)
        {
            if (bound.CompleteFrom.CompareTo(highest) > 0)
                highest = bound.CompleteFrom;
        }

        state = new(partitions, allPartitionsFrom, highest);
    }

    private void PersistLocked()
    {
        if (path is null)
            return;

        StringBuilder text = new();
        State current = state;

        if (current.AllPartitionsFrom != HLCTimestamp.Zero)
            text.Append(AllPartitionsMarker).Append(' ').Append(Format(current.AllPartitionsFrom)).Append('\n');

        foreach ((int partitionId, Bound bound) in current.Partitions)
        {
            text.Append(partitionId.ToString(CultureInfo.InvariantCulture)).Append(' ').Append(Format(bound.CompleteFrom));

            foreach (HLCTimestamp pinned in bound.Pinned)
                text.Append(' ').Append(Format(pinned));

            text.Append('\n');
        }

        string tmp = path + ".tmp";
        File.WriteAllText(tmp, text.ToString());
        File.Move(tmp, path, overwrite: true);
    }

    private void Load()
    {
        if (path is null || !File.Exists(path))
            return;

        try
        {
            Dictionary<int, Bound> partitions = [];
            HLCTimestamp allPartitionsFrom = HLCTimestamp.Zero;

            foreach (string line in File.ReadAllLines(path))
            {
                if (line.Length == 0)
                    continue;

                string[] parts = line.Split(' ', StringSplitOptions.RemoveEmptyEntries);
                if (parts.Length < 2)
                    throw new FormatException($"malformed line '{line}'");

                if (parts[0] == AllPartitionsMarker)
                {
                    allPartitionsFrom = Parse(parts[1]);
                    continue;
                }

                int partitionId = int.Parse(parts[0], NumberStyles.Integer, CultureInfo.InvariantCulture);
                HLCTimestamp[] pinned = [.. parts.Skip(2).Select(Parse)];
                partitions[partitionId] = new(Parse(parts[1]), pinned);
            }

            Publish(partitions, allPartitionsFrom);
        }
        catch (Exception ex) when (ex is FormatException or OverflowException or IOException or UnauthorizedAccessException)
        {
            // Nothing the file holds can be trusted: FenceAtStartup fences every partition.
            loadFailed = true;
            logger.LogError(ex, "The installed-history file {Path} cannot be read; every partition is fenced at startup", path);
        }
    }

    private static string Format(HLCTimestamp timestamp) =>
        string.Create(CultureInfo.InvariantCulture, $"{timestamp.N},{timestamp.L},{timestamp.C}");

    private static HLCTimestamp Parse(string text)
    {
        string[] parts = text.Split(',');
        if (parts.Length != 3)
            throw new FormatException($"malformed timestamp '{text}'");

        return new(
            int.Parse(parts[0], NumberStyles.Integer, CultureInfo.InvariantCulture),
            long.Parse(parts[1], NumberStyles.Integer, CultureInfo.InvariantCulture),
            uint.Parse(parts[2], NumberStyles.Integer, CultureInfo.InvariantCulture));
    }
}
