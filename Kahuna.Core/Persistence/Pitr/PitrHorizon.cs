
using Kommander.Time;
using Kommander.WAL;

namespace Kahuna.Server.Persistence.Pitr;

/// <summary>
/// Computes the minimum WAL index that must be retained to support the configured PITR window.
/// </summary>
internal static class PitrHorizon
{
    /// <summary>
    /// Returns the WAL index from which the partition's log must be retained for point-in-time
    /// recovery: the largest committed index whose HLC timestamp falls at or before the protected
    /// boundary <c>now − pitrWindow − baseSnapshotInterval</c>. Entries below it are covered by at
    /// least one base snapshot and may be compacted; it and everything above are needed to reach any
    /// instant inside the window.
    /// <para>
    /// The result is always a positive index, so the floor is asserted on every tick. When no retained
    /// committed entry is at or before the boundary, the whole retained log is younger than the
    /// boundary — all of it inside the window — and the first retained committed index is returned.
    /// Under load that is the steady state right after a compaction pass: the pass runs up to the
    /// checkpoint, which trails "now" by seconds, while the boundary trails it by the window. Clearing
    /// the floor here (the previous behaviour) let the next pass remove the window again, so no WAL was
    /// ever retained on a loaded node. When nothing committed is retained at all, the first index the
    /// log can hold is returned, so entries landing before the next tick are protected from the start.
    /// </para>
    /// </summary>
    internal static long ComputeProtectedIndex(
        IWAL wal,
        int partitionId,
        DateTime nowUtc,
        TimeSpan pitrWindow,
        TimeSpan baseSnapshotInterval)
    {
        DateTime boundary = nowUtc - pitrWindow - baseSnapshotInterval;
        long boundaryMs = (long)(boundary - DateTime.UnixEpoch).TotalMilliseconds;
        HLCTimestamp target = new(0, boundaryMs, 0);

        long atOrBeforeBoundary = LogTimeIndex.LastIndexAtOrBefore(wal, partitionId, target);
        if (atOrBeforeBoundary > 0)
            return atOrBeforeBoundary;

        long firstCommitted = LogTimeIndex.FirstCommittedIndex(wal, partitionId);
        if (firstCommitted > 0)
            return firstCommitted;

        return LogTimeIndex.FirstRetainedIndex(wal, partitionId);
    }
}
