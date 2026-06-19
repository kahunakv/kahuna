
using Kommander.Time;
using Kommander.WAL;

namespace Kahuna.Server.Persistence.Pitr;

/// <summary>
/// Computes the minimum WAL index that must be retained to support the configured PITR window.
/// </summary>
internal static class PitrHorizon
{
    /// <summary>
    /// Returns the largest committed WAL index whose HLC timestamp falls at or before the
    /// protected boundary <c>now − pitrWindow − baseSnapshotInterval</c>.
    /// <para>
    /// WAL entries up to (not including) this index are covered by at least one base snapshot
    /// and may be compacted. Returns -1 when no committed entry exists before the boundary
    /// (WAL empty or all entries are newer); callers should treat -1 as "floor not yet known"
    /// and pass a non-positive value to <c>IRaft.SetMinRetainIndex</c>.
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
        return LogTimeIndex.LastIndexAtOrBefore(wal, partitionId, target);
    }
}
