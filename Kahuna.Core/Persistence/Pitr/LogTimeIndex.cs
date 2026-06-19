
using Kommander.Data;
using Kommander.Time;
using Kommander.WAL;

namespace Kahuna.Server.Persistence.Pitr;

/// <summary>
/// Binary-search helpers for locating committed WAL entries by HLC timestamp.
///
/// Correctness rests on the invariant that committed entries' <see cref="RaftLog.Time"/> is
/// monotonically non-decreasing with <see cref="RaftLog.Id"/> — guaranteed by HLC assignment under
/// Raft log ordering. Uncommitted (Proposed/RolledBack) entries may be interspersed and are skipped.
///
/// Both methods execute synchronously against the raw WAL adapter (suitable for
/// background-writer actor threads). Callers on read-path actors must route through
/// the partition read-scheduler instead.
/// </summary>
internal static class LogTimeIndex
{
    // Number of entries read per probe. Large enough to skip uncommitted entries that
    // may appear between committed ones near the WAL tail without reading the whole file.
    private const int PageSize = 16;

    /// <summary>
    /// Returns the smallest committed log index whose <see cref="RaftLog.Time"/> is ≥
    /// <paramref name="target"/>, or -1 if no such entry exists (WAL empty or all entries
    /// predate the target).
    /// </summary>
    public static long FirstIndexAtOrAfter(IWAL wal, int partitionId, HLCTimestamp target)
    {
        long hi = wal.GetMaxLog(partitionId);
        if (hi <= 0)
            return -1;

        long lo = 1;
        long result = -1;

        while (lo <= hi)
        {
            long mid = lo + (hi - lo) / 2;
            RaftLog? entry = FindFirstCommitted(wal, partitionId, mid);

            if (entry is null)
            {
                // No committed entry at or after mid within a page — treat as past the
                // committed tail and search lower.
                hi = mid - 1;
                continue;
            }

            if (entry.Time >= target)
            {
                result = entry.Id;
                // Narrow strictly below mid. No committed entry exists in [mid, entry.Id), so
                // searching [lo, mid-1] is correct, and mid-1 < mid <= hi guarantees progress
                // (entry.Id may be > hi when mid landed on an uncommitted/rolled-back entry).
                hi = mid - 1;
            }
            else
            {
                lo = entry.Id + 1;
            }
        }

        return result;
    }

    /// <summary>
    /// Returns the largest committed log index whose <see cref="RaftLog.Time"/> is ≤
    /// <paramref name="target"/>, or -1 if no such entry exists (WAL empty or all entries
    /// postdate the target).
    /// </summary>
    public static long LastIndexAtOrBefore(IWAL wal, int partitionId, HLCTimestamp target)
    {
        long hi = wal.GetMaxLog(partitionId);
        if (hi <= 0)
            return -1;

        long lo = 1;
        long result = -1;

        while (lo <= hi)
        {
            long mid = lo + (hi - lo) / 2;
            RaftLog? entry = FindFirstCommitted(wal, partitionId, mid);

            if (entry is null)
            {
                // No committed entry at or after mid — search lower.
                hi = mid - 1;
                continue;
            }

            if (entry.Time <= target)
            {
                result = entry.Id;
                lo = entry.Id + 1;
            }
            else
            {
                // Narrow strictly below mid (see FirstIndexAtOrAfter): mid-1 guarantees progress
                // even when entry.Id > hi because mid landed on an uncommitted entry.
                hi = mid - 1;
            }
        }

        return result;
    }

    // Returns the first Committed or CommittedCheckpoint entry at or after fromIndex, or null only
    // when no committed entry exists in [fromIndex, GetMaxLog]. Pages forward so a run of more than
    // PageSize consecutive uncommitted/rolled-back entries (e.g. a burst of aborted transactions)
    // does not cause a false null — PageSize bounds the read per probe, never correctness.
    private static RaftLog? FindFirstCommitted(IWAL wal, int partitionId, long fromIndex)
    {
        long cursor = fromIndex;
        long maxLog = wal.GetMaxLog(partitionId);

        while (cursor <= maxLog)
        {
            List<RaftLog> batch = wal.ReadLogsRange(partitionId, cursor, PageSize);
            if (batch.Count == 0)
                return null;

            foreach (RaftLog log in batch)
            {
                if (log.Type is RaftLogType.Committed or RaftLogType.CommittedCheckpoint)
                    return log;
            }

            cursor = batch[^1].Id + 1;
        }

        return null;
    }
}
