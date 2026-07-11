
using Kahuna.Server.KeyValues;
using Kommander.Time;

namespace Kahuna.Server.KeyValues.Handlers;

/// <summary>
/// Shared range-lock helpers used by Set, Delete, and Prepare write handlers.
/// A write to key K conflicts with any foreign range lock (S or X) whose bounds
/// cover K — a write needs exclusive on [K,K], which is incompatible with both modes.
/// </summary>
internal static class RangeLockChecks
{
    /// <summary>
    /// Returns true when <paramref name="key"/> falls within the bounds of an active range lock
    /// held by a transaction other than <paramref name="txId"/>.
    /// </summary>
    internal static bool KeyCoveredByForeignRangeLock(
        KeyValueContext context,
        string key,
        string? bucket,
        HLCTimestamp txId,
        HLCTimestamp currentTime)
    {
        if (bucket is null || !context.LocksByRange.TryGetValue(bucket, out List<KeyValueRangeLock>? rangeLocks))
            return false;

        foreach (KeyValueRangeLock rangeLock in rangeLocks)
        {
            if (rangeLock.TransactionId == txId)
                continue;

            if (rangeLock.Expires != HLCTimestamp.Zero && rangeLock.Expires - currentTime <= TimeSpan.Zero)
                continue; // expired — will be cleaned up on release

            if (KeyInRange(key, rangeLock))
                return true;
        }

        return false;
    }

    /// <summary>
    /// Removes expired range locks from <paramref name="locks"/> in place, inspecting at most
    /// <paramref name="inspectionBudget"/> entries. A lock with <c>Expires == Zero</c> (no deadline)
    /// is never pruned. Iterates back-to-front so removal does not disturb the scan. Returns true when
    /// the list is now empty, letting the caller drop the owning bucket from <c>LocksByRange</c>.
    /// Expired range locks are otherwise only cleared by a matching release, so an abandoned
    /// transaction's lock would linger indefinitely and be transferred as live on a split/merge; this
    /// is the shared prune used on acquire, export, import, and the periodic collector sweep.
    /// </summary>
    internal static bool PruneExpired(List<KeyValueRangeLock> locks, HLCTimestamp currentTime, int inspectionBudget)
    {
        int inspected = 0;
        for (int i = locks.Count - 1; i >= 0 && inspected < inspectionBudget; i--)
        {
            inspected++;
            KeyValueRangeLock rl = locks[i];
            if (rl.Expires != HLCTimestamp.Zero && rl.Expires - currentTime <= TimeSpan.Zero)
                locks.RemoveAt(i);
        }

        return locks.Count == 0;
    }

    internal static bool KeyInRange(string key, KeyValueRangeLock rangeLock)
    {
        if (rangeLock.StartKey is not null)
        {
            int cmp = string.Compare(key, rangeLock.StartKey, StringComparison.Ordinal);
            if (rangeLock.StartInclusive ? cmp < 0 : cmp <= 0)
                return false;
        }
        if (rangeLock.EndKey is not null)
        {
            int cmp = string.Compare(key, rangeLock.EndKey, StringComparison.Ordinal);
            if (rangeLock.EndInclusive ? cmp > 0 : cmp >= 0)
                return false;
        }
        return true;
    }

    /// <summary>
    /// Returns true when lock A's range overlaps lock B's range.
    /// Two ranges overlap iff A.start &lt; B.end AND B.start &lt; A.end (ordinal, honoring inclusivity).
    /// An absent bound (null) represents unbounded — always "before" the other end.
    /// </summary>
    internal static bool RangesOverlap(
        string? aStart, bool aStartInclusive, string? aEnd, bool aEndInclusive,
        string? bStart, bool bStartInclusive, string? bEnd, bool bEndInclusive)
    {
        return StartBeforeEnd(aStart, aStartInclusive, bEnd, bEndInclusive)
            && StartBeforeEnd(bStart, bStartInclusive, aEnd, aEndInclusive);
    }

    /// <summary>Returns true when <paramref name="start"/> is strictly before <paramref name="end"/>.</summary>
    internal static bool StartBeforeEnd(string? start, bool startInclusive, string? end, bool endInclusive)
    {
        if (start is null || end is null)
            return true; // unbounded → always overlaps

        int cmp = string.Compare(start, end, StringComparison.Ordinal);
        if (cmp < 0) return true;
        if (cmp > 0) return false;
        return startInclusive && endInclusive;
    }
}
