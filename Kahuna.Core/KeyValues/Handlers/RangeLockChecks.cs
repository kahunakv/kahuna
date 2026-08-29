
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Logging;
using Kahuna.Server.KeyValues.Transactions;
using Kommander.Time;

namespace Kahuna.Server.KeyValues.Handlers;

/// <summary>
/// Shared range-lock helpers used by Set, Delete, and Prepare write handlers.
/// A write to key K conflicts with any foreign range lock (S or X) whose bounds
/// cover K — a write needs exclusive on [K,K], which is incompatible with both modes.
/// </summary>
internal static class RangeLockChecks
{
    /// <summary>Distinguishes range locks from point and prefix intents on the shared expiry counter.</summary>
    private static readonly KeyValuePair<string, object?> RangeLockKind = new("kind", "range_lock");

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
        // Count guard first: range locks are rare, and an empty table answers false without
        // hashing the bucket. Runs on every write, so the common workload skips the probe.
        if (context.LocksByRange.Count == 0 || bucket is null
            || !context.LocksByRange.TryGetValue(bucket, out List<KeyValueRangeLock>? rangeLocks))
            return false;

        foreach (KeyValueRangeLock rangeLock in rangeLocks)
        {
            if (rangeLock.TransactionId == txId)
                continue;

            if (!IsLive(rangeLock, currentTime, context.SessionOwnedIntentCeilingMs))
                continue; // expired — will be cleaned up on release or by the collector sweep

            if (KeyInRange(key, rangeLock))
                return true;
        }

        return false;
    }

    /// <summary>
    /// Whether a range lock still covers its range. A positive deadline is live until it passes. A
    /// zero-deadline lock is session-owned: it is live while its age stays below
    /// <paramref name="sessionOwnedCeilingMs"/>, and orphaned past it, because by then the owning session
    /// has been finalized or reaped. Without that arm an abandoned zero-deadline lock blocks every write
    /// into its range for the life of the process, which is the range-lock form of an immortal write intent.
    ///
    /// <para>The age is measured from the lock's transaction id rather than from a plant stamp, because a
    /// range lock is carried between actors by a split or a merge. The transaction id crosses that transfer
    /// and bounds the same session, while a plant stamp re-taken on import would restart the clock and
    /// resurrect an orphaned lock.</para>
    /// </summary>
    internal static bool IsLive(KeyValueRangeLock rangeLock, HLCTimestamp currentTime, int sessionOwnedCeilingMs)
    {
        if (rangeLock.Expires != HLCTimestamp.Zero)
            return rangeLock.Expires - currentTime > TimeSpan.Zero;

        if (sessionOwnedCeilingMs <= 0 || rangeLock.TransactionId == HLCTimestamp.Zero)
            return true;

        return (currentTime - rangeLock.TransactionId).TotalMilliseconds < sessionOwnedCeilingMs;
    }

    /// <summary>
    /// Removes expired range locks from <paramref name="locks"/> in place, inspecting at most
    /// <paramref name="inspectionBudget"/> entries. A zero-deadline lock is pruned once it outlives the
    /// session-owned ceiling, which is the only thing that ever clears one whose owner vanished. Iterates
    /// back-to-front so removal does not disturb the scan. Returns true when the list is now empty, letting
    /// the caller drop the owning bucket from <c>LocksByRange</c>. Expired range locks are otherwise only
    /// cleared by a matching release, so an abandoned transaction's lock would linger indefinitely and be
    /// transferred as live on a split/merge; this is the shared prune used on acquire, export, import, and
    /// the periodic collector sweep.
    /// </summary>
    internal static bool PruneExpired(
        KeyValueContext context,
        string? bucket,
        List<KeyValueRangeLock> locks,
        HLCTimestamp currentTime,
        int inspectionBudget)
    {
        int ceilingMs = context.SessionOwnedIntentCeilingMs;
        int inspected = 0;

        for (int i = locks.Count - 1; i >= 0 && inspected < inspectionBudget; i--)
        {
            inspected++;
            KeyValueRangeLock rl = locks[i];

            if (IsLive(rl, currentTime, ceilingMs))
                continue;

            // A zero deadline can only fail the policy through the ceiling arm, so this identifies an
            // orphaned lock and names its owner. An ordinary lease expiry is routine and stays unreported.
            if (rl.Expires == HLCTimestamp.Zero)
            {
                DurableTransactionMetrics.SessionOwnedIntentCeilingExpiries.Add(1, RangeLockKind);
                context.Logger.LogSessionOwnedRangeLockCeilingExpiry(bucket, rl.TransactionId, ceilingMs);
            }

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
