
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
}
