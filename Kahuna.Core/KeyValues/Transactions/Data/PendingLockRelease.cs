
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Transactions.Data;

/// <summary>The shape of a lock a transaction holds, as its release addresses it.</summary>
internal enum PendingLockShape
{
    Point,
    Prefix,
    Range
}

/// <summary>
/// A mandatory lock release of a finalized transaction that no node acknowledged yet. <see cref="Key"/> names
/// the key of a point lock or the prefix of a prefix lock; <see cref="Range"/> names the bounds of a range lock
/// and is default for the other shapes. The commit cleanup collects these and retries them, each over the
/// locator again, so a leader elected after the first attempt is found by the retry.
/// </summary>
internal readonly record struct PendingLockRelease(PendingLockShape Shape, string Key, KeyValueDurability Durability, RangeLockKey Range)
{
    internal static PendingLockRelease OfPoint(string key, KeyValueDurability durability) => new(PendingLockShape.Point, key, durability, default);

    internal static PendingLockRelease OfPrefix(string prefixKey, KeyValueDurability durability) => new(PendingLockShape.Prefix, prefixKey, durability, default);

    internal static PendingLockRelease OfRange(RangeLockKey range) => new(PendingLockShape.Range, range.Prefix, range.Durability, range);
}
