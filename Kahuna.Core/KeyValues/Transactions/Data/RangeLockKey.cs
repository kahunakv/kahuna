
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Transactions.Data;

/// <summary>
/// The logical identity of a range lock held by a transaction: the range bounds and durability.
/// The lock <see cref="RangeLockMode"/> is tracked separately (as the value of the held-range map) so a
/// shared→exclusive upgrade or a heartbeat renewal replaces the mode of the matching descriptor rather
/// than recording a second, overlapping entry. Two acquisitions of the same bounds at different modes are
/// therefore the same descriptor.
/// </summary>
public readonly record struct RangeLockKey(
    string Prefix,
    string? StartKey,
    bool StartInclusive,
    string? EndKey,
    bool EndInclusive,
    KeyValueDurability Durability);
