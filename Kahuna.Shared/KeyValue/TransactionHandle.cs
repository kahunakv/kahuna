
using Kommander.Time;

namespace Kahuna.Shared.KeyValue;

/// <summary>
/// Bundles the transaction ID with the coordinator key that pins the session to a partition.
/// Callers use this as a unit when routing commit and rollback requests to the same node
/// that accepted StartTransaction.
/// </summary>
public readonly record struct TransactionHandle(HLCTimestamp TransactionId, string CoordinatorKey)
{
    public static readonly TransactionHandle None = default;

    /// <summary>Returns true when this handle carries no meaningful identity.</summary>
    public bool IsEmpty => TransactionId == HLCTimestamp.Zero || string.IsNullOrEmpty(CoordinatorKey);
}
