
using Kommander.Time;

namespace Kahuna.Shared.KeyValue;

/// <summary>
/// Bundles the transaction ID with the coordinator key that pins the session to a partition, plus the
/// optional record anchor. Callers use this as a unit when routing commit and rollback requests to the
/// same node that accepted StartTransaction.
///
/// <para>
/// <see cref="RecordAnchorKey"/> is the first confirmed persistent modified key. It is absent until the
/// coordinator assigns it and names the data partition that owns a Durable transaction record. It is
/// deliberately excluded from <see cref="IsEmpty"/>: a handle with no anchor yet is still a valid,
/// routable identity.
/// </para>
/// </summary>
public readonly record struct TransactionHandle(HLCTimestamp TransactionId, string CoordinatorKey, string? RecordAnchorKey = null)
{
    public static readonly TransactionHandle None = default;

    /// <summary>Returns true when this handle carries no meaningful identity.</summary>
    public bool IsEmpty => TransactionId == HLCTimestamp.Zero || string.IsNullOrEmpty(CoordinatorKey);
}
