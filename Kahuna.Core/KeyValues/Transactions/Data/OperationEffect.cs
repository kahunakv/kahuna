
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Transactions.Data;

/// <summary>
/// The confirmed working-set additions produced by a successfully-executed transaction operation.
/// Reported to the coordinator on <c>CompleteOperation</c> so the server — not the client — owns the
/// authoritative set of modified keys and held locks for the transaction.
/// </summary>
internal sealed class OperationEffect
{
    /// <summary>A key that was written or deleted by the operation, with its durability.</summary>
    public (string Key, KeyValueDurability Durability)? ModifiedKey { get; init; }

    /// <summary>A point lock the operation acquired (the implicit write lock, or an explicit lock op).</summary>
    public (string Key, KeyValueDurability Durability)? PointLock { get; init; }

    /// <summary>A key observed by a non-snapshot read, recorded for read-set cleanup/validation.</summary>
    public KeyValueTransactionReadKey? ReadObservation { get; init; }
}
