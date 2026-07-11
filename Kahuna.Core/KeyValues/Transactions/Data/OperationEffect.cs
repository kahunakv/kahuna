
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

    /// <summary>A point lock the operation released, dropped from the held-lock set so it is not released again.</summary>
    public (string Key, KeyValueDurability Durability)? RemovePointLock { get; init; }

    /// <summary>A prefix lock the operation acquired.</summary>
    public (string Key, KeyValueDurability Durability)? PrefixLock { get; init; }

    /// <summary>A prefix lock the operation released, dropped from the held-prefix-lock set.</summary>
    public (string Key, KeyValueDurability Durability)? RemovePrefixLock { get; init; }

    /// <summary>
    /// A range lock the operation acquired, upgraded, or renewed. Recorded under its logical identity so
    /// a mode change replaces the held descriptor instead of adding a second one.
    /// </summary>
    public (RangeLockKey Range, RangeLockMode Mode)? RangeLock { get; init; }

    /// <summary>A range lock the operation released, dropped from the held-range-lock set.</summary>
    public RangeLockKey? RemoveRangeLock { get; init; }

    /// <summary>A key observed by a non-snapshot read, recorded for read-set cleanup/validation.</summary>
    public KeyValueTransactionReadKey? ReadObservation { get; init; }

    /// <summary>
    /// The keys returned by a scan, each recorded with the existing point-read-set semantics. A scan does
    /// not assert predicate validation — only that these exact items were observed at these revisions.
    /// </summary>
    public IReadOnlyList<KeyValueTransactionReadKey>? ReadObservations { get; init; }
}
