
namespace Kahuna.Shared.KeyValue;

/// <summary>
/// An immutable, consumer-facing view of a transaction's coordinator-owned working set: the confirmed
/// modified keys, held point and prefix locks, and read observations, plus the count of operations still
/// in flight. Returned by the working-set query and by close-and-snapshot so a consumer can, for example,
/// invalidate its own caches from exactly the set that will be finalized. Lists are sorted ordinally.
/// </summary>
public sealed class TransactionWorkingSet
{
    public List<KeyValueTransactionModifiedKey> ModifiedKeys { get; init; } = [];

    public List<KeyValueTransactionModifiedKey> AcquiredLocks { get; init; } = [];

    public List<KeyValueTransactionModifiedKey> AcquiredPrefixLocks { get; init; } = [];

    public List<KeyValueTransactionRangeLock> AcquiredRangeLocks { get; init; } = [];

    public List<KeyValueTransactionReadKey> ReadKeys { get; init; } = [];

    /// <summary>The first confirmed persistent modified key, or null if the transaction has no persistent write.</summary>
    public string? RecordAnchorKey { get; init; }

    public int PendingOperationCount { get; init; }
}
