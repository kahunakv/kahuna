
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Transactions.Data;

/// <summary>
/// An immutable snapshot of a transaction session's working set, captured once the session is frozen
/// (closed to new operations and drained) by <c>CloseTransaction</c>. The session stays finalizable, so a
/// later commit or rollback finalizes the same frozen set; callers (e.g. a consumer publishing cache
/// keyspaces before commit) use this snapshot without holding a live reference to the context.
/// </summary>
internal sealed class WorkingSetSnapshot
{
    public IReadOnlySet<(string Key, KeyValueDurability Durability)>? LocksAcquired { get; init; }
    public IReadOnlySet<(string Key, KeyValueDurability Durability)>? PrefixLocksAcquired { get; init; }
    public IReadOnlyDictionary<RangeLockKey, RangeLockMode>? RangeLocksAcquired { get; init; }
    public IReadOnlySet<(string Key, KeyValueDurability Durability)>? ModifiedKeys { get; init; }
    public IReadOnlyDictionary<(string Key, KeyValueDurability Durability), KeyValueTransactionReadKey>? ReadKeys { get; init; }

    /// <summary>The first confirmed persistent modified key, or null if the transaction has no persistent write.</summary>
    public string? RecordAnchorKey { get; init; }

    public int PendingOperationCount { get; init; }
    public SessionLifecycle Lifecycle { get; init; }
}
