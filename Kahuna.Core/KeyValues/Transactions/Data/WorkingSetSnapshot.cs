
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Transactions.Data;

/// <summary>
/// An immutable snapshot of a transaction session's working set taken just before
/// the coordinator transitions the session to <see cref="SessionLifecycle.Terminal"/>.
/// Callers use this to drive the 2PC prepare/commit/rollback steps without holding
/// a live reference to the context.
/// </summary>
internal sealed class WorkingSetSnapshot
{
    public IReadOnlySet<(string Key, KeyValueDurability Durability)>? LocksAcquired { get; init; }
    public IReadOnlySet<(string Key, KeyValueDurability Durability)>? PrefixLocksAcquired { get; init; }
    public IReadOnlySet<(string Key, KeyValueDurability Durability)>? ModifiedKeys { get; init; }
    public IReadOnlyDictionary<(string Key, KeyValueDurability Durability), KeyValueTransactionReadKey>? ReadKeys { get; init; }
    public int PendingOperationCount { get; init; }
    public SessionLifecycle Lifecycle { get; init; }
}
