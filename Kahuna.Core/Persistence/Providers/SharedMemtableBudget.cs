
using System.Collections.Concurrent;

namespace Kahuna.Server.Persistence.Providers;

/// <summary>
/// One node's memtable budget, which its Raft WAL and its key-value and lock store share
/// (<c>--storage-shared-memtable-budget-mb</c>). The node creates one and hands the same instance to the WAL
/// provider (<see cref="WalContext.SharedMemtableBudget"/>) and to the backend provider
/// (<see cref="PersistenceBackendContext.SharedMemtableBudget"/>), so two engines of the same kind can find one
/// accounting object between them: each asks <see cref="GetOrAdd{T}"/> for its engine's own type and the first
/// to ask creates it.
/// </summary>
public sealed class SharedMemtableBudget
{
    private readonly ConcurrentDictionary<Type, object> engineBudgets = new();

    /// <summary>A budget of <paramref name="bytes"/> bytes of memtable memory.</summary>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="bytes"/> is below 1.</exception>
    public SharedMemtableBudget(long bytes)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(bytes, 1);
        Bytes = bytes;
    }

    /// <summary>The budget in bytes.</summary>
    public long Bytes { get; }

    /// <summary>
    /// The engine's accounting object of type <typeparamref name="T"/> for this budget, which
    /// <paramref name="create"/> makes from <see cref="Bytes"/> on the first call for that type. Every later call
    /// for the type, from the WAL or the store, returns the same object. The node does not dispose it.
    /// </summary>
    public T GetOrAdd<T>(Func<long, T> create) where T : class
    {
        ArgumentNullException.ThrowIfNull(create);
        return (T)engineBudgets.GetOrAdd(typeof(T), static (_, state) => state.create(state.bytes), (create, bytes: Bytes));
    }
}
