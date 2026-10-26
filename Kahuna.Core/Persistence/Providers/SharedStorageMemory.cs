
using System.Collections.Concurrent;

namespace Kahuna.Server.Persistence.Providers;

/// <summary>
/// The memory one node's Raft WAL and its key-value and lock store share: a memtable budget
/// (<c>--storage-shared-memtable-budget-mb</c>) and a cache (<c>--storage-shared-cache-mb</c>). The node creates
/// one and hands the same instance to the WAL provider (<see cref="WalContext.SharedMemory"/>) and to the backend
/// provider (<see cref="PersistenceBackendContext.SharedMemory"/>), so two engines of the same kind can find one
/// set of accounting objects between them: each asks <see cref="GetOrAdd{T}"/> for its engine's own type and the
/// first to ask creates it.
/// </summary>
public sealed class SharedStorageMemory
{
    private readonly ConcurrentDictionary<Type, object> engineResources = new();

    /// <summary>
    /// Shared memory of a memtable budget of <paramref name="memtableBudgetBytes"/> and a cache of
    /// <paramref name="cacheBytes"/>; 0 is none of that kind, and at least one is above 0.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when a size is negative, or both are 0.</exception>
    public SharedStorageMemory(long memtableBudgetBytes, long cacheBytes)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(memtableBudgetBytes);
        ArgumentOutOfRangeException.ThrowIfNegative(cacheBytes);
        if (memtableBudgetBytes == 0 && cacheBytes == 0)
            throw new ArgumentOutOfRangeException(nameof(cacheBytes), "A shared memory needs a memtable budget or a cache.");

        MemtableBudgetBytes = memtableBudgetBytes;
        CacheBytes = cacheBytes;
    }

    /// <summary>The memtable budget in bytes that the WAL's and the store's memtables share; 0 when none.</summary>
    public long MemtableBudgetBytes { get; }

    /// <summary>
    /// The cache in bytes that the WAL and the store share; 0 when none. With a memtable budget too, the budget
    /// is charged into the cache, so one size bounds both, and it is at most this.
    /// </summary>
    public long CacheBytes { get; }

    /// <summary>
    /// The engine's accounting object of type <typeparamref name="T"/> for this memory, which
    /// <paramref name="create"/> makes on the first call for that type. Every later call for the type, from the
    /// WAL or the store, returns the same object. The node does not dispose it.
    /// </summary>
    public T GetOrAdd<T>(Func<SharedStorageMemory, T> create) where T : class
    {
        ArgumentNullException.ThrowIfNull(create);
        return (T)engineResources.GetOrAdd(typeof(T), static (_, state) => state.create(state.memory), (create, memory: this));
    }
}
