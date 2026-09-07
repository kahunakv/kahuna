using System.Collections.Concurrent;
using Kahuna.Shared.Routing;

namespace Kahuna.Client.Routing;

/// <summary>
/// A bounded, per-client cache of learned routes.
///
/// <para>
/// Sharded, and each shard evicts on its own. An exact global least-recently-used order would need
/// every hit to move a node in one shared list, which turns the read path — the path the cache
/// exists to make fast — into a contended write. Each shard instead evicts in insertion order
/// through a small ring, so a hit touches nothing and only learning a resource the shard has not
/// seen takes that shard's lock.
/// </para>
///
/// <para>
/// Every write is a compare-and-swap against the exact entry the writer read. A response or a
/// failure that arrives late — after a newer response already replaced the entry — therefore
/// cannot overwrite or evict the replacement; it loses the swap and is dropped.
/// </para>
/// </summary>
internal sealed class RouteCache
{
    private readonly Shard[] shards;

    private readonly int shardMask;

    /// <summary>How long a learned route is used before it must be observed again.</summary>
    public TimeSpan Lifetime { get; }

    public RouteCache(int capacity, TimeSpan lifetime)
    {
        if (capacity <= 0)
            throw new ArgumentOutOfRangeException(nameof(capacity), "Route cache capacity must be positive.");

        if (lifetime <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(lifetime), "Route hint lifetime must be positive.");

        Lifetime = lifetime;

        // One shard per core, rounded up to a power of two so the shard index is a mask rather than
        // a division, and capped so a large machine does not fragment a small cache into shards that
        // each hold a handful of entries.
        int shardCount = 1;
        int target = Math.Min(32, Math.Max(1, Environment.ProcessorCount));
        while (shardCount < target)
            shardCount <<= 1;

        // Never more shards than the cache holds entries, and still a power of two so the shard
        // index stays a mask: a count that is not one would leave whole shards unreachable.
        while (shardCount > capacity)
            shardCount >>= 1;

        shardMask = shardCount - 1;
        shards = new Shard[shardCount];

        int perShard = Math.Max(1, capacity / shardCount);

        for (int i = 0; i < shardCount; i++)
            shards[i] = new Shard(perShard);
    }

    /// <summary>Total entries held, summed across shards. For metrics and tests, not the hot path.</summary>
    public int Count
    {
        get
        {
            int total = 0;

            foreach (Shard shard in shards)
                total += shard.Entries.Count;

            return total;
        }
    }

    /// <summary>
    /// The live entry for <paramref name="key"/>, or null when none is cached or the cached one has
    /// expired. An expired entry is left in place: removing it here would turn every read into a
    /// write, and its slot is reclaimed by ordinary eviction.
    /// </summary>
    public RouteEntry? TryGet(in RouteCacheKey key)
    {
        Shard shard = shards[key.ShardOf(shardMask)];

        if (!shard.Entries.TryGetValue(key, out RouteEntry? entry))
            return null;

        return entry.IsValidAt(Environment.TickCount64) ? entry : null;
    }

    /// <summary>
    /// The entry for <paramref name="key"/> whether or not it has expired. The caller decides what
    /// an expired entry means; a writer treats it as absent, and a reader ignores it.
    /// </summary>
    public RouteEntry? Peek(in RouteCacheKey key)
    {
        shards[key.ShardOf(shardMask)].Entries.TryGetValue(key, out RouteEntry? entry);

        return entry;
    }

    /// <summary>
    /// Adds a route for a key the cache does not hold. Fails, changing nothing, when another caller
    /// added one first — which is how two requests that both missed a cold cache resolve: the loser
    /// learned the same owner, so there is nothing to repair.
    /// </summary>
    public bool TryAdd(in RouteCacheKey key, string endpoint, int partitionId, long generation, KahunaRouteProvenance provenance)
    {
        Shard shard = shards[key.ShardOf(shardMask)];

        if (!shard.Entries.TryAdd(key, Build(endpoint, partitionId, generation, provenance)))
            return false;

        shard.RecordInsertion(key);
        return true;
    }

    /// <summary>
    /// Replaces <paramref name="observed"/> with a fresh route. The write lands only while the cache
    /// still holds exactly that entry, so a response that overtook a newer one cannot undo the newer
    /// one's update.
    /// </summary>
    public bool TryReplace(in RouteCacheKey key, RouteEntry observed, string endpoint, int partitionId, long generation, KahunaRouteProvenance provenance)
    {
        Shard shard = shards[key.ShardOf(shardMask)];

        return shard.Entries.TryUpdate(key, Build(endpoint, partitionId, generation, provenance), observed);
    }

    private RouteEntry Build(string endpoint, int partitionId, long generation, KahunaRouteProvenance provenance) =>
        new(endpoint,
            partitionId,
            generation,
            provenance,
            Environment.TickCount64 + (long)Lifetime.TotalMilliseconds);

    /// <summary>
    /// One shard: its entries and the insertion ring that bounds them.
    ///
    /// <para>
    /// The ring holds the keys in the order this shard first stored them. Storing a new key past
    /// capacity overwrites the oldest slot and removes whatever key that slot named, which keeps
    /// the shard's entry count at its capacity without any per-hit bookkeeping. A key that was
    /// removed and re-added simply occupies a second slot; the stale slot removes nothing when it
    /// comes round, because the key it names is already gone or belongs to a newer slot.
    /// </para>
    /// </summary>
    private sealed class Shard
    {
        private readonly object gate = new();

        private readonly RouteCacheKey[] ring;

        private readonly bool[] occupied;

        private int next;

        public readonly ConcurrentDictionary<RouteCacheKey, RouteEntry> Entries;

        public Shard(int capacity)
        {
            ring = new RouteCacheKey[capacity];
            occupied = new bool[capacity];
            Entries = new ConcurrentDictionary<RouteCacheKey, RouteEntry>();
        }

        public void RecordInsertion(in RouteCacheKey key)
        {
            RouteCacheKey evicted = default;
            bool evict = false;

            lock (gate)
            {
                int slot = next;
                next = slot + 1 == ring.Length ? 0 : slot + 1;

                if (occupied[slot])
                {
                    evicted = ring[slot];
                    evict = true;
                }

                ring[slot] = key;
                occupied[slot] = true;
            }

            // Outside the lock: eviction touches the concurrent dictionary, and the ring's own
            // invariant does not depend on the removal having happened yet.
            if (evict && !evicted.Equals(key))
                Entries.TryRemove(evicted, out _);
        }

    }
}
