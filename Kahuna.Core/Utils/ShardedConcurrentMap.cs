using System.Collections;
using System.Collections.Concurrent;
using System.IO.Hashing;
using System.Runtime.InteropServices;

namespace Kahuna.Utils;

/// <summary>
/// Gives the hash that a <see cref="ShardedConcurrentMap{TKey,TValue,THasher}"/> selects a shard from. The
/// hash decides only how evenly the keys spread over the shards: a poor one makes some shards larger and
/// changes no result, because each shard is a complete map with the key's own comparer.
/// </summary>
internal interface IShardHasher<TKey> where TKey : notnull
{
    static abstract int ShardHash(TKey key);
}

/// <summary>Selects a shard from the key's own hash. For a key whose hash is cheap: a tuple of numbers, or a
/// key that stores its hash.</summary>
internal readonly struct KeyHashShardHasher<TKey> : IShardHasher<TKey> where TKey : notnull
{
    public static int ShardHash(TKey key) => key.GetHashCode();
}

/// <summary>
/// Selects a shard for a string key from a fast hash of all of its characters, seeded once for the process.
/// The randomized hash of <see cref="string.GetHashCode()"/> costs several times more, and the shard's map
/// hashes the string again with its own comparer, so the choice of a shard must be cheap.
/// </summary>
internal readonly struct OrdinalShardHasher : IShardHasher<string>
{
    private static readonly long Seed = Random.Shared.NextInt64();

    public static int ShardHash(string key) => (int)XxHash3.HashToUInt64(MemoryMarshal.AsBytes(key.AsSpan()), Seed);
}

/// <summary>
/// A concurrent map that is stored as a fixed number of small concurrent maps (shards), selected by a hash of
/// the key.
///
/// <para>One <see cref="ConcurrentDictionary{TKey,TValue}"/> keeps one bucket array for all of its entries and
/// replaces it at each doubling: at half a million entries that is an array of 5 MiB, and the dictionary also
/// makes a new node for every entry it holds during the resize. A map that is written on the apply path makes
/// those allocations there, where a failed allocation ends the process. With shards, a resize is that of one
/// shard: its array and its nodes are the total divided by the number of shards.</para>
///
/// <para>Each shard is a <see cref="ConcurrentDictionary{TKey,TValue}"/> with the default comparer of the key,
/// so a key is found by the same equality as in one dictionary, and a string key keeps the dictionary's fast
/// path for strings. <typeparamref name="THasher"/> gives the hash for the choice of the shard.</para>
///
/// <para>The concurrency contract is that of <see cref="ConcurrentDictionary{TKey,TValue}"/> for each
/// operation: reads take no lock and can run with writes. An enumeration walks the shards in order and is not
/// a point-in-time cut: an entry present for the whole walk is returned once, and one added or removed during
/// the walk is returned or not. The map has no count and no atomic copy: both would read every shard, and
/// neither is atomic across shards. An owner that needs a count keeps it with its writes, and an owner that
/// needs an atomic copy walks the map under the lock that its writers hold.</para>
/// </summary>
internal sealed class ShardedConcurrentMap<TKey, TValue, THasher> : IEnumerable<KeyValuePair<TKey, TValue>>
    where TKey : notnull
    where THasher : IShardHasher<TKey>
{
    // Created on the first write to a shard, so a map with few entries holds few shards.
    private readonly ConcurrentDictionary<TKey, TValue>?[] shards;

    private readonly int shardShift;

    private readonly int concurrencyLevel;

    /// <param name="shardCount">Number of shards; a power of two.</param>
    /// <param name="concurrencyLevel">Locks of one shard. 1 holds one lock object for each shard: two writers
    /// wait for each other only when they write to the same shard.</param>
    public ShardedConcurrentMap(int shardCount, int concurrencyLevel)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(shardCount, 2);
        ArgumentOutOfRangeException.ThrowIfLessThan(concurrencyLevel, 1);

        if (!int.IsPow2(shardCount))
            throw new ArgumentOutOfRangeException(nameof(shardCount), shardCount, "The number of shards must be a power of two.");

        shards = new ConcurrentDictionary<TKey, TValue>?[shardCount];
        shardShift = 32 - int.Log2(shardCount);
        this.concurrencyLevel = concurrencyLevel;
    }

    /// <summary>The hash that selects the shard of <paramref name="key"/>. A caller that probes several maps of
    /// this type for one key computes it once and passes it to each probe.</summary>
    public static int ShardHashOf(TKey key) => THasher.ShardHash(key);

    public bool TryGetValue(TKey key, out TValue value) => TryGetValue(key, THasher.ShardHash(key), out value);

    /// <summary>The form of <see cref="TryGetValue(TKey, out TValue)"/> for a caller that already has the
    /// result of <see cref="ShardHashOf"/> for the key.</summary>
    public bool TryGetValue(TKey key, int shardHash, out TValue value)
    {
        ConcurrentDictionary<TKey, TValue>? shard = Volatile.Read(ref shards[ShardOf(shardHash)]);

        if (shard is not null)
            return shard.TryGetValue(key, out value!);

        value = default!;
        return false;
    }

    public TValue this[TKey key]
    {
        set => ShardForWrite(key)[key] = value;
    }

    public bool TryAdd(TKey key, TValue value) => ShardForWrite(key).TryAdd(key, value);

    /// <summary>Replaces the value of <paramref name="key"/> when it is still <paramref name="comparisonValue"/>.</summary>
    public bool TryUpdate(TKey key, TValue newValue, TValue comparisonValue)
    {
        ConcurrentDictionary<TKey, TValue>? shard = ShardForRead(key);
        return shard is not null && shard.TryUpdate(key, newValue, comparisonValue);
    }

    public bool TryRemove(TKey key, out TValue value)
    {
        ConcurrentDictionary<TKey, TValue>? shard = ShardForRead(key);

        if (shard is not null)
            return shard.TryRemove(key, out value!);

        value = default!;
        return false;
    }

    /// <summary>Removes the entry when its value is still the one of <paramref name="item"/>.</summary>
    public bool TryRemove(KeyValuePair<TKey, TValue> item)
    {
        ConcurrentDictionary<TKey, TValue>? shard = ShardForRead(item.Key);
        return shard is not null && shard.TryRemove(item);
    }

    public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
    {
        for (int i = 0; i < shards.Length; i++)
        {
            ConcurrentDictionary<TKey, TValue>? shard = Volatile.Read(ref shards[i]);
            if (shard is null)
                continue;

            foreach (KeyValuePair<TKey, TValue> entry in shard)
                yield return entry;
        }
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    // The high bits of the multiplied hash select the shard. The shard's map selects a bucket from its own hash
    // of the key modulo its bucket count, so the two choices do not follow each other.
    private int ShardOf(int shardHash) => (int)(((uint)shardHash * 2654435769u) >> shardShift);

    private ConcurrentDictionary<TKey, TValue>? ShardForRead(TKey key) =>
        Volatile.Read(ref shards[ShardOf(THasher.ShardHash(key))]);

    private ConcurrentDictionary<TKey, TValue> ShardForWrite(TKey key)
    {
        ref ConcurrentDictionary<TKey, TValue>? slot = ref shards[ShardOf(THasher.ShardHash(key))];

        ConcurrentDictionary<TKey, TValue>? shard = Volatile.Read(ref slot);
        if (shard is not null)
            return shard;

        // Two first writers can meet here: one map is installed and both use it.
        ConcurrentDictionary<TKey, TValue> created = new(concurrencyLevel, capacity: 31);
        return Interlocked.CompareExchange(ref slot, created, null) ?? created;
    }
}
