using System.Collections;
using System.Collections.Concurrent;

namespace Kahuna.Utils;

/// <summary>
/// A string key together with its hash, computed once. A caller that probes several maps for one key builds
/// the hashed key once and passes it to each probe.
/// </summary>
internal readonly struct HashedKey : IEquatable<HashedKey>
{
    public readonly string Key;

    public readonly int Hash;

    public HashedKey(string key)
    {
        Key = key;
        Hash = key.GetHashCode();
    }

    public bool Equals(HashedKey other) => Hash == other.Hash && string.Equals(Key, other.Key);

    public override bool Equals(object? obj) => obj is HashedKey other && Equals(other);

    public override int GetHashCode() => Hash;
}

/// <summary>
/// A concurrent map from string keys to values that is stored as a fixed number of small concurrent maps
/// (shards), selected by the hash of the key.
///
/// <para>One <see cref="ConcurrentDictionary{TKey,TValue}"/> keeps one bucket array for all of its entries and
/// replaces it at each doubling: at half a million entries that is an array of 5 MiB, and the dictionary also
/// makes a new node for every entry it holds during the resize. A map that is written on the apply path makes
/// those allocations there, where a failed allocation ends the process. With shards, a resize is that of one
/// shard: its array and its nodes are the total divided by the number of shards.</para>
///
/// <para>The key is hashed once for an operation. The shard is selected from that hash, and the shard's map
/// reads the same hash from the <see cref="HashedKey"/> it stores, where a plain string key is hashed again
/// by the inner map. The stored hash adds 8 bytes to an entry.</para>
///
/// <para>The concurrency contract is that of <see cref="ConcurrentDictionary{TKey,TValue}"/> for each
/// operation: reads take no lock and can run with writes. An enumeration walks the shards in order and is not
/// a point-in-time cut: an entry present for the whole walk is returned once, and one added or removed during
/// the walk is returned or not. The map has no count: a count over the shards is not atomic, so an owner that
/// needs one keeps it with its writes.</para>
/// </summary>
internal sealed class ShardedConcurrentMap<TValue> : IEnumerable<KeyValuePair<string, TValue>>
{
    // Created on the first write to a shard, so a map with few entries holds few shards.
    private readonly ConcurrentDictionary<HashedKey, TValue>?[] shards;

    private readonly int shardShift;

    private readonly int concurrencyLevel;

    /// <param name="shardCount">Number of shards; a power of two.</param>
    /// <param name="concurrencyLevel">Writers expected on one shard at a time. 1 for a map with one writer,
    /// which then holds one lock object for each shard.</param>
    public ShardedConcurrentMap(int shardCount, int concurrencyLevel)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(shardCount, 2);
        ArgumentOutOfRangeException.ThrowIfLessThan(concurrencyLevel, 1);

        if (!int.IsPow2(shardCount))
            throw new ArgumentOutOfRangeException(nameof(shardCount), shardCount, "The number of shards must be a power of two.");

        shards = new ConcurrentDictionary<HashedKey, TValue>?[shardCount];
        shardShift = 32 - int.Log2(shardCount);
        this.concurrencyLevel = concurrencyLevel;
    }

    public bool TryGetValue(string key, out TValue value) => TryGetValue(new HashedKey(key), out value);

    public bool TryGetValue(in HashedKey key, out TValue value)
    {
        ConcurrentDictionary<HashedKey, TValue>? shard = Volatile.Read(ref shards[ShardOf(key.Hash)]);

        if (shard is not null)
            return shard.TryGetValue(key, out value!);

        value = default!;
        return false;
    }

    public TValue this[string key]
    {
        set
        {
            HashedKey hashed = new(key);
            ShardForWrite(hashed.Hash)[hashed] = value;
        }
    }

    public bool TryAdd(string key, TValue value)
    {
        HashedKey hashed = new(key);
        return ShardForWrite(hashed.Hash).TryAdd(hashed, value);
    }

    public bool TryRemove(string key, out TValue value)
    {
        HashedKey hashed = new(key);
        ConcurrentDictionary<HashedKey, TValue>? shard = Volatile.Read(ref shards[ShardOf(hashed.Hash)]);

        if (shard is not null)
            return shard.TryRemove(hashed, out value!);

        value = default!;
        return false;
    }

    public IEnumerator<KeyValuePair<string, TValue>> GetEnumerator()
    {
        for (int i = 0; i < shards.Length; i++)
        {
            ConcurrentDictionary<HashedKey, TValue>? shard = Volatile.Read(ref shards[i]);
            if (shard is null)
                continue;

            foreach (KeyValuePair<HashedKey, TValue> entry in shard)
                yield return new KeyValuePair<string, TValue>(entry.Key.Key, entry.Value);
        }
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    // The high bits of the multiplied hash select the shard. The shard's map selects a bucket from the hash
    // modulo its bucket count, so the two choices do not follow each other.
    private int ShardOf(int hash) => (int)(((uint)hash * 2654435769u) >> shardShift);

    private ConcurrentDictionary<HashedKey, TValue> ShardForWrite(int hash)
    {
        ref ConcurrentDictionary<HashedKey, TValue>? slot = ref shards[ShardOf(hash)];

        ConcurrentDictionary<HashedKey, TValue>? shard = Volatile.Read(ref slot);
        if (shard is not null)
            return shard;

        // Two first writers can meet here: one map is installed and both use it.
        ConcurrentDictionary<HashedKey, TValue> created = new(concurrencyLevel, capacity: 31);
        return Interlocked.CompareExchange(ref slot, created, null) ?? created;
    }
}
