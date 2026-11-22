using Kahuna.Utils;

namespace Kahuna.Server.Tests;

/// <summary>
/// The concurrent map that is stored as a fixed number of small concurrent maps, so that a resize is that of
/// one shard and no bucket array grows with the whole map. The ledger keeps the committed heads of a slice in
/// it: one writer under the apply gate, and readers that take no lock.
/// </summary>
public sealed class TestShardedConcurrentMap
{
    private static string Key(int i) => $"ranged1/accounts/k{i:D12}";

    [Fact]
    public void EveryEntryIsFound_SetReplaces_AndRemoveTakesOneEntryOut()
    {
        const int count = 50_000;
        ShardedConcurrentMap<long> map = new(shardCount: 32, concurrencyLevel: 1);

        Assert.False(map.TryGetValue(Key(0), out _));
        Assert.False(map.TryRemove(Key(0), out _));
        Assert.Empty(map);

        for (int i = 0; i < count; i++)
            map[Key(i)] = i;

        for (int i = 0; i < count; i++)
        {
            Assert.True(map.TryGetValue(Key(i), out long value), $"key {i} is missing");
            Assert.Equal(i, value);
        }

        Assert.False(map.TryGetValue(Key(count), out _));

        // A set over a held key replaces its value; an add over it does not.
        map[Key(7)] = -7;
        Assert.False(map.TryAdd(Key(7), 99));
        Assert.True(map.TryGetValue(Key(7), out long replaced));
        Assert.Equal(-7, replaced);

        Assert.True(map.TryAdd(Key(count), count));
        Assert.True(map.TryRemove(Key(count), out long removed));
        Assert.Equal(count, removed);
        Assert.False(map.TryGetValue(Key(count), out _));
        Assert.False(map.TryRemove(Key(count), out _));

        // A key that is equal and is not the same string object is the same key.
        Assert.True(map.TryGetValue(new string(Key(123).AsSpan()), out long byEqualKey));
        Assert.Equal(123, byEqualKey);
    }

    [Fact]
    public void AHashedKey_ProbesSeveralMaps_WithOneHash()
    {
        ShardedConcurrentMap<long> first = new(32, 1);
        ShardedConcurrentMap<long> second = new(32, 1);
        first[Key(1)] = 10;
        second[Key(2)] = 20;

        HashedKey one = new(Key(1));
        HashedKey two = new(Key(2));

        Assert.True(first.TryGetValue(one, out long a));
        Assert.Equal(10, a);
        Assert.False(second.TryGetValue(one, out _));
        Assert.True(second.TryGetValue(two, out long b));
        Assert.Equal(20, b);

        Assert.Equal(one, new HashedKey(new string(Key(1).AsSpan())));
        Assert.NotEqual(one, two);
    }

    [Fact]
    public void AnEnumeration_ReturnsEveryEntryOnce()
    {
        const int count = 20_000;
        ShardedConcurrentMap<long> map = new(64, 1);

        for (int i = 0; i < count; i++)
            map[Key(i)] = i;

        Dictionary<string, long> seen = new(StringComparer.Ordinal);
        foreach (KeyValuePair<string, long> entry in map)
            Assert.True(seen.TryAdd(entry.Key, entry.Value), $"{entry.Key} was returned twice");

        Assert.Equal(count, seen.Count);
        for (int i = 0; i < count; i++)
            Assert.Equal(i, seen[Key(i)]);
    }

    [Fact]
    public void AMapWithFewEntries_KeepsFewShards()
    {
        // The shards are made on the first write to each, so an idle slice does not pay for all of them.
        ShardedConcurrentMap<long> map = new(1_024, 1);
        map[Key(1)] = 1;
        map[Key(2)] = 2;

        long before = GC.GetAllocatedBytesForCurrentThread();
        Assert.True(map.TryGetValue(Key(1), out _));
        Assert.False(map.TryGetValue(Key(3), out _));
        Assert.False(map.TryRemove(Key(3), out _));
        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        // A read or a remove of an absent key makes no shard.
        Assert.True(allocated < 1_024, $"reads allocated {allocated} bytes");
        Assert.Equal(2, map.Count());
    }

    [Fact]
    public void TheShardCount_MustBeAPowerOfTwo()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new ShardedConcurrentMap<long>(1, 1));
        Assert.Throws<ArgumentOutOfRangeException>(() => new ShardedConcurrentMap<long>(48, 1));
        Assert.Throws<ArgumentOutOfRangeException>(() => new ShardedConcurrentMap<long>(32, 0));
    }

    [Fact]
    public async Task ReadersAndEnumerations_RunWithOneWriter_AndSeeEveryEntryFromBeforeThem()
    {
        const int before = 20_000;
        const int added = 60_000;

        ShardedConcurrentMap<long> map = new(32, 1);
        for (int i = 0; i < before; i++)
            map[Key(i)] = i;

        using CancellationTokenSource stop = new();
        CancellationToken ct = TestContext.Current.CancellationToken;

        // The writer adds entries, so every shard resizes several times, and it replaces the values of the
        // entries from before. It removes nothing: an entry from before must be seen by every reader.
        Task writer = Task.Run(() =>
        {
            for (int i = 0; i < added; i++)
            {
                map[Key(before + i)] = before + i;
                map[Key(i % before)] = -(i % before);
            }

            stop.Cancel();
        }, ct);

        Task[] readers = new Task[4];
        for (int r = 0; r < readers.Length; r++)
        {
            readers[r] = Task.Run(() =>
            {
                int walks = 0;

                while (!stop.IsCancellationRequested || walks == 0)
                {
                    for (int i = 0; i < before; i += 97)
                    {
                        Assert.True(map.TryGetValue(Key(i), out long value), $"key {i} is missing during the writes");
                        Assert.True(value == i || value == -i, $"key {i} has the value {value}");
                    }

                    HashSet<string> seen = new(StringComparer.Ordinal);
                    foreach (KeyValuePair<string, long> entry in map)
                        Assert.True(seen.Add(entry.Key), $"{entry.Key} was returned twice in one walk");

                    for (int i = 0; i < before; i++)
                        Assert.Contains(Key(i), seen);

                    walks++;
                }
            }, ct);
        }

        await writer;
        await Task.WhenAll(readers);

        Assert.Equal(before + added, map.Count());
    }
}
