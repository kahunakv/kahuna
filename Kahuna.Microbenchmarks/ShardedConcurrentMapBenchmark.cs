using System.Collections.Concurrent;
using BenchmarkDotNet.Attributes;
using Kahuna.Utils;

namespace Kahuna.Microbenchmarks;

/// <summary>
/// Cost of the sharded map against the one <see cref="ConcurrentDictionary{TKey,TValue}"/> it replaces for the
/// committed heads of a ledger slice: one write for each key a transaction settles, and lookups on the apply
/// path and on advisory reads.
///
/// <para><b>Shape:</b> <see cref="Entries"/> 30-character keys and a 40-byte value, with one writer. Each
/// lookup hashes the key for the choice of the shard and the shard's map hashes it again, so both hashes are
/// inside the numbers. The memory diagnoser shows what a fill allocates: the single map's total includes the
/// bucket array and the nodes it makes again at each doubling.</para>
/// </summary>
[MemoryDiagnoser]
public class ShardedConcurrentMapBenchmark
{
    private readonly record struct Head(long Revision, int State, long CommittedAt, long Counter, long Node);

    private const int Shards = 32;

    [Params(50_000, 500_000)]
    public int Entries;

    private string[] keys = null!;
    private string[] absent = null!;

    private ConcurrentDictionary<string, Head> single = null!;
    private ShardedConcurrentMap<string, Head, OrdinalShardHasher> sharded = null!;

    [GlobalSetup]
    public void Setup()
    {
        keys = new string[Entries];
        absent = new string[Entries];

        for (int i = 0; i < Entries; i++)
        {
            keys[i] = $"ranged1/accounts/k{i:D12}";
            absent[i] = $"ranged1/accounts/a{i:D12}";
        }

        single = FillSingle();
        sharded = FillSharded();
    }

    private ConcurrentDictionary<string, Head> FillSingle()
    {
        ConcurrentDictionary<string, Head> map = new();
        for (int i = 0; i < keys.Length; i++)
            map[keys[i]] = new Head(i, 1, i, 0, 0);
        return map;
    }

    private ShardedConcurrentMap<string, Head, OrdinalShardHasher> FillSharded()
    {
        ShardedConcurrentMap<string, Head, OrdinalShardHasher> map = new(Shards, concurrencyLevel: 1);
        for (int i = 0; i < keys.Length; i++)
            map[keys[i]] = new Head(i, 1, i, 0, 0);
        return map;
    }

    [Benchmark(Baseline = true)]
    public object FillSingleMap() => FillSingle();

    [Benchmark]
    public object FillShardedMap() => FillSharded();

    [Benchmark]
    public int LookupHitSingleMap()
    {
        int found = 0;
        foreach (string key in keys)
        {
            if (single.TryGetValue(key, out _))
                found++;
        }

        return found;
    }

    [Benchmark]
    public int LookupHitShardedMap()
    {
        int found = 0;
        foreach (string key in keys)
        {
            if (sharded.TryGetValue(key, out _))
                found++;
        }

        return found;
    }

    [Benchmark]
    public int LookupMissSingleMap()
    {
        int found = 0;
        foreach (string key in absent)
        {
            if (single.TryGetValue(key, out _))
                found++;
        }

        return found;
    }

    [Benchmark]
    public int LookupMissShardedMap()
    {
        int found = 0;
        foreach (string key in absent)
        {
            if (sharded.TryGetValue(key, out _))
                found++;
        }

        return found;
    }
}
