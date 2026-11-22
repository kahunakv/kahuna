using BenchmarkDotNet.Attributes;
using Kahuna.Server.KeyValues.Transactions;
using Kommander.Time;

namespace Kahuna.Microbenchmarks;

/// <summary>
/// Cost of the settled-identity memory on the apply path: one record for each key a transaction settles, and
/// one lookup for each prepare that applies. Both run under the apply gate of the node.
///
/// <para><b>Shape:</b> the memory holds <see cref="Identities"/> identities with 30-character keys, the
/// population of one partition at a few thousand transactions a second. The baseline is the one
/// <see cref="Dictionary{TKey,TValue}"/> the memory replaced, sized as it grows on its own. The sharded memory
/// selects a shard before each map operation and splits one shard for each few hundred records. In return no
/// array grows with the population: the memory diagnoser shows the bytes each fill allocates, and the
/// baseline's include the arrays of every resize. The identities are built in the setup, so the cost of
/// hashing the key, which both shapes pay once for each identity, is outside the numbers.</para>
/// </summary>
[MemoryDiagnoser]
public class SettledIdentityMemoryBenchmark
{
    [Params(100_000, 1_000_000)]
    public int Identities;

    private PreparedIntentStore.SettledIdentity[] identities = null!;
    private PreparedIntentStore.SettledIdentity[] absent = null!;

    private PreparedIntentStore.SettledIdentityMemory sharded = null!;
    private Dictionary<PreparedIntentStore.SettledIdentity, HLCTimestamp> single = null!;

    private static readonly HLCTimestamp SettledAt = new(0, 5_000, 0);

    [GlobalSetup]
    public void Setup()
    {
        identities = new PreparedIntentStore.SettledIdentity[Identities];
        absent = new PreparedIntentStore.SettledIdentity[Identities];

        for (int i = 0; i < Identities; i++)
        {
            identities[i] = new(new HLCTimestamp(0, 1_000 + i / 2, 0), 1, $"ranged1/accounts/k{i:D12}");
            absent[i] = new(new HLCTimestamp(0, 9_000_000 + i / 2, 0), 1, identities[i].Key);
        }

        sharded = FillSharded();
        single = FillSingle();
    }

    private PreparedIntentStore.SettledIdentityMemory FillSharded()
    {
        PreparedIntentStore.SettledIdentityMemory memory = new();
        foreach (PreparedIntentStore.SettledIdentity identity in identities)
            memory.Record(identity, SettledAt, replace: false, long.MinValue);
        return memory;
    }

    private Dictionary<PreparedIntentStore.SettledIdentity, HLCTimestamp> FillSingle()
    {
        Dictionary<PreparedIntentStore.SettledIdentity, HLCTimestamp> map = new();
        foreach (PreparedIntentStore.SettledIdentity identity in identities)
            map.TryAdd(identity, SettledAt);
        return map;
    }

    [Benchmark(Baseline = true)]
    public int RecordSingleMap() => FillSingle().Count;

    [Benchmark]
    public int RecordSharded() => FillSharded().Count;

    [Benchmark]
    public int LookupHitSingleMap()
    {
        int found = 0;
        foreach (PreparedIntentStore.SettledIdentity identity in identities)
        {
            if (single.TryGetValue(identity, out _))
                found++;
        }

        return found;
    }

    [Benchmark]
    public int LookupHitSharded()
    {
        int found = 0;
        foreach (PreparedIntentStore.SettledIdentity identity in identities)
        {
            if (sharded.TryGetValue(identity, out _))
                found++;
        }

        return found;
    }

    // The common case on the apply path: a prepare that is not a replay finds nothing.
    [Benchmark]
    public int LookupMissSingleMap()
    {
        int found = 0;
        foreach (PreparedIntentStore.SettledIdentity identity in absent)
        {
            if (single.TryGetValue(identity, out _))
                found++;
        }

        return found;
    }

    [Benchmark]
    public int LookupMissSharded()
    {
        int found = 0;
        foreach (PreparedIntentStore.SettledIdentity identity in absent)
        {
            if (sharded.TryGetValue(identity, out _))
                found++;
        }

        return found;
    }
}
