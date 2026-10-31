
using BenchmarkDotNet.Attributes;
using Kahuna.Server.KeyValues;
using Kahuna.Utils;

namespace Kahuna.Microbenchmarks;

/// <summary>
/// Point lookup cost of the resident store: the ordered <see cref="BTree{TKey,TValue}"/> alone (a binary search
/// per node that chases string references) against the <see cref="KeyValueStore"/> wrapper, whose point lookups
/// go through an ordinal hash index beside the tree.
///
/// <para>Keys have the shape <c>tbl/NN/row/NNNNNNNN</c> and are inserted in shuffled order, as a workload would
/// write them. Probe keys are distinct string instances from the stored ones, so neither structure can short-cut on
/// reference equality and every comparison reads characters. The resident set is spread over 32 stores the way
/// the actors spread it, so a cold probe misses the cache the way a request does.</para>
/// </summary>
[MemoryDiagnoser]
public class StorePointLookupBenchmark
{
    private const int Stores = 32;
    private const int ProbeBatch = 4096;

    /// <summary>Resident keys in total across the 32 stores: a small actor set, and the 1.6 M-key shape.</summary>
    [Params(50_000, 1_600_000)]
    public int TotalKeys;

    private BTree<string, KeyValueEntry>[] trees = [];
    private KeyValueStore[] stores = [];
    private string[] probes = [];
    private int[] probeStore = [];
    private int cursor;

    [GlobalSetup]
    public void Setup()
    {
        int perStore = TotalKeys / Stores;
        Random random = new(12345);

        trees = new BTree<string, KeyValueEntry>[Stores];
        stores = new KeyValueStore[Stores];
        // Stores × perStore keys exactly, so every probe index maps back to a store below the count.
        string[] all = new string[Stores * perStore];

        for (int s = 0; s < Stores; s++)
        {
            trees[s] = new BTree<string, KeyValueEntry>(KeyValueStore.DefaultOrder);
            stores[s] = new KeyValueStore(KeyValueStore.DefaultOrder, perStore);

            string[] keys = new string[perStore];
            for (int i = 0; i < perStore; i++)
                keys[i] = $"tbl/{s:D2}/row/{i:D8}";
            random.Shuffle(keys);

            for (int i = 0; i < perStore; i++)
            {
                KeyValueEntry entry = new() { Revision = i };
                trees[s].Insert(keys[i], entry);
                stores[s].Insert(keys[i], entry);
                all[(s * perStore) + i] = keys[i];
            }
        }

        // Distinct string instances for the probes, chosen uniformly over every store.
        probes = new string[ProbeBatch];
        probeStore = new int[ProbeBatch];
        for (int i = 0; i < ProbeBatch; i++)
        {
            int index = random.Next(all.Length);
            probes[i] = new string(all[index].AsSpan());
            probeStore[i] = index / perStore;
        }
    }

    [Benchmark(Baseline = true, OperationsPerInvoke = ProbeBatch)]
    public long TreeTryGetValue()
    {
        long hits = 0;
        for (int i = 0; i < ProbeBatch; i++)
        {
            if (trees[probeStore[i]].TryGetValue(probes[i], out KeyValueEntry? entry))
                hits += entry.Revision;
        }
        cursor++;
        return hits;
    }

    [Benchmark(OperationsPerInvoke = ProbeBatch)]
    public long IndexTryGetValue()
    {
        long hits = 0;
        for (int i = 0; i < ProbeBatch; i++)
        {
            if (stores[probeStore[i]].TryGetValue(probes[i], out KeyValueEntry? entry))
                hits += entry.Revision;
        }
        cursor++;
        return hits;
    }
}
