using System.Diagnostics;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Server.KeyValues;
using Kommander.Time;
using Xunit;

namespace Kahuna.Server.Tests;

/// <summary>
/// Cost model probe for the targeted revision prune that runs inside every flush cycle:
/// hot keys accumulate revisions and the prune re-walks all of them on every cycle even when
/// nothing is old enough to delete. Explicit: set KAHUNA_BENCH_PRUNE=1 to run.
/// </summary>
public class BenchmarkRevisionPruneCost
{
    private static PersistenceRequestItem MakeItem(string key, long revision, long physical) =>
        new(key, new byte[200], revision, 0, 0, 0, 0, 0, 0, 0, physical, 0, (int)KeyValueState.Set);

    [Fact]
    [Trait("Category", "Performance")]
    public void MeasurePruneCostVsRevisionsPerKey()
    {
        if (Environment.GetEnvironmentVariable("KAHUNA_BENCH_PRUNE") != "1")
            return;

        int keyCount = int.Parse(Environment.GetEnvironmentVariable("KAHUNA_BENCH_KEYS") ?? "4000");
        int rounds = int.Parse(Environment.GetEnvironmentVariable("KAHUNA_BENCH_ROUNDS") ?? "300");
        int every = int.Parse(Environment.GetEnvironmentVariable("KAHUNA_BENCH_EVERY") ?? "25");
        string output = Environment.GetEnvironmentVariable("KAHUNA_BENCH_OUT") ?? "/tmp/prune-bench.txt";

        string path = Path.Combine(Path.GetTempPath(), "kahuna-prune-bench-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(path);

        using RocksDbPersistenceBackend backend = new(path, "v1");

        string[] keys = Enumerable.Range(0, keyCount).Select(i => $"1:1|r/{i:x24}").ToArray();
        long physical = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - 60_000;

        using StreamWriter log = new(output, append: false);
        log.WriteLine("revisions_per_key,store_ms_per_1024,prune_ms_all_keys,keys_visited,deleted");

        for (int round = 1; round <= rounds; round++)
        {
            Stopwatch storeWatch = Stopwatch.StartNew();
            int batches = 0;
            for (int offset = 0; offset < keyCount; offset += 1024)
            {
                List<PersistenceRequestItem> items = [];
                for (int i = offset; i < Math.Min(keyCount, offset + 1024); i++)
                    items.Add(MakeItem(keys[i], round, physical + round));
                Assert.True(backend.StoreKeyValues(items));
                batches++;
            }
            double storePerBatch = storeWatch.Elapsed.TotalMilliseconds / batches;

            if (round % every != 0 && round != 1)
                continue;

            Stopwatch pruneWatch = Stopwatch.StartNew();
            Assert.True(backend.PruneKeyValueRevisions(keys, 0, TimeSpan.FromHours(1), 1000, HLCTimestamp.Zero, out RevisionPruneResult result));
            pruneWatch.Stop();

            log.WriteLine($"{round},{storePerBatch:F2},{pruneWatch.Elapsed.TotalMilliseconds:F0},{result.KeysVisited},{result.RevisionsDeleted}");
            log.Flush();
        }

        try { Directory.Delete(path, true); } catch { /* best effort */ }
    }
}
