using System.Diagnostics;
using System.Text;
using Kahuna;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Data;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// Latency breakdown for a single-key persistent SET, the shape the interactive CLI issues
/// (`set pp 'aa'` runs as a one-statement transaction script). Reproduces the ~10 ms observed
/// server-side (the CLI prints the server's own stopwatch around TryExecuteTransactionScript)
/// and splits it across the layers so the cost can be attributed:
///
///   raft-propose : one raw ReplicateEntries auto-commit proposal (WAL append + fsync)
///   direct-set   : LocateAndTrySetKeyValue, no script, no transaction
///   script-set   : the full CLI path, TryExecuteTransactionScript("set pp 'aa'")
///   script-eset  : the same script against an ephemeral key (no durable barrier at all)
///
/// Manual benchmark; run with --filter. Prints a report, asserts nothing about thresholds.
/// </summary>
public sealed class BenchmarkSingleKeySet
{
    private readonly ITestOutputHelper output;
    private readonly ILoggerFactory loggerFactory;

    public BenchmarkSingleKeySet(ITestOutputHelper outputHelper)
    {
        output = outputHelper;
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    private static string Ms(double v) => v.ToString("F3");

    private static double Percentile(List<double> sorted, double p)
    {
        if (sorted.Count == 0)
            return 0;

        int idx = (int)Math.Ceiling(p / 100.0 * sorted.Count) - 1;
        return sorted[Math.Clamp(idx, 0, sorted.Count - 1)];
    }

    private void Report(string name, List<double> samples)
    {
        if (samples.Count == 0)
        {
            output.WriteLine($"{name,-14} (no samples)");
            return;
        }

        samples.Sort();
        double mean = samples.Average();
        output.WriteLine(
            $"{name,-14} n={samples.Count,4}  mean={Ms(mean),8}ms  p50={Ms(Percentile(samples, 50)),8}ms  " +
            $"p90={Ms(Percentile(samples, 90)),8}ms  p99={Ms(Percentile(samples, 99)),8}ms  max={Ms(samples[^1]),8}ms");
    }

    private async Task RunAsync(string storage, string walStorage, bool walSync, int partitions, int lingerMs = 1)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        const int warmup = 20;
        const int iters = 100;

        string root = Path.Combine(Path.GetTempPath(), "kahuna-set-bench-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(Path.Combine(root, "wal"));
        Directory.CreateDirectory(Path.Combine(root, "data"));

        await using EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            Storage = storage,
            StoragePath = Path.Combine(root, "data"),
            StorageRevision = "v1",
            WalStorage = walStorage,
            WalPath = Path.Combine(root, "wal"),
            WalRevision = "v1",
            WalSyncWrites = walSync,
            InitialPartitions = partitions,
            KeyValueWriteLingerMs = lingerMs
        }, loggerFactory);

        await node.StartAsync(ct);

        for (int p = 0; p < partitions; p++)
            await node.Raft.WaitForLeader(p, ct);

        IKahuna kahuna = node.Kahuna;

        byte[] setScript = "set pp 'aa'"u8.ToArray();
        byte[] esetScript = "eset pp 'aa'"u8.ToArray();
        byte[] value = "aa"u8.ToArray();

        List<double> raftPropose = new(iters);
        List<double> directSet = new(iters);
        List<double> scriptSet = new(iters);
        List<double> scriptEset = new(iters);

        Stopwatch sw = new();

        for (int i = -warmup; i < iters; i++)
        {
            bool measure = i >= 0;

            sw.Restart();
            await kahuna.TryExecuteTransactionScript(setScript, "setpp", null);
            double scriptMs = sw.Elapsed.TotalMilliseconds;

            sw.Restart();
            await kahuna.TryExecuteTransactionScript(esetScript, "esetpp", null);
            double esetMs = sw.Elapsed.TotalMilliseconds;

            sw.Restart();
            await kahuna.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero,
                "dd",
                value,
                null,
                0,
                KeyValueFlags.Set,
                0,
                KeyValueDurability.Persistent,
                ct);
            double directMs = sw.Elapsed.TotalMilliseconds;

            sw.Restart();
            await node.Raft.ReplicateEntries(0, [
                new RaftProposalEntry("bench", Encoding.UTF8.GetBytes("bench-" + i), AutoCommit: true, ExpectedGeneration: 0)
            ], ct);
            double raftMs = sw.Elapsed.TotalMilliseconds;

            if (measure)
            {
                scriptSet.Add(scriptMs);
                scriptEset.Add(esetMs);
                directSet.Add(directMs);
                raftPropose.Add(raftMs);
            }
        }

        output.WriteLine($"=== single-key SET breakdown (storage={storage} wal={walStorage} sync={walSync} partitions={partitions} linger={lingerMs}, iters={iters}) ===");
        Report("raft-propose", raftPropose);
        Report("direct-set", directSet);
        Report("script-eset", scriptEset);
        Report("script-set", scriptSet);

        // Concurrency probe: if the per-set floor is a fixed wait (timer / linger / barrier round trip)
        // rather than CPU work, N concurrent sets to distinct keys finish in roughly one set's latency.
        foreach (int concurrency in (int[])[1, 8, 64])
        {
            sw.Restart();
            Task[] tasks = new Task[concurrency];
            for (int i = 0; i < concurrency; i++)
            {
                string key = $"conc/{concurrency}/{i}";
                tasks[i] = kahuna.LocateAndTrySetKeyValue(
                    HLCTimestamp.Zero, key, value, null, 0, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
            }

            await Task.WhenAll(tasks);
            double batchMs = sw.Elapsed.TotalMilliseconds;
            output.WriteLine($"concurrent x{concurrency,-3}  total={Ms(batchMs),8}ms  per-op={Ms(batchMs / concurrency),8}ms");
        }

        output.WriteLine("");

        try { Directory.Delete(root, true); } catch { /* best effort */ }
    }

    [Fact]
    public async Task Benchmark_TaskDelayResolution()
    {
        List<double> d1 = new(200);
        Stopwatch sw = new();

        for (int i = 0; i < 200; i++)
        {
            sw.Restart();
            await Task.Delay(TimeSpan.FromMilliseconds(1), TimeProvider.System);
            d1.Add(sw.Elapsed.TotalMilliseconds);
        }

        Report("Task.Delay(1ms)", d1);
    }

    [Fact]
    public async Task Benchmark_SingleKeySet_RocksDbSyncWal()
    {
        await RunAsync("rocksdb", "rocksdb", walSync: true, partitions: 3);
    }

    [Fact]
    public async Task Benchmark_SingleKeySet_Memory()
    {
        await RunAsync("memory", "memory", walSync: false, partitions: 3);
    }

    [Fact]
    public async Task Benchmark_SingleKeySet_MemoryNoLinger()
    {
        await RunAsync("memory", "memory", walSync: false, partitions: 3, lingerMs: 0);
    }

    [Fact]
    public async Task Benchmark_SingleKeySet_RocksDbNoSync()
    {
        await RunAsync("rocksdb", "rocksdb", walSync: false, partitions: 3);
    }

    [Fact]
    public async Task Benchmark_SingleKeySet_RocksDbSyncWalNoLinger()
    {
        await RunAsync("rocksdb", "rocksdb", walSync: true, partitions: 3, lingerMs: 0);
    }
}
