
using System.Diagnostics;
using Kahuna.Server.KeyValues;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// Quantifies the foreground write-latency impact of taking a backup checkpoint while a persistence
/// backend is under concurrent write load — the "online backup is not latency-free" claim in the
/// backups guide. Measured at the backend level (the checkpoint is exactly what a backup runs against
/// the backend: SQLite <c>VACUUM INTO</c> under a per-shard writer lock, RocksDB a near-instant
/// hard-link), so it isolates the shared-resource contention without the Raft/flush layers.
///
/// <para>Skipped by default (it is slow and its numbers are hardware-specific). Set the environment
/// variable <c>KAHUNA_BENCH=1</c> to run it, e.g.
/// <c>KAHUNA_BENCH=1 dotnet test --filter FullyQualifiedName~BenchmarkOnlineBackupImpact</c>. Read the
/// p95/p99 figures it prints from the test console output.</para>
/// </summary>
public sealed class BenchmarkOnlineBackupImpact
{
    private const int SeedKeys = 40_000;
    private const int ValueBytes = 1024;
    private const int Writers = 4;
    private const int BaselineSeconds = 5;
    private const int PostCheckpointSeconds = 3;
    private const int WarmupSeconds = 1;

    [Theory]
    [InlineData("sqlite")]
    [InlineData("rocksdb")]
    public void ForegroundWriteLatency_DuringCheckpoint(string backendName)
    {
        if (Environment.GetEnvironmentVariable("KAHUNA_BENCH") != "1")
        {
            Assert.Skip("Backup-impact benchmark. Set KAHUNA_BENCH=1 to run.");
            return;
        }

        string root = Path.Combine(Path.GetTempPath(), "kahuna_bench_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(root);
        IPersistenceBackend backend = MakeBackend(backendName, Path.Combine(root, "data"));
        try
        {
            Seed(backend);

            List<(long ticks, double ms)>[] perWriter = new List<(long, double)>[Writers];
            using CancellationTokenSource stop = new();
            long runStart = Stopwatch.GetTimestamp();

            Thread[] threads = new Thread[Writers];
            for (int w = 0; w < Writers; w++)
            {
                int id = w;
                perWriter[w] = new List<(long, double)>(200_000);
                threads[w] = new Thread(() =>
                {
                    byte[] value = new byte[ValueBytes];
                    long i = 0;
                    List<(long, double)> samples = perWriter[id];
                    while (!stop.IsCancellationRequested)
                    {
                        string key = $"bench/{id}/{i++}";
                        long ms = SystemMs();
                        List<PersistenceRequestItem> batch =
                            [new(key, value, 1, 0, 0, 0, 0, ms, 0, 0, ms, 0, (int)KeyValueState.Set)];
                        long t0 = Stopwatch.GetTimestamp();
                        backend.StoreKeyValues(batch);
                        samples.Add((t0, Stopwatch.GetElapsedTime(t0).TotalMilliseconds));
                    }
                }) { IsBackground = true };
                threads[w].Start();
            }

            Thread.Sleep(TimeSpan.FromSeconds(BaselineSeconds));

            // Take the checkpoint on this thread while the writers keep going — this is the backup's
            // bulk backend operation.
            string ckptPath = Path.Combine(root, "checkpoint_" + Guid.NewGuid().ToString("N")[..8]);
            long ckptStart = Stopwatch.GetTimestamp();
            backend.CreateCheckpoint(ckptPath, appliedIndex: 1, new HLCTimestamp(0, SystemMs(), 0));
            long ckptEnd = Stopwatch.GetTimestamp();

            Thread.Sleep(TimeSpan.FromSeconds(PostCheckpointSeconds));
            stop.Cancel();
            foreach (Thread t in threads) t.Join();

            long warmupCutoff = runStart + (long)(WarmupSeconds * Stopwatch.Frequency);
            List<double> baseline = [];
            List<double> during = [];
            foreach (List<(long ticks, double ms)> list in perWriter)
                foreach ((long ticks, double ms) in list)
                {
                    if (ticks < warmupCutoff) continue;
                    if (ticks < ckptStart) baseline.Add(ms);
                    else if (ticks <= ckptEnd) during.Add(ms);
                }

            double ckptMs = Stopwatch.GetElapsedTime(ckptStart, ckptEnd).TotalMilliseconds;
            System.Text.StringBuilder sb = new();
            void Emit(string s) { Console.WriteLine(s); sb.AppendLine(s); }
            Emit($"=== online-backup impact: {backendName} ===");
            Emit($"checkpoint duration: {ckptMs:F0} ms   writers: {Writers}   seed: {SeedKeys} x {ValueBytes}B");
            Emit(Report("baseline (no checkpoint)", baseline));
            Emit(Report("during checkpoint       ", during));
            Emit($"p99 inflation during checkpoint: {Ratio(Pct(during, 99), Pct(baseline, 99)):F1}x");

            string outPath = Environment.GetEnvironmentVariable("KAHUNA_BENCH_OUT")
                             ?? Path.Combine(Path.GetTempPath(), "kahuna_bench_results.txt");
            File.AppendAllText(outPath, sb.ToString());

            Assert.NotEmpty(baseline); // sanity: the run produced samples
        }
        finally
        {
            (backend as IDisposable)?.Dispose();
            try { Directory.Delete(root, recursive: true); } catch { /* best-effort */ }
        }
    }

    private static string Report(string label, List<double> samples)
    {
        if (samples.Count == 0) return $"{label}: (no samples)";
        return $"{label}: n={samples.Count,7}  p50={Pct(samples, 50):F3}ms  p95={Pct(samples, 95):F3}ms  " +
               $"p99={Pct(samples, 99):F3}ms  max={samples.Max():F3}ms";
    }

    private static double Pct(List<double> samples, int p)
    {
        if (samples.Count == 0) return 0;
        double[] sorted = samples.ToArray();
        Array.Sort(sorted);
        int idx = (int)Math.Ceiling(p / 100.0 * sorted.Length) - 1;
        return sorted[Math.Clamp(idx, 0, sorted.Length - 1)];
    }

    private static double Ratio(double a, double b) => b <= 0 ? 0 : a / b;

    private static long SystemMs() => (long)(DateTime.UtcNow - DateTime.UnixEpoch).TotalMilliseconds;

    private void Seed(IPersistenceBackend backend)
    {
        byte[] value = new byte[ValueBytes];
        const int batchSize = 1000;
        for (int start = 0; start < SeedKeys; start += batchSize)
        {
            List<PersistenceRequestItem> batch = new(batchSize);
            for (int i = start; i < Math.Min(start + batchSize, SeedKeys); i++)
            {
                long ms = SystemMs();
                batch.Add(new PersistenceRequestItem($"seed/{i}", value, 1, 0, 0, 0, 0, ms, 0, 0, ms, 0, (int)KeyValueState.Set));
            }
            backend.StoreKeyValues(batch);
        }
    }

    private static IPersistenceBackend MakeBackend(string name, string dir)
    {
        Directory.CreateDirectory(dir);
        return name switch
        {
            "rocksdb" => new RocksDbPersistenceBackend(dir, "bench"),
            "sqlite" => new SqlitePersistenceBackend(dir, "bench"),
            _ => throw new ArgumentException($"unknown backend {name}")
        };
    }
}
