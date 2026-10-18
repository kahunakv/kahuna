
using System.Diagnostics;
using Kahuna.Server.KeyValues;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Server.Persistence.Pitr;
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
/// <para>The plain checkpoint is the cheapest part of a full backup. <see cref="ForegroundWriteLatency_DuringCapture"/>
/// runs what a RocksDB full backup does to the disk: the as-of checkpoint with its trim and compaction, the hash
/// of the image, the publish into the backup directory, and the verify read of the published files. Set
/// <c>KAHUNA_BENCH_BACKUP_DIR</c> to a directory on another filesystem than the temp directory to measure the
/// cross-filesystem layout (a backup volume apart from the data volume); without it those cases skip.</para>
/// </summary>
public sealed class BenchmarkOnlineBackupImpact
{
    // KAHUNA_BENCH_SEED_KEYS overrides the seed size; a larger store makes the capture's disk work dominate.
    private static readonly int SeedKeys =
        int.TryParse(Environment.GetEnvironmentVariable("KAHUNA_BENCH_SEED_KEYS"), out int seedKeys) && seedKeys > 0 ? seedKeys : 40_000;
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

    /// <param name="layout">
    /// <c>same-fs</c>: the backup directory is on the store's filesystem, so RocksDB hard-links. <c>other-fs-copy</c>:
    /// the backup directory is on another filesystem and the checkpoint is written in place, so RocksDB copies every
    /// live SST. <c>other-fs-staged</c>: the same layout, staged on the store's filesystem with only the image copied.
    /// </param>
    /// <param name="throttleMbps">The capture budget in MB/s; 0 = unlimited.</param>
    [Theory]
    [InlineData("same-fs", 0)]
    [InlineData("same-fs", 50)]
    [InlineData("other-fs-copy", 0)]
    [InlineData("other-fs-copy", 50)]
    [InlineData("other-fs-staged", 0)]
    [InlineData("other-fs-staged", 50)]
    public async Task ForegroundWriteLatency_DuringCapture(string layout, int throttleMbps)
    {
        if (Environment.GetEnvironmentVariable("KAHUNA_BENCH") != "1")
        {
            Assert.Skip("Backup-impact benchmark. Set KAHUNA_BENCH=1 to run.");
            return;
        }

        string? otherFs = Environment.GetEnvironmentVariable("KAHUNA_BENCH_BACKUP_DIR");
        if (layout != "same-fs" && string.IsNullOrWhiteSpace(otherFs))
        {
            Assert.Skip("Set KAHUNA_BENCH_BACKUP_DIR to a directory on another filesystem to run the cross-filesystem cases.");
            return;
        }

        CancellationToken ct = TestContext.Current.CancellationToken;
        long throttle = throttleMbps * 1_000_000L;
        string root = Path.Combine(Path.GetTempPath(), "kahuna_bench_" + Guid.NewGuid().ToString("N"));
        string storagePath = Path.Combine(root, "data");
        string backupDir = layout == "same-fs"
            ? Path.Combine(root, "backups")
            : Path.Combine(otherFs!, "kahuna_bench_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(storagePath);

        RocksDbPersistenceBackend backend = new(storagePath, "bench", checkpointTrimBytesPerSec: throttle);
        try
        {
            Seed(backend);

            LiveCheckpointStaging? staging = layout == "other-fs-copy"
                ? null
                : new LiveCheckpointStaging(storagePath, Path.Combine(storagePath, "bench"),
                    [Path.Combine(storagePath, "backup-staging")], throttle);
            LocalDirectoryArtifactStore artifacts = new(backupDir, staging);

            async Task Capture()
            {
                Guid backupId = Guid.NewGuid();
                HLCTimestamp cut = new(0, SystemMs(), 0);
                await using (IBackupCheckpointStagingArea area = await artifacts.BeginLiveCheckpointAsync(backupId, ct))
                {
                    backend.CreateCheckpointAsOf(area.LocalPath, appliedIndex: 1, cut, ct);
                    await BackupArtifactVerifier.HashDirectoryAsync(area.LocalPath, "checkpoint/", throttle, ct);
                    BackupFilePermissions.RestrictTree(area.LocalPath);
                    await area.CommitAsync(ct);
                }

                // The verify step reads every published file again.
                await BackupArtifactVerifier.HashDirectoryAsync(
                    Path.Combine(backupDir, backupId.ToString("N"), LocalDirectoryArtifactStore.CheckpointDirectoryName),
                    "checkpoint/", throttle, ct);
            }

            await MeasureAsync($"rocksdb capture, {layout}, {(throttleMbps > 0 ? $"{throttleMbps} MB/s" : "unthrottled")}",
                backend, Capture);
        }
        finally
        {
            backend.Dispose();
            try { Directory.Delete(root, recursive: true); } catch { /* best-effort */ }
            try { Directory.Delete(backupDir, recursive: true); } catch { /* best-effort */ }
        }
    }

    // Runs the writers, then the capture on this thread while they keep going, and reports the percentiles.
    private static async Task MeasureAsync(string name, IPersistenceBackend backend, Func<Task> capture)
    {
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

        long captureStart = Stopwatch.GetTimestamp();
        await capture();
        long captureEnd = Stopwatch.GetTimestamp();

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
                if (ticks < captureStart) baseline.Add(ms);
                else if (ticks <= captureEnd) during.Add(ms);
            }

        System.Text.StringBuilder sb = new();
        void Emit(string s) { Console.WriteLine(s); sb.AppendLine(s); }
        Emit($"=== online-backup impact: {name} ===");
        Emit($"capture duration: {Stopwatch.GetElapsedTime(captureStart, captureEnd).TotalMilliseconds:F0} ms   writers: {Writers}   seed: {SeedKeys} x {ValueBytes}B");
        Emit(Report("baseline (no capture)", baseline));
        Emit(Report("during capture       ", during));
        Emit($"p99 inflation during capture: {Ratio(Pct(during, 99), Pct(baseline, 99)):F1}x");

        string outPath = Environment.GetEnvironmentVariable("KAHUNA_BENCH_OUT")
                         ?? Path.Combine(Path.GetTempPath(), "kahuna_bench_results.txt");
        File.AppendAllText(outPath, sb.ToString());

        Assert.NotEmpty(baseline);
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

    private static void Seed(IPersistenceBackend backend)
    {
        // Incompressible values, so the store's size on disk is the seed size: a zero-filled value compresses
        // to almost nothing and leaves the capture with no real disk work.
        byte[] value = new byte[ValueBytes];
        Random.Shared.NextBytes(value);
        const int batchSize = 1000;
        for (int start = 0; start < SeedKeys; start += batchSize)
        {
            List<PersistenceRequestItem> batch = new(batchSize);
            for (int i = start; i < Math.Min(start + batchSize, SeedKeys); i++)
            {
                long ms = SystemMs();
                // A different value per key: one shared value would let the compressor's dictionary find repeats.
                byte[] keyValue = (byte[])value.Clone();
                BitConverter.TryWriteBytes(keyValue, i);
                Random.Shared.NextBytes(keyValue.AsSpan(sizeof(int)));
                batch.Add(new PersistenceRequestItem($"seed/{i}", keyValue, 1, 0, 0, 0, 0, ms, 0, 0, ms, 0, (int)KeyValueState.Set));
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
