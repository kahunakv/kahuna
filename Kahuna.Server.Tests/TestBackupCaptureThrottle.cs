using System.Diagnostics;
using Kahuna.Server.KeyValues;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Server.Persistence.Pitr;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// The capture I/O budget: a RocksDB rate limiter on the trim of the checkpoint copy, and paced hash and verify
/// reads. The budget must slow the work down and must not change what the backup holds.
/// </summary>
public sealed class TestBackupCaptureThrottle : IDisposable
{
    private readonly string _tempRoot = Path.Combine(Path.GetTempPath(), "kahuna_throttle_" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        if (Directory.Exists(_tempRoot))
            Directory.Delete(_tempRoot, recursive: true);
    }

    private static HLCTimestamp T(long ticks) => new(0, ticks, 0);

    private static PersistenceRequestItem ValItem(string key, byte[] value, long rev, long physicalMs) =>
        new(key, value, rev, 0, 0, 0, 0, physicalMs, 0, 0, physicalMs, 0, 1);

    private static byte[] RandomBytes(int length)
    {
        byte[] bytes = new byte[length];
        Random.Shared.NextBytes(bytes);
        return bytes;
    }

    // Writes rows before and after the cut, then takes an as-of checkpoint; returns how long the checkpoint took.
    private TimeSpan CheckpointAsOf(string name, long trimBytesPerSec, int keys, int valueBytes)
    {
        string baseDir = Path.Combine(_tempRoot, name);
        Directory.CreateDirectory(baseDir);

        using RocksDbPersistenceBackend backend = new(baseDir, "v1", checkpointTrimBytesPerSec: trimBytesPerSec);
        for (int i = 0; i < keys; i++)
        {
            backend.StoreKeyValues([ValItem($"k/{i:D4}", RandomBytes(valueBytes), rev: 1, physicalMs: 50)]);
            backend.StoreKeyValues([ValItem($"k/{i:D4}", [0xEE], rev: 2, physicalMs: 200)]);
        }

        long start = Stopwatch.GetTimestamp();
        backend.CreateCheckpointAsOf(Path.Combine(baseDir, "cp"), appliedIndex: 1, cut: T(100), ct: TestContext.Current.CancellationToken);
        return Stopwatch.GetElapsedTime(start);
    }

    [Fact]
    public void RocksDb_TrimUnderBudget_ProducesTheSameAsOfImage()
    {
        CheckpointAsOf("budget", trimBytesPerSec: 64L * 1024 * 1024, keys: 200, valueBytes: 64);

        using RocksDbPersistenceBackend image = new(Path.Combine(_tempRoot, "budget"), "cp");
        for (int i = 0; i < 200; i++)
        {
            KeyValueEntry? entry = image.GetKeyValue($"k/{i:D4}");
            Assert.NotNull(entry);
            Assert.Equal(1, entry!.Revision);
            Assert.Equal(64, entry.Value!.Length);
        }
    }

    [Fact]
    public void RocksDb_TrimUnderBudget_TakesAtLeastTheBudgetedTime()
    {
        // About 2 MB of incompressible pre-cut values. Replaying the copied WAL, the scan and the compaction
        // each read or write them, so at 4 MB/s the trim needs well over one second; without the limiter it
        // takes a small fraction of that.
        TimeSpan unlimited = CheckpointAsOf("unlimited", trimBytesPerSec: 0, keys: 128, valueBytes: 16 * 1024);
        TimeSpan limited = CheckpointAsOf("limited", trimBytesPerSec: 4 * 1024 * 1024, keys: 128, valueBytes: 16 * 1024);

        Assert.True(limited >= TimeSpan.FromSeconds(1), $"limited trim took {limited}, unlimited {unlimited}");
        TestContext.Current.TestOutputHelper?.WriteLine($"limited trim took {limited}, unlimited {unlimited}");
    }

    [Fact]
    public async Task HashDirectory_Paced_SameDigestsAndAtLeastTheBudgetedTime()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string dir = Path.Combine(_tempRoot, "hash");
        Directory.CreateDirectory(Path.Combine(dir, "sub"));
        await File.WriteAllBytesAsync(Path.Combine(dir, "a.sst"), RandomBytes(1024 * 1024), ct);
        await File.WriteAllBytesAsync(Path.Combine(dir, "sub", "b.sst"), RandomBytes(1024 * 1024), ct);

        (Dictionary<string, string> expected, Dictionary<string, long> expectedSizes) =
            BackupArtifactVerifier.HashDirectory(dir, "checkpoint/", ct);

        long start = Stopwatch.GetTimestamp();
        (Dictionary<string, string> paced, Dictionary<string, long> pacedSizes) =
            await BackupArtifactVerifier.HashDirectoryAsync(dir, "checkpoint/", throttleBytesPerSec: 1024 * 1024, ct);
        TimeSpan elapsed = Stopwatch.GetElapsedTime(start);

        Assert.Equal(expected, paced);
        Assert.Equal(expectedSizes, pacedSizes);
        // 2 MB at 1 MB/s: the budget holds across both files, not per file.
        Assert.True(elapsed >= TimeSpan.FromSeconds(1.5), $"paced hash took {elapsed}");
    }

    [Fact]
    public async Task ThroughputPacer_Unlimited_NeverWaits()
    {
        ThroughputPacer pacer = new(0);
        Assert.True(pacer.IsUnlimited);

        long start = Stopwatch.GetTimestamp();
        for (int i = 0; i < 100; i++)
            await pacer.PaceAsync(64 * 1024 * 1024, TestContext.Current.CancellationToken);
        Assert.True(Stopwatch.GetElapsedTime(start) < TimeSpan.FromSeconds(1));
    }
}
