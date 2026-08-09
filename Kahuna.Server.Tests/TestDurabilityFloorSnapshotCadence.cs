using System.Text;
using Kahuna;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// Pins the cadence of durability-floor snapshot-channel capture. Resolving the snapshot channels
/// (<c>TransactionRecordStore</c> / <c>CompletionReceiptStore</c> / <c>PreparedIntentStore</c>)
/// rewrites each store's entire per-partition snapshot file, and under sustained durable-2PC load
/// the stores mutate continuously while retaining minutes of settled outcomes — so capturing on
/// every background flush tick rewrites a monotonically growing file once per tick. On a loaded
/// node that is tens of MB of serialization and I/O per second, growing with the retention window:
/// it stalls the flush pipeline and collapses commit throughput (observed as a −30% decaying
/// regression in CamusDB's standalone workload).
///
/// The contract pinned here:
/// <list type="bullet">
/// <item>The periodic flush path captures snapshots at most once per
/// <c>CheckpointInterval</c> — NOT on every flush tick, no matter how much pending snapshot work
/// accumulates between ticks.</item>
/// <item>An explicit flush (<c>FlushPersistenceAsync</c> — backups, node close) always captures,
/// because its completion signal promises the durable state is as advanced as it can be made.</item>
/// <item>Once the cadence elapses, the periodic path captures again — the throttle delays the
/// floor by a bounded interval, it never freezes it.</item>
/// </list>
/// </summary>
public sealed class TestDurabilityFloorSnapshotCadence
{
    private readonly ILoggerFactory loggerFactory;

    public TestDurabilityFloorSnapshotCadence(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    private static EmbeddedKahunaOptions PersistentOptions(string storagePath, string walPath, TimeSpan checkpointInterval) => new()
    {
        InitialPartitions = 1,
        Storage = "sqlite",
        StoragePath = storagePath,
        StorageRevision = "floor-cadence",
        WalStorage = "sqlite",
        WalPath = walPath,
        WalRevision = "floor-cadence-wal",
        // Fast periodic flush ticks so the test window spans many of them.
        DirtyObjectsWriterDelay = 100,
        CheckpointInterval = checkpointInterval
    };

    /// <summary>
    /// One observation of the record store's snapshot file; two rewrites always differ because the
    /// write path replaces the file (fresh mtime) with freshly serialized content.
    /// </summary>
    private readonly record struct SnapshotObservation(bool Exists, DateTime LastWriteUtc, long Length);

    private static SnapshotObservation ObserveRecordSnapshot(string storagePath)
    {
        string[] files = Directory.GetFiles(storagePath, "transactionrecord_floor-cadence_p*.snapshot", SearchOption.AllDirectories);
        if (files.Length == 0)
            return new(false, default, 0);

        // Single data partition in this test; observe the one file.
        FileInfo info = new(files[0]);
        return new(true, info.LastWriteTimeUtc, info.Length);
    }

    [Fact]
    public async Task PeriodicFlush_DoesNotRewriteSnapshotPerTick_ExplicitFlushAlwaysCaptures()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        string storagePath = CreateTempDir("kahuna-floor-cadence-store-");
        string walPath = CreateTempDir("kahuna-floor-cadence-wal-");

        try
        {
            // Checkpoint interval far beyond the test window: any snapshot rewrite observed during
            // the transaction window can only come from a per-tick capture — the bug.
            await using EmbeddedKahunaNode node = new(PersistentOptions(storagePath, walPath, TimeSpan.FromMinutes(10)), loggerFactory);
            await node.StartAsync(ct);
            await node.WaitForLeaderForKeyAsync("cadence/k0", ct);

            // Each iteration commits a durable transaction (fresh pending snapshot work) and then
            // outwaits a full flush tick, so a per-tick capture would rewrite the snapshot between
            // consecutive observations and every observation would differ.
            HashSet<SnapshotObservation> periodicObservations = [];
            for (int i = 0; i < 12; i++)
            {
                KeyValueTransactionResult result = await node.Kahuna.TryExecuteTransactionScript(
                    Encoding.UTF8.GetBytes($"BEGIN SET `cadence/k{i}` 'v{i}' COMMIT END"), null, null);
                Assert.Equal(KeyValueResponseType.Set, result.Type);

                await Task.Delay(150, ct);
                periodicObservations.Add(ObserveRecordSnapshot(storagePath));
            }

            long durableRecords = ((KahunaManager)node.Kahuna).DurableTransactionRecordStore.Count;
            Assert.True(durableRecords > 0, "no durable transaction records were created; the workload did not exercise the snapshot channels");

            // At most the initial capture (first tick after start) may have landed inside the
            // window; a per-tick capture would have produced ~12 distinct observations.
            Assert.True(periodicObservations.Count <= 2,
                $"snapshot rewritten {periodicObservations.Count} times across 12 flush ticks; periodic capture is not being throttled to the checkpoint cadence");

            // An explicit flush must capture immediately even though the checkpoint cadence is
            // nowhere near elapsed: its completion promises fully-advanced durable state.
            SnapshotObservation beforeExplicit = ObserveRecordSnapshot(storagePath);
            await node.FlushAsync();
            SnapshotObservation afterExplicit = ObserveRecordSnapshot(storagePath);

            Assert.True(afterExplicit.Exists, "explicit flush did not persist the record store snapshot");
            Assert.NotEqual(beforeExplicit, afterExplicit);
        }
        finally
        {
            TryDeleteDir(storagePath);
            TryDeleteDir(walPath);
        }
    }

    [Fact]
    public async Task PeriodicFlush_CapturesAgain_OnceCheckpointCadenceElapses()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        string storagePath = CreateTempDir("kahuna-floor-cadence2-store-");
        string walPath = CreateTempDir("kahuna-floor-cadence2-wal-");

        try
        {
            // Cadence of 1s against 100ms ticks: over ~3s of continuous durable transactions the
            // periodic path must capture more than once (the throttle delays, never freezes) and
            // far fewer times than the ~30 ticks it spans (the throttle actually throttles).
            await using EmbeddedKahunaNode node = new(PersistentOptions(storagePath, walPath, TimeSpan.FromSeconds(1)), loggerFactory);
            await node.StartAsync(ct);
            await node.WaitForLeaderForKeyAsync("cadence2/k0", ct);

            HashSet<SnapshotObservation> observations = [];
            for (int i = 0; i < 24; i++)
            {
                KeyValueTransactionResult result = await node.Kahuna.TryExecuteTransactionScript(
                    Encoding.UTF8.GetBytes($"BEGIN SET `cadence2/k{i}` 'v{i}' COMMIT END"), null, null);
                Assert.Equal(KeyValueResponseType.Set, result.Type);

                await Task.Delay(130, ct);
                observations.Add(ObserveRecordSnapshot(storagePath));
            }

            // "absent" counts as one observation, so ≥3 distinct proves ≥2 real captures.
            Assert.True(observations.Count >= 3,
                $"only {observations.Count} distinct snapshot states over ~3s with a 1s cadence; the floor appears frozen instead of merely throttled");
            Assert.True(observations.Count <= 8,
                $"{observations.Count} distinct snapshot states over ~24 ticks with a 1s cadence; capture is closer to per-tick than to the checkpoint cadence");
        }
        finally
        {
            TryDeleteDir(storagePath);
            TryDeleteDir(walPath);
        }
    }

    private static string CreateTempDir(string prefix)
    {
        string path = Path.Combine(Path.GetTempPath(), prefix + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(path);
        return path;
    }

    private static void TryDeleteDir(string path)
    {
        try { if (Directory.Exists(path)) Directory.Delete(path, recursive: true); }
        catch { /* best-effort cleanup */ }
    }
}
