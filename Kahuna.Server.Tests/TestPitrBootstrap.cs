
using System.Text;
using Google.Protobuf;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Server.Persistence.Pitr;
using Kahuna.Server.Replication;
using Kahuna.Server.Replication.Protos;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Data;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kahuna.Server.Tests;

/// <summary>
/// Unit tests for <see cref="BootstrapHelper"/>: guard rail checks, per-partition WAL
/// checkpoint seeding, and end-to-end KV data restoration through the bootstrap path.
/// These tests use in-memory backends and do not require a live cluster.
/// </summary>
public sealed class TestPitrBootstrap : IDisposable
{
    private readonly string _tempRoot =
        Path.Combine(Path.GetTempPath(), "kahuna_bootstrap_" + Guid.NewGuid().ToString("N"));

    private static readonly ILogger<IRaft> Log = NullLogger<IRaft>.Instance;

    public void Dispose()
    {
        if (Directory.Exists(_tempRoot))
            Directory.Delete(_tempRoot, recursive: true);
    }

    // ── helpers ──────────────────────────────────────────────────────────────────────────

    private string ArtifactsDir(string name) =>
        Path.Combine(_tempRoot, "artifacts_" + name);

    private BackupCatalog NewCatalog(string name) =>
        new(new LocalDirectoryStorageTarget(Path.Combine(_tempRoot, "catalog_" + name)));

    private static RaftPartitionRange Part(int id) => new()
    {
        PartitionId = id,
        State = RaftPartitionState.Active
    };

    private static HLCTimestamp T(long ms) => new(0, ms, 0);

    private static DateTime NowUtc(long ms) =>
        DateTime.UnixEpoch + TimeSpan.FromMilliseconds(ms);

    private static RaftLog KvLog(long id, long timeMs, string key, string value, long revision, long term = 1)
    {
        KeyValueMessage msg = new()
        {
            Type = (int)KeyValueRequestType.TrySet,
            Key = key,
            Value = UnsafeByteOperations.UnsafeWrap(Encoding.UTF8.GetBytes(value)),
            Revision = revision,
            LastModifiedPhysical = timeMs
        };
        return new RaftLog
        {
            Id = id,
            Term = term,
            Type = RaftLogType.Committed,
            Time = T(timeMs),
            LogType = ReplicationTypes.KeyValues,
            LogData = ReplicationSerializer.Serialize(msg)
        };
    }

    private static InMemoryWAL BuildWal(params (int partition, RaftLog log)[] entries)
    {
        InMemoryWAL wal = new(Log);
        Dictionary<int, List<RaftLog>> byPartition = [];
        foreach ((int partition, RaftLog log) in entries)
        {
            if (!byPartition.TryGetValue(partition, out List<RaftLog>? list))
                byPartition[partition] = list = [];
            list.Add(log);
        }
        foreach ((int partition, List<RaftLog> logs) in byPartition)
            wal.Write([(partition, logs)]);
        return wal;
    }

    /// Returns the CommittedCheckpoint entry written for partitionId, or null if absent.
    private static RaftLog? FindCheckpoint(InMemoryWAL wal, int partitionId)
    {
        List<RaftLog> logs = wal.ReadLogsRange(partitionId, 0);
        return logs.FirstOrDefault(l => l.Type == RaftLogType.CommittedCheckpoint);
    }

    // ── guard rail tests ──────────────────────────────────────────────────────────────────

    [Fact]
    public async Task GuardRail_TargetOlderThanWindow_ThrowsWithCompactionMessage()
    {
        // Arrange: full backup with one KV entry at t=100, window is 1 h, now is t=3,700,000 ms.
        // targetTime t=100 is far outside the 1-hour window floor.
        string artifacts = ArtifactsDir("guard_outside");
        BackupCatalog catalog = NewCatalog("guard_outside");

        InMemoryWAL wal = BuildWal((1, KvLog(1, 100, "k", "v", 1)));
        MemoryPersistenceBackend src = new();

        BackupManifest full = await BackupDriver.RunFullAsync(wal, [Part(1)], src, artifacts, catalog);

        IReadOnlyList<BackupManifest> chain = catalog.ResolveAndValidate(full.BackupId);
        string cpPath = Path.Combine(artifacts, full.BackupId.ToString("N"), "checkpoint");
        MemoryPersistenceBackend dst = MemoryPersistenceBackend.OpenCheckpoint(cpPath);
        InMemoryWAL dstWal = new(Log);

        long nowMs = 3_700_000;
        TimeSpan window = TimeSpan.FromHours(1);

        BackupDriverException ex = await Assert.ThrowsAsync<BackupDriverException>(() =>
            Task.FromResult(BootstrapHelper.BootstrapNode(
                chain, artifacts, T(100), dst, dstWal, window, NowUtc(nowMs))));

        Assert.Contains("compacted past", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task GuardRail_TargetWithinWindow_DoesNotThrow()
    {
        // Arrange: full backup at t=100, window = 1 h, now = t=200 ms (well within window).
        string artifacts = ArtifactsDir("guard_inside");
        BackupCatalog catalog = NewCatalog("guard_inside");

        InMemoryWAL wal = BuildWal((1, KvLog(1, 100, "k", "v", 1)));
        MemoryPersistenceBackend src = new();

        BackupManifest full = await BackupDriver.RunFullAsync(wal, [Part(1)], src, artifacts, catalog);
        IReadOnlyList<BackupManifest> chain = catalog.ResolveAndValidate(full.BackupId);

        string cpPath = Path.Combine(artifacts, full.BackupId.ToString("N"), "checkpoint");
        MemoryPersistenceBackend dst = MemoryPersistenceBackend.OpenCheckpoint(cpPath);
        InMemoryWAL dstWal = new(Log);

        // now = 200 ms, window = 1 h → floor ≈ −3,599,800 ms (very early) → T(100) is inside.
        BootstrapHelper.BootstrapNode(chain, artifacts, T(100), dst, dstWal,
            TimeSpan.FromHours(1), NowUtc(200));
    }

    [Fact]
    public async Task GuardRail_TargetExactlyAtFloor_Proceeds()
    {
        // T at the exact window floor (CompareTo == 0) must proceed, not throw.
        string artifacts = ArtifactsDir("guard_floor");
        BackupCatalog catalog = NewCatalog("guard_floor");

        long windowMs = 3_600_000; // 1 hour in ms
        long nowMs = 3_700_000;
        long floorMs = nowMs - windowMs; // = 100_000 ms

        InMemoryWAL wal = BuildWal((1, KvLog(1, floorMs, "k", "v", 1)));
        MemoryPersistenceBackend src = new();

        BackupManifest full = await BackupDriver.RunFullAsync(wal, [Part(1)], src, artifacts, catalog);
        IReadOnlyList<BackupManifest> chain = catalog.ResolveAndValidate(full.BackupId);

        string cpPath = Path.Combine(artifacts, full.BackupId.ToString("N"), "checkpoint");
        MemoryPersistenceBackend dst = MemoryPersistenceBackend.OpenCheckpoint(cpPath);
        InMemoryWAL dstWal = new(Log);

        // T exactly at floor — must not throw.
        BootstrapHelper.BootstrapNode(chain, artifacts, T(floorMs), dst, dstWal,
            TimeSpan.FromHours(1), NowUtc(nowMs));
    }

    // ── WAL checkpoint seeding tests ──────────────────────────────────────────────────────

    [Fact]
    public async Task WalSeeding_SinglePartition_CheckpointWrittenAtToIndex()
    {
        string artifacts = ArtifactsDir("wal_single");
        BackupCatalog catalog = NewCatalog("wal_single");

        // Full backup: partition 1, one committed entry at index 5, t=200.
        InMemoryWAL wal = BuildWal((1, KvLog(5, 200, "k", "v", 1)));
        MemoryPersistenceBackend src = new();

        BackupManifest full = await BackupDriver.RunFullAsync(wal, [Part(1)], src, artifacts, catalog);
        IReadOnlyList<BackupManifest> chain = catalog.ResolveAndValidate(full.BackupId);

        string cpPath = Path.Combine(artifacts, full.BackupId.ToString("N"), "checkpoint");
        MemoryPersistenceBackend dst = MemoryPersistenceBackend.OpenCheckpoint(cpPath);
        InMemoryWAL dstWal = new(Log);

        BootstrapHelper.BootstrapNode(chain, artifacts, T(200), dst, dstWal,
            TimeSpan.FromHours(1), NowUtc(400));

        RaftLog? cp = FindCheckpoint(dstWal, partitionId: 1);
        Assert.NotNull(cp);
        Assert.Equal(5L, cp.Id);
        Assert.Equal(1L, cp.Term); // must carry the real term for Log Matching
        Assert.Equal(RaftLogType.CommittedCheckpoint, cp.Type);
    }

    [Fact]
    public async Task WalSeeding_MultiplePartitions_EachGetsCheckpoint()
    {
        string artifacts = ArtifactsDir("wal_multi");
        BackupCatalog catalog = NewCatalog("wal_multi");

        // Two partitions: P1 at index 3, P2 at index 7.
        InMemoryWAL wal = BuildWal(
            (1, KvLog(3, 100, "a", "va", 1)),
            (2, KvLog(7, 200, "b", "vb", 1)));
        MemoryPersistenceBackend src = new();

        BackupManifest full = await BackupDriver.RunFullAsync(
            wal, [Part(1), Part(2)], src, artifacts, catalog);
        IReadOnlyList<BackupManifest> chain = catalog.ResolveAndValidate(full.BackupId);

        string cpPath = Path.Combine(artifacts, full.BackupId.ToString("N"), "checkpoint");
        MemoryPersistenceBackend dst = MemoryPersistenceBackend.OpenCheckpoint(cpPath);
        InMemoryWAL dstWal = new(Log);

        BootstrapHelper.BootstrapNode(chain, artifacts, T(300), dst, dstWal,
            TimeSpan.FromHours(1), NowUtc(500));

        RaftLog? cp1 = FindCheckpoint(dstWal, 1);
        RaftLog? cp2 = FindCheckpoint(dstWal, 2);

        Assert.NotNull(cp1);
        Assert.Equal(3L, cp1.Id);
        Assert.Equal(1L, cp1.Term);

        Assert.NotNull(cp2);
        Assert.Equal(7L, cp2.Id);
        Assert.Equal(1L, cp2.Term);
    }

    [Fact]
    public async Task WalSeeding_IncrementalChain_AdvancesHighWaterMark()
    {
        // Full at index 3 (t=100), then incremental at index 8 (t=200).
        // After bootstrap at T=300, the WAL checkpoint should be at 8, not 3.
        string artifacts = ArtifactsDir("wal_incr");
        BackupCatalog catalog = NewCatalog("wal_incr");

        InMemoryWAL wal = BuildWal(
            (1, KvLog(3, 100, "k", "v1", 1)),
            (1, KvLog(8, 200, "k", "v2", 2)));
        MemoryPersistenceBackend src = new();

        BackupManifest full = await BackupDriver.RunFullAsync(
            wal, [Part(1)], src, artifacts, catalog,
            flushBeforeCheckpoint: null);

        BackupManifest inc = BackupDriver.RunIncremental(
            wal, [Part(1)], full.BackupId, artifacts, catalog);

        IReadOnlyList<BackupManifest> chain = catalog.ResolveAndValidate(inc.BackupId);

        string cpPath = Path.Combine(artifacts, full.BackupId.ToString("N"), "checkpoint");
        MemoryPersistenceBackend dst = MemoryPersistenceBackend.OpenCheckpoint(cpPath);
        InMemoryWAL dstWal = new(Log);

        BootstrapHelper.BootstrapNode(chain, artifacts, T(300), dst, dstWal,
            TimeSpan.FromHours(1), NowUtc(500));

        RaftLog? cp = FindCheckpoint(dstWal, 1);
        Assert.NotNull(cp);
        Assert.Equal(8L, cp.Id);
        Assert.Equal(1L, cp.Term);
    }

    [Fact]
    public async Task WalSeeding_IncrementalStraddlesT_UsesFullToIndex()
    {
        // Full at index 3 (t=100). Incremental captures index 8 (t=300), so its ToHlc > T=150.
        // The incremental straddles T, so the WAL checkpoint must stay at 3 (Full's ToIndex).
        string artifacts = ArtifactsDir("wal_straddle");
        BackupCatalog catalog = NewCatalog("wal_straddle");

        // Build WAL with only the entry that goes into the Full (index 3, t=100).
        InMemoryWAL wal = BuildWal((1, KvLog(3, 100, "k", "v1", 1)));
        MemoryPersistenceBackend src = new();

        // Full backup: ToIndex=3, ToHlc=T(100).
        BackupManifest full = await BackupDriver.RunFullAsync(
            wal, [Part(1)], src, artifacts, catalog,
            flushBeforeCheckpoint: null);

        // Add the post-Full entry (index 8, t=300) — this is what the incremental will capture.
        wal.Write([(1, [KvLog(8, 300, "k", "v2", 2)])]);

        // Incremental: ToIndex=8, ToHlc=T(300) > T=150 → straddles.
        BackupManifest inc = BackupDriver.RunIncremental(
            wal, [Part(1)], full.BackupId, artifacts, catalog);

        IReadOnlyList<BackupManifest> chain = catalog.ResolveAndValidate(inc.BackupId);

        string cpPath = Path.Combine(artifacts, full.BackupId.ToString("N"), "checkpoint");
        MemoryPersistenceBackend dst = MemoryPersistenceBackend.OpenCheckpoint(cpPath);
        InMemoryWAL dstWal = new(Log);

        // T = 150 — the incremental's ToHlc (t=300) > 150, so it straddles T.
        // The bootstrap must fall back to the Full's safe ToIndex = 3.
        BootstrapHelper.BootstrapNode(chain, artifacts, T(150), dst, dstWal,
            TimeSpan.FromHours(1), NowUtc(500));

        RaftLog? cp = FindCheckpoint(dstWal, 1);
        Assert.NotNull(cp);
        Assert.Equal(3L, cp.Id); // conservative: Full's ToIndex, not the straddling incremental's
        Assert.Equal(1L, cp.Term); // term from the Full's last committed entry
    }

    // ── KV data restore test ───────────────────────────────────────────────────────────────

    [Fact]
    public async Task KvData_RestoredAfterBootstrap_KeyPresentInBackend()
    {
        string artifacts = ArtifactsDir("kv_data");
        BackupCatalog catalog = NewCatalog("kv_data");

        // Write two KV entries into the WAL: "key1" at t=100 (index 1), "key2" at t=200 (index 2).
        RaftLog log1 = KvLog(1, 100, "key1", "hello", 1);
        RaftLog log2 = KvLog(2, 200, "key2", "world", 1);
        InMemoryWAL wal = BuildWal((1, log1));

        MemoryPersistenceBackend src = new();

        // Full backup captures the checkpoint; incremental captures log2.
        BackupManifest full = await BackupDriver.RunFullAsync(
            wal, [Part(1)], src, artifacts, catalog,
            flushBeforeCheckpoint: null);

        // Add log2 to the WAL after the Full is taken.
        wal.Write([(1, [log2])]);

        BackupManifest inc = BackupDriver.RunIncremental(
            wal, [Part(1)], full.BackupId, artifacts, catalog);

        IReadOnlyList<BackupManifest> chain = catalog.ResolveAndValidate(inc.BackupId);

        string cpPath = Path.Combine(artifacts, full.BackupId.ToString("N"), "checkpoint");
        MemoryPersistenceBackend dst = MemoryPersistenceBackend.OpenCheckpoint(cpPath);
        InMemoryWAL dstWal = new(Log);

        RestoreResult result = BootstrapHelper.BootstrapNode(
            chain, artifacts, T(250), dst, dstWal,
            TimeSpan.FromHours(1), NowUtc(500));

        // At least the incremental entry was applied.
        Assert.True(result.EntriesApplied >= 1);

        // The WAL checkpoint should be at index 2 (incremental's ToIndex).
        RaftLog? cp = FindCheckpoint(dstWal, 1);
        Assert.NotNull(cp);
        Assert.Equal(2L, cp.Id);
    }

    [Fact]
    public async Task EmptyChain_NoWalEntriesWritten()
    {
        // Full backup on an empty WAL produces no PartitionRanges, so there is nothing
        // to seed. BootstrapHelper must complete without writing any WAL entries.
        string artifacts = ArtifactsDir("empty_chain");
        BackupCatalog catalog = NewCatalog("empty_chain");

        InMemoryWAL wal = new(Log);
        MemoryPersistenceBackend src = new();

        BackupManifest full = await BackupDriver.RunFullAsync(
            wal, [Part(1)], src, artifacts, catalog,
            flushBeforeCheckpoint: null);

        IReadOnlyList<BackupManifest> chain = catalog.ResolveAndValidate(full.BackupId);

        string cpPath = Path.Combine(artifacts, full.BackupId.ToString("N"), "checkpoint");
        MemoryPersistenceBackend dst = MemoryPersistenceBackend.OpenCheckpoint(cpPath);
        InMemoryWAL dstWal = new(Log);

        RestoreResult result = BootstrapHelper.BootstrapNode(
            chain, artifacts, T(100), dst, dstWal,
            TimeSpan.FromHours(1), NowUtc(500));

        Assert.Equal(0, result.EntriesApplied);
        // No WAL entries written for partition 1.
        Assert.Empty(dstWal.ReadLogsRange(1, 0));
    }
}
