
using Kahuna.Server.Persistence.Backend;
using Kahuna.Server.Persistence.Pitr;
using Kahuna.Shared.KeyValue;
using Kahuna.Server.Replication;
using Kahuna.Server.Replication.Protos;
using Google.Protobuf;
using Kommander;
using Kommander.Data;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kahuna.Server.Tests;

/// <summary>
/// Verifies that PITR bootstrap WAL seeding fails atomically: <see cref="BootstrapHelper.BootstrapNode"/>
/// checks every seed step's <see cref="RaftOperationStatus"/>, never reports success while the WAL
/// trails the state already restored into the backend, and is safe to re-run after a failed seed (the
/// backend restore and the checkpoint seed are both idempotent). Also verifies that the seeded
/// checkpoint is CERTIFIED — the WAL's persisted last-checkpoint id reaches it although nothing below it
/// is present — on the in-memory and the RocksDB backends, since that id is what makes the node present
/// to the leader at the restored index instead of at 0.
/// </summary>
public sealed class TestPitrBootstrapSeeding : IDisposable
{
    private static readonly ILogger<IRaft> Log = NullLogger<IRaft>.Instance;

    private readonly string _tempRoot =
        Path.Combine(Path.GetTempPath(), "kahuna_bootseed_" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        if (Directory.Exists(_tempRoot))
            try { Directory.Delete(_tempRoot, recursive: true); } catch { /* best-effort */ }
    }

    private static HLCTimestamp T(long ms) => new(0, ms, 0);
    private static DateTime NowUtc(long ms) => DateTime.UnixEpoch + TimeSpan.FromMilliseconds(ms);
    private static RaftPartitionRange Part(int id) => new() { PartitionId = id, State = RaftPartitionState.Active };

    private static RaftLog KvLog(long id, long timeMs, string key, string value, long revision, long term = 1)
    {
        KeyValueMessage msg = new()
        {
            Type = (int)KeyValueRequestType.TrySet,
            Key = key,
            Value = UnsafeByteOperations.UnsafeWrap(System.Text.Encoding.UTF8.GetBytes(value)),
            Revision = revision,
            LastModifiedPhysical = timeMs
        };
        return new RaftLog
        {
            Id = id, Term = term, Type = RaftLogType.Committed, Time = new HLCTimestamp(0, timeMs, 0),
            LogType = ReplicationTypes.KeyValues, LogData = ReplicationSerializer.Serialize(msg),
        };
    }

    private static InMemoryWAL BuildWal(params (int partition, RaftLog log)[] entries)
    {
        InMemoryWAL wal = new(Log);
        Dictionary<int, List<RaftLog>> byPartition = [];
        foreach ((int partition, RaftLog log) in entries)
            (byPartition.TryGetValue(partition, out List<RaftLog>? l) ? l : byPartition[partition] = []).Add(log);
        foreach ((int p, List<RaftLog> logs) in byPartition)
            wal.Write([(p, logs)]);
        return wal;
    }

    private static RaftLog? FindCheckpoint(IWAL wal, int partitionId) =>
        wal.ReadLogsRange(partitionId, 0).FirstOrDefault(l => l.Type == RaftLogType.CommittedCheckpoint);

    /// <summary>Builds a Full backup covering the given partitions and returns the resolved chain, artifacts dir, and a fresh dst backend loaded from the checkpoint.</summary>
    private async Task<(IReadOnlyList<BackupManifest> Chain, string Artifacts, MemoryPersistenceBackend Dst)>
        BuildBootstrap(string tag, params (int partition, RaftLog log)[] entries)
    {
        string artifacts = Path.Combine(_tempRoot, "artifacts_" + tag);
        BackupCatalog catalog = new(new LocalDirectoryStorageTarget(Path.Combine(_tempRoot, "cat_" + tag)));
        InMemoryWAL wal = BuildWal(entries);
        MemoryPersistenceBackend src = new();

        int[] partitions = entries.Select(e => e.partition).Distinct().ToArray();
        BackupManifest full = await BackupDriver.RunFullAsync(wal, partitions.Select(Part).ToArray(), src, BackupTestStores.Artifacts(artifacts), catalog);
        IReadOnlyList<BackupManifest> chain = await catalog.ResolveAndValidateAsync(full.BackupId);

        string cpPath = Path.Combine(artifacts, full.BackupId.ToString("N"), "checkpoint");
        MemoryPersistenceBackend dst = MemoryPersistenceBackend.OpenCheckpoint(cpPath);
        return (chain, artifacts, dst);
    }

    // ── fake WAL that injects seed-step failures, delegating everything else to an inner InMemoryWAL ──

    /// <summary>
    /// <paramref name="onSeedStep"/> sees the partition of every seed step (the boundary install and the
    /// checkpoint write alike) and returns a status to inject, or null to let the inner WAL perform it.
    /// </summary>
    private sealed class FailingWal(InMemoryWAL inner, Func<int, RaftOperationStatus?> onSeedStep) : IWAL
    {
        public int SeedCalls { get; private set; }

        public (RaftOperationStatus Status, bool SuffixTruncated) InstallSnapshotBoundary(
            int partitionId, long snapshotIndex, long lastIncludedTerm, bool sync)
        {
            SeedCalls++;
            RaftOperationStatus? injected = onSeedStep(partitionId);
            return injected is null
                ? inner.InstallSnapshotBoundary(partitionId, snapshotIndex, lastIncludedTerm, sync)
                : (injected.Value, false);
        }

        public RaftOperationStatus Write(List<(int, List<RaftLog>)> logs)
        {
            SeedCalls++;
            RaftOperationStatus? injected = onSeedStep(logs[0].Item1);
            return injected ?? inner.Write(logs);
        }

        public List<RaftLog> ReadLogs(int partitionId) => inner.ReadLogs(partitionId);
        public List<RaftLog> ReadLogsRange(int partitionId, long startLogIndex, int maxEntries = int.MaxValue) => inner.ReadLogsRange(partitionId, startLogIndex, maxEntries);
        public long GetMaxLog(int partitionId) => inner.GetMaxLog(partitionId);
        public long GetCurrentTerm(int partitionId) => inner.GetCurrentTerm(partitionId);
        public long GetLastCheckpoint(int partitionId) => inner.GetLastCheckpoint(partitionId);
        public int CountPersistedLogs(int partitionId) => inner.CountPersistedLogs(partitionId);
        public int CountRemovableLogs(int partitionId) => inner.CountRemovableLogs(partitionId);
        public string? GetMetaData(string key) => inner.GetMetaData(key);
        public bool SetMetaData(string key, string value) => inner.SetMetaData(key, value);
        public (RaftOperationStatus Status, int Removed) CompactLogsOlderThan(int partitionId, long lastCheckpoint, int compactNumberEntries, int? maxTotalEntries = null) => inner.CompactLogsOlderThan(partitionId, lastCheckpoint, compactNumberEntries, maxTotalEntries);
        public RaftOperationStatus DeletePartitionWAL(int partitionId) => inner.DeletePartitionWAL(partitionId);
        public RaftOperationStatus TruncateLogsAfter(int partitionId, long afterLogId) => inner.TruncateLogsAfter(partitionId, afterLogId);
        public (RaftOperationStatus Status, long MaxLogId) TruncateLogsAfterAndGetMax(int partitionId, long afterLogId) => inner.TruncateLogsAfterAndGetMax(partitionId, afterLogId);
        public void Dispose() => inner.Dispose();
    }

    // ── tests ────────────────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task SeedWriteFails_ReportsFailure_NotSuccess()
    {
        (IReadOnlyList<BackupManifest> chain, string artifacts, MemoryPersistenceBackend dst) =
            await BuildBootstrap("fail_all", (1, KvLog(5, 200, "k", "v", 1)));

        FailingWal wal = new(new InMemoryWAL(Log), _ => RaftOperationStatus.Errored);

        BackupDriverException ex = await Assert.ThrowsAsync<BackupDriverException>(() => BootstrapHelper.BootstrapNodeAsync(chain, BackupTestStores.Artifacts(artifacts), T(200), dst, wal, TimeSpan.FromHours(1), NowUtc(300)));
        Assert.Contains("WAL checkpoint", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Null(FindCheckpoint(wal, 1)); // nothing durably seeded
        Assert.True(wal.GetLastCheckpoint(1) <= 0);
    }

    [Fact]
    public async Task PartialPartitionFailure_ReportsFailure()
    {
        (IReadOnlyList<BackupManifest> chain, string artifacts, MemoryPersistenceBackend dst) =
            await BuildBootstrap("partial", (1, KvLog(5, 200, "k1", "v1", 1)), (2, KvLog(7, 200, "k2", "v2", 1)));

        // Fail only the write that targets partition 2; partition 1's write succeeds.
        InMemoryWAL inner = new(Log);
        FailingWal wal = new(inner, partition => partition == 2 ? RaftOperationStatus.Errored : (RaftOperationStatus?)null);

        await Assert.ThrowsAsync<BackupDriverException>(() => BootstrapHelper.BootstrapNodeAsync(chain, BackupTestStores.Artifacts(artifacts), T(200), dst, wal, TimeSpan.FromHours(1), NowUtc(300)));

        // Partition 2's checkpoint never landed; the failure is not masked as success.
        Assert.Null(FindCheckpoint(wal, 2));
    }

    [Fact]
    public async Task RetryAfterFailedSeed_Succeeds_AndSeedsIdempotently()
    {
        (IReadOnlyList<BackupManifest> chain, string artifacts, MemoryPersistenceBackend dst) =
            await BuildBootstrap("retry", (1, KvLog(5, 200, "k1", "v1", 1)));

        // Fail the first write only, then let everything through.
        int calls = 0;
        InMemoryWAL inner = new(Log);
        FailingWal wal = new(inner, _ => ++calls == 1 ? RaftOperationStatus.Errored : (RaftOperationStatus?)null);

        // First attempt fails closed — the backend was already restored, but the WAL was not seeded.
        await Assert.ThrowsAsync<BackupDriverException>(() => BootstrapHelper.BootstrapNodeAsync(chain, BackupTestStores.Artifacts(artifacts), T(200), dst, wal, TimeSpan.FromHours(1), NowUtc(300)));
        Assert.Null(FindCheckpoint(wal, 1));

        // Re-running the bootstrap (backend restore + checkpoint writes are idempotent) now succeeds.
        await BootstrapHelper.BootstrapNodeAsync(chain, BackupTestStores.Artifacts(artifacts), T(200), dst, wal, TimeSpan.FromHours(1), NowUtc(300));

        RaftLog? cp1 = FindCheckpoint(wal, 1);
        Assert.NotNull(cp1);
        Assert.Equal(5, cp1!.Id);
        Assert.Equal(5, wal.GetLastCheckpoint(1));

        // A further (successful) re-run overwrites by key — exactly one checkpoint, no duplicates.
        await BootstrapHelper.BootstrapNodeAsync(chain, BackupTestStores.Artifacts(artifacts), T(200), dst, wal, TimeSpan.FromHours(1), NowUtc(300));
        Assert.Single(wal.ReadLogsRange(1, 0), l => l.Type == RaftLogType.CommittedCheckpoint);
        Assert.Equal(5, wal.GetLastCheckpoint(1));
    }

    // ── the seed must be certified: the persisted last-checkpoint id reaches the restored index ──

    [Fact]
    public async Task Seed_EmptyWal_CertifiesCheckpointAndRecordsDurabilityFloor_InMemory()
    {
        (IReadOnlyList<BackupManifest> chain, string artifacts, MemoryPersistenceBackend dst) =
            await BuildBootstrap("certify_mem", (1, KvLog(5, 200, "k1", "v1", 1, term: 3)), (2, KvLog(7, 200, "k2", "v2", 1, term: 4)));

        InMemoryWAL wal = new(Log);
        await BootstrapHelper.BootstrapNodeAsync(chain, BackupTestStores.Artifacts(artifacts), T(200), dst, wal, TimeSpan.FromHours(1), NowUtc(300));

        AssertCertified(wal, partition: 1, index: 5, term: 3, hlc: T(200));
        AssertCertified(wal, partition: 2, index: 7, term: 4, hlc: T(200));

        // The store vouches for everything the chain delivered through the seeded index.
        Assert.Equal(5, dst.GetDurabilityFloor(1));
        Assert.Equal(7, dst.GetDurabilityFloor(2));
    }

    [Fact]
    public async Task Seed_EmptyWal_CertifiesCheckpoint_RocksDb_SurvivesReopen()
    {
        (IReadOnlyList<BackupManifest> chain, string artifacts, MemoryPersistenceBackend dst) =
            await BuildBootstrap("certify_rocks", (1, KvLog(5, 200, "k1", "v1", 1, term: 3)));

        string walRoot = Path.Combine(_tempRoot, "wal_rocks");
        Directory.CreateDirectory(walRoot);

        using (RocksDbWAL wal = new(walRoot, "seed", Log, syncWrites: false))
        {
            await BootstrapHelper.BootstrapNodeAsync(chain, BackupTestStores.Artifacts(artifacts), T(200), dst, wal, TimeSpan.FromHours(1), NowUtc(300));
            AssertCertified(wal, partition: 1, index: 5, term: 3, hlc: T(200));

            // A re-run finds its own row at the index (same term): nothing is truncated and nothing moves.
            await BootstrapHelper.BootstrapNodeAsync(chain, BackupTestStores.Artifacts(artifacts), T(200), dst, wal, TimeSpan.FromHours(1), NowUtc(300));
            AssertCertified(wal, partition: 1, index: 5, term: 3, hlc: T(200));
        }

        // The persisted id is what a restarted node reads before it presents itself to the leader.
        using RocksDbWAL reopened = new(walRoot, "seed", Log, syncWrites: false);
        AssertCertified(reopened, partition: 1, index: 5, term: 3, hlc: T(200));
    }

    private static void AssertCertified(IWAL wal, int partition, long index, long term, HLCTimestamp hlc)
    {
        Assert.Equal(index, wal.GetLastCheckpoint(partition));

        List<RaftLog> rows = wal.ReadLogsRange(partition, 0);
        RaftLog cp = Assert.Single(rows);
        Assert.Equal(index, cp.Id);
        Assert.Equal(RaftLogType.CommittedCheckpoint, cp.Type);
        Assert.Equal(term, cp.Term);
        Assert.Equal(hlc, cp.Time);
    }
}
