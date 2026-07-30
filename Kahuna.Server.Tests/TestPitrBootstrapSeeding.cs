
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
/// checks every synthetic checkpoint write's <see cref="RaftOperationStatus"/>, never reports success
/// while the WAL trails the state already restored into the backend, and is safe to re-run after a
/// failed seed (the backend restore and the checkpoint writes are both idempotent).
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
        BackupManifest full = await BackupDriver.RunFullAsync(wal, partitions.Select(Part).ToArray(), src, artifacts, catalog);
        IReadOnlyList<BackupManifest> chain = catalog.ResolveAndValidate(full.BackupId);

        string cpPath = Path.Combine(artifacts, full.BackupId.ToString("N"), "checkpoint");
        MemoryPersistenceBackend dst = MemoryPersistenceBackend.OpenCheckpoint(cpPath);
        return (chain, artifacts, dst);
    }

    // ── fake WAL that injects write failures, delegating everything else to an inner InMemoryWAL ──

    private sealed class FailingWal(InMemoryWAL inner, Func<List<(int, List<RaftLog>)>, RaftOperationStatus?> onWrite) : IWAL
    {
        public int WriteCalls { get; private set; }

        public RaftOperationStatus Write(List<(int, List<RaftLog>)> logs)
        {
            WriteCalls++;
            RaftOperationStatus? injected = onWrite(logs);
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

        BackupDriverException ex = Assert.Throws<BackupDriverException>(() =>
            BootstrapHelper.BootstrapNode(chain, artifacts, T(200), dst, wal, TimeSpan.FromHours(1), NowUtc(300)));
        Assert.Contains("seed the WAL checkpoint", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Null(FindCheckpoint(wal, 1)); // nothing durably seeded
    }

    [Fact]
    public async Task PartialPartitionFailure_ReportsFailure()
    {
        (IReadOnlyList<BackupManifest> chain, string artifacts, MemoryPersistenceBackend dst) =
            await BuildBootstrap("partial", (1, KvLog(5, 200, "k1", "v1", 1)), (2, KvLog(7, 200, "k2", "v2", 1)));

        // Fail only the write that targets partition 2; partition 1's write succeeds.
        InMemoryWAL inner = new(Log);
        FailingWal wal = new(inner, logs => logs[0].Item1 == 2 ? RaftOperationStatus.Errored : (RaftOperationStatus?)null);

        Assert.Throws<BackupDriverException>(() =>
            BootstrapHelper.BootstrapNode(chain, artifacts, T(200), dst, wal, TimeSpan.FromHours(1), NowUtc(300)));

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
        Assert.Throws<BackupDriverException>(() =>
            BootstrapHelper.BootstrapNode(chain, artifacts, T(200), dst, wal, TimeSpan.FromHours(1), NowUtc(300)));
        Assert.Null(FindCheckpoint(wal, 1));

        // Re-running the bootstrap (backend restore + checkpoint writes are idempotent) now succeeds.
        BootstrapHelper.BootstrapNode(chain, artifacts, T(200), dst, wal, TimeSpan.FromHours(1), NowUtc(300));

        RaftLog? cp1 = FindCheckpoint(wal, 1);
        Assert.NotNull(cp1);
        Assert.Equal(5, cp1!.Id);

        // A further (successful) re-run overwrites by key — exactly one checkpoint, no duplicates.
        BootstrapHelper.BootstrapNode(chain, artifacts, T(200), dst, wal, TimeSpan.FromHours(1), NowUtc(300));
        Assert.Single(wal.ReadLogsRange(1, 0), l => l.Type == RaftLogType.CommittedCheckpoint);
    }
}
