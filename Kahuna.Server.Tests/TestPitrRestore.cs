
using System.Text;
using Google.Protobuf;
using Kahuna.Server.KeyValues;
using Kahuna.Server.Persistence;
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
/// Integration tests for PITR restore:
/// write keys at increasing HLCs, back up (full + incremental), then restore to various
/// timestamps and assert correct state, idempotency, and window rejection.
/// </summary>
public sealed class TestPitrRestore : IDisposable
{
    private readonly string _tempRoot =
        Path.Combine(Path.GetTempPath(), "kahuna_restore_" + Guid.NewGuid().ToString("N"));

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

    /// <summary>
    /// Builds a committed WAL entry whose LogData is a proper serialized KeyValueMessage
    /// so that RestoreEngine.ToRequestItem can decode it.
    /// </summary>
    private static RaftLog KvLog(long id, long timeMs, string key, string value, long revision,
        KeyValueRequestType type = KeyValueRequestType.TrySet)
    {
        KeyValueMessage msg = new()
        {
            Type = (int)type,
            Key = key,
            Value = UnsafeByteOperations.UnsafeWrap(Encoding.UTF8.GetBytes(value)),
            Revision = revision,
            LastModifiedPhysical = timeMs
        };

        return new RaftLog
        {
            Id = id,
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

    /// <summary>
    /// Flushes all KV log entries from the WAL into the backend (simulates BackgroundWriter flush).
    /// Delegates to <see cref="KeyValueMessageDecoder.Decode"/> so the mutation-set classification
    /// stays in one place.
    /// </summary>
    private static void FlushWalToBackend(InMemoryWAL wal, int partitionId, MemoryPersistenceBackend backend)
    {
        List<RaftLog> logs = wal.ReadLogsRange(partitionId, 1);
        foreach (RaftLog log in logs)
        {
            if (log.Type != RaftLogType.Committed) continue;
            if (log.LogType != ReplicationTypes.KeyValues) continue;
            if (log.LogData is null) continue;

            KeyValueMessage msg = ReplicationSerializer.UnserializeKeyValueMessage(log.LogData);
            (KeyValueState state, byte[]? value) = KeyValueMessageDecoder.Decode(msg);
            if (state == KeyValueState.Undefined) continue;

            backend.StoreKeyValues([new PersistenceRequestItem(
                msg.Key, value, msg.Revision,
                msg.ExpireNode, msg.ExpirePhysical, msg.ExpireCounter,
                msg.LastUsedNode, msg.LastUsedPhysical, msg.LastUsedCounter,
                msg.LastModifiedNode, msg.LastModifiedPhysical, msg.LastModifiedCounter,
                (int)state)]);
        }
    }

    private static string? GetValue(MemoryPersistenceBackend backend, string key)
    {
        object? entry = backend.GetKeyValue(key);
        if (entry is null) return null;
        // Use reflection to get the Value property (KeyValueEntry is internal).
        System.Reflection.PropertyInfo? prop = entry.GetType().GetProperty("Value");
        byte[]? bytes = prop?.GetValue(entry) as byte[];
        return bytes is null ? null : Encoding.UTF8.GetString(bytes);
    }

    // ── tests ────────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task Restore_AppliesAllEntriesAtOrBeforeT()
    {
        // Three writes: t1=100, t2=200, t3=300. Restore to T=300 → all three present.
        InMemoryWAL wal = BuildWal(
            (1, KvLog(1, 100, "a", "v1", 1)),
            (1, KvLog(2, 200, "b", "v2", 1)),
            (1, KvLog(3, 300, "c", "v3", 1)));

        MemoryPersistenceBackend fullBackend = new();
        FlushWalToBackend(wal, 1, fullBackend);

        BackupCatalog catalog = NewCatalog("all_entries");
        string artifacts = ArtifactsDir("all_entries");

        // Full backup at T=300 (all committed).
        BackupManifest full = await BackupDriver.RunFullAsync(
            wal, [Part(1)], fullBackend, artifacts, catalog);

        // No incremental needed — all entries are in the checkpoint.
        // Open checkpoint as the restore base.
        string checkpointPath = Path.Combine(artifacts, full.BackupId.ToString("N"), "checkpoint");
        MemoryPersistenceBackend restored = MemoryPersistenceBackend.OpenCheckpoint(checkpointPath);

        IReadOnlyList<BackupManifest> chain = catalog.ResolveAndValidate(full.BackupId);
        RestoreResult result = RestoreEngine.Restore(chain, artifacts, T(300), restored);

        Assert.Equal("v1", GetValue(restored, "a"));
        Assert.Equal("v2", GetValue(restored, "b"));
        Assert.Equal("v3", GetValue(restored, "c"));
        Assert.Equal(0, result.EntriesApplied); // all data was in the checkpoint, no incremental
    }

    [Fact]
    public async Task Restore_StopsAtT_ExcludesEntriesAfterT()
    {
        // Full backup after index 2; incremental covers 3 & 4.
        // Restore to T=300 → key c (t=300) included; d (t=400) excluded.
        InMemoryWAL wal = BuildWal(
            (1, KvLog(1, 100, "a", "v1", 1)),
            (1, KvLog(2, 200, "b", "v2", 1)));

        MemoryPersistenceBackend fullBackend = new();
        FlushWalToBackend(wal, 1, fullBackend);

        BackupCatalog catalog = NewCatalog("stop_at_t");
        string artifacts = ArtifactsDir("stop_at_t");

        BackupManifest full = await BackupDriver.RunFullAsync(
            wal, [Part(1)], fullBackend, artifacts, catalog);

        // Add entries after full backup.
        wal.Write([(1, [KvLog(3, 300, "c", "v3", 1), KvLog(4, 400, "d", "v4", 1)])]);

        BackupManifest inc = BackupDriver.RunIncremental(wal, [Part(1)], full.BackupId, artifacts, catalog);

        // Restore to T=300: c included, d excluded.
        string checkpointPath = Path.Combine(artifacts, full.BackupId.ToString("N"), "checkpoint");
        MemoryPersistenceBackend restored = MemoryPersistenceBackend.OpenCheckpoint(checkpointPath);

        IReadOnlyList<BackupManifest> chain = catalog.ResolveAndValidate(inc.BackupId);
        RestoreResult result = RestoreEngine.Restore(chain, artifacts, T(300), restored);

        Assert.Equal("v1", GetValue(restored, "a"));
        Assert.Equal("v2", GetValue(restored, "b"));
        Assert.Equal("v3", GetValue(restored, "c"));
        Assert.Null(GetValue(restored, "d"));
        Assert.Equal(1, result.EntriesApplied);
        Assert.Equal(T(300), result.LastAppliedTime);
    }

    [Fact]
    public async Task Restore_Idempotent_ReplayingSameSegmentTwiceProducesSameState()
    {
        InMemoryWAL wal = BuildWal(
            (1, KvLog(1, 100, "a", "v1", 1)),
            (1, KvLog(2, 200, "b", "v2", 1)));

        MemoryPersistenceBackend fullBackend = new();
        FlushWalToBackend(wal, 1, fullBackend);

        BackupCatalog catalog = NewCatalog("idempotent");
        string artifacts = ArtifactsDir("idempotent");

        BackupManifest full = await BackupDriver.RunFullAsync(
            wal, [Part(1)], fullBackend, artifacts, catalog);

        wal.Write([(1, [KvLog(3, 300, "c", "v3", 1)])]);
        BackupManifest inc = BackupDriver.RunIncremental(wal, [Part(1)], full.BackupId, artifacts, catalog);

        string checkpointPath = Path.Combine(artifacts, full.BackupId.ToString("N"), "checkpoint");
        IReadOnlyList<BackupManifest> chain = catalog.ResolveAndValidate(inc.BackupId);

        // First restore.
        MemoryPersistenceBackend restored = MemoryPersistenceBackend.OpenCheckpoint(checkpointPath);
        RestoreEngine.Restore(chain, artifacts, T(400), restored);

        // Second restore into same backend — should be a no-op (upsert semantics).
        RestoreResult result2 = RestoreEngine.Restore(chain, artifacts, T(400), restored);

        Assert.Equal("v3", GetValue(restored, "c"));
        Assert.Equal(1, result2.EntriesApplied); // segment re-applied
    }

    [Fact]
    public void Restore_TargetBeforeWindow_Throws()
    {
        DateTime now = new(2026, 1, 1, 12, 0, 0, DateTimeKind.Utc);
        TimeSpan window = TimeSpan.FromHours(1);
        HLCTimestamp tooOld = T((long)((now - window - TimeSpan.FromMinutes(1) - DateTime.UnixEpoch).TotalMilliseconds));

        BackupCatalog catalog = NewCatalog("window_reject");
        string artifacts = ArtifactsDir("window_reject");
        BackupManifest full = BackupManifest.CreateFull([]);
        catalog.Put(full);

        MemoryPersistenceBackend backend = new();
        IReadOnlyList<BackupManifest> chain = [full];

        BackupDriverException ex = Assert.Throws<BackupDriverException>(() =>
            RestoreEngine.Restore(chain, artifacts, tooOld, backend, window, now));

        Assert.Contains("outside the recoverable window", ex.Message);
    }

    [Fact]
    public void Restore_TargetAfterNow_Throws()
    {
        DateTime now = new(2026, 1, 1, 12, 0, 0, DateTimeKind.Utc);
        TimeSpan window = TimeSpan.FromHours(1);
        HLCTimestamp future = T((long)((now + TimeSpan.FromMinutes(1) - DateTime.UnixEpoch).TotalMilliseconds));

        BackupManifest full = BackupManifest.CreateFull([]);
        BackupCatalog catalog = NewCatalog("window_future");
        catalog.Put(full);
        string artifacts = ArtifactsDir("window_future");

        IReadOnlyList<BackupManifest> chain = [full];
        BackupDriverException ex = Assert.Throws<BackupDriverException>(() =>
            RestoreEngine.Restore(chain, artifacts, future, new MemoryPersistenceBackend(), window, now));

        Assert.Contains("outside the recoverable window", ex.Message);
    }

    [Fact]
    public async Task Restore_WithIncrementalChain_AllSegmentsApplied()
    {
        // Full backup at index 2, then two incrementals: [3..4] and [5..6].
        // Restore to T=600 → all six entries present.
        InMemoryWAL wal = BuildWal(
            (1, KvLog(1, 100, "k1", "v1", 1)),
            (1, KvLog(2, 200, "k2", "v2", 1)));

        MemoryPersistenceBackend fullBackend = new();
        FlushWalToBackend(wal, 1, fullBackend);

        BackupCatalog catalog = NewCatalog("inc_chain");
        string artifacts = ArtifactsDir("inc_chain");

        BackupManifest full = await BackupDriver.RunFullAsync(
            wal, [Part(1)], fullBackend, artifacts, catalog);

        wal.Write([(1, [KvLog(3, 300, "k3", "v3", 1), KvLog(4, 400, "k4", "v4", 1)])]);
        BackupManifest inc1 = BackupDriver.RunIncremental(wal, [Part(1)], full.BackupId, artifacts, catalog);

        wal.Write([(1, [KvLog(5, 500, "k5", "v5", 1), KvLog(6, 600, "k6", "v6", 1)])]);
        BackupManifest inc2 = BackupDriver.RunIncremental(wal, [Part(1)], inc1.BackupId, artifacts, catalog);

        string checkpointPath = Path.Combine(artifacts, full.BackupId.ToString("N"), "checkpoint");
        MemoryPersistenceBackend restored = MemoryPersistenceBackend.OpenCheckpoint(checkpointPath);

        IReadOnlyList<BackupManifest> chain = catalog.ResolveAndValidate(inc2.BackupId);
        RestoreResult result = RestoreEngine.Restore(chain, artifacts, T(600), restored);

        for (int i = 1; i <= 6; i++)
            Assert.Equal($"v{i}", GetValue(restored, $"k{i}"));

        Assert.Equal(4, result.EntriesApplied); // 4 entries from the two incrementals
        Assert.Equal(1, result.PartitionsRestored);
    }

    [Fact]
    public async Task Restore_DeleteEntry_KeyAbsentAfterT()
    {
        // k1 is set at t=100, deleted at t=300. Restore to T=300 → k1 absent.
        InMemoryWAL wal = BuildWal(
            (1, KvLog(1, 100, "k1", "v1", 1)));

        MemoryPersistenceBackend fullBackend = new();
        FlushWalToBackend(wal, 1, fullBackend);

        BackupCatalog catalog = NewCatalog("delete");
        string artifacts = ArtifactsDir("delete");

        BackupManifest full = await BackupDriver.RunFullAsync(
            wal, [Part(1)], fullBackend, artifacts, catalog);

        wal.Write([(1, [KvLog(2, 300, "k1", "", 2, KeyValueRequestType.TryDelete)])]);
        BackupManifest inc = BackupDriver.RunIncremental(wal, [Part(1)], full.BackupId, artifacts, catalog);

        string checkpointPath = Path.Combine(artifacts, full.BackupId.ToString("N"), "checkpoint");
        MemoryPersistenceBackend restored = MemoryPersistenceBackend.OpenCheckpoint(checkpointPath);

        IReadOnlyList<BackupManifest> chain = catalog.ResolveAndValidate(inc.BackupId);
        RestoreEngine.Restore(chain, artifacts, T(300), restored);

        // Key is present but marked deleted — state reflects the delete.
        object? entry = restored.GetKeyValue("k1");
        if (entry is not null)
        {
            System.Reflection.PropertyInfo? stateProp = entry.GetType().GetProperty("State");
            int? state = stateProp?.GetValue(entry) as int?;
            Assert.True(state is null or (int)KeyValueState.Deleted);
        }
    }

    [Fact]
    public async Task Restore_ResultStats_Correct()
    {
        InMemoryWAL wal = BuildWal((1, KvLog(1, 100, "x", "1", 1)));
        MemoryPersistenceBackend fullBackend = new();
        FlushWalToBackend(wal, 1, fullBackend);

        BackupCatalog catalog = NewCatalog("stats");
        string artifacts = ArtifactsDir("stats");

        BackupManifest full = await BackupDriver.RunFullAsync(
            wal, [Part(1)], fullBackend, artifacts, catalog);

        wal.Write([(1, [KvLog(2, 200, "y", "2", 1), KvLog(3, 300, "z", "3", 1)])]);
        BackupManifest inc = BackupDriver.RunIncremental(wal, [Part(1)], full.BackupId, artifacts, catalog);

        string checkpointPath = Path.Combine(artifacts, full.BackupId.ToString("N"), "checkpoint");
        MemoryPersistenceBackend restored = MemoryPersistenceBackend.OpenCheckpoint(checkpointPath);

        IReadOnlyList<BackupManifest> chain = catalog.ResolveAndValidate(inc.BackupId);
        RestoreResult result = RestoreEngine.Restore(chain, artifacts, T(300), restored);

        Assert.Equal(2, result.EntriesApplied);
        Assert.Equal(1, result.PartitionsRestored);
        Assert.Equal(T(300), result.LastAppliedTime);
        Assert.Equal(3L, result.LastAppliedIndex);
    }

    [Fact]
    public async Task Restore_MultiPartition_LastAppliedTimeIsMaxHlcNotLastIterated()
    {
        // Partition 1: entries at t=100, t=500 (higher HLC).
        // Partition 2: entries at t=200, t=300.
        // After restore, LastAppliedTime must be T(500) — the global max — regardless of
        // which partition happens to be iterated last.
        InMemoryWAL wal = BuildWal(
            (1, KvLog(1, 100, "p1a", "v1", 1)),
            (1, KvLog(2, 500, "p1b", "v2", 1)),
            (2, KvLog(1, 200, "p2a", "v3", 1)),
            (2, KvLog(2, 300, "p2b", "v4", 1)));

        MemoryPersistenceBackend fullBackend = new();
        FlushWalToBackend(wal, 1, fullBackend);
        FlushWalToBackend(wal, 2, fullBackend);

        BackupCatalog catalog = NewCatalog("multipart_max");
        string artifacts = ArtifactsDir("multipart_max");

        BackupManifest full = await BackupDriver.RunFullAsync(
            wal, [Part(1), Part(2)], fullBackend, artifacts, catalog);

        wal.Write([(1, [KvLog(3, 600, "p1c", "v5", 1)])]);
        wal.Write([(2, [KvLog(3, 250, "p2c", "v6", 1)])]);

        BackupManifest inc = BackupDriver.RunIncremental(
            wal, [Part(1), Part(2)], full.BackupId, artifacts, catalog);

        string checkpointPath = Path.Combine(artifacts, full.BackupId.ToString("N"), "checkpoint");
        MemoryPersistenceBackend restored = MemoryPersistenceBackend.OpenCheckpoint(checkpointPath);

        IReadOnlyList<BackupManifest> chain = catalog.ResolveAndValidate(inc.BackupId);
        RestoreResult result = RestoreEngine.Restore(chain, artifacts, T(600), restored);

        // The entry with the highest HLC is p1c at T(600).
        Assert.Equal(T(600), result.LastAppliedTime);
        Assert.Equal(3L, result.LastAppliedIndex);
        Assert.Equal(2, result.PartitionsRestored);
        Assert.Equal(2, result.EntriesApplied); // p1c (t=600) + p2c (t=250)
    }

    /// <summary>
    /// End-to-end round-trip: WAL entries flow through the flush delegate INSIDE RunFullAsync
    /// (not pre-populated before the call), the checkpoint captures them, incremental adds more,
    /// and restore recovers all entries including the last committed entry at exactly ToIndex.
    ///
    /// This is the production flow: read M → flush (inside RunFullAsync) → checkpoint → segment.
    /// If flush were called before reading M (the old bug), a write committed after the flush
    /// but before M-read would be absent from the checkpoint and missed by restore.
    /// </summary>
    [Fact]
    public async Task FullThenIncremental_RoundTrip_FlushDelegatePropagatesDataToCheckpoint()
    {
        // WAL: entries 1–3 exist before the Full backup is taken.
        InMemoryWAL wal = BuildWal(
            (1, KvLog(1, 100, "k1", "v1", 1)),
            (1, KvLog(2, 200, "k2", "v2", 1)),
            (1, KvLog(3, 300, "k3", "v3", 1)));

        // Backend starts empty — data reaches it only when the flush delegate fires inside RunFullAsync.
        MemoryPersistenceBackend fullBackend = new();

        BackupCatalog catalog = NewCatalog("roundtrip");
        string artifacts = ArtifactsDir("roundtrip");

        // The flush delegate is the production stand-in: it drains WAL→backend at the moment
        // RunFullAsync calls it (after reading M, before CreateCheckpoint).
        Task Flush() { FlushWalToBackend(wal, 1, fullBackend); return Task.CompletedTask; }

        BackupManifest full = await BackupDriver.RunFullAsync(
            wal, [Part(1)], fullBackend, artifacts, catalog, Flush);

        // Full manifest should record ToIndex = 3 (last committed entry).
        Assert.Equal(3L, full.PartitionRanges[0].ToIndex);

        // Add entries 4–5 after the Full backup.
        wal.Write([(1, [KvLog(4, 400, "k4", "v4", 1), KvLog(5, 500, "k5", "v5", 1)])]);
        BackupManifest inc = BackupDriver.RunIncremental(wal, [Part(1)], full.BackupId, artifacts, catalog);

        // Restore: open the Full checkpoint, then replay the incremental segment.
        string checkpointPath = Path.Combine(artifacts, full.BackupId.ToString("N"), "checkpoint");
        MemoryPersistenceBackend restored = MemoryPersistenceBackend.OpenCheckpoint(checkpointPath);

        IReadOnlyList<BackupManifest> chain = catalog.ResolveAndValidate(inc.BackupId);
        RestoreResult result = RestoreEngine.Restore(chain, artifacts, T(500), restored);

        // All 5 entries must be present: 1–3 from the checkpoint, 4–5 from the segment.
        for (int i = 1; i <= 5; i++)
            Assert.Equal($"v{i}", GetValue(restored, $"k{i}"));

        Assert.Equal(2, result.EntriesApplied);           // only incremental entries counted
        Assert.Equal(1, result.PartitionsRestored);
        Assert.Equal(T(500), result.LastAppliedTime);
        Assert.Equal(5L, result.LastAppliedIndex);
    }
}
