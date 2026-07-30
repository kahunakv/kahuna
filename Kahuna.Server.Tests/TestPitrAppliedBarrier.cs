
using System.Text;
using Google.Protobuf;
using Kahuna.Server.KeyValues;
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
/// A full backup must not publish a checkpoint until every captured partition's committed writes have
/// been applied and enqueued for persistence, and must fail closed (no artifact/catalog entry) if the
/// apply pipeline has not caught up or the flush cannot persist. These driver-level tests exercise the
/// applied-index barrier and the flush-failure path directly.
/// </summary>
public sealed class TestPitrAppliedBarrier : IDisposable
{
    private static readonly ILogger<IRaft> Log = NullLogger<IRaft>.Instance;
    private readonly string _tempRoot = Path.Combine(Path.GetTempPath(), "kahuna_barrier_" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        if (Directory.Exists(_tempRoot))
            try { Directory.Delete(_tempRoot, recursive: true); } catch { /* best-effort */ }
    }

    private static HLCTimestamp T(long ms) => new(0, ms, 0);
    private string ArtifactsDir(string n) => Path.Combine(_tempRoot, "art_" + n + Guid.NewGuid().ToString("N")[..6]);
    private BackupCatalog NewCatalog(string n) => new(new LocalDirectoryStorageTarget(Path.Combine(_tempRoot, "cat_" + n + Guid.NewGuid().ToString("N")[..6])));
    private static RaftPartitionRange Part(int id) => new() { PartitionId = id, State = RaftPartitionState.Active };

    // Each entry carries a committed key-value payload whose commit HLC (LastModified) equals its WAL
    // Time (ticks). The barrier target is the max payload commit HLC among captured entries, so the probe
    // values in these tests compare against `ticks` exactly as before.
    private static InMemoryWAL BuildWal(params (int partition, long id, long ticks)[] entries)
    {
        InMemoryWAL wal = new(Log);
        Dictionary<int, List<RaftLog>> byP = [];
        foreach ((int p, long id, long ticks) in entries)
            (byP.TryGetValue(p, out List<RaftLog>? l) ? l : byP[p] = []).Add(KvLog(id, ticks));
        foreach ((int p, List<RaftLog> logs) in byP)
            wal.Write([(p, logs)]);
        return wal;
    }

    private static RaftLog KvLog(long id, long ticks) => new()
    {
        Id = id,
        Type = RaftLogType.Committed,
        Time = new HLCTimestamp(0, ticks, 0),
        LogType = ReplicationTypes.KeyValues,
        LogData = ReplicationSerializer.Serialize(new KeyValueMessage
        {
            Type = (int)KeyValueRequestType.TrySet,
            Key = "k" + id,
            Value = UnsafeByteOperations.UnsafeWrap(Encoding.UTF8.GetBytes("v")),
            Revision = 1,
            LastModifiedPhysical = ticks
        })
    };

    // A committed KV entry whose WAL entry Time (walTicks) runs ahead of its commit HLC (commitTicks) —
    // the real-node shape, where a partition stamps its WAL entry from its local clock at or after the
    // shared commit HLC that lands in the payload.
    private static RaftLog KvLogSplit(long id, long walTicks, long commitTicks) => new()
    {
        Id = id,
        Type = RaftLogType.Committed,
        Time = new HLCTimestamp(0, walTicks, 0),
        LogType = ReplicationTypes.KeyValues,
        LogData = ReplicationSerializer.Serialize(new KeyValueMessage
        {
            Type = (int)KeyValueRequestType.TrySet,
            Key = "k" + id,
            Value = UnsafeByteOperations.UnsafeWrap(Encoding.UTF8.GetBytes("v")),
            Revision = 1,
            LastModifiedPhysical = commitTicks
        })
    };

    private static void Put(MemoryPersistenceBackend b, string key, long physicalMs) =>
        b.StoreKeyValues([new(key, Encoding.UTF8.GetBytes("v"), 1, 0, 0, 0, 0, physicalMs, 0, 0, physicalMs, 0, (int)KeyValueState.Set)]);

    [Fact]
    public async Task Barrier_NotCaughtUp_TimesOut_NoPublish()
    {
        InMemoryWAL wal = BuildWal((1, 1, 100));
        MemoryPersistenceBackend backend = new();
        Put(backend, "k", 50);
        BackupCatalog cat = NewCatalog("lag");
        string art = ArtifactsDir("lag");

        // Apply pipeline stuck below the captured ToHlc (100): the barrier must time out and fail closed.
        BackupDriverException ex = await Assert.ThrowsAsync<BackupDriverException>(() =>
            BackupDriver.RunFullAsync(wal, [Part(1)], backend, art, cat,
                flushBeforeCheckpoint: null, snapshotT: null, ct: default,
                appliedHlcProbe: _ => T(50), applyBarrierTimeoutMs: 100));

        Assert.True(ex.ExactCheckpointUnavailable);
        Assert.Empty(cat.List());
        if (Directory.Exists(art)) Assert.Empty(Directory.GetDirectories(art));
    }

    [Fact]
    public async Task Barrier_TargetIsCommitHlcNotWalTime_Publishes()
    {
        // The WAL entry Time (200) runs ahead of the commit HLC (100), as on a real node; the applied
        // probe reports the commit HLC. The barrier must target the commit HLC, not the WAL entry Time,
        // otherwise the probe could never reach it and the fully-applied backup would hang until timeout.
        InMemoryWAL wal = new(Log);
        wal.Write([(1, new List<RaftLog> { KvLogSplit(1, walTicks: 200, commitTicks: 100) })]);
        MemoryPersistenceBackend backend = new();
        Put(backend, "k", 50);
        BackupCatalog cat = NewCatalog("axis");
        string art = ArtifactsDir("axis");

        BackupManifest m = await BackupDriver.RunFullAsync(wal, [Part(1)], backend, art, cat,
            flushBeforeCheckpoint: null, snapshotT: null, ct: default,
            appliedHlcProbe: _ => T(100), applyBarrierTimeoutMs: 500);

        Assert.NotNull(m);
        Assert.Single(cat.List());
    }

    [Fact]
    public async Task Barrier_CaughtUp_Publishes()
    {
        InMemoryWAL wal = BuildWal((1, 1, 100));
        MemoryPersistenceBackend backend = new();
        Put(backend, "k", 50);
        BackupCatalog cat = NewCatalog("ok");
        string art = ArtifactsDir("ok");

        BackupManifest m = await BackupDriver.RunFullAsync(wal, [Part(1)], backend, art, cat,
            flushBeforeCheckpoint: null, snapshotT: null, ct: default,
            appliedHlcProbe: _ => T(100), applyBarrierTimeoutMs: 5000);

        Assert.NotNull(m);
        Assert.Single(cat.List());
    }

    [Fact]
    public async Task Barrier_MultiPartition_OneLagging_TimesOut_NoPublish()
    {
        InMemoryWAL wal = BuildWal((1, 1, 100), (2, 5, 120));
        MemoryPersistenceBackend backend = new();
        Put(backend, "k", 50);
        BackupCatalog cat = NewCatalog("multi");
        string art = ArtifactsDir("multi");

        // Partition 1 caught up (>=100), partition 2 lagging (<120): the barrier must fail closed.
        BackupDriverException ex = await Assert.ThrowsAsync<BackupDriverException>(() =>
            BackupDriver.RunFullAsync(wal, [Part(1), Part(2)], backend, art, cat,
                flushBeforeCheckpoint: null, snapshotT: null, ct: default,
                appliedHlcProbe: p => p == 1 ? T(100) : T(50), applyBarrierTimeoutMs: 100));

        Assert.True(ex.ExactCheckpointUnavailable);
        Assert.Empty(cat.List());
    }

    [Fact]
    public async Task FlushFailure_FailsClosed_NoPublish()
    {
        InMemoryWAL wal = BuildWal((1, 1, 100));
        MemoryPersistenceBackend backend = new();
        Put(backend, "k", 50);
        BackupCatalog cat = NewCatalog("flushfail");
        string art = ArtifactsDir("flushfail");

        // A flush that cannot durably persist committed writes now throws (it no longer reports success
        // on a failed drain), so the backup must fail closed and publish nothing.
        Task FailingFlush() => throw new IOException("background writer could not persist committed key-values");

        await Assert.ThrowsAsync<IOException>(() =>
            BackupDriver.RunFullAsync(wal, [Part(1)], backend, art, cat,
                flushBeforeCheckpoint: FailingFlush, snapshotT: null, ct: default,
                appliedHlcProbe: _ => T(100), applyBarrierTimeoutMs: 5000));

        Assert.Empty(cat.List());
        if (Directory.Exists(art)) Assert.Empty(Directory.GetDirectories(art));
    }
}
