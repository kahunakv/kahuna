
using System.Text;
using System.Text.Json;
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
/// The base image step of a PITR bootstrap puts the full backup's checkpoint into the storage path before the
/// backend opens, so the chain replay that follows runs over the full's rows and not over an empty store. These
/// cases open the real RocksDB and SQLite backends on the published directory, and cover the refusals and the
/// restart rules: a store with data is never seeded, a corrupt checkpoint publishes nothing, an interrupted
/// publish is removed, and an incomplete seed continues only from its own chain.
/// </summary>
public sealed class TestPitrBootstrapBaseImage : IDisposable
{
    private static readonly ILogger<IRaft> Log = NullLogger<IRaft>.Instance;

    private const string Revision = "v1";

    // Every backup in these cases is taken at physical times near this value, and every seed runs at
    // NowMs, inside a one-hour window.
    private const long BaseMs = 1_000_000_000;

    private const long NowMs = BaseMs + 60_000;

    private readonly string _tempRoot =
        Path.Combine(Path.GetTempPath(), "kahuna_baseimage_" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        if (Directory.Exists(_tempRoot))
            try { Directory.Delete(_tempRoot, recursive: true); } catch { /* best-effort */ }
    }

    private static DateTime NowUtc(long ms) => DateTime.UnixEpoch + TimeSpan.FromMilliseconds(ms);

    private static RaftPartitionRange Part(int id) => new() { PartitionId = id, State = RaftPartitionState.Active };

    private static RaftLog KvLog(long id, long timeMs, string key, string value, long revision)
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
            Id = id, Term = 1, Type = RaftLogType.Committed, Time = new HLCTimestamp(0, timeMs, 0),
            LogType = ReplicationTypes.KeyValues, LogData = ReplicationSerializer.Serialize(msg),
        };
    }

    private static PersistenceRequestItem Item(string key, string value, long revision, long timeMs) =>
        new(key, Encoding.UTF8.GetBytes(value), revision,
            0, 0, 0, 0, 0, 0,
            0, timeMs, 0, (int)KeyValueState.Set);

    private static IPersistenceBackend OpenBackend(string storage, string path) => storage switch
    {
        "rocksdb" => new RocksDbPersistenceBackend(path, Revision),
        "sqlite" => new SqlitePersistenceBackend(path, Revision),
        _ => throw new ArgumentOutOfRangeException(nameof(storage))
    };

    private string NewDir(string name)
    {
        string dir = Path.Combine(_tempRoot, name + "_" + Guid.NewGuid().ToString("N")[..6]);
        Directory.CreateDirectory(dir);
        return dir;
    }

    private sealed record Chain(string BackupDir, BackupManifest Full, BackupManifest Leaf);

    /// <summary>
    /// Takes a full backup of a <paramref name="storage"/> store that holds the row <c>base</c>, then an
    /// incremental whose WAL entries write the row <c>incr</c>. The row <c>base</c> exists only in the full's
    /// checkpoint, so it reaches a seeded node only through the base image.
    /// </summary>
    private async Task<Chain> BuildChain(string storage, string backupDir, string? clusterId = null, long timeMs = BaseMs)
    {
        BackupCatalog catalog = new(BackupTestStores.Manifests(backupDir));
        LocalDirectoryArtifactStore artifacts = BackupTestStores.Artifacts(backupDir);
        BackupOwnerIdentity identity = new(clusterId, null, null, storage, Revision, null);

        InMemoryWAL wal = new(Log);
        for (long id = 1; id <= 5; id++)
            wal.Write([(1, [KvLog(id, timeMs + id, "walbase-" + id, "w" + id, id)])]);

        string sourceDir = NewDir("source_" + storage);
        BackupManifest full;
        using (IDisposable disposable = (IDisposable)OpenBackend(storage, sourceDir))
        {
            IPersistenceBackend source = (IPersistenceBackend)disposable;
            Assert.True(source.StoreKeyValues([Item("base", "from-the-full", 1, timeMs)]));

            full = await BackupDriver.RunFullAsync(wal, [Part(1)], source, artifacts, catalog,
                identity: identity, ct: TestContext.Current.CancellationToken);
        }

        for (long id = 6; id <= 8; id++)
            wal.Write([(1, [KvLog(id, timeMs + id, id == 8 ? "incr" : "walincr-" + id, "from-the-incremental", id)])]);

        BackupManifest incremental = await BackupDriver.RunIncrementalAsync(wal, [Part(1)], full.BackupId, artifacts, catalog,
            identity: identity, ct: TestContext.Current.CancellationToken);

        return new Chain(backupDir, full, incremental);
    }

    private static BootstrapBaseImageRequest Request(
        string storage, string storagePath, string backupDir, Guid? leaf,
        long nowMs = NowMs, string? clusterId = null, bool fallback = false, string revision = Revision) =>
        new(storage, storagePath, revision,
            new BackupStoragePair(BackupTestStores.Manifests(backupDir), BackupTestStores.Artifacts(backupDir)),
            leaf, HLCTimestamp.Zero, TimeSpan.FromHours(1), TimeSpan.FromMinutes(30), NowUtc(nowMs),
            ManifestMacKey: null, clusterId, fallback, ThrottleBytesPerSec: 0);

    private static BootstrapSeedMarker ReadMarker(string storagePath) =>
        JsonSerializer.Deserialize(
            File.ReadAllText(Path.Combine(storagePath, BootstrapBaseImage.MarkerFileName)),
            PitrIndentedJsonContext.Default.BootstrapSeedMarker)!;

    private static void WriteMarker(string storagePath, BootstrapSeedMarker marker) =>
        File.WriteAllText(
            Path.Combine(storagePath, BootstrapBaseImage.MarkerFileName),
            JsonSerializer.Serialize(marker, PitrIndentedJsonContext.Default.BootstrapSeedMarker));

    private static void AssertNoSeedDebris(string storagePath, string storage)
    {
        Assert.False(BootstrapBaseImage.HasBackendData(storagePath, storage, Revision));
        Assert.False(File.Exists(Path.Combine(storagePath, BootstrapBaseImage.MarkerFileName)));
        Assert.False(Directory.Exists(Path.Combine(storagePath, BootstrapBaseImage.StagingDirectoryName)));
    }

    // ── the seed reads every row ────────────────────────────────────────────────────────────────────

    [Theory]
    [InlineData("rocksdb")]
    [InlineData("sqlite")]
    public async Task EmptyStore_GetsTheBaseImage_AndTheSeedHoldsTheFullAndTheIncrementalRows(string storage)
    {
        Chain chain = await BuildChain(storage, NewDir("backups"));
        string storagePath = NewDir("storage");
        string walPath = NewDir("wal");

        PitrBootstrapPlan plan = await BootstrapBaseImage.PrepareAsync(
            Request(storage, storagePath, chain.BackupDir, chain.Leaf.BackupId), TestContext.Current.CancellationToken);

        Assert.Equal(PitrBootstrapAction.Seed, plan.Action);
        Assert.Equal(chain.Leaf.BackupId, plan.LeafBackupId);
        Assert.False(plan.Resumed);
        Assert.Equal(BootstrapSeedState.Materialized, ReadMarker(storagePath).State);
        Assert.False(Directory.Exists(Path.Combine(storagePath, BootstrapBaseImage.StagingDirectoryName)));

        if (storage == "rocksdb")
            Assert.True(Directory.Exists(Path.Combine(storagePath, Revision)));
        else
            Assert.NotEmpty(Directory.EnumerateFiles(storagePath, "kahuna*_" + Revision + ".db"));

        // The second step runs on the backend the node opens over the storage path, as the server does.
        IReadOnlyList<BackupManifest> resolved = await new BackupCatalog(BackupTestStores.Manifests(chain.BackupDir))
            .ResolveAndValidateAsync(chain.Leaf.BackupId, TestContext.Current.CancellationToken);

        IPersistenceBackend backend = OpenBackend(storage, storagePath);
        try
        {
            ReadOnlyKeyValueEntry? fromTheFull = backend.GetKeyValue("base");
            Assert.NotNull(fromTheFull);
            Assert.Equal("from-the-full", Encoding.UTF8.GetString(fromTheFull.Value!));

            using RocksDbWAL wal = new(walPath, "seed", Log, syncWrites: false);
            await BootstrapHelper.BootstrapNodeAsync(resolved, BackupTestStores.Artifacts(chain.BackupDir), HLCTimestamp.Zero,
                backend, wal, TimeSpan.FromHours(1), NowUtc(NowMs), TimeSpan.FromMinutes(30));

            // The full's row came through the base image, the incremental's row through the replay.
            Assert.NotNull(backend.GetKeyValue("base"));
            ReadOnlyKeyValueEntry? fromTheIncremental = backend.GetKeyValue("incr");
            Assert.NotNull(fromTheIncremental);
            Assert.Equal("from-the-incremental", Encoding.UTF8.GetString(fromTheIncremental.Value!));

            PartitionBackupRange leafRange = Assert.Single(chain.Leaf.PartitionRanges);
            Assert.Equal(leafRange.ToIndex, wal.GetLastCheckpoint(1));
        }
        finally
        {
            (backend as IDisposable)?.Dispose();
        }

        BootstrapBaseImage.MarkSeeded(plan.StoragePath);
        Assert.Equal(BootstrapSeedState.Seeded, ReadMarker(storagePath).State);

        // A restart of the seeded node never seeds again.
        PitrBootstrapPlan restart = await BootstrapBaseImage.PrepareAsync(
            Request(storage, storagePath, chain.BackupDir, chain.Leaf.BackupId), TestContext.Current.CancellationToken);
        Assert.Equal(PitrBootstrapAction.Skip, restart.Action);
    }

    // ── a store with data is never overwritten ──────────────────────────────────────────────────────

    [Theory]
    [InlineData("rocksdb")]
    [InlineData("sqlite")]
    public async Task StoreWithData_IsNotSeeded_AndKeepsItsRows(string storage)
    {
        Chain chain = await BuildChain(storage, NewDir("backups"));
        string storagePath = NewDir("storage");

        using (IDisposable live = (IDisposable)OpenBackend(storage, storagePath))
            Assert.True(((IPersistenceBackend)live).StoreKeyValues([Item("live", "from-a-node-that-ran", 1, BaseMs)]));

        PitrBootstrapPlan plan = await BootstrapBaseImage.PrepareAsync(
            Request(storage, storagePath, chain.BackupDir, chain.Leaf.BackupId), TestContext.Current.CancellationToken);

        Assert.Equal(PitrBootstrapAction.Skip, plan.Action);
        Assert.Contains("already holds data", plan.Reason);
        Assert.False(File.Exists(Path.Combine(storagePath, BootstrapBaseImage.MarkerFileName)));

        using IDisposable reopened = (IDisposable)OpenBackend(storage, storagePath);
        IPersistenceBackend backend = (IPersistenceBackend)reopened;
        Assert.NotNull(backend.GetKeyValue("live"));
        Assert.Null(backend.GetKeyValue("base"));
    }

    [Fact]
    public async Task UnrelatedContentOfTheStoragePath_DoesNotStopTheSeed_AndIsKept()
    {
        Chain chain = await BuildChain("rocksdb", NewDir("backups"));
        string storagePath = NewDir("storage");

        // A filesystem mount point carries lost+found; another revision's data is not this store.
        Directory.CreateDirectory(Path.Combine(storagePath, "lost+found"));
        Directory.CreateDirectory(Path.Combine(storagePath, "v0"));
        File.WriteAllText(Path.Combine(storagePath, "v0", "OLD"), "x");

        PitrBootstrapPlan plan = await BootstrapBaseImage.PrepareAsync(
            Request("rocksdb", storagePath, chain.BackupDir, chain.Leaf.BackupId), TestContext.Current.CancellationToken);

        Assert.Equal(PitrBootstrapAction.Seed, plan.Action);
        Assert.True(Directory.Exists(Path.Combine(storagePath, "lost+found")));
        Assert.True(File.Exists(Path.Combine(storagePath, "v0", "OLD")));
    }

    // ── nothing is published from a bad image ───────────────────────────────────────────────────────

    [Theory]
    [InlineData("rocksdb")]
    [InlineData("sqlite")]
    public async Task CorruptCheckpointObject_StopsTheSeedBeforePublish_AndTheStoreStaysEmpty(string storage)
    {
        Chain chain = await BuildChain(storage, NewDir("backups"));
        string storagePath = NewDir("storage");

        // Change one byte of the largest checkpoint file without changing its size, so only the digest
        // check can catch it.
        string checkpointDir = Path.Combine(chain.BackupDir, chain.Full.BackupId.ToString("N"), "checkpoint");
        string victim = Directory.EnumerateFiles(checkpointDir, "*", SearchOption.AllDirectories)
            .OrderByDescending(f => new FileInfo(f).Length).First();
        byte[] bytes = File.ReadAllBytes(victim);
        bytes[bytes.Length / 2] ^= 0xFF;
        File.WriteAllBytes(victim, bytes);

        await Assert.ThrowsAsync<BackupArtifactException>(() => BootstrapBaseImage.PrepareAsync(
            Request(storage, storagePath, chain.BackupDir, chain.Leaf.BackupId), TestContext.Current.CancellationToken));

        AssertNoSeedDebris(storagePath, storage);
    }

    [Fact]
    public async Task StorageMismatch_IsRefusedBeforeAnythingIsCopied()
    {
        Chain chain = await BuildChain("rocksdb", NewDir("backups"));
        string storagePath = NewDir("storage");

        BackupDriverException wrongEngine = await Assert.ThrowsAsync<BackupDriverException>(() => BootstrapBaseImage.PrepareAsync(
            Request("sqlite", storagePath, chain.BackupDir, chain.Leaf.BackupId), TestContext.Current.CancellationToken));
        Assert.Contains("'rocksdb' checkpoint", wrongEngine.Message);

        BackupDriverException wrongRevision = await Assert.ThrowsAsync<BackupDriverException>(() => BootstrapBaseImage.PrepareAsync(
            Request("rocksdb", storagePath, chain.BackupDir, chain.Leaf.BackupId, revision: "v2"), TestContext.Current.CancellationToken));
        Assert.Contains("--storage-revision v1", wrongRevision.Message);

        AssertNoSeedDebris(storagePath, "rocksdb");
        Assert.False(Directory.Exists(Path.Combine(storagePath, "v2")));
    }

    [Fact]
    public async Task MemoryStorage_IsRefused()
    {
        string storagePath = NewDir("storage");

        BackupDriverException ex = await Assert.ThrowsAsync<BackupDriverException>(() => BootstrapBaseImage.PrepareAsync(
            Request("memory", storagePath, NewDir("backups"), Guid.NewGuid()), TestContext.Current.CancellationToken));

        Assert.Contains("keeps its data on disk", ex.Message);
    }

    // ── the retention window ────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task ChainOutsideTheWindow_FailsByDefault_AndFallsBackWhenAsked_WithAnEmptyStoreEitherWay()
    {
        Chain chain = await BuildChain("rocksdb", NewDir("backups"));
        string storagePath = NewDir("storage");
        long muchLater = NowMs + (long)TimeSpan.FromDays(1).TotalMilliseconds;

        BackupDriverException ex = await Assert.ThrowsAsync<BackupDriverException>(() => BootstrapBaseImage.PrepareAsync(
            Request("rocksdb", storagePath, chain.BackupDir, chain.Leaf.BackupId, nowMs: muchLater), TestContext.Current.CancellationToken));
        Assert.True(ex.OutsidePitrWindow);
        AssertNoSeedDebris(storagePath, "rocksdb");

        PitrBootstrapPlan plan = await BootstrapBaseImage.PrepareAsync(
            Request("rocksdb", storagePath, chain.BackupDir, chain.Leaf.BackupId, nowMs: muchLater, fallback: true),
            TestContext.Current.CancellationToken);
        Assert.Equal(PitrBootstrapAction.Fallback, plan.Action);
        AssertNoSeedDebris(storagePath, "rocksdb");
    }

    [Fact]
    public void WindowCheck_IsSkippedOnlyForAnInfiniteWindow()
    {
        HLCTimestamp old = new(0, BaseMs, 0);
        DateTime muchLater = NowUtc(NowMs + (long)TimeSpan.FromDays(1).TotalMilliseconds);

        BackupDriverException ex = Assert.Throws<BackupDriverException>(() =>
            BootstrapHelper.EnsureWithinPitrWindow(old, TimeSpan.FromHours(1), muchLater, TimeSpan.FromMinutes(30)));
        Assert.True(ex.OutsidePitrWindow);

        BootstrapHelper.EnsureWithinPitrWindow(old, Timeout.InfiniteTimeSpan, muchLater, TimeSpan.FromMinutes(30));
    }

    [Fact]
    public async Task FallbackFlag_DoesNotHideACorruptChain()
    {
        Chain chain = await BuildChain("rocksdb", NewDir("backups"));
        string storagePath = NewDir("storage");
        File.Delete(Path.Combine(chain.BackupDir, chain.Full.BackupId.ToString("N") + ".manifest"));

        await Assert.ThrowsAnyAsync<Exception>(() => BootstrapBaseImage.PrepareAsync(
            Request("rocksdb", storagePath, chain.BackupDir, chain.Leaf.BackupId, fallback: true), TestContext.Current.CancellationToken));

        AssertNoSeedDebris(storagePath, "rocksdb");
    }

    // ── restart rules ───────────────────────────────────────────────────────────────────────────────

    [Theory]
    [InlineData("rocksdb")]
    [InlineData("sqlite")]
    public async Task InterruptedPublish_IsRemovedOnTheNextStart_AndTheSeedStartsClean(string storage)
    {
        Chain chain = await BuildChain(storage, NewDir("backups"));
        string storagePath = NewDir("storage");
        Directory.CreateDirectory(Path.Combine(storagePath, "lost+found"));

        await BootstrapBaseImage.PrepareAsync(
            Request(storage, storagePath, chain.BackupDir, chain.Leaf.BackupId), TestContext.Current.CancellationToken);

        // A crash after the moves and before the marker recorded them as complete: the marker still says
        // publishing. A leftover staging folder and a half-written marker are part of the same crash.
        BootstrapSeedMarker marker = ReadMarker(storagePath);
        marker.State = BootstrapSeedState.Publishing;
        WriteMarker(storagePath, marker);
        Directory.CreateDirectory(Path.Combine(storagePath, BootstrapBaseImage.StagingDirectoryName, "payload"));
        File.WriteAllText(Path.Combine(storagePath, BootstrapBaseImage.MarkerFileName + ".tmp"), "{");

        PitrBootstrapPlan plan = await BootstrapBaseImage.PrepareAsync(
            Request(storage, storagePath, chain.BackupDir, chain.Leaf.BackupId), TestContext.Current.CancellationToken);

        Assert.Equal(PitrBootstrapAction.Seed, plan.Action);
        Assert.False(plan.Resumed);
        Assert.Equal(BootstrapSeedState.Materialized, ReadMarker(storagePath).State);
        Assert.False(File.Exists(Path.Combine(storagePath, BootstrapBaseImage.MarkerFileName + ".tmp")));
        Assert.False(Directory.Exists(Path.Combine(storagePath, BootstrapBaseImage.StagingDirectoryName)));
        Assert.True(Directory.Exists(Path.Combine(storagePath, "lost+found")));

        using IDisposable reopened = (IDisposable)OpenBackend(storage, storagePath);
        Assert.NotNull(((IPersistenceBackend)reopened).GetKeyValue("base"));
    }

    [Fact]
    public async Task IncompleteSeed_ContinuesFromItsOwnChain_AndRefusesAnother()
    {
        Chain chain = await BuildChain("rocksdb", NewDir("backups"));
        string storagePath = NewDir("storage");

        await BootstrapBaseImage.PrepareAsync(
            Request("rocksdb", storagePath, chain.BackupDir, chain.Leaf.BackupId), TestContext.Current.CancellationToken);

        PitrBootstrapPlan resumed = await BootstrapBaseImage.PrepareAsync(
            Request("rocksdb", storagePath, chain.BackupDir, chain.Leaf.BackupId), TestContext.Current.CancellationToken);
        Assert.Equal(PitrBootstrapAction.Seed, resumed.Action);
        Assert.True(resumed.Resumed);
        Assert.Equal(chain.Leaf.BackupId, resumed.LeafBackupId);

        // "latest" continues the recorded chain too, whatever the catalog holds now.
        PitrBootstrapPlan latest = await BootstrapBaseImage.PrepareAsync(
            Request("rocksdb", storagePath, chain.BackupDir, leaf: null), TestContext.Current.CancellationToken);
        Assert.Equal(chain.Leaf.BackupId, latest.LeafBackupId);

        BackupDriverException ex = await Assert.ThrowsAsync<BackupDriverException>(() => BootstrapBaseImage.PrepareAsync(
            Request("rocksdb", storagePath, chain.BackupDir, Guid.NewGuid()), TestContext.Current.CancellationToken));
        Assert.True(ex.TargetConflict);
        Assert.Equal(BootstrapSeedState.Materialized, ReadMarker(storagePath).State);
    }

    // ── "latest" ────────────────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task Latest_PicksTheNewestChainOfThisCluster()
    {
        string backupDir = NewDir("backups");
        Chain older = await BuildChain("rocksdb", backupDir, clusterId: "cluster-a", timeMs: BaseMs);
        Chain newer = await BuildChain("rocksdb", backupDir, clusterId: "cluster-a", timeMs: BaseMs + 1_000);
        await BuildChain("rocksdb", backupDir, clusterId: "cluster-b", timeMs: BaseMs + 2_000);

        PitrBootstrapPlan plan = await BootstrapBaseImage.PrepareAsync(
            Request("rocksdb", NewDir("storage"), backupDir, leaf: null, clusterId: "cluster-a"), TestContext.Current.CancellationToken);

        Assert.Equal(PitrBootstrapAction.Seed, plan.Action);
        Assert.Equal(newer.Leaf.BackupId, plan.LeafBackupId);
        Assert.NotEqual(older.Leaf.BackupId, plan.LeafBackupId);
    }

    [Fact]
    public async Task Latest_WithNoChain_FailsByDefault_AndFallsBackWhenAsked()
    {
        string backupDir = NewDir("backups");
        await BuildChain("rocksdb", backupDir, clusterId: "cluster-b");
        string storagePath = NewDir("storage");

        await Assert.ThrowsAsync<BackupChainException>(() => BootstrapBaseImage.PrepareAsync(
            Request("rocksdb", storagePath, backupDir, leaf: null, clusterId: "cluster-a"), TestContext.Current.CancellationToken));

        PitrBootstrapPlan plan = await BootstrapBaseImage.PrepareAsync(
            Request("rocksdb", storagePath, backupDir, leaf: null, clusterId: "cluster-a", fallback: true),
            TestContext.Current.CancellationToken);

        Assert.Equal(PitrBootstrapAction.Fallback, plan.Action);
        Assert.Null(plan.LeafBackupId);
        AssertNoSeedDebris(storagePath, "rocksdb");
    }
}
