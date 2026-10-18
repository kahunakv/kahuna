using Kahuna.Server.KeyValues;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Server.Persistence.Pitr;
using Kommander;
using Kommander.Data;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kahuna.Server.Tests;

/// <summary>
/// Where the local backup target stages a checkpoint of the live RocksDB store. A hard link cannot cross a
/// filesystem, so when the backup directory is elsewhere the checkpoint is staged on the store's filesystem and
/// only the trimmed image is copied in. A second filesystem is not portable in a test, so the hard-link probe
/// is injected; one test runs the real probe on a single filesystem.
/// </summary>
public sealed class TestLiveCheckpointStaging : IDisposable
{
    private readonly string _tempRoot = Path.Combine(Path.GetTempPath(), "kahuna_stage_" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        if (Directory.Exists(_tempRoot))
            Directory.Delete(_tempRoot, recursive: true);
    }

    private string StoragePath => Path.Combine(_tempRoot, "store");

    private string BackupDir => Path.Combine(_tempRoot, "backups");

    private string StagingRoot => Path.Combine(StoragePath, "backup-staging");

    // A link into the backup directory fails, as it does across filesystems; every other link succeeds.
    private Func<string, string, bool> LinkRefusedInto(string directory) =>
        (_, target) => !Path.GetDirectoryName(target)!.Equals(directory, StringComparison.Ordinal);

    private LiveCheckpointStaging Staging(
        Func<string, string, bool> tryHardLink, Func<string, long>? freeSpace = null, params string[] roots) =>
        new(StoragePath, Path.Combine(StoragePath, "v1"), roots.Length > 0 ? roots : [StagingRoot],
            copyThrottleBytesPerSec: 0, NullLogger.Instance, tryHardLink, freeSpace ?? (_ => long.MaxValue));

    private static PersistenceRequestItem Item(string key, byte val, long rev, long physicalMs) =>
        new(key, [val], rev, 0, 0, 0, 0, physicalMs, 0, 0, physicalMs, 0, 1);

    // ── the probe's decision ─────────────────────────────────────────────────────────────────

    [Fact]
    public void Reserve_LinkIntoBackupDirectoryWorks_StaysInPlace()
    {
        Directory.CreateDirectory(BackupDir);
        LiveCheckpointStaging staging = Staging((_, _) => true);

        Assert.Null(staging.Reserve(Guid.NewGuid(), BackupDir));
        Assert.False(Directory.Exists(StagingRoot) && Directory.EnumerateFileSystemEntries(StagingRoot).Any());
    }

    [Fact]
    public void Reserve_LinkIntoBackupDirectoryFails_StagesOnTheStoreFilesystem()
    {
        Directory.CreateDirectory(BackupDir);
        LiveCheckpointStaging staging = Staging(LinkRefusedInto(BackupDir));
        Guid backupId = Guid.NewGuid();

        string? directory = staging.Reserve(backupId, BackupDir);

        Assert.Equal(Path.Combine(StagingRoot, backupId.ToString("N")), directory);
        Assert.True(Directory.Exists(directory));
        // The probe files are gone on both sides.
        Assert.Empty(Directory.GetFiles(StoragePath));
        Assert.Empty(Directory.GetFiles(BackupDir));
        Assert.Empty(Directory.GetFiles(StagingRoot));

        staging.Release(directory!);
        Assert.False(Directory.Exists(directory));
    }

    [Fact]
    public void Reserve_NoRootAcceptsTheLink_StaysInPlace()
    {
        Directory.CreateDirectory(BackupDir);
        LiveCheckpointStaging staging = Staging((_, _) => false);

        Assert.Null(staging.Reserve(Guid.NewGuid(), BackupDir));
    }

    [Fact]
    public void Reserve_RootWithoutRoomForTheImage_IsSkipped()
    {
        Directory.CreateDirectory(BackupDir);
        Directory.CreateDirectory(Path.Combine(StoragePath, "v1"));
        File.WriteAllBytes(Path.Combine(StoragePath, "v1", "000001.sst"), new byte[4096]);

        string scratchRoot = Path.Combine(_tempRoot, "scratch", "backup-staging");
        // The scratch root is short of the image plus the margin; the storage-path root has room.
        long Free(string root) => root.StartsWith(scratchRoot, StringComparison.Ordinal)
            ? 4096 + LiveCheckpointStaging.FreeSpaceMargin - 1
            : 4096 + LiveCheckpointStaging.FreeSpaceMargin;

        LiveCheckpointStaging staging = Staging(LinkRefusedInto(BackupDir), Free, scratchRoot, StagingRoot);
        Guid backupId = Guid.NewGuid();

        Assert.Equal(Path.Combine(StagingRoot, backupId.ToString("N")), staging.Reserve(backupId, BackupDir));
    }

    [Fact]
    public void Reserve_RealProbeOnOneFilesystem_StaysInPlace()
    {
        // The default probe calls the operating system. The temp directory is one filesystem, so the link
        // works and the checkpoint stays in the backup directory, as before.
        Directory.CreateDirectory(BackupDir);
        LiveCheckpointStaging staging = new(StoragePath, Path.Combine(StoragePath, "v1"), [StagingRoot], 0);

        Assert.Null(staging.Reserve(Guid.NewGuid(), BackupDir));
        Assert.Empty(Directory.GetFiles(StoragePath));
        Assert.Empty(Directory.GetFiles(BackupDir));
    }

    // ── the store ─────────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task StagedCheckpoint_DisposedWithoutCommit_LeavesNothingBehind()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        LocalDirectoryArtifactStore store = new(BackupDir, Staging(LinkRefusedInto(BackupDir)));
        Guid backupId = Guid.NewGuid();

        await using (IBackupCheckpointStagingArea area = await store.BeginLiveCheckpointAsync(backupId, ct))
        {
            Assert.StartsWith(StagingRoot, area.LocalPath, StringComparison.Ordinal);
            Directory.CreateDirectory(area.LocalPath);
            await File.WriteAllBytesAsync(Path.Combine(area.LocalPath, "000001.sst"), [1, 2, 3], ct);
        }

        Assert.Empty(Directory.EnumerateFileSystemEntries(StagingRoot));
        Assert.False(Directory.Exists(Path.Combine(BackupDir, backupId.ToString("N"), "checkpoint")));
    }

    [Fact]
    public async Task StagedCheckpoint_Commit_PublishesTheTreeIntoTheBackupDirectory()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        LocalDirectoryArtifactStore store = new(BackupDir, Staging(LinkRefusedInto(BackupDir)));
        Guid backupId = Guid.NewGuid();

        await using (IBackupCheckpointStagingArea area = await store.BeginLiveCheckpointAsync(backupId, ct))
        {
            Directory.CreateDirectory(Path.Combine(area.LocalPath, "sub"));
            await File.WriteAllBytesAsync(Path.Combine(area.LocalPath, "000001.sst"), [1, 2, 3], ct);
            await File.WriteAllBytesAsync(Path.Combine(area.LocalPath, "sub", "MANIFEST"), [4, 5], ct);
            await area.CommitAsync(ct);
        }

        IReadOnlyList<BackupArtifactEntry> published = await store.ListAsync(backupId, ct);
        Assert.Equal(["checkpoint/000001.sst", "checkpoint/sub/MANIFEST"], published.Select(e => e.RelativePath));
        Assert.Empty(Directory.EnumerateFileSystemEntries(StagingRoot));
        if (!OperatingSystem.IsWindows())
            Assert.Equal(UnixFileMode.UserRead | UnixFileMode.UserWrite,
                File.GetUnixFileMode(Path.Combine(BackupDir, backupId.ToString("N"), "checkpoint", "000001.sst")));
    }

    [Fact]
    public async Task Leftovers_ListOnlyUnusedStagingEntries_AndDeleteThem()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        LocalDirectoryArtifactStore store = new(BackupDir, Staging(LinkRefusedInto(BackupDir)));

        await using IBackupCheckpointStagingArea active = await store.BeginLiveCheckpointAsync(Guid.NewGuid(), ct);
        string crashed = Path.Combine(StagingRoot, Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(crashed);

        IReadOnlyList<BackupArtifactLeftover> leftovers = await store.ListLeftoversAsync(ct);

        BackupArtifactLeftover leftover = Assert.Single(leftovers);
        Assert.Equal(crashed, leftover.Handle);

        await store.DeleteLeftoverAsync(leftover, ct);
        Assert.False(Directory.Exists(crashed));
        Assert.True(Directory.Exists(Path.GetDirectoryName(active.LocalPath)));
    }

    // ── a full backup through the service ───────────────────────────────────────────────────────

    [Fact]
    public async Task TakeFull_BackupDirectoryOnAnotherFilesystem_PublishesTheTrimmedImage()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        Directory.CreateDirectory(StoragePath);

        using RocksDbPersistenceBackend backend = new(StoragePath, "v1");
        backend.StoreKeyValues([Item("kept", 1, rev: 1, physicalMs: 50)]);
        backend.StoreKeyValues([Item("after-cut", 2, rev: 1, physicalMs: 500)]);

        // A leftover from a capture that a crash interrupted; the GC after the backup reclaims it.
        string crashed = Path.Combine(StagingRoot, Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(crashed);

        InMemoryWAL wal = new(NullLogger<IRaft>.Instance);
        wal.Write([(1, [new RaftLog { Id = 1, Type = RaftLogType.Committed, Time = new HLCTimestamp(0, 100, 0) }])]);
        TestBackupService.StubRaft raft = new(wal, [new RaftPartitionRange { PartitionId = 1, State = RaftPartitionState.Active }]);

        bool stagedOnStore = false;
        bool LinkProbed(string source, string target)
        {
            if (Path.GetDirectoryName(target) == StagingRoot)
                stagedOnStore = true;
            return LinkRefusedInto(BackupDir)(source, target);
        }

        using BackupService service = new(
            raft,
            backend,
            BackupDir,
            BackupTestStores.Manifests(BackupDir),
            new LocalDirectoryArtifactStore(BackupDir, Staging(LinkProbed)),
            storageType: "rocksdb",
            storageRevision: "v1",
            flushBeforeCheckpoint: () => Task.CompletedTask,
            queryMinInFlight: () => Task.FromResult(HLCTimestamp.Zero),
            acquireRetentionHold: raft.AcquireRetentionHold);

        Kahuna.Shared.Communication.Rest.KahunaBackupInfo info = await service.TakeFullAsync(ct: ct);

        Assert.True(stagedOnStore);
        Assert.Empty(Directory.EnumerateFileSystemEntries(StagingRoot));

        // The published files match the manifest. Verify before the open below: opening an image in place
        // replays its WAL and rewrites files.
        Kahuna.Shared.Communication.Rest.KahunaBackupInfo listed =
            Assert.Single(await service.ListBackupsAsync(verifyArtifacts: true, ct), b => b.BackupId == info.BackupId);
        Assert.False(listed.IsInvalid, listed.InvalidReason);

        // The published image is the trimmed one.
        using RocksDbPersistenceBackend image = new(Path.Combine(BackupDir, info.BackupId.ToString("N")), "checkpoint");
        Assert.Equal((byte)1, image.GetKeyValue("kept")!.Value![0]);
        Assert.Null(image.GetKeyValue("after-cut"));
    }
}
