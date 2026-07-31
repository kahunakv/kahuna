
using System.Text;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Server.Persistence.Pitr;
using Kommander;
using Kommander.Data;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kahuna.Server.Tests;

/// <summary>
/// Tests for artifact integrity verification (size + digest, no missing/extra files), fail-closed
/// restore, cancellation observance, and honest catalog listing. Exercises the driver/verifier/
/// restore-engine static entry points directly so no live cluster is required.
/// </summary>
public sealed class TestPitrArtifactValidation : IDisposable
{
    private static readonly ILogger<IRaft> Log = NullLogger<IRaft>.Instance;

    private readonly string _tempRoot =
        Path.Combine(Path.GetTempPath(), "kahuna_val_" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        if (Directory.Exists(_tempRoot))
            Directory.Delete(_tempRoot, recursive: true);
    }

    // ── helpers ────────────────────────────────────────────────────────────────────────────

    private string ArtifactsDir(string name) => Path.Combine(_tempRoot, "artifacts_" + name);

    private BackupCatalog NewCatalog(string name) =>
        new(new LocalDirectoryStorageTarget(Path.Combine(_tempRoot, "catalog_" + name)));

    private static InMemoryWAL BuildWal(params (int partition, long id, long ticks)[] entries)
    {
        InMemoryWAL wal = new(Log);
        Dictionary<int, List<RaftLog>> byPartition = [];
        foreach ((int partition, long id, long ticks) in entries)
        {
            if (!byPartition.TryGetValue(partition, out List<RaftLog>? list))
                byPartition[partition] = list = [];
            list.Add(new RaftLog { Id = id, Type = RaftLogType.Committed, Time = new HLCTimestamp(0, ticks, 0) });
        }
        foreach ((int partition, List<RaftLog> logs) in byPartition)
            wal.Write([(partition, logs)]);
        return wal;
    }

    private static RaftPartitionRange Part(int id) => new() { PartitionId = id, State = RaftPartitionState.Active };

    private static void Put(MemoryPersistenceBackend b, string key, byte[] value, long rev) =>
        b.StoreKeyValues([new(key, value, rev, 0, 0, 0, 0, rev, 0, 0, rev, 0, 1)]);

    private async Task<(BackupCatalog catalog, string artifacts, BackupManifest full, BackupManifest inc)>
        BuildFullPlusIncremental(string tag)
    {
        InMemoryWAL wal = BuildWal((1, 1, 100));
        BackupCatalog catalog = NewCatalog(tag);
        string artifacts = ArtifactsDir(tag);

        MemoryPersistenceBackend backend = new();
        Put(backend, "k1", Encoding.UTF8.GetBytes("v1"), 1);

        BackupManifest full = await BackupDriver.RunFullAsync(
            wal, [Part(1)], backend, artifacts, catalog);

        wal.Write([(1, [new RaftLog { Id = 2, Type = RaftLogType.Committed, Time = new HLCTimestamp(0, 200, 0) }])]);

        BackupManifest inc = BackupDriver.RunIncremental(wal, [Part(1)], full.BackupId, artifacts, catalog);
        return (catalog, artifacts, full, inc);
    }

    private static string ArtifactFile(string artifacts, BackupManifest m, string relPath) =>
        Path.Combine(artifacts, m.BackupId.ToString("N"), relPath.Replace('/', Path.DirectorySeparatorChar));

    // ── publish-time hashing ─────────────────────────────────────────────────────────────────

    [Fact]
    public async Task RunFull_HashesAllCheckpointDataFiles_NotJustSidecar()
    {
        (_, _, BackupManifest full, _) = await BuildFullPlusIncremental("hash_all");

        Assert.Contains("checkpoint/store.json", full.Checksums.Keys);
        Assert.Contains("checkpoint/locks.json", full.Checksums.Keys);
        Assert.Contains("checkpoint/" + CheckpointManifest.FileName, full.Checksums.Keys);

        // Every checksummed file has a recorded byte length.
        Assert.Equal(full.Checksums.Count, full.Sizes.Count);
        foreach (string key in full.Checksums.Keys)
            Assert.True(full.Sizes.ContainsKey(key), $"missing size for {key}");
    }

    // ── corruption matrix: verifier fails closed ─────────────────────────────────────────────

    [Fact]
    public async Task Verify_TruncatedCheckpointFile_Throws()
    {
        (_, string artifacts, BackupManifest full, _) = await BuildFullPlusIncremental("trunc");

        File.WriteAllText(ArtifactFile(artifacts, full, "checkpoint/store.json"), "");

        Assert.Throws<BackupArtifactException>(() => BackupArtifactVerifier.Verify(full, artifacts, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Verify_SameLengthAlteredCheckpointFile_Throws()
    {
        (_, string artifacts, BackupManifest full, _) = await BuildFullPlusIncremental("flip");

        string file = ArtifactFile(artifacts, full, "checkpoint/store.json");
        byte[] bytes = File.ReadAllBytes(file);
        Assert.NotEmpty(bytes);
        bytes[0] ^= 0xFF; // same length, different digest
        File.WriteAllBytes(file, bytes);

        Assert.Throws<BackupArtifactException>(() => BackupArtifactVerifier.Verify(full, artifacts, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Verify_WrongDigestSegment_Throws()
    {
        (_, string artifacts, _, BackupManifest inc) = await BuildFullPlusIncremental("baddigest");

        string wal = ArtifactFile(artifacts, inc, "partition_1.wal");
        File.AppendAllText(wal, "garbage");

        Assert.Throws<BackupArtifactException>(() => BackupArtifactVerifier.Verify(inc, artifacts, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Verify_MissingDeclaredFile_Throws()
    {
        (_, string artifacts, BackupManifest full, _) = await BuildFullPlusIncremental("missing");

        File.Delete(ArtifactFile(artifacts, full, "checkpoint/store.json"));

        Assert.Throws<BackupArtifactException>(() => BackupArtifactVerifier.Verify(full, artifacts, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Verify_ExtraFile_Throws()
    {
        (_, string artifacts, BackupManifest full, _) = await BuildFullPlusIncremental("extra");

        File.WriteAllText(Path.Combine(artifacts, full.BackupId.ToString("N"), "stray.bin"), "x");

        Assert.Throws<BackupArtifactException>(() => BackupArtifactVerifier.Verify(full, artifacts, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Verify_LegacyFormatVersion_ThrowsUnsupportedNotCorrupt()
    {
        (_, string artifacts, BackupManifest full, _) = await BuildFullPlusIncremental("legacy");

        full.FormatVersion = 0; // simulate a pre-hardening manifest

        Assert.Throws<BackupUnsupportedFormatException>(() => BackupArtifactVerifier.Verify(full, artifacts, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Verify_TraversalKey_Throws()
    {
        (_, string artifacts, BackupManifest full, _) = await BuildFullPlusIncremental("traversal");

        full.Checksums["../escape"] = "deadbeef";

        Assert.Throws<BackupArtifactException>(() => BackupArtifactVerifier.Verify(full, artifacts, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Verify_RootedKey_Throws()
    {
        (_, string artifacts, BackupManifest full, _) = await BuildFullPlusIncremental("rooted");

        full.Checksums[Path.Combine(Path.GetTempPath(), "abs")] = "deadbeef";

        Assert.Throws<BackupArtifactException>(() => BackupArtifactVerifier.Verify(full, artifacts, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Verify_SymlinkInArtifactDir_Throws()
    {
        (_, string artifacts, BackupManifest full, _) = await BuildFullPlusIncremental("symlink");

        string link = Path.Combine(artifacts, full.BackupId.ToString("N"), "link");
        File.CreateSymbolicLink(link, Path.GetTempPath());

        Assert.Throws<BackupArtifactException>(() => BackupArtifactVerifier.Verify(full, artifacts, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Verify_FullWithNoChecksums_Throws()
    {
        (_, string artifacts, BackupManifest full, _) = await BuildFullPlusIncremental("emptyfull");

        full.Checksums.Clear();

        // A Full must have a checkpoint — empty checksums is corrupt, not "skip verification".
        Assert.Throws<BackupArtifactException>(() => BackupArtifactVerifier.Verify(full, artifacts, TestContext.Current.CancellationToken));
    }

    // ── restore fails closed ─────────────────────────────────────────────────────────────────

    [Fact]
    public async Task Restore_MissingSegment_FailsClosed()
    {
        (BackupCatalog catalog, string artifacts, _, BackupManifest inc) =
            await BuildFullPlusIncremental("restore_missing");

        IReadOnlyList<BackupManifest> chain = catalog.ResolveAndValidate(inc.BackupId, TestContext.Current.CancellationToken);
        File.Delete(ArtifactFile(artifacts, inc, "partition_1.wal"));

        MemoryPersistenceBackend target = new();
        Assert.Throws<BackupArtifactException>(() =>
            RestoreEngine.Restore(chain, artifacts, new HLCTimestamp(0, 1000, 0), target, ct: TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Restore_CorruptSegment_FailsClosed()
    {
        (BackupCatalog catalog, string artifacts, _, BackupManifest inc) =
            await BuildFullPlusIncremental("restore_corrupt");

        IReadOnlyList<BackupManifest> chain = catalog.ResolveAndValidate(inc.BackupId, TestContext.Current.CancellationToken);
        File.AppendAllText(ArtifactFile(artifacts, inc, "partition_1.wal"), "garbage");

        MemoryPersistenceBackend target = new();
        Assert.Throws<BackupArtifactException>(() =>
            RestoreEngine.Restore(chain, artifacts, new HLCTimestamp(0, 1000, 0), target, ct: TestContext.Current.CancellationToken));
    }

    // ── cancellation ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task RunFull_CancelledToken_ThrowsAndLeavesNoArtifact()
    {
        InMemoryWAL wal = BuildWal((1, 1, 100));
        BackupCatalog catalog = NewCatalog("cancel_full");
        string artifacts = ArtifactsDir("cancel_full");

        using CancellationTokenSource cts = new();
        await cts.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            BackupDriver.RunFullAsync(wal, [Part(1)], new MemoryPersistenceBackend(), artifacts, catalog,
                flushBeforeCheckpoint: null, snapshotT: null, ct: cts.Token));

        Assert.Empty(catalog.List(TestContext.Current.CancellationToken));
        if (Directory.Exists(artifacts))
            Assert.Empty(Directory.GetDirectories(artifacts));
    }

    [Fact]
    public async Task RunIncremental_CancelledToken_ThrowsAndLeavesNoArtifact()
    {
        (BackupCatalog catalog, string artifacts, BackupManifest full, _) =
            await BuildFullPlusIncremental("cancel_inc");

        int before = catalog.List(TestContext.Current.CancellationToken).Count;
        int dirsBefore = Directory.GetDirectories(artifacts).Length;

        using CancellationTokenSource cts = new();
        await cts.CancelAsync();

        InMemoryWAL wal = BuildWal((1, 1, 100), (1, 2, 200));
        Assert.ThrowsAny<OperationCanceledException>(() =>
            BackupDriver.RunIncremental(wal, [Part(1)], full.BackupId, artifacts, catalog,
                snapshotT: null, ct: cts.Token));

        // No new catalog entry and no new artifact directory beyond what already existed.
        Assert.Equal(before, catalog.List(TestContext.Current.CancellationToken).Count);
        Assert.Equal(dirsBefore, Directory.GetDirectories(artifacts).Length);
    }

    [Fact]
    public async Task Restore_CancelledToken_Throws()
    {
        (BackupCatalog catalog, string artifacts, _, BackupManifest inc) =
            await BuildFullPlusIncremental("cancel_restore");

        IReadOnlyList<BackupManifest> chain = catalog.ResolveAndValidate(inc.BackupId, TestContext.Current.CancellationToken);

        using CancellationTokenSource cts = new();
        await cts.CancelAsync();

        MemoryPersistenceBackend target = new();
        Assert.ThrowsAny<OperationCanceledException>(() =>
            RestoreEngine.Restore(chain, artifacts, new HLCTimestamp(0, 1000, 0), target, ct: cts.Token));
    }

    // ── chain semantic bounds ────────────────────────────────────────────────────────────────

    [Fact]
    public void Validate_FromHlcSortsAfterToHlc_Throws()
    {
        BackupManifest full = BackupManifest.CreateFull(
            [PartitionBackupRange.Create(1, 1, new HLCTimestamp(0, 500, 0), 5, new HLCTimestamp(0, 100, 0))]);

        Assert.Throws<BackupChainException>(() => BackupCatalog.Validate([full]));
    }

    // ── listing honesty ──────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task ListCorrupt_SurfacesUnparseableManifest()
    {
        string catalogDir = Path.Combine(_tempRoot, "corrupt_list");
        LocalDirectoryStorageTarget target = new(catalogDir);

        // One valid manifest.
        BackupManifest valid = BackupManifest.CreateFull([]);
        target.Put(valid);

        // One garbage manifest file with a recognizable id in its name.
        Guid badId = Guid.NewGuid();
        await File.WriteAllTextAsync(Path.Combine(catalogDir, badId.ToString("N") + ".manifest"), "{ not json", TestContext.Current.CancellationToken);

        Assert.Single(target.List(TestContext.Current.CancellationToken));
        IReadOnlyList<(Guid backupId, string reason)> corrupt = target.ListCorrupt(TestContext.Current.CancellationToken);
        Assert.Contains(corrupt, c => c.backupId == badId);
    }
}
