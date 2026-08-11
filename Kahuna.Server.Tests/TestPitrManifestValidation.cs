
using System.Text;
using System.Text.Json;
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
/// Corruption tests for manifest-schema and artifact-semantic verification: a self-consistent digest
/// over a structurally invalid or semantically mismatched manifest must be rejected before publish or
/// restore, not silently accepted. Each test takes a genuine backup, mutates one aspect, and asserts
/// verification fails closed.
/// </summary>
public sealed class TestPitrManifestValidation : IDisposable
{
    private static readonly ILogger<IRaft> Log = NullLogger<IRaft>.Instance;

    private readonly string _tempRoot =
        Path.Combine(Path.GetTempPath(), "kahuna_manval_" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        if (Directory.Exists(_tempRoot))
            try { Directory.Delete(_tempRoot, recursive: true); } catch { /* best-effort */ }
    }

    private string ArtifactsDir(string name) => Path.Combine(_tempRoot, "artifacts_" + name + Guid.NewGuid().ToString("N")[..6]);
    private BackupCatalog NewCatalog(string name) => new(new LocalDirectoryStorageTarget(Path.Combine(_tempRoot, "cat_" + name + Guid.NewGuid().ToString("N")[..6])));

    private static RaftPartitionRange Part(int id) => new() { PartitionId = id, State = RaftPartitionState.Active };

    private static InMemoryWAL BuildWal(int id, long ticks)
    {
        InMemoryWAL wal = new(Log);
        wal.Write([(1, new List<RaftLog> { new() { Id = id, Type = RaftLogType.Committed, Time = new HLCTimestamp(0, ticks, 0) } })]);
        return wal;
    }

    private static void Put(MemoryPersistenceBackend b, string key, long rev) =>
        b.StoreKeyValues([new(key, Encoding.UTF8.GetBytes("v" + rev), rev, 0, 0, 0, 0, rev, 0, 0, rev, 0, 1)]);

    private async Task<(string Artifacts, BackupManifest Full)> BuildFull(string tag)
    {
        InMemoryWAL wal = BuildWal(1, 100);
        BackupCatalog catalog = NewCatalog(tag);
        string artifacts = ArtifactsDir(tag);
        MemoryPersistenceBackend backend = new();
        Put(backend, "k1", 1);
        BackupManifest full = await BackupDriver.RunFullAsync(wal, [Part(1)], backend, BackupTestStores.Artifacts(artifacts), catalog);
        return (artifacts, full);
    }

    private async Task<(string Artifacts, BackupManifest Full, BackupManifest Inc)> BuildFullPlusIncremental(string tag)
    {
        InMemoryWAL wal = BuildWal(1, 100);
        BackupCatalog catalog = NewCatalog(tag);
        string artifacts = ArtifactsDir(tag);
        MemoryPersistenceBackend backend = new();
        Put(backend, "k1", 1);
        BackupManifest full = await BackupDriver.RunFullAsync(wal, [Part(1)], backend, BackupTestStores.Artifacts(artifacts), catalog);
        wal.Write([(1, [new RaftLog { Id = 2, Type = RaftLogType.Committed, Time = new HLCTimestamp(0, 200, 0) }])]);
        BackupManifest inc = await BackupDriver.RunIncrementalAsync(wal, [Part(1)], full.BackupId, BackupTestStores.Artifacts(artifacts), catalog);
        return (artifacts, full, inc);
    }

    // ── format version ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task FutureFormatVersion_Rejected()
    {
        (string artifacts, BackupManifest full) = await BuildFull("future_ver");
        full.FormatVersion = BackupManifest.CurrentFormatVersion + 1;
        await Assert.ThrowsAsync<BackupUnsupportedFormatException>(() => BackupArtifactVerifier.VerifyAsync(full, BackupTestStores.Artifacts(artifacts), TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task LegacyFormatVersion_Rejected()
    {
        (string artifacts, BackupManifest full) = await BuildFull("legacy_ver");
        full.FormatVersion = 0;
        await Assert.ThrowsAsync<BackupUnsupportedFormatException>(() => BackupArtifactVerifier.VerifyAsync(full, BackupTestStores.Artifacts(artifacts), TestContext.Current.CancellationToken));
    }

    // ── size keyset ────────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task MissingSizeForChecksum_Rejected()
    {
        (string artifacts, BackupManifest full) = await BuildFull("missing_size");
        full.Sizes.Remove(full.Sizes.Keys.First());
        await Assert.ThrowsAsync<BackupArtifactException>(() => BackupArtifactVerifier.VerifyAsync(full, BackupTestStores.Artifacts(artifacts), TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task ExtraSizeKey_Rejected()
    {
        (string artifacts, BackupManifest full) = await BuildFull("extra_size");
        full.Sizes["checkpoint/ghost.json"] = 10;
        await Assert.ThrowsAsync<BackupArtifactException>(() => BackupArtifactVerifier.VerifyAsync(full, BackupTestStores.Artifacts(artifacts), TestContext.Current.CancellationToken));
    }

    // ── partition ranges ───────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task DuplicatePartitionRange_Rejected()
    {
        (string artifacts, _, BackupManifest inc) = await BuildFullPlusIncremental("dup_range");
        inc.PartitionRanges.Add(PartitionBackupRange.Create(1, 3, new HLCTimestamp(0, 300, 0), 3, new HLCTimestamp(0, 300, 0), 0));
        await Assert.ThrowsAsync<BackupArtifactException>(() => BackupArtifactVerifier.VerifyAsync(inc, BackupTestStores.Artifacts(artifacts), TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task ToIndexBelowFromIndex_Rejected()
    {
        (string artifacts, _, BackupManifest inc) = await BuildFullPlusIncremental("badbounds");
        inc.PartitionRanges[0].ToIndex = inc.PartitionRanges[0].FromIndex - 1;
        await Assert.ThrowsAsync<BackupArtifactException>(() => BackupArtifactVerifier.VerifyAsync(inc, BackupTestStores.Artifacts(artifacts), TestContext.Current.CancellationToken));
    }

    // ── type / parent / base-cut ─────────────────────────────────────────────────────────────────

    [Fact]
    public async Task FullWithParent_Rejected()
    {
        (string artifacts, BackupManifest full) = await BuildFull("full_parent");
        full.ParentBackupId = Guid.NewGuid();
        await Assert.ThrowsAsync<BackupArtifactException>(() => BackupArtifactVerifier.VerifyAsync(full, BackupTestStores.Artifacts(artifacts), TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task FullWithoutBaseCut_Rejected()
    {
        (string artifacts, BackupManifest full) = await BuildFull("full_nobasecut");
        full.BaseCutNode = null;
        full.BaseCutPhysical = null;
        full.BaseCutCounter = null;
        await Assert.ThrowsAsync<BackupArtifactException>(() => BackupArtifactVerifier.VerifyAsync(full, BackupTestStores.Artifacts(artifacts), TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task IncrementalWithBaseCut_Rejected()
    {
        (string artifacts, _, BackupManifest inc) = await BuildFullPlusIncremental("inc_basecut");
        inc.SetBaseCut(new HLCTimestamp(0, 50, 0));
        await Assert.ThrowsAsync<BackupArtifactException>(() => BackupArtifactVerifier.VerifyAsync(inc, BackupTestStores.Artifacts(artifacts), TestContext.Current.CancellationToken));
    }

    // ── required artifact names ──────────────────────────────────────────────────────────────────

    [Fact]
    public async Task FullNonCheckpointArtifact_Rejected()
    {
        (string artifacts, BackupManifest full) = await BuildFull("full_badname");
        full.Checksums["partition_1.wal"] = new string('a', 64);
        await Assert.ThrowsAsync<BackupArtifactException>(() => BackupArtifactVerifier.VerifyAsync(full, BackupTestStores.Artifacts(artifacts), TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task IncrementalArtifactNotMatchingRange_Rejected()
    {
        (string artifacts, _, BackupManifest inc) = await BuildFullPlusIncremental("inc_extra");
        inc.Checksums["partition_9.wal"] = new string('a', 64);
        await Assert.ThrowsAsync<BackupArtifactException>(() => BackupArtifactVerifier.VerifyAsync(inc, BackupTestStores.Artifacts(artifacts), TestContext.Current.CancellationToken));
    }

    // ── semantic content ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task CheckpointSidecarBaseCutMismatch_Rejected()
    {
        (string artifacts, BackupManifest full) = await BuildFull("sidecar_mismatch");
        // A BaseCut that no longer matches the sidecar's recorded applied time.
        full.SetBaseCut(new HLCTimestamp(0, 99999, 0));
        await Assert.ThrowsAsync<BackupArtifactException>(() => BackupArtifactVerifier.VerifyAsync(full, BackupTestStores.Artifacts(artifacts), TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task SegmentEndpointIndexMismatch_Rejected()
    {
        (string artifacts, _, BackupManifest inc) = await BuildFullPlusIncremental("seg_toindex");
        // Declared ToIndex no longer matches the segment's last entry index.
        inc.PartitionRanges[0].ToIndex += 5;
        await Assert.ThrowsAsync<BackupArtifactException>(() => BackupArtifactVerifier.VerifyAsync(inc, BackupTestStores.Artifacts(artifacts), TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task SegmentFromHlcMismatch_Rejected()
    {
        (string artifacts, _, BackupManifest inc) = await BuildFullPlusIncremental("seg_fromhlc");
        // Declared FromHlc no longer matches the segment's first entry HLC.
        inc.PartitionRanges[0].FromHlcPhysical += 1;
        await Assert.ThrowsAsync<BackupArtifactException>(() => BackupArtifactVerifier.VerifyAsync(inc, BackupTestStores.Artifacts(artifacts), TestContext.Current.CancellationToken));
    }

    // ── symlinked per-backup artifact root ───────────────────────────────────────────────────────

    /// <summary>Moves a backup's per-id directory aside and replaces it with a symlink to the moved copy.</summary>
    private static void SymlinkBackupRoot(string artifacts, Guid backupId)
    {
        string root = Path.Combine(artifacts, backupId.ToString("N"));
        string real = root + "_real";
        Directory.Move(root, real);
        Directory.CreateSymbolicLink(root, real);
    }

    [Fact]
    public async Task FullBackupRootIsSymlink_Rejected()
    {
        (string artifacts, BackupManifest full) = await BuildFull("full_symroot");
        SymlinkBackupRoot(artifacts, full.BackupId);
        await Assert.ThrowsAsync<BackupArtifactException>(() => BackupArtifactVerifier.VerifyAsync(full, BackupTestStores.Artifacts(artifacts), TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task IncrementalBackupRootIsSymlink_Rejected()
    {
        (string artifacts, _, BackupManifest inc) = await BuildFullPlusIncremental("inc_symroot");
        SymlinkBackupRoot(artifacts, inc.BackupId);
        await Assert.ThrowsAsync<BackupArtifactException>(() => BackupArtifactVerifier.VerifyAsync(inc, BackupTestStores.Artifacts(artifacts), TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task RootSwappedToSymlinkAfterValidation_CaughtOnReVerify()
    {
        // The per-backup root is re-checked on every verification, so a swap to a symlink between a
        // prior validation and a later copy/replay is caught rather than silently followed.
        (string artifacts, BackupManifest full) = await BuildFull("swap_root");
        await BackupArtifactVerifier.VerifyAsync(full, BackupTestStores.Artifacts(artifacts), TestContext.Current.CancellationToken); // first pass: real directory, OK

        SymlinkBackupRoot(artifacts, full.BackupId);     // attacker swaps the root for a symlink

        await Assert.ThrowsAsync<BackupArtifactException>(() => BackupArtifactVerifier.VerifyAsync(full, BackupTestStores.Artifacts(artifacts), TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task TamperedSegmentEntryOrder_Rejected()
    {
        (string artifacts, _, BackupManifest inc) = await BuildFullPlusIncremental("seg_order");
        // Rewrite the segment with a duplicate/rolled-back index and re-hash so the digest still
        // matches — only the semantic order check can catch this.
        PartitionBackupRange r = inc.PartitionRanges[0];
        string segPath = Path.Combine(artifacts, inc.BackupId.ToString("N"), "partition_1.wal");
        var tampered = new[]
        {
            new { Id = r.ToIndex, Term = r.ToTerm, TimeNode = r.ToHlcNode, TimePhysical = r.ToHlcPhysical, TimeCounter = r.ToHlcCounter, LogType = (string?)null, LogData = (byte[]?)null },
            new { Id = r.ToIndex, Term = r.ToTerm, TimeNode = r.ToHlcNode, TimePhysical = r.ToHlcPhysical, TimeCounter = r.ToHlcCounter, LogType = (string?)null, LogData = (byte[]?)null },
        };
        File.WriteAllText(segPath, JsonSerializer.Serialize(tampered));
        // Re-point the manifest's checksum/size at the tampered bytes so integrity passes.
        inc.Checksums["partition_1.wal"] = BackupArtifactVerifier.ComputeSha256(segPath);
        inc.Sizes["partition_1.wal"] = new FileInfo(segPath).Length;

        await Assert.ThrowsAsync<BackupArtifactException>(() => BackupArtifactVerifier.VerifyAsync(inc, BackupTestStores.Artifacts(artifacts), TestContext.Current.CancellationToken));
    }
}
