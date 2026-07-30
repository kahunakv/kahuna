
using System.Text;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Server.Persistence.Pitr;
using Kommander;
using Kommander.Communication;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL;
using Kommander.WAL.IO;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kahuna.Server.Tests;

/// <summary>
/// Unit tests for <see cref="BackupService"/>: flush-hook contract, DTO mapping,
/// catalog list/chain operations, and offline restore-to-directory.
/// Does NOT require a live cluster; uses a stub IRaft backed by InMemoryWAL.
/// </summary>
public sealed class TestBackupService : IDisposable
{
    private static readonly ILogger<IRaft> Log = NullLogger<IRaft>.Instance;

    private readonly string _tempRoot =
        Path.Combine(Path.GetTempPath(), "kahuna_svc_" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        if (Directory.Exists(_tempRoot))
            Directory.Delete(_tempRoot, recursive: true);
    }

    // ── helpers ────────────────────────────────────────────────────────────────────────────

    private string BackupDir(string tag) => Path.Combine(_tempRoot, "bak_" + tag);

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

    private static StubRaft MakeRaft(InMemoryWAL wal, params int[] partitionIds) =>
        new(wal, partitionIds.Select(id => new RaftPartitionRange { PartitionId = id, State = RaftPartitionState.Active }).ToArray());

    private BackupService MakeService(
        string tag,
        InMemoryWAL wal,
        MemoryPersistenceBackend backend,
        Func<Task>? flush = null,
        Func<Task<HLCTimestamp>>? queryMinInFlight = null,
        StubRaft? raft = null,
        string? restoreRoot = null)
    {
        raft ??= MakeRaft(wal, 1);
        return new BackupService(
            raft,
            backend,
            BackupDir(tag),
            storageType: "memory",
            storageRevision: "",
            flushBeforeCheckpoint: flush ?? (() => Task.CompletedTask),
            queryMinInFlight: queryMinInFlight ?? (() => Task.FromResult(HLCTimestamp.Zero)),
            restoreRoot: restoreRoot);
    }

    private static void Put(MemoryPersistenceBackend b, string key, byte[] value, long rev) =>
        b.StoreKeyValues([new(key, value, rev, 0, 0, 0, 0, rev, 0, 0, rev, 0, 1)]);

    // ── B1: flush hook is invoked, data written by hook appears in checkpoint ─────────────

    [Fact]
    public async Task TakeFullAsync_FlushHookInvoked_DataAppearsInCheckpoint()
    {
        MemoryPersistenceBackend backend = new();
        InMemoryWAL wal = BuildWal((1, 1, 100));
        bool flushCalled = false;

        Task Flush()
        {
            flushCalled = true;
            Put(backend, "sentinel", Encoding.UTF8.GetBytes("flushed"), 1);
            return Task.CompletedTask;
        }

        BackupService svc = MakeService("flush", wal, backend, flush: Flush);
        await svc.TakeFullAsync();

        Assert.True(flushCalled, "flush delegate must be called");

        // The data written inside Flush must appear in the resulting checkpoint.
        string checkpointPath = Path.Combine(BackupDir("flush"),
            Directory.GetDirectories(BackupDir("flush")).Single(d => !d.EndsWith("catalog")),
            "checkpoint");
        // Walk: backupDir/catalogDir or backupDir/<guid>/checkpoint
        string actualCheckpointPath = FindCheckpointDir(BackupDir("flush"));
        MemoryPersistenceBackend restored = MemoryPersistenceBackend.OpenCheckpoint(actualCheckpointPath);
        Assert.NotNull(restored.GetKeyValue("sentinel"));
    }

    // ── B2: DTO mapping ────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task TakeFullAsync_ReturnsDtoWithTypeEqualToFull()
    {
        InMemoryWAL wal = BuildWal((1, 1, 100));
        BackupService svc = MakeService("dto_full", wal, new MemoryPersistenceBackend());
        Kahuna.Shared.Communication.Rest.KahunaBackupInfo dto = await svc.TakeFullAsync();
        Assert.Equal("Full", dto.Type);
        Assert.NotEqual(Guid.Empty, dto.BackupId);
    }

    [Fact]
    public async Task TakeIncrementalAsync_AfterFull_DtoHasParentId()
    {
        InMemoryWAL wal = BuildWal((1, 1, 100));
        BackupService svc = MakeService("dto_inc", wal, new MemoryPersistenceBackend());
        Kahuna.Shared.Communication.Rest.KahunaBackupInfo full = await svc.TakeFullAsync();

        wal.Write([(1, [new RaftLog { Id = 2, Type = RaftLogType.Committed, Time = new HLCTimestamp(0, 200, 0) }])]);
        Kahuna.Shared.Communication.Rest.KahunaBackupInfo inc = await svc.TakeIncrementalAsync(full.BackupId);

        Assert.Equal("Incremental", inc.Type);
        Assert.Equal(full.BackupId, inc.ParentBackupId);
    }

    // ── B3: catalog list / chain ────────────────────────────────────────────────────────────

    [Fact]
    public async Task ListBackups_IncludesFullAndIncremental()
    {
        InMemoryWAL wal = BuildWal((1, 1, 100));
        BackupService svc = MakeService("list", wal, new MemoryPersistenceBackend());
        Kahuna.Shared.Communication.Rest.KahunaBackupInfo full = await svc.TakeFullAsync();

        wal.Write([(1, [new RaftLog { Id = 2, Type = RaftLogType.Committed, Time = new HLCTimestamp(0, 200, 0) }])]);
        Kahuna.Shared.Communication.Rest.KahunaBackupInfo inc = await svc.TakeIncrementalAsync(full.BackupId);

        IReadOnlyList<Kahuna.Shared.Communication.Rest.KahunaBackupInfo> all = svc.ListBackups();
        Assert.Equal(2, all.Count);
        Assert.Contains(all, b => b.BackupId == full.BackupId);
        Assert.Contains(all, b => b.BackupId == inc.BackupId);
    }

    [Fact]
    public async Task ResolveAndValidate_ChainOrder_FullFirst()
    {
        InMemoryWAL wal = BuildWal((1, 1, 100));
        BackupService svc = MakeService("chain", wal, new MemoryPersistenceBackend());
        Kahuna.Shared.Communication.Rest.KahunaBackupInfo full = await svc.TakeFullAsync();

        wal.Write([(1, [new RaftLog { Id = 2, Type = RaftLogType.Committed, Time = new HLCTimestamp(0, 200, 0) }])]);
        Kahuna.Shared.Communication.Rest.KahunaBackupInfo inc = await svc.TakeIncrementalAsync(full.BackupId);

        IReadOnlyList<Kahuna.Shared.Communication.Rest.KahunaBackupInfo> chain = svc.ResolveAndValidate(inc.BackupId);
        Assert.Equal(2, chain.Count);
        Assert.Equal("Full", chain[0].Type);
        Assert.Equal("Incremental", chain[1].Type);
    }

    // ── B4: offline restore ─────────────────────────────────────────────────────────────────

    [Fact]
    public async Task RestoreTo_RestoredDirContainsDataFromCheckpoint()
    {
        // Arrange: full backup with data written during flush
        MemoryPersistenceBackend backend = new();
        InMemoryWAL wal = BuildWal((1, 1, 100));

        Task Flush()
        {
            Put(backend, "restore_key", Encoding.UTF8.GetBytes("hello"), 1);
            return Task.CompletedTask;
        }

        BackupService svc = MakeService("restore_data", wal, backend, flush: Flush);
        Kahuna.Shared.Communication.Rest.KahunaBackupInfo full = await svc.TakeFullAsync();

        string targetDir = Path.Combine(_tempRoot, "restored_data");

        // Act
        Kahuna.Shared.Communication.Rest.KahunaRestoreResponse result =
            svc.RestoreTo(full.BackupId, targetDir, HLCTimestamp.Zero);

        // Assert: target dir exists; chain has one entry; an OpenCheckpoint from target reads the key
        Assert.True(Directory.Exists(targetDir));
        Assert.Single(result.Chain);
        MemoryPersistenceBackend check = MemoryPersistenceBackend.OpenCheckpoint(targetDir);
        Assert.NotNull(check.GetKeyValue("restore_key"));
    }

    [Fact]
    public async Task RestoreTo_WithIncrementalWal_EntryCountNonZero()
    {
        // Arrange: full + one incremental carrying one committed entry
        MemoryPersistenceBackend backend = new();
        InMemoryWAL wal = BuildWal((1, 1, 100));
        BackupService svc = MakeService("restore_inc", wal, backend);
        Kahuna.Shared.Communication.Rest.KahunaBackupInfo full = await svc.TakeFullAsync();

        wal.Write([(1, [new RaftLog { Id = 2, Type = RaftLogType.Committed, Time = new HLCTimestamp(0, 200, 0) }])]);
        Kahuna.Shared.Communication.Rest.KahunaBackupInfo inc = await svc.TakeIncrementalAsync(full.BackupId);

        string targetDir = Path.Combine(_tempRoot, "restored_inc");

        // Act
        Kahuna.Shared.Communication.Rest.KahunaRestoreResponse result =
            svc.RestoreTo(inc.BackupId, targetDir, HLCTimestamp.Zero);

        // Full + incremental chain
        Assert.Equal(2, result.Chain.Count);
        Assert.Equal(targetDir, result.TargetDir);
    }

    /// <summary>
    /// Regression: passing HLCTimestamp.Zero (the "no --target-time-ms" default) must resolve
    /// to max(chain ToHlc) before calling RestoreEngine, not pass Zero verbatim.
    /// With Zero verbatim, the engine's stop-predicate (entry.Time > targetTime) fires on the
    /// very first entry and breaks, silently dropping all incrementals.
    /// This test verifies that the chain-tip translation fires without throwing.
    /// The full observable regression (second key present after restore) is in
    /// TestBackupStackIntegration.RestoreToAsync_ZeroTarget_AppliesIncrementals.
    /// </summary>
    [Fact]
    public async Task RestoreTo_ZeroTargetTime_DoesNotThrowAndReturnsTwoChainEntries()
    {
        MemoryPersistenceBackend backend = new();
        InMemoryWAL wal = BuildWal((1, 1, 100));
        BackupService svc = MakeService("zero_t", wal, backend);
        Kahuna.Shared.Communication.Rest.KahunaBackupInfo full = await svc.TakeFullAsync();

        wal.Write([(1, [new RaftLog { Id = 2, Type = RaftLogType.Committed, Time = new HLCTimestamp(0, 200, 0) }])]);
        Kahuna.Shared.Communication.Rest.KahunaBackupInfo inc = await svc.TakeIncrementalAsync(full.BackupId);

        string targetDir = Path.Combine(_tempRoot, "zero_t_out");

        // Must not throw; chain must be returned intact.
        Kahuna.Shared.Communication.Rest.KahunaRestoreResponse result =
            svc.RestoreTo(inc.BackupId, targetDir, HLCTimestamp.Zero);

        Assert.Equal(2, result.Chain.Count);
        Assert.Equal(targetDir, result.TargetDir);
    }

    [Fact]
    public async Task RestoreTo_MissingCheckpointDir_Throws()
    {
        InMemoryWAL wal = BuildWal((1, 1, 100));
        BackupService svc = MakeService("restore_miss", wal, new MemoryPersistenceBackend());
        Kahuna.Shared.Communication.Rest.KahunaBackupInfo full = await svc.TakeFullAsync();

        // Corrupt the backup by deleting its checkpoint dir. Up-front artifact verification catches
        // the missing declared checkpoint files and fails closed before any copy.
        string bakDir = BackupDir("restore_miss");
        string checkpointDir = Path.Combine(bakDir, full.BackupId.ToString("N"), "checkpoint");
        Directory.Delete(checkpointDir, recursive: true);

        string targetDir = Path.Combine(_tempRoot, "restored_miss");
        Assert.Throws<BackupArtifactException>(() =>
            svc.RestoreTo(full.BackupId, targetDir, HLCTimestamp.Zero));
    }

    // ── B5: flush hook NOT provided → checkpoint is taken but data may be absent ──────────

    [Fact]
    public async Task TakeFullAsync_NoFlushHook_CheckpointIsEmptyWhenBackendWasEmpty()
    {
        InMemoryWAL wal = BuildWal((1, 1, 100));
        BackupService svc = MakeService("no_flush", wal, new MemoryPersistenceBackend(), flush: null);
        Kahuna.Shared.Communication.Rest.KahunaBackupInfo dto = await svc.TakeFullAsync();

        string checkpointPath = FindCheckpointDir(BackupDir("no_flush"));
        MemoryPersistenceBackend restored = MemoryPersistenceBackend.OpenCheckpoint(checkpointPath);
        Assert.Null(restored.GetKeyValue("anything"));
        Assert.Equal("Full", dto.Type);
    }

    // ── helpers ────────────────────────────────────────────────────────────────────────────

    /// <summary>Locates the checkpoint sub-directory inside the backup artifacts root.</summary>
    private static string FindCheckpointDir(string bakDir)
    {
        foreach (string d in Directory.GetDirectories(bakDir))
        {
            string cp = Path.Combine(d, "checkpoint");
            if (Directory.Exists(cp))
                return cp;
        }
        throw new DirectoryNotFoundException($"No checkpoint found under {bakDir}");
    }

    // ── minimal IRaft stub ─────────────────────────────────────────────────────────────────

    // ── typed outcomes / substitution ───────────────────────────────────────────────────────

    [Fact]
    public async Task TakeFullAsync_ReportsRequestedAndActualKind()
    {
        InMemoryWAL wal = BuildWal((1, 1, 100));
        BackupService svc = MakeService("kind_full", wal, new MemoryPersistenceBackend());

        Kahuna.Shared.Communication.Rest.KahunaBackupInfo dto = await svc.TakeFullAsync();

        Assert.Equal("Full", dto.RequestedKind);
        Assert.Equal("Full", dto.ActualKind);
        Assert.Null(dto.SubstitutionReason);
    }

    [Fact]
    public async Task TakeIncrementalAsync_FloorPastParent_FallsBackToFull_AndReportsSubstitution()
    {
        InMemoryWAL inner = BuildWal((1, 1, 100));
        FloorWal wal = new(inner, partitionId: 1, floor: 10);
        StubRaft raft = new(wal, [new RaftPartitionRange { PartitionId = 1, State = RaftPartitionState.Active }]);

        BackupService svc = new(
            raft,
            new MemoryPersistenceBackend(),
            BackupDir("subst"),
            storageType: "memory",
            storageRevision: "",
            flushBeforeCheckpoint: () => Task.CompletedTask,
            queryMinInFlight: () => Task.FromResult(HLCTimestamp.Zero));

        Kahuna.Shared.Communication.Rest.KahunaBackupInfo full = await svc.TakeFullAsync();

        // The WAL floor (10) is past the incremental start (parent.ToIndex + 1 = 2), so the
        // incremental cannot be produced and the service substitutes a full backup.
        Kahuna.Shared.Communication.Rest.KahunaBackupInfo inc = await svc.TakeIncrementalAsync(full.BackupId);

        Assert.Equal("Incremental", inc.RequestedKind);
        Assert.Equal("Full", inc.ActualKind);
        Assert.Equal("Full", inc.Type);
        Assert.False(string.IsNullOrEmpty(inc.SubstitutionReason));
    }

    [Fact]
    public async Task RestoreTo_Success_ReportsOutcomeOk()
    {
        MemoryPersistenceBackend backend = new();
        InMemoryWAL wal = BuildWal((1, 1, 100));
        Put(backend, "k1", Encoding.UTF8.GetBytes("v1"), 1);

        BackupService svc = MakeService("restore_ok", wal, backend);
        Kahuna.Shared.Communication.Rest.KahunaBackupInfo full = await svc.TakeFullAsync();

        string targetDir = Path.Combine(_tempRoot, "restore_ok_target");
        Kahuna.Shared.Communication.Rest.KahunaRestoreResponse result =
            svc.RestoreTo(full.BackupId, targetDir, HLCTimestamp.Zero);

        Assert.Equal(Kahuna.Shared.Communication.Rest.KahunaBackupOutcome.Ok, result.Outcome);
    }

    // ── coverage bounds ──────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task RestoreTo_TargetBelowBaseCut_ThrowsOutsideCoverage()
    {
        InMemoryWAL wal = BuildWal((1, 1, 100)); // base cut = (0,100,0)
        BackupService svc = MakeService("cov_low", wal, new MemoryPersistenceBackend());
        Kahuna.Shared.Communication.Rest.KahunaBackupInfo full = await svc.TakeFullAsync();

        string targetDir = Path.Combine(_tempRoot, "cov_low_out");
        BackupDriverException ex = Assert.Throws<BackupDriverException>(() =>
            svc.RestoreTo(full.BackupId, targetDir, new HLCTimestamp(0, 50, 0)));

        Assert.True(ex.TargetOutsideCoverage);
        Assert.False(Directory.Exists(targetDir));
    }

    [Fact]
    public async Task RestoreTo_TargetAboveCoverage_ThrowsOutsideCoverage()
    {
        InMemoryWAL wal = BuildWal((1, 1, 100));
        BackupService svc = MakeService("cov_high", wal, new MemoryPersistenceBackend());
        Kahuna.Shared.Communication.Rest.KahunaBackupInfo full = await svc.TakeFullAsync();

        string targetDir = Path.Combine(_tempRoot, "cov_high_out");
        BackupDriverException ex = Assert.Throws<BackupDriverException>(() =>
            svc.RestoreTo(full.BackupId, targetDir, new HLCTimestamp(0, 999, 0)));

        Assert.True(ex.TargetOutsideCoverage);
    }

    [Fact]
    public async Task RestoreTo_Success_ExposesCoverageBounds()
    {
        MemoryPersistenceBackend backend = new();
        InMemoryWAL wal = BuildWal((1, 1, 100));
        Put(backend, "k1", Encoding.UTF8.GetBytes("v1"), 1);
        BackupService svc = MakeService("cov_ok", wal, backend);
        Kahuna.Shared.Communication.Rest.KahunaBackupInfo full = await svc.TakeFullAsync();

        string targetDir = Path.Combine(_tempRoot, "cov_ok_out");
        Kahuna.Shared.Communication.Rest.KahunaRestoreResponse result =
            svc.RestoreTo(full.BackupId, targetDir, HLCTimestamp.Zero);

        Assert.Equal(100, result.MinRecoverablePhysicalMs);
        Assert.Equal(100, result.MaxRecoverablePhysicalMs);
    }

    [Fact]
    public async Task ResolveAndValidate_HeadEntryCarriesCoverageBounds()
    {
        MemoryPersistenceBackend backend = new();
        InMemoryWAL wal = BuildWal((1, 1, 100));
        Put(backend, "k1", Encoding.UTF8.GetBytes("v1"), 1);
        BackupService svc = MakeService("chain_cov", wal, backend);

        Kahuna.Shared.Communication.Rest.KahunaBackupInfo full = await svc.TakeFullAsync();
        wal.Write([(1, [new RaftLog { Id = 2, Type = RaftLogType.Committed, Time = new HLCTimestamp(0, 300, 0) }])]);
        Kahuna.Shared.Communication.Rest.KahunaBackupInfo inc = await svc.TakeIncrementalAsync(full.BackupId);

        IReadOnlyList<Kahuna.Shared.Communication.Rest.KahunaBackupInfo> chain = svc.ResolveAndValidate(inc.BackupId);

        // Head (Full) carries the recoverable window; base cut = 100, chain tip = 300.
        Assert.Equal(100, chain[0].MinRecoverablePhysicalMs);
        Assert.Equal(300, chain[0].MaxRecoverablePhysicalMs);
        // Non-head entries do not carry bounds.
        Assert.Null(chain[1].MinRecoverablePhysicalMs);
        Assert.Null(chain[1].MaxRecoverablePhysicalMs);
    }

    [Fact]
    public async Task ListBackups_LegacyManifest_ListedHonestlyNotCorrupt()
    {
        MemoryPersistenceBackend backend = new();
        InMemoryWAL wal = BuildWal((1, 1, 100));
        Put(backend, "k1", Encoding.UTF8.GetBytes("v1"), 1);
        BackupService svc = MakeService("legacy_list", wal, backend);

        Kahuna.Shared.Communication.Rest.KahunaBackupInfo current = await svc.TakeFullAsync();

        // Simulate a pre-hardening (legacy) manifest sitting in the same backup directory.
        BackupManifest legacy = BackupManifest.CreateFull([]);
        legacy.FormatVersion = 0;
        new LocalDirectoryStorageTarget(BackupDir("legacy_list")).Put(legacy);

        IReadOnlyList<Kahuna.Shared.Communication.Rest.KahunaBackupInfo> listed = svc.ListBackups();

        Kahuna.Shared.Communication.Rest.KahunaBackupInfo legacyEntry =
            listed.Single(b => b.BackupId == legacy.BackupId);
        Assert.Equal(0, legacyEntry.FormatVersion);
        Assert.False(legacyEntry.IsInvalid); // old, not corrupt

        Kahuna.Shared.Communication.Rest.KahunaBackupInfo currentEntry =
            listed.Single(b => b.BackupId == current.BackupId);
        Assert.Equal(BackupManifest.CurrentFormatVersion, currentEntry.FormatVersion);
    }

    // ── confine restore / atomic publish ─────────────────────────────────────────────────────

    [Fact]
    public async Task RestoreTo_ExistingNonEmptyTarget_ThrowsTargetConflict()
    {
        InMemoryWAL wal = BuildWal((1, 1, 100));
        BackupService svc = MakeService("conf_exists", wal, new MemoryPersistenceBackend());
        Kahuna.Shared.Communication.Rest.KahunaBackupInfo full = await svc.TakeFullAsync();

        string targetDir = Path.Combine(_tempRoot, "conf_exists_out");
        Directory.CreateDirectory(targetDir);
        await File.WriteAllTextAsync(Path.Combine(targetDir, "occupied.txt"), "x");

        BackupDriverException ex = Assert.Throws<BackupDriverException>(() =>
            svc.RestoreTo(full.BackupId, targetDir, HLCTimestamp.Zero));
        Assert.True(ex.TargetConflict);

        // The pre-existing content is untouched.
        Assert.True(File.Exists(Path.Combine(targetDir, "occupied.txt")));
    }

    [Fact]
    public async Task RestoreTo_TargetNestedInsideBackupDir_ThrowsTargetConflict()
    {
        InMemoryWAL wal = BuildWal((1, 1, 100));
        BackupService svc = MakeService("conf_overlap", wal, new MemoryPersistenceBackend());
        Kahuna.Shared.Communication.Rest.KahunaBackupInfo full = await svc.TakeFullAsync();

        // A fresh (non-existent) directory nested under the backup root → overlap, not non-empty.
        string targetDir = Path.Combine(BackupDir("conf_overlap"), "nested_restore");
        BackupDriverException ex = Assert.Throws<BackupDriverException>(() =>
            svc.RestoreTo(full.BackupId, targetDir, HLCTimestamp.Zero));
        Assert.True(ex.TargetConflict);
    }

    [Fact]
    public async Task RestoreTo_Success_LeavesNoStagingDirectory()
    {
        MemoryPersistenceBackend backend = new();
        InMemoryWAL wal = BuildWal((1, 1, 100));
        Put(backend, "k1", Encoding.UTF8.GetBytes("v1"), 1);
        BackupService svc = MakeService("nostage", wal, backend);
        Kahuna.Shared.Communication.Rest.KahunaBackupInfo full = await svc.TakeFullAsync();

        string targetDir = Path.Combine(_tempRoot, "nostage_out");
        svc.RestoreTo(full.BackupId, targetDir, HLCTimestamp.Zero);

        Assert.True(Directory.Exists(targetDir));
        Assert.Empty(Directory.GetDirectories(_tempRoot, "nostage_out.staging_*"));
    }

    [Fact]
    public async Task RestoreTo_CorruptChain_LeavesNoTargetOrStaging()
    {
        MemoryPersistenceBackend backend = new();
        InMemoryWAL wal = BuildWal((1, 1, 100));
        Put(backend, "k1", Encoding.UTF8.GetBytes("v1"), 1);
        BackupService svc = MakeService("corrupt_restore", wal, backend);
        Kahuna.Shared.Communication.Rest.KahunaBackupInfo full = await svc.TakeFullAsync();

        // Corrupt a checkpoint data file so up-front verification fails.
        string storeJson = Path.Combine(BackupDir("corrupt_restore"), full.BackupId.ToString("N"), "checkpoint", "store.json");
        await File.WriteAllTextAsync(storeJson, "corrupted");

        string targetDir = Path.Combine(_tempRoot, "corrupt_restore_out");
        Assert.Throws<BackupArtifactException>(() =>
            svc.RestoreTo(full.BackupId, targetDir, HLCTimestamp.Zero));

        Assert.False(Directory.Exists(targetDir));
        Assert.Empty(Directory.GetDirectories(_tempRoot, "corrupt_restore_out.staging_*"));
    }

    // ── restore destination confinement (finding #8) ─────────────────────────────────────────

    [Fact]
    public async Task RestoreTo_OutsideConfiguredRoot_ThrowsTargetConflict()
    {
        string root = Path.Combine(_tempRoot, "root_ok");
        Directory.CreateDirectory(root);

        MemoryPersistenceBackend backend = new();
        InMemoryWAL wal = BuildWal((1, 1, 100));
        Put(backend, "k1", Encoding.UTF8.GetBytes("v1"), 1);
        BackupService svc = MakeService("confine_out", wal, backend, restoreRoot: root);
        Kahuna.Shared.Communication.Rest.KahunaBackupInfo full = await svc.TakeFullAsync();

        string outside = Path.Combine(_tempRoot, "outside_target");
        BackupDriverException ex = Assert.Throws<BackupDriverException>(() =>
            svc.RestoreTo(full.BackupId, outside, HLCTimestamp.Zero));
        Assert.True(ex.TargetConflict);
        Assert.False(Directory.Exists(outside));
    }

    [Fact]
    public async Task RestoreTo_InsideConfiguredRoot_Succeeds()
    {
        string root = Path.Combine(_tempRoot, "root_in");
        Directory.CreateDirectory(root);

        MemoryPersistenceBackend backend = new();
        InMemoryWAL wal = BuildWal((1, 1, 100));
        Put(backend, "k1", Encoding.UTF8.GetBytes("v1"), 1);
        BackupService svc = MakeService("confine_in", wal, backend, restoreRoot: root);
        Kahuna.Shared.Communication.Rest.KahunaBackupInfo full = await svc.TakeFullAsync();

        string target = Path.Combine(root, "restore_here");
        Kahuna.Shared.Communication.Rest.KahunaRestoreResponse result =
            svc.RestoreTo(full.BackupId, target, HLCTimestamp.Zero);
        Assert.Equal(target, result.TargetDir);
        Assert.True(Directory.Exists(target));
    }

    [Fact]
    public async Task RestoreTo_SymlinkedAncestorUnderRoot_ThrowsTargetConflict()
    {
        // An attacker plants a symlinked ancestor UNDER the server-owned root, aiming to redirect the
        // write outside it — must be rejected even though the path is lexically within the root.
        string root = Path.Combine(_tempRoot, "sym_root");
        Directory.CreateDirectory(root);
        string realElsewhere = Path.Combine(_tempRoot, "sym_elsewhere");
        Directory.CreateDirectory(realElsewhere);
        string linkUnderRoot = Path.Combine(root, "link");
        Directory.CreateSymbolicLink(linkUnderRoot, realElsewhere);

        MemoryPersistenceBackend backend = new();
        InMemoryWAL wal = BuildWal((1, 1, 100));
        Put(backend, "k1", Encoding.UTF8.GetBytes("v1"), 1);
        BackupService svc = MakeService("confine_symlink", wal, backend, restoreRoot: root);
        Kahuna.Shared.Communication.Rest.KahunaBackupInfo full = await svc.TakeFullAsync();

        string target = Path.Combine(linkUnderRoot, "restore_here");
        BackupDriverException ex = Assert.Throws<BackupDriverException>(() =>
            svc.RestoreTo(full.BackupId, target, HLCTimestamp.Zero));
        Assert.True(ex.TargetConflict);
    }

    /// <summary>
    /// Wraps an <see cref="IWAL"/> and substitutes a fixed compaction floor for one partition so
    /// the incremental floor check in <see cref="BackupDriver.RunIncremental"/> can be exercised.
    /// </summary>
    private sealed class FloorWal(IWAL inner, int partitionId, long floor) : IWAL
    {
        public long GetLastCheckpoint(int p) => p == partitionId ? floor : inner.GetLastCheckpoint(p);

        public List<RaftLog> ReadLogs(int p) => inner.ReadLogs(p);
        public List<RaftLog> ReadLogsRange(int p, long start, int max = int.MaxValue) => inner.ReadLogsRange(p, start, max);
        public RaftOperationStatus Write(List<(int, List<RaftLog>)> logs) => inner.Write(logs);
        public long GetMaxLog(int p) => inner.GetMaxLog(p);
        public long GetCurrentTerm(int p) => inner.GetCurrentTerm(p);
        public int CountPersistedLogs(int p) => inner.CountPersistedLogs(p);
        public int CountRemovableLogs(int p) => inner.CountRemovableLogs(p);
        public string? GetMetaData(string key) => inner.GetMetaData(key);
        public bool SetMetaData(string key, string value) => inner.SetMetaData(key, value);
        public (RaftOperationStatus Status, int Removed) CompactLogsOlderThan(int p, long lc, int ce, int? max = null) =>
            inner.CompactLogsOlderThan(p, lc, ce, max);
        public RaftOperationStatus DeletePartitionWAL(int p) => inner.DeletePartitionWAL(p);
        public RaftOperationStatus TruncateLogsAfter(int p, long after) => inner.TruncateLogsAfter(p, after);
        public (RaftOperationStatus Status, long MaxLogId) TruncateLogsAfterAndGetMax(int p, long after) => inner.TruncateLogsAfterAndGetMax(p, after);
        public void Dispose() => inner.Dispose();
    }

    internal sealed class StubRaft(IWAL wal, RaftPartitionRange[] partitions) : IRaft
    {
        public IWAL WalAdapter => wal;
        public IReadOnlyList<RaftPartitionRange> GetPartitionMap() => partitions;

        public RaftConfiguration Configuration { get; } = new() { Host = "localhost", Port = 19999 };
        public string GetLocalEndpoint() => "localhost:19999";
        public ClusterMemberRole LocalRole => ClusterMemberRole.Voter;
        public ClusterMembership GetMembership() => new();
        public bool Joined => true;
        public bool IsInitialized => true;
        public ICommunication Communication => null!;
        public IDiscovery Discovery => null!;
        public HybridLogicalClock HybridLogicalClock => null!;
        public IRaftReadScheduler ReadScheduler => null!;
        public IRaftWalScheduler WalScheduler => null!;

        public event Func<int, RaftLog, Task<bool>>? OnLogRestored { add { } remove { } }
        public event Func<int, RaftLog, Task<bool>>? OnReplicationReceived { add { } remove { } }
        public event Action<int, RaftLog>? OnReplicationError { add { } remove { } }
        public event Action<ClusterMembership>? OnMembershipChanged { add { } remove { } }
        public event Action<int>? OnRestoreStarted { add { } remove { } }
        public event Action<int>? OnRestoreFinished { add { } remove { } }
        public event Func<int, string, Task<bool>>? OnLeaderChanged { add { } remove { } }
        public event Action<IReadOnlyList<RaftPartitionRange>>? OnPartitionMapChanged { add { } remove { } }

        public Task JoinCluster(IEnumerable<string> seeds, CancellationToken ct = default) => Task.CompletedTask;
        public Task JoinCluster(CancellationToken ct = default) => Task.CompletedTask;
        public Task LeaveCluster(bool dispose = false, CancellationToken cancellationToken = default) => Task.CompletedTask;
        public int GetPartitionKey(string partitionKey) => 0;
        public int GetPrefixPartitionKey(string prefixPartitionKey) => 0;
        public long GetPartitionGeneration(int partitionId) => 0;
        public ValueTask<long?> GetFollowerLagAsync(int partitionId, string followerEndpoint) => ValueTask.FromResult<long?>(null);
        public ValueTask<bool> AmILeaderQuick(int partitionId) => ValueTask.FromResult(false);
        public ValueTask<bool> AmILeader(int partitionId, CancellationToken cancellationToken) => ValueTask.FromResult(false);
        public ValueTask<string> WaitForLeader(int partitionId, CancellationToken cancellationToken) => ValueTask.FromResult(string.Empty);
        public ValueTask<string> WaitForLeaderStableAsync(int partitionId, TimeSpan minStableFor, CancellationToken cancellationToken = default) => ValueTask.FromResult(string.Empty);
        public Task UpdateNodes() => Task.CompletedTask;
        public IList<RaftNode> GetNodes() => Array.Empty<RaftNode>();
        public HLCTimestamp GetLastNodeActivity(string endpoint) => HLCTimestamp.Zero;
        public IReadOnlyList<string> GetActiveNodes(TimeSpan within) => Array.Empty<string>();
        public Task Handshake(HandshakeRequest request) => Task.CompletedTask;
        public void RequestVote(RequestVotesRequest request) { }
        public void Vote(VoteRequest request) { }
        public void AppendLogs(AppendLogsRequest request) { }
        public void CompleteAppendLogs(CompleteAppendLogsRequest request) { }
        public void SetMinRetainIndex(int partitionId, long index) { }
        public int GetLocalNodeId() => 99;
        public string GetLocalNodeName() => "stub";
        public void RegisterStateMachineTransfer(IRaftStateMachineTransfer? transfer) { }

        public void RegisterSystemStateTransfer(IRaftSystemStateTransfer? transfer) { }
        public Task<RaftReplicationResult> ReplicateLogs(int partitionId, string type, byte[] data, bool autoCommit = true, long expectedGeneration = 0, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<RaftReplicationResult> ReplicateLogs(int partitionId, string type, IEnumerable<byte[]> logs, bool autoCommit = true, long expectedGeneration = 0, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<RaftBatchReplicationResult> ReplicateEntries(int partitionId, IReadOnlyList<RaftProposalEntry> entries, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<RaftReplicationResult> ReplicateCheckpoint(int partitionId, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<(bool success, RaftOperationStatus status, long commitLogId)> CommitLogs(int partitionId, HLCTimestamp ticketId, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<(bool success, RaftOperationStatus status, long commitLogId)> RollbackLogs(int partitionId, HLCTimestamp ticketId, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<RaftOperationStatus> ForceLeaderForTestingAsync(int partitionId, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<RaftOperationStatus> StepDownAsync(int partitionId, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<RaftOperationStatus> TransferLeadershipAsync(int partitionId, string targetEndpoint, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<RaftOperationStatus> SuspendHeartbeatsAsync(int partitionId, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<RaftOperationStatus> ResumeHeartbeatsAsync(int partitionId, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<RaftPartitionLifecycleResult> CreatePartitionAsync(int partitionId, RaftRoutingMode mode = RaftRoutingMode.Unrouted, (int start, int end)? hashRange = null, CancellationToken ct = default) => throw new NotImplementedException();
        public Task<RaftPartitionLifecycleResult> RemovePartitionAsync(int partitionId, CancellationToken ct = default) => throw new NotImplementedException();
        public Task<RaftPartitionLifecycleResult> SplitPartitionAsync(int sourcePartitionId, int targetPartitionId = 0, RaftSplitPlan? plan = null, CancellationToken ct = default) => throw new NotImplementedException();
        public Task<RaftPartitionLifecycleResult> MergePartitionsAsync(int survivorPartitionId, int sourcePartitionId, RaftMergePlan? plan = null, CancellationToken ct = default) => throw new NotImplementedException();
        public double GetPartitionLogOpsPerSecond(int partitionId) => 0;
        public int GetPartitionWalQueueDepth(int partitionId) => 0;
        public double GetPartitionCommitWaitMs(int partitionId) => 0;
    }
}
