
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
        string? restoreRoot = null,
        BackupRetentionPolicy retentionPolicy = default)
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
            restoreRoot: restoreRoot,
            acquireRetentionHold: raft.AcquireRetentionHold,
            retentionPolicy: retentionPolicy);
    }

    // Absolute path of the backup directory for a service tag (mirrors BackupDir(tag)).
    private string BackupDirPath(string tag) => BackupDir(tag);

    // ── I/O budget + metrics (part 4) ────────────────────────────────────────────────────────

    [Fact]
    public void ThrottleDelaySeconds_PacesToBudget()
    {
        // Unlimited budget → never wait.
        Assert.Equal(0, BackupService.ThrottleDelaySeconds(1_000_000, 0, 0.0));
        // 1 MB copied instantly against a 1 MB/s budget → wait ~1 s to hit the target rate.
        Assert.Equal(1.0, BackupService.ThrottleDelaySeconds(1_000_000, 1_000_000, 0.0), 3);
        // Already behind the target rate (1.5 s elapsed for 1 MB at 1 MB/s) → no wait.
        Assert.Equal(0, BackupService.ThrottleDelaySeconds(1_000_000, 1_000_000, 1.5));
    }

    [Fact]
    public async Task Backup_EmitsIoMetrics()
    {
        long ops = 0, bytes = 0;
        using System.Diagnostics.Metrics.MeterListener listener = new();
        listener.InstrumentPublished = (inst, l) =>
        {
            if (inst.Meter.Name == "Kahuna" && inst.Name.StartsWith("kahuna.backup.", StringComparison.Ordinal))
                l.EnableMeasurementEvents(inst);
        };
        listener.SetMeasurementEventCallback<long>((inst, val, _, _) =>
        {
            if (inst.Name == "kahuna.backup.operations") Interlocked.Add(ref ops, val);
            else if (inst.Name == "kahuna.backup.bytes") Interlocked.Add(ref bytes, val);
        });
        listener.Start();

        MemoryPersistenceBackend backend = new();
        InMemoryWAL wal = BuildWal((1, 1, 100));
        Put(backend, "k1", Encoding.UTF8.GetBytes("v1"), 1);
        BackupService svc = MakeService("io_metrics", wal, backend);
        await svc.TakeFullAsync();

        listener.Dispose();
        Assert.True(ops >= 1, "expected at least one backup operation counted");
        Assert.True(bytes > 0, "expected non-zero artifact bytes counted");
    }

    // ── chain-aware retention + orphan sweep, driven through the real backup entry points ────

    [Fact]
    public async Task TakeFull_WithMaxChainsRetention_DeletesOlderChainsAndTheirArtifacts()
    {
        MemoryPersistenceBackend backend = new();
        InMemoryWAL wal = BuildWal((1, 1, 100));
        Put(backend, "k1", Encoding.UTF8.GetBytes("v1"), 1);

        BackupService svc = MakeService("ret_chains", wal, backend,
            retentionPolicy: new BackupRetentionPolicy(MaxChains: 1, null, null));

        Kahuna.Shared.Communication.Rest.KahunaBackupInfo first = await svc.TakeFullAsync();
        // A second full backup creates a newer chain; MaxChains:1 retention (run inline) removes the first.
        Kahuna.Shared.Communication.Rest.KahunaBackupInfo second = await svc.TakeFullAsync();

        List<Guid> ids = svc.ListBackups().Select(b => b.BackupId).ToList();
        Assert.DoesNotContain(first.BackupId, ids);
        Assert.Contains(second.BackupId, ids);
        // The deleted backup's artifact directory is gone too.
        Assert.False(Directory.Exists(Path.Combine(BackupDirPath("ret_chains"), first.BackupId.ToString("N"))));
    }

    [Fact]
    public async Task TakeFull_ReclaimsPreexistingOrphanArtifactDirectory()
    {
        MemoryPersistenceBackend backend = new();
        InMemoryWAL wal = BuildWal((1, 1, 100));
        Put(backend, "k1", Encoding.UTF8.GetBytes("v1"), 1);

        BackupService svc = MakeService("ret_orphan", wal, backend);

        // Simulate an artifact directory orphaned by a crashed earlier backup (no manifest).
        string orphan = Path.Combine(BackupDirPath("ret_orphan"), Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(orphan);
        File.WriteAllText(Path.Combine(orphan, "checkpoint"), "leftover");

        await svc.TakeFullAsync(); // GC (orphan sweep) runs inline after a successful backup

        Assert.False(Directory.Exists(orphan));
    }

    [Fact]
    public async Task RestoreRacingGc_ReclaimedChain_FailsClosed_NoPartialTarget()
    {
        // An operator restores a chain that a retention pass reclaims. Deleting is manifest-first, so a
        // restore that races it either completes (chain retained) or fails closed (chain gone) — never
        // reads half-deleted artifacts or leaves a usable partial target.
        MemoryPersistenceBackend backend = new();
        InMemoryWAL wal = BuildWal((1, 1, 100));
        Put(backend, "k1", Encoding.UTF8.GetBytes("v1"), 1);
        BackupService svc = MakeService("restore_race", wal, backend,
            retentionPolicy: new BackupRetentionPolicy(MaxChains: 1, null, null));

        Kahuna.Shared.Communication.Rest.KahunaBackupInfo first = await svc.TakeFullAsync();
        // A second full backup's inline GC applies MaxChains:1 retention, reclaiming `first` entirely.
        Kahuna.Shared.Communication.Rest.KahunaBackupInfo second = await svc.TakeFullAsync();

        // Restoring the reclaimed chain fails closed (its manifest is gone) — a typed backup error.
        string goneTarget = Path.Combine(_tempRoot, "restore_race_gone");
        await Assert.ThrowsAnyAsync<Exception>(() => svc.RestoreToAsync(first.BackupId, goneTarget, HLCTimestamp.Zero));
        // No partial/usable target was produced.
        Assert.False(Directory.Exists(goneTarget) && Directory.GetFileSystemEntries(goneTarget).Length > 0);

        // The retained chain still restores cleanly (the "completes" branch).
        string okTarget = Path.Combine(_tempRoot, "restore_race_ok");
        Kahuna.Shared.Communication.Rest.KahunaRestoreResponse ok =
            await svc.RestoreToAsync(second.BackupId, okTarget, HLCTimestamp.Zero);
        Assert.Equal(okTarget, ok.TargetDir);
        Assert.True(Directory.Exists(okTarget));
    }

    [Fact]
    public async Task RunGarbageCollection_ReclaimsOrphan_WithoutTakingABackup()
    {
        // This is the action the startup sweep and periodic GC tick invoke: reclaim crash-orphaned
        // artifacts even when no backup is being taken.
        MemoryPersistenceBackend backend = new();
        InMemoryWAL wal = BuildWal((1, 1, 100));
        BackupService svc = MakeService("gc_tick", wal, backend);

        string orphan = Path.Combine(BackupDirPath("gc_tick"), Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(orphan);
        File.WriteAllText(Path.Combine(orphan, "checkpoint"), "leftover");

        BackupGcInventory inventory = await svc.RunGarbageCollectionAsync();

        Assert.False(Directory.Exists(orphan));
        Assert.Contains(inventory.OrphanReclamations, o => Path.GetFileName(o.Path) == Path.GetFileName(orphan));
    }

    [Fact]
    public async Task GarbageCollection_CorruptManifest_ProtectsItsArtifactDirectory()
    {
        // A corrupt {id}.manifest drops the backup from the parsed listing, but its id is still recovered
        // from the filename — so its artifact directory must be protected, never swept as an orphan.
        MemoryPersistenceBackend backend = new();
        InMemoryWAL wal = BuildWal((1, 1, 100));
        Put(backend, "k1", Encoding.UTF8.GetBytes("v1"), 1);
        BackupService svc = MakeService("gc_corrupt", wal, backend);

        Kahuna.Shared.Communication.Rest.KahunaBackupInfo full = await svc.TakeFullAsync();
        string root = BackupDirPath("gc_corrupt");
        string manifestPath = Path.Combine(root, full.BackupId.ToString("N") + ".manifest");
        string artifactDir = Path.Combine(root, full.BackupId.ToString("N"));
        Assert.True(File.Exists(manifestPath));
        Assert.True(Directory.Exists(artifactDir));

        // Corrupt the manifest so the parsed catalog listing can no longer see this backup.
        File.WriteAllText(manifestPath, "{ this is not valid json ");

        BackupGcInventory inventory = await svc.RunGarbageCollectionAsync();

        Assert.True(Directory.Exists(artifactDir),
            "artifact directory of a corrupt-manifest backup must survive GC");
        Assert.DoesNotContain(inventory.OrphanReclamations,
            o => Path.GetFileName(o.Path) == full.BackupId.ToString("N"));
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

    // ── listing surfaces valid-JSON structurally-corrupt / unsupported backups ────────────────

    private static void MutateManifest(string backupDir, Guid id, Action<BackupManifest> mutate)
    {
        string path = Path.Combine(backupDir, id.ToString("N") + ".manifest");
        BackupManifest m = System.Text.Json.JsonSerializer.Deserialize<BackupManifest>(File.ReadAllText(path))!;
        mutate(m);
        File.WriteAllText(path, System.Text.Json.JsonSerializer.Serialize(m));
    }

    private static Kahuna.Shared.Communication.Rest.KahunaBackupInfo Entry(
        IReadOnlyList<Kahuna.Shared.Communication.Rest.KahunaBackupInfo> list, Guid id) =>
        list.Single(b => b.BackupId == id);

    [Fact]
    public async Task ListBackups_ValidJsonUnsupportedVersion_MarkedInvalid()
    {
        InMemoryWAL wal = BuildWal((1, 1, 100));
        BackupService svc = MakeService("list_ver", wal, new MemoryPersistenceBackend());
        Kahuna.Shared.Communication.Rest.KahunaBackupInfo full = await svc.TakeFullAsync();

        MutateManifest(BackupDir("list_ver"), full.BackupId, m => m.FormatVersion = BackupManifest.CurrentFormatVersion + 1);

        Kahuna.Shared.Communication.Rest.KahunaBackupInfo e = Entry(svc.ListBackups(), full.BackupId);
        Assert.True(e.IsInvalid);
        Assert.Contains("version", e.InvalidReason!, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task ListBackups_DuplicatePartitionRange_MarkedInvalid()
    {
        InMemoryWAL wal = BuildWal((1, 1, 100));
        BackupService svc = MakeService("list_dup", wal, new MemoryPersistenceBackend());
        Kahuna.Shared.Communication.Rest.KahunaBackupInfo full = await svc.TakeFullAsync();

        MutateManifest(BackupDir("list_dup"), full.BackupId, m => m.PartitionRanges.Add(m.PartitionRanges[0]));

        Kahuna.Shared.Communication.Rest.KahunaBackupInfo e = Entry(svc.ListBackups(), full.BackupId);
        Assert.True(e.IsInvalid);
        Assert.Contains("duplicate", e.InvalidReason!, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task ListBackups_IncompleteSizeMetadata_MarkedInvalid()
    {
        InMemoryWAL wal = BuildWal((1, 1, 100));
        BackupService svc = MakeService("list_size", wal, new MemoryPersistenceBackend());
        Kahuna.Shared.Communication.Rest.KahunaBackupInfo full = await svc.TakeFullAsync();

        MutateManifest(BackupDir("list_size"), full.BackupId, m => m.Sizes.Remove(m.Sizes.Keys.First()));

        Kahuna.Shared.Communication.Rest.KahunaBackupInfo e = Entry(svc.ListBackups(), full.BackupId);
        Assert.True(e.IsInvalid);
        Assert.Contains("size", e.InvalidReason!, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task ListBackups_OneBadEntry_DoesNotFailWholeList()
    {
        InMemoryWAL wal = BuildWal((1, 1, 100));
        BackupService svc = MakeService("list_mixed", wal, new MemoryPersistenceBackend());
        Kahuna.Shared.Communication.Rest.KahunaBackupInfo full = await svc.TakeFullAsync();
        wal.Write([(1, [new RaftLog { Id = 2, Type = RaftLogType.Committed, Time = new HLCTimestamp(0, 200, 0) }])]);
        Kahuna.Shared.Communication.Rest.KahunaBackupInfo inc = await svc.TakeIncrementalAsync(full.BackupId);

        MutateManifest(BackupDir("list_mixed"), full.BackupId, m => m.PartitionRanges.Add(m.PartitionRanges[0]));

        IReadOnlyList<Kahuna.Shared.Communication.Rest.KahunaBackupInfo> all = svc.ListBackups();
        Assert.Equal(2, all.Count); // both entries still present
        Assert.True(Entry(all, full.BackupId).IsInvalid);
        Assert.False(Entry(all, inc.BackupId).IsInvalid);
    }

    [Fact]
    public async Task ListBackups_StructuralOnly_DoesNotHashArtifacts_ButVerifyArtifactsCatchesDigest()
    {
        InMemoryWAL wal = BuildWal((1, 1, 100));
        BackupService svc = MakeService("list_digest", wal, new MemoryPersistenceBackend());
        Kahuna.Shared.Communication.Rest.KahunaBackupInfo full = await svc.TakeFullAsync();

        // Corrupt a checkpoint file's bytes without touching the manifest's recorded size/digest.
        string storeJson = Path.Combine(BackupDir("list_digest"), full.BackupId.ToString("N"), "checkpoint", "store.json");
        File.AppendAllText(storeJson, "corruption");

        // Cheap structural listing does not read artifacts, so the entry is not marked invalid.
        Assert.False(Entry(svc.ListBackups(), full.BackupId).IsInvalid);

        // Full artifact verification catches the byte-level corruption.
        Kahuna.Shared.Communication.Rest.KahunaBackupInfo verified = Entry(svc.ListBackups(verifyArtifacts: true), full.BackupId);
        Assert.True(verified.IsInvalid);
    }

    [Fact]
    public async Task ListBackups_VerifyArtifacts_MissingFile_MarkedInvalid()
    {
        InMemoryWAL wal = BuildWal((1, 1, 100));
        BackupService svc = MakeService("list_missing", wal, new MemoryPersistenceBackend());
        Kahuna.Shared.Communication.Rest.KahunaBackupInfo full = await svc.TakeFullAsync();

        string storeJson = Path.Combine(BackupDir("list_missing"), full.BackupId.ToString("N"), "checkpoint", "store.json");
        File.Delete(storeJson);

        // Structural listing passes (no files read); full verification flags the missing artifact.
        Assert.False(Entry(svc.ListBackups(), full.BackupId).IsInvalid);
        Assert.True(Entry(svc.ListBackups(verifyArtifacts: true), full.BackupId).IsInvalid);
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
            await svc.RestoreToAsync(full.BackupId, targetDir, HLCTimestamp.Zero);

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
            await svc.RestoreToAsync(inc.BackupId, targetDir, HLCTimestamp.Zero);

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
            await svc.RestoreToAsync(inc.BackupId, targetDir, HLCTimestamp.Zero);

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
        await Assert.ThrowsAsync<BackupArtifactException>(() => svc.RestoreToAsync(full.BackupId, targetDir, HLCTimestamp.Zero));
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
            await svc.RestoreToAsync(full.BackupId, targetDir, HLCTimestamp.Zero);

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
        BackupDriverException ex = await Assert.ThrowsAsync<BackupDriverException>(() => svc.RestoreToAsync(full.BackupId, targetDir, new HLCTimestamp(0, 50, 0)));

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
        BackupDriverException ex = await Assert.ThrowsAsync<BackupDriverException>(() => svc.RestoreToAsync(full.BackupId, targetDir, new HLCTimestamp(0, 999, 0)));

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
            await svc.RestoreToAsync(full.BackupId, targetDir, HLCTimestamp.Zero);

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

        BackupDriverException ex = await Assert.ThrowsAsync<BackupDriverException>(() => svc.RestoreToAsync(full.BackupId, targetDir, HLCTimestamp.Zero));
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
        BackupDriverException ex = await Assert.ThrowsAsync<BackupDriverException>(() => svc.RestoreToAsync(full.BackupId, targetDir, HLCTimestamp.Zero));
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
        await svc.RestoreToAsync(full.BackupId, targetDir, HLCTimestamp.Zero);

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
        await Assert.ThrowsAsync<BackupArtifactException>(() => svc.RestoreToAsync(full.BackupId, targetDir, HLCTimestamp.Zero));

        Assert.False(Directory.Exists(targetDir));
        Assert.Empty(Directory.GetDirectories(_tempRoot, "corrupt_restore_out.staging_*"));
    }

    // ── WAL retention hold during incremental capture ────────────────────────────────────────

    [Fact]
    public async Task TakeIncremental_AcquiresRetentionHoldAtFromIndex_AndReleases()
    {
        // Full covers P1 through index 1; an incremental starting at index 2 must hold the retention
        // floor at its fromIndex (via Kommander's composable AcquireRetentionHold) while it pages the
        // WAL, and release it after the manifest is published.
        InMemoryWAL wal = BuildWal((1, 1, 100));
        StubRaft raft = MakeRaft(wal, 1);
        MemoryPersistenceBackend backend = new();
        BackupService svc = MakeService("hold_inc", wal, backend, raft: raft);

        Kahuna.Shared.Communication.Rest.KahunaBackupInfo full = await svc.TakeFullAsync();
        wal.Write([(1, [new RaftLog { Id = 2, Type = RaftLogType.Committed, Time = new HLCTimestamp(0, 200, 0) }])]);

        raft.RetentionHoldsAcquired.Clear();
        raft.RetentionHoldsReleased = 0;

        await svc.TakeIncrementalAsync(full.BackupId);

        Assert.Contains((1, 2L), raft.RetentionHoldsAcquired);      // held at fromIndex = full.ToIndex(1)+1
        Assert.Equal(raft.RetentionHoldsAcquired.Count, raft.RetentionHoldsReleased); // all released
    }

    // ── restore destination confinement ──────────────────────────────────────────────────────

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
        BackupDriverException ex = await Assert.ThrowsAsync<BackupDriverException>(() => svc.RestoreToAsync(full.BackupId, outside, HLCTimestamp.Zero));
        Assert.True(ex.TargetConflict);
        Assert.False(Directory.Exists(outside));
    }

    [Fact]
    public async Task RestoreTo_CaseDifferentSiblingOfRoot_ThrowsTargetConflict_BeforeAnyOutput()
    {
        // Configured root differs from the target only by case. On a case-sensitive volume these are
        // distinct trees, so the target escapes the root; confinement must reject it (fail-closed on
        // every volume). Assert the rejection happens before any staging/output directory is created.
        string root = Path.Combine(_tempRoot, "CaseSensRoot");
        Directory.CreateDirectory(root);

        MemoryPersistenceBackend backend = new();
        InMemoryWAL wal = BuildWal((1, 1, 100));
        Put(backend, "k1", Encoding.UTF8.GetBytes("v1"), 1);
        BackupService svc = MakeService("confine_case", wal, backend, restoreRoot: root);
        Kahuna.Shared.Communication.Rest.KahunaBackupInfo full = await svc.TakeFullAsync();

        string caseDifferentTarget = Path.Combine(_tempRoot, "casesensroot", "out");
        BackupDriverException ex = await Assert.ThrowsAsync<BackupDriverException>(() => svc.RestoreToAsync(full.BackupId, caseDifferentTarget, HLCTimestamp.Zero));
        Assert.True(ex.TargetConflict);

        Assert.False(Directory.Exists(caseDifferentTarget));
        // No staging sibling of the rejected target was created either.
        string? parent = Path.GetDirectoryName(Path.GetFullPath(caseDifferentTarget));
        if (parent is not null && Directory.Exists(parent))
            Assert.DoesNotContain(Directory.GetDirectories(parent), d => Path.GetFileName(d).StartsWith("out.staging_"));
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
            await svc.RestoreToAsync(full.BackupId, target, HLCTimestamp.Zero);
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
        BackupDriverException ex = await Assert.ThrowsAsync<BackupDriverException>(() => svc.RestoreToAsync(full.BackupId, target, HLCTimestamp.Zero));
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
        public readonly Dictionary<int, long> MinRetainIndexByPartition = [];
        public void SetMinRetainIndex(int partitionId, long index) => MinRetainIndexByPartition[partitionId] = index;

        public readonly List<(int partitionId, long index)> RetentionHoldsAcquired = [];
        public int RetentionHoldsReleased;
        public IDisposable AcquireRetentionHold(int partitionId, long index)
        {
            RetentionHoldsAcquired.Add((partitionId, index));
            return new ReleaseRecorder(this);
        }

        private sealed class ReleaseRecorder(StubRaft owner) : IDisposable
        {
            private int _disposed;
            public void Dispose()
            {
                if (Interlocked.Exchange(ref _disposed, 1) == 0)
                    owner.RetentionHoldsReleased++;
            }
        }
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
