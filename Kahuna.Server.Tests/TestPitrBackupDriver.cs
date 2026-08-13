
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
/// Unit tests for <see cref="BackupDriver"/>: full and incremental backup over a synthetic
/// WAL and an in-memory persistence backend.  Uses the internal static entry points so tests
/// do not require a live <see cref="IRaft"/> instance.
/// </summary>
public sealed class TestPitrBackupDriver : IDisposable
{
    private readonly string _tempRoot =
        Path.Combine(Path.GetTempPath(), "kahuna_test_" + Guid.NewGuid().ToString("N"));

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

    private static InMemoryWAL BuildWal(params (int partition, long id, long ticks, RaftLogType type)[] entries)
    {
        InMemoryWAL wal = new(Log);
        Dictionary<int, List<RaftLog>> byPartition = [];

        foreach ((int partition, long id, long ticks, RaftLogType type) in entries)
        {
            if (!byPartition.TryGetValue(partition, out List<RaftLog>? list))
                byPartition[partition] = list = [];

            list.Add(new RaftLog { Id = id, Type = type, Time = new HLCTimestamp(0, ticks, 0) });
        }

        foreach ((int partition, List<RaftLog> logs) in byPartition)
            wal.Write([(partition, logs)]);

        return wal;
    }

    private static RaftPartitionRange Part(int id) => new()
    {
        PartitionId = id,
        State = RaftPartitionState.Active
    };

    // Minimal helper: store one key in a MemoryPersistenceBackend.
    private static void Put(MemoryPersistenceBackend b, string key, byte[] value, long rev) =>
        b.StoreKeyValues([new(key, value, rev, 0, 0, 0, 0, rev, 0, 0, rev, 0, 1)]);

    // ── Full backup: structural correctness ────────────────────────────────────────────────

    [Fact]
    public async Task TakeFullBackup_EmptyWal_ManifestHasNoPartitionRanges()
    {
        InMemoryWAL wal = new(Log);
        BackupCatalog catalog = NewCatalog("full_empty");
        string artifacts = ArtifactsDir("full_empty");

        BackupManifest manifest = await BackupDriver.RunFullAsync(wal, [Part(1)], new MemoryPersistenceBackend(), BackupTestStores.Artifacts(artifacts), catalog, ct: TestContext.Current.CancellationToken);

        Assert.Equal(BackupType.Full, manifest.Type);
        Assert.Empty(manifest.PartitionRanges);
    }

    [Fact]
    public async Task TakeFullBackup_CommittedEntries_ManifestCoversRange()
    {
        InMemoryWAL wal = BuildWal(
            (1, 1, 100, RaftLogType.Committed),
            (1, 2, 200, RaftLogType.Committed),
            (1, 3, 300, RaftLogType.Committed));

        BackupCatalog catalog = NewCatalog("full_range");
        string artifacts = ArtifactsDir("full_range");

        BackupManifest manifest = await BackupDriver.RunFullAsync(wal, [Part(1)], new MemoryPersistenceBackend(), BackupTestStores.Artifacts(artifacts), catalog, ct: TestContext.Current.CancellationToken);

        Assert.Single(manifest.PartitionRanges);
        PartitionBackupRange r = manifest.PartitionRanges[0];
        Assert.Equal(1, r.PartitionId);
        Assert.Equal(1L, r.FromIndex);
        Assert.Equal(3L, r.ToIndex);
        Assert.Equal(new HLCTimestamp(0, 300, 0), r.ToHlc);
    }

    [Fact]
    public async Task TakeFullBackup_MixedEntries_OnlyCommittedInRange()
    {
        InMemoryWAL wal = BuildWal(
            (1, 1, 100, RaftLogType.Committed),
            (1, 2, 200, RaftLogType.Proposed),   // uncommitted — must not advance ToIndex
            (1, 3, 300, RaftLogType.Committed));

        BackupCatalog catalog = NewCatalog("full_mixed");
        string artifacts = ArtifactsDir("full_mixed");

        BackupManifest manifest = await BackupDriver.RunFullAsync(wal, [Part(1)], new MemoryPersistenceBackend(), BackupTestStores.Artifacts(artifacts), catalog, ct: TestContext.Current.CancellationToken);

        Assert.Equal(3L, manifest.PartitionRanges[0].ToIndex);
        Assert.Equal(new HLCTimestamp(0, 300, 0), manifest.PartitionRanges[0].ToHlc);
    }

    [Fact]
    public async Task TakeFullBackup_CheckpointDirCreatedOnDisk()
    {
        InMemoryWAL wal = BuildWal((1, 1, 100, RaftLogType.Committed));
        BackupCatalog catalog = NewCatalog("full_disk");
        string artifacts = ArtifactsDir("full_disk");

        BackupManifest manifest = await BackupDriver.RunFullAsync(wal, [Part(1)], new MemoryPersistenceBackend(), BackupTestStores.Artifacts(artifacts), catalog, ct: TestContext.Current.CancellationToken);

        string checkpointDir = Path.Combine(artifacts, manifest.BackupId.ToString("N"), "checkpoint");
        Assert.True(Directory.Exists(checkpointDir));
        Assert.True(File.Exists(Path.Combine(checkpointDir, CheckpointManifest.FileName)));
    }

    [Fact]
    public async Task TakeFullBackup_ManifestStoredInCatalog()
    {
        InMemoryWAL wal = BuildWal((1, 1, 100, RaftLogType.Committed));
        BackupCatalog catalog = NewCatalog("full_catalog");
        string artifacts = ArtifactsDir("full_catalog");

        BackupManifest manifest = await BackupDriver.RunFullAsync(wal, [Part(1)], new MemoryPersistenceBackend(), BackupTestStores.Artifacts(artifacts), catalog, ct: TestContext.Current.CancellationToken);

        BackupManifest? stored = await catalog.GetAsync(manifest.BackupId, TestContext.Current.CancellationToken);
        Assert.NotNull(stored);
        Assert.Equal(manifest.BackupId, stored!.BackupId);
    }

    [Fact]
    public async Task TakeFullBackup_ChecksumRecordedForCheckpointManifest()
    {
        InMemoryWAL wal = BuildWal((1, 1, 100, RaftLogType.Committed));
        BackupCatalog catalog = NewCatalog("full_checksum");
        string artifacts = ArtifactsDir("full_checksum");

        BackupManifest manifest = await BackupDriver.RunFullAsync(wal, [Part(1)], new MemoryPersistenceBackend(), BackupTestStores.Artifacts(artifacts), catalog, ct: TestContext.Current.CancellationToken);

        string key = "checkpoint/" + CheckpointManifest.FileName;
        Assert.True(manifest.Checksums.ContainsKey(key));
        Assert.NotEmpty(manifest.Checksums[key]);
    }

    [Fact]
    public async Task TakeFullBackup_DrainedPartitionsSkipped()
    {
        InMemoryWAL wal = BuildWal(
            (1, 1, 100, RaftLogType.Committed),
            (2, 1, 100, RaftLogType.Committed));

        RaftPartitionRange draining = new() { PartitionId = 2, State = RaftPartitionState.Draining };
        BackupCatalog catalog = NewCatalog("full_drained");
        string artifacts = ArtifactsDir("full_drained");

        BackupManifest manifest = await BackupDriver.RunFullAsync(wal, [Part(1), draining], new MemoryPersistenceBackend(), BackupTestStores.Artifacts(artifacts), catalog, ct: TestContext.Current.CancellationToken);

        Assert.Single(manifest.PartitionRanges);
        Assert.Equal(1, manifest.PartitionRanges[0].PartitionId);
    }

    [Fact]
    public async Task TakeFullBackup_CommittedBelowProposedTail_StillFound()
    {
        // The last committed entry is at index 1; indices 2..300 are all Proposed (in-flight).
        // A single-page backward scan (PageSize = 256) would only see [45..300] and miss index 1.
        InMemoryWAL wal = new(Log);
        List<RaftLog> logs = [new RaftLog { Id = 1, Type = RaftLogType.Committed, Time = new HLCTimestamp(0, 100, 0) }];
        for (long i = 2; i <= 300; i++)
            logs.Add(new RaftLog { Id = i, Type = RaftLogType.Proposed, Time = new HLCTimestamp(0, i * 100, 0) });
        wal.Write([(1, logs)]);

        BackupCatalog catalog = NewCatalog("buried_committed");
        string artifacts = ArtifactsDir("buried_committed");

        BackupManifest manifest = await BackupDriver.RunFullAsync(wal, [Part(1)], new MemoryPersistenceBackend(), BackupTestStores.Artifacts(artifacts), catalog, ct: TestContext.Current.CancellationToken);

        // Partition must not be dropped; ToIndex must be the committed entry at index 1.
        Assert.Single(manifest.PartitionRanges);
        Assert.Equal(1L, manifest.PartitionRanges[0].ToIndex);
        Assert.Equal(new HLCTimestamp(0, 100, 0), manifest.PartitionRanges[0].ToHlc);
    }

    // ── Full backup: flush-before-checkpoint contract ──────────────────────────────────────

    [Fact]
    public async Task TakeFullBackup_FlushCalledBeforeCheckpoint()
    {
        // Verify the flush delegate fires and that it fires BEFORE CreateCheckpoint
        // (evidenced by the flushed data appearing in the restored checkpoint).
        MemoryPersistenceBackend backend = new();
        InMemoryWAL wal = BuildWal((1, 1, 100, RaftLogType.Committed));
        BackupCatalog catalog = NewCatalog("flush_order");
        string artifacts = ArtifactsDir("flush_order");

        bool flushCalled = false;

        Task Flush()
        {
            flushCalled = true;
            Put(backend, "k1", Encoding.UTF8.GetBytes("v1"), 1);
            return Task.CompletedTask;
        }

        BackupManifest manifest = await BackupDriver.RunFullAsync(wal, [Part(1)], backend, BackupTestStores.Artifacts(artifacts), catalog, Flush, ct: TestContext.Current.CancellationToken);

        Assert.True(flushCalled, "flush delegate must be called");

        // The data written during Flush must appear in the checkpoint.
        string checkpointPath = Path.Combine(artifacts, manifest.BackupId.ToString("N"), "checkpoint");
        MemoryPersistenceBackend restored = MemoryPersistenceBackend.OpenCheckpoint(checkpointPath);
        Assert.NotNull(restored.GetKeyValue("k1"));
    }

    [Fact]
    public async Task TakeFullBackup_NoFlush_DataWrittenAfterLastFlushMissingFromCheckpoint()
    {
        // Demonstrates the gap when no flush delegate is provided: data that exists in the
        // WAL (committed) but was never written to the backend is absent from the checkpoint.
        MemoryPersistenceBackend backend = new();
        InMemoryWAL wal = BuildWal((1, 1, 100, RaftLogType.Committed));
        BackupCatalog catalog = NewCatalog("no_flush");
        string artifacts = ArtifactsDir("no_flush");

        // Do NOT populate the backend — simulates unflushed in-memory state.
        BackupManifest manifest = await BackupDriver.RunFullAsync(wal, [Part(1)], backend, BackupTestStores.Artifacts(artifacts), catalog, flushBeforeCheckpoint: null, ct: TestContext.Current.CancellationToken);

        // Manifest claims ToIndex = 1, but the checkpoint is empty.
        Assert.Equal(1L, manifest.PartitionRanges[0].ToIndex);

        string checkpointPath = Path.Combine(artifacts, manifest.BackupId.ToString("N"), "checkpoint");
        MemoryPersistenceBackend restored = MemoryPersistenceBackend.OpenCheckpoint(checkpointPath);
        // Empty backend → key not present in checkpoint.
        Assert.Null(restored.GetKeyValue("k1"));
    }

    // ── Incremental backup ─────────────────────────────────────────────────────────────────

    [Fact]
    public async Task TakeIncrementalBackup_AfterFull_ManifestContiguous()
    {
        InMemoryWAL wal = BuildWal(
            (1, 1, 100, RaftLogType.Committed),
            (1, 2, 200, RaftLogType.Committed));

        BackupCatalog catalog = NewCatalog("inc_contiguous");
        string artifacts = ArtifactsDir("inc_contiguous");

        BackupManifest full = await BackupDriver.RunFullAsync(wal, [Part(1)], new MemoryPersistenceBackend(), BackupTestStores.Artifacts(artifacts), catalog, ct: TestContext.Current.CancellationToken);

        wal.Write([(1, [
            new RaftLog { Id = 3, Type = RaftLogType.Committed, Time = new HLCTimestamp(0, 300, 0) },
            new RaftLog { Id = 4, Type = RaftLogType.Committed, Time = new HLCTimestamp(0, 400, 0) }
        ])]);

        BackupManifest inc = await BackupDriver.RunIncrementalAsync(wal, [Part(1)], full.BackupId, BackupTestStores.Artifacts(artifacts), catalog, ct: TestContext.Current.CancellationToken);

        Assert.Equal(BackupType.Incremental, inc.Type);
        Assert.Equal(full.BackupId, inc.ParentBackupId);
        PartitionBackupRange r = inc.PartitionRanges[0];
        Assert.Equal(3L, r.FromIndex);
        Assert.Equal(4L, r.ToIndex);
    }

    [Fact]
    public async Task TakeIncrementalBackup_NoNewEntries_EmptyRanges()
    {
        InMemoryWAL wal = BuildWal((1, 1, 100, RaftLogType.Committed));
        BackupCatalog catalog = NewCatalog("inc_empty");
        string artifacts = ArtifactsDir("inc_empty");

        BackupManifest full = await BackupDriver.RunFullAsync(wal, [Part(1)], new MemoryPersistenceBackend(), BackupTestStores.Artifacts(artifacts), catalog, ct: TestContext.Current.CancellationToken);

        BackupManifest inc = await BackupDriver.RunIncrementalAsync(wal, [Part(1)], full.BackupId, BackupTestStores.Artifacts(artifacts), catalog, ct: TestContext.Current.CancellationToken);

        Assert.Empty(inc.PartitionRanges);
    }

    [Fact]
    public async Task TakeIncrementalBackup_MissingParent_Throws()
    {
        InMemoryWAL wal = BuildWal((1, 1, 100, RaftLogType.Committed));
        BackupCatalog catalog = NewCatalog("inc_missing");
        string artifacts = ArtifactsDir("inc_missing");

        BackupDriverException ex = await Assert.ThrowsAsync<BackupDriverException>(async () =>
            await BackupDriver.RunIncrementalAsync(wal, [Part(1)], Guid.NewGuid(), BackupTestStores.Artifacts(artifacts), catalog, ct: TestContext.Current.CancellationToken));

        Assert.Contains("not found", ex.Message);
    }

    [Fact]
    public async Task TakeIncrementalBackup_NewPartitionAbsentFromParent_StartsAtFloor()
    {
        // Partition 2 was not in the full backup (didn't exist yet). Its WAL floor is 5.
        // The incremental should start at 5, not 1, and must not throw.
        InMemoryWAL inner = BuildWal(
            (1, 1, 100, RaftLogType.Committed),
            (2, 5, 500, RaftLogType.Committed),
            (2, 6, 600, RaftLogType.Committed));
        WalWithFloor wal = new(inner, partitionId: 2, floor: 5);

        BackupCatalog catalog = NewCatalog("inc_new_partition");
        string artifacts = ArtifactsDir("inc_new_partition");

        // Full backup covers only partition 1.
        BackupManifest full = await BackupDriver.RunFullAsync(wal, [Part(1)], new MemoryPersistenceBackend(), BackupTestStores.Artifacts(artifacts), catalog, ct: TestContext.Current.CancellationToken);

        // Incremental includes partition 2 (newly joined).
        BackupManifest inc = await BackupDriver.RunIncrementalAsync(wal, [Part(1), Part(2)], full.BackupId, BackupTestStores.Artifacts(artifacts), catalog, ct: TestContext.Current.CancellationToken);

        PartitionBackupRange? p2 = inc.PartitionRanges.FirstOrDefault(r => r.PartitionId == 2);
        Assert.NotNull(p2);
        Assert.Equal(5L, p2!.FromIndex);
        Assert.Equal(6L, p2.ToIndex);
    }

    [Fact]
    public async Task TakeIncrementalBackup_WalFloorExceeded_Throws()
    {
        // Parent covers up to index 5; WAL floor is at index 10.
        InMemoryWAL inner = BuildWal((1, 1, 100, RaftLogType.Committed));
        WalWithFloor wal = new(inner, partitionId: 1, floor: 10);

        BackupCatalog catalog = NewCatalog("inc_floor");
        string artifacts = ArtifactsDir("inc_floor");

        BackupManifest parentManifest = BackupManifest.CreateFull(
            [PartitionBackupRange.Create(1, 1, default, 5, new HLCTimestamp(0, 500, 0))]);
        await catalog.PutAsync(parentManifest, TestContext.Current.CancellationToken);

        BackupDriverException ex = await Assert.ThrowsAsync<BackupDriverException>(async () =>
            await BackupDriver.RunIncrementalAsync(wal, [Part(1)], parentManifest.BackupId, BackupTestStores.Artifacts(artifacts), catalog, ct: TestContext.Current.CancellationToken));

        Assert.Contains("compaction floor", ex.Message);
    }

    [Fact]
    public async Task TakeIncrementalBackup_WalFilesWrittenToDisk()
    {
        InMemoryWAL wal = BuildWal((1, 1, 100, RaftLogType.Committed));
        BackupCatalog catalog = NewCatalog("inc_files");
        string artifacts = ArtifactsDir("inc_files");

        BackupManifest full = await BackupDriver.RunFullAsync(wal, [Part(1)], new MemoryPersistenceBackend(), BackupTestStores.Artifacts(artifacts), catalog, ct: TestContext.Current.CancellationToken);

        wal.Write([(1, [new RaftLog { Id = 2, Type = RaftLogType.Committed, Time = new HLCTimestamp(0, 200, 0) }])]);

        BackupManifest inc = await BackupDriver.RunIncrementalAsync(wal, [Part(1)], full.BackupId, BackupTestStores.Artifacts(artifacts), catalog, ct: TestContext.Current.CancellationToken);

        string walFile = Path.Combine(artifacts, inc.BackupId.ToString("N"), "partition_1.wal");
        Assert.True(File.Exists(walFile));
        Assert.True(inc.Checksums.ContainsKey("partition_1.wal"));
    }

    [Fact]
    public async Task FullThenIncremental_ChainValidates()
    {
        InMemoryWAL wal = BuildWal(
            (1, 1, 100, RaftLogType.Committed),
            (1, 2, 200, RaftLogType.Committed));

        BackupCatalog catalog = NewCatalog("chain");
        string artifacts = ArtifactsDir("chain");

        BackupManifest full = await BackupDriver.RunFullAsync(wal, [Part(1)], new MemoryPersistenceBackend(), BackupTestStores.Artifacts(artifacts), catalog, ct: TestContext.Current.CancellationToken);

        wal.Write([(1, [new RaftLog { Id = 3, Type = RaftLogType.Committed, Time = new HLCTimestamp(0, 300, 0) }])]);

        BackupManifest inc = await BackupDriver.RunIncrementalAsync(wal, [Part(1)], full.BackupId, BackupTestStores.Artifacts(artifacts), catalog, ct: TestContext.Current.CancellationToken);

        IReadOnlyList<BackupManifest> chain = await catalog.ResolveAndValidateAsync(inc.BackupId, TestContext.Current.CancellationToken);

        Assert.Equal(2, chain.Count);
        Assert.Equal(BackupType.Full, chain[0].Type);
        Assert.Equal(BackupType.Incremental, chain[1].Type);
    }

    // ── sparse-incremental high-water marks ─────────────────────────────────────────────────

    [Fact]
    public async Task Incremental_SparseParent_ContinuesFromTransitiveHighWater()
    {
        // Full covers P1 through index 2. An empty incremental omits P1 (no new entries). A later
        // incremental must continue from the FULL's high-water (index 3), not restart at 1.
        InMemoryWAL wal = BuildWal(
            (1, 1, 100, RaftLogType.Committed),
            (1, 2, 200, RaftLogType.Committed));
        BackupCatalog catalog = NewCatalog("sparse");
        string artifacts = ArtifactsDir("sparse");

        BackupManifest full = await BackupDriver.RunFullAsync(wal, [Part(1)], new MemoryPersistenceBackend(), BackupTestStores.Artifacts(artifacts), catalog, ct: TestContext.Current.CancellationToken);

        // No new WAL entries → empty incremental, P1 omitted.
        BackupManifest inc1 = await BackupDriver.RunIncrementalAsync(wal, [Part(1)], full.BackupId, BackupTestStores.Artifacts(artifacts), catalog, ct: TestContext.Current.CancellationToken);
        Assert.Empty(inc1.PartitionRanges);

        // A new entry appears after inc1.
        wal.Write([(1, [new RaftLog { Id = 3, Type = RaftLogType.Committed, Time = new HLCTimestamp(0, 300, 0) }])]);

        BackupManifest inc2 = await BackupDriver.RunIncrementalAsync(wal, [Part(1)], inc1.BackupId, BackupTestStores.Artifacts(artifacts), catalog, ct: TestContext.Current.CancellationToken);

        PartitionBackupRange? p1 = inc2.PartitionRanges.FirstOrDefault(r => r.PartitionId == 1);
        Assert.NotNull(p1);
        Assert.Equal(3L, p1!.FromIndex); // transitive: full.ToIndex(2)+1 — NOT a restart at 1
        Assert.Equal(3L, p1.ToIndex);
    }

    [Fact]
    public void Validate_HiddenGapAcrossSparseIncremental_Throws()
    {
        BackupManifest full = BackupManifest.CreateFull(
            [PartitionBackupRange.Create(1, 1, default, 2, new HLCTimestamp(0, 200, 0))]);
        BackupManifest inc1 = BackupManifest.CreateIncremental(full.BackupId, []); // sparse: omits P1
        BackupManifest inc2 = BackupManifest.CreateIncremental(inc1.BackupId,
            [PartitionBackupRange.Create(1, 1, new HLCTimestamp(0, 100, 0), 5, new HLCTimestamp(0, 500, 0))]);

        // inc2 restarts P1 at index 1 while the transitive high-water is 2 → hidden gap, rejected.
        Assert.Throws<BackupChainException>(() => BackupCatalog.Validate([full, inc1, inc2]));
    }

    [Fact]
    public void Validate_TransitiveContiguityAcrossSparseIncremental_Accepts()
    {
        BackupManifest full = BackupManifest.CreateFull(
            [PartitionBackupRange.Create(1, 1, default, 2, new HLCTimestamp(0, 200, 0))]);
        BackupManifest inc1 = BackupManifest.CreateIncremental(full.BackupId, []); // sparse: omits P1
        BackupManifest inc2 = BackupManifest.CreateIncremental(inc1.BackupId,
            [PartitionBackupRange.Create(1, 3, new HLCTimestamp(0, 300, 0), 5, new HLCTimestamp(0, 500, 0))]);

        // inc2 continues from the full's high-water (2)+1 = 3 across the sparse link → accepted.
        BackupCatalog.Validate([full, inc1, inc2]);
    }

    // ── topology-generation capture / re-check ──────────────────────────────────────────────

    private static long Topo(IReadOnlyList<RaftPartitionRange> parts, Dictionary<int, long> gens, long membership) =>
        BackupDriver.ComposeTopologyGeneration(parts, id => gens.GetValueOrDefault(id), membership);

    [Fact]
    public void ComposeTopologyGeneration_SameInputs_Deterministic()
    {
        Dictionary<int, long> gens = new() { [1] = 5, [2] = 3 };
        Assert.Equal(Topo([Part(1), Part(2)], gens, 7), Topo([Part(1), Part(2)], gens, 7));
    }

    [Fact]
    public void ComposeTopologyGeneration_OrderIndependent()
    {
        Dictionary<int, long> gens = new() { [1] = 5, [2] = 3 };
        Assert.Equal(Topo([Part(1), Part(2)], gens, 7), Topo([Part(2), Part(1)], gens, 7));
    }

    [Fact]
    public void ComposeTopologyGeneration_ChangesOnPartitionGenerationBump()
    {
        long before = Topo([Part(1), Part(2)], new() { [1] = 5, [2] = 3 }, 7);
        long after = Topo([Part(1), Part(2)], new() { [1] = 6, [2] = 3 }, 7); // P1 split/merge
        Assert.NotEqual(before, after);
    }

    [Fact]
    public void ComposeTopologyGeneration_ChangesWhenPartitionAdded()
    {
        Dictionary<int, long> gens = new() { [1] = 5, [2] = 3 };
        Assert.NotEqual(Topo([Part(1)], gens, 7), Topo([Part(1), Part(2)], gens, 7));
    }

    [Fact]
    public void ComposeTopologyGeneration_ChangesOnMembershipVersion()
    {
        Dictionary<int, long> gens = new() { [1] = 5 };
        Assert.NotEqual(Topo([Part(1)], gens, 7), Topo([Part(1)], gens, 8));
    }

    [Fact]
    public async Task TakeFullBackup_StableTopology_StampsGenerationAndPublishes()
    {
        InMemoryWAL wal = BuildWal((1, 1, 100, RaftLogType.Committed));
        BackupCatalog catalog = NewCatalog("topo_stable");
        string artifacts = ArtifactsDir("topo_stable");

        BackupManifest manifest = await BackupDriver.RunFullAsync(
            wal, [Part(1)], new MemoryPersistenceBackend(), BackupTestStores.Artifacts(artifacts), catalog,
            topologyGenerationProbe: () => 12345L, ct: TestContext.Current.CancellationToken);

        Assert.Equal(12345L, manifest.TopologyGeneration);
        Assert.NotNull(await catalog.GetAsync(manifest.BackupId, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task TakeFullBackup_TopologyChangesMidBackup_AbortsAndPublishesNothing()
    {
        InMemoryWAL wal = BuildWal((1, 1, 100, RaftLogType.Committed));
        BackupCatalog catalog = NewCatalog("topo_moved");
        string artifacts = ArtifactsDir("topo_moved");

        // Probe returns one value at capture and a different value at the pre-publish re-check.
        int calls = 0;
        long Probe() => calls++ == 0 ? 111L : 222L;

        BackupDriverException ex = await Assert.ThrowsAsync<BackupDriverException>(() =>
            BackupDriver.RunFullAsync(wal, [Part(1)], new MemoryPersistenceBackend(), BackupTestStores.Artifacts(artifacts), catalog,
                topologyGenerationProbe: Probe, ct: TestContext.Current.CancellationToken));

        Assert.True(ex.TopologyChanged);
        // Nothing published and the half-written artifact directory was swept.
        Assert.Empty(await catalog.ListAsync(TestContext.Current.CancellationToken));
        Assert.True(!Directory.Exists(artifacts) || Directory.GetDirectories(artifacts).Length == 0);
    }

    [Fact]
    public async Task TakeIncrementalBackup_TopologyChangesMidBackup_AbortsAndPublishesNothing()
    {
        InMemoryWAL wal = BuildWal((1, 1, 100, RaftLogType.Committed));
        BackupCatalog catalog = NewCatalog("topo_moved_inc");
        string artifacts = ArtifactsDir("topo_moved_inc");

        // Full backup at a stable topology.
        BackupManifest full = await BackupDriver.RunFullAsync(
            wal, [Part(1)], new MemoryPersistenceBackend(), BackupTestStores.Artifacts(artifacts), catalog,
            topologyGenerationProbe: () => 999L, ct: TestContext.Current.CancellationToken);

        wal.Write([(1, [new RaftLog { Id = 2, Type = RaftLogType.Committed, Time = new HLCTimestamp(0, 200, 0) }])]);

        int calls = 0;
        long Probe() => calls++ == 0 ? 111L : 222L;

        BackupDriverException ex = await Assert.ThrowsAsync<BackupDriverException>(async () =>
            await BackupDriver.RunIncrementalAsync(wal, [Part(1)], full.BackupId, BackupTestStores.Artifacts(artifacts), catalog,
                snapshotT: null, acquireRetentionHold: null, identity: default,
                topologyGenerationProbe: Probe, ct: TestContext.Current.CancellationToken));

        Assert.True(ex.TopologyChanged);
        // Only the full remains; the incremental was not published and its directory was swept.
        Assert.Single(await catalog.ListAsync(TestContext.Current.CancellationToken));
        Assert.Single(Directory.GetDirectories(artifacts)); // only the full's artifact dir
    }

    // ── coordinator fence (leadership loss) ──────────────────────────────────────────────────

    [Fact]
    public async Task TakeFullBackup_CoordinatorLostMidBackup_AbortsWithLeadershipLoss()
    {
        InMemoryWAL wal = BuildWal((1, 1, 100, RaftLogType.Committed));
        BackupCatalog catalog = NewCatalog("coord_lost");
        string artifacts = ArtifactsDir("coord_lost");

        BackupDriverException ex = await Assert.ThrowsAsync<BackupDriverException>(() =>
            BackupDriver.RunFullAsync(wal, [Part(1)], new MemoryPersistenceBackend(), BackupTestStores.Artifacts(artifacts), catalog,
                verifyCoordinator: _ => Task.FromResult(false), ct: TestContext.Current.CancellationToken));

        Assert.True(ex.RetryableLeadershipLoss);
        // Nothing published; the half-written artifact directory was swept.
        Assert.Empty(await catalog.ListAsync(TestContext.Current.CancellationToken));
        Assert.True(!Directory.Exists(artifacts) || Directory.GetDirectories(artifacts).Length == 0);
    }

    [Fact]
    public async Task TakeFullBackup_CoordinatorStillValid_Publishes()
    {
        InMemoryWAL wal = BuildWal((1, 1, 100, RaftLogType.Committed));
        BackupCatalog catalog = NewCatalog("coord_ok");
        string artifacts = ArtifactsDir("coord_ok");

        BackupManifest m = await BackupDriver.RunFullAsync(
            wal, [Part(1)], new MemoryPersistenceBackend(), BackupTestStores.Artifacts(artifacts), catalog,
            verifyCoordinator: _ => Task.FromResult(true), ct: TestContext.Current.CancellationToken);

        Assert.NotNull(await catalog.GetAsync(m.BackupId, TestContext.Current.CancellationToken));
    }

    // ── test helpers ────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Wraps an <see cref="InMemoryWAL"/> and substitutes a fixed floor value for one partition
    /// so that the WAL floor check in <see cref="BackupDriver.RunIncremental"/> can be tested.
    /// </summary>
    private sealed class WalWithFloor(InMemoryWAL inner, int partitionId, long floor) : IWAL
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
}
