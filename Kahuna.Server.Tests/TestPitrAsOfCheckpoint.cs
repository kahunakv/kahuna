
using System.Text;
using Kahuna.Server.KeyValues;
using Kahuna.Server.Locks.Data;
using Kahuna.Server.Persistence;
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
/// Tests that a Full backup's checkpoint is cut as-of a single HLC: no committed state newer than
/// the cut leaks into the base image. Exercises the memory backend (exact as-of) end to end through
/// <see cref="BackupDriver.RunFullAsync"/> and the manifest base-cut record.
/// </summary>
public sealed class TestPitrAsOfCheckpoint : IDisposable
{
    private static readonly ILogger<IRaft> Log = NullLogger<IRaft>.Instance;

    private readonly string _tempRoot =
        Path.Combine(Path.GetTempPath(), "kahuna_asof_" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        if (Directory.Exists(_tempRoot))
            Directory.Delete(_tempRoot, recursive: true);
    }

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

    /// <summary>Stores <paramref name="key"/> at the given revision with LastModified = (0, physicalMs, 0).</summary>
    private static void PutAt(MemoryPersistenceBackend b, string key, string value, long revision, long physicalMs,
        KeyValueState state = KeyValueState.Set) =>
        b.StoreKeyValues([new(
            key, Encoding.UTF8.GetBytes(value), revision,
            0, 0, 0,
            0, physicalMs, 0,
            0, physicalMs, 0,
            (int)state)]);

    private static string CheckpointPath(string artifacts, BackupManifest m) =>
        Path.Combine(artifacts, m.BackupId.ToString("N"), "checkpoint");

    [Fact]
    public void MemoryBackend_AdvertisesExactAsOfSupport()
    {
        Assert.True(new MemoryPersistenceBackend().SupportsExactAsOfCheckpoint);
    }

    [Fact]
    public async Task RunFull_ExcludesStateNewerThanCut()
    {
        // WAL max committed HLC = (0,100,0) → the checkpoint cut.
        InMemoryWAL wal = BuildWal((1, 1, 100));
        BackupCatalog catalog = NewCatalog("asof");
        string artifacts = ArtifactsDir("asof");

        MemoryPersistenceBackend backend = new();
        PutAt(backend, "before", "v", 1, physicalMs: 50);   // ≤ cut → kept
        PutAt(backend, "atcut", "v", 1, physicalMs: 100);   // == cut → kept
        PutAt(backend, "future", "v", 1, physicalMs: 200);  // > cut  → excluded

        BackupManifest full = await BackupDriver.RunFullAsync(wal, [Part(1)], backend, artifacts, catalog);

        Assert.Equal(new HLCTimestamp(0, 100, 0), full.BaseCut);

        MemoryPersistenceBackend image = MemoryPersistenceBackend.OpenCheckpoint(CheckpointPath(artifacts, full));
        Assert.NotNull(image.GetKeyValue("before"));
        Assert.NotNull(image.GetKeyValue("atcut"));
        Assert.Null(image.GetKeyValue("future"));
    }

    [Fact]
    public async Task RunFull_PostCutDeleteDoesNotRemoveKeyFromImage()
    {
        InMemoryWAL wal = BuildWal((1, 1, 100)); // cut = (0,100,0)
        BackupCatalog catalog = NewCatalog("asof_del");
        string artifacts = ArtifactsDir("asof_del");

        MemoryPersistenceBackend backend = new();
        PutAt(backend, "k", "alive", 1, physicalMs: 50);                          // ≤ cut, Set
        PutAt(backend, "k", "gone", 2, physicalMs: 200, state: KeyValueState.Deleted); // > cut, Deleted

        BackupManifest full = await BackupDriver.RunFullAsync(wal, [Part(1)], backend, artifacts, catalog);

        // As-of the cut the key was still alive; the later delete must not be reflected.
        MemoryPersistenceBackend image = MemoryPersistenceBackend.OpenCheckpoint(CheckpointPath(artifacts, full));
        KeyValueEntry? entry = image.GetKeyValue("k");
        Assert.NotNull(entry);
        Assert.Equal(KeyValueState.Set, entry!.State);
    }

    [Fact]
    public async Task RunFull_CoordinatedSnapshotT_CutsAtT()
    {
        InMemoryWAL wal = BuildWal((1, 1, 100), (1, 2, 300));
        BackupCatalog catalog = NewCatalog("asof_coord");
        string artifacts = ArtifactsDir("asof_coord");

        MemoryPersistenceBackend backend = new();
        PutAt(backend, "in", "v", 1, physicalMs: 100);
        PutAt(backend, "out", "v", 1, physicalMs: 250);

        // snapshotT overrides the WAL max as the cut.
        BackupManifest full = await BackupDriver.RunFullAsync(
            wal, [Part(1)], backend, artifacts, catalog, flushBeforeCheckpoint: null,
            snapshotT: new HLCTimestamp(0, 150, 0));

        Assert.Equal(new HLCTimestamp(0, 150, 0), full.BaseCut);

        MemoryPersistenceBackend image = MemoryPersistenceBackend.OpenCheckpoint(CheckpointPath(artifacts, full));
        Assert.NotNull(image.GetKeyValue("in"));
        Assert.Null(image.GetKeyValue("out"));
    }

    [Fact]
    public async Task RunFull_CutIsMaxHlcAcrossPartitions_NotMaxIndex()
    {
        // P1 ends at a larger INDEX (2) but a smaller HLC (110); P2 ends at index 1 but HLC 500.
        // The cut must be the max HLC (500), not the HLC of whichever partition holds the max index.
        InMemoryWAL wal = BuildWal((1, 1, 100), (1, 2, 110), (2, 1, 500));
        BackupCatalog catalog = NewCatalog("hlc_cut");
        string artifacts = ArtifactsDir("hlc_cut");

        MemoryPersistenceBackend backend = new();
        PutAt(backend, "p1key", "v", 1, physicalMs: 110);
        PutAt(backend, "p2key", "v", 1, physicalMs: 500);

        BackupManifest full = await BackupDriver.RunFullAsync(wal, [Part(1), Part(2)], backend, artifacts, catalog);

        Assert.Equal(new HLCTimestamp(0, 500, 0), full.BaseCut);

        MemoryPersistenceBackend image = MemoryPersistenceBackend.OpenCheckpoint(CheckpointPath(artifacts, full));
        Assert.NotNull(image.GetKeyValue("p1key"));
        // Under the old index-derived cut (110) this key would have been wrongly excluded.
        Assert.NotNull(image.GetKeyValue("p2key"));
    }

    [Fact]
    public async Task RunFull_PinsSnapshotHistoryHoldAtCut_AndReleases()
    {
        InMemoryWAL wal = BuildWal((1, 1, 100)); // cut = 100
        BackupCatalog catalog = NewCatalog("hold_ok");
        string artifacts = ArtifactsDir("hold_ok");
        MemoryPersistenceBackend backend = new();
        PutAt(backend, "k", "v", 1, physicalMs: 50);

        HLCTimestamp? pinnedAt = null;
        string? releasedId = null;
        BackupDriver.AcquireSnapshotHoldDelegate acquire = (cut, _) => { pinnedAt = cut; return Task.FromResult<string?>("hold-1"); };
        BackupDriver.ReleaseSnapshotHoldDelegate release = (id, _) => { releasedId = id; return Task.CompletedTask; };

        BackupManifest full = await BackupDriver.RunFullAsync(
            wal, [Part(1)], backend, artifacts, catalog,
            flushBeforeCheckpoint: null, snapshotT: null, ct: default,
            acquireSnapshotHold: acquire, releaseSnapshotHold: release);

        Assert.Equal(new HLCTimestamp(0, 100, 0), pinnedAt); // pinned at the declared cut
        Assert.Equal("hold-1", releasedId);                  // released after publish
        Assert.Equal(new HLCTimestamp(0, 100, 0), full.BaseCut);
    }

    [Fact]
    public async Task RunFull_SnapshotFloorPastCut_FailsClosed()
    {
        InMemoryWAL wal = BuildWal((1, 1, 100));
        BackupCatalog catalog = NewCatalog("hold_fail");
        string artifacts = ArtifactsDir("hold_fail");
        MemoryPersistenceBackend backend = new();
        PutAt(backend, "k", "v", 1, physicalMs: 50);

        // Acquire returns null → the snapshot floor already passed the cut (history may be gone).
        BackupDriver.AcquireSnapshotHoldDelegate acquire = (_, _) => Task.FromResult<string?>(null);
        bool released = false;
        BackupDriver.ReleaseSnapshotHoldDelegate release = (_, _) => { released = true; return Task.CompletedTask; };

        BackupDriverException ex = await Assert.ThrowsAsync<BackupDriverException>(() =>
            BackupDriver.RunFullAsync(wal, [Part(1)], backend, artifacts, catalog,
                flushBeforeCheckpoint: null, snapshotT: null, ct: default,
                acquireSnapshotHold: acquire, releaseSnapshotHold: release));

        Assert.True(ex.ExactCheckpointUnavailable);
        Assert.Empty(catalog.List());
        Assert.False(released); // nothing to release — the hold was never acquired
        if (Directory.Exists(artifacts))
            Assert.Empty(Directory.GetDirectories(artifacts));
    }

    [Fact]
    public async Task RunFull_BackendWithoutExactAsOf_FailsClosed()
    {
        InMemoryWAL wal = BuildWal((1, 1, 100));
        BackupCatalog catalog = NewCatalog("no_exact");
        string artifacts = ArtifactsDir("no_exact");

        using NonExactBackend backend = new(new MemoryPersistenceBackend());

        BackupDriverException ex = await Assert.ThrowsAsync<BackupDriverException>(() =>
            BackupDriver.RunFullAsync(wal, [Part(1)], backend, artifacts, catalog));

        Assert.True(ex.ExactCheckpointUnavailable);
        Assert.Empty(catalog.List());
        if (Directory.Exists(artifacts))
            Assert.Empty(Directory.GetDirectories(artifacts));
    }

    /// <summary>
    /// A backend that retains no revision history and therefore cannot produce an exact as-of image.
    /// Delegates everything to an inner memory backend but reports
    /// <see cref="IPersistenceBackend.SupportsExactAsOfCheckpoint"/> = false, so it inherits the
    /// physical-copy default for <c>CreateCheckpointAsOf</c>.
    /// </summary>
    private sealed class NonExactBackend(MemoryPersistenceBackend inner) : IPersistenceBackend, IDisposable
    {
        public bool SupportsExactAsOfCheckpoint => false;

        public bool StoreLocks(List<PersistenceRequestItem> items) => inner.StoreLocks(items);
        public bool StoreKeyValues(List<PersistenceRequestItem> items) => inner.StoreKeyValues(items);
        public LockEntry? GetLock(string resource) => inner.GetLock(resource);
        public KeyValueEntry? GetKeyValue(string keyName) => inner.GetKeyValue(keyName);
        public KeyValueEntry? GetKeyValueRevision(string keyName, long revision) => inner.GetKeyValueRevision(keyName, revision);
        public KeyValueEntry? GetKeyValueRevisionAtOrBefore(string keyName, long maxRevision, HLCTimestamp readTimestamp) =>
            inner.GetKeyValueRevisionAtOrBefore(keyName, maxRevision, readTimestamp);
        public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByPrefix(string prefixKeyName) => inner.GetKeyValueByPrefix(prefixKeyName);
        public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByRange(string prefix, string? startKey, int limit) =>
            inner.GetKeyValueByRange(prefix, startKey, limit);
        public bool PruneKeyValueRevisions(IReadOnlyCollection<string>? keys, int retentionCount, TimeSpan retentionAge,
            int batchSize, HLCTimestamp floorTimestamp, out RevisionPruneResult result) =>
            inner.PruneKeyValueRevisions(keys, retentionCount, retentionAge, batchSize, floorTimestamp, out result);
        public CheckpointResult CreateCheckpoint(string destinationPath, long appliedIndex, HLCTimestamp appliedTime) =>
            inner.CreateCheckpoint(destinationPath, appliedIndex, appliedTime);
        public void Dispose() => inner.Dispose();
    }
}
