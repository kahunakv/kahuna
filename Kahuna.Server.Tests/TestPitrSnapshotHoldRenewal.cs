
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
/// A full backup pins an MVCC snapshot-history hold at its cut and must keep that hold's lease alive
/// for the entire checkpoint/hash/verify/publish window — a window that can outlast a single lease on
/// large stores (SQLite VACUUM, RocksDB compaction, hashing, verification). These tests drive the
/// hold lifecycle through <see cref="BackupDriver.RunFullAsync"/> with a checkpoint that blocks under
/// test control, proving:
/// <list type="bullet">
///   <item>the lease is renewed repeatedly while a long checkpoint runs, and the backup then publishes;</item>
///   <item>a lost renewal (renew returns false, e.g. leadership change, or throws, e.g. transport) fails
///         the backup closed and publishes nothing;</item>
///   <item>caller cancellation aborts and releases the hold;</item>
///   <item>a release failure after publication does not turn a committed success into a failure.</item>
/// </list>
/// </summary>
public sealed class TestPitrSnapshotHoldRenewal : IDisposable
{
    private static readonly ILogger<IRaft> Log = NullLogger<IRaft>.Instance;

    private readonly string _tempRoot =
        Path.Combine(Path.GetTempPath(), "kahuna_holdrenew_" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        if (Directory.Exists(_tempRoot))
            try { Directory.Delete(_tempRoot, recursive: true); } catch { /* best-effort */ }
    }

    private string ArtifactsDir(string name) => Path.Combine(_tempRoot, "artifacts_" + name);

    private BackupCatalog NewCatalog(string name) =>
        new(new LocalDirectoryStorageTarget(Path.Combine(_tempRoot, "catalog_" + name)));

    private static InMemoryWAL BuildWal(int partition, long id, long ticks)
    {
        InMemoryWAL wal = new(Log);
        wal.Write([(partition, new List<RaftLog>
        {
            new() { Id = id, Type = RaftLogType.Committed, Time = new HLCTimestamp(0, ticks, 0) }
        })]);
        return wal;
    }

    private static RaftPartitionRange Part(int id) => new() { PartitionId = id, State = RaftPartitionState.Active };

    private static void PutAt(MemoryPersistenceBackend b, string key, long revision, long physicalMs) =>
        b.StoreKeyValues([new(
            key, Encoding.UTF8.GetBytes("v"), revision,
            0, 0, 0, 0, physicalMs, 0, 0, physicalMs, 0, (int)KeyValueState.Set)]);

    // ── a checkpoint that blocks until released or cancelled, so renewals can run during it ──

    private sealed class GatedCheckpointBackend(MemoryPersistenceBackend inner) : IPersistenceBackend, IDisposable
    {
        public readonly ManualResetEventSlim Entered = new(false);
        public readonly ManualResetEventSlim Release = new(false);

        public bool SupportsExactAsOfCheckpoint => true;

        public CheckpointResult CreateCheckpointAsOf(string destinationPath, long appliedIndex, HLCTimestamp cut, CancellationToken ct = default)
        {
            Entered.Set();
            // Block until the test releases the checkpoint or the work is cancelled (renewal loss or
            // caller cancellation). Honoring ct is what lets a lost renewal abort the checkpoint.
            WaitHandle.WaitAny([Release.WaitHandle, ct.WaitHandle]);
            ct.ThrowIfCancellationRequested();
            return inner.CreateCheckpointAsOf(destinationPath, appliedIndex, cut, ct);
        }

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

        public void Dispose()
        {
            Entered.Dispose();
            Release.Dispose();
            inner.Dispose();
        }
    }

    // A short lease so renewal fires quickly under the blocked checkpoint (interval = lease / 3).
    private const int ShortLeaseMs = 90;

    [Fact]
    public async Task RunFull_RenewsHoldThroughLongCheckpoint_ThenPublishes()
    {
        InMemoryWAL wal = BuildWal(1, 1, 100);
        BackupCatalog catalog = NewCatalog("renew_ok");
        string artifacts = ArtifactsDir("renew_ok");
        MemoryPersistenceBackend inner = new();
        PutAt(inner, "k", 1, physicalMs: 50);
        using GatedCheckpointBackend backend = new(inner);

        int renewCount = 0;
        ManualResetEventSlim renewedTwice = new(false);
        BackupDriver.AcquireSnapshotHoldDelegate acquire = (_, _) => Task.FromResult<string?>("hold-1");
        BackupDriver.RenewSnapshotHoldDelegate renew = (_, _) =>
        {
            if (Interlocked.Increment(ref renewCount) >= 2)
                renewedTwice.Set();
            return Task.FromResult(true);
        };
        string? releasedId = null;
        BackupDriver.ReleaseSnapshotHoldDelegate release = (id, _) => { releasedId = id; return Task.CompletedTask; };

        // Offload to a pool thread: RunFullAsync runs synchronously up to the blocking checkpoint,
        // so the test thread must stay free to release the gate.
        Task<BackupManifest> run = Task.Run(() => BackupDriver.RunFullAsync(
            wal, [Part(1)], backend, artifacts, catalog,
            flushBeforeCheckpoint: null, snapshotT: null, ct: default,
            acquireSnapshotHold: acquire, releaseSnapshotHold: release,
            renewSnapshotHold: renew, snapshotHoldLeaseMs: ShortLeaseMs));

        Assert.True(backend.Entered.Wait(TimeSpan.FromSeconds(10)), "checkpoint must be reached");
        // While the checkpoint is blocked, the lease must be renewed more than once.
        Assert.True(renewedTwice.Wait(TimeSpan.FromSeconds(10)), "hold must be renewed at least twice during the checkpoint");

        backend.Release.Set();
        BackupManifest manifest = await run;

        Assert.NotNull(manifest);
        Assert.Single(catalog.List());          // published
        Assert.Equal("hold-1", releasedId);      // released after publish
        Assert.True(renewCount >= 2);
    }

    [Fact]
    public async Task RunFull_RenewalReturnsFalse_FailsClosed_NoPublish()
    {
        InMemoryWAL wal = BuildWal(1, 1, 100);
        BackupCatalog catalog = NewCatalog("renew_lost");
        string artifacts = ArtifactsDir("renew_lost");
        MemoryPersistenceBackend inner = new();
        PutAt(inner, "k", 1, physicalMs: 50);
        using GatedCheckpointBackend backend = new(inner);

        BackupDriver.AcquireSnapshotHoldDelegate acquire = (_, _) => Task.FromResult<string?>("hold-1");
        // Renewal is lost the first time it is attempted (e.g. leadership moved away).
        BackupDriver.RenewSnapshotHoldDelegate renew = (_, _) => Task.FromResult(false);
        string? releasedId = null;
        BackupDriver.ReleaseSnapshotHoldDelegate release = (id, _) => { releasedId = id; return Task.CompletedTask; };

        BackupDriverException ex = await Assert.ThrowsAsync<BackupDriverException>(() =>
            BackupDriver.RunFullAsync(
                wal, [Part(1)], backend, artifacts, catalog,
                flushBeforeCheckpoint: null, snapshotT: null, ct: default,
                acquireSnapshotHold: acquire, releaseSnapshotHold: release,
                renewSnapshotHold: renew, snapshotHoldLeaseMs: ShortLeaseMs));

        Assert.True(ex.ExactCheckpointUnavailable);
        Assert.Empty(catalog.List());                 // never published
        Assert.Equal("hold-1", releasedId);           // hold still released
        if (Directory.Exists(artifacts))
            Assert.Empty(Directory.GetDirectories(artifacts)); // artifacts cleaned up
    }

    [Fact]
    public async Task RunFull_RenewalThrows_FailsClosed_NoPublish()
    {
        InMemoryWAL wal = BuildWal(1, 1, 100);
        BackupCatalog catalog = NewCatalog("renew_throw");
        string artifacts = ArtifactsDir("renew_throw");
        MemoryPersistenceBackend inner = new();
        PutAt(inner, "k", 1, physicalMs: 50);
        using GatedCheckpointBackend backend = new(inner);

        BackupDriver.AcquireSnapshotHoldDelegate acquire = (_, _) => Task.FromResult<string?>("hold-1");
        // A transport failure during renewal is also renewal loss.
        BackupDriver.RenewSnapshotHoldDelegate renew = (_, _) => throw new InvalidOperationException("transport down");
        bool released = false;
        BackupDriver.ReleaseSnapshotHoldDelegate release = (_, _) => { released = true; return Task.CompletedTask; };

        BackupDriverException ex = await Assert.ThrowsAsync<BackupDriverException>(() =>
            BackupDriver.RunFullAsync(
                wal, [Part(1)], backend, artifacts, catalog,
                flushBeforeCheckpoint: null, snapshotT: null, ct: default,
                acquireSnapshotHold: acquire, releaseSnapshotHold: release,
                renewSnapshotHold: renew, snapshotHoldLeaseMs: ShortLeaseMs));

        Assert.True(ex.ExactCheckpointUnavailable);
        Assert.Empty(catalog.List());
        Assert.True(released);
    }

    [Fact]
    public async Task RunFull_CallerCancellation_AbortsAndReleases()
    {
        InMemoryWAL wal = BuildWal(1, 1, 100);
        BackupCatalog catalog = NewCatalog("cancel");
        string artifacts = ArtifactsDir("cancel");
        MemoryPersistenceBackend inner = new();
        PutAt(inner, "k", 1, physicalMs: 50);
        using GatedCheckpointBackend backend = new(inner);

        using CancellationTokenSource cts = new();
        BackupDriver.AcquireSnapshotHoldDelegate acquire = (_, _) => Task.FromResult<string?>("hold-1");
        BackupDriver.RenewSnapshotHoldDelegate renew = (_, _) => Task.FromResult(true);
        string? releasedId = null;
        BackupDriver.ReleaseSnapshotHoldDelegate release = (id, _) => { releasedId = id; return Task.CompletedTask; };

        // Offload to a pool thread: RunFullAsync blocks synchronously in the checkpoint, so the test
        // thread must stay free to cancel.
        Task<BackupManifest> run = Task.Run(() => BackupDriver.RunFullAsync(
            wal, [Part(1)], backend, artifacts, catalog,
            flushBeforeCheckpoint: null, snapshotT: null, ct: cts.Token,
            acquireSnapshotHold: acquire, releaseSnapshotHold: release,
            renewSnapshotHold: renew, snapshotHoldLeaseMs: ShortLeaseMs));

        Assert.True(backend.Entered.Wait(TimeSpan.FromSeconds(10)), "checkpoint must be reached");
        cts.Cancel();

        // Caller cancellation surfaces as a bare cancellation, distinct from a renewal-loss failure.
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => run);

        Assert.Empty(catalog.List());
        Assert.Equal("hold-1", releasedId); // hold released on the cancel path
    }

    [Fact]
    public async Task RunFull_ReleaseFailsAfterPublish_BackupStillSucceeds()
    {
        InMemoryWAL wal = BuildWal(1, 1, 100);
        BackupCatalog catalog = NewCatalog("release_fail");
        string artifacts = ArtifactsDir("release_fail");
        MemoryPersistenceBackend backend = new();
        PutAt(backend, "k", 1, physicalMs: 50);

        BackupDriver.AcquireSnapshotHoldDelegate acquire = (_, _) => Task.FromResult<string?>("hold-1");
        BackupDriver.RenewSnapshotHoldDelegate renew = (_, _) => Task.FromResult(true);
        // Release throws after the manifest is already published — must not become a backup failure.
        BackupDriver.ReleaseSnapshotHoldDelegate release = (_, _) => throw new InvalidOperationException("release transport down");

        BackupManifest manifest = await BackupDriver.RunFullAsync(
            wal, [Part(1)], backend, artifacts, catalog,
            flushBeforeCheckpoint: null, snapshotT: null, ct: default,
            acquireSnapshotHold: acquire, releaseSnapshotHold: release,
            renewSnapshotHold: renew, snapshotHoldLeaseMs: ShortLeaseMs);

        Assert.NotNull(manifest);
        Assert.Single(catalog.List()); // published despite the release failure
    }
}
