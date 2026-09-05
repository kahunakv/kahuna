
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
/// A full backup must fail closed when the revision history needed to reconstruct its cut has already
/// been pruned by retention before the backup began — a live snapshot hold only fences pruning from
/// that point on, it cannot restore an already-deleted boundary. Each pruning backend records a
/// durable pruned-history floor (the highest oldest-surviving boundary a prune produced); the driver
/// refuses a cut below it. The memory backend never prunes, so its floor stays Zero.
/// </summary>
public sealed class TestPitrPrunedHistoryFloor : IDisposable
{
    private static readonly ILogger<IRaft> Log = NullLogger<IRaft>.Instance;

    private readonly string _tempRoot =
        Path.Combine(Path.GetTempPath(), "kahuna_prunedfloor_" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        if (Directory.Exists(_tempRoot))
            try { Directory.Delete(_tempRoot, recursive: true); } catch { /* best-effort */ }
    }

    private string ArtifactsDir(string name) => Path.Combine(_tempRoot, "artifacts_" + name + Guid.NewGuid().ToString("N")[..6]);
    private BackupCatalog NewCatalog(string name) =>
        new(new LocalDirectoryStorageTarget(Path.Combine(_tempRoot, "catalog_" + name + Guid.NewGuid().ToString("N")[..6])));

    private static RaftPartitionRange Part(int id) => new() { PartitionId = id, State = RaftPartitionState.Active };

    private static InMemoryWAL BuildWal(int partition, long id, long ticks)
    {
        InMemoryWAL wal = new(Log);
        wal.Write([(partition, new List<RaftLog>
        {
            new() { Id = id, Type = RaftLogType.Committed, Time = new HLCTimestamp(0, ticks, 0) }
        })]);
        return wal;
    }

    private static PersistenceRequestItem ItemAt(string key, long revision, HLCTimestamp hlc) =>
        new(key, Encoding.UTF8.GetBytes("v" + revision), revision,
            0, 0, 0, 0, 0, 0,
            hlc.N, hlc.L, (uint)hlc.C, (int)KeyValueState.Set);

    private static PersistenceRequestItem ItemAt(string key, long revision, HLCTimestamp hlc, bool noRevision) =>
        new(key, Encoding.UTF8.GetBytes("v" + revision), revision,
            0, 0, 0, 0, 0, 0,
            hlc.N, hlc.L, (uint)hlc.C, (int)KeyValueState.Set, noRevision: noRevision);

    private static HLCTimestamp TS(long ms) => new(0, ms, 0);

    private (IPersistenceBackend Backend, string Dir) CreateBackend(string storage, string suffix)
    {
        string dir = Path.Combine(_tempRoot, storage + "_" + suffix + Guid.NewGuid().ToString("N")[..6]);
        Directory.CreateDirectory(dir);
        IPersistenceBackend backend = storage switch
        {
            "rocksdb" => new RocksDbPersistenceBackend(dir, "pf"),
            "sqlite"  => new SqlitePersistenceBackend(dir, "pf"),
            _         => throw new ArgumentException($"unknown storage '{storage}'", nameof(storage))
        };
        return (backend, dir);
    }

    private static IPersistenceBackend Reopen(string storage, string dir) => storage switch
    {
        "rocksdb" => new RocksDbPersistenceBackend(dir, "pf"),
        "sqlite"  => new SqlitePersistenceBackend(dir, "pf"),
        _         => throw new ArgumentException($"unknown storage '{storage}'", nameof(storage))
    };

    // ── durable backends: prune advances the floor and it survives reopen ──────────────────────

    [Theory]
    [InlineData("sqlite")]
    [InlineData("rocksdb")]
    public void Prune_AdvancesFloorToOldestSurviving_AndPersistsAcrossReopen(string storage)
    {
        const string key = "acct/pruned";
        (IPersistenceBackend backend, string dir) = CreateBackend(storage, "advance");
        try
        {
            // Revisions at T1..T5 (physical 1000..5000); rev 5 is current.
            for (long i = 1; i <= 5; i++)
                backend.StoreKeyValues([ItemAt(key, i, new HLCTimestamp(0, i * 1000L, 0))]);

            Assert.Equal(HLCTimestamp.Zero, backend.GetPrunedHistoryFloor());

            // retentionCount=1 keeps only the current revision (rev 5 @ 5000); rev 1–4 are pruned.
            Assert.True(backend.PruneKeyValueRevisions(
                [key], retentionCount: 1, TimeSpan.Zero, batchSize: 1000, HLCTimestamp.Zero,
                out RevisionPruneResult result));
            Assert.True(result.RevisionsDeleted > 0);

            // The oldest surviving boundary is rev 5 @ 5000 — cuts below it can no longer be reconstructed.
            Assert.Equal(new HLCTimestamp(0, 5000L, 0), backend.GetPrunedHistoryFloor());
        }
        finally { (backend as IDisposable)?.Dispose(); }

        // Reopen at the same path: the floor is durable (the deleted history did not come back).
        IPersistenceBackend reopened = Reopen(storage, dir);
        try
        {
            Assert.Equal(new HLCTimestamp(0, 5000L, 0), reopened.GetPrunedHistoryFloor());
        }
        finally { (reopened as IDisposable)?.Dispose(); }
    }

    [Theory]
    [InlineData("sqlite")]
    [InlineData("rocksdb")]
    public async Task Backup_BelowPrunedFloor_FailsClosed_AbovePasses(string storage)
    {
        const string key = "acct/pruned";
        (IPersistenceBackend backend, string dir) = CreateBackend(storage, "backup");
        try
        {
            for (long i = 1; i <= 5; i++)
                backend.StoreKeyValues([ItemAt(key, i, new HLCTimestamp(0, i * 1000L, 0))]);

            backend.PruneKeyValueRevisions(
                [key], retentionCount: 1, TimeSpan.Zero, batchSize: 1000, HLCTimestamp.Zero, out _);
            Assert.Equal(new HLCTimestamp(0, 5000L, 0), backend.GetPrunedHistoryFloor());

            InMemoryWAL wal = BuildWal(1, 1, 2000);

            // A cut below the pruned-history floor (3000 < 5000) cannot be reconstructed: fail closed.
            BackupCatalog catBelow = NewCatalog(storage + "_below");
            string artBelow = ArtifactsDir(storage + "_below");
            BackupDriverException ex = await Assert.ThrowsAsync<BackupDriverException>(() =>
                BackupDriver.RunFullAsync(wal, [Part(1)], backend, BackupTestStores.Artifacts(artBelow), catBelow, flushBeforeCheckpoint: null, snapshotT: new HLCTimestamp(0, 3000, 0), ct: TestContext.Current.CancellationToken));

            Assert.True(ex.ExactCheckpointUnavailable);
            Assert.Empty(await catBelow.ListAsync(TestContext.Current.CancellationToken));
            if (Directory.Exists(artBelow))
                Assert.Empty(Directory.GetDirectories(artBelow));

            // A cut at or above the floor is still reconstructable and must publish.
            BackupCatalog catAbove = NewCatalog(storage + "_above");
            string artAbove = ArtifactsDir(storage + "_above");
            InMemoryWAL walAbove = BuildWal(1, 1, 6000);
            BackupManifest ok = await BackupDriver.RunFullAsync(walAbove, [Part(1)], backend, BackupTestStores.Artifacts(artAbove), catAbove, flushBeforeCheckpoint: null, snapshotT: new HLCTimestamp(0, 6000, 0), ct: TestContext.Current.CancellationToken);
            Assert.NotNull(ok);
            Assert.Single(await catAbove.ListAsync(TestContext.Current.CancellationToken));
        }
        finally { (backend as IDisposable)?.Dispose(); }
    }

    // ── last revision row removed (revisioned → SetNoRevision) still advances the floor ────────

    [Theory]
    [InlineData("sqlite")]
    [InlineData("rocksdb")]
    public async Task Prune_RemovesLastRevisionUnderNoRevisionCurrent_AdvancesFloorToCurrent_AndBackupBelowFailsClosed(string storage)
    {
        const string key = "acct/norev-boundary";
        (IPersistenceBackend backend, string dir) = CreateBackend(storage, "norevfloor");
        try
        {
            // Revisioned write at 50 (creates a history row), then a SetNoRevision write at 100 (new
            // current revision, but NO history row of its own).
            backend.StoreKeyValues([ItemAt(key, 1, TS(50), noRevision: false)]);
            backend.StoreKeyValues([ItemAt(key, 2, TS(100), noRevision: true)]);
            Assert.Equal(HLCTimestamp.Zero, backend.GetPrunedHistoryFloor());

            // Age-based retention deletes the (ancient) revisioned history row; no revision row survives.
            Assert.True(backend.PruneKeyValueRevisions(
                [key], retentionCount: 0, TimeSpan.FromHours(1), batchSize: 1000, HLCTimestamp.Zero,
                out RevisionPruneResult result));
            Assert.True(result.RevisionsDeleted > 0);
            Assert.Null(backend.GetKeyValueRevision(key, 1)); // the boundary revision is gone

            // The floor advanced to the current (no-revision) row's HLC — not left at zero.
            Assert.Equal(TS(100), backend.GetPrunedHistoryFloor());

            // A full backup at a cut between the two writes cannot reconstruct the boundary → fail closed.
            InMemoryWAL wal = BuildWal(1, 1, 60);
            BackupCatalog cat = NewCatalog(storage + "_norev");
            string art = ArtifactsDir(storage + "_norev");
            BackupDriverException ex = await Assert.ThrowsAsync<BackupDriverException>(() =>
                BackupDriver.RunFullAsync(wal, [Part(1)], backend, BackupTestStores.Artifacts(art), cat, flushBeforeCheckpoint: null, snapshotT: TS(75), ct: TestContext.Current.CancellationToken));
            Assert.True(ex.ExactCheckpointUnavailable);
            Assert.Empty(await cat.ListAsync(TestContext.Current.CancellationToken));
        }
        finally { (backend as IDisposable)?.Dispose(); }

        // Reopen: the floor is durable.
        IPersistenceBackend reopened = Reopen(storage, dir);
        try { Assert.Equal(TS(100), reopened.GetPrunedHistoryFloor()); }
        finally { (reopened as IDisposable)?.Dispose(); }
    }

    // ── durable, crash-coupled watermark: stored in the DB, fails closed when unreadable ─────────

    [Theory]
    [InlineData("sqlite")]
    [InlineData("rocksdb")]
    public void Prune_StoresFloorInDb_NoSidecarFile(string storage)
    {
        const string key = "acct/indb";
        (IPersistenceBackend backend, string dir) = CreateBackend(storage, "indb");
        try
        {
            for (long i = 1; i <= 5; i++)
                backend.StoreKeyValues([ItemAt(key, i, new HLCTimestamp(0, i * 1000L, 0))]);
            backend.PruneKeyValueRevisions([key], retentionCount: 1, TimeSpan.Zero, 1000, HLCTimestamp.Zero, out _);
            Assert.Equal(new HLCTimestamp(0, 5000L, 0), backend.GetPrunedHistoryFloor());
        }
        finally { (backend as IDisposable)?.Dispose(); }

        // No sidecar floor file is used any more — the floor lives in the backend's own storage.
        Assert.Empty(Directory.GetFiles(dir, "pruned_history_floor*", SearchOption.AllDirectories));

        // Durable across reopen (from the DB, not a sidecar).
        IPersistenceBackend reopened = Reopen(storage, dir);
        try { Assert.Equal(new HLCTimestamp(0, 5000L, 0), reopened.GetPrunedHistoryFloor()); }
        finally { (reopened as IDisposable)?.Dispose(); }
    }

    [Fact]
    public void RocksDb_LegacySpacePrefixedFloor_IsAdoptedOnReopen_AndMigrated()
    {
        const string key = "acct/legacyfloor";
        string dir = Path.Combine(_tempRoot, "rocks_legacy_" + Guid.NewGuid().ToString("N")[..6]);
        Directory.CreateDirectory(dir);

        // A store with data but no prune under the current build: no floor under the current key.
        using (RocksDbPersistenceBackend backend = new(dir, "pf"))
        {
            backend.StoreKeyValues([ItemAt(key, 1, TS(1000))]);
            Assert.Equal(HLCTimestamp.Zero, ((IPersistenceBackend)backend).GetPrunedHistoryFloor());
        }

        // Simulate a store that pruned under a pre-rename build: its floor sits under the legacy
        // space-prefixed key only. The payload layout matches the backend's packed floor: N, L, C
        // as little-endian Int64s.
        byte[] legacyKey = " pitr_pruned_history_floor"u8.ToArray();
        byte[] packed = new byte[24];
        System.Buffers.Binary.BinaryPrimitives.WriteInt64LittleEndian(packed.AsSpan(0, 8), 0);
        System.Buffers.Binary.BinaryPrimitives.WriteInt64LittleEndian(packed.AsSpan(8, 8), 5000L);
        System.Buffers.Binary.BinaryPrimitives.WriteInt64LittleEndian(packed.AsSpan(16, 8), 0);
        byte[] currentKey = new byte[] { 0 }
            .Concat(System.Text.Encoding.UTF8.GetBytes("pitr_pruned_history_floor")).ToArray();
        RocksDbSharp.ColumnFamilies cfs = new()
        {
            { "kv", new RocksDbSharp.ColumnFamilyOptions() },
            { "locks", new RocksDbSharp.ColumnFamilyOptions() },
        };
        using (RocksDbSharp.RocksDb raw = RocksDbSharp.RocksDb.Open(
            new RocksDbSharp.DbOptions().SetCreateIfMissing(false), Path.Combine(dir, "pf"), cfs))
        {
            raw.Put(legacyKey, packed, cf: raw.GetColumnFamily("kv"));
            raw.Remove(currentKey, cf: raw.GetColumnFamily("kv"));
        }

        // Reopen with the current build: the legacy floor must be adopted, not read as "never
        // pruned" — a zero floor would let a backup trust already-deleted history.
        using (RocksDbPersistenceBackend reopened = new(dir, "pf"))
        {
            Assert.Equal(new HLCTimestamp(0, 5000L, 0), ((IPersistenceBackend)reopened).GetPrunedHistoryFloor());
        }

        // The read migrated the value to the current key, and the legacy key stays for rollback.
        using (RocksDbSharp.RocksDb raw = RocksDbSharp.RocksDb.Open(
            new RocksDbSharp.DbOptions().SetCreateIfMissing(false), Path.Combine(dir, "pf"), cfs))
        {
            Assert.Equal(packed, raw.Get(currentKey, cf: raw.GetColumnFamily("kv")));
            Assert.Equal(packed, raw.Get(legacyKey, cf: raw.GetColumnFamily("kv")));
        }
    }

    [Fact]
    public async Task RocksDb_CorruptFloorWatermark_FailsClosed()
    {
        const string key = "acct/corrupt";
        string dir = Path.Combine(_tempRoot, "rocks_corrupt_" + Guid.NewGuid().ToString("N")[..6]);
        Directory.CreateDirectory(dir);

        using (RocksDbPersistenceBackend backend = new(dir, "pf"))
        {
            for (long i = 1; i <= 5; i++)
                backend.StoreKeyValues([ItemAt(key, i, new HLCTimestamp(0, i * 1000L, 0))]);
            backend.PruneKeyValueRevisions([key], retentionCount: 1, TimeSpan.Zero, 1000, HLCTimestamp.Zero, out _);
            Assert.Equal(new HLCTimestamp(0, 5000L, 0), ((IPersistenceBackend)backend).GetPrunedHistoryFloor());
        }

        // Corrupt the durable floor: write a wrong-length payload to the reserved meta key.
        byte[] metaKey = new byte[] { 0 }
            .Concat(System.Text.Encoding.UTF8.GetBytes("pitr_pruned_history_floor")).ToArray();
        RocksDbSharp.ColumnFamilies cfs = new()
        {
            { "kv", new RocksDbSharp.ColumnFamilyOptions() },
            { "locks", new RocksDbSharp.ColumnFamilyOptions() },
        };
        using (RocksDbSharp.RocksDb raw = RocksDbSharp.RocksDb.Open(
            new RocksDbSharp.DbOptions().SetCreateIfMissing(false), Path.Combine(dir, "pf"), cfs))
        {
            raw.Put(metaKey, new byte[] { 1, 2, 3 }, cf: raw.GetColumnFamily("kv"));
        }

        // Reopen: an unreadable floor for a store that pruned must refuse every cut.
        using RocksDbPersistenceBackend reopened = new(dir, "pf");
        BackupCatalog cat = NewCatalog("rocks_corrupt");
        string art = ArtifactsDir("rocks_corrupt");
        InMemoryWAL wal = BuildWal(1, 1, 100_000);
        BackupDriverException ex = await Assert.ThrowsAsync<BackupDriverException>(() =>
            BackupDriver.RunFullAsync(wal, [Part(1)], reopened, BackupTestStores.Artifacts(art), cat, flushBeforeCheckpoint: null, snapshotT: new HLCTimestamp(0, 100_000, 0), ct: TestContext.Current.CancellationToken));
        Assert.True(ex.ExactCheckpointUnavailable);
        Assert.Empty(await cat.ListAsync(TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Sqlite_CorruptFloorWatermark_FailsClosed()
    {
        const string key = "acct/corrupt";
        string dir = Path.Combine(_tempRoot, "sql_corrupt_" + Guid.NewGuid().ToString("N")[..6]);
        Directory.CreateDirectory(dir);

        using (SqlitePersistenceBackend backend = new(dir, "pf"))
        {
            for (long i = 1; i <= 5; i++)
                backend.StoreKeyValues([ItemAt(key, i, new HLCTimestamp(0, i * 1000L, 0))]);
            backend.PruneKeyValueRevisions([key], retentionCount: 1, TimeSpan.Zero, 1000, HLCTimestamp.Zero, out _);
            Assert.Equal(new HLCTimestamp(0, 5000L, 0), ((IPersistenceBackend)backend).GetPrunedHistoryFloor());
        }

        // Corrupt every shard database file so the floor read throws — a store that pruned must then
        // refuse every cut rather than reopen with a zero floor.
        foreach (string db in Directory.GetFiles(dir, "*.db"))
            File.WriteAllBytes(db, "not a sqlite database at all"u8.ToArray());

        using SqlitePersistenceBackend reopened = new(dir, "pf");
        BackupCatalog cat = NewCatalog("sql_corrupt");
        string art = ArtifactsDir("sql_corrupt");
        InMemoryWAL wal = BuildWal(1, 1, 100_000);
        BackupDriverException ex = await Assert.ThrowsAsync<BackupDriverException>(() =>
            BackupDriver.RunFullAsync(wal, [Part(1)], reopened, BackupTestStores.Artifacts(art), cat, flushBeforeCheckpoint: null, snapshotT: new HLCTimestamp(0, 100_000, 0), ct: TestContext.Current.CancellationToken));
        Assert.True(ex.ExactCheckpointUnavailable);
        Assert.Empty(await cat.ListAsync(TestContext.Current.CancellationToken));
    }

    // ── memory backend: never prunes, so the floor stays Zero and old cuts are always allowed ──

    [Fact]
    public async Task Memory_NeverPrunes_FloorStaysZero_OldCutSucceeds()
    {
        MemoryPersistenceBackend backend = new();
        const string key = "acct/mem";
        for (long i = 1; i <= 5; i++)
            backend.StoreKeyValues([ItemAt(key, i, new HLCTimestamp(0, i * 1000L, 0))]);

        // A no-op prune must not advance the floor.
        backend.PruneKeyValueRevisions([key], retentionCount: 1, TimeSpan.Zero, batchSize: 1000, HLCTimestamp.Zero, out _);
        Assert.Equal(HLCTimestamp.Zero, ((IPersistenceBackend)backend).GetPrunedHistoryFloor());

        // A backup at an old cut (2500) succeeds: all history is retained, boundary is present.
        BackupCatalog catalog = NewCatalog("mem");
        string artifacts = ArtifactsDir("mem");
        InMemoryWAL wal = BuildWal(1, 1, 2000);
        BackupManifest ok = await BackupDriver.RunFullAsync(wal, [Part(1)], backend, BackupTestStores.Artifacts(artifacts), catalog, flushBeforeCheckpoint: null, snapshotT: new HLCTimestamp(0, 2500, 0), ct: TestContext.Current.CancellationToken);
        Assert.NotNull(ok);
        Assert.Single(await catalog.ListAsync(TestContext.Current.CancellationToken));
    }

    // ── driver check in isolation, independent of any real prune ───────────────────────────────

    /// <summary>Wraps a memory backend but reports a fixed pruned-history floor.</summary>
    private sealed class FixedFloorBackend(MemoryPersistenceBackend inner, HLCTimestamp floor) : IPersistenceBackend, IDisposable
    {
        public bool SupportsExactAsOfCheckpoint => true;
        public HLCTimestamp GetPrunedHistoryFloor() => floor;

        public CheckpointResult CreateCheckpointAsOf(string d, long i, HLCTimestamp cut, CancellationToken ct = default) =>
            inner.CreateCheckpointAsOf(d, i, cut, ct);
        public bool StoreLocks(List<PersistenceRequestItem> items) => inner.StoreLocks(items);
        public bool StoreKeyValues(List<PersistenceRequestItem> items) => inner.StoreKeyValues(items);
        public LockEntry? GetLock(string r) => inner.GetLock(r);
        public KeyValueEntry? GetKeyValue(string k) => inner.GetKeyValue(k);
        public KeyValueEntry? GetKeyValueRevision(string k, long rev) => inner.GetKeyValueRevision(k, rev);
        public KeyValueEntry? GetKeyValueRevisionAtOrBefore(string k, long maxRev, HLCTimestamp ts) => inner.GetKeyValueRevisionAtOrBefore(k, maxRev, ts);
        public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByPrefix(string p) => inner.GetKeyValueByPrefix(p);
        public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByRange(string p, string? s, int l) => inner.GetKeyValueByRange(p, s, l);
        public bool PruneKeyValueRevisions(IReadOnlyCollection<string>? keys, int rc, TimeSpan ra, int bs, HLCTimestamp floorTs, out RevisionPruneResult r) =>
            inner.PruneKeyValueRevisions(keys, rc, ra, bs, floorTs, out r);
        public CheckpointResult CreateCheckpoint(string d, long i, HLCTimestamp t) => inner.CreateCheckpoint(d, i, t);
        public void Dispose() => inner.Dispose();
    }

    [Fact]
    public async Task Driver_RefusesCutBelowReportedFloor()
    {
        MemoryPersistenceBackend inner = new();
        inner.StoreKeyValues([ItemAt("k", 1, new HLCTimestamp(0, 1000, 0))]);
        using FixedFloorBackend backend = new(inner, new HLCTimestamp(0, 4000, 0));

        InMemoryWAL wal = BuildWal(1, 1, 2000);
        BackupCatalog catalog = NewCatalog("stub");
        string artifacts = ArtifactsDir("stub");

        BackupDriverException ex = await Assert.ThrowsAsync<BackupDriverException>(() =>
            BackupDriver.RunFullAsync(wal, [Part(1)], backend, BackupTestStores.Artifacts(artifacts), catalog, flushBeforeCheckpoint: null, snapshotT: new HLCTimestamp(0, 3000, 0), ct: TestContext.Current.CancellationToken));

        Assert.True(ex.ExactCheckpointUnavailable);
        Assert.Empty(await catalog.ListAsync(TestContext.Current.CancellationToken));
    }
}
