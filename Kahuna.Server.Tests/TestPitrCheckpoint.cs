
using Kahuna.Server.KeyValues;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Server.Persistence.Pitr;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// Unit tests for the storage-engine checkpoint primitive.
/// Uses <see cref="MemoryPersistenceBackend"/> (no disk DB required):
/// write N keys → CreateCheckpoint → OpenCheckpoint → assert all N present;
/// original store remains writable throughout.
/// </summary>
public sealed class TestPitrCheckpoint : IDisposable
{
    private readonly string _tempRoot = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());

    public void Dispose()
    {
        if (Directory.Exists(_tempRoot))
            Directory.Delete(_tempRoot, recursive: true);
    }

    private string NewDir(string name = "cp") =>
        Path.Combine(_tempRoot, name);

    private static HLCTimestamp T(long ticks) => new(0, ticks, 0);

    // Minimal helper: store one key with the given value and revision.
    private static void Put(MemoryPersistenceBackend b, string key, byte[] value, long rev) =>
        b.StoreKeyValues([new(key, value, rev, 0, 0, 0, 0, rev, 0, 0, rev, 0, 1)]);

    // ── manifest tests ────────────────────────────────────────────────────────────

    [Fact]
    public void CreateCheckpoint_WritesManifestFile()
    {
        string dir = NewDir();
        using MemoryPersistenceBackend b = new();

        CheckpointResult result = b.CreateCheckpoint(dir, appliedIndex: 42, appliedTime: T(100));

        Assert.Equal(dir, result.CheckpointPath);
        Assert.Equal(42, result.AppliedIndex);
        Assert.Equal(T(100), result.AppliedTime);
        Assert.True(File.Exists(Path.Combine(dir, CheckpointManifest.FileName)));
    }

    [Fact]
    public void CreateCheckpoint_ManifestRoundtrips()
    {
        string dir = NewDir();
        using MemoryPersistenceBackend b = new();
        b.CreateCheckpoint(dir, appliedIndex: 99, appliedTime: T(777));

        CheckpointManifest m = CheckpointManifest.ReadFrom(dir);
        Assert.Equal(99, m.AppliedIndex);
        Assert.Equal(T(777), m.AppliedTime);
    }

    [Fact]
    public void CreateCheckpoint_CreatesNestedDestinationDirectory()
    {
        string dir = Path.Combine(_tempRoot, "a", "b", "c");
        using MemoryPersistenceBackend b = new();
        b.CreateCheckpoint(dir, 0, T(0));
        Assert.True(Directory.Exists(dir));
    }

    // ── reopen tests ─────────────────────────────────────────────────────────────

    [Fact]
    public void OpenCheckpoint_EmptyStore_ReturnsEmptyBackend()
    {
        string dir = NewDir();
        using MemoryPersistenceBackend b = new();
        b.CreateCheckpoint(dir, 0, T(0));

        using MemoryPersistenceBackend r = MemoryPersistenceBackend.OpenCheckpoint(dir);
        Assert.Null(r.GetKeyValue("any"));
    }

    [Fact]
    public void OpenCheckpoint_AllKeysPresent()
    {
        string dir = NewDir();
        using MemoryPersistenceBackend b = new();

        for (int i = 1; i <= 10; i++)
            Put(b, $"key/{i}", [(byte)i], i);

        b.CreateCheckpoint(dir, appliedIndex: 10, appliedTime: T(10));

        using MemoryPersistenceBackend r = MemoryPersistenceBackend.OpenCheckpoint(dir);
        for (int i = 1; i <= 10; i++)
        {
            var e = r.GetKeyValue($"key/{i}");
            Assert.NotNull(e);
            Assert.Equal(i, e!.Revision);
            Assert.Equal((byte)i, e.Value![0]);
        }
    }

    [Fact]
    public void OpenCheckpoint_PreservesRevision()
    {
        string dir = NewDir();
        using MemoryPersistenceBackend b = new();
        Put(b, "k", [0xFF], rev: 42);
        b.CreateCheckpoint(dir, 42, T(42));

        using MemoryPersistenceBackend r = MemoryPersistenceBackend.OpenCheckpoint(dir);
        Assert.Equal(42, r.GetKeyValue("k")!.Revision);
    }

    [Fact]
    public void OpenCheckpoint_PreservesTimestampMetadata()
    {
        string dir = NewDir();
        using MemoryPersistenceBackend b = new();
        b.StoreKeyValues([new(
            "meta/key", [0xAB], revision: 7,
            expiresNode: 0, expiresPhysical: 9999, expiresCounter: 0,
            lastUsedNode: 0, lastUsedPhysical: 1234, lastUsedCounter: 0,
            lastModifiedNode: 0, lastModifiedPhysical: 5678, lastModifiedCounter: 0,
            state: 1
        )]);
        b.CreateCheckpoint(dir, 7, T(5678));

        using MemoryPersistenceBackend r = MemoryPersistenceBackend.OpenCheckpoint(dir);
        var e = r.GetKeyValue("meta/key");
        Assert.NotNull(e);
        Assert.Equal(9999L, e!.Expires.L);
        Assert.Equal(1234L, e.LastUsed.L);
        Assert.Equal(5678L, e.LastModified.L);
    }

    // ── isolation tests ───────────────────────────────────────────────────────────

    [Fact]
    public void Checkpoint_SnapshotExcludesPostCheckpointWrites()
    {
        string dir = NewDir();
        using MemoryPersistenceBackend b = new();
        Put(b, "snap/key", [1], 1);
        b.CreateCheckpoint(dir, 1, T(1));

        Put(b, "after/key", [2], 2); // written after snapshot

        using MemoryPersistenceBackend r = MemoryPersistenceBackend.OpenCheckpoint(dir);
        Assert.NotNull(r.GetKeyValue("snap/key"));
        Assert.Null(r.GetKeyValue("after/key")); // not captured
    }

    [Fact]
    public void OriginalStore_RemainsWritableAfterCheckpoint()
    {
        string dir = NewDir();
        using MemoryPersistenceBackend b = new();
        Put(b, "a", [1], 1);
        b.CreateCheckpoint(dir, 1, T(1));

        Put(b, "b", [2], 2);
        Assert.NotNull(b.GetKeyValue("b"));
    }

    [Fact]
    public void TwoCheckpoints_AreIndependent()
    {
        string dir1 = NewDir("cp1"), dir2 = NewDir("cp2");
        using MemoryPersistenceBackend b = new();

        Put(b, "k", [1], 1);
        b.CreateCheckpoint(dir1, 1, T(1));

        Put(b, "k", [2], 2); // overwrite
        b.CreateCheckpoint(dir2, 2, T(2));

        using MemoryPersistenceBackend r1 = MemoryPersistenceBackend.OpenCheckpoint(dir1);
        using MemoryPersistenceBackend r2 = MemoryPersistenceBackend.OpenCheckpoint(dir2);

        Assert.Equal(1, r1.GetKeyValue("k")!.Revision);
        Assert.Equal(2, r2.GetKeyValue("k")!.Revision);
    }

    // ── RocksDB round-trip ────────────────────────────────────────────────────────

    [Fact]
    public void RocksDb_Checkpoint_AllKeysReadableFromCheckpoint()
    {
        // RocksDB db lives at {base}/v1/ (the constructor appends /{dbRevision}).
        // Checkpoint goes to {base}/cp — RocksDB must create the leaf itself.
        string baseDir = Path.Combine(_tempRoot, "rocksdb_base");
        Directory.CreateDirectory(baseDir);
        string cpDir = Path.Combine(baseDir, "cp"); // must NOT exist before Save

        using (RocksDbPersistenceBackend backend = new(baseDir, "v1"))
        {
            for (int i = 1; i <= 20; i++)
                backend.StoreKeyValues([new($"rk/{i:D3}", [(byte)i], i, 0, 0, 0, 0, i, 0, 0, i, 0, 1)]);

            backend.CreateCheckpoint(cpDir, appliedIndex: 20, appliedTime: T(20));
        }

        // Reopen the checkpoint: path=baseDir, dbRevision="cp" → opens {baseDir}/cp
        using RocksDbPersistenceBackend restored = new(baseDir, "cp");
        for (int i = 1; i <= 20; i++)
        {
            var e = restored.GetKeyValue($"rk/{i:D3}");
            Assert.NotNull(e);
            Assert.Equal(i, e!.Revision);
            Assert.Equal((byte)i, e.Value![0]);
        }

        CheckpointManifest manifest = CheckpointManifest.ReadFrom(cpDir);
        Assert.Equal(20, manifest.AppliedIndex);
        Assert.Equal(T(20), manifest.AppliedTime);
    }

    [Fact]
    public void RocksDb_Checkpoint_OriginalWritableAfterCheckpoint()
    {
        string baseDir = Path.Combine(_tempRoot, "rocksdb_rw");
        Directory.CreateDirectory(baseDir);
        string cpDir = Path.Combine(baseDir, "cp");

        using RocksDbPersistenceBackend backend = new(baseDir, "v1");
        backend.StoreKeyValues([new("a", [1], 1, 0, 0, 0, 0, 1, 0, 0, 1, 0, 1)]);
        backend.CreateCheckpoint(cpDir, 1, T(1));

        backend.StoreKeyValues([new("b", [2], 2, 0, 0, 0, 0, 2, 0, 0, 2, 0, 1)]);
        Assert.NotNull(backend.GetKeyValue("b"));
    }

    [Fact]
    public void RocksDb_Checkpoint_SnapshotExcludesPostCheckpointWrites()
    {
        string baseDir = Path.Combine(_tempRoot, "rocksdb_iso");
        Directory.CreateDirectory(baseDir);
        string cpDir = Path.Combine(baseDir, "cp");

        using (RocksDbPersistenceBackend backend = new(baseDir, "v1"))
        {
            backend.StoreKeyValues([new("snap/k", [1], 1, 0, 0, 0, 0, 1, 0, 0, 1, 0, 1)]);
            backend.CreateCheckpoint(cpDir, 1, T(1));
            backend.StoreKeyValues([new("post/k", [2], 2, 0, 0, 0, 0, 2, 0, 0, 2, 0, 1)]);
        }

        using RocksDbPersistenceBackend r = new(baseDir, "cp");
        Assert.NotNull(r.GetKeyValue("snap/k"));
        Assert.Null(r.GetKeyValue("post/k"));
    }

    // ── SQLite round-trip ─────────────────────────────────────────────────────────

    [Fact]
    public void Sqlite_Checkpoint_AllKeysReadableFromCheckpoint()
    {
        string dbDir = Path.Combine(_tempRoot, "sqlite_base");
        Directory.CreateDirectory(dbDir);
        string cpDir = Path.Combine(_tempRoot, "sqlite_cp"); // must NOT exist; CreateCheckpoint creates it

        using (SqlitePersistenceBackend backend = new(dbDir, "v1"))
        {
            for (int i = 1; i <= 20; i++)
                backend.StoreKeyValues([new($"sk/{i:D3}", [(byte)i], i, 0, 0, 0, 0, i, 0, 0, i, 0, 1)]);

            backend.CreateCheckpoint(cpDir, appliedIndex: 20, appliedTime: T(20));
        }

        // SQLite checkpoint writes {cpDir}/kahuna{shard}_v1.db — reopen with same path+revision.
        using SqlitePersistenceBackend restored = new(cpDir, "v1");
        for (int i = 1; i <= 20; i++)
        {
            var e = restored.GetKeyValue($"sk/{i:D3}");
            Assert.NotNull(e);
            Assert.Equal(i, e!.Revision);
            Assert.Equal((byte)i, e.Value![0]);
        }

        CheckpointManifest manifest = CheckpointManifest.ReadFrom(cpDir);
        Assert.Equal(20, manifest.AppliedIndex);
        Assert.Equal(T(20), manifest.AppliedTime);
    }

    [Fact]
    public void Sqlite_Checkpoint_SnapshotExcludesPostCheckpointWrites()
    {
        string dbDir = Path.Combine(_tempRoot, "sqlite_iso");
        Directory.CreateDirectory(dbDir);
        string cpDir = Path.Combine(_tempRoot, "sqlite_cp_iso"); // must NOT exist

        using (SqlitePersistenceBackend backend = new(dbDir, "v1"))
        {
            backend.StoreKeyValues([new("snap/k", [1], 1, 0, 0, 0, 0, 1, 0, 0, 1, 0, 1)]);
            backend.CreateCheckpoint(cpDir, 1, T(1));
            backend.StoreKeyValues([new("post/k", [2], 2, 0, 0, 0, 0, 2, 0, 0, 2, 0, 1)]);
        }

        using SqlitePersistenceBackend r = new(cpDir, "v1");
        Assert.NotNull(r.GetKeyValue("snap/k"));
        Assert.Null(r.GetKeyValue("post/k"));
    }

    // ── as-of (exact cut) checkpoint ─────────────────────────────────────────────────

    // Stores one key at a specific revision with LastModified = (0, physicalMs, 0).
    private static PersistenceRequestItem Item(string key, byte val, long rev, long physicalMs) =>
        new(key, [val], rev, 0, 0, 0, 0, physicalMs, 0, 0, physicalMs, 0, 1);

    [Fact]
    public void Backends_AdvertiseExactAsOfSupport()
    {
        using MemoryPersistenceBackend mem = new();
        Assert.True(mem.SupportsExactAsOfCheckpoint);

        string sqliteDir = Path.Combine(_tempRoot, "asof_sup_sqlite");
        Directory.CreateDirectory(sqliteDir);
        using SqlitePersistenceBackend sqlite = new(sqliteDir, "v1");
        Assert.True(sqlite.SupportsExactAsOfCheckpoint);

        string rocksDir = Path.Combine(_tempRoot, "asof_sup_rocks");
        Directory.CreateDirectory(rocksDir);
        using RocksDbPersistenceBackend rocks = new(rocksDir, "v1");
        Assert.True(rocks.SupportsExactAsOfCheckpoint);
    }

    [Fact]
    public void Sqlite_CreateCheckpointAsOf_CutsAtTimestamp()
    {
        string dbDir = Path.Combine(_tempRoot, "asof_sqlite_base");
        Directory.CreateDirectory(dbDir);
        string cpDir = Path.Combine(_tempRoot, "asof_sqlite_cp");

        using (SqlitePersistenceBackend backend = new(dbDir, "v1"))
        {
            backend.StoreKeyValues([Item("keep", 1, rev: 1, physicalMs: 50)]);    // ≤ cut → kept
            backend.StoreKeyValues([Item("future", 1, rev: 1, physicalMs: 200)]); // > cut → dropped
            backend.StoreKeyValues([Item("updated", 1, rev: 1, physicalMs: 50)]);  // as-of value
            backend.StoreKeyValues([Item("updated", 2, rev: 2, physicalMs: 200)]); // after cut → rolled back

            backend.CreateCheckpointAsOf(cpDir, appliedIndex: 10, cut: T(100), ct: TestContext.Current.CancellationToken);
        }

        using SqlitePersistenceBackend r = new(cpDir, "v1");
        Assert.NotNull(r.GetKeyValue("keep"));
        Assert.Null(r.GetKeyValue("future"));

        KeyValueEntry? updated = r.GetKeyValue("updated");
        Assert.NotNull(updated);
        Assert.Equal(1, updated!.Revision);        // rolled back to the pre-cut revision
        Assert.Equal((byte)1, updated.Value![0]);
    }

    [Fact]
    public void RocksDb_CreateCheckpointAsOf_CutsAtTimestamp()
    {
        string baseDir = Path.Combine(_tempRoot, "asof_rocks_base");
        Directory.CreateDirectory(baseDir);
        string cpDir = Path.Combine(baseDir, "cp");

        using (RocksDbPersistenceBackend backend = new(baseDir, "v1"))
        {
            backend.StoreKeyValues([Item("keep", 1, rev: 1, physicalMs: 50)]);
            backend.StoreKeyValues([Item("future", 1, rev: 1, physicalMs: 200)]);
            backend.StoreKeyValues([Item("updated", 1, rev: 1, physicalMs: 50)]);
            backend.StoreKeyValues([Item("updated", 2, rev: 2, physicalMs: 200)]);

            backend.CreateCheckpointAsOf(cpDir, appliedIndex: 10, cut: T(100), ct: TestContext.Current.CancellationToken);
        }

        using RocksDbPersistenceBackend r = new(baseDir, "cp");
        Assert.NotNull(r.GetKeyValue("keep"));
        Assert.Null(r.GetKeyValue("future"));

        KeyValueEntry? updated = r.GetKeyValue("updated");
        Assert.NotNull(updated);
        Assert.Equal(1, updated!.Revision);        // ~CURRENT reset to the pre-cut revision
        Assert.Equal((byte)1, updated.Value![0]);
    }

    // ── as-of: SetNoRevision keys and locks ──────────────────────────────────────────────────

    private static PersistenceRequestItem NoRevItem(string key, byte val, long rev, long physicalMs) =>
        new(key, [val], rev, 0, 0, 0, 0, physicalMs, 0, 0, physicalMs, 0, 1, noRevision: true);

    // For locks, StoreLocks maps the 'revision' field to the fencing token and 'value' to the owner.
    private static PersistenceRequestItem LockItem(string resource, long physicalMs) =>
        new(resource, [1, 2, 3], 1, 0, 0, 0, 0, 0, 0, 0, physicalMs, 0, 1);

    [Fact]
    public void Memory_CreateCheckpointAsOf_ExcludesLocksKeepsKv()
    {
        string dir = NewDir("asof_locks_mem");
        using MemoryPersistenceBackend b = new();
        Put(b, "k", [1], 1);                         // revisioned KV at LM=1
        b.StoreLocks([LockItem("res", 50)]);

        b.CreateCheckpointAsOf(dir, appliedIndex: 1, cut: T(100), ct: TestContext.Current.CancellationToken);

        MemoryPersistenceBackend r = MemoryPersistenceBackend.OpenCheckpoint(dir);
        Assert.NotNull(r.GetKeyValue("k"));
        Assert.Null(r.GetLock("res"));               // locks excluded from as-of image
    }

    [Fact]
    public void Sqlite_CreateCheckpointAsOf_ExcludesLocksKeepsKv()
    {
        string dbDir = Path.Combine(_tempRoot, "asof_locks_sql_base");
        Directory.CreateDirectory(dbDir);
        string cpDir = Path.Combine(_tempRoot, "asof_locks_sql_cp");

        using (SqlitePersistenceBackend b = new(dbDir, "v1"))
        {
            b.StoreKeyValues([Item("k", 1, rev: 1, physicalMs: 1)]);
            b.StoreLocks([LockItem("res", 50)]);
            b.CreateCheckpointAsOf(cpDir, appliedIndex: 1, cut: T(100), ct: TestContext.Current.CancellationToken);
        }

        using SqlitePersistenceBackend r = new(cpDir, "v1");
        Assert.NotNull(r.GetKeyValue("k"));
        Assert.Null(r.GetLock("res"));
    }

    [Fact]
    public void RocksDb_CreateCheckpointAsOf_ExcludesLocksKeepsKv()
    {
        string baseDir = Path.Combine(_tempRoot, "asof_locks_rocks_base");
        Directory.CreateDirectory(baseDir);
        string cpDir = Path.Combine(baseDir, "cp");

        using (RocksDbPersistenceBackend b = new(baseDir, "v1"))
        {
            b.StoreKeyValues([Item("k", 1, rev: 1, physicalMs: 1)]);
            b.StoreLocks([LockItem("res", 50)]);
            b.CreateCheckpointAsOf(cpDir, appliedIndex: 1, cut: T(100), ct: TestContext.Current.CancellationToken);
        }

        using RocksDbPersistenceBackend r = new(baseDir, "cp");
        Assert.NotNull(r.GetKeyValue("k"));
        Assert.Null(r.GetLock("res"));
    }

    // A key whose only write is a SetNoRevision write AFTER the cut did not exist at the cut, so it
    // is omitted from the as-of image — not treated as a fail-closed error. (Failing the whole backup
    // because one key is newer than the cut is the over-conservative behavior this corrects; the
    // earliest-no-revision provenance now distinguishes "created after the cut" from "overwritten".)
    [Fact]
    public void Memory_CreateCheckpointAsOf_NoRevisionKeyCreatedAfterCut_Omitted()
    {
        string dir = NewDir("asof_norev_mem");
        using MemoryPersistenceBackend b = new();
        b.StoreKeyValues([NoRevItem("nr", 9, rev: 1, physicalMs: 200)]); // only write, after the cut

        b.CreateCheckpointAsOf(dir, appliedIndex: 1, cut: T(100), ct: TestContext.Current.CancellationToken);

        using MemoryPersistenceBackend r = MemoryPersistenceBackend.OpenCheckpoint(dir);
        Assert.Null(r.GetKeyValue("nr"));
    }

    [Fact]
    public void Sqlite_CreateCheckpointAsOf_NoRevisionKeyCreatedAfterCut_Omitted()
    {
        string dbDir = Path.Combine(_tempRoot, "asof_norev_sql");
        Directory.CreateDirectory(dbDir);
        string cpDir = Path.Combine(_tempRoot, "asof_norev_sql_cp");
        using (SqlitePersistenceBackend b = new(dbDir, "v1"))
        {
            b.StoreKeyValues([NoRevItem("nr", 9, rev: 1, physicalMs: 200)]);
            b.CreateCheckpointAsOf(cpDir, appliedIndex: 1, cut: T(100), ct: TestContext.Current.CancellationToken);
        }

        using SqlitePersistenceBackend r = new(cpDir, "v1");
        Assert.Null(r.GetKeyValue("nr"));
    }

    [Fact]
    public void RocksDb_CreateCheckpointAsOf_NoRevisionKeyCreatedAfterCut_Omitted()
    {
        string baseDir = Path.Combine(_tempRoot, "asof_norev_rocks");
        Directory.CreateDirectory(baseDir);
        using (RocksDbPersistenceBackend b = new(baseDir, "v1"))
        {
            b.StoreKeyValues([NoRevItem("nr", 9, rev: 1, physicalMs: 200)]);
            b.CreateCheckpointAsOf(Path.Combine(baseDir, "cp"), appliedIndex: 1, cut: T(100), ct: TestContext.Current.CancellationToken);
        }

        using RocksDbPersistenceBackend r = new(baseDir, "cp");
        Assert.Null(r.GetKeyValue("nr"));
    }

    [Fact]
    public void Memory_CreateCheckpointAsOf_NoRevisionKeyAtOrBeforeCut_Kept()
    {
        string dir = NewDir("asof_norev_ok_mem");
        using MemoryPersistenceBackend b = new();
        b.StoreKeyValues([NoRevItem("nr", 9, rev: 1, physicalMs: 100)]); // historyless, == cut

        b.CreateCheckpointAsOf(dir, appliedIndex: 1, cut: T(100), ct: TestContext.Current.CancellationToken);

        MemoryPersistenceBackend r = MemoryPersistenceBackend.OpenCheckpoint(dir);
        Assert.NotNull(r.GetKeyValue("nr"));
    }

    // ── physical purge of post-cut bytes ─────────────────────────────────────────────────────

    private static PersistenceRequestItem ValItem(string key, byte[] value, long rev, long physicalMs) =>
        new(key, value, rev, 0, 0, 0, 0, physicalMs, 0, 0, physicalMs, 0, 1);

    private static readonly byte[] PreCutSentinel = "PRECUT-Sentinel-4a7f9c2e1b6d80"u8.ToArray();
    private static readonly byte[] PostCutSentinel = "POSTCUT-Sentinel-e3d1f0a9b8c76"u8.ToArray();

    private static bool DirContainsBytes(string dir, byte[] needle)
    {
        foreach (string file in Directory.EnumerateFiles(dir, "*", SearchOption.AllDirectories))
        {
            byte[] bytes = File.ReadAllBytes(file);
            if (IndexOf(bytes, needle) >= 0)
                return true;
        }
        return false;
    }

    private static int IndexOf(byte[] haystack, byte[] needle)
    {
        for (int i = 0; i + needle.Length <= haystack.Length; i++)
        {
            int j = 0;
            while (j < needle.Length && haystack[i + j] == needle[j]) j++;
            if (j == needle.Length) return i;
        }
        return -1;
    }

    [Fact]
    public void Sqlite_CreateCheckpointAsOf_PhysicallyPurgesPostCutBytes()
    {
        string dbDir = Path.Combine(_tempRoot, "purge_sql_base");
        Directory.CreateDirectory(dbDir);
        string cpDir = Path.Combine(_tempRoot, "purge_sql_cp");

        using (SqlitePersistenceBackend b = new(dbDir, "v1"))
        {
            b.StoreKeyValues([ValItem("k", PreCutSentinel, rev: 1, physicalMs: 50)]);   // ≤ cut
            b.StoreKeyValues([ValItem("k", PostCutSentinel, rev: 2, physicalMs: 200)]); // > cut → purged
            b.CreateCheckpointAsOf(cpDir, appliedIndex: 1, cut: T(100), ct: TestContext.Current.CancellationToken);
        }

        Assert.True(DirContainsBytes(cpDir, PreCutSentinel), "pre-cut value must remain restorable");
        Assert.False(DirContainsBytes(cpDir, PostCutSentinel), "post-cut value must be physically absent");
    }

    [Fact]
    public void RocksDb_CreateCheckpointAsOf_PhysicallyPurgesPostCutBytes()
    {
        string baseDir = Path.Combine(_tempRoot, "purge_rocks_base");
        Directory.CreateDirectory(baseDir);
        string cpDir = Path.Combine(baseDir, "cp");

        using (RocksDbPersistenceBackend b = new(baseDir, "v1"))
        {
            b.StoreKeyValues([ValItem("k", PreCutSentinel, rev: 1, physicalMs: 50)]);
            b.StoreKeyValues([ValItem("k", PostCutSentinel, rev: 2, physicalMs: 200)]);
            b.CreateCheckpointAsOf(cpDir, appliedIndex: 1, cut: T(100), ct: TestContext.Current.CancellationToken);
        }

        Assert.True(DirContainsBytes(cpDir, PreCutSentinel), "pre-cut value must remain restorable");
        Assert.False(DirContainsBytes(cpDir, PostCutSentinel), "post-cut value must be physically absent");
    }

    [Fact]
    public void RocksDb_CreateCheckpointAsOf_LargeStore_StreamsCorrectly()
    {
        // Exercises the streaming, bounded-batch trim across the flush threshold with many keys.
        string baseDir = Path.Combine(_tempRoot, "large_rocks_base");
        Directory.CreateDirectory(baseDir);
        string cpDir = Path.Combine(baseDir, "cp");

        const int n = 6000; // > TrimBatchFlushThreshold (4096)
        using (RocksDbPersistenceBackend b = new(baseDir, "v1"))
        {
            for (int i = 0; i < n; i++)
            {
                b.StoreKeyValues([Item($"k/{i:D5}", (byte)(i & 0xFF), rev: 1, physicalMs: 50)]);   // ≤ cut
                b.StoreKeyValues([Item($"k/{i:D5}", (byte)0xEE, rev: 2, physicalMs: 200)]);         // > cut
            }
            b.CreateCheckpointAsOf(cpDir, appliedIndex: 1, cut: T(100), ct: TestContext.Current.CancellationToken);
        }

        using RocksDbPersistenceBackend r = new(baseDir, "cp");
        for (int i = 0; i < n; i++)
        {
            KeyValueEntry? e = r.GetKeyValue($"k/{i:D5}");
            Assert.NotNull(e);
            Assert.Equal(1, e!.Revision);                 // rolled back to the pre-cut revision
            Assert.Equal((byte)(i & 0xFF), e.Value![0]);
        }
    }
}
