
using Kahuna.Server.Persistence.Backend;
using Kahuna.Server.Persistence.Pitr;
using Kommander.Time;

namespace Kahuna.Tests.Server;

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
}
