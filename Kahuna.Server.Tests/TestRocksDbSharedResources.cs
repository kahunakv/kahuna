using Kommander;
using Kommander.Data;
using Kommander.WAL;
using Kahuna.Server.KeyValues;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Shared.KeyValue;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kahuna.Server.Tests;

/// <summary>
/// Validates that a Kahuna RocksDB persistence backend and a Kommander <see cref="RocksDbWAL"/> can share
/// one <see cref="RocksDbSharedResources"/> bundle (block cache + WriteBufferManager) so both draw from a
/// single unified memory budget. Kommander's own suite proves the bundle in isolation and two WALs sharing
/// it; these tests cover the cross-component boundary: the KV/locks backend participating in the same
/// budget as the Raft WAL, backward compatibility of the null path, and the ownership contract.
///
/// Each test allocates a modest in-memory budget (no disk-level tuning needed for correctness) and cleans
/// up its temp directories unconditionally.
/// </summary>
public sealed class TestRocksDbSharedResources
{
    // ── helpers ──────────────────────────────────────────────────────────────────

    private static string TempPath(string tag)
    {
        string p = Path.Combine(Path.GetTempPath(), $"kahuna-shared-{tag}-{Guid.NewGuid():N}");
        Directory.CreateDirectory(p);
        return p;
    }

    private static void Cleanup(params string[] paths)
    {
        foreach (string p in paths)
            if (Directory.Exists(p))
                Directory.Delete(p, recursive: true);
    }

    private static PersistenceRequestItem MakeItem(string key, long revision, int valueBytes = 8)
    {
        byte[] value = new byte[valueBytes];
        // Fill with a non-zero, revision-dependent pattern so entries are distinct and compress poorly,
        // giving the memtable a realistic footprint.
        for (int i = 0; i < value.Length; i++)
            value[i] = (byte)(revision + i);

        return new(key, value,
            revision: revision,
            expiresNode: 0, expiresPhysical: 0, expiresCounter: 0,
            lastUsedNode: 0, lastUsedPhysical: 0, lastUsedCounter: 0,
            lastModifiedNode: 0, lastModifiedPhysical: 0, lastModifiedCounter: 0,
            state: (int)KeyValueState.Set);
    }

    private static List<PersistenceRequestItem> MakeItems(string prefix, int count, int valueBytes = 8) =>
        Enumerable.Range(0, count)
                  .Select(i => MakeItem($"{prefix}/{i:D4}", i, valueBytes))
                  .ToList();

    // ── Backward compatibility: the null path is byte-for-byte the default ─────────

    [Fact]
    public void Backend_NullBundle_AndBundle_ProduceIdenticalReads()
    {
        string pathNull = TempPath("null");
        string pathBundle = TempPath("bundle");
        try
        {
            using RocksDbSharedResources res = RocksDbSharedResources.CreateWithUnifiedBudget(
                256 * 1024 * 1024, 64 * 1024 * 1024);

            List<(string, ReadOnlyKeyValueEntry)> withoutBundle;
            using (RocksDbPersistenceBackend backend = new(pathNull, "v1", sharedResources: null))
            {
                backend.StoreKeyValues(MakeItems("svc", 25));
                withoutBundle = backend.GetKeyValueByRange("svc", null, 100);
            }

            List<(string, ReadOnlyKeyValueEntry)> withBundle;
            using (RocksDbPersistenceBackend backend = new(pathBundle, "v1", sharedResources: res))
            {
                backend.StoreKeyValues(MakeItems("svc", 25));
                withBundle = backend.GetKeyValueByRange("svc", null, 100);
            }

            Assert.Equal(withoutBundle.Count, withBundle.Count);
            for (int i = 0; i < withoutBundle.Count; i++)
            {
                Assert.Equal(withoutBundle[i].Item1, withBundle[i].Item1);
                Assert.Equal(withoutBundle[i].Item2.Revision, withBundle[i].Item2.Revision);
            }
        }
        finally { Cleanup(pathNull, pathBundle); }
    }

    // ── The backend's memtables are charged to the shared budget ──────────────────

    /// <summary>
    /// Opens only the Kahuna backend on a shared bundle and writes to it. The bundle's
    /// <see cref="RocksDbSharedResources.MemtableMemoryUsage"/> must rise, proving the backend's memtables
    /// are tracked by the shared WriteBufferManager (not a private per-DB budget).
    /// </summary>
    [Fact]
    public void Backend_WithSharedBundle_ChargesSharedMemtableBudget()
    {
        string path = TempPath("charge");
        try
        {
            using RocksDbSharedResources res = RocksDbSharedResources.CreateWithUnifiedBudget(
                256 * 1024 * 1024, 64 * 1024 * 1024);

            long usageBefore = res.MemtableMemoryUsage;

            using RocksDbPersistenceBackend backend = new(path, "v1", sharedResources: res);

            // Enough volume to leave a visible memtable footprint well below the 64 MiB flush threshold.
            backend.StoreKeyValues(MakeItems("svc", 500, valueBytes: 1024));

            long usageAfter = res.MemtableMemoryUsage;

            Assert.True(usageAfter > usageBefore,
                $"Expected the shared WBM to track the backend's memtables. Before: {usageBefore}, After: {usageAfter}");
        }
        finally { Cleanup(path); }
    }

    /// <summary>
    /// The core scenario: one bundle shared by the Kahuna backend AND a Kommander <see cref="RocksDbWAL"/>.
    /// Writes flow to both; the single WBM's usage reflects the combined activity — one budget, not two.
    /// </summary>
    [Fact]
    public void BackendAndWal_ShareOneBundle_BothDriveMemtableBudget()
    {
        string backendPath = TempPath("shared-backend");
        string walPath = TempPath("shared-wal");
        try
        {
            using RocksDbSharedResources res = RocksDbSharedResources.CreateWithUnifiedBudget(
                256 * 1024 * 1024, 64 * 1024 * 1024);

            long usageBefore = res.MemtableMemoryUsage;

            using RocksDbPersistenceBackend backend = new(backendPath, "v1", sharedResources: res);
            using RocksDbWAL wal = new(walPath, "wal", NullLogger<IRaft>.Instance,
                syncWrites: false, sharedResources: res);

            backend.StoreKeyValues(MakeItems("svc", 300, valueBytes: 1024));

            byte[] payload = new byte[1024];
            for (long id = 1; id <= 300; id++)
                wal.Write([(1, [new RaftLog { Id = id, Term = 1, Type = RaftLogType.Committed, LogData = payload }])]);

            long usageAfter = res.MemtableMemoryUsage;

            Assert.True(usageAfter > usageBefore,
                $"Expected one shared WBM to track both the backend and the WAL. Before: {usageBefore}, After: {usageAfter}");

            // Both databases remain correct with the shared bundle wired in.
            Assert.Equal(300, backend.GetKeyValueByRange("svc", null, 1000).Count);
            Assert.Equal(300, wal.ReadLogs(1).Count);
        }
        finally { Cleanup(backendPath, walPath); }
    }

    // ── Ownership / lifetime contract ─────────────────────────────────────────────

    /// <summary>
    /// Correct teardown: close the borrowing databases, then dispose the bundle. No crash.
    /// </summary>
    [Fact]
    public void DisposeOrder_DatabasesFirst_ThenBundle_NoCrash()
    {
        string backendPath = TempPath("order-a-backend");
        string walPath = TempPath("order-a-wal");
        try
        {
            RocksDbSharedResources res = RocksDbSharedResources.CreateWithUnifiedBudget(
                64 * 1024 * 1024, 16 * 1024 * 1024);

            RocksDbPersistenceBackend backend = new(backendPath, "v1", sharedResources: res);
            RocksDbWAL wal = new(walPath, "wal", NullLogger<IRaft>.Instance,
                syncWrites: false, sharedResources: res);

            backend.Dispose();
            wal.Dispose();
            res.Dispose(); // both DBs closed before the bundle is released — the intended order
        }
        finally { Cleanup(backendPath, walPath); }
    }

    /// <summary>
    /// Disposing the bundle before the databases is a usage error (budget accounting), but must not crash:
    /// each open DB holds its own native refcount on the cache and WBM.
    /// </summary>
    [Fact]
    public void DisposeOrder_BundleFirst_ThenDatabases_DoesNotCrash()
    {
        string backendPath = TempPath("order-b-backend");
        string walPath = TempPath("order-b-wal");
        try
        {
            RocksDbSharedResources res = RocksDbSharedResources.CreateWithUnifiedBudget(
                64 * 1024 * 1024, 16 * 1024 * 1024);

            RocksDbPersistenceBackend backend = new(backendPath, "v1", sharedResources: res);
            RocksDbWAL wal = new(walPath, "wal", NullLogger<IRaft>.Instance,
                syncWrites: false, sharedResources: res);

            res.Dispose(); // released early — safe because the DBs hold their own shared_ptr refs

            backend.Dispose();
            wal.Dispose();
        }
        finally { Cleanup(backendPath, walPath); }
    }
}
