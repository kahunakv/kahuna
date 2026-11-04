using System.Security.Cryptography;
using System.Text;
using Kahuna.Server.KeyValues;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using RocksDbSharp;

namespace Kahuna.Server.Tests;

/// <summary>
/// The RocksDB store's answer to storage damage. RocksDB checks every block it reads, so a flipped byte in
/// a data block is found by the next read of that block; what matters is what the backend does with the
/// finding. These tests pin that a damaged block makes a point read, a batched read and a range scan throw
/// <see cref="StorageCorruptionException"/> — a scan never ends short as if the range stopped at the damage
/// — that the first finding fail-stops the store (every later operation is refused, stores return false),
/// that the ENOSPC recovery neither reopens the store nor touches its files, and that a store whose files
/// fail at open, or whose own floor rows cannot be decoded, is refused instead of opened short.
/// </summary>
public sealed class TestStorageCorruption
{
    private const string Revision = "v1";

    private const string Prefix = "corrupt/";

    private const int Keys = 256;

    private const int ValueBytes = 2048;

    [Fact]
    public void RangeScan_OverDamagedBlock_ThrowsAndFailStops_NeverReturnsShort()
    {
        string dir = CreateTempDir("kahuna-corrupt-scan-");
        try
        {
            PrepareFlushedStore(dir);
            string sst = LargestSst(dir);
            FlipByteAtMiddle(sst);
            byte[] damagedBytes = File.ReadAllBytes(sst);

            using RocksDbPersistenceBackend backend = new(dir, Revision);

            // Bookkeeping rows sort first and live in the first block: the open and the identity read see intact data.
            Assert.Null(backend.GetFailStopReason());
            Assert.Null(backend.GetStoreIdentity());

            StorageCorruptionException thrown = Assert.Throws<StorageCorruptionException>(() => backend.GetKeyValueByRange(Prefix, null, Keys * 2));
            Assert.Contains("Corruption:", thrown.Message, StringComparison.Ordinal);
            Assert.Contains("fail-stopped", thrown.Message, StringComparison.Ordinal);

            // Latched: the reason names the engine's status, and every later operation is refused at the fence
            // with the same exception, whether or not it would touch the damaged block.
            Assert.NotNull(backend.GetFailStopReason());
            Assert.Contains("Corruption:", backend.GetFailStopReason(), StringComparison.Ordinal);
            Assert.Throws<StorageCorruptionException>(() => backend.GetKeyValue(Key(0)));
            Assert.Throws<StorageCorruptionException>(() => backend.GetKeyValueByPrefix(Prefix));
            Assert.Throws<StorageCorruptionException>(() => backend.GetStoreIdentity());
            Assert.Throws<StorageCorruptionException>(() => backend.GetDurabilityFloor(1));
            Assert.False(backend.StoreKeyValues([MakeItem("corrupt/after", 1)]));
            Assert.False(backend.StoreDurabilityFloors([(1, 10L)]));

            // Fail-stop, not reopen: recovery declines, the store stays latched, and the damaged file is byte-for-byte as it was.
            Assert.False(backend.TryRecoverFromStorageFailure());
            Assert.NotNull(backend.GetFailStopReason());
            Assert.Throws<StorageCorruptionException>(() => backend.GetKeyValue(Key(0)));
            Assert.True(File.Exists(sst));
            Assert.Equal(damagedBytes, File.ReadAllBytes(sst));
        }
        finally
        {
            TryDeleteDir(dir);
        }
    }

    [Fact]
    public void PointRead_OfDamagedBlock_ThrowsStorageCorruption()
    {
        string dir = CreateTempDir("kahuna-corrupt-point-");
        try
        {
            PrepareFlushedStore(dir);
            FlipByteAtMiddle(LargestSst(dir));

            using RocksDbPersistenceBackend backend = new(dir, Revision);

            // The damaged block holds some of the keys; a point read of one of them is the first to fail,
            // and it fails with the terminal exception — never with a null ("does not exist") or a transient one.
            int served = 0;
            StorageCorruptionException? thrown = null;

            for (int i = 0; i < Keys && thrown is null; i++)
            {
                try
                {
                    ReadOnlyKeyValueEntry? entry = backend.GetKeyValue(Key(i));
                    Assert.NotNull(entry);
                    served++;
                }
                catch (StorageCorruptionException ex)
                {
                    thrown = ex;
                }
            }

            Assert.NotNull(thrown);
            Assert.True(served < Keys, "no point read reached the damaged block");
            Assert.NotNull(backend.GetFailStopReason());
        }
        finally
        {
            TryDeleteDir(dir);
        }
    }

    [Fact]
    public void BatchedRead_OfDamagedBlock_ThrowsStorageCorruption()
    {
        string dir = CreateTempDir("kahuna-corrupt-multiget-");
        try
        {
            PrepareFlushedStore(dir);
            FlipByteAtMiddle(LargestSst(dir));

            using RocksDbPersistenceBackend backend = new(dir, Revision);

            string[] keys = new string[Keys];
            for (int i = 0; i < Keys; i++)
                keys[i] = Key(i);

            // A batched read over every key reaches the damaged block; the binding must surface the per-key
            // error as an exception, never as a missing value.
            Assert.Throws<StorageCorruptionException>(() => backend.GetKeyValues(keys));
            Assert.NotNull(backend.GetFailStopReason());
        }
        finally
        {
            TryDeleteDir(dir);
        }
    }

    [Fact]
    public void WholeFamilyScan_OverDamagedBlock_Throws()
    {
        string dir = CreateTempDir("kahuna-corrupt-family-");
        try
        {
            PrepareFlushedStore(dir);
            FlipByteAtMiddle(LargestSst(dir));

            using RocksDbPersistenceBackend backend = new(dir, Revision);

            Assert.Throws<StorageCorruptionException>(() =>
            {
                string? cursor = null;
                do
                {
                    KeyValueScanPage page = backend.ScanKeyValues(cursor, 32);
                    cursor = page.NextCursor;
                } while (cursor is not null);
            });
        }
        finally
        {
            TryDeleteDir(dir);
        }
    }

    [Fact]
    public void TruncatedSst_RefusesToOpen()
    {
        string dir = CreateTempDir("kahuna-corrupt-truncated-");
        try
        {
            PrepareFlushedStore(dir);
            string sst = LargestSst(dir);

            using (FileStream stream = new(sst, FileMode.Open, FileAccess.ReadWrite))
                stream.SetLength(stream.Length - 1024);

            StorageCorruptionException thrown = Assert.Throws<StorageCorruptionException>(() => new RocksDbPersistenceBackend(dir, Revision));
            Assert.Contains("Corruption:", thrown.Message, StringComparison.Ordinal);
        }
        finally
        {
            TryDeleteDir(dir);
        }
    }

    [Fact]
    public void DurabilityFloorRowOfWrongLength_RefusesToOpen()
    {
        string dir = CreateTempDir("kahuna-corrupt-floor-");
        try
        {
            using (RocksDbPersistenceBackend backend = new(dir, Revision))
            {
                Assert.True(backend.StoreKeyValues([MakeItem(Key(0), 1)]));
                Assert.True(backend.StoreDurabilityFloors([(1, 10L)]));
            }

            PutRawKvRow(dir, " durability_floor_1"u8.ToArray(), [1, 2, 3, 4, 5]);

            StorageCorruptionException thrown = Assert.Throws<StorageCorruptionException>(() => new RocksDbPersistenceBackend(dir, Revision));
            Assert.Contains("durability floor row", thrown.Message, StringComparison.Ordinal);
            Assert.Contains("5 bytes", thrown.Message, StringComparison.Ordinal);

            // The refusal released the engine: a corrected row opens again.
            PutRawKvRow(dir, " durability_floor_1"u8.ToArray(), BitConverter.GetBytes(10L));

            using RocksDbPersistenceBackend reopened = new(dir, Revision);
            Assert.Equal(10L, reopened.GetDurabilityFloor(1));
        }
        finally
        {
            TryDeleteDir(dir);
        }
    }

    [Fact]
    public void UndecodableIdentityRow_IsDamage()
    {
        string dir = CreateTempDir("kahuna-corrupt-identity-");
        try
        {
            using (RocksDbPersistenceBackend backend = new(dir, Revision))
                Assert.True(backend.WriteStoreIdentity(PersistentStoreIdentity.Create("localhost:1")));

            PutRawKvRow(dir, "\0store_identity"u8.ToArray(), Encoding.UTF8.GetBytes("garbage"));

            using RocksDbPersistenceBackend reopened = new(dir, Revision);
            Assert.Throws<StorageCorruptionException>(() => reopened.GetStoreIdentity());
            Assert.NotNull(reopened.GetFailStopReason());
        }
        finally
        {
            TryDeleteDir(dir);
        }
    }

    // ── Scaffolding ──────────────────────────────────────────────────────────────────────

    private static string Key(int i) => $"{Prefix}k{i:D4}";

    /// <summary>Writes the keys with incompressible values, flushes them to an SST and closes the store.</summary>
    private static void PrepareFlushedStore(string dir)
    {
        using RocksDbPersistenceBackend backend = new(dir, Revision);

        List<PersistenceRequestItem> items = new(Keys);
        for (int i = 0; i < Keys; i++)
        {
            byte[] value = new byte[ValueBytes];
            RandomNumberGenerator.Fill(value);
            items.Add(MakeItem(Key(i), 1, value));
        }

        Assert.True(backend.StoreKeyValues(items));
        Assert.True(backend.StoreDurabilityFloors([(1, 5L)]));
        backend.FlushMemTablesForTesting();
    }

    private static string LargestSst(string dir)
    {
        string[] files = Directory.GetFiles(Path.Combine(dir, Revision), "*.sst");
        Assert.NotEmpty(files);
        return files.OrderByDescending(f => new FileInfo(f).Length).First();
    }

    /// <summary>
    /// Flips one byte in the middle of the file. Data blocks come first in the block-based table and the
    /// index and footer last, so with hundreds of kilobytes of data the middle lies inside a data block of
    /// user rows, well past the first block that holds the store's bookkeeping rows.
    /// </summary>
    private static void FlipByteAtMiddle(string sst)
    {
        using FileStream stream = new(sst, FileMode.Open, FileAccess.ReadWrite);
        long offset = stream.Length / 2;
        stream.Position = offset;
        int original = stream.ReadByte();
        stream.Position = offset;
        stream.WriteByte((byte)(original ^ 0xFF));
    }

    /// <summary>Writes one raw row into the closed store's kv family through the binding itself.</summary>
    private static void PutRawKvRow(string dir, byte[] key, byte[] value)
    {
        ColumnFamilies families = new()
        {
            { "default", new ColumnFamilyOptions() },
            { "kv", new ColumnFamilyOptions() },
            { "locks", new ColumnFamilyOptions() }
        };

        using RocksDb db = RocksDb.Open(new DbOptions().SetCreateIfMissing(false), Path.Combine(dir, Revision), families);
        db.Put(key, value, cf: db.GetColumnFamily("kv"), writeOptions: new WriteOptions().SetSync(true));
    }

    private static PersistenceRequestItem MakeItem(string key, long revision, byte[]? value = null) =>
        new(key,
            value ?? Encoding.UTF8.GetBytes("val" + revision),
            revision: revision,
            expiresNode: 0, expiresPhysical: 0, expiresCounter: 0,
            lastUsedNode: 0, lastUsedPhysical: 0, lastUsedCounter: 0,
            lastModifiedNode: 0, lastModifiedPhysical: revision,
            lastModifiedCounter: 0,
            state: (int)KeyValueState.Set);

    private static string CreateTempDir(string prefix)
    {
        string path = Path.Combine(Path.GetTempPath(), prefix + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(path);
        return path;
    }

    private static void TryDeleteDir(string path)
    {
        try { if (Directory.Exists(path)) Directory.Delete(path, recursive: true); }
        catch { /* best-effort cleanup */ }
    }
}
