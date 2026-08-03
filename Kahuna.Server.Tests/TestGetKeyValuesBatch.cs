
using System.Text;
using Kahuna.Server.KeyValues;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;

namespace Kahuna.Server.Tests;

/// <summary>
/// Tests for <see cref="IPersistenceBackend.GetKeyValues"/> — the batched point lookup used by
/// the backend read scheduler to serve a drained burst of point reads in one storage call.
///
/// The contract under test: for every key, the batched result slot must be indistinguishable
/// from an individual <see cref="IPersistenceBackend.GetKeyValue"/> call (value, revision,
/// timestamps, state; <c>null</c> for a missing key), index-aligned with the input. Covers the
/// RocksDB override (native MultiGet) and the interface's default per-key loop (Memory, SQLite).
/// </summary>
public sealed class TestGetKeyValuesBatch : IDisposable
{
    private readonly string _tempRoot = Path.Combine(Path.GetTempPath(), "kahuna-multiget-" + Guid.NewGuid().ToString("N")[..8]);

    public TestGetKeyValuesBatch()
    {
        Directory.CreateDirectory(_tempRoot);
    }

    public void Dispose()
    {
        try { Directory.Delete(_tempRoot, recursive: true); } catch { /* best-effort cleanup */ }
    }

    private IPersistenceBackend CreateBackend(string storage)
    {
        string dir = Path.Combine(_tempRoot, storage + "_" + Guid.NewGuid().ToString("N")[..6]);
        Directory.CreateDirectory(dir);

        return storage switch
        {
            "rocksdb" => new RocksDbPersistenceBackend(dir, "mg"),
            "sqlite"  => new SqlitePersistenceBackend(dir, "mg"),
            "memory"  => new MemoryPersistenceBackend(),
            _         => throw new ArgumentException($"unknown storage '{storage}'", nameof(storage))
        };
    }

    private static PersistenceRequestItem MakeItem(string key, long revision) =>
        new(key,
            Encoding.UTF8.GetBytes("val-" + key + "-" + revision),
            revision: revision,
            expiresNode: 0, expiresPhysical: 0, expiresCounter: 0,
            lastUsedNode: 0, lastUsedPhysical: 100 + revision, lastUsedCounter: 0,
            lastModifiedNode: 0, lastModifiedPhysical: 200 + revision, lastModifiedCounter: 0,
            state: (int)KeyValueState.Set);

    private static void AssertSameAsPointRead(IPersistenceBackend backend, string[] keys, KeyValueEntry?[] batch)
    {
        Assert.Equal(keys.Length, batch.Length);

        for (int i = 0; i < keys.Length; i++)
        {
            KeyValueEntry? single = backend.GetKeyValue(keys[i]);

            if (single is null)
            {
                Assert.Null(batch[i]);
                continue;
            }

            Assert.NotNull(batch[i]);
            Assert.Equal(single.Value, batch[i]!.Value);
            Assert.Equal(single.Revision, batch[i]!.Revision);
            Assert.Equal(single.Expires, batch[i]!.Expires);
            Assert.Equal(single.LastUsed, batch[i]!.LastUsed);
            Assert.Equal(single.LastModified, batch[i]!.LastModified);
            Assert.Equal(single.State, batch[i]!.State);
        }
    }

    [Theory]
    [InlineData("rocksdb")]
    [InlineData("sqlite")]
    [InlineData("memory")]
    public void MixedPresentAndMissingKeys_MatchIndividualGets(string storage)
    {
        IPersistenceBackend backend = CreateBackend(storage);
        try
        {
            backend.StoreKeyValues([MakeItem("svc/a", 1), MakeItem("svc/b", 7), MakeItem("svc/c", 3)]);

            // Missing keys interleaved at the front, middle, and back so an off-by-one in
            // index alignment cannot cancel out.
            string[] keys = ["svc/zz-missing", "svc/b", "svc/a", "svc/nope", "svc/c", "svc/also-missing"];

            KeyValueEntry?[] batch = backend.GetKeyValues(keys);

            Assert.Null(batch[0]);
            Assert.NotNull(batch[1]);
            Assert.Equal(7, batch[1]!.Revision);
            Assert.Null(batch[3]);
            Assert.Null(batch[5]);

            AssertSameAsPointRead(backend, keys, batch);
        }
        finally
        {
            (backend as IDisposable)?.Dispose();
        }
    }

    [Theory]
    [InlineData("rocksdb")]
    [InlineData("sqlite")]
    [InlineData("memory")]
    public void AllMissingKeys_ReturnAllNulls(string storage)
    {
        IPersistenceBackend backend = CreateBackend(storage);
        try
        {
            string[] keys = ["ghost/1", "ghost/2", "ghost/3"];

            KeyValueEntry?[] batch = backend.GetKeyValues(keys);

            Assert.Equal(3, batch.Length);
            Assert.All(batch, Assert.Null);
        }
        finally
        {
            (backend as IDisposable)?.Dispose();
        }
    }

    [Theory]
    [InlineData("rocksdb")]
    [InlineData("sqlite")]
    [InlineData("memory")]
    public void SingleKeyBatch_MatchesIndividualGet(string storage)
    {
        IPersistenceBackend backend = CreateBackend(storage);
        try
        {
            backend.StoreKeyValues([MakeItem("solo", 42)]);

            KeyValueEntry?[] batch = backend.GetKeyValues(["solo"]);

            AssertSameAsPointRead(backend, ["solo"], batch);
        }
        finally
        {
            (backend as IDisposable)?.Dispose();
        }
    }

    /// <summary>
    /// After multiple stores of the same key, the batched read must return the CURRENT
    /// revision — the same current-marker row <see cref="IPersistenceBackend.GetKeyValue"/>
    /// serves — not an older retained revision.
    /// </summary>
    [Theory]
    [InlineData("rocksdb")]
    [InlineData("sqlite")]
    [InlineData("memory")]
    public void OverwrittenKey_BatchReturnsCurrentRevision(string storage)
    {
        IPersistenceBackend backend = CreateBackend(storage);
        try
        {
            backend.StoreKeyValues([MakeItem("hot", 1)]);
            backend.StoreKeyValues([MakeItem("hot", 2)]);
            backend.StoreKeyValues([MakeItem("hot", 3)]);

            KeyValueEntry?[] batch = backend.GetKeyValues(["hot"]);

            Assert.NotNull(batch[0]);
            Assert.Equal(3, batch[0]!.Revision);
            Assert.Equal(Encoding.UTF8.GetBytes("val-hot-3"), batch[0]!.Value);

            AssertSameAsPointRead(backend, ["hot"], batch);
        }
        finally
        {
            (backend as IDisposable)?.Dispose();
        }
    }

    /// <summary>
    /// A larger batch (beyond any small-array fast path) with duplicated keys: every slot must
    /// still align with its own input index, and duplicate keys must each get the full entry.
    /// </summary>
    [Theory]
    [InlineData("rocksdb")]
    [InlineData("sqlite")]
    [InlineData("memory")]
    public void LargeBatchWithDuplicates_EverySlotAligned(string storage)
    {
        IPersistenceBackend backend = CreateBackend(storage);
        try
        {
            List<PersistenceRequestItem> items = new();
            for (int i = 0; i < 40; i++)
                items.Add(MakeItem($"bulk/{i:D4}", i + 1));
            backend.StoreKeyValues(items);

            string[] keys = new string[80];
            for (int i = 0; i < 80; i++)
                keys[i] = (i % 3 == 2) ? $"bulk/missing-{i}" : $"bulk/{i % 40:D4}";

            KeyValueEntry?[] batch = backend.GetKeyValues(keys);

            AssertSameAsPointRead(backend, keys, batch);
        }
        finally
        {
            (backend as IDisposable)?.Dispose();
        }
    }
}
