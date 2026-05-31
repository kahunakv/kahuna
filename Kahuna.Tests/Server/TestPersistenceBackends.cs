
using Kahuna.Server.KeyValues;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;

namespace Kahuna.Tests.Server;

/// <summary>
/// Tests for GetKeyValueByRange across all three IPersistenceBackend implementations.
/// </summary>
public class TestPersistenceBackends
{
    private static PersistenceRequestItem MakeItem(string key, int i) =>
        new(key,
            System.Text.Encoding.UTF8.GetBytes("val" + i),
            revision: i,
            expiresNode: 0, expiresPhysical: 0, expiresCounter: 0,
            lastUsedNode: 0, lastUsedPhysical: 0, lastUsedCounter: 0,
            lastModifiedNode: 0, lastModifiedPhysical: 0, lastModifiedCounter: 0,
            state: (int)KeyValueState.Set);

    private static List<PersistenceRequestItem> MakeItems(string prefix, int count, int startIndex = 0) =>
        Enumerable.Range(startIndex, count)
                  .Select(i => MakeItem($"{prefix}/{i:D4}", i))
                  .ToList();

    private static void AssertOrderedAndBounded(
        List<(string, ReadOnlyKeyValueEntry)> items,
        string prefix,
        int expectedCount)
    {
        Assert.Equal(expectedCount, items.Count);
        for (int i = 0; i < items.Count; i++)
        {
            Assert.StartsWith(prefix, items[i].Item1, StringComparison.Ordinal);
            if (i > 0)
                Assert.True(string.CompareOrdinal(items[i].Item1, items[i - 1].Item1) > 0);
        }
    }

    // ─── MemoryPersistenceBackend ────────────────────────────────────────────────

    [Fact]
    public void TestMemoryGetByRangeReturnsAllInPrefix()
    {
        using MemoryPersistenceBackend backend = new();
        backend.StoreKeyValues(MakeItems("svc", 10));

        List<(string, ReadOnlyKeyValueEntry)> result = backend.GetKeyValueByRange("svc", null, 100);

        AssertOrderedAndBounded(result, "svc", 10);
    }

    [Fact]
    public void TestMemoryGetByRangeLimitCaps()
    {
        using MemoryPersistenceBackend backend = new();
        backend.StoreKeyValues(MakeItems("svc", 10));

        List<(string, ReadOnlyKeyValueEntry)> result = backend.GetKeyValueByRange("svc", null, 3);

        Assert.Equal(3, result.Count);
        Assert.Equal("svc/0000", result[0].Item1);
        Assert.Equal("svc/0001", result[1].Item1);
        Assert.Equal("svc/0002", result[2].Item1);
    }

    [Fact]
    public void TestMemoryGetByRangeStartKeyResumes()
    {
        using MemoryPersistenceBackend backend = new();
        backend.StoreKeyValues(MakeItems("svc", 10));

        List<(string, ReadOnlyKeyValueEntry)> result = backend.GetKeyValueByRange("svc", "svc/0005", 100);

        Assert.Equal(5, result.Count);
        Assert.Equal("svc/0005", result[0].Item1);
        Assert.Equal("svc/0009", result[4].Item1);
    }

    [Fact]
    public void TestMemoryGetByRangeStartKeyBetweenExistingKeys()
    {
        using MemoryPersistenceBackend backend = new();
        // Keys: svc/0000, svc/0002, svc/0004, svc/0006, svc/0008
        backend.StoreKeyValues(Enumerable.Range(0, 5)
            .Select(i => MakeItem($"svc/{i * 2:D4}", i))
            .ToList());

        // "svc/0003" falls between svc/0002 and svc/0004
        List<(string, ReadOnlyKeyValueEntry)> result = backend.GetKeyValueByRange("svc", "svc/0003", 100);

        Assert.Equal(3, result.Count);
        Assert.Equal("svc/0004", result[0].Item1);
        Assert.Equal("svc/0006", result[1].Item1);
        Assert.Equal("svc/0008", result[2].Item1);
    }

    [Fact]
    public void TestMemoryGetByRangePrefixBoundaryExcludesSiblingPrefix()
    {
        using MemoryPersistenceBackend backend = new();
        backend.StoreKeyValues(MakeItems("svc", 5));
        backend.StoreKeyValues(MakeItems("config", 5));

        List<(string, ReadOnlyKeyValueEntry)> result = backend.GetKeyValueByRange("svc", null, 100);

        Assert.Equal(5, result.Count);
        Assert.All(result, item =>
            Assert.StartsWith("svc", item.Item1, StringComparison.Ordinal));
    }

    [Fact]
    public void TestMemoryGetByRangeEmptyRange()
    {
        using MemoryPersistenceBackend backend = new();
        backend.StoreKeyValues(MakeItems("svc", 5));

        List<(string, ReadOnlyKeyValueEntry)> result = backend.GetKeyValueByRange("svc", "svc/9999", 100);

        Assert.Empty(result);
    }

    [Fact]
    public void TestMemoryGetByRangeFullPageCoverage()
    {
        using MemoryPersistenceBackend backend = new();
        backend.StoreKeyValues(MakeItems("svc", 25));

        const int pageSize = 7;
        List<string> allKeys = [];
        string? cursor = null;

        while (true)
        {
            List<(string, ReadOnlyKeyValueEntry)> page = backend.GetKeyValueByRange("svc", cursor, pageSize);
            if (page.Count == 0) break;

            allKeys.AddRange(page.Select(p => p.Item1));
            // GetKeyValueByRange is inclusive-only (no startInclusive parameter).
            // Appending "~￿" skips the last returned key's RocksDB internal
            // entries (~revision / ~CURRENT) and lands before the next logical key
            // because all test keys are fixed-width (svc/NNNN) so no key is a prefix
            // of another. Variable-length keys require the handler's exclusive-start
            // path (startInclusive=false) instead — see KeyValuesManager.LocateAndScanRange.
            cursor = page[^1].Item1 + "~￿";

            if (page.Count < pageSize) break;
        }

        Assert.Equal(25, allKeys.Count);
        Assert.Equal(allKeys.Distinct().Count(), allKeys.Count);
        for (int i = 0; i < allKeys.Count - 1; i++)
            Assert.True(string.CompareOrdinal(allKeys[i], allKeys[i + 1]) < 0);
    }

    // ─── SqlitePersistenceBackend ────────────────────────────────────────────────

    private static string SqliteTempPath()
    {
        string dir = Path.Combine(Path.GetTempPath(), "kahuna_test_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        return dir;
    }

    [Fact]
    public void TestSqliteGetByRangeReturnsAllInPrefix()
    {
        string path = SqliteTempPath();
        using SqlitePersistenceBackend backend = new(path, "v1");
        backend.StoreKeyValues(MakeItems("svc", 10));

        List<(string, ReadOnlyKeyValueEntry)> result = backend.GetKeyValueByRange("svc", null, 100);

        AssertOrderedAndBounded(result, "svc", 10);
    }

    [Fact]
    public void TestSqliteGetByRangeLimitCaps()
    {
        string path = SqliteTempPath();
        using SqlitePersistenceBackend backend = new(path, "v1");
        backend.StoreKeyValues(MakeItems("svc", 10));

        List<(string, ReadOnlyKeyValueEntry)> result = backend.GetKeyValueByRange("svc", null, 4);

        Assert.Equal(4, result.Count);
    }

    [Fact]
    public void TestSqliteGetByRangeStartKeyResumes()
    {
        string path = SqliteTempPath();
        using SqlitePersistenceBackend backend = new(path, "v1");
        backend.StoreKeyValues(MakeItems("svc", 10));

        List<(string, ReadOnlyKeyValueEntry)> result = backend.GetKeyValueByRange("svc", "svc/0005", 100);

        Assert.Equal(5, result.Count);
        Assert.Equal("svc/0005", result[0].Item1);
        Assert.Equal("svc/0009", result[4].Item1);
    }

    [Fact]
    public void TestSqliteGetByRangeStartKeyBetweenExistingKeys()
    {
        string path = SqliteTempPath();
        using SqlitePersistenceBackend backend = new(path, "v1");
        backend.StoreKeyValues(Enumerable.Range(0, 5)
            .Select(i => MakeItem($"svc/{i * 2:D4}", i))
            .ToList());

        List<(string, ReadOnlyKeyValueEntry)> result = backend.GetKeyValueByRange("svc", "svc/0003", 100);

        Assert.Equal(3, result.Count);
        Assert.Equal("svc/0004", result[0].Item1);
    }

    [Fact]
    public void TestSqliteGetByRangePrefixBoundaryExcludesSiblingPrefix()
    {
        string path = SqliteTempPath();
        using SqlitePersistenceBackend backend = new(path, "v1");
        backend.StoreKeyValues(MakeItems("svc", 5));
        backend.StoreKeyValues(MakeItems("config", 5));

        List<(string, ReadOnlyKeyValueEntry)> result = backend.GetKeyValueByRange("svc", null, 100);

        Assert.Equal(5, result.Count);
        Assert.All(result, item =>
            Assert.StartsWith("svc", item.Item1, StringComparison.Ordinal));
    }

    [Fact]
    public void TestSqliteGetByRangeEmptyRange()
    {
        string path = SqliteTempPath();
        using SqlitePersistenceBackend backend = new(path, "v1");
        backend.StoreKeyValues(MakeItems("svc", 5));

        List<(string, ReadOnlyKeyValueEntry)> result = backend.GetKeyValueByRange("svc", "svc/9999", 100);

        Assert.Empty(result);
    }

    [Fact]
    public void TestSqliteGetByRangeFullPageCoverage()
    {
        string path = SqliteTempPath();
        using SqlitePersistenceBackend backend = new(path, "v1");
        backend.StoreKeyValues(MakeItems("svc", 25));

        const int pageSize = 7;
        List<string> allKeys = [];
        string? cursor = null;

        while (true)
        {
            List<(string, ReadOnlyKeyValueEntry)> page = backend.GetKeyValueByRange("svc", cursor, pageSize);
            if (page.Count == 0) break;

            allKeys.AddRange(page.Select(p => p.Item1));
            // GetKeyValueByRange is inclusive-only (no startInclusive parameter).
            // Appending "~￿" skips the last returned key's RocksDB internal
            // entries (~revision / ~CURRENT) and lands before the next logical key
            // because all test keys are fixed-width (svc/NNNN) so no key is a prefix
            // of another. Variable-length keys require the handler's exclusive-start
            // path (startInclusive=false) instead — see KeyValuesManager.LocateAndScanRange.
            cursor = page[^1].Item1 + "~￿";

            if (page.Count < pageSize) break;
        }

        Assert.Equal(25, allKeys.Count);
        Assert.Equal(allKeys.Distinct().Count(), allKeys.Count);
        for (int i = 0; i < allKeys.Count - 1; i++)
            Assert.True(string.CompareOrdinal(allKeys[i], allKeys[i + 1]) < 0);
    }

    // ─── RocksDbPersistenceBackend ───────────────────────────────────────────────

    private static string RocksDbTempPath()
    {
        string dir = Path.Combine(Path.GetTempPath(), "kahuna_rocksdb_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        return dir;
    }

    [Fact]
    public void TestRocksDbGetByRangeReturnsAllInPrefix()
    {
        string path = RocksDbTempPath();
        using RocksDbPersistenceBackend backend = new(path, "v1");
        backend.StoreKeyValues(MakeItems("svc", 10));

        List<(string, ReadOnlyKeyValueEntry)> result = backend.GetKeyValueByRange("svc", null, 100);

        AssertOrderedAndBounded(result, "svc", 10);
    }

    [Fact]
    public void TestRocksDbGetByRangeLimitCaps()
    {
        string path = RocksDbTempPath();
        using RocksDbPersistenceBackend backend = new(path, "v1");
        backend.StoreKeyValues(MakeItems("svc", 10));

        List<(string, ReadOnlyKeyValueEntry)> result = backend.GetKeyValueByRange("svc", null, 4);

        Assert.Equal(4, result.Count);
    }

    [Fact]
    public void TestRocksDbGetByRangeStartKeyResumes()
    {
        string path = RocksDbTempPath();
        using RocksDbPersistenceBackend backend = new(path, "v1");
        backend.StoreKeyValues(MakeItems("svc", 10));

        List<(string, ReadOnlyKeyValueEntry)> result = backend.GetKeyValueByRange("svc", "svc/0005", 100);

        Assert.Equal(5, result.Count);
        Assert.Equal("svc/0005", result[0].Item1);
        Assert.Equal("svc/0009", result[4].Item1);
    }

    [Fact]
    public void TestRocksDbGetByRangeStartKeyBetweenExistingKeys()
    {
        string path = RocksDbTempPath();
        using RocksDbPersistenceBackend backend = new(path, "v1");
        backend.StoreKeyValues(Enumerable.Range(0, 5)
            .Select(i => MakeItem($"svc/{i * 2:D4}", i))
            .ToList());

        // "svc/0003" falls between svc/0002 and svc/0004
        List<(string, ReadOnlyKeyValueEntry)> result = backend.GetKeyValueByRange("svc", "svc/0003", 100);

        Assert.Equal(3, result.Count);
        Assert.Equal("svc/0004", result[0].Item1);
    }

    [Fact]
    public void TestRocksDbGetByRangePrefixBoundaryExcludesSiblingPrefix()
    {
        string path = RocksDbTempPath();
        using RocksDbPersistenceBackend backend = new(path, "v1");
        backend.StoreKeyValues(MakeItems("svc", 5));
        backend.StoreKeyValues(MakeItems("config", 5));

        List<(string, ReadOnlyKeyValueEntry)> result = backend.GetKeyValueByRange("svc", null, 100);

        Assert.Equal(5, result.Count);
        Assert.All(result, item =>
            Assert.StartsWith("svc", item.Item1, StringComparison.Ordinal));
    }

    [Fact]
    public void TestRocksDbGetByRangeEmptyRange()
    {
        string path = RocksDbTempPath();
        using RocksDbPersistenceBackend backend = new(path, "v1");
        backend.StoreKeyValues(MakeItems("svc", 5));

        List<(string, ReadOnlyKeyValueEntry)> result = backend.GetKeyValueByRange("svc", "svc/9999", 100);

        Assert.Empty(result);
    }

    [Fact]
    public void TestRocksDbGetByRangeFullPageCoverage()
    {
        string path = RocksDbTempPath();
        using RocksDbPersistenceBackend backend = new(path, "v1");
        backend.StoreKeyValues(MakeItems("svc", 25));

        const int pageSize = 7;
        List<string> allKeys = [];
        string? cursor = null;

        while (true)
        {
            List<(string, ReadOnlyKeyValueEntry)> page = backend.GetKeyValueByRange("svc", cursor, pageSize);
            if (page.Count == 0) break;

            allKeys.AddRange(page.Select(p => p.Item1));
            // GetKeyValueByRange is inclusive-only (no startInclusive parameter).
            // Appending "~￿" skips the last returned key's RocksDB internal
            // entries (~revision / ~CURRENT) and lands before the next logical key
            // because all test keys are fixed-width (svc/NNNN) so no key is a prefix
            // of another. Variable-length keys require the handler's exclusive-start
            // path (startInclusive=false) instead — see KeyValuesManager.LocateAndScanRange.
            cursor = page[^1].Item1 + "~￿";

            if (page.Count < pageSize) break;
        }

        Assert.Equal(25, allKeys.Count);
        Assert.Equal(allKeys.Distinct().Count(), allKeys.Count);
        for (int i = 0; i < allKeys.Count - 1; i++)
            Assert.True(string.CompareOrdinal(allKeys[i], allKeys[i + 1]) < 0);
    }
}
