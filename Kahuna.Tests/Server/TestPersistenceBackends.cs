
using Kommander;
using Kahuna.Server.KeyValues;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Microsoft.Data.Sqlite;

namespace Kahuna.Tests.Server;

/// <summary>
/// Tests for GetKeyValueByRange across all three IPersistenceBackend implementations.
/// </summary>
public class TestPersistenceBackends
{
    private static PersistenceRequestItem MakeItem(string key, long revision, long lastModifiedPhysical = 0) =>
        new(key,
            System.Text.Encoding.UTF8.GetBytes("val" + revision),
            revision: revision,
            expiresNode: 0, expiresPhysical: 0, expiresCounter: 0,
            lastUsedNode: 0, lastUsedPhysical: 0, lastUsedCounter: 0,
            lastModifiedNode: 0, lastModifiedPhysical: lastModifiedPhysical,
            lastModifiedCounter: 0,
            state: (int)KeyValueState.Set);

    private static PersistenceRequestItem MakeItem(string key, int i) => MakeItem(key, (long)i);

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
    public void TestMemoryPruneKeyValueRevisionsIsNoOp()
    {
        using MemoryPersistenceBackend backend = new();
        backend.StoreKeyValues(MakeItems("svc", 3));

        Assert.True(backend.PruneKeyValueRevisions(["svc/0000"], retentionCount: 1, TimeSpan.FromHours(1), batchSize: 100, out RevisionPruneResult targeted));
        Assert.Equal(0, targeted.KeysVisited);
        Assert.Equal(0, targeted.RevisionsDeleted);
        Assert.False(targeted.BatchLimitReached);

        Assert.True(backend.PruneKeyValueRevisions(null, retentionCount: 3, TimeSpan.Zero, batchSize: 50, out RevisionPruneResult sweep));
        Assert.Equal(0, sweep.KeysVisited);
        Assert.Equal(0, sweep.RevisionsDeleted);
        Assert.False(sweep.BatchLimitReached);

        Assert.True(backend.PruneKeyValueRevisions(["svc/0000"], retentionCount: 1, TimeSpan.FromHours(1), batchSize: 100, out RevisionPruneResult secondPass));
        Assert.Equal(targeted, secondPass);
    }

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

    private static void StoreRevisions(SqlitePersistenceBackend backend, string key, int count)
    {
        for (long revision = 1; revision <= count; revision++)
            backend.StoreKeyValues([MakeItem(key, revision)]);
    }

    private static int CountSqliteRevisionRows(string path, string dbRevision, string key)
    {
        int shard = (int)HashUtils.InversePrefixedHash(key, '/', 8);
        using SqliteConnection connection = new($"Data Source={path}/kahuna{shard}_{dbRevision}.db");
        connection.Open();

        using SqliteCommand command = new("SELECT COUNT(*) FROM keys_revisions WHERE key = @key", connection);
        command.Parameters.AddWithValue("@key", key);
        return Convert.ToInt32(command.ExecuteScalar());
    }

    [Fact]
    public void TestSqliteRetentionCountPrunesOldRevisions()
    {
        const string key = "retention/count";
        string path = SqliteTempPath();
        using SqlitePersistenceBackend backend = new(path, "revtest");

        StoreRevisions(backend, key, 10);

        Assert.True(backend.PruneKeyValueRevisions([key], retentionCount: 3, TimeSpan.Zero, batchSize: 1000, out RevisionPruneResult result));
        Assert.False(result.BatchLimitReached);
        Assert.Equal(3, CountSqliteRevisionRows(path, "revtest", key));

        KeyValueEntry? current = backend.GetKeyValue(key);
        Assert.NotNull(current);
        Assert.Equal(10, current.Revision);
        Assert.Equal("val10", System.Text.Encoding.UTF8.GetString(current.Value!));

        Assert.NotNull(backend.GetKeyValueRevision(key, 10));
        Assert.Null(backend.GetKeyValueRevision(key, 7));
        Assert.Null(backend.GetKeyValueRevision(key, 1));
    }

    [Fact]
    public void TestSqliteRetentionCountPreservesCurrentRevisionWhenRetentionIsOne()
    {
        const string key = "retention/current";
        string path = SqliteTempPath();
        using SqlitePersistenceBackend backend = new(path, "revtest");

        StoreRevisions(backend, key, 5);

        Assert.True(backend.PruneKeyValueRevisions([key], retentionCount: 1, TimeSpan.Zero, batchSize: 1000, out _));

        Assert.Equal(1, CountSqliteRevisionRows(path, "revtest", key));
        Assert.NotNull(backend.GetKeyValueRevision(key, 5));
        Assert.Null(backend.GetKeyValueRevision(key, 4));
    }

    [Fact]
    public void TestSqliteRetentionBatchSizeLimitsDeletesAndReportsBacklog()
    {
        const string key = "retention/batch";
        string path = SqliteTempPath();
        using SqlitePersistenceBackend backend = new(path, "revtest");

        StoreRevisions(backend, key, 10);

        Assert.True(backend.PruneKeyValueRevisions([key], retentionCount: 3, TimeSpan.Zero, batchSize: 2, out RevisionPruneResult firstPass));
        Assert.Equal(2, firstPass.RevisionsDeleted);
        Assert.True(firstPass.BatchLimitReached);
        Assert.True(CountSqliteRevisionRows(path, "revtest", key) > 3);

        Assert.True(backend.PruneKeyValueRevisions([key], retentionCount: 3, TimeSpan.Zero, batchSize: 1000, out RevisionPruneResult secondPass));
        Assert.False(secondPass.BatchLimitReached);
        Assert.Equal(3, CountSqliteRevisionRows(path, "revtest", key));
    }

    [Fact]
    public void TestSqliteRetentionAgePrunesOldRowsButProtectsCurrentRevision()
    {
        const string key = "retention/age";
        string path = SqliteTempPath();
        using SqlitePersistenceBackend backend = new(path, "revtest");

        long now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        for (long revision = 1; revision <= 5; revision++)
        {
            long lastModifiedPhysical = revision < 5 ? now - 3_600_000 : now;
            backend.StoreKeyValues([MakeItem(key, revision, lastModifiedPhysical)]);
        }

        Assert.True(backend.PruneKeyValueRevisions(
            [key],
            retentionCount: 0,
            TimeSpan.FromMinutes(30),
            batchSize: 1000,
            out RevisionPruneResult result));

        Assert.False(result.BatchLimitReached);
        Assert.Equal(1, CountSqliteRevisionRows(path, "revtest", key));
        Assert.NotNull(backend.GetKeyValue(key));
        Assert.Equal(5, backend.GetKeyValue(key)!.Revision);
        Assert.NotNull(backend.GetKeyValueRevision(key, 5));
        Assert.Null(backend.GetKeyValueRevision(key, 1));
    }

    [Fact]
    public void TestSqliteRetentionSweepPrunesWithoutTargetedKeys()
    {
        const string key = "retention/sweep";
        string path = SqliteTempPath();
        using SqlitePersistenceBackend backend = new(path, "revtest");

        StoreRevisions(backend, key, 8);

        Assert.True(backend.PruneKeyValueRevisions(null, retentionCount: 2, TimeSpan.Zero, batchSize: 1000, out RevisionPruneResult result));
        Assert.Equal(2, CountSqliteRevisionRows(path, "revtest", key));
        Assert.NotNull(backend.GetKeyValueRevision(key, 8));
        Assert.Null(backend.GetKeyValueRevision(key, 6));
    }

    [Fact]
    public void TestSqliteTargetedBacklogRequeuesOnlyKeysWithRemainingWork()
    {
        // keyA and keyB share the "ret" prefix so they hash to the same shard and are processed
        // in input order. keyA is fully prunable within the batch; keyB still has work left.
        const string keyA = "ret/a";
        const string keyB = "ret/b";
        string path = SqliteTempPath();
        using SqlitePersistenceBackend backend = new(path, "revtest");

        StoreRevisions(backend, keyA, 4);   // retentionCount 3 → 1 prunable
        StoreRevisions(backend, keyB, 10);  // retentionCount 3 → 7 prunable

        // batchSize 4: keyA fully pruned (1 delete), keyB consumes the remaining 3 and still has work.
        Assert.True(backend.PruneKeyValueRevisions([keyA, keyB], retentionCount: 3, TimeSpan.Zero, batchSize: 4, out RevisionPruneResult result));

        Assert.True(result.BatchLimitReached);
        Assert.NotNull(result.RemainingKeys);
        Assert.Equal([keyB], result.RemainingKeys!.ToArray());

        // keyA is done; keyB still over retention.
        Assert.Equal(3, CountSqliteRevisionRows(path, "revtest", keyA));
        Assert.True(CountSqliteRevisionRows(path, "revtest", keyB) > 3);
    }

    [Fact]
    public void TestSqliteSweepIsBoundedAndResumesAcrossPasses()
    {
        // 20 keys under one prefix (same shard), each with 4 revisions and 2 prunable at count 2.
        string path = SqliteTempPath();
        using SqlitePersistenceBackend backend = new(path, "revtest");

        for (int k = 0; k < 20; k++)
            StoreRevisions(backend, $"swp/k{k:D2}", 4);

        const int budget = 5;
        int passes = 0;
        int totalDeleted = 0;

        while (true)
        {
            Assert.True(backend.PruneKeyValueRevisions(null, retentionCount: 2, TimeSpan.Zero, batchSize: budget, out RevisionPruneResult r));

            // Each pass is bounded: at most `budget` keys visited and `budget` deletes.
            Assert.True(r.KeysVisited <= budget);
            Assert.True(r.RevisionsDeleted <= budget);

            totalDeleted += r.RevisionsDeleted;
            passes++;

            if (!r.BatchLimitReached)
                break;

            Assert.True(passes < 100, "sweep did not converge");
        }

        // Bounded passes means the full scan took more than one pass.
        Assert.True(passes > 1);
        Assert.Equal(40, totalDeleted);

        for (int k = 0; k < 20; k++)
            Assert.Equal(2, CountSqliteRevisionRows(path, "revtest", $"swp/k{k:D2}"));
    }

    [Fact]
    public void TestSqliteSweepWithNothingPrunableScansBoundedThenReportsNoBacklog()
    {
        // 20 keys, each with a single revision: nothing is ever prunable at count 2.
        string path = SqliteTempPath();
        using SqlitePersistenceBackend backend = new(path, "revtest");

        for (int k = 0; k < 20; k++)
            StoreRevisions(backend, $"none/k{k:D2}", 1);

        const int budget = 5;
        int passes = 0;
        bool sawBacklog = false;

        while (true)
        {
            Assert.True(backend.PruneKeyValueRevisions(null, retentionCount: 2, TimeSpan.Zero, batchSize: budget, out RevisionPruneResult r));

            Assert.Equal(0, r.RevisionsDeleted);
            Assert.True(r.KeysVisited <= budget);

            passes++;
            if (r.BatchLimitReached)
                sawBacklog = true;
            else
                break;

            Assert.True(passes < 100, "sweep did not converge");
        }

        // The scan was chunked across multiple bounded passes, then settled to no backlog.
        Assert.True(sawBacklog);
        Assert.True(passes > 1);
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

    private static void StoreRevisionsRocksDb(RocksDbPersistenceBackend backend, string key, int count)
    {
        for (long revision = 1; revision <= count; revision++)
            backend.StoreKeyValues([MakeItem(key, revision)]);
    }

    private static int CountRocksDbRevisionEntries(RocksDbPersistenceBackend backend, string key)
    {
        // Exercise GetKeyValueRevision for each possible revision up to a generous upper bound.
        // Count only entries that are found (non-null) and are not the CURRENT read path.
        // We do this by probing a range wider than any test writes (1..100).
        int count = 0;
        for (long rev = 1; rev <= 100; rev++)
        {
            if (backend.GetKeyValueRevision(key, rev) is not null)
                count++;
        }
        return count;
    }

    [Fact]
    public void TestRocksDbRetentionCountPrunesOldRevisions()
    {
        const string key = "retention/count";
        string path = RocksDbTempPath();
        using RocksDbPersistenceBackend backend = new(path, "v1");

        StoreRevisionsRocksDb(backend, key, 10);

        Assert.True(backend.PruneKeyValueRevisions([key], retentionCount: 3, TimeSpan.Zero, batchSize: 1000, out RevisionPruneResult result));
        Assert.False(result.BatchLimitReached);
        Assert.Equal(7, result.RevisionsDeleted);
        Assert.Equal(3, CountRocksDbRevisionEntries(backend, key));

        KeyValueEntry? current = backend.GetKeyValue(key);
        Assert.NotNull(current);
        Assert.Equal(10, current.Revision);
        Assert.Equal("val10", System.Text.Encoding.UTF8.GetString(current.Value!));

        Assert.NotNull(backend.GetKeyValueRevision(key, 10));
        Assert.Null(backend.GetKeyValueRevision(key, 7));
        Assert.Null(backend.GetKeyValueRevision(key, 1));
    }

    [Fact]
    public void TestRocksDbRetentionCountPreservesCurrentRevisionWhenRetentionIsOne()
    {
        const string key = "retention/current";
        string path = RocksDbTempPath();
        using RocksDbPersistenceBackend backend = new(path, "v1");

        StoreRevisionsRocksDb(backend, key, 5);

        Assert.True(backend.PruneKeyValueRevisions([key], retentionCount: 1, TimeSpan.Zero, batchSize: 1000, out _));

        Assert.Equal(1, CountRocksDbRevisionEntries(backend, key));
        Assert.NotNull(backend.GetKeyValueRevision(key, 5));
        Assert.Null(backend.GetKeyValueRevision(key, 4));
    }

    [Fact]
    public void TestRocksDbRetentionBatchSizeLimitsDeletesAndReportsBacklog()
    {
        const string key = "retention/batch";
        string path = RocksDbTempPath();
        using RocksDbPersistenceBackend backend = new(path, "v1");

        StoreRevisionsRocksDb(backend, key, 10);

        Assert.True(backend.PruneKeyValueRevisions([key], retentionCount: 3, TimeSpan.Zero, batchSize: 2, out RevisionPruneResult firstPass));
        Assert.Equal(2, firstPass.RevisionsDeleted);
        Assert.True(firstPass.BatchLimitReached);
        Assert.True(CountRocksDbRevisionEntries(backend, key) > 3);

        Assert.True(backend.PruneKeyValueRevisions([key], retentionCount: 3, TimeSpan.Zero, batchSize: 1000, out RevisionPruneResult secondPass));
        Assert.False(secondPass.BatchLimitReached);
        Assert.Equal(3, CountRocksDbRevisionEntries(backend, key));
    }

    [Fact]
    public void TestRocksDbRetentionAgePrunesOldRowsButProtectsCurrentRevision()
    {
        const string key = "retention/age";
        string path = RocksDbTempPath();
        using RocksDbPersistenceBackend backend = new(path, "v1");

        long now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        for (long revision = 1; revision <= 5; revision++)
        {
            long lastModified = revision < 5 ? now - 3_600_000 : now;
            backend.StoreKeyValues([MakeItem(key, revision, lastModified)]);
        }

        Assert.True(backend.PruneKeyValueRevisions([key], retentionCount: 0, TimeSpan.FromMinutes(30), batchSize: 1000, out RevisionPruneResult result));
        Assert.False(result.BatchLimitReached);
        Assert.Equal(1, CountRocksDbRevisionEntries(backend, key));
        Assert.NotNull(backend.GetKeyValue(key));
        Assert.Equal(5, backend.GetKeyValue(key)!.Revision);
        Assert.NotNull(backend.GetKeyValueRevision(key, 5));
        Assert.Null(backend.GetKeyValueRevision(key, 1));
    }

    [Fact]
    public void TestRocksDbRetentionSweepPrunesWithoutTargetedKeys()
    {
        const string key = "retention/sweep";
        string path = RocksDbTempPath();
        using RocksDbPersistenceBackend backend = new(path, "v1");

        StoreRevisionsRocksDb(backend, key, 8);

        Assert.True(backend.PruneKeyValueRevisions(null, retentionCount: 2, TimeSpan.Zero, batchSize: 1000, out RevisionPruneResult result));
        Assert.Equal(2, CountRocksDbRevisionEntries(backend, key));
        Assert.NotNull(backend.GetKeyValueRevision(key, 8));
        Assert.Null(backend.GetKeyValueRevision(key, 6));
    }

    [Fact]
    public void TestRocksDbRetentionCurrentMarkerNeverDeleted()
    {
        const string key = "retention/marker";
        string path = RocksDbTempPath();
        using RocksDbPersistenceBackend backend = new(path, "v1");

        StoreRevisionsRocksDb(backend, key, 5);

        Assert.True(backend.PruneKeyValueRevisions([key], retentionCount: 1, TimeSpan.Zero, batchSize: 1000, out _));

        Assert.NotNull(backend.GetKeyValue(key));
        Assert.Equal(5, backend.GetKeyValue(key)!.Revision);
    }

    [Fact]
    public void TestRocksDbRetentionHandlesKeysContainingTilde()
    {
        const string key = "prefix~part/suffix";
        string path = RocksDbTempPath();
        using RocksDbPersistenceBackend backend = new(path, "v1");

        StoreRevisionsRocksDb(backend, key, 6);

        Assert.True(backend.PruneKeyValueRevisions([key], retentionCount: 2, TimeSpan.Zero, batchSize: 1000, out RevisionPruneResult result));
        Assert.Equal(4, result.RevisionsDeleted);
        Assert.Equal(2, CountRocksDbRevisionEntries(backend, key));
        Assert.NotNull(backend.GetKeyValue(key));
        Assert.NotNull(backend.GetKeyValueRevision(key, 6));
        Assert.Null(backend.GetKeyValueRevision(key, 3));
    }

    [Fact]
    public void TestRocksDbTargetedBacklogRequeuesOnlyKeysWithRemainingWork()
    {
        const string keyA = "ret/a";
        const string keyB = "ret/b";
        string path = RocksDbTempPath();
        using RocksDbPersistenceBackend backend = new(path, "v1");

        StoreRevisionsRocksDb(backend, keyA, 4);   // retentionCount 3 → 1 prunable
        StoreRevisionsRocksDb(backend, keyB, 10);  // retentionCount 3 → 7 prunable

        // batchSize 4: keyA fully pruned (1 delete), keyB consumes the remaining 3 and still has work.
        Assert.True(backend.PruneKeyValueRevisions([keyA, keyB], retentionCount: 3, TimeSpan.Zero, batchSize: 4, out RevisionPruneResult result));

        Assert.True(result.BatchLimitReached);
        Assert.NotNull(result.RemainingKeys);
        Assert.Equal([keyB], result.RemainingKeys!.ToArray());

        Assert.Equal(3, CountRocksDbRevisionEntries(backend, keyA));
        Assert.True(CountRocksDbRevisionEntries(backend, keyB) > 3);
    }

    [Fact]
    public void TestRocksDbSweepIsBoundedAndResumesAcrossPasses()
    {
        string path = RocksDbTempPath();
        using RocksDbPersistenceBackend backend = new(path, "v1");

        for (int k = 0; k < 20; k++)
            StoreRevisionsRocksDb(backend, $"swp/k{k:D2}", 4);  // 2 prunable each at count 2

        const int budget = 5;
        int passes = 0;
        int totalDeleted = 0;

        while (true)
        {
            Assert.True(backend.PruneKeyValueRevisions(null, retentionCount: 2, TimeSpan.Zero, batchSize: budget, out RevisionPruneResult r));

            Assert.True(r.KeysVisited <= budget);
            Assert.True(r.RevisionsDeleted <= budget);

            totalDeleted += r.RevisionsDeleted;
            passes++;

            if (!r.BatchLimitReached)
                break;

            Assert.True(passes < 100, "sweep did not converge");
        }

        Assert.True(passes > 1);
        Assert.Equal(40, totalDeleted);

        for (int k = 0; k < 20; k++)
            Assert.Equal(2, CountRocksDbRevisionEntries(backend, $"swp/k{k:D2}"));
    }

    [Fact]
    public void TestRocksDbSweepWithNothingPrunableScansBoundedThenReportsNoBacklog()
    {
        string path = RocksDbTempPath();
        using RocksDbPersistenceBackend backend = new(path, "v1");

        for (int k = 0; k < 20; k++)
            StoreRevisionsRocksDb(backend, $"none/k{k:D2}", 1);

        const int budget = 5;
        int passes = 0;
        bool sawBacklog = false;

        while (true)
        {
            Assert.True(backend.PruneKeyValueRevisions(null, retentionCount: 2, TimeSpan.Zero, batchSize: budget, out RevisionPruneResult r));

            Assert.Equal(0, r.RevisionsDeleted);
            Assert.True(r.KeysVisited <= budget);

            passes++;
            if (r.BatchLimitReached)
                sawBacklog = true;
            else
                break;

            Assert.True(passes < 100, "sweep did not converge");
        }

        Assert.True(sawBacklog);
        Assert.True(passes > 1);
    }
}
