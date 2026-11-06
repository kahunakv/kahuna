
using System.Text;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Handlers;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Communication.Memory;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kahuna.Server.Tests;

/// <summary>
/// What a persistent range scan page allocates on the in-memory backend, and the two steps that decide it: the
/// backend page read and the as-of projection.
///
/// <para>A page used to cost several objects per row: a copy of the stored row in the backend, an entry built
/// only to evaluate a row that is not resident, a response per evaluated key, and lists that grew by doubling
/// from empty. A row now costs the one row object the page returns, and each list is allocated once at its
/// size. The guards here measure a page on the calling thread and hold it to that budget, so any of those
/// per-row objects coming back fails a test instead of showing up in a profile of a consumer.</para>
/// </summary>
public sealed class TestScanPageAllocation : RaftTrackingTest
{
    /// <summary>
    /// What one more row adds to a page, with every key only in the backend and with every key also resident in
    /// the actor's store: the row object the page returns and one slot in each of three row lists (the backend
    /// page, the resident batch, the result). The cost is taken as the difference between a long page and a
    /// short one, so it does not depend on what a page costs before its first row.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void EachRowOfAPage_CostsOneRowObjectAndItsListSlots(bool resident)
    {
        (KeyValueContext context, MemoryPersistenceBackend backend) = CreateContext("scan-alloc-page-" + resident);

        Seed(context, backend, rows: 1_000, resident);

        TryGetByRangeHandler handler = new(context);

        long shortPage = MeasurePage(context, handler, RangeScan("doc/", 100), expectedRows: 100, expectMore: true);
        long longPage = MeasurePage(context, handler, RangeScan("doc/", 500), expectedRows: 500, expectMore: true);

        double perRow = (longPage - shortPage) / 400.0;
        double budget = RowObjectBytes() + 3 * ListSlotBytes + RoundingBytesPerRow;

        Assert.True(
            perRow <= budget,
            $"a row of a page costs {perRow:F1} bytes (100 rows: {shortPage}, 500 rows: {longPage}); the budget is {budget:F1}: " +
            $"one row object of {RowObjectBytes()} bytes and three list slots of {ListSlotBytes}");
    }

    /// <summary>
    /// The bounded seek of an index probe: a short run of rows behind a page size of 512. The page is sized by
    /// the rows the bound admits, so the same seek costs the same whatever the page size is, its rows cost what
    /// the rows of any page cost, and what it costs before its first row stays small.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void BoundedSeek_IsSizedByItsRows_NotByThePageSize(bool resident)
    {
        (KeyValueContext context, MemoryPersistenceBackend backend) = CreateContext("scan-alloc-seek-" + resident);

        Seed(context, backend, rows: 2_000, resident);

        TryGetByRangeHandler handler = new(context);

        // Rows doc/000100 .. doc/000110, and doc/000100 .. doc/000210.
        long elevenRows = MeasurePage(context, handler, BoundedScan("doc/000100", "doc/000111", limit: 512), expectedRows: 11, expectMore: false);
        long elevenRowsSmallPage = MeasurePage(context, handler, BoundedScan("doc/000100", "doc/000111", limit: 16), expectedRows: 11, expectMore: false);
        long manyRows = MeasurePage(context, handler, BoundedScan("doc/000100", "doc/000211", limit: 512), expectedRows: 111, expectMore: false);

        Assert.True(
            Math.Abs(elevenRows - elevenRowsSmallPage) <= 64,
            $"an 11-row seek allocated {elevenRows} bytes with a page size of 512 and {elevenRowsSmallPage} with a page size of 16");

        double perRow = (manyRows - elevenRows) / 100.0;
        double rowBudget = RowObjectBytes() + 3 * ListSlotBytes + RoundingBytesPerRow;

        Assert.True(perRow <= rowBudget, $"a row of a bounded seek costs {perRow:F1} bytes; the budget is {rowBudget:F1}");

        long fixedCost = elevenRows - (long)(11 * perRow);

        Assert.True(
            fixedCost <= FixedPageBytes,
            $"an 11-row seek allocated {elevenRows} bytes, {fixedCost} of them before its first row; the budget for those is {FixedPageBytes}");
    }

    // ── The backend page ──────────────────────────────────────────────────────────────────

    /// <summary>
    /// The in-memory backend finds both ends of a page by binary search and hands out its stored rows. For random
    /// prefixes, start keys, end bounds and limits the page must be what a plain walk gives: the keys from the
    /// seek position that carry the prefix, up to the bound and the limit, in order; and each row must be the
    /// stored object, in a list that holds exactly the page.
    /// </summary>
    [Fact]
    public void BackendPage_MatchesAPlainWalk_AndHandsOutTheStoredRows()
    {
        Random random = new(20261106);
        MemoryPersistenceBackend backend = new();

        SortedSet<string> keys = new(StringComparer.Ordinal);
        List<PersistenceRequestItem> rows = [];

        // Buckets whose names are prefixes of each other, a bucket with a tilde sibling, and keys with no bucket.
        string[] buckets = ["a", "a/b", "a0", "ab", "b", "b~", "c"];
        for (int i = 0; i < 1_500; i++)
        {
            string key = random.Next(12) == 0
                ? $"loose{random.Next(40):D2}"
                : $"{buckets[random.Next(buckets.Length)]}/{random.Next(400):D4}";

            if (keys.Add(key))
                rows.Add(Row(key));
        }

        Assert.True(backend.StoreKeyValues(rows));

        List<string> ordered = [.. keys];

        for (int probe = 0; probe < 3_000; probe++)
        {
            string prefix = random.Next(6) switch
            {
                0 => "",
                1 => buckets[random.Next(buckets.Length)],
                2 => "zz/",
                _ => buckets[random.Next(buckets.Length)] + "/"
            };

            string? startKey = random.Next(3) == 0 ? null : PickKey(random, ordered, buckets);
            string? endKey = random.Next(3) == 0 ? null : PickKey(random, ordered, buckets);
            bool endInclusive = random.Next(2) == 0;
            int limit = random.Next(7) switch { 0 => 0, 1 => 1, 2 => int.MaxValue, _ => random.Next(1, 600) };

            KeyValueScanTally tally = default;
            List<(string Key, ReadOnlyKeyValueEntry Entry)> page =
                backend.GetKeyValueByRange(prefix, startKey, endKey, endInclusive, limit, ref tally);

            List<string> expected = PlainWalk(ordered, prefix, startKey, endKey, endInclusive, limit);

            Assert.Equal(expected, page.Select(static row => row.Key).ToList());
            Assert.True(page.Capacity == page.Count, $"the page holds {page.Count} rows in a list of capacity {page.Capacity}");

            foreach ((string key, ReadOnlyKeyValueEntry entry) in page)
                Assert.Same(backend.GetKeyValue(key), entry);
        }

        // The prefix read of a bucket scan is the same run with no bound.
        foreach (string bucket in buckets)
        {
            string prefix = bucket + "/";
            List<(string Key, ReadOnlyKeyValueEntry Entry)> byPrefix = backend.GetKeyValueByPrefix(prefix);

            Assert.Equal(ordered.Where(key => key.StartsWith(prefix, StringComparison.Ordinal)).ToList(), byPrefix.Select(static row => row.Key).ToList());
            Assert.All(byPrefix, row => Assert.Same(backend.GetKeyValue(row.Key), row.Entry));
        }
    }

    /// <summary>
    /// A row is replaced by a later write and never changed, so a page read before the write still shows the
    /// version it read: handing out the stored row is as safe as the copy it replaced.
    /// </summary>
    [Fact]
    public void BackendPage_ARowReadBeforeALaterWrite_StillShowsTheVersionItRead()
    {
        MemoryPersistenceBackend backend = new();
        Assert.True(backend.StoreKeyValues([Row("doc/a", "first", revision: 1, physical: 1_000)]));

        KeyValueScanTally tally = default;
        ReadOnlyKeyValueEntry read = Assert.Single(backend.GetKeyValueByRange("doc/", null, null, false, 10, ref tally)).Item2;

        Assert.True(backend.StoreKeyValues([Row("doc/a", "second", revision: 2, physical: 2_000)]));

        Assert.Equal(1L, read.Revision);
        Assert.Equal("first", Encoding.UTF8.GetString(read.Value!));
        Assert.Equal(2L, backend.GetKeyValue("doc/a")!.Revision);
    }

    // ── The as-of projection ──────────────────────────────────────────────────────────────

    /// <summary>
    /// A page with no row past the snapshot is its own projection: the raw page is handed on, with no second list.
    /// </summary>
    [Fact]
    public void Projection_NoRowPastTheSnapshot_HandsOnTheRawPage()
    {
        MemoryPersistenceBackend backend = new();
        Assert.True(backend.StoreKeyValues([Row("doc/a", physical: 1_000), Row("doc/b", physical: 1_500), Row("doc/c", physical: 2_000)]));

        KeyValueScanTally tally = default;
        List<(string, ReadOnlyKeyValueEntry)> raw = backend.GetKeyValueByRange("doc/", null, null, false, 10, ref tally);

        RangeDiskPage page = TryGetByRangeHandler.ProjectSnapshotPage(
            raw, pageSize: 10, isSnapshotRead: true, snapshotTs: new HLCTimestamp(0, 2_000, 0), currentTime: new HLCTimestamp(0, 9_000, 0), backend);

        Assert.Same(raw, page.Projected);
        Assert.False(page.RawHasMore);
        Assert.Null(page.RawNextCursor);
        Assert.Null(page.HistoryFencedKeys);
    }

    /// <summary>
    /// The copy starts at the first row past the snapshot. The rows before it are carried over as they are, a row
    /// with an older revision at or before the snapshot is replaced by that revision (the backend's own row), a
    /// row with none is dropped, and the rows after it follow. The raw pagination facts do not depend on what
    /// the projection dropped, and the raw page itself is not changed.
    /// </summary>
    [Fact]
    public void Projection_RowsPastTheSnapshot_AreReplacedOrDropped_AndTheRestCarriedOver()
    {
        MemoryPersistenceBackend backend = new();
        Assert.True(backend.StoreKeyValues(
        [
            Row("doc/a", physical: 1_000),
            Row("doc/b", "b-old", revision: 1, physical: 1_200),
            Row("doc/b", "b-new", revision: 2, physical: 3_000),
            Row("doc/c", "c-new", revision: 1, physical: 3_500),
            Row("doc/d", physical: 1_800)
        ]));

        KeyValueScanTally tally = default;
        List<(string Key, ReadOnlyKeyValueEntry Entry)> raw = backend.GetKeyValueByRange("doc/", null, null, false, 4, ref tally);
        List<(string Key, ReadOnlyKeyValueEntry Entry)> rawBefore = [.. raw];

        RangeDiskPage page = TryGetByRangeHandler.ProjectSnapshotPage(
            raw, pageSize: 4, isSnapshotRead: true, snapshotTs: new HLCTimestamp(0, 2_000, 0), currentTime: new HLCTimestamp(0, 9_000, 0), backend);

        Assert.NotSame(raw, page.Projected);
        Assert.Equal(rawBefore, raw);

        Assert.Equal(["doc/a", "doc/b", "doc/d"], page.Projected.Select(static row => row.Key).ToList());
        Assert.Same(raw[0].Entry, page.Projected[0].Entry);
        Assert.Equal("b-old", Encoding.UTF8.GetString(page.Projected[1].Entry.Value!));
        Assert.Same(backend.GetKeyValueRevision("doc/b", 1), page.Projected[1].Entry);
        Assert.Same(raw[3].Entry, page.Projected[2].Entry);

        // The raw page was full, so more rows may follow it, whatever the projection kept.
        Assert.True(page.RawHasMore);
        Assert.Equal("doc/d", page.RawNextCursor);

        // doc/c had no revision at or before the snapshot: it is counted as an inspected, dead entry.
        Assert.Equal(1, page.Tally.DeadEntriesSkipped);
    }

    /// <summary>
    /// An expired continuation stops the projection. The page then holds only the rows projected so far, also
    /// when none of them was past the snapshot and no copy had started.
    /// </summary>
    [Fact]
    public void Projection_AbortedBeforeAnyRowPastTheSnapshot_KeepsOnlyTheRowsProjectedSoFar()
    {
        MemoryPersistenceBackend backend = new();
        Assert.True(backend.StoreKeyValues([Row("doc/a"), Row("doc/b"), Row("doc/c"), Row("doc/d")]));

        KeyValueScanTally tally = default;
        List<(string Key, ReadOnlyKeyValueEntry Entry)> raw = backend.GetKeyValueByRange("doc/", null, null, false, 10, ref tally);

        int polls = 0;

        RangeDiskPage page = TryGetByRangeHandler.ProjectSnapshotPage(
            raw, pageSize: 10, isSnapshotRead: true, snapshotTs: new HLCTimestamp(0, 5_000, 0), currentTime: new HLCTimestamp(0, 9_000, 0), backend,
            shouldAbort: () => ++polls > 2);

        Assert.Equal(["doc/a", "doc/b"], page.Projected.Select(static row => row.Key).ToList());
        Assert.Equal(4, raw.Count);
    }

    // ── helpers ───────────────────────────────────────────────────────────────────────────

    /// <summary>The slots of a row list: a key reference and a row reference.</summary>
    private const int ListSlotBytes = 16;

    /// <summary>Room for the array headers of the lists, spread over the rows of the difference measured.</summary>
    private const double RoundingBytesPerRow = 2;

    /// <summary>
    /// What a page may allocate that does not grow with its rows: the continuation, the page and result objects,
    /// the list headers and the response.
    /// </summary>
    private const int FixedPageBytes = 3_072;

    /// <summary>The size of one returned row object, measured, so the budget follows the runtime's layout.</summary>
    private static long RowObjectBytes()
    {
        const int samples = 1_000;
        ReadOnlyKeyValueEntry[] keep = new ReadOnlyKeyValueEntry[samples];

        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < samples; i++)
            keep[i] = new ReadOnlyKeyValueEntry(null, i, HLCTimestamp.Zero, HLCTimestamp.Zero, HLCTimestamp.Zero, KeyValueState.Set);
        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        GC.KeepAlive(keep);
        return allocated / samples;
    }

    /// <summary>
    /// Runs the page on the calling thread, as the actor runs it for a backend that serves reads from memory, and
    /// returns the bytes one page allocates: the smallest of several runs after a warm-up. Only the page is inside
    /// the measured region; its answer is checked afterwards.
    /// </summary>
    private static long MeasurePage(KeyValueContext context, TryGetByRangeHandler handler, KeyValueRequest request, int expectedRows, bool expectMore)
    {
        KeyValueResponse? RunPage()
        {
            HLCTimestamp currentTime = context.Raft.HybridLogicalClock.TrySendOrLocalEvent(context.Raft.GetLocalNodeId());
            RangeScanContinuation scan = handler.CreatePersistentScan(request, currentTime, default);
            return scan.RunInlinePages(context, long.MaxValue);
        }

        KeyValueResponse? response = null;

        for (int i = 0; i < 30; i++)
            response = RunPage();

        long smallest = long.MaxValue;

        for (int run = 0; run < 5; run++)
        {
            long before = GC.GetAllocatedBytesForCurrentThread();
            response = RunPage();
            smallest = Math.Min(smallest, GC.GetAllocatedBytesForCurrentThread() - before);
        }

        Assert.NotNull(response);
        Assert.Equal(KeyValueResponseType.Get, response.Type);
        Assert.Equal(expectedRows, response.RangeResult!.Items.Count);
        Assert.Equal(expectMore, response.RangeResult.HasMore);

        return smallest;
    }

    private static KeyValueRequest BoundedScan(string startKey, string endKey, int limit)
    {
        KeyValueRequest request = RangeScan("doc/", limit);
        request.StartKey = startKey;
        request.StartInclusive = true;
        request.EndKey = endKey;
        request.EndInclusive = false;
        return request;
    }

    private static void Seed(KeyValueContext context, MemoryPersistenceBackend backend, int rows, bool resident)
    {
        byte[] value = Encoding.UTF8.GetBytes("{\"id\":1,\"name\":\"scan page row\"}");
        List<PersistenceRequestItem> items = new(rows);

        for (int i = 0; i < rows; i++)
        {
            string key = $"doc/{i:D6}";
            items.Add(Row(key, value, revision: 1, physical: 1_000));

            if (!resident)
                continue;

            // The resident copy of the same committed row, as a read that loaded it would leave it.
            context.InsertStoreEntry(key, new KeyValueEntry
            {
                Bucket = "doc",
                Value = value,
                State = KeyValueState.Set,
                LastUsed = new HLCTimestamp(0, 1_000, 0),
                LastModified = new HLCTimestamp(0, 1_000, 0),
                Revision = 1,
                FlushedRevision = 1
            });
        }

        Assert.True(backend.StoreKeyValues(items));
    }

    private static PersistenceRequestItem Row(string key, string value = "v", long revision = 1, long physical = 1_000) =>
        Row(key, Encoding.UTF8.GetBytes(value), revision, physical);

    private static PersistenceRequestItem Row(string key, byte[] value, long revision, long physical) =>
        new(
            key: key,
            value: value,
            revision: revision,
            expiresNode: 0, expiresPhysical: 0, expiresCounter: 0,
            lastUsedNode: 0, lastUsedPhysical: physical, lastUsedCounter: 0,
            lastModifiedNode: 0, lastModifiedPhysical: physical, lastModifiedCounter: 0,
            state: (int)KeyValueState.Set);

    private static string PickKey(Random random, List<string> ordered, string[] buckets) =>
        random.Next(5) switch
        {
            0 => ordered[random.Next(ordered.Count)],
            1 => buckets[random.Next(buckets.Length)] + "/",
            2 => buckets[random.Next(buckets.Length)] + "0",
            3 => "",
            _ => $"{buckets[random.Next(buckets.Length)]}/{random.Next(400):D4}x"
        };

    /// <summary>The page by the definition of the read: seek, then walk while the prefix, the bound and the limit hold.</summary>
    private static List<string> PlainWalk(List<string> ordered, string prefix, string? startKey, string? endKey, bool endInclusive, int limit)
    {
        string seek = startKey is not null && string.CompareOrdinal(startKey, prefix) > 0 ? startKey : prefix;
        List<string> page = [];

        foreach (string key in ordered)
        {
            if (page.Count >= limit)
                break;

            if (string.CompareOrdinal(key, seek) < 0)
                continue;

            if (endKey is not null)
            {
                int cmp = string.CompareOrdinal(key, endKey);
                if (cmp > 0 || (cmp == 0 && !endInclusive))
                    break;
            }

            if (!key.StartsWith(prefix, StringComparison.Ordinal))
                break;

            page.Add(key);
        }

        return page;
    }

    private static KeyValueRequest RangeScan(string prefix, int limit)
    {
        KeyValueRequest request = new(
            KeyValueRequestType.GetByRange,
            HLCTimestamp.Zero, HLCTimestamp.Zero,
            prefix,
            null, null, -1,
            KeyValueFlags.None, 0,
            HLCTimestamp.Zero,
            KeyValueDurability.Persistent,
            0, 1, default);

        request.Limit = limit;
        request.StartInclusive = true;
        return request;
    }

    private (KeyValueContext, MemoryPersistenceBackend) CreateContext(string nodeName)
    {
        ILogger<IKahuna> logger = NullLogger<IKahuna>.Instance;
        ILogger<IRaft> raftLogger = NullLogger<IRaft>.Instance;

        RaftManager raft = Track(new RaftManager(
            new RaftConfiguration
            {
                NodeName = nodeName,
                NodeId = 1,
                Host = "localhost",
                Port = 0,
                InitialPartitions = 1,
                EnableQuiescence = false,
                PartitionExecutorPoolSize = 1
            },
            new StaticDiscovery([]),
            new InMemoryWAL(raftLogger),
            new InMemoryCommunication(),
            new HybridLogicalClock(),
            raftLogger));

        KahunaConfiguration configuration = ConfigurationValidator.Validate(new()
        {
            LocksWorkers = 1,
            KeyValueWorkers = 1,
            BackgroundWriterWorkers = 1,
            Storage = "memory",
            CacheEntryTtl = TimeSpan.FromMinutes(30),
            CacheEntriesToRemove = 1000,
            MaxEntriesPerActor = 1_000_000,
            MaxBytesPerActor = 1024L * 1024 * 1024,
            CollectBatchMax = 1000,
            RevisionRetention = 16
        });

        MemoryPersistenceBackend backend = new();

        KeyValueContext context = new(
            null!,
            new KeyValueStore(),
            new Dictionary<string, KeyValueWriteIntent>(),
            new Dictionary<string, List<KeyValueRangeLock>>(),
            new Dictionary<int, KeyValueProposal>(),
            null!,
            null!,
            backend,
            raft,
            raft.ReadScheduler,
            new KeySpaceRegistry(),
            new RangeMapStore(raft, null, null, logger),
            configuration,
            logger);

        return (context, backend);
    }
}
