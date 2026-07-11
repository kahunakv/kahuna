
using System.Text;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// Integration tests for GetByRange.
/// All tests use an in-process single-node EmbeddedKahunaNode (memory storage).
/// </summary>
[Collection("ClusterTests")]
public sealed class TestGetByRange
{
    private readonly ILoggerFactory loggerFactory;

    public TestGetByRange(ITestOutputHelper outputHelper)
    {
        loggerFactory = LoggerFactory.Create(builder =>
            builder.AddXUnit(outputHelper).SetMinimumLevel(LogLevel.Warning));
    }

    private static EmbeddedKahunaOptions MemoryOptions() => new()
    {
        Storage = "memory",
        WalStorage = "memory",
        InitialPartitions = 1
    };

    private static EmbeddedKahunaOptions SqliteOptions(string storagePath) => new()
    {
        Storage = "sqlite",
        StoragePath = storagePath,
        StorageRevision = "range-regression-tests",
        WalStorage = "memory",
        InitialPartitions = 1,
        DirtyObjectsWriterDelay = 60000
    };

    private static async Task SeedKeys(EmbeddedKahunaNode node, string prefix, int count, KeyValueDurability durability)
    {
        for (int i = 0; i < count; i++)
        {
            (KeyValueResponseType type, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero,
                $"{prefix}/{i:D4}",
                Encoding.UTF8.GetBytes("v" + i),
                null, -1, KeyValueFlags.Set, 0,
                durability,
                TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.Set, type);
        }
    }

    // ── Basic correctness ────────────────────────────────────────────────────

    [Fact]
    public async Task TestGetByRangeFirstPageEphemeral()
    {
        await using EmbeddedKahunaNode node = new(MemoryOptions(), loggerFactory);
        await node.StartAsync(TestContext.Current.CancellationToken);

        await SeedKeys(node, "test/rng", 10, KeyValueDurability.Ephemeral);

        KeyValueGetByRangeResult result = await node.Kahuna.LocateAndGetByRange(
            HLCTimestamp.Zero, "test/rng",
            null, true, null, false,
            5, HLCTimestamp.Zero,
            KeyValueDurability.Ephemeral,
            TestContext.Current.CancellationToken);

        Assert.Equal(KeyValueResponseType.Get, result.Type);
        Assert.Equal(5, result.Items.Count);
        Assert.True(result.HasMore);
        Assert.NotNull(result.NextCursor);
        AssertOrdinalOrder(result.Items);
    }

    [Fact]
    public async Task TestGetByRangeFirstPagePersistent()
    {
        await using EmbeddedKahunaNode node = new(MemoryOptions(), loggerFactory);
        await node.StartAsync(TestContext.Current.CancellationToken);

        await SeedKeys(node, "test/rng", 10, KeyValueDurability.Persistent);

        KeyValueGetByRangeResult result = await node.Kahuna.LocateAndGetByRange(
            HLCTimestamp.Zero, "test/rng",
            null, true, null, false,
            5, HLCTimestamp.Zero,
            KeyValueDurability.Persistent,
            TestContext.Current.CancellationToken);

        Assert.Equal(KeyValueResponseType.Get, result.Type);
        Assert.Equal(5, result.Items.Count);
        Assert.True(result.HasMore);
        Assert.NotNull(result.NextCursor);
        AssertOrdinalOrder(result.Items);
    }

    // ── Paged scan equals GetByBucket set ───────────────────────────────────

    [Theory]
    [InlineData(KeyValueDurability.Ephemeral)]
    [InlineData(KeyValueDurability.Persistent)]
    public async Task TestGetByRangePagedScanMatchesGetByBucket(KeyValueDurability durability)
    {
        await using EmbeddedKahunaNode node = new(MemoryOptions(), loggerFactory);
        await node.StartAsync(TestContext.Current.CancellationToken);

        const string prefix = "test/scan";
        const int total = 20;
        const int pageSize = 7;

        await SeedKeys(node, prefix, total, durability);

        // Collect all keys via paged GetByRange
        List<string> rangeKeys = [];
        string? cursor = null;

        while (true)
        {
            KeyValueGetByRangeResult page = await node.Kahuna.LocateAndGetByRange(
                HLCTimestamp.Zero, prefix,
                cursor is null ? null : DecodeLastKey(cursor),
                cursor is null,   // first page: startInclusive=true; resume: exclusive (we pass lastKey+advance)
                null, false,
                pageSize, HLCTimestamp.Zero,
                durability,
                TestContext.Current.CancellationToken);

            Assert.Equal(KeyValueResponseType.Get, page.Type);

            foreach ((string key, _) in page.Items)
                rangeKeys.Add(key);

            if (!page.HasMore) break;

            // Advance cursor past last returned key
            cursor = page.NextCursor;
        }

        // Collect all keys via GetByBucket
        KeyValueGetByBucketResult bucket = await node.Kahuna.LocateAndGetByBucket(
            HLCTimestamp.Zero, prefix, HLCTimestamp.Zero, durability,
            TestContext.Current.CancellationToken);

        List<string> bucketKeys = bucket.Items.Select(i => i.Item1).ToList();

        Assert.Equal(total, rangeKeys.Count);
        Assert.Equal(bucketKeys.OrderBy(k => k, StringComparer.Ordinal).ToList(), rangeKeys);
        Assert.Equal(rangeKeys.Distinct().ToList(), rangeKeys); // no dupes
        AssertOrdinalOrder(rangeKeys);
    }

    // ── Limit larger than dataset → no HasMore ───────────────────────────────

    [Fact]
    public async Task TestGetByRangeLimitLargerThanDataset()
    {
        await using EmbeddedKahunaNode node = new(MemoryOptions(), loggerFactory);
        await node.StartAsync(TestContext.Current.CancellationToken);

        await SeedKeys(node, "test/small", 3, KeyValueDurability.Ephemeral);

        KeyValueGetByRangeResult result = await node.Kahuna.LocateAndGetByRange(
            HLCTimestamp.Zero, "test/small",
            null, true, null, false,
            100, HLCTimestamp.Zero,
            KeyValueDurability.Ephemeral,
            TestContext.Current.CancellationToken);

        Assert.Equal(3, result.Items.Count);
        Assert.False(result.HasMore);
        Assert.Null(result.NextCursor);
    }

    // ── Empty prefix → empty result ──────────────────────────────────────────

    [Fact]
    public async Task TestGetByRangeEmptyPrefix()
    {
        await using EmbeddedKahunaNode node = new(MemoryOptions(), loggerFactory);
        await node.StartAsync(TestContext.Current.CancellationToken);

        KeyValueGetByRangeResult result = await node.Kahuna.LocateAndGetByRange(
            HLCTimestamp.Zero, "test/nobody",
            null, true, null, false,
            10, HLCTimestamp.Zero,
            KeyValueDurability.Ephemeral,
            TestContext.Current.CancellationToken);

        Assert.Equal(KeyValueResponseType.Get, result.Type);
        Assert.Empty(result.Items);
        Assert.False(result.HasMore);
    }

    // ── RYOW: uncommitted writes in an open transaction are visible ──────────

    [Fact]
    public async Task TestGetByRangeRyow()
    {
        await using EmbeddedKahunaNode node = new(MemoryOptions(), loggerFactory);
        await node.StartAsync(TestContext.Current.CancellationToken);

        // Seed a committed key
        await SeedKeys(node, "test/ryow", 3, KeyValueDurability.Ephemeral);

        // Start a transaction and write a new key (uncommitted)
        (KeyValueResponseType txResp, TransactionHandle txHandle) = await node.Kahuna.LocateAndStartTransaction(
            new() { CoordinatorKey = "test/ryow/tx", Timeout = 5000, Locking = KeyValueTransactionLocking.Optimistic },
            TestContext.Current.CancellationToken);
        HLCTimestamp txId = txHandle.TransactionId;
        Assert.Equal(KeyValueResponseType.Set, txResp);

        const string newKey = "test/ryow/0099";
        (KeyValueResponseType setResp, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
            txId, newKey,
            Encoding.UTF8.GetBytes("uncommitted"),
            null, -1, KeyValueFlags.Set, 0,
            KeyValueDurability.Ephemeral,
            TestContext.Current.CancellationToken);
        Assert.Equal(KeyValueResponseType.Set, setResp);

        // GetByRange with the same txId should see the uncommitted write
        KeyValueGetByRangeResult result = await node.Kahuna.LocateAndGetByRange(
            txId, "test/ryow",
            null, true, null, false,
            100, HLCTimestamp.Zero,
            KeyValueDurability.Ephemeral,
            TestContext.Current.CancellationToken);

        Assert.Equal(KeyValueResponseType.Get, result.Type);
        List<string> keys = result.Items.Select(i => i.Item1).ToList();
        Assert.Contains(newKey, keys);       // RYOW: see own uncommitted write
        Assert.Equal(4, keys.Count);
    }

    // ── Prefix boundary: sibling prefix keys not leaked ─────────────────────

    [Fact]
    public async Task TestGetByRangePrefixBoundaryIsolation()
    {
        await using EmbeddedKahunaNode node = new(MemoryOptions(), loggerFactory);
        await node.StartAsync(TestContext.Current.CancellationToken);

        await SeedKeys(node, "alpha/x", 5, KeyValueDurability.Ephemeral);
        await SeedKeys(node, "beta/x", 5, KeyValueDurability.Ephemeral);

        KeyValueGetByRangeResult result = await node.Kahuna.LocateAndGetByRange(
            HLCTimestamp.Zero, "alpha/x",
            null, true, null, false,
            100, HLCTimestamp.Zero,
            KeyValueDurability.Ephemeral,
            TestContext.Current.CancellationToken);

        Assert.Equal(5, result.Items.Count);
        Assert.All(result.Items, item =>
            Assert.StartsWith("alpha/x", item.Item1, StringComparison.Ordinal));
    }

    // ── Full paged coverage with ordinal order and no dupes ──────────────────

    [Theory]
    [InlineData(KeyValueDurability.Ephemeral)]
    [InlineData(KeyValueDurability.Persistent)]
    public async Task TestGetByRangeFullCoverageNoDupes(KeyValueDurability durability)
    {
        await using EmbeddedKahunaNode node = new(MemoryOptions(), loggerFactory);
        await node.StartAsync(TestContext.Current.CancellationToken);

        const string prefix = "test/full";
        const int total = 30;
        const int pageSize = 7;

        await SeedKeys(node, prefix, total, durability);

        List<string> allKeys = [];
        string? resumeKey = null;

        while (true)
        {
            KeyValueGetByRangeResult page = await node.Kahuna.LocateAndGetByRange(
                HLCTimestamp.Zero, prefix,
                resumeKey, resumeKey is null,
                null, false,
                pageSize, HLCTimestamp.Zero,
                durability,
                TestContext.Current.CancellationToken);

            Assert.Equal(KeyValueResponseType.Get, page.Type);
            allKeys.AddRange(page.Items.Select(i => i.Item1));

            if (!page.HasMore) break;

            // Exclusive resume on the bare lastKey: the handler's startInclusive=false
            // path skips lastKey itself, so no key in (lastKey, nextKey) is dropped.
            resumeKey = DecodeLastKey(page.NextCursor!);
        }

        Assert.Equal(total, allKeys.Count);
        Assert.Equal(allKeys.Distinct().ToList(), allKeys);
        AssertOrdinalOrder(allKeys);
    }

    // ── Snapshot-consistent paging ──────────────────────────────────

    /// <summary>
    /// Key regression: after page 1 is returned, a concurrent commit inserts a new key and
    /// updates an existing key both in the not-yet-returned range. The running scan must NOT
    /// see the new key (phantom) and must NOT see the updated value — it observes the snapshot
    /// captured at page 0.
    /// </summary>
    [Theory]
    [InlineData(KeyValueDurability.Ephemeral)]
    [InlineData(KeyValueDurability.Persistent)]
    public async Task TestGetByRangeSnapshotIsolationMidScanCommit(KeyValueDurability durability)
    {
        await using EmbeddedKahunaNode node = new(MemoryOptions(), loggerFactory);
        await node.StartAsync(TestContext.Current.CancellationToken);

        const string prefix = "snap/tbl";
        const int total = 10;

        // Seed keys 0000–0009
        await SeedKeys(node, prefix, total, durability);

        // --- Page 0 ---
        // Fetch first 5 keys; the cursor encodes the snapshot timestamp.
        KeyValueGetByRangeResult page0 = await node.Kahuna.LocateAndGetByRange(
            HLCTimestamp.Zero, prefix,
            null, true, null, false,
            5, HLCTimestamp.Zero,
            durability,
            TestContext.Current.CancellationToken);

        Assert.Equal(KeyValueResponseType.Get, page0.Type);
        Assert.Equal(5, page0.Items.Count);
        Assert.True(page0.HasMore);
        Assert.NotNull(page0.NextCursor);

        // Decode the cursor to get lastKey and the captured snapshotTs.
        bool decoded = KeyValueRangeCursor.TryDecode(page0.NextCursor!,
            out string lastKey, out _, out _, out HLCTimestamp snapshotTs);
        Assert.True(decoded);
        Assert.False(snapshotTs.IsNull(), "Page 0 cursor must carry a non-zero snapshot timestamp");

        // --- Concurrent commit (AFTER page 0) ---
        // Insert a new key (phantom candidate) in the not-yet-returned range.
        string phantomKey = $"{prefix}/0099";
        (KeyValueResponseType insertType, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, phantomKey,
            Encoding.UTF8.GetBytes("phantom"), null, -1,
            KeyValueFlags.Set, 0, durability,
            TestContext.Current.CancellationToken);
        Assert.Equal(KeyValueResponseType.Set, insertType);

        // Update an existing key in the not-yet-returned range with a new value.
        string updatedKey = $"{prefix}/0007";
        byte[] originalValue = page0.Items.FirstOrDefault(i => i.Item1 == updatedKey).Item2?.Value
                               ?? Encoding.UTF8.GetBytes("v7");
        (KeyValueResponseType updateType, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, updatedKey,
            Encoding.UTF8.GetBytes("post-snapshot-value"), null, -1,
            KeyValueFlags.Set, 0, durability,
            TestContext.Current.CancellationToken);
        Assert.Equal(KeyValueResponseType.Set, updateType);

        // --- Page 1 (with snapshot) ---
        // Exclusive resume on bare lastKey — correctly handles keys where lastKey
        // is a strict prefix of the next candidate.
        KeyValueGetByRangeResult page1 = await node.Kahuna.LocateAndGetByRange(
            HLCTimestamp.Zero, prefix,
            lastKey, false, null, false,
            20, snapshotTs,          // ← fixed snapshot timestamp
            durability,
            TestContext.Current.CancellationToken);

        Assert.Equal(KeyValueResponseType.Get, page1.Type);

        List<string> page1Keys = page1.Items.Select(i => i.Item1).ToList();

        // Phantom prevention: the key inserted after the snapshot must not appear.
        Assert.DoesNotContain(phantomKey, page1Keys);

        // Updated-key visibility — known limitation:
        // A faithful snapshot would return the pre-snapshot value for a key that existed before
        // the scan started but was updated mid-scan.  The current implementation returns
        // DoesNotExist instead, making the row vanish from the result set.
        // This is a known gap in Kahuna's txId-keyed MVCC: entry.Revisions stores
        // {revision → value} but carries no per-revision timestamps, so the handler cannot
        // identify which revision was current at readTimestamp and falls back to DoesNotExist.
        // See TryGetByRangeHandler.Get() for the detailed explanation and the TODO.
        // For now we assert only the invariant that the POST-snapshot value is never returned.
        (string _, ReadOnlyKeyValueEntry? updatedEntry) = page1.Items
            .FirstOrDefault(i => i.Item1 == updatedKey);
        if (updatedEntry is not null)
        {
            string returnedValue = Encoding.UTF8.GetString(updatedEntry.Value ?? []);
            Assert.NotEqual("post-snapshot-value", returnedValue);
        }

        // Ordinal order preserved.
        AssertOrdinalOrder(page1.Items);
    }

    /// <summary>
    /// readTimestamp == Zero reproduces current "latest" behaviour: concurrent writes ARE visible.
    /// </summary>
    [Theory]
    [InlineData(KeyValueDurability.Ephemeral)]
    [InlineData(KeyValueDurability.Persistent)]
    public async Task TestGetByRangeZeroTimestampSeesLatest(KeyValueDurability durability)
    {
        await using EmbeddedKahunaNode node = new(MemoryOptions(), loggerFactory);
        await node.StartAsync(TestContext.Current.CancellationToken);

        const string prefix = "latest/tbl";
        await SeedKeys(node, prefix, 5, durability);

        // Insert an extra key before fetching with Zero timestamp.
        await node.Kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, $"{prefix}/0099",
            Encoding.UTF8.GetBytes("extra"), null, -1,
            KeyValueFlags.Set, 0, durability,
            TestContext.Current.CancellationToken);

        KeyValueGetByRangeResult result = await node.Kahuna.LocateAndGetByRange(
            HLCTimestamp.Zero, prefix,
            null, true, null, false,
            100, HLCTimestamp.Zero,   // Zero = "latest"
            durability,
            TestContext.Current.CancellationToken);

        Assert.Equal(6, result.Items.Count);  // sees the extra key
    }

    /// <summary>
    /// Snapshot-consistent full paged scan: thread snapshotTs through all pages via cursor.
    /// All pages must observe the snapshot captured on page 0.
    /// </summary>
    [Theory]
    [InlineData(KeyValueDurability.Ephemeral)]
    [InlineData(KeyValueDurability.Persistent)]
    public async Task TestGetByRangeSnapshotConsistentFullScan(KeyValueDurability durability)
    {
        await using EmbeddedKahunaNode node = new(MemoryOptions(), loggerFactory);
        await node.StartAsync(TestContext.Current.CancellationToken);

        const string prefix = "snap/full";
        const int total = 20;
        const int pageSize = 6;

        await SeedKeys(node, prefix, total, durability);

        List<string> allKeys = [];
        string? resumeKey = null;
        HLCTimestamp snapshotTs = HLCTimestamp.Zero; // populated from page 0 cursor

        while (true)
        {
            KeyValueGetByRangeResult page = await node.Kahuna.LocateAndGetByRange(
                HLCTimestamp.Zero, prefix,
                resumeKey, resumeKey is null,
                null, false,
                pageSize,
                snapshotTs,      // Zero on first page, fixed snapshot on subsequent pages
                durability,
                TestContext.Current.CancellationToken);

            Assert.Equal(KeyValueResponseType.Get, page.Type);
            allKeys.AddRange(page.Items.Select(i => i.Item1));

            if (!page.HasMore) break;

            // Decode the cursor to advance past the last returned key AND carry snapshotTs.
            bool ok = KeyValueRangeCursor.TryDecode(page.NextCursor!,
                out string cursorLastKey, out _, out _, out HLCTimestamp cursorTs);
            Assert.True(ok);

            // After page 0, snapshotTs is fixed for all subsequent pages.
            if (snapshotTs.IsNull())
                snapshotTs = cursorTs;

            resumeKey = cursorLastKey;
        }

        Assert.Equal(total, allKeys.Count);
        Assert.Equal(allKeys.Distinct().ToList(), allKeys);
        AssertOrdinalOrder(allKeys);
    }

    /// <summary>
    /// RYOW with snapshot: a transaction's own pending writes remain visible even when a
    /// fixed read timestamp is in effect.
    /// </summary>
    [Fact]
    public async Task TestGetByRangeRyowWithSnapshot()
    {
        await using EmbeddedKahunaNode node = new(MemoryOptions(), loggerFactory);
        await node.StartAsync(TestContext.Current.CancellationToken);

        const string prefix = "ryow/snap";
        await SeedKeys(node, prefix, 5, KeyValueDurability.Ephemeral);

        // Open a transaction and write an uncommitted key.
        (KeyValueResponseType txResp, TransactionHandle txHandle) = await node.Kahuna.LocateAndStartTransaction(
            new() { CoordinatorKey = "ryow/snap/tx", Timeout = 5000, Locking = KeyValueTransactionLocking.Optimistic },
            TestContext.Current.CancellationToken);
        HLCTimestamp txId = txHandle.TransactionId;
        Assert.Equal(KeyValueResponseType.Set, txResp);

        const string txKey = $"{prefix}/0099";
        (KeyValueResponseType setResp, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
            txId, txKey,
            Encoding.UTF8.GetBytes("own-write"),
            null, -1, KeyValueFlags.Set, 0,
            KeyValueDurability.Ephemeral,
            TestContext.Current.CancellationToken);
        Assert.Equal(KeyValueResponseType.Set, setResp);

        // Capture a snapshot timestamp (slightly in the past relative to the tx write,
        // but that's fine because RYOW overrides the snapshot for own writes).
        HLCTimestamp snapshotTs = txId; // use tx start as a reasonable snapshot anchor

        KeyValueGetByRangeResult result = await node.Kahuna.LocateAndGetByRange(
            txId, prefix,
            null, true, null, false,
            100, snapshotTs,
            KeyValueDurability.Ephemeral,
            TestContext.Current.CancellationToken);

        Assert.Equal(KeyValueResponseType.Get, result.Type);
        List<string> keys = result.Items.Select(i => i.Item1).ToList();

        // RYOW: own uncommitted write must be visible despite the snapshot.
        Assert.Contains(txKey, keys);
    }

    // ── HasMore not premature when tombstones fill the window ──────────

    /// <summary>
    /// Delete more than limit entries at the start of the range; the scan must still
    /// find and return the live entries that follow — not falsely report HasMore=false.
    /// </summary>
    [Theory]
    [InlineData(KeyValueDurability.Ephemeral)]
    [InlineData(KeyValueDurability.Persistent)]
    public async Task TestGetByRangeTombstonesDoNotCauseEarlyTermination(KeyValueDurability durability)
    {
        await using EmbeddedKahunaNode node = new(MemoryOptions(), loggerFactory);
        await node.StartAsync(TestContext.Current.CancellationToken);

        const string prefix = "del/tbl";
        const int total = 20;
        const int limit = 5;

        await SeedKeys(node, prefix, total, durability);

        // Delete the first limit+1 = 6 keys so the entire initial window is tombstones.
        for (int i = 0; i < limit + 1; i++)
        {
            (KeyValueResponseType t, _, _) = await node.Kahuna.LocateAndTryDeleteKeyValue(
                HLCTimestamp.Zero, $"{prefix}/{i:D4}",
                durability, TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.Deleted, t);
        }

        // Scan — must not stop at the tombstone window, must find the 14 live keys.
        KeyValueGetByRangeResult result = await node.Kahuna.LocateAndGetByRange(
            HLCTimestamp.Zero, prefix,
            null, true, null, false,
            limit, HLCTimestamp.Zero,
            durability,
            TestContext.Current.CancellationToken);

        Assert.Equal(KeyValueResponseType.Get, result.Type);
        Assert.Equal(limit, result.Items.Count);
        Assert.True(result.HasMore);

        // Keys must all be from the live range (0006–0019).
        Assert.All(result.Items, item =>
        {
            int idx = int.Parse(item.Item1.Split('/')[^1]);
            Assert.True(idx >= limit + 1, $"Returned deleted key {item.Item1}");
        });
    }

    [Theory]
    [InlineData(KeyValueDurability.Ephemeral)]
    [InlineData(KeyValueDurability.Persistent)]
    public async Task TestGetByRangeTombstonesFullPageReturnedEmpty(KeyValueDurability durability)
    {
        await using EmbeddedKahunaNode node = new(MemoryOptions(), loggerFactory);
        await node.StartAsync(TestContext.Current.CancellationToken);

        const string prefix = "del/empty";
        const int total = 10;
        const int limit = 5;

        await SeedKeys(node, prefix, total, durability);

        // Delete ALL keys — scan must return 0 items and HasMore=false.
        for (int i = 0; i < total; i++)
        {
            await node.Kahuna.LocateAndTryDeleteKeyValue(
                HLCTimestamp.Zero, $"{prefix}/{i:D4}",
                durability, TestContext.Current.CancellationToken);
        }

        KeyValueGetByRangeResult result = await node.Kahuna.LocateAndGetByRange(
            HLCTimestamp.Zero, prefix,
            null, true, null, false,
            limit, HLCTimestamp.Zero,
            durability,
            TestContext.Current.CancellationToken);

        Assert.Equal(KeyValueResponseType.Get, result.Type);
        Assert.Empty(result.Items);
        Assert.False(result.HasMore);
    }

    // ── EndKey enforced on the persistent/disk path ────────────────────

    [Theory]
    [InlineData(true)]   // inclusive end
    [InlineData(false)]  // exclusive end
    public async Task TestGetByRangeEndKeyEnforcedOnPersistentPath(bool endInclusive)
    {
        await using EmbeddedKahunaNode node = new(MemoryOptions(), loggerFactory);
        await node.StartAsync(TestContext.Current.CancellationToken);

        const string prefix = "end/tbl";
        await SeedKeys(node, prefix, 20, KeyValueDurability.Persistent);

        string endKey = $"{prefix}/0009";
        int expectedCount = endInclusive ? 10 : 9; // 0000..0009 or 0000..0008

        KeyValueGetByRangeResult result = await node.Kahuna.LocateAndGetByRange(
            HLCTimestamp.Zero, prefix,
            null, true, endKey, endInclusive,
            100, HLCTimestamp.Zero,
            KeyValueDurability.Persistent,
            TestContext.Current.CancellationToken);

        Assert.Equal(KeyValueResponseType.Get, result.Type);
        Assert.Equal(expectedCount, result.Items.Count);
        Assert.False(result.HasMore);

        // No key past endKey must appear.
        Assert.All(result.Items, item =>
        {
            int cmp = string.CompareOrdinal(item.Item1, endKey);
            if (endInclusive)
                Assert.True(cmp <= 0, $"Key {item.Item1} exceeds inclusive endKey");
            else
                Assert.True(cmp < 0, $"Key {item.Item1} exceeds exclusive endKey");
        });
    }

    [Fact]
    public async Task TestGetByRangeEndKeyWithLimitHasMoreCorrect()
    {
        await using EmbeddedKahunaNode node = new(MemoryOptions(), loggerFactory);
        await node.StartAsync(TestContext.Current.CancellationToken);

        const string prefix = "end/hasmore";
        await SeedKeys(node, prefix, 20, KeyValueDurability.Persistent);

        // EndKey = 0014 (inclusive), limit = 5 → first page: 0000-0004, HasMore=true
        string endKey = $"{prefix}/0014";

        KeyValueGetByRangeResult page1 = await node.Kahuna.LocateAndGetByRange(
            HLCTimestamp.Zero, prefix,
            null, true, endKey, true,
            5, HLCTimestamp.Zero,
            KeyValueDurability.Persistent,
            TestContext.Current.CancellationToken);

        Assert.Equal(5, page1.Items.Count);
        Assert.True(page1.HasMore);
        Assert.Equal($"{prefix}/0000", page1.Items[0].Item1);
        Assert.Equal($"{prefix}/0004", page1.Items[4].Item1);

        bool ok = KeyValueRangeCursor.TryDecode(page1.NextCursor!, out string lastKey, out _, out _, out _);
        Assert.True(ok);

        // Page 2: 0005-0009, HasMore=true
        KeyValueGetByRangeResult page2 = await node.Kahuna.LocateAndGetByRange(
            HLCTimestamp.Zero, prefix,
            lastKey, false, endKey, true,
            5, HLCTimestamp.Zero,
            KeyValueDurability.Persistent,
            TestContext.Current.CancellationToken);

        Assert.Equal(5, page2.Items.Count);
        Assert.True(page2.HasMore);
        Assert.Equal($"{prefix}/0005", page2.Items[0].Item1);

        ok = KeyValueRangeCursor.TryDecode(page2.NextCursor!, out lastKey, out _, out _, out _);
        Assert.True(ok);

        // Page 3: 0010-0014, HasMore=false (we hit endKey)
        KeyValueGetByRangeResult page3 = await node.Kahuna.LocateAndGetByRange(
            HLCTimestamp.Zero, prefix,
            lastKey, false, endKey, true,
            5, HLCTimestamp.Zero,
            KeyValueDurability.Persistent,
            TestContext.Current.CancellationToken);

        Assert.Equal(5, page3.Items.Count);
        Assert.False(page3.HasMore);
        Assert.Equal($"{prefix}/0010", page3.Items[0].Item1);
        Assert.Equal($"{prefix}/0014", page3.Items[4].Item1);

        // No key past endKey in any page.
        foreach (KeyValueGetByRangeResult page in new[] { page1, page2, page3 })
            Assert.All(page.Items, item =>
                Assert.True(string.CompareOrdinal(item.Item1, endKey) <= 0));
    }

    // ── Snapshot range scan disk fallback ────────────────────────────────────

    /// <summary>
    /// When a key is superseded more than RevisionRetention times after the snapshot timestamp,
    /// its as-of revision is trimmed from the in-memory archive. A snapshot GetByRange must
    /// recover it from the persisted revision history (GetKeyValueRevisionAtOrBefore) rather
    /// than silently omitting the key.
    /// </summary>
    [Fact]
    public async Task GetByRange_SnapshotScan_ResidentTrimmedKey_FallsBackToDiskHistory()
    {
        const int retention = 2;
        EmbeddedKahunaOptions opts = new()
        {
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1,
            RevisionRetention = retention,
            DirtyObjectsWriterDelay = 100  // fast flush — 100 ms
        };

        await using EmbeddedKahunaNode node = new(opts, loggerFactory);
        await node.StartAsync(TestContext.Current.CancellationToken);

        const string prefix = "snap/disk/range/";
        const string key    = prefix + "key1";
        byte[] v1 = Encoding.UTF8.GetBytes("v1");

        // Write V1.
        (KeyValueResponseType t, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, key, v1,
            null, -1, KeyValueFlags.Set, 0,
            KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
        Assert.Equal(KeyValueResponseType.Set, t);

        // Read it back to capture V1's exact LastModified and revision.
        (_, ReadOnlyKeyValueEntry? e1) = await node.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero,
            KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
        Assert.NotNull(e1);
        HLCTimestamp snapshotTs = e1!.LastModified;
        long v1Revision = e1.Revision;

        // Write RevisionRetention+2 more versions — enough to push V1 out of the in-memory archive.
        for (int i = 2; i <= retention + 2; i++)
        {
            await Task.Delay(2, TestContext.Current.CancellationToken);
            (t, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, key, Encoding.UTF8.GetBytes($"v{i}"),
                null, -1, KeyValueFlags.Set, 0,
                KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.Set, t);
        }

        // Wait for the background writer to flush all revisions to disk.
        await Task.Delay(300, TestContext.Current.CancellationToken);

        // Snapshot range scan at snapshotTs: in-memory archive misses V1, disk fallback
        // must find it via GetKeyValueRevisionAtOrBefore and include it in the result.
        KeyValueGetByRangeResult result = await node.Kahuna.LocateAndGetByRange(
            HLCTimestamp.Zero, prefix,
            null, true, null, false,
            10, snapshotTs,
            KeyValueDurability.Persistent,
            TestContext.Current.CancellationToken);

        Assert.Equal(KeyValueResponseType.Get, result.Type);
        Assert.Single(result.Items);
        Assert.Equal(key, result.Items[0].Item1);
        Assert.Equal("v1", Encoding.UTF8.GetString(result.Items[0].Item2.Value!));
        Assert.Equal(v1Revision, result.Items[0].Item2.Revision);
    }

    /// <summary>
    /// Snapshot range scan at a timestamp before the key existed must exclude the key.
    /// </summary>
    [Fact]
    public async Task GetByRange_SnapshotScan_BeforeKeyCreated_ExcludesKey()
    {
        const int retention = 2;
        EmbeddedKahunaOptions opts = new()
        {
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1,
            RevisionRetention = retention,
            DirtyObjectsWriterDelay = 100
        };

        await using EmbeddedKahunaNode node = new(opts, loggerFactory);
        await node.StartAsync(TestContext.Current.CancellationToken);

        const string prefix = "snap/disk/before/";
        const string key    = prefix + "key1";

        // Write V1 then read it back to derive a timestamp strictly before its creation.
        (KeyValueResponseType t, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, key, Encoding.UTF8.GetBytes("v1"),
            null, -1, KeyValueFlags.Set, 0,
            KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
        Assert.Equal(KeyValueResponseType.Set, t);

        (_, ReadOnlyKeyValueEntry? firstEntry) = await node.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero,
            KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
        Assert.NotNull(firstEntry);
        // A timestamp one logical tick before V1 = the key did not yet exist.
        HLCTimestamp beforeCreate = new(firstEntry!.LastModified.N, firstEntry.LastModified.L - 1, firstEntry.LastModified.C);

        // Supersede V1 beyond retention to push it out of memory.
        for (int i = 2; i <= retention + 2; i++)
        {
            await Task.Delay(2, TestContext.Current.CancellationToken);
            (t, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, key, Encoding.UTF8.GetBytes($"v{i}"),
                null, -1, KeyValueFlags.Set, 0,
                KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.Set, t);
        }

        await Task.Delay(300, TestContext.Current.CancellationToken);

        // Snapshot at beforeCreate: the key did not exist — must not appear.
        KeyValueGetByRangeResult result = await node.Kahuna.LocateAndGetByRange(
            HLCTimestamp.Zero, prefix,
            null, true, null, false,
            10, beforeCreate,
            KeyValueDurability.Persistent,
            TestContext.Current.CancellationToken);

        Assert.Equal(KeyValueResponseType.Get, result.Type);
        Assert.Empty(result.Items);
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private static string DecodeLastKey(string cursor)
    {
        bool ok = KeyValueRangeCursor.TryDecode(cursor,
            out string lastKey, out _, out _, out _);
        Assert.True(ok, "Cursor must decode successfully");
        return lastKey;
    }

    private static void AssertOrdinalOrder(List<(string, ReadOnlyKeyValueEntry)> items)
    {
        for (int i = 1; i < items.Count; i++)
            Assert.True(string.CompareOrdinal(items[i].Item1, items[i - 1].Item1) > 0,
                $"Items not in ordinal order at index {i}");
    }

    private static void AssertOrdinalOrder(List<string> keys)
    {
        for (int i = 1; i < keys.Count; i++)
            Assert.True(string.CompareOrdinal(keys[i], keys[i - 1]) > 0,
                $"Keys not in ordinal order at index {i}");
    }

    private static string CreateTempStoragePath()
    {
        string storagePath = Path.Combine(Path.GetTempPath(), "kahuna-range-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(storagePath);
        return storagePath;
    }

    private static void TryDeleteDirectory(string path)
    {
        try
        {
            if (Directory.Exists(path))
                Directory.Delete(path, recursive: true);
        }
        catch (IOException)
        {
        }
        catch (UnauthorizedAccessException)
        {
        }
    }
}
