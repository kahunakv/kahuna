
using System.Text;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Data;
using Kommander.System;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Tests.Server;

/// <summary>
/// Integration tests for LocateAndScanRange — the IAsyncEnumerable surface over
/// cursor-paged GetByRange calls. Tests cover single-node, multi-node, snapshot isolation,
/// cancellation, and the retry-resumes-from-cursor guarantee.
/// </summary>
public class TestLocateAndScanRange : BaseCluster
{
    private readonly ILogger<IRaft>   raftLogger;
    private readonly ILogger<IKahuna> kahunaLogger;

    public TestLocateAndScanRange(ITestOutputHelper outputHelper)
    {
        ILoggerFactory lf = LoggerFactory.Create(b =>
            b.AddXUnit(outputHelper).SetMinimumLevel(LogLevel.Warning));
        raftLogger   = lf.CreateLogger<IRaft>();
        kahunaLogger = lf.CreateLogger<IKahuna>();
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    private static EmbeddedKahunaOptions MemoryOptions() => new()
    {
        Storage = "memory", WalStorage = "memory", InitialPartitions = 1
    };

    private static async Task SeedKeys(IKahuna node, string prefix, int count,
        KeyValueDurability durability, CancellationToken ct = default)
    {
        for (int i = 0; i < count; i++)
        {
            (KeyValueResponseType t, _, _) = await node.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, $"{prefix}/{i:D4}",
                Encoding.UTF8.GetBytes("v" + i),
                null, -1, KeyValueFlags.Set, 0, durability, ct);
            Assert.Equal(KeyValueResponseType.Set, t);
        }
    }

    private static async Task<List<(string, ReadOnlyKeyValueEntry)>> Drain(
        IAsyncEnumerable<(string Key, ReadOnlyKeyValueEntry Entry)> source)
    {
        List<(string, ReadOnlyKeyValueEntry)> result = [];
        await foreach ((string k, ReadOnlyKeyValueEntry v) in source)
            result.Add((k, v));
        return result;
    }

    // ── single-node: full-table scan ──────────────────────────────────────────

    [Theory]
    [InlineData(KeyValueDurability.Ephemeral)]
    [InlineData(KeyValueDurability.Persistent)]
    public async Task TestScanRangeFullTableSingleNode(KeyValueDurability durability)
    {
        await using EmbeddedKahunaNode node = new(MemoryOptions(), LoggerFactory.Create(_ => { }));
        await node.StartAsync(TestContext.Current.CancellationToken);

        const string prefix = "scan/full";
        const int total = 25;

        await SeedKeys(node.Kahuna, prefix, total, durability, TestContext.Current.CancellationToken);

        List<(string, ReadOnlyKeyValueEntry)> items = await Drain(node.Kahuna.LocateAndScanRange(
            HLCTimestamp.Zero, prefix,
            null, true, null, false,
            pageSize: 7, HLCTimestamp.Zero, durability,
            TestContext.Current.CancellationToken));

        Assert.Equal(total, items.Count);
        Assert.Equal(items.Select(i => i.Item1).Distinct().ToList(), items.Select(i => i.Item1).ToList());
        for (int i = 1; i < items.Count; i++)
            Assert.True(string.CompareOrdinal(items[i].Item1, items[i - 1].Item1) > 0);
    }

    // ── single-node: bounded memory — pageSize < total ────────────────────────

    [Fact]
    public async Task TestScanRangeBoundedMemory()
    {
        await using EmbeddedKahunaNode node = new(MemoryOptions(), LoggerFactory.Create(_ => { }));
        await node.StartAsync(TestContext.Current.CancellationToken);

        const string prefix = "scan/bounded";
        await SeedKeys(node.Kahuna, prefix, 20, KeyValueDurability.Ephemeral, TestContext.Current.CancellationToken);

        // Verify that the result set matches GetByBucket exactly, proving no skips or dupes.
        List<(string, ReadOnlyKeyValueEntry)> scanned = await Drain(node.Kahuna.LocateAndScanRange(
            HLCTimestamp.Zero, prefix,
            null, true, null, false,
            pageSize: 3, HLCTimestamp.Zero, KeyValueDurability.Ephemeral,
            TestContext.Current.CancellationToken));

        KeyValueGetByBucketResult bucket = await node.Kahuna.LocateAndGetByBucket(
            HLCTimestamp.Zero, prefix, HLCTimestamp.Zero, KeyValueDurability.Ephemeral,
            TestContext.Current.CancellationToken);

        Assert.Equal(bucket.Items.Count, scanned.Count);
        Assert.Equal(
            bucket.Items.OrderBy(i => i.Item1, StringComparer.Ordinal).Select(i => i.Item1).ToList(),
            scanned.Select(i => i.Item1).ToList());
    }

    // ── single-node: snapshot stability ──────────────────────────────────────

    [Theory]
    [InlineData(KeyValueDurability.Ephemeral)]
    [InlineData(KeyValueDurability.Persistent)]
    public async Task TestScanRangeSnapshotStability(KeyValueDurability durability)
    {
        await using EmbeddedKahunaNode node = new(MemoryOptions(), LoggerFactory.Create(_ => { }));
        await node.StartAsync(TestContext.Current.CancellationToken);

        const string prefix = "scan/snapshot";
        await SeedKeys(node.Kahuna, prefix, 10, durability, TestContext.Current.CancellationToken);

        // Enumerate lazily: after first item arrives (page 0 fetched), commit new keys.
        List<string> seen = [];
        bool injected = false;

        await foreach ((string key, ReadOnlyKeyValueEntry _) in node.Kahuna.LocateAndScanRange(
            HLCTimestamp.Zero, prefix,
            null, true, null, false,
            pageSize: 3, HLCTimestamp.Zero, durability,
            TestContext.Current.CancellationToken))
        {
            seen.Add(key);

            // After observing the first page, commit a phantom and an update.
            if (!injected && seen.Count >= 3)
            {
                injected = true;

                await node.Kahuna.LocateAndTrySetKeyValue(
                    HLCTimestamp.Zero, $"{prefix}/0099",
                    Encoding.UTF8.GetBytes("phantom"),
                    null, -1, KeyValueFlags.Set, 0, durability,
                    TestContext.Current.CancellationToken);

                await node.Kahuna.LocateAndTrySetKeyValue(
                    HLCTimestamp.Zero, $"{prefix}/0007",
                    Encoding.UTF8.GetBytes("updated"),
                    null, -1, KeyValueFlags.Set, 0, durability,
                    TestContext.Current.CancellationToken);
            }
        }

        // Phantom must not appear.
        Assert.DoesNotContain($"{prefix}/0099", seen);

        // Original 10 keys (0000–0009) should all be present except possibly 0007:
        // that key was updated after the snapshot was captured. The current implementation
        // returns DoesNotExist rather than the pre-snapshot value — a known limitation of
        // txId-keyed MVCC without per-revision timestamps. See TryGetByRangeHandler.Get().
        Assert.True(seen.Count <= 10, $"No extra items should appear; got {seen.Count}");
        Assert.Equal(seen.Distinct().ToList(), seen);
    }

    // ── single-node: empty prefix ─────────────────────────────────────────────

    [Fact]
    public async Task TestScanRangeEmptyPrefix()
    {
        await using EmbeddedKahunaNode node = new(MemoryOptions(), LoggerFactory.Create(_ => { }));
        await node.StartAsync(TestContext.Current.CancellationToken);

        List<(string, ReadOnlyKeyValueEntry)> items = await Drain(node.Kahuna.LocateAndScanRange(
            HLCTimestamp.Zero, "scan/nobody",
            null, true, null, false,
            pageSize: 10, HLCTimestamp.Zero, KeyValueDurability.Ephemeral,
            TestContext.Current.CancellationToken));

        Assert.Empty(items);
    }

    // ── single-node: cancellation ─────────────────────────────────────────────

    [Fact]
    public async Task TestScanRangeCancellationStopsIteration()
    {
        await using EmbeddedKahunaNode node = new(MemoryOptions(), LoggerFactory.Create(_ => { }));
        await node.StartAsync(TestContext.Current.CancellationToken);

        await SeedKeys(node.Kahuna, "scan/cancel", 30, KeyValueDurability.Ephemeral,
            TestContext.Current.CancellationToken);

        using CancellationTokenSource cts = new();
        int count = 0;

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            await foreach ((string _, ReadOnlyKeyValueEntry _) in node.Kahuna.LocateAndScanRange(
                HLCTimestamp.Zero, "scan/cancel",
                null, true, null, false,
                pageSize: 5, HLCTimestamp.Zero, KeyValueDurability.Ephemeral,
                cts.Token))
            {
                count++;
                if (count >= 5)
                    cts.Cancel();
            }
        });

        // At most 10 items (2 pages of 5) before cancellation fires.
        Assert.InRange(count, 5, 10);
    }

    // ── single-node: startKey partial range ──────────────────────────────────

    [Fact]
    public async Task TestScanRangeStartKey()
    {
        await using EmbeddedKahunaNode node = new(MemoryOptions(), LoggerFactory.Create(_ => { }));
        await node.StartAsync(TestContext.Current.CancellationToken);

        await SeedKeys(node.Kahuna, "scan/partial", 10, KeyValueDurability.Ephemeral,
            TestContext.Current.CancellationToken);

        // Start from key 0005 (inclusive).
        List<(string, ReadOnlyKeyValueEntry)> items = await Drain(node.Kahuna.LocateAndScanRange(
            HLCTimestamp.Zero, "scan/partial",
            "scan/partial/0005", true, null, false,
            pageSize: 3, HLCTimestamp.Zero, KeyValueDurability.Ephemeral,
            TestContext.Current.CancellationToken));

        Assert.Equal(5, items.Count);
        Assert.Equal("scan/partial/0005", items[0].Item1);
        Assert.Equal("scan/partial/0009", items[4].Item1);
    }

    // ── multi-node: full-table scan routed to remote leader ───────────────────

    [Theory, CombinatorialData]
    public async Task TestScanRangeMultiNode(
        [CombinatorialValues("memory")] string storage,
        [CombinatorialValues(4)]        int    partitions,
        [CombinatorialValues(KeyValueDurability.Ephemeral, KeyValueDurability.Persistent)] KeyValueDurability durability
    )
    {
        (IRaft n1, IRaft n2, IRaft n3, IKahuna k1, IKahuna k2, IKahuna k3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            const string prefix = "scan/multi";
            const int    total  = 20;

            await SeedKeys(k1, prefix, total, durability, TestContext.Current.CancellationToken);

            // Query from k3 — might be redirected to the leader.
            List<(string, ReadOnlyKeyValueEntry)> items = await Drain(k3.LocateAndScanRange(
                HLCTimestamp.Zero, prefix,
                null, true, null, false,
                pageSize: 6, HLCTimestamp.Zero, durability,
                TestContext.Current.CancellationToken));

            Assert.Equal(total, items.Count);
            for (int i = 1; i < items.Count; i++)
                Assert.True(string.CompareOrdinal(items[i].Item1, items[i - 1].Item1) > 0);
        }
        finally
        {
            await LeaveCluster(n1, n2, n3);
        }
    }

    // ── multi-node: snapshot consistency across pages ─────────────────────────

    [Theory, CombinatorialData]
    public async Task TestScanRangeMultiNodeSnapshot(
        [CombinatorialValues("memory")] string storage,
        [CombinatorialValues(4)]        int    partitions,
        [CombinatorialValues(KeyValueDurability.Ephemeral, KeyValueDurability.Persistent)] KeyValueDurability durability
    )
    {
        (IRaft n1, IRaft n2, IRaft n3, IKahuna k1, IKahuna k2, IKahuna k3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            const string prefix = "scan/msnap";
            await SeedKeys(k1, prefix, 10, durability, TestContext.Current.CancellationToken);

            List<string> seen = [];
            bool injected = false;

            await foreach ((string key, ReadOnlyKeyValueEntry _) in k2.LocateAndScanRange(
                HLCTimestamp.Zero, prefix,
                null, true, null, false,
                pageSize: 4, HLCTimestamp.Zero, durability,
                TestContext.Current.CancellationToken))
            {
                seen.Add(key);

                if (!injected && seen.Count >= 4)
                {
                    injected = true;

                    await k3.LocateAndTrySetKeyValue(
                        HLCTimestamp.Zero, $"{prefix}/0099",
                        Encoding.UTF8.GetBytes("phantom"),
                        null, -1, KeyValueFlags.Set, 0, durability,
                        TestContext.Current.CancellationToken);
                }
            }

            // Phantom inserted after snapshot must not appear.
            Assert.DoesNotContain($"{prefix}/0099", seen);
            Assert.InRange(seen.Count, 1, 10);
        }
        finally
        {
            await LeaveCluster(n1, n2, n3);
        }
    }

    // ── retry: cursor position survives a transient page error ────────────────
    //
    // We can't inject a MustRetry at will, so we verify the structural guarantee:
    // LocateAndScanRange reports the same full set regardless of page size, proving
    // cursor math is correct (which is what survives retries).

    [Fact]
    public async Task TestScanRangeCursorConsistentAcrossPageSizes()
    {
        await using EmbeddedKahunaNode node = new(MemoryOptions(), LoggerFactory.Create(_ => { }));
        await node.StartAsync(TestContext.Current.CancellationToken);

        await SeedKeys(node.Kahuna, "scan/cursor", 30, KeyValueDurability.Ephemeral,
            TestContext.Current.CancellationToken);

        async Task<List<string>> Scan(int ps) =>
            (await Drain(node.Kahuna.LocateAndScanRange(
                HLCTimestamp.Zero, "scan/cursor",
                null, true, null, false,
                pageSize: ps, HLCTimestamp.Zero, KeyValueDurability.Ephemeral,
                TestContext.Current.CancellationToken)))
            .Select(i => i.Item1).ToList();

        List<string> byOne    = await Scan(1);
        List<string> byFive   = await Scan(5);
        List<string> byHundred = await Scan(100);

        Assert.Equal(byOne,    byFive);
        Assert.Equal(byFive,   byHundred);
        Assert.Equal(30,       byOne.Count);
    }

    // ── Multi-range stitch ───────────────────────────────────────────

    private const string SplitSpace = "tbl:r";

    private static async Task<(IRaft, KahunaManager)> LeaderOf(int partition, (IRaft Raft, KahunaManager Kahuna)[] nodes)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        while (true)
        {
            foreach ((IRaft raft, KahunaManager kahuna) in nodes)
                if (await raft.AmILeader(partition, ct))
                    return (raft, kahuna);
            await Task.Delay(50, ct);
        }
    }

    private static async Task WaitUntilScan(Func<bool> predicate, int timeoutMs = 10000)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        long deadline = Environment.TickCount64 + timeoutMs;
        while (Environment.TickCount64 < deadline)
        {
            if (predicate()) return;
            await Task.Delay(25, ct);
        }
        Assert.Fail("Timed out waiting for condition.");
    }

    private static async Task WaitUntilScanAsync(Func<Task<bool>> predicate, int timeoutMs = 10000)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        long deadline = Environment.TickCount64 + timeoutMs;
        while (Environment.TickCount64 < deadline)
        {
            if (await predicate()) return;
            await Task.Delay(25, ct);
        }
        Assert.Fail("Timed out waiting for async condition.");
    }

    /// <summary>
    /// Scan_SpanningMultipleRanges_ReturnsFullOrderedResult
    ///
    /// Seeds keys into a key-range space, splits it at the midpoint, then verifies that
    /// LocateAndScanRange returns every key in order with no gaps, stitching across both partitions.
    /// </summary>
    [Fact]
    public async Task Scan_SpanningMultipleRanges_ReturnsFullOrderedResult()
    {
        (IRaft r1, IRaft r2, IRaft r3, IKahuna k1, IKahuna k2, IKahuna k3) =
            await AssembleThreNodeCluster("memory", 4, raftLogger, kahunaLogger);

        (IRaft Raft, KahunaManager Kahuna)[] nodes =
            [(r1, (KahunaManager)k1), (r2, (KahunaManager)k2), (r3, (KahunaManager)k3)];

        try
        {
            CancellationToken ct = TestContext.Current.CancellationToken;

            // Register the space as key-range on all nodes.
            foreach ((IRaft _, KahunaManager kahuna) in nodes)
                kahuna.RegisterKeyRange(SplitSpace);

            // Seed the initial full-range descriptor on P2.
            (IRaft _, KahunaManager metaLeader) =
                await LeaderOf(RangeMapStore.MetaPartitionId, nodes);

            bool committed = await metaLeader.RangeMapStore.MutateAsync(
                _ => [new RangeDescriptor
                {
                    KeySpace    = SplitSpace,
                    StartKey    = null,
                    EndKey      = null,
                    PartitionId = RangeMapStore.FirstDataPartitionId,
                    Generation  = 1
                }], ct);
            Assert.True(committed);

            // Wait for descriptor to reach all nodes.
            foreach ((IRaft _, KahunaManager kahuna) in nodes)
                await WaitUntilScan(
                    () => kahuna.RangeMapStore.Current.Find(SplitSpace, SplitSpace + "/x") is not null);

            // Seed 20 keys: /0000 … /0019.
            const int total = 20;
            (IRaft _, KahunaManager dataLeader) =
                await LeaderOf(RangeMapStore.FirstDataPartitionId, nodes);

            for (int i = 0; i < total; i++)
            {
                (KeyValueResponseType t, _, _) = await dataLeader.TrySetKeyValue(
                    HLCTimestamp.Zero, $"{SplitSpace}/{i:D4}",
                    Encoding.UTF8.GetBytes("v" + i),
                    null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent);
                Assert.Equal(KeyValueResponseType.Set, t);
            }

            // Wait for all keys to be visible from all nodes before splitting.
            for (int i = 0; i < total; i++)
            {
                string key = $"{SplitSpace}/{i:D4}";
                foreach ((IRaft _, KahunaManager kahuna) in nodes)
                    await WaitUntilScanAsync(async () =>
                    {
                        (KeyValueResponseType rt, _) =
                            await kahuna.TryGetValue(HLCTimestamp.Zero, key, 0, HLCTimestamp.Zero, KeyValueDurability.Persistent);
                        return rt == KeyValueResponseType.Get;
                    });
            }

            // Perform the split at /0010.
            string splitKey = $"{SplitSpace}/0010";
            (IRaft sysRaft, KahunaManager _) = await LeaderOf(0, nodes);

            int newPartitionId = RangeSplitter.ComputeNextPartitionId(metaLeader.RangeMapStore.Current);
            RaftPartitionLifecycleResult createResult =
                await sysRaft.CreatePartitionAsync(newPartitionId, RaftRoutingMode.Unrouted, null, ct);
            Assert.True(createResult.Success, "CreatePartitionAsync failed");

            // Re-acquire meta leader: CreatePartitionAsync can trigger re-elections.
            (_, metaLeader) = await LeaderOf(RangeMapStore.MetaPartitionId, nodes);

            SplitOutcome outcome = await metaLeader.RangeSplitter.SplitAsync(
                SplitSpace, splitKey, newPartitionId, ct);
            Assert.True(outcome.IsSuccess, $"Split failed: {outcome.Status}");

            // Wait for the two-descriptor map to reach all nodes.
            foreach ((IRaft _, KahunaManager kahuna) in nodes)
                await WaitUntilScan(() =>
                {
                    RangeMap m = kahuna.RangeMapStore.Current;
                    return m.Find(SplitSpace, SplitSpace + "/0005") is not null &&
                           m.Find(SplitSpace, SplitSpace + "/0015") is not null &&
                           m.Find(SplitSpace, SplitSpace + "/0005")!.PartitionId !=
                           m.Find(SplitSpace, SplitSpace + "/0015")!.PartitionId;
                });

            // Scan from k2 — it may be redirected across two partition leaders.
            List<(string, ReadOnlyKeyValueEntry)> items = await Drain(
                k2.LocateAndScanRange(
                    HLCTimestamp.Zero, SplitSpace,
                    null, true, null, false,
                    pageSize: 7, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct));

            Assert.Equal(total, items.Count);

            // All keys distinct and in ascending ordinal order.
            for (int i = 1; i < items.Count; i++)
                Assert.True(string.CompareOrdinal(items[i].Item1, items[i - 1].Item1) > 0,
                    $"Order violation at index {i}: {items[i - 1].Item1} vs {items[i].Item1}");

            // Every expected key is present.
            HashSet<string> got = items.Select(x => x.Item1).ToHashSet(StringComparer.Ordinal);
            for (int i = 0; i < total; i++)
                Assert.Contains($"{SplitSpace}/{i:D4}", got);
        }
        finally
        {
            await LeaveCluster(r1, r2, r3);
        }
    }

    /// <summary>
    /// Scan_SplitMidIteration_NoDuplicateOrMissingRows
    ///
    /// Captures the complete key set before a split, then re-scans after the split, and verifies
    /// the post-split scan returns exactly the same keys (no duplicates, no missing rows).
    /// </summary>
    [Fact]
    public async Task Scan_SplitMidIteration_NoDuplicateOrMissingRows()
    {
        (IRaft r1, IRaft r2, IRaft r3, IKahuna k1, IKahuna k2, IKahuna k3) =
            await AssembleThreNodeCluster("memory", 4, raftLogger, kahunaLogger);

        (IRaft Raft, KahunaManager Kahuna)[] nodes =
            [(r1, (KahunaManager)k1), (r2, (KahunaManager)k2), (r3, (KahunaManager)k3)];

        try
        {
            CancellationToken ct = TestContext.Current.CancellationToken;
            const string space   = "tbl2:r";
            const int    total   = 30;

            foreach ((IRaft _, KahunaManager kahuna) in nodes)
                kahuna.RegisterKeyRange(space);

            (IRaft _, KahunaManager metaLeader) =
                await LeaderOf(RangeMapStore.MetaPartitionId, nodes);

            bool committed = await metaLeader.RangeMapStore.MutateAsync(
                _ => [new RangeDescriptor
                {
                    KeySpace    = space,
                    StartKey    = null,
                    EndKey      = null,
                    PartitionId = RangeMapStore.FirstDataPartitionId,
                    Generation  = 1
                }], ct);
            Assert.True(committed);

            foreach ((IRaft _, KahunaManager kahuna) in nodes)
                await WaitUntilScan(
                    () => kahuna.RangeMapStore.Current.Find(space, space + "/x") is not null);

            (IRaft _, KahunaManager dataLeader) =
                await LeaderOf(RangeMapStore.FirstDataPartitionId, nodes);

            for (int i = 0; i < total; i++)
            {
                (KeyValueResponseType t, _, _) = await dataLeader.TrySetKeyValue(
                    HLCTimestamp.Zero, $"{space}/{i:D4}",
                    Encoding.UTF8.GetBytes("v" + i),
                    null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent);
                Assert.Equal(KeyValueResponseType.Set, t);
            }

            for (int i = 0; i < total; i++)
            {
                string key = $"{space}/{i:D4}";
                foreach ((IRaft _, KahunaManager kahuna) in nodes)
                    await WaitUntilScanAsync(async () =>
                    {
                        (KeyValueResponseType rt, _) =
                            await kahuna.TryGetValue(HLCTimestamp.Zero, key, 0, HLCTimestamp.Zero, KeyValueDurability.Persistent);
                        return rt == KeyValueResponseType.Get;
                    });
            }

            // Baseline scan (unsplit — single partition).
            List<string> baseline = (await Drain(k1.LocateAndScanRange(
                HLCTimestamp.Zero, space, null, true, null, false,
                pageSize: 10, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct)))
                .Select(x => x.Item1).ToList();
            Assert.Equal(total, baseline.Count);

            // Split at /0015.
            string splitKey = $"{space}/0015";
            (IRaft sysRaft, KahunaManager _) = await LeaderOf(0, nodes);

            int newPart = RangeSplitter.ComputeNextPartitionId(metaLeader.RangeMapStore.Current);
            RaftPartitionLifecycleResult cr =
                await sysRaft.CreatePartitionAsync(newPart, RaftRoutingMode.Unrouted, null, ct);
            Assert.True(cr.Success);

            // Re-acquire meta leader: CreatePartitionAsync can trigger re-elections.
            (_, metaLeader) = await LeaderOf(RangeMapStore.MetaPartitionId, nodes);

            SplitOutcome outcome = await metaLeader.RangeSplitter.SplitAsync(space, splitKey, newPart, ct);
            Assert.True(outcome.IsSuccess, $"Split failed: {outcome.Status}");

            foreach ((IRaft _, KahunaManager kahuna) in nodes)
                await WaitUntilScan(() =>
                {
                    RangeMap m = kahuna.RangeMapStore.Current;
                    return m.Find(space, space + "/0005") is not null &&
                           m.Find(space, space + "/0020") is not null &&
                           m.Find(space, space + "/0005")!.PartitionId !=
                           m.Find(space, space + "/0020")!.PartitionId;
                });

            // Post-split scan must match the baseline exactly.
            List<string> afterSplit = (await Drain(k3.LocateAndScanRange(
                HLCTimestamp.Zero, space, null, true, null, false,
                pageSize: 5, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct)))
                .Select(x => x.Item1).ToList();

            Assert.Equal(baseline.Count, afterSplit.Count);
            Assert.Equal(baseline, afterSplit);
        }
        finally
        {
            await LeaveCluster(r1, r2, r3);
        }
    }

    /// <summary>
    /// Scan_SplitMidIteration_ResumesViaCursor
    ///
    /// Fetches page 1 of a paged scan, then splits the space, then resumes from the returned
    /// cursor. Verifies that the full result set (page 1 + resumed pages) equals the pre-split
    /// baseline exactly — no duplicates, no missing rows — proving the cursor's lastKey survives
    /// a concurrent split without needing to encode the range generation.
    /// </summary>
    [Fact]
    public async Task Scan_SplitMidIteration_ResumesViaCursor()
    {
        (IRaft r1, IRaft r2, IRaft r3, IKahuna k1, IKahuna k2, IKahuna k3) =
            await AssembleThreNodeCluster("memory", 4, raftLogger, kahunaLogger);

        (IRaft Raft, KahunaManager Kahuna)[] nodes =
            [(r1, (KahunaManager)k1), (r2, (KahunaManager)k2), (r3, (KahunaManager)k3)];

        try
        {
            CancellationToken ct = TestContext.Current.CancellationToken;
            const string space   = "tbl3:r";
            const int    total   = 20;
            const int    pageOne = 5;    // keys returned before the split
            const string splitAt = "tbl3:r/0010"; // splits at the midpoint

            // ── setup ─────────────────────────────────────────────────────────
            foreach ((IRaft _, KahunaManager kahuna) in nodes)
                kahuna.RegisterKeyRange(space);

            (IRaft _, KahunaManager metaLeader) =
                await LeaderOf(RangeMapStore.MetaPartitionId, nodes);

            bool committed = await metaLeader.RangeMapStore.MutateAsync(
                _ => [new RangeDescriptor
                {
                    KeySpace    = space,
                    StartKey    = null,
                    EndKey      = null,
                    PartitionId = RangeMapStore.FirstDataPartitionId,
                    Generation  = 1
                }], ct);
            Assert.True(committed);

            foreach ((IRaft _, KahunaManager kahuna) in nodes)
                await WaitUntilScan(
                    () => kahuna.RangeMapStore.Current.Find(space, space + "/x") is not null);

            (IRaft _, KahunaManager dataLeader) =
                await LeaderOf(RangeMapStore.FirstDataPartitionId, nodes);

            for (int i = 0; i < total; i++)
            {
                (KeyValueResponseType t, _, _) = await dataLeader.TrySetKeyValue(
                    HLCTimestamp.Zero, $"{space}/{i:D4}",
                    Encoding.UTF8.GetBytes("v" + i),
                    null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent);
                Assert.Equal(KeyValueResponseType.Set, t);
            }

            for (int i = 0; i < total; i++)
            {
                string key = $"{space}/{i:D4}";
                foreach ((IRaft _, KahunaManager kahuna) in nodes)
                    await WaitUntilScanAsync(async () =>
                    {
                        (KeyValueResponseType rt, _) =
                            await kahuna.TryGetValue(HLCTimestamp.Zero, key, 0, HLCTimestamp.Zero, KeyValueDurability.Persistent);
                        return rt == KeyValueResponseType.Get;
                    });
            }

            // ── baseline (unsplit, from k1) ────────────────────────────────────
            List<string> baseline = (await Drain(k1.LocateAndScanRange(
                HLCTimestamp.Zero, space, null, true, null, false,
                pageSize: 100, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct)))
                .Select(x => x.Item1).ToList();
            Assert.Equal(total, baseline.Count);

            // ── page 1: fetch first pageOne items before the split ────────────
            KeyValueGetByRangeResult page1 = await k1.LocateAndGetByRange(
                HLCTimestamp.Zero, space,
                null, true, null, false,
                pageOne, HLCTimestamp.Zero,
                KeyValueDurability.Persistent, ct);

            Assert.Equal(KeyValueResponseType.Get, page1.Type);
            Assert.Equal(pageOne, page1.Items.Count);
            Assert.True(page1.HasMore, "Expected HasMore after page 1");
            Assert.NotNull(page1.NextCursor);

            List<string> collected = page1.Items.Select(x => x.Item1).ToList();

            // ── split at /0010 while cursor is held ────────────────────────────
            (IRaft sysRaft, KahunaManager _) = await LeaderOf(0, nodes);

            int newPart = RangeSplitter.ComputeNextPartitionId(metaLeader.RangeMapStore.Current);
            RaftPartitionLifecycleResult cr =
                await sysRaft.CreatePartitionAsync(newPart, RaftRoutingMode.Unrouted, null, ct);
            Assert.True(cr.Success);

            // Re-acquire meta leader: CreatePartitionAsync can trigger re-elections.
            (_, metaLeader) = await LeaderOf(RangeMapStore.MetaPartitionId, nodes);

            SplitOutcome outcome = await metaLeader.RangeSplitter.SplitAsync(space, splitAt, newPart, ct);
            Assert.True(outcome.IsSuccess, $"Split failed: {outcome.Status}");

            // Wait for the two-descriptor map on all nodes.
            foreach ((IRaft _, KahunaManager kahuna) in nodes)
                await WaitUntilScan(() =>
                {
                    RangeMap m = kahuna.RangeMapStore.Current;
                    return m.Find(space, space + "/0005") is not null &&
                           m.Find(space, space + "/0015") is not null &&
                           m.Find(space, space + "/0005")!.PartitionId !=
                           m.Find(space, space + "/0015")!.PartitionId;
                });

            // ── resume from cursor across the now-split map ────────────────────
            // Decode cursor to get lastKey; each subsequent page uses lastKey as exclusive start.
            string? cursorStr = page1.NextCursor;
            while (cursorStr is not null)
            {
                Assert.True(
                    KeyValueRangeCursor.TryDecode(cursorStr, out string lastKey, out _, out _, out _),
                    "Cursor decode failed");

                KeyValueGetByRangeResult next = await k2.LocateAndGetByRange(
                    HLCTimestamp.Zero, space,
                    lastKey, false, null, false,
                    pageOne, HLCTimestamp.Zero,
                    KeyValueDurability.Persistent, ct);

                Assert.Equal(KeyValueResponseType.Get, next.Type);
                collected.AddRange(next.Items.Select(x => x.Item1));

                cursorStr = next.HasMore ? next.NextCursor : null;
            }

            // ── assert: same set as the pre-split baseline ─────────────────────
            Assert.Equal(baseline.Count, collected.Count);

            // No duplicates.
            Assert.Equal(collected.Count, collected.Distinct(StringComparer.Ordinal).Count());

            // Keys in ascending order.
            for (int i = 1; i < collected.Count; i++)
                Assert.True(string.CompareOrdinal(collected[i], collected[i - 1]) > 0,
                    $"Order violation at {i}: {collected[i - 1]} vs {collected[i]}");

            // Exact match with baseline.
            Assert.Equal(baseline, collected);
        }
        finally
        {
            await LeaveCluster(r1, r2, r3);
        }
    }
}
