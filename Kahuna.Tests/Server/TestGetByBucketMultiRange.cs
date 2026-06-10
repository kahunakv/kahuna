
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
/// Integration tests for Task 10b — multi-range GetByBucket fan-out.
/// Verifies that a key-range space split across partitions returns a complete, ordered union
/// and that hash/schema-log buckets still use the existing single-leader path.
///
/// Tests requiring RPC-level instrumentation (Bucket_QueriesOnlySpannedPartitions,
/// Bucket_SplitMidScan_RetriesOnlyAffectedRange) are deferred until a mock transport is available.
/// </summary>
public sealed class TestGetByBucketMultiRange : BaseCluster
{
    private readonly ILogger<IRaft>   raftLogger;
    private readonly ILogger<IKahuna> kahunaLogger;

    public TestGetByBucketMultiRange(ITestOutputHelper outputHelper)
    {
        ILoggerFactory lf = LoggerFactory.Create(b =>
            b.AddXUnit(outputHelper).SetMinimumLevel(LogLevel.Warning));
        raftLogger   = lf.CreateLogger<IRaft>();
        kahunaLogger = lf.CreateLogger<IKahuna>();
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    private static async Task<(IRaft, KahunaManager)> LeaderOf(
        int partition, (IRaft Raft, KahunaManager Kahuna)[] nodes)
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

    private static async Task WaitFor(Func<bool> predicate, int timeoutMs = 10000)
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

    private static async Task WaitForAsync(Func<Task<bool>> predicate, int timeoutMs = 10000)
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
    /// Assembles a 4-partition 3-node cluster, registers <paramref name="space"/> as KeyRange,
    /// seeds a full-range descriptor on P2, and writes <paramref name="count"/> keys.
    /// Returns the node tuple and the pre-split baseline key list.
    /// </summary>
    private async Task<((IRaft, KahunaManager)[] Nodes, List<string> Baseline)> SetupWithKeys(
        string space, int count)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft r1, IRaft r2, IRaft r3, IKahuna k1, IKahuna k2, IKahuna k3) =
            await AssembleThreNodeCluster("memory", 4, raftLogger, kahunaLogger);

        (IRaft, KahunaManager)[] nodes =
            [(r1, (KahunaManager)k1), (r2, (KahunaManager)k2), (r3, (KahunaManager)k3)];

        foreach ((IRaft _, KahunaManager kahuna) in nodes)
            kahuna.RegisterKeyRange(space);

        (IRaft _, KahunaManager metaLeader) = await LeaderOf(RangeMapStore.MetaPartitionId, nodes);

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
            await WaitFor(() => kahuna.RangeMapStore.Current.Find(space, space + "/x") is not null);

        (IRaft _, KahunaManager dataLeader) =
            await LeaderOf(RangeMapStore.FirstDataPartitionId, nodes);

        var baseline = new List<string>();
        for (int i = 0; i < count; i++)
        {
            string key = $"{space}/{i:D4}";
            (KeyValueResponseType t, _, _) = await dataLeader.TrySetKeyValue(
                HLCTimestamp.Zero, key,
                Encoding.UTF8.GetBytes("v" + i),
                null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent);
            Assert.Equal(KeyValueResponseType.Set, t);
            baseline.Add(key);
        }
        baseline.Sort(StringComparer.Ordinal);

        // Wait until every key is visible on all nodes.
        foreach (string key in baseline)
        {
            string k = key;
            foreach ((IRaft _, KahunaManager kahuna) in nodes)
                await WaitForAsync(async () =>
                {
                    (KeyValueResponseType rt, _) =
                        await kahuna.TryGetValue(HLCTimestamp.Zero, k, 0, KeyValueDurability.Persistent);
                    return rt == KeyValueResponseType.Get;
                });
        }

        return (nodes, baseline);
    }

    private static async Task<SplitOutcome> SplitAt(
        string space, string splitKey,
        (IRaft, KahunaManager)[] nodes,
        CancellationToken ct)
    {
        (IRaft _, KahunaManager metaLeader) = await LeaderOf(RangeMapStore.MetaPartitionId, nodes);
        (IRaft sysRaft, KahunaManager _)    = await LeaderOf(0, nodes);

        int newId = RangeSplitter.ComputeNextPartitionId(metaLeader.RangeMapStore.Current);
        RaftPartitionLifecycleResult cr =
            await sysRaft.CreatePartitionAsync(newId, RaftRoutingMode.Unrouted, null, ct);
        Assert.True(cr.Success);

        return await metaLeader.RangeSplitter.SplitAsync(space, splitKey, newId, ct);
    }

    // ── Bucket_SpanningMultipleRanges_ReturnsCompleteOrderedSet ──────────────

    /// <summary>
    /// A bucket split across ≥2 ranges returns every key, ordered, equal to the pre-split
    /// baseline (no partial result).
    /// </summary>
    [Fact]
    public async Task Bucket_SpanningMultipleRanges_ReturnsCompleteOrderedSet()
    {
        const string space = "bkt:r";
        const int    total = 20;

        ((IRaft, KahunaManager)[] nodes, List<string> baseline) =
            await SetupWithKeys(space, total);

        (IRaft r1, IRaft r2, IRaft r3) =
            (nodes[0].Item1, nodes[1].Item1, nodes[2].Item1);

        IKahuna k1 = (IKahuna)nodes[0].Item2;
        IKahuna k3 = (IKahuna)nodes[2].Item2;

        try
        {
            CancellationToken ct = TestContext.Current.CancellationToken;

            SplitOutcome outcome = await SplitAt(space, $"{space}/0010", nodes, ct);
            Assert.True(outcome.IsSuccess, $"Split failed: {outcome.Status}");

            // Wait for two-descriptor map on all nodes.
            foreach ((IRaft _, KahunaManager kahuna) in nodes)
                await WaitFor(() =>
                {
                    RangeMap m = kahuna.RangeMapStore.Current;
                    return m.Find(space, space + "/0005") is not null &&
                           m.Find(space, space + "/0015") is not null &&
                           m.Find(space, space + "/0005")!.PartitionId !=
                           m.Find(space, space + "/0015")!.PartitionId;
                });

            // Query from k3 — exercises inter-node fan-out via the multi-range path.
            KeyValueGetByBucketResult result = await k3.LocateAndGetByBucket(
                HLCTimestamp.Zero, space, KeyValueDurability.Persistent, ct);

            Assert.Equal(KeyValueResponseType.Get, result.Type);
            Assert.Equal(total, result.Items.Count);

            // Ordered.
            for (int i = 1; i < result.Items.Count; i++)
                Assert.True(
                    string.CompareOrdinal(result.Items[i].Item1, result.Items[i - 1].Item1) > 0,
                    $"Order violation at {i}: {result.Items[i - 1].Item1} vs {result.Items[i].Item1}");

            // Complete and exact.
            Assert.Equal(baseline, result.Items.Select(x => x.Item1).ToList());
        }
        finally
        {
            await LeaveCluster(r1, r2, r3);
        }
    }

    // ── Bucket_SingleRange_UsesFastPath ───────────────────────────────────────

    /// <summary>
    /// A bucket whose key space has not been split (IsPrefixOpSafe = true) returns the correct
    /// result via the single-leader path — no scatter-gather overhead.
    /// Verified by confirming the result matches a baseline scan of a single-partition space.
    /// </summary>
    [Fact]
    public async Task Bucket_SingleRange_UsesFastPath()
    {
        const string space = "bkt2:r";
        const int    total = 15;

        ((IRaft, KahunaManager)[] nodes, List<string> baseline) =
            await SetupWithKeys(space, total);

        (IRaft r1, IRaft r2, IRaft r3) =
            (nodes[0].Item1, nodes[1].Item1, nodes[2].Item1);

        IKahuna k2 = (IKahuna)nodes[1].Item2;

        try
        {
            CancellationToken ct = TestContext.Current.CancellationToken;

            // Confirm the space is still unsplit (IsPrefixOpSafe must hold).
            foreach ((IRaft _, KahunaManager kahuna) in nodes)
            {
                RangeMap m = kahuna.RangeMapStore.Current;
                RangeDescriptor? d = m.Find(space, space + "/x");
                Assert.NotNull(d);
                Assert.Null(d.StartKey);
                Assert.Null(d.EndKey);
            }

            KeyValueGetByBucketResult result = await k2.LocateAndGetByBucket(
                HLCTimestamp.Zero, space, KeyValueDurability.Persistent, ct);

            Assert.Equal(KeyValueResponseType.Get, result.Type);
            Assert.Equal(total, result.Items.Count);
            Assert.Equal(baseline, result.Items.Select(x => x.Item1).OrderBy(x => x, StringComparer.Ordinal).ToList());
        }
        finally
        {
            await LeaveCluster(r1, r2, r3);
        }
    }

    // ── Bucket_HashAndSchemaLog_KeepSingleLeaderPath ──────────────────────────

    /// <summary>
    /// Hash-routed buckets (not registered as KeyRange) continue to use the single-leader path
    /// and return correct results regardless of how many partitions exist.
    /// </summary>
    [Fact]
    public async Task Bucket_HashAndSchemaLog_KeepSingleLeaderPath()
    {
        // Use a 4-partition cluster but do NOT register the space as KeyRange.
        (IRaft r1, IRaft r2, IRaft r3, IKahuna k1, IKahuna k2, IKahuna k3) =
            await AssembleThreNodeCluster("memory", 4, raftLogger, kahunaLogger);

        try
        {
            CancellationToken ct = TestContext.Current.CancellationToken;
            const string prefix = "hash/bucket";
            const int    total  = 10;

            for (int i = 0; i < total; i++)
            {
                (KeyValueResponseType t, _, _) = await k1.LocateAndTrySetKeyValue(
                    HLCTimestamp.Zero, $"{prefix}/{i:D4}",
                    Encoding.UTF8.GetBytes("v" + i),
                    null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
                Assert.Equal(KeyValueResponseType.Set, t);
            }

            // Hash space — IsPrefixOpSafe always true; single-leader path; no scatter.
            KeyValueGetByBucketResult result = await k3.LocateAndGetByBucket(
                HLCTimestamp.Zero, prefix, KeyValueDurability.Persistent, ct);

            Assert.Equal(KeyValueResponseType.Get, result.Type);
            Assert.Equal(total, result.Items.Count);
        }
        finally
        {
            await LeaveCluster(r1, r2, r3);
        }
    }
}
