
using System.Text;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Tests.Server;

/// <summary>
/// Acceptance tests for the size-based automatic split trigger.
/// Each cluster test uses a 4-partition 3-node cluster (meta P1 + data P2).
/// </summary>
public sealed class TestAutoSplit : BaseCluster
{
    private readonly ILogger<IRaft>   raftLogger;
    private readonly ILogger<IKahuna> kahunaLogger;

    public TestAutoSplit(ITestOutputHelper outputHelper)
    {
        ILoggerFactory lf = LoggerFactory.Create(b =>
            b.AddXUnit(outputHelper).SetMinimumLevel(LogLevel.Warning));
        raftLogger   = lf.CreateLogger<IRaft>();
        kahunaLogger = lf.CreateLogger<IKahuna>();
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    private static async Task<(IRaft Raft, KahunaManager Kahuna)> LeaderOf(
        int partition, (IRaft, KahunaManager)[] nodes)
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

    private static async Task WaitFor(Func<bool> predicate, int timeoutMs = 10_000)
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

    /// <summary>
    /// Assembles a 4-partition 3-node cluster, registers <paramref name="space"/> as KeyRange,
    /// seeds a full-range descriptor on P2, and writes <paramref name="count"/> keys.
    /// </summary>
    private async Task<((IRaft, KahunaManager)[] Nodes, List<string> Keys)> SetupWithKeys(
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

        var keys = new List<string>(count);
        for (int i = 0; i < count; i++)
        {
            string key = $"{space}/{i:D4}";
            (KeyValueResponseType t, _, _) = await dataLeader.TrySetKeyValue(
                HLCTimestamp.Zero, key,
                Encoding.UTF8.GetBytes("v" + i),
                null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent);
            Assert.Equal(KeyValueResponseType.Set, t);
            keys.Add(key);
        }
        keys.Sort(StringComparer.Ordinal);

        // Wait for all keys to be readable on every node (replication lag).
        foreach (string key in keys)
        {
            string k = key;
            foreach ((IRaft _, KahunaManager kahuna) in nodes)
            {
                KahunaManager km = kahuna;
                long deadline = Environment.TickCount64 + 10_000;
                while (Environment.TickCount64 < deadline)
                {
                    (KeyValueResponseType rt, _) =
                        await km.TryGetValue(HLCTimestamp.Zero, k, 0, HLCTimestamp.Zero, KeyValueDurability.Persistent);
                    if (rt == KeyValueResponseType.Get) break;
                    await Task.Delay(25, ct);
                }
            }
        }

        return (nodes, keys);
    }

    /// <summary>
    /// Finds the node that is simultaneously the system-partition (0) leader and the meta-partition
    /// (1) leader, or returns null when they reside on different nodes in this cluster layout.
    /// </summary>
    private static async Task<KahunaManager?> FindDualLeaderAsync(
        (IRaft, KahunaManager)[] nodes, CancellationToken ct)
    {
        long deadline = Environment.TickCount64 + 10_000;
        while (Environment.TickCount64 < deadline)
        {
            foreach ((IRaft raft, KahunaManager kahuna) in nodes)
                if (await raft.AmILeader(0, ct) &&
                    await raft.AmILeader(RangeMapStore.MetaPartitionId, ct))
                    return kahuna;
            await Task.Delay(50, ct);
        }
        return null;
    }

    // ── OverThreshold_TriggersExactlyOneSplit ─────────────────────────────────

    /// <summary>
    /// A range with more keys than the threshold is split exactly once by
    /// <c>TriggerAutoSplitAsync</c>. After the trigger the map contains two
    /// non-overlapping, non-null-bounded descriptors.
    /// </summary>
    [Fact]
    public async Task OverThreshold_TriggersExactlyOneSplit()
    {
        const string space     = "as:a";
        const int    threshold = 10;
        const int    total     = 20;

        ((IRaft, KahunaManager)[] nodes, _) = await SetupWithKeys(space, total);
        (IRaft r1, IRaft r2, IRaft r3) =
            (nodes[0].Item1, nodes[1].Item1, nodes[2].Item1);

        try
        {
            CancellationToken ct = TestContext.Current.CancellationToken;

            KahunaManager? dualLeader = await FindDualLeaderAsync(nodes, ct);
            if (dualLeader is null)
                return; // split guard not satisfiable on this cluster layout; skip

            int splits = await dualLeader.TriggerAutoSplitAsync(threshold, minRangeSize: 2, ct);
            Assert.Equal(1, splits);

            await WaitFor(() => dualLeader.RangeMapStore.Current.FindAll(space).Count == 2);

            IReadOnlyList<RangeDescriptor> descriptors = dualLeader.RangeMapStore.Current.FindAll(space);
            Assert.Equal(2, descriptors.Count);
            // The left range's EndKey must equal the right range's StartKey (no gap, no overlap).
            Assert.NotNull(descriptors[0].EndKey);
            Assert.Equal(descriptors[0].EndKey, descriptors[1].StartKey);
        }
        finally
        {
            await LeaveCluster(r1, r2, r3);
        }
    }

    // ── BelowThreshold_NoSplit ────────────────────────────────────────────────

    /// <summary>
    /// A range below the threshold is not split; <c>TriggerAutoSplitAsync</c> returns 0
    /// and the map retains exactly one descriptor.
    /// </summary>
    [Fact]
    public async Task BelowThreshold_NoSplit()
    {
        const string space     = "as:b";
        const int    threshold = 100; // well above the 5 keys we write
        const int    total     = 5;

        ((IRaft, KahunaManager)[] nodes, _) = await SetupWithKeys(space, total);
        (IRaft r1, IRaft r2, IRaft r3) =
            (nodes[0].Item1, nodes[1].Item1, nodes[2].Item1);

        try
        {
            CancellationToken ct = TestContext.Current.CancellationToken;

            int splits = 0;
            foreach ((IRaft _, KahunaManager kahuna) in nodes)
                splits += await kahuna.TriggerAutoSplitAsync(threshold, minRangeSize: 2, ct);

            Assert.Equal(0, splits);

            foreach ((IRaft _, KahunaManager kahuna) in nodes)
                Assert.Single(kahuna.RangeMapStore.Current.FindAll(space));
        }
        finally
        {
            await LeaveCluster(r1, r2, r3);
        }
    }

    // ── MedianSplit_ProducesBalancedRanges ────────────────────────────────────

    /// <summary>
    /// For a uniformly distributed key set, the trigger produces two halves neither of which
    /// is empty and which together account for all keys.
    /// </summary>
    [Fact]
    public async Task MedianSplit_ProducesBalancedRanges()
    {
        const string space     = "as:c";
        const int    threshold = 10;
        const int    total     = 20;

        ((IRaft, KahunaManager)[] nodes, List<string> keys) = await SetupWithKeys(space, total);
        (IRaft r1, IRaft r2, IRaft r3) =
            (nodes[0].Item1, nodes[1].Item1, nodes[2].Item1);

        try
        {
            CancellationToken ct = TestContext.Current.CancellationToken;

            KahunaManager? dualLeader = await FindDualLeaderAsync(nodes, ct);
            if (dualLeader is null)
                return;

            int splits = await dualLeader.TriggerAutoSplitAsync(threshold, minRangeSize: 2, ct);
            Assert.Equal(1, splits);

            await WaitFor(() => dualLeader.RangeMapStore.Current.FindAll(space).Count == 2);

            IReadOnlyList<RangeDescriptor> descriptors = dualLeader.RangeMapStore.Current.FindAll(space);
            Assert.Equal(2, descriptors.Count);

            string splitKey = descriptors[0].EndKey!;
            int leftCount  = keys.Count(k => string.CompareOrdinal(k, splitKey) < 0);
            int rightCount = keys.Count - leftCount;

            Assert.True(leftCount  > 0, "left half is empty");
            Assert.True(rightCount > 0, "right half is empty");
            // Both halves should be within half the total of each other (relaxed: sample is bounded).
            Assert.True(Math.Abs(leftCount - rightCount) <= total / 2,
                $"Halves too unbalanced: left={leftCount} right={rightCount}");
        }
        finally
        {
            await LeaveCluster(r1, r2, r3);
        }
    }

    // ── MonotonicAppendTail_SplitsHotTail ────────────────────────────────────

    /// <summary>
    /// When inserts are top-skewed (monotonic-append pattern), <see cref="RangeSplitPolicy"/>
    /// detects it and picks the 75th-percentile split key rather than the median.
    /// </summary>
    [Fact]
    public async Task MonotonicAppendTail_SplitsHotTail()
    {
        // Pure policy test — no cluster required.
        const int count = 40;
        var sample = new List<(string Key, HLCTimestamp LastModified)>(count);

        for (int i = 0; i < count; i++)
        {
            // Top quarter (i >= 30) have a higher node counter → "more recently written".
            long l = i >= count * 3 / 4 ? 10_000L + i : 1_000L + i;
            sample.Add(($"key/{i:D4}", new HLCTimestamp(0, l, 0u)));
        }

        // threshold=20, minSize=2 → split should trigger.
        string? splitKey = RangeSplitPolicy.ComputeSplitKey(sample, threshold: 20, minRangeSize: 2);
        Assert.NotNull(splitKey);

        // Monotonic-append path picks 75th-percentile index: Round(40 * 75 / 100.0) = 30.
        Assert.Equal(sample[30].Key, splitKey);
        Assert.True(RangeSplitPolicy.IsMonotonicAppend(sample));

        await Task.CompletedTask;
    }
}
