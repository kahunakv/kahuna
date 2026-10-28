using System.Text;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// A range move holds its quiesce with two guards that share one TTL: the write-fence range lock on the moving
/// range and the deadline on the replicated descriptor. Both reopen the range on their own when the TTL passes.
/// A move still running at that point — a long settle barrier, a slow catch-up copy or a slow hand-off under
/// load — no longer excludes writers: a write admitted on the source after the catch-up copy commits there and
/// is acknowledged, and a cutover after it routes the range away from that write for good. The move must refuse
/// its attempt instead of cutting over, and the acknowledged write must stay visible.
/// </summary>
public sealed class TestSplitQuiesceExpiry : BaseCluster
{
    private const string Space = "t:qx";

    private const string SplitKey = Space + "/m";

    private static readonly TimeSpan ShortQuiesce = TimeSpan.FromSeconds(2);

    private readonly ILogger<IRaft> raftLogger;

    private readonly ILogger<IKahuna> kahunaLogger;

    private readonly ITestOutputHelper output;

    public TestSplitQuiesceExpiry(ITestOutputHelper outputHelper)
    {
        output = outputHelper;
        ILoggerFactory loggerFactory = TestLogFactory.Create(outputHelper);
        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    private static async Task<(IRaft Raft, KahunaManager Kahuna)> LeaderOf(int partition, (IRaft, KahunaManager)[] nodes, CancellationToken ct)
    {
        while (true)
        {
            foreach ((IRaft raft, KahunaManager kahuna) in nodes)
                if (await raft.AmILeaderIfHosted(partition, ct))
                    return (raft, kahuna);
            await Task.Delay(50, ct);
        }
    }

    private static async Task<KeyValueResponseType> CommitThroughScript(IKahuna kahuna, string key, string value)
    {
        KeyValueTransactionResult result = await RetryOnMustRetry(
            kahuna, Encoding.UTF8.GetBytes($"BEGIN SET `{key}` '{value}' COMMIT END"), null, null);
        return result.Type;
    }

    private static async Task<string?> ReadThroughLocator(KahunaManager kahuna, string key, CancellationToken ct)
    {
        (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await RetryOnMustRetryAsync(
            () => kahuna.LocateAndTryGetValue(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct),
            r => r.Item1, timeoutMs: 30_000);

        return type == KeyValueResponseType.Get && entry?.Value is not null ? Encoding.UTF8.GetString(entry.Value) : null;
    }

    /// <summary>
    /// A replication-factor-1 cluster of six partitions, the layout of the cross-node write-loss tests: each data
    /// partition lives on exactly one node, so a write stranded on the source after a cutover is observable
    /// through the locator instead of being masked by a node-global store that every replica shares. The space's
    /// descriptor is seeded on a partition the split driver (the meta leader) does not host.
    /// </summary>
    private async Task<((IRaft, KahunaManager)[] Nodes, int SourcePartition)> SetupOneRange(CancellationToken ct)
    {
        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleCluster(
            3, "memory", 6, raftLogger, kahunaLogger, replicationFactor: 1,
            configureKahuna: configuration => configuration.RangeMoveQuiesceTtl = ShortQuiesce);

        (IRaft, KahunaManager)[] nodes = [.. rafts.Select((raft, i) => (raft, (KahunaManager)kahunas[i]))];

        foreach ((IRaft _, KahunaManager kahuna) in nodes)
            kahuna.RegisterKeyRange(Space);

        (IRaft driverRaft, KahunaManager metaLeader) = await LeaderOf(RangeMapStore.MetaPartitionId, nodes, ct);

        int sourcePartition = 0;
        for (int partitionId = 1; partitionId <= 6; partitionId++)
        {
            if (!driverRaft.HostsPartition(partitionId))
            {
                sourcePartition = partitionId;
                break;
            }
        }

        Assert.NotEqual(0, sourcePartition);

        Assert.True(await metaLeader.RangeMapStore.MutateAsync(
            _ => [new RangeDescriptor { KeySpace = Space, StartKey = null, EndKey = null, PartitionId = sourcePartition, Generation = 1 }], ct));

        foreach ((IRaft _, KahunaManager kahuna) in nodes)
            await WaitUntilAsync(() => kahuna.RangeMapStore.Current.Find(Space, Space + "/x")?.Generation == 1, timeoutMs: 30_000);

        foreach (string key in new[] { Space + "/a", Space + "/b", Space + "/p", Space + "/q" })
            Assert.Equal(KeyValueResponseType.Set, await CommitThroughScript(metaLeader, key, "seed"));

        return (nodes, sourcePartition);
    }

    /// <summary>
    /// The split's in-window work outlives the quiesce (the test seam holds the window past the TTL), then a
    /// durable write lands in the moving half. The split must refuse its cutover, the map must keep routing the
    /// half to the source, and the write — acknowledged or refused — must never be lost: every node reads back
    /// whatever was acknowledged.
    /// </summary>
    [Fact]
    public async Task Split_WhoseQuiesceLapsedBeforeTheCutover_RefusesTheCutover_AndKeepsTheLateWriteVisible()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        ((IRaft, KahunaManager)[] nodes, int sourcePartition) = await SetupOneRange(ct);

        try
        {
            string lateExisting = Space + "/q";
            string lateNew = Space + "/zz-late";
            KeyValueResponseType lateExistingType = KeyValueResponseType.Errored;
            KeyValueResponseType lateNewType = KeyValueResponseType.Errored;

            SplitOutcome outcome = default;
            for (int attempt = 0; attempt < 10; attempt++)
            {
                (IRaft _, KahunaManager driver) = await LeaderOf(RangeMapStore.MetaPartitionId, nodes, ct);

                outcome = await driver.ForceSplitAtKeyAsync(Space, SplitKey, duringQuiesce: async () =>
                {
                    // Outlive the quiesce, then write into the moving half: both guards have reopened the range.
                    await Task.Delay(ShortQuiesce + TimeSpan.FromMilliseconds(800), ct);

                    lateExistingType = await CommitThroughScript(nodes[1].Item2, lateExisting, "late");
                    lateNewType = await CommitThroughScript(nodes[2].Item2, lateNew, "late");
                }, ct);

                if (outcome.Status is not (SplitStatus.ProbeIndeterminate or SplitStatus.CutoverFailed or SplitStatus.UnsettledMovingIntents))
                    break;

                await Task.Delay(200, ct);
            }

            int destinationPartition = outcome.NewPartitionId;
            string hosts(int partition) => string.Join(",", nodes.Select((n, i) => n.Item1.HostsPartition(partition) ? i.ToString() : null).Where(s => s is not null));
            output.WriteLine($"split outcome {outcome.Status}; source P{sourcePartition} on nodes [{hosts(sourcePartition)}]; destination P{destinationPartition} on nodes [{hosts(destinationPartition)}]; late writes: {lateExisting}={lateExistingType}, {lateNew}={lateNewType}");

            // Whatever the late writes were told, every node reads it back through the locator. Checked first:
            // this is the loss the guard exists to prevent, and it is what a cutover after the lapse produces.
            List<string> lost = [];
            for (int n = 0; n < nodes.Length; n++)
            {
                string? existing = await ReadThroughLocator(nodes[n].Item2, lateExisting, ct);
                string expectedExisting = lateExistingType == KeyValueResponseType.Set ? "late" : "seed";
                if (existing != expectedExisting)
                    lost.Add($"{lateExisting} on node {n}: read {existing ?? "null"}, the write was answered {lateExistingType}");

                string? fresh = await ReadThroughLocator(nodes[n].Item2, lateNew, ct);
                string? expectedFresh = lateNewType == KeyValueResponseType.Set ? "late" : null;
                if (fresh != expectedFresh)
                    lost.Add($"{lateNew} on node {n}: read {fresh ?? "null"}, the write was answered {lateNewType}");
            }

            Assert.True(lost.Count == 0, $"split outcome {outcome.Status}; acknowledged writes lost after the lapsed quiesce:\n{string.Join("\n", lost)}");

            // The window reached the seam, the writes were admitted after the lapse, and the cutover was refused.
            Assert.Equal(KeyValueResponseType.Set, lateExistingType);
            Assert.Equal(KeyValueResponseType.Set, lateNewType);
            Assert.Equal(SplitStatus.QuiesceExpired, outcome.Status);

            // No cutover: the space still has its one descriptor on the source partition.
            foreach ((IRaft _, KahunaManager kahuna) in nodes)
            {
                Assert.Single(kahuna.RangeMapStore.Current.FindAll(Space));
                Assert.Equal(sourcePartition, kahuna.RangeMapStore.Current.Find(Space, lateExisting)?.PartitionId);
            }

            // The range is open again: a split that finishes inside its quiesce still succeeds afterwards.
            for (int attempt = 0; attempt < 20; attempt++)
            {
                (IRaft _, KahunaManager driver) = await LeaderOf(RangeMapStore.MetaPartitionId, nodes, ct);
                outcome = await driver.ForceSplitAtKeyAsync(Space, SplitKey, null, ct);
                if (outcome.IsSuccess)
                    break;
                await Task.Delay(200, ct);
            }

            Assert.True(outcome.IsSuccess, $"the follow-up split failed: {outcome.Status}");

            foreach ((IRaft _, KahunaManager kahuna) in nodes)
            {
                await WaitUntilAsync(() => kahuna.RangeMapStore.Current.FindAll(Space).Count == 2, timeoutMs: 30_000);
                Assert.Equal(lateExistingType == KeyValueResponseType.Set ? "late" : "seed", await ReadThroughLocator(kahuna, lateExisting, ct));
            }
        }
        finally
        {
            await LeaveCluster(nodes[0].Item1, nodes[1].Item1, nodes[2].Item1);
        }
    }

    /// <summary>The merge counterpart: the right range's quiesce lapses during the window, a write lands in it,
    /// and the merge refuses its cutover instead of retiring the partition that now holds the write.</summary>
    [Fact]
    public async Task Merge_WhoseQuiesceLapsedBeforeTheCutover_RefusesTheCutover_AndKeepsTheLateWriteVisible()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft r1, IRaft r2, IRaft r3, IKahuna k1, IKahuna k2, IKahuna k3) =
            await AssembleThreNodeCluster("memory", 4, raftLogger, kahunaLogger,
                configure: configuration => configuration.RangeMoveQuiesceTtl = ShortQuiesce);

        (IRaft, KahunaManager)[] nodes = [(r1, (KahunaManager)k1), (r2, (KahunaManager)k2), (r3, (KahunaManager)k3)];

        try
        {
            foreach ((IRaft _, KahunaManager kahuna) in nodes)
                kahuna.RegisterKeyRange(Space);

            const int leftPartition = RangeMapStore.FirstDataPartitionId;
            const int rightPartition = RangeMapStore.FirstDataPartitionId + 1;

            RangeDescriptor left = new() { KeySpace = Space, StartKey = null, EndKey = SplitKey, PartitionId = leftPartition, Generation = 1 };
            RangeDescriptor right = new() { KeySpace = Space, StartKey = SplitKey, EndKey = null, PartitionId = rightPartition, Generation = 1 };

            (IRaft _, KahunaManager metaLeader) = await LeaderOf(RangeMapStore.MetaPartitionId, nodes, ct);
            Assert.True(await metaLeader.RangeMapStore.MutateAsync(_ => [left, right], ct));

            foreach ((IRaft _, KahunaManager kahuna) in nodes)
                await WaitUntilAsync(() => kahuna.RangeMapStore.Current.FindAll(Space).Count == 2);

            foreach (string key in new[] { Space + "/a", Space + "/p", Space + "/q" })
                Assert.Equal(KeyValueResponseType.Set, await CommitThroughScript(k1, key, "seed"));

            string lateExisting = Space + "/q";
            KeyValueResponseType lateExistingType = KeyValueResponseType.Errored;

            MergeOutcome outcome = default;
            for (int attempt = 0; attempt < 10; attempt++)
            {
                (IRaft _, metaLeader) = await LeaderOf(RangeMapStore.MetaPartitionId, nodes, ct);

                outcome = await metaLeader.RangeMerger.MergeAsync(Space, left, right, duringQuiesce: async () =>
                {
                    await Task.Delay(ShortQuiesce + TimeSpan.FromMilliseconds(800), ct);
                    lateExistingType = await CommitThroughScript(nodes[1].Item2, lateExisting, "late");
                }, ct);

                if (outcome.Status is not (MergeStatus.CutoverFailed or MergeStatus.UnsettledMovingIntents or MergeStatus.ConcurrentChange))
                    break;

                await Task.Delay(200, ct);
            }

            Assert.Equal(KeyValueResponseType.Set, lateExistingType);
            Assert.Equal(MergeStatus.QuiesceExpired, outcome.Status);

            foreach ((IRaft _, KahunaManager kahuna) in nodes)
            {
                Assert.Equal(2, kahuna.RangeMapStore.Current.FindAll(Space).Count);
                Assert.Equal(rightPartition, kahuna.RangeMapStore.Current.Find(Space, lateExisting)?.PartitionId);
                Assert.Equal(lateExistingType == KeyValueResponseType.Set ? "late" : "seed", await ReadThroughLocator(kahuna, lateExisting, ct));
            }
        }
        finally
        {
            await LeaveCluster(r1, r2, r3);
        }
    }
}
