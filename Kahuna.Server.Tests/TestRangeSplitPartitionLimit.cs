using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using System.Text;
using Grpc.Core;
using Kahuna.Communication.External.Grpc;
using Kahuna.Communication.External.Rest;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Shared.Communication.Rest;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Data;
using Kommander.System;
using Kommander.Time;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// A range split needs one more Raft partition, and two bounds can refuse it: Kommander's cluster-wide
/// partition ceiling (<c>RaftConfiguration.MaxPartitions</c>) and the per-key-space range quota
/// (<c>RangeMaxRangesPerKeySpace</c>). Both refuse before anything is created, so the answer must be a
/// determinate <c>PartitionLimitReached</c> that leaves the range map, the partition map and the
/// partition-id allocator unchanged — not the <c>PartitionCreationFailed</c> that warns of an unused
/// partition. The automatic splitter must stop re-asking while the cluster stays full, and resume once a
/// merge frees a slot.
/// </summary>
public sealed class TestRangeSplitPartitionLimit : BaseCluster
{
    private readonly ILoggerFactory loggerFactory;
    private readonly ILogger<IRaft> raftLogger;
    private readonly ILogger<IKahuna> kahunaLogger;

    public TestRangeSplitPartitionLimit(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper, quietKommander: true);
        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    private static byte[] V(string s) => Encoding.UTF8.GetBytes(s);

    private static string EndpointOf(int index) => $"localhost:{8001 + index}";

    private static int DataPartitions(IRaft raft) =>
        raft.GetPartitionMap().Count(r => r.PartitionId != RaftSystemConfig.SystemPartition);

    private static async Task WriteKeys(KahunaManager node, IEnumerable<string> keys, CancellationToken ct)
    {
        foreach (string key in keys)
        {
            (KeyValueResponseType type, _, _) = await RetryOnMustRetryAsync(
                () => node.LocateAndTrySetKeyValue(
                    HLCTimestamp.Zero, key, V("v"), null, -1, KeyValueFlags.Set, 0,
                    KeyValueDurability.Persistent, ct),
                r => r.Item1);
            Assert.Equal(KeyValueResponseType.Set, type);
        }
    }

    private static void AssertLimitRefusal(KahunaSplitRangeResponse response)
    {
        Assert.False(response.Success);
        Assert.Equal(nameof(SplitStatus.PartitionLimitReached), response.Status);
        Assert.True(response.Determinate);
        Assert.False(string.IsNullOrEmpty(response.Reason));
        // Nothing was created, so the "a partition may be left unused" caveat must not appear.
        Assert.DoesNotContain("left unused", response.Reason);
        Assert.Equal(StatusCodes.Status409Conflict, RangesHandlers.ToStatusCode(response.Success, response.Status));
    }

    /// <summary>
    /// Counts <c>kahuna.range.split.limit_refusals</c> for one key space, by reason. The tag filter keeps
    /// other tests running in parallel out of the count.
    /// </summary>
    private sealed class LimitRefusalCounter : IDisposable
    {
        private readonly MeterListener listener = new();
        private readonly ConcurrentDictionary<string, long> byReason = new(StringComparer.Ordinal);

        public LimitRefusalCounter(string keySpace)
        {
            listener.InstrumentPublished = (instrument, l) =>
            {
                if (instrument.Name == "kahuna.range.split.limit_refusals")
                    l.EnableMeasurementEvents(instrument);
            };
            listener.SetMeasurementEventCallback<long>((_, value, tags, _) =>
            {
                string? reason = null;
                bool matches = false;

                foreach (KeyValuePair<string, object?> tag in tags)
                {
                    if (tag.Key == "keyspace")
                        matches = Equals(tag.Value, keySpace);
                    else if (tag.Key == "reason")
                        reason = tag.Value as string;
                }

                if (matches && reason is not null)
                    byReason.AddOrUpdate(reason, value, (_, current) => current + value);
            });
            listener.Start();
        }

        public long this[string reason] => byReason.GetValueOrDefault(reason);

        public void Dispose() => listener.Dispose();
    }

    private sealed class StubServerCallContext : ServerCallContext
    {
        protected override CancellationToken CancellationTokenCore => CancellationToken.None;
        protected override string MethodCore => "test";
        protected override string HostCore => "test";
        protected override string PeerCore => "test";
        protected override DateTime DeadlineCore => DateTime.MaxValue;
        protected override Metadata RequestHeadersCore => new();
        protected override Metadata ResponseTrailersCore => new();
        protected override Status StatusCore { get; set; }
        protected override WriteOptions? WriteOptionsCore { get; set; }
        protected override AuthContext AuthContextCore => throw new NotSupportedException();
        protected override ContextPropagationToken CreatePropagationTokenCore(ContextPropagationOptions? options) => throw new NotSupportedException();
        protected override Task WriteResponseHeadersAsyncCore(Metadata responseHeaders) => throw new NotSupportedException();
    }

    // ── Single node ──────────────────────────────────────────────────────────

    /// <summary>
    /// At Kommander's partition ceiling, a manual split over REST and over gRPC both answer a determinate
    /// <c>PartitionLimitReached</c>, and neither the range map, the partition map nor the next partition id
    /// moves.
    /// </summary>
    [Fact]
    public async Task ManualSplit_AtThePartitionLimit_IsADeterminateRefusal_OnRestAndGrpc()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        const string space = "limit/cluster";
        using LimitRefusalCounter refusals = new(space);

        await using EmbeddedKahunaNode node = new(new()
        {
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1,
            MaxPartitions = 1
        }, loggerFactory);
        await node.StartAsync(ct);

        KahunaManager kahuna = (KahunaManager)node.Kahuna;
        Assert.True((await kahuna.RegisterKeyRangeWithOutcomeAsync(space, ct)).Success);
        await WriteKeys(kahuna, [space + "/a", space + "/b", space + "/p", space + "/q"], ct);

        int partitionsBefore = DataPartitions(node.Raft);
        int nextIdBefore = node.Raft.GetNextAvailablePartitionId();

        KahunaSplitRangeResponse rest = await kahuna.SplitRangeAtKeyWithOutcomeAsync(space, space + "/m", ct);
        AssertLimitRefusal(rest);

        GrpcSplitRangeResponse grpc = await new ClusterService(node.Raft, kahuna).SplitRange(
            new GrpcSplitRangeRequest { KeySpace = space, SplitKey = space + "/m" }, new StubServerCallContext());
        Assert.Equal(rest.Success, grpc.Success);
        Assert.Equal(rest.Status, grpc.Status);
        Assert.Equal(rest.Determinate, grpc.Determinate);
        Assert.Equal(rest.Reason ?? "", grpc.Reason);
        Assert.Equal(0, grpc.NewPartitionId);

        Assert.Single(kahuna.GetRangeMap(space).KeySpaces[0].Descriptors);
        Assert.Equal(partitionsBefore, DataPartitions(node.Raft));
        Assert.Equal(nextIdBefore, node.Raft.GetNextAvailablePartitionId());
        Assert.Equal(2, refusals[RangeSplitMetrics.PartitionLimitReason]);
    }

    /// <summary>
    /// A key space at its range quota is refused with the same determinate status, with a reason that
    /// names the space and the quota, while another key space can still split.
    /// </summary>
    [Fact]
    public async Task KeySpaceQuota_RefusesTheFullSpace_AndAnotherSpaceStillSplits()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        const string full = "quota/full";
        const string other = "quota/other";
        using LimitRefusalCounter refusals = new(full);

        await using EmbeddedKahunaNode node = new(new()
        {
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1,
            RangeMaxRangesPerKeySpace = 2
        }, loggerFactory);
        await node.StartAsync(ct);

        KahunaManager kahuna = (KahunaManager)node.Kahuna;

        Assert.True((await kahuna.RegisterKeyRangeWithOutcomeAsync(full, ct)).Success);
        await WriteKeys(kahuna, [full + "/a", full + "/b", full + "/c", full + "/d", full + "/p", full + "/q"], ct);

        KahunaSplitRangeResponse first = await kahuna.SplitRangeAtKeyWithOutcomeAsync(full, full + "/m", ct);
        Assert.True(first.Success, $"split refused: {first.Status} — {first.Reason}");

        int partitionsAtQuota = DataPartitions(node.Raft);

        // The lower half {a,b,c,d} divides at c, so only the quota stands in the way.
        KahunaSplitRangeResponse second = await kahuna.SplitRangeAtKeyWithOutcomeAsync(full, full + "/c", ct);
        AssertLimitRefusal(second);
        Assert.Contains(full, second.Reason);
        Assert.Contains("2", second.Reason);
        Assert.Equal(2, kahuna.GetRangeMap(full).KeySpaces[0].Descriptors.Count);
        Assert.Equal(partitionsAtQuota, DataPartitions(node.Raft));
        Assert.Equal(1, refusals[RangeSplitMetrics.KeySpaceQuotaReason]);

        Assert.True((await kahuna.RegisterKeyRangeWithOutcomeAsync(other, ct)).Success);
        await WriteKeys(kahuna, [other + "/a", other + "/b", other + "/p", other + "/q"], ct);

        KahunaSplitRangeResponse otherSplit = await kahuna.SplitRangeAtKeyWithOutcomeAsync(other, other + "/m", ct);
        Assert.True(otherSplit.Success, $"split refused: {otherSplit.Status} — {otherSplit.Reason}");
    }

    // ── Three nodes ──────────────────────────────────────────────────────────

    private async Task<(IRaft[] Rafts, KahunaManager[] Nodes)> AssembleLimitedCluster(
        int maxPartitions, Action<Kahuna.Server.Configuration.KahunaConfiguration>? configureKahuna = null)
    {
        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleCluster(
            3, "memory", 3, raftLogger, kahunaLogger,
            configureRaft: c => c.MaxPartitions = maxPartitions,
            configureKahuna: configureKahuna);

        return (rafts, [.. kahunas.Cast<KahunaManager>()]);
    }

    private static async Task<int> MetaLeaderIndex(IRaft[] rafts, CancellationToken ct)
    {
        while (true)
        {
            for (int i = 0; i < rafts.Length; i++)
                if (await rafts[i].AmILeaderIfHosted(RangeMapStore.MetaPartitionId, ct))
                    return i;

            await Task.Delay(25, ct);
        }
    }

    private static async Task RegisterEverywhere(KahunaManager[] nodes, string space, CancellationToken ct)
    {
        foreach (KahunaManager node in nodes)
            Assert.True((await node.RegisterKeyRangeWithOutcomeAsync(space, ct)).Success);

        foreach (KahunaManager node in nodes)
            await WaitUntilAsync(() => node.GetRangeMap(space).KeySpaces[0].Descriptors.Count == 1);
    }

    /// <summary>
    /// The refusal holds across a meta-leader change: the first leader refuses at the ceiling, P0
    /// leadership moves, and the next leader refuses the same split the same way. Every node still sees
    /// one range and the original partition count.
    /// </summary>
    [Fact]
    public async Task ManualSplit_AtThePartitionLimit_IsRefusedAcrossAMetaLeaderChange()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        const string space = "limit/leader-change";
        (IRaft[] rafts, KahunaManager[] nodes) = await AssembleLimitedCluster(maxPartitions: 3);

        try
        {
            await RegisterEverywhere(nodes, space, ct);
            await WriteKeys(nodes[0], [space + "/a", space + "/b", space + "/p", space + "/q"], ct);

            int first = await MetaLeaderIndex(rafts, ct);
            int nextIdBefore = rafts[first].GetNextAvailablePartitionId();

            AssertLimitRefusal(await nodes[first].SplitRangeAtKeyWithOutcomeAsync(space, space + "/m", ct));

            int second = (first + 1) % rafts.Length;
            RaftOperationStatus moved = await rafts[first].TransferLeadershipAsync(
                RangeMapStore.MetaPartitionId, EndpointOf(second), ct);
            Assert.Equal(RaftOperationStatus.Success, moved);
            await WaitUntilAsync(async () => await rafts[second].AmILeaderIfHosted(RangeMapStore.MetaPartitionId, ct), timeoutMs: 30_000);

            AssertLimitRefusal(await nodes[second].SplitRangeAtKeyWithOutcomeAsync(space, space + "/m", ct));

            for (int i = 0; i < nodes.Length; i++)
            {
                Assert.Single(nodes[i].GetRangeMap(space).KeySpaces[0].Descriptors);
                Assert.Equal(3, DataPartitions(rafts[i]));
            }

            Assert.Equal(nextIdBefore, rafts[second].GetNextAvailablePartitionId());
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    /// <summary>
    /// The automatic splitter at the ceiling: its first attempt is refused and counted, later passes do
    /// not ask again while the cluster stays full, and once a merge frees a slot the next pass splits.
    /// </summary>
    [Fact]
    public async Task AutoSplit_AtThePartitionLimit_StopsRetrying_AndResumesAfterAMerge()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        const string space = "limit/auto";
        using LimitRefusalCounter refusals = new(space);

        (IRaft[] rafts, KahunaManager[] nodes) = await AssembleLimitedCluster(maxPartitions: 4, c =>
        {
            c.RangeSplitThreshold = 4;
            c.RangeSplitMinRangeSize = 2;
            // The periodic checker must not split on its own cadence while the test drives the passes.
            c.CollectionInterval = TimeSpan.FromMinutes(10);
        });

        try
        {
            await RegisterEverywhere(nodes, space, ct);
            await WriteKeys(nodes[0],
                [.. "abcdefghij".Select(c => $"{space}/{c}"), .. "pqrstuvwxy".Select(c => $"{space}/{c}")], ct);

            int leader = await MetaLeaderIndex(rafts, ct);

            // Take the last slot by hand: 3 initial partitions + 1 = the ceiling.
            KahunaSplitRangeResponse manual = await nodes[leader].SplitRangeAtKeyWithOutcomeAsync(space, space + "/m", ct);
            Assert.True(manual.Success, $"split refused: {manual.Status} — {manual.Reason}");
            Assert.Equal(4, DataPartitions(rafts[leader]));

            // Both halves are above the threshold; the first one tried is refused and the pass stops.
            Assert.Equal(0, await nodes[leader].TriggerAutoSplitAsync(ct));
            Assert.Equal(1, refusals[RangeSplitMetrics.PartitionLimitReason]);

            // While the cluster stays full, later passes do not ask Kommander again.
            Assert.Equal(0, await nodes[leader].TriggerAutoSplitAsync(ct));
            Assert.Equal(0, await nodes[leader].TriggerAutoSplitAsync(ct));
            Assert.Equal(1, refusals[RangeSplitMetrics.PartitionLimitReason]);
            Assert.Equal(2, nodes[leader].GetRangeMap(space).KeySpaces[0].Descriptors.Count);

            // Fold the two halves back together; the merged-away partition frees the slot.
            await WaitUntilAsync(async () =>
            {
                await nodes[leader].KeyValues.TriggerAutoMergeAsync(1000, ct);
                return DataPartitions(rafts[leader]) == 3;
            }, timeoutMs: 30_000);

            int splits = 0;
            await WaitUntilAsync(async () =>
            {
                splits += await nodes[leader].TriggerAutoSplitAsync(ct);
                return splits > 0;
            }, timeoutMs: 30_000);

            Assert.Equal(4, DataPartitions(rafts[leader]));
            Assert.Equal(1, refusals[RangeSplitMetrics.PartitionLimitReason]);
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }
}
