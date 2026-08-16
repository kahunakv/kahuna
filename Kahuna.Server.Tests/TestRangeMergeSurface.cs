using System.Text;
using System.Text.Json;
using Kahuna.Communication.External.Rest;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Shared.Communication.Rest;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Time;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// Tests for the on-demand merge surface.
/// <para>
/// The whole point of this endpoint is that <c>0</c> stops meaning two different things. The
/// underlying trigger guards itself with a leadership check and returns 0 on every other node, so a
/// caller that merely forwarded the count could not tell "I am not the leader, nothing ran" from
/// "I am the leader and nothing was eligible" — and would conclude the cluster had no merges to do
/// while asking a node that never looked.
/// </para>
/// </summary>
public sealed class TestRangeMergeSurface : BaseCluster
{
    private const string Space = "merge-surface";

    private readonly ILogger<IRaft> raftLogger;
    private readonly ILogger<IKahuna> kahunaLogger;

    public TestRangeMergeSurface(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = TestLogFactory.Create(outputHelper, quietKommander: true);
        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    private static byte[] V(string s) => Encoding.UTF8.GetBytes(s);

    private async Task<(IRaft[] Rafts, KahunaManager[] Nodes)> AssembleCluster()
    {
        (IRaft r1, IRaft r2, IRaft r3, IKahuna k1, IKahuna k2, IKahuna k3) =
            await AssembleThreNodeCluster("memory", 3, raftLogger, kahunaLogger);

        return ([r1, r2, r3], [(KahunaManager)k1, (KahunaManager)k2, (KahunaManager)k3]);
    }

    /// <summary>
    /// Pins leadership of the partition that owns the range map onto one node, so a test asserting
    /// leader behaviour cannot pass vacuously by asking a node that would have skipped the work.
    /// </summary>
    private static async Task<KahunaManager> ForceMetaLeader(
        IRaft raft, KahunaManager node, CancellationToken ct)
    {
        await raft.ForceLeaderForTestingAsync(RangeMapStore.MetaPartitionId, ct);

        long deadline = Environment.TickCount64 + 10_000;
        while (Environment.TickCount64 < deadline)
        {
            if (await raft.AmILeader(RangeMapStore.MetaPartitionId, ct))
                return node;

            await Task.Delay(25, ct);
        }

        Assert.Fail("leadership of the range-map partition was not granted within 10 s");
        return null!;
    }

    private static async Task<int> IndexOfMetaFollower(IRaft[] rafts)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        while (true)
        {
            for (int i = 0; i < rafts.Length; i++)
                if (!await rafts[i].AmILeader(RangeMapStore.MetaPartitionId, ct))
                    return i;

            await Task.Delay(25, ct);
        }
    }

    private static List<KahunaRangeDescriptorResponse> DescriptorsOn(KahunaManager node) =>
        node.GetRangeMap(Space).KeySpaces[0].Descriptors;

    /// <summary>
    /// The regression this endpoint exists for: a follower must refuse, not report zero merges.
    /// Asserted against a cluster that genuinely has an eligible pair, so a bare count would have
    /// been wrong as well as ambiguous — the merges were there, this node just never looked.
    /// </summary>
    [Fact]
    public async Task Merge_OnFollower_RefusesInsteadOfReportingZeroMerges()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (IRaft[] rafts, KahunaManager[] nodes) = await AssembleCluster();

        try
        {
            await SetupTwoUnderMinRanges(rafts, nodes, ct);

            int follower = await IndexOfMetaFollower(rafts);
            KahunaMergeRangesResponse response = await nodes[follower].MergeRangesWithOutcomeAsync(ct);

            Assert.False(response.Success);
            Assert.Equal("NotLeader", response.Status);
            Assert.True(response.Determinate);
            Assert.Equal(0, response.Merges);
            Assert.False(string.IsNullOrEmpty(response.Reason));
            Assert.Equal(StatusCodes.Status409Conflict,
                RangesHandlers.ToStatusCode(response.Success, response.Status));

            // The pair really was eligible, so "0 merges" would have been a wrong answer and not
            // merely an ambiguous one: the ranges are still there for a leader to fold.
            foreach (KahunaManager node in nodes)
                Assert.Equal(2, DescriptorsOn(node).Count);
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    /// <summary>
    /// A leader folds the eligible pair and says how many it folded; the map collapses back to one
    /// whole-space range.
    /// </summary>
    [Fact]
    public async Task Merge_OnLeader_FoldsTheAdjacentUnderMinPair()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (IRaft[] rafts, KahunaManager[] nodes) = await AssembleCluster();

        try
        {
            await SetupTwoUnderMinRanges(rafts, nodes, ct);

            KahunaManager leader = await ForceMetaLeader(rafts[0], nodes[0], ct);
            KahunaMergeRangesResponse response = await leader.MergeRangesWithOutcomeAsync(ct);

            Assert.True(response.Success, $"merge refused: {response.Status} — {response.Reason}");
            Assert.Equal("Completed", response.Status);
            Assert.True(response.Determinate);
            Assert.Equal(1, response.Merges);
            Assert.Null(response.Reason);
            Assert.Equal(StatusCodes.Status200OK,
                RangesHandlers.ToStatusCode(response.Success, response.Status));

            await WaitUntilAsync(() => DescriptorsOn(leader).Count == 1);

            KahunaRangeDescriptorResponse survivor = Assert.Single(DescriptorsOn(leader));
            Assert.Null(survivor.StartKey);
            Assert.Null(survivor.EndKey);
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    /// <summary>
    /// A leader with nothing eligible answers success with zero merges. Paired with the follower
    /// test above, this is the distinction the endpoint exists to make: the same number, two
    /// different answers, told apart by the status rather than left to the caller to guess.
    /// </summary>
    [Fact]
    public async Task Merge_OnLeaderWithNothingEligible_SucceedsWithZeroMerges()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (IRaft[] rafts, KahunaManager[] nodes) = await AssembleCluster();

        try
        {
            // A single whole-space range has no adjacent pair to fold.
            foreach (KahunaManager node in nodes)
                Assert.True((await node.RegisterKeyRangeWithOutcomeAsync(Space, ct)).Success);

            foreach (KahunaManager node in nodes)
                await WaitUntilAsync(() => DescriptorsOn(node).Count == 1);

            KahunaManager leader = await ForceMetaLeader(rafts[0], nodes[0], ct);
            KahunaMergeRangesResponse response = await leader.MergeRangesWithOutcomeAsync(ct);

            Assert.True(response.Success);
            Assert.Equal("Completed", response.Status);
            Assert.True(response.Determinate);
            Assert.Equal(0, response.Merges);

            // Same count as the follower's refusal, opposite meaning — and the map is untouched.
            Assert.Single(DescriptorsOn(leader));
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    /// <summary>
    /// Registers the space, splits it, and leaves both halves under the merge threshold so the pass
    /// has exactly one eligible pair. Driven through the public split surface, so the two admin
    /// endpoints are exercised against each other rather than against a hand-built map.
    /// </summary>
    private async Task SetupTwoUnderMinRanges(IRaft[] rafts, KahunaManager[] nodes, CancellationToken ct)
    {
        foreach (KahunaManager node in nodes)
            Assert.True((await node.RegisterKeyRangeWithOutcomeAsync(Space, ct)).Success);

        foreach (KahunaManager node in nodes)
            await WaitUntilAsync(() => DescriptorsOn(node).Count == 1);

        // Three keys per half: comfortably below the default minimum, so the pair is eligible.
        foreach (string key in (string[])[Space + "/a", Space + "/b", Space + "/c",
                                          Space + "/p", Space + "/q", Space + "/r"])
        {
            (KeyValueResponseType type, _, _) = await nodes[0].LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, key, V("v"), null, -1, KeyValueFlags.Set, 0,
                KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Set, type);
        }

        KahunaManager splitLeader = await ForceMetaLeader(rafts[0], nodes[0], ct);
        KahunaSplitRangeResponse split =
            await splitLeader.SplitRangeAtKeyWithOutcomeAsync(Space, Space + "/m", ct);
        Assert.True(split.Success, $"split refused: {split.Status} — {split.Reason}");

        foreach (KahunaManager node in nodes)
            await WaitUntilAsync(() => DescriptorsOn(node).Count == 2);
    }

    /// <summary>The wire shape, including the flag a harness branches on.</summary>
    [Fact]
    public void MergeResponse_PinsItsWireNames()
    {
        KahunaMergeRangesResponse response = new()
        {
            Success = true, Status = "Completed", Determinate = true, Merges = 2
        };

        using (JsonDocument document = JsonDocument.Parse(
            JsonSerializer.Serialize(response, KahunaJsonContext.Default.KahunaMergeRangesResponse)))
        {
            Assert.True(document.RootElement.GetProperty("success").GetBoolean());
            Assert.Equal("Completed", document.RootElement.GetProperty("status").GetString());
            Assert.True(document.RootElement.GetProperty("determinate").GetBoolean());
            Assert.Equal(2, document.RootElement.GetProperty("merges").GetInt32());
            Assert.Equal(JsonValueKind.Null, document.RootElement.GetProperty("leaderHint").ValueKind);
            Assert.Equal(JsonValueKind.Null, document.RootElement.GetProperty("reason").ValueKind);
        }

        KahunaMergeRangesResponse refused = new()
        {
            Success = false, Status = "NotLeader", Determinate = true,
            LeaderHint = "localhost:8002", Reason = "not the leader"
        };

        using (JsonDocument document = JsonDocument.Parse(
            JsonSerializer.Serialize(refused, KahunaJsonContext.Default.KahunaMergeRangesResponse)))
        {
            Assert.Equal("NotLeader", document.RootElement.GetProperty("status").GetString());
            Assert.Equal(0, document.RootElement.GetProperty("merges").GetInt32());
            Assert.Equal("localhost:8002", document.RootElement.GetProperty("leaderHint").GetString());
        }
    }
}
