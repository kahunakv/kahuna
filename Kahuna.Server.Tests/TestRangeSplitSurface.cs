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
/// Tests for the manual split surface — the deterministic force-split an operator or a chaos run
/// uses instead of writing past a threshold and hoping the sampler fires inside the run.
/// <para>
/// The property under test is not "the split works" (the splitter's own suites cover that) but that
/// the <b>answer</b> is usable: a refusal says so without pretending, and an outcome the caller
/// cannot resolve from its side is never dressed up as a decision. A harness that reads "did not
/// happen" from a cutover that may still land will report a phantom violation.
/// </para>
/// </summary>
public sealed class TestRangeSplitSurface : BaseCluster
{
    private const string Space = "split-surface";
    private const string SplitKey = Space + "/m";

    private readonly ILogger<IRaft> raftLogger;
    private readonly ILogger<IKahuna> kahunaLogger;

    public TestRangeSplitSurface(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = TestLogFactory.Create(outputHelper, quietKommander: true);
        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    private static byte[] V(string s) => Encoding.UTF8.GetBytes(s);

    /// <summary>Every status that means "the map may still change"; the rest are decisions.</summary>
    private static readonly HashSet<string> IndeterminateStatuses = new(StringComparer.Ordinal)
    {
        nameof(SplitStatus.TransferFailed),
        nameof(SplitStatus.QuiesceFailed),
        nameof(SplitStatus.CutoverFailed),
        nameof(SplitStatus.ConcurrentSplit),
        "Indeterminate"
    };

    /// <summary>
    /// The invariant every response must hold regardless of which outcome the cluster produced:
    /// the <c>determinate</c> flag agrees with what the status means. A status that migrates between
    /// the two groups without the flag following is the bug this guards.
    /// </summary>
    private static void AssertDeterminacyMatchesStatus(KahunaSplitRangeResponse response)
    {
        Assert.Equal(!IndeterminateStatuses.Contains(response.Status), response.Determinate);

        if (!response.Success)
            Assert.False(string.IsNullOrEmpty(response.Reason), $"{response.Status} must carry a reason");
    }

    private async Task<(IRaft[] Rafts, KahunaManager[] Nodes)> AssembleCluster()
    {
        (IRaft r1, IRaft r2, IRaft r3, IKahuna k1, IKahuna k2, IKahuna k3) =
            await AssembleThreNodeCluster("memory", 3, raftLogger, kahunaLogger);

        return ([r1, r2, r3], [(KahunaManager)k1, (KahunaManager)k2, (KahunaManager)k3]);
    }

    private static async Task<KahunaManager> MetaLeader(IRaft[] rafts, KahunaManager[] nodes)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        while (true)
        {
            for (int i = 0; i < rafts.Length; i++)
                if (await rafts[i].AmILeader(RangeMapStore.MetaPartitionId, ct))
                    return nodes[i];

            await Task.Delay(25, ct);
        }
    }

    private static async Task<KahunaManager> MetaFollower(IRaft[] rafts, KahunaManager[] nodes)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        while (true)
        {
            for (int i = 0; i < rafts.Length; i++)
                if (!await rafts[i].AmILeader(RangeMapStore.MetaPartitionId, ct))
                    return nodes[i];

            await Task.Delay(25, ct);
        }
    }

    /// <summary>Registers the space on every node and writes the given keys through the router.</summary>
    private async Task<(IRaft[] Rafts, KahunaManager[] Nodes)> SetupSpace(params string[] keys)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (IRaft[] rafts, KahunaManager[] nodes) = await AssembleCluster();

        foreach (KahunaManager node in nodes)
            Assert.True((await node.RegisterKeyRangeWithOutcomeAsync(Space, ct)).Success);

        foreach (KahunaManager node in nodes)
            await WaitUntilAsync(() => node.GetRangeMap(Space).KeySpaces[0].Descriptors.Count == 1);

        foreach (string key in keys)
        {
            (KeyValueResponseType type, _, _) = await nodes[0].LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, key, V("v"), null, -1, KeyValueFlags.Set, 0,
                KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Set, type);
        }

        return (rafts, nodes);
    }

    private static List<KahunaRangeDescriptorResponse> DescriptorsOn(KahunaManager node) =>
        node.GetRangeMap(Space).KeySpaces[0].Descriptors;

    /// <summary>
    /// The happy path through the public entry point: the map gains two adjacent ranges on two
    /// partitions, and the response names the partition now serving the upper half.
    /// </summary>
    [Fact]
    public async Task Split_AtAKeyInsideTheRange_SucceedsAndIsDeterminate()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (IRaft[] rafts, KahunaManager[] nodes) = await SetupSpace(
            Space + "/a", Space + "/b", Space + "/p", Space + "/q");

        try
        {
            KahunaManager leader = await MetaLeader(rafts, nodes);
            KahunaSplitRangeResponse response = await leader.SplitRangeAtKeyWithOutcomeAsync(Space, SplitKey, ct);

            Assert.True(response.Success, $"split refused: {response.Status} — {response.Reason}");
            Assert.Equal(nameof(SplitStatus.Succeeded), response.Status);
            AssertDeterminacyMatchesStatus(response);
            Assert.True(response.NewPartitionId > 0);
            Assert.True(response.NewGeneration > 0);
            Assert.Null(response.Reason);
            Assert.Equal(StatusCodes.Status200OK, RangesHandlers.ToStatusCode(response.Success, response.Status));

            foreach (KahunaManager node in nodes)
                await WaitUntilAsync(() => DescriptorsOn(node).Count == 2);

            List<KahunaRangeDescriptorResponse> descriptors = DescriptorsOn(leader);
            Assert.Equal(SplitKey, descriptors[0].EndKey);
            Assert.Equal(SplitKey, descriptors[1].StartKey);
            Assert.Equal(response.NewPartitionId, descriptors[1].PartitionId);
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    /// <summary>
    /// A follower refuses before attempting anything, so the refusal is final and the map is
    /// untouched. Reaching partition creation first would have reported this as
    /// <c>PartitionCreationFailed</c> — the same status a genuine creation failure produces, which a
    /// caller must handle differently.
    /// </summary>
    [Fact]
    public async Task Split_OnAFollower_RefusesAsNotLeaderAndLeavesTheMapUntouched()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (IRaft[] rafts, KahunaManager[] nodes) = await SetupSpace(Space + "/a", Space + "/p");

        try
        {
            KahunaManager follower = await MetaFollower(rafts, nodes);
            KahunaSplitRangeResponse response = await follower.SplitRangeAtKeyWithOutcomeAsync(Space, SplitKey, ct);

            Assert.False(response.Success);
            Assert.Equal("NotLeader", response.Status);
            AssertDeterminacyMatchesStatus(response);
            Assert.NotEqual(nameof(SplitStatus.PartitionCreationFailed), response.Status);
            Assert.Equal(0, response.NewPartitionId);
            Assert.Equal(StatusCodes.Status409Conflict, RangesHandlers.ToStatusCode(response.Success, response.Status));

            // Nothing was attempted: every node still sees the single whole-space descriptor.
            foreach (KahunaManager node in nodes)
                Assert.Single(DescriptorsOn(node));
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    /// <summary>
    /// Splitting a range at its own start key would produce an empty lower half, and splitting a key
    /// space that has no descriptor has nothing to cut. Both are decisions — the map definitely did
    /// not change — and both must say so rather than surfacing as a generic failure.
    /// </summary>
    [Fact]
    public async Task Split_AtARangeStartOrOnAnUnregisteredSpace_IsRefusedAsADecision()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (IRaft[] rafts, KahunaManager[] nodes) = await SetupSpace(
            Space + "/a", Space + "/b", Space + "/p", Space + "/q");

        try
        {
            KahunaManager leader = await MetaLeader(rafts, nodes);
            Assert.True((await leader.SplitRangeAtKeyWithOutcomeAsync(Space, SplitKey, ct)).Success);
            await WaitUntilAsync(() => DescriptorsOn(leader).Count == 2);

            // The upper range now starts exactly at the split key. Cutting there again would leave
            // an empty half. (A key "outside the range" is unreachable through this entry point by
            // construction: the covering descriptor is the one the key itself falls in.)
            KahunaSplitRangeResponse atStart =
                await leader.SplitRangeAtKeyWithOutcomeAsync(Space, SplitKey, ct);

            Assert.False(atStart.Success);
            Assert.Equal(nameof(SplitStatus.InvalidSplitKey), atStart.Status);
            AssertDeterminacyMatchesStatus(atStart);

            KahunaSplitRangeResponse noRange =
                await leader.SplitRangeAtKeyWithOutcomeAsync("split-surface-absent", "split-surface-absent/m", ct);

            Assert.False(noRange.Success);
            Assert.Equal(nameof(SplitStatus.NoRange), noRange.Status);
            AssertDeterminacyMatchesStatus(noRange);

            // Both refusals left the two ranges from the first split exactly as they were.
            Assert.Equal(2, DescriptorsOn(leader).Count);
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    /// <summary>
    /// A split that would leave one half empty is refused by policy, and the refusal is a decision.
    /// This is the guard that stops a manual split from creating a range no key can ever route to.
    /// </summary>
    [Fact]
    public async Task Split_WhereOneHalfWouldBeEmpty_IsRefusedBelowMinRangeSize()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        // Every key sorts below the split key, so the upper half would hold nothing.
        (IRaft[] rafts, KahunaManager[] nodes) = await SetupSpace(Space + "/a", Space + "/b");

        try
        {
            KahunaManager leader = await MetaLeader(rafts, nodes);
            KahunaSplitRangeResponse response = await leader.SplitRangeAtKeyWithOutcomeAsync(Space, SplitKey, ct);

            Assert.False(response.Success);
            Assert.Equal(nameof(SplitStatus.BelowMinRangeSize), response.Status);
            AssertDeterminacyMatchesStatus(response);

            foreach (KahunaManager node in nodes)
                Assert.Single(DescriptorsOn(node));
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    /// <summary>
    /// Two callers racing to split the same range at the same key. Whatever the loser is told, only
    /// one split may land: the postcondition is two ranges, not three, and no gap or overlap. The
    /// loser's status is not pinned — which of the several failure points it hits depends on
    /// timing — but its determinacy must match whichever one it reports, because that flag is what a
    /// harness acts on.
    /// </summary>
    [Fact]
    public async Task Split_ConcurrentAtTheSameKey_LandsOnceAndClassifiesTheLoserHonestly()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (IRaft[] rafts, KahunaManager[] nodes) = await SetupSpace(
            Space + "/a", Space + "/b", Space + "/p", Space + "/q");

        try
        {
            KahunaManager leader = await MetaLeader(rafts, nodes);

            Task<KahunaSplitRangeResponse> first = leader.SplitRangeAtKeyWithOutcomeAsync(Space, SplitKey, ct);
            Task<KahunaSplitRangeResponse> second = leader.SplitRangeAtKeyWithOutcomeAsync(Space, SplitKey, ct);

            KahunaSplitRangeResponse[] responses = await Task.WhenAll(first, second);

            foreach (KahunaSplitRangeResponse response in responses)
                AssertDeterminacyMatchesStatus(response);

            Assert.Equal(1, responses.Count(r => r.Success));

            // The map tiles the space with exactly the two ranges one split produces. A second split
            // landing as well would show up here as three.
            foreach (KahunaManager node in nodes)
                await WaitUntilAsync(() => DescriptorsOn(node).Count == 2);

            foreach (KahunaManager node in nodes)
            {
                List<KahunaRangeDescriptorResponse> descriptors = DescriptorsOn(node);
                Assert.Equal(2, descriptors.Count);
                Assert.Null(descriptors[0].StartKey);
                Assert.Equal(SplitKey, descriptors[0].EndKey);
                Assert.Equal(SplitKey, descriptors[1].StartKey);
                Assert.Null(descriptors[1].EndKey);
            }
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    /// <summary>
    /// Malformed requests are the caller's to fix and are answered as such, without reaching Raft.
    /// </summary>
    [Fact]
    public async Task Split_WithMalformedInput_IsRefusedAsBadInput()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (IRaft[] rafts, KahunaManager[] nodes) = await AssembleCluster();

        try
        {
            foreach ((string keySpace, string splitKey) in
                     ((string, string)[])[("", "k"), ("db/meta", "db/meta/k"), (Space, "")])
            {
                KahunaSplitRangeResponse response =
                    await nodes[0].SplitRangeAtKeyWithOutcomeAsync(keySpace, splitKey, ct);

                Assert.False(response.Success);
                Assert.Equal("InvalidInput", response.Status);
                AssertDeterminacyMatchesStatus(response);
                Assert.Equal(StatusCodes.Status400BadRequest,
                    RangesHandlers.ToStatusCode(response.Success, response.Status));
            }
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    /// <summary>
    /// The wire shape a cross-repo consumer reads, including the determinacy flag it branches on.
    /// </summary>
    [Fact]
    public void SplitResponse_PinsItsWireNames()
    {
        KahunaSplitRangeResponse response = new()
        {
            Success = true,
            Status = "Succeeded",
            Determinate = true,
            NewPartitionId = 4,
            NewGeneration = 7
        };

        using (JsonDocument document = JsonDocument.Parse(
            JsonSerializer.Serialize(response, KahunaJsonContext.Default.KahunaSplitRangeResponse)))
        {
            Assert.True(document.RootElement.GetProperty("success").GetBoolean());
            Assert.Equal("Succeeded", document.RootElement.GetProperty("status").GetString());
            Assert.True(document.RootElement.GetProperty("determinate").GetBoolean());
            Assert.Equal(4, document.RootElement.GetProperty("newPartitionId").GetInt32());
            Assert.Equal(7, document.RootElement.GetProperty("newGeneration").GetInt64());
            Assert.Equal(JsonValueKind.Null, document.RootElement.GetProperty("leaderHint").ValueKind);
            Assert.Equal(JsonValueKind.Null, document.RootElement.GetProperty("reason").ValueKind);
        }

        KahunaSplitRangeRequest request = new() { KeySpace = "t:r", SplitKey = "t:r/m" };
        string requestJson = JsonSerializer.Serialize(request, KahunaJsonContext.Default.KahunaSplitRangeRequest);

        Assert.Contains("\"keySpace\":\"t:r\"", requestJson);
        Assert.Contains("\"splitKey\":\"t:r/m\"", requestJson);

        KahunaSplitRangeRequest? back =
            JsonSerializer.Deserialize(requestJson, KahunaJsonContext.Default.KahunaSplitRangeRequest);
        Assert.Equal("t:r/m", back!.SplitKey);
    }
}
