using Grpc.Core;
using Kahuna.Communication.External.Grpc;
using Kahuna.Communication.External.Rest;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Shared.Communication.Rest;
using Kommander;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kahuna.Server.Tests;

/// <summary>
/// Tests for the key-space registration surface — the call that puts a key space under key-range
/// routing, and its teardown counterpart.
/// <para>
/// Registration has two halves that behave differently and are easy to confuse: the routing-mode
/// flip is node-local and unreplicated, while the whole-space seed descriptor is a replicated
/// meta-partition write that a follower forwards to the leader. The outcome these calls report has
/// to keep the two apart, because a caller that reads "success" while the descriptor never landed
/// has a key space that routes by key range with nothing to route to — and every write to it throws.
/// </para>
/// </summary>
public sealed class TestKeyRangeRegistrationSurface : BaseCluster
{
    private readonly ILogger<IRaft> raftLogger;
    private readonly ILogger<IKahuna> kahunaLogger;

    public TestKeyRangeRegistrationSurface(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = TestLogFactory.Create(outputHelper, quietKommander: true);
        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    private async Task<(IRaft[] Rafts, KahunaManager[] Nodes)> AssembleCluster()
    {
        (IRaft r1, IRaft r2, IRaft r3, IKahuna k1, IKahuna k2, IKahuna k3) =
            await AssembleThreNodeCluster("memory", 3, raftLogger, kahunaLogger);

        return ([r1, r2, r3], [(KahunaManager)k1, (KahunaManager)k2, (KahunaManager)k3]);
    }

    /// <summary>Any node that does not lead the meta partition, so the forwarding path is exercised.</summary>
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

    /// <summary>
    /// A follower answers the registration itself: the seed is forwarded to the meta-partition
    /// leader, so this call is not leader-only and must not refuse. The second call reports that
    /// someone already seeded, which is still a success — the caller has to send this to every node,
    /// and only the first of those calls can be the one that seeds.
    /// </summary>
    [Fact]
    public async Task Register_OnFollower_SeedsThenReportsAlreadySeeded()
    {
        const string space = "registration:follower";
        (IRaft[] rafts, KahunaManager[] nodes) = await AssembleCluster();

        try
        {
            CancellationToken ct = TestContext.Current.CancellationToken;
            KahunaManager follower = await MetaFollower(rafts, nodes);

            KahunaRegisterKeyRangeResponse first =
                await follower.RegisterKeyRangeWithOutcomeAsync(space, ct);

            Assert.True(first.Success, $"register refused: {first.Status} — {first.Reason}");
            Assert.Equal("Seeded", first.Status);
            Assert.True(first.Seeded);
            Assert.Equal(nameof(RoutingMode.KeyRange), first.RoutingMode);
            Assert.Equal(1, first.DescriptorCount);
            Assert.Null(first.Reason);

            // Registering again is what every other node in the cluster does; it is a success that
            // did not seed, which is exactly what a caller needs to tell apart from a refusal.
            KahunaRegisterKeyRangeResponse second =
                await follower.RegisterKeyRangeWithOutcomeAsync(space, ct);

            Assert.True(second.Success);
            Assert.Equal("AlreadySeeded", second.Status);
            Assert.False(second.Seeded);
            Assert.Equal(1, second.DescriptorCount);

            // The descriptor is replicated even though the routing mode is not, so every node ends
            // up covering the space.
            foreach (KahunaManager node in nodes)
                await WaitUntilAsync(() => node.GetRangeMap(space).KeySpaces[0].Descriptors.Count == 1);
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    /// <summary>
    /// Empty and <c>/meta</c> key spaces are refused with a status, not an exception. The registry
    /// throws <see cref="ArgumentException"/> for both, and an unguarded call would surface that to
    /// a caller as an unclassifiable 500 rather than "you sent the wrong thing".
    /// </summary>
    [Fact]
    public async Task Register_InvalidKeySpace_IsRefusedWithoutThrowing()
    {
        (IRaft[] rafts, KahunaManager[] nodes) = await AssembleCluster();

        try
        {
            CancellationToken ct = TestContext.Current.CancellationToken;

            foreach (string invalid in (string[])["", "db/meta"])
            {
                KahunaRegisterKeyRangeResponse registered =
                    await nodes[0].RegisterKeyRangeWithOutcomeAsync(invalid, ct);

                Assert.False(registered.Success);
                Assert.Equal("InvalidInput", registered.Status);
                Assert.False(registered.Seeded);
                Assert.Equal(nameof(RoutingMode.Hash), registered.RoutingMode);
                Assert.False(string.IsNullOrEmpty(registered.Reason));
                Assert.Equal(StatusCodes.Status400BadRequest,
                    RangesHandlers.ToStatusCode(registered.Success, registered.Status));

                KahunaRemoveKeyRangeResponse removed =
                    await nodes[0].RemoveKeyRangeWithOutcomeAsync(invalid, ct);

                Assert.False(removed.Success);
                Assert.Equal("InvalidInput", removed.Status);
                Assert.Equal(StatusCodes.Status400BadRequest,
                    RangesHandlers.ToStatusCode(removed.Success, removed.Status));
            }

            // The refusals left nothing behind: no mode flip, no descriptor.
            Assert.Empty(nodes[0].GetRangeMap().KeySpaces);
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    /// <summary>
    /// Teardown drops the descriptors and says so. Removal is idempotent, so a second call reports
    /// the same settled state rather than inventing a failure.
    /// </summary>
    [Fact]
    public async Task Unregister_RemovesDescriptorsAndIsIdempotent()
    {
        const string space = "registration:teardown";
        (IRaft[] rafts, KahunaManager[] nodes) = await AssembleCluster();

        try
        {
            CancellationToken ct = TestContext.Current.CancellationToken;

            Assert.True((await nodes[0].RegisterKeyRangeWithOutcomeAsync(space, ct)).Success);
            foreach (KahunaManager node in nodes)
                await WaitUntilAsync(() => node.GetRangeMap(space).KeySpaces[0].Descriptors.Count == 1);

            KahunaRemoveKeyRangeResponse removed = await nodes[0].RemoveKeyRangeWithOutcomeAsync(space, ct);

            Assert.True(removed.Success, $"unregister refused: {removed.Status} — {removed.Reason}");
            Assert.Equal("Removed", removed.Status);
            Assert.Equal(0, removed.DescriptorCount);

            KahunaRemoveKeyRangeResponse again = await nodes[0].RemoveKeyRangeWithOutcomeAsync(space, ct);
            Assert.True(again.Success);
            Assert.Equal("Removed", again.Status);

            foreach (KahunaManager node in nodes)
                await WaitUntilAsync(() => node.GetRangeMap(space).KeySpaces[0].Descriptors.Count == 0);
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    /// <summary>
    /// The external gRPC call seeds. It used to flip the routing mode alone and answer
    /// <c>Success = true</c> unconditionally, which handed the caller a key space routed by key range
    /// with no descriptor behind it — so the very next write to that space threw. A test that only
    /// asserted <c>Success</c> would have passed against that bug, which is why this one asserts the
    /// descriptor exists.
    /// </summary>
    [Fact]
    public async Task RegisterKeyRangeOverGrpc_SeedsTheDescriptor()
    {
        const string space = "registration:grpc";
        (IRaft[] rafts, KahunaManager[] nodes) = await AssembleCluster();

        try
        {
            KeyValuesService service = new(nodes[0], NullLogger<IKahuna>.Instance);

            GrpcRegisterKeyRangeResponse response = await service.RegisterKeyRange(
                new GrpcRegisterKeyRangeRequest { KeySpace = space }, new StubServerCallContext());

            Assert.True(response.Success, $"register refused: {response.Status} — {response.Reason}");
            Assert.Equal("Seeded", response.Status);
            Assert.True(response.Seeded);
            Assert.Equal(nameof(RoutingMode.KeyRange), response.RoutingMode);
            Assert.Equal(1, response.DescriptorCount);

            // The claim that matters: the space is actually covered, not merely flagged registered.
            KahunaRangeMapResponse map = nodes[0].GetRangeMap(space);
            Assert.Single(map.KeySpaces[0].Descriptors);
            Assert.Null(map.KeySpaces[0].Descriptors[0].StartKey);
            Assert.Null(map.KeySpaces[0].Descriptors[0].EndKey);

            // An invalid space crosses the wire as a typed refusal, not an RpcException.
            GrpcRegisterKeyRangeResponse refused = await service.RegisterKeyRange(
                new GrpcRegisterKeyRangeRequest { KeySpace = "db/meta" }, new StubServerCallContext());

            Assert.False(refused.Success);
            Assert.Equal("InvalidInput", refused.Status);
            Assert.False(string.IsNullOrEmpty(refused.Reason));
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    /// <summary>
    /// The status-to-code mapping a caller relies on to know whether to fix its request or retry.
    /// Malformed input is the caller's to correct; every other refusal is a condition of the cluster,
    /// and only the body distinguishes "never going to work" from "not visible here yet".
    /// </summary>
    [Fact]
    public void StatusCodes_SeparateBadInputFromClusterConditions()
    {
        Assert.Equal(StatusCodes.Status200OK, RangesHandlers.ToStatusCode(true, "Seeded"));
        Assert.Equal(StatusCodes.Status200OK, RangesHandlers.ToStatusCode(true, "AlreadySeeded"));
        Assert.Equal(StatusCodes.Status200OK, RangesHandlers.ToStatusCode(true, "Removed"));

        Assert.Equal(StatusCodes.Status400BadRequest, RangesHandlers.ToStatusCode(false, "InvalidInput"));

        Assert.Equal(StatusCodes.Status409Conflict, RangesHandlers.ToStatusCode(false, "Indeterminate"));
        Assert.Equal(StatusCodes.Status409Conflict, RangesHandlers.ToStatusCode(false, "QuiesceWindowOpen"));
        Assert.Equal(StatusCodes.Status409Conflict, RangesHandlers.ToStatusCode(false, "KeyRangeDisabled"));
    }

    /// <summary>
    /// The wire names and the null-vs-empty handling of <c>reason</c>: a successful registration
    /// carries no reason, and a refusal always carries one.
    /// </summary>
    [Fact]
    public void RegistrationResponses_PinTheirWireNames()
    {
        KahunaRegisterKeyRangeResponse register = new()
        {
            Success = true, Status = "Seeded", Seeded = true,
            RoutingMode = "KeyRange", DescriptorCount = 1
        };

        using (System.Text.Json.JsonDocument document = System.Text.Json.JsonDocument.Parse(
            System.Text.Json.JsonSerializer.Serialize(
                register, KahunaJsonContext.Default.KahunaRegisterKeyRangeResponse)))
        {
            Assert.True(document.RootElement.GetProperty("success").GetBoolean());
            Assert.Equal("Seeded", document.RootElement.GetProperty("status").GetString());
            Assert.True(document.RootElement.GetProperty("seeded").GetBoolean());
            Assert.Equal("KeyRange", document.RootElement.GetProperty("routingMode").GetString());
            Assert.Equal(1, document.RootElement.GetProperty("descriptorCount").GetInt32());
            Assert.Equal(System.Text.Json.JsonValueKind.Null,
                document.RootElement.GetProperty("reason").ValueKind);
        }

        KahunaRemoveKeyRangeResponse remove = new()
        {
            Success = false, Status = "QuiesceWindowOpen",
            RoutingMode = "KeyRange", DescriptorCount = 2, Reason = "retry shortly"
        };

        using (System.Text.Json.JsonDocument document = System.Text.Json.JsonDocument.Parse(
            System.Text.Json.JsonSerializer.Serialize(
                remove, KahunaJsonContext.Default.KahunaRemoveKeyRangeResponse)))
        {
            Assert.False(document.RootElement.GetProperty("success").GetBoolean());
            Assert.Equal("QuiesceWindowOpen", document.RootElement.GetProperty("status").GetString());
            Assert.Equal(2, document.RootElement.GetProperty("descriptorCount").GetInt32());
            Assert.Equal("retry shortly", document.RootElement.GetProperty("reason").GetString());
        }

        // The request the caller sends, in the source-generated context that carries it.
        KahunaKeyRangeRequest request = new() { KeySpace = "jepsen/register" };
        string requestJson = System.Text.Json.JsonSerializer.Serialize(
            request, KahunaJsonContext.Default.KahunaKeyRangeRequest);

        Assert.Contains("\"keySpace\":\"jepsen/register\"", requestJson);
        Assert.Equal("jepsen/register", System.Text.Json.JsonSerializer.Deserialize(
            requestJson, KahunaJsonContext.Default.KahunaKeyRangeRequest)!.KeySpace);
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
}
