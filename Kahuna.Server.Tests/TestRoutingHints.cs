using Google.Protobuf;
using Grpc.Core;
using Kahuna.Client.Routing;
using Kahuna.Communication.External.Grpc;
using Kahuna.Server.Configuration;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Routing;
using Kommander;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kahuna.Server.Tests;

/// <summary>
/// The server half of learned routing: a response tells the caller where the resource it addressed
/// actually lives, so the next operation on that resource can start there.
///
/// <para>
/// These drive the real gRPC entry points against a three-node cluster and round-trip every reply
/// through protobuf, so an unset field cannot pass for a populated one. The distinction that
/// matters is who answered: a node that owns the resource reports itself as the executor, and a
/// node that forwarded reports where it sent the operation.
/// </para>
/// </summary>
public sealed class TestRoutingHints : BaseCluster
{
    private const int Partitions = 4;

    /// <summary>
    /// The nodes route on <c>localhost:800N</c> and advertise <c>http://localhost:800N</c>, which is
    /// the derivation an operator gets by default.
    /// </summary>
    private static void AdvertiseOverHttp(KahunaConfiguration configuration) =>
        configuration.AdvertisedClientScheme = "http://";

    private static ServerCallContext Context() => new StubServerCallContext();

    /// <summary>The partition a hash-routed key or lock resource lands on, as the server routes it.</summary>
    private static int PartitionOf(string resource) =>
        1 + (int)HashUtils.InversePrefixedHash(resource, '/', Partitions);

    private static async Task<(IRaft Raft, IKahuna Kahuna)> Owner(
        int partitionId, (IRaft, IKahuna)[] nodes, CancellationToken cancellationToken)
    {
        foreach ((IRaft raft, IKahuna kahuna) in nodes)
        {
            if (await raft.AmILeader(partitionId, cancellationToken))
                return (raft, kahuna);
        }

        throw new InvalidOperationException($"No node leads partition {partitionId}.");
    }

    private static (IRaft Raft, IKahuna Kahuna) NonOwner(IRaft owner, (IRaft, IKahuna)[] nodes)
    {
        foreach ((IRaft raft, IKahuna kahuna) in nodes)
        {
            if (!ReferenceEquals(raft, owner))
                return (raft, kahuna);
        }

        throw new InvalidOperationException("Every node owns the partition.");
    }

    /// <summary>
    /// The behaviour the whole feature rests on: a request deliberately sent to a node that does not own the key executes
    /// once and comes back naming the owner, and a request sent to the owner names the owner as the
    /// executor. Together those are what lets a client stop forwarding after one operation.
    /// </summary>
    [Fact]
    public async Task AKeyValueWrite_ReportsWhereItWasAdmitted()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft raft1, IRaft raft2, IRaft raft3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster("memory", Partitions, NullLogger<IRaft>.Instance, NullLogger<IKahuna>.Instance, AdvertiseOverHttp);

        try
        {
            (IRaft, IKahuna)[] nodes = [(raft1, kahuna1), (raft2, kahuna2), (raft3, kahuna3)];

            string key = "hints/" + Guid.NewGuid().ToString("N")[..8];

            (IRaft ownerRaft, IKahuna ownerKahuna) = await Owner(PartitionOf(key), nodes, ct);
            (IRaft otherRaft, IKahuna otherKahuna) = NonOwner(ownerRaft, nodes);

            string ownerUrl = "http://" + ownerRaft.GetLocalEndpoint();

            // Sent to a node that does not own the key: it forwards, and reports the destination.
            KeyValuesService forwarding = new(otherKahuna, NullLogger<IKahuna>.Instance);

            GrpcTrySetKeyValueResponse forwarded = await OnWire(await forwarding.TrySetKeyValue(
                new GrpcTrySetKeyValueRequest { Key = key, Value = Payload("v1"), ExpiresMs = 0, Durability = GrpcKeyValueDurability.Persistent },
                Context()));

            Assert.Equal(GrpcKeyValueResponseType.TypeSet, forwarded.Type);
            Assert.NotNull(forwarded.Route);
            Assert.Equal(ownerUrl, forwarded.Route.Endpoint);
            Assert.Equal(PartitionOf(key), forwarded.Route.PartitionId);

            // The endpoint is the part a client acts on, and it names the owner either way. The
            // provenance depends on the transport: over gRPC the forwarded operation is served in
            // another process, so the forwarder's suggestion is what comes back, while this test's
            // in-memory transport serves it in the same flow and the executor's own record — which is
            // strictly better information — supersedes it.
            Assert.NotEqual(GrpcRouteProvenance.RouteProvenanceUnknown, forwarded.Route.Provenance);

            // Sent to the owner: it executes, and reports itself.
            KeyValuesService owning = new(ownerKahuna, NullLogger<IKahuna>.Instance);

            GrpcTrySetKeyValueResponse executed = await OnWire(await owning.TrySetKeyValue(
                new GrpcTrySetKeyValueRequest { Key = key, Value = Payload("v2"), ExpiresMs = 0, Durability = GrpcKeyValueDurability.Persistent },
                Context()));

            Assert.Equal(GrpcKeyValueResponseType.TypeSet, executed.Type);
            Assert.NotNull(executed.Route);
            Assert.Equal(ownerUrl, executed.Route.Endpoint);
            Assert.Equal(GrpcRouteProvenance.RouteProvenanceExecuted, executed.Route.Provenance);

            // The two nodes really are different ones, so the forwarded leg above was a forward.
            Assert.NotEqual(ownerRaft.GetLocalEndpoint(), otherRaft.GetLocalEndpoint());
        }
        finally
        {
            await LeaveCluster(raft1, raft2, raft3);
        }
    }

    /// <summary>
    /// A key that does not exist is a terminal answer about a key whose owner was resolved. Without
    /// a hint on it, a read-mostly workload over absent keys would never learn a route at all.
    /// </summary>
    [Fact]
    public async Task AReadThatFindsNothing_StillReportsTheRoute()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft raft1, IRaft raft2, IRaft raft3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster("memory", Partitions, NullLogger<IRaft>.Instance, NullLogger<IKahuna>.Instance, AdvertiseOverHttp);

        try
        {
            (IRaft, IKahuna)[] nodes = [(raft1, kahuna1), (raft2, kahuna2), (raft3, kahuna3)];

            string key = "absent/" + Guid.NewGuid().ToString("N")[..8];

            (IRaft ownerRaft, IKahuna ownerKahuna) = await Owner(PartitionOf(key), nodes, ct);

            KeyValuesService service = new(ownerKahuna, NullLogger<IKahuna>.Instance);

            GrpcTryGetKeyValueResponse missing = await OnWire(await service.TryGetKeyValue(
                new GrpcTryGetKeyValueRequest { Key = key, Revision = -1, Durability = GrpcKeyValueDurability.Persistent },
                Context()));

            Assert.Equal(GrpcKeyValueResponseType.TypeDoesNotExist, missing.Type);
            Assert.NotNull(missing.Route);
            Assert.Equal("http://" + ownerRaft.GetLocalEndpoint(), missing.Route.Endpoint);
        }
        finally
        {
            await LeaveCluster(raft1, raft2, raft3);
        }
    }

    /// <summary>
    /// Locks route in their own name space, through the hash router rather than the range map, and
    /// their responses carry the route the same way a key's do.
    /// </summary>
    [Fact]
    public async Task ALockAcquisition_ReportsWhereItWasAdmitted()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft raft1, IRaft raft2, IRaft raft3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster("memory", Partitions, NullLogger<IRaft>.Instance, NullLogger<IKahuna>.Instance, AdvertiseOverHttp);

        try
        {
            (IRaft, IKahuna)[] nodes = [(raft1, kahuna1), (raft2, kahuna2), (raft3, kahuna3)];

            string resource = "locks/" + Guid.NewGuid().ToString("N")[..8];

            (IRaft ownerRaft, _) = await Owner(PartitionOf(resource), nodes, ct);
            (_, IKahuna otherKahuna) = NonOwner(ownerRaft, nodes);

            LocksService service = new(otherKahuna, new KahunaConfiguration(), ownerRaft, NullLogger<IKahuna>.Instance);

            GrpcTryLockResponse response = GrpcTryLockResponse.Parser.ParseFrom((await service.TryLock(
                new GrpcTryLockRequest
                {
                    Resource = resource,
                    Owner = ByteString.CopyFromUtf8("owner-1"),
                    ExpiresMs = 5000,
                    Durability = GrpcLockDurability.LockDurabilityEphemeral
                },
                Context())).ToByteArray());

            Assert.Equal(GrpcLockResponseType.LockResponseTypeLocked, response.Type);
            Assert.NotNull(response.Route);
            Assert.Equal("http://" + ownerRaft.GetLocalEndpoint(), response.Route.Endpoint);
            Assert.Equal(PartitionOf(resource), response.Route.PartitionId);
        }
        finally
        {
            await LeaveCluster(raft1, raft2, raft3);
        }
    }

    /// <summary>
    /// A batch spanning several owners cannot be described by one endpoint, so each item points at
    /// its own entry in a deduplicated table — and one endpoint is written once however many items
    /// resolved to it.
    /// </summary>
    [Fact]
    public async Task ABatchedRead_ReportsARoutePerItem()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft raft1, IRaft raft2, IRaft raft3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster("memory", Partitions, NullLogger<IRaft>.Instance, NullLogger<IKahuna>.Instance, AdvertiseOverHttp);

        try
        {
            KeyValuesService writer = new(kahuna1, NullLogger<IKahuna>.Instance);

            List<string> keys = [];

            // Enough distinct key spaces that the batch is very unlikely to land on one partition.
            for (int i = 0; i < 12; i++)
            {
                string key = $"batch{i}/" + Guid.NewGuid().ToString("N")[..8];
                keys.Add(key);

                GrpcTrySetKeyValueResponse set = await writer.TrySetKeyValue(
                    new GrpcTrySetKeyValueRequest { Key = key, Value = Payload("v"), ExpiresMs = 0, Durability = GrpcKeyValueDurability.Persistent },
                    Context());

                Assert.Equal(GrpcKeyValueResponseType.TypeSet, set.Type);
            }

            GrpcTryGetManyValuesRequest request = new();

            foreach (string key in keys)
                request.Items.Add(new GrpcTryManyValuesRequestItem { Key = key, Revision = -1, Durability = GrpcKeyValueDurability.Persistent });

            GrpcTryGetManyValuesResponse response = GrpcTryGetManyValuesResponse.Parser.ParseFrom(
                (await writer.TryGetManyValues(request, Context())).ToByteArray());

            Assert.Equal(keys.Count, response.Items.Count);
            Assert.NotEmpty(response.Routes);

            // The table is deduplicated per distinct route, so it is bounded by the partitions the
            // batch touched — never by the number of items, which is the point of having a table.
            Assert.True(response.Routes.Count <= Partitions, $"routes={response.Routes.Count}");
            Assert.True(response.Routes.Count < keys.Count);

            HashSet<string> endpoints = new(StringComparer.Ordinal);

            foreach (GrpcRouteHint hint in response.Routes)
                endpoints.Add(hint.Endpoint);

            // A three-node cluster cannot need more distinct endpoints than it has nodes.
            Assert.True(endpoints.Count <= 3, $"endpoints={endpoints.Count}");

            foreach (GrpcTryGetManyValuesResponseItem item in response.Items)
            {
                Assert.InRange(item.RouteIndex, 1, response.Routes.Count);

                GrpcRouteHint hint = response.Routes[item.RouteIndex - 1];

                Assert.StartsWith("http://localhost:800", hint.Endpoint, StringComparison.Ordinal);
                Assert.Equal(PartitionOf(item.Key), hint.PartitionId);
            }
        }
        finally
        {
            await LeaveCluster(raft1, raft2, raft3);
        }
    }

    /// <summary>
    /// With hints turned off the responses are exactly what they were before routing existed, so an
    /// operator can switch the whole feature off without changing any outcome.
    /// </summary>
    [Fact]
    public async Task WithHintsDisabled_ResponsesCarryNone()
    {
        (IRaft raft1, IRaft raft2, IRaft raft3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster("memory", Partitions, NullLogger<IRaft>.Instance, NullLogger<IKahuna>.Instance,
                configuration =>
                {
                    AdvertiseOverHttp(configuration);
                    configuration.RoutingHintsEnabled = false;
                });

        try
        {
            string key = "off/" + Guid.NewGuid().ToString("N")[..8];

            KeyValuesService service = new(kahuna1, NullLogger<IKahuna>.Instance);

            GrpcTrySetKeyValueResponse set = await OnWire(await service.TrySetKeyValue(
                new GrpcTrySetKeyValueRequest { Key = key, Value = Payload("v"), ExpiresMs = 0, Durability = GrpcKeyValueDurability.Persistent },
                Context()));

            Assert.Equal(GrpcKeyValueResponseType.TypeSet, set.Type);
            Assert.Null(set.Route);

            _ = kahuna2;
            _ = kahuna3;
        }
        finally
        {
            await LeaveCluster(raft1, raft2, raft3);
        }
    }

    /// <summary>
    /// The payoff of learned routing, measured rather than asserted structurally: once the
    /// client has seen a resource, its next operation on that resource must reach the owner without
    /// a forward.
    ///
    /// <para>
    /// Forwarding is counted from the server's own answer: a response whose route names the node the
    /// request was sent to was executed there, and any other answer means the operation crossed to
    /// another node. The cold pass is measured too, so the comparison is against this cluster's own
    /// rotation baseline rather than an assumed one.
    /// </para>
    /// </summary>
    [Fact]
    public async Task AWarmClient_ReachesTheOwnerWithoutForwarding()
    {
        (IRaft raft1, IRaft raft2, IRaft raft3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster("memory", Partitions, NullLogger<IRaft>.Instance, NullLogger<IKahuna>.Instance, AdvertiseOverHttp);

        try
        {
            string[] cluster =
            [
                "http://" + raft1.GetLocalEndpoint(),
                "http://" + raft2.GetLocalEndpoint(),
                "http://" + raft3.GetLocalEndpoint()
            ];

            Dictionary<string, KeyValuesService> services = new(StringComparer.Ordinal)
            {
                [cluster[0]] = new KeyValuesService(kahuna1, NullLogger<IKahuna>.Instance),
                [cluster[1]] = new KeyValuesService(kahuna2, NullLogger<IKahuna>.Instance),
                [cluster[2]] = new KeyValuesService(kahuna3, NullLogger<IKahuna>.Instance)
            };

            ClientRouteResolver resolver = new(
                KahunaRoutingMode.Learned,
                new RouteCache(1024, TimeSpan.FromMinutes(1)),
                new RoutingEndpointPolicy(cluster, null, allowUnlisted: false),
                null,
                () => cluster[0],
                TimeSpan.FromMinutes(1),
                TimeSpan.FromSeconds(5),
                NullLogger.Instance);

            List<string> keys = [];

            for (int i = 0; i < 40; i++)
                keys.Add($"warm{i}/" + Guid.NewGuid().ToString("N")[..8]);

            int rotation = 0;

            // Cold pass: nothing is known, so every request lands wherever rotation put it.
            (int coldDirect, int coldTotal) = await RunPass(keys, resolver, services, cluster, () => rotation++);

            // Warm pass: the same resources, now with learned routes.
            (int warmDirect, int warmTotal) = await RunPass(keys, resolver, services, cluster, () => rotation++);

            Assert.Equal(keys.Count, coldTotal);
            Assert.Equal(keys.Count, warmTotal);

            // At least 99% of the warm operations executed where they were sent.
            Assert.True(warmDirect * 100 >= warmTotal * 99, $"warmDirect={warmDirect}/{warmTotal}");

            // And that is a real improvement over this cluster's own rotation baseline, not an
            // artefact of a cluster where every node happens to own everything.
            Assert.True(warmDirect > coldDirect, $"cold={coldDirect}/{coldTotal} warm={warmDirect}/{warmTotal}");
        }
        finally
        {
            await LeaveCluster(raft1, raft2, raft3);
        }
    }

    /// <summary>
    /// A route that names the wrong node still produces a correct result — the receiving node
    /// resolves the key itself — and the answer it comes back with repairs the route, so the cost is
    /// one forward rather than a forward per request until the entry expires.
    /// </summary>
    [Fact]
    public async Task AStaleRoute_CostsOneForwardAndThenRepairsItself()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft raft1, IRaft raft2, IRaft raft3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster("memory", Partitions, NullLogger<IRaft>.Instance, NullLogger<IKahuna>.Instance, AdvertiseOverHttp);

        try
        {
            (IRaft, IKahuna)[] nodes = [(raft1, kahuna1), (raft2, kahuna2), (raft3, kahuna3)];

            string[] cluster =
            [
                "http://" + raft1.GetLocalEndpoint(),
                "http://" + raft2.GetLocalEndpoint(),
                "http://" + raft3.GetLocalEndpoint()
            ];

            string key = "stale/" + Guid.NewGuid().ToString("N")[..8];

            (IRaft ownerRaft, _) = await Owner(PartitionOf(key), nodes, ct);
            (IRaft wrongRaft, IKahuna wrongKahuna) = NonOwner(ownerRaft, nodes);

            string ownerUrl = "http://" + ownerRaft.GetLocalEndpoint();
            string wrongUrl = "http://" + wrongRaft.GetLocalEndpoint();

            ClientRouteResolver resolver = new(
                KahunaRoutingMode.Learned,
                new RouteCache(64, TimeSpan.FromMinutes(1)),
                new RoutingEndpointPolicy(cluster, null, allowUnlisted: false),
                null,
                () => cluster[0],
                TimeSpan.FromMinutes(1),
                TimeSpan.FromSeconds(5),
                NullLogger.Instance);

            // A route that names a node which does not own the key — the shape a leadership change
            // leaves behind.
            resolver.Learn(KahunaRoutingDomain.KeyValue, key, 0, wrongUrl, KahunaRouteProvenance.Executed, 0, wrongUrl);
            Assert.Equal(wrongUrl, resolver.Select(KahunaRoutingDomain.KeyValue, key));

            KeyValuesService wrongService = new(wrongKahuna, NullLogger<IKahuna>.Instance);

            GrpcTryGetKeyValueResponse read = await OnWire(await wrongService.TryGetKeyValue(
                new GrpcTryGetKeyValueRequest { Key = key, Revision = -1, Durability = GrpcKeyValueDurability.Persistent },
                Context()));

            // The result is correct despite the stale route, and the answer names the real owner.
            Assert.Equal(GrpcKeyValueResponseType.TypeDoesNotExist, read.Type);
            Assert.NotNull(read.Route);
            Assert.Equal(ownerUrl, read.Route.Endpoint);

            resolver.Learn(
                KahunaRoutingDomain.KeyValue, key, read.Route.PartitionId, read.Route.Endpoint,
                (KahunaRouteProvenance)read.Route.Provenance, read.Route.Generation, wrongUrl);

            Assert.Equal(ownerUrl, resolver.Select(KahunaRoutingDomain.KeyValue, key));
        }
        finally
        {
            await LeaveCluster(raft1, raft2, raft3);
        }
    }

    /// <summary>
    /// Runs one read per key through the endpoint the resolver chooses, learns from each answer, and
    /// reports how many of them executed on the node they were sent to.
    /// </summary>
    private static async Task<(int Direct, int Total)> RunPass(
        List<string> keys,
        ClientRouteResolver resolver,
        Dictionary<string, KeyValuesService> services,
        string[] cluster,
        Func<int> nextRotation)
    {
        int direct = 0;

        foreach (string key in keys)
        {
            string url = resolver.Select(KahunaRoutingDomain.KeyValue, key) ?? cluster[Math.Abs(nextRotation()) % cluster.Length];

            GrpcTryGetKeyValueResponse read = await services[url].TryGetKeyValue(
                new GrpcTryGetKeyValueRequest { Key = key, Revision = -1, Durability = GrpcKeyValueDurability.Persistent },
                Context());

            Assert.NotNull(read.Route);

            if (string.Equals(read.Route.Endpoint, url, StringComparison.Ordinal))
                direct++;

            resolver.Learn(
                KahunaRoutingDomain.KeyValue, key, read.Route.PartitionId, read.Route.Endpoint,
                (KahunaRouteProvenance)read.Route.Provenance, read.Route.Generation, url);
        }

        return (direct, keys.Count);
    }

    private static ByteString Payload(string value) => ByteString.CopyFromUtf8(value);

    /// <summary>
    /// Round-trips a response through protobuf so an unset field cannot be read as a populated one.
    /// </summary>
    private static Task<GrpcTrySetKeyValueResponse> OnWire(GrpcTrySetKeyValueResponse response) =>
        Task.FromResult(GrpcTrySetKeyValueResponse.Parser.ParseFrom(response.ToByteArray()));

    private static Task<GrpcTryGetKeyValueResponse> OnWire(GrpcTryGetKeyValueResponse response) =>
        Task.FromResult(GrpcTryGetKeyValueResponse.Parser.ParseFrom(response.ToByteArray()));

    /// <summary>Minimal context: the services read only the cancellation token.</summary>
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
