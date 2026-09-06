using Kahuna.Client;
using Kahuna.Client.Communication;
using Kahuna.Client.Routing;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Routing;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kahuna.Server.Tests;

/// <summary>
/// The client-side half of learned routing: what the cache keeps, what it refuses to keep, and what
/// it does when the endpoint it learned stops answering.
///
/// <para>
/// These drive the cache and resolver directly because the behaviour under test is invisible from a
/// public operation's result — every case here ends with the same correct outcome and differs only
/// in which node the next request is sent to.
/// </para>
/// </summary>
public sealed class TestClientRouteCache
{
    private const string NodeA = "https://node-a:8000";
    private const string NodeB = "https://node-b:8000";
    private const string NodeC = "https://node-c:8000";

    private static readonly string[] Cluster = [NodeA, NodeB, NodeC];

    private static ClientRouteResolver Resolver(
        KahunaRoutingMode mode = KahunaRoutingMode.Learned,
        int capacity = 64,
        TimeSpan lifetime = default,
        TimeSpan cooldown = default,
        IReadOnlyDictionary<string, string>? map = null,
        bool allowUnlisted = false,
        IKahunaRoutingTransport? metadata = null)
    {
        return new ClientRouteResolver(
            mode,
            new RouteCache(capacity, lifetime == default ? TimeSpan.FromMinutes(1) : lifetime),
            new RoutingEndpointPolicy(Cluster, map, allowUnlisted),
            metadata,
            () => NodeA,
            TimeSpan.FromMinutes(1),
            cooldown == default ? TimeSpan.FromSeconds(5) : cooldown,
            NullLogger.Instance);
    }

    [Fact]
    public void ALearnedRoute_IsReusedForTheSameResource()
    {
        ClientRouteResolver resolver = Resolver();

        Assert.Null(resolver.Select(KahunaRoutingDomain.KeyValue, "orders/1"));

        resolver.Learn(KahunaRoutingDomain.KeyValue, "orders/1", 2, NodeB, KahunaRouteProvenance.Executed, 0, NodeA);

        Assert.Equal(NodeB, resolver.Select(KahunaRoutingDomain.KeyValue, "orders/1"));
    }

    /// <summary>
    /// The three subsystems have separate name spaces, so one domain's route must never answer for
    /// another's. A shared entry would send a lock to the key's partition on every request.
    /// </summary>
    [Fact]
    public void ARouteLearnedForAKey_DoesNotAnswerForALockOfTheSameName()
    {
        ClientRouteResolver resolver = Resolver();

        resolver.Learn(KahunaRoutingDomain.KeyValue, "orders/1", 2, NodeB, KahunaRouteProvenance.Executed, 0, NodeA);

        Assert.Equal(NodeB, resolver.Select(KahunaRoutingDomain.KeyValue, "orders/1"));
        Assert.Null(resolver.Select(KahunaRoutingDomain.Lock, "orders/1"));
    }

    /// <summary>
    /// The guard that makes a delayed response harmless. Two requests go out on node A; the second
    /// one's reply lands first and moves the route to C. The first reply then arrives saying B — it
    /// describes a route that no longer exists, and must not undo the newer answer.
    /// </summary>
    [Fact]
    public void ALateResponse_CannotOverwriteANewerRoute()
    {
        ClientRouteResolver resolver = Resolver();

        // Both requests were sent to node A, which is where the cache pointed at the time.
        resolver.Learn(KahunaRoutingDomain.KeyValue, "orders/1", 2, NodeA, KahunaRouteProvenance.Executed, 0, NodeA);
        Assert.Equal(NodeA, resolver.Select(KahunaRoutingDomain.KeyValue, "orders/1"));

        // The newer reply moves the route to C.
        resolver.Learn(KahunaRoutingDomain.KeyValue, "orders/1", 2, NodeC, KahunaRouteProvenance.Executed, 0, NodeA);
        Assert.Equal(NodeC, resolver.Select(KahunaRoutingDomain.KeyValue, "orders/1"));

        // The older reply, from a request sent to A, arrives last and names B. The cached route no
        // longer names A, so this reply describes a state that is gone.
        resolver.Learn(KahunaRoutingDomain.KeyValue, "orders/1", 2, NodeB, KahunaRouteProvenance.Executed, 0, NodeA);

        Assert.Equal(NodeC, resolver.Select(KahunaRoutingDomain.KeyValue, "orders/1"));
    }

    /// <summary>
    /// The repair case the guard must not block: a response from a request sent to the endpoint the
    /// route names is exactly the one entitled to move it.
    /// </summary>
    [Fact]
    public void AResponseFromTheCachedEndpoint_MovesTheRoute()
    {
        ClientRouteResolver resolver = Resolver();

        resolver.Learn(KahunaRoutingDomain.KeyValue, "orders/1", 2, NodeB, KahunaRouteProvenance.Executed, 0, NodeA);
        Assert.Equal(NodeB, resolver.Select(KahunaRoutingDomain.KeyValue, "orders/1"));

        // Sent to B — the cached endpoint — and B says the owner is now C.
        resolver.Learn(KahunaRoutingDomain.KeyValue, "orders/1", 2, NodeC, KahunaRouteProvenance.Forwarded, 0, NodeB);

        Assert.Equal(NodeC, resolver.Select(KahunaRoutingDomain.KeyValue, "orders/1"));
    }

    [Fact]
    public async Task AnExpiredRoute_IsNotUsedAndIsReplaceable()
    {
        ClientRouteResolver resolver = Resolver(lifetime: TimeSpan.FromMilliseconds(30));

        resolver.Learn(KahunaRoutingDomain.KeyValue, "orders/1", 2, NodeB, KahunaRouteProvenance.Executed, 0, NodeA);
        Assert.Equal(NodeB, resolver.Select(KahunaRoutingDomain.KeyValue, "orders/1"));

        await Task.Delay(80, TestContext.Current.CancellationToken);

        Assert.Null(resolver.Select(KahunaRoutingDomain.KeyValue, "orders/1"));

        // An expired entry is no longer a newer answer, so a reply from anywhere may replace it.
        resolver.Learn(KahunaRoutingDomain.KeyValue, "orders/1", 2, NodeC, KahunaRouteProvenance.Executed, 0, NodeC);

        Assert.Equal(NodeC, resolver.Select(KahunaRoutingDomain.KeyValue, "orders/1"));
    }

    /// <summary>
    /// A workload over an unbounded key space must cost bounded memory. Past capacity the cache
    /// drops entries and degrades to endpoint rotation rather than growing.
    /// </summary>
    [Fact]
    public void HighCardinalityTraffic_KeepsTheCacheBounded()
    {
        const int capacity = 64;

        ClientRouteResolver resolver = Resolver(capacity: capacity);

        for (int i = 0; i < 10_000; i++)
            resolver.Learn(KahunaRoutingDomain.KeyValue, "orders/" + i, 2, NodeB, KahunaRouteProvenance.Executed, 0, NodeA);

        // Each shard evicts on its own, so the total is bounded by capacity rather than exactly it.
        Assert.InRange(resolver.CachedRouteCount, 1, capacity);
    }

    [Fact]
    public void ConcurrentLearningOfManyResources_StaysBounded()
    {
        const int capacity = 128;

        ClientRouteResolver resolver = Resolver(capacity: capacity);

        Parallel.For(0, 8, worker =>
        {
            for (int i = 0; i < 2_000; i++)
                resolver.Learn(KahunaRoutingDomain.KeyValue, $"orders/{worker}/{i}", 2, NodeB, KahunaRouteProvenance.Executed, 0, NodeA);
        });

        Assert.InRange(resolver.CachedRouteCount, 1, capacity);
    }

    /// <summary>
    /// A response must not be able to steer the client — and its credentials and TLS trust — at an
    /// address the operator never configured.
    /// </summary>
    [Fact]
    public void AHintNamingAnUnconfiguredEndpoint_IsRefused()
    {
        ClientRouteResolver resolver = Resolver();

        resolver.Learn(KahunaRoutingDomain.KeyValue, "orders/1", 2, "https://attacker.example:8000", KahunaRouteProvenance.Executed, 0, NodeA);

        Assert.Null(resolver.Select(KahunaRoutingDomain.KeyValue, "orders/1"));
    }

    [Fact]
    public void AnUnconfiguredEndpoint_IsAcceptedOnlyWhenTheCallerOptedIn()
    {
        ClientRouteResolver resolver = Resolver(allowUnlisted: true);

        resolver.Learn(KahunaRoutingDomain.KeyValue, "orders/1", 2, "https://node-d:8000", KahunaRouteProvenance.Executed, 0, NodeA);

        Assert.Equal("https://node-d:8000", resolver.Select(KahunaRoutingDomain.KeyValue, "orders/1"));
    }

    /// <summary>
    /// The deployment where the address a node advertises is not the address a client dials: the
    /// mapping resolves it, and the result is the configured URL instance so the transport's
    /// existing connection pool is reused rather than a second one opened.
    /// </summary>
    [Fact]
    public void AnAdvertisedEndpoint_IsMappedOntoTheConfiguredUrl()
    {
        Dictionary<string, string> map = new()
        {
            ["https://172.30.0.3:8084"] = NodeB
        };

        ClientRouteResolver resolver = Resolver(map: map);

        resolver.Learn(KahunaRoutingDomain.KeyValue, "orders/1", 2, "https://172.30.0.3:8084", KahunaRouteProvenance.Executed, 0, NodeA);

        string? selected = resolver.Select(KahunaRoutingDomain.KeyValue, "orders/1");

        Assert.Equal(NodeB, selected);
        Assert.Same(NodeB, selected);
    }

    [Fact]
    public void ATrailingSlashOrLetterCase_DoesNotMakeOneNodeLookLikeTwo()
    {
        ClientRouteResolver resolver = Resolver();

        resolver.Learn(KahunaRoutingDomain.KeyValue, "orders/1", 2, "https://NODE-B:8000/", KahunaRouteProvenance.Executed, 0, NodeA);

        Assert.Equal(NodeB, resolver.Select(KahunaRoutingDomain.KeyValue, "orders/1"));
    }

    /// <summary>
    /// A node that stopped answering must stop attracting traffic, and the cached route must come
    /// back on its own once the node answers again.
    /// </summary>
    [Fact]
    public void AFailedEndpoint_IsHeldOutAndComesBackWhenItAnswers()
    {
        ClientRouteResolver resolver = Resolver();

        resolver.Learn(KahunaRoutingDomain.KeyValue, "orders/1", 2, NodeB, KahunaRouteProvenance.Executed, 0, NodeA);
        Assert.Equal(NodeB, resolver.Select(KahunaRoutingDomain.KeyValue, "orders/1"));

        resolver.ReportEndpointFailure(NodeB);

        // Held out: the caller falls back to endpoint rotation instead of queueing behind a dead node.
        Assert.Null(resolver.Select(KahunaRoutingDomain.KeyValue, "orders/1"));

        // A response from that node proves it is reachable, which is what the cooldown was waiting for.
        resolver.Learn(KahunaRoutingDomain.KeyValue, "orders/2", 2, NodeB, KahunaRouteProvenance.Executed, 0, NodeA);

        Assert.Equal(NodeB, resolver.Select(KahunaRoutingDomain.KeyValue, "orders/1"));
    }

    /// <summary>
    /// A route whose endpoint failed is replaceable by a response that came back from elsewhere —
    /// otherwise a stale route to a dead node would survive until it expired.
    /// </summary>
    [Fact]
    public void ARouteToAFailedEndpoint_IsRepairedByAResponseFromAnotherNode()
    {
        ClientRouteResolver resolver = Resolver();

        resolver.Learn(KahunaRoutingDomain.KeyValue, "orders/1", 2, NodeB, KahunaRouteProvenance.Executed, 0, NodeA);
        resolver.ReportEndpointFailure(NodeB);

        // The retry went out on rotation to C, which answers that it owns the key now.
        resolver.Learn(KahunaRoutingDomain.KeyValue, "orders/1", 2, NodeC, KahunaRouteProvenance.Executed, 0, NodeC);

        Assert.Equal(NodeC, resolver.Select(KahunaRoutingDomain.KeyValue, "orders/1"));
    }

    [Fact]
    public async Task AFailureCooldown_Lapses()
    {
        ClientRouteResolver resolver = Resolver(cooldown: TimeSpan.FromMilliseconds(30));

        resolver.Learn(KahunaRoutingDomain.KeyValue, "orders/1", 2, NodeB, KahunaRouteProvenance.Executed, 0, NodeA);
        resolver.ReportEndpointFailure(NodeB);

        Assert.Null(resolver.Select(KahunaRoutingDomain.KeyValue, "orders/1"));

        await Task.Delay(80, TestContext.Current.CancellationToken);

        Assert.Equal(NodeB, resolver.Select(KahunaRoutingDomain.KeyValue, "orders/1"));
    }

    /// <summary>A hint with no resolved owner carries nothing and must not be stored.</summary>
    [Fact]
    public void AHintWithoutAResolvedOwner_IsIgnored()
    {
        ClientRouteResolver resolver = Resolver();

        resolver.Learn(KahunaRoutingDomain.KeyValue, "orders/1", 0, "", KahunaRouteProvenance.Unknown, 0, NodeA);

        Assert.Null(resolver.Select(KahunaRoutingDomain.KeyValue, "orders/1"));
    }

    /// <summary>
    /// A handle's own affinity is still subject to the endpoint policy and the cooldown: it says
    /// which node served the handle, not that any address may be dialled.
    /// </summary>
    [Fact]
    public void AHandleAffinity_PassesThePolicyAndTheCooldown()
    {
        ClientRouteResolver resolver = Resolver();

        Assert.Equal(NodeB, resolver.TryUseAffinity(NodeB));
        Assert.Null(resolver.TryUseAffinity("https://attacker.example:8000"));

        resolver.ReportEndpointFailure(NodeB);

        Assert.Null(resolver.TryUseAffinity(NodeB));
    }

    /// <summary>
    /// The default resolves against the endpoints the client was given: several endpoints mean it can
    /// act on any hint, so it learns; a single endpoint means it could act on almost none, so it does
    /// not keep a cache it cannot use.
    /// </summary>
    [Fact]
    public void TheDefault_LearnsOnlyWhenTheClientKnowsSeveralEndpoints()
    {
        Assert.Equal(KahunaRoutingMode.Auto, new KahunaOptions().Routing);

        KahunaClient single = new(NodeA, communication: new GrpcCommunication(new KahunaOptions(), null));

        Assert.Equal(KahunaRoutingMode.RoundRobin, single.EffectiveRouting);
        Assert.Null(single.Router);

        KahunaClient pooled = new(Cluster, communication: new GrpcCommunication(new KahunaOptions(), null));

        Assert.Equal(KahunaRoutingMode.Learned, pooled.EffectiveRouting);
        Assert.NotNull(pooled.Router);
    }

    /// <summary>
    /// An explicit mode is never second-guessed, in either direction: a caller that asks for rotation
    /// on a pooled client gets it, and one that asks to learn on a single-endpoint client gets that.
    /// </summary>
    [Fact]
    public void AnExplicitMode_OverridesTheEndpointCount()
    {
        KahunaClient pooledRotation = new(
            Cluster,
            communication: new GrpcCommunication(new KahunaOptions(), null),
            options: new KahunaOptions { Routing = KahunaRoutingMode.RoundRobin });

        Assert.Equal(KahunaRoutingMode.RoundRobin, pooledRotation.EffectiveRouting);
        Assert.Null(pooledRotation.Router);

        KahunaClient singleLearned = new(
            NodeA,
            communication: new GrpcCommunication(new KahunaOptions(), null),
            options: new KahunaOptions { Routing = KahunaRoutingMode.Learned });

        Assert.Equal(KahunaRoutingMode.Learned, singleLearned.EffectiveRouting);
        Assert.NotNull(singleLearned.Router);
    }

    /// <summary>
    /// What a learned route records is what the server reported: which partition owns the resource,
    /// which range generation admitted it, and whether the answering node executed the operation or
    /// forwarded it. A route that kept only an endpoint could not be diagnosed.
    /// </summary>
    [Fact]
    public void ALearnedRoute_KeepsWhatTheServerReported()
    {
        ClientRouteResolver resolver = Resolver();

        resolver.Learn(KahunaRoutingDomain.KeyValue, "ranged:r/k", 7, NodeB, KahunaRouteProvenance.Forwarded, 42, NodeA);

        RouteEntry? entry = resolver.Peek(KahunaRoutingDomain.KeyValue, "ranged:r/k");

        Assert.NotNull(entry);
        Assert.Equal(NodeB, entry!.Endpoint);
        Assert.Equal(7, entry.PartitionId);
        Assert.Equal(42, entry.Generation);
        Assert.Equal(KahunaRouteProvenance.Forwarded, entry.Provenance);
    }

    /// <summary>
    /// A transport written outside this assembly implements neither routing hook, so a client using
    /// one keeps working and simply learns nothing. Driven through real operations rather than a
    /// type check: the point is that the operations still succeed.
    /// </summary>
    [Fact]
    public async Task ATransportWithoutRoutingSupport_LeavesTheClientWorking()
    {
        Assert.False(typeof(InProcessKahunaCommunication).IsAssignableTo(typeof(IKahunaRouteSinkReceiver)));

        await using EmbeddedKahunaNode node = new(new()
        {
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1
        }, NullLoggerFactory.Instance);

        await node.StartAsync(TestContext.Current.CancellationToken);

        InProcessKahunaCommunication transport = new(node.Kahuna);

        KahunaClient client = new(
            NodeA,
            communication: transport,
            options: new KahunaOptions { Routing = KahunaRoutingMode.Learned });

        string key = "foreign/" + Guid.NewGuid().ToString("N")[..8];

        Assert.True((await client.SetKeyValue(key, "v", 0, KeyValueFlags.Set, KeyValueDurability.Persistent, TestContext.Current.CancellationToken)).Success);
        Assert.True((await client.GetKeyValue(key, KeyValueDurability.Persistent, cancellationToken: TestContext.Current.CancellationToken)).Success);

        // The routing cache exists but stayed empty: nothing reported a hint to it.
        Assert.NotNull(client.Router);
        Assert.Equal(0, client.Router!.CachedRouteCount);
    }
}
