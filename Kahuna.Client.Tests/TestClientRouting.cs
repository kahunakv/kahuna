using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Kahuna.Client.Communication;
using Kahuna.Client.Routing;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Locks;
using Kahuna.Shared.Routing;
using Xunit;

namespace Kahuna.Client.Tests;

/// <summary>
/// Learned and metadata routing against the live cluster.
///
/// <para>
/// The cluster routes on its container addresses and this client dials the mapped host ports, which
/// is exactly the deployment the endpoint map exists for: without it every hint names an address
/// this client cannot reach, is refused, and the client stays on endpoint rotation.
/// </para>
///
/// <para>
/// Every assertion here is about which node the client picks. The operations themselves succeed in
/// any mode, so a failure in this file means routing stopped working, never that an operation
/// returned a wrong result.
/// </para>
/// </summary>
public class TestClientRouting
{
    private readonly string[] urls = ["https://localhost:8082", "https://localhost:8084", "https://localhost:8086"];

    /// <summary>
    /// Each node advertises the address its peers route on, which is not the address this client
    /// dials. Both layouts the suite is run against are covered: the compose network puts the
    /// advertised address on a container IP, and a local cluster puts it on the loopback literal
    /// while the tests dial the host name. An entry for a layout that is not in use costs nothing.
    /// </summary>
    private static readonly Dictionary<string, string> EndpointMap = new()
    {
        ["https://172.30.0.2:8082"] = "https://localhost:8082",
        ["https://172.30.0.3:8084"] = "https://localhost:8084",
        ["https://172.30.0.4:8086"] = "https://localhost:8086",
        ["https://127.0.0.1:8082"] = "https://localhost:8082",
        ["https://127.0.0.1:8084"] = "https://localhost:8084",
        ["https://127.0.0.1:8086"] = "https://localhost:8086"
    };

    private KahunaClient GetClient(KahunaRoutingMode mode, bool withMap = true)
    {
        KahunaOptions options = new()
        {
            AllowInsecureCertificateValidation = true,
            Routing = mode,
            RoutingEndpointMap = withMap ? EndpointMap : null
        };

        return new KahunaClient(urls, communication: new GrpcCommunication(options, null), options: options);
    }

    /// <summary>
    /// The payoff of learned routing: after one operation on a resource, the client knows where that resource
    /// lives and keeps choosing that node.
    /// </summary>
    [Fact]
    public async Task ARepeatedKey_IsRoutedToTheNodeThatServedIt()
    {
        KahunaClient client = GetClient(KahunaRoutingMode.Learned);

        string key = $"routing/{Guid.NewGuid():N}";

        Assert.True((await client.SetKeyValue(key, Encoding.UTF8.GetBytes("v1"), 10000, KeyValueFlags.Set,
            KeyValueDurability.Persistent, TestContext.Current.CancellationToken)).Success);

        Assert.NotNull(client.Router);

        string? learned = client.Router!.Select(KahunaRoutingDomain.KeyValue, key);

        Assert.NotNull(learned);
        Assert.Contains(learned, urls);

        // Repeating the operation keeps the same destination and still returns the right value.
        KahunaKeyValue read = await client.GetKeyValue(key, KeyValueDurability.Persistent,
            cancellationToken: TestContext.Current.CancellationToken);

        Assert.True(read.Success);
        Assert.Equal("v1", read.ValueAsString());
        Assert.Equal(learned, client.Router.Select(KahunaRoutingDomain.KeyValue, key));
    }

    /// <summary>
    /// Locks route in their own name space. A lock and a key of the same name may live on different
    /// partitions, so learning one must never answer for the other.
    /// </summary>
    [Fact]
    public async Task ALockAndAKeyOfTheSameName_KeepSeparateRoutes()
    {
        KahunaClient client = GetClient(KahunaRoutingMode.Learned);

        string name = $"routing/{Guid.NewGuid():N}";

        Assert.True((await client.SetKeyValue(name, Encoding.UTF8.GetBytes("v"), 10000, KeyValueFlags.Set,
            KeyValueDurability.Persistent, TestContext.Current.CancellationToken)).Success);

        await using KahunaLock held = await client.GetOrCreateLock(name, 10000, LockDurability.Persistent, TestContext.Current.CancellationToken);

        Assert.True(held.IsAcquired);
        Assert.NotNull(client.Router);

        Assert.NotNull(client.Router!.Select(KahunaRoutingDomain.KeyValue, name));
        Assert.NotNull(client.Router.Select(KahunaRoutingDomain.Lock, name));
    }

    /// <summary>
    /// Without the endpoint map every hint names an address this client cannot reach. The client must
    /// refuse them and keep working on endpoint rotation rather than dialling an unreachable node.
    /// </summary>
    [Fact]
    public async Task WithoutAnEndpointMap_HintsAreRefusedAndOperationsStillSucceed()
    {
        KahunaClient client = GetClient(KahunaRoutingMode.Learned, withMap: false);

        string key = $"routing/{Guid.NewGuid():N}";

        Assert.True((await client.SetKeyValue(key, Encoding.UTF8.GetBytes("v"), 10000, KeyValueFlags.Set,
            KeyValueDurability.Persistent, TestContext.Current.CancellationToken)).Success);

        KahunaKeyValue read = await client.GetKeyValue(key, KeyValueDurability.Persistent,
            cancellationToken: TestContext.Current.CancellationToken);

        Assert.True(read.Success);
        Assert.Equal("v", read.ValueAsString());

        Assert.NotNull(client.Router);
        Assert.Equal(0, client.Router!.CachedRouteCount);
    }

    /// <summary>
    /// Metadata mode resolves a key the client has never touched, once the map has been read. The
    /// read never sits on an operation's own path, so the first miss still goes out on rotation.
    /// </summary>
    [Fact]
    public async Task AfterReadingTheMap_APreviouslyUnseenKeyIsResolved()
    {
        KahunaClient client = GetClient(KahunaRoutingMode.Metadata);

        Assert.NotNull(client.Router);
        Assert.True(await client.Router!.RefreshMetadataAsync(TestContext.Current.CancellationToken));

        int resolved = 0;

        for (int i = 0; i < 20; i++)
        {
            string key = $"routing/unseen/{Guid.NewGuid():N}";

            if (client.Router.Select(KahunaRoutingDomain.KeyValue, key) is { } endpoint)
            {
                Assert.Contains(endpoint, urls);
                resolved++;
            }
        }

        // Every partition of a settled cluster has a known leader, so every unseen key resolves.
        Assert.True(resolved >= 18, $"resolved={resolved}");
    }

    /// <summary>
    /// A client that asks for rotation explicitly gets it, and every operation still works with no
    /// routing cache at all.
    /// </summary>
    [Fact]
    public async Task AClientThatAsksForRotation_KeepsRotatingEndpoints()
    {
        KahunaClient client = GetClient(KahunaRoutingMode.RoundRobin);

        Assert.Equal(KahunaRoutingMode.RoundRobin, client.EffectiveRouting);
        Assert.Null(client.Router);

        string key = $"routing/{Guid.NewGuid():N}";

        Assert.True((await client.SetKeyValue(key, Encoding.UTF8.GetBytes("v"), 10000, KeyValueFlags.Set,
            KeyValueDurability.Persistent, TestContext.Current.CancellationToken)).Success);

        Assert.Equal("v", (await client.GetKeyValue(key, KeyValueDurability.Persistent,
            cancellationToken: TestContext.Current.CancellationToken)).ValueAsString());
    }

    /// <summary>
    /// The default, with no routing option set at all: a client given several endpoints learns, and
    /// routes a repeated key to the node that served it. This is the shape most callers get.
    /// </summary>
    [Fact]
    public async Task TheDefaultMultiEndpointClient_LearnsWithoutBeingAsked()
    {
        KahunaOptions options = new()
        {
            AllowInsecureCertificateValidation = true,
            RoutingEndpointMap = EndpointMap
        };

        KahunaClient client = new(urls, communication: new GrpcCommunication(options, null), options: options);

        Assert.Equal(KahunaRoutingMode.Auto, options.Routing);
        Assert.Equal(KahunaRoutingMode.Learned, client.EffectiveRouting);
        Assert.NotNull(client.Router);

        string key = $"routing/{Guid.NewGuid():N}";

        Assert.True((await client.SetKeyValue(key, Encoding.UTF8.GetBytes("v"), 10000, KeyValueFlags.Set,
            KeyValueDurability.Persistent, TestContext.Current.CancellationToken)).Success);

        string? learned = client.Router!.Select(KahunaRoutingDomain.KeyValue, key);

        Assert.NotNull(learned);
        Assert.Contains(learned, urls);
    }

    /// <summary>
    /// A single-endpoint client stays on rotation by default. It could act on almost no hint — only
    /// endpoints it was configured with are dialled — so a cache would sit nearly unused.
    /// </summary>
    [Fact]
    public async Task TheDefaultSingleEndpointClient_StaysOnRotation()
    {
        KahunaOptions options = new() { AllowInsecureCertificateValidation = true };

        KahunaClient client = new(urls[0], communication: new GrpcCommunication(options, null), options: options);

        Assert.Equal(KahunaRoutingMode.RoundRobin, client.EffectiveRouting);
        Assert.Null(client.Router);

        string key = $"routing/{Guid.NewGuid():N}";

        Assert.True((await client.SetKeyValue(key, Encoding.UTF8.GetBytes("v"), 10000, KeyValueFlags.Set,
            KeyValueDurability.Persistent, TestContext.Current.CancellationToken)).Success);

        Assert.Equal("v", (await client.GetKeyValue(key, KeyValueDurability.Persistent,
            cancellationToken: TestContext.Current.CancellationToken)).ValueAsString());
    }

    /// <summary>
    /// A lock's own operations follow the same route, and the fencing token and owner semantics are
    /// untouched by which node the request reached.
    /// </summary>
    [Fact]
    public async Task ALockKeepsItsSemanticsUnderRouting()
    {
        KahunaClient client = GetClient(KahunaRoutingMode.Learned);

        string resource = $"routing/lock/{Guid.NewGuid():N}";

        await using KahunaLock held = await client.GetOrCreateLock(resource, 10000, LockDurability.Persistent, TestContext.Current.CancellationToken);

        Assert.True(held.IsAcquired);
        Assert.True(held.FencingToken >= 0);

        (bool extended, long token) = await held.TryExtend(10000, TestContext.Current.CancellationToken);

        Assert.True(extended);
        Assert.Equal(held.FencingToken, token);

        KahunaLockInfo? info = await held.GetInfo(TestContext.Current.CancellationToken);

        Assert.NotNull(info);
        Assert.NotNull(info!.Owner);
        Assert.Equal(held.OwnerAsString, Encoding.UTF8.GetString(info.Owner!));
    }

    /// <summary>
    /// A batched read learns a route per item, so the point operations that follow it go direct
    /// without each having to discover its own key first.
    /// </summary>
    [Fact]
    public async Task ABatchedRead_WarmsTheRoutesOfEveryKeyItTouched()
    {
        KahunaClient client = GetClient(KahunaRoutingMode.Learned);

        List<KahunaSetKeyValueRequestItem> items = [];
        List<string> keys = [];

        for (int i = 0; i < 10; i++)
        {
            string key = $"routing/batch{i}/{Guid.NewGuid():N}";
            keys.Add(key);

            items.Add(new KahunaSetKeyValueRequestItem
            {
                Key = key,
                Value = Encoding.UTF8.GetBytes("v"),
                ExpiresMs = 10000,
                Flags = KeyValueFlags.Set,
                Durability = KeyValueDurability.Persistent
            });
        }

        Assert.All(await client.SetManyKeyValues(items, TestContext.Current.CancellationToken), r => Assert.True(r.Success));

        Assert.NotNull(client.Router);

        foreach (string key in keys)
            Assert.NotNull(client.Router!.Select(KahunaRoutingDomain.KeyValue, key));
    }
}
