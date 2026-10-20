using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using Grpc.Core;
using Grpc.Net.Client;
using Kahuna.Client;
using Kahuna.Client.Communication;
using Kahuna.Communication.External.Grpc;
using Kahuna.Communication.External.Rest;
using Kahuna.Server.Communication;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues.Watch;
using Kahuna.Shared.Communication.Rest;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Time;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kahuna.Server.Tests;

/// <summary>
/// Watches through the remote surfaces: the bidirectional gRPC rpc and the REST server-sent-events route, both
/// hosted on a real Kestrel listener over a real node, read by the real client transports. Also the client's
/// fan-out across replicas when no node hosts every partition of a scope.
///
/// <para>What these tests pin: an event crosses each wire with its kind, revision and value intact, and a delete
/// or keys-only event stays apart from a zero-byte value; an ephemeral watch is refused on every surface before
/// anything registers; one gRPC call carries several watches and a cancel closes one of them; a client that goes
/// away, cleanly or by aborting the connection, leaves no registration behind; and under replica placement the
/// client opens the watch on the nodes that host each partition and ends it as one.</para>
/// </summary>
public sealed class TestKeyValueWatchRemote : BaseCluster
{
    private readonly ILoggerFactory loggerFactory;

    public TestKeyValueWatchRemote(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper, quietKommander: true);
    }

    [Theory]
    [InlineData("grpc")]
    [InlineData("rest")]
    public async Task ClientTransport_DeliversEveryEventKind_WithTheValueShapeIntact(string transport)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using HostedNode host = await HostedNode.StartAsync(loggerFactory, ct);
        KahunaClient client = host.Client(transport);

        string prefix = $"remote-{transport}/{Guid.NewGuid():N}/";

        await using KahunaWatch watch = await client.WatchKeyValues(new(KeyValueWatchScope.ForPrefix(prefix)), ct);
        await using KahunaWatch keysOnly = await client.WatchKeyValues(new(KeyValueWatchScope.ForPrefix(prefix), KeysOnly: true), ct);

        Assert.Equal(2, host.OpenWatches);

        await host.Set(prefix + "a", Encoding.UTF8.GetBytes("v1"), ct);
        await host.Set(prefix + "empty", [], ct);
        await host.Set(prefix + "null", null, ct);
        await host.Extend(prefix + "a", ct);
        await host.Delete(prefix + "a", ct);

        List<KeyValueWatchEvent> events = await ReadEvents(watch, 5, ct);

        Assert.Equal(prefix + "a", events[0].Key);
        Assert.Equal(KeyValueWatchEventKind.Set, events[0].Kind);
        Assert.Equal("v1", Encoding.UTF8.GetString(events[0].Value!));
        Assert.True(events[0].CommitTimestamp != HLCTimestamp.Zero);
        Assert.Equal(HLCTimestamp.Zero, events[0].TransactionId);

        // A zero-byte value and no value must arrive as two different things.
        Assert.NotNull(events[1].Value);
        Assert.Empty(events[1].Value!);
        Assert.Null(events[2].Value);

        Assert.Equal(KeyValueWatchEventKind.Extended, events[3].Kind);
        Assert.Null(events[3].Value);
        Assert.Equal(KeyValueWatchEventKind.Deleted, events[4].Kind);
        Assert.Null(events[4].Value);
        Assert.True(events[4].Revision > events[0].Revision);

        List<KeyValueWatchEvent> keys = await ReadEvents(keysOnly, 5, ct);
        Assert.Equal(events.Select(e => (e.Key, e.Kind, e.Revision)), keys.Select(e => (e.Key, e.Kind, e.Revision)));
        Assert.All(keys, e => Assert.Null(e.Value));

        await watch.DisposeAsync();
        await keysOnly.DisposeAsync();

        // Closing the client watch closes the call or the request, and the node unregisters the watch.
        await WaitUntilAsync(() => host.OpenWatches == 0);
    }

    [Theory]
    [InlineData("grpc")]
    [InlineData("rest")]
    public async Task HistoryHolds_TravelThroughEveryClientTransport(string transport)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using HostedNode host = await HostedNode.StartAsync(loggerFactory, ct);
        KahunaClient client = host.Client(transport);

        string holder = $"cdc-{transport}-{Guid.NewGuid():N}";
        HLCTimestamp from = new(1, 1_000, 3);

        HistoryHoldAnswer acquired = await RetryOnMustRetryAsync(() => client.AcquireHistoryHolds(holder, [1, 2], from, 60_000, ct), a => a.Type);
        Assert.Equal(KeyValueResponseType.Set, acquired.Type);
        Assert.Equal([1, 2], acquired.Holds.Select(h => h.PartitionId).Order());
        Assert.All(acquired.Holds, h =>
        {
            Assert.Equal(holder, h.HolderId);
            Assert.Equal(from, h.Timestamp);
            Assert.True(h.LeaseExpiry != HLCTimestamp.Zero);
        });

        HistoryHoldAnswer listed = await client.ListHistoryHolds(holder, ct);
        Assert.Equal(KeyValueResponseType.Get, listed.Type);
        Assert.Equal(acquired.Holds.Select(h => h.HoldId).Order(), listed.Holds.Select(h => h.HoldId).Order());

        HLCTimestamp forward = new(1, 2_000, 0);
        HistoryHoldAnswer renewed = await client.RenewHistoryHolds([.. acquired.Holds.Select(h => h.HoldId)], 60_000, forward, ct);
        Assert.Equal(KeyValueResponseType.Set, renewed.Type);
        Assert.All(renewed.Holds, h => Assert.Equal(forward, h.Timestamp));

        HistoryHoldAnswer back = await client.RenewHistoryHolds([acquired.Holds[0].HoldId], 60_000, from, ct);
        Assert.Equal(KeyValueResponseType.InvalidInput, back.Type);

        HistoryHoldAnswer released = await client.ReleaseHistoryHolds([.. acquired.Holds.Select(h => h.HoldId)], ct);
        Assert.Equal(KeyValueResponseType.Deleted, released.Type);
        Assert.Empty((await client.ListHistoryHolds(holder, ct)).Holds);
    }

    [Fact]
    public async Task EphemeralWatch_IsRefusedOnEverySurface_BeforeAnythingRegisters()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using HostedNode host = await HostedNode.StartAsync(loggerFactory, ct);
        KeyValueWatchRequest ephemeral = new(KeyValueWatchScope.ForPrefix("ephemeral/"), KeyValueDurability.Ephemeral);

        // The client refuses it before it dials a node.
        KahunaException refused = await Assert.ThrowsAsync<KahunaException>(() => host.Client("grpc").WatchKeyValues(ephemeral, ct));
        Assert.Equal(KeyValueResponseType.WatchUnsupportedOnEphemeral, refused.KeyValueErrorCode);

        // The gRPC and REST surfaces refuse it too, for a caller that skips the client's check.
        KahunaWatchOpening grpc = await new GrpcCommunication(null, null).OpenWatch(host.GrpcUrl, ephemeral, ct);
        Assert.Equal(KeyValueResponseType.WatchUnsupportedOnEphemeral, grpc.Type);
        Assert.Null(grpc.Stream);

        KahunaWatchOpening rest = await new RestCommunication(null).OpenWatch(host.RestUrl, ephemeral, ct);
        Assert.Equal(KeyValueResponseType.WatchUnsupportedOnEphemeral, rest.Type);
        Assert.Null(rest.Stream);

        // The REST answer is a stream with one refused frame, not an error status.
        using HttpClient http = new();
        using HttpResponseMessage response = await http.PostAsync(
            host.RestUrl + "/v1/kv/watch",
            new StringContent(JsonSerializer.Serialize(new KahunaWatchRequest
            {
                ScopeKind = KeyValueWatchScopeKind.Prefix,
                ScopeValue = "ephemeral/",
                Durability = KeyValueDurability.Ephemeral
            }, KahunaJsonContext.Default.KahunaWatchRequest), Encoding.UTF8, "application/json"),
            ct);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("text/event-stream", response.Content.Headers.ContentType?.MediaType);

        string body = await response.Content.ReadAsStringAsync(ct);
        KahunaWatchFrameResponse frame = JsonSerializer.Deserialize(body.Trim()["data: ".Length..], KahunaJsonContext.Default.KahunaWatchFrameResponse)!;
        Assert.Equal(KeyValueWatchFrameType.Refused, frame.Frame);
        Assert.Equal(KeyValueResponseType.WatchUnsupportedOnEphemeral, frame.Type);

        Assert.Equal(0, host.OpenWatches);
    }

    [Fact]
    public async Task OneGrpcCall_CarriesSeveralWatches_AndACancelClosesOnlyOne()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using HostedNode host = await HostedNode.StartAsync(loggerFactory, ct);
        string run = Guid.NewGuid().ToString("N");

        using AsyncDuplexStreamingCall<GrpcWatchRequest, GrpcWatchResponse> call = host.GrpcClient().Watch(cancellationToken: ct);
        WatchFrameReader reader = new(call.ResponseStream);

        await call.RequestStream.WriteAsync(Open(1, $"mux-a/{run}/"), ct);
        await call.RequestStream.WriteAsync(Open(2, $"mux-b/{run}/"), ct);

        // A malformed open is refused for its own id and leaves the call and the other watches alone.
        await call.RequestStream.WriteAsync(new GrpcWatchRequest { Type = GrpcWatchRequestType.WatchRequestOpen, WatchId = 3, ScopeValue = "" }, ct);
        await call.RequestStream.WriteAsync(Open(1, "duplicate/"), ct);

        List<GrpcWatchFrame> answers = await reader.Read(4, ct);
        Assert.Equal(GrpcWatchFrameType.WatchFrameOpened, Assert.Single(answers, f => f.WatchId == 2).Type);
        Assert.Equal(GrpcWatchFrameType.WatchFrameOpened, answers.First(f => f.WatchId == 1).Type);
        Assert.Equal(GrpcKeyValueResponseType.TypeInvalidInput, Assert.Single(answers, f => f.WatchId == 3).Refusal);
        Assert.Equal(GrpcKeyValueResponseType.TypeInvalidInput, answers.Last(f => f.WatchId == 1).Refusal);

        Assert.Equal(2, host.OpenWatches);

        await host.Set($"mux-a/{run}/k", Encoding.UTF8.GetBytes("a"), ct);
        await host.Set($"mux-b/{run}/k", Encoding.UTF8.GetBytes("b"), ct);

        List<GrpcWatchFrame> events = await reader.Read(2, ct);
        Assert.Equal($"mux-a/{run}/k", Assert.Single(events, f => f.WatchId == 1).Key);
        Assert.Equal($"mux-b/{run}/k", Assert.Single(events, f => f.WatchId == 2).Key);

        await call.RequestStream.WriteAsync(new GrpcWatchRequest { Type = GrpcWatchRequestType.WatchRequestCancel, WatchId = 1 }, ct);
        await WaitUntilAsync(() => host.OpenWatches == 1);

        await host.Set($"mux-a/{run}/k", Encoding.UTF8.GetBytes("a2"), ct);
        await host.Set($"mux-b/{run}/k", Encoding.UTF8.GetBytes("b2"), ct);

        GrpcWatchFrame after = Assert.Single(await reader.Read(1, ct));
        Assert.Equal(2, after.WatchId);
        Assert.Equal("b2", after.Value.ToStringUtf8());

        // Ending the request stream ends the call and closes the last watch.
        await call.RequestStream.CompleteAsync();
        await WaitUntilAsync(() => host.OpenWatches == 0);
    }

    [Theory]
    [InlineData("grpc")]
    [InlineData("rest")]
    public async Task AbortedConnection_UnregistersEveryWatchOfTheStream(string transport)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using HostedNode host = await HostedNode.StartAsync(loggerFactory, ct);
        using CancellationTokenSource abort = CancellationTokenSource.CreateLinkedTokenSource(ct);

        if (transport == "grpc")
        {
            // No request-stream close and no cancel frame: the call is torn down from the client side.
            AsyncDuplexStreamingCall<GrpcWatchRequest, GrpcWatchResponse> call = host.GrpcClient().Watch(cancellationToken: abort.Token);
            WatchFrameReader reader = new(call.ResponseStream);

            await call.RequestStream.WriteAsync(Open(1, "abort-a/"), ct);
            await call.RequestStream.WriteAsync(Open(2, "abort-b/"), ct);
            Assert.All(await reader.Read(2, ct), f => Assert.Equal(GrpcWatchFrameType.WatchFrameOpened, f.Type));
            Assert.Equal(2, host.OpenWatches);

            await abort.CancelAsync();
            call.Dispose();
        }
        else
        {
            KahunaWatchOpening opening = await new RestCommunication(null).OpenWatch(host.RestUrl, new(KeyValueWatchScope.ForPrefix("abort-rest/")), ct);
            Assert.NotNull(opening.Stream);
            Assert.Equal(1, host.OpenWatches);

            await opening.Stream!.DisposeAsync();
        }

        await WaitUntilAsync(() => host.OpenWatches == 0);
    }

    [Fact]
    public async Task ReplicaPlacement_TheClientOpensTheWatchOnTheNodesThatHostEachPartition_AndEndsItAsOne()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        const int partitions = 4;

        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleCluster(
            4, "memory", partitions, loggerFactory.CreateLogger<IRaft>(), loggerFactory.CreateLogger<IKahuna>(), replicationFactor: 1);

        try
        {
            KahunaManager[] nodes = [.. kahunas.Cast<KahunaManager>()];
            Dictionary<string, IKahuna> byUrl = new(StringComparer.Ordinal);
            for (int i = 0; i < nodes.Length; i++)
                byUrl["https://" + rafts[i].GetLocalEndpoint()] = nodes[i];

            string run = Guid.NewGuid().ToString("N");
            KeyValueWatchScope scope = KeyValueWatchScope.ForPrefix($"placed/{run}/");

            // A node that does not host every partition refuses and names where each one is hosted.
            KeyValueWatchOpening refusal = await nodes[0].WatchKeyValues(new(scope), ct);
            Assert.Equal(KeyValueResponseType.DoesNotExist, refusal.Type);
            Assert.Null(refusal.Watch);
            Assert.Equal(partitions, refusal.Routes.Count);

            foreach (KeyValueWatchRoute route in refusal.Routes)
            {
                Assert.Equal(rafts[0].HostsPartition(route.PartitionId), route.HostedHere);
                if (!route.HostedHere)
                {
                    Assert.NotEmpty(route.Endpoints);
                    Assert.DoesNotContain("https://" + rafts[0].GetLocalEndpoint(), route.Endpoints);
                }
            }

            // A client that may dial only the first node cannot reach the other replicas.
            KahunaClient narrow = new([byUrl.Keys.First()], communication: new InProcessKahunaCommunication(byUrl));
            KahunaException unreachable = await Assert.ThrowsAsync<KahunaException>(() => narrow.WatchKeyValues(new(scope), ct));
            Assert.Equal(KeyValueResponseType.DoesNotExist, unreachable.KeyValueErrorCode);

            KahunaClient client = new([.. byUrl.Keys], communication: new InProcessKahunaCommunication(byUrl));
            KahunaWatch watch = await client.WatchKeyValues(new(scope), ct);

            Assert.Equal(Enumerable.Range(1, partitions), watch.Partitions);
            Assert.True(watch.Endpoints.Count > 1);
            Assert.Equal(partitions, nodes.Sum(n => n.KeyValues.WatchBroker.RegistrationCount));

            // One key in each of many key spaces, so the writes land on every partition.
            const int keys = 16;
            for (int i = 0; i < keys; i++)
                await Set(nodes[i % nodes.Length], $"placed/{run}/{i}/k", ct);

            List<KeyValueWatchEvent> events = await ReadEvents(watch, keys, ct);
            Assert.Equal(keys, events.Select(e => e.Key).Distinct().Count());
            Assert.True(events.Select(e => e.PartitionId).Distinct().Count() > 1);

            // A loss of continuity on one node ends the whole watch and closes it on every other node.
            KahunaManager owner = nodes.First(n => n.KeyValues.WatchBroker.RegistrationCount > 0);
            int lostPartition = watch.Partitions.First(p => owner.KeyValues.WatchBroker.HasSubscribers(p));
            owner.KeyValues.WatchBroker.EndPartition(lostPartition, KeyValueWatchLagReason.PartitionStateReplaced);

            KeyValueWatchFrame last = (await ReadToEnd(watch, ct))[^1];
            Assert.Equal(KeyValueWatchFrameType.Lagged, last.Type);
            Assert.Equal(KeyValueWatchLagReason.PartitionStateReplaced, last.LagReason);
            Assert.Equal(lostPartition, last.PartitionId);

            await WaitUntilAsync(() => nodes.All(n => n.KeyValues.WatchBroker.RegistrationCount == 0));
            await watch.DisposeAsync();
        }
        finally
        {
            await Task.WhenAll(rafts.Select(LeaveCluster));
        }
    }

    private static GrpcWatchRequest Open(long watchId, string prefix) => new()
    {
        Type = GrpcWatchRequestType.WatchRequestOpen,
        WatchId = watchId,
        ScopeKind = GrpcWatchScopeKind.WatchScopePrefix,
        ScopeValue = prefix,
        Durability = GrpcKeyValueDurability.Persistent
    };

    private static async Task Set(IKahuna node, string key, CancellationToken ct)
    {
        (KeyValueResponseType type, _, _) = await RetryOnMustRetryAsync(
            () => node.LocateAndTrySetKeyValue(HLCTimestamp.Zero, key, [1], null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct),
            r => r.Item1);

        Assert.Equal(KeyValueResponseType.Set, type);
    }

    private static async Task<List<KeyValueWatchEvent>> ReadEvents(KahunaWatch watch, int count, CancellationToken ct)
    {
        using CancellationTokenSource timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeout.CancelAfter(TimeSpan.FromSeconds(30));

        List<KeyValueWatchEvent> events = [];

        await foreach (KeyValueWatchFrame frame in watch.ReadAllAsync(timeout.Token))
        {
            Assert.False(frame.IsTerminal, $"the watch ended with {frame.Type} ({frame.LagReason}, {frame.Refusal})");

            if (frame.Event is not null)
                events.Add(frame.Event);

            if (events.Count == count)
                break;
        }

        return events;
    }

    private static async Task<List<KeyValueWatchFrame>> ReadToEnd(KahunaWatch watch, CancellationToken ct)
    {
        using CancellationTokenSource timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeout.CancelAfter(TimeSpan.FromSeconds(30));

        List<KeyValueWatchFrame> frames = [];
        await foreach (KeyValueWatchFrame frame in watch.ReadAllAsync(timeout.Token))
            frames.Add(frame);

        return frames;
    }

    /// <summary>Reads frames off a raw gRPC watch call, across response messages.</summary>
    private sealed class WatchFrameReader(IAsyncStreamReader<GrpcWatchResponse> responses)
    {
        private readonly Queue<GrpcWatchFrame> pending = new();

        public async Task<List<GrpcWatchFrame>> Read(int count, CancellationToken ct)
        {
            using CancellationTokenSource timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
            timeout.CancelAfter(TimeSpan.FromSeconds(30));

            List<GrpcWatchFrame> frames = [];

            while (frames.Count < count)
            {
                if (pending.TryDequeue(out GrpcWatchFrame? frame))
                {
                    frames.Add(frame);
                    continue;
                }

                Assert.True(await responses.MoveNext(timeout.Token), "the watch call ended");
                foreach (GrpcWatchFrame next in responses.Current.Frames)
                    pending.Enqueue(next);
            }

            return frames;
        }
    }

    /// <summary>
    /// One embedded node behind a real Kestrel listener: the gRPC services on an HTTP/2 port and the key-value
    /// REST routes on an HTTP/1.1 port, wired the way the server wires them.
    /// </summary>
    private sealed class HostedNode : IAsyncDisposable
    {
        private readonly EmbeddedKahunaNode node;

        private readonly WebApplication app;

        private HostedNode(EmbeddedKahunaNode node, WebApplication app, int grpcPort, int restPort)
        {
            this.node = node;
            this.app = app;
            GrpcUrl = $"http://localhost:{grpcPort}";
            RestUrl = $"http://localhost:{restPort}";
        }

        public string GrpcUrl { get; }

        public string RestUrl { get; }

        public KeyValueWatchBroker Broker => ((KahunaManager)node.Kahuna).KeyValues.WatchBroker;

        /// <summary>Open watches on the node, each counted once whatever partitions it covers.</summary>
        public int OpenWatches => Broker.OpenWatches().Count;

        public static async Task<HostedNode> StartAsync(ILoggerFactory loggerFactory, CancellationToken ct)
        {
            EmbeddedKahunaNode node = new(new() { Storage = "memory", WalStorage = "memory", InitialPartitions = 2 }, loggerFactory);
            await node.StartAsync(ct);

            int grpcPort = FreePort();
            int restPort = FreePort();

            WebApplicationBuilder builder = WebApplication.CreateBuilder();
            builder.Logging.ClearProviders();
            builder.Services.AddSingleton(node.Kahuna);
            builder.Services.AddSingleton(node.Raft);
            builder.Services.AddSingleton(new KahunaConfiguration());
            builder.Services.AddSingleton(NodeTransportGate.Disabled);
            builder.Services.AddSingleton<ILogger<IKahuna>>(NullLogger<IKahuna>.Instance);
            builder.Services.AddGrpc(static options => options.EnableDetailedErrors = true);
            builder.Services.ConfigureHttpJsonOptions(static options => options.SerializerOptions.TypeInfoResolverChain.Insert(0, KahunaJsonContext.Default));
            builder.WebHost.ConfigureKestrel(kestrel =>
            {
                kestrel.Listen(IPAddress.Loopback, grpcPort, static listen => listen.Protocols = HttpProtocols.Http2);
                kestrel.Listen(IPAddress.Loopback, restPort, static listen => listen.Protocols = HttpProtocols.Http1);
            });

            WebApplication app = builder.Build();
            app.UseRetryableExceptionMapping();
            app.MapGrpcKahunaRoutes();
            KeyValuesHandlers.MapKeyValueRoutes(app);
            await app.StartAsync(ct);

            return new(node, app, grpcPort, restPort);
        }

        public KahunaClient Client(string transport) => transport == "grpc"
            ? new(GrpcUrl, communication: new GrpcCommunication(null, null))
            : new(RestUrl, communication: new RestCommunication(null));

        public KeyValuer.KeyValuerClient GrpcClient() => new(GrpcChannel.ForAddress(GrpcUrl));

        public async Task Set(string key, byte[]? value, CancellationToken ct)
        {
            (KeyValueResponseType type, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, key, value, null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Set, type);
        }

        public async Task Extend(string key, CancellationToken ct)
        {
            (KeyValueResponseType type, _, _) = await node.Kahuna.LocateAndTryExtendKeyValue(HLCTimestamp.Zero, key, 60_000, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Extended, type);
        }

        public async Task Delete(string key, CancellationToken ct)
        {
            (KeyValueResponseType type, _, _) = await node.Kahuna.LocateAndTryDeleteKeyValue(HLCTimestamp.Zero, key, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Deleted, type);
        }

        public async ValueTask DisposeAsync()
        {
            using CancellationTokenSource stop = new(TimeSpan.FromSeconds(2));
            try { await app.StopAsync(stop.Token); } catch { /* best-effort */ }
            await app.DisposeAsync();
            await node.DisposeAsync();
        }

        private static int FreePort()
        {
            using TcpListener listener = new(IPAddress.Loopback, 0);
            listener.Start();
            return ((IPEndPoint)listener.LocalEndpoint).Port;
        }
    }
}
