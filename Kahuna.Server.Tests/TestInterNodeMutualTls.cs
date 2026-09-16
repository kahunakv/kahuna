
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Grpc.Core;
using Grpc.Net.Client;
using Kahuna.Communication.External.Grpc;
using Kahuna.Server.Communication;
using Kahuna.Server.Communication.Internode;
using Kahuna.Server.Configuration;
using Kahuna.Server.Routing;
using Kahuna.Shared.Communication.Grpc;
using Kahuna.Shared.Locks;
using Kahuna.Shared.Sequences;
using Kommander;
using Kommander.Communication.Grpc;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kahuna.Server.Tests;

/// <summary>
/// Node-to-node mutual TLS over real Kestrel listeners and real TLS handshakes: Raft and Kahuna's own
/// inter-node gRPC on one certificate per node, with the production listener policy and inbound gate.
/// <para>
/// The in-memory transport the rest of the suite uses has no handshake, so these are the only tests that
/// prove a certificate is actually presented, pinned and checked.
/// </para>
/// </summary>
public sealed class TestInterNodeMutualTls : IDisposable
{
    private readonly ILoggerFactory loggerFactory;

    private readonly List<X509Certificate2> certificates = [];

    public TestInterNodeMutualTls(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    public void Dispose()
    {
        foreach (X509Certificate2 certificate in certificates)
            certificate.Dispose();

        loggerFactory.Dispose();
    }

    /// <summary>
    /// Kahuna reaches each peer URL before Raft does, so every shared channel pool is built by Kahuna's
    /// options. Raft must still replicate over those pools.
    /// </summary>
    [Fact]
    public Task ClusterReplicatesAndForwards_WhenKahunaDialsEachPeerFirst() => ClusterReplicatesAndForwards(kahunaDialsFirst: true);

    /// <summary>Raft builds every pool first; Kahuna's forwarding must still present the certificate.</summary>
    [Fact]
    public Task ClusterReplicatesAndForwards_WhenRaftDialsEachPeerFirst() => ClusterReplicatesAndForwards(kahunaDialsFirst: false);

    private async Task ClusterReplicatesAndForwards(bool kahunaDialsFirst)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        X509Certificate2 certificate1 = CreateCertificate("node1");
        X509Certificate2 certificate2 = CreateCertificate("node2");
        string[] trusted = [Thumbprint(certificate1), Thumbprint(certificate2)];

        int[] clusterPorts = [FreePort(), FreePort()];
        List<RaftNode> roster = [.. clusterPorts.Select(port => new RaftNode($"localhost:{port}"))];

        await using MutualTlsNode node1 = await MutualTlsNode.StartAsync(certificate1, trusted, clusterPorts[0], roster, loggerFactory, ct);
        await using MutualTlsNode node2 = await MutualTlsNode.StartAsync(certificate2, trusted, clusterPorts[1], roster, loggerFactory, ct);

        if (kahunaDialsFirst)
        {
            await AssertForwardsAsync(node1, node2, ct);
            await AssertForwardsAsync(node2, node1, ct);
        }

        await Task.WhenAll(node1.Raft.JoinCluster(ct), node2.Raft.JoinCluster(ct));

        RaftManager leader = await WaitForLeaderAsync([node1.Raft, node2.Raft], ct);

        // Two voters: success needs the follower's acknowledgement, which crosses the cluster listener.
        RaftReplicationResult replicated = await leader.ReplicateLogs(0, "mtls-test", "payload"u8.ToArray(), cancellationToken: ct);
        Assert.True(replicated.Success, $"Replication over mutual TLS failed: {replicated.Status}");

        if (!kahunaDialsFirst)
        {
            await AssertForwardsAsync(node1, node2, ct);
            await AssertForwardsAsync(node2, node1, ct);
        }

        // Under the startup policy's required settings, no hint names a Raft endpoint.
        ClientEndpointAdvertiser advertiser = ClientEndpointAdvertiserFactory.Create(node1.Raft, new KahunaConfiguration
        {
            AdvertisedClientEndpoint = $"https://localhost:{node1.ApplicationPort}",
            AdvertisePeerEndpoints = false,
            RoutingHintsEnabled = true
        });

        Assert.Equal($"https://localhost:{node1.ApplicationPort}", advertiser.Advertise(node1.Raft.GetLocalEndpoint()));
        Assert.Equal("", advertiser.Advertise(node2.Raft.GetLocalEndpoint()));
    }

    [Fact]
    public async Task PeerWithAnUntrustedCertificateIsRefusedBeforeItIsServed()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        X509Certificate2 nodeCertificate = CreateCertificate("node");
        X509Certificate2 intruderCertificate = CreateCertificate("intruder");
        int port = FreePort();

        await using MutualTlsNode node = await MutualTlsNode.StartAsync(
            nodeCertificate, [Thumbprint(nodeCertificate)], port, [new RaftNode($"localhost:{port}")], loggerFactory, ct);

        // The intruder pins the node's server certificate and presents its own, which the node does not list.
        GrpcInterNodeCommunication intruder = InterNodeClient(MutualTlsOptions(intruderCertificate, [Thumbprint(nodeCertificate)]));

        RpcException refused = await Assert.ThrowsAsync<RpcException>(() =>
            intruder.GetSequence(node.ClusterEndpoint, "seq", SequenceDurability.Persistent, ct));

        Assert.Equal(StatusCode.Unauthenticated, refused.StatusCode);
        Assert.Equal(nameof(RaftTransportAuthenticationStatus.CertificateUntrusted), refused.Status.Detail);

        // The batched lock stream is torn down before any request is served; the transport answers that
        // as the operation's own MustRetry rather than leaking the stream failure to the caller.
        (LockResponseType lockType, _) = await intruder.TryLock(node.ClusterEndpoint, "resource", [1], 1000, LockDurability.Ephemeral, ct);

        Assert.Equal(LockResponseType.MustRetry, lockType);
        Assert.Empty(node.Kahuna.Calls);
    }

    [Fact]
    public async Task CallerWithoutACertificateFailsAtTheHandshake()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        X509Certificate2 nodeCertificate = CreateCertificate("node");
        int port = FreePort();

        await using MutualTlsNode node = await MutualTlsNode.StartAsync(
            nodeCertificate, [Thumbprint(nodeCertificate)], port, [new RaftNode($"localhost:{port}")], loggerFactory, ct);

        // Pins the server certificate, so the handshake fails only for the missing client certificate.
        GrpcInterNodeCommunication anonymous = InterNodeClient(new RaftTransportSecurityOptions
        {
            TrustedServerCertificateThumbprints = [Thumbprint(nodeCertificate)]
        });

        // Refused by the transport, not by the gate: the request never reached a service. A transport-class
        // refusal is answered as the operation's typed MustRetry; the gate's Unauthenticated refusal is not
        // a transport failure and would still surface as an exception here.
        (SequenceResponseType type, ReadOnlySequenceEntry? entry) =
            await anonymous.GetSequence(node.ClusterEndpoint, "seq", SequenceDurability.Persistent, ct);

        Assert.Equal(SequenceResponseType.MustRetry, type);
        Assert.Null(entry);
        Assert.Empty(node.Kahuna.Calls);
    }

    [Fact]
    public async Task ApplicationListenerServesClientsButNeverForwardedCalls()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        X509Certificate2 nodeCertificate = CreateCertificate("node");
        int port = FreePort();

        await using MutualTlsNode node = await MutualTlsNode.StartAsync(
            nodeCertificate, [Thumbprint(nodeCertificate)], port, [new RaftNode($"localhost:{port}")], loggerFactory, ct);

        // An application client: no client certificate, server certificate pinned.
        using GrpcChannel channel = GrpcChannel.ForAddress($"https://localhost:{node.ApplicationPort}", new GrpcChannelOptions
        {
            HttpHandler = new SocketsHttpHandler
            {
                SslOptions = { RemoteCertificateValidationCallback = (_, certificate, _, _) => certificate is not null && Thumbprint(certificate) == Thumbprint(nodeCertificate) }
            }
        });

        global::Sequencer.SequencerClient client = new(channel);

        await client.GetSequenceAsync(new GrpcGetSequenceRequest { Name = "seq" }, cancellationToken: ct);
        Assert.Equal(["LocateAndGet"], node.Kahuna.Calls);

        RpcException forged = await Assert.ThrowsAsync<RpcException>(async () =>
            await client.GetSequenceAsync(new GrpcGetSequenceRequest { Name = "seq" }, new Metadata { { "kahuna-forwarded", "1" } }, cancellationToken: ct));

        Assert.Equal(StatusCode.Unauthenticated, forged.StatusCode);

        // Even a trusted peer is refused here: this listener never asks for its certificate.
        GrpcInterNodeCommunication peer = InterNodeClient(MutualTlsOptions(nodeCertificate, [Thumbprint(nodeCertificate)]));

        RpcException misrouted = await Assert.ThrowsAsync<RpcException>(() =>
            peer.GetSequence($"localhost:{node.ApplicationPort}", "seq", SequenceDurability.Persistent, ct));

        Assert.Equal(nameof(RaftTransportAuthenticationStatus.CertificateRequired), misrouted.Status.Detail);
        Assert.Equal(["LocateAndGet"], node.Kahuna.Calls);
    }

    [Fact]
    public void EveryInterNodeChannelIsBuiltWithTheNodeOptions()
    {
        RaftTransportSecurityOptions options = new() { NodeAuthenticationMode = RaftNodeAuthenticationMode.MutualTls };
        GrpcInterNodeCommunication client = InterNodeClient(options);

        Assert.Same(options, client.TransportSecurity);
        Assert.Same(options, client.GetSharedBatcher("localhost:1").TransportSecurity);
    }

    private static async Task AssertForwardsAsync(MutualTlsNode from, MutualTlsNode to, CancellationToken ct)
    {
        int before = to.Kahuna.Calls.Count;
        GrpcInterNodeCommunication client = InterNodeClient(from.Raft.Configuration.GetTransportAuthenticator().Options);

        // Unary sequence forward.
        (SequenceResponseType sequenceType, _) = await client.GetSequence(to.ClusterEndpoint, "seq", SequenceDurability.Persistent, ct);
        Assert.Equal(SequenceResponseType.NotFound, sequenceType);

        // Lock forward over the duplex batch stream.
        (LockResponseType lockType, long fencingToken) = await client.TryLock(to.ClusterEndpoint, "resource", [1], 1000, LockDurability.Ephemeral, ct);
        Assert.Equal(LockResponseType.Locked, lockType);
        Assert.Equal(7, fencingToken);

        Assert.Equal(["Get", "LocateAndTryLock"], to.Kahuna.Calls.Skip(before));
    }

    private static GrpcInterNodeCommunication InterNodeClient(RaftTransportSecurityOptions options) =>
        new(new KahunaConfiguration { InterNodeGrpcScheme = "https://" }, options, NullLogger<GrpcInterNodeCommunication>.Instance);

    private static RaftTransportSecurityOptions MutualTlsOptions(X509Certificate2 certificate, IReadOnlyCollection<string> trusted) => new()
    {
        NodeAuthenticationMode = RaftNodeAuthenticationMode.MutualTls,
        ClientCertificate = certificate,
        TrustedClientCertificateThumbprints = trusted,
        TrustedServerCertificateThumbprints = trusted
    };

    private static async Task<RaftManager> WaitForLeaderAsync(RaftManager[] rafts, CancellationToken ct)
    {
        using CancellationTokenSource timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeout.CancelAfter(TimeSpan.FromSeconds(30));

        while (true)
        {
            foreach (RaftManager raft in rafts)
            {
                if (await raft.AmILeaderQuick(0))
                    return raft;
            }

            await Task.Delay(100, timeout.Token);
        }
    }

    /// <summary>
    /// Round-tripped through PKCS#12 so the private key is usable by a TLS server on every platform;
    /// macOS rejects the ephemeral key a freshly created certificate carries.
    /// </summary>
    private X509Certificate2 CreateCertificate(string name)
    {
        using ECDsa key = ECDsa.Create(ECCurve.NamedCurves.nistP256);
        using X509Certificate2 created = new CertificateRequest($"CN={name}", key, HashAlgorithmName.SHA256)
            .CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(30));

        X509Certificate2 certificate = X509CertificateLoader.LoadPkcs12(created.Export(X509ContentType.Pkcs12, "test"), "test", X509KeyStorageFlags.Exportable);
        certificates.Add(certificate);
        return certificate;
    }

    private static string Thumbprint(X509Certificate certificate) =>
        Convert.ToHexString(SHA256.HashData(certificate.GetRawCertData()));

    private static int FreePort()
    {
        using TcpListener listener = new(IPAddress.Loopback, 0);
        listener.Start();
        return ((IPEndPoint)listener.LocalEndpoint).Port;
    }

    /// <summary>
    /// One node: a Raft manager on the gRPC transport, a cluster listener on its Raft port and an application
    /// listener beside it, both configured by the production listener policy.
    /// </summary>
    private sealed class MutualTlsNode : IAsyncDisposable
    {
        private readonly WebApplication app;

        public RaftManager Raft { get; }

        public RecordingKahuna Kahuna { get; }

        public int ApplicationPort { get; }

        public string ClusterEndpoint { get; }

        private MutualTlsNode(WebApplication app, RaftManager raft, RecordingKahuna kahuna, int clusterPort, int applicationPort)
        {
            this.app = app;
            Raft = raft;
            Kahuna = kahuna;
            ApplicationPort = applicationPort;
            ClusterEndpoint = $"localhost:{clusterPort}";
        }

        public static async Task<MutualTlsNode> StartAsync(
            X509Certificate2 certificate,
            IReadOnlyCollection<string> trusted,
            int clusterPort,
            IReadOnlyList<RaftNode> roster,
            ILoggerFactory loggerFactory,
            CancellationToken ct)
        {
            int applicationPort = FreePort();
            ILogger<IRaft> raftLogger = loggerFactory.CreateLogger<IRaft>();

            RaftManager raft = new(
                new RaftConfiguration
                {
                    NodeId = clusterPort,
                    Host = "localhost",
                    Port = clusterPort,
                    InitialPartitions = 1,
                    GrpcScheme = "https://",
                    HeartbeatInterval = TimeSpan.FromMilliseconds(150),
                    VotingTimeout = TimeSpan.FromMilliseconds(500),
                    StartElectionTimeout = 600,
                    EndElectionTimeout = 900,
                    EnableQuiescence = false,
                    TimerInitialDelay = TimeSpan.FromMilliseconds(500),
                    PingInterval = TimeSpan.Zero,
                    TransportSecurity = MutualTlsOptions(certificate, trusted)
                },
                new StaticDiscovery([.. roster.Where(node => node.Endpoint != $"localhost:{clusterPort}")]),
                new InMemoryWAL(raftLogger),
                new GrpcCommunication(),
                new HybridLogicalClock(),
                raftLogger);

            RecordingKahuna kahuna = new();

            WebApplicationBuilder builder = WebApplication.CreateBuilder();
            builder.Logging.ClearProviders();
            builder.Services.AddSingleton<IRaft>(raft);
            builder.Services.AddSingleton(raftLogger);
            builder.Services.AddSingleton<IKahuna>(kahuna);
            builder.Services.AddSingleton(new KahunaConfiguration());
            builder.Services.AddNodeTransportGate();
            builder.Services.AddGrpc();

            builder.WebHost.ConfigureKestrel(kestrel =>
            {
                foreach (int port in (int[])[clusterPort, applicationPort])
                {
                    bool clusterListener = NodeTransportSecurityPolicy.IsClusterListener(raft.Configuration.TransportSecurity, port, clusterPort);

                    kestrel.Listen(IPAddress.Loopback, port, listenOptions =>
                    {
                        listenOptions.Protocols = NodeTransportSecurityPolicy.GetHttpsProtocols(clusterListener);
                        listenOptions.UseHttps(certificate, httpsOptions => NodeTransportSecurityPolicy.ConfigureClientCertificate(httpsOptions, clusterListener));
                    });
                }
            });

            WebApplication app = builder.Build();
            app.MapGrpcRaftRoutes();
            app.MapGrpcKahunaRoutes();

            await app.StartAsync(ct);

            return new(app, raft, kahuna, clusterPort, applicationPort);
        }

        public async ValueTask DisposeAsync()
        {
            Raft.Dispose();

            using CancellationTokenSource stop = new(TimeSpan.FromSeconds(2));
            try { await app.StopAsync(stop.Token); } catch { /* best-effort */ }
            await app.DisposeAsync();
        }
    }

    /// <summary>Records which entry points a peer's forwarded call reached.</summary>
    private sealed class RecordingKahuna : FakeKahunaBase
    {
        private readonly List<string> calls = [];

        public IReadOnlyList<string> Calls
        {
            get { lock (calls) return [.. calls]; }
        }

        private void Record(string call)
        {
            lock (calls) calls.Add(call);
        }

        public override Task<(SequenceResponseType, ReadOnlySequenceEntry?)> GetSequence(string name, SequenceDurability durability, CancellationToken cancellationToken)
        {
            Record("Get");
            return Task.FromResult<(SequenceResponseType, ReadOnlySequenceEntry?)>((SequenceResponseType.NotFound, null));
        }

        public override Task<(SequenceResponseType, ReadOnlySequenceEntry?)> LocateAndGetSequence(string name, SequenceDurability durability, CancellationToken cancellationToken)
        {
            Record("LocateAndGet");
            return Task.FromResult<(SequenceResponseType, ReadOnlySequenceEntry?)>((SequenceResponseType.NotFound, null));
        }

        public override Task<(LockResponseType, long)> LocateAndTryLock(string resource, byte[] owner, int expiresMs, LockDurability durability, CancellationToken cancellationToken)
        {
            Record("LocateAndTryLock");
            return Task.FromResult((LockResponseType.Locked, 7L));
        }
    }
}
