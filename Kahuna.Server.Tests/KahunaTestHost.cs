using System.Net;
using System.Net.Sockets;
using Kahuna.Client;
using Kahuna.Client.Communication;
using Kahuna.Communication.External.Grpc;
using Kahuna.Communication.External.Rest;
using Kahuna.Server.Communication;
using Kahuna.Server.Configuration;
using Kahuna.Shared.Communication.Rest;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kahuna.Server.Tests;

/// <summary>
/// One embedded node behind a real Kestrel listener: the gRPC services on an HTTP/2 port and the REST routes on
/// an HTTP/1.1 port, with the admin gate registered the way the server registers it. The REST routes read their
/// bounds from the configuration given to <see cref="StartAsync"/>, as the server reads them from its own.
/// </summary>
internal sealed class KahunaTestHost : IAsyncDisposable
{
    private readonly EmbeddedKahunaNode node;

    private readonly WebApplication app;

    private KahunaTestHost(EmbeddedKahunaNode node, WebApplication app, int grpcPort, int restPort)
    {
        this.node = node;
        this.app = app;
        GrpcUrl = $"http://localhost:{grpcPort}";
        RestUrl = $"http://localhost:{restPort}";
    }

    public string GrpcUrl { get; }

    public string RestUrl { get; }

    /// <summary>The node behind the listeners, for setting up and checking state without a transport.</summary>
    public IKahuna Kahuna => node.Kahuna;

    public static async Task<KahunaTestHost> StartAsync(
    ILoggerFactory loggerFactory, CancellationToken ct, IReadOnlyList<string>? adminKeys = null, KahunaConfiguration? configuration = null)
    {
        EmbeddedKahunaNode node = new(new() { Storage = "memory", WalStorage = "memory", InitialPartitions = 2 }, loggerFactory);
        await node.StartAsync(ct);

        int grpcPort = FreePort();
        int restPort = FreePort();

        WebApplicationBuilder builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.Services.AddSingleton(node.Kahuna);
        builder.Services.AddSingleton(node.Raft);
        builder.Services.AddSingleton(configuration ?? new KahunaConfiguration());
        builder.Services.AddSingleton(NodeTransportGate.Disabled);
        builder.Services.AddSingleton<ILogger<IKahuna>>(NullLogger<IKahuna>.Instance);
        builder.Services.AddGrpc(static options => options.EnableDetailedErrors = true);
        builder.Services.AddAdminGate(adminKeys ?? []);
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
        RangesHandlers.MapRangesRoutes(app);
        ClusterHandlers.MapClusterRoutes(app);
        BackupsHandlers.MapBackupsRoutes(app);
        await app.StartAsync(ct);

        return new(node, app, grpcPort, restPort);
    }

    public KahunaClient Client(string transport, string? adminKey)
    {
        KahunaOptions options = new() { AdminApiKey = adminKey };

        return transport == "grpc"
            ? new(GrpcUrl, communication: new GrpcCommunication(options, null), options: options)
            : new(RestUrl, communication: new RestCommunication(null, options), options: options);
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
