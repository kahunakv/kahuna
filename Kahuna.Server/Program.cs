
using Nixie;
using System.Net;
using System.Security.Cryptography.X509Certificates;
using CommandLine;

using Kahuna;
using Kahuna.Communication.Grpc;
using Kahuna.Communication.Rest;
using Kahuna.Configuration;
using Kahuna.Locks;
using Kahuna.Services;

using Kommander;
using Kommander.Communication.Grpc;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.AspNetCore.Server.Kestrel.Core;

Console.WriteLine("  _           _                     ");
Console.WriteLine(" | | ____ _| |__  _   _ _ __   __ _ ");
Console.WriteLine(" | |/ / _` | '_ \\| | | | '_ \\ / _` |");
Console.WriteLine(" |   < (_| | | | | |_| | | | | (_| |");
Console.WriteLine(" |_|\\_\\__,_|_| |_|\\__,_|_| |_|\\__,_|");
Console.WriteLine("");

ParserResult<KahunaCommandLineOptions> optsResult = Parser.Default.ParseArguments<KahunaCommandLineOptions>(args);

KahunaCommandLineOptions? opts = optsResult.Value;
if (opts is null)
    return;

WebApplicationBuilder builder = WebApplication.CreateBuilder(args);

// Try to assemble a Kahuna cluster from static discovery
builder.Services.AddSingleton<IRaft>(services =>
{
    if (string.IsNullOrEmpty(opts.RaftNodeId))
        opts.RaftNodeId = Environment.MachineName;
    
    RaftConfiguration configuration = new()
    {
        NodeId = opts.RaftNodeId,
        Host = opts.RaftHost,
        Port = opts.RaftPort,
        MaxPartitions = opts.InitialClusterPartitions
    };

    IWAL walAdapter = opts.WalStorage switch
    {
        "rocksdb" => new RocksDbWAL(path: opts.WalPath, revision: opts.WalRevision),
        "sqlite" => new SqliteWAL(path: opts.WalPath, revision: opts.WalRevision),
        _ => throw new KahunaServerException("Invalid WAL storage")
    };

    return new RaftManager(
        services.GetRequiredService<ActorSystem>(),
        configuration,
        new StaticDiscovery(opts.InitialCluster is not null ? [.. opts.InitialCluster.Select(k => new RaftNode(k))] : []),
        walAdapter,
        new GrpcCommunication(),
        new HybridLogicalClock(),
        services.GetRequiredService<ILogger<IRaft>>()
    );
});

builder.Services.AddSingleton<ActorSystem>(services => new(services, services.GetRequiredService<ILogger<IRaft>>()));
builder.Services.AddSingleton<IKahuna, LockManager>();
builder.Services.AddHostedService<ReplicationService>();

builder.Services.AddGrpc();
builder.Services.AddGrpcReflection();

// Listen on all http/https ports in the configuration    
builder.WebHost.ConfigureKestrel(options =>
{
    if (opts.HttpPorts is null || !opts.HttpPorts.Any())
        options.Listen(IPAddress.Any, 2070, listenOptions =>
        {
            listenOptions.Protocols = HttpProtocols.Http1AndHttp2AndHttp3;
        });
    else
        foreach (string port in opts.HttpPorts)
            options.Listen(IPAddress.Any, int.Parse(port), listenOptions =>
            {
                listenOptions.Protocols = HttpProtocols.Http1AndHttp2AndHttp3;
            });

    if (opts.HttpsPorts is null || !opts.HttpsPorts.Any())
        options.Listen(IPAddress.Any, 2071, listenOptions =>
        {
            listenOptions.Protocols = HttpProtocols.Http1AndHttp2AndHttp3;
            listenOptions.UseHttps(opts.HttpsCertificate, opts.HttpsCertificatePassword);
        });
    else
    {
        foreach (string port in opts.HttpsPorts)
        {
            options.Listen(IPAddress.Any, int.Parse(port), listenOptions =>
            {
                listenOptions.Protocols = HttpProtocols.Http1AndHttp2AndHttp3;
                listenOptions.UseHttps(opts.HttpsCertificate, opts.HttpsCertificatePassword);
            });
        }
    }
});

// Build Kahuna configuration
KahunaConfiguration configuration = new()
{
    HttpsCertificate = opts.HttpsCertificate,
    HttpsCertificatePassword = opts.HttpsCertificatePassword,
    LocksWorkers = opts.LocksWorkers,
    BackgroundWriterWorkers = opts.BackgroundWritersWorkers
};

// @todo move somewhere else
if (!string.IsNullOrEmpty(configuration.HttpsCertificate))
{
    if (!File.Exists(configuration.HttpsCertificate))
        throw new KahunaServerException("Invalid HTTPS certificate");
    
#pragma warning disable SYSLIB0057
    X509Certificate2 xcertificate = new(configuration.HttpsCertificate, configuration.HttpsCertificatePassword);
#pragma warning restore SYSLIB0057
    configuration.HttpsTrustedThumbprint = xcertificate.Thumbprint;
}

builder.Services.AddSingleton(configuration);

// Start server
WebApplication app = builder.Build();

app.MapRestKahunaRoutes();
app.MapGrpcRaftRoutes();
app.MapGrpcKahunaRoutes();
app.MapGrpcReflectionService();

app.Run();

