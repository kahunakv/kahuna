
using Nixie;
using System.Net;
using System.Runtime.CompilerServices;
using CommandLine;
using Flurl.Http;

using Kahuna;
using Kahuna.Services;
using Kahuna.Server.Configuration;
using Kahuna.Communication.External.Grpc;
using Kahuna.Communication.External.Rest;
using Kahuna.Server.Communication.Internode;
using Kahuna.Server.Locks;
using Kommander;
using Kommander.Time;
using Kommander.WAL;
using Kommander.Discovery;
using Kommander.Communication.Grpc;
using Kommander.Communication.Rest;

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
    if (string.IsNullOrEmpty(opts.RaftNodeName))
        opts.RaftNodeName = Environment.MachineName;

    ILogger<IRaft> logger = services.GetRequiredService<ILogger<IRaft>>();
    
    RaftConfiguration configuration = new()
    {
        NodeName = opts.RaftNodeName,
        NodeId = opts.RaftNodeId,
        Host = opts.RaftHost,
        Port = opts.RaftPort,
        InitialPartitions = opts.InitialClusterPartitions,
        ReadIOThreads = opts.ReadIOThreads,
        WriteIOThreads = opts.WriteIOThreads
    };

    IWAL walAdapter = opts.WalStorage switch
    {
        "rocksdb" => new RocksDbWAL(path: opts.WalPath, revision: opts.WalRevision, logger),
        "sqlite" => new SqliteWAL(path: opts.WalPath, revision: opts.WalRevision, logger),
        _ => throw new KahunaServerException("Invalid WAL storage")
    };

    //IWAL walAdapter = new InMemoryWAL(logger);
    
    return new RaftManager(
        services.GetRequiredService<ActorSystem>(),
        configuration,
        new StaticDiscovery(opts.InitialCluster is not null ? [.. opts.InitialCluster.Select(k => new RaftNode(k))] : []),
        walAdapter,
        new GrpcCommunication(),
        new HybridLogicalClock(),
        logger
    );
});

builder.Services.AddSingleton<ActorSystem>(services => new(services, services.GetRequiredService<ILogger<IRaft>>()));
builder.Services.AddSingleton<IKahuna, KahunaManager>();
builder.Services.AddSingleton<IInterNodeCommunication, GrpcInterNodeCommunication>();
builder.Services.AddHostedService<ReplicationService>();

builder.Services.AddGrpc();
builder.Services.AddGrpcReflection();

// Listen on all http/https ports in the configuration    
builder.WebHost.ConfigureKestrel(options =>
{
    options.AllowSynchronousIO = false;
    
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

ThreadPool.SetMinThreads(256, 128);
    
// @todo Review certificate validation
FlurlHttp.Clients.WithDefaults(x => x.ConfigureInnerHandler(ih => ih.ServerCertificateCustomValidationCallback = (a, b, c, d) => true));

builder.Services.AddSingleton(ConfigurationValidator.Validate(opts));

// Start server
WebApplication app = builder.Build();

app.MapRestRaftRoutes();
app.MapRestKahunaRoutes();

app.MapGrpcRaftRoutes();
app.MapGrpcKahunaRoutes();
app.MapGrpcReflectionService();

app.Run();

