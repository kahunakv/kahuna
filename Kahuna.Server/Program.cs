
using Nixie;
using System.Net;
using CommandLine;

using Kahuna;
using Kahuna.Communication.Grpc;
using Kahuna.Communication.Rest;
using Kahuna.Locks;
using Kahuna.Services;

using Kommander;
using Kommander.Communication;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;

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
    RaftConfiguration configuration = new()
    {
        Host = opts.RaftHost,
        Port = opts.RaftPort,
        MaxPartitions = opts.InitialClusterPartitions
    };
    
    return new RaftManager(
        services.GetRequiredService<ActorSystem>(),
        configuration,
        new StaticDiscovery(opts.InitialCluster is not null ? [.. opts.InitialCluster.Select(k => new RaftNode(k))] : []),
        new SqliteWAL(path: opts.SqliteWalPath, version: opts.SqliteWalRevision),
        new HttpCommunication(),
        new HybridLogicalClock(),
        services.GetRequiredService<ILogger<IRaft>>()
    );
});

builder.Services.AddSingleton<ActorSystem>(services => new(services, services.GetRequiredService<ILogger<IRaft>>()));
builder.Services.AddSingleton<IKahuna, LockManager>();
builder.Services.AddHostedService<ReplicationService>();

builder.Services.AddGrpc();
builder.Services.AddGrpcReflection();

foreach (var x in Environment.GetCommandLineArgs())
    Console.WriteLine(x);

builder.WebHost.ConfigureKestrel(options =>
{
    if (opts.HttpPorts is null || !opts.HttpPorts.Any())
        options.Listen(IPAddress.Any, 2070, _ => { });
    else
        foreach (string port in opts.HttpPorts)
        {
            Console.WriteLine($"Listening on port {port}");
            options.Listen(IPAddress.Any, int.Parse(port), _ => { });
        }

    if (opts.HttpsPorts is null || !opts.HttpsPorts.Any())
        options.Listen(IPAddress.Any, 2071, _ => { });
    else
    {
        foreach (string port in opts.HttpsPorts)
        {
            options.Listen(IPAddress.Any, int.Parse(port), listenOptions =>
            {
                listenOptions.UseHttps(opts.HttpsCertificate, opts.HttpsCertificatePassword);
            });
        }
    }
});

WebApplication app = builder.Build();

app.MapRaftRoutes();
app.MapRestKahunaRoutes();
app.MapGrpcKahunaRoutes();
app.MapGrpcReflectionService();
app.MapGet("/", () => "Kahuna.Server");

app.Run();

