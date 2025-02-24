
using CommandLine;
using Kahuna;
using Kahuna.Services;
using Kommander;
using Kommander.Communication;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Nixie;

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
    string[] cluster = opts.InitialCluster.Split(',');
    
    RaftConfiguration configuration = new()
    {
        Host = opts.Host,
        Port = opts.Port,
        MaxPartitions = opts.InitialClusterPartitions
    };
    
    IRaft node = new RaftManager(
        services.GetRequiredService<ActorSystem>(),
        configuration,
        new StaticDiscovery(cluster.Select(x => new RaftNode(x.Trim())).ToList()),
        new SqliteWAL(),
        new HttpCommunication(),
        new HybridLogicalClock(),
        services.GetRequiredService<ILogger<IRaft>>()
    );

    if (cluster.Length > 0)
        node.JoinCluster();

    return node;
});

builder.Services.AddSingleton<ActorSystem>(services => new(services, services.GetRequiredService<ILogger<IRaft>>()));
builder.Services.AddSingleton<IKahuna, LockManager>();
builder.Services.AddHostedService<InstrumentationService>();

WebApplication app = builder.Build();

app.MapRaftRoutes();
app.MapKahunaRoutes();
app.MapGet("/", () => "Kahuna.Server");

app.Run($"http://{opts.Host}:{opts.Port}");

