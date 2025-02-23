
using CommandLine;
using Kahuna;
using Kommander;
using Kommander.Communication;
using Kommander.Discovery;
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

bool assembleCluster = false;
ActorSystem actorSystem = new();

if (string.IsNullOrEmpty(opts.InitialCluster))
    builder.Services.AddSingleton<RaftManager>();
else
{
    // Try to assemble a Kahuna cluster from static discovery
    string[] cluster = opts.InitialCluster.Split(',');
    if (cluster.Length > 0)
    {
        assembleCluster = true;
        
        RaftConfiguration configuration = new()
        {
            Host = opts.Host,
            Port = opts.Port,
            MaxPartitions = opts.InitialClusterPartitions
        };
        
        builder.Services.AddSingleton(new RaftManager(
            actorSystem, 
            configuration, 
            new StaticDiscovery(cluster.Select(x => new RaftNode(x.Trim())).ToList()), 
            new SqliteWAL(), 
            new HttpCommunication()
        ));
    }
}

builder.Services.AddSingleton(actorSystem);
builder.Services.AddSingleton<IKahuna, LockManager>();

WebApplication app = builder.Build();

if (assembleCluster)
    app.MapRaftRoutes();

app.MapKahunaRoutes();

app.MapGet("/", () => "Kahuna.Server");

app.Run($"http://{opts.Host}:{opts.Port}");

