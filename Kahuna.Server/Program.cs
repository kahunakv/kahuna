
using System.Net;
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

List<RaftNode> nodes = [];
    
string[] cluster = !string.IsNullOrEmpty(opts.InitialCluster) ? opts.InitialCluster.Split(',') : [];

if (cluster.Length == 0)
{
    string? kahunaHost = Environment.GetEnvironmentVariable("KAHUNA_HOST");
    if (opts.Host == "*" && !string.IsNullOrEmpty(kahunaHost))
    {                        
        opts.Host = Dns.GetHostAddresses(kahunaHost)[0].ToString();            
            
        nodes =
        [
            new(Dns.GetHostAddresses("kahuna1")[0] + ":8081"),
            new(Dns.GetHostAddresses("kahuna2")[0] + ":8082"),
            new(Dns.GetHostAddresses("kahuna3")[0] + ":8083")
        ];                    
    }

    string? kahunaPort = Environment.GetEnvironmentVariable("KAHUNA_PORT");
    if (opts.Port == 2070 && !string.IsNullOrEmpty(kahunaPort))
        opts.Port = int.Parse(kahunaPort);

    nodes.RemoveAll(x => x.Endpoint == opts.Host + ":" + opts.Port);
}

// Try to assemble a Kahuna cluster from static discovery
builder.Services.AddSingleton<IRaft>(services =>
{
    RaftConfiguration configuration = new()
    {
        Host = opts.Host,
        Port = opts.Port,
        MaxPartitions = opts.InitialClusterPartitions
    };
    
    return new RaftManager(
        services.GetRequiredService<ActorSystem>(),
        configuration,
        new StaticDiscovery(nodes),
        new SqliteWAL(),
        new HttpCommunication(),
        new HybridLogicalClock(),
        services.GetRequiredService<ILogger<IRaft>>()
    );
});

builder.Services.AddSingleton<ActorSystem>(services => new(services, services.GetRequiredService<ILogger<IRaft>>()));
builder.Services.AddSingleton<IKahuna, LockManager>();
builder.Services.AddHostedService<InstrumentationService>();

WebApplication app = builder.Build();

app.MapRaftRoutes();
app.MapKahunaRoutes();
app.MapGet("/", () => "Kahuna.Server");

Console.WriteLine("Kahuna host detected: {0} {1}", opts.Host, opts.Port);

app.Run();

