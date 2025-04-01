
using Kommander;
using Kahuna.Server.Diagnostics;

namespace Kahuna.Services;

public sealed class ReplicationService : BackgroundService //, IDisposable
{
    private readonly IKahuna kahuna;
    
    private readonly IRaft raft;
    
    private readonly ThreadStatsLogger statsLogger;
    
    private readonly ILogger<IRaft> logger;

    public ReplicationService(IKahuna kahuna, IRaft raft, ILogger<IRaft> logger)
    {
        this.kahuna = kahuna;
        this.raft = raft;
        this.logger = logger;
        this.statsLogger = new(logger);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (raft.Configuration.Host == "*")
            return;
        
        raft.OnReplicationReceived += kahuna.OnReplicationReceived;
        raft.OnReplicationError += kahuna.OnReplicationError;
        
        await raft.JoinCluster();
        
        while (true)
        {
            await raft.UpdateNodes();

            await Task.Delay(3000, stoppingToken);
        }
    }
}
