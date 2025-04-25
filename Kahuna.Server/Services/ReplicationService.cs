
using Kommander;
using Kahuna.Server.Diagnostics;

namespace Kahuna.Services;

/// <summary>
/// The ReplicationService class is a background service responsible for managing the replication process
/// within a Kahuna cluster. This class handles interactions between the Raft distributed consensus
/// algorithm and the application's main functionality by subscribing to and responding to Raft events.
/// </summary>
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
        
        raft.OnLogRestored += kahuna.OnLogRestored;
        raft.OnReplicationReceived += kahuna.OnReplicationReceived;
        raft.OnReplicationError += kahuna.OnReplicationError;
        
        await raft.JoinCluster();
    }
}
