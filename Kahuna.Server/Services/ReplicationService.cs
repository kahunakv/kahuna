
using Kommander;

namespace Kahuna.Services;

public sealed class ReplicationService : BackgroundService //, IDisposable
{
    private readonly IKahuna kahuna;
    
    private readonly IRaft raft;

    public ReplicationService(IKahuna kahuna, IRaft raft)
    {
        this.kahuna = kahuna;
        this.raft = raft;
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