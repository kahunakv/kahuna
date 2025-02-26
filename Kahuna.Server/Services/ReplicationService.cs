
using Kommander;

namespace Kahuna.Services;

public class ReplicationService : BackgroundService //, IDisposable
{
    private readonly IKahuna kahuna;
    
    private readonly IRaft raftManager;

    public ReplicationService(IKahuna kahuna, IRaft raftManager)
    {
        this.kahuna = kahuna;
        this.raftManager = raftManager;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (raftManager.Configuration.Host == "*")
            return;

        raftManager.OnReplicationReceived += kahuna.OnReplicationReceived;
        
        await raftManager.JoinCluster();
        
        while (true)
        {
            await raftManager.UpdateNodes();

            await Task.Delay(3000, stoppingToken);
        }
    }
}