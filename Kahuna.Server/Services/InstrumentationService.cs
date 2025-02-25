
using Kommander;

namespace Kahuna.Services;

public class InstrumentationService : BackgroundService //, IDisposable
{
    private readonly IRaft raftManager;

    public InstrumentationService(IRaft raftManager)
    {
        this.raftManager = raftManager;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (raftManager.Configuration.Host == "*")
            return;
        
        await raftManager.JoinCluster();
        
        while (true)
        {
            await raftManager.UpdateNodes();

            await Task.Delay(3000, stoppingToken);
        }
    }
}