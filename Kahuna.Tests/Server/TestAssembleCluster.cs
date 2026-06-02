
using Kahuna.Shared.Locks;
using Kommander;
using Microsoft.Extensions.Logging;

namespace Kahuna.Tests.Server;

public sealed class TestAssembleCluster : BaseCluster
{
    private readonly ILogger<IRaft> raftLogger;
    
    private readonly ILogger<IKahuna> kahunaLogger;
    
    public TestAssembleCluster(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = TestLogFactory.Create(outputHelper);

        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }
    
    [Theory, CombinatorialData]
    public async Task TestJoinClusterAndDecideLeaderOnManyPartitions(
        [CombinatorialValues("memory")] string walStorage,
        [CombinatorialValues(1, 4, 8, 16)] int partitions
    )
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) = await AssembleThreNodeCluster(walStorage, partitions, raftLogger, kahunaLogger);
        
        await node1.LeaveCluster(true);
        await node2.LeaveCluster(true);
        await node3.LeaveCluster(true);
    }
}
