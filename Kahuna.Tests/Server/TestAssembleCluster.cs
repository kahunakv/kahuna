
using Kommander;
using Kommander.Communication.Memory;
using Microsoft.Extensions.Logging;

namespace Kahuna.Tests.Server;

public sealed class TestAssembleCluster : BaseCluster
{
    private readonly ILogger<IRaft> raftLogger;
    
    private readonly ILogger<IKahuna> kahunaLogger;
    
    public TestAssembleCluster(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = LoggerFactory.Create(builder =>
        {
            builder
                .AddXUnit(outputHelper)
                .SetMinimumLevel(LogLevel.Debug);
        });

        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }
    
    [Theory, CombinatorialData]
    public async Task TestJoinClusterAndDecideLeaderOnManyPartitions(
        [CombinatorialValues("memory")] string walStorage,
        [CombinatorialValues(1, 4, 8, 16)] int partitions
    )
    {
        (IRaft node1, IRaft node2, IRaft node3) = await AssembleThreNodeCluster(walStorage, partitions);
        
        await node1.LeaveCluster(true);
        await node2.LeaveCluster(true);
        await node3.LeaveCluster(true);
    }
    
    private async Task<(IRaft, IRaft, IRaft)> AssembleThreNodeCluster(string walStorage, int partitions)
    {
        InMemoryCommunication communication = new();
        
        (IRaft raft1, IKahuna kahuna1) = GetNode1(communication, walStorage, partitions, raftLogger, kahunaLogger);
        (IRaft raft2, IKahuna kahuna2) = GetNode2(communication, walStorage, partitions, raftLogger, kahunaLogger);
        (IRaft raft3, IKahuna kahuna3) = GetNode3(communication, walStorage, partitions, raftLogger, kahunaLogger);
        
        await WaitForClusterToAssemble(communication, partitions, raft1, raft2, raft3);
        
        return (raft1, raft2, raft3);
    }
}