
using Kommander;
using Kahuna.Shared.Locks;
using Microsoft.Extensions.Logging;

namespace Kahuna.Tests.Server;

public class TestLocksInServer : BaseCluster
{
    private readonly ILogger<IRaft> raftLogger;
    
    private readonly ILogger<IKahuna> kahunaLogger;
    
    public TestLocksInServer(ITestOutputHelper outputHelper)
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
    public async Task TestLocks(
        [CombinatorialValues("memory")] string walStorage,
        [CombinatorialValues(4, 8, 16)] int partitions
    )
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) = await AssembleThreNodeCluster(walStorage, partitions, raftLogger, kahunaLogger);
        
        (LockResponseType response, long fencingToken) = await kahuna1.TryLock("some", "hello"u8.ToArray(), 10000, LockDurability.Ephemeral);
        Assert.Equal(LockResponseType.Locked, response);
        Assert.Equal(0, fencingToken);
        
        response = await kahuna1.TryUnlock("some", "hello"u8.ToArray(), LockDurability.Ephemeral);
        Assert.Equal(LockResponseType.Unlocked, response);
        
        (response, fencingToken) = await kahuna1.TryLock("some", "hello"u8.ToArray(), 10000, LockDurability.Ephemeral);
        Assert.Equal(LockResponseType.Locked, response);
        Assert.Equal(1, fencingToken);
        
        await node1.LeaveCluster(true);
        await node2.LeaveCluster(true);
        await node3.LeaveCluster(true);
    }
}