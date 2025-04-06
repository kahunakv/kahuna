
using System.Text;
using Kommander;
using Kahuna.Shared.Locks;
using Microsoft.Extensions.Logging;

namespace Kahuna.Tests.Server;

public class TestLocks : BaseCluster
{
    private readonly ILogger<IRaft> raftLogger;
    
    private readonly ILogger<IKahuna> kahunaLogger;
    
    public TestLocks(ITestOutputHelper outputHelper)
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
    
    private static string GetRandomLockName()
    {
        return Guid.NewGuid().ToString("N")[..10];
    }
    
    [Theory, CombinatorialData]
    public async Task TestLockAndUnlock(
        [CombinatorialValues("memory")] string storage,
        [CombinatorialValues(8, 16)] int partitions,
        [CombinatorialValues(LockDurability.Ephemeral, LockDurability.Persistent)] LockDurability durability
        //[CombinatorialValues("memory")] string storage,
        //[CombinatorialValues(16)] int partitions,
        //[CombinatorialValues(LockDurability.Ephemeral)] LockDurability durability
    )
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) = await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);
        
        string lockName = GetRandomLockName();
        
        byte[] ownerA = Encoding.UTF8.GetBytes(GetRandomLockName());
        byte[] ownerB = Encoding.UTF8.GetBytes(GetRandomLockName());
        
        (LockResponseType response, long fencingToken) = await kahuna1.LocateAndTryLock("", ownerA, 10000, durability, TestContext.Current.CancellationToken);
        Assert.Equal(LockResponseType.InvalidInput, response);
        Assert.Equal(0, fencingToken);
        
        (response, fencingToken) = await kahuna1.LocateAndTryLock(lockName, ""u8.ToArray(), 10000, durability, TestContext.Current.CancellationToken);
        Assert.Equal(LockResponseType.InvalidInput, response);
        Assert.Equal(0, fencingToken);
        
        (response, fencingToken) = await kahuna1.LocateAndTryLock(lockName, ownerA, 0, durability, TestContext.Current.CancellationToken);
        Assert.Equal(LockResponseType.InvalidInput, response);
        Assert.Equal(0, fencingToken);
        
        (response, fencingToken) = await kahuna1.LocateAndTryLock(lockName, ownerA, 10000, durability, TestContext.Current.CancellationToken);
        Assert.Equal(LockResponseType.Locked, response);
        Assert.Equal(0, fencingToken);
        
        (response, fencingToken) = await kahuna1.LocateAndTryLock(lockName, ownerB, 10000, durability, TestContext.Current.CancellationToken);
        Assert.Equal(LockResponseType.Busy, response);
        Assert.Equal(0, fencingToken);
        
        response = await kahuna1.LocateAndTryUnlock(lockName, ownerA, durability, TestContext.Current.CancellationToken);
        Assert.Equal(LockResponseType.Unlocked, response);
        
        (response, fencingToken) = await kahuna1.LocateAndTryLock(lockName, ownerA, 10000, durability, TestContext.Current.CancellationToken);
        Assert.Equal(LockResponseType.Locked, response);
        Assert.Equal(1, fencingToken);
        
        (response, fencingToken) = await kahuna1.LocateAndTryExtendLock(lockName, ownerA, 5000, durability, TestContext.Current.CancellationToken);
        Assert.Equal(LockResponseType.Extended, response);
        Assert.Equal(1, fencingToken);
        
        response = await kahuna1.LocateAndTryUnlock(lockName, ownerA, durability, TestContext.Current.CancellationToken);
        Assert.Equal(LockResponseType.Unlocked, response);
        
        await node1.LeaveCluster(true);
        await node2.LeaveCluster(true);
        await node3.LeaveCluster(true);
    }
}