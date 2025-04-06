using System.Text;
using Kahuna.Server.Locks;
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

        (response, fencingToken) = await kahuna1.LocateAndTryExtendLock(lockName, ownerA, 15000, durability, TestContext.Current.CancellationToken);
        Assert.Equal(LockResponseType.Extended, response);
        Assert.Equal(1, fencingToken);

        (response, ReadOnlyLockContext? lockContext) = await kahuna1.LocateAndGetLock(lockName, durability, TestContext.Current.CancellationToken);
        Assert.Equal(LockResponseType.Got, response);
        Assert.NotNull(lockContext);

        response = await kahuna1.LocateAndTryUnlock(lockName, ownerA, durability, TestContext.Current.CancellationToken);
        Assert.Equal(LockResponseType.Unlocked, response);

        await node1.LeaveCluster(true);
        await node2.LeaveCluster(true);
        await node3.LeaveCluster(true);
    }

    [Theory, CombinatorialData]
    public async Task TestLockOperationsAcrossNodes(
        [CombinatorialValues("memory")] string storage,
        [CombinatorialValues(8, 16)] int partitions,
        [CombinatorialValues(LockDurability.Ephemeral, LockDurability.Persistent)] LockDurability durability
    )
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) = await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        string lockName = GetRandomLockName();
        byte[] ownerA = Encoding.UTF8.GetBytes(GetRandomLockName());
        byte[] ownerB = Encoding.UTF8.GetBytes(GetRandomLockName());

        // Test acquiring lock from different nodes
        (LockResponseType response, long fencingToken) = await kahuna1.LocateAndTryLock(lockName, ownerA, 10000, durability, TestContext.Current.CancellationToken);
        Assert.Equal(LockResponseType.Locked, response);
        Assert.Equal(0, fencingToken);

        // Try to acquire same lock from another node
        (response, fencingToken) = await kahuna2.LocateAndTryLock(lockName, ownerB, 10000, durability, TestContext.Current.CancellationToken);
        Assert.Equal(LockResponseType.Busy, response);
        Assert.Equal(0, fencingToken);

        // Extend lock from a different node
        (response, fencingToken) = await kahuna3.LocateAndTryExtendLock(lockName, ownerA, 15000, durability, TestContext.Current.CancellationToken);
        Assert.Equal(LockResponseType.Extended, response);
        Assert.Equal(0, fencingToken);

        // Unlock from a different node
        response = await kahuna2.LocateAndTryUnlock(lockName, ownerA, durability, TestContext.Current.CancellationToken);
        Assert.Equal(LockResponseType.Unlocked, response);

        // Verify lock is released by trying to acquire from another node
        (response, fencingToken) = await kahuna3.LocateAndTryLock(lockName, ownerB, 10000, durability, TestContext.Current.CancellationToken);
        Assert.Equal(LockResponseType.Locked, response);
        Assert.Equal(1, fencingToken);

        // Get lock context from a different node
        (response, ReadOnlyLockContext? lockContext) = await kahuna1.LocateAndGetLock(lockName, durability, TestContext.Current.CancellationToken);
        Assert.Equal(LockResponseType.Got, response);
        Assert.NotNull(lockContext);

        await node1.LeaveCluster(true);
        await node2.LeaveCluster(true);
        await node3.LeaveCluster(true);
    }

    [Theory, CombinatorialData]
    public async Task TestLockEdgeCases(
        [CombinatorialValues("memory")] string storage,
        [CombinatorialValues(8)] int partitions,
        [CombinatorialValues(LockDurability.Ephemeral, LockDurability.Persistent)] LockDurability durability
    )
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) = await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        string lockName = GetRandomLockName();
        byte[] ownerA = Encoding.UTF8.GetBytes(GetRandomLockName());
        byte[] ownerB = Encoding.UTF8.GetBytes(GetRandomLockName());

        // Test invalid expireMs values
        (LockResponseType response, long fencingToken) = await kahuna1.LocateAndTryLock(lockName, ownerA, -1, durability, TestContext.Current.CancellationToken);
        Assert.Equal(LockResponseType.InvalidInput, response);
        Assert.Equal(0, fencingToken);

        // Test invalid owner (null)
        (response, fencingToken) = await kahuna1.LocateAndTryLock(lockName, [], 1000, durability, TestContext.Current.CancellationToken);
        Assert.Equal(LockResponseType.InvalidInput, response);
        Assert.Equal(0, fencingToken);

        // Test operations on non-existent lock
        response = await kahuna1.LocateAndTryUnlock(lockName, ownerA, durability, TestContext.Current.CancellationToken);
        Assert.Equal(LockResponseType.LockDoesNotExist, response);

        (response, fencingToken) = await kahuna1.LocateAndTryExtendLock(lockName, ownerA, 1000, durability, TestContext.Current.CancellationToken);
        Assert.Equal(LockResponseType.LockDoesNotExist, response);
        Assert.Equal(0, fencingToken);

        (response, ReadOnlyLockContext? lockContext) = await kahuna1.LocateAndGetLock(lockName, durability, TestContext.Current.CancellationToken);
        Assert.Equal(LockResponseType.LockDoesNotExist, response);
        //Assert.Null(lockContext);

        await node1.LeaveCluster(true);
        await node2.LeaveCluster(true);
        await node3.LeaveCluster(true);
    }

    [Theory, CombinatorialData]
    public async Task TestLockOperationsOutOfOrder(
        [CombinatorialValues("memory")] string storage,
        [CombinatorialValues(8)] int partitions,
        [CombinatorialValues(LockDurability.Ephemeral, LockDurability.Persistent)] LockDurability durability
    )
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) = await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        string lockName = GetRandomLockName();
        byte[] ownerA = Encoding.UTF8.GetBytes(GetRandomLockName());
        byte[] ownerB = Encoding.UTF8.GetBytes(GetRandomLockName());

        // Try to extend non-existent lock
        (LockResponseType response, long fencingToken) = await kahuna1.LocateAndTryExtendLock(lockName, ownerA, 1000, durability, TestContext.Current.CancellationToken);
        Assert.Equal(LockResponseType.LockDoesNotExist, response);
        Assert.Equal(0, fencingToken);

        // Try to unlock non-existent lock
        response = await kahuna1.LocateAndTryUnlock(lockName, ownerA, durability, TestContext.Current.CancellationToken);
        Assert.Equal(LockResponseType.LockDoesNotExist, response);

        // Acquire lock with ownerA
        (response, fencingToken) = await kahuna1.LocateAndTryLock(lockName, ownerA, 1000, durability, TestContext.Current.CancellationToken);
        Assert.Equal(LockResponseType.Locked, response);
        Assert.Equal(0, fencingToken);

        // Try to extend with different owner
        (response, fencingToken) = await kahuna2.LocateAndTryExtendLock(lockName, ownerB, 1000, durability, TestContext.Current.CancellationToken);
        Assert.Equal(LockResponseType.InvalidOwner, response);
        Assert.Equal(0, fencingToken);

        // Try to unlock with different owner
        response = await kahuna2.LocateAndTryUnlock(lockName, ownerB, durability, TestContext.Current.CancellationToken);
        Assert.Equal(LockResponseType.InvalidOwner, response);

        // Unlock with correct owner
        response = await kahuna1.LocateAndTryUnlock(lockName, ownerA, durability, TestContext.Current.CancellationToken);
        Assert.Equal(LockResponseType.Unlocked, response);

        // Try to extend after unlock
        (response, fencingToken) = await kahuna1.LocateAndTryExtendLock(lockName, ownerA, 1000, durability, TestContext.Current.CancellationToken);
        Assert.Equal(LockResponseType.LockDoesNotExist, response);
        Assert.Equal(0, fencingToken);

        await node1.LeaveCluster(true);
        await node2.LeaveCluster(true);
        await node3.LeaveCluster(true);
    }

    [Theory, CombinatorialData]
    public async Task TestLockConcurrentOperations(
        [CombinatorialValues("memory")] string storage,
        [CombinatorialValues(8)] int partitions,
        [CombinatorialValues(LockDurability.Ephemeral, LockDurability.Persistent)] LockDurability durability
    )
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) = await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        string lockName = GetRandomLockName();
        byte[] ownerA = Encoding.UTF8.GetBytes(GetRandomLockName());
        byte[] ownerB = Encoding.UTF8.GetBytes(GetRandomLockName());
        byte[] ownerC = Encoding.UTF8.GetBytes(GetRandomLockName());

        // Start multiple concurrent lock attempts
        Task<(LockResponseType, long)> task1 = kahuna1.LocateAndTryLock(lockName, ownerA, 1000, durability, TestContext.Current.CancellationToken);
        Task<(LockResponseType, long)> task2 = kahuna2.LocateAndTryLock(lockName, ownerB, 1000, durability, TestContext.Current.CancellationToken);
        Task<(LockResponseType, long)> task3 = kahuna3.LocateAndTryLock(lockName, ownerC, 1000, durability, TestContext.Current.CancellationToken);

        (LockResponseType, long)[] results = await Task.WhenAll(task1, task2, task3);

        // One should succeed, others should be busy
        int lockedCount = results.Count(r => r.Item1 == LockResponseType.Locked);
        int busyCount = results.Count(r => r.Item1 == LockResponseType.Busy);

        Assert.Equal(1, lockedCount);
        Assert.Equal(2, busyCount);

        // Try to extend lock from all nodes concurrently
        Task<(LockResponseType, long)> extendTask1 = kahuna1.LocateAndTryExtendLock(lockName, ownerA, 1000, durability, TestContext.Current.CancellationToken);
        Task<(LockResponseType, long)> extendTask2 = kahuna2.LocateAndTryExtendLock(lockName, ownerB, 1000, durability, TestContext.Current.CancellationToken);
        Task<(LockResponseType, long)> extendTask3 = kahuna3.LocateAndTryExtendLock(lockName, ownerC, 1000, durability, TestContext.Current.CancellationToken);

        (LockResponseType, long)[] extendResults = await Task.WhenAll(extendTask1, extendTask2, extendTask3);

        // Only one should succeed (the owner), others should be not owner
        int extendedCount = extendResults.Count(r => r.Item1 == LockResponseType.Extended);
        int notOwnerCount = extendResults.Count(r => r.Item1 == LockResponseType.InvalidOwner);

        Assert.Equal(1, extendedCount);
        Assert.Equal(2, notOwnerCount);

        await node1.LeaveCluster(true);
        await node2.LeaveCluster(true);
        await node3.LeaveCluster(true);
    }
}