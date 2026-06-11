using System.Text;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.Locks;
using Kahuna.Server.Locks.Data;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Time;
using Kahuna.Shared.Locks;
using Microsoft.Extensions.Logging;

namespace Kahuna.Tests.Server;

public class TestLocks : BaseCluster
{
    private readonly ILogger<IRaft> raftLogger;

    private readonly ILogger<IKahuna> kahunaLogger;

    public TestLocks(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = TestLogFactory.Create(outputHelper);

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
        [CombinatorialValues(8)] int partitions,
        [CombinatorialValues(LockDurability.Ephemeral, LockDurability.Persistent)] LockDurability durability
        //[CombinatorialValues("memory")] string storage,
        //[CombinatorialValues(16)] int partitions,
        //[CombinatorialValues(LockDurability.Ephemeral)] LockDurability durability
    )
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) = await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
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

            (response, ReadOnlyLockEntry? lockContext) = await kahuna1.LocateAndGetLock(lockName, durability, TestContext.Current.CancellationToken);
            Assert.Equal(LockResponseType.Got, response);
            Assert.NotNull(lockContext);

            response = await kahuna1.LocateAndTryUnlock(lockName, ownerA, durability, TestContext.Current.CancellationToken);
            Assert.Equal(LockResponseType.Unlocked, response);

        }
        finally
        {
            await node1.LeaveCluster(true);
            await node2.LeaveCluster(true);
            await node3.LeaveCluster(true);
        }
    }

    [Theory, CombinatorialData]
    public async Task TestLockOperationsAcrossNodes(
        [CombinatorialValues("memory")] string storage,
        [CombinatorialValues(8)] int partitions,
        [CombinatorialValues(LockDurability.Ephemeral, LockDurability.Persistent)] LockDurability durability
    )
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) = await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
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
            (response, ReadOnlyLockEntry? lockContext) = await kahuna1.LocateAndGetLock(lockName, durability, TestContext.Current.CancellationToken);
            Assert.Equal(LockResponseType.Got, response);
            Assert.NotNull(lockContext);

        }
        finally
        {
            await node1.LeaveCluster(true);
            await node2.LeaveCluster(true);
            await node3.LeaveCluster(true);
        }
    }

    [Theory, CombinatorialData]
    public async Task TestLockEdgeCases(
        [CombinatorialValues("memory")] string storage,
        [CombinatorialValues(8)] int partitions,
        [CombinatorialValues(LockDurability.Ephemeral, LockDurability.Persistent)] LockDurability durability
    )
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) = await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
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

            (response, ReadOnlyLockEntry? lockContext) = await kahuna1.LocateAndGetLock(lockName, durability, TestContext.Current.CancellationToken);
            Assert.Equal(LockResponseType.LockDoesNotExist, response);
            //Assert.Null(lockContext);

        }
        finally
        {
            await node1.LeaveCluster(true);
            await node2.LeaveCluster(true);
            await node3.LeaveCluster(true);
        }
    }

    [Theory, CombinatorialData]
    public async Task TestLockOperationsOutOfOrder(
        [CombinatorialValues("memory")] string storage,
        [CombinatorialValues(8)] int partitions,
        [CombinatorialValues(LockDurability.Ephemeral, LockDurability.Persistent)] LockDurability durability
    )
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) = await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
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

        }
        finally
        {
            await node1.LeaveCluster(true);
            await node2.LeaveCluster(true);
            await node3.LeaveCluster(true);
        }
    }

    [Theory, CombinatorialData]
    public async Task TestLockConcurrentOperations(
        [CombinatorialValues("memory")] string storage,
        [CombinatorialValues(8)] int partitions,
        [CombinatorialValues(LockDurability.Ephemeral, LockDurability.Persistent)] LockDurability durability
    )
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) = await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            string lockName = GetRandomLockName();
            byte[] ownerA = Encoding.UTF8.GetBytes(GetRandomLockName());
            byte[] ownerB = Encoding.UTF8.GetBytes(GetRandomLockName());
            byte[] ownerC = Encoding.UTF8.GetBytes(GetRandomLockName());

            // Start multiple concurrent lock attempts
            Task<(LockResponseType, long)> task1 = kahuna1.LocateAndTryLock(lockName, ownerA, 1000, durability, TestContext.Current.CancellationToken);
            Task<(LockResponseType, long)> task2 = kahuna2.LocateAndTryLock(lockName, ownerB, 1000, durability, TestContext.Current.CancellationToken);
            Task<(LockResponseType, long)> task3 = kahuna3.LocateAndTryLock(lockName, ownerC, 1000, durability, TestContext.Current.CancellationToken);

            (LockResponseType, long)[] results = await Task.WhenAll(task1, task2, task3);

            // One should succeed; the other two are rejected (Busy/MustRetry) or transiently
            // errored under high Raft concurrency — all are definitive non-grants.
            int lockedCount = results.Count(r => r.Item1 == LockResponseType.Locked);
            int rejectedCount = results.Count(r => r.Item1 != LockResponseType.Locked);

            Assert.Equal(1, lockedCount);
            Assert.Equal(2, rejectedCount);

            // Try to extend lock from all nodes concurrently
            Task<(LockResponseType, long)> extendTask1 = kahuna1.LocateAndTryExtendLock(lockName, ownerA, 1000, durability, TestContext.Current.CancellationToken);
            Task<(LockResponseType, long)> extendTask2 = kahuna2.LocateAndTryExtendLock(lockName, ownerB, 1000, durability, TestContext.Current.CancellationToken);
            Task<(LockResponseType, long)> extendTask3 = kahuna3.LocateAndTryExtendLock(lockName, ownerC, 1000, durability, TestContext.Current.CancellationToken);

            (LockResponseType, long)[] extendResults = await Task.WhenAll(extendTask1, extendTask2, extendTask3);

            // Only one should succeed. In the persistent path, concurrent non-owner
            // extensions can observe the owner's proposal in flight and return retryable statuses.
            int extendedCount = extendResults.Count(r => r.Item1 == LockResponseType.Extended);
            int notExtendedCount = extendResults.Count(r => r.Item1 != LockResponseType.Extended);

            Assert.Equal(1, extendedCount);
            Assert.Equal(2, notExtendedCount);

            byte[] winningOwner = results[0].Item1 == LockResponseType.Locked
                ? ownerA
                : results[1].Item1 == LockResponseType.Locked
                    ? ownerB
                    : ownerC;

            foreach (byte[] owner in new[] { ownerA, ownerB, ownerC }.Where(owner => !owner.SequenceEqual(winningOwner)))
            {
                (LockResponseType response, _) = await kahuna1.LocateAndTryExtendLock(lockName, owner, 1000, durability, TestContext.Current.CancellationToken);
                Assert.Equal(LockResponseType.InvalidOwner, response);
            }

        }
        finally
        {
            await node1.LeaveCluster(true);
            await node2.LeaveCluster(true);
            await node3.LeaveCluster(true);
        }
    }

    // ── Per-range lock tests (Task 11) ───────────────────────────────────────────

    private const string RlSpace = "t:rl";
    private const string RlSplit = RlSpace + "/m";

    private static async Task<(IRaft Raft, KahunaManager Kahuna)> RlLeaderOf(
        int partition, (IRaft, KahunaManager)[] nodes)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        while (true)
        {
            foreach ((IRaft raft, KahunaManager kahuna) in nodes)
                if (await raft.AmILeader(partition, ct))
                    return (raft, kahuna);
            await Task.Delay(50, ct);
        }
    }

    private static async Task RlWaitUntil(Func<bool> predicate, int timeoutMs = 8000)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        long deadline = Environment.TickCount64 + timeoutMs;
        while (Environment.TickCount64 < deadline)
        {
            if (predicate()) return;
            await Task.Delay(25, ct);
        }
        Assert.Fail("Timed out waiting for condition.");
    }

    /// <summary>
    /// 4-partition cluster with <see cref="RlSpace"/> seeded with two adjacent descriptors:
    /// <c>[−∞, t:rl/m)@P2</c> and <c>[t:rl/m, +∞)@P3</c>.
    /// Uses direct <c>MutateAsync</c> (same pattern as <see cref="TestRangeMerge"/>) —
    /// P2 and P3 already exist in a 4-partition cluster so no <c>CreatePartitionAsync</c> needed.
    /// </summary>
    private async Task<(IRaft, KahunaManager)[]> SetupSplitKeyspace()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft r1, IRaft r2, IRaft r3, IKahuna k1, IKahuna k2, IKahuna k3) =
            await AssembleThreNodeCluster("memory", 4, raftLogger, kahunaLogger);

        (IRaft, KahunaManager)[] nodes =
            [(r1, (KahunaManager)k1), (r2, (KahunaManager)k2), (r3, (KahunaManager)k3)];

        foreach ((IRaft _, KahunaManager kahuna) in nodes)
            kahuna.RegisterKeyRange(RlSpace);

        const int leftPid  = RangeMapStore.FirstDataPartitionId;     // P2
        const int rightPid = RangeMapStore.FirstDataPartitionId + 1; // P3

        (IRaft _, KahunaManager metaLeader) = await RlLeaderOf(RangeMapStore.MetaPartitionId, nodes);
        bool seeded = await metaLeader.RangeMapStore.MutateAsync(_ => [
            new RangeDescriptor { KeySpace = RlSpace, StartKey = null,    EndKey = RlSplit, PartitionId = leftPid,  Generation = 1 },
            new RangeDescriptor { KeySpace = RlSpace, StartKey = RlSplit, EndKey = null,    PartitionId = rightPid, Generation = 1 }
        ], ct);
        Assert.True(seeded);

        foreach ((IRaft _, KahunaManager kahuna) in nodes)
            await RlWaitUntil(() => kahuna.RangeMapStore.Current.FindAll(RlSpace).Count == 2);

        return nodes;
    }

    /// <summary>
    /// Core payoff: holding an exclusive lock on <c>[−∞, t:rl/m)</c> (left half) must NOT block
    /// a concurrent write to the right half <c>[t:rl/m, +∞)</c>.
    /// </summary>
    [Fact]
    public async Task RangeLock_DisjointRange_NotBlocked()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (IRaft, KahunaManager)[] nodes = await SetupSplitKeyspace();
        try
        {
            IKahuna node = nodes[0].Item2;

            HLCTimestamp tx1 = nodes[0].Item1.HybridLogicalClock.TrySendOrLocalEvent(nodes[0].Item1.GetLocalNodeId());

            // Acquire exclusive range lock on the LEFT half [−∞, t:rl/m) for TX1.
            KeyValueResponseType lockResult = await node.LocateAndTryAcquireExclusiveRangeLock(
                tx1, RlSpace, null, true, RlSplit, false, 30_000, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Locked, lockResult);

            // A DIFFERENT transaction writes to the RIGHT half — must succeed.
            HLCTimestamp tx2 = nodes[0].Item1.HybridLogicalClock.TrySendOrLocalEvent(nodes[0].Item1.GetLocalNodeId());
            (KeyValueResponseType writeResult, _, _) = await node.LocateAndTrySetKeyValue(
                tx2, RlSpace + "/z", Encoding.UTF8.GetBytes("v"), null, -1,
                KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);

            Assert.Equal(KeyValueResponseType.Set, writeResult);
        }
        finally
        {
            foreach ((IRaft raft, KahunaManager _) in nodes)
                await raft.LeaveCluster(true);
        }
    }

    /// <summary>
    /// Safety: a conflicting range lock on the SAME range by a different transaction must be
    /// rejected (no over-narrowing of the lock scope).
    /// </summary>
    [Fact]
    public async Task RangeLock_SameRange_StillContends()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (IRaft, KahunaManager)[] nodes = await SetupSplitKeyspace();
        try
        {
            IKahuna node = nodes[0].Item2;

            HLCTimestamp tx1 = nodes[0].Item1.HybridLogicalClock.TrySendOrLocalEvent(nodes[0].Item1.GetLocalNodeId());
            HLCTimestamp tx2 = nodes[0].Item1.HybridLogicalClock.TrySendOrLocalEvent(nodes[0].Item1.GetLocalNodeId());

            KeyValueResponseType first = await node.LocateAndTryAcquireExclusiveRangeLock(
                tx1, RlSpace, null, true, RlSplit, false, 30_000, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Locked, first);

            // TX2 tries the same range — must be blocked.
            KeyValueResponseType second = await node.LocateAndTryAcquireExclusiveRangeLock(
                tx2, RlSpace, null, true, RlSplit, false, 30_000, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.AlreadyLocked, second);
        }
        finally
        {
            foreach ((IRaft raft, KahunaManager _) in nodes)
                await raft.LeaveCluster(true);
        }
    }

    /// <summary>
    /// A full-range lock <c>[−∞, +∞)</c> spanning both descriptors acquires sub-locks on each
    /// partition; writes to BOTH halves are blocked, and both are unblocked after the lock is released.
    /// </summary>
    [Fact]
    public async Task RangeLock_ScopedToTouchedRanges()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (IRaft, KahunaManager)[] nodes = await SetupSplitKeyspace();
        try
        {
            IKahuna node = nodes[0].Item2;

            HLCTimestamp tx1 = nodes[0].Item1.HybridLogicalClock.TrySendOrLocalEvent(nodes[0].Item1.GetLocalNodeId());

            // Full-range lock spans both descriptors.
            KeyValueResponseType locked = await node.LocateAndTryAcquireExclusiveRangeLock(
                tx1, RlSpace, null, true, null, false, 30_000, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Locked, locked);

            // Writes from another transaction to BOTH halves must be blocked.
            HLCTimestamp tx2 = nodes[0].Item1.HybridLogicalClock.TrySendOrLocalEvent(nodes[0].Item1.GetLocalNodeId());

            (KeyValueResponseType leftWrite, _, _) = await node.LocateAndTrySetKeyValue(
                tx2, RlSpace + "/a", Encoding.UTF8.GetBytes("v"), null, -1,
                KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.MustRetry, leftWrite);

            (KeyValueResponseType rightWrite, _, _) = await node.LocateAndTrySetKeyValue(
                tx2, RlSpace + "/z", Encoding.UTF8.GetBytes("v"), null, -1,
                KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.MustRetry, rightWrite);

            // Release the full-range lock.
            KeyValueResponseType released = await node.LocateAndTryReleaseExclusiveRangeLock(
                tx1, RlSpace, null, true, null, false, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Unlocked, released);

            // After release, both halves must be writable again.
            (KeyValueResponseType leftAfter, _, _) = await node.LocateAndTrySetKeyValue(
                tx2, RlSpace + "/a", Encoding.UTF8.GetBytes("v"), null, -1,
                KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Set, leftAfter);

            (KeyValueResponseType rightAfter, _, _) = await node.LocateAndTrySetKeyValue(
                tx2, RlSpace + "/z", Encoding.UTF8.GetBytes("v"), null, -1,
                KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Set, rightAfter);
        }
        finally
        {
            foreach ((IRaft raft, KahunaManager _) in nodes)
                await raft.LeaveCluster(true);
        }
    }

    /// <summary>
    /// F2: a split that commits in the window between <c>FindIntersecting</c> and the sub-lock RPC
    /// is detected by the post-acquire generation fence. The acquire returns <c>MustRetry</c> and
    /// releases all sub-locks, proven by a follow-up acquire on the now-split map succeeding.
    /// </summary>
    [Fact]
    public async Task RangeLock_SplitDuringAcquire_FencesAndRetries()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft r1, IRaft r2, IRaft r3, IKahuna k1, IKahuna k2, IKahuna k3) =
            await AssembleThreNodeCluster("memory", 4, raftLogger, kahunaLogger);

        (IRaft, KahunaManager)[] nodes =
            [(r1, (KahunaManager)k1), (r2, (KahunaManager)k2), (r3, (KahunaManager)k3)];

        try
        {
            foreach ((IRaft _, KahunaManager kahuna) in nodes)
                kahuna.RegisterKeyRange(RlSpace);

            // Start with the whole key space as a single descriptor on P2.
            const int singlePid = RangeMapStore.FirstDataPartitionId; // P2
            const int rightPid  = RangeMapStore.FirstDataPartitionId + 1; // P3

            (IRaft _, KahunaManager metaLeader) = await RlLeaderOf(RangeMapStore.MetaPartitionId, nodes);
            bool seeded = await metaLeader.RangeMapStore.MutateAsync(_ => [
                new RangeDescriptor { KeySpace = RlSpace, StartKey = null, EndKey = null, PartitionId = singlePid, Generation = 1 }
            ], ct);
            Assert.True(seeded);

            foreach ((IRaft _, KahunaManager kahuna) in nodes)
                await RlWaitUntil(() => kahuna.RangeMapStore.Current.FindAll(RlSpace).Count == 1);

            KahunaManager node = nodes[0].Item2;
            HLCTimestamp tx1 = nodes[0].Item1.HybridLogicalClock.TrySendOrLocalEvent(nodes[0].Item1.GetLocalNodeId());

            // Inject a split into the window between FindIntersecting and AcquireRangeLockOnPartition.
            // The hook commits a split so the post-acquire fence sees a changed descriptor set.
            KeyValueResponseType fenced = await node.AcquireExclusiveRangeLockWithHook(
                tx1, RlSpace, null, true, null, false, 30_000, KeyValueDurability.Persistent,
                afterSnapshot: async () =>
                {
                    bool split = await metaLeader.RangeMapStore.MutateAsync(_ => [
                        new RangeDescriptor { KeySpace = RlSpace, StartKey = null,    EndKey = RlSplit, PartitionId = singlePid, Generation = 2 },
                        new RangeDescriptor { KeySpace = RlSpace, StartKey = RlSplit, EndKey = null,    PartitionId = rightPid,  Generation = 2 },
                    ], ct);
                    Assert.True(split);

                    // Wait for the new map to propagate to this node before the fence re-checks.
                    await RlWaitUntil(() => node.RangeMapStore.Current.FindAll(RlSpace).Count == 2);
                },
                cancellationToken: ct);

            Assert.Equal(KeyValueResponseType.MustRetry, fenced);

            // Prove no sub-lock was left held: a follow-up acquire on the same transaction
            // with the now-split map must succeed (AlreadyLocked would mean a leaked sub-lock).
            KeyValueResponseType retry = await node.LocateAndTryAcquireExclusiveRangeLock(
                tx1, RlSpace, null, true, null, false, 30_000, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Locked, retry);
        }
        finally
        {
            foreach ((IRaft raft, KahunaManager _) in nodes)
                await raft.LeaveCluster(true);
        }
    }

    /// <summary>
    /// Deprecation (F4): the legacy exclusive <b>prefix</b> lock is unsupported on a key-range-split
    /// space and must return the typed <see cref="KeyValueResponseType.PrefixLockUnsupportedOnRangedSpace"/>
    /// — not a generic <c>Errored</c> — so callers migrate to the range lock deliberately. Both
    /// acquire and release surface the typed response.
    /// </summary>
    [Fact]
    public async Task PrefixLock_OnSplitSpace_ReturnsTypedUnsupported()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (IRaft, KahunaManager)[] nodes = await SetupSplitKeyspace();
        try
        {
            IKahuna node = nodes[0].Item2;
            HLCTimestamp tx = nodes[0].Item1.HybridLogicalClock.TrySendOrLocalEvent(nodes[0].Item1.GetLocalNodeId());

            KeyValueResponseType acquire = await node.LocateAndTryAcquireExclusivePrefixLock(
                tx, RlSpace, 30_000, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.PrefixLockUnsupportedOnRangedSpace, acquire);

            KeyValueResponseType release = await node.LocateAndTryReleaseExclusivePrefixLock(
                tx, RlSpace, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.PrefixLockUnsupportedOnRangedSpace, release);

            // The range lock IS supported on the same space (the designed replacement).
            KeyValueResponseType rangeLock = await node.LocateAndTryAcquireExclusiveRangeLock(
                tx, RlSpace, null, true, null, false, 30_000, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Locked, rangeLock);
        }
        finally
        {
            foreach ((IRaft raft, KahunaManager _) in nodes)
                await raft.LeaveCluster(true);
        }
    }
}
