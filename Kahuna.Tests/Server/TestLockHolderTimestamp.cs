using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Tests.Server;

/// <summary>
/// Acceptance tests for HolderTransactionId surfacing on denied lock acquire responses.
/// </summary>
[Collection("ClusterTests")]
public sealed class TestLockHolderTimestamp : BaseCluster
{
    private const string Prefix    = "t:lht";
    private const string StartKey  = "a";
    private const string EndKey    = "z";
    private const int    ExpiresMs = 30_000;

    private readonly ILogger<IRaft>   raftLogger;
    private readonly ILogger<IKahuna> kahunaLogger;

    public TestLockHolderTimestamp(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = LoggerFactory.Create(builder =>
            builder.AddXUnit(outputHelper).SetMinimumLevel(LogLevel.Warning));

        raftLogger   = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    private static HLCTimestamp NextTx(IRaft raft) =>
        raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

    private static async Task<(IRaft raft, KahunaManager leader)> GetLeader(IRaft r1, IRaft r2, IRaft r3, IKahuna k1, IKahuna k2, IKahuna k3)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        while (true)
        {
            if (await r1.AmILeader(1, ct)) return (r1, (KahunaManager)k1);
            if (await r2.AmILeader(1, ct)) return (r2, (KahunaManager)k2);
            if (await r3.AmILeader(1, ct)) return (r3, (KahunaManager)k3);
            await Task.Delay(50, ct);
        }
    }

    /// <summary>
    /// X vs X on the same range — the denied response carries the first holder's transaction id.
    /// </summary>
    [Fact]
    public async Task RangeLock_XvsX_DeniedCarriesHolder()
    {
        (IRaft r1, IRaft r2, IRaft r3, IKahuna k1, IKahuna k2, IKahuna k3) =
            await AssembleThreNodeCluster("memory", 1, raftLogger, kahunaLogger);
        try
        {
            (IRaft leaderRaft, KahunaManager leader) = await GetLeader(r1, r2, r3, k1, k2, k3);

            HLCTimestamp tx1 = NextTx(leaderRaft);
            HLCTimestamp tx2 = NextTx(leaderRaft);

            (KeyValueResponseType r1Type, _) = await leader.TryAcquireRangeLock(
                tx1, Prefix, StartKey, true, EndKey, false, ExpiresMs,
                KeyValueDurability.Persistent, RangeLockMode.Exclusive);
            Assert.Equal(KeyValueResponseType.Locked, r1Type);

            (KeyValueResponseType r2Type, HLCTimestamp holder) = await leader.TryAcquireRangeLock(
                tx2, Prefix, StartKey, true, EndKey, false, ExpiresMs,
                KeyValueDurability.Persistent, RangeLockMode.Exclusive);
            Assert.Equal(KeyValueResponseType.AlreadyLocked, r2Type);
            Assert.Equal(tx1, holder);
        }
        finally
        {
            await LeaveCluster(r1, r2, r3);
        }
    }

    /// <summary>
    /// S→X upgrade blocked by a foreign Shared lock — denial carries the foreign holder's id.
    /// </summary>
    [Fact]
    public async Task RangeLock_UpgradeBlockedByForeignShared_CarriesHolder()
    {
        (IRaft r1, IRaft r2, IRaft r3, IKahuna k1, IKahuna k2, IKahuna k3) =
            await AssembleThreNodeCluster("memory", 1, raftLogger, kahunaLogger);
        try
        {
            (IRaft leaderRaft, KahunaManager leader) = await GetLeader(r1, r2, r3, k1, k2, k3);

            HLCTimestamp tx1 = NextTx(leaderRaft);
            HLCTimestamp tx2 = NextTx(leaderRaft);

            // tx1 holds Shared
            (KeyValueResponseType sType, _) = await leader.TryAcquireRangeLock(
                tx1, Prefix + "2", StartKey, true, EndKey, false, ExpiresMs,
                KeyValueDurability.Persistent, RangeLockMode.Shared);
            Assert.Equal(KeyValueResponseType.Locked, sType);

            // tx2 also holds Shared (S∩S is allowed)
            (KeyValueResponseType s2Type, _) = await leader.TryAcquireRangeLock(
                tx2, Prefix + "2", StartKey, true, EndKey, false, ExpiresMs,
                KeyValueDurability.Persistent, RangeLockMode.Shared);
            Assert.Equal(KeyValueResponseType.Locked, s2Type);

            // tx1 tries to upgrade S→X: blocked by tx2's Shared; denial must name tx2
            (KeyValueResponseType upgradeType, HLCTimestamp holder) = await leader.TryAcquireRangeLock(
                tx1, Prefix + "2", StartKey, true, EndKey, false, ExpiresMs,
                KeyValueDurability.Persistent, RangeLockMode.Exclusive);
            Assert.Equal(KeyValueResponseType.AlreadyLocked, upgradeType);
            Assert.Equal(tx2, holder);
        }
        finally
        {
            await LeaveCluster(r1, r2, r3);
        }
    }

    /// <summary>
    /// Point (exclusive) lock contention — denial carries the write-intent owner's transaction id.
    /// </summary>
    [Fact]
    public async Task ExclusiveLock_Contention_DeniedCarriesHolder()
    {
        (IRaft r1, IRaft r2, IRaft r3, IKahuna k1, IKahuna k2, IKahuna k3) =
            await AssembleThreNodeCluster("memory", 1, raftLogger, kahunaLogger);
        try
        {
            (IRaft leaderRaft, KahunaManager leader) = await GetLeader(r1, r2, r3, k1, k2, k3);

            HLCTimestamp tx1 = NextTx(leaderRaft);
            HLCTimestamp tx2 = NextTx(leaderRaft);

            (KeyValueResponseType r1Type, _, _, _) = await leader.LocateAndTryAcquireExclusiveLock(
                tx1, Prefix + ":key1", ExpiresMs, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.Locked, r1Type);

            (KeyValueResponseType r2Type, _, _, HLCTimestamp holder) = await leader.LocateAndTryAcquireExclusiveLock(
                tx2, Prefix + ":key1", ExpiresMs, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.AlreadyLocked, r2Type);
            Assert.Equal(tx1, holder);
        }
        finally
        {
            await LeaveCluster(r1, r2, r3);
        }
    }

    /// <summary>
    /// Clean acquire returns Zero holder; WaitingForReplication (no contention) also returns Zero.
    /// </summary>
    [Fact]
    public async Task CleanAcquire_HolderIsZero()
    {
        (IRaft r1, IRaft r2, IRaft r3, IKahuna k1, IKahuna k2, IKahuna k3) =
            await AssembleThreNodeCluster("memory", 1, raftLogger, kahunaLogger);
        try
        {
            (IRaft leaderRaft, KahunaManager leader) = await GetLeader(r1, r2, r3, k1, k2, k3);

            HLCTimestamp tx1 = NextTx(leaderRaft);

            (KeyValueResponseType rType, HLCTimestamp holder) = await leader.TryAcquireRangeLock(
                tx1, Prefix + "3", StartKey, true, EndKey, false, ExpiresMs,
                KeyValueDurability.Persistent, RangeLockMode.Exclusive);
            Assert.Equal(KeyValueResponseType.Locked, rType);
            Assert.Equal(HLCTimestamp.Zero, holder);
        }
        finally
        {
            await LeaveCluster(r1, r2, r3);
        }
    }

    /// <summary>
    /// Remote partition path: holder survives the inter-node routing round-trip via MemoryInterNodeCommunication.
    /// The second acquire is sent through a confirmed non-leader node so the request is guaranteed
    /// to be forwarded to the partition leader rather than served locally, exercising the inter-node path.
    /// </summary>
    [Fact]
    public async Task RangeLock_RemoteRouting_HolderSurvivesRoundTrip()
    {
        (IRaft r1, IRaft r2, IRaft r3, IKahuna k1, IKahuna k2, IKahuna k3) =
            await AssembleThreNodeCluster("memory", 3, raftLogger, kahunaLogger);
        try
        {
            CancellationToken ct = TestContext.Current.CancellationToken;
            const string prefix = Prefix + "4";

            (IRaft, IKahuna)[] allNodes = [(r1, k1), (r2, k2), (r3, k3)];

            HLCTimestamp tx1 = r1.HybridLogicalClock.TrySendOrLocalEvent(r1.GetLocalNodeId());
            HLCTimestamp tx2 = r1.HybridLogicalClock.TrySendOrLocalEvent(r1.GetLocalNodeId());

            // tx1 acquires Exclusive via k1 (any node — the locator forwards to the leader).
            (KeyValueResponseType t1, _) = await k1.LocateAndTryAcquireExclusiveRangeLock(
                tx1, prefix, StartKey, true, EndKey, false, ExpiresMs,
                KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Locked, t1);

            // Determine which partition the prefix hashes to (prefix + "/" mirrors RoutePrefixKey).
            int partitionId = ((KahunaManager)k1).GetDataPartitionForKey(prefix + "/");

            // Find a confirmed non-leader node so the second request must cross inter-node.
            IKahuna? nonLeader = null;
            foreach ((IRaft raft, IKahuna kahuna) in allNodes)
            {
                if (!await raft.AmILeader(partitionId, ct))
                {
                    nonLeader = kahuna;
                    break;
                }
            }
            Assert.NotNull(nonLeader);

            (KeyValueResponseType t2, HLCTimestamp holder) = await nonLeader.LocateAndTryAcquireExclusiveRangeLock(
                tx2, prefix, StartKey, true, EndKey, false, ExpiresMs,
                KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.AlreadyLocked, t2);
            Assert.Equal(tx1, holder);
        }
        finally
        {
            await LeaveCluster(r1, r2, r3);
        }
    }
}
