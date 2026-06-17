
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Tests.Server;

/// <summary>
/// Acceptance tests for the RangeLockMode enum + S/X compatibility matrix.
///
/// These tests use a 1-partition in-memory cluster (hash-space, no range-map setup needed)
/// and drive the handler via <see cref="KahunaManager.TryAcquireRangeLock"/>.
/// All locks use the prefix "t:rlm" so they land on the same partition actor.
/// </summary>
[Collection("ClusterTests")]
public sealed class TestRangeLockModes : BaseCluster
{
    private const string Prefix    = "t:rlm";
    private const string StartKey  = "10";
    private const string MidKey    = "25";
    private const string EndKey    = "50";
    private const string OuterKey  = "99";
    private const int    ExpiresMs = 30_000;

    private readonly ILogger<IRaft>    raftLogger;
    private readonly ILogger<IKahuna>  kahunaLogger;

    public TestRangeLockModes(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = LoggerFactory.Create(builder =>
            builder.AddXUnit(outputHelper).SetMinimumLevel(LogLevel.Warning));

        raftLogger   = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    // ── helpers ──────────────────────────────────────────────────────────────────

    private static HLCTimestamp NextTx(IRaft raft) =>
        raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

    private static async Task<KahunaManager> LeaderNode(IRaft r1, IRaft r2, IRaft r3, IKahuna k1, IKahuna k2, IKahuna k3)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        while (true)
        {
            if (await r1.AmILeader(1, ct)) return (KahunaManager)k1;
            if (await r2.AmILeader(1, ct)) return (KahunaManager)k2;
            if (await r3.AmILeader(1, ct)) return (KahunaManager)k3;
            await Task.Delay(50, ct);
        }
    }

    // ── tests ─────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Two different txs each acquire a Shared lock over the same overlapping range.
    /// Both must succeed — S∩S is compatible.
    /// </summary>
    [Fact]
    public async Task SharedSharedOverlapping_BothAcquire()
    {
        (IRaft r1, IRaft r2, IRaft r3, IKahuna k1, IKahuna k2, IKahuna k3) =
            await AssembleThreNodeCluster("memory", 1, raftLogger, kahunaLogger);
        try
        {
            KahunaManager leader = await LeaderNode(r1, r2, r3, k1, k2, k3);
            IRaft leaderRaft = await r1.AmILeader(1, TestContext.Current.CancellationToken) ? r1
                             : await r2.AmILeader(1, TestContext.Current.CancellationToken) ? r2 : r3;

            HLCTimestamp tx1 = NextTx(leaderRaft);
            HLCTimestamp tx2 = NextTx(leaderRaft);

            (KeyValueResponseType r, _) = await leader.TryAcquireRangeLock(
                tx1, Prefix, StartKey, true, EndKey, false, ExpiresMs,
                KeyValueDurability.Persistent, RangeLockMode.Shared);
            Assert.Equal(KeyValueResponseType.Locked, r);

            (KeyValueResponseType r2Result, _) = await leader.TryAcquireRangeLock(
                tx2, Prefix, StartKey, true, EndKey, false, ExpiresMs,
                KeyValueDurability.Persistent, RangeLockMode.Shared);
            Assert.Equal(KeyValueResponseType.Locked, r2Result);
        }
        finally
        {
            await LeaveCluster(r1, r2, r3);
        }
    }

    /// <summary>
    /// Held Shared, requested Exclusive (other tx) → AlreadyLocked.
    /// Then held Exclusive, requested Shared (other tx) → AlreadyLocked (reverse order).
    /// </summary>
    [Fact]
    public async Task SharedExclusiveOverlapping_Conflicts()
    {
        (IRaft r1, IRaft r2, IRaft r3, IKahuna k1, IKahuna k2, IKahuna k3) =
            await AssembleThreNodeCluster("memory", 1, raftLogger, kahunaLogger);
        try
        {
            KahunaManager leader = await LeaderNode(r1, r2, r3, k1, k2, k3);
            IRaft leaderRaft = await r1.AmILeader(1, TestContext.Current.CancellationToken) ? r1
                             : await r2.AmILeader(1, TestContext.Current.CancellationToken) ? r2 : r3;

            // S held, X requested by other tx → conflict
            HLCTimestamp txS = NextTx(leaderRaft);
            HLCTimestamp txX = NextTx(leaderRaft);

            (KeyValueResponseType sharedResult, _) = await leader.TryAcquireRangeLock(
                txS, Prefix, StartKey, true, EndKey, false, ExpiresMs,
                KeyValueDurability.Persistent, RangeLockMode.Shared);
            Assert.Equal(KeyValueResponseType.Locked, sharedResult);

            (KeyValueResponseType exclusiveResult, _) = await leader.TryAcquireRangeLock(
                txX, Prefix, StartKey, true, EndKey, false, ExpiresMs,
                KeyValueDurability.Persistent, RangeLockMode.Exclusive);
            Assert.Equal(KeyValueResponseType.AlreadyLocked, exclusiveResult);

            // X held, S requested by other tx → conflict (different prefix to avoid prior state)
            string prefix2 = Prefix + "2";
            HLCTimestamp txX2 = NextTx(leaderRaft);
            HLCTimestamp txS2 = NextTx(leaderRaft);

            (KeyValueResponseType exclusiveFirst, _) = await leader.TryAcquireRangeLock(
                txX2, prefix2, StartKey, true, EndKey, false, ExpiresMs,
                KeyValueDurability.Persistent, RangeLockMode.Exclusive);
            Assert.Equal(KeyValueResponseType.Locked, exclusiveFirst);

            (KeyValueResponseType sharedSecond, _) = await leader.TryAcquireRangeLock(
                txS2, prefix2, StartKey, true, EndKey, false, ExpiresMs,
                KeyValueDurability.Persistent, RangeLockMode.Shared);
            Assert.Equal(KeyValueResponseType.AlreadyLocked, sharedSecond);
        }
        finally
        {
            await LeaveCluster(r1, r2, r3);
        }
    }

    /// <summary>
    /// X∩X: second exclusive from a different tx over the same range → AlreadyLocked.
    /// Unchanged behavior from before range-lock modes existed.
    /// </summary>
    [Fact]
    public async Task ExclusiveExclusiveOverlapping_Conflicts()
    {
        (IRaft r1, IRaft r2, IRaft r3, IKahuna k1, IKahuna k2, IKahuna k3) =
            await AssembleThreNodeCluster("memory", 1, raftLogger, kahunaLogger);
        try
        {
            KahunaManager leader = await LeaderNode(r1, r2, r3, k1, k2, k3);
            IRaft leaderRaft = await r1.AmILeader(1, TestContext.Current.CancellationToken) ? r1
                             : await r2.AmILeader(1, TestContext.Current.CancellationToken) ? r2 : r3;

            HLCTimestamp tx1 = NextTx(leaderRaft);
            HLCTimestamp tx2 = NextTx(leaderRaft);

            (KeyValueResponseType first, _) = await leader.TryAcquireRangeLock(
                tx1, Prefix, StartKey, true, EndKey, false, ExpiresMs,
                KeyValueDurability.Persistent, RangeLockMode.Exclusive);
            Assert.Equal(KeyValueResponseType.Locked, first);

            (KeyValueResponseType second, _) = await leader.TryAcquireRangeLock(
                tx2, Prefix, StartKey, true, EndKey, false, ExpiresMs,
                KeyValueDurability.Persistent, RangeLockMode.Exclusive);
            Assert.Equal(KeyValueResponseType.AlreadyLocked, second);
        }
        finally
        {
            await LeaveCluster(r1, r2, r3);
        }
    }

    /// <summary>
    /// Disjoint ranges [10,50) and [50,99] with any mode pairing → both Locked.
    /// </summary>
    [Fact]
    public async Task NonOverlapping_AnyMode_BothAcquire()
    {
        (IRaft r1, IRaft r2, IRaft r3, IKahuna k1, IKahuna k2, IKahuna k3) =
            await AssembleThreNodeCluster("memory", 1, raftLogger, kahunaLogger);
        try
        {
            KahunaManager leader = await LeaderNode(r1, r2, r3, k1, k2, k3);
            IRaft leaderRaft = await r1.AmILeader(1, TestContext.Current.CancellationToken) ? r1
                             : await r2.AmILeader(1, TestContext.Current.CancellationToken) ? r2 : r3;

            HLCTimestamp tx1 = NextTx(leaderRaft);
            HLCTimestamp tx2 = NextTx(leaderRaft);

            // tx1 takes Exclusive [10, 50)
            (KeyValueResponseType left, _) = await leader.TryAcquireRangeLock(
                tx1, Prefix, StartKey, true, EndKey, false, ExpiresMs,
                KeyValueDurability.Persistent, RangeLockMode.Exclusive);
            Assert.Equal(KeyValueResponseType.Locked, left);

            // tx2 takes Shared [50, 99] — disjoint → must succeed
            (KeyValueResponseType right, _) = await leader.TryAcquireRangeLock(
                tx2, Prefix, EndKey, true, OuterKey, true, ExpiresMs,
                KeyValueDurability.Persistent, RangeLockMode.Shared);
            Assert.Equal(KeyValueResponseType.Locked, right);
        }
        finally
        {
            await LeaveCluster(r1, r2, r3);
        }
    }

    /// <summary>
    /// tx1 S[10,50]; tx2 S[10,50]; tx1 tries X[10,50] → AlreadyLocked.
    /// The upgrade must pass the conflict gate — tx2's Shared lock is still live.
    /// </summary>
    [Fact]
    public async Task Upgrade_BlockedByOtherShared()
    {
        (IRaft r1, IRaft r2, IRaft r3, IKahuna k1, IKahuna k2, IKahuna k3) =
            await AssembleThreNodeCluster("memory", 1, raftLogger, kahunaLogger);
        try
        {
            KahunaManager leader = await LeaderNode(r1, r2, r3, k1, k2, k3);
            IRaft leaderRaft = await r1.AmILeader(1, TestContext.Current.CancellationToken) ? r1
                             : await r2.AmILeader(1, TestContext.Current.CancellationToken) ? r2 : r3;

            HLCTimestamp tx1 = NextTx(leaderRaft);
            HLCTimestamp tx2 = NextTx(leaderRaft);

            (KeyValueResponseType _lockTx1, _) = await leader.TryAcquireRangeLock(
                tx1, Prefix, StartKey, true, EndKey, false, ExpiresMs,
                KeyValueDurability.Persistent, RangeLockMode.Shared);
            Assert.Equal(KeyValueResponseType.Locked, _lockTx1);

            (KeyValueResponseType _lockTx2, _) = await leader.TryAcquireRangeLock(
                tx2, Prefix, StartKey, true, EndKey, false, ExpiresMs,
                KeyValueDurability.Persistent, RangeLockMode.Shared);
            Assert.Equal(KeyValueResponseType.Locked, _lockTx2);

            // tx1 tries to upgrade to X — tx2's Shared is still live → must be rejected
            (KeyValueResponseType _upgradeResult, _) = await leader.TryAcquireRangeLock(
                tx1, Prefix, StartKey, true, EndKey, false, ExpiresMs,
                KeyValueDurability.Persistent, RangeLockMode.Exclusive);
            Assert.Equal(KeyValueResponseType.AlreadyLocked, _upgradeResult);
        }
        finally
        {
            await LeaveCluster(r1, r2, r3);
        }
    }

    /// <summary>
    /// Same tx re-acquires identical bounds → Locked (idempotent).
    /// Same tx re-acquires with Exclusive when it holds Shared (S→X upgrade) → Locked and
    /// a subsequent Shared from a DIFFERENT tx is blocked (proves mode became Exclusive).
    /// </summary>
    [Fact]
    public async Task SameTx_Reentrant_AndUpgrade()
    {
        (IRaft r1, IRaft r2, IRaft r3, IKahuna k1, IKahuna k2, IKahuna k3) =
            await AssembleThreNodeCluster("memory", 1, raftLogger, kahunaLogger);
        try
        {
            KahunaManager leader = await LeaderNode(r1, r2, r3, k1, k2, k3);
            IRaft leaderRaft = await r1.AmILeader(1, TestContext.Current.CancellationToken) ? r1
                             : await r2.AmILeader(1, TestContext.Current.CancellationToken) ? r2 : r3;

            HLCTimestamp tx1 = NextTx(leaderRaft);

            // Acquire Shared
            (KeyValueResponseType first, _) = await leader.TryAcquireRangeLock(
                tx1, Prefix, StartKey, true, EndKey, false, ExpiresMs,
                KeyValueDurability.Persistent, RangeLockMode.Shared);
            Assert.Equal(KeyValueResponseType.Locked, first);

            // Re-acquire same bounds same tx → idempotent
            (KeyValueResponseType reentry, _) = await leader.TryAcquireRangeLock(
                tx1, Prefix, StartKey, true, EndKey, false, ExpiresMs,
                KeyValueDurability.Persistent, RangeLockMode.Shared);
            Assert.Equal(KeyValueResponseType.Locked, reentry);

            // S→X upgrade same tx, same bounds → Locked
            (KeyValueResponseType upgrade, _) = await leader.TryAcquireRangeLock(
                tx1, Prefix, StartKey, true, EndKey, false, ExpiresMs,
                KeyValueDurability.Persistent, RangeLockMode.Exclusive);
            Assert.Equal(KeyValueResponseType.Locked, upgrade);

            // After upgrade, a different tx requesting Shared on the same range must be blocked
            HLCTimestamp tx2 = NextTx(leaderRaft);
            (KeyValueResponseType blocked, _) = await leader.TryAcquireRangeLock(
                tx2, Prefix, StartKey, true, EndKey, false, ExpiresMs,
                KeyValueDurability.Persistent, RangeLockMode.Shared);
            Assert.Equal(KeyValueResponseType.AlreadyLocked, blocked);
        }
        finally
        {
            await LeaveCluster(r1, r2, r3);
        }
    }
}
