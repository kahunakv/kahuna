
using System.Text;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Tests.Server;

/// <summary>
/// Acceptance tests for write-path phantom enforcement for Delete and Prepare.
/// Set is already gated (regression guard). All tests use a 1-partition in-memory
/// cluster and drive handlers directly on the leader via <see cref="KahunaManager"/>.
///
/// Convention: prefix "t:rwf"; range lock [t:rwf/10, t:rwf/50); inside key t:rwf/25;
/// outside key t:rwf/99. Ordinal comparison ensures the string positions are correct.
/// </summary>
[Collection("ClusterTests")]
public sealed class TestRangeLockWriteFence : BaseCluster
{
    private const string Prefix   = "t:rwf";
    private const string StartKey = Prefix + "/10";
    private const string InsideKey = Prefix + "/25";
    private const string EndKey   = Prefix + "/50";
    private const string OutsideKey = Prefix + "/99";
    private const int    ExpiresMs = 30_000;

    private static readonly byte[] SomeValue = Encoding.UTF8.GetBytes("v");

    private readonly ILogger<IRaft>   raftLogger;
    private readonly ILogger<IKahuna> kahunaLogger;

    public TestRangeLockWriteFence(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = LoggerFactory.Create(builder =>
            builder.AddXUnit(outputHelper).SetMinimumLevel(LogLevel.Warning));

        raftLogger   = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    // ── helpers ──────────────────────────────────────────────────────────────────

    private static HLCTimestamp NextTx(IRaft raft) =>
        raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

    private static async Task<(IRaft Leader, KahunaManager Manager)> GetLeader(
        IRaft r1, IRaft r2, IRaft r3, IKahuna k1, IKahuna k2, IKahuna k3)
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

    // ── tests ─────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Regression guard: tx A holds X [10,50); tx B inserts a new key inside → MustRetry.
    /// This was already handled by TrySetHandler; confirms refactor to shared helper didn't regress it.
    /// </summary>
    [Fact]
    public async Task Write_PhantomInsertIntoExclusiveRange_Conflicts()
    {
        (IRaft r1, IRaft r2, IRaft r3, IKahuna k1, IKahuna k2, IKahuna k3) =
            await AssembleThreNodeCluster("memory", 1, raftLogger, kahunaLogger);
        try
        {
            (IRaft leaderRaft, KahunaManager leader) = await GetLeader(r1, r2, r3, k1, k2, k3);

            HLCTimestamp txA = NextTx(leaderRaft);
            HLCTimestamp txB = NextTx(leaderRaft);

            (KeyValueResponseType _lockType, _) = await leader.TryAcquireRangeLock(
                txA, Prefix, StartKey, true, EndKey, false, ExpiresMs,
                KeyValueDurability.Persistent, RangeLockMode.Exclusive);
            Assert.Equal(KeyValueResponseType.Locked, _lockType);

            (KeyValueResponseType setResult, _, _) = await leader.TrySetKeyValue(
                txB, InsideKey, SomeValue, null, -1, KeyValueFlags.Set, 0,
                KeyValueDurability.Persistent);
            Assert.Equal(KeyValueResponseType.MustRetry, setResult);
        }
        finally
        {
            await LeaveCluster(r1, r2, r3);
        }
    }

    /// <summary>
    /// Tx A holds X [10,50); tx B deletes an existing key inside → MustRetry.
    /// </summary>
    [Fact]
    public async Task Delete_PhantomIntoExclusiveRange_Conflicts()
    {
        (IRaft r1, IRaft r2, IRaft r3, IKahuna k1, IKahuna k2, IKahuna k3) =
            await AssembleThreNodeCluster("memory", 1, raftLogger, kahunaLogger);
        try
        {
            (IRaft leaderRaft, KahunaManager leader) = await GetLeader(r1, r2, r3, k1, k2, k3);

            // Create the key without a transaction first so it exists.
            (KeyValueResponseType createResult, _, _) = await leader.TrySetKeyValue(
                HLCTimestamp.Zero, InsideKey, SomeValue, null, -1, KeyValueFlags.Set, 0,
                KeyValueDurability.Persistent);
            Assert.Equal(KeyValueResponseType.Set, createResult);

            HLCTimestamp txA = NextTx(leaderRaft);
            HLCTimestamp txB = NextTx(leaderRaft);

            (KeyValueResponseType _lockType, _) = await leader.TryAcquireRangeLock(
                txA, Prefix, StartKey, true, EndKey, false, ExpiresMs,
                KeyValueDurability.Persistent, RangeLockMode.Exclusive);
            Assert.Equal(KeyValueResponseType.Locked, _lockType);

            (KeyValueResponseType deleteResult, _, _) = await leader.TryDeleteKeyValue(
                txB, InsideKey, KeyValueDurability.Persistent);
            Assert.Equal(KeyValueResponseType.MustRetry, deleteResult);
        }
        finally
        {
            await LeaveCluster(r1, r2, r3);
        }
    }

    /// <summary>
    /// Tx A holds X [10,50); tx B prepares a mutation on a key inside → MustRetry.
    /// </summary>
    [Fact]
    public async Task Prepare_IntoExclusiveRange_Conflicts()
    {
        (IRaft r1, IRaft r2, IRaft r3, IKahuna k1, IKahuna k2, IKahuna k3) =
            await AssembleThreNodeCluster("memory", 1, raftLogger, kahunaLogger);
        try
        {
            (IRaft leaderRaft, KahunaManager leader) = await GetLeader(r1, r2, r3, k1, k2, k3);

            // tx B stages a mutation on the inside key.
            HLCTimestamp txB = NextTx(leaderRaft);
            (KeyValueResponseType setResult, _, _) = await leader.TrySetKeyValue(
                txB, InsideKey, SomeValue, null, -1, KeyValueFlags.Set, 0,
                KeyValueDurability.Persistent);
            Assert.Equal(KeyValueResponseType.Set, setResult);

            // tx A acquires exclusive range lock covering that key.
            HLCTimestamp txA = NextTx(leaderRaft);
            (KeyValueResponseType _lockType, _) = await leader.TryAcquireRangeLock(
                txA, Prefix, StartKey, true, EndKey, false, ExpiresMs,
                KeyValueDurability.Persistent, RangeLockMode.Exclusive);
            Assert.Equal(KeyValueResponseType.Locked, _lockType);

            // tx B tries to prepare — tx A's range lock blocks it.
            HLCTimestamp commitId = NextTx(leaderRaft);
            (KeyValueResponseType prepResult, _, _, _) = await leader.TryPrepareMutations(
                txB, commitId, InsideKey, KeyValueDurability.Persistent);
            Assert.Equal(KeyValueResponseType.MustRetry, prepResult);
        }
        finally
        {
            await LeaveCluster(r1, r2, r3);
        }
    }

    /// <summary>
    /// A write (set and delete) to a key inside a SHARED range lock from another tx → MustRetry.
    /// A write needs exclusive on [K,K], which conflicts with S.
    /// </summary>
    [Fact]
    public async Task Write_IntoSharedRange_Conflicts()
    {
        (IRaft r1, IRaft r2, IRaft r3, IKahuna k1, IKahuna k2, IKahuna k3) =
            await AssembleThreNodeCluster("memory", 1, raftLogger, kahunaLogger);
        try
        {
            (IRaft leaderRaft, KahunaManager leader) = await GetLeader(r1, r2, r3, k1, k2, k3);

            // Create the key so delete has something to find.
            (KeyValueResponseType createResult, _, _) = await leader.TrySetKeyValue(
                HLCTimestamp.Zero, InsideKey, SomeValue, null, -1, KeyValueFlags.Set, 0,
                KeyValueDurability.Persistent);
            Assert.Equal(KeyValueResponseType.Set, createResult);

            HLCTimestamp txA = NextTx(leaderRaft);
            HLCTimestamp txB = NextTx(leaderRaft);

            (KeyValueResponseType _lockType, _) = await leader.TryAcquireRangeLock(
                txA, Prefix, StartKey, true, EndKey, false, ExpiresMs,
                KeyValueDurability.Persistent, RangeLockMode.Shared);
            Assert.Equal(KeyValueResponseType.Locked, _lockType);

            // Set inside shared range → MustRetry
            (KeyValueResponseType setResult, _, _) = await leader.TrySetKeyValue(
                txB, InsideKey, SomeValue, null, -1, KeyValueFlags.Set, 0,
                KeyValueDurability.Persistent);
            Assert.Equal(KeyValueResponseType.MustRetry, setResult);

            // Delete inside shared range → MustRetry
            (KeyValueResponseType deleteResult, _, _) = await leader.TryDeleteKeyValue(
                txB, InsideKey, KeyValueDurability.Persistent);
            Assert.Equal(KeyValueResponseType.MustRetry, deleteResult);
        }
        finally
        {
            await LeaveCluster(r1, r2, r3);
        }
    }

    /// <summary>
    /// Writes to a key OUTSIDE the locked range succeed regardless of mode.
    /// </summary>
    [Fact]
    public async Task Write_OutsideRange_Succeeds()
    {
        (IRaft r1, IRaft r2, IRaft r3, IKahuna k1, IKahuna k2, IKahuna k3) =
            await AssembleThreNodeCluster("memory", 1, raftLogger, kahunaLogger);
        try
        {
            (IRaft leaderRaft, KahunaManager leader) = await GetLeader(r1, r2, r3, k1, k2, k3);

            // Create outside key so delete has something to find.
            (KeyValueResponseType createResult, _, _) = await leader.TrySetKeyValue(
                HLCTimestamp.Zero, OutsideKey, SomeValue, null, -1, KeyValueFlags.Set, 0,
                KeyValueDurability.Persistent);
            Assert.Equal(KeyValueResponseType.Set, createResult);

            HLCTimestamp txA = NextTx(leaderRaft);
            HLCTimestamp txB = NextTx(leaderRaft);

            (KeyValueResponseType _lockType, _) = await leader.TryAcquireRangeLock(
                txA, Prefix, StartKey, true, EndKey, false, ExpiresMs,
                KeyValueDurability.Persistent, RangeLockMode.Exclusive);
            Assert.Equal(KeyValueResponseType.Locked, _lockType);

            (KeyValueResponseType setResult, _, _) = await leader.TrySetKeyValue(
                txB, OutsideKey, SomeValue, null, -1, KeyValueFlags.Set, 0,
                KeyValueDurability.Persistent);
            Assert.Equal(KeyValueResponseType.Set, setResult);

            (KeyValueResponseType deleteResult, _, _) = await leader.TryDeleteKeyValue(
                txB, OutsideKey, KeyValueDurability.Persistent);
            Assert.Equal(KeyValueResponseType.Deleted, deleteResult);
        }
        finally
        {
            await LeaveCluster(r1, r2, r3);
        }
    }

    /// <summary>
    /// The same tx that holds a range lock may write inside its own range — same-tx skip honored.
    /// </summary>
    [Fact]
    public async Task Write_SameTx_InsideOwnRange_Succeeds()
    {
        (IRaft r1, IRaft r2, IRaft r3, IKahuna k1, IKahuna k2, IKahuna k3) =
            await AssembleThreNodeCluster("memory", 1, raftLogger, kahunaLogger);
        try
        {
            (IRaft leaderRaft, KahunaManager leader) = await GetLeader(r1, r2, r3, k1, k2, k3);

            HLCTimestamp txA = NextTx(leaderRaft);

            (KeyValueResponseType _lockType, _) = await leader.TryAcquireRangeLock(
                txA, Prefix, StartKey, true, EndKey, false, ExpiresMs,
                KeyValueDurability.Persistent, RangeLockMode.Exclusive);
            Assert.Equal(KeyValueResponseType.Locked, _lockType);

            // txA writes inside its own exclusive range → must succeed
            (KeyValueResponseType setResult, _, _) = await leader.TrySetKeyValue(
                txA, InsideKey, SomeValue, null, -1, KeyValueFlags.Set, 0,
                KeyValueDurability.Persistent);
            Assert.Equal(KeyValueResponseType.Set, setResult);
        }
        finally
        {
            await LeaveCluster(r1, r2, r3);
        }
    }
}
