using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Tests.Server;

/// <summary>
/// Acceptance tests — lock-snapshot serialization round-trip and release-by-overlap.
/// These tests exercise the lock-transfer plumbing (proto, export, import, clamping, dedup, release fix)
/// <b>without</b> wiring split or merge, so they cannot regress the topology paths.
/// </summary>
public sealed class TestRangeLockTransfer : BaseCluster
{
    private const string Space = "t:lt";

    private readonly ILogger<IRaft> raftLogger;
    private readonly ILogger<IKahuna> kahunaLogger;

    public TestRangeLockTransfer(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = LoggerFactory.Create(builder =>
            builder.AddXUnit(outputHelper).SetMinimumLevel(LogLevel.Warning));

        raftLogger  = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    // ── helpers ──────────────────────────────────────────────────────────────────

    private static HLCTimestamp NextTx(IRaft raft) =>
        raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

    private static async Task<(IRaft Raft, KahunaManager Kahuna)> LeaderOf(
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

    // ── Test 1 ───────────────────────────────────────────────────────────────

    /// <summary>
    /// Verifies that a set of live range-lock entries (Shared + Exclusive, various bounds)
    /// round-trips through <c>ExportLocksAsync</c> / <c>ImportLocks</c>:
    /// <list type="bullet">
    ///   <item>Mode, clamped bounds, and Expires are preserved.</item>
    ///   <item>Expired entries are excluded from the export.</item>
    ///   <item>A duplicate import (same tx + overlapping bounds) is deduped.</item>
    /// </list>
    /// </summary>
    [Fact]
    public async Task RangeSnapshot_RoundTripsLockEntries()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft r1, IRaft r2, IRaft r3, IKahuna k1, IKahuna k2, IKahuna k3) =
            await AssembleThreNodeCluster("memory", 1, raftLogger, kahunaLogger);

        (IRaft, KahunaManager)[] nodes =
            [(r1, (KahunaManager)k1), (r2, (KahunaManager)k2), (r3, (KahunaManager)k3)];

        try
        {
            (IRaft leaderRaft, KahunaManager leader) = await LeaderOf(1, nodes);

            HLCTimestamp txS = NextTx(leaderRaft);
            HLCTimestamp txX = NextTx(leaderRaft);
            HLCTimestamp txExp = NextTx(leaderRaft);

            // Shared lock [−∞, Space+"/m")
            (KeyValueResponseType _lockS, _) = await leader.TryAcquireRangeLock(
                txS, Space, null, true, Space + "/m", false, 60_000, KeyValueDurability.Persistent, RangeLockMode.Shared);
            Assert.Equal(KeyValueResponseType.Locked, _lockS);

            // Exclusive lock [Space+"/m", Space+"/z")
            (KeyValueResponseType _lockX, _) = await leader.TryAcquireRangeLock(
                txX, Space, Space + "/m", true, Space + "/z", false, 60_000, KeyValueDurability.Persistent, RangeLockMode.Exclusive);
            Assert.Equal(KeyValueResponseType.Locked, _lockX);

            // An already-expired lock: expiresMs = 1 then wait just enough
            (KeyValueResponseType _lockExp, _) = await leader.TryAcquireRangeLock(
                txExp, Space, Space + "/e", true, Space + "/f", false, 1, KeyValueDurability.Persistent, RangeLockMode.Shared);
            Assert.Equal(KeyValueResponseType.Locked, _lockExp);

            // Advance HLC so expiresTs of the 1 ms lock is definitely in the past.
            await Task.Delay(20, ct);
            HLCTimestamp now = leaderRaft.HybridLogicalClock.TrySendOrLocalEvent(leaderRaft.GetLocalNodeId());

            // ── Export: capture locks overlapping [Space, +∞)  ──────────────────
            KvStateMachineTransfer transfer = leader.KvStateMachineTransfer;

            Stream exportStream = await transfer.ExportLocksAsync(
                Space, Space, null, now, ct);

            List<KeyValueRangeLock> imported = KvStateMachineTransfer.ImportLocks(
                exportStream, Space, null, now);

            // Expired entry must NOT appear.
            Assert.DoesNotContain(imported, e => e.TransactionId == txExp);

            // Shared lock must be present with Mode = Shared and clamped start = Space.
            KeyValueRangeLock? sharedEntry = imported.Find(e => e.TransactionId == txS);
            Assert.NotNull(sharedEntry);
            Assert.Equal(RangeLockMode.Shared, sharedEntry.Mode);
            // Start clamped to destStartKey (Space) since original start was null.
            Assert.Equal(Space, sharedEntry.StartKey);
            Assert.True(sharedEntry.StartInclusive);
            Assert.Equal(Space + "/m", sharedEntry.EndKey);
            Assert.False(sharedEntry.EndInclusive);

            // Exclusive lock must be present with Mode = Exclusive and bounds unchanged (fully inside dest range).
            KeyValueRangeLock? exclusiveEntry = imported.Find(e => e.TransactionId == txX);
            Assert.NotNull(exclusiveEntry);
            Assert.Equal(RangeLockMode.Exclusive, exclusiveEntry.Mode);
            Assert.Equal(Space + "/m", exclusiveEntry.StartKey);
            Assert.Equal(Space + "/z", exclusiveEntry.EndKey);

            // ── Dedup: import the same stream a second time; count must not increase ──
            exportStream.Position = 0;
            List<KeyValueRangeLock> importedAgain = KvStateMachineTransfer.ImportLocks(
                exportStream, Space, null, now);

            // Inject both batches into a fresh destination.
            string destSpace = Space + "-dest";
            await leader.ImportRangeLocksAsync(destSpace, imported);
            await leader.ImportRangeLocksAsync(destSpace, importedAgain); // second import must dedup

            List<KeyValueRangeLock> stored = await leader.GetRangeLocksAsync(destSpace);

            // Exactly 2 non-expired locks (txS and txX), not 4.
            Assert.Equal(2, stored.Count);
        }
        finally
        {
            await LeaveCluster(r1, r2, r3);
        }
    }

    // ── Test 2 ───────────────────────────────────────────────────────────────

    /// <summary>
    /// Verifies the release-by-overlap change: a clamped-bounds lock entry (as the destination
    /// partition would have after a split) is correctly matched and removed when the caller
    /// releases using the original (unclamped) bounds.
    ///
    /// <para>Setup: inject a clamped entry [Space+"/m", +∞) for tx1. Release with original
    /// bounds [−∞, +∞). The release handler must match by txId + overlap and remove the entry.</para>
    /// </summary>
    [Fact]
    public async Task Release_MatchesClampedEntry()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft r1, IRaft r2, IRaft r3, IKahuna k1, IKahuna k2, IKahuna k3) =
            await AssembleThreNodeCluster("memory", 1, raftLogger, kahunaLogger);

        (IRaft, KahunaManager)[] nodes =
            [(r1, (KahunaManager)k1), (r2, (KahunaManager)k2), (r3, (KahunaManager)k3)];

        try
        {
            (IRaft leaderRaft, KahunaManager leader) = await LeaderOf(1, nodes);

            HLCTimestamp tx1 = NextTx(leaderRaft);

            // Inject a clamped entry directly (simulates what the split transfer would do for P').
            string destSpace = Space + "-clamp";
            List<KeyValueRangeLock> clamped =
            [
                new KeyValueRangeLock
                {
                    TransactionId  = tx1,
                    Expires        = tx1 + 60_000,
                    StartKey       = Space + "/m", // clamped — original was null (unbounded left)
                    StartInclusive = true,
                    EndKey         = null,
                    EndInclusive   = false,
                    Mode           = RangeLockMode.Shared,
                }
            ];

            await leader.ImportRangeLocksAsync(destSpace, clamped);

            // Verify it's there.
            List<KeyValueRangeLock> before = await leader.GetRangeLocksAsync(destSpace);
            Assert.Single(before);

            // Release using ORIGINAL (unclamped) bounds [−∞, +∞) — overlap matching must find it.
            KeyValueResponseType releaseResult = await leader.TryReleaseExclusiveRangeLock(
                tx1, destSpace, null, true, null, false, KeyValueDurability.Persistent);

            Assert.Equal(KeyValueResponseType.Unlocked, releaseResult);

            // The entry must be gone.
            List<KeyValueRangeLock> after = await leader.GetRangeLocksAsync(destSpace);
            Assert.Empty(after);
        }
        finally
        {
            await LeaveCluster(r1, r2, r3);
        }
    }

    // ── Test 3 ───────────────────────────────────────────────────────────────

    /// <summary>
    /// Regression guard for the exact-first / overlap-fallback logic in the release handler.
    ///
    /// <para>A single tx holds two distinct (non-overlapping) range locks on the same prefix.
    /// Releasing one by its exact bounds must remove exactly that entry and leave the other
    /// intact — overlap-only matching would have picked the first overlapping entry, which
    /// could be the wrong one.</para>
    /// </summary>
    [Fact]
    public async Task Release_ExactBoundsPreferred_MultiLockTx()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft r1, IRaft r2, IRaft r3, IKahuna k1, IKahuna k2, IKahuna k3) =
            await AssembleThreNodeCluster("memory", 1, raftLogger, kahunaLogger);

        (IRaft, KahunaManager)[] nodes =
            [(r1, (KahunaManager)k1), (r2, (KahunaManager)k2), (r3, (KahunaManager)k3)];

        try
        {
            (IRaft leaderRaft, KahunaManager leader) = await LeaderOf(1, nodes);

            HLCTimestamp tx1 = NextTx(leaderRaft);

            string space = Space + "-exact";

            // Same tx acquires two non-overlapping Shared locks — acquire handler allows this
            // because the conflict loop skips same-tx entries.
            (KeyValueResponseType _lockA, _) = await leader.TryAcquireRangeLock(
                tx1, space, space + "/a", true, space + "/m", false,
                60_000, KeyValueDurability.Persistent, RangeLockMode.Shared);
            Assert.Equal(KeyValueResponseType.Locked, _lockA);

            (KeyValueResponseType _lockM, _) = await leader.TryAcquireRangeLock(
                tx1, space, space + "/m", true, space + "/z", false,
                60_000, KeyValueDurability.Persistent, RangeLockMode.Shared);
            Assert.Equal(KeyValueResponseType.Locked, _lockM);

            List<KeyValueRangeLock> before = await leader.GetRangeLocksAsync(space);
            Assert.Equal(2, before.Count);

            // Release only the second lock [m, z) — exact match must choose it, not [a, m).
            KeyValueResponseType releaseResult = await leader.TryReleaseExclusiveRangeLock(
                tx1, space, space + "/m", true, space + "/z", false, KeyValueDurability.Persistent);

            Assert.Equal(KeyValueResponseType.Unlocked, releaseResult);

            List<KeyValueRangeLock> after = await leader.GetRangeLocksAsync(space);
            Assert.Single(after);

            // The remaining lock must be [a, m), not [m, z).
            Assert.Equal(space + "/a", after[0].StartKey);
            Assert.Equal(space + "/m", after[0].EndKey);
        }
        finally
        {
            await LeaveCluster(r1, r2, r3);
        }
    }
}
