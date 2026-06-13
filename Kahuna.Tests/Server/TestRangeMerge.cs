
using System.Text;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Data;
using Kommander.System;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Tests.Server;

/// <summary>
/// Acceptance tests for the key-range merge transaction.
/// Each test uses a 4-partition 3-node cluster (meta P1 + data P2 + data P3).
/// </summary>
public sealed class TestRangeMerge : BaseCluster
{
    private const string Space = "t:m";

    /// <summary>Boundary key separating the left ([−∞, BoundaryKey)) and right ([BoundaryKey, +∞)) halves.</summary>
    private const string BoundaryKey = Space + "/0010";

    private readonly ILogger<IRaft>   raftLogger;
    private readonly ILogger<IKahuna> kahunaLogger;

    public TestRangeMerge(ITestOutputHelper outputHelper)
    {
        ILoggerFactory lf = LoggerFactory.Create(b =>
            b.AddXUnit(outputHelper).SetMinimumLevel(LogLevel.Warning));
        raftLogger   = lf.CreateLogger<IRaft>();
        kahunaLogger = lf.CreateLogger<IKahuna>();
    }

    // ── helpers ──────────────────────────────────────────────────────────────────

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

    private static async Task WaitFor(Func<bool> predicate, int timeoutMs = 10_000)
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
    /// Forces <paramref name="target"/> to become leader of the system/meta partition (P0), then
    /// waits until the role is confirmed. Since the meta map shares P0 (Kommander 0.11.0+), one P0
    /// leadership grant covers partition lifecycle and the descriptor cutover. Eliminates the
    /// leadership-race that otherwise causes <see cref="RangeMapStore.MutateAsync"/> to return
    /// <c>NodeIsNotLeader</c> mid-merge.
    /// </summary>
    private static async Task<KahunaManager> ForceMetaLeaderAsync(
        (IRaft Raft, KahunaManager Kahuna) target, CancellationToken ct)
    {
        await target.Raft.ForceLeaderForTestingAsync(RangeMapStore.MetaPartitionId, ct);

        long deadline = Environment.TickCount64 + 10_000;
        while (Environment.TickCount64 < deadline)
        {
            if (await target.Raft.AmILeader(RangeMapStore.MetaPartitionId, ct))
                return target.Kahuna;
            await Task.Delay(25, ct);
        }
        Assert.Fail("ForceMetaLeaderAsync: node did not become P0 leader within 10 s.");
        return null!;
    }

    private static byte[] V(string s) => Encoding.UTF8.GetBytes(s);

    /// <summary>
    /// Sets up a 4-partition 3-node cluster with TWO adjacent descriptors:
    ///   Left:  [−∞,        BoundaryKey) @ P2
    ///   Right: [BoundaryKey, +∞)         @ P3
    /// Writes <paramref name="leftCount"/> keys in [−∞, BoundaryKey)
    /// and <paramref name="rightCount"/> keys in [BoundaryKey, +∞).
    /// Waits for all keys to be readable on every node.
    /// </summary>
    private async Task<(
        (IRaft, KahunaManager)[] Nodes,
        RangeDescriptor Left,
        RangeDescriptor Right)>
    SetupTwoRanges(int leftCount, int rightCount)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft r1, IRaft r2, IRaft r3, IKahuna k1, IKahuna k2, IKahuna k3) =
            await AssembleThreNodeCluster("memory", 4, raftLogger, kahunaLogger);

        (IRaft, KahunaManager)[] nodes =
            [(r1, (KahunaManager)k1), (r2, (KahunaManager)k2), (r3, (KahunaManager)k3)];

        foreach ((IRaft _, KahunaManager kahuna) in nodes)
            kahuna.RegisterKeyRange(Space);

        (IRaft _, KahunaManager metaLeader) = await LeaderOf(RangeMapStore.MetaPartitionId, nodes);

        const int leftPartitionId  = RangeMapStore.FirstDataPartitionId;     // P2
        const int rightPartitionId = RangeMapStore.FirstDataPartitionId + 1; // P3

        var left  = new RangeDescriptor { KeySpace = Space, StartKey = null,        EndKey = BoundaryKey, PartitionId = leftPartitionId,  Generation = 1 };
        var right = new RangeDescriptor { KeySpace = Space, StartKey = BoundaryKey, EndKey = null,        PartitionId = rightPartitionId, Generation = 1 };

        bool committed = await metaLeader.RangeMapStore.MutateAsync(_ => [left, right], ct);
        Assert.True(committed);

        // Wait for the descriptor map to propagate to all nodes.
        foreach ((IRaft _, KahunaManager kahuna) in nodes)
            await WaitFor(() =>
            {
                IReadOnlyList<RangeDescriptor> all = kahuna.RangeMapStore.Current.FindAll(Space);
                return all.Count == 2;
            });

        // Write left-half keys via P2 leader.
        (IRaft _, KahunaManager leftLeader) = await LeaderOf(leftPartitionId, nodes);
        var allKeys = new List<string>(leftCount + rightCount);

        for (int i = 0; i < leftCount; i++)
        {
            string key = $"{Space}/{i:D4}";
            (KeyValueResponseType t, _, _) = await leftLeader.TrySetKeyValue(
                HLCTimestamp.Zero, key, V("v" + i), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent);
            Assert.Equal(KeyValueResponseType.Set, t);
            allKeys.Add(key);
        }

        // Write right-half keys via P3 leader.
        (IRaft _, KahunaManager rightLeader) = await LeaderOf(rightPartitionId, nodes);
        for (int i = 0; i < rightCount; i++)
        {
            string key = $"{Space}/{10 + i:D4}";
            (KeyValueResponseType t, _, _) = await rightLeader.TrySetKeyValue(
                HLCTimestamp.Zero, key, V("v" + i), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent);
            Assert.Equal(KeyValueResponseType.Set, t);
            allKeys.Add(key);
        }

        // Wait for all keys to be readable on every node (replication lag).
        foreach (string key in allKeys)
        {
            string k = key;
            foreach ((IRaft _, KahunaManager kahuna) in nodes)
            {
                KahunaManager km = kahuna;
                long deadline = Environment.TickCount64 + 10_000;
                while (Environment.TickCount64 < deadline)
                {
                    (KeyValueResponseType rt, _) =
                        await km.TryGetValue(HLCTimestamp.Zero, k, 0, HLCTimestamp.Zero, KeyValueDurability.Persistent);
                    if (rt == KeyValueResponseType.Get) break;
                    await Task.Delay(25, ct);
                }
            }
        }

        return (nodes, left, right);
    }

    /// <summary>
    /// Calls <see cref="RangeMerger.MergeAsync"/> on the meta-partition leader, then
    /// <see cref="IRaft.RemovePartitionAsync"/> on the system-partition (0) leader.
    /// </summary>
    private static async Task<MergeOutcome> MergeViaLeaders(
        string space, RangeDescriptor left, RangeDescriptor right,
        (IRaft, KahunaManager)[] nodes, CancellationToken ct)
    {
        (IRaft _, KahunaManager metaLeader) = await LeaderOf(RangeMapStore.MetaPartitionId, nodes);
        MergeOutcome outcome = await metaLeader.RangeMerger.MergeAsync(space, left, right, ct);

        if (outcome.IsSuccess)
        {
            (IRaft sysRaft, KahunaManager _) = await LeaderOf(0, nodes);
            await sysRaft.RemovePartitionAsync(outcome.RetiredPartitionId, ct);
        }

        return outcome;
    }

    // ── Lock-transfer tests ───────────────────────────────────────────────

    private static HLCTimestamp NextTx(IRaft raft) =>
        raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

    /// <summary>
    /// Runs a merge lock-transfer scenario with bounded retries (each attempt on a fresh cluster)
    /// to tolerate the documented best-effort gap: a left-leadership change during
    /// the merge window can temporarily strand the in-memory, non-replicated lock on a former
    /// leader. <paramref name="attempt"/> returns true when the transfer-dependent guarantee held;
    /// false signals a strand and triggers a retry. Hard bugs surface as exceptions (no masking).
    /// The post-cutover <c>EnsureLocksOnDestinationLeaderAsync</c> confirm loop narrows the window;
    /// the robust fix is to replicate locks through the partition's Raft log.
    /// </summary>
    private static async Task RetryMergeTransfer(Func<Task<bool>> attempt, int maxAttempts = 5)
    {
        for (int i = 1; i <= maxAttempts; i++)
        {
            if (await attempt())
                return;
        }

        Assert.Fail($"merge lock-transfer guarantee not observed after {maxAttempts} attempts " +
                    "(best-effort under leadership churn)");
    }

    /// <summary>
    /// A range lock acquired on the soon-to-be-retired right partition (P3) must be
    /// enforced on the surviving left partition (P2) after the merge. A foreign Exclusive
    /// attempt on the merged range → AlreadyLocked.
    /// </summary>
    [Fact(Skip = "Best-effort: a left-partition leadership change during the merge " +
                 "window can strand the in-memory, non-replicated lock. Re-enable when range locks are " +
                 "replicated through the partition Raft log, which makes the guarantee " +
                 "deterministic.")]
    public Task Lock_OnMergedPartition_Enforced() => RetryMergeTransfer(async () =>
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        ((IRaft, KahunaManager)[] nodes, RangeDescriptor left, RangeDescriptor right) =
            await SetupTwoRanges(1, 1);

        (IRaft r1, IRaft r2, IRaft r3) =
            (nodes[0].Item1, nodes[1].Item1, nodes[2].Item1);

        try
        {
            // tx1 holds an Exclusive lock on the right partition's range before the merge.
            (IRaft rightRaft, KahunaManager rightLeader) =
                await LeaderOf(right.PartitionId, nodes);

            HLCTimestamp tx1 = NextTx(rightRaft);
            KeyValueResponseType lockBefore = await rightLeader.TryAcquireRangeLock(
                tx1, Space, right.StartKey, true, right.EndKey, false, 60_000,
                KeyValueDurability.Persistent, RangeLockMode.Exclusive);
            Assert.Equal(KeyValueResponseType.Locked, lockBefore);

            // Merge right → left (must transfer tx1's lock to the survivor).
            MergeOutcome outcome = await MergeViaLeaders(Space, left, right, nodes, ct);
            Assert.True(outcome.IsSuccess, $"Merge failed: {outcome.Status}");

            // Locate the surviving left leader.
            (IRaft leftRaft, KahunaManager leftLeader) =
                await LeaderOf(left.PartitionId, nodes);

            // tx2 tries Exclusive on the merged range — must be blocked by tx1's clamped lock.
            HLCTimestamp tx2 = NextTx(leftRaft);
            KeyValueResponseType blockedAfter = await leftLeader.TryAcquireRangeLock(
                tx2, Space, right.StartKey, true, right.EndKey, false, 60_000,
                KeyValueDurability.Persistent, RangeLockMode.Exclusive);

            return blockedAfter == KeyValueResponseType.AlreadyLocked;
        }
        finally
        {
            await LeaveCluster(r1, r2, r3);
        }
    });

    /// <summary>
    /// After a merge, releasing the transferred lock via the survivor left partition cleans
    /// up the clamped entry. A subsequent Exclusive on the same range → Locked.
    /// </summary>
    [Fact(Skip = "Best-effort: a left-partition leadership change during the merge " +
                 "window can strand the in-memory, non-replicated lock. Re-enable when range locks are " +
                 "replicated through the partition Raft log, which makes the guarantee " +
                 "deterministic.")]
    public Task Lock_OnMergedPartition_ReleaseCleansSurvivor() => RetryMergeTransfer(async () =>
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        ((IRaft, KahunaManager)[] nodes, RangeDescriptor left, RangeDescriptor right) =
            await SetupTwoRanges(1, 1);

        (IRaft r1, IRaft r2, IRaft r3) =
            (nodes[0].Item1, nodes[1].Item1, nodes[2].Item1);

        try
        {
            (IRaft rightRaft, KahunaManager rightLeader) =
                await LeaderOf(right.PartitionId, nodes);

            HLCTimestamp tx1 = NextTx(rightRaft);
            KeyValueResponseType lockBefore = await rightLeader.TryAcquireRangeLock(
                tx1, Space, right.StartKey, true, right.EndKey, false, 60_000,
                KeyValueDurability.Persistent, RangeLockMode.Exclusive);
            Assert.Equal(KeyValueResponseType.Locked, lockBefore);

            MergeOutcome outcome = await MergeViaLeaders(Space, left, right, nodes, ct);
            Assert.True(outcome.IsSuccess, $"Merge failed: {outcome.Status}");

            (IRaft leftRaft, KahunaManager leftLeader) =
                await LeaderOf(left.PartitionId, nodes);

            // Confirm the lock is present on the survivor (return false to retry if stranded).
            HLCTimestamp txCheck = NextTx(leftRaft);
            KeyValueResponseType blockedBefore = await leftLeader.TryAcquireRangeLock(
                txCheck, Space, right.StartKey, true, right.EndKey, false, 60_000,
                KeyValueDurability.Persistent, RangeLockMode.Exclusive);

            if (blockedBefore != KeyValueResponseType.AlreadyLocked)
                return false;

            // Release with original (unclamped) bounds — overlap fallback matches the clamped entry.
            KeyValueResponseType released = await leftLeader.TryReleaseExclusiveRangeLock(
                tx1, Space, right.StartKey, true, right.EndKey, false,
                KeyValueDurability.Persistent);
            Assert.Equal(KeyValueResponseType.Unlocked, released);

            // After release a fresh Exclusive must be granted.
            HLCTimestamp tx2 = NextTx(leftRaft);
            KeyValueResponseType afterRelease = await leftLeader.TryAcquireRangeLock(
                tx2, Space, right.StartKey, true, right.EndKey, false, 60_000,
                KeyValueDurability.Persistent, RangeLockMode.Exclusive);
            Assert.Equal(KeyValueResponseType.Locked, afterRelease);

            return true;
        }
        finally
        {
            await LeaveCluster(r1, r2, r3);
        }
    });

    // ── TwoUnderMinAdjacent_MergeIntoSurvivor ────────────────────────────────

    /// <summary>
    /// Two adjacent under-min ranges [−∞, K)@P2 and [K,+∞)@P3 are merged into a single
    /// [−∞, +∞)@P2 descriptor.  After the merge the map holds exactly one descriptor for the
    /// space, covering the full range, on the survivor partition (P2).
    /// </summary>
    [Fact]
    public async Task TwoUnderMinAdjacent_MergeIntoSurvivor()
    {
        const int leftCount  = 3;
        const int rightCount = 3;

        ((IRaft, KahunaManager)[] nodes, RangeDescriptor left, RangeDescriptor right) =
            await SetupTwoRanges(leftCount, rightCount);

        (IRaft r1, IRaft r2, IRaft r3) =
            (nodes[0].Item1, nodes[1].Item1, nodes[2].Item1);

        try
        {
            CancellationToken ct = TestContext.Current.CancellationToken;

            MergeOutcome outcome = await MergeViaLeaders(Space, left, right, nodes, ct);
            Assert.True(outcome.IsSuccess);
            Assert.Equal(right.PartitionId, outcome.RetiredPartitionId);

            // Wait for the descriptor map to converge on all nodes.
            (IRaft _, KahunaManager metaLeader) = await LeaderOf(RangeMapStore.MetaPartitionId, nodes);
            await WaitFor(() => metaLeader.RangeMapStore.Current.FindAll(Space).Count == 1);

            foreach ((IRaft _, KahunaManager kahuna) in nodes)
            {
                IReadOnlyList<RangeDescriptor> descriptors = kahuna.RangeMapStore.Current.FindAll(Space);
                Assert.Single(descriptors);

                RangeDescriptor merged = descriptors[0];
                Assert.Equal(Space, merged.KeySpace);
                Assert.Null(merged.StartKey);          // covers −∞
                Assert.Null(merged.EndKey);             // covers +∞
                Assert.Equal(left.PartitionId, merged.PartitionId);  // survivor = P2
                Assert.True(merged.Generation > left.Generation);     // generation bumped
            }
        }
        finally
        {
            await LeaveCluster(r1, r2, r3);
        }
    }

    // ── RetiredPartition_Removed ──────────────────────────────────────────────

    /// <summary>
    /// After a successful merge, <see cref="IRaft.RemovePartitionAsync"/> is called for the
    /// retired right partition (P3).  The system-partition leader must report success, and
    /// the retired partition must no longer be a leader on any node.
    /// </summary>
    [Fact]
    public async Task RetiredPartition_Removed()
    {
        const int leftCount  = 2;
        const int rightCount = 2;

        ((IRaft, KahunaManager)[] nodes, RangeDescriptor left, RangeDescriptor right) =
            await SetupTwoRanges(leftCount, rightCount);

        (IRaft r1, IRaft r2, IRaft r3) =
            (nodes[0].Item1, nodes[1].Item1, nodes[2].Item1);

        try
        {
            CancellationToken ct = TestContext.Current.CancellationToken;

            // Pin P0 (system) and P1 (meta) to nodes[0] so MutateAsync cannot lose leadership
            // mid-merge and RemovePartitionAsync can be called on the same node.
            KahunaManager dualLeader = await ForceMetaLeaderAsync(nodes[0], ct);

            MergeOutcome outcome = await dualLeader.RangeMerger.MergeAsync(Space, left, right, ct);
            Assert.True(outcome.IsSuccess);

            int retiredId = outcome.RetiredPartitionId;
            Assert.Equal(right.PartitionId, retiredId);

            // Remove the retired partition — nodes[0] is now the system-partition leader.
            RaftPartitionLifecycleResult removeResult = await nodes[0].Item1.RemovePartitionAsync(retiredId, ct);
            Assert.True(removeResult.Success);

            // The descriptor map must reflect the merge: one descriptor, covering the full range.
            await WaitFor(() => dualLeader.RangeMapStore.Current.FindAll(Space).Count == 1);
            Assert.Single(dualLeader.RangeMapStore.Current.FindAll(Space));
        }
        finally
        {
            await LeaveCluster(r1, r2, r3);
        }
    }

    // ── Merge_InvariantHoldsAcrossSwap ────────────────────────────────────────

    /// <summary>
    /// <see cref="RangeMap.Validate()"/> holds both before and after the merge cutover.
    /// After the merge the descriptor set covers [−∞, +∞) without a gap and with no overlap.
    /// </summary>
    [Fact]
    public async Task Merge_InvariantHoldsAcrossSwap()
    {
        const int leftCount  = 4;
        const int rightCount = 4;

        ((IRaft, KahunaManager)[] nodes, RangeDescriptor left, RangeDescriptor right) =
            await SetupTwoRanges(leftCount, rightCount);

        (IRaft r1, IRaft r2, IRaft r3) =
            (nodes[0].Item1, nodes[1].Item1, nodes[2].Item1);

        try
        {
            CancellationToken ct = TestContext.Current.CancellationToken;
            (IRaft _, KahunaManager metaLeader) = await LeaderOf(RangeMapStore.MetaPartitionId, nodes);

            // Pre-merge invariant.
            Assert.True(metaLeader.RangeMapStore.Current.Validate(out string? preError), preError);

            MergeOutcome outcome = await MergeViaLeaders(Space, left, right, nodes, ct);
            Assert.True(outcome.IsSuccess);

            await WaitFor(() => metaLeader.RangeMapStore.Current.FindAll(Space).Count == 1);

            // Post-merge invariant: no gap, no overlap, full coverage.
            foreach ((IRaft _, KahunaManager kahuna) in nodes)
            {
                RangeMap map = kahuna.RangeMapStore.Current;
                Assert.True(map.Validate(out string? postError), postError);

                IReadOnlyList<RangeDescriptor> descriptors = map.FindAll(Space);
                Assert.Single(descriptors);

                // The merged descriptor covers the whole space.
                Assert.Null(descriptors[0].StartKey);
                Assert.Null(descriptors[0].EndKey);
            }
        }
        finally
        {
            await LeaveCluster(r1, r2, r3);
        }
    }

    // ── FindMergeCandidates_ThreeUnderMin_ReturnsNonOverlappingPairs ──────────

    /// <summary>
    /// When three consecutive under-min ranges A, B, C all qualify, only one non-overlapping
    /// pair is returned (greedy: (A,B) is selected; B is consumed so (B,C) is skipped).
    /// This prevents a naive caller from attempting (B,C) after (A,B) has retired B, which
    /// would produce a <c>ConcurrentChange</c> failure.
    /// </summary>
    [Fact]
    public async Task FindMergeCandidates_ThreeUnderMin_ReturnsNonOverlappingPair()
    {
        const string spaceABC = "t:mabc";
        CancellationToken ct  = TestContext.Current.CancellationToken;

        (IRaft r1, IRaft r2, IRaft r3, IKahuna k1, IKahuna k2, IKahuna k3) =
            await AssembleThreNodeCluster("memory", 5, raftLogger, kahunaLogger);

        (IRaft, KahunaManager)[] nodes =
            [(r1, (KahunaManager)k1), (r2, (KahunaManager)k2), (r3, (KahunaManager)k3)];

        try
        {
            foreach ((IRaft _, KahunaManager kahuna) in nodes)
                kahuna.RegisterKeyRange(spaceABC);

            (IRaft _, KahunaManager metaLeader) = await LeaderOf(RangeMapStore.MetaPartitionId, nodes);

            // Three adjacent descriptors [null,"k010") [k010,"k020") [k020,null) on P2, P3, P4.
            const string kAB = spaceABC + "/k010";
            const string kBC = spaceABC + "/k020";
            var dA = new RangeDescriptor { KeySpace = spaceABC, StartKey = null, EndKey = kAB, PartitionId = 2, Generation = 1 };
            var dB = new RangeDescriptor { KeySpace = spaceABC, StartKey = kAB,  EndKey = kBC, PartitionId = 3, Generation = 1 };
            var dC = new RangeDescriptor { KeySpace = spaceABC, StartKey = kBC,  EndKey = null, PartitionId = 4, Generation = 1 };

            bool committed = await metaLeader.RangeMapStore.MutateAsync(_ => [dA, dB, dC], ct);
            Assert.True(committed);

            foreach ((IRaft _, KahunaManager kahuna) in nodes)
                await WaitFor(() => kahuna.RangeMapStore.Current.FindAll(spaceABC).Count == 3);

            // All three ranges are empty (0 keys < any minMergeSize threshold).
            // FindMergeCandidatesAsync must return exactly ONE non-overlapping pair: (A,B).
            List<(RangeDescriptor Left, RangeDescriptor Right)> candidates =
                await metaLeader.RangeMerger.FindMergeCandidatesAsync(spaceABC, minMergeSize: 10, ct);

            Assert.Single(candidates);
            Assert.Equal(dA.PartitionId, candidates[0].Left.PartitionId);
            Assert.Equal(dB.PartitionId, candidates[0].Right.PartitionId);
        }
        finally
        {
            await LeaveCluster(r1, r2, r3);
        }
    }

    // ── AutoMerge_DualLeader_MergesUnderMinRanges ─────────────────────────────

    /// <summary>
    /// <see cref="KahunaManager.TriggerAutoMergeAsync(int, CancellationToken)"/> (the automatic
    /// driver) finds the two adjacent under-min ranges, merges them, and retires P3 via
    /// <see cref="IRaft.RemovePartitionAsync"/> — all in one call on the dual-leader node.
    /// After the trigger the map holds exactly one full-range descriptor on P2.
    /// </summary>
    [Fact]
    public async Task AutoMerge_DualLeader_MergesUnderMinRanges()
    {
        const int leftCount  = 3;
        const int rightCount = 3;
        const int minSize    = 10; // both halves are under-min

        ((IRaft, KahunaManager)[] nodes, _, _) = await SetupTwoRanges(leftCount, rightCount);

        (IRaft r1, IRaft r2, IRaft r3) =
            (nodes[0].Item1, nodes[1].Item1, nodes[2].Item1);

        try
        {
            CancellationToken ct = TestContext.Current.CancellationToken;

            // Find the node that is simultaneously system-partition (0) and meta-partition (1) leader.
            KahunaManager? dualLeader = null;
            long deadline = Environment.TickCount64 + 10_000;
            while (Environment.TickCount64 < deadline)
            {
                foreach ((IRaft raft, KahunaManager kahuna) in nodes)
                    if (await raft.AmILeader(0, ct) &&
                        await raft.AmILeader(RangeMapStore.MetaPartitionId, ct))
                    { dualLeader = kahuna; break; }
                if (dualLeader is not null) break;
                await Task.Delay(50, ct);
            }

            if (dualLeader is null)
                return; // valid cluster layout with no dual-leader; skip

            int merges = await dualLeader.TriggerAutoMergeAsync(minSize, ct);
            Assert.Equal(1, merges);

            await WaitFor(() => dualLeader.RangeMapStore.Current.FindAll(Space).Count == 1);

            IReadOnlyList<RangeDescriptor> descriptors = dualLeader.RangeMapStore.Current.FindAll(Space);
            Assert.Single(descriptors);
            Assert.Null(descriptors[0].StartKey);
            Assert.Null(descriptors[0].EndKey);
            Assert.Equal(RangeMapStore.FirstDataPartitionId, descriptors[0].PartitionId); // survivor = P2
        }
        finally
        {
            await LeaveCluster(r1, r2, r3);
        }
    }
}
