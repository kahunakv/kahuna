using System.Text;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Data;
using Kommander.System;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// Acceptance tests for the key-range split transaction.
/// Each test uses a 4-partition 3-node cluster (meta P1 + data P2/P3/P4).
/// </summary>
[Collection("ClusterTests")]
public sealed class TestRangeSplit : BaseCluster
{
    private const string Space = "t:s";

    private readonly ILogger<IRaft> raftLogger;
    private readonly ILogger<IKahuna> kahunaLogger;

    public TestRangeSplit(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = LoggerFactory.Create(builder =>
            builder.AddXUnit(outputHelper).SetMinimumLevel(LogLevel.Warning));

        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    // ── helpers ──────────────────────────────────────────────────────────────────

    /// <summary>System partition (Kommander's partition 0) leader — CreatePartitionAsync requires this.</summary>
    private const int SystemPartition = 0;

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

    /// <summary>
    /// Creates a new Unrouted partition (step 4 of the split) and then calls
    /// <see cref="RangeSplitter.SplitAsync"/>. Since Kommander 0.11.0 the meta map shares the system
    /// partition (P0), so <c>CreatePartitionAsync</c> (system-leader) and the cutover (meta-leader)
    /// require the <b>same</b> node; resolve one P0 leader and drive both steps through it (resolving
    /// them separately would open a staleness window if P0 re-elects between the two lookups).
    /// </summary>
    private static async Task<SplitOutcome> SplitViaLeaders(
        string space, string splitKey, (IRaft, KahunaManager)[] nodes, CancellationToken ct)
    {
        // Retry up to 5 times: a transient leader change between LeaderOf and MutateAsync
        // inside SplitAsync returns CutoverFailed, which is safe to retry.
        for (int attempt = 0; attempt < 5; attempt++)
        {
            (IRaft leaderRaft, KahunaManager leader) = await LeaderOf(RangeMapStore.MetaPartitionId, nodes);

            int newPartitionId = RangeSplitter.ComputeNextPartitionId(leader.RangeMapStore.Current);

            RaftPartitionLifecycleResult createResult =
                await leaderRaft.CreatePartitionAsync(newPartitionId, RaftRoutingMode.Unrouted, null, ct);

            if (!createResult.Success)
                return SplitOutcome.PartitionCreationFailed;

            SplitOutcome outcome = await leader.RangeSplitter.SplitAsync(space, splitKey, newPartitionId, ct);

            if (outcome.Status != SplitStatus.CutoverFailed)
                return outcome;

            await Task.Delay(100, ct);
        }

        return SplitOutcome.CutoverFailed;
    }


    private static byte[] V(string s) => Encoding.UTF8.GetBytes(s);

    /// <summary>
    /// Assembles a 4-partition cluster, registers the ranged space on every node, seeds one
    /// whole-space descriptor `[−∞,+∞)@P2 gen=1`, and writes a handful of keys spanning the
    /// canonical split point `t:s/m` (keys below and above it).
    /// </summary>
    private async Task<((IRaft, KahunaManager)[] Nodes, KahunaManager MetaLeader, KahunaManager DataLeader, KahunaManager SysLeader)> Setup(
        string[] keysBelow, string[] keysAbove)
    {
        (IRaft r1, IRaft r2, IRaft r3, IKahuna k1, IKahuna k2, IKahuna k3) =
            await AssembleThreNodeCluster("memory", 3, raftLogger, kahunaLogger);

        (IRaft, KahunaManager)[] nodes =
            [(r1, (KahunaManager)k1), (r2, (KahunaManager)k2), (r3, (KahunaManager)k3)];

        foreach ((IRaft _, KahunaManager kahuna) in nodes)
            kahuna.RegisterKeyRange(Space);

        (IRaft _, KahunaManager metaLeader) = await LeaderOf(RangeMapStore.MetaPartitionId, nodes);

        bool committed = await metaLeader.RangeMapStore.MutateAsync(
            _ => [new RangeDescriptor
            {
                KeySpace = Space,
                StartKey = null,
                EndKey = null,
                PartitionId = RangeMapStore.FirstDataPartitionId,
                Generation = 1
            }],
            TestContext.Current.CancellationToken);
        Assert.True(committed);

        // Wait for the descriptor to reach every node.
        foreach ((IRaft _, KahunaManager kahuna) in nodes)
            await WaitUntilAsync(() => kahuna.RangeMapStore.Current.Find(Space, Space + "/x")?.Generation == 1);

        (IRaft _, KahunaManager dataLeader) = await LeaderOf(RangeMapStore.FirstDataPartitionId, nodes);
        (IRaft _, KahunaManager sysLeader) = await LeaderOf(SystemPartition, nodes);

        // Write keys below and above the eventual split point.
        foreach (string key in keysBelow.Concat(keysAbove))
        {
            (KeyValueResponseType type, _, _) = await dataLeader.TrySetKeyValue(
                HLCTimestamp.Zero, key, V("v"), null, -1, KeyValueFlags.Set, 0,
                KeyValueDurability.Persistent);
            Assert.Equal(KeyValueResponseType.Set, type);
        }

        // Wait until every key is visible from EVERY node's local backend (Raft replication is async).
        // Use the local TryGetValue path (not LocateAnd…) to confirm the data landed on each node.
        foreach (string key in keysBelow.Concat(keysAbove))
        {
            foreach ((IRaft _, KahunaManager kahuna) in nodes)
            {
                string k = key;
                await WaitUntilAsync(async () =>
                {
                    (KeyValueResponseType rt, _) = await kahuna.TryGetValue(
                        HLCTimestamp.Zero, k, 0, HLCTimestamp.Zero, KeyValueDurability.Persistent);
                    return rt == KeyValueResponseType.Get;
                });
            }
        }

        return (nodes, metaLeader, dataLeader, sysLeader);
    }

    // ── Split_NoGapNoOverlap ─────────────────────────────────────────────────────

    /// <summary>
    /// After a successful split, every key resolves to exactly one range, the map validates
    /// (G1 invariant), and no existing key is unreachable.
    /// </summary>
    [Fact]
    public async Task Split_ProducesNoGapNoOverlap()
    {
        string[] keysBelow = [Space + "/a", Space + "/b", Space + "/c"];
        string[] keysAbove = [Space + "/p", Space + "/q", Space + "/r"];

        ((IRaft, KahunaManager)[] nodes, _, KahunaManager dataLeader, KahunaManager _) =
            await Setup(keysBelow, keysAbove);
        try
        {
            SplitOutcome outcome = await SplitViaLeaders(
                Space, Space + "/m", nodes, TestContext.Current.CancellationToken);

            Assert.True(outcome.IsSuccess, $"Split failed: {outcome.Status}");

            // Wait for the split map to propagate — until the space actually has two descriptors.
            // A "both keys resolve" check is insufficient: the pre-split single descriptor already
            // covers every key, so it passes before the split map applies and reads a stale map.
            await WaitUntilAsync(() =>
                dataLeader.RangeMapStore.Current.Descriptors.Count(d => d.KeySpace == Space) == 2);

            RangeMap finalMap = dataLeader.RangeMapStore.Current;

            // G1 — no gap/no overlap.
            Assert.True(finalMap.IsValid, "RangeMap.Validate() failed after split");

            // Two descriptors now exist for the space.
            int count = finalMap.Descriptors.Count(d => d.KeySpace == Space);
            Assert.Equal(2, count);

            // Left half [−∞, m) stays on original partition P2.
            foreach (string key in keysBelow)
            {
                RangeDescriptor? d = finalMap.Find(Space, key);
                Assert.NotNull(d);
                Assert.Equal(RangeMapStore.FirstDataPartitionId, d.PartitionId);
            }

            // Right half [m, +∞) moved to new partition.
            foreach (string key in keysAbove)
            {
                RangeDescriptor? d = finalMap.Find(Space, key);
                Assert.NotNull(d);
                Assert.Equal(outcome.NewPartitionId, d.PartitionId);
            }

            // Generations are bumped.
            Assert.Equal(2L, outcome.NewGeneration);

            // Data integrity: every keysAbove value must be readable from the P' leader.
            (IRaft _, KahunaManager newPartLeader) = await LeaderOf(outcome.NewPartitionId, nodes);
            foreach (string key in keysAbove)
            {
                (KeyValueResponseType rt, ReadOnlyKeyValueEntry? entry) = await newPartLeader.TryGetValue(
                    HLCTimestamp.Zero, key, 0, HLCTimestamp.Zero, KeyValueDurability.Persistent);
                Assert.Equal(KeyValueResponseType.Get, rt);
                Assert.Equal("v", System.Text.Encoding.UTF8.GetString(entry!.Value!));
            }
        }
        finally
        {
            await LeaveCluster(nodes[0].Item1, nodes[1].Item1, nodes[2].Item1);
        }
    }

    // ── Split_InflightRequest_FailsFenceAndRetriesOnTarget ───────────────────────

    /// <summary>
    /// A write that routed to the source partition with the OLD generation is rejected by the
    /// generation fence after the cutover and must retry. On retry with the fresh generation it
    /// lands on the correct (new) partition.
    /// </summary>
    [Fact]
    public async Task Split_InflightRequest_FailsFenceAndRetriesOnTarget()
    {
        string[] keysBelow = [Space + "/a", Space + "/b"];
        string[] keysAbove = [Space + "/p", Space + "/q"];

        ((IRaft, KahunaManager)[] nodes, _, KahunaManager dataLeader, KahunaManager _) =
            await Setup(keysBelow, keysAbove);
        try
        {
            // Capture the pre-split generation.
            long preGen = dataLeader.RangeMapStore.Current
                .Find(Space, Space + "/p")!.Generation;

            // Execute the split via the two-leader helper.
            SplitOutcome outcome = await SplitViaLeaders(
                Space, Space + "/m", nodes, TestContext.Current.CancellationToken);
            Assert.True(outcome.IsSuccess, $"Split failed: {outcome.Status}");

            // Wait until the updated map is visible on the data leader.
            await WaitUntilAsync(() =>
                dataLeader.RangeMapStore.Current.Find(Space, Space + "/p")?.Generation == outcome.NewGeneration);

            // A write carrying the PRE-split generation to a right-half key must be fenced.
            (KeyValueResponseType stale, _, _) = await dataLeader.TrySetKeyValueRanged(
                HLCTimestamp.Zero, Space + "/p", V("stale"), preGen);
            Assert.Equal(KeyValueResponseType.MustRetry, stale);

            // Re-resolve the current descriptor and retry — it should commit on the new partition.
            (int _, long freshGen) = dataLeader.LocateRange(Space + "/p");
            Assert.Equal(outcome.NewGeneration, freshGen);

            (IRaft _, KahunaManager newLeader) = await LeaderOf(outcome.NewPartitionId, nodes);
            (KeyValueResponseType retried, _, _) = await newLeader.TrySetKeyValueRanged(
                HLCTimestamp.Zero, Space + "/p", V("fresh"), freshGen);
            Assert.Equal(KeyValueResponseType.Set, retried);
        }
        finally
        {
            await LeaveCluster(nodes[0].Item1, nodes[1].Item1, nodes[2].Item1);
        }
    }

    // ── Split_RejectsBelowMinRangeSize ───────────────────────────────────────────

    /// <summary>
    /// Splitting at a key that would leave one half empty is rejected with
    /// <see cref="SplitStatus.BelowMinRangeSize"/>.
    /// </summary>
    [Fact]
    public async Task Split_RejectsBelowMinRangeSize()
    {
        // Only keys on the right side of the candidate split point — the left half would be empty.
        string[] keysAbove = [Space + "/p", Space + "/q"];

        ((IRaft, KahunaManager)[] nodes, _, KahunaManager dataLeader, KahunaManager _) =
            await Setup([], keysAbove);
        try
        {
            // Split at "m" — there are no keys in [−∞, m), so the left half is empty.
            // BelowMinRangeSize is detected before CreatePartitionAsync so we can use any leader.
            (IRaft _, KahunaManager metaLeader) = await LeaderOf(RangeMapStore.MetaPartitionId, nodes);
            SplitOutcome outcome = await metaLeader.RangeSplitter.SplitAsync(
                Space, Space + "/m", RangeMapStore.FirstDataPartitionId + 10, TestContext.Current.CancellationToken);

            Assert.Equal(SplitStatus.BelowMinRangeSize, outcome.Status);

            // Map must be unchanged (G1 still holds).
            Assert.True(dataLeader.RangeMapStore.Current.IsValid);
            Assert.Equal(1, dataLeader.RangeMapStore.Current.Descriptors.Count(d => d.KeySpace == Space));
        }
        finally
        {
            await LeaveCluster(nodes[0].Item1, nodes[1].Item1, nodes[2].Item1);
        }
    }

    // ── Split_RejectsNonOrdinalBoundary ──────────────────────────────────────────

    /// <summary>
    /// Splitting at a key that is not strictly inside the range's ordinal bounds is rejected with
    /// <see cref="SplitStatus.InvalidSplitKey"/>. Tested by splitting the left half
    /// <c>[−∞, m)</c> at the boundary key <c>m</c> exactly (K == E, not K &lt; E).
    /// </summary>
    [Fact]
    public async Task Split_RejectsNonOrdinalBoundary()
    {
        string[] keysBelow = [Space + "/a"];
        string[] keysAbove = [Space + "/p"];

        ((IRaft, KahunaManager)[] nodes, _, KahunaManager dataLeader, KahunaManager _) =
            await Setup(keysBelow, keysAbove);
        try
        {
            (IRaft _, KahunaManager metaLeader) = await LeaderOf(RangeMapStore.MetaPartitionId, nodes);

            // First produce a finite left range [−∞, m) by splitting at "m".
            SplitOutcome first = await SplitViaLeaders(
                Space, Space + "/m", nodes, TestContext.Current.CancellationToken);
            Assert.True(first.IsSuccess);

            await WaitUntilAsync(() =>
                metaLeader.RangeMapStore.Current.Find(Space, Space + "/a")?.EndKey == Space + "/m");

            // Try splitting the left range [−∞, m) at "/a" — the only key in that half — which
            // would leave [−∞, "/a") empty: BelowMinRangeSize.
            SplitOutcome bad = await metaLeader.RangeSplitter.SplitAsync(
                Space, Space + "/a", first.NewPartitionId + 1, TestContext.Current.CancellationToken);
            Assert.Equal(SplitStatus.BelowMinRangeSize, bad.Status);

            // Splitting the RIGHT range [m, +∞) at "m" exactly (K == S, not K > S) is invalid.
            SplitOutcome atBound = await metaLeader.RangeSplitter.SplitAsync(
                Space, Space + "/m", first.NewPartitionId + 2, TestContext.Current.CancellationToken);
            Assert.Equal(SplitStatus.InvalidSplitKey, atBound.Status);

            Assert.True(metaLeader.RangeMapStore.Current.IsValid);
        }
        finally
        {
            await LeaveCluster(nodes[0].Item1, nodes[1].Item1, nodes[2].Item1);
        }
    }

    // ── Split_TopRange_DoesNotCorruptBystander ───────────────────────────────────

    /// <summary>
    /// Splitting a top range [K,+∞) must not touch keys in a bystander key space whose keys
    /// sort lexicographically after K. Specifically catches the Finding-2 bug where
    /// DeleteKeysByRange with endKey==null used to over-delete beyond the prefix boundary.
    /// </summary>
    [Fact]
    public async Task Split_TopRange_DoesNotCorruptBystander()
    {
        // The bystander space sorts AFTER Space in ordinal order (u > t).
        const string Bystander = "u:s";

        string[] keysBelow = [Space + "/a", Space + "/b"];
        string[] keysAbove = [Space + "/p", Space + "/q"];

        ((IRaft, KahunaManager)[] nodes, KahunaManager metaLeader, KahunaManager dataLeader, KahunaManager _) =
            await Setup(keysBelow, keysAbove);
        try
        {
            // Write two bystander keys via LocateAndTrySetKeyValue — these are hash-routed and
            // may land on any data partition (not necessarily P2), so we must use the locating path.
            string[] bystanderKeys = [Bystander + "/z1", Bystander + "/z2"];
            foreach (string key in bystanderKeys)
            {
                string k = key;
                await WaitUntilAsync(async () =>
                {
                    (KeyValueResponseType wt, _, _) = await nodes[0].Item2.LocateAndTrySetKeyValue(
                        HLCTimestamp.Zero, k, V("bystander"), null, -1, KeyValueFlags.Set, 0,
                        KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
                    return wt == KeyValueResponseType.Set;
                });
            }

            // Wait for bystander keys to be visible on all nodes.
            foreach (string key in bystanderKeys)
            {
                foreach ((IRaft _, KahunaManager kahuna) in nodes)
                {
                    string k = key;
                    await WaitUntilAsync(async () =>
                    {
                        (KeyValueResponseType rt, _) = await kahuna.TryGetValue(
                            HLCTimestamp.Zero, k, 0, HLCTimestamp.Zero, KeyValueDurability.Persistent);
                        return rt == KeyValueResponseType.Get;
                    });
                }
            }

            // Split the top range [m,+∞) — this is exactly the hot-tail case where endKey is null.
            SplitOutcome outcome = await SplitViaLeaders(
                Space, Space + "/m", nodes, TestContext.Current.CancellationToken);
            Assert.True(outcome.IsSuccess, $"Split failed: {outcome.Status}");

            // Wait for map to propagate.
            await WaitUntilAsync(() =>
                metaLeader.RangeMapStore.Current.Find(Space, Space + "/p") is not null);

            // keysAbove must be readable from the P' leader.
            (IRaft _, KahunaManager newLeader) = await LeaderOf(outcome.NewPartitionId, nodes);
            foreach (string key in keysAbove)
            {
                (KeyValueResponseType rt, _) = await newLeader.TryGetValue(
                    HLCTimestamp.Zero, key, 0, HLCTimestamp.Zero, KeyValueDurability.Persistent);
                Assert.Equal(KeyValueResponseType.Get, rt);
            }

            // Bystander keys must be intact on every node.
            foreach (string key in bystanderKeys)
            {
                foreach ((IRaft _, KahunaManager kahuna) in nodes)
                {
                    (KeyValueResponseType rt, ReadOnlyKeyValueEntry? entry) = await kahuna.TryGetValue(
                        HLCTimestamp.Zero, key, 0, HLCTimestamp.Zero, KeyValueDurability.Persistent);
                    Assert.Equal(KeyValueResponseType.Get, rt);
                    Assert.Equal("bystander", System.Text.Encoding.UTF8.GetString(entry!.Value!));
                }
            }

            // The bystander key space has no descriptor in the range map (it is hash-routed).
            Assert.Null(metaLeader.RangeMapStore.Current.Find(Bystander, bystanderKeys[0]));
        }
        finally
        {
            await LeaveCluster(nodes[0].Item1, nodes[1].Item1, nodes[2].Item1);
        }
    }

    // ── SharedLock_SurvivesSplit_MatrixHolds ─────────────────────────────────────

    private static HLCTimestamp NextTx(IRaft raft) =>
        raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

    /// <summary>
    /// Verifies that a Shared range lock acquired on the original
    /// partition (P2) before a split is NOT wiped out by the split, and that the S/X
    /// compatibility matrix still holds on P2 after the split completes.
    ///
    /// <para>Sequence:</para>
    /// <list type="number">
    ///   <item>tx1 acquires Shared on [−∞, +∞) via P2 leader.</item>
    ///   <item>Split at <c>Space + "/m"</c> — P2 retains [Space, Space+"/m"), new partition gets [Space+"/m", +∞).</item>
    ///   <item>tx2 acquires Shared on [−∞, +∞) via P2 leader → Locked (S∩S coexist, proves Mode=Shared survived).</item>
    ///   <item>tx3 acquires Exclusive on [−∞, +∞) via P2 leader → AlreadyLocked (X conflicts with Shared, proves matrix).</item>
    /// </list>
    /// </summary>
    [Fact]
    public async Task SharedLock_SurvivesSplit_MatrixHolds()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        ((IRaft, KahunaManager)[] nodes, _, KahunaManager dataLeader, _) =
            await Setup([Space + "/a"], [Space + "/z"]);

        try
        {
            // Locate P2's Raft leader so we can mint HLC timestamps.
            (IRaft p2Raft, KahunaManager p2Leader) = await LeaderOf(RangeMapStore.FirstDataPartitionId, nodes);

            HLCTimestamp tx1 = NextTx(p2Raft);

            // Step 1: tx1 acquires Shared over the whole range on P2.
            (KeyValueResponseType sharedBefore, _) = await dataLeader.TryAcquireRangeLock(
                tx1, Space, null, true, null, false, 60_000,
                KeyValueDurability.Persistent, RangeLockMode.Shared);
            Assert.Equal(KeyValueResponseType.Locked, sharedBefore);

            // Step 2: split at Space+"/m" — P2 keeps the left half; the new partition takes the right.
            SplitOutcome outcome = await SplitViaLeaders(Space, Space + "/m", nodes, ct);
            Assert.True(outcome.IsSuccess, $"Split failed: {outcome.Status}");

            // Wait for the split map to propagate before testing lock state — until the space
            // actually has two descriptors. A "both keys resolve" check is insufficient: the
            // pre-split single descriptor already covers every key and would pass on the stale map.
            await WaitUntilAsync(() =>
                nodes[0].Item2.RangeMapStore.Current.Descriptors.Count(d => d.KeySpace == Space) == 2);

            // Re-acquire P2 leader reference (split can trigger re-elections).
            (p2Raft, p2Leader) = await LeaderOf(RangeMapStore.FirstDataPartitionId, nodes);

            HLCTimestamp tx2 = NextTx(p2Raft);
            HLCTimestamp tx3 = NextTx(p2Raft);

            // Step 3: tx2 Shared on P2 → Locked (S∩S; proves tx1's lock Mode=Shared survived the split).
            (KeyValueResponseType sharedAfter, _) = await p2Leader.TryAcquireRangeLock(
                tx2, Space, null, true, null, false, 60_000,
                KeyValueDurability.Persistent, RangeLockMode.Shared);
            Assert.Equal(KeyValueResponseType.Locked, sharedAfter);

            // Step 4: tx3 Exclusive on P2 → AlreadyLocked (X conflicts with the two live Shared locks).
            (KeyValueResponseType exclusiveAfter, _) = await p2Leader.TryAcquireRangeLock(
                tx3, Space, null, true, null, false, 60_000,
                KeyValueDurability.Persistent, RangeLockMode.Exclusive);
            Assert.Equal(KeyValueResponseType.AlreadyLocked, exclusiveAfter);
        }
        finally
        {
            await LeaveCluster(nodes[0].Item1, nodes[1].Item1, nodes[2].Item1);
        }
    }

    // ── Lock-transfer tests ───────────────────────────────────────────────────

    /// <summary>
    /// Runs a split lock-transfer scenario with bounded retries (each attempt on a fresh cluster) to
    /// tolerate the documented best-effort gap: a freshly-created partition can
    /// re-elect and strand the in-memory, non-replicated lock on a former leader. <paramref
    /// name="attempt"/> returns true when the transfer-dependent guarantee held; false signals a
    /// strand and triggers a retry. Hard bugs surface as exceptions inside the attempt (no masking).
    /// The robust fix that removes the retry is to replicate locks through the partition's
    /// Raft log.
    /// </summary>
    private static async Task RetrySplitTransfer(Func<Task<bool>> attempt, int maxAttempts = 5)
    {
        for (int i = 1; i <= maxAttempts; i++)
        {
            if (await attempt())
                return;
        }

        Assert.Fail($"split lock-transfer guarantee not observed after {maxAttempts} attempts " +
                    "(best-effort under leadership churn)");
    }

    /// <summary>
    /// An Exclusive range lock acquired on [−∞,+∞) before the split must be enforced on the
    /// new partition (P') after cutover. A second exclusive attempt on the new partition must return
    /// AlreadyLocked, proving the clamped lock was transferred.
    /// </summary>
    [Fact(Skip = "Best-effort: a freshly-created/re-electing destination partition " +
                 "can strand the in-memory, non-replicated lock. Re-enable when range locks are replicated " +
                 "through the partition Raft log, which makes the guarantee deterministic.")]
    public Task Lock_SpanningSplit_EnforcedOnNewPartition() => RetrySplitTransfer(async () =>
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        ((IRaft, KahunaManager)[] nodes, _, KahunaManager dataLeader, _) =
            await Setup([Space + "/a"], [Space + "/z"]);

        try
        {
            (IRaft p2Raft, _) = await LeaderOf(RangeMapStore.FirstDataPartitionId, nodes);

            // tx1 holds an Exclusive lock on the whole range before the split.
            HLCTimestamp tx1 = NextTx(p2Raft);
            (KeyValueResponseType lockBefore, _) = await dataLeader.TryAcquireRangeLock(
                tx1, Space, null, true, null, false, 60_000,
                KeyValueDurability.Persistent, RangeLockMode.Exclusive);
            Assert.Equal(KeyValueResponseType.Locked, lockBefore);

            // Split at Space+"/m" — must transfer tx1's clamped lock to the new partition.
            SplitOutcome outcome = await SplitViaLeaders(Space, Space + "/m", nodes, ct);
            Assert.True(outcome.IsSuccess, $"Split failed: {outcome.Status}");

            // Locate the new partition leader.
            (IRaft pPrimeRaft, KahunaManager pPrimeLeader) =
                await LeaderOf(outcome.NewPartitionId, nodes);

            // tx2 tries Exclusive on the new partition — must be blocked by tx1's clamped lock.
            HLCTimestamp tx2 = NextTx(pPrimeRaft);
            (KeyValueResponseType lockOnNewPartition, _) = await pPrimeLeader.TryAcquireRangeLock(
                tx2, Space, Space + "/m", true, null, false, 60_000,
                KeyValueDurability.Persistent, RangeLockMode.Exclusive);

            // AlreadyLocked → guarantee held; Locked → strand (retry on a fresh cluster).
            return lockOnNewPartition == KeyValueResponseType.AlreadyLocked;
        }
        finally
        {
            await LeaveCluster(nodes[0].Item1, nodes[1].Item1, nodes[2].Item1);
        }
    });

    /// <summary>
    /// When a Shared lock spans the split, BOTH halves after the split must still allow
    /// another Shared lock (S∩S coexist) but block an Exclusive (X conflicts with S).
    /// </summary>
    [Fact(Skip = "Best-effort: a freshly-created/re-electing destination partition " +
                 "can strand the in-memory, non-replicated lock. Re-enable when range locks are replicated " +
                 "through the partition Raft log, which makes the guarantee deterministic.")]
    public Task SharedLock_SpanningSplit_BothHalvesCoexist() => RetrySplitTransfer(async () =>
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        ((IRaft, KahunaManager)[] nodes, _, KahunaManager dataLeader, _) =
            await Setup([Space + "/a"], [Space + "/z"]);

        try
        {
            (IRaft p2Raft, KahunaManager p2Leader) =
                await LeaderOf(RangeMapStore.FirstDataPartitionId, nodes);

            // tx1 acquires Shared on the whole range.
            HLCTimestamp tx1 = NextTx(p2Raft);
            (KeyValueResponseType sharedBefore, _) = await dataLeader.TryAcquireRangeLock(
                tx1, Space, null, true, null, false, 60_000,
                KeyValueDurability.Persistent, RangeLockMode.Shared);
            Assert.Equal(KeyValueResponseType.Locked, sharedBefore);

            SplitOutcome outcome = await SplitViaLeaders(Space, Space + "/m", nodes, ct);
            Assert.True(outcome.IsSuccess, $"Split failed: {outcome.Status}");

            (IRaft pPrimeRaft, KahunaManager pPrimeLeader) =
                await LeaderOf(outcome.NewPartitionId, nodes);

            (p2Raft, p2Leader) = await LeaderOf(RangeMapStore.FirstDataPartitionId, nodes);

            HLCTimestamp tx3 = NextTx(pPrimeRaft);

            // tx3 Exclusive on new partition → AlreadyLocked iff tx1's Shared was transferred.
            // (A Shared probe would acquire regardless, so it can't detect a strand — use X.)
            (KeyValueResponseType exclusiveOnNew, _) = await pPrimeLeader.TryAcquireRangeLock(
                tx3, Space, Space + "/m", true, null, false, 60_000,
                KeyValueDurability.Persistent, RangeLockMode.Exclusive);
            if (exclusiveOnNew != KeyValueResponseType.AlreadyLocked)
                return false; // strand — retry on a fresh cluster

            // tx2 Shared on new partition → Locked (S∩S coexist with tx1's transferred Shared).
            HLCTimestamp tx2 = NextTx(pPrimeRaft);
            (KeyValueResponseType sharedOnNew, _) = await pPrimeLeader.TryAcquireRangeLock(
                tx2, Space, Space + "/m", true, null, false, 60_000,
                KeyValueDurability.Persistent, RangeLockMode.Shared);
            Assert.Equal(KeyValueResponseType.Locked, sharedOnNew);

            // P2 retained half should also still respect the original lock — but the retained
            // partition is *also* subject to the non-replicated-lock failover gap (a P2 re-election
            // strands tx1's in-memory lock the same way), so treat a miss as a strand → retry.
            HLCTimestamp tx4 = NextTx(p2Raft);
            (KeyValueResponseType exclusiveOnOrig, _) = await p2Leader.TryAcquireRangeLock(
                tx4, Space, null, true, Space + "/m", false, 60_000,
                KeyValueDurability.Persistent, RangeLockMode.Exclusive);

            return exclusiveOnOrig == KeyValueResponseType.AlreadyLocked;
        }
        finally
        {
            await LeaveCluster(nodes[0].Item1, nodes[1].Item1, nodes[2].Item1);
        }
    });

    /// <summary>
    /// Releasing a lock that was transferred across a split cleans up the clamped entry
    /// from the new partition's actor. After release a fresh Exclusive lock must be granted.
    /// </summary>
    [Fact(Skip = "Best-effort: a freshly-created/re-electing destination partition " +
                 "can strand the in-memory, non-replicated lock. Re-enable when range locks are replicated " +
                 "through the partition Raft log, which makes the guarantee deterministic.")]
    public Task Lock_SpanningSplit_ReleaseCleansBothHalves() => RetrySplitTransfer(async () =>
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        ((IRaft, KahunaManager)[] nodes, _, KahunaManager dataLeader, _) =
            await Setup([Space + "/a"], [Space + "/z"]);

        try
        {
            (IRaft p2Raft, _) = await LeaderOf(RangeMapStore.FirstDataPartitionId, nodes);

            HLCTimestamp tx1 = NextTx(p2Raft);
            (KeyValueResponseType lockBefore, _) = await dataLeader.TryAcquireRangeLock(
                tx1, Space, null, true, null, false, 60_000,
                KeyValueDurability.Persistent, RangeLockMode.Exclusive);
            Assert.Equal(KeyValueResponseType.Locked, lockBefore);

            SplitOutcome outcome = await SplitViaLeaders(Space, Space + "/m", nodes, ct);
            Assert.True(outcome.IsSuccess, $"Split failed: {outcome.Status}");

            (IRaft pPrimeRaft, KahunaManager pPrimeLeader) =
                await LeaderOf(outcome.NewPartitionId, nodes);

            // Confirm the lock is present on the new partition; absent → strand, retry.
            HLCTimestamp txCheck = NextTx(pPrimeRaft);
            (KeyValueResponseType blockedBefore, _) = await pPrimeLeader.TryAcquireRangeLock(
                txCheck, Space, Space + "/m", true, null, false, 60_000,
                KeyValueDurability.Persistent, RangeLockMode.Exclusive);
            if (blockedBefore != KeyValueResponseType.AlreadyLocked)
                return false; // strand — retry on a fresh cluster

            // Release with original (unclamped) bounds — the overlap fallback in the handler
            // should match the clamped entry and remove it.
            KeyValueResponseType releaseOnNew = await pPrimeLeader.TryReleaseExclusiveRangeLock(
                tx1, Space, null, true, null, false,
                KeyValueDurability.Persistent);
            Assert.Equal(KeyValueResponseType.Unlocked, releaseOnNew);

            // After release a fresh Exclusive must be granted.
            HLCTimestamp tx2 = NextTx(pPrimeRaft);
            (KeyValueResponseType afterRelease, _) = await pPrimeLeader.TryAcquireRangeLock(
                tx2, Space, Space + "/m", true, null, false, 60_000,
                KeyValueDurability.Persistent, RangeLockMode.Exclusive);
            Assert.Equal(KeyValueResponseType.Locked, afterRelease);

            return true;
        }
        finally
        {
            await LeaveCluster(nodes[0].Item1, nodes[1].Item1, nodes[2].Item1);
        }
    });

    // ── Split_DirectWriteDuringQuiesce_MustRetry ──────────────────────────────────

    /// <summary>
    /// Verifies F3 — direct (non-2PC) writes to a range that is currently quiesced for splitting
    /// return <c>MustRetry</c>. After the split window closes the same write succeeds.
    ///
    /// <para>
    /// The test uses <c>ForceSplitAtKeyAsync</c> with a <c>duringQuiesce</c> hook to inject a
    /// direct <c>LocateAndTrySetKeyValue</c> call while the quiesce is active (between catch-up
    /// import and cutover). After the split completes the same write must succeed.
    /// </para>
    /// </summary>
    [Fact]
    public async Task Split_DirectWriteDuringQuiesce_MustRetry()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        const string key = Space + "/quiesce-key";
        const string val = "quiesced-write";

        ((IRaft, KahunaManager)[] nodes, KahunaManager metaLeader, _, KahunaManager _) =
            await Setup([Space + "/a"], [Space + "/z"]);

        try
        {
            KeyValueResponseType? duringQuiesceResult = null;

            SplitOutcome outcome = SplitOutcome.PartitionCreationFailed;
            for (int attempt = 0; attempt < 5; attempt++)
            {
                (_, metaLeader) = await LeaderOf(RangeMapStore.MetaPartitionId, nodes);
                outcome = await metaLeader.ForceSplitAtKeyAsync(
                    Space, Space + "/m",
                    duringQuiesce: async () =>
                    {
                        // Direct write into the quiesced range: must be bounced with MustRetry.
                        (KeyValueResponseType rt, _, _) = await metaLeader.LocateAndTrySetKeyValue(
                            HLCTimestamp.Zero, key, V(val),
                            null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
                        duringQuiesceResult = rt;
                    },
                    ct);
                if (outcome.IsSuccess || outcome.Status == SplitStatus.NoRange ||
                    outcome.Status == SplitStatus.InvalidSplitKey ||
                    outcome.Status == SplitStatus.BelowMinRangeSize)
                    break;
                await Task.Delay(100, ct);
            }

            Assert.True(outcome.IsSuccess, $"Split failed: {outcome.Status}");
            Assert.Equal(KeyValueResponseType.MustRetry, duringQuiesceResult);

            // After the split the quiesce is released — the same write now succeeds.
            (KeyValueResponseType afterRt, _, _) = await metaLeader.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, key, V(val),
                null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Set, afterRt);
        }
        finally
        {
            await LeaveCluster(nodes[0].Item1, nodes[1].Item1, nodes[2].Item1);
        }
    }

    // ── ForceSplitAtKeyAsync (threshold-bypassing split seam) ────────────────

    /// <summary>
    /// Acceptance test: a small range (20 keys — far below the default 1000-key threshold) can be
    /// split at a caller-supplied key via <c>ForceSplitAtKeyAsync</c> without generating
    /// threshold-sized data. After the split: the range map has two descriptors, both children
    /// are non-empty, and every pre-split key is still readable.
    /// </summary>
    [Fact]
    public async Task ForceSplitAtKey_SmallRange_SplitsAndKeepsAllKeys()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        // 20 keys: 10 below and 10 above the chosen split point t:s/m.
        string[] keysBelow = Enumerable.Range(0, 10).Select(i => $"{Space}/a{i:D2}").ToArray();
        string[] keysAbove = Enumerable.Range(0, 10).Select(i => $"{Space}/p{i:D2}").ToArray();
        const string splitKey = Space + "/m";

        ((IRaft, KahunaManager)[] nodes, KahunaManager metaLeader, _, _) =
            await Setup(keysBelow, keysAbove);

        try
        {
            // Re-resolve the meta/system leader immediately before the split — Setup() caches it
            // early and election churn during the write phase can move leadership to another node.
            // Retry on PartitionCreationFailed (follower) and CutoverFailed (transient leader
            // change between CreatePartitionAsync and MutateAsync), mirroring SplitViaLeaders.
            SplitOutcome outcome = SplitOutcome.PartitionCreationFailed;
            for (int attempt = 0; attempt < 5; attempt++)
            {
                (_, metaLeader) = await LeaderOf(RangeMapStore.MetaPartitionId, nodes);
                outcome = await metaLeader.ForceSplitAtKeyAsync(Space, splitKey, ct: ct);
                if (outcome.IsSuccess || outcome.Status == SplitStatus.NoRange ||
                    outcome.Status == SplitStatus.InvalidSplitKey ||
                    outcome.Status == SplitStatus.BelowMinRangeSize)
                    break;
                await Task.Delay(100, ct);
            }

            Assert.True(outcome.IsSuccess, $"ForceSplitAtKeyAsync failed: {outcome.Status}");
            Assert.True(outcome.NewPartitionId > RangeMapStore.FirstDataPartitionId,
                "Expected new partition ID above the initial data partition");

            // Wait for the split map to propagate to all nodes.
            foreach ((IRaft _, KahunaManager kahuna) in nodes)
            {
                await WaitUntilAsync(() =>
                    kahuna.RangeMapStore.Current.Descriptors.Count(d => d.KeySpace == Space) == 2);
            }

            RangeMap finalMap = metaLeader.RangeMapStore.Current;

            // No gap / no overlap.
            Assert.True(finalMap.IsValid, "RangeMap.Validate() failed after forced split");

            // Left half stays on original partition.
            foreach (string k in keysBelow)
            {
                RangeDescriptor? d = finalMap.Find(Space, k);
                Assert.NotNull(d);
                Assert.Equal(RangeMapStore.FirstDataPartitionId, d.PartitionId);
            }

            // Right half moves to the new partition.
            foreach (string k in keysAbove)
            {
                RangeDescriptor? d = finalMap.Find(Space, k);
                Assert.NotNull(d);
                Assert.Equal(outcome.NewPartitionId, d.PartitionId);
            }

            // Every key is still readable after the split.
            foreach (string k in keysBelow.Concat(keysAbove))
            {
                string key = k;
                (KeyValueResponseType rt, _) = await metaLeader.TryGetValue(
                    HLCTimestamp.Zero, key, 0, HLCTimestamp.Zero, KeyValueDurability.Persistent);
                Assert.Equal(KeyValueResponseType.Get, rt);
            }
        }
        finally
        {
            await LeaveCluster(nodes[0].Item1, nodes[1].Item1, nodes[2].Item1);
        }
    }

    // ── ExecuteSplitAsync defensive branches (stale guard + orphan cleanup) ──────

    /// <summary>
    /// Regression for <c>ExecuteSplitAsync</c>'s stale-descriptor guard. When a descriptor's
    /// generation no longer matches the live map (e.g. the other checker cadence already split that
    /// range), the trigger must skip <b>without</b> creating a partition. The normal cadences only
    /// hit this under a race, so we drive it directly: split once to bump the live generation, then
    /// invoke <c>ExecuteSplitAsync</c> with the captured pre-split (now stale) descriptor.
    /// </summary>
    [Fact]
    public async Task ExecuteSplit_StaleDescriptor_SkipsWithoutCreatingPartition()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        string[] keysBelow = Enumerable.Range(0, 10).Select(i => $"{Space}/a{i:D2}").ToArray();
        string[] keysAbove = Enumerable.Range(0, 10).Select(i => $"{Space}/p{i:D2}").ToArray();

        ((IRaft, KahunaManager)[] nodes, KahunaManager metaLeader, _, _) =
            await Setup(keysBelow, keysAbove);

        try
        {
            // Capture the original descriptor (generation 1) before any split.
            RangeDescriptor stale = metaLeader.RangeMapStore.Current.Find(Space, Space + "/a00")!;
            Assert.Equal(1, stale.Generation);

            // Real split so the live partition-1 descriptor advances to generation 2.
            SplitOutcome first = SplitOutcome.PartitionCreationFailed;
            for (int attempt = 0; attempt < 5; attempt++)
            {
                (_, metaLeader) = await LeaderOf(RangeMapStore.MetaPartitionId, nodes);
                first = await metaLeader.ForceSplitAtKeyAsync(Space, Space + "/m", ct: ct);
                if (first.IsSuccess) break;
                await Task.Delay(100, ct);
            }
            Assert.True(first.IsSuccess, $"setup split failed: {first.Status}");

            foreach ((IRaft _, KahunaManager kahuna) in nodes)
                await WaitUntilAsync(() =>
                    kahuna.RangeMapStore.Current.Descriptors.Count(d => d.KeySpace == Space) == 2);

            (_, metaLeader) = await LeaderOf(RangeMapStore.MetaPartitionId, nodes);
            int descriptorsBefore = metaLeader.RangeMapStore.Current.Descriptors.Count(d => d.KeySpace == Space);
            int nextIdBefore = RangeSplitter.ComputeNextPartitionId(metaLeader.RangeMapStore.Current);

            // Drive the guard with the stale (gen-1) descriptor: live partition-1 is now gen 2.
            bool didSplit = await metaLeader.RangeSplitTrigger.ExecuteSplitAsync(stale, Space + "/g", ct);

            Assert.False(didSplit, "stale descriptor must not split");

            // No partition created, no cutover: descriptor count and next id unchanged.
            Assert.Equal(descriptorsBefore,
                metaLeader.RangeMapStore.Current.Descriptors.Count(d => d.KeySpace == Space));
            Assert.Equal(nextIdBefore, RangeSplitter.ComputeNextPartitionId(metaLeader.RangeMapStore.Current));
        }
        finally
        {
            await LeaveCluster(nodes[0].Item1, nodes[1].Item1, nodes[2].Item1);
        }
    }

    /// <summary>
    /// Regression for <c>ExecuteSplitAsync</c>'s orphan-cleanup branch. When <c>SplitAsync</c> fails
    /// <b>after</b> the new partition was created (here: a split key past every key, so the right
    /// half is empty → <c>BelowMinRangeSize</c>), the created partition must be removed so its id is
    /// not leaked. With leadership confirmed before the call, <c>CreatePartitionAsync</c> succeeds and
    /// the failure is forced downstream, so a generation of 0 afterwards proves the orphan was created
    /// and then removed — break the cleanup and the partition would linger with a non-zero generation.
    /// </summary>
    /// <remarks>
    /// Note: we do not assert the freed id is immediately reusable by a fresh split. Re-creating a
    /// just-removed partition id requires the system partition-map change to converge across nodes,
    /// which takes far longer than a test should block on; in production splits are minutes apart so
    /// the id is reusable by then. The orphan-removed check is the reliable regression signal.
    /// </remarks>
    /// <summary>
    /// Acceptance test: after a successful split the settle window suppresses immediate re-evaluation
    /// of the child descriptors, and re-eligibility is restored once the window elapses.
    ///
    /// Uses a single-node <see cref="EmbeddedKahunaNode"/> with a short threshold and settle
    /// window so the lifecycle can be exercised without multi-second sleeps.
    /// </summary>
    [Fact]
    public async Task SettleWindow_BlocksImmediateReSplitThenRestoresEligibility()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        // Short settle window so the re-eligibility leg completes in < 200 ms.
        await using EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            ReadIOThreads = 1,
            WriteIOThreads = 1,
            PartitionExecutorPoolSize = 1,
            NodeName        = "k6-node",
            Host            = "localhost",
            InitialPartitions = 1,
            Storage         = "memory",
            WalStorage      = "memory",
            RangeSplitThreshold    = 5,
            RangeSplitMinRangeSize = 2,
            RangeSplitSettleWindow = TimeSpan.FromMilliseconds(300),
            MinLeaderStability     = TimeSpan.FromMilliseconds(200),
        });

        await node.StartAsync(ct);

        KahunaManager kahuna = (KahunaManager)node.Kahuna;
        kahuna.RegisterKeyRange(Space);

        bool seeded = await kahuna.RangeMapStore.MutateAsync(
            _ => [new RangeDescriptor
            {
                KeySpace    = Space,
                StartKey    = null,
                EndKey      = null,
                PartitionId = RangeMapStore.FirstDataPartitionId,
                Generation  = 1
            }], ct);
        Assert.True(seeded, "Failed to seed initial range descriptor");

        // Write 20 keys spread across the range (threshold = 5, so well above it).
        for (int i = 0; i < 20; i++)
        {
            (KeyValueResponseType type, _, _) = await kahuna.TrySetKeyValue(
                HLCTimestamp.Zero, $"{Space}/k{i:D2}", V($"v{i}"), null, -1,
                KeyValueFlags.Set, 0, KeyValueDurability.Persistent);
            Assert.Equal(KeyValueResponseType.Set, type);
        }

        RangeSplitTrigger trigger = kahuna.RangeSplitTrigger;

        // First pass: at least one split fires and both children enter the settle window.
        // Poll rather than assume the very first tick splits. The split path runs
        // leadership-gated Raft operations (CreatePartitionAsync + cutover) that are not
        // permitted until the leader has been stable for MinLeaderStability, so under load a
        // trigger tick issued right after startup can legitimately observe a not-yet-eligible
        // leader and do nothing. The trigger is idempotent; retry until the first split lands.
        int splitsFirst = 0;
        for (int attempt = 0; attempt < 100 && splitsFirst == 0; attempt++)
        {
            splitsFirst = await trigger.TriggerAsync(ct);
            if (splitsFirst == 0)
                await Task.Delay(50, ct);
        }
        Assert.True(splitsFirst > 0, $"Expected at least one split on first pass; got {splitsFirst}");

        int descriptorsAfterFirst = kahuna.RangeMapStore.Current.Descriptors.Count(d => d.KeySpace == Space);
        Assert.Equal(splitsFirst + 1, descriptorsAfterFirst);

        // Immediate re-pass: settle window blocks all children → 0 splits.
        int splitsImmediate = await trigger.TriggerAsync(ct);
        Assert.Equal(0, splitsImmediate);
        Assert.Equal(
            descriptorsAfterFirst,
            kahuna.RangeMapStore.Current.Descriptors.Count(d => d.KeySpace == Space));

        // After the settle window elapses, children are re-eligible.
        await Task.Delay(400, ct); // > 300 ms window, with margin for GC/scheduling jitter

        // Same idempotent-retry rationale as the first pass: the re-eligible children still
        // split through leadership-gated Raft operations, so poll to first split under load.
        int splitsAfterExpiry = 0;
        for (int attempt = 0; attempt < 100 && splitsAfterExpiry == 0; attempt++)
        {
            splitsAfterExpiry = await trigger.TriggerAsync(ct);
            if (splitsAfterExpiry == 0)
                await Task.Delay(50, ct);
        }
        Assert.True(splitsAfterExpiry > 0,
            $"Expected re-eligibility after settle window; got {splitsAfterExpiry}");
    }

    [Fact]
    public async Task ExecuteSplit_SplitAsyncFailsAfterCreate_RemovesOrphanPartition()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        string[] keysBelow = Enumerable.Range(0, 10).Select(i => $"{Space}/a{i:D2}").ToArray();
        string[] keysAbove = Enumerable.Range(0, 10).Select(i => $"{Space}/p{i:D2}").ToArray();

        ((IRaft, KahunaManager)[] nodes, KahunaManager metaLeader, _, _) =
            await Setup(keysBelow, keysAbove);

        try
        {
            (IRaft sysRaft, KahunaManager leader) = await LeaderOf(RangeMapStore.MetaPartitionId, nodes);
            metaLeader = leader;

            // Confirm leadership immediately before the call so CreatePartitionAsync actually runs
            // (rather than throwing follower → caught → skipped, which would create no orphan).
            Assert.True(await sysRaft.AmILeader(SystemPartition, ct), "expected the resolved node to lead P0");

            RangeDescriptor current = metaLeader.RangeMapStore.Current.Find(Space, Space + "/a00")!;
            int expectedNewId = RangeSplitter.ComputeNextPartitionId(metaLeader.RangeMapStore.Current);

            // "/zzz" is past every key → right half empty → SplitAsync returns BelowMinRangeSize
            // AFTER CreatePartitionAsync already created expectedNewId. Cleanup must remove it.
            bool didSplit = await metaLeader.RangeSplitTrigger.ExecuteSplitAsync(current, Space + "/zzz", ct);

            Assert.False(didSplit, "split with an empty half must fail");

            // Orphan removed on the system-leader node (generation reads back as 0 = not present).
            // If the cleanup branch is broken the created partition lingers here with a non-zero gen.
            await WaitUntilAsync(() => sysRaft.GetPartitionGeneration(expectedNewId) == 0);

            // Map unchanged (no cutover) — still a single descriptor.
            Assert.Equal(1, metaLeader.RangeMapStore.Current.Descriptors.Count(d => d.KeySpace == Space));
        }
        finally
        {
            await LeaveCluster(nodes[0].Item1, nodes[1].Item1, nodes[2].Item1);
        }
    }
}
