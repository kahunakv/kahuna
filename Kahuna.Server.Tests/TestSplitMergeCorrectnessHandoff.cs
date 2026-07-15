using System.Text;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Data;
using Kommander.System;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// Acceptance tests for the replicated, cutover-gating split/merge handoff of correctness metadata —
/// persistent-participant completion receipts and coordinator decision records. These assert the two
/// guarantees a memory-only import to the current leader cannot provide:
/// <list type="number">
///   <item>the handoff is committed to the destination partition's Raft log, so <b>every replica</b> of
///     the destination range holds it and a destination-leader change right after cutover preserves it; and</item>
///   <item>a handoff that cannot be made durable <b>aborts cutover</b> — a live split or merge fails instead
///     of retiring the source range with the only outstanding receipt/decision lost.</item>
/// </list>
/// The transfer-plumbing tests (<see cref="TestCompletionReceiptTransfer"/> / <see cref="TestCoordinatorDecisionTransfer"/>)
/// only check that the routing reaches the destination leader; these check what happens on the other replicas
/// and on failure.
/// </summary>
[Collection("ClusterTests")]
public sealed class TestSplitMergeCorrectnessHandoff : BaseCluster
{
    private const int Pid = RangeMapStore.FirstDataPartitionId;

    private const string SplitKey = "k:m";
    private const string MovedLow = "k:mmm";  // >= SplitKey — moves
    private const string MovedHigh = "k:zzz"; // >= SplitKey — moves

    private readonly ILogger<IRaft> raftLogger;
    private readonly ILogger<IKahuna> kahunaLogger;

    public TestSplitMergeCorrectnessHandoff(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = LoggerFactory.Create(builder =>
            builder.AddXUnit(outputHelper).SetMinimumLevel(LogLevel.Warning));

        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    // ── helpers ────────────────────────────────────────────────────────────────────────────────

    private static HLCTimestamp NextTx(IRaft raft) =>
        raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

    private static byte[] V(string s) => Encoding.UTF8.GetBytes(s);

    private static CoordinatorDecisionRecord Decision(HLCTimestamp txId, string anchor, CoordinatorDecisionStatus status)
    {
        List<CoordinatorParticipant> participants =
            [new CoordinatorParticipant(anchor, KeyValueDurability.Persistent, HLCTimestamp.Zero, false, false)];
        return new CoordinatorDecisionRecord(txId, anchor, anchor, txId, status, participants, [], txId, HLCTimestamp.Zero);
    }

    private static async Task<(IRaft Raft, KahunaManager Kahuna)> LeaderOf(int partition, (IRaft, KahunaManager)[] nodes)
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

    /// <summary>Forces <paramref name="target"/> to lead the meta/system partition (P0) — required for cutover.</summary>
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

    // ── 1. Receipt handoff reaches every destination replica ─────────────────────────────────────

    /// <summary>
    /// A split/merge receipt handoff is replicated onto the destination partition's Raft log, so it lands on
    /// <b>every</b> replica of that partition — not just the leader that ran the import. The receipts are
    /// seeded only on the source node (a direct inject, no commit replication), so the only way they can reach
    /// the other two nodes is the replicated handoff. A memory-only import would leave the followers empty.
    /// </summary>
    [Fact]
    public async Task ReceiptHandoff_ReplicatesToEveryDestinationReplica()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft r1, IRaft r2, IRaft r3, IKahuna k1, IKahuna k2, IKahuna k3) =
            await AssembleThreNodeCluster("memory", 3, raftLogger, kahunaLogger);

        (IRaft, KahunaManager)[] nodes =
            [(r1, (KahunaManager)k1), (r2, (KahunaManager)k2), (r3, (KahunaManager)k3)];

        try
        {
            (IRaft _, KahunaManager destLeader) = await LeaderOf(Pid, nodes);

            // Route from a node that is NOT the destination leader, so the handoff crosses the inter-node hop
            // before it is replicated on the destination partition.
            (IRaft sourceRaft, KahunaManager source) =
                nodes.First(n => !ReferenceEquals(n.Item2, destLeader));

            HLCTimestamp tx = NextTx(sourceRaft);

            // Seed only on the source store — no commit, so the record is on exactly one node until the handoff.
            source.CompletionReceiptStore.Record(tx, MovedLow, MovedLow, KeyValueDurability.Persistent);
            source.CompletionReceiptStore.Record(tx, MovedHigh, MovedHigh, KeyValueDurability.Persistent);

            IReadOnlyCollection<CompletionReceiptRecord> moved =
                source.KeyValues.GetLocalCompletionReceiptsForRange(SplitKey, null);
            Assert.Equal(2, moved.Count);

            bool durable = await source.KeyValues.ImportCompletionReceiptsToPartitionLeaderAsync(Pid, moved, ct);
            Assert.True(durable);

            // Every replica of the destination partition holds both moved receipts.
            await WaitUntilAsync(() => nodes.All(n =>
                n.Item2.CompletionReceiptStore.Contains(tx, MovedLow, KeyValueDurability.Persistent) &&
                n.Item2.CompletionReceiptStore.Contains(tx, MovedHigh, KeyValueDurability.Persistent)));

            foreach ((IRaft _, KahunaManager kahuna) in nodes)
            {
                Assert.True(kahuna.CompletionReceiptStore.Contains(tx, MovedLow, KeyValueDurability.Persistent));
                Assert.True(kahuna.CompletionReceiptStore.Contains(tx, MovedHigh, KeyValueDurability.Persistent));
            }
        }
        finally
        {
            await LeaveCluster(r1, r2, r3);
        }
    }

    // ── 2. Decision handoff reaches every destination replica ────────────────────────────────────

    /// <summary>Same guarantee as <see cref="ReceiptHandoff_ReplicatesToEveryDestinationReplica"/>, for decision records.</summary>
    [Fact]
    public async Task DecisionHandoff_ReplicatesToEveryDestinationReplica()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft r1, IRaft r2, IRaft r3, IKahuna k1, IKahuna k2, IKahuna k3) =
            await AssembleThreNodeCluster("memory", 3, raftLogger, kahunaLogger);

        (IRaft, KahunaManager)[] nodes =
            [(r1, (KahunaManager)k1), (r2, (KahunaManager)k2), (r3, (KahunaManager)k3)];

        try
        {
            (IRaft _, KahunaManager destLeader) = await LeaderOf(Pid, nodes);
            (IRaft sourceRaft, KahunaManager source) =
                nodes.First(n => !ReferenceEquals(n.Item2, destLeader));

            HLCTimestamp txLow = NextTx(sourceRaft);
            HLCTimestamp txHigh = NextTx(sourceRaft);

            await source.ImportCoordinatorDecisions(
            [
                Decision(txLow, MovedLow, CoordinatorDecisionStatus.CommitDecided),
                Decision(txHigh, MovedHigh, CoordinatorDecisionStatus.Completed)
            ]);

            IReadOnlyList<CoordinatorDecisionRecord> moved =
                source.KeyValues.GetLocalDecisionsForRange(SplitKey, null);
            Assert.Equal(2, moved.Count);

            bool durable = await source.KeyValues.ImportCoordinatorDecisionsToPartitionLeaderAsync(Pid, moved, ct);
            Assert.True(durable);

            // Every replica of the destination partition holds both moved records, with status preserved.
            await WaitUntilAsync(() => nodes.All(n =>
                n.Item2.CoordinatorDecisionStore.TryGet(txLow, out _) &&
                n.Item2.CoordinatorDecisionStore.TryGet(txHigh, out _)));

            foreach ((IRaft _, KahunaManager kahuna) in nodes)
            {
                Assert.True(kahuna.CoordinatorDecisionStore.TryGet(txHigh, out CoordinatorDecisionRecord high));
                Assert.Equal(CoordinatorDecisionStatus.Completed, high.Status);
            }
        }
        finally
        {
            await LeaveCluster(r1, r2, r3);
        }
    }

    // ── 3. Handoff survives a destination-leadership change ──────────────────────────────────────

    /// <summary>
    /// Because the handoff is replicated, a destination-leadership change after cutover preserves it: the node
    /// that wins the new election is an up-to-date follower that already applied the receipt and decision, so a
    /// re-commit / re-drive routed to the new leader still resolves. A memory-only import would have stranded
    /// both on the former leader.
    /// </summary>
    [Fact]
    public async Task Handoff_SurvivesDestinationLeadershipChange()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft r1, IRaft r2, IRaft r3, IKahuna k1, IKahuna k2, IKahuna k3) =
            await AssembleThreNodeCluster("memory", 3, raftLogger, kahunaLogger);

        (IRaft, KahunaManager)[] nodes =
            [(r1, (KahunaManager)k1), (r2, (KahunaManager)k2), (r3, (KahunaManager)k3)];

        try
        {
            (IRaft oldLeaderRaft, KahunaManager oldLeader) = await LeaderOf(Pid, nodes);
            (IRaft sourceRaft, KahunaManager source) =
                nodes.First(n => !ReferenceEquals(n.Item2, oldLeader));

            HLCTimestamp tx = NextTx(sourceRaft);
            source.CompletionReceiptStore.Record(tx, MovedHigh, MovedHigh, KeyValueDurability.Persistent);
            await source.ImportCoordinatorDecisions([Decision(tx, MovedHigh, CoordinatorDecisionStatus.CommitDecided)]);

            IReadOnlyCollection<CompletionReceiptRecord> movedReceipts =
                source.KeyValues.GetLocalCompletionReceiptsForRange(SplitKey, null);
            IReadOnlyList<CoordinatorDecisionRecord> movedDecisions =
                source.KeyValues.GetLocalDecisionsForRange(SplitKey, null);

            Assert.True(await source.KeyValues.ImportCompletionReceiptsToPartitionLeaderAsync(Pid, movedReceipts, ct));
            Assert.True(await source.KeyValues.ImportCoordinatorDecisionsToPartitionLeaderAsync(Pid, movedDecisions, ct));

            await WaitUntilAsync(() => nodes.All(n =>
                n.Item2.CompletionReceiptStore.Contains(tx, MovedHigh, KeyValueDurability.Persistent) &&
                n.Item2.CoordinatorDecisionStore.TryGet(tx, out _)));

            // Force the destination leader to step down; a different node takes over the partition.
            string oldEndpoint = oldLeaderRaft.GetLocalEndpoint();
            await oldLeaderRaft.StepDownAsync(Pid, ct);

            (IRaft newLeaderRaft, KahunaManager newLeader) = (null!, null!);
            await WaitUntilAsync(async () =>
            {
                foreach ((IRaft raft, KahunaManager kahuna) in nodes)
                    if (raft.GetLocalEndpoint() != oldEndpoint && await raft.AmILeader(Pid, ct))
                    {
                        (newLeaderRaft, newLeader) = (raft, kahuna);
                        return true;
                    }
                return false;
            });

            // The freshly-elected leader is a former follower that already applied both replicated records —
            // asserted for every node above, before the step-down. A memory-only import would have stranded
            // them on the retired leader, leaving this one with nothing; the surviving durable decision record
            // is the witness that the handoff was replicated rather than memory-only. Acquiring data-partition
            // leadership also wakes the decision-recovery driver, which legitimately re-drives this committed
            // decision and asynchronously releases its completion receipt — so the receipt is transient here and
            // must not be asserted post-election (that is the race). The decision record is retained (marked
            // Completed) for the outcome-retention window, so it is the race-free survival check.
            Assert.True(newLeader.CoordinatorDecisionStore.TryGet(tx, out _));
        }
        finally
        {
            await LeaveCluster(r1, r2, r3);
        }
    }

    // ── 4. Live merge aborts cutover when the handoff is not durable ─────────────────────────────

    /// <summary>
    /// A live merge whose decision handoff to the survivor cannot be made durable must abort <b>before</b>
    /// cutover — the source (right) partition stays live and both descriptors remain, so the outstanding
    /// decision is never lost by retiring its owner. Without the gate the merge would cut over and retire the
    /// right partition regardless of the failed import.
    /// </summary>
    [Fact]
    public async Task LiveMerge_AbortsCutover_WhenDecisionHandoffNotDurable()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        const string space = "t:hm";
        string boundary = space + "/0010";

        (IRaft r1, IRaft r2, IRaft r3, IKahuna k1, IKahuna k2, IKahuna k3) =
            await AssembleThreNodeCluster("memory", 4, raftLogger, kahunaLogger);

        (IRaft, KahunaManager)[] nodes =
            [(r1, (KahunaManager)k1), (r2, (KahunaManager)k2), (r3, (KahunaManager)k3)];

        try
        {
            foreach ((IRaft _, KahunaManager kahuna) in nodes)
                kahuna.RegisterKeyRange(space);

            const int leftPid = RangeMapStore.FirstDataPartitionId;
            const int rightPid = RangeMapStore.FirstDataPartitionId + 1;

            var left = new RangeDescriptor { KeySpace = space, StartKey = null, EndKey = boundary, PartitionId = leftPid, Generation = 1 };
            var right = new RangeDescriptor { KeySpace = space, StartKey = boundary, EndKey = null, PartitionId = rightPid, Generation = 1 };

            (IRaft _, KahunaManager metaLeader0) = await LeaderOf(RangeMapStore.MetaPartitionId, nodes);
            Assert.True(await metaLeader0.RangeMapStore.MutateAsync(_ => [left, right], ct));

            foreach ((IRaft _, KahunaManager kahuna) in nodes)
                await WaitUntilAsync(() => kahuna.RangeMapStore.Current.FindAll(space).Count == 2);

            // One key in each half so both ranges are non-empty merge candidates.
            (IRaft _, KahunaManager leftLeader) = await LeaderOf(leftPid, nodes);
            (IRaft _, KahunaManager rightLeader) = await LeaderOf(rightPid, nodes);
            string leftKey = space + "/0001";
            string rightKey = space + "/0011";
            Assert.Equal(KeyValueResponseType.Set, (await leftLeader.TrySetKeyValue(
                HLCTimestamp.Zero, leftKey, V("v"), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent)).Item1);
            Assert.Equal(KeyValueResponseType.Set, (await rightLeader.TrySetKeyValue(
                HLCTimestamp.Zero, rightKey, V("v"), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent)).Item1);

            // Pin the meta leader, seed an outstanding decision anchored in the moving (right) range on it, and
            // make the decision handoff to the survivor non-durable on every node.
            (IRaft metaRaft, KahunaManager metaLeader) = await LeaderOf(RangeMapStore.MetaPartitionId, nodes);
            await ForceMetaLeaderAsync((metaRaft, metaLeader), ct);

            HLCTimestamp tx = NextTx(metaRaft);
            await metaLeader.ImportCoordinatorDecisions([Decision(tx, rightKey, CoordinatorDecisionStatus.CommitDecided)]);

            foreach ((IRaft _, KahunaManager kahuna) in nodes)
                kahuna.CoordinatorDecisionStore.ReplicateImportFault = _ => true;

            MergeOutcome outcome = await metaLeader.RangeMerger.MergeAsync(space, left, right, ct);

            Assert.Equal(MergeStatus.TransferFailed, outcome.Status);

            // Cutover did not happen: both descriptors remain and the right partition is still live.
            IReadOnlyList<RangeDescriptor> after = metaLeader.RangeMapStore.Current.FindAll(space);
            Assert.Equal(2, after.Count);
            Assert.Contains(after, d => d.PartitionId == rightPid);
        }
        finally
        {
            foreach ((IRaft _, KahunaManager kahuna) in nodes)
                kahuna.CoordinatorDecisionStore.ReplicateImportFault = null;
            await LeaveCluster(r1, r2, r3);
        }
    }

    // ── 4b. Live merge survives a source-side leadership change and still transfers the decision ──

    /// <summary>
    /// A leadership change on the <b>source</b> (retiring/right) partition around cutover must not lose the moved
    /// correctness record: the record is replicated across the source partition's replicas, so a freshly-elected
    /// source leader still holds it, and the merge — gated on the handoff being durable on the survivor — still
    /// transfers it and cuts over. Complements <see cref="Handoff_SurvivesDestinationLeadershipChange"/>, which
    /// exercises the destination side.
    /// </summary>
    [Fact]
    public async Task LiveMerge_SurvivesSourceLeadershipChange_TransfersDecisionToSurvivor()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        const string space = "t:hms";
        string boundary = space + "/0010";

        (IRaft r1, IRaft r2, IRaft r3, IKahuna k1, IKahuna k2, IKahuna k3) =
            await AssembleThreNodeCluster("memory", 4, raftLogger, kahunaLogger);

        (IRaft, KahunaManager)[] nodes =
            [(r1, (KahunaManager)k1), (r2, (KahunaManager)k2), (r3, (KahunaManager)k3)];

        try
        {
            foreach ((IRaft _, KahunaManager kahuna) in nodes)
                kahuna.RegisterKeyRange(space);

            const int leftPid = RangeMapStore.FirstDataPartitionId;
            const int rightPid = RangeMapStore.FirstDataPartitionId + 1;

            var left = new RangeDescriptor { KeySpace = space, StartKey = null, EndKey = boundary, PartitionId = leftPid, Generation = 1 };
            var right = new RangeDescriptor { KeySpace = space, StartKey = boundary, EndKey = null, PartitionId = rightPid, Generation = 1 };

            (IRaft _, KahunaManager metaLeader0) = await LeaderOf(RangeMapStore.MetaPartitionId, nodes);
            Assert.True(await metaLeader0.RangeMapStore.MutateAsync(_ => [left, right], ct));

            foreach ((IRaft _, KahunaManager kahuna) in nodes)
                await WaitUntilAsync(() => kahuna.RangeMapStore.Current.FindAll(space).Count == 2);

            // One key in each half so both ranges are non-empty merge candidates.
            (IRaft _, KahunaManager leftLeader) = await LeaderOf(leftPid, nodes);
            (IRaft _, KahunaManager rightLeader) = await LeaderOf(rightPid, nodes);
            string leftKey = space + "/0001";
            string rightKey = space + "/0011";
            Assert.Equal(KeyValueResponseType.Set, (await leftLeader.TrySetKeyValue(
                HLCTimestamp.Zero, leftKey, V("v"), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent)).Item1);
            Assert.Equal(KeyValueResponseType.Set, (await rightLeader.TrySetKeyValue(
                HLCTimestamp.Zero, rightKey, V("v"), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent)).Item1);

            // Change the SOURCE (right, retiring) partition's leadership before cutover: step the current right
            // leader down and let a former follower take over.
            (IRaft rightRaft, KahunaManager _) = await LeaderOf(rightPid, nodes);
            string oldRightEndpoint = rightRaft.GetLocalEndpoint();
            await rightRaft.StepDownAsync(rightPid, ct);
            await WaitUntilAsync(async () =>
            {
                foreach ((IRaft raft, KahunaManager _) in nodes)
                    if (raft.GetLocalEndpoint() != oldRightEndpoint && await raft.AmILeader(rightPid, ct))
                        return true;
                return false;
            });

            // Pin the meta leader and seed an outstanding decision anchored in the moving (right) range.
            (IRaft metaRaft, KahunaManager metaLeader) = await LeaderOf(RangeMapStore.MetaPartitionId, nodes);
            await ForceMetaLeaderAsync((metaRaft, metaLeader), ct);

            HLCTimestamp tx = NextTx(metaRaft);
            await metaLeader.ImportCoordinatorDecisions([Decision(tx, rightKey, CoordinatorDecisionStatus.CommitDecided)]);

            // The merge succeeds despite the source-side leadership change, cuts over, and hands the decision to
            // the survivor — where every replica holds it.
            MergeOutcome outcome = await metaLeader.RangeMerger.MergeAsync(space, left, right, ct);
            Assert.True(outcome.IsSuccess);

            await WaitUntilAsync(() => metaLeader.RangeMapStore.Current.FindAll(space).Count == 1);
            await WaitUntilAsync(() => nodes.All(n => n.Item2.CoordinatorDecisionStore.TryGet(tx, out _)));
        }
        finally
        {
            await LeaveCluster(r1, r2, r3);
        }
    }

    // ── 5. Live split aborts cutover when the handoff is not durable ─────────────────────────────

    /// <summary>
    /// A live split whose receipt handoff to the new partition cannot be made durable must abort before
    /// cutover — the original whole-range descriptor is left intact, so the moving range's receipt is never
    /// stranded behind a cutover that made the new partition the sole route.
    /// </summary>
    [Fact]
    public async Task LiveSplit_AbortsCutover_WhenReceiptHandoffNotDurable()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        const string space = "t:hs";
        string splitKey = space + "/m";

        (IRaft r1, IRaft r2, IRaft r3, IKahuna k1, IKahuna k2, IKahuna k3) =
            await AssembleThreNodeCluster("memory", 3, raftLogger, kahunaLogger);

        (IRaft, KahunaManager)[] nodes =
            [(r1, (KahunaManager)k1), (r2, (KahunaManager)k2), (r3, (KahunaManager)k3)];

        try
        {
            foreach ((IRaft _, KahunaManager kahuna) in nodes)
                kahuna.RegisterKeyRange(space);

            (IRaft _, KahunaManager metaLeader0) = await LeaderOf(RangeMapStore.MetaPartitionId, nodes);
            Assert.True(await metaLeader0.RangeMapStore.MutateAsync(
                _ => [new RangeDescriptor { KeySpace = space, StartKey = null, EndKey = null, PartitionId = Pid, Generation = 1 }], ct));

            foreach ((IRaft _, KahunaManager kahuna) in nodes)
                await WaitUntilAsync(() => kahuna.RangeMapStore.Current.Find(space, space + "/x")?.Generation == 1);

            // Keys below and above the split point so both halves are non-empty.
            (IRaft _, KahunaManager dataLeader) = await LeaderOf(Pid, nodes);
            string belowKey = space + "/a";
            string aboveKey = space + "/z";
            foreach (string key in new[] { belowKey, aboveKey })
                Assert.Equal(KeyValueResponseType.Set, (await dataLeader.TrySetKeyValue(
                    HLCTimestamp.Zero, key, V("v"), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent)).Item1);

            foreach (string key in new[] { belowKey, aboveKey })
                foreach ((IRaft _, KahunaManager kahuna) in nodes)
                    await WaitUntilAsync(async () =>
                        (await kahuna.TryGetValue(HLCTimestamp.Zero, key, 0, HLCTimestamp.Zero, KeyValueDurability.Persistent)).Item1
                        == KeyValueResponseType.Get);

            // Seed an outstanding receipt for a key in the moving [split, +∞) half on every node (so whichever
            // node runs the split reads it), and make the receipt handoff to the new partition non-durable.
            HLCTimestamp tx = NextTx(r1);
            foreach ((IRaft _, KahunaManager kahuna) in nodes)
            {
                kahuna.CompletionReceiptStore.Record(tx, aboveKey, aboveKey, KeyValueDurability.Persistent);
                kahuna.KeyValues.ReplicateReceiptImportFault = _ => true;
            }

            SplitOutcome outcome = await SplitViaLeaders(space, splitKey, nodes, ct);

            Assert.Equal(SplitStatus.TransferFailed, outcome.Status);

            // Cutover did not happen: the range is still one whole descriptor, not split.
            (IRaft _, KahunaManager metaLeader) = await LeaderOf(RangeMapStore.MetaPartitionId, nodes);
            IReadOnlyList<RangeDescriptor> after = metaLeader.RangeMapStore.Current.FindAll(space);
            Assert.Single(after);
            Assert.Null(after[0].StartKey);
            Assert.Null(after[0].EndKey);
        }
        finally
        {
            foreach ((IRaft _, KahunaManager kahuna) in nodes)
                kahuna.KeyValues.ReplicateReceiptImportFault = null;
            await LeaveCluster(r1, r2, r3);
        }
    }

    /// <summary>
    /// Creates the new Unrouted partition (as the real trigger does) then drives one split attempt. Retries only
    /// the transient cutover race, so a deliberate <see cref="SplitStatus.TransferFailed"/> surfaces directly.
    /// </summary>
    private static async Task<SplitOutcome> SplitViaLeaders(
        string space, string splitKey, (IRaft, KahunaManager)[] nodes, CancellationToken ct)
    {
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
}
