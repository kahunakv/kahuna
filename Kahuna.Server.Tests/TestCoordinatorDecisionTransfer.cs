using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// Acceptance tests for the coordinator-decision transfer plumbing that split/merge uses to hand a moved
/// key range's decision records to the destination partition leader. Like
/// <see cref="TestCompletionReceiptTransfer"/>, these exercise the transfer pieces (anchor-range selection
/// + partition-leader routing, including the inter-node forward) <b>without</b> driving a live split, so
/// they isolate the transfer from replication (which in an all-nodes-in-all-groups test cluster would
/// otherwise place a record on every node regardless of the transfer).
/// </summary>
[Collection("ClusterTests")]
public sealed class TestCoordinatorDecisionTransfer : BaseCluster
{
    private const int Pid = RangeMapStore.FirstDataPartitionId;

    private const string Below = "k:aaa";    // < SplitKey — stays behind
    private const string SplitKey = "k:m";
    private const string MovedLow = "k:mmm"; // >= SplitKey — moves
    private const string MovedHigh = "k:zzz"; // >= SplitKey — moves

    private readonly ILogger<IRaft> raftLogger;
    private readonly ILogger<IKahuna> kahunaLogger;

    public TestCoordinatorDecisionTransfer(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = LoggerFactory.Create(builder =>
            builder.AddXUnit(outputHelper).SetMinimumLevel(LogLevel.Warning));

        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    private static HLCTimestamp NextTx(IRaft raft) =>
        raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

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

    // ── Moved range routed to a remote partition leader ────────────────────────────────────────────

    [Fact]
    public async Task MovedDecisions_RoutedToRemotePartitionLeader_LandOnLeaderStore()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft r1, IRaft r2, IRaft r3, IKahuna k1, IKahuna k2, IKahuna k3) =
            await AssembleThreNodeCluster("memory", 3, raftLogger, kahunaLogger);

        (IRaft, KahunaManager)[] nodes =
            [(r1, (KahunaManager)k1), (r2, (KahunaManager)k2), (r3, (KahunaManager)k3)];

        try
        {
            (IRaft leaderRaft, KahunaManager destLeader) = await LeaderOf(Pid, nodes);

            // A source node that is NOT the destination leader, so routing exercises the inter-node hop.
            (IRaft sourceRaft, KahunaManager source) =
                nodes.First(n => !ReferenceEquals(n.Item2, destLeader));

            // Each record is a distinct transaction (a record is keyed by transaction id).
            HLCTimestamp txLow = NextTx(sourceRaft);
            HLCTimestamp txHigh = NextTx(sourceRaft);
            HLCTimestamp txBelow = NextTx(sourceRaft);

            // Seed three records only on the source node's store (local inject, no replication): one whose
            // anchor is below the split key (stays behind), two at/above it (move to the destination).
            await source.ImportCoordinatorDecisions(
            [
                Decision(txBelow, Below, CoordinatorDecisionStatus.CommitDecided),
                Decision(txLow, MovedLow, CoordinatorDecisionStatus.CommitDecided),
                Decision(txHigh, MovedHigh, CoordinatorDecisionStatus.Completed)
            ]);

            // Select the moved range [SplitKey, +∞) by anchor — the below-split anchor is excluded.
            IReadOnlyList<CoordinatorDecisionRecord> moved =
                source.KeyValues.GetLocalDecisionsForRange(SplitKey, null);
            Assert.Equal(2, moved.Count);
            Assert.Contains(moved, r => r.RecordAnchorKey == MovedLow);
            Assert.Contains(moved, r => r.RecordAnchorKey == MovedHigh);
            Assert.DoesNotContain(moved, r => r.RecordAnchorKey == Below);

            await source.KeyValues.ImportCoordinatorDecisionsToPartitionLeaderAsync(Pid, moved, ct);

            // Both moved records (CommitDecided and Completed) land on the destination leader over the
            // inter-node hop; the below-split record was never routed.
            await WaitUntilAsync(() =>
                destLeader.CoordinatorDecisionStore.TryGet(txLow, out _) &&
                destLeader.CoordinatorDecisionStore.TryGet(txHigh, out _));
            Assert.True(destLeader.CoordinatorDecisionStore.TryGet(txHigh, out CoordinatorDecisionRecord high));
            Assert.Equal(CoordinatorDecisionStatus.Completed, high.Status);
            Assert.False(destLeader.CoordinatorDecisionStore.TryGet(txBelow, out _));
        }
        finally
        {
            await LeaveCluster(r1, r2, r3);
        }
    }

    // ── Distinct transactions on the moved range each land, idempotent under replay ────────────────

    [Fact]
    public async Task MovedDecisions_DistinctTransactions_LandAndReplayIsIdempotent()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft r1, IRaft r2, IRaft r3, IKahuna k1, IKahuna k2, IKahuna k3) =
            await AssembleThreNodeCluster("memory", 3, raftLogger, kahunaLogger);

        (IRaft, KahunaManager)[] nodes =
            [(r1, (KahunaManager)k1), (r2, (KahunaManager)k2), (r3, (KahunaManager)k3)];

        try
        {
            (IRaft leaderRaft, KahunaManager destLeader) = await LeaderOf(Pid, nodes);
            (IRaft sourceRaft, KahunaManager source) =
                nodes.First(n => !ReferenceEquals(n.Item2, destLeader));

            HLCTimestamp txLow = NextTx(sourceRaft);
            HLCTimestamp txHigh = NextTx(sourceRaft);
            HLCTimestamp txBelow = NextTx(sourceRaft);

            await source.ImportCoordinatorDecisions(
            [
                Decision(txBelow, Below, CoordinatorDecisionStatus.CommitDecided),
                Decision(txLow, MovedLow, CoordinatorDecisionStatus.CommitDecided),
                Decision(txHigh, MovedHigh, CoordinatorDecisionStatus.Completed)
            ]);

            IReadOnlyList<CoordinatorDecisionRecord> moved =
                source.KeyValues.GetLocalDecisionsForRange(SplitKey, null);
            Assert.Equal(2, moved.Count);

            int before = destLeader.CoordinatorDecisionStore.Count;

            // Route twice — the second import is an idempotent no-op (keyed by transaction id).
            await source.KeyValues.ImportCoordinatorDecisionsToPartitionLeaderAsync(Pid, moved, ct);
            await source.KeyValues.ImportCoordinatorDecisionsToPartitionLeaderAsync(Pid, moved, ct);

            await WaitUntilAsync(() =>
                destLeader.CoordinatorDecisionStore.TryGet(txLow, out _) &&
                destLeader.CoordinatorDecisionStore.TryGet(txHigh, out _));

            Assert.True(destLeader.CoordinatorDecisionStore.TryGet(txLow, out CoordinatorDecisionRecord low));
            Assert.True(destLeader.CoordinatorDecisionStore.TryGet(txHigh, out CoordinatorDecisionRecord high));
            Assert.Equal(MovedLow, low.RecordAnchorKey);
            Assert.Equal(CoordinatorDecisionStatus.Completed, high.Status);

            // The below-split record never routed; exactly the two moved records were added.
            Assert.False(destLeader.CoordinatorDecisionStore.TryGet(txBelow, out _));
            Assert.Equal(before + 2, destLeader.CoordinatorDecisionStore.Count);
        }
        finally
        {
            await LeaveCluster(r1, r2, r3);
        }
    }

    // ── Local import when the routing node is the partition leader ─────────────────────────────────

    [Fact]
    public async Task MovedDecisions_ImportedLocally_WhenNodeIsPartitionLeader()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft r1, IRaft r2, IRaft r3, IKahuna k1, IKahuna k2, IKahuna k3) =
            await AssembleThreNodeCluster("memory", 3, raftLogger, kahunaLogger);

        (IRaft, KahunaManager)[] nodes =
            [(r1, (KahunaManager)k1), (r2, (KahunaManager)k2), (r3, (KahunaManager)k3)];

        try
        {
            (IRaft leaderRaft, KahunaManager leader) = await LeaderOf(Pid, nodes);

            HLCTimestamp txMoved = NextTx(leaderRaft);
            HLCTimestamp txBelow = NextTx(leaderRaft);

            await leader.ImportCoordinatorDecisions(
            [
                Decision(txBelow, Below, CoordinatorDecisionStatus.CommitDecided),
                Decision(txMoved, MovedLow, CoordinatorDecisionStatus.CommitDecided)
            ]);

            IReadOnlyList<CoordinatorDecisionRecord> moved =
                leader.KeyValues.GetLocalDecisionsForRange(SplitKey, null);
            Assert.Single(moved);

            await leader.KeyValues.ImportCoordinatorDecisionsToPartitionLeaderAsync(Pid, moved, ct);

            Assert.True(leader.CoordinatorDecisionStore.TryGet(txMoved, out _));
            Assert.True(leader.CoordinatorDecisionStore.TryGet(txBelow, out _)); // seeded here, unaffected
        }
        finally
        {
            await LeaveCluster(r1, r2, r3);
        }
    }
}
