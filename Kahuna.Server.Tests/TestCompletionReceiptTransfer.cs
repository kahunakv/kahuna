using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// Acceptance tests for the completion-receipt transfer plumbing that split/merge uses to hand a moved
/// key range's receipts to the destination partition leader. Like <see cref="TestRangeLockTransfer"/>,
/// these exercise the transfer pieces (range selection + partition-leader routing, including the
/// inter-node forward when the source is not the leader) <b>without</b> driving a live split, so they
/// isolate the transfer path from replication (which in an all-nodes-in-all-groups test cluster would
/// otherwise place a receipt on every node regardless of the transfer).
/// </summary>
[Collection("ClusterTests")]
public sealed class TestCompletionReceiptTransfer : BaseCluster
{
    // Data partition that owns the moved range in these tests.
    private const int Pid = RangeMapStore.FirstDataPartitionId;

    private const string Below = "k:aaa"; // < SplitKey — stays behind
    private const string SplitKey = "k:m";
    private const string MovedLow = "k:mmm"; // >= SplitKey — moves
    private const string MovedHigh = "k:zzz"; // >= SplitKey — moves

    private readonly ILogger<IRaft> raftLogger;
    private readonly ILogger<IKahuna> kahunaLogger;

    public TestCompletionReceiptTransfer(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = LoggerFactory.Create(builder =>
            builder.AddXUnit(outputHelper).SetMinimumLevel(LogLevel.Warning));

        raftLogger = loggerFactory.CreateLogger<IRaft>();
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

    // ── Test 1: moved range routed to a remote partition leader ───────────────────

    /// <summary>
    /// When the source node is not the leader of the destination partition, transferring the moved range's
    /// receipts routes them over the inter-node transport onto the leader's node-global store: exactly the
    /// receipts whose key is in <c>[SplitKey, +∞)</c> land there, and the receipt that stayed behind
    /// (below the split key) is never delivered.
    /// </summary>
    [Fact]
    public async Task MovedReceipts_RoutedToRemotePartitionLeader_LandOnLeaderStore()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft r1, IRaft r2, IRaft r3, IKahuna k1, IKahuna k2, IKahuna k3) =
            await AssembleThreNodeCluster("memory", 3, raftLogger, kahunaLogger);

        (IRaft, KahunaManager)[] nodes =
            [(r1, (KahunaManager)k1), (r2, (KahunaManager)k2), (r3, (KahunaManager)k3)];

        try
        {
            (IRaft leaderRaft, KahunaManager destLeader) = await LeaderOf(Pid, nodes);

            // Pick a source node that is NOT the destination leader, so the routing exercises the
            // inter-node forward rather than a local import.
            (IRaft sourceRaft, KahunaManager source) =
                nodes.First(n => !ReferenceEquals(n.Item2, destLeader));

            HLCTimestamp tx = NextTx(sourceRaft);

            // Seed three receipts only on the source node's store: one below the split key (stays behind),
            // two at/above it (move to the destination partition).
            source.CompletionReceiptStore.Record(tx, Below, Below, KeyValueDurability.Persistent);
            source.CompletionReceiptStore.Record(tx, MovedLow, MovedLow, KeyValueDurability.Persistent);
            source.CompletionReceiptStore.Record(tx, MovedHigh, MovedHigh, KeyValueDurability.Persistent);

            // Select the moved range [SplitKey, +∞): the range filter must exclude the below-split key.
            IReadOnlyCollection<CompletionReceiptRecord> moved =
                source.KeyValues.GetLocalCompletionReceiptsForRange(SplitKey, null);

            Assert.Equal(2, moved.Count);
            Assert.Contains(moved, r => r.Key == MovedLow);
            Assert.Contains(moved, r => r.Key == MovedHigh);
            Assert.DoesNotContain(moved, r => r.Key == Below);

            // Route the moved receipts to the destination partition leader (inter-node forward).
            await source.KeyValues.ImportCompletionReceiptsToPartitionLeaderAsync(Pid, moved, ct);

            // The destination leader now holds exactly the moved receipts — never the one left behind.
            await WaitUntilAsync(() =>
                destLeader.CompletionReceiptStore.Contains(tx, MovedLow, KeyValueDurability.Persistent) &&
                destLeader.CompletionReceiptStore.Contains(tx, MovedHigh, KeyValueDurability.Persistent));

            Assert.True(destLeader.CompletionReceiptStore.Contains(tx, MovedLow, KeyValueDurability.Persistent));
            Assert.True(destLeader.CompletionReceiptStore.Contains(tx, MovedHigh, KeyValueDurability.Persistent));
            Assert.False(destLeader.CompletionReceiptStore.Contains(tx, Below, KeyValueDurability.Persistent));
        }
        finally
        {
            await LeaveCluster(r1, r2, r3);
        }
    }

    // ── Test 2: local import when the routing node is the partition leader ─────────

    /// <summary>
    /// When the routing node is itself the leader of the destination partition, the moved receipts are
    /// imported into its own store directly (no inter-node hop), and the below-split receipt is untouched.
    /// </summary>
    [Fact]
    public async Task MovedReceipts_ImportedLocally_WhenNodeIsPartitionLeader()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft r1, IRaft r2, IRaft r3, IKahuna k1, IKahuna k2, IKahuna k3) =
            await AssembleThreNodeCluster("memory", 3, raftLogger, kahunaLogger);

        (IRaft, KahunaManager)[] nodes =
            [(r1, (KahunaManager)k1), (r2, (KahunaManager)k2), (r3, (KahunaManager)k3)];

        try
        {
            (IRaft leaderRaft, KahunaManager leader) = await LeaderOf(Pid, nodes);

            HLCTimestamp tx = NextTx(leaderRaft);

            leader.CompletionReceiptStore.Record(tx, Below, Below, KeyValueDurability.Persistent);
            leader.CompletionReceiptStore.Record(tx, MovedLow, MovedLow, KeyValueDurability.Persistent);

            IReadOnlyCollection<CompletionReceiptRecord> moved =
                leader.KeyValues.GetLocalCompletionReceiptsForRange(SplitKey, null);
            Assert.Single(moved);

            // Routing on the leader itself imports locally.
            await leader.KeyValues.ImportCompletionReceiptsToPartitionLeaderAsync(Pid, moved, ct);

            Assert.True(leader.CompletionReceiptStore.Contains(tx, MovedLow, KeyValueDurability.Persistent));
            Assert.True(leader.CompletionReceiptStore.Contains(tx, Below, KeyValueDurability.Persistent)); // seeded here, unaffected
        }
        finally
        {
            await LeaveCluster(r1, r2, r3);
        }
    }
}
