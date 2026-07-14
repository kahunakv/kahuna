using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// Acceptance test for the cluster-wide completion-receipt release: a durable coordinator acknowledgement forgets
/// a participant's receipt as a <b>replicated</b> operation on the participant partition's Raft log, so every
/// replica of that partition drops the proof — not just the node that ran the coordinator drive. A node-local
/// forget would leave the followers holding the receipt forever while the decision record reports it released.
/// </summary>
[Collection("ClusterTests")]
public sealed class TestReceiptReleaseReplication : BaseCluster
{
    private const int Pid = RangeMapStore.FirstDataPartitionId;

    private readonly ILogger<IRaft> raftLogger;
    private readonly ILogger<IKahuna> kahunaLogger;

    public TestReceiptReleaseReplication(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = LoggerFactory.Create(builder =>
            builder.AddXUnit(outputHelper).SetMinimumLevel(LogLevel.Warning));

        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
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

    [Fact]
    public async Task ReceiptForget_ReplicatesToEveryParticipantReplica()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft r1, IRaft r2, IRaft r3, IKahuna k1, IKahuna k2, IKahuna k3) =
            await AssembleThreNodeCluster("memory", 3, raftLogger, kahunaLogger);

        (IRaft, KahunaManager)[] nodes =
            [(r1, (KahunaManager)k1), (r2, (KahunaManager)k2), (r3, (KahunaManager)k3)];

        try
        {
            (IRaft leaderRaft, KahunaManager _) = await LeaderOf(Pid, nodes);

            const string key = "receipt:release:key";
            const string anchor = "receipt:release:anchor";
            HLCTimestamp tx = leaderRaft.HybridLogicalClock.TrySendOrLocalEvent(leaderRaft.GetLocalNodeId());

            // A committed persistent participant records its receipt on every replica of its partition. Model that
            // starting state by recording it directly on all three nodes.
            foreach ((IRaft _, KahunaManager kahuna) in nodes)
                kahuna.CompletionReceiptStore.Record(tx, key, anchor, KeyValueDurability.Persistent);

            foreach ((IRaft _, KahunaManager kahuna) in nodes)
                Assert.True(kahuna.CompletionReceiptStore.Contains(tx, key, KeyValueDurability.Persistent));

            // A durable acknowledgement forgets the receipt, replicated on the participant partition.
            bool durable = await nodes[0].Item2.KeyValues.ForgetCompletionReceiptsToPartitionLeaderAsync(
                Pid, [new CompletionReceiptRecord(tx, key, anchor, KeyValueDurability.Persistent)], ct);
            Assert.True(durable);

            // Every replica dropped the proof — not just the leader that ran the forget.
            await WaitUntilAsync(() => nodes.All(n =>
                !n.Item2.CompletionReceiptStore.Contains(tx, key, KeyValueDurability.Persistent)));

            foreach ((IRaft _, KahunaManager kahuna) in nodes)
                Assert.False(kahuna.CompletionReceiptStore.Contains(tx, key, KeyValueDurability.Persistent));
        }
        finally
        {
            await LeaveCluster(r1, r2, r3);
        }
    }
}
