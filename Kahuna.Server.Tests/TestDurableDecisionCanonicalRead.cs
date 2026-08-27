using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// The decision winner and the resolution direction must come from the transaction's canonical
/// record at its anchor, never from a node-local record store. A local store can diverge from the
/// canonical record: a decision forwarded to a remote anchor leader can lose there to one that
/// already won (a routed presumed abort racing a commit), and a sender-side projection of the
/// losing delta then mints a local record the canonical terminal-transition rules will never let
/// the true winner overwrite. A finalize or a settle pass that trusts that local answer
/// materializes an aborted transaction's prepared leg as committed — the conserved-total drift
/// signature. This test hand-builds the divergent state and requires the routed read to answer the
/// anchor's truth from the poisoned node.
/// </summary>
public sealed class TestDurableDecisionCanonicalRead : BaseCluster
{
    private const int Partitions = 6;

    private readonly Microsoft.Extensions.Logging.ILogger<IRaft> raftLogger;

    private readonly Microsoft.Extensions.Logging.ILogger<IKahuna> kahunaLogger;

    public TestDurableDecisionCanonicalRead(ITestOutputHelper outputHelper)
    {
        Microsoft.Extensions.Logging.ILoggerFactory loggerFactory = TestLogFactory.Create(outputHelper);
        raftLogger = Microsoft.Extensions.Logging.LoggerFactoryExtensions.CreateLogger<IRaft>(loggerFactory);
        kahunaLogger = Microsoft.Extensions.Logging.LoggerFactoryExtensions.CreateLogger<IKahuna>(loggerFactory);
    }

    private static async Task<(IRaft Raft, KahunaManager Kahuna, int Index)> LeaderOf(
        int partition, IRaft[] rafts, KahunaManager[] kahunas, CancellationToken ct)
    {
        while (true)
        {
            for (int i = 0; i < rafts.Length; i++)
                if (await rafts[i].AmILeaderIfHosted(partition, ct))
                    return (rafts[i], kahunas[i], i);

            await Task.Delay(50, ct);
        }
    }

    [Fact]
    public async Task RoutedLookup_FromNodeWithDivergentLocalCommit_AnswersTheCanonicalAbort()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleCluster(
            3, "memory", Partitions, raftLogger, kahunaLogger, replicationFactor: 0);

        KahunaManager[] managers = [.. kahunas.Cast<KahunaManager>()];

        const string anchorKey = "xd-tx/anchor-divergent";
        int anchorPartition = managers[0].KeyValues.LocateDurablePartition(anchorKey).PartitionId;
        (_, _, int leaderIndex) = await LeaderOf(anchorPartition, rafts, managers, ct);

        // The poisoned node: any node that does not lead the anchor partition.
        int poisonedIndex = (leaderIndex + 1) % 3;
        Assert.False(await rafts[poisonedIndex].AmILeaderIfHosted(anchorPartition, ct));

        HLCTimestamp now = rafts[0].HybridLogicalClock.TrySendOrLocalEvent(rafts[0].GetLocalNodeId());
        HLCTimestamp txId = new(now.N, now.L - 5_000, now.C);
        HLCTimestamp deadline = new(now.N, now.L + 60_000, now.C);

        InitializeTransactionCommand init = new(
            txId, 1, "xd-coord", anchorKey, txId, deadline, 42,
            [new TransactionParticipantRef(anchorKey, KeyValueDurability.Persistent)],
            HLCTimestamp.Zero, txId);

        // Every node starts from the same init: the record is Undecided cluster-wide, exactly the
        // state a coordinator's init projection leaves on its own node.
        foreach (KahunaManager manager in managers)
            manager.DurableTransactionRecordStore.Apply(init);

        // The race's outcome, built by hand. Canonically (on the anchor leader and the third node)
        // the presumed abort won. On the poisoned node the coordinator's losing commit projection
        // applied first — Undecided accepts it — and the canonical abort can never overwrite it.
        AbortTransactionCommand abort = new(
            txId, 1, 42, TransactionAbortClass.PresumedAbort, OpId: now, AttemptHlc: now,
            anchorKey, CommitTimestamp: txId, DecisionDeadline: deadline, CreatedAt: txId);
        CommitTransactionCommand commit = new(txId, 1, 42, now, now);

        for (int i = 0; i < managers.Length; i++)
        {
            if (i == poisonedIndex)
                managers[i].DurableTransactionRecordStore.Apply(commit);
            else
                managers[i].DurableTransactionRecordStore.Apply(abort);
        }

        // The poison is real: the local store on the poisoned node answers Commit...
        Assert.Equal(TransactionDecision.Commit,
            managers[poisonedIndex].DurableTransactionRecordStore.Get(txId, 1)!.Decision);

        // ...and the canonical answer at the anchor leader is Abort.
        Assert.Equal(TransactionDecision.Abort,
            managers[leaderIndex].DurableTransactionRecordStore.Get(txId, 1)!.Decision);

        // The read the finalizer's decision read-back, its resolution direction, and the recovery
        // settle passes all use: routed by the anchor key. From the poisoned node it must answer
        // the anchor's truth, not the local divergence — an aborted transaction must never be
        // materialized as committed off the poisoned local answer.
        TransactionRecord? routed = await managers[poisonedIndex].KeyValues.LookupDurableRecordRouted(
            txId, 1, anchorKey, ct);

        Assert.NotNull(routed);
        Assert.Equal(TransactionDecision.Abort, routed.Decision);
    }
}
