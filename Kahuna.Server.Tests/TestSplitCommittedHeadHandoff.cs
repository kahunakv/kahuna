using System.Text;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.KeyValues.Writes;
using Kahuna.Server.Replication;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// The committed-head hand-off of a range move on a live cluster. A split or merge moves a key's rows to another
/// partition; the head its last transactional commit left in the staged-base fence memory stays in the SOURCE
/// partition's ledger slice unless the move hands it on. Without the hand-off the destination's fence judged a
/// moved key from each node's copy of the source slice — a copy the destination's leader could hold behind its
/// replicas, or not at all — so a leader could admit a prepare its replicas refused: the acknowledged stale-base
/// commit a split cascade produced under load. These tests prove the hand-off lands the moved keys' heads in the
/// destination slice on every replica before the cutover, for a split and for a merge, and that the destination's
/// own slice then refuses a stale prepare with no source memory to lean on.
/// </summary>
public sealed class TestSplitCommittedHeadHandoff : BaseCluster
{
    private const string Space = "t:hh";

    private const string SplitKey = Space + "/m";

    private static readonly string[] KeysBelow = [Space + "/a", Space + "/b"];

    private static readonly string[] KeysAbove = [Space + "/p", Space + "/q"];

    private readonly ILogger<IRaft> raftLogger;

    private readonly ILogger<IKahuna> kahunaLogger;

    public TestSplitCommittedHeadHandoff(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = TestLogFactory.Create(outputHelper);
        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    private static async Task<(IRaft Raft, KahunaManager Kahuna)> LeaderOf(int partition, (IRaft, KahunaManager)[] nodes)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        while (true)
        {
            foreach ((IRaft raft, KahunaManager kahuna) in nodes)
                if (await raft.AmILeaderIfHosted(partition, ct))
                    return (raft, kahuna);
            await Task.Delay(50, ct);
        }
    }

    /// <summary>Commits one durable transaction on <paramref name="key"/>, which is what feeds the fence memory
    /// (a write outside a transaction never does).</summary>
    private static async Task CommitThroughScript(IKahuna kahuna, string key, string value)
    {
        KeyValueTransactionResult result = await RetryOnMustRetry(
            kahuna, Encoding.UTF8.GetBytes($"BEGIN SET `{key}` '{value}' COMMIT END"), null, null);
        Assert.Equal(KeyValueResponseType.Set, result.Type);
    }

    /// <summary>Waits until every node's fence memory holds a head for <paramref name="key"/> at or above
    /// <paramref name="atLeast"/>, and returns that head's revision as the first node remembers it.</summary>
    private static async Task<long> WaitForHeadEverywhere((IRaft, KahunaManager)[] nodes, string key, long atLeast)
    {
        foreach ((IRaft _, KahunaManager kahuna) in nodes)
        {
            KahunaManager manager = kahuna;
            await WaitUntilAsync(
                () => manager.DurablePreparedIntentStore.TryGetCommittedHead(key, out long revision, out _) && revision >= atLeast,
                timeoutMs: 30_000);
        }

        Assert.True(nodes[0].Item2.DurablePreparedIntentStore.TryGetCommittedHead(key, out long head, out _));
        return head;
    }

    private static bool HoldsHead(KahunaManager kahuna, int partitionId, string key, long expected) =>
        kahuna.DurablePreparedIntentStore.TryGetLedgerHead(partitionId, key, out long revision, out _, out _) && revision == expected;

    /// <summary>
    /// Assembles a 4-partition 3-node cluster, registers the ranged space on every node, seeds one whole-space
    /// descriptor on the first data partition, and commits every key through durable transactions — the keys
    /// above the split point twice, so their heads sit above a first commit's revision and a head carried from
    /// the wrong revision would be visible. Returns the nodes and the head every key reached.
    /// </summary>
    private async Task<((IRaft, KahunaManager)[] Nodes, Dictionary<string, long> Heads)> SetupOneRange()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft r1, IRaft r2, IRaft r3, IKahuna k1, IKahuna k2, IKahuna k3) =
            await AssembleThreNodeCluster("memory", 4, raftLogger, kahunaLogger);

        (IRaft, KahunaManager)[] nodes = [(r1, (KahunaManager)k1), (r2, (KahunaManager)k2), (r3, (KahunaManager)k3)];

        foreach ((IRaft _, KahunaManager kahuna) in nodes)
            kahuna.RegisterKeyRange(Space);

        (IRaft _, KahunaManager metaLeader) = await LeaderOf(RangeMapStore.MetaPartitionId, nodes);

        bool committed = await metaLeader.RangeMapStore.MutateAsync(
            _ => [new RangeDescriptor { KeySpace = Space, StartKey = null, EndKey = null, PartitionId = RangeMapStore.FirstDataPartitionId, Generation = 1 }],
            ct);
        Assert.True(committed);

        foreach ((IRaft _, KahunaManager kahuna) in nodes)
            await WaitUntilAsync(() => kahuna.RangeMapStore.Current.Find(Space, Space + "/x")?.Generation == 1);

        Dictionary<string, long> heads = [];

        foreach (string key in KeysBelow)
        {
            await CommitThroughScript(nodes[0].Item2, key, "one");
            heads[key] = await WaitForHeadEverywhere(nodes, key, atLeast: 0);
        }

        foreach (string key in KeysAbove)
        {
            await CommitThroughScript(nodes[0].Item2, key, "one");
            long first = await WaitForHeadEverywhere(nodes, key, atLeast: 0);
            await CommitThroughScript(nodes[0].Item2, key, "two");
            heads[key] = await WaitForHeadEverywhere(nodes, key, atLeast: first + 1);
        }

        return (nodes, heads);
    }

    /// <summary>
    /// Drives the split at <see cref="SplitKey"/> from the meta-partition leader, retrying the transient
    /// cutover race, and runs <paramref name="duringQuiesce"/> inside the window after the hand-off and before
    /// the cutover. Returns the id of the partition the upper half moved to.
    /// </summary>
    private static async Task<int> SplitAsync((IRaft, KahunaManager)[] nodes, Func<int, Task>? duringQuiesce, CancellationToken ct)
    {
        for (int attempt = 0; attempt < 5; attempt++)
        {
            (IRaft metaRaft, KahunaManager metaLeader) = await LeaderOf(RangeMapStore.MetaPartitionId, nodes);
            int newPartition = RangeSplitter.ComputeNextPartitionId(metaRaft, metaLeader.RangeMapStore.Current);

            SplitOutcome outcome = await metaLeader.ForceSplitAtKeyAsync(
                Space, SplitKey,
                duringQuiesce: duringQuiesce is null ? null : () => duringQuiesce(newPartition),
                ct);

            if (outcome.IsSuccess)
            {
                foreach ((IRaft _, KahunaManager kahuna) in nodes)
                    await WaitUntilAsync(() => kahuna.RangeMapStore.Current.Find(Space, KeysAbove[0])?.PartitionId == newPartition, timeoutMs: 30_000);

                return newPartition;
            }

            if (outcome.Status is not (SplitStatus.CutoverFailed or SplitStatus.ProbeIndeterminate or SplitStatus.UnsettledMovingIntents))
                Assert.Fail($"Split failed: {outcome.Status}");

            await Task.Delay(100, ct);
        }

        Assert.Fail("Split did not succeed within five attempts");
        return 0;
    }

    /// <summary>
    /// Every replica of the destination holds the moved keys' heads, at the revisions the source remembered,
    /// while the source range is still quiesced — the hand-off is ordered ahead of the cutover — and holds no
    /// head for a key that stays behind.
    /// </summary>
    [Fact]
    public async Task Split_SeedsTheMovedKeysCommittedHeads_OnEveryDestinationReplica_BeforeTheCutover()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        ((IRaft, KahunaManager)[] nodes, Dictionary<string, long> heads) = await SetupOneRange();

        try
        {
            string? windowFailure = null;
            bool windowRan = false;

            int newPartition = await SplitAsync(nodes, async destination =>
            {
                windowRan = true;

                try
                {
                    foreach ((IRaft _, KahunaManager kahuna) in nodes)
                    {
                        KahunaManager manager = kahuna;

                        foreach (string key in KeysAbove)
                        {
                            string k = key;
                            await WaitUntilAsync(() => HoldsHead(manager, destination, k, heads[k]), timeoutMs: 10_000);
                        }

                        foreach (string key in KeysBelow)
                            Assert.False(manager.DurablePreparedIntentStore.TryGetLedgerHead(destination, key, out _, out _, out _));
                    }
                }
                catch (Exception ex)
                {
                    windowFailure = ex.ToString();
                }
            }, ct);

            Assert.True(windowRan, "the quiesce-window hook did not run");
            Assert.Null(windowFailure);

            // The seeded slice is the destination's own, so it is what every node answers for the moved keys.
            foreach ((IRaft _, KahunaManager kahuna) in nodes)
            {
                foreach (string key in KeysAbove)
                {
                    Assert.True(HoldsHead(kahuna, newPartition, key, heads[key]));
                    Assert.True(kahuna.DurablePreparedIntentStore.TryGetCommittedHead(key, out long nodeWide, out _));
                    Assert.Equal(heads[key], nodeWide);
                }
            }
        }
        finally
        {
            await LeaveCluster(nodes[0].Item1, nodes[1].Item1, nodes[2].Item1);
        }
    }

    /// <summary>
    /// After the split the destination's leader is stripped of its copy of the source slice — the memory a
    /// leader that lagged the source's apply, never hosted the source, or purged it after a merge does not have.
    /// A prepare validated against the base the source's last commit moved past is still refused at its apply,
    /// by the seeded head alone, and recorded for the finalizer as a stale base; a prepare validated against the
    /// seeded head is acknowledged. The late-veto witness never moves: no stale-base commit is acknowledged.
    /// </summary>
    [Fact]
    public async Task StalePrepareOnTheDestination_IsRefusedBySeededHeadAlone()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        ((IRaft, KahunaManager)[] nodes, Dictionary<string, long> heads) = await SetupOneRange();

        try
        {
            int newPartition = await SplitAsync(nodes, duringQuiesce: null, ct);

            (IRaft leaderRaft, KahunaManager leader) = await LeaderOf(newPartition, nodes);
            foreach (string key in KeysAbove)
                await WaitUntilAsync(() => HoldsHead(leader, newPartition, key, heads[key]), timeoutMs: 30_000);

            // Forget the source slice on the destination's leader: only the seeded destination slice remains.
            leader.DurablePreparedIntentStore.PurgePartitionLedger(RangeMapStore.FirstDataPartitionId);
            Assert.True(leader.DurablePreparedIntentStore.TryGetCommittedHead(KeysAbove[1], out long remembered, out _));
            Assert.Equal(heads[KeysAbove[1]], remembered);

            long lateBefore = DurableTransactionMetrics.StaleBaseVetoesLateCount;

            // Validated against the revision before the source's last commit of the key.
            string staleKey = KeysAbove[1];
            HLCTimestamp staleTx = leaderRaft.HybridLogicalClock.TrySendOrLocalEvent(leaderRaft.GetLocalNodeId());
            PreparedIntent stale = Intent(staleTx, staleKey, revision: heads[staleKey], baseRevision: heads[staleKey] - 1);

            // The durable seam folds the leader's prepare acknowledgement into its answer: a refused prepare is
            // a false result, which is what the finalizer reads as "not prepared" before it classifies the refusal.
            Assert.False(await leader.KeyValues.ReplicateDurableThroughScheduler(
                newPartition, ReplicationTypes.PreparedIntent, PreparedIntentStore.SerializeDelta([new PrepareIntentCommand(stale)]),
                WriteAdmissionClass.Ordinary, WriteSubmissionStage.Prepare, ct));

            PrepareRejectionKind kind = PrepareRejectionKind.None;
            await WaitUntilAsync(() => leader.DurablePreparedIntentStore.TryTakePrepareRejection(staleTx, 1, staleKey, out kind));
            Assert.Equal(PrepareRejectionKind.StaleBase, kind);

            // Validated against the seeded head itself: the ordinary read-modify-write after the move.
            string currentKey = KeysAbove[0];
            HLCTimestamp currentTx = leaderRaft.HybridLogicalClock.TrySendOrLocalEvent(leaderRaft.GetLocalNodeId());
            PreparedIntent current = Intent(currentTx, currentKey, revision: heads[currentKey] + 1, baseRevision: heads[currentKey]);

            Assert.True(await leader.KeyValues.ReplicateDurableThroughScheduler(
                newPartition, ReplicationTypes.PreparedIntent, PreparedIntentStore.SerializeDelta([new PrepareIntentCommand(current)]),
                WriteAdmissionClass.Ordinary, WriteSubmissionStage.Prepare, ct));

            await WaitUntilAsync(() => leader.DurablePreparedIntentStore.GetByIdentity(currentTx, 1, currentKey) is not null);
            Assert.False(leader.DurablePreparedIntentStore.TryTakePrepareRejection(currentTx, 1, currentKey, out _));

            // The refused prepare has no commit to find: the veto that rides the refusal cannot arrive late.
            await Task.Delay(250, ct);
            Assert.Equal(lateBefore, DurableTransactionMetrics.StaleBaseVetoesLateCount);
        }
        finally
        {
            await LeaveCluster(nodes[0].Item1, nodes[1].Item1, nodes[2].Item1);
        }
    }

    private static PreparedIntent Intent(HLCTimestamp txId, string key, long revision, long baseRevision) => new(
        TransactionId: txId, Epoch: 1, Key: key,
        ManifestHash: 0, RecordAnchorKey: key,
        CommitTimestamp: new HLCTimestamp(txId.N, txId.L + 1, txId.C),
        State: KeyValueState.Set, Value: "staged"u8.ToArray(), Bucket: null,
        Revision: revision, Expires: HLCTimestamp.Zero, NoRevision: false,
        BaseRevision: baseRevision, BaseState: KeyValueState.Set,
        RecoveryDeadline: HLCTimestamp.Zero, Resolution: PreparedIntentResolution.Pending);

    /// <summary>
    /// A merge hands the right range's heads to the survivor's slice on every replica; once the retired
    /// partition is removed the heads are still remembered, through the survivor's slice.
    /// </summary>
    [Fact]
    public async Task Merge_SeedsTheMovedKeysCommittedHeads_IntoTheSurvivorSlice_OnEveryReplica()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft r1, IRaft r2, IRaft r3, IKahuna k1, IKahuna k2, IKahuna k3) =
            await AssembleThreNodeCluster("memory", 4, raftLogger, kahunaLogger);

        (IRaft, KahunaManager)[] nodes = [(r1, (KahunaManager)k1), (r2, (KahunaManager)k2), (r3, (KahunaManager)k3)];

        try
        {
            foreach ((IRaft _, KahunaManager kahuna) in nodes)
                kahuna.RegisterKeyRange(Space);

            const int leftPartition = RangeMapStore.FirstDataPartitionId;
            const int rightPartition = RangeMapStore.FirstDataPartitionId + 1;

            RangeDescriptor left = new() { KeySpace = Space, StartKey = null, EndKey = SplitKey, PartitionId = leftPartition, Generation = 1 };
            RangeDescriptor right = new() { KeySpace = Space, StartKey = SplitKey, EndKey = null, PartitionId = rightPartition, Generation = 1 };

            (IRaft _, KahunaManager metaLeader) = await LeaderOf(RangeMapStore.MetaPartitionId, nodes);
            Assert.True(await metaLeader.RangeMapStore.MutateAsync(_ => [left, right], ct));

            foreach ((IRaft _, KahunaManager kahuna) in nodes)
                await WaitUntilAsync(() => kahuna.RangeMapStore.Current.FindAll(Space).Count == 2);

            Dictionary<string, long> heads = [];

            foreach (string key in KeysBelow)
            {
                await CommitThroughScript(nodes[0].Item2, key, "one");
                heads[key] = await WaitForHeadEverywhere(nodes, key, atLeast: 0);
            }

            foreach (string key in KeysAbove)
            {
                await CommitThroughScript(nodes[0].Item2, key, "one");
                long first = await WaitForHeadEverywhere(nodes, key, atLeast: 0);
                await CommitThroughScript(nodes[0].Item2, key, "two");
                heads[key] = await WaitForHeadEverywhere(nodes, key, atLeast: first + 1);
            }

            // The right range's heads were fed by the right partition's log, so they sit in that slice only.
            foreach ((IRaft _, KahunaManager kahuna) in nodes)
            {
                foreach (string key in KeysAbove)
                    Assert.False(kahuna.DurablePreparedIntentStore.TryGetLedgerHead(leftPartition, key, out _, out _, out _));
            }

            MergeOutcome outcome = default;
            for (int attempt = 0; attempt < 5; attempt++)
            {
                (IRaft _, metaLeader) = await LeaderOf(RangeMapStore.MetaPartitionId, nodes);
                outcome = await metaLeader.RangeMerger.MergeAsync(Space, left, right, ct);

                if (outcome.IsSuccess || outcome.Status is not (MergeStatus.CutoverFailed or MergeStatus.UnsettledMovingIntents))
                    break;

                await Task.Delay(100, ct);
            }

            Assert.True(outcome.IsSuccess, $"Merge failed: {outcome.Status}");
            Assert.Equal(rightPartition, outcome.RetiredPartitionId);

            foreach ((IRaft _, KahunaManager kahuna) in nodes)
            {
                KahunaManager manager = kahuna;

                foreach (string key in KeysAbove)
                {
                    string k = key;
                    await WaitUntilAsync(() => HoldsHead(manager, leftPartition, k, heads[k]), timeoutMs: 30_000);
                }
            }

            (IRaft sysRaft, KahunaManager _) = await LeaderOf(0, nodes);
            Assert.True((await sysRaft.RemovePartitionAsync(outcome.RetiredPartitionId, ct)).Success);

            foreach ((IRaft _, KahunaManager kahuna) in nodes)
            {
                foreach (string key in KeysAbove)
                {
                    Assert.True(HoldsHead(kahuna, leftPartition, key, heads[key]));
                    Assert.True(kahuna.DurablePreparedIntentStore.TryGetCommittedHead(key, out long nodeWide, out _));
                    Assert.Equal(heads[key], nodeWide);
                }
            }
        }
        finally
        {
            await LeaveCluster(r1, r2, r3);
        }
    }
}
