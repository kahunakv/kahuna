using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// Read-path routed decision resolution (the cross-node half of durable-intent read visibility): when a latest
/// read meets a foreign prepared intent that is still <c>Pending</c> and whose canonical transaction record lives
/// on a <b>different</b> partition led by another node, the read routes a lookup to that anchor leader and resolves
/// the outcome — serving the committed value (or the prior value on abort) — instead of spinning until settlement
/// propagates the decision to the key's partition.
/// </summary>
[Collection("ClusterTests")]
public sealed class TestDurableReadRoutedResolution : BaseCluster
{
    private readonly ILogger<IRaft> raftLogger;
    private readonly ILogger<IKahuna> kahunaLogger;

    public TestDurableReadRoutedResolution(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = LoggerFactory.Create(builder =>
            builder.AddXUnit(outputHelper).SetMinimumLevel(LogLevel.Warning));
        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    [Fact]
    public void ForeignDecisionHint_AppliesOnlyForMatchingTerminalIdentity()
    {
        HLCTimestamp tx = new(0, 100, 0);
        ForeignDecisionHint commit = new(tx, 1, TransactionDecision.Commit);

        Assert.True(commit.Applies(tx, 1));                                   // exact identity + terminal
        Assert.False(commit.Applies(new HLCTimestamp(0, 999, 0), 1));         // different txn
        Assert.False(commit.Applies(tx, 2));                                  // different epoch
        Assert.False(new ForeignDecisionHint(tx, 1, TransactionDecision.Undecided).Applies(tx, 1)); // not terminal
        Assert.False(default(ForeignDecisionHint).Applies(HLCTimestamp.Zero, 0)); // no hint (zero identity)
    }

    [Fact]
    public async Task CrossNodeRead_CommittedUnsettledIntent_RoutesAnchorDecision_ServesCommittedValue()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft r1, IRaft r2, IRaft r3, IKahuna k1, IKahuna k2, IKahuna k3) =
            await AssembleThreNodeCluster("memory", 8, raftLogger, kahunaLogger);

        (IRaft Raft, IKahuna Kahuna)[] nodes = [(r1, k1), (r2, k2), (r3, k3)];
        try
        {
            // Find a key whose partition and a distinct anchor key whose partition are led by different nodes.
            (string key, IKahuna keyLeader, string anchor, IKahuna anchorLeader) = await FindCrossNodeKeyAndAnchor(nodes, ct);

            HLCTimestamp txId = new(0, 5_000, 0);
            const long epoch = 1;
            HLCTimestamp commitTs = new(0, 5_100, 0);
            HLCTimestamp deadline = new(0, 9_000, 0);
            HLCTimestamp opId = new(0, 5_050, 0);
            byte[] value = "committed-via-routed-lookup"u8.ToArray();

            List<TransactionParticipantRef> manifest = [new(key, KeyValueDurability.Persistent)];
            long hash = TransactionManifest.ComputeHash(txId, epoch, anchor, commitTs, manifest);

            // Committed canonical record lives ONLY on the anchor partition's leader (not replicated to the key leader).
            TransactionRecordStore recordStore = ((KahunaManager)anchorLeader).DurableTransactionRecordStore;
            recordStore.Apply(new InitializeTransactionCommand(txId, epoch, "coord", anchor, commitTs, deadline, hash, manifest, opId, new HLCTimestamp(0, 5_000, 0)));
            recordStore.Apply(new CommitTransactionCommand(txId, epoch, hash, opId, commitTs));

            // A still-pending prepared intent for the key lives on the key partition's leader — the committed value
            // has not settled here, and the record is not local, so a naive read would retry until settlement.
            PreparedIntentStore intentStore = ((KahunaManager)keyLeader).DurablePreparedIntentStore;
            PreparedIntent intent = new(
                txId, epoch, key, hash, anchor, commitTs,
                KeyValueState.Set, value, Bucket: null, Revision: 1, Expires: HLCTimestamp.Zero,
                NoRevision: false, BaseRevision: 0, BaseState: KeyValueState.Set,
                RecoveryDeadline: new HLCTimestamp(0, long.MaxValue, 0), Resolution: PreparedIntentResolution.Pending);
            intentStore.Apply(new PrepareIntentCommand(intent));

            // Sanity: the key leader has no local record for the transaction; it must route to resolve.
            Assert.Null(((KahunaManager)keyLeader).DurableTransactionRecordStore.Get(txId, epoch));

            // The read on the key leader routes the decision lookup to the anchor leader and serves the committed value.
            (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await keyLeader.LocateAndTryGetValue(
                HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);

            Assert.Equal(KeyValueResponseType.Get, type);
            Assert.Equal(value, entry!.Value);
        }
        finally
        {
            await LeaveCluster(r1, r2, r3);
        }
    }

    [Fact]
    public async Task CrossNodeRead_AbortedUnsettledIntent_RoutesAnchorDecision_ServesPriorValue()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft r1, IRaft r2, IRaft r3, IKahuna k1, IKahuna k2, IKahuna k3) =
            await AssembleThreNodeCluster("memory", 8, raftLogger, kahunaLogger);

        (IRaft Raft, IKahuna Kahuna)[] nodes = [(r1, k1), (r2, k2), (r3, k3)];
        try
        {
            (string key, IKahuna keyLeader, string anchor, IKahuna anchorLeader) = await FindCrossNodeKeyAndAnchor(nodes, ct);

            HLCTimestamp txId = new(0, 6_000, 0);
            const long epoch = 1;
            HLCTimestamp commitTs = new(0, 6_100, 0);
            HLCTimestamp deadline = new(0, 9_000, 0);
            HLCTimestamp opId = new(0, 6_050, 0);

            List<TransactionParticipantRef> manifest = [new(key, KeyValueDurability.Persistent)];
            long hash = TransactionManifest.ComputeHash(txId, epoch, anchor, commitTs, manifest);

            // Aborted canonical record on the anchor leader.
            TransactionRecordStore recordStore = ((KahunaManager)anchorLeader).DurableTransactionRecordStore;
            recordStore.Apply(new InitializeTransactionCommand(txId, epoch, "coord", anchor, commitTs, deadline, hash, manifest, opId, new HLCTimestamp(0, 6_000, 0)));
            recordStore.Apply(new AbortTransactionCommand(txId, epoch, hash, TransactionAbortClass.Conflict, opId, commitTs, anchor, commitTs, deadline, new HLCTimestamp(0, 6_000, 0)));

            PreparedIntentStore intentStore = ((KahunaManager)keyLeader).DurablePreparedIntentStore;
            PreparedIntent intent = new(
                txId, epoch, key, hash, anchor, commitTs,
                KeyValueState.Set, "should-not-be-seen"u8.ToArray(), Bucket: null, Revision: 1, Expires: HLCTimestamp.Zero,
                NoRevision: false, BaseRevision: 0, BaseState: KeyValueState.Set,
                RecoveryDeadline: new HLCTimestamp(0, long.MaxValue, 0), Resolution: PreparedIntentResolution.Pending);
            intentStore.Apply(new PrepareIntentCommand(intent));

            // The aborted intent is invisible: the read resolves the abort via routing and serves the prior value,
            // which does not exist here (nothing was ever committed for the key).
            (KeyValueResponseType type, _) = await keyLeader.LocateAndTryGetValue(
                HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);

            Assert.Equal(KeyValueResponseType.DoesNotExist, type);
        }
        finally
        {
            await LeaveCluster(r1, r2, r3);
        }
    }

    [Fact]
    public async Task CrossNodeExists_CommittedUnsettledIntent_RoutesAnchorDecision_ReportsExists()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft r1, IRaft r2, IRaft r3, IKahuna k1, IKahuna k2, IKahuna k3) =
            await AssembleThreNodeCluster("memory", 8, raftLogger, kahunaLogger);

        (IRaft Raft, IKahuna Kahuna)[] nodes = [(r1, k1), (r2, k2), (r3, k3)];
        try
        {
            (string key, IKahuna keyLeader, string anchor, IKahuna anchorLeader) = await FindCrossNodeKeyAndAnchor(nodes, ct);

            HLCTimestamp txId = new(0, 7_000, 0);
            const long epoch = 1;
            HLCTimestamp commitTs = new(0, 7_100, 0);
            HLCTimestamp deadline = new(0, 9_000, 0);
            HLCTimestamp opId = new(0, 7_050, 0);

            List<TransactionParticipantRef> manifest = [new(key, KeyValueDurability.Persistent)];
            long hash = TransactionManifest.ComputeHash(txId, epoch, anchor, commitTs, manifest);

            TransactionRecordStore recordStore = ((KahunaManager)anchorLeader).DurableTransactionRecordStore;
            recordStore.Apply(new InitializeTransactionCommand(txId, epoch, "coord", anchor, commitTs, deadline, hash, manifest, opId, new HLCTimestamp(0, 7_000, 0)));
            recordStore.Apply(new CommitTransactionCommand(txId, epoch, hash, opId, commitTs));

            PreparedIntentStore intentStore = ((KahunaManager)keyLeader).DurablePreparedIntentStore;
            PreparedIntent intent = new(
                txId, epoch, key, hash, anchor, commitTs,
                KeyValueState.Set, "v"u8.ToArray(), Bucket: null, Revision: 1, Expires: HLCTimestamp.Zero,
                NoRevision: false, BaseRevision: 0, BaseState: KeyValueState.Set,
                RecoveryDeadline: new HLCTimestamp(0, long.MaxValue, 0), Resolution: PreparedIntentResolution.Pending);
            intentStore.Apply(new PrepareIntentCommand(intent));

            // Exists on the key leader routes the committed decision from the anchor leader and reports the key as
            // existing, instead of retrying until settlement.
            (KeyValueResponseType type, _) = await keyLeader.LocateAndTryExistsValue(
                HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);

            Assert.Equal(KeyValueResponseType.Exists, type);
        }
        finally
        {
            await LeaveCluster(r1, r2, r3);
        }
    }

    // Finds a key and a distinct anchor key whose partitions are led by two different nodes, so a read of the key
    // must route the anchor-record lookup cross-node.
    private static async Task<(string Key, IKahuna KeyLeader, string Anchor, IKahuna AnchorLeader)> FindCrossNodeKeyAndAnchor(
        (IRaft Raft, IKahuna Kahuna)[] nodes, CancellationToken ct)
    {
        Dictionary<IKahuna, string> byLeader = new();

        for (int i = 0; i < 512 && byLeader.Count < 2; i++)
        {
            string candidate = $"site2-{i}/k";
            int partition = ((KahunaManager)nodes[0].Kahuna).LocateRange(candidate).PartitionId;

            foreach ((IRaft raft, IKahuna kahuna) in nodes)
            {
                if (!await raft.AmILeader(partition, ct).ConfigureAwait(false))
                    continue;
                byLeader.TryAdd(kahuna, candidate);
                break;
            }
        }

        Assert.True(byLeader.Count >= 2, "could not find keys led by two different nodes");
        List<KeyValuePair<IKahuna, string>> pair = [.. byLeader];
        return (pair[0].Value, pair[0].Key, pair[1].Value, pair[1].Key);
    }
}
