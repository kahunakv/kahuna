using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.KeyValues.Writes;
using Kahuna.Server.Replication;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Data;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// Acceptance tests for the durable-intent 2PC state transfer that split/merge uses to hand a moved key range's
/// canonical transaction records and prepared intents to the destination partition leader. Like the completion
/// receipt transfer tests, these exercise the transfer pieces (range selection + partition-leader routing,
/// including the inter-node forward when the source is not the leader) without driving a live split, so they
/// isolate the transfer path.
/// </summary>
[Collection("ClusterTests")]
public sealed class TestDurableStateTransfer : BaseCluster
{
    private const int Pid = RangeMapStore.FirstDataPartitionId;

    private const string Below = "k:aaa";     // < SplitKey — stays behind
    private const string SplitKey = "k:m";
    private const string MovedRecordAnchor = "k:mmm";  // >= SplitKey — moves
    private const string MovedIntentKey = "k:zzz";     // >= SplitKey — moves

    private readonly ILogger<IRaft> raftLogger;
    private readonly ILogger<IKahuna> kahunaLogger;

    public TestDurableStateTransfer(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = LoggerFactory.Create(builder =>
            builder.AddXUnit(outputHelper).SetMinimumLevel(LogLevel.Warning));

        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    private static HLCTimestamp Ts(long l) => new(0, l, 0);

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

    private static void SeedCommittedRecord(KahunaManager node, HLCTimestamp txId, string anchor)
    {
        List<TransactionParticipantRef> manifest = [new(anchor, KeyValueDurability.Persistent)];
        node.DurableTransactionRecordStore.Apply(new InitializeTransactionCommand(
            txId, 1, "coord", anchor, Ts(1100), Ts(9000), 42, manifest, HLCTimestamp.Zero, Ts(1000)));
        node.DurableTransactionRecordStore.Apply(new CommitTransactionCommand(txId, 1, 42, Ts(1200), Ts(1200)));
    }

    private static void SeedPendingIntent(KahunaManager node, HLCTimestamp txId, string key, string anchor)
    {
        PreparedIntent intent = new(
            txId, 1, key, ManifestHash: 42, RecordAnchorKey: anchor, CommitTimestamp: Ts(1100),
            State: KeyValueState.Set, Value: [1, 2], Bucket: null, Revision: 3, Expires: HLCTimestamp.Zero,
            NoRevision: false, BaseRevision: 2, BaseState: KeyValueState.Set, RecoveryDeadline: Ts(6000),
            Resolution: PreparedIntentResolution.Pending);
        node.DurablePreparedIntentStore.Apply(new PrepareIntentCommand(intent));
    }

    /// <summary>
    /// When the source node is not the leader of the destination partition, transferring the moved range's durable
    /// records and intents routes them over the inter-node transport onto the destination leader's stores:
    /// exactly the record/intent whose anchor/key is in <c>[SplitKey, +∞)</c> land there (reconstructed
    /// faithfully), and the record/intent that stayed behind is never delivered.
    /// </summary>
    [Fact]
    public async Task MovedDurableState_RoutedToRemotePartitionLeader_LandsOnLeaderStores()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft r1, IRaft r2, IRaft r3, IKahuna k1, IKahuna k2, IKahuna k3) =
            await AssembleThreNodeCluster("memory", 3, raftLogger, kahunaLogger);

        (IRaft, KahunaManager)[] nodes =
            [(r1, (KahunaManager)k1), (r2, (KahunaManager)k2), (r3, (KahunaManager)k3)];

        try
        {
            (IRaft _, KahunaManager destLeader) = await LeaderOf(Pid, nodes);

            // A source node that is NOT the destination leader, so the transfer exercises the inter-node forward.
            (IRaft _, KahunaManager source) = nodes.First(n => !ReferenceEquals(n.Item2, destLeader));

            HLCTimestamp movedTx = Ts(5000);
            HLCTimestamp belowTx = Ts(6000);

            // Seed durable state only on the source node's stores: one record + one intent that move (anchor/key
            // at/above the split key), and one record + one intent that stay behind (below the split key).
            SeedCommittedRecord(source, movedTx, MovedRecordAnchor);
            SeedCommittedRecord(source, belowTx, Below);
            SeedPendingIntent(source, movedTx, MovedIntentKey, MovedRecordAnchor);
            SeedPendingIntent(source, belowTx, Below, Below);

            // Select the moved range [SplitKey, +∞): the filter must exclude the below-split anchor/key.
            IReadOnlyList<TransactionRecord> movedRecords = source.KeyValues.GetLocalTransactionRecordsForRange(SplitKey, null);
            IReadOnlyList<PreparedIntent> movedIntents = source.KeyValues.GetLocalPreparedIntentsForRange(SplitKey, null);

            Assert.Single(movedRecords);
            Assert.Equal(MovedRecordAnchor, movedRecords[0].RecordAnchorKey);
            Assert.Single(movedIntents);
            Assert.Equal(MovedIntentKey, movedIntents[0].Key);

            // Route the moved durable state to the destination partition leader (inter-node forward).
            Assert.True(await source.KeyValues.ImportDurableTransactionStateToPartitionLeaderAsync(Pid, movedRecords, movedIntents, ct));

            // The destination leader reconstructs exactly the moved record (as Commit) and the moved intent — and
            // never the state left behind.
            await WaitUntilAsync(() =>
                destLeader.DurableTransactionRecordStore.Get(movedTx, 1) is { Decision: TransactionDecision.Commit } &&
                destLeader.DurablePreparedIntentStore.Get(MovedIntentKey) is not null);

            Assert.Equal(TransactionDecision.Commit, destLeader.DurableTransactionRecordStore.Get(movedTx, 1)!.Decision);
            PreparedIntent? movedIntent = destLeader.DurablePreparedIntentStore.Get(MovedIntentKey);
            Assert.NotNull(movedIntent);
            Assert.Equal(PreparedIntentResolution.Pending, movedIntent!.Resolution);

            Assert.Null(destLeader.DurableTransactionRecordStore.Get(belowTx, 1));
            Assert.Null(destLeader.DurablePreparedIntentStore.Get(Below));
        }
        finally
        {
            await LeaveCluster(r1, r2, r3);
        }
    }

    /// <summary>
    /// The reconstruction delta rebuilds every record shape faithfully through the ordinary deterministic apply: a
    /// committed record, an abort record with a manifest, an undecided record, and a manifestless abort tombstone
    /// all reappear with the same decision, manifest presence, and winner on a fresh (destination) store.
    /// </summary>
    [Fact]
    public void ReconstructionDelta_RebuildsEveryRecordShape_Faithfully()
    {
        TransactionRecordStore source = new();

        HLCTimestamp committedTx = Ts(100), abortTx = Ts(200), undecidedTx = Ts(300), tombstoneTx = Ts(400);
        List<TransactionParticipantRef> manifest = [new("a", KeyValueDurability.Persistent)];

        // Committed.
        source.Apply(new InitializeTransactionCommand(committedTx, 1, "c", "a", Ts(1100), Ts(9000), 42, manifest, HLCTimestamp.Zero, Ts(1000)));
        source.Apply(new CommitTransactionCommand(committedTx, 1, 42, Ts(1200), Ts(1200)));
        // Aborted (retryable) with a manifest.
        source.Apply(new InitializeTransactionCommand(abortTx, 1, "c", "a", Ts(1100), Ts(9000), 42, manifest, HLCTimestamp.Zero, Ts(1000)));
        source.Apply(new AbortTransactionCommand(abortTx, 1, 42, TransactionAbortClass.RetryableFailure, Ts(1300), Ts(1300), "a", Ts(1100), Ts(9000), Ts(1000)));
        // Undecided (in flight).
        source.Apply(new InitializeTransactionCommand(undecidedTx, 1, "c", "a", Ts(1100), Ts(9000), 42, manifest, HLCTimestamp.Zero, Ts(1000)));
        // Manifestless abort tombstone (created from absence).
        source.Apply(new AbortTransactionCommand(tombstoneTx, 1, 77, TransactionAbortClass.PresumedAbort, Ts(1400), Ts(1400), "a", Ts(1100), Ts(9000), Ts(1000)));

        byte[] delta = TransactionRecordStore.SerializeReconstructionDelta(source.Snapshot());

        TransactionRecordStore dest = new();
        dest.Replicate(0, new RaftLog { LogType = ReplicationTypes.TransactionRecord, LogData = delta });

        TransactionRecord committed = dest.Get(committedTx, 1)!;
        Assert.Equal(TransactionDecision.Commit, committed.Decision);
        Assert.True(committed.ManifestPresent);
        Assert.Equal(Ts(1200), committed.WinningOpId);

        TransactionRecord aborted = dest.Get(abortTx, 1)!;
        Assert.Equal(TransactionDecision.Abort, aborted.Decision);
        Assert.Equal(TransactionAbortClass.RetryableFailure, aborted.AbortClass);
        Assert.True(aborted.ManifestPresent);

        TransactionRecord undecided = dest.Get(undecidedTx, 1)!;
        Assert.Equal(TransactionDecision.Undecided, undecided.Decision);

        TransactionRecord tombstone = dest.Get(tombstoneTx, 1)!;
        Assert.Equal(TransactionDecision.Abort, tombstone.Decision);
        Assert.False(tombstone.ManifestPresent);
    }

    private sealed class StubFence : IWriteRangeFence
    {
        public readonly HashSet<string> StaleKeys = [];
        public bool IsStale(string key, long admittedGeneration, int admittedPartitionId) => StaleKeys.Contains(key);
    }

    /// <summary>
    /// A durable submission re-fences at dispatch only when it carries a routing key: a keyed submission whose
    /// range descriptor moved since freeze is released retryably (so it is not appended to the retired partition),
    /// a keyed submission that did not move dispatches, and an unkeyed submission (post-decision settle, recovery,
    /// state-transfer import) never fences.
    /// </summary>
    [Fact]
    public void DurableSubmission_ReFencesOnlyWhenKeyed_AndReleasesWhenDescriptorMoved()
    {
        StubFence fence = new();
        fence.StaleKeys.Add("k:moved");
        RaftProposalEntry[] entries = [new(ReplicationTypes.TransactionRecord, [1], AutoCommit: true, ExpectedGeneration: 0)];

        DurableProposalSubmission unfenced = new(1, entries, new TaskCompletionSource<bool>(), applyOnCommit: null, fenceKey: null);
        Assert.False(unfenced.IsStale(fence));

        DurableProposalSubmission moved = new(1, entries, new TaskCompletionSource<bool>(), applyOnCommit: null, fenceKey: "k:moved", fenceGeneration: 3);
        Assert.True(moved.IsStale(fence));

        DurableProposalSubmission stable = new(1, entries, new TaskCompletionSource<bool>(), applyOnCommit: null, fenceKey: "k:stable", fenceGeneration: 3);
        Assert.False(stable.IsStale(fence));
    }
}
