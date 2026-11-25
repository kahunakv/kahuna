using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.KeyValues.Writes;
using Kahuna.Server.Replication;
using Kahuna.Shared.KeyValue;
using Kommander.Data;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// A node that catches up the source log and the destination log of a moved key at the same time applies the two
/// logs interleaved. <see cref="TestRangeMoveCatchUpSkew"/> pins the leg that the move split across the two logs
/// (the prepare in the source log, the settle in the destination log). These tests pin a leg that is whole in the
/// source log: the transaction prepared, committed and settled on the source before the cutover, and the key's next
/// transaction is a one-phase bundle on the destination.
///
/// <para>The peers applied the source's settle before the destination existed, so the key was free when the bundle
/// applied there. The catching-up node replays the source's prepare, then receives the destination's bundle, and
/// replays the source's settle only afterwards. The intent map is one map for the node, keyed by the key: the
/// replayed source intent holds the key, the bundle's prepare is rejected as "key already has a prepared intent from
/// another transaction", and the bundled commit gate answers <c>PrepareMissing</c> on this node alone. The node does
/// not apply a commit its peers applied and stays one revision behind on the key.</para>
///
/// <para>CamusDB bank-split-kill-window, 2026-11-22 (Kahuna.Core 1.18.3): the restarted node caught up P1 and the P8
/// that a forced split created while it was down. Between 13:21:08 and 13:21:11 it rejected 17 bundles as "bundled
/// prepare not held" (11 on P8, 6 on P1) while it replayed 2,888 source records of the moved keys; no peer rejected
/// one. It was elected leader of P8 three seconds later and validated three transfers against its stale rows. Both
/// peers reported three late stale-base vetoes, and the reconciliation found three rows one write short.</para>
/// </summary>
public sealed class TestRangeMoveCatchUpInterleave
{
    private const int SourcePartition = 1;

    private const int DestinationPartition = 8;

    private static HLCTimestamp Ts(long l) => new(0, l, 0);

    private static PreparedIntent MakeIntent(
        string key, HLCTimestamp txId, long revision, long baseRevision, long manifestHash = 0) => new(
        TransactionId: txId, Epoch: 1, Key: key,
        ManifestHash: manifestHash, RecordAnchorKey: key,
        CommitTimestamp: new HLCTimestamp(txId.N, txId.L + 1, txId.C),
        State: KeyValueState.Set, Value: [1, 2, 3], Bucket: null,
        Revision: revision, Expires: HLCTimestamp.Zero, NoRevision: false,
        BaseRevision: baseRevision, BaseState: KeyValueState.Set,
        RecoveryDeadline: HLCTimestamp.Zero, Resolution: PreparedIntentResolution.Pending);

    private static RaftLog IntentLog(long id, long time, params PreparedIntentCommand[] commands) =>
        new() { Id = id, Time = Ts(time), LogType = ReplicationTypes.PreparedIntent, LogData = [.. PreparedIntentStore.SerializeDelta(commands)] };

    private static SeedCommittedHeadCommand Seed(string key, long revision, long committedAt) =>
        new(key, revision, KeyValueState.Set, Ts(committedAt));

    /// <summary>The settle the finalizer replicates for a committed leg: a materializing resolve and the removal.</summary>
    private static PreparedIntentCommand[] Settle(PreparedIntent intent) =>
    [
        new ResolveIntentCommand(intent.TransactionId, intent.Epoch, intent.Key, Commit: true, MaterializeOnResolve: true, intent.CommitTimestamp),
        new RemoveIntentCommand(intent.TransactionId, intent.Epoch, intent.Key)
    ];

    private static (TransactionRecordStore Records, PreparedIntentStore Intents) Stores()
    {
        TransactionRecordStore records = new();
        PreparedIntentStore intents = new();
        records.AttachBundledCommitJudge(intents.JudgeBundledCommit);
        return (records, intents);
    }

    /// <summary>A one-key transaction's bundled commit, the shape every one-phase commit of a bank transfer takes.</summary>
    private static CommitTransactionCommand Bundle(HLCTimestamp txId, string key, long manifestHash) =>
        new(txId, 1, manifestHash, OpId: Ts(txId.L + 2), AttemptHlc: Ts(txId.L + 2), BundledPrepareKeys: [key], ApplyTimeValidation: true);

    /// <summary>
    /// Applies the destination's one-phase bundle of the key's next transaction, as a replica of the destination
    /// applies it: the record, the prepare, and the verdict of the bundled commit gate.
    /// </summary>
    private static (TransactionApplyOutcome Prepare, BundledCommitJudgement Gate) ApplyDestinationBundle(
        TransactionRecordStore records, PreparedIntentStore intents, string key, long revision, long baseRevision)
    {
        HLCTimestamp yId = Ts(4_000);
        const long hash = 77;
        PreparedIntent y = MakeIntent(key, yId, revision, baseRevision, manifestHash: hash);
        List<TransactionParticipantRef> manifest = [new(key, KeyValueDurability.Persistent)];
        InitializeTransactionCommand init = new(yId, 1, "coord", key, Ts(yId.L + 1), Ts(yId.L + 9_000), hash, manifest, Ts(yId.L + 2), yId);

        Assert.Equal(TransactionApplyOutcome.Applied, records.Apply(init, DestinationPartition).Outcome);

        TransactionApplyOutcome prepare = intents.Apply(new PrepareIntentCommand(y), DestinationPartition).Outcome;
        return (prepare, intents.JudgeBundledCommit(DestinationPartition, Bundle(yId, key, hash)));
    }

    // ── the order the peers applied: the reference ───────────────────────────────

    [Fact]
    public void InLiveOrder_TheSourceLegSettlesBeforeTheMove_AndTheDestinationBundleIsAdmitted()
    {
        (TransactionRecordStore records, PreparedIntentStore intents) = Stores();

        Assert.True(intents.Replicate(SourcePartition, IntentLog(100, 1_000, Seed("r/live", 10, 1_000))));

        // X prepares, commits and settles on the source, all before the cutover.
        PreparedIntent x = MakeIntent("r/live", Ts(2_000), revision: 11, baseRevision: 10);
        Assert.True(intents.ApplyDeltaAckPrepares(SourcePartition, IntentLog(200, 2_000, new PrepareIntentCommand(x))));
        Assert.True(intents.Replicate(SourcePartition, IntentLog(201, 2_100, Settle(x))));
        Assert.Null(intents.Get("r/live"));

        // The hand-off seeds the key's head as of the cutover; the next transaction is a bundle on the destination.
        Assert.True(intents.Replicate(DestinationPartition, IntentLog(300, 3_000, Seed("r/live", 11, 2_001))));

        (TransactionApplyOutcome prepare, BundledCommitJudgement gate) = ApplyDestinationBundle(records, intents, "r/live", revision: 12, baseRevision: 11);

        Assert.Equal(TransactionApplyOutcome.Applied, prepare);
        Assert.Equal(BundledCommitJudgement.Admitted, gate);
    }

    // ── the catching-up node: the destination's bundle between the source's prepare and its settle ──

    [Fact]
    public void SourcePrepareReplayed_ItsSettleStillBehind_TheDestinationBundleIsAdmitted()
    {
        (TransactionRecordStore records, PreparedIntentStore intents) = Stores();

        Assert.True(intents.Replicate(SourcePartition, IntentLog(100, 1_000, Seed("r/interleave", 10, 1_000))));

        // The source's backlog reaches X's prepare. X's settle is a later entry of the same backlog.
        PreparedIntent x = MakeIntent("r/interleave", Ts(2_000), revision: 11, baseRevision: 10);
        Assert.True(intents.ApplyDeltaAckPrepares(SourcePartition, IntentLog(200, 2_000, new PrepareIntentCommand(x))));

        // The destination's log arrives meanwhile: the hand-off seed, which reflects X's commit (revision 11 was
        // the key's head at the cutover), and the bundle of the key's next transaction.
        Assert.True(intents.Replicate(DestinationPartition, IntentLog(300, 3_000, Seed("r/interleave", 11, 2_001))));

        (TransactionApplyOutcome prepare, BundledCommitJudgement gate) = ApplyDestinationBundle(records, intents, "r/interleave", revision: 12, baseRevision: 11);

        // Every peer admitted this bundle. A node that refuses it here does not apply the commit.
        Assert.Equal(TransactionApplyOutcome.Applied, prepare);
        Assert.Equal(BundledCommitJudgement.Admitted, gate);
    }

    [Fact]
    public void SourceLegCommittedButNotSettled_TheDestinationBundleIsAdmitted()
    {
        (TransactionRecordStore records, PreparedIntentStore intents) = Stores();

        Assert.True(intents.Replicate(SourcePartition, IntentLog(100, 1_000, Seed("r/decided", 10, 1_000))));

        // The backlog reaches X's prepare and its commit decision; the removal is still behind (deferred settlement
        // leaves a decided intent in place until the finalizer's remove).
        PreparedIntent x = MakeIntent("r/decided", Ts(2_000), revision: 11, baseRevision: 10);
        Assert.True(intents.ApplyDeltaAckPrepares(SourcePartition, IntentLog(200, 2_000, new PrepareIntentCommand(x))));
        Assert.True(intents.Replicate(SourcePartition, IntentLog(201, 2_100,
            new ResolveIntentCommand(x.TransactionId, x.Epoch, x.Key, Commit: true, MaterializeOnResolve: true, x.CommitTimestamp))));

        Assert.True(intents.Replicate(DestinationPartition, IntentLog(300, 3_000, Seed("r/decided", 11, 2_001))));

        (TransactionApplyOutcome prepare, BundledCommitJudgement gate) = ApplyDestinationBundle(records, intents, "r/decided", revision: 12, baseRevision: 11);

        Assert.Equal(TransactionApplyOutcome.Applied, prepare);
        Assert.Equal(BundledCommitJudgement.Admitted, gate);
    }

    [Fact]
    public void AfterTheInterleave_TheSourceSettleArrives_AndTheNodeHoldsTheStateOfItsPeers()
    {
        (TransactionRecordStore records, PreparedIntentStore intents) = Stores();

        Assert.True(intents.Replicate(SourcePartition, IntentLog(100, 1_000, Seed("r/converge", 10, 1_000))));

        PreparedIntent x = MakeIntent("r/converge", Ts(2_000), revision: 11, baseRevision: 10);
        Assert.True(intents.ApplyDeltaAckPrepares(SourcePartition, IntentLog(200, 2_000, new PrepareIntentCommand(x))));
        Assert.True(intents.Replicate(DestinationPartition, IntentLog(300, 3_000, Seed("r/converge", 11, 2_001))));

        (TransactionApplyOutcome prepare, BundledCommitJudgement gate) = ApplyDestinationBundle(records, intents, "r/converge", revision: 12, baseRevision: 11);
        Assert.Equal(TransactionApplyOutcome.Applied, prepare);
        Assert.Equal(BundledCommitJudgement.Admitted, gate);

        // The source's backlog now reaches X's settle. It names X, and the key is held by Y: it removes nothing of Y.
        intents.Replicate(SourcePartition, IntentLog(201, 2_100, Settle(x)));

        PreparedIntent? holder = intents.Get("r/converge");
        Assert.NotNull(holder);
        Assert.Equal(Ts(4_000), holder.TransactionId);
        Assert.Equal(1, intents.LiveIntentCount);
    }
}
