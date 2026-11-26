using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.KeyValues.Writes;
using Kahuna.Server.Persistence;
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
///
/// <para>The store keeps the partition whose log carried each live intent, and a prepare is refused only by an
/// intent that the same log carried. The cases below pin that rule in both orders of the two logs, for a
/// committed and for an aborted source leg, across a checkpoint and a restart, and through a whole-partition
/// export and install. The last cases compare every verdict and both ledger slices of a node that applies the
/// two logs in a random order with those of a node that applies them as they were written.</para>
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

    // ── helpers of the cases below ───────────────────────────────────────────────

    /// <summary>The settle of an aborted leg: the abort resolution and the removal.</summary>
    private static PreparedIntentCommand[] Abort(PreparedIntent intent) =>
    [
        new ResolveIntentCommand(intent.TransactionId, intent.Epoch, intent.Key, Commit: false),
        new RemoveIntentCommand(intent.TransactionId, intent.Epoch, intent.Key)
    ];

    private const long ManifestHash = 77;

    private static InitializeTransactionCommand Init(PreparedIntent intent)
    {
        HLCTimestamp id = intent.TransactionId;
        List<TransactionParticipantRef> manifest = [new(intent.Key, KeyValueDurability.Persistent)];
        return new(id, 1, "coord", intent.Key, Ts(id.L + 1), Ts(id.L + 9_000), ManifestHash, manifest, Ts(id.L + 2), id);
    }

    /// <summary>Applies a one-phase bundle of one key as a replica of <paramref name="partition"/> applies it:
    /// the record, the prepare, and the commit through the record store, whose gate reads the intent store.</summary>
    private static (TransactionApplyOutcome Prepare, TransactionRecordApplyResult Commit) ApplyBundle(
        TransactionRecordStore records, PreparedIntentStore intents, int partition, PreparedIntent intent)
    {
        Assert.Equal(TransactionApplyOutcome.Applied, records.Apply(Init(intent), partition).Outcome);

        TransactionApplyOutcome prepare = intents.Apply(new PrepareIntentCommand(intent), partition).Outcome;
        return (prepare, records.Apply(Bundle(intent.TransactionId, intent.Key, ManifestHash), partition));
    }

    // ── an aborted source leg: no seeded head reflects it ────────────────────────

    [Fact]
    public void AbortedSourceLeg_ItsSettleStillBehind_TheDestinationBundleIsAdmitted()
    {
        (TransactionRecordStore records, PreparedIntentStore intents) = Stores();

        Assert.True(intents.Replicate(SourcePartition, IntentLog(100, 1_000, Seed("r/aborted", 10, 1_000))));

        // X prepared on the source and aborted there before the cutover. The backlog is at its prepare.
        PreparedIntent x = MakeIntent("r/aborted", Ts(2_000), revision: 11, baseRevision: 10);
        Assert.True(intents.ApplyDeltaAckPrepares(SourcePartition, IntentLog(200, 2_000, new PrepareIntentCommand(x))));

        // The seed is the head before X: the destination's next transaction stages the revision X staged.
        Assert.True(intents.Replicate(DestinationPartition, IntentLog(300, 3_000, Seed("r/aborted", 10, 1_000))));

        (TransactionApplyOutcome prepare, BundledCommitJudgement gate) = ApplyDestinationBundle(records, intents, "r/aborted", revision: 11, baseRevision: 10);

        Assert.Equal(TransactionApplyOutcome.Applied, prepare);
        Assert.Equal(BundledCommitJudgement.Admitted, gate);

        // The source's abort arrives. It removes X and leaves the destination's intent and both heads as they are.
        Assert.True(intents.Replicate(SourcePartition, IntentLog(201, 2_100, Abort(x))));

        Assert.Equal(Ts(4_000), intents.Get("r/aborted")!.TransactionId);
        Assert.Equal(1, intents.LiveIntentCount);
        Assert.Equal(0, intents.CoveredIntentCount);
        Assert.True(intents.TryGetLedgerHead(SourcePartition, "r/aborted", out long sourceHead, out _, out _));
        Assert.Equal(10, sourceHead);
    }

    // ── the source log keeps its own state while the destination holds the key ────

    [Fact]
    public void AfterTheInterleave_TheSourceSettle_FeedsTheSourceSliceAsOnThePeers()
    {
        (TransactionRecordStore records, PreparedIntentStore intents) = Stores();

        Assert.True(intents.Replicate(SourcePartition, IntentLog(100, 1_000, Seed("r/slice", 10, 1_000))));

        PreparedIntent x = MakeIntent("r/slice", Ts(2_000), revision: 11, baseRevision: 10);
        Assert.True(intents.ApplyDeltaAckPrepares(SourcePartition, IntentLog(200, 2_000, new PrepareIntentCommand(x))));
        Assert.True(intents.Replicate(DestinationPartition, IntentLog(300, 3_000, Seed("r/slice", 11, 2_001))));

        (TransactionApplyOutcome prepare, BundledCommitJudgement gate) = ApplyDestinationBundle(records, intents, "r/slice", revision: 12, baseRevision: 11);
        Assert.Equal(TransactionApplyOutcome.Applied, prepare);
        Assert.Equal(BundledCommitJudgement.Admitted, gate);

        // X is live for the source log only: the node shows the destination's intent.
        Assert.Equal(Ts(4_000), intents.Get("r/slice")!.TransactionId);
        Assert.NotNull(intents.GetByIdentity(x.TransactionId, x.Epoch, x.Key));
        Assert.Equal(1, intents.CoveredIntentCount);

        Assert.True(intents.Replicate(SourcePartition, IntentLog(201, 2_100, Settle(x))));

        // The source slice holds what a peer's source slice holds: the head X committed and its settled identity.
        Assert.True(intents.TryGetLedgerHead(SourcePartition, "r/slice", out long sourceHead, out _, out HLCTimestamp committedAt));
        Assert.Equal(11, sourceHead);
        Assert.Equal(x.CommitTimestamp, committedAt);

        PreparedIntentStore.SettledIdentityEntry settled = Assert.Single(intents.SnapshotSettledIdentities(SourcePartition));
        Assert.Equal(x.TransactionId, settled.TransactionId);
        Assert.Equal(x.CommitTimestamp, settled.SettledAt);

        // The destination slice still holds the seed: its own transaction has not settled.
        Assert.True(intents.TryGetLedgerHead(DestinationPartition, "r/slice", out long destinationHead, out _, out _));
        Assert.Equal(11, destinationHead);
        Assert.Empty(intents.SnapshotSettledIdentities(DestinationPartition));
        Assert.Equal(0, intents.CoveredIntentCount);
    }

    [Fact]
    public void SourceCompetitorOfTheReplayedLeg_IsStillRefused()
    {
        (TransactionRecordStore records, PreparedIntentStore intents) = Stores();

        Assert.True(intents.Replicate(SourcePartition, IntentLog(100, 1_000, Seed("r/same-log", 10, 1_000))));

        PreparedIntent x = MakeIntent("r/same-log", Ts(2_000), revision: 11, baseRevision: 10);
        Assert.True(intents.ApplyDeltaAckPrepares(SourcePartition, IntentLog(200, 2_000, new PrepareIntentCommand(x))));
        Assert.True(intents.Replicate(DestinationPartition, IntentLog(300, 3_000, Seed("r/same-log", 11, 2_001))));

        (TransactionApplyOutcome prepare, _) = ApplyDestinationBundle(records, intents, "r/same-log", revision: 12, baseRevision: 11);
        Assert.Equal(TransactionApplyOutcome.Applied, prepare);

        // A second transaction of the source log on the key, before X's settle: every peer refused it, because X
        // held the key in the source log. X is covered here, and it still refuses.
        PreparedIntent w = MakeIntent("r/same-log", Ts(2_050), revision: 11, baseRevision: 10);
        Assert.False(intents.ApplyDeltaAckPrepares(SourcePartition, IntentLog(201, 2_050, new PrepareIntentCommand(w))));
        Assert.True(intents.TryTakePrepareRejection(w.TransactionId, 1, "r/same-log", out PrepareRejectionKind kind));
        Assert.Equal(PrepareRejectionKind.KeyHeld, kind);

        // A second transaction of the destination log on the key is refused by the destination's holder.
        PreparedIntent z = MakeIntent("r/same-log", Ts(4_100), revision: 12, baseRevision: 11);
        Assert.False(intents.ApplyDeltaAckPrepares(DestinationPartition, IntentLog(301, 4_100, new PrepareIntentCommand(z))));
        Assert.True(intents.TryTakePrepareRejection(z.TransactionId, 1, "r/same-log", out kind));
        Assert.Equal(PrepareRejectionKind.KeyHeld, kind);

        Assert.Equal(1, intents.LiveIntentCount);
        Assert.Equal(1, intents.CoveredIntentCount);
    }

    // ── the opposite order: source history replayed while the destination holds the key ──

    [Fact]
    public void DestinationIntentLive_TheSourceBundleReplayed_IsAdmitted_AndTheNodeShowsTheDestinationIntent()
    {
        (TransactionRecordStore records, PreparedIntentStore intents) = Stores();

        // The destination's log is ahead: the seed, and a transaction that prepared and has not settled.
        Assert.True(intents.Replicate(DestinationPartition, IntentLog(300, 3_000, Seed("r/opposite", 11, 2_001))));

        PreparedIntent z = MakeIntent("r/opposite", Ts(4_000), revision: 12, baseRevision: 11, manifestHash: ManifestHash);
        (TransactionApplyOutcome zPrepare, TransactionRecordApplyResult zCommit) = ApplyBundle(records, intents, DestinationPartition, z);
        Assert.Equal(TransactionApplyOutcome.Applied, zPrepare);
        Assert.Equal(TransactionApplyOutcome.Applied, zCommit.Outcome);

        // The source's backlog reaches a bundle of the key from before the cutover.
        Assert.True(intents.Replicate(SourcePartition, IntentLog(100, 1_000, Seed("r/opposite", 10, 1_000))));

        PreparedIntent x = MakeIntent("r/opposite", Ts(2_000), revision: 11, baseRevision: 10, manifestHash: ManifestHash);
        (TransactionApplyOutcome xPrepare, TransactionRecordApplyResult xCommit) = ApplyBundle(records, intents, SourcePartition, x);

        // Every peer applied this bundle. A node that refuses it keeps a record of the transaction that its
        // peers do not have, and its source slice never records the head of the transaction for the key.
        Assert.Equal(TransactionApplyOutcome.Applied, xPrepare);
        Assert.Equal(TransactionApplyOutcome.Applied, xCommit.Outcome);

        // The node shows the write of the key's present partition. The source leg waits for its own settle.
        Assert.Equal(z.TransactionId, intents.Get("r/opposite")!.TransactionId);
        Assert.Equal(1, intents.LiveIntentCount);
        Assert.Equal(1, intents.CoveredIntentCount);

        Assert.True(intents.Replicate(SourcePartition, IntentLog(201, 2_100, Settle(x))));

        Assert.Equal(z.TransactionId, intents.Get("r/opposite")!.TransactionId);
        Assert.Equal(0, intents.CoveredIntentCount);
        Assert.True(intents.TryGetLedgerHead(SourcePartition, "r/opposite", out long sourceHead, out _, out _));
        Assert.Equal(11, sourceHead);

        // The destination's settle of its own transaction is not disturbed by the source history.
        Assert.True(intents.Replicate(DestinationPartition, IntentLog(301, 4_100, Settle(z))));
        Assert.Null(intents.Get("r/opposite"));
        Assert.True(intents.TryGetLedgerHead(DestinationPartition, "r/opposite", out long destinationHead, out _, out _));
        Assert.Equal(12, destinationHead);
    }

    [Fact]
    public void SourceIntentReplayedOverAFreeKey_DoesNotRefuseTheNextDestinationBundle()
    {
        (TransactionRecordStore records, PreparedIntentStore intents) = Stores();

        // The destination settled everything it has for the key. The key is free when the source's backlog
        // installs a leg from before the cutover.
        Assert.True(intents.Replicate(DestinationPartition, IntentLog(300, 3_000, Seed("r/free", 11, 2_001))));
        Assert.True(intents.Replicate(SourcePartition, IntentLog(100, 1_000, Seed("r/free", 10, 1_000))));

        PreparedIntent x = MakeIntent("r/free", Ts(2_000), revision: 11, baseRevision: 10);
        Assert.True(intents.ApplyDeltaAckPrepares(SourcePartition, IntentLog(200, 2_000, new PrepareIntentCommand(x))));
        Assert.Equal(x.TransactionId, intents.Get("r/free")!.TransactionId);

        (TransactionApplyOutcome prepare, BundledCommitJudgement gate) = ApplyDestinationBundle(records, intents, "r/free", revision: 12, baseRevision: 11);

        Assert.Equal(TransactionApplyOutcome.Applied, prepare);
        Assert.Equal(BundledCommitJudgement.Admitted, gate);
        Assert.Equal(Ts(4_000), intents.Get("r/free")!.TransactionId);
    }

    // ── a read dependency of a bundle ────────────────────────────────────────────

    [Fact]
    public void ReadDependencyOnAKeyThatASourceLegHolds_IsNotAStaleRead()
    {
        (TransactionRecordStore records, PreparedIntentStore intents) = Stores();

        // A source leg of the key the bundle reads, replayed with its settle still behind.
        Assert.True(intents.Replicate(SourcePartition, IntentLog(100, 1_000, Seed("r/read", 5, 1_000))));
        PreparedIntent sourceLeg = MakeIntent("r/read", Ts(2_000), revision: 6, baseRevision: 5);
        Assert.True(intents.ApplyDeltaAckPrepares(SourcePartition, IntentLog(200, 2_000, new PrepareIntentCommand(sourceLeg))));

        Assert.True(intents.Replicate(DestinationPartition, IntentLog(300, 3_000, Seed("r/read", 6, 2_001), Seed("r/written", 11, 2_001))));

        PreparedIntent y = MakeIntent("r/written", Ts(4_000), revision: 12, baseRevision: 11, manifestHash: ManifestHash);
        Assert.Equal(TransactionApplyOutcome.Applied, records.Apply(Init(y), DestinationPartition).Outcome);
        Assert.Equal(TransactionApplyOutcome.Applied, intents.Apply(new PrepareIntentCommand(y), DestinationPartition).Outcome);

        CommitTransactionCommand bundle = new(
            y.TransactionId, 1, ManifestHash, OpId: Ts(4_002), AttemptHlc: Ts(4_002), BundledPrepareKeys: ["r/written"],
            ApplyTimeValidation: true, BundledReadDependencies: [new BundledReadDependency("r/read", 6, true)]);

        Assert.Equal(BundledCommitJudgement.Admitted, intents.JudgeBundledCommit(DestinationPartition, bundle));
    }

    [Fact]
    public void ReadDependencyOnAKeyThatADestinationIntentHolds_IsStillAStaleRead()
    {
        (TransactionRecordStore records, PreparedIntentStore intents) = Stores();

        Assert.True(intents.Replicate(DestinationPartition, IntentLog(300, 3_000, Seed("r/read2", 6, 2_001), Seed("r/written2", 11, 2_001))));

        PreparedIntent competitor = MakeIntent("r/read2", Ts(3_500), revision: 7, baseRevision: 6);
        Assert.True(intents.ApplyDeltaAckPrepares(DestinationPartition, IntentLog(301, 3_500, new PrepareIntentCommand(competitor))));

        PreparedIntent y = MakeIntent("r/written2", Ts(4_000), revision: 12, baseRevision: 11, manifestHash: ManifestHash);
        Assert.Equal(TransactionApplyOutcome.Applied, records.Apply(Init(y), DestinationPartition).Outcome);
        Assert.Equal(TransactionApplyOutcome.Applied, intents.Apply(new PrepareIntentCommand(y), DestinationPartition).Outcome);

        CommitTransactionCommand bundle = new(
            y.TransactionId, 1, ManifestHash, OpId: Ts(4_002), AttemptHlc: Ts(4_002), BundledPrepareKeys: ["r/written2"],
            ApplyTimeValidation: true, BundledReadDependencies: [new BundledReadDependency("r/read2", 6, true)]);

        Assert.Equal(BundledCommitVerdict.StaleRead, intents.JudgeBundledCommit(DestinationPartition, bundle).Verdict);
    }

    // ── an intent the move handed to the destination holds the key there ─────────

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void IntentHandedToTheDestination_RefusesTheDestinationPrepare(bool sourceFirst)
    {
        PreparedIntentStore intents = new();

        PreparedIntent x = MakeIntent("r/handed", Ts(2_000), revision: 11, baseRevision: 10);

        // The move found X unsettled and re-logged its prepare on the destination. A replica of both logs
        // applies the two copies in either order.
        if (sourceFirst)
        {
            Assert.True(intents.ApplyDeltaAckPrepares(SourcePartition, IntentLog(200, 2_000, new PrepareIntentCommand(x))));
            Assert.True(intents.ApplyDeltaAckPrepares(DestinationPartition, IntentLog(300, 3_000, new PrepareIntentCommand(x))));
        }
        else
        {
            Assert.True(intents.ApplyDeltaAckPrepares(DestinationPartition, IntentLog(300, 3_000, new PrepareIntentCommand(x))));
            Assert.True(intents.ApplyDeltaAckPrepares(SourcePartition, IntentLog(200, 2_000, new PrepareIntentCommand(x))));
        }

        // X is the destination's state from the hand-off on: the next transaction of the key waits for it.
        PreparedIntent y = MakeIntent("r/handed", Ts(4_000), revision: 12, baseRevision: 11);
        Assert.False(intents.ApplyDeltaAckPrepares(DestinationPartition, IntentLog(301, 4_000, new PrepareIntentCommand(y))));
        Assert.True(intents.TryTakePrepareRejection(y.TransactionId, 1, "r/handed", out PrepareRejectionKind kind));
        Assert.Equal(PrepareRejectionKind.KeyHeld, kind);

        // Its settle through the destination's log frees the key, as before.
        Assert.True(intents.Replicate(DestinationPartition, IntentLog(302, 4_100, Settle(x))));
        Assert.Null(intents.Get("r/handed"));
        Assert.Equal(0, intents.CoveredIntentCount);
        Assert.True(intents.ApplyDeltaAckPrepares(DestinationPartition, IntentLog(303, 4_200, new PrepareIntentCommand(y))));
    }

    // ── a settle that finds another transaction at the key ───────────────────────

    [Fact]
    public void SettleOfALegThatIsNotLive_WhileAnotherTransactionHoldsTheKey_IsRemembered()
    {
        PreparedIntentStore intents = new();

        // The destination's stream is ahead: its own transaction holds the key when the settle of X arrives,
        // a leg whose prepare the source log carries and has not delivered yet.
        PreparedIntent z = MakeIntent("r/remembered", Ts(4_000), revision: 12, baseRevision: 11);
        Assert.True(intents.ApplyDeltaAckPrepares(DestinationPartition, IntentLog(300, 4_000, new PrepareIntentCommand(z))));

        PreparedIntent x = MakeIntent("r/remembered", Ts(2_000), revision: 11, baseRevision: 10);
        Assert.True(intents.Replicate(DestinationPartition, IntentLog(301, 4_050, Settle(x))));

        // The settle names X, so Z stays. The leg is remembered as settled, and its prepare installs nothing.
        Assert.Equal(z.TransactionId, intents.Get("r/remembered")!.TransactionId);

        Assert.False(intents.ApplyDeltaAckPrepares(SourcePartition, IntentLog(200, 2_000, new PrepareIntentCommand(x))));
        Assert.True(intents.TryTakePrepareRejection(x.TransactionId, 1, "r/remembered", out PrepareRejectionKind kind));
        Assert.Equal(PrepareRejectionKind.Settled, kind);
        Assert.Equal(0, intents.CoveredIntentCount);
        Assert.Equal(1, intents.LiveIntentCount);
    }

    [Fact]
    public void AbortOfARefusedPrepare_WhileTheHolderOfItsOwnLogHoldsTheKey_IsNotRemembered()
    {
        PreparedIntentStore intents = new();

        // Z holds the key in the destination's log. X is refused there, and its coordinator rolls the attempt
        // back while Z still holds the key.
        PreparedIntent z = MakeIntent("r/refused", Ts(4_000), revision: 12, baseRevision: 11);
        Assert.True(intents.ApplyDeltaAckPrepares(DestinationPartition, IntentLog(300, 4_000, new PrepareIntentCommand(z))));

        PreparedIntent x = MakeIntent("r/refused", Ts(4_010), revision: 12, baseRevision: 11);
        Assert.False(intents.ApplyDeltaAckPrepares(DestinationPartition, IntentLog(301, 4_010, new PrepareIntentCommand(x))));
        Assert.True(intents.TryTakePrepareRejection(x.TransactionId, 1, "r/refused", out PrepareRejectionKind kind));
        Assert.Equal(PrepareRejectionKind.KeyHeld, kind);

        Assert.True(intents.Replicate(DestinationPartition, IntentLog(302, 4_020, Abort(x))));
        Assert.Empty(intents.SnapshotSettledIdentities(DestinationPartition));
        Assert.Equal(z.TransactionId, intents.Get("r/refused")!.TransactionId);

        // The same attempt is prepared again while Z holds the key: it is refused as held, not as settled, so
        // its coordinator still counts a live conflict.
        Assert.False(intents.ApplyDeltaAckPrepares(DestinationPartition, IntentLog(303, 4_030, new PrepareIntentCommand(x))));
        Assert.True(intents.TryTakePrepareRejection(x.TransactionId, 1, "r/refused", out kind));
        Assert.Equal(PrepareRejectionKind.KeyHeld, kind);
    }

    // ── the map shows the latest live intent of the key ──────────────────────────

    [Fact]
    public void DestinationIntentSettlesFirst_TheSourceLegReturnsToTheMap_UntilItsOwnSettle()
    {
        (TransactionRecordStore records, PreparedIntentStore intents) = Stores();

        Assert.True(intents.Replicate(SourcePartition, IntentLog(100, 1_000, Seed("r/promoted", 10, 1_000))));

        PreparedIntent x = MakeIntent("r/promoted", Ts(2_000), revision: 11, baseRevision: 10);
        Assert.True(intents.ApplyDeltaAckPrepares(SourcePartition, IntentLog(200, 2_000, new PrepareIntentCommand(x))));
        Assert.True(intents.Replicate(DestinationPartition, IntentLog(300, 3_000, Seed("r/promoted", 11, 2_001))));

        PreparedIntent y = MakeIntent("r/promoted", Ts(4_000), revision: 12, baseRevision: 11, manifestHash: ManifestHash);
        (TransactionApplyOutcome prepare, TransactionRecordApplyResult commit) = ApplyBundle(records, intents, DestinationPartition, y);
        Assert.Equal(TransactionApplyOutcome.Applied, prepare);
        Assert.Equal(TransactionApplyOutcome.Applied, commit.Outcome);
        Assert.Equal(1, intents.CoveredIntentCount);

        // The destination's transaction settles before the source's backlog reaches X's settle.
        Assert.True(intents.Replicate(DestinationPartition, IntentLog(301, 4_100, Settle(y))));

        // X is the one live intent of the key now, and it is in the map: a sweep of the map finds a leg whose
        // own settle never arrives. It still does not hold the key against the destination.
        Assert.Equal(x.TransactionId, intents.Get("r/promoted")!.TransactionId);
        Assert.Equal(0, intents.CoveredIntentCount);
        Assert.Equal(1, intents.LiveIntentCount);

        PreparedIntent next = MakeIntent("r/promoted", Ts(5_000), revision: 13, baseRevision: 12);
        Assert.True(intents.ApplyDeltaAckPrepares(DestinationPartition, IntentLog(302, 5_000, new PrepareIntentCommand(next))));
        Assert.Equal(next.TransactionId, intents.Get("r/promoted")!.TransactionId);

        Assert.True(intents.Replicate(SourcePartition, IntentLog(201, 2_100, Settle(x))));
        Assert.Equal(next.TransactionId, intents.Get("r/promoted")!.TransactionId);
        Assert.Equal(0, intents.CoveredIntentCount);
        Assert.Equal(1, intents.LiveIntentCount);
    }

    // ── a checkpoint inside the interleave, then a restart ───────────────────────

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void CheckpointInsideTheInterleave_ThenARestart_KeepsTheCarrierOfEachIntent(bool destinationIntentLive)
    {
        string directory = Path.Combine(Path.GetTempPath(), "kahuna-interleave-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);

        try
        {
            PreparedIntent x = MakeIntent("r/restart", Ts(2_000), revision: 11, baseRevision: 10);
            PreparedIntent y = MakeIntent("r/restart", Ts(4_000), revision: 12, baseRevision: 11, manifestHash: ManifestHash);

            // The node routes the key to the destination: it applied the cutover.
            PreparedIntentStore store = new(directory, "rev", null);
            store.AttachPartitionResolver(_ => DestinationPartition);

            Assert.True(store.Replicate(SourcePartition, IntentLog(100, 1_000, Seed("r/restart", 10, 1_000))));
            Assert.True(store.ApplyDeltaAckPrepares(SourcePartition, IntentLog(200, 2_000, new PrepareIntentCommand(x))));
            Assert.True(store.Replicate(DestinationPartition, IntentLog(300, 3_000, Seed("r/restart", 11, 2_001))));

            if (destinationIntentLive)
                Assert.True(store.ApplyDeltaAckPrepares(DestinationPartition, IntentLog(301, 4_000, new PrepareIntentCommand(y))));

            Assert.True(store.PersistSnapshot(SourcePartition));
            Assert.True(store.PersistSnapshot(DestinationPartition));

            PreparedIntentStore reloaded = new(directory, "rev", null);
            reloaded.AttachPartitionResolver(_ => DestinationPartition);

            TransactionRecordStore records = new();
            records.AttachBundledCommitJudge(reloaded.JudgeBundledCommit);

            // The source leg came back as an intent of the source log: it does not hold the key against the
            // destination, whose file it was written to.
            Assert.NotNull(reloaded.GetByIdentity(x.TransactionId, x.Epoch, x.Key));

            Assert.Equal(TransactionApplyOutcome.Applied, records.Apply(Init(y), DestinationPartition).Outcome);

            if (destinationIntentLive)
            {
                Assert.Equal(y.TransactionId, reloaded.Get("r/restart")!.TransactionId);
                Assert.Equal(1, reloaded.CoveredIntentCount);
            }
            else
                Assert.Equal(TransactionApplyOutcome.Applied, reloaded.Apply(new PrepareIntentCommand(y), DestinationPartition).Outcome);

            Assert.Equal(TransactionApplyOutcome.Applied, records.Apply(Bundle(y.TransactionId, y.Key, ManifestHash), DestinationPartition).Outcome);

            // The source's settle, replayed after the restart, finds its leg and feeds the source slice.
            Assert.True(reloaded.Replicate(SourcePartition, IntentLog(201, 2_100, Settle(x))));

            Assert.Equal(y.TransactionId, reloaded.Get("r/restart")!.TransactionId);
            Assert.Equal(0, reloaded.CoveredIntentCount);
            Assert.True(reloaded.TryGetLedgerHead(SourcePartition, "r/restart", out long sourceHead, out _, out _));
            Assert.Equal(11, sourceHead);
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { /* best effort */ }
        }
    }

    // ── a whole-partition export and its install ─────────────────────────────────

    [Fact]
    public void ExportOfTheDestination_CarriesItsOwnIntents_AndLeavesOutTheSourceLeg()
    {
        PreparedIntentStore exporter = new();

        PreparedIntent x = MakeIntent("r/export", Ts(2_000), revision: 11, baseRevision: 10);
        PreparedIntent y = MakeIntent("r/export", Ts(4_000), revision: 12, baseRevision: 11);
        PreparedIntent sourceOnly = MakeIntent("r/export-source", Ts(2_100), revision: 4, baseRevision: 3);
        PreparedIntent handed = MakeIntent("r/export-handed", Ts(2_200), revision: 8, baseRevision: 7);

        Assert.True(exporter.ApplyDeltaAckPrepares(SourcePartition, IntentLog(200, 2_000, new PrepareIntentCommand(x))));
        Assert.True(exporter.ApplyDeltaAckPrepares(SourcePartition, IntentLog(201, 2_100, new PrepareIntentCommand(sourceOnly))));
        Assert.True(exporter.ApplyDeltaAckPrepares(SourcePartition, IntentLog(202, 2_200, new PrepareIntentCommand(handed))));
        Assert.True(exporter.ApplyDeltaAckPrepares(DestinationPartition, IntentLog(300, 3_000, new PrepareIntentCommand(handed))));
        Assert.True(exporter.ApplyDeltaAckPrepares(DestinationPartition, IntentLog(301, 4_000, new PrepareIntentCommand(y))));

        using MemoryStream payload = new();
        int written = exporter.WritePartitionSection(payload, DestinationPartition, static _ => true);

        PreparedIntentStore.PartitionIntentSection section = PreparedIntentStore.DeserializePartitionIntents(payload.ToArray());

        // The destination's own transaction and the intent the move handed to it. The two source legs are the
        // state of the source log: installed as the destination's, each would refuse the destination's prepares.
        Assert.Equal(2, written);
        Assert.Equal(
            [handed.TransactionId, y.TransactionId],
            section.Intents.Select(intent => intent.TransactionId).OrderBy(id => id.L).ToArray());

        // The export of the source carries the source legs, the covered one too, and not the destination's own.
        using MemoryStream sourcePayload = new();
        Assert.Equal(3, exporter.WritePartitionSection(sourcePayload, SourcePartition, static _ => true));
        Assert.Equal(
            [x.TransactionId, sourceOnly.TransactionId, handed.TransactionId],
            PreparedIntentStore.DeserializePartitionIntents(sourcePayload.ToArray()).Intents.Select(intent => intent.TransactionId).OrderBy(id => id.L).ToArray());
    }

    [Fact]
    public void InstallOfTheDestination_ReplacesItsOwnIntents_AndKeepsTheSourceLeg()
    {
        PreparedIntentStore exporter = new();
        PreparedIntent y = MakeIntent("r/install", Ts(4_000), revision: 12, baseRevision: 11);
        Assert.True(exporter.Replicate(DestinationPartition, IntentLog(300, 3_000, Seed("r/install", 11, 2_001))));
        Assert.True(exporter.ApplyDeltaAckPrepares(DestinationPartition, IntentLog(301, 4_000, new PrepareIntentCommand(y))));

        using MemoryStream payload = new();
        exporter.WritePartitionSection(payload, DestinationPartition, static _ => true);
        PreparedIntentStore.PartitionIntentSection section = PreparedIntentStore.DeserializePartitionIntents(payload.ToArray());

        // The receiver holds a source leg of the key, whose settle its source log has not delivered, and an intent
        // of the destination log that the exporter settled long ago.
        PreparedIntentStore receiver = new();
        PreparedIntent x = MakeIntent("r/install", Ts(2_000), revision: 11, baseRevision: 10);
        PreparedIntent stale = MakeIntent("r/install-stale", Ts(3_500), revision: 3, baseRevision: 2);

        Assert.True(receiver.Replicate(SourcePartition, IntentLog(100, 1_000, Seed("r/install", 10, 1_000))));
        Assert.True(receiver.ApplyDeltaAckPrepares(SourcePartition, IntentLog(200, 2_000, new PrepareIntentCommand(x))));
        Assert.True(receiver.ApplyDeltaAckPrepares(DestinationPartition, IntentLog(250, 3_500, new PrepareIntentCommand(stale))));

        // A second stale intent of the destination log, covered by a later source leg of its key.
        PreparedIntent staleCovered = MakeIntent("r/install-covered", Ts(3_600), revision: 3, baseRevision: 2);
        PreparedIntent laterSourceLeg = MakeIntent("r/install-covered", Ts(3_700), revision: 3, baseRevision: 2);
        Assert.True(receiver.ApplyDeltaAckPrepares(DestinationPartition, IntentLog(251, 3_600, new PrepareIntentCommand(staleCovered))));
        Assert.True(receiver.ApplyDeltaAckPrepares(SourcePartition, IntentLog(202, 3_700, new PrepareIntentCommand(laterSourceLeg))));

        // A third stale intent of the destination log, which covers an earlier source leg of its key.
        PreparedIntent coveredSourceLeg = MakeIntent("r/install-kept", Ts(2_500), revision: 3, baseRevision: 2);
        PreparedIntent staleOver = MakeIntent("r/install-kept", Ts(3_800), revision: 3, baseRevision: 2);
        Assert.True(receiver.ApplyDeltaAckPrepares(SourcePartition, IntentLog(203, 2_500, new PrepareIntentCommand(coveredSourceLeg))));
        Assert.True(receiver.ApplyDeltaAckPrepares(DestinationPartition, IntentLog(252, 3_800, new PrepareIntentCommand(staleOver))));
        Assert.Equal(staleOver.TransactionId, receiver.Get("r/install-kept")!.TransactionId);
        Assert.Equal(2, receiver.CoveredIntentCount);

        receiver.ReplacePartitionIntents(DestinationPartition, section, requireLedger: true, static _ => true);

        // The covered source leg outlives the destination intent that covered it, and the map shows it.
        Assert.Equal(coveredSourceLeg.TransactionId, receiver.Get("r/install-kept")!.TransactionId);
        Assert.Null(receiver.GetByIdentity(staleOver.TransactionId, staleOver.Epoch, staleOver.Key));

        // The destination's state is the exporter's. The source leg is the state of the source log, which the
        // install does not replace: its settle still finds it and still records the head on the source slice.
        Assert.Null(receiver.Get("r/install-stale"));
        Assert.Null(receiver.GetByIdentity(staleCovered.TransactionId, staleCovered.Epoch, staleCovered.Key));
        Assert.Equal(laterSourceLeg.TransactionId, receiver.Get("r/install-covered")!.TransactionId);
        Assert.Equal(y.TransactionId, receiver.Get("r/install")!.TransactionId);
        Assert.NotNull(receiver.GetByIdentity(x.TransactionId, x.Epoch, x.Key));

        Assert.True(receiver.Replicate(SourcePartition, IntentLog(201, 2_100, Settle(x))));
        Assert.True(receiver.TryGetLedgerHead(SourcePartition, "r/install", out long sourceHead, out _, out _));
        Assert.Equal(11, sourceHead);
        Assert.Equal(0, receiver.CoveredIntentCount);
    }

    // ── lookups by the identity of a transaction reach a covered intent ──────────

    [Fact]
    public void CoveredLeg_SettledThroughTheOtherLog_IsFoundByItsIdentity()
    {
        (TransactionRecordStore records, PreparedIntentStore intents) = Stores();

        Assert.True(intents.Replicate(SourcePartition, IntentLog(100, 1_000, Seed("r/cross", 10, 1_000))));

        PreparedIntent x = MakeIntent("r/cross", Ts(2_000), revision: 11, baseRevision: 10);
        Assert.True(intents.ApplyDeltaAckPrepares(SourcePartition, IntentLog(200, 2_000, new PrepareIntentCommand(x))));
        Assert.True(intents.Replicate(DestinationPartition, IntentLog(300, 3_000, Seed("r/cross", 10, 1_000))));

        (TransactionApplyOutcome prepare, _) = ApplyDestinationBundle(records, intents, "r/cross", revision: 11, baseRevision: 10);
        Assert.Equal(TransactionApplyOutcome.Applied, prepare);
        Assert.Equal(1, intents.CoveredIntentCount);

        // The settle of the source leg arrives through the destination's log, which did not carry its prepare.
        // It names its transaction: it settles that intent and leaves the destination's holder in place.
        Assert.True(intents.Replicate(DestinationPartition, IntentLog(301, 4_050, Settle(x))));

        Assert.Null(intents.GetByIdentity(x.TransactionId, x.Epoch, x.Key));
        Assert.Equal(0, intents.CoveredIntentCount);
        Assert.Equal(Ts(4_000), intents.Get("r/cross")!.TransactionId);
        Assert.Equal(x.TransactionId, Assert.Single(intents.SnapshotSettledIdentities(DestinationPartition)).TransactionId);
    }

    private sealed class RecordingInstaller : IResolvedIntentInstaller
    {
        public List<(int Partition, HLCTimestamp TransactionId, long Revision)> Installed { get; } = [];

        public List<(int Partition, HLCTimestamp TransactionId, string Key, HLCTimestamp CommitTimestamp)> UnresolvedOnApply { get; } = [];

        public void Install(int partitionId, long logIndex, PreparedIntent intent, bool replay) =>
            Installed.Add((partitionId, intent.TransactionId, intent.Revision));

        public void CompleteEntry(int partitionId, long logIndex, bool replay)
        {
        }

        public void NoteUnresolvedOnReplay(int partitionId, long logIndex, HLCTimestamp transactionId, long epoch, string key, HLCTimestamp commitTimestamp)
        {
        }

        public void NoteUnresolvedOnApply(int partitionId, long logIndex, HLCTimestamp transactionId, long epoch, string key, HLCTimestamp commitTimestamp) =>
            UnresolvedOnApply.Add((partitionId, transactionId, key, commitTimestamp));
    }

    [Fact]
    public void MaterializingSettleOfACoveredLeg_InstallsTheValueOfThatLeg()
    {
        (TransactionRecordStore records, PreparedIntentStore intents) = Stores();

        RecordingInstaller installer = new();
        intents.AttachResolvedIntentInstaller(installer);

        Assert.True(intents.Replicate(SourcePartition, IntentLog(100, 1_000, Seed("r/installed", 10, 1_000))));

        PreparedIntent x = MakeIntent("r/installed", Ts(2_000), revision: 11, baseRevision: 10);
        Assert.True(intents.ApplyDeltaAckPrepares(SourcePartition, IntentLog(200, 2_000, new PrepareIntentCommand(x))));
        Assert.True(intents.Replicate(DestinationPartition, IntentLog(300, 3_000, Seed("r/installed", 11, 2_001))));

        (TransactionApplyOutcome prepare, _) = ApplyDestinationBundle(records, intents, "r/installed", revision: 12, baseRevision: 11);
        Assert.Equal(TransactionApplyOutcome.Applied, prepare);

        // The source's settle installs the value of the source leg, as on every peer, and not the value of the
        // intent the key shows.
        Assert.True(intents.Replicate(SourcePartition, IntentLog(201, 2_100, Settle(x))));

        Assert.Equal((SourcePartition, x.TransactionId, 11L), Assert.Single(installer.Installed));
    }

    // ── a materializing resolve with no intent of its transaction is reported ─────

    [Fact]
    public void LiveMaterializingResolve_WithNoIntentOfItsTransaction_IsReportedToTheInstaller()
    {
        PreparedIntentStore intents = new();
        RecordingInstaller installer = new();
        intents.AttachResolvedIntentInstaller(installer);

        // The prepare of X was refused on this node: Z holds the key in the same log. The peers installed X's
        // prepare, so the log carries X's settle.
        PreparedIntent z = MakeIntent("r/missing", Ts(4_000), revision: 12, baseRevision: 11);
        PreparedIntent x = MakeIntent("r/missing", Ts(4_010), revision: 12, baseRevision: 11);

        Assert.True(intents.ApplyDeltaAckPrepares(DestinationPartition, IntentLog(300, 4_000, new PrepareIntentCommand(z))));
        Assert.False(intents.ApplyDeltaAckPrepares(DestinationPartition, IntentLog(301, 4_010, new PrepareIntentCommand(x))));

        Assert.True(intents.Replicate(DestinationPartition, IntentLog(302, 4_020, Settle(x))));

        // Nothing installs, and the installer is told once, by the resolve: it decides if the value is missing.
        Assert.Empty(installer.Installed);
        Assert.Equal((DestinationPartition, x.TransactionId, "r/missing", x.CommitTimestamp), Assert.Single(installer.UnresolvedOnApply));
    }

    [Fact]
    public void ResolveThatInstalls_AndSettlesThatCarryNoValue_AreNotReported()
    {
        PreparedIntentStore intents = new();
        RecordingInstaller installer = new();
        intents.AttachResolvedIntentInstaller(installer);

        PreparedIntent x = MakeIntent("r/reported", Ts(2_000), revision: 11, baseRevision: 10);
        Assert.True(intents.ApplyDeltaAckPrepares(SourcePartition, IntentLog(200, 2_000, new PrepareIntentCommand(x))));

        // The settle of a present intent installs its value.
        Assert.True(intents.Replicate(SourcePartition, IntentLog(201, 2_100, Settle(x))));
        Assert.Single(installer.Installed);
        Assert.Empty(installer.UnresolvedOnApply);

        // An abort and a commit that does not materialize carry no value: there is nothing to miss.
        PreparedIntent y = MakeIntent("r/reported", Ts(3_000), revision: 12, baseRevision: 11);
        Assert.True(intents.Replicate(SourcePartition, IntentLog(202, 3_000, Abort(y))));
        Assert.True(intents.Replicate(SourcePartition, IntentLog(203, 3_100,
            new ResolveIntentCommand(y.TransactionId, y.Epoch, y.Key, Commit: true),
            new RemoveIntentCommand(y.TransactionId, y.Epoch, y.Key))));
        Assert.Empty(installer.UnresolvedOnApply);

        // A second copy of X's settle finds no intent. It is reported: the installer dismisses it, because the
        // first copy installed the value.
        Assert.True(intents.Replicate(SourcePartition, IntentLog(204, 3_200, Settle(x))));
        Assert.Equal(x.TransactionId, Assert.Single(installer.UnresolvedOnApply).TransactionId);

        // A restart replay goes to the replay report and not to this one.
        Assert.True(intents.Restore(SourcePartition, IntentLog(205, 3_300, Settle(x))));
        Assert.Single(installer.UnresolvedOnApply);

        // A commit over an intent that this node holds as aborted is refused by the state machine. The node
        // holds the intent, so this is not the report of a transaction with no intent here.
        PreparedIntent aborted = MakeIntent("r/reported", Ts(5_000), revision: 12, baseRevision: 11);
        Assert.True(intents.ApplyDeltaAckPrepares(SourcePartition, IntentLog(206, 5_000, new PrepareIntentCommand(aborted))));
        Assert.True(intents.Replicate(SourcePartition, IntentLog(207, 5_100,
            new ResolveIntentCommand(aborted.TransactionId, aborted.Epoch, aborted.Key, Commit: false))));
        Assert.True(intents.Replicate(SourcePartition, IntentLog(208, 5_200,
            new ResolveIntentCommand(aborted.TransactionId, aborted.Epoch, aborted.Key, Commit: true, MaterializeOnResolve: true, aborted.CommitTimestamp))));
        Assert.Single(installer.UnresolvedOnApply);
        Assert.Single(installer.Installed);
    }

    [Fact]
    public void ValueOfAResolveWithNoIntent_IsProvenFromMemory_ByAQueuedWriteOrAHeadAtOrAfterTheCommit()
    {
        PreparedIntentStore intents = new();
        PreparedIntent x = MakeIntent("r/proof", Ts(2_000), revision: 11, baseRevision: 10);
        Assert.True(intents.ApplyDeltaAckPrepares(DestinationPartition, IntentLog(300, 2_000, new PrepareIntentCommand(x))));
        Assert.True(intents.Replicate(DestinationPartition, IntentLog(301, 2_100, Settle(x))));

        HLCTimestamp commit = x.CommitTimestamp;
        HLCTimestamp before = new(commit.N, commit.L - 1, commit.C);
        HLCTimestamp after = new(commit.N, commit.L + 1, commit.C);

        // The committed head of the key: the commit itself and an earlier commit are proven, a later one is not.
        Assert.True(KeyValueReplicator.ResolvedValueInMemory(null, intents, DestinationPartition, 302, "r/proof", commit));
        Assert.True(KeyValueReplicator.ResolvedValueInMemory(null, intents, DestinationPartition, 302, "r/proof", before));
        Assert.False(KeyValueReplicator.ResolvedValueInMemory(null, intents, DestinationPartition, 302, "r/proof", after));
        Assert.False(KeyValueReplicator.ResolvedValueInMemory(null, intents, DestinationPartition, 302, "r/other", commit));
        Assert.False(KeyValueReplicator.ResolvedValueInMemory(null, null, DestinationPartition, 302, "r/proof", commit));

        // An entry of another log is proven by the same head, with the same bounds: a range that moved leaves the
        // history of its keys in the slice of the other partition.
        Assert.True(KeyValueReplicator.ResolvedValueInMemory(null, intents, SourcePartition, 302, "r/proof", commit));
        Assert.False(KeyValueReplicator.ResolvedValueInMemory(null, intents, SourcePartition, 302, "r/proof", after));

        // The slice of the entry's own partition holds an older head and another slice a later one.
        PreparedIntent older = MakeIntent("r/two-slices", Ts(1_000), revision: 3, baseRevision: 2);
        PreparedIntent later = MakeIntent("r/two-slices", Ts(3_000), revision: 4, baseRevision: 3);
        Assert.True(intents.ApplyDeltaAckPrepares(SourcePartition, IntentLog(100, 1_000, new PrepareIntentCommand(older))));
        Assert.True(intents.Replicate(SourcePartition, IntentLog(101, 1_100, Settle(older))));
        Assert.True(intents.ApplyDeltaAckPrepares(DestinationPartition, IntentLog(303, 3_000, new PrepareIntentCommand(later))));
        Assert.True(intents.Replicate(DestinationPartition, IntentLog(304, 3_100, Settle(later))));

        Assert.True(KeyValueReplicator.ResolvedValueInMemory(null, intents, SourcePartition, 102, "r/two-slices", older.CommitTimestamp));
        Assert.True(KeyValueReplicator.ResolvedValueInMemory(null, intents, SourcePartition, 102, "r/two-slices", later.CommitTimestamp));
        Assert.False(KeyValueReplicator.ResolvedValueInMemory(
            null, intents, SourcePartition, 102, "r/two-slices", new HLCTimestamp(0, later.CommitTimestamp.L + 1, 0)));

        // A queued write of the key, with the same bounds.
        UnflushedKeyValueWritesIndex overlay = new();
        overlay.Record("r/queued", [1], revision: 4, HLCTimestamp.Zero, commit, lastModified: commit, KeyValueState.Set, noRevision: false);

        Assert.True(KeyValueReplicator.ResolvedValueInMemory(overlay, null, DestinationPartition, 302, "r/queued", commit));
        Assert.True(KeyValueReplicator.ResolvedValueInMemory(overlay, null, DestinationPartition, 302, "r/queued", before));
        Assert.False(KeyValueReplicator.ResolvedValueInMemory(overlay, null, DestinationPartition, 302, "r/queued", after));
        Assert.False(KeyValueReplicator.ResolvedValueInMemory(overlay, null, DestinationPartition, 302, "r/proof", commit));

        // An installed slice brings the head of the exporter. At an entry that the install reflects, that head
        // says nothing of the rows of this node; a queued write still does. Above that entry the head counts.
        using MemoryStream payload = new();
        intents.WritePartitionSection(payload, DestinationPartition, static _ => true);
        PreparedIntentStore receiver = new();
        receiver.ReplacePartitionIntents(
            DestinationPartition, PreparedIntentStore.DeserializePartitionIntents(payload.ToArray()), requireLedger: true, static _ => true);

        long reflected = receiver.GetLedgerReflectedThroughIndex(DestinationPartition);
        Assert.True(reflected > 0);
        Assert.False(KeyValueReplicator.ResolvedValueInMemory(null, receiver, DestinationPartition, reflected, "r/proof", commit));
        Assert.True(KeyValueReplicator.ResolvedValueInMemory(null, receiver, DestinationPartition, reflected + 1, "r/proof", commit));

        overlay.Record("r/proof", [1], revision: 11, HLCTimestamp.Zero, commit, lastModified: commit, KeyValueState.Set, noRevision: false);
        Assert.True(KeyValueReplicator.ResolvedValueInMemory(overlay, receiver, DestinationPartition, reflected, "r/proof", commit));
    }

    [Fact]
    public void ReplicaFenceVerdict_IsGivenForACoveredLeg()
    {
        (TransactionRecordStore records, PreparedIntentStore intents) = Stores();

        Assert.True(intents.Replicate(SourcePartition, IntentLog(100, 1_000, Seed("r/verdict", 10, 1_000))));

        PreparedIntent x = MakeIntent("r/verdict", Ts(2_000), revision: 11, baseRevision: 10);
        Assert.True(intents.ApplyDeltaAckPrepares(SourcePartition, IntentLog(200, 2_000, new PrepareIntentCommand(x))));
        Assert.True(intents.Replicate(DestinationPartition, IntentLog(300, 3_000, Seed("r/verdict", 10, 1_000))));

        (TransactionApplyOutcome prepare, _) = ApplyDestinationBundle(records, intents, "r/verdict", revision: 11, baseRevision: 10);
        Assert.Equal(TransactionApplyOutcome.Applied, prepare);
        Assert.Equal(1, intents.CoveredIntentCount);

        // The node applied the prepare of the covered leg, so it attests for it.
        KeyValueStagedBaseVerdictEntry verdict = Assert.Single(intents.EvaluateReplicaFenceVerdicts(x.TransactionId, x.Epoch, ["r/verdict"]));
        Assert.Equal(KeyValueStagedBaseVerdict.Clear, verdict.Verdict);
    }

    [Fact]
    public void ThreeLogsOfOneKey_TheMapShowsTheLatestLiveIntent()
    {
        const int thirdPartition = 9;

        foreach (int[] order in (int[][])[[0, 1, 2], [2, 0, 1], [1, 2, 0]])
        {
            PreparedIntentStore intents = new();

            // The key moved twice. Each log carries one live intent of it on a node that catches up all three.
            (int Partition, PreparedIntent Intent)[] legs =
            [
                (SourcePartition, MakeIntent("r/three", Ts(2_000), revision: 11, baseRevision: 10)),
                (DestinationPartition, MakeIntent("r/three", Ts(4_000), revision: 12, baseRevision: 11)),
                (thirdPartition, MakeIntent("r/three", Ts(6_000), revision: 13, baseRevision: 12))
            ];

            foreach (int index in order)
                Assert.True(intents.ApplyDeltaAckPrepares(legs[index].Partition, IntentLog(200, legs[index].Intent.TransactionId.L, new PrepareIntentCommand(legs[index].Intent))));

            Assert.Equal(legs[2].Intent.TransactionId, intents.Get("r/three")!.TransactionId);
            Assert.Equal(2, intents.CoveredIntentCount);

            // Each settle of the intent in the map brings back the latest of the covered ones.
            Assert.True(intents.Replicate(thirdPartition, IntentLog(201, 6_100, Settle(legs[2].Intent))));
            Assert.Equal(legs[1].Intent.TransactionId, intents.Get("r/three")!.TransactionId);
            Assert.Equal(1, intents.CoveredIntentCount);

            Assert.True(intents.Replicate(DestinationPartition, IntentLog(201, 4_100, Settle(legs[1].Intent))));
            Assert.Equal(legs[0].Intent.TransactionId, intents.Get("r/three")!.TransactionId);
            Assert.Equal(0, intents.CoveredIntentCount);

            Assert.True(intents.Replicate(SourcePartition, IntentLog(201, 2_100, Settle(legs[0].Intent))));
            Assert.Null(intents.Get("r/three"));
        }
    }

    [Fact]
    public void ExportCapture_DoesNotTakeACoveredLegForASettledOne()
    {
        PreparedIntentStore exporter = new();

        Assert.True(exporter.Replicate(DestinationPartition, IntentLog(300, 3_000, Seed("r/capture", 11, 2_001))));

        // A source leg of the key is the one live intent of it when the export captures the destination's ledger.
        PreparedIntent x = MakeIntent("r/capture", Ts(2_000), revision: 11, baseRevision: 10);
        Assert.True(exporter.ApplyDeltaAckPrepares(SourcePartition, IntentLog(200, 2_000, new PrepareIntentCommand(x))));

        // Between the capture and the walk, the destination commits one transaction of the key and prepares the
        // next. The source leg is covered when the walk ends. It did not settle, so the section keeps the head
        // the capture read: the entries of the two destination transactions replay on top of it.
        PreparedIntent first = MakeIntent("r/capture", Ts(4_000), revision: 12, baseRevision: 11);
        PreparedIntent second = MakeIntent("r/capture", Ts(5_000), revision: 13, baseRevision: 12);

        exporter.AfterLedgerCaptureForTesting = () =>
        {
            Assert.True(exporter.ApplyDeltaAckPrepares(DestinationPartition, IntentLog(301, 4_000, new PrepareIntentCommand(first))));
            Assert.True(exporter.Replicate(DestinationPartition, IntentLog(302, 4_100, Settle(first))));
            Assert.True(exporter.ApplyDeltaAckPrepares(DestinationPartition, IntentLog(303, 5_000, new PrepareIntentCommand(second))));
        };

        using MemoryStream payload = new();
        exporter.WritePartitionSection(payload, DestinationPartition, static _ => true);

        Assert.Equal(1, exporter.CoveredIntentCount);

        PreparedIntentStore.PartitionIntentSection section = PreparedIntentStore.DeserializePartitionIntents(payload.ToArray());
        Assert.Equal(11, Assert.Single(section.Ledger!).Revision);
    }

    // ── the views of the key that a move and a purge read ────────────────────────

    [Fact]
    public void RangeGatherOfAMove_AndThePurgeOfAKey_ReachBothIntentsOfTheKey()
    {
        PreparedIntentStore intents = new();

        PreparedIntent x = MakeIntent("r/views", Ts(2_000), revision: 11, baseRevision: 10);
        PreparedIntent y = MakeIntent("r/views", Ts(4_000), revision: 12, baseRevision: 11);
        PreparedIntent other = MakeIntent("s/other", Ts(4_100), revision: 2, baseRevision: 1);

        Assert.True(intents.ApplyDeltaAckPrepares(SourcePartition, IntentLog(200, 2_000, new PrepareIntentCommand(x))));
        Assert.True(intents.ApplyDeltaAckPrepares(DestinationPartition, IntentLog(300, 4_000, new PrepareIntentCommand(y))));
        Assert.True(intents.ApplyDeltaAckPrepares(DestinationPartition, IntentLog(301, 4_100, new PrepareIntentCommand(other))));

        // The settle barrier of a move reads the range: a covered intent is live for its log and must settle too.
        Assert.Equal(
            [x.TransactionId, y.TransactionId],
            intents.SnapshotRange("r/", "r0").Select(intent => intent.TransactionId).OrderBy(id => id.L).ToArray());

        // A scan of the key shows one intent, the latest.
        Assert.Equal(y.TransactionId, Assert.Single(intents.SnapshotScanWindow("r/", true, "r0", false)).TransactionId);
        Assert.Equal(y.TransactionId, Assert.Single(intents.SnapshotPrefix("r/")).TransactionId);

        // The purge of a key takes both of its intents.
        Assert.Equal(2, intents.PurgeWhere(key => key.StartsWith("r/", StringComparison.Ordinal)));
        Assert.Null(intents.Get("r/views"));
        Assert.Null(intents.GetByIdentity(x.TransactionId, x.Epoch, x.Key));
        Assert.Equal(0, intents.CoveredIntentCount);
        Assert.Equal(1, intents.LiveIntentCount);
    }

    [Fact]
    public void IntentLoadedFromTheSetsOfTwoPartitions_HoldsTheKeyAgainstBoth()
    {
        PreparedIntentStore intents = new();

        // Two snapshot files hold the same intent: one written before the routing changed and one after.
        PreparedIntent x = MakeIntent("r/two-files", Ts(2_000), revision: 11, baseRevision: 10);
        intents.ImportIntents([x], SourcePartition);
        intents.ImportIntents([x], DestinationPartition);

        Assert.Equal(1, intents.LiveIntentCount);
        Assert.Equal(0, intents.CoveredIntentCount);

        PreparedIntent y = MakeIntent("r/two-files", Ts(4_000), revision: 12, baseRevision: 11);
        Assert.False(intents.ApplyDeltaAckPrepares(DestinationPartition, IntentLog(300, 4_000, new PrepareIntentCommand(y))));
        Assert.False(intents.ApplyDeltaAckPrepares(SourcePartition, IntentLog(200, 4_000, new PrepareIntentCommand(y))));
    }

    // ── the loss witness: a replayed prepare is not judged against its own outcome ──

    [Fact]
    public void SourcePrepareReplayedOverTheSeedOfItsOwnCommit_IsNotRefused()
    {
        PreparedIntentStore store = new();

        // An insert that committed on the source before the cutover. The source slice never held the key, and
        // the destination's seed is the head that the insert's own commit made.
        PreparedIntent insert = MakeIntent("i/own", Ts(2_000), revision: 0, baseRevision: -1) with { BaseState = KeyValueState.Undefined };

        Assert.True(store.Replicate(DestinationPartition, IntentLog(300, 3_000,
            new SeedCommittedHeadCommand("i/own", 0, KeyValueState.Set, insert.CommitTimestamp))));

        int vetoes = 0;
        store.AttachStaleBaseVetoer((_, _) => vetoes++);

        Assert.True(store.ApplyDeltaAckPrepares(SourcePartition, IntentLog(200, 2_000, new PrepareIntentCommand(insert))));
        Assert.False(store.TryTakePrepareRejection(insert.TransactionId, 1, "i/own", out _));
        Assert.Equal(0, vetoes);
    }

    [Fact]
    public void PrepareOverTheCommitOfAnotherTransactionAtItsRevision_IsStillRefused()
    {
        PreparedIntentStore store = new();

        // The head at revision 0 is another transaction's insert: a true conflict for a second insert.
        PreparedIntent insert = MakeIntent("i/other", Ts(2_000), revision: 0, baseRevision: -1) with { BaseState = KeyValueState.Undefined };

        Assert.True(store.Replicate(DestinationPartition, IntentLog(300, 3_000,
            new SeedCommittedHeadCommand("i/other", 0, KeyValueState.Set, Ts(1_900)))));

        List<long> vetoedHeads = [];
        store.AttachStaleBaseVetoer((_, head) => vetoedHeads.Add(head));

        Assert.False(store.ApplyDeltaAckPrepares(SourcePartition, IntentLog(200, 2_000, new PrepareIntentCommand(insert))));
        Assert.True(store.TryTakePrepareRejection(insert.TransactionId, 1, "i/other", out PrepareRejectionKind kind));
        Assert.Equal(PrepareRejectionKind.StaleBase, kind);
        Assert.Equal(0, Assert.Single(vetoedHeads));
    }

    [Fact]
    public void PrepareOverAHeadOfItsOwnSlice_WithItsRevisionAndCommitTimestamp_IsStillRefused()
    {
        PreparedIntentStore store = new();

        // The slice of the prepare's own log holds the head. The log orders every commit of the key before a
        // prepare that follows it, so the prepare is judged whatever the head's commit timestamp is.
        PreparedIntent prepare = MakeIntent("r/own-slice", Ts(2_000), revision: 11, baseRevision: 10);

        Assert.True(store.Replicate(SourcePartition, IntentLog(100, 1_000,
            new SeedCommittedHeadCommand("r/own-slice", 11, KeyValueState.Set, prepare.CommitTimestamp))));

        Assert.False(store.ApplyDeltaAckPrepares(SourcePartition, IntentLog(200, 2_000, new PrepareIntentCommand(prepare))));
        Assert.True(store.TryTakePrepareRejection(prepare.TransactionId, 1, "r/own-slice", out PrepareRejectionKind kind));
        Assert.Equal(PrepareRejectionKind.StaleBase, kind);
    }

    [Theory]
    [InlineData(DestinationPartition, true)]
    [InlineData(SourcePartition, false)]
    public void SourcePrepareWithNoSourceHead_IsJudgedAgainstAnotherSlice_OnlyWhileTheNodeRoutesTheKeyToTheSource(int routedTo, bool acknowledged)
    {
        PreparedIntentStore store = new();
        store.AttachPartitionResolver(_ => routedTo);

        // The source slice holds nothing for the key. Another slice holds a head above the prepare's base.
        Assert.True(store.Replicate(DestinationPartition, IntentLog(300, 3_000, Seed("r/routed", 14, 2_900))));

        int vetoes = 0;
        store.AttachStaleBaseVetoer((_, _) => vetoes++);

        PreparedIntent prepare = MakeIntent("r/routed", Ts(1_500), revision: 11, baseRevision: 10);
        bool acked = store.ApplyDeltaAckPrepares(SourcePartition, IntentLog(200, 1_500, new PrepareIntentCommand(prepare)));

        // Routed to the destination, the prepare is history of a key that moved, and the other slice's head is
        // from after it. Routed to the source, the other slice is where the key came from, and its head is the
        // last commit this node knows: the protection of a range that moved before the hand-off carried heads.
        Assert.Equal(acknowledged, acked);
        Assert.Equal(acknowledged ? 0 : 1, vetoes);
    }

    // ── every order of the two logs gives the verdicts and the slices of the written order ──

    private sealed record LogStep(int Partition, string Id, Func<TransactionRecordStore, PreparedIntentStore, long, string> Run);

    private const int HistoryKeys = 5;

    /// <summary>Builds the two logs of a set of moved keys. Each key has a seed and several transactions in the
    /// source log, then the hand-off seed and several transactions in the destination log. A transaction is a
    /// one-phase bundle, a bundle with a stale base, a two-phase commit or an abort, and some bundles arrive
    /// before the settle of their predecessor in the same log. The steps of one key keep their order in each
    /// log; the keys are mixed.</summary>
    private static (List<LogStep> Source, List<LogStep> Destination) BuildHistory(int seed)
    {
        Random random = new(seed);

        List<List<LogStep>> sourceByKey = [];
        List<List<LogStep>> destinationByKey = [];

        for (int k = 0; k < HistoryKeys; k++)
        {
            string key = $"r/history-{k}";
            long head = 5;
            HLCTimestamp headCommittedAt = Ts(100);

            List<LogStep> source = [SeedStep(SourcePartition, key, head, headCommittedAt)];
            AddTransactions(random, source, SourcePartition, key, firstTransaction: 1_000 + k * 1_000, ref head, ref headCommittedAt);
            sourceByKey.Add(source);

            List<LogStep> destination = [SeedStep(DestinationPartition, key, head, headCommittedAt)];
            AddTransactions(random, destination, DestinationPartition, key, firstTransaction: 100_000 + k * 1_000, ref head, ref headCommittedAt);
            destinationByKey.Add(destination);
        }

        return (Merge(random, sourceByKey), Merge(random, destinationByKey));
    }

    private static LogStep SeedStep(int partition, string key, long revision, HLCTimestamp committedAt) => new(
        partition, $"{partition}:seed:{key}",
        (_, intents, logId) => intents.Replicate(
            partition, IntentLog(logId, 10, new SeedCommittedHeadCommand(key, revision, KeyValueState.Set, committedAt))).ToString());

    private static void AddTransactions(
        Random random, List<LogStep> log, int partition, string key, long firstTransaction, ref long head, ref HLCTimestamp headCommittedAt)
    {
        int count = random.Next(2, 5);

        // The settle of a committed bundle that the next transaction of the key overtakes in the log.
        LogStep? deferredSettle = null;

        for (int t = 0; t < count; t++)
        {
            HLCTimestamp id = Ts(firstTransaction + t * 10);
            int kind = random.Next(100);
            bool overtakes = deferredSettle is not null;

            // A stale base stages the revision the head already has.
            bool stale = !overtakes && kind is >= 60 and < 70;
            long baseRevision = stale ? head - 1 : head;
            PreparedIntent intent = MakeIntent(key, id, baseRevision + 1, baseRevision, ManifestHash);
            string name = $"{partition}:{key}:{id.L}";

            log.Add(new LogStep(partition, name + ":init", (records, _, _) => records.Apply(Init(intent), partition).Outcome.ToString()));
            log.Add(new LogStep(partition, name + ":prepare", (_, intents, logId) =>
                intents.ApplyDeltaAckPrepares(partition, IntentLog(logId, id.L, new PrepareIntentCommand(intent))).ToString()));

            bool twoPhaseAbort = !overtakes && kind is >= 80 and < 90;

            if (!twoPhaseAbort)
            {
                bool bundled = overtakes || kind < 70;

                log.Add(new LogStep(partition, name + ":commit", (records, _, _) =>
                {
                    CommitTransactionCommand commit = bundled
                        ? Bundle(id, key, ManifestHash)
                        : new CommitTransactionCommand(id, 1, ManifestHash, Ts(id.L + 2), Ts(id.L + 2));

                    TransactionRecordApplyResult applied = records.Apply(commit, partition);
                    return $"{applied.Outcome}:{applied.RejectReason}";
                }));
            }

            if (overtakes)
            {
                // The predecessor still held the key: this bundle was refused. The predecessor settles, and
                // the refused transaction is aborted.
                log.Add(deferredSettle!);
                deferredSettle = null;
                log.Add(SettleStep(partition, name + ":abort", id.L + 5, Abort(intent)));
                continue;
            }

            if (stale || twoPhaseAbort)
            {
                log.Add(SettleStep(partition, name + ":abort", id.L + 5, Abort(intent)));
                continue;
            }

            head = intent.Revision;
            headCommittedAt = intent.CommitTimestamp;

            LogStep settle = SettleStep(partition, name + ":settle", id.L + 5, Settle(intent));

            if (kind < 60 && t + 1 < count && random.Next(4) == 0)
                deferredSettle = settle;
            else
                log.Add(settle);
        }

        if (deferredSettle is not null)
            log.Add(deferredSettle);
    }

    private static LogStep SettleStep(int partition, string id, long time, PreparedIntentCommand[] commands) => new(
        partition, id, (_, intents, logId) => intents.Replicate(partition, IntentLog(logId, time, commands)).ToString());

    /// <summary>Merges lists into one, in a random order that keeps the order of each list.</summary>
    private static List<LogStep> Merge(Random random, List<List<LogStep>> lists)
    {
        int[] next = new int[lists.Count];
        int remaining = lists.Sum(list => list.Count);
        List<LogStep> merged = new(remaining);

        while (remaining > 0)
        {
            int pick = random.Next(remaining);

            for (int i = 0; i < lists.Count; i++)
            {
                int left = lists[i].Count - next[i];

                if (pick < left)
                {
                    merged.Add(lists[i][next[i]++]);
                    break;
                }

                pick -= left;
            }

            remaining--;
        }

        return merged;
    }

    /// <summary>What a node holds after it applied the two logs: the verdict of each step and the stores.</summary>
    private sealed record AppliedHistory(Dictionary<string, string> Verdicts, PreparedIntentStore Intents);

    private static AppliedHistory ApplyInOrder(IEnumerable<LogStep> order)
    {
        (TransactionRecordStore records, PreparedIntentStore intents) = Stores();
        Dictionary<string, string> verdicts = [];
        Dictionary<int, long> logIds = [];

        foreach (LogStep step in order)
        {
            long logId = logIds[step.Partition] = logIds.GetValueOrDefault(step.Partition) + 1;
            verdicts.Add(step.Id, step.Run(records, intents, logId));
        }

        return new AppliedHistory(verdicts, intents);
    }

    private static void AssertSameAsWritten(AppliedHistory written, AppliedHistory applied, string order)
    {
        foreach ((string step, string verdict) in written.Verdicts)
            Assert.True(verdict == applied.Verdicts[step], $"{order}: step {step} answered {applied.Verdicts[step]}, and {verdict} in the written order");

        foreach (int partition in (int[])[SourcePartition, DestinationPartition])
        {
            Assert.Equal(
                written.Intents.SnapshotLedger(partition).OrderBy(head => head.Key, StringComparer.Ordinal),
                applied.Intents.SnapshotLedger(partition).OrderBy(head => head.Key, StringComparer.Ordinal));

            Assert.Equal(
                written.Intents.SnapshotSettledIdentities(partition).OrderBy(entry => entry.Key, StringComparer.Ordinal).ThenBy(entry => entry.TransactionId.L),
                applied.Intents.SnapshotSettledIdentities(partition).OrderBy(entry => entry.Key, StringComparer.Ordinal).ThenBy(entry => entry.TransactionId.L));
        }

        Assert.Equal(written.Intents.LiveIntentCount, applied.Intents.LiveIntentCount);
        Assert.Equal(0, applied.Intents.LiveIntentCount);
        Assert.Equal(0, applied.Intents.CoveredIntentCount);
    }

    [Fact]
    public void AnyOrderOfTheTwoLogs_GivesTheVerdictsAndTheSlicesOfTheWrittenOrder()
    {
        int refused = 0;

        for (int seed = 0; seed < 150; seed++)
        {
            (List<LogStep> source, List<LogStep> destination) = BuildHistory(seed);

            // The written order: a peer applied the whole source history of the keys before the destination's.
            AppliedHistory written = ApplyInOrder(source.Concat(destination));
            refused += written.Verdicts.Count(verdict => verdict.Value.StartsWith("Rejected", StringComparison.Ordinal) || verdict.Value == "False");

            AssertSameAsWritten(written, ApplyInOrder(destination.Concat(source)), $"seed {seed}, destination first");

            Random random = new(seed * 7919 + 1);

            for (int round = 0; round < 6; round++)
                AssertSameAsWritten(written, ApplyInOrder(Merge(random, [source, destination])), $"seed {seed}, merge {round}");
        }

        // The histories hold refusals too, so the comparison is not one of admissions only.
        Assert.True(refused > 100, $"the histories held {refused} refusals");
    }

    [Fact]
    public async Task TwoApplyThreads_GiveTheVerdictsAndTheSlicesOfTheWrittenOrder()
    {
        for (int seed = 0; seed < 40; seed++)
        {
            (List<LogStep> source, List<LogStep> destination) = BuildHistory(1_000 + seed);
            AppliedHistory written = ApplyInOrder(source.Concat(destination));

            (TransactionRecordStore records, PreparedIntentStore intents) = Stores();
            System.Collections.Concurrent.ConcurrentDictionary<string, string> verdicts = new();

            using Barrier start = new(2);

            Task Apply(List<LogStep> log) => Task.Run(() =>
            {
                start.SignalAndWait();

                long logId = 0;
                foreach (LogStep step in log)
                    verdicts[step.Id] = step.Run(records, intents, ++logId);
            });

            await Task.WhenAll(Apply(source), Apply(destination));

            AssertSameAsWritten(written, new AppliedHistory(new Dictionary<string, string>(verdicts), intents), $"seed {seed}, two threads");
        }
    }
}
