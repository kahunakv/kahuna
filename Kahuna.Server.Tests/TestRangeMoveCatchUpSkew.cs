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
/// A range move splits one transaction leg across two logs: the prepare is carried by the source partition's
/// log, and the settle goes to the partition that owns the key at settlement time, the destination. A node that
/// applies both logs in real time sees the prepare before the settle. A node that applies them with a skew sees
/// the settle first: a restarted node that catches up the source and a destination created while it was down,
/// or a follower that lags on one partition. These tests pin what such a node must do at the store level.
///
/// <para>CamusDB bank-split, 2026-11-08: the row space cut over from P1 to P7 inside a node's kill window. The
/// restarted node applied the P7 settles of two P1 transactions before their P1 prepares, re-installed the
/// prepares as phantom holders, rejected 39 one-phase bundles of later transactions on those keys as "bundled
/// prepare not held", fell one revision behind its peers, and served the stale rows once it led P7. The same
/// node judged the P1 prepares against heads the P7 hand-off had seeded from a later time, refused 11,401 of
/// them and fired 6,090 vetoes that found the commits already recorded.</para>
/// </summary>
public sealed class TestRangeMoveCatchUpSkew
{
    private const int SourcePartition = 1;

    private const int DestinationPartition = 7;

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

    // ── the settled identity across the two logs ─────────────────────────────────

    [Fact]
    public void SettleAheadOfItsPrepare_ThePrepareInstallsNothing()
    {
        PreparedIntentStore store = new();

        // The key had ten commits on the source before the move.
        Assert.True(store.Replicate(SourcePartition, IntentLog(100, 1_000, Seed("r/moved", 10, 1_000))));

        PreparedIntent x = MakeIntent("r/moved", Ts(2_000), revision: 11, baseRevision: 10);

        // The destination's log carries X's settle; this node applies it before the source's prepare of X.
        Assert.True(store.Replicate(DestinationPartition, IntentLog(300, 3_000, Settle(x))));
        Assert.Null(store.Get("r/moved"));

        // The source's prepare of X arrives afterwards: a settled leg, so nothing installs and no veto fires.
        int vetoes = 0;
        store.AttachStaleBaseVetoer((_, _) => vetoes++);

        bool acked = store.ApplyDeltaAckPrepares(SourcePartition, IntentLog(200, 2_000, new PrepareIntentCommand(x)));

        Assert.False(acked);
        Assert.True(store.TryTakePrepareRejection(x.TransactionId, 1, "r/moved", out PrepareRejectionKind kind));
        Assert.Equal(PrepareRejectionKind.Settled, kind);
        Assert.Null(store.Get("r/moved"));
        Assert.Equal(0, store.LiveIntentCount);
        Assert.Equal(0, vetoes);
    }

    [Fact]
    public void SettleAheadOfItsPrepare_RemembersTheIdentityWithTheCommitTimestamp()
    {
        PreparedIntentStore store = new();
        PreparedIntent x = MakeIntent("r/stamp", Ts(2_000), revision: 11, baseRevision: 10);

        Assert.True(store.Replicate(DestinationPartition, IntentLog(300, 3_000, Settle(x))));

        PreparedIntentStore.SettledIdentityEntry settled = Assert.Single(store.SnapshotSettledIdentities(DestinationPartition));
        Assert.Equal(x.TransactionId, settled.TransactionId);
        Assert.Equal(1, settled.Epoch);
        Assert.Equal("r/stamp", settled.Key);

        // The same time base a node that held the intent records, so the two memories compare equal.
        Assert.Equal(x.CommitTimestamp, settled.SettledAt);
        Assert.Empty(store.SnapshotSettledIdentities(SourcePartition));
    }

    [Fact]
    public void RemovalWithoutACommitTimestamp_RemembersTheIdentityAtTheEntryTime()
    {
        PreparedIntentStore store = new();
        PreparedIntent x = MakeIntent("r/abort", Ts(2_000), revision: 11, baseRevision: 10);

        // An abort's settle carries no commit timestamp; the removal still remembers the leg, at the entry's time.
        Assert.True(store.Replicate(DestinationPartition, IntentLog(300, 3_000,
            new ResolveIntentCommand(x.TransactionId, x.Epoch, x.Key, Commit: false),
            new RemoveIntentCommand(x.TransactionId, x.Epoch, x.Key))));

        PreparedIntentStore.SettledIdentityEntry settled = Assert.Single(store.SnapshotSettledIdentities(DestinationPartition));
        Assert.Equal(Ts(3_000), settled.SettledAt);

        Assert.False(store.ApplyDeltaAckPrepares(SourcePartition, IntentLog(200, 2_000, new PrepareIntentCommand(x))));
        Assert.Null(store.Get("r/abort"));
    }

    [Fact]
    public void ReDrivenPrepareAfterACrossLogSettle_InstallsNothing()
    {
        PreparedIntentStore store = new();
        PreparedIntent x = MakeIntent("r/redriven", Ts(2_000), revision: 11, baseRevision: 10);

        // The ordinary order: the prepare on the source, then the settle through the destination's log.
        Assert.Equal(TransactionApplyOutcome.Applied, store.Apply(new PrepareIntentCommand(x), SourcePartition).Outcome);
        Assert.True(store.Replicate(DestinationPartition, IntentLog(300, 3_000, Settle(x))));
        Assert.Null(store.Get("r/redriven"));

        // A re-driven copy of the prepare on the source (a proposal released at a step-down and re-proposed) must
        // not re-install the settled leg: its settlement is remembered on the destination's slice.
        Assert.False(store.ApplyDeltaAckPrepares(SourcePartition, IntentLog(201, 2_100, new PrepareIntentCommand(x))));
        Assert.True(store.TryTakePrepareRejection(x.TransactionId, 1, "r/redriven", out PrepareRejectionKind kind));
        Assert.Equal(PrepareRejectionKind.Settled, kind);
        Assert.Null(store.Get("r/redriven"));
    }

    [Fact]
    public void InOrderOnOneLog_SettleThenPrepareStillRejectsAndPrepareThenSettleStillWorks()
    {
        PreparedIntentStore store = new();

        PreparedIntent first = MakeIntent("r/order", Ts(2_000), revision: 11, baseRevision: 10);
        Assert.Equal(TransactionApplyOutcome.Applied, store.Apply(new PrepareIntentCommand(first), SourcePartition).Outcome);
        Assert.True(store.Replicate(SourcePartition, IntentLog(201, 2_100, Settle(first))));
        Assert.Null(store.Get("r/order"));

        // The next transaction of the key prepares as before: the settled identity is the first's, not the key's.
        PreparedIntent second = MakeIntent("r/order", Ts(2_200), revision: 12, baseRevision: 11);
        Assert.True(store.ApplyDeltaAckPrepares(SourcePartition, IntentLog(202, 2_200, new PrepareIntentCommand(second))));
        Assert.NotNull(store.Get("r/order"));
    }

    // ── what the phantom cost: the bundled commits of later transactions ─────────

    [Fact]
    public void AfterTheSkew_ALaterOnePhaseBundleOnTheDestination_IsAdmitted()
    {
        (TransactionRecordStore records, PreparedIntentStore intents) = Stores();

        Assert.True(intents.Replicate(SourcePartition, IntentLog(100, 1_000, Seed("r/bundle", 10, 1_000))));

        PreparedIntent x = MakeIntent("r/bundle", Ts(2_000), revision: 11, baseRevision: 10);

        // The destination's hand-off seeds the key's head as of the cutover (X committed before it), then X's
        // settle; the source's prepare of X applies last.
        Assert.True(intents.Replicate(DestinationPartition, IntentLog(300, 3_000, Seed("r/bundle", 11, 2_001))));
        Assert.True(intents.Replicate(DestinationPartition, IntentLog(301, 3_100, Settle(x))));
        Assert.False(intents.ApplyDeltaAckPrepares(SourcePartition, IntentLog(200, 2_000, new PrepareIntentCommand(x))));
        Assert.Null(intents.Get("r/bundle"));

        // The next transaction of the key is a one-phase bundle on the destination: its prepare installs, and the
        // bundled commit gate admits it. With the phantom in place the prepare was rejected as held by another
        // transaction and the gate answered PrepareMissing on this node alone.
        HLCTimestamp yId = Ts(4_000);
        long hash = 77;
        PreparedIntent y = MakeIntent("r/bundle", yId, revision: 12, baseRevision: 11, manifestHash: hash);
        List<TransactionParticipantRef> manifest = [new("r/bundle", KeyValueDurability.Persistent)];
        InitializeTransactionCommand init = new(yId, 1, "coord", "r/bundle", Ts(yId.L + 1), Ts(yId.L + 9_000), hash, manifest, Ts(yId.L + 2), yId);

        Assert.Equal(TransactionApplyOutcome.Applied, records.Apply(init, DestinationPartition).Outcome);
        Assert.Equal(TransactionApplyOutcome.Applied, intents.Apply(new PrepareIntentCommand(y), DestinationPartition).Outcome);
        Assert.Equal(BundledCommitJudgement.Admitted, intents.JudgeBundledCommit(DestinationPartition, Bundle(yId, "r/bundle", hash)));
    }

    // ── the fence judges a source prepare against the source slice ───────────────

    [Fact]
    public void SourcePrepareBehindADestinationSeed_IsNotRefusedByTheSeed()
    {
        PreparedIntentStore store = new();

        // The source slice remembers the key at 10; the destination's hand-off seeded 14, the head as of a later
        // cutover. The source's prepare validated against 10 predates every commit the seed reflects.
        Assert.True(store.Replicate(SourcePartition, IntentLog(100, 1_000, Seed("r/fence", 10, 1_000))));
        Assert.True(store.Replicate(DestinationPartition, IntentLog(300, 3_000, Seed("r/fence", 14, 2_900))));

        int vetoes = 0;
        store.AttachStaleBaseVetoer((_, _) => vetoes++);

        PreparedIntent stale = MakeIntent("r/fence", Ts(1_500), revision: 11, baseRevision: 10);
        bool acked = store.ApplyDeltaAckPrepares(SourcePartition, IntentLog(200, 1_500, new PrepareIntentCommand(stale)));

        Assert.True(acked);
        Assert.False(store.TryTakePrepareRejection(stale.TransactionId, 1, "r/fence", out _));
        Assert.Equal(0, vetoes);
    }

    [Fact]
    public void SourcePrepareBehindTheSourceSlice_IsStillRefused_AndTheVetoNamesTheSourceHead()
    {
        PreparedIntentStore store = new();

        Assert.True(store.Replicate(SourcePartition, IntentLog(100, 1_000, Seed("r/stale", 12, 1_000))));
        Assert.True(store.Replicate(DestinationPartition, IntentLog(300, 3_000, Seed("r/stale", 14, 2_900))));

        List<long> vetoedHeads = [];
        store.AttachStaleBaseVetoer((_, head) => vetoedHeads.Add(head));

        PreparedIntent stale = MakeIntent("r/stale", Ts(1_500), revision: 12, baseRevision: 11);
        bool acked = store.ApplyDeltaAckPrepares(SourcePartition, IntentLog(200, 1_500, new PrepareIntentCommand(stale)));

        Assert.False(acked);
        Assert.True(store.TryTakePrepareRejection(stale.TransactionId, 1, "r/stale", out PrepareRejectionKind kind));
        Assert.Equal(PrepareRejectionKind.StaleBase, kind);
        Assert.Equal(12, Assert.Single(vetoedHeads));
    }

    [Fact]
    public void PrepareOnASliceWithNoHeadForTheKey_IsStillJudgedAgainstTheOtherSlices()
    {
        PreparedIntentStore store = new();

        // A range moved on a build before the hand-off carried heads: the destination's slice holds nothing for
        // the key and the source's slice holds its last commit. The widest memory still protects the key.
        Assert.True(store.Replicate(SourcePartition, IntentLog(100, 1_000, Seed("r/legacy", 12, 1_000))));

        PreparedIntent stale = MakeIntent("r/legacy", Ts(1_500), revision: 12, baseRevision: 11);
        Assert.False(store.ApplyDeltaAckPrepares(DestinationPartition, IntentLog(300, 1_500, new PrepareIntentCommand(stale))));
        Assert.True(store.TryTakePrepareRejection(stale.TransactionId, 1, "r/legacy", out PrepareRejectionKind kind));
        Assert.Equal(PrepareRejectionKind.StaleBase, kind);

        // The refused prepare still installed (the verdict changes no transition); the coordinator's abort rolls
        // it back, and the next transaction, validated against the real head, is acknowledged.
        Assert.True(store.Replicate(DestinationPartition, IntentLog(301, 1_550,
            new ResolveIntentCommand(stale.TransactionId, stale.Epoch, stale.Key, Commit: false),
            new RemoveIntentCommand(stale.TransactionId, stale.Epoch, stale.Key))));

        PreparedIntent current = MakeIntent("r/legacy", Ts(1_600), revision: 13, baseRevision: 12);
        Assert.True(store.ApplyDeltaAckPrepares(DestinationPartition, IntentLog(302, 1_600, new PrepareIntentCommand(current))));
    }
}
