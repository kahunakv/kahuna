using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Replication;
using Kahuna.Shared.KeyValue;
using Kommander.Data;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// Unit coverage of the parts that let a transaction prove its locks are still held: the capture a lock grant
/// reports its leadership term into, the coordinator-side record of one term per partition, and the apply gate
/// that refuses a one-phase bundled commit proposed in another term than the one its locks were granted under.
/// The end-to-end behaviour across a real leader change is in <see cref="TestLeaderChangeLostRangeLock"/>.
/// </summary>
public sealed class TestLockGrantTermFence
{
    private const int Partition = 7;

    private static HLCTimestamp Ts(long l) => new(0, l, 0);

    private static TransactionContext NewContext() =>
        new() { CoordinatorKey = "c", TransactionId = new HLCTimestamp(1, 100, 0) };

    // ── capture ────────────────────────────────────────────────────────────────

    [Fact]
    public void Capture_KeepsOneEntryPerPartitionAndTerm()
    {
        LockGrantCapture capture = new();
        Assert.Null(capture.Take());

        capture.Record(new LockGrantTerm(1, 4, "a/1"));
        capture.Record(new LockGrantTerm(1, 4, "a/2"));
        capture.Record(new LockGrantTerm(2, 4, "b/1"));
        capture.Record(new LockGrantTerm(1, 5, "a/3"));

        List<LockGrantTerm>? grants = capture.Take();
        Assert.NotNull(grants);
        Assert.Equal(
            [new LockGrantTerm(1, 4, "a/1"), new LockGrantTerm(2, 4, "b/1"), new LockGrantTerm(1, 5, "a/3")],
            grants);
    }

    [Fact]
    public async Task Scope_CollectsGrantsRecordedByAwaitedCalls_AndRestoresTheOuterCapture()
    {
        Assert.Null(LockGrantScope.Current);
        LockGrantScope.Record(1, 4, "ignored");

        LockGrantCapture outer;
        LockGrantCapture inner;

        using (LockGrantScope.Begin(out outer))
        {
            await RecordAfterYield(1, 4, "a/1");

            using (LockGrantScope.Begin(out inner))
                await RecordAfterYield(2, 9, "b/1");

            Assert.Same(outer, LockGrantScope.Current);
            await RecordAfterYield(3, 4, "c/1");
        }

        Assert.Null(LockGrantScope.Current);
        Assert.Equal([new LockGrantTerm(1, 4, "a/1"), new LockGrantTerm(3, 4, "c/1")], outer.Take());
        Assert.Equal([new LockGrantTerm(2, 9, "b/1")], inner.Take());

        static async Task RecordAfterYield(int partitionId, long term, string key)
        {
            await Task.Yield();
            LockGrantScope.Record(partitionId, term, key);
        }
    }

    // ── coordinator record ─────────────────────────────────────────────────────

    [Fact]
    public void Context_KeepsTheFirstTermOfEachPartition()
    {
        TransactionContext context = NewContext();
        Assert.Null(context.SnapshotLockGrantTerms());

        context.RecordLockGrantTerms([new LockGrantTerm(1, 4, "a/1"), new LockGrantTerm(2, 6, "b/1")]);
        context.RecordLockGrantTerms([new LockGrantTerm(1, 4, "a/2")]);

        Assert.Null(context.LockGrantTermChange);

        List<LockGrantTerm>? grants = context.SnapshotLockGrantTerms();
        Assert.NotNull(grants);
        Assert.Equal(
            [new LockGrantTerm(1, 4, "a/1"), new LockGrantTerm(2, 6, "b/1")],
            grants.OrderBy(static g => g.PartitionId));
    }

    [Fact]
    public void Context_ALaterGrantUnderAnotherTerm_IsALeaderChange()
    {
        TransactionContext context = NewContext();

        context.RecordLockGrantTerms([new LockGrantTerm(1, 4, "a/1")]);
        context.RecordLockGrantTerms([new LockGrantTerm(1, 5, "a/1")]);

        string? change = context.LockGrantTermChange;
        Assert.NotNull(change);
        Assert.Contains("partition 1", change);
        Assert.Contains("term 4", change);
        Assert.Contains("term 5", change);

        // The first term stays the one the commit-time proof is asked about, and the first change is the one
        // reported.
        context.RecordLockGrantTerms([new LockGrantTerm(1, 6, "a/1")]);
        Assert.Equal(change, context.LockGrantTermChange);
        Assert.Equal([new LockGrantTerm(1, 4, "a/1")], context.SnapshotLockGrantTerms());
    }

    [Fact]
    public void Context_AGrantWithoutATerm_CarriesNoEvidence()
    {
        TransactionContext context = NewContext();

        context.RecordLockGrantTerms([new LockGrantTerm(1, 0, "a/1"), new LockGrantTerm(2, -1, "b/1")]);
        Assert.Null(context.SnapshotLockGrantTerms());

        context.RecordLockGrantTerms([new LockGrantTerm(1, 4, "a/1")]);
        context.RecordLockGrantTerms([new LockGrantTerm(1, 0, "a/1")]);
        Assert.Null(context.LockGrantTermChange);
        Assert.Equal([new LockGrantTerm(1, 4, "a/1")], context.SnapshotLockGrantTerms());
    }

    [Fact]
    public void Context_AnOperationCompletion_FoldsItsGrantTerms()
    {
        TransactionContext context = NewContext();
        TransactionOperationId first = TransactionOperationId.NewRandom();
        TransactionOperationId second = TransactionOperationId.NewRandom();

        Assert.Equal(OperationRegistrationOutcome.New, context.BeginOperation(first, OperationKind.RangeLock, null).Outcome);
        context.CompleteOperation(first, new OperationCompletionPayload
        {
            AcquiredRangeLock = (new RangeLockKey("a", "a/1", true, "a/1", true, KeyValueDurability.Persistent), RangeLockMode.Shared),
            LockGrantTerms = [new LockGrantTerm(1, 4, "a/1")],
            Durability = KeyValueDurability.Persistent,
            CachedType = KeyValueResponseType.Locked
        }, KeyValueResponseType.Locked);

        Assert.Equal([new LockGrantTerm(1, 4, "a/1")], context.SnapshotLockGrantTerms());
        Assert.Null(context.LockGrantTermChange);

        // The upgrade of the same lock, answered by a leader of a later term.
        Assert.Equal(OperationRegistrationOutcome.New, context.BeginOperation(second, OperationKind.RangeLock, null).Outcome);
        context.CompleteOperation(second, new OperationCompletionPayload
        {
            AcquiredRangeLock = (new RangeLockKey("a", "a/1", true, "a/1", true, KeyValueDurability.Persistent), RangeLockMode.Exclusive),
            LockGrantTerms = [new LockGrantTerm(1, 5, "a/1")],
            Durability = KeyValueDurability.Persistent,
            CachedType = KeyValueResponseType.Locked
        }, KeyValueResponseType.Locked);

        Assert.NotNull(context.LockGrantTermChange);
    }

    // ── one-phase bundled commit gate ──────────────────────────────────────────

    private static PreparedIntent MakeIntent(string key, HLCTimestamp txId, long manifestHash) => new(
        TransactionId: txId, Epoch: 1, Key: key,
        ManifestHash: manifestHash, RecordAnchorKey: key,
        CommitTimestamp: new HLCTimestamp(txId.N, txId.L + 1, txId.C),
        State: KeyValueState.Set, Value: [1, 2, 3], Bucket: null,
        Revision: 1, Expires: HLCTimestamp.Zero, NoRevision: false,
        BaseRevision: PreparedIntent.UnknownBaseRevision, BaseState: KeyValueState.Undefined,
        RecoveryDeadline: HLCTimestamp.Zero, Resolution: PreparedIntentResolution.Pending);

    private static (TransactionRecordStore Records, PreparedIntentStore Intents) Stores()
    {
        TransactionRecordStore records = new();
        PreparedIntentStore intents = new();
        records.AttachBundledCommitJudge(intents.JudgeBundledCommit);
        return (records, intents);
    }

    /// <summary>Initializes the record and prepares the intent of a one-key bundle, and returns its commit.</summary>
    private static CommitTransactionCommand PrepareBundle(
        TransactionRecordStore records, PreparedIntentStore intents, HLCTimestamp txId, string key, HLCTimestamp opId, long lockGrantTerm)
    {
        List<TransactionParticipantRef> manifest = [new(key, KeyValueDurability.Persistent)];
        long hash = TransactionManifest.ComputeHash(txId, 1, key, Ts(txId.L + 1), manifest);

        Assert.Equal(TransactionApplyOutcome.Applied, records.Apply(
            new InitializeTransactionCommand(txId, 1, "coord", key, Ts(txId.L + 1), Ts(txId.L + 9_000), hash, manifest, opId, txId),
            Partition).Outcome);
        Assert.Equal(TransactionApplyOutcome.Applied, intents.Apply(new PrepareIntentCommand(MakeIntent(key, txId, hash)), Partition).Outcome);

        return new CommitTransactionCommand(txId, 1, hash, opId, AttemptHlc: opId, BundledPrepareKeys: [key],
            ApplyTimeValidation: true, LockGrantTerm: lockGrantTerm);
    }

    [Fact]
    public void BundleProposedInTheTermOfItsLockGrants_Commits()
    {
        (TransactionRecordStore records, PreparedIntentStore intents) = Stores();
        HLCTimestamp txId = Ts(1_000);

        CommitTransactionCommand commit = PrepareBundle(records, intents, txId, "t/same", Ts(1_100), lockGrantTerm: 4);

        Assert.Equal(TransactionApplyOutcome.Applied, records.Apply(commit, Partition, logTerm: 4).Outcome);
        Assert.Equal(TransactionDecision.Commit, records.Get(txId, 1)!.Decision);
    }

    [Fact]
    public void BundleProposedInAnotherTerm_IsRejected_AndTheRejectionIsMemoed()
    {
        (TransactionRecordStore records, PreparedIntentStore intents) = Stores();
        HLCTimestamp txId = Ts(2_000);
        HLCTimestamp opId = Ts(2_100);

        CommitTransactionCommand commit = PrepareBundle(records, intents, txId, "t/other", opId, lockGrantTerm: 4);

        TransactionRecordApplyResult result = records.Apply(commit, Partition, logTerm: 5);
        Assert.Equal(TransactionApplyOutcome.Rejected, result.Outcome);

        TransactionRecord record = records.Get(txId, 1)!;
        Assert.Equal(TransactionDecision.Undecided, record.Decision);
        Assert.True(record.WasBundledCommitRejected(opId), "the rejection must be memoed on the record");

        Assert.True(records.TryTakeGatedRejectionVerdict(txId, 1, opId, out BundledCommitVerdict verdict));
        Assert.Equal(BundledCommitVerdict.LeaderChanged, verdict);

        // A replay of the same entry stays rejected, whatever term the replay is attributed to.
        Assert.Equal(TransactionApplyOutcome.Rejected, records.Apply(commit, Partition, logTerm: 4).Outcome);
        Assert.Equal(TransactionDecision.Undecided, records.Get(txId, 1)!.Decision);
    }

    [Fact]
    public void BundleWithoutALockGrantTerm_IsNotJudgedByTerm()
    {
        (TransactionRecordStore records, PreparedIntentStore intents) = Stores();
        HLCTimestamp txId = Ts(3_000);

        CommitTransactionCommand commit = PrepareBundle(records, intents, txId, "t/none", Ts(3_100), lockGrantTerm: 0);

        Assert.Equal(TransactionApplyOutcome.Applied, records.Apply(commit, Partition, logTerm: 9).Outcome);
    }

    [Fact]
    public void ApplyWithoutALogEntry_HasNoTermToCompare()
    {
        (TransactionRecordStore records, PreparedIntentStore intents) = Stores();
        HLCTimestamp txId = Ts(4_000);

        CommitTransactionCommand commit = PrepareBundle(records, intents, txId, "t/direct", Ts(4_100), lockGrantTerm: 4);

        Assert.Equal(TransactionApplyOutcome.Applied, records.Apply(commit, Partition).Outcome);
    }

    [Fact]
    public void LockGrantTerm_SurvivesTheReplicatedLog_AndIsJudgedAgainstTheEntryTerm()
    {
        (TransactionRecordStore records, PreparedIntentStore intents) = Stores();
        HLCTimestamp txId = Ts(5_000);
        HLCTimestamp opId = Ts(5_100);

        CommitTransactionCommand commit = PrepareBundle(records, intents, txId, "t/wire", opId, lockGrantTerm: 4);

        // A fresh array, so the store decodes the bytes instead of reusing the proposer's command instances.
        byte[] data = [.. TransactionRecordStore.SerializeDelta([commit])];

        Assert.True(records.Replicate(Partition, new RaftLog { Term = 6, LogType = ReplicationTypes.TransactionRecord, LogData = data }));

        Assert.Equal(TransactionDecision.Undecided, records.Get(txId, 1)!.Decision);
        Assert.True(records.TryTakeGatedRejectionVerdict(txId, 1, opId, out BundledCommitVerdict verdict));
        Assert.Equal(BundledCommitVerdict.LeaderChanged, verdict);
    }
}
