using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// Exhaustive unit tests for the pure canonical transaction-record CAS state machine
/// (<see cref="TransactionRecordStateMachine"/>) — the linearization point of a transaction's outcome under the
/// durable-intent 2PC model. Covers every transition, idempotent replay, invariant rejections, the decision
/// deadline, absence→abort tombstones, commit/abort races decided by log order, and manifest-hash determinism.
/// </summary>
public sealed class TestTransactionRecordStateMachine
{
    private static HLCTimestamp Ts(long l) => new(0, l, 0);

    private static readonly HLCTimestamp TxId = Ts(1000);
    private const long Epoch = 1;
    private const string Anchor = "acct/42";

    private static IReadOnlyList<TransactionParticipantRef> Manifest() =>
    [
        new("acct/42", KeyValueDurability.Persistent),
        new("idx/name/bob", KeyValueDurability.Persistent)
    ];

    private static long Hash(IReadOnlyList<TransactionParticipantRef> participants, HLCTimestamp commitTs) =>
        TransactionManifest.ComputeHash(TxId, Epoch, Anchor, commitTs, participants);

    private static InitializeTransactionCommand Init(HLCTimestamp commitTs, HLCTimestamp deadline, long? hash = null) =>
        new(TxId, Epoch, "coord/1", Anchor, commitTs, deadline, hash ?? Hash(Manifest(), commitTs),
            Manifest(), OpId: Ts(500), CreatedAt: Ts(400));

    private static CommitTransactionCommand Commit(long hash, HLCTimestamp opId, HLCTimestamp attempt) =>
        new(TxId, Epoch, hash, opId, attempt);

    private static AbortTransactionCommand Abort(long hash, TransactionAbortClass cls, HLCTimestamp opId, HLCTimestamp attempt, HLCTimestamp commitTs, HLCTimestamp deadline) =>
        new(TxId, Epoch, hash, cls, opId, attempt, Anchor, commitTs, deadline, CreatedAt: Ts(400));

    // ── initialize ────────────────────────────────────────────────────────────────

    [Fact]
    public void Initialize_FromAbsent_CreatesUndecided()
    {
        TransactionRecordApplyResult r = TransactionRecordStateMachine.Apply(null, Init(Ts(2000), Ts(9000)));

        Assert.Equal(TransactionApplyOutcome.Applied, r.Outcome);
        Assert.NotNull(r.Record);
        Assert.Equal(TransactionDecision.Undecided, r.Record!.Decision);
        Assert.True(r.Record.ManifestPresent);
        Assert.Equal(HLCTimestamp.Zero, r.Record.WinningOpId);
        Assert.Equal(2, r.Record.Participants.Count);
    }

    [Fact]
    public void Initialize_ExactReplay_IsNoop()
    {
        TransactionRecord rec = TransactionRecordStateMachine.Apply(null, Init(Ts(2000), Ts(9000))).Record!;
        TransactionRecordApplyResult r = TransactionRecordStateMachine.Apply(rec, Init(Ts(2000), Ts(9000)));

        Assert.Equal(TransactionApplyOutcome.IdempotentNoop, r.Outcome);
        Assert.Same(rec, r.Record);
    }

    [Fact]
    public void Initialize_MismatchedManifest_SameTxnEpoch_Rejected()
    {
        TransactionRecord rec = TransactionRecordStateMachine.Apply(null, Init(Ts(2000), Ts(9000))).Record!;

        // Same (TxnId, Epoch) but a different frozen manifest hash: a frozen-identity violation.
        TransactionRecordApplyResult r = TransactionRecordStateMachine.Apply(rec, Init(Ts(2000), Ts(9000), hash: rec.ManifestHash + 1));

        Assert.Equal(TransactionApplyOutcome.Rejected, r.Outcome);
        Assert.Same(rec, r.Record);
        Assert.NotNull(r.RejectReason);
    }

    // ── commit ────────────────────────────────────────────────────────────────────

    [Fact]
    public void Commit_FromUndecided_InsideDeadline_Commits()
    {
        TransactionRecord rec = TransactionRecordStateMachine.Apply(null, Init(Ts(2000), Ts(9000))).Record!;
        TransactionRecordApplyResult r = TransactionRecordStateMachine.Apply(rec, Commit(rec.ManifestHash, opId: Ts(3000), attempt: Ts(3000)));

        Assert.Equal(TransactionApplyOutcome.Applied, r.Outcome);
        Assert.Equal(TransactionDecision.Commit, r.Record!.Decision);
        Assert.Equal(Ts(3000), r.Record.WinningOpId);
        Assert.Equal(Ts(3000), r.Record.DecidedAt);
    }

    [Fact]
    public void Commit_PastDecisionDeadline_Rejected()
    {
        TransactionRecord rec = TransactionRecordStateMachine.Apply(null, Init(Ts(2000), Ts(5000))).Record!;
        TransactionRecordApplyResult r = TransactionRecordStateMachine.Apply(rec, Commit(rec.ManifestHash, opId: Ts(6000), attempt: Ts(6000)));

        Assert.Equal(TransactionApplyOutcome.Rejected, r.Outcome);
        Assert.Equal(TransactionDecision.Undecided, r.Record!.Decision);
    }

    [Fact]
    public void Commit_FromAbsence_Rejected()
    {
        TransactionRecordApplyResult r = TransactionRecordStateMachine.Apply(null, Commit(Hash(Manifest(), Ts(2000)), Ts(3000), Ts(3000)));

        Assert.Equal(TransactionApplyOutcome.Rejected, r.Outcome);
        Assert.Null(r.Record);
    }

    [Fact]
    public void Commit_ManifestHashMismatch_Rejected()
    {
        TransactionRecord rec = TransactionRecordStateMachine.Apply(null, Init(Ts(2000), Ts(9000))).Record!;
        TransactionRecordApplyResult r = TransactionRecordStateMachine.Apply(rec, Commit(rec.ManifestHash + 7, Ts(3000), Ts(3000)));

        Assert.Equal(TransactionApplyOutcome.Rejected, r.Outcome);
        Assert.Equal(TransactionDecision.Undecided, r.Record!.Decision);
    }

    [Fact]
    public void Commit_Idempotent_KeepsWinner()
    {
        TransactionRecord rec = TransactionRecordStateMachine.Apply(null, Init(Ts(2000), Ts(9000))).Record!;
        TransactionRecord committed = TransactionRecordStateMachine.Apply(rec, Commit(rec.ManifestHash, Ts(3000), Ts(3000))).Record!;

        // A second commit with a different op id must not change the winner.
        TransactionRecordApplyResult r = TransactionRecordStateMachine.Apply(committed, Commit(rec.ManifestHash, opId: Ts(4000), attempt: Ts(4000)));

        Assert.Equal(TransactionApplyOutcome.IdempotentNoop, r.Outcome);
        Assert.Equal(Ts(3000), r.Record!.WinningOpId);
    }

    // ── abort ─────────────────────────────────────────────────────────────────────

    [Fact]
    public void Abort_FromUndecided_Aborts_WithClassAndWinner()
    {
        TransactionRecord rec = TransactionRecordStateMachine.Apply(null, Init(Ts(2000), Ts(9000))).Record!;
        TransactionRecordApplyResult r = TransactionRecordStateMachine.Apply(rec,
            Abort(rec.ManifestHash, TransactionAbortClass.Conflict, opId: Ts(3000), attempt: Ts(3000), Ts(2000), Ts(9000)));

        Assert.Equal(TransactionApplyOutcome.Applied, r.Outcome);
        Assert.Equal(TransactionDecision.Abort, r.Record!.Decision);
        Assert.Equal(TransactionAbortClass.Conflict, r.Record.AbortClass);
        Assert.Equal(Ts(3000), r.Record.WinningOpId);
    }

    [Fact]
    public void Abort_FromAbsence_CreatesManifestlessTombstone()
    {
        TransactionRecordApplyResult r = TransactionRecordStateMachine.Apply(null,
            Abort(Hash(Manifest(), Ts(2000)), TransactionAbortClass.PresumedAbort, Ts(3000), Ts(3000), Ts(2000), Ts(9000)));

        Assert.Equal(TransactionApplyOutcome.Applied, r.Outcome);
        Assert.Equal(TransactionDecision.Abort, r.Record!.Decision);
        Assert.False(r.Record.ManifestPresent);
        Assert.Empty(r.Record.Participants);
        Assert.Equal(TransactionAbortClass.PresumedAbort, r.Record.AbortClass);
    }

    [Fact]
    public void Abort_Idempotent_KeepsOriginalClassAndWinner()
    {
        TransactionRecord rec = TransactionRecordStateMachine.Apply(null, Init(Ts(2000), Ts(9000))).Record!;
        TransactionRecord aborted = TransactionRecordStateMachine.Apply(rec,
            Abort(rec.ManifestHash, TransactionAbortClass.Conflict, Ts(3000), Ts(3000), Ts(2000), Ts(9000))).Record!;

        // A second abort with a different class and op id cannot rewrite the winner.
        TransactionRecordApplyResult r = TransactionRecordStateMachine.Apply(aborted,
            Abort(rec.ManifestHash, TransactionAbortClass.RetryableFailure, opId: Ts(4000), attempt: Ts(4000), Ts(2000), Ts(9000)));

        Assert.Equal(TransactionApplyOutcome.IdempotentNoop, r.Outcome);
        Assert.Equal(TransactionAbortClass.Conflict, r.Record!.AbortClass);
        Assert.Equal(Ts(3000), r.Record.WinningOpId);
    }

    // ── terminal cross-rejections ──────────────────────────────────────────────────

    [Fact]
    public void Commit_AfterAbort_Rejected()
    {
        TransactionRecord rec = TransactionRecordStateMachine.Apply(null, Init(Ts(2000), Ts(9000))).Record!;
        TransactionRecord aborted = TransactionRecordStateMachine.Apply(rec,
            Abort(rec.ManifestHash, TransactionAbortClass.Conflict, Ts(3000), Ts(3000), Ts(2000), Ts(9000))).Record!;

        TransactionRecordApplyResult r = TransactionRecordStateMachine.Apply(aborted, Commit(rec.ManifestHash, Ts(4000), Ts(4000)));

        Assert.Equal(TransactionApplyOutcome.Rejected, r.Outcome);
        Assert.Equal(TransactionDecision.Abort, r.Record!.Decision);
    }

    [Fact]
    public void Abort_AfterCommit_Rejected()
    {
        TransactionRecord rec = TransactionRecordStateMachine.Apply(null, Init(Ts(2000), Ts(9000))).Record!;
        TransactionRecord committed = TransactionRecordStateMachine.Apply(rec, Commit(rec.ManifestHash, Ts(3000), Ts(3000))).Record!;

        TransactionRecordApplyResult r = TransactionRecordStateMachine.Apply(committed,
            Abort(rec.ManifestHash, TransactionAbortClass.Conflict, Ts(4000), Ts(4000), Ts(2000), Ts(9000)));

        Assert.Equal(TransactionApplyOutcome.Rejected, r.Outcome);
        Assert.Equal(TransactionDecision.Commit, r.Record!.Decision);
    }

    // ── commit/abort race: log order chooses exactly one winner, deterministically ──

    [Fact]
    public void CommitThenAbort_LogOrderCommitFirst_CommitWins()
    {
        TransactionRecord rec = TransactionRecordStateMachine.Apply(null, Init(Ts(2000), Ts(9000))).Record!;
        TransactionRecord afterCommit = TransactionRecordStateMachine.Apply(rec, Commit(rec.ManifestHash, opId: Ts(3000), attempt: Ts(3000))).Record!;
        TransactionRecordApplyResult r = TransactionRecordStateMachine.Apply(afterCommit,
            Abort(rec.ManifestHash, TransactionAbortClass.PresumedAbort, opId: Ts(3500), attempt: Ts(3500), Ts(2000), Ts(9000)));

        Assert.Equal(TransactionApplyOutcome.Rejected, r.Outcome);
        Assert.Equal(TransactionDecision.Commit, r.Record!.Decision);
        Assert.Equal(Ts(3000), r.Record.WinningOpId);
    }

    [Fact]
    public void AbortThenCommit_LogOrderAbortFirst_AbortWins()
    {
        TransactionRecord rec = TransactionRecordStateMachine.Apply(null, Init(Ts(2000), Ts(9000))).Record!;
        TransactionRecord afterAbort = TransactionRecordStateMachine.Apply(rec,
            Abort(rec.ManifestHash, TransactionAbortClass.PresumedAbort, opId: Ts(3500), attempt: Ts(3500), Ts(2000), Ts(9000))).Record!;
        TransactionRecordApplyResult r = TransactionRecordStateMachine.Apply(afterAbort, Commit(rec.ManifestHash, opId: Ts(3000), attempt: Ts(3000)));

        Assert.Equal(TransactionApplyOutcome.Rejected, r.Outcome);
        Assert.Equal(TransactionDecision.Abort, r.Record!.Decision);
        Assert.Equal(Ts(3500), r.Record.WinningOpId);
    }

    [Fact]
    public void Winner_LoserOpId_DoesNotMatchTerminalWinner()
    {
        TransactionRecord rec = TransactionRecordStateMachine.Apply(null, Init(Ts(2000), Ts(9000))).Record!;
        TransactionRecord committed = TransactionRecordStateMachine.Apply(rec, Commit(rec.ManifestHash, opId: Ts(3000), attempt: Ts(3000))).Record!;

        HLCTimestamp myLosingOpId = Ts(3500);
        TransactionRecordApplyResult r = TransactionRecordStateMachine.Apply(committed,
            Abort(rec.ManifestHash, TransactionAbortClass.PresumedAbort, opId: myLosingOpId, attempt: Ts(3500), Ts(2000), Ts(9000)));

        // The losing caller learns it lost: the terminal record's winner is not its op id.
        Assert.NotEqual(myLosingOpId, r.Record!.WinningOpId);
    }

    // ── identity ────────────────────────────────────────────────────────────────

    [Fact]
    public void Transition_DifferentEpoch_Rejected()
    {
        TransactionRecord rec = TransactionRecordStateMachine.Apply(null, Init(Ts(2000), Ts(9000))).Record!;
        CommitTransactionCommand wrongEpoch = new(TxId, Epoch + 1, rec.ManifestHash, Ts(3000), Ts(3000));

        TransactionRecordApplyResult r = TransactionRecordStateMachine.Apply(rec, wrongEpoch);

        Assert.Equal(TransactionApplyOutcome.Rejected, r.Outcome);
    }

    // ── manifest hash determinism ─────────────────────────────────────────────────

    [Fact]
    public void ManifestHash_IsOrderIndependent_AndDistinguishesSets()
    {
        TransactionParticipantRef a = new("acct/42", KeyValueDurability.Persistent);
        TransactionParticipantRef b = new("idx/name/bob", KeyValueDurability.Persistent);

        long h1 = TransactionManifest.ComputeHash(TxId, Epoch, Anchor, Ts(2000), [a, b]);
        long h2 = TransactionManifest.ComputeHash(TxId, Epoch, Anchor, Ts(2000), [b, a]);
        Assert.Equal(h1, h2);

        long different = TransactionManifest.ComputeHash(TxId, Epoch, Anchor, Ts(2000), [a]);
        Assert.NotEqual(h1, different);

        long differentAnchor = TransactionManifest.ComputeHash(TxId, Epoch, "acct/99", Ts(2000), [a, b]);
        Assert.NotEqual(h1, differentAnchor);
    }
}
