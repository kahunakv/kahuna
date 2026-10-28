using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// The replica half of the pre-decision fence confirmation, answered from the committed-head memory alone. A
/// replica used to attest only once the transaction's prepare had applied there, so a replica that lagged the
/// prepare answered <see cref="KeyValueStagedBaseVerdict.NotApplied"/> once the wait ran out and the commit
/// proceeded unattested — the window where the detached veto raced the commit and lost. The ask now carries the
/// judged fields of every fenced intent, and a replica without the intent judges from its heads: heads only
/// advance and the key's head is frozen while the intent is live, so the memory verdict is at least as strict as
/// the apply-time one, and the one head that is the transaction's own (its settlement already applied here)
/// answers Clear through the settled-identity memory.
/// </summary>
public sealed class TestReplicaFenceMemoryVerdicts
{
    private static HLCTimestamp Ts(long l) => new(0, l, 0);

    private static PreparedIntent MakeIntent(
        string key, HLCTimestamp txId, long epoch, long revision, long baseRevision, KeyValueState baseState) => new(
        TransactionId: txId, Epoch: epoch, Key: key,
        ManifestHash: 0, RecordAnchorKey: key,
        CommitTimestamp: new HLCTimestamp(txId.N, txId.L + 1, txId.C),
        State: KeyValueState.Set, Value: [1, 2, 3], Bucket: null,
        Revision: revision, Expires: HLCTimestamp.Zero, NoRevision: false,
        BaseRevision: baseRevision, BaseState: baseState,
        RecoveryDeadline: HLCTimestamp.Zero, Resolution: PreparedIntentResolution.Pending);

    private static KeyValueStagedBaseProbe Probe(long baseRevision, long revision, bool noRevision = false) =>
        new(baseRevision, (int)(baseRevision == PreparedIntent.UnknownBaseRevision ? KeyValueState.Undefined : KeyValueState.Set), revision, noRevision);

    private static void CommitThroughStore(PreparedIntentStore store, PreparedIntent intent)
    {
        Assert.Equal(TransactionApplyOutcome.Applied, store.Apply(new PrepareIntentCommand(intent)).Outcome);
        Assert.Equal(TransactionApplyOutcome.Applied,
            store.Apply(new ResolveIntentCommand(intent.TransactionId, intent.Epoch, intent.Key, Commit: true)).Outcome);
        Assert.Equal(TransactionApplyOutcome.Applied,
            store.Apply(new RemoveIntentCommand(intent.TransactionId, intent.Epoch, intent.Key)).Outcome);
    }

    [Fact]
    public void AbsentIntentBehindTheHead_AnswersStaleBaseFromMemory()
    {
        PreparedIntentStore store = new();
        CommitThroughStore(store, MakeIntent("mem/stale", Ts(1_000), epoch: 1, revision: 6, baseRevision: 5, KeyValueState.Set));

        // The prepare has not applied here (no intent), but the ask says it was validated against revision 5.
        KeyValueStagedBaseVerdictEntry[] verdicts = store.EvaluateReplicaFenceVerdicts(Ts(1_100), 1, ["mem/stale"], [Probe(baseRevision: 5, revision: 7)]);

        KeyValueStagedBaseVerdictEntry verdict = Assert.Single(verdicts);
        Assert.Equal(KeyValueStagedBaseVerdict.StaleBase, verdict.Verdict);
        Assert.Equal(6, verdict.HeadRevision);
    }

    [Fact]
    public void AbsentIntentAtTheHead_AnswersClearFromMemory()
    {
        PreparedIntentStore store = new();
        CommitThroughStore(store, MakeIntent("mem/current", Ts(1_000), epoch: 1, revision: 6, baseRevision: 5, KeyValueState.Set));

        KeyValueStagedBaseVerdictEntry[] verdicts = store.EvaluateReplicaFenceVerdicts(Ts(1_100), 1, ["mem/current"], [Probe(baseRevision: 6, revision: 7)]);

        KeyValueStagedBaseVerdictEntry verdict = Assert.Single(verdicts);
        Assert.Equal(KeyValueStagedBaseVerdict.Clear, verdict.Verdict);
        Assert.Equal(6, verdict.HeadRevision);
    }

    [Fact]
    public void AbsentIntentWithNoRememberedHead_AnswersClear()
    {
        PreparedIntentStore store = new();

        KeyValueStagedBaseVerdictEntry[] verdicts = store.EvaluateReplicaFenceVerdicts(Ts(1_100), 1, ["mem/none"], [Probe(baseRevision: 5, revision: 6)]);

        KeyValueStagedBaseVerdictEntry verdict = Assert.Single(verdicts);
        Assert.Equal(KeyValueStagedBaseVerdict.Clear, verdict.Verdict);
        Assert.Equal(-1, verdict.HeadRevision);
    }

    [Fact]
    public void TheTransactionsOwnSettledLeg_AnswersClear_NotStaleBase()
    {
        PreparedIntentStore store = new();

        // This transaction's leg already settled here: the head IS its commit, two revisions past its base. A
        // memory read that did not know the identity would refuse it as stale.
        PreparedIntent own = MakeIntent("mem/own", Ts(1_000), epoch: 1, revision: 6, baseRevision: 5, KeyValueState.Set);
        CommitThroughStore(store, own);

        KeyValueStagedBaseVerdictEntry[] verdicts = store.EvaluateReplicaFenceVerdicts(own.TransactionId, own.Epoch, ["mem/own"], [Probe(baseRevision: 5, revision: 6)]);

        Assert.Equal(KeyValueStagedBaseVerdict.Clear, Assert.Single(verdicts).Verdict);

        // A different attempt of a different transaction on the same base is still refused.
        verdicts = store.EvaluateReplicaFenceVerdicts(Ts(1_200), 1, ["mem/own"], [Probe(baseRevision: 5, revision: 6)]);
        Assert.Equal(KeyValueStagedBaseVerdict.StaleBase, Assert.Single(verdicts).Verdict);
    }

    [Fact]
    public void BlindWrites_JudgeOnlyTheStagedRevision()
    {
        PreparedIntentStore store = new();
        CommitThroughStore(store, MakeIntent("mem/blind", Ts(1_000), epoch: 1, revision: 6, baseRevision: 5, KeyValueState.Set));

        // Revision-free: nothing the memory can check.
        KeyValueStagedBaseVerdictEntry[] verdicts = store.EvaluateReplicaFenceVerdicts(
            Ts(1_100), 1, ["mem/blind"], [Probe(PreparedIntent.UnknownBaseRevision, revision: 0, noRevision: true)]);
        Assert.Equal(KeyValueStagedBaseVerdict.Clear, Assert.Single(verdicts).Verdict);

        // Revision-carrying and staged at the head: another transaction already committed that revision.
        verdicts = store.EvaluateReplicaFenceVerdicts(Ts(1_100), 1, ["mem/blind"], [Probe(PreparedIntent.UnknownBaseRevision, revision: 6)]);
        Assert.Equal(KeyValueStagedBaseVerdict.StaleBase, Assert.Single(verdicts).Verdict);

        // Revision-carrying and staged above the head: admitted.
        verdicts = store.EvaluateReplicaFenceVerdicts(Ts(1_100), 1, ["mem/blind"], [Probe(PreparedIntent.UnknownBaseRevision, revision: 7)]);
        Assert.Equal(KeyValueStagedBaseVerdict.Clear, Assert.Single(verdicts).Verdict);
    }

    [Fact]
    public void AppliedIntent_IsStillJudgedFromTheIntent_NotTheProbe()
    {
        PreparedIntentStore store = new();
        CommitThroughStore(store, MakeIntent("mem/applied", Ts(1_000), epoch: 1, revision: 6, baseRevision: 5, KeyValueState.Set));

        // The applied intent is current; a probe that disagrees (an impossible ask) must not override it.
        PreparedIntent current = MakeIntent("mem/applied", Ts(1_100), epoch: 1, revision: 7, baseRevision: 6, KeyValueState.Set);
        Assert.Equal(TransactionApplyOutcome.Applied, store.Apply(new PrepareIntentCommand(current)).Outcome);

        KeyValueStagedBaseVerdictEntry[] verdicts = store.EvaluateReplicaFenceVerdicts(current.TransactionId, 1, ["mem/applied"], [Probe(baseRevision: 5, revision: 7)]);
        Assert.Equal(KeyValueStagedBaseVerdict.Clear, Assert.Single(verdicts).Verdict);
    }

    [Fact]
    public void MisalignedProbes_FallBackToNotApplied()
    {
        PreparedIntentStore store = new();
        CommitThroughStore(store, MakeIntent("mem/misaligned", Ts(1_000), epoch: 1, revision: 6, baseRevision: 5, KeyValueState.Set));

        KeyValueStagedBaseVerdictEntry[] verdicts = store.EvaluateReplicaFenceVerdicts(Ts(1_100), 1, ["mem/misaligned"], []);

        Assert.Equal(KeyValueStagedBaseVerdict.NotApplied, Assert.Single(verdicts).Verdict);
    }

    [Fact]
    public async Task WaitingVerdictRead_WithProbes_AnswersAtOnceWithoutTheApplyWait()
    {
        PreparedIntentStore store = new();
        CommitThroughStore(store, MakeIntent("mem/wait", Ts(1_000), epoch: 1, revision: 6, baseRevision: 5, KeyValueState.Set));

        // A full apply wait that would have parked the read until the prepare applied — which it never does here.
        Task<KeyValueStagedBaseVerdictEntry[]> read = store.EvaluateReplicaFenceVerdictsAsync(
            Ts(1_100), 1, ["mem/wait"], waitMs: 60_000, TestContext.Current.CancellationToken, [Probe(baseRevision: 5, revision: 7)]);

        KeyValueStagedBaseVerdictEntry[] verdicts = await read.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        KeyValueStagedBaseVerdictEntry verdict = Assert.Single(verdicts);
        Assert.Equal(KeyValueStagedBaseVerdict.StaleBase, verdict.Verdict);
        Assert.Equal(6, verdict.HeadRevision);
    }
}
