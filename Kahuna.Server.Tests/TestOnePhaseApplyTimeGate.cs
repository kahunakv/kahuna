using Kahuna;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Replication;
using Kahuna.Shared.KeyValue;
using Kommander.Data;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// Store-level coverage of the apply-time validation of a one-phase bundled commit: the transaction-record
/// store asks the prepared-intent store for a verdict on the bundle's validated bases and carried read-only
/// dependencies against the partition's committed-head ledger, at the commit's own apply position. Every
/// input is replicated state, so the verdict is the same on every replica of the partition; these tests pin
/// the verdict rules, the wire round-trip of the carried fields, the per-partition scoping of the ledger,
/// the fail-closed behaviour without a judge, and the rejection memo that keeps a replayed rejection a
/// rejection.
/// </summary>
public sealed class TestOnePhaseApplyTimeGate
{
    private const int Partition = 7;

    private static HLCTimestamp Ts(long l) => new(0, l, 0);

    private static PreparedIntent MakeIntent(
        string key, HLCTimestamp txId, long revision, long baseRevision, KeyValueState baseState,
        KeyValueState state = KeyValueState.Set, long manifestHash = 0, string? anchor = null) => new(
        TransactionId: txId, Epoch: 1, Key: key,
        ManifestHash: manifestHash, RecordAnchorKey: anchor ?? key,
        CommitTimestamp: new HLCTimestamp(txId.N, txId.L + 1, txId.C),
        State: state, Value: state == KeyValueState.Set ? [1, 2, 3] : null, Bucket: null,
        Revision: revision, Expires: HLCTimestamp.Zero, NoRevision: false,
        BaseRevision: baseRevision, BaseState: baseState,
        RecoveryDeadline: HLCTimestamp.Zero, Resolution: PreparedIntentResolution.Pending);

    /// <summary>Runs one competitor's commit lifecycle through the store on a partition's log so its head lands
    /// in that partition's ledger slice.</summary>
    private static void CommitThroughStore(PreparedIntentStore store, PreparedIntent intent, int partitionId = Partition)
    {
        Assert.Equal(TransactionApplyOutcome.Applied, store.Apply(new PrepareIntentCommand(intent), partitionId).Outcome);
        Assert.Equal(TransactionApplyOutcome.Applied,
            store.Apply(new ResolveIntentCommand(intent.TransactionId, intent.Epoch, intent.Key, Commit: true), partitionId).Outcome);
        Assert.Equal(TransactionApplyOutcome.Applied,
            store.Apply(new RemoveIntentCommand(intent.TransactionId, intent.Epoch, intent.Key), partitionId).Outcome);
    }

    private static (TransactionRecordStore Records, PreparedIntentStore Intents) Stores()
    {
        TransactionRecordStore records = new();
        PreparedIntentStore intents = new();
        records.AttachBundledCommitJudge(intents.JudgeBundledCommit);
        return (records, intents);
    }

    /// <summary>A one-key transaction's record init plus its bundled commit, sharing one manifest hash.</summary>
    private static (InitializeTransactionCommand Init, CommitTransactionCommand Commit, long Hash) Bundle(
        HLCTimestamp txId, string key, HLCTimestamp opId, bool applyTimeValidation = true,
        IReadOnlyList<BundledReadDependency>? reads = null)
    {
        List<TransactionParticipantRef> manifest = [new(key, KeyValueDurability.Persistent)];
        long hash = TransactionManifest.ComputeHash(txId, 1, key, Ts(txId.L + 1), manifest);

        InitializeTransactionCommand init = new(txId, 1, "coord", key, Ts(txId.L + 1), Ts(txId.L + 9_000), hash, manifest, opId, txId);
        CommitTransactionCommand commit = new(txId, 1, hash, opId, AttemptHlc: opId, BundledPrepareKeys: [key],
            ApplyTimeValidation: applyTimeValidation, BundledReadDependencies: reads);

        return (init, commit, hash);
    }

    /// <summary>Applies [init, prepare, commit] in one batch order on a partition's log, exactly as the bundle
    /// applies on every replica, and returns the commit's apply outcome.</summary>
    private static TransactionApplyOutcome ApplyBundle(
        TransactionRecordStore records, PreparedIntentStore intents,
        HLCTimestamp txId, PreparedIntent intent, HLCTimestamp opId,
        bool applyTimeValidation = true, IReadOnlyList<BundledReadDependency>? reads = null, int partitionId = Partition)
    {
        (InitializeTransactionCommand init, CommitTransactionCommand commit, long hash) = Bundle(txId, intent.Key, opId, applyTimeValidation, reads);

        Assert.Equal(TransactionApplyOutcome.Applied, records.Apply(init, partitionId).Outcome);
        intents.Apply(new PrepareIntentCommand(intent with { ManifestHash = hash }), partitionId);
        return records.Apply(commit, partitionId).Outcome;
    }

    // ── validated base against the ledger ──────────────────────────────────────

    [Fact]
    public void BaseAtTheHead_Admits()
    {
        (TransactionRecordStore records, PreparedIntentStore intents) = Stores();
        CommitThroughStore(intents, MakeIntent("g/at", Ts(1_000), revision: 6, baseRevision: 5, KeyValueState.Set));

        HLCTimestamp txId = Ts(1_100);
        Assert.Equal(TransactionApplyOutcome.Applied,
            ApplyBundle(records, intents, txId, MakeIntent("g/at", txId, revision: 7, baseRevision: 6, KeyValueState.Set), Ts(1_200)));
        Assert.Equal(TransactionDecision.Commit, records.Get(txId, 1)!.Decision);
    }

    [Fact]
    public void HeadAboveTheBase_Rejects_RecordStaysUndecided_AndIsMemoed()
    {
        (TransactionRecordStore records, PreparedIntentStore intents) = Stores();
        CommitThroughStore(intents, MakeIntent("g/moved", Ts(1_000), revision: 6, baseRevision: 5, KeyValueState.Set));

        long before = DurableTransactionMetrics.OnePhaseGatedCommitStaleBaseRejectionsCount;

        // Validated against revision 5; the competitor's settled commit already moved the head to 6.
        HLCTimestamp txId = Ts(1_100);
        HLCTimestamp opId = Ts(1_200);
        Assert.Equal(TransactionApplyOutcome.Rejected,
            ApplyBundle(records, intents, txId, MakeIntent("g/moved", txId, revision: 6, baseRevision: 5, KeyValueState.Set), opId));

        TransactionRecord record = records.Get(txId, 1)!;
        Assert.Equal(TransactionDecision.Undecided, record.Decision);
        Assert.True(record.WasBundledCommitRejected(opId), "the rejection must be memoed on the record");
        Assert.Equal(before + 1, DurableTransactionMetrics.OnePhaseGatedCommitStaleBaseRejectionsCount);

        // The proposing finalizer learns the verdict kind from the store, exactly once.
        Assert.True(records.TryTakeGatedRejectionVerdict(txId, 1, opId, out BundledCommitVerdict verdict));
        Assert.Equal(BundledCommitVerdict.StaleBase, verdict);
        Assert.False(records.TryTakeGatedRejectionVerdict(txId, 1, opId, out _));

        // The intent installed identically (the replicated transition never changes); only the record withheld.
        Assert.NotNull(intents.Get("g/moved"));
    }

    [Fact]
    public void HeadBehindTheBase_Admits()
    {
        (TransactionRecordStore records, PreparedIntentStore intents) = Stores();
        CommitThroughStore(intents, MakeIntent("g/lagging", Ts(1_000), revision: 6, baseRevision: 5, KeyValueState.Set));

        // Non-transactional writes advanced the key past the remembered head; the transaction read revision 9.
        HLCTimestamp txId = Ts(1_100);
        Assert.Equal(TransactionApplyOutcome.Applied,
            ApplyBundle(records, intents, txId, MakeIntent("g/lagging", txId, revision: 10, baseRevision: 9, KeyValueState.Set), Ts(1_200)));
        Assert.Equal(TransactionDecision.Commit, records.Get(txId, 1)!.Decision);
    }

    [Fact]
    public void ValidatedAbsentBase_AgainstASetHead_Rejects_AgainstADeletedHead_Admits()
    {
        (TransactionRecordStore records, PreparedIntentStore intents) = Stores();

        CommitThroughStore(intents, MakeIntent("g/appeared", Ts(1_000), revision: 0, PreparedIntent.UnknownBaseRevision, KeyValueState.Undefined));
        HLCTimestamp stale = Ts(1_100);
        Assert.Equal(TransactionApplyOutcome.Rejected,
            ApplyBundle(records, intents, stale, MakeIntent("g/appeared", stale, revision: 0, baseRevision: -1, KeyValueState.Undefined), Ts(1_200)));
        Assert.Equal(TransactionDecision.Undecided, records.Get(stale, 1)!.Decision);

        CommitThroughStore(intents, MakeIntent("g/deleted", Ts(1_000), revision: 4, baseRevision: 3, KeyValueState.Set, state: KeyValueState.Deleted));
        HLCTimestamp clean = Ts(1_150);
        Assert.Equal(TransactionApplyOutcome.Applied,
            ApplyBundle(records, intents, clean, MakeIntent("g/deleted", clean, revision: 0, baseRevision: -1, KeyValueState.Undefined), Ts(1_250)));
        Assert.Equal(TransactionDecision.Commit, records.Get(clean, 1)!.Decision);
    }

    [Fact]
    public void AbsentHead_Admits()
    {
        (TransactionRecordStore records, PreparedIntentStore intents) = Stores();

        HLCTimestamp txId = Ts(1_100);
        Assert.Equal(TransactionApplyOutcome.Applied,
            ApplyBundle(records, intents, txId, MakeIntent("g/fresh", txId, revision: 6, baseRevision: 5, KeyValueState.Set), Ts(1_200)));
        Assert.Equal(TransactionDecision.Commit, records.Get(txId, 1)!.Decision);
    }

    [Fact]
    public void TransactionOlderThanTheRetentionHorizon_IsRefused()
    {
        (TransactionRecordStore records, PreparedIntentStore intents) = Stores();
        intents.ConfigureStagedBaseFence(retentionMs: 1_000);

        // An unrelated key's commit advances the partition's watermark far past the horizon.
        CommitThroughStore(intents, MakeIntent("g/other", Ts(100_000), revision: 1, baseRevision: 0, KeyValueState.Set));

        // A transaction that began long before the horizon: the head its base depends on may be pruned, so
        // absence of evidence must refuse rather than admit.
        HLCTimestamp ancient = Ts(10);
        Assert.Equal(TransactionApplyOutcome.Rejected,
            ApplyBundle(records, intents, ancient, MakeIntent("g/ancient", ancient, revision: 3, baseRevision: 2, KeyValueState.Set), Ts(100_100)));
        Assert.Equal(TransactionDecision.Undecided, records.Get(ancient, 1)!.Decision);
    }

    [Fact]
    public void HeadOutsideTheRetentionWindow_IsAbsentToTheGate_EvenWhileStillPhysicallyHeld()
    {
        (TransactionRecordStore records, PreparedIntentStore intents) = Stores();
        intents.ConfigureStagedBaseFence(retentionMs: 1_000);

        // The head for the key was committed at 1_100. A commit at 2_000 crosses into a new retention bucket
        // and physically prunes below 1_000 (the head survives), and a later commit at 2_150 stays in that
        // bucket (no prune) while moving the logical window past 1_100: the head is now physically present
        // but outside the window.
        CommitThroughStore(intents, MakeIntent("g/old", Ts(1_099), revision: 6, baseRevision: 5, KeyValueState.Set));
        CommitThroughStore(intents, MakeIntent("g/other", Ts(1_999), revision: 1, baseRevision: 0, KeyValueState.Set));
        CommitThroughStore(intents, MakeIntent("g/other2", Ts(2_149), revision: 1, baseRevision: 0, KeyValueState.Set));
        Assert.True(intents.TryGetLedgerHead(Partition, "g/old", out long physicalRevision, out _, out _));
        Assert.Equal(6, physicalRevision);

        // A young transaction (it passes the staleness gate) whose base (5) predates that head: the gate must
        // not judge by a head the window no longer retains, so physical pruning is unobservable to the verdict.
        HLCTimestamp young = Ts(2_140);
        Assert.Equal(TransactionApplyOutcome.Applied,
            ApplyBundle(records, intents, young, MakeIntent("g/old", young, revision: 7, baseRevision: 5, KeyValueState.Set), Ts(2_200)));
        Assert.Equal(TransactionDecision.Commit, records.Get(young, 1)!.Decision);
    }

    [Fact]
    public void BlindWrite_IsNeverJudged()
    {
        (TransactionRecordStore records, PreparedIntentStore intents) = Stores();
        CommitThroughStore(intents, MakeIntent("g/blind", Ts(1_000), revision: 6, baseRevision: 5, KeyValueState.Set));

        HLCTimestamp txId = Ts(1_100);
        Assert.Equal(TransactionApplyOutcome.Applied,
            ApplyBundle(records, intents, txId, MakeIntent("g/blind", txId, revision: 7, PreparedIntent.UnknownBaseRevision, KeyValueState.Undefined), Ts(1_200)));
        Assert.Equal(TransactionDecision.Commit, records.Get(txId, 1)!.Decision);
    }

    [Fact]
    public void WithoutApplyTimeValidation_TheBaseIsNotJudged()
    {
        // The shipped gate: presence only. A commit that does not ask for apply-time validation commits over a
        // moved base at apply exactly as before, so a group that has not enabled the option behaves as today.
        (TransactionRecordStore records, PreparedIntentStore intents) = Stores();
        CommitThroughStore(intents, MakeIntent("g/legacy", Ts(1_000), revision: 6, baseRevision: 5, KeyValueState.Set));

        HLCTimestamp txId = Ts(1_100);
        Assert.Equal(TransactionApplyOutcome.Applied,
            ApplyBundle(records, intents, txId, MakeIntent("g/legacy", txId, revision: 6, baseRevision: 5, KeyValueState.Set), Ts(1_200),
                applyTimeValidation: false));
        Assert.Equal(TransactionDecision.Commit, records.Get(txId, 1)!.Decision);
    }

    // ── read-only dependencies ────────────────────────────────────────────────────

    [Fact]
    public void ReadDependency_HeldByAForeignPendingIntent_Rejects()
    {
        (TransactionRecordStore records, PreparedIntentStore intents) = Stores();

        // A competitor's undecided write on the key this transaction read.
        Assert.Equal(TransactionApplyOutcome.Applied,
            intents.Apply(new PrepareIntentCommand(MakeIntent("g/read", Ts(900), revision: 6, baseRevision: 5, KeyValueState.Set)), Partition).Outcome);

        long before = DurableTransactionMetrics.OnePhaseGatedCommitStaleReadRejectionsCount;

        HLCTimestamp txId = Ts(1_100);
        HLCTimestamp opId = Ts(1_200);
        Assert.Equal(TransactionApplyOutcome.Rejected,
            ApplyBundle(records, intents, txId, MakeIntent("g/write", txId, revision: 1, PreparedIntent.UnknownBaseRevision, KeyValueState.Undefined), opId,
                reads: [new BundledReadDependency("g/read", 5, ObservedExists: true)]));

        Assert.Equal(TransactionDecision.Undecided, records.Get(txId, 1)!.Decision);
        Assert.Equal(before + 1, DurableTransactionMetrics.OnePhaseGatedCommitStaleReadRejectionsCount);
        Assert.True(records.TryTakeGatedRejectionVerdict(txId, 1, opId, out BundledCommitVerdict verdict));
        Assert.Equal(BundledCommitVerdict.StaleRead, verdict);
    }

    [Fact]
    public void ReadDependency_HeldByAForeignAbortedIntent_Admits()
    {
        (TransactionRecordStore records, PreparedIntentStore intents) = Stores();

        PreparedIntent aborted = MakeIntent("g/read", Ts(900), revision: 6, baseRevision: 5, KeyValueState.Set);
        intents.Apply(new PrepareIntentCommand(aborted), Partition);
        intents.Apply(new ResolveIntentCommand(aborted.TransactionId, 1, "g/read", Commit: false), Partition);

        HLCTimestamp txId = Ts(1_100);
        Assert.Equal(TransactionApplyOutcome.Applied,
            ApplyBundle(records, intents, txId, MakeIntent("g/write", txId, revision: 1, PreparedIntent.UnknownBaseRevision, KeyValueState.Undefined), Ts(1_200),
                reads: [new BundledReadDependency("g/read", 5, ObservedExists: true)]));
        Assert.Equal(TransactionDecision.Commit, records.Get(txId, 1)!.Decision);
    }

    [Fact]
    public void ReadDependency_HeadAboveTheObservedRevision_Rejects_AtTheRevision_Admits()
    {
        (TransactionRecordStore records, PreparedIntentStore intents) = Stores();
        CommitThroughStore(intents, MakeIntent("g/read", Ts(1_000), revision: 6, baseRevision: 5, KeyValueState.Set));

        // Observed revision 5, but a settled commit moved the key to 6 before the bundle applied.
        HLCTimestamp stale = Ts(1_100);
        Assert.Equal(TransactionApplyOutcome.Rejected,
            ApplyBundle(records, intents, stale, MakeIntent("g/write1", stale, revision: 1, PreparedIntent.UnknownBaseRevision, KeyValueState.Undefined), Ts(1_200),
                reads: [new BundledReadDependency("g/read", 5, ObservedExists: true)]));
        Assert.Equal(TransactionDecision.Undecided, records.Get(stale, 1)!.Decision);

        // Observed exactly the committed head: current.
        HLCTimestamp current = Ts(1_150);
        Assert.Equal(TransactionApplyOutcome.Applied,
            ApplyBundle(records, intents, current, MakeIntent("g/write2", current, revision: 1, PreparedIntent.UnknownBaseRevision, KeyValueState.Undefined), Ts(1_250),
                reads: [new BundledReadDependency("g/read", 6, ObservedExists: true)]));
        Assert.Equal(TransactionDecision.Commit, records.Get(current, 1)!.Decision);
    }

    [Fact]
    public void ReadDependency_ObservedAbsent_AgainstASetHead_Rejects()
    {
        (TransactionRecordStore records, PreparedIntentStore intents) = Stores();
        CommitThroughStore(intents, MakeIntent("g/read", Ts(1_000), revision: 0, PreparedIntent.UnknownBaseRevision, KeyValueState.Undefined));

        HLCTimestamp txId = Ts(1_100);
        Assert.Equal(TransactionApplyOutcome.Rejected,
            ApplyBundle(records, intents, txId, MakeIntent("g/write", txId, revision: 1, PreparedIntent.UnknownBaseRevision, KeyValueState.Undefined), Ts(1_200),
                reads: [new BundledReadDependency("g/read", -1, ObservedExists: false)]));
        Assert.Equal(TransactionDecision.Undecided, records.Get(txId, 1)!.Decision);
    }

    [Fact]
    public void ReadDependency_AbsentHead_Admits()
    {
        (TransactionRecordStore records, PreparedIntentStore intents) = Stores();

        HLCTimestamp txId = Ts(1_100);
        Assert.Equal(TransactionApplyOutcome.Applied,
            ApplyBundle(records, intents, txId, MakeIntent("g/write", txId, revision: 1, PreparedIntent.UnknownBaseRevision, KeyValueState.Undefined), Ts(1_200),
                reads: [new BundledReadDependency("g/never-written", 3, ObservedExists: true)]));
        Assert.Equal(TransactionDecision.Commit, records.Get(txId, 1)!.Decision);
    }

    // ── determinism: the ledger is per partition ────────────────────────────────

    [Fact]
    public void HeadFedThroughAnotherPartitionsLog_IsInvisibleToTheGate_ButVisibleToTheAdvisoryFence()
    {
        (TransactionRecordStore records, PreparedIntentStore intents) = Stores();

        // The competitor's settlement applied through partition 8's log — a replica that hosts only partition
        // 7 never saw it, so partition 7's gate must not judge by it or the two replicas would fork.
        CommitThroughStore(intents, MakeIntent("g/elsewhere", Ts(1_000), revision: 6, baseRevision: 5, KeyValueState.Set), partitionId: 8);
        Assert.True(intents.TryGetCommittedHead("g/elsewhere", out long advisoryHead, out _));
        Assert.Equal(6, advisoryHead);
        Assert.False(intents.TryGetLedgerHead(Partition, "g/elsewhere", out _, out _, out _));

        HLCTimestamp txId = Ts(1_100);
        Assert.Equal(TransactionApplyOutcome.Applied,
            ApplyBundle(records, intents, txId, MakeIntent("g/elsewhere", txId, revision: 6, baseRevision: 5, KeyValueState.Set), Ts(1_200)));
        Assert.Equal(TransactionDecision.Commit, records.Get(txId, 1)!.Decision);
    }

    // ── gate mechanics ────────────────────────────────────────────────────────────

    [Fact]
    public void WithoutAJudge_TheGateFailsClosed()
    {
        TransactionRecordStore probeless = new();
        PreparedIntentStore intents = new();

        HLCTimestamp txId = Ts(1_100);
        (InitializeTransactionCommand init, CommitTransactionCommand commit, long hash) = Bundle(txId, "g/closed", Ts(1_200));
        Assert.Equal(TransactionApplyOutcome.Applied, probeless.Apply(init, Partition).Outcome);
        intents.Apply(new PrepareIntentCommand(MakeIntent("g/closed", txId, revision: 1, PreparedIntent.UnknownBaseRevision, KeyValueState.Undefined, manifestHash: hash)), Partition);

        Assert.Equal(TransactionApplyOutcome.Rejected, probeless.Apply(commit, Partition).Outcome);
        Assert.Equal(TransactionDecision.Undecided, probeless.Get(txId, 1)!.Decision);
    }

    [Fact]
    public void ReplayAgainstATerminalRecord_SkipsTheGate()
    {
        (TransactionRecordStore records, PreparedIntentStore intents) = Stores();

        HLCTimestamp txId = Ts(1_100);
        (InitializeTransactionCommand init, CommitTransactionCommand commit, long hash) = Bundle(txId, "g/terminal", Ts(1_200));
        PreparedIntent intent = MakeIntent("g/terminal", txId, revision: 6, baseRevision: 5, KeyValueState.Set, manifestHash: hash);

        Assert.Equal(TransactionApplyOutcome.Applied, records.Apply(init, Partition).Outcome);
        intents.Apply(new PrepareIntentCommand(intent), Partition);
        Assert.Equal(TransactionApplyOutcome.Applied, records.Apply(commit, Partition).Outcome);

        // Settlement removes the intent and records the head; a replay of the same commit is an idempotent
        // no-op even though the judge would now see the transaction's own head above its base.
        intents.Apply(new ResolveIntentCommand(txId, 1, "g/terminal", Commit: true), Partition);
        intents.Apply(new RemoveIntentCommand(txId, 1, "g/terminal"), Partition);
        Assert.Equal(TransactionApplyOutcome.IdempotentNoop, records.Apply(commit, Partition).Outcome);
        Assert.Equal(TransactionDecision.Commit, records.Get(txId, 1)!.Decision);
    }

    [Fact]
    public void RejectionMemo_KeepsAReplayedRejection_EvenWhenTheLedgerNoLongerObjects()
    {
        (TransactionRecordStore records, PreparedIntentStore intents) = Stores();
        CommitThroughStore(intents, MakeIntent("g/memo", Ts(1_000), revision: 6, baseRevision: 5, KeyValueState.Set));

        HLCTimestamp txId = Ts(1_100);
        HLCTimestamp opId = Ts(1_200);
        (InitializeTransactionCommand init, CommitTransactionCommand commit, long hash) = Bundle(txId, "g/memo", opId);
        PreparedIntent intent = MakeIntent("g/memo", txId, revision: 6, baseRevision: 5, KeyValueState.Set, manifestHash: hash);

        Assert.Equal(TransactionApplyOutcome.Applied, records.Apply(init, Partition).Outcome);
        intents.Apply(new PrepareIntentCommand(intent), Partition);
        Assert.Equal(TransactionApplyOutcome.Rejected, records.Apply(commit, Partition).Outcome);

        // The ledger a restarted replica would reload may no longer hold the head that refused the commit
        // (the window moved on); the memo keeps the replayed verdict identical to the live one.
        intents.ForgetCommittedHeadsForTesting();
        Assert.Equal(BundledCommitJudgement.Admitted, intents.JudgeBundledCommit(Partition, commit));

        Assert.Equal(TransactionApplyOutcome.Rejected, records.Apply(commit, Partition).Outcome);
        Assert.Equal(TransactionDecision.Undecided, records.Get(txId, 1)!.Decision);

        // A later attempt with a fresh operation id is judged on its merits (and now admits).
        CommitTransactionCommand fresh = commit with { OpId = Ts(1_300), AttemptHlc = Ts(1_300) };
        Assert.Equal(TransactionApplyOutcome.Applied, records.Apply(fresh, Partition).Outcome);
        Assert.Equal(TransactionDecision.Commit, records.Get(txId, 1)!.Decision);
    }

    [Fact]
    public void RejectionMemo_SurvivesTheRecordSnapshot_AndStateTransfer()
    {
        (TransactionRecordStore records, PreparedIntentStore intents) = Stores();
        CommitThroughStore(intents, MakeIntent("g/persist", Ts(1_000), revision: 6, baseRevision: 5, KeyValueState.Set));

        HLCTimestamp txId = Ts(1_100);
        HLCTimestamp opId = Ts(1_200);
        (InitializeTransactionCommand init, CommitTransactionCommand commit, long hash) = Bundle(txId, "g/persist", opId);
        records.Apply(init, Partition);
        intents.Apply(new PrepareIntentCommand(MakeIntent("g/persist", txId, revision: 6, baseRevision: 5, KeyValueState.Set, manifestHash: hash)), Partition);
        Assert.Equal(TransactionApplyOutcome.Rejected, records.Apply(commit, Partition).Outcome);

        // Through the state-transfer serializer (the same entry shape the durable snapshot streams).
        IReadOnlyList<TransactionRecord> transferred = TransactionRecordStore.DeserializeRecords(
            TransactionRecordStore.SerializeRecords(records.SnapshotRange(null, null)));
        TransactionRecord restored = Assert.Single(transferred);
        Assert.True(restored.WasBundledCommitRejected(opId));

        TransactionRecordStore seeded = new();
        seeded.AttachBundledCommitJudge(intents.JudgeBundledCommit);
        seeded.ImportRecords(transferred);
        intents.ForgetCommittedHeadsForTesting();
        Assert.Equal(TransactionApplyOutcome.Rejected, seeded.Apply(commit, Partition).Outcome);
    }

    // ── rolling-upgrade hazard ────────────────────────────────────────────────────

    /// <summary>
    /// Why the option must stay off until every node applies the extended gate: an applier from before the
    /// check (modeled as a judge that knows only the shipped presence rule) commits the same bundled commit
    /// that a current applier refuses, and the two replicas' records fork on the same log entry.
    /// </summary>
    [Fact]
    public void ApplierWithoutTheCheck_CommitsWhereACurrentOneRefuses_WhichForksTheRecord()
    {
        PreparedIntentStore intents = new();
        CommitThroughStore(intents, MakeIntent("g/fork", Ts(1_000), revision: 6, baseRevision: 5, KeyValueState.Set));

        TransactionRecordStore current = new();
        current.AttachBundledCommitJudge(intents.JudgeBundledCommit);

        // The pre-ledger gate: presence only, whatever the command asks for.
        TransactionRecordStore legacy = new();
        legacy.AttachBundledCommitJudge((_, commit) =>
        {
            foreach (string bundledKey in commit.BundledPrepareKeys!)
                if (intents.Get(bundledKey) is not { } live || live.TransactionId != commit.TransactionId || live.Epoch != commit.Epoch)
                    return new(BundledCommitVerdict.PrepareMissing, "bundled prepare not applied");
            return BundledCommitJudgement.Admitted;
        });

        HLCTimestamp txId = Ts(1_100);
        (InitializeTransactionCommand init, CommitTransactionCommand commit, long hash) = Bundle(txId, "g/fork", Ts(1_200));
        intents.Apply(new PrepareIntentCommand(MakeIntent("g/fork", txId, revision: 6, baseRevision: 5, KeyValueState.Set, manifestHash: hash)), Partition);

        current.Apply(init, Partition);
        legacy.Apply(init, Partition);
        Assert.Equal(TransactionApplyOutcome.Rejected, current.Apply(commit, Partition).Outcome);
        Assert.Equal(TransactionApplyOutcome.Applied, legacy.Apply(commit, Partition).Outcome);

        Assert.Equal(TransactionDecision.Undecided, current.Get(txId, 1)!.Decision);
        Assert.Equal(TransactionDecision.Commit, legacy.Get(txId, 1)!.Decision);
    }

    [Fact]
    public void Option_ThreadsThroughTheEmbeddedConfiguration_OnBothConstructionPaths()
    {
        EmbeddedKahunaOptions options = new() { OnePhaseApplyTimeValidation = true };

        Assert.True(EmbeddedKahunaNode.CreateKahunaConfiguration(options, singleProcessRaftGroup: false).OnePhaseApplyTimeValidation);
        Assert.True(EmbeddedKahunaNode.CreateKahunaConfiguration(options, singleProcessRaftGroup: true).OnePhaseApplyTimeValidation);
        Assert.False(EmbeddedKahunaNode.CreateKahunaConfiguration(new EmbeddedKahunaOptions(), singleProcessRaftGroup: false).OnePhaseApplyTimeValidation);
        Assert.False(new Configuration.KahunaConfiguration().OnePhaseApplyTimeValidation);
    }

    // ── wire ──────────────────────────────────────────────────────────────────────

    [Fact]
    public void ApplyTimeValidationAndReadDependencies_SurviveTheDeltaWireRoundTrip()
    {
        (TransactionRecordStore records, PreparedIntentStore intents) = Stores();
        CommitThroughStore(intents, MakeIntent("g/wire-read", Ts(1_000), revision: 6, baseRevision: 5, KeyValueState.Set));

        HLCTimestamp txId = Ts(1_100);
        (InitializeTransactionCommand init, CommitTransactionCommand commit, long hash) = Bundle(txId, "g/wire", Ts(1_200),
            reads: [new BundledReadDependency("g/wire-read", 5, ObservedExists: true)]);
        records.Apply(init, Partition);
        intents.Apply(new PrepareIntentCommand(MakeIntent("g/wire", txId, revision: 1, PreparedIntent.UnknownBaseRevision, KeyValueState.Undefined, manifestHash: hash)), Partition);

        // Serialize, then defeat the producer-side decoded-command cache by copying the bytes, forcing the
        // proto decode path a follower or a WAL replay would take. The decoded command must still carry the
        // read dependency and the validation request: the moved read rejects it.
        byte[] wireCopy = [.. TransactionRecordStore.SerializeDelta([commit])];
        records.Replicate(Partition, new RaftLog { LogType = ReplicationTypes.TransactionRecord, LogData = wireCopy });
        Assert.Equal(TransactionDecision.Undecided, records.Get(txId, 1)!.Decision);

        // The same bundle without the validation request commits: the flag itself survived the wire.
        HLCTimestamp legacyTx = Ts(1_150);
        (InitializeTransactionCommand legacyInit, CommitTransactionCommand legacyCommit, long legacyHash) = Bundle(legacyTx, "g/wire2", Ts(1_250),
            applyTimeValidation: false, reads: [new BundledReadDependency("g/wire-read", 5, ObservedExists: true)]);
        records.Apply(legacyInit, Partition);
        intents.Apply(new PrepareIntentCommand(MakeIntent("g/wire2", legacyTx, revision: 1, PreparedIntent.UnknownBaseRevision, KeyValueState.Undefined, manifestHash: legacyHash)), Partition);
        byte[] legacyCopy = [.. TransactionRecordStore.SerializeDelta([legacyCommit])];
        records.Replicate(Partition, new RaftLog { LogType = ReplicationTypes.TransactionRecord, LogData = legacyCopy });
        Assert.Equal(TransactionDecision.Commit, records.Get(legacyTx, 1)!.Decision);
    }
}
