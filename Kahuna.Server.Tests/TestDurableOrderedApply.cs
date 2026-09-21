using Kahuna;
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
/// The durable record/intent stores have exactly one live writer: the ordered consumer apply Raft drives in log
/// order. The write scheduler's completion for a locally proposed entry — which runs at quorum durability, ahead of
/// or behind the leader's own consumer apply — waits for that apply and reads its result; it never applies the
/// delta itself, and it never holds its producer on a node that stopped leading. These tests script the two completion timings that used to fork the proposing node's state from
/// its peers' — the shape in which an ex-leader alone rejects a bundled commit its peers admitted right after a
/// graceful handover, and one acknowledged write is missing on that replica:
///
/// <list type="bullet">
/// <item>a completion that trails the ordered apply by more than the result window, whose re-apply re-installed a
/// prepare the ordered stream had already settled and removed — a zombie intent that made the next transaction's
/// prepare on the key read as a foreign holder on this node only;</item>
/// <item>a completion that overtakes the ordered apply of the entries below it, whose early apply judged a prepare
/// against a competitor's intent the ordered stream had not yet removed, and whose verdict the ordered apply then
/// adopted instead of recomputing.</item>
/// </list>
///
/// <para>The consumer side is scripted exactly as <c>KeyValueReplicationDispatcher.OnReplicationReceived</c> runs it:
/// apply the delta to the store at its log index, then record the result in the ledger.</para>
/// </summary>
public sealed class TestDurableOrderedApply
{
    private const int Partition = 3;

    private const string Key = "ordered/acct";

    private static HLCTimestamp Ts(long l) => new(0, l, 0);

    private static PreparedIntent Intent(HLCTimestamp txId, long revision) => new(
        TransactionId: txId, Epoch: 1, Key: Key,
        ManifestHash: 0, RecordAnchorKey: Key,
        CommitTimestamp: new HLCTimestamp(txId.N, txId.L + 1, txId.C),
        State: KeyValueState.Set, Value: [1, 2, 3], Bucket: null,
        Revision: revision, Expires: HLCTimestamp.Zero, NoRevision: false,
        BaseRevision: PreparedIntent.UnknownBaseRevision, BaseState: KeyValueState.Undefined,
        RecoveryDeadline: HLCTimestamp.Zero, Resolution: PreparedIntentResolution.Pending);

    private sealed class Node
    {
        internal readonly TransactionRecordStore Records = new();
        internal readonly PreparedIntentStore Intents = new();
        internal readonly DurableApplyResultLedger Ledger = new();
        internal readonly DurableOrderedApplyAwaiter Completion;

        internal Node(TimeSpan? waitTimeout = null)
        {
            Records.AttachBundledCommitJudge(Intents.JudgeBundledCommit);
            Completion = new DurableOrderedApplyAwaiter(Ledger, Intents, logger: null, waitTimeout);
        }

        /// <summary>The ordered consumer apply of one intent delta at its log index (the dispatcher's path).</summary>
        internal bool ConsumerApplyIntents(long logIndex, byte[] delta)
        {
            bool acknowledged = Intents.ApplyDeltaAckPrepares(Partition, new RaftLog { Id = logIndex, LogType = ReplicationTypes.PreparedIntent, LogData = delta });
            Ledger.RecordApplied(Partition, logIndex, acknowledged);
            return acknowledged;
        }

        /// <summary>The ordered consumer apply of one record delta at its log index (the dispatcher's path).</summary>
        internal bool ConsumerApplyRecords(long logIndex, byte[] delta)
        {
            bool applied = Records.Replicate(Partition, new RaftLog { Id = logIndex, LogType = ReplicationTypes.TransactionRecord, LogData = delta });
            Ledger.RecordApplied(Partition, logIndex, applied);
            return applied;
        }
    }

    private static byte[] Prepare(PreparedIntent intent) => PreparedIntentStore.SerializeDelta([new PrepareIntentCommand(intent)]);

    private static byte[] Resolve(HLCTimestamp txId, bool commit) => PreparedIntentStore.SerializeDelta([new ResolveIntentCommand(txId, 1, Key, commit)]);

    private static byte[] Remove(HLCTimestamp txId) => PreparedIntentStore.SerializeDelta([new RemoveIntentCommand(txId, 1, Key)]);

    private static (byte[] Init, byte[] Commit, long Hash) Bundle(HLCTimestamp txId, HLCTimestamp opId)
    {
        List<TransactionParticipantRef> manifest = [new(Key, KeyValueDurability.Persistent)];
        long hash = TransactionManifest.ComputeHash(txId, 1, Key, Ts(txId.L + 1), manifest);

        byte[] init = TransactionRecordStore.SerializeDelta([new InitializeTransactionCommand(txId, 1, "coord", Key, Ts(txId.L + 1), Ts(txId.L + 60_000), hash, manifest, opId, txId)]);
        byte[] commit = TransactionRecordStore.SerializeDelta([new CommitTransactionCommand(txId, 1, hash, opId, AttemptHlc: opId, BundledPrepareKeys: [Key])]);
        return (init, commit, hash);
    }

    private static RaftProposalEntry IntentEntry(byte[] delta) => new(ReplicationTypes.PreparedIntent, delta, AutoCommit: true, ExpectedGeneration: 0);

    private static RaftProposalEntry RecordEntry(byte[] delta) => new(ReplicationTypes.TransactionRecord, delta, AutoCommit: true, ExpectedGeneration: 0);

    // ── the handover shape: a late completion after the entry was settled ─────────

    /// <summary>
    /// Transaction A prepared, aborted and was removed by the ordered apply; more than a window of entries followed,
    /// so A's recorded result is gone. A's completion then runs. It must NOT re-install A's prepare (the key is
    /// free: A is gone from the log's point of view), so the next transaction's prepare on the key is admitted here
    /// exactly as on every other replica. The completion reads A's acknowledgement back from the store: A no longer
    /// holds the key, so the producer is answered unacknowledged — the conservative answer for a transaction that
    /// already aborted.
    /// </summary>
    [Fact]
    public async Task LateCompletion_AfterTheOrderedApplySettledTheEntry_DoesNotResurrectTheIntent()
    {
        Node node = new();
        HLCTimestamp a = Ts(1_000);
        byte[] prepareA = Prepare(Intent(a, revision: 1));

        Assert.True(node.ConsumerApplyIntents(10, prepareA));
        Assert.True(node.ConsumerApplyIntents(11, Resolve(a, commit: false)));
        Assert.True(node.ConsumerApplyIntents(12, Remove(a)));
        Assert.Null(node.Intents.Get(Key));

        // Unrelated traffic displaces A's slot from the bounded result window.
        for (long index = 13; index <= 13 + 1_500; index++)
            node.Ledger.RecordApplied(Partition, index, result: true);

        long displacedBefore = DurableTransactionMetrics.OrderedApplyResultsDisplacedCount;

        DurableCompletionAnswer answer = await node.Completion.AwaitAppliedAsync(Partition, [IntentEntry(prepareA)], [10], CancellationToken.None);

        Assert.Equal(DurableCompletionAnswer.Refused, answer);
        Assert.Null(node.Intents.Get(Key));
        Assert.Equal(displacedBefore + 1, DurableTransactionMetrics.OrderedApplyResultsDisplacedCount);

        // The next transaction on the key is admitted here as it is everywhere else: no zombie holder.
        HLCTimestamp b = Ts(2_000);
        Assert.True(node.ConsumerApplyIntents(2_000, Prepare(Intent(b, revision: 2))));
        Assert.NotNull(node.Intents.GetByIdentity(b, 1, Key));
    }

    // ── the other timing: a completion ahead of the ordered apply ───────────────

    /// <summary>
    /// A competitor P holds the key with a committed intent whose removal (log index 10) the consumer has not applied
    /// yet when the completion for X's one-phase bundle [prepare X (11), commit X (12)] runs — the quorum-durable
    /// release outran the leader's own consumer. The completion must not judge the bundle now (it would refuse X's
    /// prepare against P's still-live intent and memo the bundled commit as rejected on this node only); it waits
    /// until the ordered apply reaches 12, and reports the acknowledgement that apply produced: X admitted, record
    /// Commit, no rejection memo — the verdict every other replica reaches for the same log.
    /// </summary>
    [Fact]
    public async Task CompletionAheadOfTheOrderedApply_WaitsForIt_AndReportsItsVerdict()
    {
        Node node = new();
        HLCTimestamp p = Ts(900);
        HLCTimestamp x = Ts(1_100);
        HLCTimestamp opId = Ts(1_150);

        Assert.True(node.ConsumerApplyIntents(5, Prepare(Intent(p, revision: 1))));
        Assert.True(node.ConsumerApplyIntents(6, Resolve(p, commit: true)));

        (byte[] initX, byte[] commitX, long hash) = Bundle(x, opId);
        byte[] prepareX = Prepare(Intent(x, revision: 2) with { ManifestHash = hash });
        Assert.True(node.ConsumerApplyRecords(9, initX));

        Task<DurableCompletionAnswer> completion = node.Completion.AwaitAppliedAsync(
            Partition, [IntentEntry(prepareX), RecordEntry(commitX)], [11, 12], CancellationToken.None);

        await Task.Delay(50);
        Assert.False(completion.IsCompleted);

        // Nothing moved on the completion's account: P still holds the key, X's record is untouched.
        Assert.Equal(p, node.Intents.Get(Key)!.TransactionId);
        TransactionRecord recordBefore = node.Records.Get(x, 1)!;
        Assert.Equal(TransactionDecision.Undecided, recordBefore.Decision);
        Assert.True(recordBefore.RejectedBundledCommitOpIds is not { Count: > 0 });

        // The ordered apply arrives, in log order.
        Assert.True(node.ConsumerApplyIntents(10, Remove(p)));
        Assert.True(node.ConsumerApplyIntents(11, prepareX));
        Assert.True(node.ConsumerApplyRecords(12, commitX));

        Assert.Equal(DurableCompletionAnswer.Acknowledged, await completion);

        TransactionRecord record = node.Records.Get(x, 1)!;
        Assert.Equal(TransactionDecision.Commit, record.Decision);
        Assert.True(record.RejectedBundledCommitOpIds is not { Count: > 0 });
        Assert.NotNull(node.Intents.GetByIdentity(x, 1, Key));
    }

    // ── faithful results ─────────────────────────────────────────────────────────

    [Fact]
    public async Task ARefusedPrepare_ReachesTheProducerAsUnacknowledged()
    {
        Node node = new();
        HLCTimestamp w = Ts(700);
        HLCTimestamp y = Ts(800);

        Assert.True(node.ConsumerApplyIntents(20, Prepare(Intent(w, revision: 1))));

        // Y's prepare lands behind W's live intent: refused by the ordered apply, and the completion says so.
        byte[] prepareY = Prepare(Intent(y, revision: 2));
        Assert.False(node.ConsumerApplyIntents(21, prepareY));

        Assert.Equal(DurableCompletionAnswer.Refused, await node.Completion.AwaitAppliedAsync(Partition, [IntentEntry(prepareY)], [21], CancellationToken.None));
        Assert.Equal(w, node.Intents.Get(Key)!.TransactionId);
    }

    [Fact]
    public async Task ABundleWhosePreparesWereAllAdmitted_IsAcknowledged()
    {
        Node node = new();
        HLCTimestamp z = Ts(3_000);
        (byte[] initZ, _, long hash) = Bundle(z, Ts(3_050));
        byte[] prepareZ = Prepare(Intent(z, revision: 1) with { ManifestHash = hash });

        Assert.True(node.ConsumerApplyRecords(30, initZ));
        Assert.True(node.ConsumerApplyIntents(31, prepareZ));

        Assert.Equal(DurableCompletionAnswer.Acknowledged, await node.Completion.AwaitAppliedAsync(Partition, [RecordEntry(initZ), IntentEntry(prepareZ)], [30, 31], CancellationToken.None));
    }

    // ── the completion never applies ─────────────────────────────────────────────

    /// <summary>An entry the ordered apply never delivers here (the node stopped replicating the partition, or a
    /// snapshot install covered it): the completion waits out its bound and answers unobserved, which the producer
    /// treats as a clean retry. It does not apply the delta — the store stays exactly as the log left it.</summary>
    [Fact]
    public async Task AnEntryTheOrderedApplyNeverDelivers_TimesOutUnobserved_AndAppliesNothing()
    {
        Node node = new(waitTimeout: TimeSpan.FromMilliseconds(100));
        HLCTimestamp z = Ts(4_000);
        byte[] prepareZ = Prepare(Intent(z, revision: 1));

        long timeoutsBefore = DurableTransactionMetrics.OrderedApplyWaitTimeoutsCount;

        DurableCompletionAnswer answer = await node.Completion.AwaitAppliedAsync(Partition, [IntentEntry(prepareZ)], [50], CancellationToken.None);

        Assert.Equal(DurableCompletionAnswer.Unobserved, answer);
        Assert.Null(node.Intents.Get(Key));
        Assert.Equal(0, node.Intents.LiveIntentCount);
        Assert.Equal(timeoutsBefore + 1, DurableTransactionMetrics.OrderedApplyWaitTimeoutsCount);
    }

    [Fact]
    public async Task AnEntryWithoutALogIndex_IsUnobserved_AndAppliesNothing()
    {
        Node node = new(waitTimeout: TimeSpan.FromMilliseconds(100));
        HLCTimestamp z = Ts(5_000);
        byte[] prepareZ = Prepare(Intent(z, revision: 1));

        Assert.Equal(DurableCompletionAnswer.Unobserved, await node.Completion.AwaitAppliedAsync(Partition, [IntentEntry(prepareZ)], entryLogIndices: null, CancellationToken.None));
        Assert.Null(node.Intents.Get(Key));
    }

    [Fact]
    public async Task ACancelledCompletion_IsUnobserved_AndAppliesNothing()
    {
        Node node = new();
        HLCTimestamp z = Ts(6_000);
        byte[] prepareZ = Prepare(Intent(z, revision: 1));
        using CancellationTokenSource cts = new();

        Task<DurableCompletionAnswer> completion = node.Completion.AwaitAppliedAsync(Partition, [IntentEntry(prepareZ)], [60], cts.Token);
        cts.Cancel();

        Assert.Equal(DurableCompletionAnswer.Unobserved, await completion);
        Assert.Null(node.Intents.Get(Key));
    }

    // ── a stalled or demoted leader does not hold its producers ──────────────────

    /// <summary>
    /// The shape of a leader whose device stalls: its proposals are quorum-durable, its own ordered apply cannot
    /// advance, and Kommander steps it down within seconds. The completions parked on that apply must resolve the
    /// moment leadership moves away — as unobserved, the clean retry — not serve out the wait bound; the new
    /// leader applies and judges the same entries, and the re-drive is idempotent there. Nothing is applied here.
    /// </summary>
    [Fact]
    public async Task ParkedCompletions_AreReleasedWhenThisNodeStopsLeading_AndAnswerUnobserved()
    {
        Node node = new();
        HLCTimestamp z = Ts(7_000);
        (byte[] initZ, byte[] commitZ, long hash) = Bundle(z, Ts(7_050));
        byte[] prepareZ = Prepare(Intent(z, revision: 1) with { ManifestHash = hash });

        long releasedBefore = DurableTransactionMetrics.OrderedApplyWaitsReleasedOnLeadershipLossCount;

        Task<DurableCompletionAnswer> completion = node.Completion.AwaitAppliedAsync(
            Partition, [RecordEntry(initZ), IntentEntry(prepareZ), RecordEntry(commitZ)], [70, 71, 72], CancellationToken.None);

        await Task.Delay(50);
        Assert.False(completion.IsCompleted);

        Assert.Equal(1, node.Ledger.NoteLeadershipLost(Partition));

        Assert.Equal(DurableCompletionAnswer.Unobserved, await completion);
        Assert.Equal(releasedBefore + 1, DurableTransactionMetrics.OrderedApplyWaitsReleasedOnLeadershipLossCount);
        Assert.Null(node.Intents.Get(Key));
        Assert.Null(node.Records.Get(z, 1));

        // A completion that arrives after the loss does not park either.
        Assert.Equal(DurableCompletionAnswer.Unobserved,
            await node.Completion.AwaitAppliedAsync(Partition, [IntentEntry(prepareZ)], [71], CancellationToken.None));

        // Leading again: completions park and the ordered apply answers them as usual.
        node.Ledger.NoteLeadershipRegained(Partition);
        Task<DurableCompletionAnswer> again = node.Completion.AwaitAppliedAsync(Partition, [IntentEntry(prepareZ)], [71], CancellationToken.None);
        await Task.Delay(20);
        Assert.False(again.IsCompleted);
        Assert.True(node.ConsumerApplyIntents(71, prepareZ));
        Assert.Equal(DurableCompletionAnswer.Acknowledged, await again);
    }

    /// <summary>One bound covers the whole submission: three entries the ordered apply never delivers cost one
    /// bound, not three, before the producer is answered.</summary>
    [Fact]
    public async Task TheWaitBound_IsSharedAcrossASubmissionsEntries()
    {
        Node node = new(waitTimeout: TimeSpan.FromMilliseconds(300));
        HLCTimestamp z = Ts(8_000);
        (byte[] initZ, byte[] commitZ, long hash) = Bundle(z, Ts(8_050));
        byte[] prepareZ = Prepare(Intent(z, revision: 1) with { ManifestHash = hash });

        long start = Environment.TickCount64;
        DurableCompletionAnswer answer = await node.Completion.AwaitAppliedAsync(
            Partition, [RecordEntry(initZ), IntentEntry(prepareZ), RecordEntry(commitZ)], [80, 81, 82], CancellationToken.None);
        long elapsed = Environment.TickCount64 - start;

        Assert.Equal(DurableCompletionAnswer.Unobserved, answer);
        Assert.InRange(elapsed, 250, 800);
    }

    // ── the submission adapter ───────────────────────────────────────────────────

    [Fact]
    public async Task Submission_ResolvesTheProducer_FromTheAwaitedAdapter()
    {
        TaskCompletionSource<bool> producer = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource<DurableCompletionAnswer> ordered = new(TaskCreationOptions.RunContinuationsAsynchronously);

        DurableProposalSubmission submission = new(
            Partition, [IntentEntry([1, 2, 3])], producer, WriteAdmissionClass.Ordinary, WriteSubmissionStage.Prepare,
            (_, _, _) => ordered.Task);

        submission.Complete([77]);
        await Task.Delay(20);
        Assert.False(submission.Committed.IsCompleted);
        Assert.Equal(DurableCompletionAnswer.NotCommitted, submission.Answer);

        ordered.SetResult(DurableCompletionAnswer.Acknowledged);
        Assert.True(await submission.Committed);
        Assert.Equal(DurableCompletionAnswer.Acknowledged, submission.Answer);
        Assert.True(submission.BatchObserved);
    }

    [Fact]
    public async Task Submission_AnAdapterThatFaults_ResolvesTheProducerUnobserved()
    {
        TaskCompletionSource<bool> producer = new(TaskCreationOptions.RunContinuationsAsynchronously);

        DurableProposalSubmission submission = new(
            Partition, [IntentEntry([1, 2, 3])], producer, WriteAdmissionClass.Ordinary, WriteSubmissionStage.Prepare,
            (_, _, _) => Task.FromException<DurableCompletionAnswer>(new InvalidOperationException("boom")));

        submission.Complete([78]);
        Assert.False(await submission.Committed);
        Assert.Equal(DurableCompletionAnswer.Unobserved, submission.Answer);
        Assert.False(submission.BatchObserved);
    }

    [Fact]
    public async Task Submission_ARefusedPrepare_IsDurableButNotAcknowledged()
    {
        TaskCompletionSource<bool> producer = new(TaskCreationOptions.RunContinuationsAsynchronously);

        DurableProposalSubmission submission = new(
            Partition, [IntentEntry([1, 2, 3])], producer, WriteAdmissionClass.Ordinary, WriteSubmissionStage.Prepare,
            (_, _, _) => Task.FromResult(DurableCompletionAnswer.Refused));

        submission.Complete([79]);
        Assert.False(await submission.Committed);
        Assert.Equal(DurableCompletionAnswer.Refused, submission.Answer);
        Assert.True(submission.BatchObserved);
    }

    [Fact]
    public async Task Submission_Released_IsNotCommitted()
    {
        TaskCompletionSource<bool> producer = new(TaskCreationOptions.RunContinuationsAsynchronously);

        DurableProposalSubmission submission = new(
            Partition, [IntentEntry([1, 2, 3])], producer, WriteAdmissionClass.Ordinary, WriteSubmissionStage.Prepare,
            (_, _, _) => Task.FromResult(DurableCompletionAnswer.Acknowledged));

        submission.Release(transient: true);
        Assert.False(await submission.Committed);
        Assert.Equal(DurableCompletionAnswer.NotCommitted, submission.Answer);
        Assert.False(submission.BatchObserved);
    }
}
