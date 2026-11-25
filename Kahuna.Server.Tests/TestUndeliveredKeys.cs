using System.Text;

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
/// The marks of the keys that undelivered log entries of a node can change, and which keys each kind of submission
/// marks. A read of a key without a mark skips the wait for the applied frontier, so the tests ask above all that a
/// mark is there from before the proposal until the delivery, whatever the outcome of the proposal is.
/// </summary>
public sealed class TestUndeliveredKeys
{
    private const int Partition = 3;

    private sealed class Submission(string[] types, string[]? keys) : IProposalSubmission
    {
        public Submission(string type, params string[]? keys) : this([type], keys)
        {
        }

        public int PartitionId => Partition;
        public WriteAdmissionClass AdmissionClass => WriteAdmissionClass.Ordinary;
        public WriteSubmissionStage Stage => WriteSubmissionStage.Other;
        public int ByteLength => 1;
        public IReadOnlyList<RaftProposalEntry> Entries { get; } = [.. types.Select(static type => new RaftProposalEntry(type, [1], AutoCommit: true, ExpectedGeneration: 0))];
        public long EnqueueTicks { get; set; }
        public bool IsStale(IWriteRangeFence fence) => false;
        public bool IsQuiesced(IWriteRangeFence fence) => false;
        public int ChangedKeyCount => keys?.Length ?? -1;
        public string ChangedKey(int index) => keys![index];
        public void Complete(IReadOnlyList<long>? entryLogIndices) { }
        public void Release(bool transient) { }
    }

    private static Submission Writes(params string[] keys) => new(ReplicationTypes.KeyValues, keys);

    private static Submission Unnamed() => new([ReplicationTypes.KeyValues], null);

    /// <summary>One dispatched batch: its marks and the entries it handed to Raft.</summary>
    private sealed record Dispatched(UndeliveredKeys.Batch? Batch, RaftProposalEntry[] Entries)
    {
        /// <summary>The proposal answered: its entries are at consecutive indexes from <paramref name="firstIndex"/>.</summary>
        public void Commit(long firstIndex) => Answer((i, _) => new RaftEntryResult(RaftOperationStatus.Success, firstIndex + i, HLCTimestamp.Zero));

        public void Fail(RaftOperationStatus status) => Answer((_, _) => new RaftEntryResult(status, -1, HLCTimestamp.Zero));

        public void Throw() => UndeliveredKeys.Settle(Batch, Entries, null);

        public void Answer(Func<int, RaftProposalEntry, RaftEntryResult> result) =>
            UndeliveredKeys.Settle(Batch, Entries, new RaftBatchReplicationResult(
                true, RaftOperationStatus.Success, HLCTimestamp.Zero, [.. Entries.Select((entry, i) => result(i, entry))]));
    }

    private static Dispatched Dispatch(UndeliveredKeys marks, params IProposalSubmission[] submissions) =>
        new(marks.Begin(Partition, submissions), [.. submissions.SelectMany(submission => submission.Entries)]);

    // ── the life of a mark ──────────────────────────────────────────────────────

    [Fact]
    public void AKeyIsMarkedFromTheDispatchUntilTheDeliveryOfItsEntry()
    {
        UndeliveredKeys marks = new();
        Assert.False(marks.MayChange(Partition, "a"));

        Dispatched batch = Dispatch(marks, Writes("a"), Writes("b"));
        Assert.True(marks.MayChange(Partition, "a"));
        Assert.True(marks.MayChange(Partition, "b"));

        // Committed is not delivered.
        batch.Commit(firstIndex: 10);
        marks.Delivered(Partition, 9);
        Assert.True(marks.MayChange(Partition, "a"));
        Assert.True(marks.MayChange(Partition, "b"));

        // Each submission goes with its own entry.
        marks.Delivered(Partition, 10);
        Assert.False(marks.MayChange(Partition, "a"));
        Assert.True(marks.MayChange(Partition, "b"));

        marks.Delivered(Partition, 11);
        Assert.False(marks.MayChange(Partition, "b"));
    }

    [Fact]
    public void ASubmissionOfSeveralEntries_IsMarkedUntilItsLastEntryIsDelivered()
    {
        UndeliveredKeys marks = new();

        // The shape of a one-phase bundle: the decision comes after the prepare, and makes its values readable.
        Dispatched batch = Dispatch(
            marks,
            new Submission([ReplicationTypes.TransactionRecord, ReplicationTypes.PreparedIntent, ReplicationTypes.TransactionRecord], ["a", "b"]),
            new Submission([ReplicationTypes.TransactionRecord], []),
            Writes("c"));
        batch.Commit(firstIndex: 10);

        marks.Delivered(Partition, 11);
        Assert.True(marks.MayChange(Partition, "a"));
        Assert.True(marks.MayChange(Partition, "b"));

        marks.Delivered(Partition, 12);
        Assert.False(marks.MayChange(Partition, "a"));
        Assert.False(marks.MayChange(Partition, "b"));
        Assert.True(marks.MayChange(Partition, "c"));

        // Index 13 is the record between them, which marks nothing.
        marks.Delivered(Partition, 13);
        Assert.True(marks.MayChange(Partition, "c"));

        marks.Delivered(Partition, 14);
        Assert.False(marks.MayChange(Partition, "c"));
    }

    [Fact]
    public void ADeliveryThatPassesSeveralEntries_ReleasesAllOfThem()
    {
        UndeliveredKeys marks = new();

        Dispatched first = Dispatch(marks, Writes("a"), Writes("b"));
        Dispatched second = Dispatch(marks, Writes("c"), Writes("d"));
        first.Commit(firstIndex: 10);
        second.Commit(firstIndex: 12);

        marks.Delivered(Partition, 12);
        Assert.False(marks.MayChange(Partition, "a"));
        Assert.False(marks.MayChange(Partition, "b"));
        Assert.False(marks.MayChange(Partition, "c"));
        Assert.True(marks.MayChange(Partition, "d"));
    }

    [Fact]
    public void TheDeliveryAheadOfTheAnswer_ReleasesWhenTheAnswerArrives()
    {
        UndeliveredKeys marks = new();

        // The single-fsync commit delivers on the Raft executor while the answer of the proposal is on its way.
        Dispatched batch = Dispatch(marks, Writes("a"));
        marks.Delivered(Partition, 10);
        Assert.True(marks.MayChange(Partition, "a"));

        batch.Commit(firstIndex: 10);
        Assert.False(marks.MayChange(Partition, "a"));
    }

    [Fact]
    public void AKeyOfTwoBatches_StaysMarkedUntilBothAreDelivered()
    {
        UndeliveredKeys marks = new();

        Dispatched first = Dispatch(marks, Writes("a"));
        Dispatched second = Dispatch(marks, Writes("a"));

        first.Commit(firstIndex: 10);
        second.Commit(firstIndex: 11);

        marks.Delivered(Partition, 10);
        Assert.True(marks.MayChange(Partition, "a"));

        marks.Delivered(Partition, 11);
        Assert.False(marks.MayChange(Partition, "a"));
    }

    [Fact]
    public void AnswersOutOfOrder_ReleaseEachBatchAtItsOwnIndex()
    {
        UndeliveredKeys marks = new();

        Dispatched first = Dispatch(marks, Writes("a"));
        Dispatched second = Dispatch(marks, Writes("b"));

        second.Commit(firstIndex: 20);
        first.Commit(firstIndex: 10);

        marks.Delivered(Partition, 10);
        Assert.False(marks.MayChange(Partition, "a"));
        Assert.True(marks.MayChange(Partition, "b"));

        marks.Delivered(Partition, 20);
        Assert.False(marks.MayChange(Partition, "b"));
    }

    [Fact]
    public void ABatchWithoutAnAnswerYet_HoldsItsMarksAndThoseOfTheBatchesAfterIt()
    {
        UndeliveredKeys marks = new();

        Dispatched first = Dispatch(marks, Writes("a"));
        Dispatched second = Dispatch(marks, Writes("b"));

        // Nothing says where the entry of the first batch is, so no delivery can be taken for its own.
        second.Commit(firstIndex: 20);
        Assert.True(marks.MayChange(Partition, "a"));

        marks.Delivered(Partition, 10);
        marks.Delivered(Partition, 20);
        Assert.True(marks.MayChange(Partition, "a"));
        Assert.True(marks.MayChange(Partition, "b"));

        first.Commit(firstIndex: 10);
        Assert.False(marks.MayChange(Partition, "a"));
        Assert.False(marks.MayChange(Partition, "b"));
    }

    [Fact]
    public void PartitionsAreApart()
    {
        UndeliveredKeys marks = new();

        Dispatched batch = Dispatch(marks, Writes("a"));
        Assert.False(marks.MayChange(Partition + 1, "a"));

        batch.Commit(firstIndex: 10);
        marks.Delivered(Partition + 1, 100);
        Assert.True(marks.MayChange(Partition, "a"));
    }

    // ── idle partitions ─────────────────────────────────────────────────────────

    [Fact]
    public void AnIdlePartition_GivesItsTableUp_AndALaterBatchStartsOver()
    {
        UndeliveredKeys marks = new();

        Dispatch(marks, Writes("a")).Commit(firstIndex: 10);
        marks.Delivered(Partition, 10);

        // The first look only notes how far the partition got.
        marks.ReleaseIdle(static _ => true);
        Assert.Equal(1, marks.PartitionCount);
        marks.ReleaseIdle(static _ => true);
        Assert.Equal(0, marks.PartitionCount);

        Assert.False(marks.MayChange(Partition, "a"));
        marks.Delivered(Partition, 11);

        Dispatched later = Dispatch(marks, Writes("a"));
        Assert.True(marks.MayChange(Partition, "a"));
        later.Commit(firstIndex: 12);
        Assert.True(marks.MayChange(Partition, "a"));
        marks.Delivered(Partition, 12);
        Assert.False(marks.MayChange(Partition, "a"));
    }

    [Fact]
    public void APartitionWithABatchSinceTheLastLook_KeepsItsTable()
    {
        UndeliveredKeys marks = new();

        for (int i = 1; i <= 3; i++)
        {
            Dispatch(marks, Writes("a")).Commit(firstIndex: i);
            marks.Delivered(Partition, i);
            marks.ReleaseIdle(static _ => true);
            Assert.Equal(1, marks.PartitionCount);
        }
    }

    [Theory]
    [InlineData("no-answer")]
    [InlineData("not-delivered")]
    [InlineData("unknown")]
    public void AHostedPartitionThatMarksAnything_KeepsItsTable(string state)
    {
        UndeliveredKeys marks = new();
        Dispatched batch = Dispatch(marks, Writes("a"));

        if (state == "not-delivered")
            batch.Commit(firstIndex: 10);
        else if (state == "unknown")
            batch.Throw();

        for (int i = 0; i < 3; i++)
            marks.ReleaseIdle(static _ => true);

        Assert.True(marks.MayChange(Partition, "a"));
    }

    [Theory]
    [InlineData("no-answer", false)]
    [InlineData("not-delivered", true)]
    [InlineData("unknown", true)]
    public void APartitionThatIsNotHostedAnyMore_GivesItsTableUp_OnceNoProposalIsOut(string state, bool released)
    {
        UndeliveredKeys marks = new();
        Dispatched batch = Dispatch(marks, Writes("a"));

        if (state == "not-delivered")
            batch.Commit(firstIndex: 10);
        else if (state == "unknown")
            batch.Throw();

        // Noted while it is hosted, and not released by that.
        marks.ReleaseIdle(static _ => true);
        Assert.Equal(1, marks.PartitionCount);

        marks.ReleaseIdle(static partition => partition != Partition);
        Assert.Equal(released ? 0 : 1, marks.PartitionCount);

        // The answer can come after the table is gone.
        if (!released)
            batch.Throw();
    }

    [Fact]
    public async Task ABatchThatMeetsTheReleaseOfItsPartition_KeepsItsMarks()
    {
        UndeliveredKeys marks = new();
        using CancellationTokenSource stop = new();

        Task release = Task.Run(() =>
        {
            while (!stop.IsCancellationRequested)
                marks.ReleaseIdle(static _ => true);
        }, TestContext.Current.CancellationToken);

        int lost = 0;

        for (int i = 1; i <= 50_000; i++)
        {
            Dispatched batch = Dispatch(marks, Writes("a"));
            if (!marks.MayChange(Partition, "a"))
                lost++;

            batch.Commit(firstIndex: i);
            if (!marks.MayChange(Partition, "a"))
                lost++;

            marks.Delivered(Partition, i);
            if (marks.MayChange(Partition, "a"))
                lost++;
        }

        await stop.CancelAsync();
        await release;
        Assert.Equal(0, lost);
    }

    [Fact]
    public void AnEntryOfAnotherSubsystem_DoesNotHoldTheMarks()
    {
        UndeliveredKeys marks = new();

        // The key-value apply sees none of the lock entries, and the first and the last entry are such.
        Dispatched batch = Dispatch(
            marks, new Submission(ReplicationTypes.Locks, []), Writes("a"), new Submission([ReplicationTypes.KeyValues, ReplicationTypes.Locks], ["b"]));
        batch.Commit(firstIndex: 10);

        marks.Delivered(Partition, 11);
        Assert.False(marks.MayChange(Partition, "a"));
        Assert.True(marks.MayChange(Partition, "b"));

        marks.Delivered(Partition, 12);
        Assert.False(marks.MayChange(Partition, "b"));
    }

    [Fact]
    public void ManyKeys_AreAllMarkedAndAllReleased()
    {
        UndeliveredKeys marks = new();
        string[] keys = [.. Enumerable.Range(0, 5_000).Select(i => "space/key-" + i)];

        Dispatched batch = Dispatch(marks, Writes(keys));
        Assert.All(keys, key => Assert.True(marks.MayChange(Partition, key)));

        batch.Commit(firstIndex: 10);
        marks.Delivered(Partition, 10);
        Assert.All(keys, key => Assert.False(marks.MayChange(Partition, key)));
    }

    [Fact]
    public void FewMarksRarelyCoverOtherKeys()
    {
        UndeliveredKeys marks = new();
        Dispatch(marks, Writes([.. Enumerable.Range(0, 64).Select(i => "space/written-" + i)]));

        int covered = Enumerable.Range(0, 20_000).Count(i => marks.MayChange(Partition, "space/other-" + i));

        // 128 of 4,096 counters are covered, so a key has both of its counters covered about once in a thousand.
        Assert.InRange(covered, 0, 400);
    }

    // ── doubt ───────────────────────────────────────────────────────────────────

    [Fact]
    public void ASubmissionThatDoesNotNameItsKeys_MarksThePartition()
    {
        UndeliveredKeys marks = new();

        Dispatched batch = Dispatch(marks, Writes("a"), Unnamed());
        Assert.True(marks.MayChange(Partition, "any other key"));

        batch.Commit(firstIndex: 10);
        marks.Delivered(Partition, 10);
        Assert.True(marks.MayChange(Partition, "any other key"));

        marks.Delivered(Partition, 11);
        Assert.False(marks.MayChange(Partition, "any other key"));
        Assert.False(marks.MayChange(Partition, "a"));
    }

    public static TheoryData<string> Failures => ["throw", "timeout", "not-leader", "unknown", "short", "no-index", "one-without-index", "one-entry"];

    [Theory]
    [MemberData(nameof(Failures))]
    public void AnOutcomeThatIsNotAPlainSuccess_KeepsThePartitionMarked(string failure)
    {
        UndeliveredKeys marks = new();

        Dispatched batch = Dispatch(marks, Writes("a"), Writes("b"));

        switch (failure)
        {
            case "throw":
                batch.Throw();
                break;
            case "timeout":
                batch.Fail(RaftOperationStatus.ProposalTimeout);
                break;
            case "not-leader":
                batch.Fail(RaftOperationStatus.NodeIsNotLeader);
                break;
            case "unknown":
                batch.Fail(RaftOperationStatus.ProposalOutcomeUnknown);
                break;
            case "short":
                UndeliveredKeys.Settle(batch.Batch, batch.Entries, new RaftBatchReplicationResult(
                    true, RaftOperationStatus.Success, HLCTimestamp.Zero, [new RaftEntryResult(RaftOperationStatus.Success, 10, HLCTimestamp.Zero)]));
                break;
            case "no-index":
                batch.Answer((_, _) => new RaftEntryResult(RaftOperationStatus.Success, -1, HLCTimestamp.Zero));
                break;
            case "one-without-index":
                batch.Answer((i, _) => new RaftEntryResult(RaftOperationStatus.Success, i == 0 ? 10 : -1, HLCTimestamp.Zero));
                break;
            default:
                batch.Answer((i, _) => new RaftEntryResult(i == 0 ? RaftOperationStatus.Success : RaftOperationStatus.PartitionMoved, 10 + i, HLCTimestamp.Zero));
                break;
        }

        Assert.True(marks.MayChange(Partition, "a"));
        Assert.True(marks.MayChange(Partition, "any other key"));

        // Deliveries of entries that this node did not dispatch after the failure prove nothing about it.
        marks.Delivered(Partition, 1_000);
        Assert.True(marks.MayChange(Partition, "a"));

        // The delivery of an entry of a batch that was dispatched after the failure was known does, whatever is left of
        // that batch.
        Dispatched later = Dispatch(marks, Writes("c"), Writes("d"));
        later.Commit(firstIndex: 1_001);
        Assert.True(marks.MayChange(Partition, "any other key"));

        marks.Delivered(Partition, 1_001);
        Assert.False(marks.MayChange(Partition, "a"));
        Assert.False(marks.MayChange(Partition, "any other key"));
        Assert.True(marks.MayChange(Partition, "d"));
    }

    [Fact]
    public void ABatchDispatchedBeforeTheFailureWasKnown_DoesNotClearIt()
    {
        UndeliveredKeys marks = new();

        // Two batches in flight together: the second one can be below the first one in the log.
        Dispatched failed = Dispatch(marks, Writes("a"));
        Dispatched sibling = Dispatch(marks, Writes("b"));

        failed.Throw();
        sibling.Commit(firstIndex: 10);
        marks.Delivered(Partition, 10);
        Assert.True(marks.MayChange(Partition, "a"));

        Dispatched later = Dispatch(marks, Writes("c"));
        later.Commit(firstIndex: 11);
        marks.Delivered(Partition, 11);
        Assert.False(marks.MayChange(Partition, "a"));
    }

    [Fact]
    public void ABatchThatChangesNothing_ClearsAnEarlierFailureWithItsDelivery()
    {
        UndeliveredKeys marks = new();
        Dispatch(marks, Writes("a")).Throw();

        // The decision of a transaction marks no key, and its delivery proves the same.
        Dispatched decision = Dispatch(marks, new Submission(ReplicationTypes.TransactionRecord, []));
        Assert.NotNull(decision.Batch);
        decision.Commit(firstIndex: 10);
        Assert.True(marks.MayChange(Partition, "a"));

        marks.Delivered(Partition, 10);
        Assert.False(marks.MayChange(Partition, "a"));
    }

    [Fact]
    public void TheFailureOfABatchThatMarkedNothing_LeavesNothingInDoubt()
    {
        UndeliveredKeys marks = new();
        Dispatch(marks, Writes("a")).Throw();

        // Dispatched after the failure was known, so its delivery clears it, whatever becomes of the batch between them.
        Dispatched decision = Dispatch(marks, new Submission(ReplicationTypes.TransactionRecord, []));
        Dispatched later = Dispatch(marks, Writes("b"));
        decision.Throw();
        Assert.True(marks.MayChange(Partition, "a"));

        later.Commit(firstIndex: 10);
        marks.Delivered(Partition, 10);
        Assert.False(marks.MayChange(Partition, "a"));
    }

    [Fact]
    public void ABatchOfAnotherSubsystem_DoesNotClearAnEarlierFailure()
    {
        UndeliveredKeys marks = new();
        Dispatch(marks, Writes("a")).Throw();

        // Nothing reports the delivery of a lock entry here.
        Dispatched locks = Dispatch(marks, new Submission(ReplicationTypes.Locks, []));
        locks.Commit(firstIndex: 10);
        marks.Delivered(Partition, 50);

        Assert.True(marks.MayChange(Partition, "a"));
    }

    [Fact]
    public void ABatchThatChangesNothing_HasNoMarks()
    {
        UndeliveredKeys marks = new();

        Dispatched batch = Dispatch(marks, new Submission(ReplicationTypes.TransactionRecord, []));
        Assert.Null(batch.Batch);

        // Its outcome does not matter either.
        batch.Throw();
        Assert.False(marks.MayChange(Partition, "a"));
    }

    [Fact]
    public async Task TheAnswerAndTheDeliveryRace_AndNoMarkIsLost()
    {
        UndeliveredKeys marks = new();
        string[] keys = ["a", "b", "c", "d"];

        for (long index = 1; index <= 20_000; index++)
        {
            Dispatched batch = Dispatch(marks, Writes(keys[index % keys.Length]));
            long at = index;

            Task delivery = Task.Run(() => marks.Delivered(Partition, at), TestContext.Current.CancellationToken);
            batch.Commit(firstIndex: at);
            await delivery;

            // Released by whichever of the two came last. A lost wake would leave the mark in place.
            Assert.False(marks.MayChange(Partition, keys[index % keys.Length]));
        }
    }

    [Fact]
    public async Task AReaderNeverFindsAKeyOfAFailedBatchUnmarked()
    {
        UndeliveredKeys marks = new();
        bool stop = false;
        long missed = 0;

        // The batch below is always either in flight or ended with an unknown outcome, so the key is marked throughout.
        Dispatch(marks, Writes("seed")).Throw();

        Task[] readers =
        [
            .. Enumerable.Range(0, 3).Select(_ => Task.Run(() =>
            {
                while (!Volatile.Read(ref stop))
                {
                    if (!marks.MayChange(Partition, "a"))
                        Interlocked.Increment(ref missed);
                }
            }, TestContext.Current.CancellationToken))
        ];

        for (int i = 0; i < 50_000; i++)
            Dispatch(marks, Writes("a")).Throw();

        Volatile.Write(ref stop, true);
        await Task.WhenAll(readers);

        Assert.Equal(0, missed);
    }

    [Fact]
    public async Task AReaderFindsTheMarkWhenTheUnknownOutcomeReplacesIt()
    {
        // Each round starts with a partition that has neither, so the reader sees the hand-over from one to the other.
        for (int round = 0; round < 5_000; round++)
        {
            UndeliveredKeys marks = new();
            Dispatched batch = Dispatch(marks, Writes("a"));
            using Barrier barrier = new(2);

            Task<bool> reader = Task.Run(() =>
            {
                barrier.SignalAndWait();
                bool marked = true;
                for (int i = 0; i < 50; i++)
                    marked &= marks.MayChange(Partition, "a");

                return marked;
            }, TestContext.Current.CancellationToken);

            barrier.SignalAndWait(TestContext.Current.CancellationToken);
            batch.Throw();

            Assert.True(await reader, $"the reader found the key unmarked in round {round}");
        }
    }

    // ── what each submission marks ──────────────────────────────────────────────

    private static PreparedIntent Intent(string key, HLCTimestamp transactionId) => new(
        transactionId, Epoch: 1, key, ManifestHash: 1, RecordAnchorKey: key, CommitTimestamp: new HLCTimestamp(1, 100, 0),
        KeyValueState.Set, Encoding.UTF8.GetBytes("value"), Bucket: null, Revision: 1, Expires: HLCTimestamp.Zero, NoRevision: false,
        BaseRevision: PreparedIntent.UnknownBaseRevision, BaseState: KeyValueState.Undefined,
        RecoveryDeadline: new HLCTimestamp(1, 1_000, 0), PreparedIntentResolution.Pending);

    private static readonly HLCTimestamp Transaction = new(1, 10, 0);

    private static string[]? KeysOf(WriteSubmissionStage stage, params (string Type, byte[] Data)[] entries)
    {
        DurableProposalSubmission submission = new(
            Partition,
            [.. entries.Select(entry => new RaftProposalEntry(entry.Type, entry.Data, AutoCommit: true, ExpectedGeneration: 0))],
            new TaskCompletionSource<bool>(),
            WriteAdmissionClass.Ordinary,
            stage);

        return submission.ChangedKeyCount < 0
            ? null
            : [.. Enumerable.Range(0, submission.ChangedKeyCount).Select(submission.ChangedKey)];
    }

    private static byte[] Prepare(params string[] keys) =>
        PreparedIntentStore.SerializeDelta([.. keys.Select(key => new PrepareIntentCommand(Intent(key, Transaction), 0))]);

    private static byte[] Settle(string key, bool materialize) =>
        PreparedIntentStore.SerializeDelta(DurableTransactionFinalizer.BuildSettleCommands([Intent(key, Transaction)], commit: true, materialize));

    private static readonly byte[] Record = [1, 2, 3];

    [Theory]
    [InlineData(WriteSubmissionStage.Prepare)]
    [InlineData(WriteSubmissionStage.RePrepare)]
    [InlineData(WriteSubmissionStage.OnePhase)]
    [InlineData(WriteSubmissionStage.Other)]
    public void APrepareMarksItsKeys_WhateverTheStageSays(WriteSubmissionStage stage)
    {
        Assert.Equal(["a", "b"], KeysOf(stage, (ReplicationTypes.PreparedIntent, Prepare("a", "b")))!);
    }

    [Fact]
    public void AForwardedPrepare_IsReadFromItsBytes_AndTheApplyReusesTheDecodedCommands()
    {
        byte[] forwarded = [.. Prepare("a", "b")];

        Assert.Equal(["a", "b"], KeysOf(WriteSubmissionStage.Prepare, (ReplicationTypes.PreparedIntent, forwarded))!);

        // The commands decoded for the marks are what the apply of these bytes gets.
        PreparedIntentStore store = new();
        Assert.True(store.ApplyDeltaAckPrepares(Partition, new RaftLog { Id = 1, LogType = ReplicationTypes.PreparedIntent, LogData = forwarded }));
        Assert.NotNull(store.Get("a"));
        Assert.NotNull(store.Get("b"));
    }

    [Fact]
    public void AOnePhaseBundle_MarksTheKeysItSettlesAndTheKeysItPrepares()
    {
        Assert.Equal(["a", "b", "c"], KeysOf(
            WriteSubmissionStage.OnePhase,
            (ReplicationTypes.PreparedIntent, Settle("c", materialize: true)),
            (ReplicationTypes.TransactionRecord, Record),
            (ReplicationTypes.PreparedIntent, Prepare("a", "b")),
            (ReplicationTypes.TransactionRecord, Record))!.Order(StringComparer.Ordinal));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void ASettlementMarksEachOfItsKeysOnce(bool materialize)
    {
        byte[] settle = PreparedIntentStore.SerializeDelta(DurableTransactionFinalizer.BuildSettleCommands(
            [Intent("a", Transaction), Intent("b", Transaction)], commit: true, materialize));

        Assert.Equal(["a", "b"], KeysOf(WriteSubmissionStage.Settle, (ReplicationTypes.PreparedIntent, settle))!);
    }

    [Fact]
    public void RecordsAndMaterializations_MarkNothing()
    {
        Assert.Empty(KeysOf(WriteSubmissionStage.RecordInit, (ReplicationTypes.TransactionRecord, Record))!);
        Assert.Empty(KeysOf(WriteSubmissionStage.Decision, (ReplicationTypes.TransactionRecord, Record))!);
        Assert.Empty(KeysOf(WriteSubmissionStage.Materialize, (ReplicationTypes.KeyValues, Record))!);
        Assert.Empty(KeysOf(WriteSubmissionStage.Other, (ReplicationTypes.ProgressTick, []))!);
        Assert.Empty(KeysOf(WriteSubmissionStage.Other, (ReplicationTypes.CompletionReceipt, Record))!);
    }

    [Fact]
    public void WhatDoesNotTellItsKeys_MarksThePartition()
    {
        // A copied page of a range move, an entry this version does not know, and bytes that are not a delta.
        Assert.Null(KeysOf(WriteSubmissionStage.Other, (ReplicationTypes.KeyValues, Record)));
        Assert.Null(KeysOf(WriteSubmissionStage.Other, ("some later type", Record)));
        Assert.Null(KeysOf(WriteSubmissionStage.Prepare, (ReplicationTypes.PreparedIntent, [0xff, 0xff, 0xff])));
        Assert.Null(KeysOf(WriteSubmissionStage.Prepare, (ReplicationTypes.PreparedIntent, Prepare("a")), (ReplicationTypes.KeyValues, Record)));
    }
}
