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
/// The merge of adjacent settles into one log entry: which submissions a run takes, the shape and the identity of
/// each command of the merged entry, the answer that the merged submission gives to each settle it carries, and the
/// stores that a schedule of merged entries leaves, which must equal those of the same schedule with an entry for each
/// settle.
/// </summary>
public sealed class TestSettleRunMerger
{
    private const int PartitionId = 3;

    private const long Term = 7;

    private static HLCTimestamp Ts(long physical) => new(0, physical, 0);

    private static PreparedIntent Intent(long transaction, string key, long revision) =>
        new(
            TransactionId: Ts(transaction), Epoch: 1, Key: key, ManifestHash: 42, RecordAnchorKey: "anchor",
            CommitTimestamp: Ts(transaction + 100_000),
            State: KeyValueState.Set, Value: [(byte)transaction, (byte)revision], Bucket: null, Revision: revision, Expires: HLCTimestamp.Zero,
            NoRevision: false, BaseRevision: PreparedIntent.UnknownBaseRevision, BaseState: KeyValueState.Undefined,
            RecoveryDeadline: Ts(transaction + 500_000), Resolution: PreparedIntentResolution.Pending);

    private static byte[] SettleDelta(bool commit, params PreparedIntent[] intents) =>
        PreparedIntentStore.SerializeDelta(DurableTransactionFinalizer.BuildSettleCommands(intents, commit, materializeOnResolve: commit));

    private static DurableProposalSubmission Submission(
        byte[] data,
        WriteSubmissionStage stage = WriteSubmissionStage.Settle,
        long term = Term,
        long generation = 0,
        IReadOnlyList<RangeFence>? fences = null,
        bool autoCommit = true,
        int entries = 1,
        string logType = ReplicationTypes.PreparedIntent) =>
        new(
            PartitionId,
            [.. Enumerable.Range(0, entries).Select(_ => new RaftProposalEntry(logType, data, AutoCommit: autoCommit, ExpectedGeneration: generation, ExpectedTerm: term))],
            new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously),
            WriteAdmissionClass.Terminal,
            stage,
            fences: fences);

    private static DurableProposalSubmission Settle(long transaction, string key, bool commit = true, long term = Term) =>
        Submission(SettleDelta(commit, Intent(transaction, key, 1)), term: term);

    private static PreparedIntentCommand[] CommandsOf(IProposalSubmission submission) =>
        PreparedIntentStore.DecodeDelta([.. submission.Entries[0].Data]);

    // ── which submissions merge ─────────────────────────────────────────────────

    [Fact]
    public void AdjacentSettles_BecomeOneEntry_WithEveryCommandInQueueOrder()
    {
        List<IProposalSubmission> batch = [Settle(1, "a"), Settle(2, "b"), Settle(3, "c")];

        List<IProposalSubmission> merged = SettleRunMerger.Merge(batch, 512, awaiter: null);

        MergedSettleSubmission entry = Assert.IsType<MergedSettleSubmission>(Assert.Single(merged));
        Assert.Equal(batch, entry.Members);
        Assert.Single(entry.Entries);
        Assert.Equal(ReplicationTypes.PreparedIntent, entry.Entries[0].Type);
        Assert.Equal(Term, entry.Entries[0].ExpectedTerm);
        Assert.Equal(0, entry.Entries[0].ExpectedGeneration);
        Assert.True(entry.Entries[0].AutoCommit);
        Assert.Equal(WriteSubmissionStage.Settle, entry.Stage);
        Assert.Equal(WriteAdmissionClass.Terminal, entry.AdmissionClass);
        Assert.Equal(entry.Entries[0].Data.Length, entry.ByteLength);

        // The commands of the members, in queue order, each with the identity of its own transaction.
        PreparedIntentCommand[] expected = [.. batch.SelectMany(CommandsOf)];
        Assert.Equal(expected, CommandsOf(entry));

        // The keys of every member wait for the delivery of the merged entry.
        Assert.Equal(["a", "b", "c"], Enumerable.Range(0, entry.ChangedKeyCount).Select(entry.ChangedKey));
    }

    [Fact]
    public void AMergedEntry_RemovesTheRegistrationsOfItsMembers_AndARunOfOneKeepsItsOwn()
    {
        PreparedIntentCommand[] first = DurableTransactionFinalizer.BuildSettleCommands([Intent(1, "a", 1)], commit: true, materializeOnResolve: true);
        PreparedIntentCommand[] second = DurableTransactionFinalizer.BuildSettleCommands([Intent(2, "b", 1)], commit: true, materializeOnResolve: true);
        PreparedIntentCommand[] alone = DurableTransactionFinalizer.BuildSettleCommands([Intent(3, "c", 1)], commit: true, materializeOnResolve: true);

        DurableProposalSubmission s1 = Submission(PreparedIntentStore.SerializeDelta(first));
        DurableProposalSubmission s2 = Submission(PreparedIntentStore.SerializeDelta(second));
        DurableProposalSubmission record = Submission([1, 2, 3], WriteSubmissionStage.Decision);
        DurableProposalSubmission s3 = Submission(PreparedIntentStore.SerializeDelta(alone));

        List<IProposalSubmission> merged = SettleRunMerger.Merge([s1, s2, record, s3], 512, awaiter: null);

        Assert.IsType<MergedSettleSubmission>(merged[0]);
        Assert.Same(s3, merged[2]);

        // The merged entry goes to the log in place of the two members: their bytes have no registration, and the
        // commands of a member that is sent again are decoded.
        Assert.True(PreparedIntentStore.TryGetProposedCommands(s1.Entries[0].Data, out PreparedIntentCommand[]? decodedFirst));
        Assert.NotSame(first, decodedFirst);
        Assert.Equal(first.Length, decodedFirst.Length);
        Assert.True(PreparedIntentStore.TryGetProposedCommands(s2.Entries[0].Data, out PreparedIntentCommand[]? decodedSecond));
        Assert.NotSame(second, decodedSecond);

        // A run of one member goes to the log with its own bytes, and its apply reuses its commands.
        Assert.True(PreparedIntentStore.TryGetProposedCommands(s3.Entries[0].Data, out PreparedIntentCommand[]? kept));
        Assert.Same(alone, kept);
    }

    [Fact]
    public void OtherSubmissions_KeepTheirPlace_AndARunOfOneKeepsItsOwnEntry()
    {
        DurableProposalSubmission s1 = Settle(1, "a"), s2 = Settle(2, "b"), s3 = Settle(3, "c"), s4 = Settle(4, "d");
        DurableProposalSubmission prepare = Submission(PreparedIntentStore.SerializeDelta([new PrepareIntentCommand(Intent(9, "z", 1))]), WriteSubmissionStage.OnePhase);
        DurableProposalSubmission record = Submission([1, 2, 3], WriteSubmissionStage.Decision);

        List<IProposalSubmission> merged = SettleRunMerger.Merge([s1, prepare, s2, s3, record, s4], 512, awaiter: null);

        Assert.Equal(5, merged.Count);
        Assert.Same(s1, merged[0]);
        Assert.Same(prepare, merged[1]);
        Assert.Equal([s2, s3], Assert.IsType<MergedSettleSubmission>(merged[2]).Members);
        Assert.Same(record, merged[3]);
        Assert.Same(s4, merged[4]);
    }

    [Fact]
    public void NoRunOfTwo_ReturnsTheSameBatch()
    {
        List<IProposalSubmission> batch = [Settle(1, "a"), Submission([1], WriteSubmissionStage.Decision), Settle(2, "b")];
        Assert.Same(batch, SettleRunMerger.Merge(batch, 512, awaiter: null));
    }

    [Fact]
    public void ASettleOfAnotherTerm_EndsTheRun_AndStartsItsOwn()
    {
        DurableProposalSubmission s1 = Settle(1, "a"), s2 = Settle(2, "b");
        DurableProposalSubmission s3 = Settle(3, "c", term: Term + 1), s4 = Settle(4, "d", term: Term + 1);

        List<IProposalSubmission> merged = SettleRunMerger.Merge([s1, s2, s3, s4], 512, awaiter: null);

        Assert.Equal(2, merged.Count);
        Assert.Equal([s1, s2], Assert.IsType<MergedSettleSubmission>(merged[0]).Members);
        Assert.Equal(Term, merged[0].Entries[0].ExpectedTerm);
        Assert.Equal([s3, s4], Assert.IsType<MergedSettleSubmission>(merged[1]).Members);
        Assert.Equal(Term + 1, merged[1].Entries[0].ExpectedTerm);
    }

    public static TheoryData<string> Unmergeable => ["fence", "generation", "manual", "two_entries", "stage", "type", "prepare", "seed", "empty"];

    [Theory]
    [MemberData(nameof(Unmergeable))]
    public void ASubmissionThatFailsACondition_KeepsItsOwnEntry_AndEndsTheRun(string shape)
    {
        byte[] settle = SettleDelta(true, Intent(5, "x", 1));
        DurableProposalSubmission odd = shape switch
        {
            "fence" => Submission(settle, fences: [new RangeFence("x", 3)]),
            "generation" => Submission(settle, generation: 4),
            "manual" => Submission(settle, autoCommit: false),
            "two_entries" => Submission(settle, entries: 2),
            "stage" => Submission(settle, WriteSubmissionStage.Other),
            "type" => Submission(settle, logType: ReplicationTypes.KeyValues),
            "prepare" => Submission(PreparedIntentStore.SerializeDelta([new PrepareIntentCommand(Intent(5, "x", 1))])),
            "seed" => Submission(PreparedIntentStore.SerializeDelta([new SeedCommittedHeadCommand("x", 1, KeyValueState.Set, Ts(9))])),
            _ => Submission([])
        };

        DurableProposalSubmission s1 = Settle(1, "a"), s2 = Settle(2, "b");

        List<IProposalSubmission> merged = SettleRunMerger.Merge([s1, odd, s2], 512, awaiter: null);
        Assert.Equal([s1, odd, s2], merged);

        merged = SettleRunMerger.Merge([s1, s2, odd], 512, awaiter: null);
        Assert.Equal(2, merged.Count);
        Assert.Equal([s1, s2], Assert.IsType<MergedSettleSubmission>(merged[0]).Members);
        Assert.Same(odd, merged[1]);
    }

    [Fact]
    public void ARunAboveTheCap_ContinuesInASecondEntry()
    {
        // Each settle of one key has two commands: a resolve and a remove.
        DurableProposalSubmission s1 = Settle(1, "a"), s2 = Settle(2, "b"), s3 = Settle(3, "c"), s4 = Settle(4, "d"), s5 = Settle(5, "e");

        List<IProposalSubmission> merged = SettleRunMerger.Merge([s1, s2, s3, s4, s5], maxCommands: 4, awaiter: null);

        Assert.Equal(3, merged.Count);
        Assert.Equal([s1, s2], Assert.IsType<MergedSettleSubmission>(merged[0]).Members);
        Assert.Equal([s3, s4], Assert.IsType<MergedSettleSubmission>(merged[1]).Members);
        Assert.Same(s5, merged[2]);
        Assert.Equal(4, CommandsOf(merged[0]).Length);
    }

    [Fact]
    public void ASettleOfSeveralTransactions_IsAMemberLikeAnyOther()
    {
        // The carried settle of a predecessor already holds the commands of several transactions.
        DurableProposalSubmission carried = Submission(PreparedIntentStore.SerializeDelta([
            .. DurableTransactionFinalizer.BuildSettleCommands([Intent(1, "a", 1)], commit: true, materializeOnResolve: true),
            .. DurableTransactionFinalizer.BuildSettleCommands([Intent(2, "b", 1)], commit: true, materializeOnResolve: true)]));
        DurableProposalSubmission s3 = Settle(3, "c");

        MergedSettleSubmission entry = Assert.IsType<MergedSettleSubmission>(Assert.Single(SettleRunMerger.Merge([carried, s3], 512, awaiter: null)));
        Assert.Equal(6, CommandsOf(entry).Length);
    }

    // ── the codec of a merged entry ─────────────────────────────────────────────

    [Fact]
    public void AMergedEntry_DecodesEachCommandWithItsOwnTransactionEpochAndCommitTimestamp()
    {
        PreparedIntent first = Intent(1, "a", 1);
        PreparedIntent second = Intent(2, "b", 1) with { Epoch = 3 };

        MergedSettleSubmission entry = Assert.IsType<MergedSettleSubmission>(Assert.Single(SettleRunMerger.Merge(
            [Submission(SettleDelta(true, first)), Submission(SettleDelta(false, second))], 512, awaiter: null)));

        PreparedIntentCommand[] commands = CommandsOf(entry);

        Assert.Equal(
            [
                new ResolveIntentCommand(first.TransactionId, 1, "a", Commit: true, MaterializeOnResolve: true, first.CommitTimestamp),
                new RemoveIntentCommand(first.TransactionId, 1, "a"),
                new ResolveIntentCommand(second.TransactionId, 3, "b", Commit: false),
                new RemoveIntentCommand(second.TransactionId, 3, "b")
            ],
            commands);
    }

    // ── the answer of a merged entry ────────────────────────────────────────────

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task TheAnswerOfTheEntry_GoesToEveryMember_AfterOneWait(bool committed)
    {
        DurableCompletionAnswer answer = committed ? DurableCompletionAnswer.Acknowledged : DurableCompletionAnswer.Unobserved;
        DurableProposalSubmission s1 = Settle(1, "a"), s2 = Settle(2, "b"), s3 = Settle(3, "c");
        TaskCompletionSource<DurableCompletionAnswer> applied = new(TaskCreationOptions.RunContinuationsAsynchronously);
        List<IReadOnlyList<long>?> waits = [];

        MergedSettleSubmission entry = Assert.IsType<MergedSettleSubmission>(Assert.Single(SettleRunMerger.Merge(
            [s1, s2, s3], 512,
            (partition, entries, indices) =>
            {
                Assert.Equal(PartitionId, partition);
                Assert.Single(entries);
                waits.Add(indices);
                return applied.Task;
            })));

        entry.Complete([42]);
        Assert.False(s1.Committed.IsCompleted);

        applied.SetResult(answer);

        foreach (DurableProposalSubmission member in new[] { s1, s2, s3 })
        {
            Assert.Equal(committed, await member.Committed.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken));
            Assert.Equal(answer, member.Answer);
        }

        Assert.Equal([42L], Assert.Single(waits));
    }

    [Fact]
    public async Task AWaitThatFaults_AnswersEveryMemberUnobserved()
    {
        DurableProposalSubmission s1 = Settle(1, "a"), s2 = Settle(2, "b");

        MergedSettleSubmission entry = Assert.IsType<MergedSettleSubmission>(Assert.Single(SettleRunMerger.Merge(
            [s1, s2], 512, (_, _, _) => Task.FromException<DurableCompletionAnswer>(new InvalidOperationException("apply wait failed")))));

        entry.Complete([42]);

        Assert.False(await s1.Committed.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken));
        Assert.False(await s2.Committed.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken));
        Assert.Equal(DurableCompletionAnswer.Unobserved, s1.Answer);
        Assert.Equal(DurableCompletionAnswer.Unobserved, s2.Answer);
    }

    [Fact]
    public async Task AWaitThatThrows_AnswersEveryMemberUnobserved()
    {
        DurableProposalSubmission s1 = Settle(1, "a"), s2 = Settle(2, "b");

        MergedSettleSubmission entry = Assert.IsType<MergedSettleSubmission>(Assert.Single(SettleRunMerger.Merge(
            [s1, s2], 512, (_, _, _) => throw new InvalidOperationException("the wait could not start"))));

        entry.Complete([42]);

        Assert.False(await s1.Committed.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken));
        Assert.False(await s2.Committed.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken));
        Assert.Equal(DurableCompletionAnswer.Unobserved, s1.Answer);
        Assert.Equal(DurableCompletionAnswer.Unobserved, s2.Answer);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task AReleasedEntry_ReleasesEveryMember(bool transient)
    {
        DurableProposalSubmission s1 = Settle(1, "a"), s2 = Settle(2, "b");
        bool waited = false;

        MergedSettleSubmission entry = Assert.IsType<MergedSettleSubmission>(Assert.Single(SettleRunMerger.Merge(
            [s1, s2], 512, (_, _, _) => { waited = true; return Task.FromResult(DurableCompletionAnswer.Acknowledged); })));

        entry.Release(transient);

        Assert.False(await s1.Committed.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken));
        Assert.False(await s2.Committed.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken));
        Assert.Equal(DurableCompletionAnswer.NotCommitted, s1.Answer);
        Assert.Equal(DurableCompletionAnswer.NotCommitted, s2.Answer);
        Assert.False(waited);
    }

    // ── the stores a merged schedule leaves ─────────────────────────────────────

    private sealed class RecordingInstaller : IResolvedIntentInstaller
    {
        public List<(HLCTimestamp TransactionId, long Epoch, string Key, long Revision)> Installs { get; } = [];

        public List<long> Completed { get; } = [];

        public void Install(int partitionId, long logIndex, PreparedIntent intent, bool replay) =>
            Installs.Add((intent.TransactionId, intent.Epoch, intent.Key, intent.Revision));

        public void CompleteEntry(int partitionId, long logIndex, bool replay) => Completed.Add(logIndex);

        public void NoteUnresolvedOnReplay(int partitionId, long logIndex, HLCTimestamp transactionId, long epoch, string key, HLCTimestamp commitTimestamp) { }

        public void NoteUnresolvedOnApply(int partitionId, long logIndex, HLCTimestamp transactionId, long epoch, string key, HLCTimestamp commitTimestamp) { }
    }

    private static RaftLog Log(long id, long timeMs, byte[] data) => new()
    {
        Id = id,
        Type = RaftLogType.Committed,
        Time = Ts(timeMs),
        LogType = ReplicationTypes.PreparedIntent,
        // A copy, so the apply decodes the bytes as a node that did not propose them.
        LogData = [.. data]
    };

    /// <summary>
    /// One random schedule of prepares and groups of adjacent settles: commits, aborts, a settle sent two times, and a
    /// settle of a transaction whose prepare the store refused. One store applies each settle as an entry of its own,
    /// the other applies each group as the entry that the merger builds for it. Every entry of a group has the time of
    /// its batch, as the entries of one proposal do.
    /// </summary>
    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    [InlineData(5)]
    [InlineData(6)]
    [InlineData(7)]
    [InlineData(8)]
    public void ARandomSchedule_LeavesEqualStores_AsSeparateAndAsMergedSettles(int seed)
    {
        Random random = new(seed);
        string[] keys = ["k0", "k1", "k2", "k3", "k4", "k5"];

        PreparedIntentStore separate = new(), merged = new();
        RecordingInstaller separateInstalls = new(), mergedInstalls = new();
        separate.AttachResolvedIntentInstaller(separateInstalls);
        merged.AttachResolvedIntentInstaller(mergedInstalls);

        List<PreparedIntent[]> pending = [];
        List<PreparedIntent[]> everyTransaction = [];
        Dictionary<string, long> revisions = [];
        long logId = 1, time = 1_000, transaction = 1;
        int mergedEntries = 0;

        for (int step = 0; step < 200; step++)
        {
            time += 10;

            if (pending.Count == 0 || random.Next(3) > 0)
            {
                // A prepare of one or two keys. A key that another transaction holds refuses it in both stores.
                int count = random.Next(1, 3);
                PreparedIntent[] intents = [.. keys.OrderBy(_ => random.Next()).Take(count).Order(StringComparer.Ordinal)
                    .Select(key => Intent(transaction, key, revisions[key] = revisions.GetValueOrDefault(key) + 1))];
                transaction++;

                byte[] prepare = PreparedIntentStore.SerializeDelta(intents.Select(intent => (PreparedIntentCommand)new PrepareIntentCommand(intent)));
                Assert.Equal(
                    separate.ApplyDeltaAckPrepares(PartitionId, Log(logId, time, prepare)),
                    merged.ApplyDeltaAckPrepares(PartitionId, Log(logId, time, prepare)));
                logId++;

                pending.Add(intents);
                everyTransaction.Add(intents);
                continue;
            }

            // A group of adjacent settles in one batch: some commit, some abort, and one can repeat.
            int settles = random.Next(1, 4);
            List<DurableProposalSubmission> group = [];
            for (int i = 0; i < settles && pending.Count > 0; i++)
            {
                int pick = random.Next(pending.Count);
                PreparedIntent[] intents = pending[pick];
                if (random.Next(4) > 0)
                    pending.RemoveAt(pick);

                group.Add(Submission(SettleDelta(commit: random.Next(5) > 0, intents)));
            }

            foreach (DurableProposalSubmission settle in group)
                separate.ApplyDeltaAckPrepares(PartitionId, Log(logId++, time, settle.Entries[0].Data));

            List<IProposalSubmission> batch = SettleRunMerger.Merge([.. group], 512, awaiter: null);
            Assert.Single(batch);
            if (batch[0] is MergedSettleSubmission)
                mergedEntries++;

            // The merged entry takes the index of the last settle of the group, so the later entries keep their indexes.
            merged.ApplyDeltaAckPrepares(PartitionId, Log(logId - 1, time, batch[0].Entries[0].Data));
        }

        Assert.True(mergedEntries > 5);

        Assert.Equal(separate.Count, merged.Count);
        Assert.Equal(separate.LiveIntentCountForPartition(PartitionId), merged.LiveIntentCountForPartition(PartitionId));

        foreach (PreparedIntent[] intents in everyTransaction)
        {
            foreach (PreparedIntent intent in intents)
            {
                Assert.Equal(
                    Shape(separate.GetByIdentity(intent.TransactionId, intent.Epoch, intent.Key)),
                    Shape(merged.GetByIdentity(intent.TransactionId, intent.Epoch, intent.Key)));
            }
        }

        Assert.Equal(
            separate.SnapshotLedger(PartitionId).OrderBy(head => head.Key, StringComparer.Ordinal),
            merged.SnapshotLedger(PartitionId).OrderBy(head => head.Key, StringComparer.Ordinal));

        Assert.Equal(
            separate.SnapshotSettledIdentities(PartitionId).OrderBy(entry => (entry.TransactionId, entry.Key)),
            merged.SnapshotSettledIdentities(PartitionId).OrderBy(entry => (entry.TransactionId, entry.Key)));

        Assert.Equal(separateInstalls.Installs, mergedInstalls.Installs);
        Assert.True(mergedInstalls.Completed.Count < separateInstalls.Completed.Count);
    }

    // A record compares its value array by reference, so the value is compared by its bytes.
    private static (PreparedIntent? Intent, string? Value) Shape(PreparedIntent? intent) =>
        intent is null ? (null, null) : (intent with { Value = null }, intent.Value is null ? null : Convert.ToHexString(intent.Value));

    [Fact]
    public void TheSameSettleTwoTimes_InOneEntryAndInTwoEntries_InstallsOneTime()
    {
        PreparedIntent intent = Intent(1, "a", 1);
        byte[] settle = SettleDelta(true, intent);

        foreach (bool oneEntry in new[] { true, false })
        {
            PreparedIntentStore store = new();
            RecordingInstaller installs = new();
            store.AttachResolvedIntentInstaller(installs);
            store.ApplyDeltaAckPrepares(PartitionId, Log(1, 100, PreparedIntentStore.SerializeDelta([new PrepareIntentCommand(intent)])));

            if (oneEntry)
            {
                List<IProposalSubmission> batch = SettleRunMerger.Merge([Submission(settle), Submission(settle)], 512, awaiter: null);
                store.ApplyDeltaAckPrepares(PartitionId, Log(2, 110, Assert.Single(batch).Entries[0].Data));
            }
            else
            {
                store.ApplyDeltaAckPrepares(PartitionId, Log(2, 110, settle));
                store.ApplyDeltaAckPrepares(PartitionId, Log(3, 110, settle));
            }

            Assert.Single(installs.Installs);
            Assert.Equal(0, store.Count);
        }
    }
}
