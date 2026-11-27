using System.Text;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Replication;
using Kahuna.Shared.KeyValue;
using Kommander.Data;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// The one log entry of a one-phase commit: its codec, and the stores that it leaves, which must equal the stores that
/// the three entries of the same commit leave, for every verdict of the gate and on a replay.
/// </summary>
public sealed class TestOnePhaseBundleEntry
{
    private const int Partition = 7;

    private const long Term = 3;

    private static HLCTimestamp Ts(long l) => new(0, l, 0);

    // ── codec ───────────────────────────────────────────────────────────────────

    private static (byte[] Init, byte[] Prepare, byte[] Decision) Parts(HLCTimestamp txId, string key)
    {
        List<TransactionParticipantRef> manifest = [new(key, KeyValueDurability.Persistent)];
        long hash = TransactionManifest.ComputeHash(txId, 1, key, Ts(txId.L + 1), manifest);

        byte[] init = TransactionRecordStore.SerializeDelta([new InitializeTransactionCommand(txId, 1, "coord", key, Ts(txId.L + 1), Ts(txId.L + 9_000), hash, manifest, Ts(txId.L + 2), txId)]);
        byte[] prepare = PreparedIntentStore.SerializeDelta([new PrepareIntentCommand(Intent(key, txId, 1, PreparedIntent.UnknownBaseRevision, KeyValueState.Undefined, hash))]);
        byte[] decision = TransactionRecordStore.SerializeDelta([new CommitTransactionCommand(txId, 1, hash, Ts(txId.L + 2), Ts(txId.L + 2), BundledPrepareKeys: [key])]);
        return (init, prepare, decision);
    }

    [Fact]
    public void Codec_ThePartsReadBackAsTheyWereWritten()
    {
        (byte[] init, byte[] prepare, byte[] decision) = Parts(Ts(1_000), "codec/a");

        byte[] entry = OnePhaseBundleEntry.Serialize(init, prepare, decision);
        OnePhaseBundleEntry.Parts parts = OnePhaseBundleEntry.ReadParts(entry);

        Assert.Equal(init, entry[parts.Init]);
        Assert.Equal(prepare, entry[parts.Prepare]);
        Assert.Equal(decision, entry[parts.Decision]);

        // A node that did not serialize the entry reads the prepare part from the bytes.
        PreparedIntentCommand[] commands = OnePhaseBundleEntry.PrepareCommandsOf([.. entry]);
        Assert.Equal("codec/a", Assert.IsType<PrepareIntentCommand>(Assert.Single(commands)).Intent.Key);

        Assert.True(OnePhaseBundleEntry.TryGetIntentCommands(ReplicationTypes.OnePhaseBundle, [.. entry], out PreparedIntentCommand[]? fromBundle));
        Assert.Single(fromBundle);
        Assert.True(OnePhaseBundleEntry.TryGetIntentCommands(ReplicationTypes.PreparedIntent, prepare, out PreparedIntentCommand[]? fromDelta));
        Assert.Single(fromDelta);
        Assert.False(OnePhaseBundleEntry.TryGetIntentCommands(ReplicationTypes.TransactionRecord, init, out _));
        Assert.False(OnePhaseBundleEntry.TryGetIntentCommands(ReplicationTypes.OnePhaseBundle, [], out _));
    }

    [Fact]
    public void Serialize_MovesTheCommandsOfTheParts_ToTheRegistrationOfTheEntry()
    {
        HLCTimestamp txId = Ts(2_000);
        const string key = "registrations/a";
        List<TransactionParticipantRef> manifest = [new(key, KeyValueDurability.Persistent)];
        long hash = TransactionManifest.ComputeHash(txId, 1, key, Ts(txId.L + 1), manifest);

        TransactionRecordCommand[] initCommands = [new InitializeTransactionCommand(txId, 1, "coord", key, Ts(txId.L + 1), Ts(txId.L + 9_000), hash, manifest, Ts(txId.L + 2), txId)];
        PreparedIntentCommand[] prepareCommands = [new PrepareIntentCommand(Intent(key, txId, 1, PreparedIntent.UnknownBaseRevision, KeyValueState.Undefined, hash))];
        TransactionRecordCommand[] decisionCommands = [new CommitTransactionCommand(txId, 1, hash, Ts(txId.L + 2), Ts(txId.L + 2), BundledPrepareKeys: [key])];

        byte[] init = TransactionRecordStore.SerializeDelta(initCommands);
        byte[] prepare = PreparedIntentStore.SerializeDelta(prepareCommands);
        byte[] decision = TransactionRecordStore.SerializeDelta(decisionCommands);

        Assert.True(PreparedIntentStore.TryGetProposedCommands(prepare, out PreparedIntentCommand[]? registered));
        Assert.Same(prepareCommands, registered);

        byte[] entry = OnePhaseBundleEntry.Serialize(init, prepare, decision);

        // The apply of the entry on this node reuses the commands of the three parts.
        Assert.True(OnePhaseBundleEntry.TryTakeCommands(entry, out OnePhaseBundleEntry.Commands? commands));
        Assert.Same(initCommands, commands.Init);
        Assert.Same(prepareCommands, commands.Prepare);
        Assert.Same(decisionCommands, commands.Decision);

        // No apply sees the part arrays, so their registrations are gone. A part array that goes to the log later as
        // an entry of its own is decoded.
        Assert.False(TransactionRecordStore.TryClaimProposedCommands(init, out _));
        Assert.False(TransactionRecordStore.TryClaimProposedCommands(decision, out _));
        Assert.True(PreparedIntentStore.TryGetProposedCommands(prepare, out PreparedIntentCommand[]? decoded));
        Assert.NotSame(prepareCommands, decoded);
        PrepareIntentCommand again = Assert.IsType<PrepareIntentCommand>(Assert.Single(decoded));
        Assert.Equal(key, again.Intent.Key);
        Assert.Equal(((PrepareIntentCommand)prepareCommands[0]).Intent.Value, again.Intent.Value);
    }

    [Fact]
    public void Serialize_OfPartsWithNoRegistration_DecodesThePrepare_AndLeavesTheRecordsToTheApply()
    {
        (byte[] init, byte[] prepare, byte[] decision) = Parts(Ts(3_000), "registrations/b");

        // Arrays that this node did not serialize, as a remote leader receives them.
        byte[] entry = OnePhaseBundleEntry.Serialize([.. init], [.. prepare], [.. decision]);

        Assert.True(OnePhaseBundleEntry.TryTakeCommands(entry, out OnePhaseBundleEntry.Commands? commands));
        Assert.Null(commands.Init);
        Assert.Null(commands.Decision);
        Assert.Equal("registrations/b", Assert.IsType<PrepareIntentCommand>(Assert.Single(commands.Prepare)).Intent.Key);
    }

    [Fact]
    public void Serialize_OfAnEntryLongerThanTheCacheLimit_RegistersNothing_AndTheApplyDecodesIt()
    {
        HLCTimestamp txId = Ts(4_000);
        const string key = "registrations/large";
        List<TransactionParticipantRef> manifest = [new(key, KeyValueDurability.Persistent)];
        long hash = TransactionManifest.ComputeHash(txId, 1, key, Ts(txId.L + 1), manifest);
        byte[] value = new byte[ProposedDeltaCache<OnePhaseBundleEntry.Commands>.DefaultMaxRegisteredBytes];
        value.AsSpan().Fill(7);

        byte[] init = TransactionRecordStore.SerializeDelta([new InitializeTransactionCommand(txId, 1, "coord", key, Ts(txId.L + 1), Ts(txId.L + 9_000), hash, manifest, Ts(txId.L + 2), txId)]);
        PreparedIntent intent = Intent(key, txId, 1, PreparedIntent.UnknownBaseRevision, KeyValueState.Undefined, hash) with { Value = value };
        byte[] prepare = PreparedIntentStore.SerializeDelta([new PrepareIntentCommand(intent)]);
        byte[] decision = TransactionRecordStore.SerializeDelta([new CommitTransactionCommand(txId, 1, hash, Ts(txId.L + 2), Ts(txId.L + 2), BundledPrepareKeys: [key])]);

        byte[] entry = OnePhaseBundleEntry.Serialize(init, prepare, decision);

        Assert.True(entry.Length > ProposedDeltaCache<OnePhaseBundleEntry.Commands>.DefaultMaxRegisteredBytes);
        Assert.False(OnePhaseBundleEntry.TryTakeCommands(entry, out _));
        Assert.False(TransactionRecordStore.TryClaimProposedCommands(init, out _));
        Assert.False(TransactionRecordStore.TryClaimProposedCommands(decision, out _));

        Replica replica = new();
        RaftLog log = Log(1, 10, ReplicationTypes.OnePhaseBundle, entry);
        Assert.True(OnePhaseBundleEntry.Apply(replica.Records, replica.Intents, tracker: null, Partition, log, entry, replay: false));
        Assert.Equal(TransactionDecision.Commit, replica.Records.Get(txId, 1)!.Decision);
        Assert.Equal(value, replica.Intents.GetByIdentity(txId, 1, key)!.Value);
    }

    [Fact]
    public void Codec_AFieldOfANewerWriter_IsSkipped()
    {
        (byte[] init, byte[] prepare, byte[] decision) = Parts(Ts(1_000), "codec/b");
        byte[] entry = OnePhaseBundleEntry.Serialize(init, prepare, decision);

        // Field 9 as a varint, and field 10 as a length-delimited value, after the three parts.
        byte[] extended = [.. entry, 9 << 3 | 0, 42, 10 << 3 | 2, 2, 7, 7];
        OnePhaseBundleEntry.Parts parts = OnePhaseBundleEntry.ReadParts(extended);

        Assert.Equal(prepare, extended[parts.Prepare]);
        Assert.Equal(decision, extended[parts.Decision]);
    }

    public static TheoryData<string> Malformed => ["no_decision", "no_init", "two_prepares", "overrun", "empty", "group"];

    // One length-delimited field: its tag, a one-byte length, the bytes.
    private static byte[] Field(int number, byte[] payload) => [(byte)(number << 3 | 2), (byte)payload.Length, .. payload];

    [Theory]
    [MemberData(nameof(Malformed))]
    public void Codec_AMalformedEntry_Fails(string shape)
    {
        byte[] part = [1, 2, 3];
        byte[] entry = [.. Field(1, part), .. Field(2, part), .. Field(3, part)];
        Assert.Equal(new Range(2, 5), OnePhaseBundleEntry.ReadParts(entry).Init);

        byte[] bad = shape switch
        {
            "no_decision" => [.. Field(1, part), .. Field(2, part)],
            "no_init" => [.. Field(2, part), .. Field(3, part)],
            "two_prepares" => [.. Field(1, part), .. Field(2, part), .. Field(2, part), .. Field(3, part)],
            "overrun" => entry[..^1],
            "group" => [.. entry, 11 << 3 | 3],
            _ => []
        };

        Assert.Throws<InvalidDataException>(() => OnePhaseBundleEntry.ReadParts(bad));
    }

    // ── the stores of three entries and of one entry ────────────────────────────

    private static PreparedIntent Intent(string key, HLCTimestamp txId, long revision, long baseRevision, KeyValueState baseState, long hash) => new(
        TransactionId: txId, Epoch: 1, Key: key, ManifestHash: hash, RecordAnchorKey: key,
        CommitTimestamp: Ts(txId.L + 1),
        State: KeyValueState.Set, Value: Encoding.UTF8.GetBytes($"{key}@{txId.L}"), Bucket: null,
        Revision: revision, Expires: HLCTimestamp.Zero, NoRevision: false,
        BaseRevision: baseRevision, BaseState: baseState,
        RecoveryDeadline: HLCTimestamp.Zero, Resolution: PreparedIntentResolution.Pending);

    /// <summary>The two stores of one replica, with the gate of the bundled commit attached.</summary>
    private sealed class Replica
    {
        public readonly TransactionRecordStore Records = new();
        public readonly PreparedIntentStore Intents = new();
        public readonly List<bool> Acknowledgements = [];

        public Replica() => Records.AttachBundledCommitJudge(Intents.JudgeBundledCommit);
    }

    private static RaftLog Log(long id, long time, string type, byte[] data) => new()
    {
        Id = id,
        Term = Term,
        Type = RaftLogType.Committed,
        Time = Ts(time),
        LogType = type,
        // A copy, so each store decodes the bytes as a node that did not propose them.
        LogData = [.. data]
    };

    /// <summary>One transaction of a schedule: its three deltas, and the identity the comparison reads back.</summary>
    private sealed record Commit(HLCTimestamp TransactionId, HLCTimestamp OpId, string[] Keys, byte[] Init, byte[] Prepare, byte[] Decision, PreparedIntent[] Intents);

    /// <summary>
    /// Random schedules of one-phase commits that meet every verdict of the gate: a key that another transaction holds,
    /// a stale validated base, a stale read, a revision that a head already has, a lock granted in another term, a
    /// late deadline, and a recovery abort of the transaction before its commit. Some commits apply again as a restart
    /// replay, and some keys are settled ahead of the next writer's prepare, as a carried settle is. One replica applies
    /// each commit as three entries, the other as one bundle entry at the index of the last of the three; both get the
    /// same entries in between.
    /// </summary>
    [Theory]
    [InlineData(11)]
    [InlineData(12)]
    [InlineData(13)]
    [InlineData(14)]
    [InlineData(15)]
    [InlineData(16)]
    [InlineData(17)]
    [InlineData(18)]
    [InlineData(19)]
    [InlineData(20)]
    public void ARandomSchedule_LeavesEqualStores_AsThreeEntriesAndAsOneEntry(int seed)
    {
        Random random = new(seed);
        string[] keys = ["g/0", "g/1", "g/2", "g/3", "g/4"];

        Replica three = new(), one = new();
        List<Commit> commits = [];
        List<Commit> unsettled = [];
        long logId = 1, time = 10_000, tx = 1;
        int verdicts = 0;

        void Both(string type, byte[] data, bool replay = false)
        {
            foreach (Replica replica in new[] { three, one })
            {
                RaftLog log = Log(logId, time, type, data);
                if (type == ReplicationTypes.TransactionRecord)
                    _ = replay ? replica.Records.Restore(Partition, log) : replica.Records.Replicate(Partition, log);
                else if (replay)
                    replica.Intents.Restore(Partition, log);
                else
                    replica.Intents.ApplyDeltaAckPrepares(Partition, log);
            }

            logId++;
        }

        void Settle(Commit commit)
        {
            TransactionRecord? record = three.Records.Get(commit.TransactionId, 1);
            bool commitIt = record?.Decision == TransactionDecision.Commit;

            // An undecided record is aborted first, as the recovery sweep does.
            if (record?.Decision == TransactionDecision.Undecided)
            {
                long hash = commit.Intents[0].ManifestHash;
                Both(ReplicationTypes.TransactionRecord, TransactionRecordStore.SerializeDelta([new AbortTransactionCommand(
                    commit.TransactionId, 1, hash, TransactionAbortClass.PresumedAbort, Ts(time), Ts(time), commit.Keys[0],
                    Ts(commit.TransactionId.L + 1), Ts(commit.TransactionId.L + 9_000), commit.TransactionId)]));
            }

            Both(ReplicationTypes.PreparedIntent, PreparedIntentStore.SerializeDelta(
                DurableTransactionFinalizer.BuildSettleCommands(commit.Intents, commitIt, materializeOnResolve: false)));
        }

        for (int step = 0; step < 120; step++)
        {
            time += 10;

            // Settle some earlier transactions, or the holders of the keys the next commit writes, first.
            if (unsettled.Count > 0 && random.Next(3) == 0)
            {
                Commit settled = unsettled[random.Next(unsettled.Count)];
                unsettled.Remove(settled);
                Settle(settled);
                continue;
            }

            HLCTimestamp txId = Ts(tx * 100);
            HLCTimestamp opId = Ts(tx * 100 + 2);
            tx++;

            string[] written = [.. keys.OrderBy(_ => random.Next()).Take(random.Next(1, 3)).Order(StringComparer.Ordinal)];
            string anchor = written[0];

            List<TransactionParticipantRef> manifest = [.. written.Select(k => new TransactionParticipantRef(k, KeyValueDurability.Persistent))];
            long hash = TransactionManifest.ComputeHash(txId, 1, anchor, Ts(txId.L + 1), manifest);

            PreparedIntent[] intents = [.. written.Select(key =>
            {
                bool hasHead = three.Intents.TryGetCommittedHead(key, out long head, out KeyValueState headState);
                long current = hasHead ? head : 0;

                int shape = random.Next(10);
                long revision = shape == 0 ? current : current + 1;                                  // a revision collision
                long baseRevision = shape <= 4 ? PreparedIntent.UnknownBaseRevision                  // a blind write
                    : shape == 9 ? current - 1                                                       // a stale base
                    : current;
                KeyValueState baseState = baseRevision == PreparedIntent.UnknownBaseRevision ? KeyValueState.Undefined
                    : hasHead ? headState : KeyValueState.Undefined;

                return Intent(key, txId, revision, baseRevision, baseState, hash);
            })];

            List<BundledReadDependency>? reads = null;
            if (random.Next(4) == 0)
            {
                string read = keys[random.Next(keys.Length)];
                bool exists = three.Intents.TryGetCommittedHead(read, out long readHead, out _);
                reads = [new BundledReadDependency(read, random.Next(3) == 0 ? readHead - 1 : readHead, exists)];
            }

            HLCTimestamp deadline = random.Next(10) == 0 ? Ts(txId.L) : Ts(txId.L + 9_000);   // a late deadline
            long lockTerm = random.Next(10) == 0 ? Term + 1 : 0;                              // a lock of another term

            byte[] init = TransactionRecordStore.SerializeDelta([new InitializeTransactionCommand(txId, 1, "coord", anchor, Ts(txId.L + 1), deadline, hash, manifest, opId, txId)]);
            byte[] prepare = PreparedIntentStore.SerializeDelta(intents.Select(intent => (PreparedIntentCommand)new PrepareIntentCommand(intent)));
            byte[] decision = TransactionRecordStore.SerializeDelta([new CommitTransactionCommand(
                txId, 1, hash, opId, AttemptHlc: opId, BundledPrepareKeys: written,
                ApplyTimeValidation: true, BundledReadDependencies: reads, LockGrantTerm: lockTerm)]);

            Commit commit = new(txId, opId, written, init, prepare, decision, intents);
            commits.Add(commit);

            // A recovery abort ordered ahead of the commit.
            if (random.Next(10) == 0)
                Both(ReplicationTypes.TransactionRecord, TransactionRecordStore.SerializeDelta([new AbortTransactionCommand(
                    txId, 1, hash, TransactionAbortClass.PresumedAbort, Ts(time), Ts(time), anchor, Ts(txId.L + 1), deadline, txId)]));

            ApplyCommit(three, one, commit, logId, time, replay: false);
            logId += 3;

            // A restart replays the same commit.
            if (random.Next(7) == 0)
            {
                ApplyCommit(three, one, commit, logId, time, replay: true);
                logId += 3;
            }

            if (three.Records.Get(txId, 1)?.Decision != TransactionDecision.Commit)
                verdicts++;

            unsettled.Add(commit);
        }

        Assert.True(verdicts > 5, "the schedule must meet the refusals of the gate");
        Assert.Equal(three.Acknowledgements, one.Acknowledgements);

        foreach (Commit commit in commits)
        {
            TransactionRecord? a = three.Records.Get(commit.TransactionId, 1);
            TransactionRecord? b = one.Records.Get(commit.TransactionId, 1);
            Assert.Equal(a?.Decision, b?.Decision);
            Assert.Equal(a?.AbortClass, b?.AbortClass);
            Assert.Equal(a?.WasBundledCommitRejected(commit.OpId), b?.WasBundledCommitRejected(commit.OpId));

            bool verdictA = three.Records.TryTakeGatedRejectionVerdict(commit.TransactionId, 1, commit.OpId, out BundledCommitVerdict va);
            bool verdictB = one.Records.TryTakeGatedRejectionVerdict(commit.TransactionId, 1, commit.OpId, out BundledCommitVerdict vb);
            Assert.Equal((verdictA, va), (verdictB, vb));

            foreach (PreparedIntent intent in commit.Intents)
            {
                Assert.Equal(Shape(three.Intents.GetByIdentity(intent.TransactionId, 1, intent.Key)), Shape(one.Intents.GetByIdentity(intent.TransactionId, 1, intent.Key)));

                bool rejectedA = three.Intents.TryTakePrepareRejection(intent.TransactionId, 1, intent.Key, out var kindA);
                bool rejectedB = one.Intents.TryTakePrepareRejection(intent.TransactionId, 1, intent.Key, out var kindB);
                Assert.Equal((rejectedA, kindA), (rejectedB, kindB));
            }
        }

        Assert.Equal(three.Intents.Count, one.Intents.Count);
        Assert.Equal(
            three.Intents.SnapshotLedger(Partition).OrderBy(h => h.Key, StringComparer.Ordinal),
            one.Intents.SnapshotLedger(Partition).OrderBy(h => h.Key, StringComparer.Ordinal));
        Assert.Equal(
            three.Intents.SnapshotSettledIdentities(Partition).OrderBy(s => (s.TransactionId, s.Key)),
            one.Intents.SnapshotSettledIdentities(Partition).OrderBy(s => (s.TransactionId, s.Key)));
    }

    /// <summary>Applies a commit as the three entries <c>[init, prepare, decision]</c> to <paramref name="three"/> and as
    /// one bundle entry at the index of the decision to <paramref name="one"/>.</summary>
    private static void ApplyCommit(Replica three, Replica one, Commit commit, long firstId, long time, bool replay)
    {
        RaftLog init = Log(firstId, time, ReplicationTypes.TransactionRecord, commit.Init);
        RaftLog prepare = Log(firstId + 1, time, ReplicationTypes.PreparedIntent, commit.Prepare);
        RaftLog decision = Log(firstId + 2, time, ReplicationTypes.TransactionRecord, commit.Decision);

        if (replay)
        {
            three.Records.Restore(Partition, init);
            three.Intents.Restore(Partition, prepare);
            three.Records.Restore(Partition, decision);
        }
        else
        {
            three.Records.Replicate(Partition, init);
            three.Acknowledgements.Add(three.Intents.ApplyDeltaAckPrepares(Partition, prepare));
            three.Records.Replicate(Partition, decision);
        }

        RaftLog bundle = Log(firstId + 2, time, ReplicationTypes.OnePhaseBundle, OnePhaseBundleEntry.Serialize(commit.Init, commit.Prepare, commit.Decision));
        bool acknowledged = OnePhaseBundleEntry.Apply(one.Records, one.Intents, tracker: null, Partition, bundle, bundle.LogData!, replay);
        if (!replay)
            one.Acknowledgements.Add(acknowledged);
    }

    // A record compares its value array by reference, so the value is compared by its bytes.
    private static (PreparedIntent? Intent, string? Value) Shape(PreparedIntent? intent) =>
        intent is null ? (null, null) : (intent with { Value = null }, intent.Value is null ? null : Convert.ToHexString(intent.Value));

    // ── the readers of the leader ───────────────────────────────────────────────

    private static RaftProposalEntry BundleProposal(byte[] init, byte[] prepare, byte[] decision) =>
        new(ReplicationTypes.OnePhaseBundle, OnePhaseBundleEntry.Serialize(init, prepare, decision), AutoCommit: true, ExpectedGeneration: 0, ExpectedTerm: Term);

    [Fact]
    public void ABundleSubmission_NamesTheKeysOfItsPreparePart()
    {
        HLCTimestamp txId = Ts(7_000);
        List<TransactionParticipantRef> manifest = [new("keys/a", KeyValueDurability.Persistent), new("keys/b", KeyValueDurability.Persistent)];
        long hash = TransactionManifest.ComputeHash(txId, 1, "keys/a", Ts(txId.L + 1), manifest);

        byte[] init = TransactionRecordStore.SerializeDelta([new InitializeTransactionCommand(txId, 1, "coord", "keys/a", Ts(txId.L + 1), Ts(txId.L + 9_000), hash, manifest, Ts(txId.L + 2), txId)]);
        byte[] prepare = PreparedIntentStore.SerializeDelta([
            new PrepareIntentCommand(Intent("keys/a", txId, 1, PreparedIntent.UnknownBaseRevision, KeyValueState.Undefined, hash)),
            new PrepareIntentCommand(Intent("keys/b", txId, 1, PreparedIntent.UnknownBaseRevision, KeyValueState.Undefined, hash))]);
        byte[] decision = TransactionRecordStore.SerializeDelta([new CommitTransactionCommand(txId, 1, hash, Ts(txId.L + 2), Ts(txId.L + 2), BundledPrepareKeys: ["keys/a", "keys/b"])]);

        // Also with bytes this node did not serialize, as a remote origin's.
        foreach (RaftProposalEntry entry in new[] { BundleProposal(init, prepare, decision), BundleProposal(init, prepare, decision) with { Data = [.. BundleProposal(init, prepare, decision).Data] } })
        {
            Kahuna.Server.KeyValues.Writes.DurableProposalSubmission submission = new(
                Partition, [entry], new TaskCompletionSource<bool>(), Kahuna.Server.KeyValues.Writes.WriteAdmissionClass.Ordinary, Kahuna.Server.KeyValues.Writes.WriteSubmissionStage.OnePhase);

            Assert.Equal(["keys/a", "keys/b"], Enumerable.Range(0, submission.ChangedKeyCount).Select(submission.ChangedKey));
        }
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task TheCompletionOfABundleEntry_AnswersThePrepareResultOfTheEntry(bool acknowledged)
    {
        Kahuna.Server.KeyValues.Writes.DurableCompletionAnswer expected = acknowledged
            ? Kahuna.Server.KeyValues.Writes.DurableCompletionAnswer.Acknowledged
            : Kahuna.Server.KeyValues.Writes.DurableCompletionAnswer.Refused;
        (byte[] init, byte[] prepare, byte[] decision) = Parts(Ts(8_000), "await/a");
        DurableApplyResultLedger ledger = new();
        Kahuna.Server.KeyValues.Writes.DurableOrderedApplyAwaiter awaiter = new(ledger, new PreparedIntentStore(), logger: null);

        ledger.RecordApplied(Partition, 40, acknowledged);

        Assert.Equal(expected, await awaiter.AwaitAppliedAsync(Partition, [BundleProposal(init, prepare, decision)], [40], CancellationToken.None));
    }

    /// <summary>When the result of the entry is displaced from the bounded window, the completion reads the prepare part
    /// back from the store: an intent held under the transaction's own identity is an acknowledgement.</summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task ADisplacedResult_IsReadBackFromThePreparePart(bool held)
    {
        HLCTimestamp txId = Ts(9_000);
        (byte[] init, byte[] prepare, byte[] decision) = Parts(txId, "await/b");
        Replica replica = new();
        DurableApplyResultLedger ledger = new();
        Kahuna.Server.KeyValues.Writes.DurableOrderedApplyAwaiter awaiter = new(ledger, replica.Intents, logger: null);

        if (held)
        {
            RaftLog log = Log(10, 100, ReplicationTypes.OnePhaseBundle, OnePhaseBundleEntry.Serialize(init, prepare, decision));
            OnePhaseBundleEntry.Apply(replica.Records, replica.Intents, tracker: null, Partition, log, log.LogData!, replay: false);
        }

        ledger.RecordApplied(Partition, 10, true);
        for (long index = 11; index <= 11 + 1_500; index++)
            ledger.RecordApplied(Partition, index, result: true);

        Kahuna.Server.KeyValues.Writes.DurableCompletionAnswer answer = await awaiter.AwaitAppliedAsync(Partition, [BundleProposal(init, prepare, decision)], [10], CancellationToken.None);
        Assert.Equal(held ? Kahuna.Server.KeyValues.Writes.DurableCompletionAnswer.Acknowledged : Kahuna.Server.KeyValues.Writes.DurableCompletionAnswer.Refused, answer);
    }

    /// <summary>A bundle entry between two plain entries: the durability floor of the partition passes it only when the
    /// snapshots of the record store and of the intent store both cover it, in either order.</summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void ABundleEntry_HoldsTheDurabilityFloor_UntilBothStoreSnapshotsCoverIt(bool recordsFirst)
    {
        (byte[] init, byte[] prepare, byte[] decision) = Parts(Ts(11_000), "floor/a");
        Kahuna.Server.Persistence.PartitionDurabilityTracker tracker = new();
        Replica replica = new();

        tracker.RegisterPending(Partition, 5, Kahuna.Server.Persistence.DurabilityChannel.Flush);
        tracker.Resolve(Partition, 5);

        RaftLog log = Log(6, 100, ReplicationTypes.OnePhaseBundle, OnePhaseBundleEntry.Serialize(init, prepare, decision));
        Assert.True(OnePhaseBundleEntry.Apply(replica.Records, replica.Intents, tracker, Partition, log, log.LogData!, replay: false));

        tracker.RegisterPending(Partition, 7, Kahuna.Server.Persistence.DurabilityChannel.Flush);
        tracker.Resolve(Partition, 7);

        Assert.Equal(5, tracker.GetWatermark(Partition));
        Assert.Equal(6, tracker.GetHighestApplied(Partition, Kahuna.Server.Persistence.DurabilityChannel.TransactionRecords));
        Assert.Equal(6, tracker.GetHighestApplied(Partition, Kahuna.Server.Persistence.DurabilityChannel.PreparedIntents));

        Kahuna.Server.Persistence.DurabilityChannel first = recordsFirst ? Kahuna.Server.Persistence.DurabilityChannel.TransactionRecords : Kahuna.Server.Persistence.DurabilityChannel.PreparedIntents;
        Kahuna.Server.Persistence.DurabilityChannel second = recordsFirst ? Kahuna.Server.Persistence.DurabilityChannel.PreparedIntents : Kahuna.Server.Persistence.DurabilityChannel.TransactionRecords;

        tracker.ResolveUpTo(Partition, first, 6);
        Assert.Equal(5, tracker.GetWatermark(Partition));

        tracker.ResolveUpTo(Partition, second, 6);
        Assert.Equal(7, tracker.GetWatermark(Partition));
    }

    /// <summary>A replay applies the prepare part as a replay: a prepare over a held key answers nothing and leaves no
    /// rejection memo, where the live apply of the same entry refuses the prepare and memos the refusal.</summary>
    [Fact]
    public void AReplayedBundleEntry_AppliesItsPrepareAsAReplay()
    {
        (byte[] holderInit, byte[] holderPrepare, byte[] holderDecision) = Parts(Ts(12_000), "replay/a");
        (byte[] init, byte[] prepare, byte[] decision) = Parts(Ts(13_000), "replay/a");

        foreach (bool replay in new[] { true, false })
        {
            Replica replica = new();
            RaftLog holder = Log(1, 100, ReplicationTypes.OnePhaseBundle, OnePhaseBundleEntry.Serialize(holderInit, holderPrepare, holderDecision));
            Assert.True(OnePhaseBundleEntry.Apply(replica.Records, replica.Intents, tracker: null, Partition, holder, holder.LogData!, replay: false));

            // The holder is committed and not settled: its intent holds the key.
            RaftLog second = Log(2, 110, ReplicationTypes.OnePhaseBundle, OnePhaseBundleEntry.Serialize(init, prepare, decision));
            bool acknowledged = OnePhaseBundleEntry.Apply(replica.Records, replica.Intents, tracker: null, Partition, second, second.LogData!, replay);

            Assert.Equal(replay, acknowledged);
            Assert.Equal(!replay, replica.Intents.TryTakePrepareRejection(Ts(13_000), 1, "replay/a", out _));
            Assert.Equal(TransactionDecision.Undecided, replica.Records.Get(Ts(13_000), 1)!.Decision);
        }
    }

    [Fact]
    public void TheWriteMetrics_TagABundleEntryAsABundle()
    {
        Assert.Equal("bundle", Kahuna.Server.KeyValues.Writes.PartitionWriteAggregatorMetrics.TypeTag(ReplicationTypes.OnePhaseBundle).Value);
        Assert.Equal("intent", Kahuna.Server.KeyValues.Writes.PartitionWriteAggregatorMetrics.TypeTag(ReplicationTypes.PreparedIntent).Value);
        Assert.Equal("record", Kahuna.Server.KeyValues.Writes.PartitionWriteAggregatorMetrics.TypeTag(ReplicationTypes.TransactionRecord).Value);
    }

    [Fact]
    public void TheEntryThatThisNodeSerialized_AppliesItsRegisteredCommands_AndAnotherNodeDecodesThem()
    {
        (byte[] init, byte[] prepare, byte[] decision) = Parts(Ts(5_000), "local/a");
        byte[] entry = OnePhaseBundleEntry.Serialize(init, prepare, decision);

        Assert.True(OnePhaseBundleEntry.TryTakeCommands(entry, out OnePhaseBundleEntry.Commands? commands));
        Assert.NotNull(commands.Init);
        Assert.NotNull(commands.Decision);
        Assert.Single(commands.Prepare);

        // The bytes of another node have no registration, and the apply decodes the parts.
        Replica replica = new();
        RaftLog log = Log(1, 10, ReplicationTypes.OnePhaseBundle, entry);
        Assert.False(OnePhaseBundleEntry.TryTakeCommands(log.LogData!, out _));
        Assert.True(OnePhaseBundleEntry.Apply(replica.Records, replica.Intents, tracker: null, Partition, log, log.LogData!, replay: false));
        Assert.Equal(TransactionDecision.Commit, replica.Records.Get(Ts(5_000), 1)!.Decision);
        Assert.NotNull(replica.Intents.GetByIdentity(Ts(5_000), 1, "local/a"));
    }
}
