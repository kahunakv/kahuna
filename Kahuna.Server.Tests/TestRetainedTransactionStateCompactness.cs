using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.KeyValues.Writes;
using Kahuna.Server.Replication;
using Kahuna.Shared.KeyValue;
using Kahuna.Utils;
using Kommander.Data;
using Kommander.Time;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kahuna.Server.Tests;

/// <summary>
/// The size of what a node keeps for each committed transaction: the layout of a transaction record, the
/// coordinator key that a decided record drops, the participants of a decoded record, and the key strings that
/// the record, the completion receipts and the settled identities of one node share.
/// </summary>
public sealed class TestRetainedTransactionStateCompactness
{
    private const int Partition = 3;

    private const long Epoch = 1;

    private const string Anchor = "ranged1/accounts/k000000000001";

    private const string Other = "ranged1/accounts/k000000000002";

    private static HLCTimestamp Ts(long l) => new(0, l, 0);

    // A new string with the characters of the argument, as a decoder makes for each log entry.
    private static string Copy(string value) => new(value.AsSpan());

    private static InitializeTransactionCommand Init(HLCTimestamp txId, string coordinatorKey = "0123456789abcdef0123456789abcdef")
    {
        List<TransactionParticipantRef> manifest =
        [
            new(Copy(Anchor), KeyValueDurability.Persistent),
            new(Copy(Other), KeyValueDurability.Persistent)
        ];

        return new(txId, Epoch, coordinatorKey, Copy(Anchor), Ts(txId.L + 100), Ts(txId.L + 9_000),
            TransactionManifest.ComputeHash(txId, Epoch, Anchor, Ts(txId.L + 100), manifest),
            manifest, OpId: Ts(txId.L + 1), CreatedAt: Ts(txId.L + 2));
    }

    // A log entry whose bytes no cache of proposed entries knows, so the store decodes it as a follower does.
    private static RaftLog DecodedEntry(params TransactionRecordCommand[] commands) => new()
    {
        LogType = ReplicationTypes.TransactionRecord,
        LogData = (byte[])TransactionRecordStore.SerializeDelta(commands).Clone()
    };

    private static PreparedIntent Intent(HLCTimestamp txId, string key) => new(
        TransactionId: txId, Epoch: Epoch, Key: key, ManifestHash: 0, RecordAnchorKey: Copy(Anchor), CommitTimestamp: Ts(txId.L + 100),
        State: KeyValueState.Set, Value: [1], Bucket: null, Revision: 1, Expires: HLCTimestamp.Zero, NoRevision: true,
        BaseRevision: PreparedIntent.UnknownBaseRevision, BaseState: KeyValueState.Undefined,
        RecoveryDeadline: HLCTimestamp.Zero, Resolution: PreparedIntentResolution.Pending);

    private static void Settle(PreparedIntentStore intents, HLCTimestamp txId, string key)
    {
        intents.Apply(new PrepareIntentCommand(Intent(txId, Copy(key))), Partition);
        intents.Apply(new ResolveIntentCommand(txId, Epoch, Copy(key), Commit: true), Partition);
        intents.Apply(new RemoveIntentCommand(txId, Epoch, Copy(key)), Partition);
    }

    // ── the record ──────────────────────────────────────────────────────────────

    [Fact]
    public void ARecord_ReturnsEveryTimestampItWasBuiltWith()
    {
        // Every component at a bound of its type: a packed timestamp must lose no bit.
        HLCTimestamp txId = new(int.MaxValue, long.MaxValue, uint.MaxValue);
        HLCTimestamp commit = new(int.MinValue, long.MinValue, 0);
        HLCTimestamp deadline = new(7, 1_700_000_000_123, 41);
        HLCTimestamp winner = new(-3, 5, 6);
        HLCTimestamp created = new(1, 2, 3);
        HLCTimestamp decided = new(9, 8, 7);

        TransactionRecord record = new(
            txId, Epoch, "coordinator", Anchor, commit, deadline, 42, [], ManifestPresent: true,
            TransactionDecision.Commit, TransactionAbortClass.None, winner, created, decided);

        Assert.Equal(txId, record.TransactionId);
        Assert.Equal(commit, record.CommitTimestamp);
        Assert.Equal(deadline, record.DecisionDeadline);
        Assert.Equal(winner, record.WinningOpId);
        Assert.Equal(created, record.CreatedAt);
        Assert.Equal(decided, record.DecidedAt);
    }

    [Fact]
    public void ACopyOfARecord_ChangesOnlyTheTimestampItNames()
    {
        TransactionRecord record = new(
            Ts(1_000), Epoch, "coordinator", Anchor, Ts(1_100), Ts(9_000), 42, [], ManifestPresent: true,
            TransactionDecision.Undecided, TransactionAbortClass.None, HLCTimestamp.Zero, Ts(1_002), HLCTimestamp.Zero);

        TransactionRecord extended = record with { DecisionDeadline = Ts(12_000) };

        Assert.Equal(Ts(12_000), extended.DecisionDeadline);
        Assert.Equal(record.TransactionId, extended.TransactionId);
        Assert.Equal(record.CommitTimestamp, extended.CommitTimestamp);
        Assert.Equal(record.CreatedAt, extended.CreatedAt);
        Assert.NotEqual(record, extended);
        Assert.Equal(record, extended with { DecisionDeadline = Ts(9_000) });
    }

    [Fact]
    public void ARecordObject_Is176Bytes()
    {
        TransactionParticipantRef[] participants = [];

        static TransactionRecord Build(TransactionParticipantRef[] participants) => new(
            Ts(1_000), Epoch, "coordinator", Anchor, Ts(1_100), Ts(9_000), 42, participants, ManifestPresent: true,
            TransactionDecision.Undecided, TransactionAbortClass.None, HLCTimestamp.Zero, Ts(1_002), HLCTimestamp.Zero);

        // The first construction runs the static initializers that the record needs.
        Build(participants);

        long before = GC.GetAllocatedBytesForCurrentThread();
        TransactionRecord record = Build(participants);
        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        // Six timestamps of 16 bytes. With the 24-byte timestamp of the clock the object is 224 bytes, and a
        // node holds one for each transaction of the retention window.
        Assert.Equal(176, allocated);
        Assert.Equal(Ts(1_000), record.TransactionId);
    }

    [Fact]
    public void APackedTimestamp_Is16Bytes()
    {
        Assert.Equal(16, System.Runtime.CompilerServices.Unsafe.SizeOf<PackedTimestamp>());
        Assert.Equal(24, System.Runtime.CompilerServices.Unsafe.SizeOf<HLCTimestamp>());

        HLCTimestamp value = new(int.MinValue, long.MaxValue, uint.MaxValue);
        PackedTimestamp packed = new(value);

        Assert.Equal(value, packed.Value);
        Assert.Equal(value.L, packed.L);
        Assert.Equal(packed, new PackedTimestamp(value));
        Assert.NotEqual(packed, new PackedTimestamp(new HLCTimestamp(int.MinValue, long.MaxValue, 0)));
    }

    // ── the coordinator key ─────────────────────────────────────────────────────

    [Fact]
    public void AnUndecidedRecord_KeepsItsCoordinatorKey_AndADecidedOneDoesNot()
    {
        InitializeTransactionCommand init = Init(Ts(1_000));
        TransactionRecord undecided = TransactionRecordStateMachine.Apply(null, init).Record!;
        Assert.Equal(init.CoordinatorKey, undecided.CoordinatorKey);

        TransactionRecord extended = TransactionRecordStateMachine.Apply(
            undecided, new ExtendDecisionDeadlineCommand(init.TransactionId, Epoch, init.ManifestHash, Ts(20_000), Ts(1_500), Ts(1_600))).Record!;
        Assert.Equal(init.CoordinatorKey, extended.CoordinatorKey);

        TransactionRecord committed = TransactionRecordStateMachine.Apply(
            undecided, new CommitTransactionCommand(init.TransactionId, Epoch, init.ManifestHash, Ts(1_200), Ts(1_300))).Record!;
        Assert.Equal(TransactionDecision.Commit, committed.Decision);
        Assert.Equal(string.Empty, committed.CoordinatorKey);

        TransactionRecord aborted = TransactionRecordStateMachine.Apply(
            undecided, new AbortTransactionCommand(init.TransactionId, Epoch, init.ManifestHash, TransactionAbortClass.Conflict,
                Ts(1_200), Ts(1_300), init.RecordAnchorKey, init.CommitTimestamp, init.DecisionDeadline, init.CreatedAt)).Record!;
        Assert.Equal(TransactionDecision.Abort, aborted.Decision);
        Assert.Equal(string.Empty, aborted.CoordinatorKey);

        // The decision changes nothing else that the record froze at its initialization.
        foreach (TransactionRecord decided in new[] { committed, aborted })
        {
            Assert.Equal(undecided.TransactionId, decided.TransactionId);
            Assert.Equal(undecided.Epoch, decided.Epoch);
            Assert.Same(undecided.RecordAnchorKey, decided.RecordAnchorKey);
            Assert.Equal(undecided.CommitTimestamp, decided.CommitTimestamp);
            Assert.Equal(undecided.DecisionDeadline, decided.DecisionDeadline);
            Assert.Equal(undecided.ManifestHash, decided.ManifestHash);
            Assert.Same(undecided.Participants, decided.Participants);
            Assert.Equal(undecided.CreatedAt, decided.CreatedAt);
            Assert.True(decided.ManifestPresent);
            Assert.Equal(Ts(1_200), decided.WinningOpId);
            Assert.Equal(Ts(1_300), decided.DecidedAt);
        }
    }

    [Fact]
    public void ADecidedRecord_StillAnswersAReplayOfItsDecisionAndOfItsInitialization()
    {
        InitializeTransactionCommand init = Init(Ts(1_000));
        CommitTransactionCommand commit = new(init.TransactionId, Epoch, init.ManifestHash, Ts(1_200), Ts(1_300));

        TransactionRecord committed = TransactionRecordStateMachine.Apply(TransactionRecordStateMachine.Apply(null, init).Record!, commit).Record!;

        Assert.Equal(TransactionApplyOutcome.IdempotentNoop, TransactionRecordStateMachine.Apply(committed, commit).Outcome);
        Assert.Equal(TransactionApplyOutcome.IdempotentNoop, TransactionRecordStateMachine.Apply(committed, init).Outcome);
        Assert.Equal(TransactionApplyOutcome.Rejected, TransactionRecordStateMachine.Apply(committed,
            new AbortTransactionCommand(init.TransactionId, Epoch, init.ManifestHash, TransactionAbortClass.Conflict,
                Ts(1_400), Ts(1_500), init.RecordAnchorKey, init.CommitTimestamp, init.DecisionDeadline, init.CreatedAt)).Outcome);
    }

    [Fact]
    public void ASnapshotOfADecidedRecordWithACoordinatorKey_LoadsWithoutTheKey()
    {
        // The form a version before this rule wrote: a decided record that still names its coordinator.
        TransactionRecord decided = new(
            Ts(1_000), Epoch, "coordinator", Anchor, Ts(1_100), Ts(9_000), 42,
            [new TransactionParticipantRef(Anchor, KeyValueDurability.Persistent)], ManifestPresent: true,
            TransactionDecision.Commit, TransactionAbortClass.None, Ts(1_200), Ts(1_002), Ts(1_300));
        TransactionRecord undecided = decided with
        {
            TransactionId = Ts(2_000), Decision = TransactionDecision.Undecided, WinningOpId = HLCTimestamp.Zero, DecidedAt = HLCTimestamp.Zero
        };

        IReadOnlyList<TransactionRecord> loaded = TransactionRecordStore.DeserializeRecords(TransactionRecordStore.SerializeRecords([decided, undecided]));

        Assert.Equal(string.Empty, loaded[0].CoordinatorKey);
        Assert.Equal(TransactionDecision.Commit, loaded[0].Decision);
        Assert.Equal(decided.WinningOpId, loaded[0].WinningOpId);
        Assert.Equal(decided.DecidedAt, loaded[0].DecidedAt);
        Assert.Equal("coordinator", loaded[1].CoordinatorKey);
    }

    [Fact]
    public void ADecidedRecord_MovesToAnotherStoreWithItsDecision()
    {
        TransactionRecordStore source = new();
        InitializeTransactionCommand init = Init(Ts(1_000));
        source.Apply(init);
        source.Apply(new CommitTransactionCommand(init.TransactionId, Epoch, init.ManifestHash, Ts(1_200), Ts(1_300)));

        TransactionRecord moved = source.Get(init.TransactionId, Epoch)!;
        Assert.Equal(string.Empty, moved.CoordinatorKey);

        // The hand-off of a range move: the destination applies the transitions that rebuild the record.
        TransactionRecordStore destination = new();
        destination.Replicate(Partition, new RaftLog
        {
            LogType = ReplicationTypes.TransactionRecord,
            LogData = (byte[])TransactionRecordStore.SerializeReconstructionDelta([moved]).Clone()
        });

        TransactionRecord rebuilt = destination.Get(init.TransactionId, Epoch)!;
        Assert.Equal(TransactionDecision.Commit, rebuilt.Decision);
        Assert.Equal(moved.WinningOpId, rebuilt.WinningOpId);
        Assert.Equal(moved.DecidedAt, rebuilt.DecidedAt);
        Assert.Equal(moved.ManifestHash, rebuilt.ManifestHash);
        Assert.Equal(moved.CommitTimestamp, rebuilt.CommitTimestamp);
        Assert.Equal(moved.DecisionDeadline, rebuilt.DecisionDeadline);
        Assert.Equal(moved.CreatedAt, rebuilt.CreatedAt);
        Assert.Equal(moved.RecordAnchorKey, rebuilt.RecordAnchorKey);
        Assert.Equal(moved.Participants, rebuilt.Participants);
        Assert.Equal(string.Empty, rebuilt.CoordinatorKey);
    }

    // ── the participants and the strings of one store ───────────────────────────

    [Fact]
    public void ADecodedRecord_HoldsItsParticipantsInAnArray_AndOneStringForTheAnchorAndItsParticipant()
    {
        TransactionRecordStore store = new();
        InitializeTransactionCommand init = Init(Ts(1_000));

        store.Replicate(Partition, DecodedEntry(init));

        TransactionRecord record = store.Get(init.TransactionId, Epoch)!;

        TransactionParticipantRef[] participants = Assert.IsType<TransactionParticipantRef[]>(record.Participants);
        Assert.Equal(
            new TransactionParticipantRef[] { new(Anchor, KeyValueDurability.Persistent), new(Other, KeyValueDurability.Persistent) },
            participants);

        // The anchor is the key of the first participant: one string, not two.
        Assert.Same(record.RecordAnchorKey, participants[0].Key);
        Assert.NotSame(init.RecordAnchorKey, record.RecordAnchorKey);
    }

    [Fact]
    public void TwoDecodedRecordsOfOneKey_HoldOneString()
    {
        TransactionRecordStore store = new();

        store.Replicate(Partition, DecodedEntry(Init(Ts(1_000))));
        store.Replicate(Partition, DecodedEntry(Init(Ts(2_000))));

        TransactionRecord first = store.Get(Ts(1_000), Epoch)!;
        TransactionRecord second = store.Get(Ts(2_000), Epoch)!;

        Assert.Same(first.RecordAnchorKey, second.RecordAnchorKey);
        Assert.Same(first.Participants[1].Key, second.Participants[1].Key);
    }

    [Fact]
    public void ARecordSnapshotReadWithTheStringsOfAStore_HoldsTheStringsOfThatStore()
    {
        TransactionRecordStore store = new();
        store.Replicate(Partition, DecodedEntry(Init(Ts(1_000))));
        TransactionRecord held = store.Get(Ts(1_000), Epoch)!;

        byte[] snapshot = TransactionRecordStore.SerializeRecords([held with { TransactionId = Ts(5_000) }]);

        TransactionRecord shared = Assert.Single(TransactionRecordStore.ReadRecords(new MemoryStream(snapshot), store.KeyStrings));
        Assert.Same(held.RecordAnchorKey, shared.RecordAnchorKey);
        Assert.Same(held.Participants[1].Key, shared.Participants[1].Key);
        Assert.IsType<TransactionParticipantRef[]>(shared.Participants);

        // A reader that does not keep the records gets strings of its own.
        TransactionRecord unshared = Assert.Single(TransactionRecordStore.ReadRecords(new MemoryStream(snapshot)));
        Assert.NotSame(held.RecordAnchorKey, unshared.RecordAnchorKey);
        Assert.Equal(held.RecordAnchorKey, unshared.RecordAnchorKey);
    }

    [Fact]
    public void TheRecordEstimate_CountsTheAnchorOnce()
    {
        TransactionRecordStore store = new();
        InitializeTransactionCommand init = Init(Ts(1_000), coordinatorKey: "");
        store.Replicate(Partition, DecodedEntry(init));

        TransactionRecord decoded = store.Get(init.TransactionId, Epoch)!;

        // The same record with an anchor string of its own retains one more string.
        TransactionRecord withOwnAnchor = decoded with { RecordAnchorKey = Copy(Anchor) };

        Assert.Equal(26 + 2 * Anchor.Length, withOwnAnchor.EstimateBytes() - decoded.EstimateBytes());
    }

    // ── the manifest that the coordinator freezes ───────────────────────────────

    [Fact]
    public void TheFrozenManifest_IsAnArrayOfTheExactSize_AndSharesItsStringsWithTheIntents()
    {
        SharedKeyStrings keyStrings = new();

        // The node already holds a string for each of the two keys, from an earlier transaction.
        string heldAnchor = keyStrings.Share(Copy(Anchor));
        string heldOther = keyStrings.Share(Copy(Other));

        // The session holds strings of its own, decoded from its requests.
        string sessionAnchor = Copy(Anchor);
        string sessionOther = Copy(Other);
        (string, KeyValueDurability)[] modifiedKeys = [(sessionOther, KeyValueDurability.Persistent), (sessionAnchor, KeyValueDurability.Persistent)];
        Dictionary<string, StagedValue> staged = new()
        {
            [sessionAnchor] = new StagedValue([1], KeyValueState.Set, 1, ExpiresMs: 0, NoRevision: false),
            [sessionOther] = new StagedValue([2], KeyValueState.Set, 1, ExpiresMs: 0, NoRevision: false)
        };

        static DurableKeyRoute Locate(string key) => new(Partition, 7L, RangeStartKey: null, IsKeyRange: true);

        Assert.True(DurableFinalizeInputBuilder.TryBuild(
            Ts(1_000), Epoch, "coordinator", sessionAnchor, Ts(1_100), Ts(9_000), modifiedKeys, staged, Locate,
            out DurableFinalizeInput? input, keyStrings: keyStrings));

        TransactionParticipantRef[] manifest = Assert.IsType<TransactionParticipantRef[]>(input!.Manifest);
        Assert.Equal(2, manifest.Length);
        Assert.Same(heldOther, manifest[0].Key);
        Assert.Same(heldAnchor, manifest[1].Key);
        Assert.Same(heldAnchor, input.RecordAnchorKey);

        // The intents carry the same strings, in the order of the manifest.
        IReadOnlyList<PreparedIntent> intents = Assert.Single(input.Partitions).Intents;
        Assert.Same(heldOther, intents[0].Key);
        Assert.Same(heldAnchor, intents[1].Key);
        Assert.Same(heldAnchor, intents[0].RecordAnchorKey);
        Assert.Equal(new byte[] { 2 }, intents[0].Value);
        Assert.Equal(new byte[] { 1 }, intents[1].Value);

        // The manifest hash reads the characters, so it is the hash of a manifest built with other strings.
        Assert.True(DurableFinalizeInputBuilder.TryBuild(
            Ts(1_000), Epoch, "coordinator", sessionAnchor, Ts(1_100), Ts(9_000), modifiedKeys, staged, Locate,
            out DurableFinalizeInput? unshared));
        Assert.Equal(unshared!.ManifestHash, input.ManifestHash);
        Assert.Same(sessionAnchor, unshared.RecordAnchorKey);
        Assert.Same(sessionOther, unshared.Manifest[0].Key);
    }

    // ── the strings of the three stores of one node ─────────────────────────────

    [Fact]
    public void TheStoresOfOneNode_HoldOneStringForAKey()
    {
        SharedKeyStrings keyStrings = new();
        TransactionRecordStore records = new(null, null, null, keyStrings);
        CompletionReceiptStore receipts = new(null, "rev", NullLogger<IKahuna>.Instance, keyStrings);
        PreparedIntentStore intents = new(null, null, null, keyStrings);

        HLCTimestamp txId = Ts(1_000);
        records.Replicate(Partition, DecodedEntry(Init(txId)));
        TransactionRecord record = records.Get(txId, Epoch)!;
        string anchor = record.RecordAnchorKey;
        string other = record.Participants[1].Key;

        // Each store receives strings that its own log entry decoded.
        receipts.Record(txId, Copy(Anchor), Copy(Anchor), KeyValueDurability.Persistent);
        receipts.Record(txId, Copy(Other), Copy(Anchor), KeyValueDurability.Persistent);
        Settle(intents, txId, Anchor);
        Settle(intents, txId, Other);

        foreach (CompletionReceiptRecord receipt in receipts.SnapshotRange(null, null))
        {
            Assert.Same(receipt.Key == Anchor ? anchor : other, receipt.Key);
            Assert.Same(anchor, receipt.RecordAnchorKey);
        }

        Assert.Equal(2, receipts.Count);

        IReadOnlyList<PreparedIntentStore.SettledIdentityEntry> settled = intents.SnapshotSettledIdentities(Partition);
        Assert.Equal(2, settled.Count);
        foreach (PreparedIntentStore.SettledIdentityEntry identity in settled)
            Assert.Same(identity.Key == Anchor ? anchor : other, identity.Key);

        IReadOnlyList<(string Key, long Revision, KeyValueState State, HLCTimestamp CommittedAt)> heads = intents.SnapshotLedger(Partition);
        Assert.Equal(2, heads.Count);
        foreach ((string key, _, _, _) in heads)
            Assert.Same(key == Anchor ? anchor : other, key);
    }

    [Fact]
    public void AReceipt_CountsAStringOnlyWhenItBroughtTheStringToTheNode()
    {
        const int entryBytes = 88;
        int anchorBytes = 26 + 2 * Anchor.Length;
        int otherBytes = 26 + 2 * Other.Length;

        // The record came first: it holds the strings and counts them, so each receipt counts its entry alone.
        SharedKeyStrings keyStrings = new();
        TransactionRecordStore records = new(null, null, null, keyStrings);
        CompletionReceiptStore afterRecord = new(null, "rev", NullLogger<IKahuna>.Instance, keyStrings);

        records.Replicate(Partition, DecodedEntry(Init(Ts(1_000))));
        afterRecord.Record(Ts(1_000), Copy(Anchor), Copy(Anchor), KeyValueDurability.Persistent);
        afterRecord.Record(Ts(1_000), Copy(Other), Copy(Anchor), KeyValueDurability.Persistent);

        Assert.Equal(2 * entryBytes, afterRecord.EstimatedBytes);

        // No record on this node (it does not host the anchor partition): the first receipt of a string counts
        // it, and the anchor that the two receipts share is counted once.
        CompletionReceiptStore alone = new();

        alone.Record(Ts(1_000), Copy(Other), Copy(Anchor), KeyValueDurability.Persistent);
        Assert.Equal(entryBytes + otherBytes + anchorBytes, alone.EstimatedBytes);

        alone.Record(Ts(1_000), Copy(Anchor), Copy(Anchor), KeyValueDurability.Persistent);
        Assert.Equal(2 * entryBytes + otherBytes + anchorBytes, alone.EstimatedBytes);

        // Each removal takes back what its receipt added.
        Assert.True(alone.Forget(Ts(1_000), Anchor));
        Assert.Equal(entryBytes + otherBytes + anchorBytes, alone.EstimatedBytes);
        Assert.True(alone.Forget(Ts(1_000), Other));
        Assert.Equal(0, alone.EstimatedBytes);
        Assert.Equal(0, alone.Count);

        Assert.Equal(2, afterRecord.CollectExpired(Ts(1_000_000), TimeSpan.FromSeconds(5)));
        Assert.Equal(0, afterRecord.EstimatedBytes);
    }

    [Fact]
    public void AReceipt_StillAnswersForItsKeyDurabilityAndAnchor()
    {
        CompletionReceiptStore receipts = new();
        receipts.Record(Ts(1_000), Copy(Other), Copy(Anchor), KeyValueDurability.Persistent);

        Assert.True(receipts.Contains(Ts(1_000), Copy(Other), KeyValueDurability.Persistent));
        Assert.True(receipts.Contains(Ts(1_000), Copy(Other), KeyValueDurability.Persistent, Copy(Anchor)));
        Assert.False(receipts.Contains(Ts(1_000), Copy(Other), KeyValueDurability.Ephemeral));
        Assert.False(receipts.Contains(Ts(1_000), Copy(Other), KeyValueDurability.Persistent, Other));
        Assert.False(receipts.Contains(Ts(1_001), Copy(Other), KeyValueDurability.Persistent));
        Assert.False(receipts.Contains(Ts(1_000), Copy(Anchor), KeyValueDurability.Persistent));
    }

    [Fact]
    public void TheLedgerEstimate_CountsTheKeyOfASettledIdentityOnlyWhenTheIdentityBroughtIt()
    {
        const int identityBytes = 94;

        // A store with strings of its own: the head of the key is recorded before the identity, and the head
        // counts the key string, so the identity counts its entry alone.
        PreparedIntentStore intents = new();
        Settle(intents, Ts(1_000), Anchor);

        long afterFirst = intents.CommittedHeadLedgerBytes;

        // A second transaction on the same key adds one identity and no head.
        Settle(intents, Ts(2_000), Anchor);
        Assert.Equal(identityBytes, intents.CommittedHeadLedgerBytes - afterFirst);
        Assert.Equal(2, intents.SettledIdentityCount);

        // A settle that finds no intent and no head brings the string itself, and counts it.
        intents.Apply(new RemoveIntentCommand(Ts(3_000), Epoch, Copy(Other)), Partition);
        Assert.Equal(2 * identityBytes + 26 + 2 * Other.Length, intents.CommittedHeadLedgerBytes - afterFirst);
    }
}
