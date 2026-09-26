using Google.Protobuf;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Pitr;
using Kahuna.Server.Replication;
using Kahuna.Server.Replication.Protos;
using Kahuna.Shared.KeyValue;
using Kommander.Data;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// Guards the contract between the producers and the consumers of the key-value log. A request type that a
/// producer logs must be applied by every consumer — the replicator on live apply, the restorer on restart replay,
/// and the point-in-time restore engine — or the write is skipped as an unknown message and silently lost on the
/// node that replays it. That happened more than once when each consumer kept its own list of types.
///
/// <para>The consumers now dispatch on <see cref="KeyValueRecordKind"/> from one classification, and producers
/// take the logged number from <see cref="KeyValueMessageDecoder.ToLoggedType"/>, which refuses a type outside the
/// mutation kinds. These tests hold both ends: every request type must be classified, every logged type must be
/// applied by the restore paths, and no producer may log a type the consumers do not apply. The theories derive
/// their cases from the enum, so a newly logged type is covered with no edit here.</para>
/// </summary>
public sealed class TestLoggedRecordTypeCoverage
{
    private const int PartitionId = 3;

    private const string Key = "coverage/1";

    private const long Revision = 9;

    private static bool IsLogged(KeyValueRequestType type) =>
        KeyValueMessageDecoder.Classify(type) is KeyValueRecordKind.ValueMutation or KeyValueRecordKind.ByReferenceMutation;

    public static TheoryData<KeyValueRequestType> LoggedTypes()
    {
        TheoryData<KeyValueRequestType> data = new();
        foreach (KeyValueRequestType type in Enum.GetValues<KeyValueRequestType>())
            if (IsLogged(type))
                data.Add(type);
        return data;
    }

    public static TheoryData<KeyValueRequestType> NotLoggedTypes()
    {
        TheoryData<KeyValueRequestType> data = new();
        foreach (KeyValueRequestType type in Enum.GetValues<KeyValueRequestType>())
            if (KeyValueMessageDecoder.Classify(type) == KeyValueRecordKind.NotLogged)
                data.Add(type);
        return data;
    }

    private static PreparedIntent Intent() =>
        new(
            TransactionId: new HLCTimestamp(0, 1_000, 0), Epoch: 1, Key: Key, ManifestHash: 42, RecordAnchorKey: "anchor",
            CommitTimestamp: new HLCTimestamp(0, 1_234, 0),
            State: KeyValueState.Set, Value: [7, 8, 9], Bucket: null, Revision: Revision, Expires: HLCTimestamp.Zero,
            NoRevision: false, BaseRevision: Revision - 1, BaseState: KeyValueState.Set,
            RecoveryDeadline: new HLCTimestamp(0, 6_000, 0), Resolution: PreparedIntentResolution.Committed);

    /// <summary>
    /// Builds the log record a producer writes for <paramref name="type"/>, the prepared intent a by-reference
    /// record needs on the consuming node, and the state the replay must write.
    /// </summary>
    private static (byte[] Record, PreparedIntent? Intent, KeyValueState Expected) RecordFor(KeyValueRequestType type)
    {
        if (KeyValueMessageDecoder.Classify(type) == KeyValueRecordKind.ByReferenceMutation)
        {
            PreparedIntent intent = Intent();
            byte[] byReference = PreparedIntentMaterializer.ToKeyValueRecord(intent, new KeyValueMessage(), byReference: true);
            Assert.Equal((int)type, ReplicationSerializer.UnserializeKeyValueMessage(byReference).Type);
            return (byReference, intent, intent.State);
        }

        KeyValueMessage message = new()
        {
            Type = KeyValueMessageDecoder.ToLoggedType(type),
            Key = Key,
            Revision = Revision,
            Value = UnsafeByteOperations.UnsafeWrap(new byte[] { 7, 8, 9 }),
            LastModifiedPhysical = 1_234
        };

        // The expected state must be a real one: a type classified as a value mutation that decodes to no state
        // would otherwise replay as Undefined and match itself.
        KeyValueState expected = KeyValueMessageDecoder.Decode(message).state;
        Assert.True(expected != KeyValueState.Undefined, $"{type} is logged as a value mutation but decodes to no state");

        return (ReplicationSerializer.Serialize(message), null, expected);
    }

    private static RaftLog KvLog(byte[] record) =>
        new() { Id = 10, Type = RaftLogType.Committed, LogType = ReplicationTypes.KeyValues, LogData = [.. record] };

    [Fact]
    public void EveryRequestTypeHasARecordKind()
    {
        foreach (KeyValueRequestType type in Enum.GetValues<KeyValueRequestType>())
            Assert.True(
                KeyValueMessageDecoder.Classify(type) != KeyValueRecordKind.Unknown,
                $"{type} ({(int)type}) has no record kind. Classify it in KeyValueMessageDecoder.Classify: NotLogged " +
                "if no producer writes it into a log, otherwise the mutation kind every consumer must apply it as.");
    }

    [Fact]
    public void EveryValueMutationDecodesToAState()
    {
        foreach (KeyValueRequestType type in Enum.GetValues<KeyValueRequestType>())
        {
            if (KeyValueMessageDecoder.Classify(type) != KeyValueRecordKind.ValueMutation)
                continue;

            (KeyValueState state, _) = KeyValueMessageDecoder.Decode(new KeyValueMessage { Type = (int)type, Key = Key });
            Assert.True(state != KeyValueState.Undefined, $"{type} is a value mutation but KeyValueMessageDecoder.Decode gives it no state");
        }
    }

    [Theory]
    [MemberData(nameof(NotLoggedTypes))]
    public void ProducersCannotLogATypeNoConsumerApplies(KeyValueRequestType type)
    {
        Assert.Throws<InvalidOperationException>(() => KeyValueMessageDecoder.ToLoggedType(type));
    }

    [Theory]
    [MemberData(nameof(LoggedTypes))]
    public void ProducersLogAMutationTypeUnderItsOwnNumber(KeyValueRequestType type)
    {
        Assert.Equal((int)type, KeyValueMessageDecoder.ToLoggedType(type));
    }

    [Theory]
    [MemberData(nameof(LoggedTypes))]
    public void Restorer_AppliesEveryLoggedType(KeyValueRequestType type)
    {
        (KeyValueRestorer restorer, UnflushedKeyValueWritesIndex overlay, PreparedIntentStore intents, IDisposable lifetime) =
            KeyValueRestorerHarness.Build(out _);

        using (lifetime)
        {
            (byte[] record, PreparedIntent? intent, KeyValueState expected) = RecordFor(type);

            // The prepare delta replays before the by-reference record on a real node.
            if (intent is not null)
                intents.Apply(new PrepareIntentCommand(intent));

            Assert.True(restorer.Restore(PartitionId, KvLog(record)));

            Assert.True(overlay.TryGet(Key, out UnflushedKeyValueWrite replayed), $"the restorer skipped a {type} record");
            Assert.Equal(Revision, replayed.Revision);
            Assert.Equal(expected, replayed.State);
        }
    }

    [Theory]
    [MemberData(nameof(LoggedTypes))]
    public void PointInTimeRestore_AppliesEveryLoggedType(KeyValueRequestType type)
    {
        (byte[] record, PreparedIntent? intent, KeyValueState expected) = RecordFor(type);

        Dictionary<PreparedIntentIdentity, PreparedIntent> liveIntents = new();
        if (intent is not null)
            liveIntents[new PreparedIntentIdentity(intent.TransactionId, intent.Epoch, intent.Key)] = intent;

        (PersistenceRequestItem item, HLCTimestamp _)? decoded =
            RestoreEngine.ToRequestItem(WalSegmentEntry.From(KvLog(record)), liveIntents, Guid.Empty);

        Assert.True(decoded.HasValue, $"point-in-time restore skipped a {type} record");
        Assert.Equal(Key, decoded.Value.item.Key);
        Assert.Equal(Revision, decoded.Value.item.Revision);
        Assert.Equal((int)expected, decoded.Value.item.State);
    }
}
