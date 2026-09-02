using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Replication;
using Kahuna.Server.Replication.Protos;
using Kahuna.Shared.KeyValue;
using Kommander.Data;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// A prepared-intent delta normally carries one transaction attempt's transitions for one partition, so the
/// transaction identity, manifest hash, commit timestamp, recovery deadline and record-anchor key are identical on
/// every command. Those are encoded once for the batch instead of once per key.
///
/// <para>The compaction must be invisible: a delta that is compacted, one whose commands disagree and so is not, and
/// one written before the compact form existed must all reconstruct exactly the same transitions.</para>
/// </summary>
public sealed class TestPreparedIntentDeltaEncoding
{
    private const int PartitionId = 3;

    private static HLCTimestamp Ts(long l) => new(0, l, 0);

    private static PreparedIntent Intent(HLCTimestamp txId, long epoch, string key, string anchor = "anchor") =>
        new(txId, epoch, key, ManifestHash: 4242, RecordAnchorKey: anchor, CommitTimestamp: Ts(1100),
            State: KeyValueState.Set, Value: [7, 8, 9], Bucket: "bucket", Revision: 5, Expires: Ts(50000),
            NoRevision: false, BaseRevision: 4, BaseState: KeyValueState.Set, RecoveryDeadline: Ts(6000),
            Resolution: PreparedIntentResolution.Pending);

    // Copy the serialized bytes so the apply cannot recognize them as this process's own proposal and must decode
    // them — the follower's view, which is exactly what these encoding tests exist to exercise.
    private static RaftLog IntentLog(params PreparedIntentCommand[] commands) =>
        new() { LogType = ReplicationTypes.PreparedIntent, LogData = [.. PreparedIntentStore.SerializeDelta(commands)] };

    [Fact]
    public void SingleTransactionBatch_RoundTripsEveryField()
    {
        HLCTimestamp txId = Ts(1000);
        const long epoch = 3;

        PreparedIntentCommand[] commands =
        [
            new PrepareIntentCommand(Intent(txId, epoch, "row/1")),
            new PrepareIntentCommand(Intent(txId, epoch, "row/2")),
            new PrepareIntentCommand(Intent(txId, epoch, "row/3"))
        ];

        PreparedIntentStore store = new();
        Assert.True(store.Replicate(PartitionId, IntentLog(commands)));

        foreach (string key in new[] { "row/1", "row/2", "row/3" })
        {
            PreparedIntent? intent = store.Get(key);
            Assert.NotNull(intent);

            // The hoisted fields are what would silently corrupt if the header were mis-read.
            Assert.Equal(txId, intent!.TransactionId);
            Assert.Equal(epoch, intent.Epoch);
            Assert.Equal(4242, intent.ManifestHash);
            Assert.Equal("anchor", intent.RecordAnchorKey);
            Assert.Equal(Ts(1100), intent.CommitTimestamp);
            Assert.Equal(Ts(6000), intent.RecoveryDeadline);

            // The per-command fields must survive alongside them.
            Assert.Equal(key, intent.Key);
            Assert.Equal(KeyValueState.Set, intent.State);
            Assert.Equal(new byte[] { 7, 8, 9 }, intent.Value);
            Assert.Equal("bucket", intent.Bucket);
            Assert.Equal(5, intent.Revision);
            Assert.Equal(Ts(50000), intent.Expires);
            Assert.Equal(4, intent.BaseRevision);
        }
    }

    [Fact]
    public void SingleTransactionBatch_IsSmallerThanOneThatCannotBeCompacted()
    {
        HLCTimestamp txId = Ts(1000);
        const long epoch = 3;

        // Same shape and the same anchor-key length either way; only agreement differs, so the size gap is exactly
        // what hoisting the shared fields buys.
        PreparedIntentCommand[] shared = new PreparedIntentCommand[16];
        PreparedIntentCommand[] divergent = new PreparedIntentCommand[16];

        for (int i = 0; i < 16; i++)
        {
            shared[i] = new PrepareIntentCommand(Intent(txId, epoch, $"row/{i:D2}", "anchor00"));
            divergent[i] = new PrepareIntentCommand(Intent(txId, epoch, $"row/{i:D2}", $"anchor{i:D2}"));
        }

        int sharedBytes = PreparedIntentStore.SerializeDelta(shared).Length;
        int divergentBytes = PreparedIntentStore.SerializeDelta(divergent).Length;

        Assert.True(sharedBytes < divergentBytes,
            $"expected the compacted delta to be smaller, got shared={sharedBytes} divergent={divergentBytes}");
    }

    [Fact]
    public void MixedTransactionBatch_IsNotCompactedAndStillRoundTrips()
    {
        HLCTimestamp txA = Ts(1000);
        HLCTimestamp txB = Ts(2000);

        PreparedIntentStore store = new();
        Assert.True(store.Replicate(PartitionId, IntentLog(
            new PrepareIntentCommand(Intent(txA, 1, "row/a")),
            new PrepareIntentCommand(Intent(txB, 7, "row/b")))));

        // Each key keeps its own transaction identity — a header applied across disagreeing commands would collapse
        // both onto one transaction and silently misattribute an intent.
        Assert.Equal(txA, store.Get("row/a")!.TransactionId);
        Assert.Equal(1, store.Get("row/a")!.Epoch);
        Assert.Equal(txB, store.Get("row/b")!.TransactionId);
        Assert.Equal(7, store.Get("row/b")!.Epoch);
    }

    [Fact]
    public void MixedKindBatch_HoistsOnlyWhatEveryCommandAgreesOn()
    {
        HLCTimestamp txId = Ts(1000);
        const long epoch = 2;

        PreparedIntentStore store = new();
        Assert.True(store.Replicate(PartitionId, IntentLog(
            new PrepareIntentCommand(Intent(txId, epoch, "row/1")),
            new PrepareIntentCommand(Intent(txId, epoch, "row/2")),
            new ResolveIntentCommand(txId, epoch, "row/1", Commit: true))));

        // The resolve targeted the same identity, so it must have matched the live intent rather than been rejected
        // as belonging to a different transaction.
        Assert.Equal(PreparedIntentResolution.Committed, store.Get("row/1")!.Resolution);
        Assert.Equal(PreparedIntentResolution.Pending, store.Get("row/2")!.Resolution);
    }

    [Fact]
    public void LocallyProposedDelta_AppliesIdenticallyToItsDecodedForm()
    {
        HLCTimestamp txId = Ts(1000);
        const long epoch = 3;

        PreparedIntentCommand[] commands =
        [
            new PrepareIntentCommand(Intent(txId, epoch, "row/1")),
            new PrepareIntentCommand(Intent(txId, epoch, "row/2")),
            new ResolveIntentCommand(txId, epoch, "row/1", Commit: true)
        ];

        byte[] data = PreparedIntentStore.SerializeDelta(commands);

        // The proposing node applies the very byte array it produced (Raft hands it back on commit) and may reuse
        // the pre-serialization commands; a follower decodes a fresh copy of the same bytes. Both must land on the
        // same state, or leader and follower silently diverge.
        PreparedIntentStore proposer = new();
        Assert.True(proposer.Replicate(PartitionId, new RaftLog { LogType = ReplicationTypes.PreparedIntent, LogData = data }));

        PreparedIntentStore follower = new();
        Assert.True(follower.Replicate(PartitionId, new RaftLog { LogType = ReplicationTypes.PreparedIntent, LogData = [.. data] }));

        foreach (string key in new[] { "row/1", "row/2" })
        {
            PreparedIntent? local = proposer.Get(key);
            PreparedIntent? decoded = follower.Get(key);
            Assert.NotNull(local);
            Assert.NotNull(decoded);

            // Record equality compares the value array by reference; compare its content separately and the rest
            // of the record with the array reference neutralized.
            Assert.Equal(decoded!.Value, local!.Value);
            Assert.Equal(decoded, local with { Value = decoded.Value });
        }

        Assert.Equal(PreparedIntentResolution.Committed, proposer.Get("row/1")!.Resolution);
        Assert.Equal(PreparedIntentResolution.Pending, proposer.Get("row/2")!.Resolution);

        // Re-applying the same entry (redelivery/replay) must stay an idempotent no-op. Whether the second
        // apply reuses the registered commands (take budget left by co-hosted nodes) or falls back to
        // decoding, the state must not change.
        Assert.True(proposer.Replicate(PartitionId, new RaftLog { LogType = ReplicationTypes.PreparedIntent, LogData = data }));
        Assert.Equal(PreparedIntentResolution.Committed, proposer.Get("row/1")!.Resolution);
    }

    /// <summary>The proposal path registers the serialized delta's decoded commands against the produced
    /// byte array, so a local apply of the exact same array reuses the producer's instances instead of
    /// decoding. Observable through identity: the serializer aliases the intent's value array into the
    /// wire bytes, while a decoded intent carries a fresh copy.</summary>
    [Fact]
    public void ProposalBytes_ReuseTheProducersCommandsOnLocalApply()
    {
        PrepareIntentCommand prepare = new(Intent(Ts(1000), 3, "row/reuse"));
        byte[] data = PreparedIntentStore.SerializeDelta([prepare]);

        PreparedIntentStore store = new();
        Assert.True(store.Replicate(PartitionId, new RaftLog { LogType = ReplicationTypes.PreparedIntent, LogData = data }));

        Assert.Same(prepare.Intent.Value, store.Get("row/reuse")!.Value);
    }

    /// <summary>The live decoder reads the wire directly; the generated proto messages remain as the
    /// reference decoder. Decodes a delta through both and compares, with every descriptor field of the
    /// prepare carrying a non-default value, so a field the direct reader fails to consume — for example
    /// one newly added to the proto — fails here instead of being silently dropped. The negative profile
    /// forces the ten-byte varint form of every int32/int64 field, where a truncation mismatch between
    /// the decoders would hide.</summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void DirectReader_MatchesReferenceDecoderOnEveryDescriptorField(bool negative)
    {
        PreparedIntentCommandMessage prepare = new();

        foreach (Google.Protobuf.Reflection.FieldDescriptor field in PreparedIntentCommandMessage.Descriptor.Fields.InDeclarationOrder())
        {
            object nonDefault = field.FieldType switch
            {
                Google.Protobuf.Reflection.FieldType.Enum => Enum.ToObject(field.EnumType.ClrType, field.EnumType.Values[1].Number),
                Google.Protobuf.Reflection.FieldType.Int32 => negative ? -42 - field.FieldNumber : 42 + field.FieldNumber,
                Google.Protobuf.Reflection.FieldType.Int64 => negative ? -42L - field.FieldNumber : 42L + field.FieldNumber,
                Google.Protobuf.Reflection.FieldType.UInt32 => negative ? uint.MaxValue - (uint)field.FieldNumber : 42u + (uint)field.FieldNumber,
                Google.Protobuf.Reflection.FieldType.Bool => true,
                Google.Protobuf.Reflection.FieldType.String => negative ? $"fïeld-µ{field.FieldNumber}" : $"field-{field.FieldNumber}",
                Google.Protobuf.Reflection.FieldType.Bytes => Google.Protobuf.ByteString.CopyFrom((byte)field.FieldNumber, 2, 3),
                _ => throw new InvalidOperationException(
                    $"field '{field.Name}' has unhandled type {field.FieldType}; extend this sweep and both decoders together")
            };

            field.Accessor.SetValue(prepare, nonDefault);
        }

        // A prepare consumes every payload field; the null markers must stay false so the value and the
        // bucket flow into the decoded intent instead of being nulled away.
        prepare.Kind = PreparedIntentCommandKindMessage.PreparedIntentPrepare;
        prepare.ValueNull = false;
        prepare.BucketNull = false;

        PreparedIntentDeltaMessage delta = new();
        delta.Commands.Add(prepare);
        delta.Commands.Add(new PreparedIntentCommandMessage
        {
            Kind = PreparedIntentCommandKindMessage.PreparedIntentResolve,
            TransactionIdNode = 1, TransactionIdPhysical = 2, TransactionIdCounter = 3,
            Epoch = 4, Key = "row/resolve", Commit = true
        });

        // Per-command form: the commands disagree, so this delta carries no shared header.
        AssertDecodersAgree(ReplicationSerializer.Serialize(delta));
    }

    /// <summary>Compacted-form twin of the descriptor sweep: the writer hoists the shared header, so the
    /// direct reader's header binding is compared against the reference decoder too.</summary>
    [Fact]
    public void DirectReader_MatchesReferenceDecoderOnACompactedDelta()
    {
        HLCTimestamp txId = Ts(1000);
        const long epoch = 3;

        AssertDecodersAgree(PreparedIntentStore.SerializeDelta([
            new PrepareIntentCommand(Intent(txId, epoch, "row/1")),
            new PrepareIntentCommand(Intent(txId, epoch, "row/2")),
            new ResolveIntentCommand(txId, epoch, "row/1", Commit: true)]));
    }

    private static void AssertDecodersAgree(byte[] bytes)
    {
        PreparedIntentCommand[] direct = PreparedIntentStore.DecodeDelta(bytes);
        PreparedIntentDeltaMessage reference = ReplicationSerializer.UnserializePreparedIntentDeltaMessage(bytes);

        Assert.Equal(reference.Commands.Count, direct.Length);

        for (int i = 0; i < direct.Length; i++)
        {
            PreparedIntentCommand expected = PreparedIntentStore.ToCommand(reference.Commands[i], reference.Header);

            if (expected is PrepareIntentCommand expectedPrepare && direct[i] is PrepareIntentCommand directPrepare)
            {
                // Record equality compares the value array by reference; compare its content separately and
                // the rest of the record with the array reference neutralized.
                Assert.Equal(expectedPrepare.Intent.Value, directPrepare.Intent.Value);
                Assert.Equal(expectedPrepare.Intent, directPrepare.Intent with { Value = expectedPrepare.Intent.Value });
            }
            else
                Assert.Equal(expected, direct[i]);
        }
    }

    /// <summary>The serializer recycles proto command messages across deltas on the same thread. A prepare fills
    /// every payload field; a later settle command reusing that message sets only its own few, so any field the
    /// recycle path failed to reset would leak the earlier prepare's payload into the settle's wire bytes. Serialize
    /// a fully populated prepare first, then a settle on the same thread, and assert at the wire level that the
    /// settle commands carry nothing but their own fields.</summary>
    [Fact]
    public void RecycledMessages_DoNotLeakAcrossDeltas()
    {
        HLCTimestamp txId = Ts(1000);
        const long epoch = 3;

        // Fully populates the pooled messages (value, bucket, anchor, timestamps, revisions ...).
        PreparedIntentStore.SerializeDelta([
            new PrepareIntentCommand(Intent(txId, epoch, "row/1")),
            new PrepareIntentCommand(Intent(txId, epoch, "row/2"))]);

        // Same thread, so this delta is built on the recycled messages.
        byte[] settleBytes = PreparedIntentStore.SerializeDelta([
            new ResolveIntentCommand(txId, epoch, "row/1", Commit: true),
            new RemoveIntentCommand(txId, epoch, "row/1")]);

        PreparedIntentDeltaMessage decoded = ReplicationSerializer.UnserializePreparedIntentDeltaMessage([.. settleBytes]);
        Assert.Equal(2, decoded.Commands.Count);

        foreach (PreparedIntentCommandMessage command in decoded.Commands)
        {
            // Prepare payload must be entirely absent from a resolve/remove command.
            Assert.Equal(0, command.ManifestHash);
            Assert.Equal(string.Empty, command.RecordAnchorKey);
            Assert.Equal(0, command.CommitTimestampPhysical);
            Assert.Equal(0, command.State);
            Assert.True(command.Value.IsEmpty);
            Assert.False(command.ValueNull);
            Assert.Equal(string.Empty, command.Bucket);
            Assert.False(command.BucketNull);
            Assert.Equal(0, command.Revision);
            Assert.Equal(0, command.ExpiresPhysical);
            Assert.Equal(0, command.BaseRevision);
            Assert.Equal(0, command.RecoveryDeadlinePhysical);
            Assert.Equal(0, command.Resolution);
        }

        Assert.Equal(PreparedIntentCommandKindMessage.PreparedIntentResolve, decoded.Commands[0].Kind);
        Assert.True(decoded.Commands[0].Commit);
        Assert.Equal(PreparedIntentCommandKindMessage.PreparedIntentRemove, decoded.Commands[1].Kind);
        Assert.False(decoded.Commands[1].Commit);
    }

    /// <summary>Every field of the command message must return to its proto3 default when recycled — the fill
    /// paths set only the fields their kind carries and trust the rest to be clean. Sweeps the message descriptor
    /// so a field added to the proto but missed by the reset fails here instead of leaking payload on the wire.</summary>
    [Fact]
    public void ResetCommandMessage_RestoresEveryFieldToDefault()
    {
        PreparedIntentCommandMessage message = new();

        foreach (Google.Protobuf.Reflection.FieldDescriptor field in PreparedIntentCommandMessage.Descriptor.Fields.InDeclarationOrder())
        {
            object nonDefault = field.FieldType switch
            {
                Google.Protobuf.Reflection.FieldType.Enum => Enum.ToObject(field.EnumType.ClrType, field.EnumType.Values[1].Number),
                Google.Protobuf.Reflection.FieldType.Int32 => 42,
                Google.Protobuf.Reflection.FieldType.Int64 => 42L,
                Google.Protobuf.Reflection.FieldType.UInt32 => 42u,
                Google.Protobuf.Reflection.FieldType.Bool => true,
                Google.Protobuf.Reflection.FieldType.String => "leak",
                Google.Protobuf.Reflection.FieldType.Bytes => Google.Protobuf.ByteString.CopyFrom(1, 2, 3),
                _ => throw new InvalidOperationException(
                    $"field '{field.Name}' has unhandled type {field.FieldType}; extend this sweep and ResetCommandMessage together")
            };

            field.Accessor.SetValue(message, nonDefault);
        }

        Assert.NotEqual(new PreparedIntentCommandMessage(), message);

        PreparedIntentStore.ResetCommandMessage(message);

        Assert.Equal(new PreparedIntentCommandMessage(), message);
    }

    [Fact]
    public void DeltaWrittenWithoutAHeader_IsStillRead()
    {
        HLCTimestamp txId = Ts(1000);
        const long epoch = 4;

        // The form written before the header existed, and the form still written whenever commands disagree: every
        // field lives on the command. Built here directly so the reader is exercised against it explicitly rather
        // than only through whatever the current writer happens to emit.
        PreparedIntentDeltaMessage delta = new();
        delta.Commands.Add(new PreparedIntentCommandMessage
        {
            Kind = PreparedIntentCommandKindMessage.PreparedIntentPrepare,
            TransactionIdNode = txId.N, TransactionIdPhysical = txId.L, TransactionIdCounter = txId.C,
            Epoch = epoch, Key = "row/legacy",
            ManifestHash = 4242, RecordAnchorKey = "anchor",
            CommitTimestampNode = Ts(1100).N, CommitTimestampPhysical = Ts(1100).L, CommitTimestampCounter = Ts(1100).C,
            State = (int)KeyValueState.Set,
            Value = Google.Protobuf.ByteString.CopyFrom(7, 8, 9), ValueNull = false,
            Bucket = "bucket", BucketNull = false,
            Revision = 5,
            ExpiresNode = Ts(50000).N, ExpiresPhysical = Ts(50000).L, ExpiresCounter = Ts(50000).C,
            NoRevision = false,
            BaseRevision = 4, BaseState = (int)KeyValueState.Set,
            RecoveryDeadlineNode = Ts(6000).N, RecoveryDeadlinePhysical = Ts(6000).L, RecoveryDeadlineCounter = Ts(6000).C,
            Resolution = (int)PreparedIntentResolution.Pending
        });

        PreparedIntentStore store = new();
        Assert.True(store.Replicate(PartitionId, new RaftLog
        {
            LogType = ReplicationTypes.PreparedIntent,
            LogData = ReplicationSerializer.Serialize(delta)
        }));

        PreparedIntent? intent = store.Get("row/legacy");
        Assert.NotNull(intent);
        Assert.Equal(txId, intent!.TransactionId);
        Assert.Equal(epoch, intent.Epoch);
        Assert.Equal(4242, intent.ManifestHash);
        Assert.Equal("anchor", intent.RecordAnchorKey);
        Assert.Equal(Ts(1100), intent.CommitTimestamp);
        Assert.Equal(Ts(6000), intent.RecoveryDeadline);
        Assert.Equal(new byte[] { 7, 8, 9 }, intent.Value);
    }
}
