using System.Text;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Handlers;
using Kahuna.Server.Replication;
using Kahuna.Server.Replication.Protos;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// The leader applies a direct-write record it serialized itself from the proposal it still holds, not from a
/// re-parse of the bytes. That is only sound while the proposal yields the same mutation the parse would, so
/// these tests pin both halves: the serializer registers the proposal for the exact array it hands to Raft (and
/// for nothing else), and the mutation built from the proposal equals the one decoded from the record.
/// </summary>
public sealed class TestLocallyProposedKeyValueRecords
{
    private static readonly HLCTimestamp Expires = new(1, 5_000_000, 7);
    private static readonly HLCTimestamp LastUsed = new(1, 6_000_000, 8);
    private static readonly HLCTimestamp LastModified = new(1, 6_500_000, 9);
    private static readonly HLCTimestamp Now = new(2, 7_000_000, 11);

    private static KeyValueProposal Proposal(KeyValueRequestType type, byte[]? value, KeyValueState state, bool noRevision = false) =>
        new(type, "space/k", value, revision: 42, noRevision, Expires, LastUsed, LastModified, state, KeyValueDurability.Persistent);

    [Fact]
    public void Serialize_RegistersTheProposal_ForTheExactArrayOnly()
    {
        KeyValueProposal proposal = Proposal(KeyValueRequestType.TrySet, Encoding.UTF8.GetBytes("hello"), KeyValueState.Set);

        byte[] bytes = BaseHandler.SerializeProposal(KeyValueRequestType.TrySet, proposal, Now);

        // A fresh materialization of the same bytes (WAL replay, a record received over the network) must
        // parse, never inherit another array's proposal.
        Assert.False(LocallyProposedKeyValueRecords.TryTake([.. bytes], out _));

        Assert.True(LocallyProposedKeyValueRecords.TryTake(bytes, out KeyValueProposal? taken));
        Assert.Same(proposal, taken);
    }

    [Fact]
    public void Serialize_UnderAnotherType_DoesNotRegister()
    {
        KeyValueProposal proposal = Proposal(KeyValueRequestType.TrySet, Encoding.UTF8.GetBytes("v"), KeyValueState.Set);

        // The apply path reads the type from the proposal, so a record logged under a different type than the
        // proposal's own must be parsed to apply as logged.
        byte[] bytes = BaseHandler.SerializeProposal(KeyValueRequestType.TryDelete, proposal, Now);

        Assert.False(LocallyProposedKeyValueRecords.TryTake(bytes, out _));
    }

    [Theory]
    [InlineData(KeyValueRequestType.TrySet, "hello", false)]
    [InlineData(KeyValueRequestType.TrySet, "", true)]
    [InlineData(KeyValueRequestType.TryExtend, "kept", false)]
    [InlineData(KeyValueRequestType.TryDelete, null, false)]
    public void FromProposal_EqualsTheMutationDecodedFromTheRecord(KeyValueRequestType type, string? text, bool noRevision)
    {
        byte[]? value = text is null ? null : Encoding.UTF8.GetBytes(text);
        KeyValueState state = KeyValueMessageDecoder.StateOf(type);
        KeyValueProposal proposal = Proposal(type, value, state, noRevision);

        byte[] bytes = BaseHandler.SerializeProposal(type, proposal, Now);

        KeyValueMessage record = ReplicationSerializer.UnserializeKeyValueMessage(bytes);
        (KeyValueState decodedState, byte[]? decodedValue) = KeyValueMessageDecoder.Decode(record);
        CommittedKeyValueMutation fromRecord = CommittedKeyValueMutation.FromRecord(record, decodedValue, decodedState);

        CommittedKeyValueMutation fromProposal = CommittedKeyValueMutation.FromProposal(proposal, state);

        // The value is compared by content below: the parse owns a fresh copy, the proposal the original array.
        Assert.Equal(fromRecord with { Value = null }, fromProposal with { Value = null });
        Assert.Equal(fromRecord.Value, fromProposal.Value);
        Assert.False(fromProposal.IsTransactional);
    }
}
