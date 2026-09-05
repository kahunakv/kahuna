using Google.Protobuf;
using Kahuna.Communication.External.Grpc;
using Kahuna.Communication.External.Grpc.KeyValues;
using Kahuna.Server.Communication.Internode;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions.Data;

namespace Kahuna.Server.Tests;

/// <summary>
/// The staged committed-value effect must survive the inter-node gRPC wire, so a registered completion forwarded
/// to a remote coordinator carries the staged mutation and finalizes through the durable-intent path instead of
/// falling back to the manual ticket path. Covers the payload↔proto mapping symmetry (the explicit set-vs-delete
/// state, the deletion vs empty-value vs valueless-set distinctions, and the old-sender state fallback), and a
/// full protobuf serialize/parse of the completion request carrying the effects.
/// </summary>
public sealed class TestStagedMutationGrpcWire
{
    private static void AssertRoundTrips(StagedMutationEffect original)
    {
        GrpcStagedMutationEffect grpc = GrpcInterNodeCommunication.ToGrpcStagedMutation(original);

        // Serialize through a real proto round trip so the wire encoding (including the optional-value presence
        // bit) is exercised, not just the in-memory mapping.
        GrpcStagedMutationEffect onWire = GrpcStagedMutationEffect.Parser.ParseFrom(grpc.ToByteArray());

        StagedMutationEffect restored = KeyValuesService.FromGrpcStagedMutation(onWire);

        Assert.Equal(original.Key, restored.Key);
        Assert.Equal(original.State, restored.State);
        Assert.Equal(original.Revision, restored.Revision);
        Assert.Equal(original.ExpiresMs, restored.ExpiresMs);
        Assert.Equal(original.NoRevision, restored.NoRevision);

        if (original.Value is null)
            Assert.Null(restored.Value); // an absent value stays absent, never an empty one
        else
        {
            Assert.NotNull(restored.Value);
            Assert.Equal(original.Value, restored.Value);
        }
    }

    [Fact]
    public void StagedMutation_ValuePresent_RoundTrips()
        => AssertRoundTrips(new StagedMutationEffect("acct/1", "hello world"u8.ToArray(), KeyValueState.Set, Revision: 3, ExpiresMs: 60_000, NoRevision: true));

    [Fact]
    public void StagedMutation_Deletion_NullValue_StaysNull()
        => AssertRoundTrips(new StagedMutationEffect("acct/2", Value: null, KeyValueState.Deleted, Revision: 4, ExpiresMs: 0, NoRevision: false));

    [Fact]
    public void StagedMutation_EmptyValue_StaysEmptyNotNull()
        => AssertRoundTrips(new StagedMutationEffect("acct/3", Value: [], KeyValueState.Set, Revision: 5, ExpiresMs: 0, NoRevision: false));

    [Fact]
    public void StagedMutation_ValuelessSet_StaysSet_NotDeletion()
        => AssertRoundTrips(new StagedMutationEffect("acct/4", Value: null, KeyValueState.Set, Revision: 6, ExpiresMs: 0, NoRevision: false));

    [Fact]
    public void StagedMutation_OldSender_UndefinedState_FallsBackToValuePresence()
    {
        // A sender from a build without the State field leaves it at STATE_UNDEFINED. The decode then falls
        // back to the old presence rule, so mixed-version completions keep their pre-field meaning.
        GrpcStagedMutationEffect deletionShaped = new() { Key = "old/1", Revision = 1 };
        Assert.Equal(KeyValueState.Deleted, KeyValuesService.FromGrpcStagedMutation(deletionShaped).State);

        GrpcStagedMutationEffect setShaped = new() { Key = "old/2", Revision = 2, Value = ByteString.CopyFrom([7]) };
        Assert.Equal(KeyValueState.Set, KeyValuesService.FromGrpcStagedMutation(setShaped).State);
    }

    [Fact]
    public void CompleteOperationRequest_CarriesStagedMutations_ThroughProtobuf()
    {
        GrpcCompleteOperationRequest request = new()
        {
            CoordinatorKey = "coord",
            TransactionIdNode = 1,
            TransactionIdPhysical = 200,
            TransactionIdCounter = 0
        };
        request.StagedMutations.Add(GrpcInterNodeCommunication.ToGrpcStagedMutation(
            new StagedMutationEffect("k1", "v1"u8.ToArray(), KeyValueState.Set, 1, 0, false)));
        request.StagedMutations.Add(GrpcInterNodeCommunication.ToGrpcStagedMutation(
            new StagedMutationEffect("k2", null, KeyValueState.Deleted, 2, 0, true)));
        request.StagedMutations.Add(GrpcInterNodeCommunication.ToGrpcStagedMutation(
            new StagedMutationEffect("k3", null, KeyValueState.Set, 3, 0, false))); // valueless set

        GrpcCompleteOperationRequest parsed = GrpcCompleteOperationRequest.Parser.ParseFrom(request.ToByteArray());

        Assert.Equal(3, parsed.StagedMutations.Count);
        Assert.Equal("k1", parsed.StagedMutations[0].Key);
        Assert.True(parsed.StagedMutations[0].HasValue);
        Assert.Equal("v1"u8.ToArray(), parsed.StagedMutations[0].Value.ToByteArray());
        Assert.Equal(GrpcKeyValueState.StateSet, parsed.StagedMutations[0].State);
        Assert.Equal("k2", parsed.StagedMutations[1].Key);
        Assert.False(parsed.StagedMutations[1].HasValue); // deletion carries no value
        Assert.True(parsed.StagedMutations[1].NoRevision);
        Assert.Equal(GrpcKeyValueState.StateDeleted, parsed.StagedMutations[1].State);
        Assert.Equal("k3", parsed.StagedMutations[2].Key);
        Assert.False(parsed.StagedMutations[2].HasValue);
        Assert.Equal(GrpcKeyValueState.StateSet, parsed.StagedMutations[2].State); // the state keeps it a set
    }
}
