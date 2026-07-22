using Google.Protobuf;
using Kahuna.Communication.External.Grpc;
using Kahuna.Communication.External.Grpc.KeyValues;
using Kahuna.Server.Communication.Internode;
using Kahuna.Server.KeyValues.Transactions.Data;

namespace Kahuna.Server.Tests;

/// <summary>
/// The staged committed-value effect must survive the inter-node gRPC wire, so a registered completion forwarded
/// to a remote coordinator carries the staged mutation and finalizes through the durable-intent path instead of
/// falling back to the manual ticket path. Covers the payload↔proto mapping symmetry (including the deletion vs
/// empty-value distinction), and a full protobuf serialize/parse of the completion request carrying the effects.
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
        Assert.Equal(original.Revision, restored.Revision);
        Assert.Equal(original.ExpiresMs, restored.ExpiresMs);
        Assert.Equal(original.NoRevision, restored.NoRevision);

        if (original.Value is null)
            Assert.Null(restored.Value); // a deletion stays a deletion, never an empty value
        else
        {
            Assert.NotNull(restored.Value);
            Assert.Equal(original.Value, restored.Value);
        }
    }

    [Fact]
    public void StagedMutation_ValuePresent_RoundTrips()
        => AssertRoundTrips(new StagedMutationEffect("acct/1", "hello world"u8.ToArray(), Revision: 3, ExpiresMs: 60_000, NoRevision: true));

    [Fact]
    public void StagedMutation_Deletion_NullValue_StaysNull()
        => AssertRoundTrips(new StagedMutationEffect("acct/2", Value: null, Revision: 4, ExpiresMs: 0, NoRevision: false));

    [Fact]
    public void StagedMutation_EmptyValue_StaysEmptyNotNull()
        => AssertRoundTrips(new StagedMutationEffect("acct/3", Value: [], Revision: 5, ExpiresMs: 0, NoRevision: false));

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
            new StagedMutationEffect("k1", "v1"u8.ToArray(), 1, 0, false)));
        request.StagedMutations.Add(GrpcInterNodeCommunication.ToGrpcStagedMutation(
            new StagedMutationEffect("k2", null, 2, 0, true))); // deletion

        GrpcCompleteOperationRequest parsed = GrpcCompleteOperationRequest.Parser.ParseFrom(request.ToByteArray());

        Assert.Equal(2, parsed.StagedMutations.Count);
        Assert.Equal("k1", parsed.StagedMutations[0].Key);
        Assert.True(parsed.StagedMutations[0].HasValue);
        Assert.Equal("v1"u8.ToArray(), parsed.StagedMutations[0].Value.ToByteArray());
        Assert.Equal("k2", parsed.StagedMutations[1].Key);
        Assert.False(parsed.StagedMutations[1].HasValue); // deletion carries no value
        Assert.True(parsed.StagedMutations[1].NoRevision);
    }
}
