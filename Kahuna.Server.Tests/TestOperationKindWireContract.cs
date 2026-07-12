using Kahuna.Server.KeyValues.Transactions.Data;

namespace Kahuna.Server.Tests;

/// <summary>
/// Guards the wire contract between the server-side <see cref="OperationKind"/> and the protobuf
/// <c>GrpcOperationKind</c>. The inter-node transport blind-casts the C# enum integer straight onto the
/// gRPC field, so the two enumerations must share identical integer values for every member. These tests
/// fail if either enum is reordered or a member is added on only one side — the exact drift that made
/// <c>DeleteMany</c> and <c>RangeLock</c> disagree across the schema.
/// </summary>
public sealed class TestOperationKindWireContract
{
    [Theory]
    [InlineData(OperationKind.Set, GrpcOperationKind.GrpcOpKindSet)]
    [InlineData(OperationKind.Delete, GrpcOperationKind.GrpcOpKindDelete)]
    [InlineData(OperationKind.DeleteMany, GrpcOperationKind.GrpcOpKindDeleteMany)]
    [InlineData(OperationKind.Extend, GrpcOperationKind.GrpcOpKindExtend)]
    [InlineData(OperationKind.Get, GrpcOperationKind.GrpcOpKindGet)]
    [InlineData(OperationKind.Exists, GrpcOperationKind.GrpcOpKindExists)]
    [InlineData(OperationKind.Scan, GrpcOperationKind.GrpcOpKindScan)]
    [InlineData(OperationKind.PointLock, GrpcOperationKind.GrpcOpKindPointLock)]
    [InlineData(OperationKind.PrefixLock, GrpcOperationKind.GrpcOpKindPrefixLock)]
    [InlineData(OperationKind.RangeLock, GrpcOperationKind.GrpcOpKindRangeLock)]
    public void OperationKind_MatchesGrpcWireValue_AndRoundTrips(OperationKind kind, GrpcOperationKind wire)
    {
        Assert.Equal((int)kind, (int)wire);
        Assert.Equal(kind, (OperationKind)(GrpcOperationKind)kind);
        Assert.Equal(wire, (GrpcOperationKind)(OperationKind)wire);
    }

    [Fact]
    public void OperationKind_EveryMemberHasAMatchingWireValue()
    {
        foreach (OperationKind kind in Enum.GetValues<OperationKind>())
            Assert.True(Enum.IsDefined((GrpcOperationKind)kind), $"OperationKind.{kind} ({(int)kind}) has no GrpcOperationKind member");
    }

    [Fact]
    public void OperationKind_UnknownWireValueFromNewerPeer_SurvivesAsItsInteger()
    {
        // A kind a newer peer sends that this node does not recognize must survive the blind cast as its
        // raw integer so digest binding and duplicate detection stay stable, rather than collapsing to a
        // recognized default.
        const int unknown = 42;
        Assert.Equal(unknown, (int)(OperationKind)unknown);
        Assert.Equal(unknown, (int)(GrpcOperationKind)unknown);
    }
}
