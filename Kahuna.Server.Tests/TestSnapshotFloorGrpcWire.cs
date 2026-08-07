using Google.Protobuf;
using Grpc.Core;
using Kommander.Time;
using Microsoft.Extensions.Logging.Abstractions;

using Kahuna.Communication.External.Grpc;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.Tests;

/// <summary>
/// The snapshot-floor gRPC response must carry a response type over the wire. A refusal
/// (MustRetry — no node with confirmed meta-partition leadership could answer) that loses its
/// type on the wire deserializes as an empty success: zero live holds and a Zero floor, the value
/// that means "reclaim anything". These tests drive the real gRPC entry point and round-trip the
/// reply through protobuf so proto defaults cannot masquerade as real values.
/// </summary>
public sealed class TestSnapshotFloorGrpcWire
{
    private static readonly HLCTimestamp Floor = new(3, 5_000_000_000L, 4_000_000_123u);

    private static ServerCallContext Context() => new StubServerCallContext();

    [Fact]
    public async Task GetSnapshotFloor_AuthoritativeAnswer_CarriesTypeFloorAndCountOverTheWire()
    {
        KeyValuesService service = new(
            new FixedFloorKahuna((KeyValueResponseType.Get, Floor, 3)), NullLogger<IKahuna>.Instance);

        GrpcGetSnapshotFloorResponse response = await service.GetSnapshotFloor(
            new GrpcGetSnapshotFloorRequest(), Context());

        GrpcGetSnapshotFloorResponse onWire = GrpcGetSnapshotFloorResponse.Parser.ParseFrom(response.ToByteArray());

        Assert.Equal(GrpcKeyValueResponseType.TypeGot, onWire.Type);
        Assert.Equal(Floor, new HLCTimestamp(onWire.EffectiveFloorNode, onWire.EffectiveFloorPhysical, onWire.EffectiveFloorCounter));
        Assert.Equal(3, onWire.LiveHolds);
    }

    [Fact]
    public async Task GetSnapshotFloor_Refusal_SurvivesWireAsMustRetry()
    {
        KeyValuesService service = new(
            new FixedFloorKahuna((KeyValueResponseType.MustRetry, HLCTimestamp.Zero, 0)), NullLogger<IKahuna>.Instance);

        GrpcGetSnapshotFloorResponse response = await service.GetSnapshotFloor(
            new GrpcGetSnapshotFloorRequest(), Context());

        GrpcGetSnapshotFloorResponse onWire = GrpcGetSnapshotFloorResponse.Parser.ParseFrom(response.ToByteArray());

        // The refusal must be distinguishable from "no holds anywhere".
        Assert.Equal(GrpcKeyValueResponseType.TypeMustRetry, onWire.Type);
    }

    private sealed class FixedFloorKahuna((KeyValueResponseType, HLCTimestamp, int) result) : FakeKahunaBase
    {
        public override Task<(KeyValueResponseType Type, HLCTimestamp EffectiveFloor, int LiveHolds)>
            GetSnapshotFloor(CancellationToken ct) => Task.FromResult(result);
    }

    /// <summary>Minimal context: the service reads only the cancellation token.</summary>
    private sealed class StubServerCallContext : ServerCallContext
    {
        protected override CancellationToken CancellationTokenCore => CancellationToken.None;
        protected override string MethodCore => "test";
        protected override string HostCore => "test";
        protected override string PeerCore => "test";
        protected override System.DateTime DeadlineCore => System.DateTime.MaxValue;
        protected override Metadata RequestHeadersCore => new();
        protected override Metadata ResponseTrailersCore => new();
        protected override Status StatusCore { get; set; }
        protected override WriteOptions? WriteOptionsCore { get; set; }
        protected override AuthContext AuthContextCore => throw new NotSupportedException();
        protected override ContextPropagationToken CreatePropagationTokenCore(ContextPropagationOptions? options) => throw new NotSupportedException();
        protected override Task WriteResponseHeadersAsyncCore(Metadata responseHeaders) => throw new NotSupportedException();
    }
}
