using Google.Protobuf;
using Grpc.Core;
using Kahuna.Communication.External.Grpc;
using Kahuna.Server.Communication;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kahuna.Server.Tests;

/// <summary>
/// The wire form of a range move's quiesce confirmation. The interval's bounds are optional on the wire, and an
/// absent bound means that side's infinity: it must reach the node as null, never as an empty key, or the node
/// would judge another interval than the one the move quiesced. The owner is a full timestamp and must survive
/// the wire in all three parts, because the node compares it with the stamp on its range map.
/// </summary>
public class TestConfirmRangeQuiesceGrpc
{
    // A physical component above int.MaxValue and a large counter, so a field that is too narrow or two fields
    // that are swapped change the value.
    private static readonly HLCTimestamp Owner = new(3, 5_000_000_000L, 4_000_000_123u);

    private static KeyValuesService NewService(RecordingKahuna node) => new(node, NodeTransportGate.Disabled, NullLogger<IKahuna>.Instance);

    private static T WireRoundTrip<T>(T message, MessageParser<T> parser) where T : IMessage<T> =>
        parser.ParseFrom(message.ToByteArray());

    [Fact]
    public async Task BoundsAndOwner_SurviveTheWire_AndTheAnswerComesBack()
    {
        GrpcConfirmRangeQuiesceRequest request = new()
        {
            PartitionId   = 7,
            KeySpace      = "t:r",
            StartKey      = "t:r/m",
            EndKey        = "t:r/t",
            OwnerNode     = Owner.N,
            OwnerPhysical = Owner.L,
            OwnerCounter  = Owner.C
        };

        RecordingKahuna node = new() { Answer = KeyValueResponseType.Locked };

        GrpcConfirmRangeQuiesceResponse response = await NewService(node).ConfirmRangeQuiesce(
            WireRoundTrip(request, GrpcConfirmRangeQuiesceRequest.Parser), new StubServerCallContext());

        Assert.Equal(7, node.PartitionId);
        Assert.Equal("t:r", node.KeySpace);
        Assert.Equal("t:r/m", node.StartKey);
        Assert.Equal("t:r/t", node.EndKey);
        Assert.Equal(Owner, node.Owner);

        Assert.Equal(KeyValueResponseType.Locked, (KeyValueResponseType)WireRoundTrip(response, GrpcConfirmRangeQuiesceResponse.Parser).Type);
    }

    [Fact]
    public async Task AbsentBounds_ReachTheNodeAsNull()
    {
        GrpcConfirmRangeQuiesceRequest request = new() { PartitionId = 2, KeySpace = "t:r" };

        RecordingKahuna node = new() { Answer = KeyValueResponseType.MustRetry, StartKey = "unset", EndKey = "unset" };

        GrpcConfirmRangeQuiesceResponse response = await NewService(node).ConfirmRangeQuiesce(
            WireRoundTrip(request, GrpcConfirmRangeQuiesceRequest.Parser), new StubServerCallContext());

        Assert.Null(node.StartKey);
        Assert.Null(node.EndKey);
        Assert.Equal(HLCTimestamp.Zero, node.Owner);

        Assert.Equal(KeyValueResponseType.MustRetry, (KeyValueResponseType)WireRoundTrip(response, GrpcConfirmRangeQuiesceResponse.Parser).Type);
    }

    private sealed class RecordingKahuna : FakeKahunaBase
    {
        public KeyValueResponseType Answer;

        public int PartitionId;

        public string? KeySpace;

        public string? StartKey;

        public string? EndKey;

        public HLCTimestamp Owner;

        public override Task<KeyValueResponseType> ConfirmRangeQuiesce(
            int partitionId, string keySpace, string? startKey, string? endKey, HLCTimestamp owner, CancellationToken ct)
        {
            PartitionId = partitionId;
            KeySpace = keySpace;
            StartKey = startKey;
            EndKey = endKey;
            Owner = owner;

            return Task.FromResult(Answer);
        }
    }

    /// <summary>Minimal context: the handler reads only the cancellation token.</summary>
    private sealed class StubServerCallContext : ServerCallContext
    {
        protected override CancellationToken CancellationTokenCore => CancellationToken.None;
        protected override string MethodCore => "test";
        protected override string HostCore => "test";
        protected override string PeerCore => "test";
        protected override DateTime DeadlineCore => DateTime.MaxValue;
        protected override Metadata RequestHeadersCore => new();
        protected override Metadata ResponseTrailersCore => new();
        protected override Status StatusCore { get; set; }
        protected override WriteOptions? WriteOptionsCore { get; set; }
        protected override AuthContext AuthContextCore => throw new NotSupportedException();
        protected override ContextPropagationToken CreatePropagationTokenCore(ContextPropagationOptions? options) => throw new NotSupportedException();
        protected override Task WriteResponseHeadersAsyncCore(Metadata responseHeaders) => throw new NotSupportedException();
    }
}
