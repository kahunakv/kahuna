using Google.Protobuf;
using Grpc.Core;
using Kahuna.Communication.External.Grpc;
using Kahuna.Server.Communication;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kahuna.Server.Tests;

/// <summary>
/// The wire form of a forwarded helping pass. The asking transaction's identity excludes its own intents from the
/// pass and must survive in all its parts. The hops the request made count against the forward budget of the node
/// that receives it: without them two nodes that each take the other for the leader would send it back and forth.
/// </summary>
public class TestSettleDecidedBlockersGrpc
{
    // A physical component above int.MaxValue and a large counter, so a field that is too narrow or two fields
    // that are swapped change the value.
    private static readonly HLCTimestamp Asker = new(3, 5_000_000_000L, 4_000_000_123u);

    private static KeyValuesService NewService(RecordingKahuna node) => new(node, NodeTransportGate.Disabled, NullLogger<IKahuna>.Instance);

    private static T WireRoundTrip<T>(T message, MessageParser<T> parser) where T : IMessage<T> =>
        parser.ParseFrom(message.ToByteArray());

    [Fact]
    public async Task RequestAndAnswer_SurviveTheWire()
    {
        GrpcSettleDecidedBlockersRequest request = new()
        {
            PartitionId           = 7,
            TransactionIdNode     = Asker.N,
            TransactionIdPhysical = Asker.L,
            TransactionIdCounter  = Asker.C,
            Epoch                 = 9_000_000_000L,
            ForwardHops           = 1
        };
        request.Keys.Add(["a/1", "", "b/2"]);

        RecordingKahuna node = new() { Answer = (KeyValueResponseType.Get, 2, 3, 4) };

        GrpcSettleDecidedBlockersResponse response = WireRoundTrip(
            await NewService(node).SettleDecidedBlockers(WireRoundTrip(request, GrpcSettleDecidedBlockersRequest.Parser), new StubServerCallContext()),
            GrpcSettleDecidedBlockersResponse.Parser);

        Assert.Equal(7, node.PartitionId);
        Assert.Equal(["a/1", "", "b/2"], node.Keys);
        Assert.Equal(Asker, node.TransactionId);
        Assert.Equal(9_000_000_000L, node.Epoch);

        Assert.Equal(KeyValueResponseType.Get, (KeyValueResponseType)response.Type);
        Assert.Equal((2, 3, 4), (response.Settled, response.Decided, response.Undecided));
    }

    [Theory]
    [InlineData(0, 1, true)]
    [InlineData(1, 1, true)]
    [InlineData(2, 2, false)]
    [InlineData(3, 3, false)]
    public async Task ForwardHops_CountAgainstTheBudgetOfTheReceiver(int sent, int observed, bool canForward)
    {
        RecordingKahuna node = new() { Answer = (KeyValueResponseType.MustRetry, 0, 0, 0) };

        GrpcSettleDecidedBlockersResponse response = await NewService(node).SettleDecidedBlockers(
            WireRoundTrip(new GrpcSettleDecidedBlockersRequest { PartitionId = 1, ForwardHops = sent }, GrpcSettleDecidedBlockersRequest.Parser),
            new StubServerCallContext());

        Assert.Equal(observed, node.Hops);
        Assert.Equal(canForward, node.CanForward);
        Assert.Equal(KeyValueResponseType.MustRetry, (KeyValueResponseType)response.Type);
    }

    private sealed class RecordingKahuna : FakeKahunaBase
    {
        public (KeyValueResponseType Type, int Settled, int Decided, int Undecided) Answer;

        public int PartitionId;

        public string[] Keys = [];

        public HLCTimestamp TransactionId;

        public long Epoch;

        public int Hops;

        public bool CanForward;

        public override Task<(KeyValueResponseType Type, int Settled, int Decided, int Undecided)> SettleDecidedBlockers(
            int partitionId, IReadOnlyList<string> keys, HLCTimestamp transactionId, long epoch, CancellationToken ct)
        {
            PartitionId = partitionId;
            Keys = [.. keys];
            TransactionId = transactionId;
            Epoch = epoch;
            Hops = ForwardedRequestScope.ChainedHops;
            CanForward = ForwardedRequestScope.CanForward;

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
