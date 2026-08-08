using Google.Protobuf;
using Grpc.Core;
using Kommander.Time;
using Microsoft.Extensions.Logging.Abstractions;

using Kahuna;
using Kahuna.Communication.External.Grpc;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.Communication.Rest;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.Tests;

/// <summary>
/// The admission refusal is only useful if it survives a hop. A client talking to a saturated remote node is
/// precisely the case the distinct code exists for, and a mapper that rounded it back to <c>MustRetry</c>
/// would reinstate the ambiguity — the client would spin against the saturation instead of backing off.
///
/// <para>These drive the real gRPC entry point and round-trip the reply through protobuf, so a default value
/// cannot masquerade as a real one. They also pin the caller's admission budget travelling the other way:
/// dropped on the request leg, a Begin forwarded to a leader silently falls back to that node's default and
/// the caller's own patience is lost.</para>
/// </summary>
public sealed class TestAdmissionRefusedWire
{
    private static ServerCallContext Context() => new StubServerCallContext();

    [Fact]
    public async Task AdmissionRefused_SurvivesTheGrpcHopAsItself()
    {
        RecordingStartTransactionKahuna kahuna = new(KeyValueResponseType.AdmissionRefused);
        KeyValuesService service = new(kahuna, NullLogger<IKahuna>.Instance);

        GrpcStartTransactionResponse response = await service.StartTransaction(
            new GrpcStartTransactionRequest { CoordinatorKey = "tx-1" }, Context());

        GrpcStartTransactionResponse onWire = GrpcStartTransactionResponse.Parser.ParseFrom(response.ToByteArray());

        // Named on the wire rather than an unrecognized ordinal, so a client can switch on it.
        Assert.Equal(GrpcKeyValueResponseType.TypeAdmissionRefused, onWire.Type);
        Assert.Equal(108, (int)onWire.Type);

        // And back to the C# enum the client actually reacts to. This is the cast the client performs.
        Assert.Equal(KeyValueResponseType.AdmissionRefused, (KeyValueResponseType)onWire.Type);
        Assert.NotEqual(KeyValueResponseType.MustRetry, (KeyValueResponseType)onWire.Type);
    }

    [Fact]
    public async Task ATransientRefusal_StillCrossesTheWireAsMustRetry()
    {
        // The regression that would silently make every warm-up look like load shedding.
        RecordingStartTransactionKahuna kahuna = new(KeyValueResponseType.MustRetry);
        KeyValuesService service = new(kahuna, NullLogger<IKahuna>.Instance);

        GrpcStartTransactionResponse response = await service.StartTransaction(
            new GrpcStartTransactionRequest { CoordinatorKey = "tx-2" }, Context());

        GrpcStartTransactionResponse onWire = GrpcStartTransactionResponse.Parser.ParseFrom(response.ToByteArray());

        Assert.Equal(GrpcKeyValueResponseType.TypeMustRetry, onWire.Type);
        Assert.Equal(KeyValueResponseType.MustRetry, (KeyValueResponseType)onWire.Type);
    }

    [Fact]
    public async Task TheCallersAdmissionBudget_ReachesTheServerOverGrpc()
    {
        RecordingStartTransactionKahuna kahuna = new(KeyValueResponseType.Set);
        KeyValuesService service = new(kahuna, NullLogger<IKahuna>.Instance);

        // Round-tripped through protobuf first, so the field is proven to serialize rather than merely being
        // set on an in-memory object the service then reads back.
        GrpcStartTransactionRequest request = GrpcStartTransactionRequest.Parser.ParseFrom(
            new GrpcStartTransactionRequest { CoordinatorKey = "tx-3", Timeout = 3_600_000, AdmissionWaitMs = 750 }
                .ToByteArray());

        await service.StartTransaction(request, Context());

        Assert.NotNull(kahuna.Received);
        Assert.Equal(750, kahuna.Received!.AdmissionWaitMs);

        // The two clocks stay distinct across the hop: a long session did not become a long wait.
        Assert.Equal(3_600_000, kahuna.Received.Timeout);
    }

    [Fact]
    public void TheRestContract_CarriesBothClocksSeparately()
    {
        KahunaStartTransactionRequest request = new()
        {
            CoordinatorKey = "tx-4",
            Timeout = 3_600_000,
            AdmissionWaitMs = 750
        };

        string json = System.Text.Json.JsonSerializer.Serialize(
            request, KahunaJsonContext.Default.KahunaStartTransactionRequest);

        Assert.Contains("\"admissionWaitMs\":750", json, StringComparison.Ordinal);

        KahunaStartTransactionRequest roundTripped = System.Text.Json.JsonSerializer.Deserialize(
            json, KahunaJsonContext.Default.KahunaStartTransactionRequest)!;

        Assert.Equal(750, roundTripped.AdmissionWaitMs);
        Assert.Equal(3_600_000, roundTripped.Timeout);
    }

    [Fact]
    public void TheRestResponseContract_CarriesAdmissionRefused()
    {
        KahunaStartTransactionResponse response = new() { Type = KeyValueResponseType.AdmissionRefused };

        string json = System.Text.Json.JsonSerializer.Serialize(
            response, KahunaJsonContext.Default.KahunaStartTransactionResponse);

        KahunaStartTransactionResponse roundTripped = System.Text.Json.JsonSerializer.Deserialize(
            json, KahunaJsonContext.Default.KahunaStartTransactionResponse)!;

        Assert.Equal(KeyValueResponseType.AdmissionRefused, roundTripped.Type);
        Assert.NotEqual(KeyValueResponseType.MustRetry, roundTripped.Type);
    }

    /// <summary>Answers Begin with one fixed outcome and keeps the options it was handed.</summary>
    private sealed class RecordingStartTransactionKahuna : FakeKahunaBase
    {
        private readonly KeyValueResponseType type;

        public KeyValueTransactionOptions? Received { get; private set; }

        public RecordingStartTransactionKahuna(KeyValueResponseType type)
        {
            this.type = type;
        }

        public override Task<(KeyValueResponseType, Shared.KeyValue.TransactionHandle)> LocateAndStartTransaction(
            KeyValueTransactionOptions options, CancellationToken cancellationToken)
        {
            Received = options;

            return Task.FromResult((type, new Shared.KeyValue.TransactionHandle(HLCTimestamp.Zero, options.CoordinatorKey)));
        }
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
