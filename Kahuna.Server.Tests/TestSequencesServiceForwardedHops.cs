using Grpc.Core;
using Kahuna.Communication.External.Grpc;
using Kahuna.Server.Communication;
using Kahuna.Shared.Sequences;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kahuna.Server.Tests;

/// <summary>
/// The gRPC sequence service serves a forwarded request under the hop count the sender stamped on
/// it, so the forward budget spans the whole chain instead of restarting at the process boundary.
/// A client request is served with no marker at all. These tests drive each entry point and read
/// the marker from inside the routed call it makes.
/// </summary>
public sealed class TestSequencesServiceForwardedHops
{
    public static TheoryData<string> Operations() => new(
        nameof(SequencesService.CreateSequence),
        nameof(SequencesService.UpdateSequence),
        nameof(SequencesService.GetSequence),
        nameof(SequencesService.NextSequenceValue),
        nameof(SequencesService.ReserveSequenceRange),
        nameof(SequencesService.DeleteSequence));

    [Theory]
    [MemberData(nameof(Operations))]
    public async Task AClientRequestIsServedWithoutTheForwardedMarker(string operation)
    {
        HopCapturingKahuna kahuna = new();
        SequencesService service = new(kahuna, NodeTransportGate.Disabled, NullLogger<IKahuna>.Instance);

        await Invoke(service, operation, forwarded: false, forwardHops: 2);

        Assert.Equal(0, kahuna.ObservedHops);
    }

    [Theory]
    [MemberData(nameof(Operations))]
    public async Task AForwardedRequestIsServedAtTheHopCountItCarries(string operation)
    {
        HopCapturingKahuna kahuna = new();
        SequencesService service = new(kahuna, NodeTransportGate.Disabled, NullLogger<IKahuna>.Instance);

        await Invoke(service, operation, forwarded: true, forwardHops: 2);

        Assert.Equal(2, kahuna.ObservedHops);
    }

    /// <summary>An older peer sends the marker without a count; the request still counts as one hop.</summary>
    [Theory]
    [MemberData(nameof(Operations))]
    public async Task AForwardedRequestWithoutACountIsServedAsOneHop(string operation)
    {
        HopCapturingKahuna kahuna = new();
        SequencesService service = new(kahuna, NodeTransportGate.Disabled, NullLogger<IKahuna>.Instance);

        await Invoke(service, operation, forwarded: true, forwardHops: 0);

        Assert.Equal(1, kahuna.ObservedHops);
    }

    /// <summary>The marker is scoped to the one call: the next request on the flow starts clean.</summary>
    [Fact]
    public async Task TheMarkerDoesNotLeakPastTheCall()
    {
        HopCapturingKahuna kahuna = new();
        SequencesService service = new(kahuna, NodeTransportGate.Disabled, NullLogger<IKahuna>.Instance);

        await Invoke(service, nameof(SequencesService.NextSequenceValue), forwarded: true, forwardHops: 2);
        Assert.Equal(2, kahuna.ObservedHops);

        await Invoke(service, nameof(SequencesService.NextSequenceValue), forwarded: false, forwardHops: 0);
        Assert.Equal(0, kahuna.ObservedHops);
    }

    private static Task Invoke(SequencesService service, string operation, bool forwarded, int forwardHops)
    {
        ServerCallContext context = new StubServerCallContext(forwarded);

        return operation switch
        {
            nameof(SequencesService.CreateSequence) => service.CreateSequence(
                new GrpcCreateSequenceRequest { Name = "orders", Increment = 1, ForwardHops = forwardHops }, context),
            nameof(SequencesService.UpdateSequence) => service.UpdateSequence(
                new GrpcUpdateSequenceRequest { Name = "orders", CurrentValue = 10, ForwardHops = forwardHops }, context),
            nameof(SequencesService.GetSequence) => service.GetSequence(
                new GrpcGetSequenceRequest { Name = "orders", ForwardHops = forwardHops }, context),
            nameof(SequencesService.NextSequenceValue) => service.NextSequenceValue(
                new GrpcNextSequenceRequest { Name = "orders", ForwardHops = forwardHops }, context),
            nameof(SequencesService.ReserveSequenceRange) => service.ReserveSequenceRange(
                new GrpcReserveSequenceRangeRequest { Name = "orders", Count = 3, ForwardHops = forwardHops }, context),
            nameof(SequencesService.DeleteSequence) => service.DeleteSequence(
                new GrpcDeleteSequenceRequest { Name = "orders", ForwardHops = forwardHops }, context),
            _ => throw new ArgumentOutOfRangeException(nameof(operation), operation, null)
        };
    }

    /// <summary>Records the forwarded marker as seen from inside each routed entry point.</summary>
    private sealed class HopCapturingKahuna : FakeKahunaBase
    {
        public int ObservedHops { get; private set; } = -1;

        private void Observe() => ObservedHops = ForwardedRequestScope.ChainedHops;

        public override Task<(SequenceResponseType, long)> LocateAndCreateSequence(
            string name, long initialValue, long increment, long? maxValue, int? blockSize, SequenceDurability durability, CancellationToken cancellationToken)
        {
            Observe();
            return Task.FromResult((SequenceResponseType.Success, 1L));
        }

        public override Task<(SequenceResponseType, long)> LocateAndUpdateSequence(
            string name, SequenceUpdate update, SequenceDurability durability, CancellationToken cancellationToken)
        {
            Observe();
            return Task.FromResult((SequenceResponseType.Success, 1L));
        }

        public override Task<(SequenceResponseType, ReadOnlySequenceEntry?)> LocateAndGetSequence(
            string name, SequenceDurability durability, CancellationToken cancellationToken)
        {
            Observe();
            return Task.FromResult<(SequenceResponseType, ReadOnlySequenceEntry?)>((SequenceResponseType.NotFound, null));
        }

        public override Task<(SequenceResponseType, SequenceAllocation)> LocateAndNextSequenceValue(
            string name, string? idempotencyKey, SequenceDurability durability, CancellationToken cancellationToken)
        {
            Observe();
            return Task.FromResult((SequenceResponseType.Success, new SequenceAllocation(name, 1, 1, 1, 1)));
        }

        public override Task<(SequenceResponseType, SequenceAllocation)> LocateAndReserveSequenceRange(
            string name, int count, string? idempotencyKey, SequenceDurability durability, CancellationToken cancellationToken)
        {
            Observe();
            return Task.FromResult((SequenceResponseType.Success, new SequenceAllocation(name, 1, count, count, 1)));
        }

        public override Task<SequenceResponseType> LocateAndDeleteSequence(
            string name, SequenceDurability durability, CancellationToken cancellationToken)
        {
            Observe();
            return Task.FromResult(SequenceResponseType.Success);
        }
    }

    /// <summary>Minimal context: the service reads only the cancellation token and the forwarded marker.</summary>
    private sealed class StubServerCallContext : ServerCallContext
    {
        private readonly Metadata headers;

        public StubServerCallContext(bool forwarded)
        {
            headers = forwarded ? new Metadata { { InterNodeHeaders.Forwarded, "1" } } : new Metadata();
        }

        protected override CancellationToken CancellationTokenCore => CancellationToken.None;
        protected override string MethodCore => "test";
        protected override string HostCore => "test";
        protected override string PeerCore => "test";
        protected override DateTime DeadlineCore => DateTime.MaxValue;
        protected override Metadata RequestHeadersCore => headers;
        protected override Metadata ResponseTrailersCore => new();
        protected override Status StatusCore { get; set; }
        protected override WriteOptions? WriteOptionsCore { get; set; }
        protected override AuthContext AuthContextCore => throw new NotSupportedException();
        protected override ContextPropagationToken CreatePropagationTokenCore(ContextPropagationOptions? options) => throw new NotSupportedException();
        protected override Task WriteResponseHeadersAsyncCore(Metadata responseHeaders) => throw new NotSupportedException();
    }
}
