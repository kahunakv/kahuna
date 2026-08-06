using Google.Protobuf;
using Grpc.Core;

using Kahuna.Communication.External.Grpc;
using Kahuna.Shared.Sequences;

namespace Kahuna.Server.Tests;

/// <summary>
/// Every non-success sequence reply carries a <c>default</c> allocation, whose name is null. Protobuf string
/// fields reject null, so a naive conversion throws inside the gRPC handler and the caller — a node that
/// forwarded the request to the sequence's owner — sees an opaque transport failure instead of NotFound /
/// MustRetry / MaxValueExceeded. That turns a routine "the sequence does not exist" into an unretryable
/// server error precisely when redirects are most common. These tests drive the real gRPC entry points and
/// round-trip the reply through protobuf so the failure types survive the wire intact.
/// </summary>
public sealed class TestSequenceAllocationGrpcWire
{
    private static readonly SequenceResponseType[] NonSuccessTypes =
    [
        SequenceResponseType.NotFound,
        SequenceResponseType.MustRetry,
        SequenceResponseType.MaxValueExceeded,
        SequenceResponseType.AlreadyExists,
        SequenceResponseType.InvalidInput,
        SequenceResponseType.Aborted,
        SequenceResponseType.Error
    ];

    public static TheoryData<SequenceResponseType, bool> NonSuccessCases()
    {
        TheoryData<SequenceResponseType, bool> data = new();
        foreach (SequenceResponseType type in NonSuccessTypes)
        {
            data.Add(type, false);
            data.Add(type, true);
        }
        return data;
    }

    [Theory]
    [MemberData(nameof(NonSuccessCases))]
    public async Task NextSequenceValue_NonSuccess_ReturnsItsTypeOverTheWire(SequenceResponseType type, bool forwarded)
    {
        SequencesService service = new(new FixedSequenceResultKahuna(type, default));

        GrpcSequenceAllocationResponse response = await service.NextSequenceValue(
            new GrpcNextSequenceRequest { Name = "missing" }, Context(forwarded));

        GrpcSequenceAllocationResponse onWire = GrpcSequenceAllocationResponse.Parser.ParseFrom(response.ToByteArray());

        Assert.Equal((GrpcSequenceResponseType)type, onWire.Type);
        Assert.Null(onWire.Allocation);
    }

    [Theory]
    [MemberData(nameof(NonSuccessCases))]
    public async Task ReserveSequenceRange_NonSuccess_ReturnsItsTypeOverTheWire(SequenceResponseType type, bool forwarded)
    {
        SequencesService service = new(new FixedSequenceResultKahuna(type, default));

        GrpcSequenceAllocationResponse response = await service.ReserveSequenceRange(
            new GrpcReserveSequenceRangeRequest { Name = "missing", Count = 5 }, Context(forwarded));

        GrpcSequenceAllocationResponse onWire = GrpcSequenceAllocationResponse.Parser.ParseFrom(response.ToByteArray());

        Assert.Equal((GrpcSequenceResponseType)type, onWire.Type);
        Assert.Null(onWire.Allocation);
    }

    [Fact]
    public async Task Success_CarriesTheWholeAllocationOverTheWire()
    {
        SequenceAllocation allocation = new("orders", 41, 50, 10, 7);
        SequencesService service = new(new FixedSequenceResultKahuna(SequenceResponseType.Success, allocation));

        GrpcSequenceAllocationResponse response = await service.ReserveSequenceRange(
            new GrpcReserveSequenceRangeRequest { Name = "orders", Count = 10 }, Context(forwarded: true));

        GrpcSequenceAllocationResponse onWire = GrpcSequenceAllocationResponse.Parser.ParseFrom(response.ToByteArray());

        Assert.Equal(GrpcSequenceResponseType.SequenceSuccess, onWire.Type);
        Assert.NotNull(onWire.Allocation);
        Assert.Equal(allocation.Name, onWire.Allocation.Name);
        Assert.Equal(allocation.Start, onWire.Allocation.Start);
        Assert.Equal(allocation.End, onWire.Allocation.End);
        Assert.Equal(allocation.Count, onWire.Allocation.Count);
        Assert.Equal(allocation.Revision, onWire.Allocation.Revision);
    }

    /// <summary>A default allocation is what every failure path produces; converting it must never throw.</summary>
    [Fact]
    public void BuildAllocationResponse_DefaultAllocation_OmitsIt()
    {
        GrpcSequenceAllocationResponse response = SequencesService.BuildAllocationResponse(
            SequenceResponseType.NotFound, default, Kommander.Diagnostics.ValueStopwatch.StartNew());

        Assert.Equal(GrpcSequenceResponseType.SequenceNotFound, response.Type);
        Assert.Null(response.Allocation);
    }

    private static ServerCallContext Context(bool forwarded) => new StubServerCallContext(forwarded);

    /// <summary>Answers both the routed and the already-forwarded entry points with one fixed result.</summary>
    private sealed class FixedSequenceResultKahuna : FakeKahunaBase
    {
        private readonly (SequenceResponseType, SequenceAllocation) result;

        public FixedSequenceResultKahuna(SequenceResponseType type, SequenceAllocation allocation)
        {
            result = (type, allocation);
        }

        public override Task<(SequenceResponseType, SequenceAllocation)> LocateAndNextSequenceValue(
            string name, string? idempotencyKey, SequenceDurability durability, CancellationToken cancellationToken)
            => Task.FromResult(result);

        public override Task<(SequenceResponseType, SequenceAllocation)> NextSequenceValue(
            string name, string? idempotencyKey, SequenceDurability durability, CancellationToken cancellationToken)
            => Task.FromResult(result);

        public override Task<(SequenceResponseType, SequenceAllocation)> LocateAndReserveSequenceRange(
            string name, int count, string? idempotencyKey, SequenceDurability durability, CancellationToken cancellationToken)
            => Task.FromResult(result);

        public override Task<(SequenceResponseType, SequenceAllocation)> ReserveSequenceRange(
            string name, int count, string? idempotencyKey, SequenceDurability durability, CancellationToken cancellationToken)
            => Task.FromResult(result);
    }

    /// <summary>Minimal context: the service reads only the cancellation token and the forwarded marker.</summary>
    private sealed class StubServerCallContext : ServerCallContext
    {
        private readonly Metadata headers;

        public StubServerCallContext(bool forwarded)
        {
            headers = forwarded ? new Metadata { { "kahuna-forwarded", "1" } } : new Metadata();
        }

        protected override CancellationToken CancellationTokenCore => CancellationToken.None;
        protected override string MethodCore => "test";
        protected override string HostCore => "test";
        protected override string PeerCore => "test";
        protected override System.DateTime DeadlineCore => System.DateTime.MaxValue;
        protected override Metadata RequestHeadersCore => headers;
        protected override Metadata ResponseTrailersCore => new();
        protected override Status StatusCore { get; set; }
        protected override WriteOptions? WriteOptionsCore { get; set; }
        protected override AuthContext AuthContextCore => throw new NotSupportedException();
        protected override ContextPropagationToken CreatePropagationTokenCore(ContextPropagationOptions? options) => throw new NotSupportedException();
        protected override Task WriteResponseHeadersAsyncCore(Metadata responseHeaders) => throw new NotSupportedException();
    }
}
