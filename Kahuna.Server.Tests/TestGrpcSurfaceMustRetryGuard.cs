using Google.Protobuf;
using Grpc.Core;
using Kommander;
using Kommander.Time;
using Microsoft.Extensions.Logging.Abstractions;

using Kahuna;
using Kahuna.Communication.External.Grpc;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Configuration;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Locks;
using Kahuna.Shared.Sequences;

namespace Kahuna.Server.Tests;

/// <summary>
/// A retryable infrastructure failure — a Raft resolution failure, or an inter-node transport failure
/// such as a dead pooled HTTP/2 connection reporting Internal — means no definitive answer was
/// produced, and a client retry loop must be told exactly that. Without a guard on the gRPC surface
/// those escape the service method and the framework answers status Unknown, which the SDK does not
/// retry; the caller sees a hard failure for a condition that is, by contract, MustRetry. Genuine
/// bugs must keep surfacing as errors, or a real server fault would be retried forever.
/// </summary>
public sealed class TestGrpcSurfaceMustRetryGuard
{
    public static TheoryData<Exception> RetryableEscapes() =>
    [
        new RaftException("Invalid partition: 3"),
        new RpcException(new Status(StatusCode.Unavailable, "response ended prematurely")),
        new RpcException(new Status(
            StatusCode.Internal,
            "Error starting gRPC call.",
            new HttpRequestException("The HTTP/2 server didn't respond to a ping request."))),
        new AggregateException("One or more errors occurred.",
            new RpcException(new Status(StatusCode.Unavailable, "response ended prematurely")))
    ];

    public static TheoryData<Exception> NonRetryableEscapes() =>
    [
        new InvalidOperationException("bug"),
        new RpcException(new Status(StatusCode.Internal, "server fault"))
    ];

    [Theory]
    [MemberData(nameof(RetryableEscapes))]
    public async Task TrySetKeyValue_RetryableEscape_AnswersMustRetry(Exception escape)
    {
        KeyValuesService service = new(new ThrowingKahuna(escape), NullLogger<IKahuna>.Instance);

        GrpcTrySetKeyValueResponse response = await service.TrySetKeyValue(
            new GrpcTrySetKeyValueRequest { Key = "greeting", ExpiresMs = 0 }, new StubServerCallContext());

        Assert.Equal(KeyValueResponseType.MustRetry, (KeyValueResponseType)response.Type);
    }

    [Theory]
    [MemberData(nameof(RetryableEscapes))]
    public async Task TryGetKeyValue_RetryableEscape_AnswersMustRetry(Exception escape)
    {
        KeyValuesService service = new(new ThrowingKahuna(escape), NullLogger<IKahuna>.Instance);

        GrpcTryGetKeyValueResponse response = await service.TryGetKeyValue(
            new GrpcTryGetKeyValueRequest { Key = "greeting", Revision = -1 }, new StubServerCallContext());

        Assert.Equal(KeyValueResponseType.MustRetry, (KeyValueResponseType)response.Type);
    }

    [Theory]
    [MemberData(nameof(NonRetryableEscapes))]
    public async Task TrySetKeyValue_NonRetryableEscape_KeepsPropagating(Exception escape)
    {
        KeyValuesService service = new(new ThrowingKahuna(escape), NullLogger<IKahuna>.Instance);

        Exception thrown = await Assert.ThrowsAnyAsync<Exception>(() => service.TrySetKeyValue(
            new GrpcTrySetKeyValueRequest { Key = "greeting", ExpiresMs = 0 }, new StubServerCallContext()));

        Assert.Same(escape, thrown);
    }

    /// <summary>
    /// A multi-key response has no message-level outcome, so refusing with an empty item list would
    /// read as "none of these keys produced a result". Every requested key must come back refused.
    /// </summary>
    [Fact]
    public async Task TryGetManyValues_RetryableEscape_RefusesEveryRequestedKey()
    {
        KeyValuesService service = new(
            new ThrowingKahuna(new RaftException("Invalid partition: 3")), NullLogger<IKahuna>.Instance);

        GrpcTryGetManyValuesRequest request = new();
        request.Items.Add(new GrpcTryManyValuesRequestItem { Key = "alpha", Revision = -1 });
        request.Items.Add(new GrpcTryManyValuesRequestItem { Key = "beta", Revision = -1 });

        GrpcTryGetManyValuesResponse response = await service.TryGetManyValues(request, new StubServerCallContext());

        Assert.Equal(2, response.Items.Count);
        Assert.All(response.Items, item => Assert.Equal(KeyValueResponseType.MustRetry, (KeyValueResponseType)item.Type));
        Assert.Equal(["alpha", "beta"], response.Items.Select(static i => i.Key));
    }

    /// <summary>Client cancellation stays unmapped on every surface: the caller gave up, so there is
    /// nobody left to answer and inventing a MustRetry would hide the cancellation.</summary>
    [Fact]
    public async Task TrySetKeyValue_Cancellation_KeepsPropagating()
    {
        KeyValuesService service = new(
            new ThrowingKahuna(new OperationCanceledException()), NullLogger<IKahuna>.Instance);

        await Assert.ThrowsAsync<OperationCanceledException>(() => service.TrySetKeyValue(
            new GrpcTrySetKeyValueRequest { Key = "greeting", ExpiresMs = 0 }, new StubServerCallContext()));
    }

    [Theory]
    [MemberData(nameof(RetryableEscapes))]
    public async Task TryLock_RetryableEscape_AnswersMustRetry(Exception escape)
    {
        LocksService service = new(
            new ThrowingKahuna(escape), new KahunaConfiguration(), null!, NullLogger<IKahuna>.Instance);

        GrpcTryLockResponse response = await service.TryLock(
            new GrpcTryLockRequest { Resource = "resource", Owner = ByteString.CopyFrom([1]), ExpiresMs = 1000 },
            new StubServerCallContext());

        Assert.Equal(LockResponseType.MustRetry, (LockResponseType)response.Type);
    }

    [Theory]
    [MemberData(nameof(NonRetryableEscapes))]
    public async Task TryLock_NonRetryableEscape_KeepsPropagating(Exception escape)
    {
        LocksService service = new(
            new ThrowingKahuna(escape), new KahunaConfiguration(), null!, NullLogger<IKahuna>.Instance);

        Exception thrown = await Assert.ThrowsAnyAsync<Exception>(() => service.TryLock(
            new GrpcTryLockRequest { Resource = "resource", Owner = ByteString.CopyFrom([1]), ExpiresMs = 1000 },
            new StubServerCallContext()));

        Assert.Same(escape, thrown);
    }

    [Theory]
    [MemberData(nameof(RetryableEscapes))]
    public async Task NextSequenceValue_RetryableEscape_AnswersMustRetry(Exception escape)
    {
        SequencesService service = new(new ThrowingKahuna(escape), NullLogger<IKahuna>.Instance);

        GrpcSequenceAllocationResponse response = await service.NextSequenceValue(
            new GrpcNextSequenceRequest { Name = "orders" }, new StubServerCallContext());

        Assert.Equal(SequenceResponseType.MustRetry, (SequenceResponseType)response.Type);
    }

    [Theory]
    [MemberData(nameof(RetryableEscapes))]
    public async Task CreateSequence_RetryableEscape_AnswersMustRetry(Exception escape)
    {
        SequencesService service = new(new ThrowingKahuna(escape), NullLogger<IKahuna>.Instance);

        GrpcSequenceResponse response = await service.CreateSequence(
            new GrpcCreateSequenceRequest { Name = "orders", Increment = 1 }, new StubServerCallContext());

        Assert.Equal(SequenceResponseType.MustRetry, (SequenceResponseType)response.Type);
    }

    [Theory]
    [MemberData(nameof(NonRetryableEscapes))]
    public async Task NextSequenceValue_NonRetryableEscape_KeepsPropagating(Exception escape)
    {
        SequencesService service = new(new ThrowingKahuna(escape), NullLogger<IKahuna>.Instance);

        Exception thrown = await Assert.ThrowsAnyAsync<Exception>(() => service.NextSequenceValue(
            new GrpcNextSequenceRequest { Name = "orders" }, new StubServerCallContext()));

        Assert.Same(escape, thrown);
    }

    /// <summary>Fails every exercised entry point with one configured exception.</summary>
    private sealed class ThrowingKahuna : FakeKahunaBase
    {
        private readonly Exception escape;

        public ThrowingKahuna(Exception escape) => this.escape = escape;

        public override Task<(KeyValueResponseType, long, HLCTimestamp)> LocateAndTrySetKeyValue(
            HLCTimestamp transactionId, string key, byte[]? value, byte[]? compareValue, long compareRevision,
            KeyValueFlags flags, int expiresMs, KeyValueDurability durability, CancellationToken cancellationToken,
            long routedGeneration = 0, string coordinatorKey = "", TransactionOperationId operationId = default)
            => throw escape;

        public override Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> LocateAndTryGetValue(
            HLCTimestamp transactionId, string key, long revision, HLCTimestamp readTimestamp,
            KeyValueDurability durability, CancellationToken cancellationToken,
            string coordinatorKey = "", TransactionOperationId operationId = default)
            => throw escape;

        public override Task<List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)>> LocateAndTryGetManyValues(
            HLCTimestamp transactionId, HLCTimestamp readTimestamp,
            List<(string key, long revision, KeyValueDurability durability)> keys, CancellationToken cancellationToken,
            string coordinatorKey = "", TransactionOperationId operationId = default)
            => throw escape;

        public override Task<(LockResponseType, long)> LocateAndTryLock(
            string resource, byte[] owner, int expiresMs, LockDurability durability, CancellationToken cancellationToken)
            => throw escape;

        public override Task<(SequenceResponseType, SequenceAllocation)> LocateAndNextSequenceValue(
            string name, string? idempotencyKey, SequenceDurability durability, CancellationToken cancellationToken)
            => throw escape;

        public override Task<(SequenceResponseType, long)> LocateAndCreateSequence(
            string name, long initialValue, long increment, long? maxValue, SequenceDurability durability,
            CancellationToken cancellationToken)
            => throw escape;
    }

    /// <summary>Minimal context: the handlers read only the cancellation token.</summary>
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
