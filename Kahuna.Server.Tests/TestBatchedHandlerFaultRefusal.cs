using Grpc.Core;
using Kommander;
using Kommander.Time;
using Microsoft.Extensions.Logging.Abstractions;

using Kahuna;
using Kahuna.Communication.External.Grpc;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Locks;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Locks;

namespace Kahuna.Server.Tests;

/// <summary>
/// Batched requests share one duplex stream and are answered by RequestId, so a handler that throws
/// used to leave its caller's promise unresolved forever: the request hung until the caller's own
/// deadline, with nothing telling it to retry — strictly worse than an error response. These tests
/// drive the real streaming entry points with a handler that faults and assert the failed request is
/// answered with MustRetry while a healthy request sharing the same stream still completes normally.
/// </summary>
public sealed class TestBatchedHandlerFaultRefusal
{
    private static readonly HLCTimestamp LastModified = new(1, 7000, 4);

    [Fact]
    public async Task ClientBatch_FaultedHandler_AnswersMustRetryAndLeavesTheStreamAlive()
    {
        KeyValuesService service = new(new ThrowingSetKahuna(), NullLogger<IKahuna>.Instance);

        CapturingStreamWriter<GrpcBatchClientKeyValueResponse> responses = new();

        await service.BatchClientKeyValueRequests(
            new ListStreamReader<GrpcBatchClientKeyValueRequest>([
                new GrpcBatchClientKeyValueRequest
                {
                    Type = GrpcClientBatchType.TrySetKeyValue,
                    RequestId = 1,
                    TrySetKeyValue = new GrpcTrySetKeyValueRequest { Key = "poisoned", ExpiresMs = 0 }
                },
                new GrpcBatchClientKeyValueRequest
                {
                    Type = GrpcClientBatchType.TryGetKeyValue,
                    RequestId = 2,
                    TryGetKeyValue = new GrpcTryGetKeyValueRequest { Key = "healthy", Revision = -1 }
                }
            ]),
            responses,
            new StubServerCallContext());

        // Both requests are answered: the faulted one is refused, the healthy one succeeds. Neither
        // is dropped, and the streaming call returned normally instead of tearing the stream down.
        Assert.Equal(2, responses.Written.Count);

        GrpcBatchClientKeyValueResponse refused = responses.Written.Single(r => r.RequestId == 1);
        Assert.Equal(GrpcClientBatchType.TrySetKeyValue, refused.Type);
        Assert.Equal(KeyValueResponseType.MustRetry, (KeyValueResponseType)refused.TrySetKeyValue.Type);

        GrpcBatchClientKeyValueResponse served = responses.Written.Single(r => r.RequestId == 2);
        Assert.Equal(GrpcClientBatchType.TryGetKeyValue, served.Type);
        Assert.Equal(KeyValueResponseType.Get, (KeyValueResponseType)served.TryGetKeyValue.Type);
    }

    /// <summary>
    /// The set-many response carries no envelope-level type — the outcome lives on each item — so an
    /// empty item list would read as "no keys were touched" rather than "retry". Every requested key
    /// must come back refused.
    /// </summary>
    [Fact]
    public async Task ServerBatch_FaultedManyHandler_RefusesEveryRequestedKey()
    {
        KeyValuesService service = new(new ThrowingSetKahuna(), NullLogger<IKahuna>.Instance);

        CapturingStreamWriter<GrpcBatchServerKeyValueResponse> responses = new();

        GrpcTrySetManyKeyValueRequest setMany = new();
        setMany.Items.Add(new GrpcTrySetManyKeyValueRequestItem { Key = "alpha", Durability = GrpcKeyValueDurability.Ephemeral });
        setMany.Items.Add(new GrpcTrySetManyKeyValueRequestItem { Key = "beta", Durability = GrpcKeyValueDurability.Persistent });

        await service.BatchServerKeyValueRequests(
            new ListStreamReader<GrpcBatchServerKeyValueRequest>([
                new GrpcBatchServerKeyValueRequest
                {
                    Type = GrpcServerBatchType.ServerTrySetManyKeyValue,
                    RequestId = 7,
                    TrySetManyKeyValue = setMany
                }
            ]),
            responses,
            new StubServerCallContext());

        GrpcBatchServerKeyValueResponse refused = Assert.Single(responses.Written);
        Assert.Equal(7, refused.RequestId);
        Assert.Equal(GrpcServerBatchType.ServerTrySetManyKeyValue, refused.Type);

        Assert.Equal(2, refused.TrySetManyKeyValue.Items.Count);
        Assert.All(refused.TrySetManyKeyValue.Items,
            item => Assert.Equal(KeyValueResponseType.MustRetry, (KeyValueResponseType)item.Type));

        // The keys are echoed so the caller can tell which of its keys went unanswered.
        Assert.Equal(["alpha", "beta"], refused.TrySetManyKeyValue.Items.Select(static i => i.Key));
    }

    [Fact]
    public async Task ClientLockBatch_FaultedHandler_AnswersMustRetry()
    {
        // The lock handler under test reads neither the configuration nor raft — it forwards straight
        // to IKahuna — so the batcher path is exercised without standing up a Raft node.
        LocksService service = new(
            new ThrowingLockKahuna(), new KahunaConfiguration(), null!, NullLogger<IKahuna>.Instance);

        CapturingStreamWriter<GrpcBatchClientLockResponse> responses = new();

        await service.BatchClientLockRequests(
            new ListStreamReader<GrpcBatchClientLockRequest>([
                new GrpcBatchClientLockRequest
                {
                    Type = GrpcLockClientBatchType.TypeTryLock,
                    RequestId = 3,
                    TryLock = new GrpcTryLockRequest { Resource = "poisoned", Owner = Google.Protobuf.ByteString.CopyFrom([1]), ExpiresMs = 1000 }
                }
            ]),
            responses,
            new StubServerCallContext());

        GrpcBatchClientLockResponse refused = Assert.Single(responses.Written);
        Assert.Equal(3, refused.RequestId);
        Assert.Equal(GrpcLockClientBatchType.TypeTryLock, refused.Type);
        Assert.Equal(LockResponseType.MustRetry, (LockResponseType)refused.TryLock.Type);
    }


    [Fact]
    public async Task ServerLockBatch_FaultedHandler_AnswersMustRetry()
    {
        LocksService service = new(
            new ThrowingLockKahuna(), new KahunaConfiguration(), null!, NullLogger<IKahuna>.Instance);

        CapturingStreamWriter<GrpcBatchServerLockResponse> responses = new();

        await service.BatchServerLockRequests(
            new ListStreamReader<GrpcBatchServerLockRequest>([
                new GrpcBatchServerLockRequest
                {
                    Type = GrpcLockServerBatchType.ServerTypeTryLock,
                    RequestId = 4,
                    TryLock = new GrpcTryLockRequest { Resource = "poisoned", Owner = Google.Protobuf.ByteString.CopyFrom([1]), ExpiresMs = 1000 }
                }
            ]),
            responses,
            new StubServerCallContext());

        GrpcBatchServerLockResponse refused = Assert.Single(responses.Written);
        Assert.Equal(4, refused.RequestId);
        Assert.Equal(GrpcLockServerBatchType.ServerTypeTryLock, refused.Type);
        Assert.Equal(LockResponseType.MustRetry, (LockResponseType)refused.TryLock.Type);
    }

    /// <summary>
    /// Some inter-node payloads answer with a bare Found/Success flag and cannot express "retry": a
    /// fabricated negative would be indistinguishable from a real answer. Those are refused with the
    /// envelope's None type, which the caller-side reader turns into a retryable transport failure —
    /// still an answer, so the request never hangs.
    /// </summary>
    [Fact]
    public async Task ServerBatch_FaultedTypelessHandler_AnswersNoneEnvelopeWithoutPayload()
    {
        KeyValuesService service = new(new ThrowingLookupKahuna(), NullLogger<IKahuna>.Instance);

        CapturingStreamWriter<GrpcBatchServerKeyValueResponse> responses = new();

        await service.BatchServerKeyValueRequests(
            new ListStreamReader<GrpcBatchServerKeyValueRequest>([
                new GrpcBatchServerKeyValueRequest
                {
                    Type = GrpcServerBatchType.ServerLookupTransactionRecord,
                    RequestId = 11,
                    LookupTransactionRecord = new GrpcLookupTransactionRecordRequest { AnchorKey = "anchor" }
                }
            ]),
            responses,
            new StubServerCallContext());

        GrpcBatchServerKeyValueResponse refused = Assert.Single(responses.Written);
        Assert.Equal(11, refused.RequestId);
        Assert.Equal(GrpcServerBatchType.ServerTypeNone, refused.Type);

        // No payload is attached: claiming "not found" would be a definitive answer the node never reached.
        Assert.Null(refused.LookupTransactionRecord);
    }

    /// <summary>Sets fail with a Raft resolution failure; reads succeed.</summary>
    private sealed class ThrowingSetKahuna : FakeKahunaBase
    {
        public override Task<(KeyValueResponseType, long, HLCTimestamp)> LocateAndTrySetKeyValue(
            HLCTimestamp transactionId, string key, byte[]? value, byte[]? compareValue, long compareRevision,
            KeyValueFlags flags, int expiresMs, KeyValueDurability durability, CancellationToken cancellationToken,
            long routedGeneration = 0, string coordinatorKey = "", TransactionOperationId operationId = default)
            => throw new RaftException("Invalid partition: 3");

        public override Task<List<KahunaSetKeyValueResponseItem>> LocateAndTrySetManyKeyValue(
            List<KahunaSetKeyValueRequestItem> setManyItems, CancellationToken cancellationToken,
            string coordinatorKey = "", TransactionOperationId operationId = default)
            => throw new RaftException("Invalid partition: 3");

        public override Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> LocateAndTryGetValue(
            HLCTimestamp transactionId, string key, long revision, HLCTimestamp readTimestamp,
            KeyValueDurability durability, CancellationToken cancellationToken,
            string coordinatorKey = "", TransactionOperationId operationId = default)
            => Task.FromResult<(KeyValueResponseType, ReadOnlyKeyValueEntry?)>((
                KeyValueResponseType.Get,
                new ReadOnlyKeyValueEntry([1, 2, 3], 1, HLCTimestamp.Zero, HLCTimestamp.Zero, LastModified, KeyValueState.Set)));
    }

    /// <summary>Fails the transaction-record lookup, whose response cannot carry a MustRetry.</summary>
    private sealed class ThrowingLookupKahuna : FakeKahunaBase
    {
        public override Task<byte[]?> LookupTransactionRecordLocal(
            int partitionId, HLCTimestamp transactionId, long epoch, string anchorKey, CancellationToken cancellationToken)
            => throw new RaftException("Invalid partition: 3");
    }

    private sealed class ThrowingLockKahuna : FakeKahunaBase
    {
        public override Task<(LockResponseType, long)> LocateAndTryLock(
            string resource, byte[] owner, int expiresMs, LockDurability durability, CancellationToken cancellationToken)
            => throw new RaftException("Invalid partition: 3");
    }

    /// <summary>Feeds a fixed list of requests to a streaming handler, then closes the stream.</summary>
    private sealed class ListStreamReader<T> : IAsyncStreamReader<T>
    {
        private readonly IReadOnlyList<T> items;

        private int index = -1;

        public ListStreamReader(IReadOnlyList<T> items) => this.items = items;

        public T Current => items[index];

        public Task<bool> MoveNext(CancellationToken cancellationToken) => Task.FromResult(++index < items.Count);
    }

    /// <summary>Captures everything a handler writes back onto the response stream.</summary>
    private sealed class CapturingStreamWriter<T> : IServerStreamWriter<T>
    {
        private readonly Lock mutex = new();

        public List<T> Written { get; } = [];

        public WriteOptions? WriteOptions { get; set; }

        public Task WriteAsync(T message)
        {
            lock (mutex)
                Written.Add(message);

            return Task.CompletedTask;
        }
    }

    /// <summary>Minimal context: the batchers read only the cancellation token.</summary>
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
