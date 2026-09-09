using System.Collections.Concurrent;
using System.Reflection;

using Google.Protobuf;
using Grpc.Core;
using Kommander.Time;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

using Kahuna;
using Kahuna.Communication.External.Grpc;
using Kahuna.Communication.External.Grpc.KeyValues;
using Kahuna.Server.Communication.Internode.Grpc;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.Tests;

/// <summary>
/// Tests for the coalesced inter-node transport: queued key-value operations share one stream
/// message on the way out, coalesced carriers unpack on both ends, envelopes spread over every
/// stream to the peer with lock and key-value writes owned separately, and the server's response
/// writer bundles replies that queued up behind an in-flight write.
/// </summary>
public sealed class TestInterNodeCoalescing
{
    private static readonly Type BatcherType = typeof(GrpcServerBatcher);

    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);

    private static ConcurrentDictionary<int, long> RequestStreamRefs()
        => (ConcurrentDictionary<int, long>)BatcherType
            .GetField("requestStreamRefs", BindingFlags.NonPublic | BindingFlags.Static)!
            .GetValue(null)!;

    private static ConcurrentDictionary<string, Lazy<List<GrpcServerSharedStreaming>>> Streamings()
        => (ConcurrentDictionary<string, Lazy<List<GrpcServerSharedStreaming>>>)BatcherType
            .GetField("streamings", BindingFlags.NonPublic | BindingFlags.Static)!
            .GetValue(null)!;

    // ── bundle planning ──────────────────────────────────────────────────────

    private static GrpcServerBatcherItem MakeKeyValueItem(int requestId, out TaskCompletionSource<GrpcServerBatcherResponse> promise)
    {
        promise = new(TaskCreationOptions.RunContinuationsAsynchronously);
        return new(GrpcServerBatcherItemType.KeyValues, requestId, new(new GrpcLookupTransactionRecordRequest { AnchorKey = "anchor" }), promise);
    }

    private static GrpcServerBatcherItem MakeSetItem(int requestId, int valueBytes)
    {
        TaskCompletionSource<GrpcServerBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);
        GrpcTrySetKeyValueRequest set = new()
        {
            Key = "payload-sized",
            Value = UnsafeByteOperations.UnsafeWrap(new byte[valueBytes])
        };
        return new(GrpcServerBatcherItemType.KeyValues, requestId, new(set), promise);
    }

    [Fact]
    public void CountCoalescedItems_RespectsTheItemCap()
    {
        List<GrpcServerBatcherItem> items = new(GrpcServerBatcher.MaxOpsPerCoalescedEnvelope + 1);
        for (int i = 0; i < GrpcServerBatcher.MaxOpsPerCoalescedEnvelope + 1; i++)
            items.Add(MakeKeyValueItem(9_960_000 + i, out _));

        Assert.Equal(GrpcServerBatcher.MaxOpsPerCoalescedEnvelope, GrpcServerBatcher.CountCoalescedItems(items, 0));
        Assert.Equal(1, GrpcServerBatcher.CountCoalescedItems(items, GrpcServerBatcher.MaxOpsPerCoalescedEnvelope));
    }

    [Fact]
    public void CountCoalescedItems_RespectsTheByteCap_AndShipsAnOversizedItemAlone()
    {
        // Two items just over half the byte cap cannot share an envelope.
        int half = (int)(GrpcServerBatcher.MaxCoalescedEnvelopeBytes / 2) + 1024;
        List<GrpcServerBatcherItem> halves = [MakeSetItem(9_961_001, half), MakeSetItem(9_961_002, half)];

        Assert.Equal(1, GrpcServerBatcher.CountCoalescedItems(halves, 0));
        Assert.Equal(1, GrpcServerBatcher.CountCoalescedItems(halves, 1));

        // An item alone above the cap still travels: the first item is always taken.
        int oversized = (int)GrpcServerBatcher.MaxCoalescedEnvelopeBytes + 1024;
        List<GrpcServerBatcherItem> big = [MakeSetItem(9_961_003, oversized), MakeSetItem(9_961_004, 16)];

        Assert.Equal(1, GrpcServerBatcher.CountCoalescedItems(big, 0));
    }

    // ── drain plan ───────────────────────────────────────────────────────────

    [Fact]
    public async Task RunBatch_CoalescesQueuedKeyValueItemsIntoOneCarrier()
    {
        const string url = "test://coalesce-two-items";
        const long streamId = 9_810_001;
        const int firstId = 9_962_001;
        const int secondId = 9_962_002;

        RecordingClientStreamWriter<GrpcBatchServerKeyValueRequest> keyValueWriter = new();
        GrpcServerSharedStreaming streaming = MakeSharedStreaming(streamId, keyValueWriter, new RecordingClientStreamWriter<GrpcBatchServerLockRequest>());
        Streamings()[url] = CreatedLazy([streaming]);

        GrpcServerBatcherItem first = MakeKeyValueItem(firstId, out TaskCompletionSource<GrpcServerBatcherResponse> firstPromise);
        GrpcServerBatcherItem second = MakeKeyValueItem(secondId, out TaskCompletionSource<GrpcServerBatcherResponse> secondPromise);
        Assert.True(GrpcServerBatcher.TryAdmit(first));
        Assert.True(GrpcServerBatcher.TryAdmit(second));

        try
        {
            await InvokeRunBatch(url, [first, second]);

            GrpcBatchServerKeyValueRequest written = Assert.Single(keyValueWriter.Written);
            Assert.Equal(GrpcServerBatchType.ServerCoalesced, written.Type);
            Assert.Equal(2, written.Coalesced.Count);
            Assert.Equal(firstId, written.Coalesced[0].RequestId);
            Assert.Equal(secondId, written.Coalesced[1].RequestId);
            Assert.Equal(GrpcServerBatchType.ServerLookupTransactionRecord, written.Coalesced[0].Type);

            Assert.Equal(streamId, RequestStreamRefs()[firstId]);
            Assert.Equal(streamId, RequestStreamRefs()[secondId]);
        }
        finally
        {
            Streamings().TryRemove(url, out _);
            Cleanup(firstId, firstPromise);
            Cleanup(secondId, secondPromise);
        }
    }

    [Fact]
    public async Task RunBatch_SpreadsEnvelopesAcrossStreams_AndSplitsLockAndKeyValueOwnership()
    {
        const string url = "test://spread-two-streams";
        const long streamIdA = 9_810_002;
        const long streamIdB = 9_810_003;
        const int keyValueOneId = 9_963_001;
        const int keyValueTwoId = 9_963_002;
        const int lockOneId = 9_963_003;
        const int lockTwoId = 9_963_004;

        RecordingClientStreamWriter<GrpcBatchServerKeyValueRequest> keyValueWriterA = new();
        RecordingClientStreamWriter<GrpcBatchServerKeyValueRequest> keyValueWriterB = new();
        RecordingClientStreamWriter<GrpcBatchServerLockRequest> lockWriterA = new();
        RecordingClientStreamWriter<GrpcBatchServerLockRequest> lockWriterB = new();

        GrpcServerSharedStreaming streamingA = MakeSharedStreaming(streamIdA, keyValueWriterA, lockWriterA);
        GrpcServerSharedStreaming streamingB = MakeSharedStreaming(streamIdB, keyValueWriterB, lockWriterB);
        Streamings()[url] = CreatedLazy([streamingA, streamingB]);

        GrpcServerBatcherItem keyValueOne = MakeKeyValueItem(keyValueOneId, out TaskCompletionSource<GrpcServerBatcherResponse> keyValueOnePromise);
        GrpcServerBatcherItem keyValueTwo = MakeKeyValueItem(keyValueTwoId, out TaskCompletionSource<GrpcServerBatcherResponse> keyValueTwoPromise);

        TaskCompletionSource<GrpcServerBatcherResponse> lockOnePromise = new(TaskCreationOptions.RunContinuationsAsynchronously);
        GrpcServerBatcherItem lockOne = new(GrpcServerBatcherItemType.Locks, lockOneId, new(new GrpcTryLockRequest()), lockOnePromise);
        TaskCompletionSource<GrpcServerBatcherResponse> lockTwoPromise = new(TaskCreationOptions.RunContinuationsAsynchronously);
        GrpcServerBatcherItem lockTwo = new(GrpcServerBatcherItemType.Locks, lockTwoId, new(new GrpcTryLockRequest()), lockTwoPromise);

        Assert.True(GrpcServerBatcher.TryAdmit(keyValueOne));
        Assert.True(GrpcServerBatcher.TryAdmit(keyValueTwo));
        Assert.True(GrpcServerBatcher.TryAdmit(lockOne));
        Assert.True(GrpcServerBatcher.TryAdmit(lockTwo));

        try
        {
            await InvokeRunBatch(url, [keyValueOne, lockOne, keyValueTwo, lockTwo]);

            // Both key-value items form one carrier on the first stream; the two lock items
            // continue the round-robin onto the second stream and back to the first. Lock
            // envelopes never coalesce.
            GrpcBatchServerKeyValueRequest carrier = Assert.Single(keyValueWriterA.Written);
            Assert.Equal(GrpcServerBatchType.ServerCoalesced, carrier.Type);
            Assert.Equal(2, carrier.Coalesced.Count);
            Assert.Empty(keyValueWriterB.Written);

            GrpcBatchServerLockRequest firstLock = Assert.Single(lockWriterB.Written);
            Assert.Equal(lockOneId, firstLock.RequestId);
            GrpcBatchServerLockRequest secondLock = Assert.Single(lockWriterA.Written);
            Assert.Equal(lockTwoId, secondLock.RequestId);

            Assert.Equal(streamIdA, RequestStreamRefs()[keyValueOneId]);
            Assert.Equal(streamIdA, RequestStreamRefs()[keyValueTwoId]);
            Assert.Equal(streamIdB, RequestStreamRefs()[lockOneId]);
            Assert.Equal(streamIdA, RequestStreamRefs()[lockTwoId]);
        }
        finally
        {
            Streamings().TryRemove(url, out _);
            Cleanup(keyValueOneId, keyValueOnePromise);
            Cleanup(keyValueTwoId, keyValueTwoPromise);
            Cleanup(lockOneId, lockOnePromise);
            Cleanup(lockTwoId, lockTwoPromise);
        }
    }

    // ── client read loop ─────────────────────────────────────────────────────

    [Fact]
    public async Task ReadLoop_SettlesEveryResponseInACoalescedCarrier()
    {
        const string url = "test://read-coalesced-carrier";
        const long streamId = 9_810_004;
        const int firstId = 9_964_001;
        const int secondId = 9_964_002;

        GrpcServerBatcherItem first = MakeKeyValueItem(firstId, out TaskCompletionSource<GrpcServerBatcherResponse> firstPromise);
        GrpcServerBatcherItem second = MakeKeyValueItem(secondId, out TaskCompletionSource<GrpcServerBatcherResponse> secondPromise);
        Assert.True(GrpcServerBatcher.TryAdmit(first));
        Assert.True(GrpcServerBatcher.TryAdmit(second));
        RequestStreamRefs()[firstId] = streamId;
        RequestStreamRefs()[secondId] = streamId;

        GrpcBatchServerKeyValueResponse carrier = new() { Type = GrpcServerBatchType.ServerCoalesced };
        carrier.Coalesced.Add(new GrpcBatchServerKeyValueResponse
        {
            Type = GrpcServerBatchType.ServerLookupTransactionRecord,
            RequestId = firstId,
            LookupTransactionRecord = new GrpcLookupTransactionRecordResponse()
        });
        carrier.Coalesced.Add(new GrpcBatchServerKeyValueResponse
        {
            Type = GrpcServerBatchType.ServerLookupTransactionRecord,
            RequestId = secondId,
            LookupTransactionRecord = new GrpcLookupTransactionRecordResponse()
        });

        AsyncDuplexStreamingCall<GrpcBatchServerKeyValueRequest, GrpcBatchServerKeyValueResponse> call = new(
            new NoopClientStreamWriter<GrpcBatchServerKeyValueRequest>(),
            new ListStreamReader<GrpcBatchServerKeyValueResponse>([carrier]),
            Task.FromResult(new Metadata()),
            static () => Status.DefaultSuccess,
            static () => [],
            static () => { });

        try
        {
            await InvokeReadKeyValueMessages(url, streamId, call);

            Assert.True(firstPromise.Task.IsCompletedSuccessfully);
            Assert.True(secondPromise.Task.IsCompletedSuccessfully);
            Assert.NotNull(firstPromise.Task.Result.LookupTransactionRecord);
            Assert.NotNull(secondPromise.Task.Result.LookupTransactionRecord);

            Assert.False(RequestStreamRefs().ContainsKey(firstId));
            Assert.False(RequestStreamRefs().ContainsKey(secondId));
        }
        finally
        {
            Cleanup(firstId, firstPromise);
            Cleanup(secondId, secondPromise);
        }
    }

    // ── server unpack ────────────────────────────────────────────────────────

    [Fact]
    public async Task ServerBatch_UnpacksACoalescedCarrier_AndAnswersEveryInnerRequest()
    {
        KeyValuesService service = new(new HealthyGetKahuna(), NullLogger<IKahuna>.Instance);
        CapturingStreamWriter<GrpcBatchServerKeyValueResponse> responses = new();

        GrpcBatchServerKeyValueRequest carrier = new() { Type = GrpcServerBatchType.ServerCoalesced };
        carrier.Coalesced.Add(new GrpcBatchServerKeyValueRequest
        {
            Type = GrpcServerBatchType.ServerTryGetKeyValue,
            RequestId = 21,
            TryGetKeyValue = new GrpcTryGetKeyValueRequest { Key = "alpha", Revision = -1 }
        });
        carrier.Coalesced.Add(new GrpcBatchServerKeyValueRequest
        {
            Type = GrpcServerBatchType.ServerTryGetKeyValue,
            RequestId = 22,
            TryGetKeyValue = new GrpcTryGetKeyValueRequest { Key = "beta", Revision = -1 }
        });

        await service.BatchServerKeyValueRequests(
            new ListStreamReader<GrpcBatchServerKeyValueRequest>([carrier]),
            responses,
            new StubServerCallContext()).WaitAsync(Timeout, TestContext.Current.CancellationToken);

        // Replies may themselves ride a coalesced carrier; count the operations inside.
        List<GrpcBatchServerKeyValueResponse> operations = [];
        foreach (GrpcBatchServerKeyValueResponse message in responses.Written)
        {
            if (message.Type == GrpcServerBatchType.ServerCoalesced)
                operations.AddRange(message.Coalesced);
            else
                operations.Add(message);
        }

        Assert.Equal(2, operations.Count);
        Assert.Equal([21, 22], operations.Select(static o => o.RequestId).Order());
        Assert.All(operations, static o => Assert.Equal(GrpcServerBatchType.ServerTryGetKeyValue, o.Type));
        Assert.All(operations, static o => Assert.NotNull(o.TryGetKeyValue));
    }

    // ── server response writer ───────────────────────────────────────────────

    [Fact]
    public async Task ResponseWriter_BundlesResponsesParkedBehindAnInFlightWrite()
    {
        GatedServerStreamWriter<GrpcBatchServerKeyValueResponse> stream = new();
        KeyValueServerBatcher.CoalescingResponseWriter writer = new(stream);

        Task first = writer.WriteAsync(MakeGetResponse(31));
        await stream.FirstWriteStarted.Task.WaitAsync(Timeout, TestContext.Current.CancellationToken);

        // These two park behind the in-flight write and must leave together in one carrier.
        Task second = writer.WriteAsync(MakeGetResponse(32));
        Task third = writer.WriteAsync(MakeGetResponse(33));
        Assert.False(second.IsCompleted);
        Assert.False(third.IsCompleted);

        stream.Release();
        await Task.WhenAll(first, second, third).WaitAsync(Timeout, TestContext.Current.CancellationToken);

        Assert.Equal(2, stream.Written.Count);
        Assert.Equal(31, stream.Written[0].RequestId);
        Assert.Equal(GrpcServerBatchType.ServerCoalesced, stream.Written[1].Type);
        Assert.Equal([32, 33], stream.Written[1].Coalesced.Select(static o => o.RequestId));
    }

    [Fact]
    public async Task ResponseWriter_FailsParkedResponsesWhenTheStreamDies()
    {
        GatedServerStreamWriter<GrpcBatchServerKeyValueResponse> stream = new(throwOnRelease: true);
        KeyValueServerBatcher.CoalescingResponseWriter writer = new(stream);

        Task first = writer.WriteAsync(MakeGetResponse(41));
        await stream.FirstWriteStarted.Task.WaitAsync(Timeout, TestContext.Current.CancellationToken);

        Task second = writer.WriteAsync(MakeGetResponse(42));
        Task third = writer.WriteAsync(MakeGetResponse(43));

        stream.Release();

        // Every caller observes the failure: a parked response behind a dead stream can never
        // be written, and a silent wait would hang its handler forever.
        await Assert.ThrowsAsync<RpcException>(() => first.WaitAsync(Timeout, TestContext.Current.CancellationToken));
        await Assert.ThrowsAsync<RpcException>(() => second.WaitAsync(Timeout, TestContext.Current.CancellationToken));
        await Assert.ThrowsAsync<RpcException>(() => third.WaitAsync(Timeout, TestContext.Current.CancellationToken));
    }

    private static GrpcBatchServerKeyValueResponse MakeGetResponse(int requestId) => new()
    {
        Type = GrpcServerBatchType.ServerTryGetKeyValue,
        RequestId = requestId,
        TryGetKeyValue = new GrpcTryGetKeyValueResponse()
    };

    // ── helpers ──────────────────────────────────────────────────────────────

    private static void Cleanup(int requestId, TaskCompletionSource<GrpcServerBatcherResponse> promise)
    {
        GrpcServerBatcher.TryTakeRequest(requestId, out _);
        RequestStreamRefs().TryRemove(requestId, out _);
        promise.TrySetCanceled(TestContext.Current.CancellationToken);
    }

    private static GrpcServerSharedStreaming MakeSharedStreaming(
        long streamId,
        IClientStreamWriter<GrpcBatchServerKeyValueRequest> keyValueWriter,
        IClientStreamWriter<GrpcBatchServerLockRequest> lockWriter)
    {
        AsyncDuplexStreamingCall<GrpcBatchServerLockRequest, GrpcBatchServerLockResponse> lockCall = new(
            lockWriter,
            new PendingAsyncStreamReader<GrpcBatchServerLockResponse>(),
            Task.FromResult(new Metadata()),
            static () => Status.DefaultSuccess,
            static () => [],
            static () => { });

        AsyncDuplexStreamingCall<GrpcBatchServerKeyValueRequest, GrpcBatchServerKeyValueResponse> keyValueCall = new(
            keyValueWriter,
            new PendingAsyncStreamReader<GrpcBatchServerKeyValueResponse>(),
            Task.FromResult(new Metadata()),
            static () => Status.DefaultSuccess,
            static () => [],
            static () => { });

        return new GrpcServerSharedStreaming(streamId, lockCall, keyValueCall);
    }

    private static Lazy<List<GrpcServerSharedStreaming>> CreatedLazy(List<GrpcServerSharedStreaming> nodeStreamings)
    {
        Lazy<List<GrpcServerSharedStreaming>> lazy = new(() => nodeStreamings);
        _ = lazy.Value;
        return lazy;
    }

    private static Task InvokeRunBatch(string url, List<GrpcServerBatcherItem> requests)
    {
        GrpcServerBatcher batcher = new(url, NullLogger.Instance);

        MethodInfo method = BatcherType.GetMethod("RunBatch", BindingFlags.NonPublic | BindingFlags.Instance)!;

        try
        {
            return (Task)method.Invoke(batcher, [requests])!;
        }
        catch (TargetInvocationException ex) when (ex.InnerException is not null)
        {
            return Task.FromException(ex.InnerException);
        }
    }

    private static Task InvokeReadKeyValueMessages(
        string url, long streamId,
        AsyncDuplexStreamingCall<GrpcBatchServerKeyValueRequest, GrpcBatchServerKeyValueResponse> streaming)
        => (Task)BatcherType
            .GetMethod("ReadKeyValueMessages", BindingFlags.NonPublic | BindingFlags.Static)!
            .Invoke(null, [url, streamId, streaming, NullLogger.Instance])!;

    /// <summary>Answers every read with a fixed value; everything else uses the fake defaults.</summary>
    private sealed class HealthyGetKahuna : FakeKahunaBase
    {
        public override Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> LocateAndTryGetValue(
            HLCTimestamp transactionId, string key, long revision, HLCTimestamp readTimestamp,
            KeyValueDurability durability, CancellationToken cancellationToken,
            string coordinatorKey = "", TransactionOperationId operationId = default)
            => Task.FromResult<(KeyValueResponseType, ReadOnlyKeyValueEntry?)>((
                KeyValueResponseType.Get,
                new ReadOnlyKeyValueEntry([1, 2, 3], 1, HLCTimestamp.Zero, HLCTimestamp.Zero, new HLCTimestamp(1, 7000, 4), KeyValueState.Set)));
    }

    private sealed class ListStreamReader<T> : IAsyncStreamReader<T>
    {
        private readonly IReadOnlyList<T> items;

        private int index = -1;

        public ListStreamReader(IReadOnlyList<T> items) => this.items = items;

        public T Current => items[index];

        public Task<bool> MoveNext(CancellationToken cancellationToken) => Task.FromResult(++index < items.Count);
    }

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

    /// <summary>
    /// Records every write; the first write parks on a gate until the test releases it, so the
    /// test can queue responses behind an in-flight write deterministically. With
    /// <c>throwOnRelease</c> the released write fails instead, modeling a stream that died while
    /// a write was in flight.
    /// </summary>
    private sealed class GatedServerStreamWriter<T> : IServerStreamWriter<T>
    {
        private readonly TaskCompletionSource gate = new(TaskCreationOptions.RunContinuationsAsynchronously);

        private readonly bool throwOnRelease;

        private int firstWrite = 1;

        public GatedServerStreamWriter(bool throwOnRelease = false) => this.throwOnRelease = throwOnRelease;

        public TaskCompletionSource FirstWriteStarted { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public List<T> Written { get; } = [];

        public WriteOptions? WriteOptions { get; set; }

        public void Release() => gate.TrySetResult();

        public async Task WriteAsync(T message)
        {
            if (1 == Interlocked.Exchange(ref firstWrite, 0))
            {
                FirstWriteStarted.TrySetResult();
                await gate.Task;
            }

            if (throwOnRelease)
                throw new RpcException(new(StatusCode.Unavailable, "The transport failed."));

            Written.Add(message);
        }
    }

    private sealed class PendingAsyncStreamReader<T> : IAsyncStreamReader<T>
    {
        private readonly TaskCompletionSource<bool> never = new();
        public T Current => default!;
        public Task<bool> MoveNext(CancellationToken cancellationToken) => never.Task;
    }

    private sealed class NoopClientStreamWriter<T> : IClientStreamWriter<T>
    {
        public WriteOptions? WriteOptions { get; set; }
        public Task CompleteAsync() => Task.CompletedTask;
        public Task WriteAsync(T message) => Task.CompletedTask;
    }

    private sealed class RecordingClientStreamWriter<T> : IClientStreamWriter<T>
    {
        private readonly ConcurrentQueue<T> written = new();
        public IReadOnlyList<T> Written => written.ToArray();
        public WriteOptions? WriteOptions { get; set; }
        public Task CompleteAsync() => Task.CompletedTask;

        public Task WriteAsync(T message)
        {
            written.Enqueue(message);
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
