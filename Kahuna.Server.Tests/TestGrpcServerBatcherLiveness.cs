using System.Collections.Concurrent;
using System.Reflection;

using Grpc.Core;
using Kahuna.Server.Communication.Internode.Grpc;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kahuna.Server.Tests;

/// <summary>
/// Liveness tests for the inter-node <see cref="GrpcServerBatcher"/>: a stream that goes quiet
/// WITHOUT dying (a SIGSTOPed peer leaves the TCP session open and the HTTP/2 window stalls with
/// no error) must not wedge the batcher forever. Three bounds close that hole, each covered here:
///
/// <list type="bullet">
///   <item>the per-request deadline reaper fails and scrubs requests nothing will ever answer;</item>
///   <item>a write bounded by the write timeout fails retryably, releases the pipeline, and
///         evicts the stalled stream instead of holding the per-stream semaphore forever;</item>
///   <item>a read loop that exits — for any reason — evicts and disposes its URL's shared
///         streams so the next enqueue rebuilds them instead of writing to dead calls.</item>
/// </list>
/// </summary>
public sealed class TestGrpcServerBatcherLiveness
{
    private static readonly Type BatcherType = typeof(GrpcServerBatcher);

    private static ConcurrentDictionary<int, GrpcServerBatcherItem> RequestRefs()
        => (ConcurrentDictionary<int, GrpcServerBatcherItem>)BatcherType
            .GetField("requestRefs", BindingFlags.NonPublic | BindingFlags.Static)!
            .GetValue(null)!;

    private static ConcurrentDictionary<int, long> RequestStreamRefs()
        => (ConcurrentDictionary<int, long>)BatcherType
            .GetField("requestStreamRefs", BindingFlags.NonPublic | BindingFlags.Static)!
            .GetValue(null)!;

    private static ConcurrentDictionary<string, Lazy<List<GrpcServerSharedStreaming>>> Streamings()
        => (ConcurrentDictionary<string, Lazy<List<GrpcServerSharedStreaming>>>)BatcherType
            .GetField("streamings", BindingFlags.NonPublic | BindingFlags.Static)!
            .GetValue(null)!;

    private static TaskCompletionSource<GrpcServerBatcherResponse> SeedPending(int requestId, long streamId)
    {
        TaskCompletionSource<GrpcServerBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcServerBatcherItem item = new(
            GrpcServerBatcherItemType.KeyValues,
            requestId,
            new GrpcServerBatcherRequest(new GrpcLookupTransactionRecordRequest()),
            promise);

        RequestRefs()[requestId] = item;
        RequestStreamRefs()[requestId] = streamId;

        return promise;
    }

    // ── deadline reaper ──────────────────────────────────────────────────────

    /// <summary>
    /// The sweep must fail exactly the requests older than the deadline — with a retryable
    /// Unavailable — and scrub them from both dictionaries, leaving fresh requests untouched.
    /// The clock is passed in, so expiry is driven deterministically.
    /// </summary>
    [Fact]
    public void SweepExpiredRequests_FailsOnlyRequestsPastTheDeadline()
    {
        const long streamId = 9_600_001;
        const int oldRequest = 9_700_001;
        const int freshRequest = 9_700_002;

        TaskCompletionSource<GrpcServerBatcherResponse> old = SeedPending(oldRequest, streamId);
        TaskCompletionSource<GrpcServerBatcherResponse> fresh = SeedPending(freshRequest, streamId);

        try
        {
            // A "now" one deadline past the enqueue instant expires both; sweeping with the real
            // now first proves a fresh request survives a sweep.
            GrpcServerBatcher.SweepExpiredRequests(Environment.TickCount64, NullLogger.Instance);
            Assert.False(old.Task.IsCompleted);
            Assert.False(fresh.Task.IsCompleted);

            long farFuture = Environment.TickCount64 + (long)GrpcServerBatcher.RequestDeadline.TotalMilliseconds + 1_000;
            GrpcServerBatcher.SweepExpiredRequests(farFuture, NullLogger.Instance);

            Assert.True(old.Task.IsFaulted);
            RpcException failure = Assert.IsType<RpcException>(old.Task.Exception!.InnerException);
            Assert.Equal(StatusCode.Unavailable, failure.StatusCode);

            Assert.False(RequestRefs().ContainsKey(oldRequest));
            Assert.False(RequestStreamRefs().ContainsKey(oldRequest));
        }
        finally
        {
            RequestRefs().TryRemove(oldRequest, out _);
            RequestRefs().TryRemove(freshRequest, out _);
            RequestStreamRefs().TryRemove(oldRequest, out _);
            RequestStreamRefs().TryRemove(freshRequest, out _);
            old.TrySetCanceled();
            fresh.TrySetCanceled();
        }
    }

    // ── bounded writes ───────────────────────────────────────────────────────

    /// <summary>
    /// A previous write stuck on a stalled stream holds the per-stream semaphore. The next write
    /// must not queue behind it forever: after the write timeout it fails retryably, fails the
    /// stream's pending requests, and evicts the URL's streams so later enqueues rebuild.
    /// This wedge — one silent stuck write serializing all forwarding to a peer — was the
    /// permanent-outage mechanism of the Caraxes run-J soak.
    /// </summary>
    [Fact]
    public async Task WriteBounded_SemaphoreHeldPastTimeout_FailsRetryablyAndEvicts()
    {
        const string url = "test://write-semaphore-stall";
        const long streamId = 9_800_001;
        const int requestId = 9_900_001;

        TimeSpan savedTimeout = GrpcServerBatcher.WriteTimeout;
        GrpcServerBatcher.WriteTimeout = TimeSpan.FromMilliseconds(150);

        (GrpcServerSharedStreaming streaming, DisposeCounter disposes) = MakeSharedStreaming(streamId, new NoopClientStreamWriter<GrpcBatchServerKeyValueRequest>());
        Streamings()[url] = CreatedLazy(streaming);
        TaskCompletionSource<GrpcServerBatcherResponse> pending = SeedPending(requestId, streamId);

        try
        {
            await streaming.Semaphore.WaitAsync();     // the stuck previous writer

            RpcException failure = await Assert.ThrowsAsync<RpcException>(() =>
                InvokeWriteBounded(url, streaming, new GrpcBatchServerKeyValueRequest()));

            Assert.Equal(StatusCode.Unavailable, failure.StatusCode);
            Assert.True(pending.Task.IsFaulted);
            Assert.False(Streamings().ContainsKey(url));
            Assert.True(disposes.Count > 0);
        }
        finally
        {
            GrpcServerBatcher.WriteTimeout = savedTimeout;
            Streamings().TryRemove(url, out _);
            RequestRefs().TryRemove(requestId, out _);
            RequestStreamRefs().TryRemove(requestId, out _);
            pending.TrySetCanceled();
        }
    }

    /// <summary>
    /// A write whose <c>WriteAsync</c> never completes (the stalled-HTTP/2-window case) must fail
    /// after the write timeout, release the semaphore for the pipeline, and evict the stream.
    /// </summary>
    [Fact]
    public async Task WriteBounded_HangingWrite_TimesOutAndReleasesThePipeline()
    {
        const string url = "test://write-hang";
        const long streamId = 9_800_002;

        TimeSpan savedTimeout = GrpcServerBatcher.WriteTimeout;
        GrpcServerBatcher.WriteTimeout = TimeSpan.FromMilliseconds(150);

        (GrpcServerSharedStreaming streaming, _) = MakeSharedStreaming(streamId, new HangingClientStreamWriter<GrpcBatchServerKeyValueRequest>());
        Streamings()[url] = CreatedLazy(streaming);

        try
        {
            RpcException failure = await Assert.ThrowsAsync<RpcException>(() =>
                InvokeWriteBounded(url, streaming, new GrpcBatchServerKeyValueRequest()));

            Assert.Equal(StatusCode.Unavailable, failure.StatusCode);
            Assert.False(Streamings().ContainsKey(url));

            // The semaphore was released on the failure path: the pipeline is not wedged.
            Assert.True(await streaming.Semaphore.WaitAsync(TimeSpan.FromSeconds(1)));
            streaming.Semaphore.Release();
        }
        finally
        {
            GrpcServerBatcher.WriteTimeout = savedTimeout;
            Streamings().TryRemove(url, out _);
        }
    }

    // ── read-loop eviction ───────────────────────────────────────────────────

    /// <summary>
    /// A read loop that observes its stream end must evict and dispose the URL's shared streams —
    /// the registry is populated once per URL for the process lifetime, so without eviction every
    /// later enqueue would keep targeting the dead call objects forever.
    /// </summary>
    [Fact]
    public async Task ReadLoopExit_EvictsAndDisposesTheUrlStreams()
    {
        const string url = "test://read-loop-evict";
        const long streamId = 9_800_003;

        (GrpcServerSharedStreaming streaming, DisposeCounter disposes) = MakeSharedStreaming(streamId, new NoopClientStreamWriter<GrpcBatchServerKeyValueRequest>());
        Streamings()[url] = CreatedLazy(streaming);

        AsyncDuplexStreamingCall<GrpcBatchServerKeyValueRequest, GrpcBatchServerKeyValueResponse> closedCall = new(
            new NoopClientStreamWriter<GrpcBatchServerKeyValueRequest>(),
            new EmptyAsyncStreamReader<GrpcBatchServerKeyValueResponse>(),
            Task.FromResult(new Metadata()),
            static () => Status.DefaultSuccess,
            static () => [],
            static () => { });

        try
        {
            await (Task)BatcherType
                .GetMethod("ReadKeyValueMessages", BindingFlags.NonPublic | BindingFlags.Static)!
                .Invoke(null, [url, streamId, closedCall, NullLogger.Instance])!;

            Assert.False(Streamings().ContainsKey(url));
            Assert.True(disposes.Count > 0);
        }
        finally
        {
            Streamings().TryRemove(url, out _);
        }
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    private sealed class DisposeCounter
    {
        private int count;
        public int Count => Volatile.Read(ref count);
        public void Increment() => Interlocked.Increment(ref count);
    }

    private static (GrpcServerSharedStreaming, DisposeCounter) MakeSharedStreaming(
        long streamId, IClientStreamWriter<GrpcBatchServerKeyValueRequest> keyValueWriter)
    {
        DisposeCounter disposes = new();

        AsyncDuplexStreamingCall<GrpcBatchServerLockRequest, GrpcBatchServerLockResponse> lockCall = new(
            new NoopClientStreamWriter<GrpcBatchServerLockRequest>(),
            new PendingAsyncStreamReader<GrpcBatchServerLockResponse>(),
            Task.FromResult(new Metadata()),
            static () => Status.DefaultSuccess,
            static () => [],
            disposes.Increment);

        AsyncDuplexStreamingCall<GrpcBatchServerKeyValueRequest, GrpcBatchServerKeyValueResponse> keyValueCall = new(
            keyValueWriter,
            new PendingAsyncStreamReader<GrpcBatchServerKeyValueResponse>(),
            Task.FromResult(new Metadata()),
            static () => Status.DefaultSuccess,
            static () => [],
            disposes.Increment);

        return (new GrpcServerSharedStreaming(streamId, lockCall, keyValueCall), disposes);
    }

    private static Lazy<List<GrpcServerSharedStreaming>> CreatedLazy(GrpcServerSharedStreaming streaming)
    {
        Lazy<List<GrpcServerSharedStreaming>> lazy = new(() => [streaming]);
        _ = lazy.Value;    // force creation so eviction reaches the dispose path
        return lazy;
    }

    private static Task InvokeWriteBounded(string url, GrpcServerSharedStreaming streaming, GrpcBatchServerKeyValueRequest request)
    {
        GrpcServerBatcher batcher = new(url, NullLogger.Instance);

        MethodInfo method = BatcherType
            .GetMethod("WriteBoundedAsync", BindingFlags.NonPublic | BindingFlags.Instance)!
            .MakeGenericMethod(typeof(GrpcBatchServerKeyValueRequest));

        try
        {
            return (Task)method.Invoke(batcher, [streaming, streaming.KeyValueStreaming.RequestStream, request])!;
        }
        catch (TargetInvocationException ex) when (ex.InnerException is not null)
        {
            return Task.FromException(ex.InnerException);
        }
    }

    /// <summary>Never yields an item and never closes — a stream that has simply gone quiet.</summary>
    private sealed class PendingAsyncStreamReader<T> : IAsyncStreamReader<T>
    {
        private readonly TaskCompletionSource<bool> never = new();
        public T Current => default!;
        public Task<bool> MoveNext(CancellationToken cancellationToken) => never.Task;
    }

    private sealed class EmptyAsyncStreamReader<T> : IAsyncStreamReader<T>
    {
        public T Current => default!;
        public Task<bool> MoveNext(CancellationToken cancellationToken) => Task.FromResult(false);
    }

    private sealed class NoopClientStreamWriter<T> : IClientStreamWriter<T>
    {
        public WriteOptions? WriteOptions { get; set; }
        public Task CompleteAsync() => Task.CompletedTask;
        public Task WriteAsync(T message) => Task.CompletedTask;
    }

    /// <summary>A write onto a stalled HTTP/2 session: accepted, never completed.</summary>
    private sealed class HangingClientStreamWriter<T> : IClientStreamWriter<T>
    {
        private readonly TaskCompletionSource never = new();
        public WriteOptions? WriteOptions { get; set; }
        public Task CompleteAsync() => Task.CompletedTask;
        public Task WriteAsync(T message) => never.Task;
    }
}
