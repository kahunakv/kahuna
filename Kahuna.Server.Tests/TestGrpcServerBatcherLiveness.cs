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

        // Seed through the real admission path so the pending-request accounting stays balanced
        // when the sweep (or the test's cleanup) later removes the request.
        Assert.True(GrpcServerBatcher.TryAdmit(item));
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
            GrpcServerBatcher.TryTakeRequest(oldRequest, out _);
            GrpcServerBatcher.TryTakeRequest(freshRequest, out _);
            RequestStreamRefs().TryRemove(oldRequest, out _);
            RequestStreamRefs().TryRemove(freshRequest, out _);
            old.TrySetCanceled(TestContext.Current.CancellationToken);
            fresh.TrySetCanceled(TestContext.Current.CancellationToken);
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
            await streaming.KeyValueWriteSemaphore.WaitAsync(TestContext.Current.CancellationToken);     // the stuck previous writer

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
            GrpcServerBatcher.TryTakeRequest(requestId, out _);
            RequestStreamRefs().TryRemove(requestId, out _);
            pending.TrySetCanceled(TestContext.Current.CancellationToken);
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
            Assert.True(await streaming.KeyValueWriteSemaphore.WaitAsync(TimeSpan.FromSeconds(1), TestContext.Current.CancellationToken));
            streaming.KeyValueWriteSemaphore.Release();
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

    // ── deadline coverage of the queue ───────────────────────────────────────

    /// <summary>
    /// A request is registered for the deadline when it is admitted, not when it is written to
    /// the stream. The reaper must therefore also fail a request that never reached a send —
    /// here the second request, queued behind a first write that hangs on a stalled stream.
    /// Before that registration moved to admission time, only the sent request was visible to
    /// the sweep and the queued one waited on the much later write-timeout cascade.
    /// </summary>
    [Fact]
    public async Task Reaper_FailsRequestsQueuedBehindABlockedWrite()
    {
        const string url = "test://queued-behind-blocked-write";
        const long streamId = 9_800_004;

        (GrpcServerSharedStreaming streaming, _) = MakeSharedStreaming(streamId, new HangingClientStreamWriter<GrpcBatchServerKeyValueRequest>());
        Streamings()[url] = CreatedLazy(streaming);

        GrpcServerBatcher batcher = new(url, NullLogger.Instance);

        try
        {
            Task<GrpcServerBatcherResponse> first = batcher.Enqueue(new GrpcLookupTransactionRecordRequest());
            Task<GrpcServerBatcherResponse> second = batcher.Enqueue(new GrpcLookupTransactionRecordRequest());

            long farFuture = Environment.TickCount64 + (long)GrpcServerBatcher.RequestDeadline.TotalMilliseconds + 1_000;
            GrpcServerBatcher.SweepExpiredRequests(farFuture, NullLogger.Instance);

            // The sweep settles both promises synchronously: both requests were admitted into
            // requestRefs before Enqueue returned, whether or not either reached a write.
            Assert.True(first.IsFaulted);
            Assert.True(second.IsFaulted);

            RpcException firstFailure = await Assert.ThrowsAsync<RpcException>(() => first);
            RpcException secondFailure = await Assert.ThrowsAsync<RpcException>(() => second);
            Assert.Equal(StatusCode.Unavailable, firstFailure.StatusCode);
            Assert.Equal(StatusCode.Unavailable, secondFailure.StatusCode);
        }
        finally
        {
            Streamings().TryRemove(url, out _);
        }
    }

    /// <summary>
    /// A request the reaper already failed while it waited in the inbox must not be written to
    /// the stream: nobody listens to its promise, and the peer's answer would arrive as an
    /// orphan response. The batch loop must skip it and send only the live request.
    /// </summary>
    [Fact]
    public async Task RunBatch_SkipsRequestsTheReaperAlreadyFailed()
    {
        const string url = "test://run-batch-skips-settled";
        const long streamId = 9_800_005;
        const int settledId = 9_950_101;
        const int liveId = 9_950_102;

        RecordingClientStreamWriter<GrpcBatchServerKeyValueRequest> writer = new();
        (GrpcServerSharedStreaming streaming, _) = MakeSharedStreaming(streamId, writer);
        Streamings()[url] = CreatedLazy(streaming);

        TaskCompletionSource<GrpcServerBatcherResponse> settledPromise = new(TaskCreationOptions.RunContinuationsAsynchronously);
        GrpcServerBatcherItem settledItem = new(
            GrpcServerBatcherItemType.KeyValues, settledId,
            new GrpcServerBatcherRequest(new GrpcLookupTransactionRecordRequest()), settledPromise);

        TaskCompletionSource<GrpcServerBatcherResponse> livePromise = new(TaskCreationOptions.RunContinuationsAsynchronously);
        GrpcServerBatcherItem liveItem = new(
            GrpcServerBatcherItemType.KeyValues, liveId,
            new GrpcServerBatcherRequest(new GrpcLookupTransactionRecordRequest()), livePromise);

        // The reaper failed the first request before the batch loop reached it: its promise is
        // settled and its requestRefs entry is gone.
        settledPromise.TrySetException(new RpcException(new(StatusCode.Unavailable, "expired before dispatch")));
        Assert.True(GrpcServerBatcher.TryAdmit(liveItem));

        try
        {
            await InvokeRunBatch(url, [settledItem, liveItem]);

            GrpcBatchServerKeyValueRequest written = Assert.Single(writer.Written);
            Assert.Equal(liveId, written.RequestId);

            Assert.False(RequestStreamRefs().ContainsKey(settledId));
            Assert.True(RequestStreamRefs().ContainsKey(liveId));
        }
        finally
        {
            Streamings().TryRemove(url, out _);
            GrpcServerBatcher.TryTakeRequest(liveId, out _);
            RequestStreamRefs().TryRemove(liveId, out _);
            livePromise.TrySetCanceled(TestContext.Current.CancellationToken);
        }
    }

    // ── dispatch buffer ownership ────────────────────────────────────────────

    /// <summary>
    /// The dispatch loop hands <c>RunBatch</c> the buffer it reuses for the next drain.
    /// <c>RunBatch</c> must leave that list exactly as it found it: no clear, no reuse, and no
    /// release to a pool the loop knows nothing about.
    /// </summary>
    [Fact]
    public async Task RunBatch_LeavesTheCallersListUntouched()
    {
        const string url = "test://run-batch-list-ownership";
        const long streamId = 9_800_010;
        const int firstId = 9_950_301;
        const int secondId = 9_950_302;

        RecordingClientStreamWriter<GrpcBatchServerKeyValueRequest> writer = new();
        (GrpcServerSharedStreaming streaming, _) = MakeSharedStreaming(streamId, writer);
        Streamings()[url] = CreatedLazy(streaming);

        // Both promises are settled, so RunBatch skips both items and writes nothing. What
        // matters here is only what it does to the list it was handed.
        TaskCompletionSource<GrpcServerBatcherResponse> firstPromise = new(TaskCreationOptions.RunContinuationsAsynchronously);
        firstPromise.TrySetCanceled(TestContext.Current.CancellationToken);
        TaskCompletionSource<GrpcServerBatcherResponse> secondPromise = new(TaskCreationOptions.RunContinuationsAsynchronously);
        secondPromise.TrySetCanceled(TestContext.Current.CancellationToken);

        List<GrpcServerBatcherItem> requests =
        [
            new(GrpcServerBatcherItemType.KeyValues, firstId, new(new GrpcLookupTransactionRecordRequest()), firstPromise),
            new(GrpcServerBatcherItemType.KeyValues, secondId, new(new GrpcLookupTransactionRecordRequest()), secondPromise)
        ];

        try
        {
            await InvokeRunBatch(url, requests);

            Assert.Empty(writer.Written);
            Assert.Equal(2, requests.Count);
            Assert.Equal(firstId, requests[0].RequestId);
            Assert.Equal(secondId, requests[1].RequestId);
        }
        finally
        {
            Streamings().TryRemove(url, out _);
        }
    }

    /// <summary>
    /// A backlog larger than one drain must reach the wire completely, exactly once per request,
    /// through drains that reuse one buffer. Afterwards the buffer must be empty — an idle
    /// batcher pins no payloads and no promises — and its backing array must not have grown past
    /// the drain bound, because the bound exists so one burst cannot leave a backlog-sized array
    /// attached to the batcher for its whole life.
    /// </summary>
    [Fact]
    public async Task DispatchLoop_BoundsTheDrainAndReusesTheBuffer()
    {
        const string url = "test://dispatch-buffer-bound";
        const long streamId = 9_800_011;

        // Larger than one drain, so the loop needs more than one round for the backlog.
        const int backlog = GrpcServerBatcher.MaxItemsPerDrain + 476;

        GatedRecordingClientStreamWriter<GrpcBatchServerKeyValueRequest> writer = new();
        (GrpcServerSharedStreaming streaming, _) = MakeSharedStreaming(streamId, writer);
        Streamings()[url] = CreatedLazy(streaming);

        GrpcServerBatcher batcher = new(url, NullLogger.Instance);
        List<Task<GrpcServerBatcherResponse>> pending = new(backlog + 1);

        try
        {
            // The first request starts the dispatch loop; its write parks on the writer's gate,
            // so the backlog below lands in the inbox while the loop is mid-batch.
            pending.Add(batcher.Enqueue(new GrpcLookupTransactionRecordRequest()));
            await writer.FirstWriteStarted.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

            for (int i = 0; i < backlog; i++)
                pending.Add(batcher.Enqueue(new GrpcLookupTransactionRecordRequest()));

            writer.Release();

            // Queued key-value work travels in coalesced envelopes, so the wire carries fewer
            // messages than requests; count the operations inside the carriers.
            static IEnumerable<GrpcBatchServerKeyValueRequest> Operations(IReadOnlyList<GrpcBatchServerKeyValueRequest> messages)
                => messages.SelectMany(static m => m.Type == GrpcServerBatchType.ServerCoalesced
                    ? m.Coalesced
                    : (IEnumerable<GrpcBatchServerKeyValueRequest>)[m]);

            await WaitUntilAsync(() => Operations(writer.Written).Count() == backlog + 1, TimeSpan.FromSeconds(10));

            IReadOnlyList<GrpcBatchServerKeyValueRequest> written = writer.Written;
            Assert.Equal(backlog + 1, Operations(written).Count());
            Assert.Equal(backlog + 1, Operations(written).Select(static w => w.RequestId).Distinct().Count());

            // The loop clears the buffer after the final drain; wait out that last step.
            await WaitUntilAsync(() => DispatchBuffer(batcher) is { Count: 0 }, TimeSpan.FromSeconds(5));

            List<GrpcServerBatcherItem>? buffer = DispatchBuffer(batcher);
            Assert.NotNull(buffer);
            Assert.True(buffer.Capacity <= GrpcServerBatcher.MaxItemsPerDrain,
                $"Dispatch buffer capacity {buffer.Capacity} exceeds the drain bound {GrpcServerBatcher.MaxItemsPerDrain}.");
        }
        finally
        {
            Streamings().TryRemove(url, out _);

            // No peer ever answers the recorded writes, so release each admission and settle each
            // promise here instead of leaving them to the deadline reaper.
            HashSet<Task> mine = new(pending.Count);
            foreach (Task<GrpcServerBatcherResponse> task in pending)
                mine.Add(task);

            foreach (KeyValuePair<int, GrpcServerBatcherItem> entry in RequestRefs().ToArray())
            {
                if (!mine.Contains(entry.Value.Promise.Task))
                    continue;

                GrpcServerBatcher.TryTakeRequest(entry.Key, out _);
                RequestStreamRefs().TryRemove(entry.Key, out _);
                entry.Value.Promise.TrySetCanceled(TestContext.Current.CancellationToken);
            }
        }
    }

    /// <summary>
    /// A transport failure fails the drained requests, and the loop must keep working: a later
    /// round drains fresh requests into the same reused buffer. A buffer whose lifetime was wrong
    /// — cleared by the batch path, or handed to a pool on the failure path — would show up here
    /// as a request that never completes.
    /// </summary>
    [Fact]
    public async Task DispatchLoop_KeepsWorkingAcrossTransportFailures()
    {
        const string url = "test://dispatch-buffer-failures";
        const long streamIdBase = 9_800_020;

        GrpcServerBatcher batcher = new(url, NullLogger.Instance);

        try
        {
            for (int round = 0; round < 3; round++)
            {
                // Each failed write evicts the URL's streams, so every round injects a fresh one.
                (GrpcServerSharedStreaming streaming, _) = MakeSharedStreaming(
                    streamIdBase + round, new ThrowingClientStreamWriter<GrpcBatchServerKeyValueRequest>());
                Streamings()[url] = CreatedLazy(streaming);

                // One request per round: a failed write evicts the URL's streams, and a second
                // in-flight request would rebuild them through the real channel factory, which
                // cannot serve a test:// URL.
                Task<GrpcServerBatcherResponse> request = batcher.Enqueue(new GrpcLookupTransactionRecordRequest());

                RpcException failure = await Assert.ThrowsAsync<RpcException>(() => request);
                Assert.Equal(StatusCode.Unavailable, failure.StatusCode);
            }
        }
        finally
        {
            Streamings().TryRemove(url, out _);
        }
    }

    // ── admission ────────────────────────────────────────────────────────────

    /// <summary>
    /// When the pending item limit is reached, an enqueue must fail immediately with a retryable
    /// Unavailable — before the request enters the inbox or the tracking dictionaries — so
    /// sustained overload sheds new work instead of growing the pending set without bound.
    /// </summary>
    [Fact]
    public void Enqueue_PendingItemLimitReached_FailsRetryablyBeforeQueueing()
    {
        int savedMax = GrpcServerBatcher.MaxPendingRequests;
        GrpcServerBatcher.MaxPendingRequests = 0;

        try
        {
            GrpcServerBatcher batcher = new("test://admission-item-limit", NullLogger.Instance);
            Task<GrpcServerBatcherResponse> task = batcher.Enqueue(new GrpcLookupTransactionRecordRequest());

            Assert.True(task.IsFaulted);
            RpcException failure = Assert.IsType<RpcException>(task.Exception!.InnerException);
            Assert.Equal(StatusCode.Unavailable, failure.StatusCode);
        }
        finally
        {
            GrpcServerBatcher.MaxPendingRequests = savedMax;
        }
    }

    /// <summary>
    /// The byte limit sheds work the same way as the item limit, so a burst of few but large
    /// requests cannot hold unbounded memory while it waits out the deadline.
    /// </summary>
    [Fact]
    public void Enqueue_PendingByteLimitReached_FailsRetryablyBeforeQueueing()
    {
        long savedMax = GrpcServerBatcher.MaxPendingRequestBytes;
        GrpcServerBatcher.MaxPendingRequestBytes = 0;

        try
        {
            GrpcServerBatcher batcher = new("test://admission-byte-limit", NullLogger.Instance);
            Task<GrpcServerBatcherResponse> task = batcher.Enqueue(new GrpcLookupTransactionRecordRequest());

            Assert.True(task.IsFaulted);
            RpcException failure = Assert.IsType<RpcException>(task.Exception!.InnerException);
            Assert.Equal(StatusCode.Unavailable, failure.StatusCode);
        }
        finally
        {
            GrpcServerBatcher.MaxPendingRequestBytes = savedMax;
        }
    }

    /// <summary>
    /// One admission releases exactly once: the first take wins and returns the item with its
    /// payload accounting; a second take must lose, so no settle path can release the same
    /// admission twice.
    /// </summary>
    [Fact]
    public void AdmitAndTake_ReleasesTheAdmissionExactlyOnce()
    {
        const int requestId = 9_950_201;

        TaskCompletionSource<GrpcServerBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);
        GrpcServerBatcherItem item = new(
            GrpcServerBatcherItemType.KeyValues, requestId,
            new GrpcServerBatcherRequest(new GrpcLookupTransactionRecordRequest { AnchorKey = "accounting-anchor" }), promise);

        Assert.True(item.PayloadBytes > 0);

        try
        {
            Assert.True(GrpcServerBatcher.TryAdmit(item));
            Assert.True(RequestRefs().ContainsKey(requestId));

            Assert.True(GrpcServerBatcher.TryTakeRequest(requestId, out GrpcServerBatcherItem taken));
            Assert.Equal(item.PayloadBytes, taken.PayloadBytes);

            Assert.False(GrpcServerBatcher.TryTakeRequest(requestId, out _));
            Assert.False(RequestRefs().ContainsKey(requestId));
        }
        finally
        {
            GrpcServerBatcher.TryTakeRequest(requestId, out _);
            promise.TrySetCanceled(TestContext.Current.CancellationToken);
        }
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    private static List<GrpcServerBatcherItem>? DispatchBuffer(GrpcServerBatcher batcher)
        => (List<GrpcServerBatcherItem>?)BatcherType
            .GetField("dispatchBuffer", BindingFlags.NonPublic | BindingFlags.Instance)!
            .GetValue(batcher);

    private static async Task WaitUntilAsync(Func<bool> condition, TimeSpan timeout)
    {
        long deadline = Environment.TickCount64 + (long)timeout.TotalMilliseconds;

        while (!condition())
        {
            Assert.True(Environment.TickCount64 < deadline, "The condition was not reached within the timeout.");
            await Task.Delay(10, TestContext.Current.CancellationToken);
        }
    }

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

    private static Task InvokeWriteBounded(string url, GrpcServerSharedStreaming streaming, GrpcBatchServerKeyValueRequest request)
    {
        GrpcServerBatcher batcher = new(url, NullLogger.Instance);

        MethodInfo method = BatcherType
            .GetMethod("WriteBoundedAsync", BindingFlags.NonPublic | BindingFlags.Instance)!
            .MakeGenericMethod(typeof(GrpcBatchServerKeyValueRequest));

        try
        {
            return (Task)method.Invoke(batcher, [streaming, streaming.KeyValueWriteSemaphore, streaming.KeyValueStreaming.RequestStream, request])!;
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

    /// <summary>Accepts every write immediately and records it for assertions.</summary>
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

    /// <summary>
    /// Records every write; the first write parks on a gate until the test releases it, so the
    /// test can build an inbox backlog while the dispatch loop is mid-batch.
    /// </summary>
    private sealed class GatedRecordingClientStreamWriter<T> : IClientStreamWriter<T>
    {
        private readonly TaskCompletionSource gate = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly ConcurrentQueue<T> written = new();
        private int firstWrite = 1;

        public TaskCompletionSource FirstWriteStarted { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public IReadOnlyList<T> Written => written.ToArray();
        public WriteOptions? WriteOptions { get; set; }
        public Task CompleteAsync() => Task.CompletedTask;
        public void Release() => gate.TrySetResult();

        public async Task WriteAsync(T message)
        {
            if (1 == Interlocked.Exchange(ref firstWrite, 0))
            {
                FirstWriteStarted.TrySetResult();
                await gate.Task;
            }

            written.Enqueue(message);
        }
    }

    /// <summary>Fails every write retryably — a stream whose transport is gone.</summary>
    private sealed class ThrowingClientStreamWriter<T> : IClientStreamWriter<T>
    {
        public WriteOptions? WriteOptions { get; set; }
        public Task CompleteAsync() => Task.CompletedTask;

        public Task WriteAsync(T message)
            => Task.FromException(new RpcException(new(StatusCode.Unavailable, "The transport failed.")));
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
