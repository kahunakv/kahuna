using Google.Protobuf;
using Grpc.Core;

using Kahuna.Client;
using Kahuna.Client.Communication;
using Kahuna.Shared.Communication.Grpc;

namespace Kahuna.Server.Tests;

/// <summary>
/// The client batcher may send the key-value requests it drained together as one stream message. These
/// tests drive <c>RunBatch</c> against a stream they control and check what may never change: a node that
/// did not announce frames never receives one, every request is written exactly once, a cancelled request
/// is left out without disturbing the others, and the limits of a frame hold.
///
/// <para>Hermetic: the stream is a hand-built duplex call, so no node and no network are involved.</para>
/// </summary>
public sealed class TestClientRequestFrames
{
    // The batcher tracks in-flight requests in a static table keyed by request id, shared by every
    // batcher in the process, so each test takes ids no other test uses.
    private static int nextRequestId = -5_000_000;

    [Fact]
    public async Task NodeThatAnnouncedNothing_NeverReceivesAFrame()
    {
        using TestStream stream = TestStream.WithHeaders(new Metadata());
        GrpcBatcher batcher = stream.NewBatcher();

        List<GrpcBatcherItem> items = NewGets(6);

        await batcher.RunBatch(items);

        Assert.Equal(6, stream.Written.Count);
        Assert.DoesNotContain(stream.Written, m => m.Type == GrpcClientBatchType.ClientBatchFrame);
        Assert.Equal(items.Select(i => i.RequestId).Order(), stream.SentRequestIds().Order());

        stream.AnswerAll();
    }

    [Fact]
    public async Task HeadersStillOnTheirWay_MeanSingleMessages_AndFramesStartOnceTheyArrive()
    {
        TaskCompletionSource<Metadata> headers = new(TaskCreationOptions.RunContinuationsAsynchronously);

        using TestStream stream = new(headers.Task);
        GrpcBatcher batcher = stream.NewBatcher();

        // The node has not said anything yet. Waiting for it would stall requests behind a header that an
        // old node never sends, so the batch goes out the old way.
        await batcher.RunBatch(NewGets(4));

        Assert.Equal(4, stream.Written.Count);
        Assert.DoesNotContain(stream.Written, m => m.Type == GrpcClientBatchType.ClientBatchFrame);

        headers.SetResult(SupportHeaders());
        await headers.Task;

        List<GrpcBatcherItem> later = NewGets(5);

        await batcher.RunBatch(later);

        GrpcBatchClientKeyValueRequest frame = Assert.Single(stream.Written.Skip(4));
        Assert.Equal(GrpcClientBatchType.ClientBatchFrame, frame.Type);
        Assert.Equal(later.Select(i => i.RequestId), frame.Frame.Items.Select(i => i.RequestId));

        // Each item is the complete single request it would have been on its own.
        Assert.All(frame.Frame.Items, i =>
        {
            Assert.Equal(GrpcClientBatchType.TryGetKeyValue, i.Type);
            Assert.NotNull(i.TryGetKeyValue);
        });

        stream.AnswerAll();
    }

    [Fact]
    public async Task FramesSwitchedOffInTheOptions_MeanSingleMessages()
    {
        using TestStream stream = TestStream.WithHeaders(SupportHeaders());
        GrpcBatcher batcher = stream.NewBatcher(new KahunaOptions { GrpcRequestFrames = false });

        await batcher.RunBatch(NewGets(5));

        Assert.Equal(5, stream.Written.Count);
        Assert.DoesNotContain(stream.Written, m => m.Type == GrpcClientBatchType.ClientBatchFrame);

        stream.AnswerAll();
    }

    [Fact]
    public async Task RequestThatIsAlone_TravelsAsThePlainMessageItAlwaysWas()
    {
        using TestStream stream = TestStream.WithHeaders(SupportHeaders());
        GrpcBatcher batcher = stream.NewBatcher();

        await batcher.RunBatch(NewGets(1));

        GrpcBatchClientKeyValueRequest only = Assert.Single(stream.Written);
        Assert.Equal(GrpcClientBatchType.TryGetKeyValue, only.Type);

        stream.AnswerAll();
    }

    [Fact]
    public async Task CancelledRequest_IsLeftOutOfTheFrame()
    {
        using TestStream stream = TestStream.WithHeaders(SupportHeaders());
        GrpcBatcher batcher = stream.NewBatcher();

        List<GrpcBatcherItem> items = NewGets(4);
        items[1].Promise.TrySetCanceled();

        await batcher.RunBatch(items);

        GrpcBatchClientKeyValueRequest frame = Assert.Single(stream.Written);
        Assert.Equal(
            new[] { items[0].RequestId, items[2].RequestId, items[3].RequestId },
            frame.Frame.Items.Select(i => i.RequestId));

        stream.AnswerAll();
    }

    [Fact]
    public async Task FrameLeftWithOneRequest_IsSentAsThatRequest_AndWithNone_IsNotSent()
    {
        using TestStream stream = TestStream.WithHeaders(SupportHeaders());
        GrpcBatcher batcher = stream.NewBatcher();

        List<GrpcBatcherItem> pair = NewGets(2);
        pair[0].Promise.TrySetCanceled();

        await batcher.RunBatch(pair);

        GrpcBatchClientKeyValueRequest survivor = Assert.Single(stream.Written);
        Assert.Equal(GrpcClientBatchType.TryGetKeyValue, survivor.Type);
        Assert.Equal(pair[1].RequestId, survivor.RequestId);

        List<GrpcBatcherItem> none = NewGets(3);
        foreach (GrpcBatcherItem item in none)
            item.Promise.TrySetCanceled();

        await batcher.RunBatch(none);

        Assert.Single(stream.Written);

        stream.AnswerAll();
    }

    [Fact]
    public async Task RequestCancelledWhileTheFrameWaitsForTheStream_IsDropped_AndTheOthersStillGo()
    {
        // No permit: the frame parks on the stream's write lock the way it does behind another batcher's
        // slow write.
        using TestStream stream = TestStream.WithHeaders(SupportHeaders(), initialPermits: 0);
        GrpcBatcher batcher = stream.NewBatcher();

        using CancellationTokenSource cancelFirst = new();

        List<GrpcBatcherItem> items =
        [
            NewGet(cancelFirst.Token),
            NewGet(CancellationToken.None),
            NewGet(CancellationToken.None)
        ];

        Task batch = batcher.RunBatch(items);

        await Task.Delay(30, TestContext.Current.CancellationToken);
        Assert.False(batch.IsCompleted);

        // The wait is bounded by the first live request's token. Cancelling it must not strand the batch:
        // the wait starts again behind the next live request.
        cancelFirst.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => items[0].Promise.Task);

        stream.Semaphore.Release();

        await batch.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        GrpcBatchClientKeyValueRequest frame = Assert.Single(stream.Written);
        Assert.Equal(
            new[] { items[1].RequestId, items[2].RequestId },
            frame.Frame.Items.Select(i => i.RequestId));

        stream.AnswerAll();
    }

    [Fact]
    public async Task BatchLargerThanAFrame_BecomesSeveralFrames_AndEveryRequestIsWrittenOnce()
    {
        using TestStream stream = TestStream.WithHeaders(SupportHeaders());
        GrpcBatcher batcher = stream.NewBatcher();

        List<GrpcBatcherItem> items = NewGets(ClientBatchFrames.MaxItems * 2 + 7);

        await batcher.RunBatch(items);

        Assert.All(stream.Written, m => Assert.Equal(GrpcClientBatchType.ClientBatchFrame, m.Type));
        Assert.All(stream.Written, m => Assert.InRange(m.Frame.Items.Count, 2, ClientBatchFrames.MaxItems));
        Assert.Equal(3, stream.Written.Count);

        // Order is kept, nothing is lost, and nothing is sent twice.
        Assert.Equal(items.Select(i => i.RequestId), stream.SentRequestIds());

        stream.AnswerAll();
    }

    [Fact]
    public async Task ByteBudget_SplitsTheBatch_AndAnOversizedRequestTravelsAlone()
    {
        using TestStream stream = TestStream.WithHeaders(SupportHeaders());
        GrpcBatcher batcher = stream.NewBatcher();

        // Two of the large values cannot share a frame; the huge one is over the budget by itself.
        int[] valueSizes = [600 * 1024, 600 * 1024, 16, ClientBatchFrames.MaxBytes + 4096, 16, 16, 600 * 1024];

        List<GrpcBatcherItem> items = valueSizes.Select(NewSet).ToList();

        await batcher.RunBatch(items);

        foreach (GrpcBatchClientKeyValueRequest written in stream.Written)
        {
            if (written.Type != GrpcClientBatchType.ClientBatchFrame)
                continue;

            Assert.True(
                written.Frame.Items.Sum(i => i.CalculateSize()) <= ClientBatchFrames.MaxBytes,
                "a request frame went over the byte budget");
        }

        GrpcBatchClientKeyValueRequest oversized = Assert.Single(
            stream.Written, m => m.Type == GrpcClientBatchType.TrySetKeyValue && m.RequestId == items[3].RequestId);
        Assert.Equal(valueSizes[3], oversized.TrySetKeyValue.Value.Length);

        Assert.Equal(items.Select(i => i.RequestId), stream.SentRequestIds());

        stream.AnswerAll();
    }

    [Fact]
    public async Task LockRequests_KeepTheirOwnStream_AndKeyValueRequestsAroundThemStillShareAFrame()
    {
        using TestStream stream = TestStream.WithHeaders(SupportHeaders());
        GrpcBatcher batcher = stream.NewBatcher();

        GrpcBatcherItem lockItem = new(
            GrpcBatcherItemType.Locks,
            Interlocked.Decrement(ref nextRequestId),
            new GrpcBatcherRequest(new GrpcTryLockRequest { Resource = "r" }),
            new(TaskCreationOptions.RunContinuationsAsynchronously),
            CancellationToken.None);

        List<GrpcBatcherItem> items = [NewGet(CancellationToken.None), lockItem, NewGet(CancellationToken.None)];

        await batcher.RunBatch(items);

        GrpcBatchClientLockRequest sentLock = Assert.Single(stream.WrittenLocks);
        Assert.Equal(lockItem.RequestId, sentLock.RequestId);

        GrpcBatchClientKeyValueRequest frame = Assert.Single(stream.Written);
        Assert.Equal(new[] { items[0].RequestId, items[2].RequestId }, frame.Frame.Items.Select(i => i.RequestId));

        stream.AnswerAll();
        GrpcBatcher.DispatchKeyValueResponse(new() { RequestId = lockItem.RequestId });
        lockItem.Promise.TrySetCanceled();
    }

    [Fact]
    public async Task ResponseFrame_CompletesEachRequestWithItsOwnAnswer()
    {
        using TestStream stream = TestStream.WithHeaders(SupportHeaders());
        GrpcBatcher batcher = stream.NewBatcher();

        List<GrpcBatcherItem> items = NewGets(3);

        await batcher.RunBatch(items);

        GrpcBatchClientKeyValueResponse frame = new() { Type = GrpcClientBatchType.ClientBatchFrame, Frame = new() };

        // Answers arrive in the order they were ready, not in the order the requests were sent, and an
        // answer for a request nobody waits for any more can ride along.
        foreach (GrpcBatcherItem item in items.AsEnumerable().Reverse())
        {
            frame.Frame.Items.Add(new GrpcBatchClientKeyValueResponse
            {
                Type = GrpcClientBatchType.TryGetKeyValue,
                RequestId = item.RequestId,
                TryGetKeyValue = new() { Value = ByteString.CopyFromUtf8("answer-" + item.RequestId), Revision = item.RequestId }
            });
        }

        frame.Frame.Items.Add(new GrpcBatchClientKeyValueResponse
        {
            Type = GrpcClientBatchType.TryGetKeyValue,
            RequestId = Interlocked.Decrement(ref nextRequestId),
            TryGetKeyValue = new()
        });

        // A frame inside a frame is not followed.
        frame.Frame.Items.Add(new GrpcBatchClientKeyValueResponse { Type = GrpcClientBatchType.ClientBatchFrame, Frame = new() });

        GrpcBatcher.DispatchKeyValueResponse(frame);

        foreach (GrpcBatcherItem item in items)
        {
            GrpcBatcherResponse answer = await item.Promise.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

            Assert.Equal("answer-" + item.RequestId, answer.TryGetKeyValue!.Value.ToStringUtf8());
            Assert.Equal(item.RequestId, answer.TryGetKeyValue.Revision);
        }
    }

    [Fact]
    public async Task WriteFailure_FailsEveryRequestOfTheFrame_AndLeavesNoneTracked()
    {
        using TestStream stream = TestStream.WithHeaders(SupportHeaders());
        stream.FailWrites(new RpcException(new Status(StatusCode.Unavailable, "connection lost")));

        GrpcBatcher batcher = stream.NewBatcher();

        List<GrpcBatcherItem> items = NewGets(5);

        await batcher.RunBatch(items);

        // A frame that failed to leave may or may not have reached the node, and that is true of every
        // request in it, so none of them is resent here: each caller gets the transport failure and its
        // own retry contract decides what happens next.
        foreach (GrpcBatcherItem item in items)
        {
            RpcException ex = await Assert.ThrowsAsync<RpcException>(() => item.Promise.Task);
            Assert.Equal(StatusCode.Unavailable, ex.StatusCode);
        }

        // Nothing is left waiting for an answer that cannot come: a late answer finds no request.
        foreach (GrpcBatcherItem item in items)
            GrpcBatcher.DispatchKeyValueResponse(new() { Type = GrpcClientBatchType.TryGetKeyValue, RequestId = item.RequestId, TryGetKeyValue = new() });

        Assert.All(items, i => Assert.True(i.Promise.Task.IsFaulted));
    }

    private static Metadata SupportHeaders() => new()
    {
        { ClientBatchFrames.SupportHeader, ClientBatchFrames.SupportVersion }
    };

    private static List<GrpcBatcherItem> NewGets(int count)
    {
        List<GrpcBatcherItem> items = new(count);

        for (int i = 0; i < count; i++)
            items.Add(NewGet(CancellationToken.None));

        return items;
    }

    private static GrpcBatcherItem NewGet(CancellationToken cancellationToken)
    {
        int requestId = Interlocked.Decrement(ref nextRequestId);

        return new(
            GrpcBatcherItemType.KeyValues,
            requestId,
            new GrpcBatcherRequest(new GrpcTryGetKeyValueRequest { Key = "key" + requestId, Revision = -1 }),
            new(TaskCreationOptions.RunContinuationsAsynchronously),
            cancellationToken);
    }

    private static GrpcBatcherItem NewSet(int valueSize)
    {
        int requestId = Interlocked.Decrement(ref nextRequestId);

        return new(
            GrpcBatcherItemType.KeyValues,
            requestId,
            new GrpcBatcherRequest(new GrpcTrySetKeyValueRequest
            {
                Key = "key" + requestId,
                Value = UnsafeByteOperations.UnsafeWrap(new byte[valueSize])
            }),
            new(TaskCreationOptions.RunContinuationsAsynchronously),
            CancellationToken.None);
    }

    /// <summary>
    /// A shared stream whose two request writers record what they are given and whose response headers the
    /// test decides. Injected for a URL of its own, so it never meets another test's stream.
    /// </summary>
    private sealed class TestStream : IDisposable
    {
        private static int nextStream;

        private readonly string url = "https://request-frames-test-" + Interlocked.Increment(ref nextStream) + ":99";

        private readonly RecordingWriter<GrpcBatchClientKeyValueRequest> keyValueWriter = new();

        private readonly RecordingWriter<GrpcBatchClientLockRequest> lockWriter = new();

        private readonly GrpcSharedStreaming streaming;

        public TestStream(Task<Metadata> headers, int initialPermits = 1)
        {
            streaming = new(
                9100 + nextStream,
                NewCall<GrpcBatchClientLockRequest, GrpcBatchClientLockResponse>(lockWriter, Task.FromResult(new Metadata())),
                NewCall<GrpcBatchClientKeyValueRequest, GrpcBatchClientKeyValueResponse>(keyValueWriter, headers));

            if (initialPermits == 0)
                streaming.Semaphore.Wait();

            GrpcBatcher.InjectTestSharedStreaming(url, streaming);
        }

        public static TestStream WithHeaders(Metadata headers, int initialPermits = 1) =>
            new(Task.FromResult(headers), initialPermits);

        public SemaphoreSlim Semaphore => streaming.Semaphore;

        public List<GrpcBatchClientKeyValueRequest> Written => keyValueWriter.Written;

        public List<GrpcBatchClientLockRequest> WrittenLocks => lockWriter.Written;

        public GrpcBatcher NewBatcher(KahunaOptions? options = null) => new(url, securityOptions: options);

        public void FailWrites(Exception ex) => keyValueWriter.Failure = ex;

        /// <summary>The ids of every key-value request written, alone or inside a frame, in write order.</summary>
        public List<int> SentRequestIds()
        {
            List<int> ids = [];

            foreach (GrpcBatchClientKeyValueRequest written in Written)
            {
                if (written.Type == GrpcClientBatchType.ClientBatchFrame)
                    ids.AddRange(written.Frame.Items.Select(i => i.RequestId));
                else
                    ids.Add(written.RequestId);
            }

            return ids;
        }

        /// <summary>
        /// Answers everything that was written, so no request of this test stays in the batcher's
        /// process-wide tracking table after the test ends.
        /// </summary>
        public void AnswerAll()
        {
            foreach (int requestId in SentRequestIds())
            {
                GrpcBatcher.DispatchKeyValueResponse(new()
                {
                    Type = GrpcClientBatchType.TryGetKeyValue,
                    RequestId = requestId,
                    TryGetKeyValue = new()
                });
            }
        }

        public void Dispose()
        {
            GrpcBatcher.RemoveTestSharedStreaming(url);
            streaming.Dispose();
        }

        private static AsyncDuplexStreamingCall<TRequest, TResponse> NewCall<TRequest, TResponse>(
            IClientStreamWriter<TRequest> writer, Task<Metadata> headers) =>
            new(
                writer,
                new EmptyReader<TResponse>(),
                headers,
                () => Status.DefaultSuccess,
                () => new Metadata(),
                () => { });
    }

    /// <summary>
    /// Records a copy of every message. A copy, because the batcher refills its frame envelope as soon as
    /// a write completes, as it may with a real transport that has serialized the message by then.
    /// </summary>
    private sealed class RecordingWriter<T> : IClientStreamWriter<T> where T : IDeepCloneable<T>
    {
        public List<T> Written { get; } = [];

        public Exception? Failure { get; set; }

        public WriteOptions? WriteOptions { get; set; }

        public Task WriteAsync(T message)
        {
            if (Failure is not null)
                return Task.FromException(Failure);

            lock (Written)
                Written.Add(message.Clone());

            return Task.CompletedTask;
        }

        public Task CompleteAsync() => Task.CompletedTask;
    }

    private sealed class EmptyReader<T> : IAsyncStreamReader<T>
    {
        public T Current => default!;

        public Task<bool> MoveNext(CancellationToken cancellationToken) => Task.FromResult(false);
    }
}
