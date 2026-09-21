using Google.Protobuf;
using Grpc.Core;
using Kommander;
using Kommander.Time;
using Microsoft.Extensions.Logging.Abstractions;

using Kahuna;
using Kahuna.Communication.External.Grpc;
using Kahuna.Server.Communication;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.Communication.Grpc;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.Tests;

/// <summary>
/// The client key-value batch stream can carry several requests in one message, and several responses in
/// one message. These tests drive the real streaming entry point and check the three things a frame must
/// never change: every request is answered exactly once by its own id, one request's failure stays its own,
/// and a peer that never sent a frame never receives one.
/// </summary>
public sealed class TestClientBatchFrames
{
    private static readonly HLCTimestamp LastModified = new(1, 7000, 4);

    [Fact]
    public async Task Stream_AnnouncesFrameSupportOnItsResponseHeaders()
    {
        KeyValuesService service = NewService(new ServingKahuna());
        HeaderCapturingContext context = new();

        await service.BatchClientKeyValueRequests(
            new ListStreamReader<GrpcBatchClientKeyValueRequest>([]),
            new CapturingStreamWriter(),
            context);

        Metadata.Entry? header = context.ResponseHeaders?.Get(ClientBatchFrames.SupportHeader);

        Assert.NotNull(header);
        Assert.Equal(ClientBatchFrames.SupportVersion, header.Value);
    }

    [Fact]
    public async Task Frame_EveryItemIsAnsweredByItsOwnId_AndAFaultedItemStaysAlone()
    {
        KeyValuesService service = NewService(new ServingKahuna());
        CapturingStreamWriter responses = new();

        await service.BatchClientKeyValueRequests(
            new ListStreamReader<GrpcBatchClientKeyValueRequest>([
                Frame(
                    Get(1, "a"),
                    Set(2, "poisoned"),
                    Get(3, "b"),
                    Exists(4, "c"))
            ]),
            responses,
            new HeaderCapturingContext());

        List<GrpcBatchClientKeyValueResponse> answers = responses.Flattened();

        Assert.Equal([1, 2, 3, 4], answers.Select(r => r.RequestId).Order());

        Assert.Equal(KeyValueResponseType.Get, (KeyValueResponseType)answers.Single(r => r.RequestId == 1).TryGetKeyValue.Type);
        Assert.Equal(KeyValueResponseType.Get, (KeyValueResponseType)answers.Single(r => r.RequestId == 3).TryGetKeyValue.Type);
        Assert.Equal(KeyValueResponseType.Exists, (KeyValueResponseType)answers.Single(r => r.RequestId == 4).TryExistsKeyValue.Type);

        // The set faulted inside its handler. It is refused by its own id with its own typed response, and
        // the three items that shared its message were served as if it had not been there.
        GrpcBatchClientKeyValueResponse refused = answers.Single(r => r.RequestId == 2);
        Assert.Equal(GrpcClientBatchType.TrySetKeyValue, refused.Type);
        Assert.Equal(KeyValueResponseType.MustRetry, (KeyValueResponseType)refused.TrySetKeyValue.Type);
    }

    [Fact]
    public async Task Frame_AndTheSameRequestsSentAlone_ProduceTheSameAnswers()
    {
        GrpcBatchClientKeyValueRequest[] requests = [Get(1, "a"), Set(2, "poisoned"), Exists(3, "c")];

        CapturingStreamWriter alone = new();
        await NewService(new ServingKahuna()).BatchClientKeyValueRequests(
            new ListStreamReader<GrpcBatchClientKeyValueRequest>(requests.Select(r => r.Clone()).ToList()),
            alone,
            new HeaderCapturingContext());

        CapturingStreamWriter framed = new();
        await NewService(new ServingKahuna()).BatchClientKeyValueRequests(
            new ListStreamReader<GrpcBatchClientKeyValueRequest>([Frame(requests)]),
            framed,
            new HeaderCapturingContext());

        Assert.Equal(
            alone.Flattened().OrderBy(r => r.RequestId).Select(WithoutTiming),
            framed.Flattened().OrderBy(r => r.RequestId).Select(WithoutTiming));
    }

    [Fact]
    public async Task PeerThatNeverSentAFrame_NeverReceivesOne()
    {
        KeyValuesService service = NewService(new ServingKahuna());

        // The first write parks until the stream has delivered everything, so every later response is
        // already waiting when the writer loop comes back for it: the exact condition that packs a frame.
        GatedStreamWriter responses = new();

        List<GrpcBatchClientKeyValueRequest> requests = [];
        for (int i = 1; i <= 40; i++)
            requests.Add(Get(i, "k" + i));

        await service.BatchClientKeyValueRequests(
            new ListStreamReader<GrpcBatchClientKeyValueRequest>(requests, onExhausted: responses.Open),
            responses,
            new HeaderCapturingContext());

        Assert.Equal(40, responses.Written.Count);
        Assert.DoesNotContain(responses.Written, r => r.Type == GrpcClientBatchType.ClientBatchFrame);
    }

    [Fact]
    public async Task PeerThatSentAFrame_ReceivesTheReadyResponsesTogether()
    {
        KeyValuesService service = NewService(new ServingKahuna());
        GatedStreamWriter responses = new();

        GrpcBatchClientKeyValueRequest[] items = new GrpcBatchClientKeyValueRequest[40];
        for (int i = 0; i < items.Length; i++)
            items[i] = Get(i + 1, "k" + i);

        await service.BatchClientKeyValueRequests(
            new ListStreamReader<GrpcBatchClientKeyValueRequest>([Frame(items)], onExhausted: responses.Open),
            responses,
            new HeaderCapturingContext());

        List<GrpcBatchClientKeyValueResponse> answers = responses.Flattened();

        Assert.Equal(Enumerable.Range(1, 40), answers.Select(r => r.RequestId).Order());

        // Whatever was written before the gate opened went alone; everything queued behind it shares
        // messages, so forty answers take far fewer than forty writes.
        Assert.Contains(responses.Written, r => r.Type == GrpcClientBatchType.ClientBatchFrame && r.Frame.Items.Count > 1);
        Assert.True(responses.Written.Count < 40, $"expected packed writes, saw {responses.Written.Count}");
    }

    [Fact]
    public async Task Frame_OverTheItemLimit_RunsTheLimitAndRefusesTheExcess()
    {
        CountingKahuna kahuna = new();
        KeyValuesService service = NewService(kahuna);
        GatedStreamWriter responses = new();

        const int excess = 5;

        GrpcBatchClientKeyValueRequest[] items = new GrpcBatchClientKeyValueRequest[ClientBatchFrames.MaxItems + excess];
        for (int i = 0; i < items.Length; i++)
            items[i] = Get(i + 1, "k" + i);

        await service.BatchClientKeyValueRequests(
            new ListStreamReader<GrpcBatchClientKeyValueRequest>([Frame(items)], onExhausted: responses.Open),
            responses,
            new HeaderCapturingContext());

        List<GrpcBatchClientKeyValueResponse> answers = responses.Flattened();

        // Nothing is dropped: every item is answered once. The items inside the limit ran; the ones past it
        // were refused without reaching a handler, with the retryable answer their own type defines.
        Assert.Equal(Enumerable.Range(1, items.Length), answers.Select(r => r.RequestId).Order());
        Assert.Equal(ClientBatchFrames.MaxItems, kahuna.Reads);

        Assert.All(
            answers.Where(r => r.RequestId <= ClientBatchFrames.MaxItems),
            r => Assert.Equal(KeyValueResponseType.Get, (KeyValueResponseType)r.TryGetKeyValue.Type));

        Assert.All(
            answers.Where(r => r.RequestId > ClientBatchFrames.MaxItems),
            r => Assert.Equal(KeyValueResponseType.MustRetry, (KeyValueResponseType)r.TryGetKeyValue.Type));

        // The limit binds the node's own frames as well.
        Assert.All(
            responses.Written.Where(r => r.Type == GrpcClientBatchType.ClientBatchFrame),
            r => Assert.InRange(r.Frame.Items.Count, 2, ClientBatchFrames.MaxItems));
    }

    [Fact]
    public async Task ResponseFrames_StayInsideTheByteBudget()
    {
        // Two of these cannot share a frame, and one of them alone is over the budget.
        int[] valueSizes = [600 * 1024, 600 * 1024, ClientBatchFrames.MaxBytes + 1024, 600 * 1024, 16, 16, 16];

        KeyValuesService service = NewService(new SizedValueKahuna(valueSizes));
        GatedStreamWriter responses = new();

        GrpcBatchClientKeyValueRequest[] items = new GrpcBatchClientKeyValueRequest[valueSizes.Length];
        for (int i = 0; i < items.Length; i++)
            items[i] = Get(i + 1, i.ToString());

        await service.BatchClientKeyValueRequests(
            new ListStreamReader<GrpcBatchClientKeyValueRequest>([Frame(items)], onExhausted: responses.Open),
            responses,
            new HeaderCapturingContext());

        Assert.Equal(Enumerable.Range(1, items.Length), responses.Flattened().Select(r => r.RequestId).Order());

        foreach (GrpcBatchClientKeyValueResponse written in responses.Written)
        {
            if (written.Type != GrpcClientBatchType.ClientBatchFrame)
                continue;

            Assert.True(
                written.Frame.Items.Sum(item => item.CalculateSize()) <= ClientBatchFrames.MaxBytes,
                "a response frame went over the byte budget");
        }

        // The response that is over the budget on its own still arrives, as a plain single message.
        Assert.Contains(responses.Written, r => r.Type == GrpcClientBatchType.TryGetKeyValue && r.RequestId == 3);
    }

    [Fact]
    public async Task EmptyFrame_AndAFrameInsideAFrame_StartNothing_AndLeaveTheStreamAlive()
    {
        CountingKahuna kahuna = new();
        KeyValuesService service = NewService(kahuna);
        CapturingStreamWriter responses = new();

        await service.BatchClientKeyValueRequests(
            new ListStreamReader<GrpcBatchClientKeyValueRequest>([
                Frame(),
                Frame(Frame(Get(1, "nested")), Get(2, "sibling")),
                Get(3, "after")
            ]),
            responses,
            new HeaderCapturingContext());

        // The nested frame is not a request the contract allows, so nothing inside it runs. Its sibling in
        // the same message and the request after it are served, and the call ends normally.
        Assert.Equal([2, 3], responses.Flattened().Select(r => r.RequestId).Order());
        Assert.Equal(2, kahuna.Reads);
    }

    private static KeyValuesService NewService(IKahuna kahuna) =>
        new(kahuna, NodeTransportGate.Disabled, NullLogger<IKahuna>.Instance);

    private static GrpcBatchClientKeyValueRequest Frame(params GrpcBatchClientKeyValueRequest[] items)
    {
        GrpcBatchClientKeyValueRequest frame = new()
        {
            Type = GrpcClientBatchType.ClientBatchFrame,
            Frame = new()
        };

        frame.Frame.Items.AddRange(items);

        return frame;
    }

    private static GrpcBatchClientKeyValueRequest Get(int requestId, string key) => new()
    {
        Type = GrpcClientBatchType.TryGetKeyValue,
        RequestId = requestId,
        TryGetKeyValue = new GrpcTryGetKeyValueRequest { Key = key, Revision = -1 }
    };

    private static GrpcBatchClientKeyValueRequest Exists(int requestId, string key) => new()
    {
        Type = GrpcClientBatchType.TryExistsKeyValue,
        RequestId = requestId,
        TryExistsKeyValue = new GrpcTryExistsKeyValueRequest { Key = key, Revision = -1 }
    };

    private static GrpcBatchClientKeyValueRequest Set(int requestId, string key) => new()
    {
        Type = GrpcClientBatchType.TrySetKeyValue,
        RequestId = requestId,
        TrySetKeyValue = new GrpcTrySetKeyValueRequest { Key = key, ExpiresMs = 0 }
    };

    /// <summary>
    /// A response with its elapsed-time fields cleared, so two runs of the same request compare equal.
    /// </summary>
    private static GrpcBatchClientKeyValueResponse WithoutTiming(GrpcBatchClientKeyValueResponse response)
    {
        GrpcBatchClientKeyValueResponse copy = response.Clone();

        if (copy.TryGetKeyValue is not null)
            copy.TryGetKeyValue.TimeElapsedMs = 0;

        if (copy.TrySetKeyValue is not null)
            copy.TrySetKeyValue.TimeElapsedMs = 0;

        if (copy.TryExistsKeyValue is not null)
            copy.TryExistsKeyValue.TimeElapsedMs = 0;

        return copy;
    }

    /// <summary>Reads and existence checks succeed; sets fail inside the handler.</summary>
    private class ServingKahuna : FakeKahunaBase
    {
        public override Task<(KeyValueResponseType, long, HLCTimestamp)> LocateAndTrySetKeyValue(
            HLCTimestamp transactionId, string key, byte[]? value, byte[]? compareValue, long compareRevision,
            KeyValueFlags flags, int expiresMs, KeyValueDurability durability, CancellationToken cancellationToken,
            long routedGeneration = 0, string coordinatorKey = "", TransactionOperationId operationId = default)
            => throw new RaftException("Invalid partition: 3");

        public override Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> LocateAndTryGetValue(
            HLCTimestamp transactionId, string key, long revision, HLCTimestamp readTimestamp,
            KeyValueDurability durability, CancellationToken cancellationToken,
            string coordinatorKey = "", TransactionOperationId operationId = default)
            => Task.FromResult<(KeyValueResponseType, ReadOnlyKeyValueEntry?)>((
                KeyValueResponseType.Get,
                new ReadOnlyKeyValueEntry(ValueFor(key), 1, HLCTimestamp.Zero, HLCTimestamp.Zero, LastModified, KeyValueState.Set)));

        public override Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> LocateAndTryExistsValue(
            HLCTimestamp transactionId, string key, long revision, HLCTimestamp readTimestamp,
            KeyValueDurability durability, CancellationToken cancellationToken,
            string coordinatorKey = "", TransactionOperationId operationId = default)
            => Task.FromResult<(KeyValueResponseType, ReadOnlyKeyValueEntry?)>((
                KeyValueResponseType.Exists,
                new ReadOnlyKeyValueEntry(null, 1, HLCTimestamp.Zero, HLCTimestamp.Zero, LastModified, KeyValueState.Set)));

        protected virtual byte[] ValueFor(string key) => [1, 2, 3];
    }

    /// <summary>Counts the reads that reached the store, to tell a served item from a refused one.</summary>
    private sealed class CountingKahuna : ServingKahuna
    {
        private int reads;

        public int Reads => Volatile.Read(ref reads);

        protected override byte[] ValueFor(string key)
        {
            Interlocked.Increment(ref reads);

            return [1, 2, 3];
        }
    }

    /// <summary>Answers the read of key <c>"i"</c> with a value of the i-th configured size.</summary>
    private sealed class SizedValueKahuna(int[] sizes) : ServingKahuna
    {
        protected override byte[] ValueFor(string key) => new byte[sizes[int.Parse(key)]];
    }

    /// <summary>Feeds a fixed list of requests to the streaming handler, then closes the stream.</summary>
    private sealed class ListStreamReader<T>(IReadOnlyList<T> items, Action? onExhausted = null) : IAsyncStreamReader<T>
    {
        private int index = -1;

        public T Current => items[index];

        public Task<bool> MoveNext(CancellationToken cancellationToken)
        {
            if (++index < items.Count)
                return Task.FromResult(true);

            // The handlers of these tests complete synchronously, so by the time the stream asks for a
            // message that is not there, every response of every earlier message is already queued.
            onExhausted?.Invoke();

            return Task.FromResult(false);
        }
    }

    /// <summary>
    /// Captures a copy of everything written to the response stream. A copy, because the batcher reuses its
    /// frame envelope as soon as a write completes, exactly as it may with a real transport that has
    /// serialized the message by then.
    /// </summary>
    private class CapturingStreamWriter : IServerStreamWriter<GrpcBatchClientKeyValueResponse>
    {
        private readonly Lock mutex = new();

        public List<GrpcBatchClientKeyValueResponse> Written { get; } = [];

        public WriteOptions? WriteOptions { get; set; }

        public virtual Task WriteAsync(GrpcBatchClientKeyValueResponse message)
        {
            lock (mutex)
                Written.Add(message.Clone());

            return Task.CompletedTask;
        }

        /// <summary>Every answer the stream carried, whether it travelled alone or inside a frame.</summary>
        public List<GrpcBatchClientKeyValueResponse> Flattened()
        {
            List<GrpcBatchClientKeyValueResponse> answers = [];

            lock (mutex)
            {
                foreach (GrpcBatchClientKeyValueResponse written in Written)
                {
                    if (written.Type == GrpcClientBatchType.ClientBatchFrame)
                        answers.AddRange(written.Frame.Items);
                    else
                        answers.Add(written);
                }
            }

            return answers;
        }
    }

    /// <summary>
    /// Holds the first write until <see cref="Open"/>, so the responses behind it pile up in the batcher's
    /// channel the way they do behind a slow socket.
    /// </summary>
    private sealed class GatedStreamWriter : CapturingStreamWriter
    {
        private readonly TaskCompletionSource gate = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public void Open() => gate.TrySetResult();

        public override async Task WriteAsync(GrpcBatchClientKeyValueResponse message)
        {
            await gate.Task;

            await base.WriteAsync(message);
        }
    }

    /// <summary>Minimal context: the batcher reads the cancellation token and writes the response headers.</summary>
    private sealed class HeaderCapturingContext : ServerCallContext
    {
        public Metadata? ResponseHeaders { get; private set; }

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

        protected override Task WriteResponseHeadersAsyncCore(Metadata responseHeaders)
        {
            ResponseHeaders = responseHeaders;

            return Task.CompletedTask;
        }
    }
}
