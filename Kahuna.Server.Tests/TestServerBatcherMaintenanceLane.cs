using Kahuna.Server.Communication;
using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using System.Threading.Channels;
using Grpc.Core;
using Kommander;
using Kommander.Time;
using Microsoft.Extensions.Logging.Abstractions;

using Kahuna;
using Kahuna.Communication.External.Grpc;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Writes;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.Tests;

/// <summary>
/// The inter-node batcher serializes placement/replication handlers per stream in arrival order,
/// on a dedicated lane. The lane must not capture the response-write gate while a service call
/// runs: a slow seed or import must not delay the write of a completed unrelated response on the
/// same stream. These tests drive the real streaming entry point and assert both halves — the
/// unrelated response escapes while a maintenance call is stuck, and the lane still executes its
/// operations one at a time, in arrival order, through faults and stream cancellation.
/// The durable transaction requests share the stream but not the lane: they run concurrently with
/// each other and with the lane's operations.
/// </summary>
public sealed class TestServerBatcherMaintenanceLane
{
    private static readonly HLCTimestamp LastModified = new(1, 7000, 4);

    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);

    [Fact]
    public async Task StuckMaintenanceCall_DoesNotBlockCompletedUnrelatedResponse()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        BlockingSeedKahuna kahuna = new();
        KeyValuesService service = new(kahuna, NodeTransportGate.Disabled, NullLogger<IKahuna>.Instance);
        ChannelStreamWriter responses = new();

        Task batch = service.BatchServerKeyValueRequests(
            new ListStreamReader<GrpcBatchServerKeyValueRequest>([
                new GrpcBatchServerKeyValueRequest
                {
                    Type = GrpcServerBatchType.ServerTryEnsureKeyRangeSeeded,
                    RequestId = 1,
                    EnsureKeyRangeSeeded = new GrpcEnsureKeyRangeSeededRequest { KeySpace = "alpha" }
                },
                new GrpcBatchServerKeyValueRequest
                {
                    Type = GrpcServerBatchType.ServerTryGetKeyValue,
                    RequestId = 2,
                    TryGetKeyValue = new GrpcTryGetKeyValueRequest { Key = "healthy", Revision = -1 }
                }
            ]),
            responses,
            new StubServerCallContext(CancellationToken.None));

        // The seed is inside its service call and holds nothing else up.
        await kahuna.SeedEntered.Task.WaitAsync(Timeout, ct);

        // The completed read escapes the stream while the seed is still stuck.
        GrpcBatchServerKeyValueResponse first = await responses.Written.ReadAsync(ct).AsTask().WaitAsync(Timeout, ct);
        Assert.Equal(2, first.RequestId);
        Assert.Equal(GrpcServerBatchType.ServerTryGetKeyValue, first.Type);
        Assert.False(batch.IsCompleted);

        kahuna.SeedGate.TrySetResult(true);

        GrpcBatchServerKeyValueResponse second = await responses.Written.ReadAsync(ct).AsTask().WaitAsync(Timeout, ct);
        Assert.Equal(1, second.RequestId);
        Assert.Equal(GrpcServerBatchType.ServerTryEnsureKeyRangeSeeded, second.Type);
        Assert.True(second.EnsureKeyRangeSeeded.Success);

        await batch.WaitAsync(Timeout, ct);
    }

    [Fact]
    public async Task LaneOperations_ExecuteOneAtATime_InArrivalOrder()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        OrderRecordingSeedKahuna kahuna = new(gatedKeySpace: "alpha");
        KeyValuesService service = new(kahuna, NodeTransportGate.Disabled, NullLogger<IKahuna>.Instance);
        ChannelStreamWriter responses = new();

        Task batch = service.BatchServerKeyValueRequests(
            new ListStreamReader<GrpcBatchServerKeyValueRequest>([
                new GrpcBatchServerKeyValueRequest
                {
                    Type = GrpcServerBatchType.ServerTryEnsureKeyRangeSeeded,
                    RequestId = 1,
                    EnsureKeyRangeSeeded = new GrpcEnsureKeyRangeSeededRequest { KeySpace = "alpha" }
                },
                new GrpcBatchServerKeyValueRequest
                {
                    Type = GrpcServerBatchType.ServerTryEnsureKeyRangeSeeded,
                    RequestId = 2,
                    EnsureKeyRangeSeeded = new GrpcEnsureKeyRangeSeededRequest { KeySpace = "beta" }
                }
            ]),
            responses,
            new StubServerCallContext(CancellationToken.None));

        string firstEntered = await kahuna.Entered.Reader.ReadAsync(ct).AsTask().WaitAsync(Timeout, ct);
        Assert.Equal("alpha", firstEntered);

        // The second lane operation must wait for the first. A short grace period gives a broken
        // implementation the chance to start it early.
        await Task.Delay(200, ct);
        Assert.False(kahuna.Entered.Reader.TryRead(out _));

        kahuna.Gate.TrySetResult(true);

        string secondEntered = await kahuna.Entered.Reader.ReadAsync(ct).AsTask().WaitAsync(Timeout, ct);
        Assert.Equal("beta", secondEntered);

        // Lane responses keep arrival order too: the first write completes before the second runs.
        GrpcBatchServerKeyValueResponse first = await responses.Written.ReadAsync(ct).AsTask().WaitAsync(Timeout, ct);
        GrpcBatchServerKeyValueResponse second = await responses.Written.ReadAsync(ct).AsTask().WaitAsync(Timeout, ct);
        Assert.Equal(1, first.RequestId);
        Assert.Equal(2, second.RequestId);

        await batch.WaitAsync(Timeout, ct);
    }

    [Fact]
    public async Task FaultedLaneOperation_IsRefused_AndReleasesTheLane()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        ThrowingSeedKahuna kahuna = new(poisonedKeySpace: "poisoned");
        KeyValuesService service = new(kahuna, NodeTransportGate.Disabled, NullLogger<IKahuna>.Instance);
        ChannelStreamWriter responses = new();

        await service.BatchServerKeyValueRequests(
            new ListStreamReader<GrpcBatchServerKeyValueRequest>([
                new GrpcBatchServerKeyValueRequest
                {
                    Type = GrpcServerBatchType.ServerTryEnsureKeyRangeSeeded,
                    RequestId = 1,
                    EnsureKeyRangeSeeded = new GrpcEnsureKeyRangeSeededRequest { KeySpace = "poisoned" }
                },
                new GrpcBatchServerKeyValueRequest
                {
                    Type = GrpcServerBatchType.ServerTryEnsureKeyRangeSeeded,
                    RequestId = 2,
                    EnsureKeyRangeSeeded = new GrpcEnsureKeyRangeSeededRequest { KeySpace = "healthy" }
                }
            ]),
            responses,
            new StubServerCallContext(CancellationToken.None)).WaitAsync(Timeout, ct);

        // The faulted operation answers first, with the None-envelope refusal a seed response
        // cannot express any other way; the successor is untouched by the fault.
        GrpcBatchServerKeyValueResponse refused = await responses.Written.ReadAsync(ct).AsTask().WaitAsync(Timeout, ct);
        Assert.Equal(1, refused.RequestId);
        Assert.Equal(GrpcServerBatchType.ServerTypeNone, refused.Type);

        GrpcBatchServerKeyValueResponse served = await responses.Written.ReadAsync(ct).AsTask().WaitAsync(Timeout, ct);
        Assert.Equal(2, served.RequestId);
        Assert.Equal(GrpcServerBatchType.ServerTryEnsureKeyRangeSeeded, served.Type);
        Assert.True(served.EnsureKeyRangeSeeded.Success);
    }

    [Fact]
    public async Task CancelledStream_DrainsQueuedLaneWorkWithoutRunningIt()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        using CancellationTokenSource cancellation = new();

        OrderRecordingSeedKahuna kahuna = new(gatedKeySpace: "alpha");
        KeyValuesService service = new(kahuna, NodeTransportGate.Disabled, NullLogger<IKahuna>.Instance);
        ChannelStreamWriter responses = new();

        Task batch = service.BatchServerKeyValueRequests(
            new ListStreamReader<GrpcBatchServerKeyValueRequest>([
                new GrpcBatchServerKeyValueRequest
                {
                    Type = GrpcServerBatchType.ServerTryEnsureKeyRangeSeeded,
                    RequestId = 1,
                    EnsureKeyRangeSeeded = new GrpcEnsureKeyRangeSeededRequest { KeySpace = "alpha" }
                },
                new GrpcBatchServerKeyValueRequest
                {
                    Type = GrpcServerBatchType.ServerTryEnsureKeyRangeSeeded,
                    RequestId = 2,
                    EnsureKeyRangeSeeded = new GrpcEnsureKeyRangeSeededRequest { KeySpace = "beta" }
                }
            ]),
            responses,
            new StubServerCallContext(cancellation.Token));

        string firstEntered = await kahuna.Entered.Reader.ReadAsync(ct).AsTask().WaitAsync(Timeout, ct);
        Assert.Equal("alpha", firstEntered);

        // The caller is gone. The stuck call still finishes, and the batcher drains: the queued
        // lane operation is skipped instead of invoked against a dead stream.
        cancellation.Cancel();
        kahuna.Gate.TrySetResult(true);

        await batch.WaitAsync(Timeout, ct);

        Assert.False(kahuna.Entered.Reader.TryRead(out _));
        Assert.False(responses.Written.TryRead(out _));
    }

    /// <summary>
    /// Two forwarded durable requests of one kind on one stream: the second starts and answers while the first
    /// is still inside its service call. A durable request holds no lane turn, so forwarded durable work on a
    /// stream is not limited to one Raft round at a time.
    /// </summary>
    [Theory]
    [InlineData(GrpcServerBatchType.ServerDurableOperation)]
    [InlineData(GrpcServerBatchType.ServerDurableBundle)]
    [InlineData(GrpcServerBatchType.ServerDurableOnePhase)]
    [InlineData(GrpcServerBatchType.ServerDurableDecision)]
    [InlineData(GrpcServerBatchType.ServerLookupTransactionRecord)]
    public async Task DurableRequestBehindAnUnansweredOne_DoesNotWaitForIt(GrpcServerBatchType type)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        const int GatedPartition = 7;
        const int FreePartition = 8;

        GatedDurableKahuna kahuna = new(GatedPartition);
        KeyValuesService service = new(kahuna, NodeTransportGate.Disabled, NullLogger<IKahuna>.Instance);
        ChannelStreamWriter responses = new();

        Task batch = service.BatchServerKeyValueRequests(
            new ListStreamReader<GrpcBatchServerKeyValueRequest>([
                DurableRequest(type, requestId: 1, GatedPartition),
                DurableRequest(type, requestId: 2, FreePartition)
            ]),
            responses,
            new StubServerCallContext(CancellationToken.None));

        // Both requests are inside their service calls at once, in arrival order.
        Assert.Equal(GatedPartition, await kahuna.Entered.Reader.ReadAsync(ct).AsTask().WaitAsync(Timeout, ct));
        Assert.Equal(FreePartition, await kahuna.Entered.Reader.ReadAsync(ct).AsTask().WaitAsync(Timeout, ct));

        // The second answers while the first is still held.
        GrpcBatchServerKeyValueResponse first = await responses.Written.ReadAsync(ct).AsTask().WaitAsync(Timeout, ct);
        Assert.Equal(2, first.RequestId);
        Assert.Equal(type, first.Type);
        Assert.False(batch.IsCompleted);

        kahuna.Gate.TrySetResult();

        GrpcBatchServerKeyValueResponse second = await responses.Written.ReadAsync(ct).AsTask().WaitAsync(Timeout, ct);
        Assert.Equal(1, second.RequestId);
        Assert.Equal(type, second.Type);

        await batch.WaitAsync(Timeout, ct);
    }

    /// <summary>
    /// A durable request that arrives behind a stuck lane operation is not queued behind it, and a lane operation
    /// that arrives behind a stuck durable request is not queued behind that either: the two share a stream, not
    /// a lane.
    /// </summary>
    [Fact]
    public async Task DurableRequestAndLaneOperation_DoNotWaitForEachOther()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        const int GatedPartition = 7;
        const int FreePartition = 8;

        GatedDurableKahuna kahuna = new(GatedPartition, gatedKeySpace: "alpha");
        KeyValuesService service = new(kahuna, NodeTransportGate.Disabled, NullLogger<IKahuna>.Instance);
        ChannelStreamWriter responses = new();

        Task batch = service.BatchServerKeyValueRequests(
            new ListStreamReader<GrpcBatchServerKeyValueRequest>([
                new GrpcBatchServerKeyValueRequest
                {
                    Type = GrpcServerBatchType.ServerTryEnsureKeyRangeSeeded,
                    RequestId = 1,
                    EnsureKeyRangeSeeded = new GrpcEnsureKeyRangeSeededRequest { KeySpace = "alpha" }
                },
                DurableRequest(GrpcServerBatchType.ServerDurableOperation, requestId: 2, GatedPartition),
                DurableRequest(GrpcServerBatchType.ServerDurableOperation, requestId: 3, FreePartition),
                new GrpcBatchServerKeyValueRequest
                {
                    Type = GrpcServerBatchType.ServerTryEnsureKeyRangeSeeded,
                    RequestId = 4,
                    EnsureKeyRangeSeeded = new GrpcEnsureKeyRangeSeededRequest { KeySpace = "beta" }
                }
            ]),
            responses,
            new StubServerCallContext(CancellationToken.None));

        // The free durable request answers past the stuck seed and the stuck durable request.
        GrpcBatchServerKeyValueResponse first = await responses.Written.ReadAsync(ct).AsTask().WaitAsync(Timeout, ct);
        Assert.Equal(3, first.RequestId);

        // The second seed still waits for the first: the lane keeps its own order.
        await Task.Delay(200, ct);
        Assert.False(responses.Written.TryRead(out _));
        Assert.DoesNotContain("beta", kahuna.SeedsEntered);

        // Releasing the seed lets the lane run on while the gated durable request is still held.
        kahuna.SeedGate.TrySetResult(true);

        GrpcBatchServerKeyValueResponse seeded = await responses.Written.ReadAsync(ct).AsTask().WaitAsync(Timeout, ct);
        GrpcBatchServerKeyValueResponse seededNext = await responses.Written.ReadAsync(ct).AsTask().WaitAsync(Timeout, ct);
        Assert.Equal(1, seeded.RequestId);
        Assert.Equal(4, seededNext.RequestId);
        Assert.False(batch.IsCompleted);

        kahuna.Gate.TrySetResult();

        GrpcBatchServerKeyValueResponse last = await responses.Written.ReadAsync(ct).AsTask().WaitAsync(Timeout, ct);
        Assert.Equal(2, last.RequestId);

        await batch.WaitAsync(Timeout, ct);
    }

    /// <summary>
    /// Two seeds on one stream: the second waits on the lane until the first answers, and the lane-wait histogram
    /// records that wait for the second and a free lane for the first.
    /// </summary>
    [Fact]
    public async Task LaneOperationBehindAnUnansweredOne_RecordsItsLaneWait()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        const int HeldMs = 300;

        ConcurrentBag<(string Request, double Ms)> waits = [];

        using MeterListener listener = new();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Name == "kahuna.internode.ordered_lane_wait_ms")
                l.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<double>((_, value, tags, _) =>
        {
            foreach (KeyValuePair<string, object?> tag in tags)
                if (tag.Key == "request")
                    waits.Add(((string)tag.Value!, value));
        });
        listener.Start();

        OrderRecordingSeedKahuna kahuna = new(gatedKeySpace: "alpha");
        KeyValuesService service = new(kahuna, NodeTransportGate.Disabled, NullLogger<IKahuna>.Instance);
        ChannelStreamWriter responses = new();

        Task batch = service.BatchServerKeyValueRequests(
            new ListStreamReader<GrpcBatchServerKeyValueRequest>([
                new GrpcBatchServerKeyValueRequest
                {
                    Type = GrpcServerBatchType.ServerTryEnsureKeyRangeSeeded,
                    RequestId = 1,
                    EnsureKeyRangeSeeded = new GrpcEnsureKeyRangeSeededRequest { KeySpace = "alpha" }
                },
                new GrpcBatchServerKeyValueRequest
                {
                    Type = GrpcServerBatchType.ServerTryEnsureKeyRangeSeeded,
                    RequestId = 2,
                    EnsureKeyRangeSeeded = new GrpcEnsureKeyRangeSeededRequest { KeySpace = "beta" }
                }
            ]),
            responses,
            new StubServerCallContext(CancellationToken.None));

        Assert.Equal("alpha", await kahuna.Entered.Reader.ReadAsync(ct).AsTask().WaitAsync(Timeout, ct));

        await Task.Delay(HeldMs, ct);
        kahuna.Gate.TrySetResult(true);

        Assert.Equal("beta", await kahuna.Entered.Reader.ReadAsync(ct).AsTask().WaitAsync(Timeout, ct));
        await batch.WaitAsync(Timeout, ct);

        listener.Dispose();

        List<double> seedWaits = [.. waits.Where(w => w.Request == "seed_range").Select(w => w.Ms)];

        // Other tests can record on the same process-wide meter, so the assertions look for this test's two
        // samples instead of counting every sample.
        Assert.Contains(seedWaits, ms => ms >= HeldMs * 0.8);
        Assert.Contains(seedWaits, ms => ms < HeldMs * 0.5);
    }

    private static GrpcBatchServerKeyValueRequest DurableRequest(GrpcServerBatchType type, int requestId, int partitionId)
    {
        GrpcBatchServerKeyValueRequest request = new() { Type = type, RequestId = requestId };

        switch (type)
        {
            case GrpcServerBatchType.ServerDurableOperation:
                request.DurableOperation = new GrpcDurableOperationRequest { PartitionId = partitionId };
                break;

            case GrpcServerBatchType.ServerDurableBundle:
                request.DurableBundle = new GrpcDurableBundleRequest { PartitionId = partitionId };
                break;

            case GrpcServerBatchType.ServerDurableOnePhase:
                request.DurableOnePhase = new GrpcDurableOnePhaseRequest { PartitionId = partitionId };
                break;

            case GrpcServerBatchType.ServerDurableDecision:
                request.DurableDecision = new GrpcDurableDecisionRequest { PartitionId = partitionId };
                break;

            case GrpcServerBatchType.ServerLookupTransactionRecord:
                request.LookupTransactionRecord = new GrpcLookupTransactionRecordRequest { PartitionId = partitionId };
                break;

            default:
                throw new ArgumentOutOfRangeException(nameof(type), type, "Not a durable request type");
        }

        return request;
    }

    /// <summary>Every durable request for one partition blocks until the gate opens; every other partition
    /// answers at once. Each request that starts reports its partition. Seeding one key space blocks on its own
    /// gate.</summary>
    private sealed class GatedDurableKahuna : FakeKahunaBase
    {
        private readonly int gatedPartition;

        private readonly string? gatedKeySpace;

        public Channel<int> Entered { get; } = Channel.CreateUnbounded<int>();

        public ConcurrentQueue<string> SeedsEntered { get; } = new();

        public TaskCompletionSource Gate { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource<bool> SeedGate { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public GatedDurableKahuna(int gatedPartition, string? gatedKeySpace = null)
        {
            this.gatedPartition = gatedPartition;
            this.gatedKeySpace = gatedKeySpace;
        }

        private async Task EnterAsync(int partitionId)
        {
            Entered.Writer.TryWrite(partitionId);

            if (partitionId == gatedPartition)
                await Gate.Task;
        }

        public override async Task<bool> RegisterKeyRangeAsync(string keySpace, CancellationToken cancellationToken = default)
        {
            SeedsEntered.Enqueue(keySpace);

            if (keySpace == gatedKeySpace)
                return await SeedGate.Task;

            return true;
        }

        public override async Task<bool> DurableOperationLocal(int partitionId, int kind, string logType, byte[] payload, CancellationToken cancellationToken)
        {
            await EnterAsync(partitionId);
            return true;
        }

        public override async Task<byte[]?> LookupTransactionRecordLocal(int partitionId, HLCTimestamp transactionId, long epoch, string anchorKey, CancellationToken cancellationToken)
        {
            await EnterAsync(partitionId);
            return null;
        }

        public override async Task<DurableBundleWireReply?> DurableBundleLocal(
            int partitionId, IReadOnlyList<(string LogType, byte[] Payload)> entries, bool terminal, int stage,
            IReadOnlyList<RangeFence> fences, CancellationToken cancellationToken)
        {
            await EnterAsync(partitionId);
            return null;
        }

        public override async Task<DurableDecisionWireReply?> DurableDecisionLocal(
            int partitionId, byte[] decisionDelta, HLCTimestamp transactionId, long epoch,
            IReadOnlyList<RangeFence> fences, CancellationToken cancellationToken)
        {
            await EnterAsync(partitionId);
            return null;
        }

        public override async Task<DurableOnePhaseWireReply?> DurableOnePhaseLocal(
            int partitionId, byte[] recordInitDelta, byte[] anchorPrepareDelta, byte[] decisionDelta,
            HLCTimestamp transactionId, long epoch, HLCTimestamp opId, IReadOnlyList<RangeFence> fences,
            long expectedTerm, CancellationToken cancellationToken)
        {
            await EnterAsync(partitionId);
            return null;
        }
    }

    /// <summary>Seeding blocks until the gate opens; reads answer immediately.</summary>
    private sealed class BlockingSeedKahuna : FakeKahunaBase
    {
        public TaskCompletionSource<bool> SeedGate { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource SeedEntered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public override async Task<bool> RegisterKeyRangeAsync(string keySpace, CancellationToken cancellationToken = default)
        {
            SeedEntered.TrySetResult();
            return await SeedGate.Task;
        }

        public override ValueTask<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> LocateAndTryGetValue(
            HLCTimestamp transactionId, string key, long revision, HLCTimestamp readTimestamp,
            KeyValueDurability durability, CancellationToken cancellationToken,
            string coordinatorKey = "", TransactionOperationId operationId = default, KeyValueReadMode readMode = KeyValueReadMode.Default, KeyValueIntentWait intentWait = default)
            => ValueTask.FromResult<(KeyValueResponseType, ReadOnlyKeyValueEntry?)>((
                KeyValueResponseType.Get,
                new ReadOnlyKeyValueEntry([1, 2, 3], 1, HLCTimestamp.Zero, HLCTimestamp.Zero, LastModified, KeyValueState.Set)));
    }

    /// <summary>Reports every seed entry in order; one key space blocks until the gate opens.</summary>
    private sealed class OrderRecordingSeedKahuna : FakeKahunaBase
    {
        private readonly string gatedKeySpace;

        public Channel<string> Entered { get; } = Channel.CreateUnbounded<string>();

        public TaskCompletionSource<bool> Gate { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public OrderRecordingSeedKahuna(string gatedKeySpace) => this.gatedKeySpace = gatedKeySpace;

        public override async Task<bool> RegisterKeyRangeAsync(string keySpace, CancellationToken cancellationToken = default)
        {
            Entered.Writer.TryWrite(keySpace);

            if (keySpace == gatedKeySpace)
                return await Gate.Task;

            return true;
        }
    }

    /// <summary>Seeding one key space faults; every other key space succeeds.</summary>
    private sealed class ThrowingSeedKahuna : FakeKahunaBase
    {
        private readonly string poisonedKeySpace;

        public ThrowingSeedKahuna(string poisonedKeySpace) => this.poisonedKeySpace = poisonedKeySpace;

        public override Task<bool> RegisterKeyRangeAsync(string keySpace, CancellationToken cancellationToken = default)
            => keySpace == poisonedKeySpace
                ? throw new RaftException("Invalid partition: 3")
                : Task.FromResult(true);
    }

    /// <summary>Feeds a fixed list of requests to the streaming handler, then closes the stream.</summary>
    private sealed class ListStreamReader<T> : IAsyncStreamReader<T>
    {
        private readonly IReadOnlyList<T> items;

        private int index = -1;

        public ListStreamReader(IReadOnlyList<T> items) => this.items = items;

        public T Current => items[index];

        public Task<bool> MoveNext(CancellationToken cancellationToken) => Task.FromResult(++index < items.Count);
    }

    /// <summary>Exposes handler writes as an awaitable ordered sequence.</summary>
    private sealed class ChannelStreamWriter : IServerStreamWriter<GrpcBatchServerKeyValueResponse>
    {
        private readonly Channel<GrpcBatchServerKeyValueResponse> channel = Channel.CreateUnbounded<GrpcBatchServerKeyValueResponse>();

        public ChannelReader<GrpcBatchServerKeyValueResponse> Written => channel.Reader;

        public WriteOptions? WriteOptions { get; set; }

        public Task WriteAsync(GrpcBatchServerKeyValueResponse message)
        {
            channel.Writer.TryWrite(message);
            return Task.CompletedTask;
        }
    }

    /// <summary>Minimal context: the batcher reads only the cancellation token.</summary>
    private sealed class StubServerCallContext : ServerCallContext
    {
        private readonly CancellationToken cancellationToken;

        public StubServerCallContext(CancellationToken cancellationToken) => this.cancellationToken = cancellationToken;

        protected override CancellationToken CancellationTokenCore => cancellationToken;
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
