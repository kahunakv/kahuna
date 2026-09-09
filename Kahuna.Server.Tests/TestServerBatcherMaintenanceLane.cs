using System.Threading.Channels;
using Grpc.Core;
using Kommander;
using Kommander.Time;
using Microsoft.Extensions.Logging.Abstractions;

using Kahuna;
using Kahuna.Communication.External.Grpc;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.Tests;

/// <summary>
/// The inter-node batcher serializes placement/replication handlers per stream in arrival order,
/// on a dedicated lane. The lane must not capture the response-write gate while a service call
/// runs: a slow seed or import must not delay the write of a completed unrelated response on the
/// same stream. These tests drive the real streaming entry point and assert both halves — the
/// unrelated response escapes while a maintenance call is stuck, and the lane still executes its
/// operations one at a time, in arrival order, through faults and stream cancellation.
/// </summary>
public sealed class TestServerBatcherMaintenanceLane
{
    private static readonly HLCTimestamp LastModified = new(1, 7000, 4);

    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);

    [Fact]
    public async Task StuckMaintenanceCall_DoesNotBlockCompletedUnrelatedResponse()
    {
        BlockingSeedKahuna kahuna = new();
        KeyValuesService service = new(kahuna, NullLogger<IKahuna>.Instance);
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
        await kahuna.SeedEntered.Task.WaitAsync(Timeout);

        // The completed read escapes the stream while the seed is still stuck.
        GrpcBatchServerKeyValueResponse first = await responses.Written.ReadAsync().AsTask().WaitAsync(Timeout);
        Assert.Equal(2, first.RequestId);
        Assert.Equal(GrpcServerBatchType.ServerTryGetKeyValue, first.Type);
        Assert.False(batch.IsCompleted);

        kahuna.SeedGate.TrySetResult(true);

        GrpcBatchServerKeyValueResponse second = await responses.Written.ReadAsync().AsTask().WaitAsync(Timeout);
        Assert.Equal(1, second.RequestId);
        Assert.Equal(GrpcServerBatchType.ServerTryEnsureKeyRangeSeeded, second.Type);
        Assert.True(second.EnsureKeyRangeSeeded.Success);

        await batch.WaitAsync(Timeout);
    }

    [Fact]
    public async Task LaneOperations_ExecuteOneAtATime_InArrivalOrder()
    {
        OrderRecordingSeedKahuna kahuna = new(gatedKeySpace: "alpha");
        KeyValuesService service = new(kahuna, NullLogger<IKahuna>.Instance);
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

        string firstEntered = await kahuna.Entered.Reader.ReadAsync().AsTask().WaitAsync(Timeout);
        Assert.Equal("alpha", firstEntered);

        // The second lane operation must wait for the first. A short grace period gives a broken
        // implementation the chance to start it early.
        await Task.Delay(200);
        Assert.False(kahuna.Entered.Reader.TryRead(out _));

        kahuna.Gate.TrySetResult(true);

        string secondEntered = await kahuna.Entered.Reader.ReadAsync().AsTask().WaitAsync(Timeout);
        Assert.Equal("beta", secondEntered);

        // Lane responses keep arrival order too: the first write completes before the second runs.
        GrpcBatchServerKeyValueResponse first = await responses.Written.ReadAsync().AsTask().WaitAsync(Timeout);
        GrpcBatchServerKeyValueResponse second = await responses.Written.ReadAsync().AsTask().WaitAsync(Timeout);
        Assert.Equal(1, first.RequestId);
        Assert.Equal(2, second.RequestId);

        await batch.WaitAsync(Timeout);
    }

    [Fact]
    public async Task FaultedLaneOperation_IsRefused_AndReleasesTheLane()
    {
        ThrowingSeedKahuna kahuna = new(poisonedKeySpace: "poisoned");
        KeyValuesService service = new(kahuna, NullLogger<IKahuna>.Instance);
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
            new StubServerCallContext(CancellationToken.None)).WaitAsync(Timeout);

        // The faulted operation answers first, with the None-envelope refusal a seed response
        // cannot express any other way; the successor is untouched by the fault.
        GrpcBatchServerKeyValueResponse refused = await responses.Written.ReadAsync().AsTask().WaitAsync(Timeout);
        Assert.Equal(1, refused.RequestId);
        Assert.Equal(GrpcServerBatchType.ServerTypeNone, refused.Type);

        GrpcBatchServerKeyValueResponse served = await responses.Written.ReadAsync().AsTask().WaitAsync(Timeout);
        Assert.Equal(2, served.RequestId);
        Assert.Equal(GrpcServerBatchType.ServerTryEnsureKeyRangeSeeded, served.Type);
        Assert.True(served.EnsureKeyRangeSeeded.Success);
    }

    [Fact]
    public async Task CancelledStream_DrainsQueuedLaneWorkWithoutRunningIt()
    {
        using CancellationTokenSource cancellation = new();

        OrderRecordingSeedKahuna kahuna = new(gatedKeySpace: "alpha");
        KeyValuesService service = new(kahuna, NullLogger<IKahuna>.Instance);
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

        string firstEntered = await kahuna.Entered.Reader.ReadAsync().AsTask().WaitAsync(Timeout);
        Assert.Equal("alpha", firstEntered);

        // The caller is gone. The stuck call still finishes, and the batcher drains: the queued
        // lane operation is skipped instead of invoked against a dead stream.
        cancellation.Cancel();
        kahuna.Gate.TrySetResult(true);

        await batch.WaitAsync(Timeout);

        Assert.False(kahuna.Entered.Reader.TryRead(out _));
        Assert.False(responses.Written.TryRead(out _));
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

        public override Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> LocateAndTryGetValue(
            HLCTimestamp transactionId, string key, long revision, HLCTimestamp readTimestamp,
            KeyValueDurability durability, CancellationToken cancellationToken,
            string coordinatorKey = "", TransactionOperationId operationId = default)
            => Task.FromResult<(KeyValueResponseType, ReadOnlyKeyValueEntry?)>((
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
