using System.Collections.Concurrent;
using System.Reflection;

using Grpc.Core;
using Kahuna.Server.Communication.Internode.Grpc;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kahuna.Server.Tests;

/// <summary>
/// Unit tests for the inter-node <see cref="GrpcServerBatcher"/> failure-cleanup path: when a
/// shared response stream closes or faults, every pending request bound to that stream must be
/// completed (faulted) and removed from the tracking dictionaries, while requests bound to other
/// streams are left untouched.
///
/// The production code reaches these dictionaries and the read loop only through real gRPC
/// streaming, so the in-process cluster suite (which uses the in-memory inter-node transport)
/// never exercises it. These tests drive the real private statics directly via reflection and feed
/// the read loop a fake duplex stream that closes immediately.
/// </summary>
public sealed class TestGrpcServerBatcher
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

    private static void InvokeFailPendingRequests(long streamId, Exception ex)
        => BatcherType
            .GetMethod("FailPendingRequests", BindingFlags.NonPublic | BindingFlags.Static)!
            .Invoke(null, [streamId, ex]);

    private static Task InvokeReadLockMessages(
        long streamId,
        AsyncDuplexStreamingCall<GrpcBatchServerLockRequest, GrpcBatchServerLockResponse> streaming,
        ILogger logger)
        => (Task)BatcherType
            .GetMethod("ReadLockMessages", BindingFlags.NonPublic | BindingFlags.Static)!
            .Invoke(null, [streamId, streaming, logger])!;

    private static TaskCompletionSource<GrpcServerBatcherResponse> SeedPending(int requestId, long streamId)
    {
        TaskCompletionSource<GrpcServerBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcServerBatcherItem item = new(
            GrpcServerBatcherItemType.Locks,
            requestId,
            new GrpcServerBatcherRequest(new GrpcTryLockRequest()),
            promise);

        RequestRefs()[requestId] = item;
        RequestStreamRefs()[requestId] = streamId;

        return promise;
    }

    [Fact]
    public void FailPendingRequests_FailsOnlyRequestsOnTheClosedStream()
    {
        const long streamA = 9_000_001;
        const long streamB = 9_000_002;

        TaskCompletionSource<GrpcServerBatcherResponse> a1 = SeedPending(9_100_001, streamA);
        TaskCompletionSource<GrpcServerBatcherResponse> a2 = SeedPending(9_100_002, streamA);
        TaskCompletionSource<GrpcServerBatcherResponse> b1 = SeedPending(9_100_003, streamB);

        RpcException error = new(new Status(StatusCode.Unavailable, "stream closed"));

        InvokeFailPendingRequests(streamA, error);

        // Both requests bound to streamA are faulted with the supplied exception...
        Assert.True(a1.Task.IsFaulted);
        Assert.True(a2.Task.IsFaulted);
        Assert.Same(error, a1.Task.Exception!.InnerException);
        Assert.Same(error, a2.Task.Exception!.InnerException);

        // ...and removed from both tracking dictionaries.
        Assert.False(RequestRefs().ContainsKey(9_100_001));
        Assert.False(RequestRefs().ContainsKey(9_100_002));
        Assert.False(RequestStreamRefs().ContainsKey(9_100_001));
        Assert.False(RequestStreamRefs().ContainsKey(9_100_002));

        // The unrelated request on streamB is untouched.
        Assert.False(b1.Task.IsCompleted);
        Assert.True(RequestRefs().ContainsKey(9_100_003));
        Assert.True(RequestStreamRefs().ContainsKey(9_100_003));

        // Cleanup the streamB entry so global state is left clean.
        RequestRefs().TryRemove(9_100_003, out _);
        RequestStreamRefs().TryRemove(9_100_003, out _);
        b1.TrySetException(new OperationCanceledException());
    }

    [Fact]
    public async Task ReadLockMessages_OnStreamClose_FailsPendingAndCleansRefs()
    {
        const long streamId = 9_200_001;
        const int requestId = 9_300_001;

        TaskCompletionSource<GrpcServerBatcherResponse> pending = SeedPending(requestId, streamId);

        // A duplex call whose response stream is already closed (MoveNext returns false on the
        // first read) drives the read loop straight into its normal-close cleanup branch.
        AsyncDuplexStreamingCall<GrpcBatchServerLockRequest, GrpcBatchServerLockResponse> call = new(
            new NoopClientStreamWriter<GrpcBatchServerLockRequest>(),
            new EmptyAsyncStreamReader<GrpcBatchServerLockResponse>(),
            Task.FromResult(new Metadata()),
            static () => Status.DefaultSuccess,
            static () => [],
            static () => { });

        await InvokeReadLockMessages(streamId, call, NullLogger.Instance);

        Assert.True(pending.Task.IsFaulted);
        Assert.IsType<RpcException>(pending.Task.Exception!.InnerException);
        Assert.False(RequestRefs().ContainsKey(requestId));
        Assert.False(RequestStreamRefs().ContainsKey(requestId));
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
}
