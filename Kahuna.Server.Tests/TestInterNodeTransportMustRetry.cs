
using Grpc.Core;

using Kahuna.Communication.External.Rest;
using Kahuna.Server.Communication.Internode;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Locks;
using Kahuna.Shared.Sequences;

using Kommander;
using Kommander.Time;

using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kahuna.Server.Tests;

/// <summary>
/// Coverage for the two guards that keep a mid-forward transport failure from reaching REST clients
/// as an unhandled HTTP 500: the typed MustRetry mapping on the transaction-session forwarding
/// methods of <see cref="GrpcInterNodeCommunication"/> (the leader was resolved but died before
/// answering), and the last-resort REST exception mapping that classifies any remaining escape.
/// </summary>
public sealed class TestInterNodeTransportMustRetry
{
    /// <summary>Nothing listens on port 1, so the duplex stream fails with a retryable transport
    /// status the moment the batcher tries to reach the "leader".</summary>
    private const string UnreachableNode = "https://localhost:1";

    private static GrpcInterNodeCommunication BuildTransport() =>
        new(new KahunaConfiguration(), NullLogger<GrpcInterNodeCommunication>.Instance);

    [Fact]
    public async Task StartTransaction_LeaderUnreachable_ReturnsMustRetry()
    {
        (KeyValueResponseType type, TransactionHandle handle) = await BuildTransport().StartTransaction(
            UnreachableNode,
            new() { CoordinatorKey = "coordinator-key" },
            TestContext.Current.CancellationToken);

        Assert.Equal(KeyValueResponseType.MustRetry, type);
        Assert.True(handle.IsEmpty);
    }

    [Fact]
    public async Task CommitTransaction_LeaderUnreachable_ReturnsMustRetryAndKeepsAnchor()
    {
        TransactionHandle handle = new(new HLCTimestamp(1, 100, 0), "coordinator-key", "anchor-key");

        (KeyValueResponseType type, string? anchor) = await BuildTransport().CommitTransaction(
            UnreachableNode, handle, TestContext.Current.CancellationToken);

        Assert.Equal(KeyValueResponseType.MustRetry, type);

        // The caller's anchor must survive the failed forward: a commit retry that supplies it can
        // consult the durable decision even though this attempt's outcome is indeterminate.
        Assert.Equal("anchor-key", anchor);
    }

    [Fact]
    public async Task RollbackTransaction_LeaderUnreachable_ReturnsMustRetry()
    {
        TransactionHandle handle = new(new HLCTimestamp(1, 100, 0), "coordinator-key", null);

        KeyValueResponseType type = await BuildTransport().RollbackTransaction(
            UnreachableNode, handle, TestContext.Current.CancellationToken);

        Assert.Equal(KeyValueResponseType.MustRetry, type);
    }

    // ── REST last-resort mapping ─────────────────────────────────────────────

    [Fact]
    public void RestMapping_ClassifiesRetryableExceptions()
    {
        Assert.True(RetryableExceptionMapping.IsRetryable(new RaftException("Invalid partition: 3")));
        Assert.True(RetryableExceptionMapping.IsRetryable(new RaftNodeNotReadyException("not initialized")));
        Assert.True(RetryableExceptionMapping.IsRetryable(new RpcException(new Status(StatusCode.Unavailable, "response ended prematurely"))));
        Assert.True(RetryableExceptionMapping.IsRetryable(new RpcException(new Status(StatusCode.DeadlineExceeded, "too slow"))));
        Assert.True(RetryableExceptionMapping.IsRetryable(new RpcException(new Status(StatusCode.Cancelled, "stream reset"))));

        // A remote application error or an arbitrary bug is not retryable and must keep propagating.
        Assert.False(RetryableExceptionMapping.IsRetryable(new RpcException(new Status(StatusCode.Internal, "server fault"))));
        Assert.False(RetryableExceptionMapping.IsRetryable(new InvalidOperationException("bug")));
        Assert.False(RetryableExceptionMapping.IsRetryable(new OperationCanceledException()));
    }

    [Fact]
    public void RestMapping_CoversRetryableSurfacesOnly()
    {
        Assert.NotNull(RetryableExceptionMapping.TryGetMustRetryBody(new PathString("/v1/kv/start-tx-session")));
        Assert.NotNull(RetryableExceptionMapping.TryGetMustRetryBody(new PathString("/v1/locks/try-lock")));
        Assert.NotNull(RetryableExceptionMapping.TryGetMustRetryBody(new PathString("/v1/sequences/next")));

        // Admin/operator surfaces and inter-node Raft routes keep their exceptions.
        Assert.Null(RetryableExceptionMapping.TryGetMustRetryBody(new PathString("/v1/cluster/health")));
        Assert.Null(RetryableExceptionMapping.TryGetMustRetryBody(new PathString("/v1/backups/create")));
        Assert.Null(RetryableExceptionMapping.TryGetMustRetryBody(new PathString("/v1/raft/append-logs")));
        Assert.Null(RetryableExceptionMapping.TryGetMustRetryBody(new PathString("/")));
    }

    /// <summary>Pins the serialized bodies to the shared enums so the constants cannot drift.</summary>
    [Fact]
    public void RestMapping_BodiesMatchEnumValues()
    {
        Assert.Equal($"{{\"type\":{(int)KeyValueResponseType.MustRetry}}}",
            RetryableExceptionMapping.TryGetMustRetryBody(new PathString("/v1/kv/get")));

        Assert.Equal($"{{\"type\":{(int)LockResponseType.MustRetry}}}",
            RetryableExceptionMapping.TryGetMustRetryBody(new PathString("/v1/locks/try-lock")));

        Assert.Equal($"{{\"type\":{(int)SequenceResponseType.MustRetry}}}",
            RetryableExceptionMapping.TryGetMustRetryBody(new PathString("/v1/sequences/next")));
    }
}
