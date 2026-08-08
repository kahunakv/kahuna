
using System.Text.Json;

using Grpc.Core;

using Kahuna.Client.Communication;
using Kahuna.Communication.External.Rest;
using Kahuna.Shared.Communication.Rest;
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

    /// <summary>
    /// The routed snapshot-floor read is the one exit of <c>KeyValuesManager.GetSnapshotFloor</c>
    /// that crosses the network; a transport failure there must come back as the endpoint's own
    /// typed MustRetry (with a floor that means nothing), never escape as an exception the REST
    /// surface would answer 500 with.
    /// </summary>
    [Fact]
    public async Task GetSnapshotFloor_LeaderUnreachable_ReturnsMustRetry()
    {
        (KeyValueResponseType type, HLCTimestamp floor, int liveHolds) = await BuildTransport().GetSnapshotFloor(
            UnreachableNode, TestContext.Current.CancellationToken);

        Assert.Equal(KeyValueResponseType.MustRetry, type);
        Assert.Equal(HLCTimestamp.Zero, floor);
        Assert.Equal(0, liveHolds);
    }

    // ── REST last-resort mapping ─────────────────────────────────────────────

    /// <summary>
    /// The REST middleware must not carry a classification rule of its own: every surface answers
    /// retryable failures with its own typed MustRetry, so they all have to agree on what retryable
    /// means. Pinning the delegation keeps a future surface-local copy from drifting.
    /// </summary>
    [Fact]
    public void RestMapping_DelegatesToTheSharedClassifier()
    {
        Exception[] cases =
        [
            new RaftException("Invalid partition: 3"),
            new RpcException(new Status(StatusCode.Unavailable, "response ended prematurely")),
            new RpcException(new Status(StatusCode.Internal, "dead pool", new HttpRequestException("ping timeout"))),
            new AggregateException(new RpcException(new Status(StatusCode.Cancelled, "stream reset"))),
            new RpcException(new Status(StatusCode.Internal, "server fault")),
            new InvalidOperationException("bug"),
            new OperationCanceledException()
        ];

        foreach (Exception ex in cases)
            Assert.Equal(RetryableFailureClassifier.IsRetryable(ex), RetryableExceptionMapping.IsRetryable(ex));
    }

    /// <summary>
    /// The SDK cannot reference the server assembly, so it carries its own copy of the transport rule
    /// for the servers that predate the typed-refusal contract. A copy is only safe while it agrees
    /// with the original — this pins the two together so a change to one that is not mirrored in the
    /// other fails here rather than in production, where the SDK would stop retrying a dead pooled
    /// connection (or start retrying a genuine server fault forever).
    /// </summary>
    [Fact]
    public void ClientTransportClassifier_AgreesWithTheServerClassifier()
    {
        RpcException[] cases =
        [
            new(new Status(StatusCode.Unavailable, "response ended prematurely")),
            new(new Status(StatusCode.DeadlineExceeded, "too slow")),
            new(new Status(StatusCode.Cancelled, "stream reset")),
            new(new Status(StatusCode.Internal, "dead pool", new HttpRequestException("ping timeout"))),
            new(new Status(StatusCode.Internal, "request aborted", new IOException("connection reset by peer"))),
            new(new Status(StatusCode.Internal, "server fault")),
            new(new Status(StatusCode.NotFound, "missing")),
            new(new Status(StatusCode.InvalidArgument, "bad request"))
        ];

        foreach (RpcException ex in cases)
            Assert.Equal(RetryableFailureClassifier.IsRetryable(ex), RetryableTransportFailure.IsRetryable(ex));
    }

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

    /// <summary>
    /// A dead pooled HTTP/2 connection (e.g. a keep-alive ping timeout after a partition) surfaces
    /// as StatusCode.Internal with the transport exception as the status's debug exception —
    /// "no definitive answer was produced", so it must classify as retryable. A plain Internal
    /// (remote application error) must not; that distinction is what keeps genuine server faults
    /// visible as 500s.
    /// </summary>
    [Fact]
    public void RestMapping_ClassifiesInternalByTransportCause()
    {
        RpcException deadConnection = new(new Status(
            StatusCode.Internal,
            "Error starting gRPC call. HttpRequestException: The HTTP/2 server didn't respond to a ping request.",
            new HttpRequestException("The HTTP/2 server didn't respond to a ping request within the configured KeepAlivePingDelay.")));

        Assert.True(RetryableExceptionMapping.IsRetryable(deadConnection));

        RpcException brokenPipe = new(new Status(
            StatusCode.Internal, "request aborted", new IOException("connection reset by peer")));

        Assert.True(RetryableExceptionMapping.IsRetryable(brokenPipe));

        // Same detail text but no transport cause: still a remote application error.
        Assert.False(RetryableExceptionMapping.IsRetryable(
            new RpcException(new Status(StatusCode.Internal, "Error starting gRPC call."))));
    }

    /// <summary>
    /// Retryable failures often arrive wrapped — an AggregateException from task plumbing, or as
    /// another exception's InnerException. The classification must unwrap before deciding, or a
    /// genuinely retryable transport failure escapes as an unclassifiable 500.
    /// </summary>
    [Fact]
    public void RestMapping_UnwrapsWrappedRetryableExceptions()
    {
        RpcException unavailable = new(new Status(StatusCode.Unavailable, "response ended prematurely"));

        Assert.True(RetryableExceptionMapping.IsRetryable(
            new AggregateException("One or more errors occurred.", unavailable)));
        Assert.True(RetryableExceptionMapping.IsRetryable(
            new AggregateException(new InvalidOperationException("bug"), unavailable)));
        Assert.True(RetryableExceptionMapping.IsRetryable(
            new InvalidOperationException("forward failed", unavailable)));
        Assert.True(RetryableExceptionMapping.IsRetryable(
            new AggregateException(new InvalidOperationException("outer", new RaftException("Invalid partition: 3")))));

        // Wrapping must not manufacture retryability where none exists.
        Assert.False(RetryableExceptionMapping.IsRetryable(
            new AggregateException(new InvalidOperationException("bug"))));
        Assert.False(RetryableExceptionMapping.IsRetryable(
            new InvalidOperationException("outer", new InvalidOperationException("inner"))));
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

    /// <summary>
    /// The substituted MustRetry body must be a legal instance of the snapshot-floor response
    /// contract. Before the DTO carried a type, the body deserialized as an empty success — zero
    /// live holds under HTTP 200 — which a floor-polling backup/PITR coordinator reads as "my hold
    /// was lost" and acts on, for a request that never reached the floor registry.
    /// </summary>
    [Fact]
    public void RestMapping_SnapshotFloorMustRetryBodyDeserializesIntoItsDto()
    {
        string? body = RetryableExceptionMapping.TryGetMustRetryBody(new PathString("/v1/kv/snapshot-floor"));

        Assert.NotNull(body);

        KahunaGetSnapshotFloorResponse? response = JsonSerializer.Deserialize(
            body, KahunaJsonContext.Default.KahunaGetSnapshotFloorResponse);

        Assert.NotNull(response);
        Assert.Equal(KeyValueResponseType.MustRetry, response.Type);
    }

    /// <summary>
    /// A successful floor read must serialize a type distinguishable from both MustRetry and the
    /// enum default, so a client can tell "measured" from "refused" and from "field absent".
    /// </summary>
    [Fact]
    public void SnapshotFloorSuccess_SerializesDistinguishableType()
    {
        string json = JsonSerializer.Serialize(
            new KahunaGetSnapshotFloorResponse
            {
                Type = KeyValueResponseType.Get,
                EffectiveFloor = new HLCTimestamp(1, 100, 2),
                LiveHolds = 3
            },
            KahunaJsonContext.Default.KahunaGetSnapshotFloorResponse);

        Assert.Contains($"\"type\":{(int)KeyValueResponseType.Get}", json);
        Assert.NotEqual(default, KeyValueResponseType.Get);
        Assert.NotEqual(KeyValueResponseType.MustRetry, KeyValueResponseType.Get);

        KahunaGetSnapshotFloorResponse? roundTripped = JsonSerializer.Deserialize(
            json, KahunaJsonContext.Default.KahunaGetSnapshotFloorResponse);

        Assert.NotNull(roundTripped);
        Assert.Equal(KeyValueResponseType.Get, roundTripped.Type);
        Assert.Equal(new HLCTimestamp(1, 100, 2), roundTripped.EffectiveFloor);
        Assert.Equal(3, roundTripped.LiveHolds);
    }

    /// <summary>
    /// The mapping substitutes one body per URL prefix, but each endpoint's DTO is its own contract:
    /// any response type on a mapped surface that cannot express the surface's MustRetry silently
    /// turns a refusal into a well-formed empty success. This pins the single-response contracts of
    /// the snapshot subsystem.
    /// </summary>
    [Fact]
    public void RestMapping_KvMustRetryBodyDeserializesIntoSnapshotHoldDtos()
    {
        string body = RetryableExceptionMapping.TryGetMustRetryBody(new PathString("/v1/kv/snapshot-hold/acquire"))!;

        Assert.Equal(KeyValueResponseType.MustRetry,
            JsonSerializer.Deserialize(body, KahunaJsonContext.Default.KahunaAcquireSnapshotHoldResponse)!.Type);
        Assert.Equal(KeyValueResponseType.MustRetry,
            JsonSerializer.Deserialize(body, KahunaJsonContext.Default.KahunaRenewSnapshotHoldResponse)!.Type);
        Assert.Equal(KeyValueResponseType.MustRetry,
            JsonSerializer.Deserialize(body, KahunaJsonContext.Default.KahunaReleaseSnapshotHoldResponse)!.Type);
    }

    /// <summary>
    /// The batch envelopes classify outcomes per item, so a substituted MustRetry body used to
    /// deserialize as an empty item list — "none of these keys exist" / "nothing was written" for a
    /// request that never reached a handler. The envelope-level type makes the refusal expressible;
    /// the null item list distinguishes it from a real answer about zero keys.
    /// </summary>
    [Fact]
    public void RestMapping_KvMustRetryBodyDeserializesIntoBatchEnvelopes()
    {
        string body = RetryableExceptionMapping.TryGetMustRetryBody(new PathString("/v1/kv/try-get-many"))!;

        KahunaManyKeyValuesResponse getMany =
            JsonSerializer.Deserialize(body, KahunaJsonContext.Default.KahunaManyKeyValuesResponse)!;
        Assert.Equal(KeyValueResponseType.MustRetry, getMany.Type);
        Assert.Null(getMany.Items);

        KahunaSetManyKeyValueResponse setMany =
            JsonSerializer.Deserialize(body, KahunaJsonContext.Default.KahunaSetManyKeyValueResponse)!;
        Assert.Equal(KeyValueResponseType.MustRetry, setMany.Type);
        Assert.Null(setMany.Items);

        KahunaDeleteManyKeyValueResponse deleteMany =
            JsonSerializer.Deserialize(body, KahunaJsonContext.Default.KahunaDeleteManyKeyValueResponse)!;
        Assert.Equal(KeyValueResponseType.MustRetry, deleteMany.Type);
        Assert.Null(deleteMany.Items);
    }

    /// <summary>An answered batch read must serialize an envelope type distinguishable from both
    /// MustRetry and the enum default, so an old-server body (no type) is also tellable apart.</summary>
    [Fact]
    public void BatchGetManySuccess_SerializesDistinguishableEnvelopeType()
    {
        string json = JsonSerializer.Serialize(
            new KahunaManyKeyValuesResponse { Type = KeyValueResponseType.Get, Items = [], TimeElapsedMs = 1 },
            KahunaJsonContext.Default.KahunaManyKeyValuesResponse);

        Assert.Contains($"\"type\":{(int)KeyValueResponseType.Get}", json);

        KahunaManyKeyValuesResponse roundTripped =
            JsonSerializer.Deserialize(json, KahunaJsonContext.Default.KahunaManyKeyValuesResponse)!;
        Assert.Equal(KeyValueResponseType.Get, roundTripped.Type);
        Assert.NotNull(roundTripped.Items);
        Assert.Empty(roundTripped.Items);
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
