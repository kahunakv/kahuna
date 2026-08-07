
using System.Text.Json;

using Grpc.Core;

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
