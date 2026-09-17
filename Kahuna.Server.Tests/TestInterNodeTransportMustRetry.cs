
using System.Text.Json;

using System.Collections.Concurrent;
using System.Diagnostics;

using Grpc.Core;

using Kahuna.Client.Communication;
using Kahuna.Communication.External.Rest;
using Kahuna.Shared.Communication.Rest;
using Kahuna.Server.Communication.Internode;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Locks.Data;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Locks;
using Kahuna.Shared.Sequences;

using Kommander;
using Kommander.Time;

using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kahuna.Server.Tests;

/// <summary>
/// Coverage for the two guards that keep a mid-forward transport failure from reaching the caller
/// as a raw exception: the typed MustRetry mapping on every forwarding method of
/// <see cref="GrpcInterNodeCommunication"/> (the leader was resolved but died before answering),
/// and the last-resort REST exception mapping that classifies any remaining escape.
/// </summary>
public sealed class TestInterNodeTransportMustRetry
{
    /// <summary>Nothing listens on port 1, so the duplex stream fails with a retryable transport
    /// status the moment the batcher tries to reach the "leader".</summary>
    private const string UnreachableNode = "https://localhost:1";

    private static GrpcInterNodeCommunication BuildTransport(ILogger<GrpcInterNodeCommunication>? logger = null) =>
        new(new KahunaConfiguration(), new RaftTransportSecurityOptions(), logger ?? NullLogger<GrpcInterNodeCommunication>.Instance);

    private static readonly HLCTimestamp TransactionId = new(1, 100, 0);

    // ── key-value and lock forwards ──────────────────────────────────────────

    /// <summary>
    /// A refused connection sends nothing, so the read demonstrably did not run: the answer is the
    /// operation's own MustRetry, which every embedding caller already handles, never the transport's
    /// exception, which the caller would have to know Kahuna's transport to classify.
    /// </summary>
    [Fact]
    public async Task TryGetValue_LeaderUnreachable_ReturnsMustRetry()
    {
        (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await BuildTransport().TryGetValue(
            UnreachableNode, TransactionId, "key", -1, HLCTimestamp.Zero, KeyValueDurability.Persistent,
            TestContext.Current.CancellationToken);

        Assert.Equal(KeyValueResponseType.MustRetry, type);
        Assert.Null(entry);
    }

    [Fact]
    public async Task TryExistsValue_LeaderUnreachable_ReturnsMustRetry()
    {
        (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await BuildTransport().TryExistsValue(
            UnreachableNode, TransactionId, "key", -1, HLCTimestamp.Zero, KeyValueDurability.Persistent,
            TestContext.Current.CancellationToken);

        Assert.Equal(KeyValueResponseType.MustRetry, type);
        Assert.Null(entry);
    }

    [Fact]
    public async Task TrySetKeyValue_LeaderUnreachable_ReturnsMustRetry()
    {
        (KeyValueResponseType type, long revision, HLCTimestamp lastModified) = await BuildTransport().TrySetKeyValue(
            UnreachableNode, TransactionId, "key", [1, 2, 3], null, -1, KeyValueFlags.Set, 0,
            KeyValueDurability.Persistent, 0, TestContext.Current.CancellationToken);

        Assert.Equal(KeyValueResponseType.MustRetry, type);
        Assert.Equal(0, revision);
        Assert.Equal(HLCTimestamp.Zero, lastModified);
    }

    [Fact]
    public async Task TryDeleteKeyValue_LeaderUnreachable_ReturnsMustRetry()
    {
        (KeyValueResponseType type, _, _) = await BuildTransport().TryDeleteKeyValue(
            UnreachableNode, TransactionId, "key", KeyValueDurability.Persistent, TestContext.Current.CancellationToken);

        Assert.Equal(KeyValueResponseType.MustRetry, type);
    }

    [Fact]
    public async Task TryExtendKeyValue_LeaderUnreachable_ReturnsMustRetry()
    {
        (KeyValueResponseType type, _, _) = await BuildTransport().TryExtendKeyValue(
            UnreachableNode, TransactionId, "key", 1000, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);

        Assert.Equal(KeyValueResponseType.MustRetry, type);
    }

    /// <summary>
    /// Callers of a batch forward correlate results by key, so a refusal has to name every key it
    /// covers: one MustRetry per requested key, in the shared response list, under the shared lock.
    /// </summary>
    [Fact]
    public async Task TryGetManyNodeValues_LeaderUnreachable_AnswersMustRetryPerKey()
    {
        List<(string key, long revision, KeyValueDurability durability)> keys =
        [
            ("a", -1, KeyValueDurability.Persistent),
            ("b", -1, KeyValueDurability.Ephemeral),
            ("c", -1, KeyValueDurability.Persistent)
        ];

        List<(KeyValueResponseType type, string key, KeyValueDurability durability, ReadOnlyKeyValueEntry? entry)> responses = [];

        await BuildTransport().TryGetManyNodeValues(
            UnreachableNode, TransactionId, HLCTimestamp.Zero, keys, new Lock(), responses, TestContext.Current.CancellationToken);

        Assert.Equal(keys.Count, responses.Count);

        for (int i = 0; i < keys.Count; i++)
        {
            Assert.Equal(KeyValueResponseType.MustRetry, responses[i].type);
            Assert.Equal(keys[i].key, responses[i].key);
            Assert.Equal(keys[i].durability, responses[i].durability);
            Assert.Null(responses[i].entry);
        }
    }

    [Fact]
    public async Task TrySetManyNodeKeyValue_LeaderUnreachable_AnswersMustRetryPerKey()
    {
        List<KahunaSetKeyValueRequestItem> items =
        [
            new() { Key = "a", Value = [1], Durability = KeyValueDurability.Persistent },
            new() { Key = "b", Value = [2], Durability = KeyValueDurability.Ephemeral }
        ];

        List<KahunaSetKeyValueResponseItem> responses = [];

        await BuildTransport().TrySetManyNodeKeyValue(
            UnreachableNode, items, new Lock(), responses, TestContext.Current.CancellationToken);

        Assert.Equal(items.Count, responses.Count);

        for (int i = 0; i < items.Count; i++)
        {
            Assert.Equal(KeyValueResponseType.MustRetry, responses[i].Type);
            Assert.Equal(items[i].Key, responses[i].Key);
            Assert.Equal(items[i].Durability, responses[i].Durability);
        }
    }

    [Fact]
    public async Task TryDeleteManyNodeKeyValue_LeaderUnreachable_AnswersMustRetryPerKey()
    {
        List<KahunaDeleteKeyValueRequestItem> items =
        [
            new() { Key = "a", Durability = KeyValueDurability.Persistent },
            new() { Key = "b", Durability = KeyValueDurability.Ephemeral }
        ];

        List<KahunaDeleteKeyValueResponseItem> responses = [];

        await BuildTransport().TryDeleteManyNodeKeyValue(
            UnreachableNode, items, new Lock(), responses, TestContext.Current.CancellationToken);

        Assert.Equal(items.Count, responses.Count);

        for (int i = 0; i < items.Count; i++)
        {
            Assert.Equal(KeyValueResponseType.MustRetry, responses[i].Type);
            Assert.Equal(items[i].Key, responses[i].Key);
            Assert.Equal(items[i].Durability, responses[i].Durability);
        }
    }

    [Fact]
    public async Task TryCheckManyWriteIntents_LeaderUnreachable_AnswersMustRetryPerKey()
    {
        List<KeyValueConflictProbe> probes =
        [
            new("a", KeyValueDurability.Persistent, KeyValueConflictChecks.WriteIntent),
            new("b", KeyValueDurability.Ephemeral, KeyValueConflictChecks.WriteIntent)
        ];

        List<(KeyValueResponseType type, string key, KeyValueDurability durability)> responses =
            await BuildTransport().TryCheckManyWriteIntents(UnreachableNode, TransactionId, probes, TestContext.Current.CancellationToken);

        Assert.Equal(probes.Count, responses.Count);

        for (int i = 0; i < probes.Count; i++)
        {
            Assert.Equal(KeyValueResponseType.MustRetry, responses[i].type);
            Assert.Equal(probes[i].Key, responses[i].key);
            Assert.Equal(probes[i].Durability, responses[i].durability);
        }
    }

    [Fact]
    public async Task TryAcquireExclusiveLock_LeaderUnreachable_ReturnsMustRetryWithTheKey()
    {
        (KeyValueResponseType type, string key, KeyValueDurability durability, HLCTimestamp holder) =
            await BuildTransport().TryAcquireExclusiveLock(
                UnreachableNode, TransactionId, "key", 1000, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);

        Assert.Equal(KeyValueResponseType.MustRetry, type);
        Assert.Equal("key", key);
        Assert.Equal(KeyValueDurability.Persistent, durability);
        Assert.Equal(HLCTimestamp.Zero, holder);
    }

    [Fact]
    public async Task TryAcquireNodeExclusiveLocks_LeaderUnreachable_AnswersMustRetryPerKey()
    {
        List<(string key, int expiresMs, KeyValueDurability durability)> keys =
        [
            ("a", 1000, KeyValueDurability.Persistent),
            ("b", 1000, KeyValueDurability.Ephemeral)
        ];

        List<(KeyValueResponseType type, string key, KeyValueDurability durability, HLCTimestamp holder)> responses = [];

        await BuildTransport().TryAcquireNodeExclusiveLocks(
            UnreachableNode, TransactionId, keys, new Lock(), responses, TestContext.Current.CancellationToken);

        Assert.Equal(keys.Count, responses.Count);

        for (int i = 0; i < keys.Count; i++)
        {
            Assert.Equal(KeyValueResponseType.MustRetry, responses[i].type);
            Assert.Equal(keys[i].key, responses[i].key);
            Assert.Equal(keys[i].durability, responses[i].durability);
        }
    }

    [Fact]
    public async Task TryAcquireExclusiveRangeLock_LeaderUnreachable_ReturnsMustRetry()
    {
        (KeyValueResponseType type, HLCTimestamp holder) = await BuildTransport().TryAcquireExclusiveRangeLock(
            UnreachableNode, TransactionId, "prefix", "a", true, "z", false, 1000, KeyValueDurability.Persistent,
            TestContext.Current.CancellationToken);

        Assert.Equal(KeyValueResponseType.MustRetry, type);
        Assert.Equal(HLCTimestamp.Zero, holder);
    }

    [Fact]
    public async Task TryReleaseExclusiveLock_LeaderUnreachable_ReturnsMustRetryWithTheKey()
    {
        (KeyValueResponseType type, string key) = await BuildTransport().TryReleaseExclusiveLock(
            UnreachableNode, TransactionId, "key", KeyValueDurability.Persistent, TestContext.Current.CancellationToken);

        Assert.Equal(KeyValueResponseType.MustRetry, type);
        Assert.Equal("key", key);
    }

    /// <summary>
    /// The 2PC participant forwards answer the same way the participant itself does on leadership loss,
    /// so the coordinator's existing retry handles a dead participant node and a re-elected one alike.
    /// </summary>
    [Fact]
    public async Task TryPrepareMutations_LeaderUnreachable_ReturnsMustRetryWithTheKey()
    {
        (KeyValueResponseType type, HLCTimestamp ticket, string key, KeyValueDurability durability) =
            await BuildTransport().TryPrepareMutations(
                UnreachableNode, TransactionId, new HLCTimestamp(1, 200, 0), "key", KeyValueDurability.Persistent, 0,
                TestContext.Current.CancellationToken);

        Assert.Equal(KeyValueResponseType.MustRetry, type);
        Assert.Equal(HLCTimestamp.Zero, ticket);
        Assert.Equal("key", key);
        Assert.Equal(KeyValueDurability.Persistent, durability);
    }

    [Fact]
    public async Task TryCommitNodeMutations_LeaderUnreachable_AnswersMustRetryPerKey()
    {
        List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)> keys =
        [
            ("a", new HLCTimestamp(1, 300, 0), KeyValueDurability.Persistent),
            ("b", new HLCTimestamp(1, 300, 1), KeyValueDurability.Persistent)
        ];

        List<(KeyValueResponseType type, string key, long, KeyValueDurability durability)> responses = [];

        await BuildTransport().TryCommitNodeMutations(
            UnreachableNode, TransactionId, keys, new Lock(), responses, TestContext.Current.CancellationToken);

        Assert.Equal(keys.Count, responses.Count);

        for (int i = 0; i < keys.Count; i++)
        {
            Assert.Equal(KeyValueResponseType.MustRetry, responses[i].type);
            Assert.Equal(keys[i].key, responses[i].key);
        }
    }

    [Fact]
    public async Task GetByRange_LeaderUnreachable_ReturnsMustRetryWithNoPage()
    {
        KeyValueGetByRangeResult result = await BuildTransport().GetByRange(
            UnreachableNode, TransactionId, "prefix", null, true, null, true, 100, HLCTimestamp.Zero,
            KeyValueDurability.Persistent, TestContext.Current.CancellationToken);

        Assert.Equal(KeyValueResponseType.MustRetry, result.Type);
        Assert.Empty(result.Items);
        Assert.False(result.HasMore);
        Assert.Null(result.NextCursor);
    }

    [Fact]
    public async Task GetByBucket_LeaderUnreachable_ReturnsMustRetryWithNoItems()
    {
        KeyValueGetByBucketResult result = await BuildTransport().GetByBucket(
            UnreachableNode, TransactionId, "prefix", HLCTimestamp.Zero, KeyValueDurability.Persistent,
            TestContext.Current.CancellationToken);

        Assert.Equal(KeyValueResponseType.MustRetry, result.Type);
        Assert.Empty(result.Items);
    }

    /// <summary>The coordinator-session hops answer the same shape the locator gives an unrouted call.</summary>
    [Fact]
    public async Task BeginOperation_LeaderUnreachable_ReturnsPendingMustRetry()
    {
        (OperationRegistrationOutcome outcome, KeyValueResponseType cachedType, _, _, string? anchor, _) =
            await BuildTransport().BeginOperation(
                UnreachableNode, "coordinator-key", TransactionId, new TransactionOperationId(1, 2), OperationKind.Set, null,
                TestContext.Current.CancellationToken);

        Assert.Equal(OperationRegistrationOutcome.AlreadyPending, outcome);
        Assert.Equal(KeyValueResponseType.MustRetry, cachedType);
        Assert.Null(anchor);
    }

    [Fact]
    public async Task TryLock_LeaderUnreachable_ReturnsMustRetry()
    {
        (LockResponseType type, long fencingToken) = await BuildTransport().TryLock(
            UnreachableNode, "resource", [1, 2, 3], 1000, LockDurability.Persistent, TestContext.Current.CancellationToken);

        Assert.Equal(LockResponseType.MustRetry, type);
        Assert.Equal(0, fencingToken);
    }

    [Fact]
    public async Task GetLock_LeaderUnreachable_ReturnsMustRetry()
    {
        (LockResponseType type, ReadOnlyLockEntry? entry) = await BuildTransport().GetLock(
            UnreachableNode, "resource", LockDurability.Persistent, TestContext.Current.CancellationToken);

        Assert.Equal(LockResponseType.MustRetry, type);
        Assert.Null(entry);
    }

    /// <summary>Sequence forwards are plain unary calls rather than batched streams; they follow the same rule.</summary>
    [Fact]
    public async Task NextSequenceValue_LeaderUnreachable_ReturnsMustRetry()
    {
        (SequenceResponseType type, SequenceAllocation allocation) = await BuildTransport().NextSequenceValue(
            UnreachableNode, "sequence", null, SequenceDurability.Persistent, TestContext.Current.CancellationToken);

        Assert.Equal(SequenceResponseType.MustRetry, type);
        Assert.Equal(default, allocation);
    }

    [Fact]
    public async Task AcquireSnapshotHold_LeaderUnreachable_ReturnsMustRetry()
    {
        (KeyValueResponseType type, string holdId, HLCTimestamp leaseExpiry) = await BuildTransport().AcquireSnapshotHold(
            UnreachableNode, "holder", new HLCTimestamp(1, 100, 0), 1000, TestContext.Current.CancellationToken);

        Assert.Equal(KeyValueResponseType.MustRetry, type);
        Assert.Equal(string.Empty, holdId);
        Assert.Equal(HLCTimestamp.Zero, leaseExpiry);
    }

    /// <summary>
    /// A dead leader fails thousands of forwards per second until the placement moves; a warning per
    /// forward was itself an operational problem. The transport logs one line per peer per quiet
    /// window and folds the rest into that line's count, while still answering every forward.
    /// </summary>
    [Fact]
    public async Task TransportFailureLog_IsGatedPerPeer()
    {
        const int forwards = 64;

        CapturingLogger logger = new();
        GrpcInterNodeCommunication transport = BuildTransport(logger);

        Stopwatch elapsed = Stopwatch.StartNew();

        Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)>[] calls = new Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)>[forwards];

        for (int i = 0; i < forwards; i++)
            calls[i] = transport.TryGetValue(
                UnreachableNode, TransactionId, "key-" + i, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent,
                TestContext.Current.CancellationToken);

        (KeyValueResponseType, ReadOnlyKeyValueEntry?)[] answers = await Task.WhenAll(calls);

        elapsed.Stop();

        Assert.All(answers, answer => Assert.Equal(KeyValueResponseType.MustRetry, answer.Item1));

        // The batcher shares the transport's logger and reports each stream eviction on its own;
        // only the forwarding refusal line is under test here.
        string[] lines = logger.Lines.Where(static line => line.Contains("returning MustRetry", StringComparison.Ordinal)).ToArray();

        // The first failure always logs; later ones log at most once per quiet window.
        long windows = 1 + elapsed.ElapsedMilliseconds / GrpcInterNodeCommunication.TransportFailureLogQuietMs;
        Assert.InRange(lines.Length, 1, windows);

        // Every failure is either its own line or counted on a later line, never dropped silently. The last
        // window's suppressed failures are still pending on the gate, so the sum is a floor, not an equality.
        long counted = 0;

        foreach (string line in lines)
        {
            int open = line.IndexOf('(');
            int space = line.IndexOf(' ', open);
            counted += long.Parse(line.AsSpan(open + 1, space - open - 1));
        }

        Assert.InRange(lines.Length + counted, lines.Length, forwards);
    }

    private sealed class CapturingLogger : ILogger<GrpcInterNodeCommunication>
    {
        public readonly ConcurrentQueue<string> Lines = new();

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => logLevel >= LogLevel.Warning;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (IsEnabled(logLevel))
                Lines.Enqueue(formatter(state, exception));
        }
    }

    // ── transaction-session forwards ─────────────────────────────────────────

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
