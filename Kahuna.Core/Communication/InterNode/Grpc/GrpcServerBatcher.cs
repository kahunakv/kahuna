
using System.Collections.Concurrent;

using Grpc.Core;
using Grpc.Net.Client;
using Kommander.Communication.Grpc;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Communication.Internode.Grpc;

/// <summary>
/// A server-side batching utility designed to handle gRPC-based communication requests at scale.
/// The primary function of this class is to queue various gRPC request types for processing
/// and to return the appropriate responses.
///
/// <para><b>Liveness contract.</b> A promise returned by <c>Enqueue</c> always settles: either a
/// response arrives on the shared duplex stream, the stream visibly dies
/// (<see cref="FailPendingRequests"/>), or the reaper fails it after <see cref="RequestDeadline"/>.
/// The deadline is the only backstop for a stream that goes quiet WITHOUT dying — a SIGSTOPed peer
/// leaves the TCP session established and the HTTP/2 window stalls with no error, so nothing else
/// would ever complete the promise (observed as 126k unresolved promises and a permanently silent
/// data plane in the Caraxes run-J soak). Writes are bounded by <see cref="WriteTimeout"/> for the
/// same reason: one stuck <c>WriteAsync</c> would otherwise hold the per-stream semaphore forever
/// and silently wedge every future forwarded operation to that peer. A failed or stalled stream is
/// evicted from the process-wide registry so the next enqueue rebuilds fresh streams instead of
/// writing to dead call objects for the rest of the process lifetime.</para>
///
/// <para><b>Queue coverage and admission.</b> A request is registered for the deadline the moment
/// it is admitted, before it enters the inbox, so the reaper also covers requests that still wait
/// in the queue or behind a blocked write — not only requests already sent. Enqueue refuses new
/// work retryably once the process-wide pending item or byte limit is reached: the deadline alone
/// does not bound memory when the offered load stays above capacity.</para>
/// </summary>
internal sealed class GrpcServerBatcher
{
    private static readonly ConcurrentDictionary<string, Lazy<List<GrpcServerSharedStreaming>>> streamings = new();

    private static readonly ConcurrentDictionary<int, GrpcServerBatcherItem> requestRefs = new();

    private static readonly ConcurrentDictionary<int, long> requestStreamRefs = new();

    private static int requestId;

    private static long streamingId;

    private static int reaperStarted;

    /// <summary>
    /// Count of admitted requests that did not settle yet, across every batcher in the process.
    /// A request counts from admission in <see cref="TryAdmit"/> until its removal in
    /// <see cref="TryTakeRequest"/> — that span covers inbox residence and the in-flight wait.
    /// </summary>
    private static int pendingRequests;

    /// <summary>
    /// Sum of the payload bytes of the requests counted by <see cref="pendingRequests"/>.
    /// </summary>
    private static long pendingRequestBytes;

    /// <summary>
    /// Upper bound on how long an enqueued request may wait for its response before the reaper
    /// fails it with a retryable <see cref="StatusCode.Unavailable"/>. Mutable only so tests can
    /// shrink it; production code must treat it as a constant.
    /// </summary>
    internal static TimeSpan RequestDeadline = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Upper bound on acquiring the per-stream write semaphore and on a single
    /// <c>RequestStream.WriteAsync</c>. Exceeding either marks the stream stalled: it is disposed,
    /// evicted from the registry, and its pending requests are failed retryably. Mutable only so
    /// tests can shrink it.
    /// </summary>
    internal static TimeSpan WriteTimeout = TimeSpan.FromSeconds(5);

    /// <summary>Sweep cadence of the deadline reaper. Mutable only so tests can shrink it.</summary>
    internal static TimeSpan ReaperInterval = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Upper bound on requests admitted and not yet settled (queued plus in-flight), across every
    /// batcher in the process. Above it, an enqueue fails immediately with a retryable
    /// <see cref="StatusCode.Unavailable"/>: the response deadline alone does not bound memory
    /// when the offered load stays above capacity, because new work keeps arriving while old
    /// work waits out its deadline. Mutable only so tests can shrink it.
    /// </summary>
    internal static int MaxPendingRequests = 131_072;

    /// <summary>
    /// Upper bound on the summed payload bytes of the requests bounded by
    /// <see cref="MaxPendingRequests"/>. It rejects fewer, larger requests than the item cap
    /// does, so a burst of bulk writes cannot hold an unbounded amount of memory while it waits.
    /// Mutable only so tests can shrink it.
    /// </summary>
    internal static long MaxPendingRequestBytes = 256L * 1024 * 1024;

    /// <summary>Requests admitted and not yet settled. Exposed for test assertions.</summary>
    internal static int PendingRequests => Volatile.Read(ref pendingRequests);

    /// <summary>Payload bytes admitted and not yet settled. Exposed for test assertions.</summary>
    internal static long PendingRequestBytes => Volatile.Read(ref pendingRequestBytes);

    private readonly string url;

    private readonly ILogger logger;

    private readonly ConcurrentQueue<GrpcServerBatcherItem> inbox = new();

    /// <summary>
    /// Largest number of inbox items one drain may collect before it dispatches. The bound caps
    /// the dispatch buffer's backing array, which the loop keeps between drains, and it caps how
    /// many requests one transport failure fails at once.
    /// Admission allows up to <see cref="MaxPendingRequests"/> pending items; an unbounded drain
    /// could grow the buffer to that full backlog. The outer drain loop picks up the remainder
    /// in later rounds without a pause.
    /// </summary>
    internal const int MaxItemsPerDrain = 1024;

    /// <summary>
    /// Largest number of single-operation envelopes one coalesced stream message may carry.
    /// Mutable only so tests can shrink it.
    /// </summary>
    internal static int MaxOpsPerCoalescedEnvelope = 64;

    /// <summary>
    /// Upper bound on the summed payload bytes of one coalesced stream message. Both peers run
    /// the gRPC default 4 MB receive limit, so the bound keeps a bundle far from it; an item
    /// whose own payload exceeds the bound still travels, alone, exactly as it does today.
    /// Mutable only so tests can shrink it.
    /// </summary>
    internal static long MaxCoalescedEnvelopeBytes = 1024L * 1024;

    /// <summary>
    /// The list the dispatch loop drains into, reused across drains. It is instance state with one
    /// owner rather than a pooled object: at most one dispatch loop runs per batcher, and a
    /// thread-local pool would strand lists, because a rent and its return often run on different
    /// worker threads across the awaits in between.
    /// </summary>
    private List<GrpcServerBatcherItem>? dispatchBuffer;

    /// <summary>Scratch lists of one drain's live lock and key-value items, reused across drains.</summary>
    private List<GrpcServerBatcherItem>? lockScratch, keyValueScratch;

    /// <summary>
    /// Round-robin cursor over the peer's streams. Owned by the single dispatch loop, so it
    /// needs no synchronization; the unsigned cast keeps the index valid through wraparound.
    /// </summary>
    private uint streamCursor;

    private int processing = 1;

    public GrpcServerBatcher(string url, ILogger logger)
    {
        this.url = url;
        this.logger = logger;
    }
    
    public Task<GrpcServerBatcherResponse> Enqueue(GrpcTryLockRequest message)
    {
        TaskCompletionSource<GrpcServerBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcServerBatcherItem grpcBatcherItem = new(GrpcServerBatcherItemType.Locks, Interlocked.Increment(ref requestId), new(message), promise);

        return TryProcessQueue(grpcBatcherItem, promise);
    }
    
    public Task<GrpcServerBatcherResponse> Enqueue(GrpcUnlockRequest message)
    {
        TaskCompletionSource<GrpcServerBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcServerBatcherItem grpcBatcherItem = new(GrpcServerBatcherItemType.Locks, Interlocked.Increment(ref requestId), new(message), promise);

        return TryProcessQueue(grpcBatcherItem, promise);
    }
    
    public Task<GrpcServerBatcherResponse> Enqueue(GrpcExtendLockRequest message)
    {
        TaskCompletionSource<GrpcServerBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcServerBatcherItem grpcBatcherItem = new(GrpcServerBatcherItemType.Locks, Interlocked.Increment(ref requestId), new(message), promise);

        return TryProcessQueue(grpcBatcherItem, promise);
    }
    
    public Task<GrpcServerBatcherResponse> Enqueue(GrpcGetLockRequest message)
    {
        TaskCompletionSource<GrpcServerBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcServerBatcherItem grpcBatcherItem = new(GrpcServerBatcherItemType.Locks, Interlocked.Increment(ref requestId), new(message), promise);

        return TryProcessQueue(grpcBatcherItem, promise);
    }
    
    public Task<GrpcServerBatcherResponse> Enqueue(GrpcTrySetKeyValueRequest message)
    {
        TaskCompletionSource<GrpcServerBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcServerBatcherItem grpcBatcherItem = new(GrpcServerBatcherItemType.KeyValues, Interlocked.Increment(ref requestId), new(message), promise);

        return TryProcessQueue(grpcBatcherItem, promise);
    }
    
    public Task<GrpcServerBatcherResponse> Enqueue(GrpcTrySetManyKeyValueRequest message)
    {
        TaskCompletionSource<GrpcServerBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcServerBatcherItem grpcBatcherItem = new(GrpcServerBatcherItemType.KeyValues, Interlocked.Increment(ref requestId), new(message), promise);

        return TryProcessQueue(grpcBatcherItem, promise);
    }

    public Task<GrpcServerBatcherResponse> Enqueue(GrpcTryDeleteManyKeyValueRequest message)
    {
        TaskCompletionSource<GrpcServerBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcServerBatcherItem grpcBatcherItem = new(GrpcServerBatcherItemType.KeyValues, Interlocked.Increment(ref requestId), new(message), promise);

        return TryProcessQueue(grpcBatcherItem, promise);
    }
    
    public Task<GrpcServerBatcherResponse> Enqueue(GrpcTryGetKeyValueRequest message)
    {
        TaskCompletionSource<GrpcServerBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcServerBatcherItem grpcBatcherItem = new(GrpcServerBatcherItemType.KeyValues, Interlocked.Increment(ref requestId), new(message), promise);

        return TryProcessQueue(grpcBatcherItem, promise);
    }

    public Task<GrpcServerBatcherResponse> Enqueue(GrpcTryGetManyValuesRequest message)
    {
        TaskCompletionSource<GrpcServerBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcServerBatcherItem grpcBatcherItem = new(GrpcServerBatcherItemType.KeyValues, Interlocked.Increment(ref requestId), new(message), promise);

        return TryProcessQueue(grpcBatcherItem, promise);
    }
    
    public Task<GrpcServerBatcherResponse> Enqueue(GrpcTryDeleteKeyValueRequest message)
    {
        TaskCompletionSource<GrpcServerBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcServerBatcherItem grpcBatcherItem = new(GrpcServerBatcherItemType.KeyValues, Interlocked.Increment(ref requestId), new(message), promise);

        return TryProcessQueue(grpcBatcherItem, promise);
    }
    
    public Task<GrpcServerBatcherResponse> Enqueue(GrpcTryExtendKeyValueRequest message)
    {
        TaskCompletionSource<GrpcServerBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcServerBatcherItem grpcBatcherItem = new(GrpcServerBatcherItemType.KeyValues, Interlocked.Increment(ref requestId), new(message), promise);

        return TryProcessQueue(grpcBatcherItem, promise);
    }
    
    public Task<GrpcServerBatcherResponse> Enqueue(GrpcTryExistsKeyValueRequest message)
    {
        TaskCompletionSource<GrpcServerBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcServerBatcherItem grpcBatcherItem = new(GrpcServerBatcherItemType.KeyValues, Interlocked.Increment(ref requestId), new(message), promise);

        return TryProcessQueue(grpcBatcherItem, promise);
    }

    public Task<GrpcServerBatcherResponse> Enqueue(GrpcTryExistsManyValuesRequest message)
    {
        TaskCompletionSource<GrpcServerBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcServerBatcherItem grpcBatcherItem = new(GrpcServerBatcherItemType.KeyValues, Interlocked.Increment(ref requestId), new(message), promise);

        return TryProcessQueue(grpcBatcherItem, promise);
    }
    
    public Task<GrpcServerBatcherResponse> Enqueue(GrpcTryCheckWriteIntentRequest message)
    {
        TaskCompletionSource<GrpcServerBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcServerBatcherItem grpcBatcherItem = new(GrpcServerBatcherItemType.KeyValues, Interlocked.Increment(ref requestId), new(message), promise);

        return TryProcessQueue(grpcBatcherItem, promise);
    }

    public Task<GrpcServerBatcherResponse> Enqueue(GrpcTryCheckManyWriteIntentsRequest message)
    {
        TaskCompletionSource<GrpcServerBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcServerBatcherItem grpcBatcherItem = new(GrpcServerBatcherItemType.KeyValues, Interlocked.Increment(ref requestId), new(message), promise);

        return TryProcessQueue(grpcBatcherItem, promise);
    }

    public Task<GrpcServerBatcherResponse> Enqueue(GrpcGetByBucketRequest message)
    {
        TaskCompletionSource<GrpcServerBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcServerBatcherItem grpcBatcherItem = new(GrpcServerBatcherItemType.KeyValues, Interlocked.Increment(ref requestId), new(message), promise);

        return TryProcessQueue(grpcBatcherItem, promise);
    }

    public Task<GrpcServerBatcherResponse> Enqueue(GrpcGetByRangeRequest message)
    {
        TaskCompletionSource<GrpcServerBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcServerBatcherItem grpcBatcherItem = new(GrpcServerBatcherItemType.KeyValues, Interlocked.Increment(ref requestId), new(message), promise);

        return TryProcessQueue(grpcBatcherItem, promise);
    }
    
    public Task<GrpcServerBatcherResponse> Enqueue(GrpcScanByPrefixRequest message)
    {
        TaskCompletionSource<GrpcServerBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcServerBatcherItem grpcBatcherItem = new(GrpcServerBatcherItemType.KeyValues, Interlocked.Increment(ref requestId), new(message), promise);

        return TryProcessQueue(grpcBatcherItem, promise);
    }       
    
    public Task<GrpcServerBatcherResponse> Enqueue(GrpcTryExecuteTransactionScriptRequest message)
    {
        TaskCompletionSource<GrpcServerBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcServerBatcherItem grpcBatcherItem = new(GrpcServerBatcherItemType.KeyValues, Interlocked.Increment(ref requestId), new(message), promise);

        return TryProcessQueue(grpcBatcherItem, promise);
    }
    
    public Task<GrpcServerBatcherResponse> Enqueue(GrpcTryAcquireExclusiveLockRequest message)
    {
        TaskCompletionSource<GrpcServerBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcServerBatcherItem grpcBatcherItem = new(GrpcServerBatcherItemType.KeyValues, Interlocked.Increment(ref requestId), new(message), promise);

        return TryProcessQueue(grpcBatcherItem, promise);
    }
    
    public Task<GrpcServerBatcherResponse> Enqueue(GrpcTryAcquireExclusivePrefixLockRequest message)
    {
        TaskCompletionSource<GrpcServerBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcServerBatcherItem grpcBatcherItem = new(GrpcServerBatcherItemType.KeyValues, Interlocked.Increment(ref requestId), new(message), promise);

        return TryProcessQueue(grpcBatcherItem, promise);
    }
    
    public Task<GrpcServerBatcherResponse> Enqueue(GrpcTryAcquireManyExclusiveLocksRequest message)
    {
        TaskCompletionSource<GrpcServerBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcServerBatcherItem grpcBatcherItem = new(GrpcServerBatcherItemType.KeyValues, Interlocked.Increment(ref requestId), new(message), promise);

        return TryProcessQueue(grpcBatcherItem, promise);
    }
    
    public Task<GrpcServerBatcherResponse> Enqueue(GrpcTryReleaseExclusiveLockRequest message)
    {
        TaskCompletionSource<GrpcServerBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcServerBatcherItem grpcBatcherItem = new(GrpcServerBatcherItemType.KeyValues, Interlocked.Increment(ref requestId), new(message), promise);

        return TryProcessQueue(grpcBatcherItem, promise);
    }
    
    public Task<GrpcServerBatcherResponse> Enqueue(GrpcTryAcquireExclusiveRangeLockRequest message)
    {
        TaskCompletionSource<GrpcServerBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcServerBatcherItem grpcBatcherItem = new(GrpcServerBatcherItemType.KeyValues, Interlocked.Increment(ref requestId), new(message), promise);

        return TryProcessQueue(grpcBatcherItem, promise);
    }

    public Task<GrpcServerBatcherResponse> Enqueue(GrpcTryReleaseExclusivePrefixLockRequest message)
    {
        TaskCompletionSource<GrpcServerBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcServerBatcherItem grpcBatcherItem = new(GrpcServerBatcherItemType.KeyValues, Interlocked.Increment(ref requestId), new(message), promise);

        return TryProcessQueue(grpcBatcherItem, promise);
    }

    public Task<GrpcServerBatcherResponse> Enqueue(GrpcTryReleaseExclusiveRangeLockRequest message)
    {
        TaskCompletionSource<GrpcServerBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcServerBatcherItem grpcBatcherItem = new(GrpcServerBatcherItemType.KeyValues, Interlocked.Increment(ref requestId), new(message), promise);

        return TryProcessQueue(grpcBatcherItem, promise);
    }

    public Task<GrpcServerBatcherResponse> Enqueue(GrpcTryReleaseManyExclusiveLocksRequest message)
    {
        TaskCompletionSource<GrpcServerBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcServerBatcherItem grpcBatcherItem = new(GrpcServerBatcherItemType.KeyValues, Interlocked.Increment(ref requestId), new(message), promise);

        return TryProcessQueue(grpcBatcherItem, promise);
    }
    
    public Task<GrpcServerBatcherResponse> Enqueue(GrpcTryPrepareMutationsRequest message)
    {
        TaskCompletionSource<GrpcServerBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcServerBatcherItem grpcBatcherItem = new(GrpcServerBatcherItemType.KeyValues, Interlocked.Increment(ref requestId), new(message), promise);

        return TryProcessQueue(grpcBatcherItem, promise);
    }
    
    public Task<GrpcServerBatcherResponse> Enqueue(GrpcTryPrepareManyMutationsRequest message)
    {
        TaskCompletionSource<GrpcServerBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcServerBatcherItem grpcBatcherItem = new(GrpcServerBatcherItemType.KeyValues, Interlocked.Increment(ref requestId), new(message), promise);

        return TryProcessQueue(grpcBatcherItem, promise);
    }
    
    public Task<GrpcServerBatcherResponse> Enqueue(GrpcTryCommitMutationsRequest message)
    {
        TaskCompletionSource<GrpcServerBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcServerBatcherItem grpcBatcherItem = new(GrpcServerBatcherItemType.KeyValues, Interlocked.Increment(ref requestId), new(message), promise);

        return TryProcessQueue(grpcBatcherItem, promise);
    }
    
    public Task<GrpcServerBatcherResponse> Enqueue(GrpcTryRollbackMutationsRequest message)
    {
        TaskCompletionSource<GrpcServerBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcServerBatcherItem grpcBatcherItem = new(GrpcServerBatcherItemType.KeyValues, Interlocked.Increment(ref requestId), new(message), promise);

        return TryProcessQueue(grpcBatcherItem, promise);
    }
    
    public Task<GrpcServerBatcherResponse> Enqueue(GrpcTryCommitManyMutationsRequest message)
    {
        TaskCompletionSource<GrpcServerBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcServerBatcherItem grpcBatcherItem = new(GrpcServerBatcherItemType.KeyValues, Interlocked.Increment(ref requestId), new(message), promise);

        return TryProcessQueue(grpcBatcherItem, promise);
    }
    
    public Task<GrpcServerBatcherResponse> Enqueue(GrpcTryRollbackManyMutationsRequest message)
    {
        TaskCompletionSource<GrpcServerBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcServerBatcherItem grpcBatcherItem = new(GrpcServerBatcherItemType.KeyValues, Interlocked.Increment(ref requestId), new(message), promise);

        return TryProcessQueue(grpcBatcherItem, promise);
    }
    
    public Task<GrpcServerBatcherResponse> Enqueue(GrpcStartTransactionRequest message)
    {
        TaskCompletionSource<GrpcServerBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcServerBatcherItem grpcBatcherItem = new(GrpcServerBatcherItemType.KeyValues, Interlocked.Increment(ref requestId), new(message), promise);

        return TryProcessQueue(grpcBatcherItem, promise);
    }
    
    public Task<GrpcServerBatcherResponse> Enqueue(GrpcCommitTransactionRequest message)
    {
        TaskCompletionSource<GrpcServerBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcServerBatcherItem grpcBatcherItem = new(GrpcServerBatcherItemType.KeyValues, Interlocked.Increment(ref requestId), new(message), promise);

        return TryProcessQueue(grpcBatcherItem, promise);
    }
    
    public Task<GrpcServerBatcherResponse> Enqueue(GrpcRollbackTransactionRequest message)
    {
        TaskCompletionSource<GrpcServerBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcServerBatcherItem grpcBatcherItem = new(GrpcServerBatcherItemType.KeyValues, Interlocked.Increment(ref requestId), new(message), promise);

        return TryProcessQueue(grpcBatcherItem, promise);
    }

    public Task<GrpcServerBatcherResponse> Enqueue(GrpcBeginOperationRequest message)
    {
        TaskCompletionSource<GrpcServerBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcServerBatcherItem grpcBatcherItem = new(GrpcServerBatcherItemType.KeyValues, Interlocked.Increment(ref requestId), new(message), promise);

        return TryProcessQueue(grpcBatcherItem, promise);
    }

    public Task<GrpcServerBatcherResponse> Enqueue(GrpcCompleteOperationRequest message)
    {
        TaskCompletionSource<GrpcServerBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcServerBatcherItem grpcBatcherItem = new(GrpcServerBatcherItemType.KeyValues, Interlocked.Increment(ref requestId), new(message), promise);

        return TryProcessQueue(grpcBatcherItem, promise);
    }

    public Task<GrpcServerBatcherResponse> Enqueue(GrpcGetTransactionWorkingSetRequest message)
    {
        TaskCompletionSource<GrpcServerBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcServerBatcherItem grpcBatcherItem = new(GrpcServerBatcherItemType.KeyValues, Interlocked.Increment(ref requestId), new(message), promise);

        return TryProcessQueue(grpcBatcherItem, promise);
    }

    public Task<GrpcServerBatcherResponse> Enqueue(GrpcCloseTransactionRequest message)
    {
        TaskCompletionSource<GrpcServerBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcServerBatcherItem grpcBatcherItem = new(GrpcServerBatcherItemType.KeyValues, Interlocked.Increment(ref requestId), new(message), promise);

        return TryProcessQueue(grpcBatcherItem, promise);
    }

    public Task<GrpcServerBatcherResponse> Enqueue(GrpcEnsureKeyRangeSeededRequest message)
    {
        TaskCompletionSource<GrpcServerBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcServerBatcherItem grpcBatcherItem = new(GrpcServerBatcherItemType.KeyValues, Interlocked.Increment(ref requestId), new(message), promise);

        return TryProcessQueue(grpcBatcherItem, promise);
    }

    public Task<GrpcServerBatcherResponse> Enqueue(GrpcEnsureKeyRangeRemovedRequest message)
    {
        TaskCompletionSource<GrpcServerBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcServerBatcherItem grpcBatcherItem = new(GrpcServerBatcherItemType.KeyValues, Interlocked.Increment(ref requestId), new(message), promise);

        return TryProcessQueue(grpcBatcherItem, promise);
    }

    public Task<GrpcServerBatcherResponse> Enqueue(GrpcGetRangeLocksRequest message)
    {
        TaskCompletionSource<GrpcServerBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcServerBatcherItem grpcBatcherItem = new(GrpcServerBatcherItemType.KeyValues, Interlocked.Increment(ref requestId), new(message), promise);

        return TryProcessQueue(grpcBatcherItem, promise);
    }

    public Task<GrpcServerBatcherResponse> Enqueue(GrpcImportRangeLocksRequest message)
    {
        TaskCompletionSource<GrpcServerBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcServerBatcherItem grpcBatcherItem = new(GrpcServerBatcherItemType.KeyValues, Interlocked.Increment(ref requestId), new(message), promise);

        return TryProcessQueue(grpcBatcherItem, promise);
    }

    public Task<GrpcServerBatcherResponse> Enqueue(GrpcImportCompletionReceiptsRequest message)
    {
        TaskCompletionSource<GrpcServerBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcServerBatcherItem grpcBatcherItem = new(GrpcServerBatcherItemType.KeyValues, Interlocked.Increment(ref requestId), new(message), promise);

        return TryProcessQueue(grpcBatcherItem, promise);
    }

    public Task<GrpcServerBatcherResponse> Enqueue(GrpcDurableOperationRequest message)
    {
        TaskCompletionSource<GrpcServerBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcServerBatcherItem grpcBatcherItem = new(GrpcServerBatcherItemType.KeyValues, Interlocked.Increment(ref requestId), new(message), promise);

        return TryProcessQueue(grpcBatcherItem, promise);
    }

    public Task<GrpcServerBatcherResponse> Enqueue(GrpcDurableBundleRequest message)
    {
        TaskCompletionSource<GrpcServerBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcServerBatcherItem grpcBatcherItem = new(GrpcServerBatcherItemType.KeyValues, Interlocked.Increment(ref requestId), new(message), promise);

        return TryProcessQueue(grpcBatcherItem, promise);
    }

    public Task<GrpcServerBatcherResponse> Enqueue(GrpcDurableDecisionRequest message)
    {
        TaskCompletionSource<GrpcServerBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcServerBatcherItem grpcBatcherItem = new(GrpcServerBatcherItemType.KeyValues, Interlocked.Increment(ref requestId), new(message), promise);

        return TryProcessQueue(grpcBatcherItem, promise);
    }

    public Task<GrpcServerBatcherResponse> Enqueue(GrpcDurableOnePhaseRequest message)
    {
        TaskCompletionSource<GrpcServerBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcServerBatcherItem grpcBatcherItem = new(GrpcServerBatcherItemType.KeyValues, Interlocked.Increment(ref requestId), new(message), promise);

        return TryProcessQueue(grpcBatcherItem, promise);
    }

    public Task<GrpcServerBatcherResponse> Enqueue(GrpcReplicateKeyValueRangePageRequest message)
    {
        TaskCompletionSource<GrpcServerBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcServerBatcherItem grpcBatcherItem = new(GrpcServerBatcherItemType.KeyValues, Interlocked.Increment(ref requestId), new(message), promise);

        return TryProcessQueue(grpcBatcherItem, promise);
    }

    public Task<GrpcServerBatcherResponse> Enqueue(GrpcGetRangeTransactionStateRequest message)
    {
        TaskCompletionSource<GrpcServerBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcServerBatcherItem grpcBatcherItem = new(GrpcServerBatcherItemType.KeyValues, Interlocked.Increment(ref requestId), new(message), promise);

        return TryProcessQueue(grpcBatcherItem, promise);
    }

    public Task<GrpcServerBatcherResponse> Enqueue(GrpcLookupTransactionRecordRequest message)
    {
        TaskCompletionSource<GrpcServerBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcServerBatcherItem grpcBatcherItem = new(GrpcServerBatcherItemType.KeyValues, Interlocked.Increment(ref requestId), new(message), promise);

        return TryProcessQueue(grpcBatcherItem, promise);
    }

    public Task<GrpcServerBatcherResponse> Enqueue(GrpcGetStagedBaseVerdictsRequest message)
    {
        TaskCompletionSource<GrpcServerBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcServerBatcherItem grpcBatcherItem = new(GrpcServerBatcherItemType.KeyValues, Interlocked.Increment(ref requestId), new(message), promise);

        return TryProcessQueue(grpcBatcherItem, promise);
    }

    public Task<GrpcServerBatcherResponse> Enqueue(GrpcAcquireSnapshotHoldRequest message)
    {
        TaskCompletionSource<GrpcServerBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcServerBatcherItem grpcBatcherItem = new(GrpcServerBatcherItemType.KeyValues, Interlocked.Increment(ref requestId), new(message), promise);

        return TryProcessQueue(grpcBatcherItem, promise);
    }

    public Task<GrpcServerBatcherResponse> Enqueue(GrpcRenewSnapshotHoldRequest message)
    {
        TaskCompletionSource<GrpcServerBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcServerBatcherItem grpcBatcherItem = new(GrpcServerBatcherItemType.KeyValues, Interlocked.Increment(ref requestId), new(message), promise);

        return TryProcessQueue(grpcBatcherItem, promise);
    }

    public Task<GrpcServerBatcherResponse> Enqueue(GrpcReleaseSnapshotHoldRequest message)
    {
        TaskCompletionSource<GrpcServerBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcServerBatcherItem grpcBatcherItem = new(GrpcServerBatcherItemType.KeyValues, Interlocked.Increment(ref requestId), new(message), promise);

        return TryProcessQueue(grpcBatcherItem, promise);
    }

    public Task<GrpcServerBatcherResponse> Enqueue(GrpcGetSnapshotFloorRequest message)
    {
        TaskCompletionSource<GrpcServerBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcServerBatcherItem grpcBatcherItem = new(GrpcServerBatcherItemType.KeyValues, Interlocked.Increment(ref requestId), new(message), promise);

        return TryProcessQueue(grpcBatcherItem, promise);
    }

    private Task<GrpcServerBatcherResponse> TryProcessQueue(GrpcServerBatcherItem grpcBatcherItem, TaskCompletionSource<GrpcServerBatcherResponse> promise)
    {
        EnsureReaperStarted(logger);

        if (!TryAdmit(grpcBatcherItem))
            return Task.FromException<GrpcServerBatcherResponse>(new RpcException(new(
                StatusCode.Unavailable,
                "The inter-node request was refused before dispatch: the pending-request admission limit was reached.")));

        inbox.Enqueue(grpcBatcherItem);

        if (1 == Interlocked.Exchange(ref processing, 0))
            _ = DeliverMessages();

        return promise.Task;
    }

    /// <summary>
    /// Admits a request into the process-wide pending set, or refuses it when either admission
    /// limit is reached. Admission registers the item in <see cref="requestRefs"/> immediately,
    /// so the deadline reaper covers the request from enqueue — including its time in the inbox
    /// and behind blocked writes, where no response and no stream error can ever settle it.
    /// The limit checks and the counter updates are not one atomic step: concurrent producers
    /// can overshoot a limit by their own count. That overshoot is small and bounded; the limits
    /// exist to stop unbounded growth under sustained overload, not to enforce an exact ceiling.
    /// </summary>
    internal static bool TryAdmit(in GrpcServerBatcherItem item)
    {
        if (Volatile.Read(ref pendingRequests) >= MaxPendingRequests ||
            Volatile.Read(ref pendingRequestBytes) >= MaxPendingRequestBytes)
            return false;

        Interlocked.Increment(ref pendingRequests);
        Interlocked.Add(ref pendingRequestBytes, item.PayloadBytes);
        requestRefs[item.RequestId] = item;

        return true;
    }

    /// <summary>
    /// Removes one admitted request and releases its admission accounting. Every path that
    /// settles a request (response arrival, stream failure, deadline expiry, batch failure) must
    /// remove it through this method and nowhere else: the winner of the TryRemove race releases
    /// the accounting exactly once, and a loser must not touch the counters.
    /// </summary>
    internal static bool TryTakeRequest(int itemRequestId, out GrpcServerBatcherItem item)
    {
        if (!requestRefs.TryRemove(itemRequestId, out item))
            return false;

        Interlocked.Decrement(ref pendingRequests);
        Interlocked.Add(ref pendingRequestBytes, -item.PayloadBytes);

        return true;
    }

    /// <summary>
    /// Starts the process-wide deadline reaper on first use. The reaper is the liveness backstop
    /// for requests whose stream went quiet without dying: no response and no stream error will
    /// ever complete them, so an unswept <see cref="requestRefs"/> entry would hang its caller
    /// forever and leak. One loop serves every batcher instance because the dictionaries are
    /// process-wide statics.
    /// </summary>
    private static void EnsureReaperStarted(ILogger logger)
    {
        if (Interlocked.Exchange(ref reaperStarted, 1) == 1)
            return;

        _ = Task.Run(async () =>
        {
            while (true)
            {
                try
                {
                    await Task.Delay(ReaperInterval).ConfigureAwait(false);
                    SweepExpiredRequests(Environment.TickCount64, logger);
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "GrpcServerBatcher reaper sweep failed: {ExType}: {Message}", ex.GetType().Name, ex.Message);
                }
            }
        });
    }

    /// <summary>
    /// Fails every tracked request older than <see cref="RequestDeadline"/> with a retryable
    /// <see cref="StatusCode.Unavailable"/> and scrubs it from both tracking dictionaries.
    /// Exposed with an explicit clock input so tests can drive expiry deterministically.
    /// </summary>
    internal static void SweepExpiredRequests(long nowTicks, ILogger logger)
    {
        double deadlineMs = RequestDeadline.TotalMilliseconds;
        int expired = 0;

        // Weakly-consistent enumeration is fine: a request completed concurrently loses the
        // TryRemove race and is skipped; a request enqueued concurrently is younger than the
        // deadline and is skipped by the age check.
        foreach (KeyValuePair<int, GrpcServerBatcherItem> entry in requestRefs)
        {
            if (nowTicks - entry.Value.EnqueuedAtTicks < deadlineMs)
                continue;

            if (!TryTakeRequest(entry.Key, out GrpcServerBatcherItem item))
                continue;

            requestStreamRefs.TryRemove(entry.Key, out _);
            item.Promise.TrySetException(new RpcException(new(
                StatusCode.Unavailable, "The remote node did not answer within the inter-node request deadline.")));
            expired++;
        }

        if (expired > 0)
            logger.LogWarning("GrpcServerBatcher reaper expired {Count} request(s) past the {Deadline} deadline — the peer stream is quiet; requests fail retryably instead of hanging.", expired, RequestDeadline);
    }

    /// <summary>
    /// Evicts and disposes every shared streaming for <paramref name="streamUrl"/> so the next
    /// enqueue rebuilds fresh streams. Without the eviction the registry — populated once per URL
    /// for the process lifetime — would keep handing out dead call objects forever, which was half
    /// of the permanent-wedge failure mode. Disposal also unblocks any <c>WriteAsync</c> stuck on
    /// the stalled session and drives the read loops into their failure cleanup. Idempotent and
    /// safe to race from the write path, both read loops, and sibling streams.
    /// <para>
    /// When <paramref name="failingStreamId"/> is given, the eviction only happens if the current
    /// registry entry still contains that stream: a slow failure surfacing AFTER the URL was
    /// already evicted and rebuilt must not tear down the healthy replacement (transiently
    /// harmless — it would be rebuilt again — but a failure storm would churn streams for
    /// nothing).
    /// </para>
    /// </summary>
    private static void InvalidateStreamingsForUrl(string streamUrl, ILogger logger, string reason, long? failingStreamId = null)
    {
        if (!streamings.TryGetValue(streamUrl, out Lazy<List<GrpcServerSharedStreaming>>? current))
            return;

        if (failingStreamId is not null)
        {
            // An entry still being lazily created cannot contain the failing stream (ids are
            // assigned during creation and the failing stream was in use); leave it alone.
            if (!current.IsValueCreated)
                return;

            bool containsFailing = false;
            foreach (GrpcServerSharedStreaming streaming in current.Value)
            {
                if (streaming.Id == failingStreamId.Value)
                {
                    containsFailing = true;
                    break;
                }
            }

            if (!containsFailing)
                return;
        }

        if (!streamings.TryRemove(new KeyValuePair<string, Lazy<List<GrpcServerSharedStreaming>>>(streamUrl, current)))
            return;

        logger.LogWarning("GrpcServerBatcher evicting shared streams for {Url}: {Reason}", streamUrl, reason);

        if (!current.IsValueCreated)
            return;

        foreach (GrpcServerSharedStreaming streaming in current.Value)
            streaming.Dispose();
    }

    /// <summary>
    /// It retrieves a message from the inbox and invokes the actor by passing one message
    /// at a time until the pending message list is cleared. A drain failure is logged, never
    /// propagated: the handoff below must always run, because a dispatcher that exits with the
    /// flag still claimed leaves every later enqueue unable to start a replacement.
    /// </summary>
    /// <returns></returns>
    private async Task DeliverMessages()
    {
        while (true)
        {
            try
            {
                do
                {
                    // One buffer owned by this loop, reused for every drain. At most one dispatch
                    // loop runs per batcher, and RunBatch is awaited to completion before the next
                    // drain begins, so no reader can still hold the list when it is cleared.
                    List<GrpcServerBatcherItem> messages = dispatchBuffer ??= new(2);

                    try
                    {
                        while (messages.Count < MaxItemsPerDrain && inbox.TryDequeue(out GrpcServerBatcherItem message))
                            messages.Add(message);

                        if (messages.Count > 0)
                            await RunBatch(messages);
                    }
                    finally
                    {
                        // Drop the drained items now rather than at the next drain: they hold
                        // request payloads and promises, and an idle batcher must not pin them.
                        // Clear() keeps the backing array, and the drain bound keeps that array
                        // at or under MaxItemsPerDrain entries, so the retention stays small.
                        messages.Clear();
                    }

                } while (!inbox.IsEmpty);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "GrpcServerBatcher DeliverMessages failed: {ExType}: {Message}", ex.GetType().Name, ex.Message);
            }

            if (TryReleaseDispatch())
                return;
        }
    }

    /// <summary>
    /// Hands dispatch ownership back and decides whether this dispatcher may exit. The release
    /// must come before the final emptiness decision: a producer that enqueued between the drain
    /// loop's empty check and this release observed a claimed flag and started no dispatcher, so
    /// without the recheck its item would wait until unrelated later traffic arrived — or, on a
    /// quiet batcher, until the deadline reaper failed it. Returns true when the inbox is empty
    /// after the release, or when a racing producer already claimed ownership and its dispatcher
    /// now owns the queue. Returns false when this dispatcher reclaimed ownership and must keep
    /// the drain loop running.
    /// </summary>
    internal bool TryReleaseDispatch()
    {
        Interlocked.Exchange(ref processing, 1);

        if (inbox.IsEmpty)
            return true;

        return Interlocked.Exchange(ref processing, 0) != 1;
    }

    /// <summary>
    /// Processes one drained batch: partitions the live items by stream kind, plans one write
    /// list per stream, and runs the per-stream writers concurrently. Lock and key-value writes
    /// never share a writer, and the plan spreads envelopes round-robin over every stream to the
    /// peer, so a drain of N items no longer funnels through one sequential writer. When the peer
    /// advertised the capability, queued key-value items share coalesced envelopes under the item
    /// and byte caps; a lone item keeps the single-operation envelope. The caller keeps ownership
    /// of <paramref name="requests"/>: this method reads the list and never clears, reuses or
    /// releases it. The dispatch loop relies on that, because the list it passes is the buffer it
    /// reuses for the next drain.
    /// </summary>
    private async Task RunBatch(List<GrpcServerBatcherItem> requests)
    {
        try
        {
            List<GrpcServerSharedStreaming> nodeStreamings = GetSharedStreamingList();
            int streamCount = nodeStreamings.Count;

            List<GrpcServerBatcherItem> lockItems = lockScratch ??= [];
            List<GrpcServerBatcherItem> keyValueItems = keyValueScratch ??= [];
            lockItems.Clear();
            keyValueItems.Clear();

            foreach (GrpcServerBatcherItem request in requests)
            {
                // The request entered requestRefs at admission time, so the deadline reaper may
                // have failed it while it waited in the inbox or behind an earlier blocked write.
                // A settled promise has nobody listening: skip the send instead of spending
                // stream bandwidth on it and producing an orphan response.
                if (request.Promise.Task.IsCompleted)
                    continue;

                switch (request.Type)
                {
                    case GrpcServerBatcherItemType.Locks:
                        lockItems.Add(request);
                        break;

                    case GrpcServerBatcherItemType.KeyValues:
                        keyValueItems.Add(request);
                        break;

                    default:
                        throw new KahunaServerException("Unknown request type: " + request.Type);
                }
            }

            bool coalesce = keyValueItems.Count > 1;

            List<GrpcBatchServerKeyValueRequest>?[] keyValuePlans = new List<GrpcBatchServerKeyValueRequest>?[streamCount];
            List<GrpcBatchServerLockRequest>?[] lockPlans = new List<GrpcBatchServerLockRequest>?[streamCount];

            int index = 0;
            while (index < keyValueItems.Count)
            {
                int count = coalesce ? CountCoalescedItems(keyValueItems, index) : 1;
                int streamIndex = (int)(streamCursor++ % (uint)streamCount);
                GrpcServerSharedStreaming stream = nodeStreamings[streamIndex];

                GrpcBatchServerKeyValueRequest envelope;
                if (count == 1)
                    envelope = BuildKeyValueEnvelope(keyValueItems[index]);
                else
                {
                    envelope = new() { Type = GrpcServerBatchType.ServerCoalesced };
                    envelope.Coalesced.Capacity = count;

                    for (int i = index; i < index + count; i++)
                        envelope.Coalesced.Add(BuildKeyValueEnvelope(keyValueItems[i]));
                }

                // Bind every covered item to its stream before any writer starts, so a stream
                // failure fails the whole plan for that stream — unsent items included.
                for (int i = index; i < index + count; i++)
                    requestStreamRefs[keyValueItems[i].RequestId] = stream.Id;

                (keyValuePlans[streamIndex] ??= []).Add(envelope);
                index += count;
            }

            foreach (GrpcServerBatcherItem lockItem in lockItems)
            {
                int streamIndex = (int)(streamCursor++ % (uint)streamCount);
                GrpcServerSharedStreaming stream = nodeStreamings[streamIndex];

                requestStreamRefs[lockItem.RequestId] = stream.Id;
                (lockPlans[streamIndex] ??= []).Add(BuildLockEnvelope(lockItem));
            }

            List<Task> writers = new(streamCount * 2);

            for (int s = 0; s < streamCount; s++)
            {
                if (keyValuePlans[s] is { } keyValueEnvelopes)
                    writers.Add(WriteKeyValueEnvelopes(nodeStreamings[s], keyValueEnvelopes));

                if (lockPlans[s] is { } lockEnvelopes)
                    writers.Add(WriteLockEnvelopes(nodeStreamings[s], lockEnvelopes));
            }

            if (writers.Count == 1)
                await writers[0].ConfigureAwait(false);
            else if (writers.Count > 1)
                await Task.WhenAll(writers).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            foreach (GrpcServerBatcherItem request in requests)
            {
                TryTakeRequest(request.RequestId, out _);
                requestStreamRefs.TryRemove(request.RequestId, out _);
                request.Promise.TrySetException(ex);
            }

            logger.LogError(ex, "GrpcServerBatcher RunBatch failed: {ExType}: {Message}", ex.GetType().Name, ex.Message);
        }
    }

    /// <summary>
    /// Returns how many items, from <paramref name="start"/>, the next coalesced envelope takes:
    /// at least one, at most <see cref="MaxOpsPerCoalescedEnvelope"/>, and never past the point
    /// where the summed payload bytes would exceed <see cref="MaxCoalescedEnvelopeBytes"/>. The
    /// first item is always taken, so an item larger than the byte cap still travels — alone,
    /// exactly as it does on the single-operation path.
    /// </summary>
    internal static int CountCoalescedItems(List<GrpcServerBatcherItem> items, int start)
    {
        int count = 1;
        long bytes = items[start].PayloadBytes;

        while (start + count < items.Count && count < MaxOpsPerCoalescedEnvelope)
        {
            long nextBytes = items[start + count].PayloadBytes;

            if (bytes + nextBytes > MaxCoalescedEnvelopeBytes)
                break;

            bytes += nextBytes;
            count++;
        }

        return count;
    }

    /// <summary>
    /// Writes one stream's planned key-value envelopes in order. A failure stays scoped to this
    /// stream: <see cref="WriteBoundedAsync{T}"/> already evicted it and failed its pending
    /// requests — this drain's unsent items included, because their stream refs were recorded
    /// before any writer started — so the writers on the peer's other streams continue.
    /// </summary>
    private async Task WriteKeyValueEnvelopes(GrpcServerSharedStreaming sharedStreaming, List<GrpcBatchServerKeyValueRequest> envelopes)
    {
        try
        {
            foreach (GrpcBatchServerKeyValueRequest envelope in envelopes)
                await WriteBoundedAsync(sharedStreaming, sharedStreaming.KeyValueWriteSemaphore, sharedStreaming.KeyValueStreaming.RequestStream, envelope).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogWarning("GrpcServerBatcher key-value writer for stream {StreamId} failed: {ExType}: {Message}", sharedStreaming.Id, ex.GetType().Name, ex.Message);
        }
    }

    /// <summary>
    /// Writes one stream's planned lock envelopes in order, with the same failure scoping as
    /// <see cref="WriteKeyValueEnvelopes"/>.
    /// </summary>
    private async Task WriteLockEnvelopes(GrpcServerSharedStreaming sharedStreaming, List<GrpcBatchServerLockRequest> envelopes)
    {
        try
        {
            foreach (GrpcBatchServerLockRequest envelope in envelopes)
                await WriteBoundedAsync(sharedStreaming, sharedStreaming.LockWriteSemaphore, sharedStreaming.LockStreaming.RequestStream, envelope).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogWarning("GrpcServerBatcher lock writer for stream {StreamId} failed: {ExType}: {Message}", sharedStreaming.Id, ex.GetType().Name, ex.Message);
        }
    }

    private static void FailPendingRequests(long sharedStreamingId, Exception ex)
    {
        // Enumerate the concurrent dictionary directly: its enumerator is weakly consistent and safe
        // under concurrent TryRemove, so a snapshot array (allocated precisely while a stream is
        // failing and in-flight work is peaking) is unnecessary.
        foreach (KeyValuePair<int, long> entry in requestStreamRefs)
        {
            if (entry.Value != sharedStreamingId)
                continue;

            requestStreamRefs.TryRemove(entry.Key, out _);

            if (TryTakeRequest(entry.Key, out GrpcServerBatcherItem item))
                item.Promise.TrySetException(ex);
        }
    }

    /// <summary>
    /// Serializes one write onto the given stream writer under its own semaphore, bounded by
    /// <see cref="WriteTimeout"/> on both the semaphore acquisition and the write itself. Either
    /// bound firing means the stream is stalled (the run-J failure mode: a SIGSTOPed peer's
    /// HTTP/2 window fills and the write never completes, holding the semaphore forever): the
    /// stream is evicted and disposed, its pending requests fail retryably, and the thrown
    /// <see cref="RpcException"/> makes the caller's write group fail the same way instead of
    /// wedging.
    /// </summary>
    private async Task WriteBoundedAsync<T>(GrpcServerSharedStreaming sharedStreaming, SemaphoreSlim writeSemaphore, IClientStreamWriter<T> writer, T batchRequest)
    {
        if (!await writeSemaphore.WaitAsync(WriteTimeout).ConfigureAwait(false))
        {
            RpcException stalled = new(new(StatusCode.Unavailable, "gRPC inter-node stream stalled: the write pipeline is blocked past the write timeout."));
            InvalidateStreamingsForUrl(url, logger, "write semaphore held past the write timeout — a previous write is stuck on a stalled stream", sharedStreaming.Id);
            FailPendingRequests(sharedStreaming.Id, stalled);
            throw stalled;
        }

        try
        {
            await writer.WriteAsync(batchRequest).WaitAsync(WriteTimeout).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            RpcException failure = ex as RpcException ?? new RpcException(new(
                StatusCode.Unavailable, $"gRPC inter-node stream write failed or timed out: {ex.GetType().Name}."));
            InvalidateStreamingsForUrl(url, logger, $"write failed or timed out ({ex.GetType().Name})", sharedStreaming.Id);
            FailPendingRequests(sharedStreaming.Id, failure);
            throw failure;
        }
        finally
        {
            writeSemaphore.Release();
        }
    }

    /// <summary>
    /// Builds the single-operation lock envelope for one item. Pure: the caller owns the write.
    /// </summary>
    internal static GrpcBatchServerLockRequest BuildLockEnvelope(in GrpcServerBatcherItem request)
    {
        GrpcBatchServerLockRequest batchRequest = new()
        {
            RequestId = request.RequestId,

            // This write is the next hop of the chain the item recorded at enqueue time.
            ForwardHops = request.ForwardHops + 1
        };

        GrpcServerBatcherRequest itemRequest = request.Request;

        if (itemRequest.TryLock is not null)
        {
            batchRequest.Type = GrpcLockServerBatchType.ServerTypeTryLock;
            batchRequest.TryLock = itemRequest.TryLock;
        } 
        else if (itemRequest.Unlock is not null)
        {
            batchRequest.Type = GrpcLockServerBatchType.ServerTypeUnlock;
            batchRequest.Unlock = itemRequest.Unlock;
        }
        else if (itemRequest.ExtendLock is not null)
        {
            batchRequest.Type = GrpcLockServerBatchType.ServerTypeExtendLock;
            batchRequest.ExtendLock = itemRequest.ExtendLock;
        }
        else if (itemRequest.GetLock is not null)
        {
            batchRequest.Type = GrpcLockServerBatchType.ServerTypeGetLock;
            batchRequest.GetLock = itemRequest.GetLock;
        }
        else
            throw new KahunaServerException("Unknown request type");

        return batchRequest;
    }

    /// <summary>
    /// Builds the single-operation key-value envelope for one item. Pure: the caller owns the
    /// write, and a coalesced envelope carries these as its inner entries unchanged.
    /// </summary>
    internal static GrpcBatchServerKeyValueRequest BuildKeyValueEnvelope(in GrpcServerBatcherItem request)
    {
        GrpcBatchServerKeyValueRequest batchRequest = new()
        {
            RequestId = request.RequestId,

            // This write is the next hop of the chain the item recorded at enqueue time.
            ForwardHops = request.ForwardHops + 1
        };

        GrpcServerBatcherRequest itemRequest = request.Request;

        if (itemRequest.TrySetKeyValue is not null)
        {
            batchRequest.Type = GrpcServerBatchType.ServerTrySetKeyValue;
            batchRequest.TrySetKeyValue = itemRequest.TrySetKeyValue;
        }
        else if (itemRequest.TrySetManyKeyValue is not null)
        {
            batchRequest.Type = GrpcServerBatchType.ServerTrySetManyKeyValue;
            batchRequest.TrySetManyKeyValue = itemRequest.TrySetManyKeyValue;
        }
        else if (itemRequest.TryDeleteManyKeyValue is not null)
        {
            batchRequest.Type = GrpcServerBatchType.ServerTryDeleteManyKeyValue;
            batchRequest.TryDeleteManyKeyValue = itemRequest.TryDeleteManyKeyValue;
        }
        else if (itemRequest.TryGetKeyValue is not null)
        {
            batchRequest.Type = GrpcServerBatchType.ServerTryGetKeyValue;
            batchRequest.TryGetKeyValue = itemRequest.TryGetKeyValue;
        }
        else if (itemRequest.TryGetManyValues is not null)
        {
            batchRequest.Type = GrpcServerBatchType.ServerTryGetManyValues;
            batchRequest.TryGetManyValues = itemRequest.TryGetManyValues;
        }
        else if (itemRequest.TryDeleteKeyValue is not null)
        {
            batchRequest.Type = GrpcServerBatchType.ServerTryDeleteKeyValue;
            batchRequest.TryDeleteKeyValue = itemRequest.TryDeleteKeyValue;
        } 
        else if (itemRequest.TryExtendKeyValue is not null)
        {
            batchRequest.Type = GrpcServerBatchType.ServerTryExtendKeyValue;
            batchRequest.TryExtendKeyValue = itemRequest.TryExtendKeyValue;
        } 
        else if (itemRequest.TryExistsKeyValue is not null)
        {
            batchRequest.Type = GrpcServerBatchType.ServerTryExistsKeyValue;
            batchRequest.TryExistsKeyValue = itemRequest.TryExistsKeyValue;
        }
        else if (itemRequest.TryExistsManyValues is not null)
        {
            batchRequest.Type = GrpcServerBatchType.ServerTryExistsManyValues;
            batchRequest.TryExistsManyValues = itemRequest.TryExistsManyValues;
        }
        else if (itemRequest.TryCheckWriteIntent is not null)
        {
            batchRequest.Type = GrpcServerBatchType.ServerTryCheckWriteIntent;
            batchRequest.TryCheckWriteIntent = itemRequest.TryCheckWriteIntent;
        }
        else if (itemRequest.TryCheckManyWriteIntents is not null)
        {
            batchRequest.Type = GrpcServerBatchType.ServerTryCheckManyWriteIntents;
            batchRequest.TryCheckManyWriteIntents = itemRequest.TryCheckManyWriteIntents;
        }
        else if (itemRequest.GetByBucket is not null)
        {
            batchRequest.Type = GrpcServerBatchType.ServerTryGetByBucket;
            batchRequest.GetByBucket = itemRequest.GetByBucket;
        }
        else if (itemRequest.GetByRange is not null)
        {
            batchRequest.Type = GrpcServerBatchType.ServerTryGetByRange;
            batchRequest.GetByRange = itemRequest.GetByRange;
        }
        else if (itemRequest.ScanByPrefix is not null)
        {
            batchRequest.Type = GrpcServerBatchType.ServerTryScanByPrefix;
            batchRequest.ScanByPrefix = itemRequest.ScanByPrefix;
        }
        else if (itemRequest.TryExecuteTransactionScript is not null)
        {
            batchRequest.Type = GrpcServerBatchType.ServerTryExecuteTransactionScript;
            batchRequest.TryExecuteTransactionScript = itemRequest.TryExecuteTransactionScript;
        } 
        else if (itemRequest.TryAcquireExclusiveLock is not null)
        {
            batchRequest.Type = GrpcServerBatchType.ServerTryAcquireExclusiveLock;
            batchRequest.TryAcquireExclusiveLock = itemRequest.TryAcquireExclusiveLock;
        }
        else if (itemRequest.TryAcquireExclusivePrefixLock is not null)
        {
            batchRequest.Type = GrpcServerBatchType.ServerTryAcquireExclusivePrefixLock;
            batchRequest.TryAcquireExclusivePrefixLock = itemRequest.TryAcquireExclusivePrefixLock;
        }
        else if (itemRequest.TryAcquireExclusiveRangeLock is not null)
        {
            batchRequest.Type = GrpcServerBatchType.ServerTryAcquireExclusiveRangeLock;
            batchRequest.TryAcquireExclusiveRangeLock = itemRequest.TryAcquireExclusiveRangeLock;
        }
        else if (itemRequest.TryAcquireManyExclusiveLocks is not null)
        {
            batchRequest.Type = GrpcServerBatchType.ServerTryAcquireManyExclusiveLocks;
            batchRequest.TryAcquireManyExclusiveLocks = itemRequest.TryAcquireManyExclusiveLocks;
        }
        else if (itemRequest.TryReleaseExclusiveLock is not null)
        {
            batchRequest.Type = GrpcServerBatchType.ServerTryReleaseExclusiveLock;
            batchRequest.TryReleaseExclusiveLock = itemRequest.TryReleaseExclusiveLock;
        }
        else if (itemRequest.TryReleaseExclusivePrefixLock is not null)
        {
            batchRequest.Type = GrpcServerBatchType.ServerTryReleaseExclusivePrefixLock;
            batchRequest.TryReleaseExclusivePrefixLock = itemRequest.TryReleaseExclusivePrefixLock;
        }
        else if (itemRequest.TryReleaseExclusiveRangeLock is not null)
        {
            batchRequest.Type = GrpcServerBatchType.ServerTryReleaseExclusiveRangeLock;
            batchRequest.TryReleaseExclusiveRangeLock = itemRequest.TryReleaseExclusiveRangeLock;
        }
        else if (itemRequest.TryReleaseManyExclusiveLocks is not null)
        {
            batchRequest.Type = GrpcServerBatchType.ServerTryReleaseManyExclusiveLocks;
            batchRequest.TryReleaseManyExclusiveLocks = itemRequest.TryReleaseManyExclusiveLocks;
        }
        else if (itemRequest.TryPrepareMutations is not null)
        {
            batchRequest.Type = GrpcServerBatchType.ServerTryPrepareMutations;
            batchRequest.TryPrepareMutations = itemRequest.TryPrepareMutations;
        }
        else if (itemRequest.TryPrepareManyMutations is not null)
        {
            batchRequest.Type = GrpcServerBatchType.ServerTryPrepareManyMutations;
            batchRequest.TryPrepareManyMutations = itemRequest.TryPrepareManyMutations;
        }
        else if (itemRequest.TryCommitMutations is not null)
        {
            batchRequest.Type = GrpcServerBatchType.ServerTryCommitMutations;
            batchRequest.TryCommitMutations = itemRequest.TryCommitMutations;
        }
        else if (itemRequest.TryCommitManyMutations is not null)
        {
            batchRequest.Type = GrpcServerBatchType.ServerTryCommitManyMutations;
            batchRequest.TryCommitManyMutations = itemRequest.TryCommitManyMutations;
        }
        else if (itemRequest.TryRollbackMutations is not null)
        {
            batchRequest.Type = GrpcServerBatchType.ServerTryRollbackMutations;
            batchRequest.TryRollbackMutations = itemRequest.TryRollbackMutations;
        }
        else if (itemRequest.TryRollbackManyMutations is not null)
        {
            batchRequest.Type = GrpcServerBatchType.ServerTryRollbackManyMutations;
            batchRequest.TryRollbackManyMutations = itemRequest.TryRollbackManyMutations;
        }
        else if (itemRequest.StartTransaction is not null)
        {
            batchRequest.Type = GrpcServerBatchType.ServerTryStartTransaction;
            batchRequest.StartTransaction = itemRequest.StartTransaction;
        }
        else if (itemRequest.CommitTransaction is not null)
        {
            batchRequest.Type = GrpcServerBatchType.ServerTryCommitTransaction;
            batchRequest.CommitTransaction = itemRequest.CommitTransaction;
        }
        else if (itemRequest.RollbackTransaction is not null)
        {
            batchRequest.Type = GrpcServerBatchType.ServerTryRollbackTransaction;
            batchRequest.RollbackTransaction = itemRequest.RollbackTransaction;
        }
        else if (itemRequest.BeginOperation is not null)
        {
            batchRequest.Type = GrpcServerBatchType.ServerBeginOperation;
            batchRequest.BeginOperation = itemRequest.BeginOperation;
        }
        else if (itemRequest.CompleteOperation is not null)
        {
            batchRequest.Type = GrpcServerBatchType.ServerCompleteOperation;
            batchRequest.CompleteOperation = itemRequest.CompleteOperation;
        }
        else if (itemRequest.GetTransactionWorkingSet is not null)
        {
            batchRequest.Type = GrpcServerBatchType.ServerGetTransactionWorkingSet;
            batchRequest.GetTransactionWorkingSet = itemRequest.GetTransactionWorkingSet;
        }
        else if (itemRequest.CloseTransaction is not null)
        {
            batchRequest.Type = GrpcServerBatchType.ServerCloseTransaction;
            batchRequest.CloseTransaction = itemRequest.CloseTransaction;
        }
        else if (itemRequest.EnsureKeyRangeSeeded is not null)
        {
            batchRequest.Type = GrpcServerBatchType.ServerTryEnsureKeyRangeSeeded;
            batchRequest.EnsureKeyRangeSeeded = itemRequest.EnsureKeyRangeSeeded;
        }
        else if (itemRequest.EnsureKeyRangeRemoved is not null)
        {
            batchRequest.Type = GrpcServerBatchType.ServerTryEnsureKeyRangeRemoved;
            batchRequest.EnsureKeyRangeRemoved = itemRequest.EnsureKeyRangeRemoved;
        }
        else if (itemRequest.GetRangeLocks is not null)
        {
            batchRequest.Type = GrpcServerBatchType.ServerTryGetRangeLocks;
            batchRequest.GetRangeLocks = itemRequest.GetRangeLocks;
        }
        else if (itemRequest.ImportRangeLocks is not null)
        {
            batchRequest.Type = GrpcServerBatchType.ServerTryImportRangeLocks;
            batchRequest.ImportRangeLocks = itemRequest.ImportRangeLocks;
        }
        else if (itemRequest.ImportCompletionReceipts is not null)
        {
            batchRequest.Type = GrpcServerBatchType.ServerImportCompletionReceipts;
            batchRequest.ImportCompletionReceipts = itemRequest.ImportCompletionReceipts;
        }
        else if (itemRequest.DurableOperation is not null)
        {
            batchRequest.Type = GrpcServerBatchType.ServerDurableOperation;
            batchRequest.DurableOperation = itemRequest.DurableOperation;
        }
        else if (itemRequest.DurableBundle is not null)
        {
            batchRequest.Type = GrpcServerBatchType.ServerDurableBundle;
            batchRequest.DurableBundle = itemRequest.DurableBundle;
        }
        else if (itemRequest.DurableDecision is not null)
        {
            batchRequest.Type = GrpcServerBatchType.ServerDurableDecision;
            batchRequest.DurableDecision = itemRequest.DurableDecision;
        }
        else if (itemRequest.DurableOnePhase is not null)
        {
            batchRequest.Type = GrpcServerBatchType.ServerDurableOnePhase;
            batchRequest.DurableOnePhase = itemRequest.DurableOnePhase;
        }
        else if (itemRequest.LookupTransactionRecord is not null)
        {
            batchRequest.Type = GrpcServerBatchType.ServerLookupTransactionRecord;
            batchRequest.LookupTransactionRecord = itemRequest.LookupTransactionRecord;
        }
        else if (itemRequest.ReplicateKeyValueRangePage is not null)
        {
            batchRequest.Type = GrpcServerBatchType.ServerReplicateKeyValueRangePage;
            batchRequest.ReplicateKeyValueRangePage = itemRequest.ReplicateKeyValueRangePage;
        }
        else if (itemRequest.GetRangeTransactionState is not null)
        {
            batchRequest.Type = GrpcServerBatchType.ServerGetRangeTransactionState;
            batchRequest.GetRangeTransactionState = itemRequest.GetRangeTransactionState;
        }
        else if (itemRequest.GetStagedBaseVerdicts is not null)
        {
            batchRequest.Type = GrpcServerBatchType.ServerGetStagedBaseVerdicts;
            batchRequest.GetStagedBaseVerdicts = itemRequest.GetStagedBaseVerdicts;
        }
        else if (itemRequest.AcquireSnapshotHold is not null)
        {
            batchRequest.Type = GrpcServerBatchType.ServerTryAcquireSnapshotHold;
            batchRequest.AcquireSnapshotHold = itemRequest.AcquireSnapshotHold;
        }
        else if (itemRequest.RenewSnapshotHold is not null)
        {
            batchRequest.Type = GrpcServerBatchType.ServerTryRenewSnapshotHold;
            batchRequest.RenewSnapshotHold = itemRequest.RenewSnapshotHold;
        }
        else if (itemRequest.ReleaseSnapshotHold is not null)
        {
            batchRequest.Type = GrpcServerBatchType.ServerTryReleaseSnapshotHold;
            batchRequest.ReleaseSnapshotHold = itemRequest.ReleaseSnapshotHold;
        }
        else if (itemRequest.GetSnapshotFloor is not null)
        {
            batchRequest.Type = GrpcServerBatchType.ServerTryGetSnapshotFloor;
            batchRequest.GetSnapshotFloor = itemRequest.GetSnapshotFloor;
        }
        else
            throw new KahunaServerException("Unknown request type");

        return batchRequest;
    }

    private static async Task ReadLockMessages(string streamUrl, long sharedStreamingId, AsyncDuplexStreamingCall<GrpcBatchServerLockRequest, GrpcBatchServerLockResponse> streaming, ILogger logger)
    {
        try
        {
            await foreach (GrpcBatchServerLockResponse response in streaming.ResponseStream.ReadAllAsync())
            {
                if (!TryTakeRequest(response.RequestId, out GrpcServerBatcherItem item))
                {
                    logger.LogWarning("GrpcServerBatcher lock response: request not found {RequestId}", response.RequestId);
                    continue;
                }

                requestStreamRefs.TryRemove(response.RequestId, out _);

                switch (response.Type)
                {
                    case GrpcLockServerBatchType.ServerTypeTryLock:
                        item.Promise.TrySetResult(new(response.TryLock));
                        break;

                    case GrpcLockServerBatchType.ServerTypeUnlock:
                        item.Promise.TrySetResult(new(response.Unlock));
                        break;

                    case GrpcLockServerBatchType.ServerTypeExtendLock:
                        item.Promise.TrySetResult(new(response.ExtendLock));
                        break;

                    case GrpcLockServerBatchType.ServerTypeGetLock:
                        item.Promise.TrySetResult(new(response.GetLock));
                        break;

                    // The peer answered the request but could not express an outcome in its payload:
                    // it produced no definitive answer, which is exactly the retryable condition.
                    case GrpcLockServerBatchType.ServerTypeNone:
                        item.Promise.TrySetException(new RpcException(new(
                            StatusCode.Unavailable, "The remote node could not answer the lock request.")));
                        break;

                    default:
                        item.Promise.TrySetException(new KahunaServerException("Unknown response type: " + response.Type));
                        break;
                }
            }

            RpcException streamClosed = new(new(StatusCode.Unavailable, "gRPC inter-node lock stream closed."));
            InvalidateStreamingsForUrl(streamUrl, logger, "lock response stream ended", sharedStreamingId);
            FailPendingRequests(sharedStreamingId, streamClosed);
        }
        catch (RpcException ex) when (ex.StatusCode is StatusCode.Unavailable or StatusCode.Cancelled)
        {
            logger.LogWarning("GrpcServerBatcher lock stream closed: {Status}", ex.Status);
            InvalidateStreamingsForUrl(streamUrl, logger, "lock stream closed", sharedStreamingId);
            FailPendingRequests(sharedStreamingId, ex);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "GrpcServerBatcher ReadLockMessages failed: {ExType}: {Message}", ex.GetType().Name, ex.Message);
            InvalidateStreamingsForUrl(streamUrl, logger, "lock read loop faulted", sharedStreamingId);
            FailPendingRequests(sharedStreamingId, ex);
        }
    }

    private static async Task ReadKeyValueMessages(string streamUrl, long sharedStreamingId, AsyncDuplexStreamingCall<GrpcBatchServerKeyValueRequest, GrpcBatchServerKeyValueResponse> streaming, ILogger logger)
    {
        try
        {
            await foreach (GrpcBatchServerKeyValueResponse response in streaming.ResponseStream.ReadAllAsync())
            {
                // A coalesced carrier bundles independent responses and answers no request of
                // its own; each inner response settles exactly like a directly-read one.
                if (response.Type == GrpcServerBatchType.ServerCoalesced)
                {
                    foreach (GrpcBatchServerKeyValueResponse inner in response.Coalesced)
                        SettleKeyValueResponse(inner, logger);
                }
                else
                    SettleKeyValueResponse(response, logger);
            }

            RpcException streamClosed = new(new(StatusCode.Unavailable, "gRPC inter-node key-value stream closed."));
            InvalidateStreamingsForUrl(streamUrl, logger, "key-value response stream ended", sharedStreamingId);
            FailPendingRequests(sharedStreamingId, streamClosed);
        }
        catch (RpcException ex) when (ex.StatusCode is StatusCode.Unavailable or StatusCode.Cancelled)
        {
            logger.LogWarning("GrpcServerBatcher key-value stream closed: {Status}", ex.Status);
            InvalidateStreamingsForUrl(streamUrl, logger, "key-value stream closed", sharedStreamingId);
            FailPendingRequests(sharedStreamingId, ex);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "GrpcServerBatcher ReadKeyValueMessages failed: {ExType}: {Message}", ex.GetType().Name, ex.Message);
            InvalidateStreamingsForUrl(streamUrl, logger, "key-value read loop faulted", sharedStreamingId);
            FailPendingRequests(sharedStreamingId, ex);
        }
    }

    /// <summary>
    /// Settles one key-value response against its pending request: matches by id, releases the
    /// admission accounting, and completes the promise from the typed payload. Shared by
    /// directly-read responses and the inner entries of a coalesced carrier.
    /// </summary>
    private static void SettleKeyValueResponse(GrpcBatchServerKeyValueResponse response, ILogger logger)
    {
        if (!TryTakeRequest(response.RequestId, out GrpcServerBatcherItem item))
        {
            logger.LogWarning("GrpcServerBatcher key-value response: request not found {RequestId}", response.RequestId);
            return;
        }

        requestStreamRefs.TryRemove(response.RequestId, out _);

        switch (response.Type)
        {
            case GrpcServerBatchType.ServerTrySetKeyValue:
                item.Promise.TrySetResult(new(response.TrySetKeyValue));
                break;

            case GrpcServerBatchType.ServerTrySetManyKeyValue:
                item.Promise.TrySetResult(new(response.TrySetManyKeyValue));
                break;

            case GrpcServerBatchType.ServerTryDeleteManyKeyValue:
                item.Promise.TrySetResult(new(response.TryDeleteManyKeyValue));
                break;

            case GrpcServerBatchType.ServerTryGetKeyValue:
                item.Promise.TrySetResult(new(response.TryGetKeyValue));
                break;

            case GrpcServerBatchType.ServerTryGetManyValues:
                item.Promise.TrySetResult(new(response.TryGetManyValues));
                break;

            case GrpcServerBatchType.ServerTryDeleteKeyValue:
                item.Promise.TrySetResult(new(response.TryDeleteKeyValue));
                break;

            case GrpcServerBatchType.ServerTryExtendKeyValue:
                item.Promise.TrySetResult(new(response.TryExtendKeyValue));
                break;

            case GrpcServerBatchType.ServerTryExistsKeyValue:
                item.Promise.TrySetResult(new(response.TryExistsKeyValue));
                break;

            case GrpcServerBatchType.ServerTryExistsManyValues:
                item.Promise.TrySetResult(new(response.TryExistsManyValues));
                break;

            case GrpcServerBatchType.ServerTryCheckWriteIntent:
                item.Promise.TrySetResult(new(response.TryCheckWriteIntent));
                break;

            case GrpcServerBatchType.ServerTryCheckManyWriteIntents:
                item.Promise.TrySetResult(new(response.TryCheckManyWriteIntents));
                break;

            case GrpcServerBatchType.ServerTryGetByBucket:
                item.Promise.TrySetResult(new(response.GetByBucket));
                break;

            case GrpcServerBatchType.ServerTryGetByRange:
                item.Promise.TrySetResult(new(response.GetByRange));
                break;

            case GrpcServerBatchType.ServerTryScanByPrefix:
                item.Promise.TrySetResult(new(response.ScanByPrefix));
                break;

            case GrpcServerBatchType.ServerTryExecuteTransactionScript:
                item.Promise.TrySetResult(new(response.TryExecuteTransactionScript));
                break;

            case GrpcServerBatchType.ServerTryAcquireExclusiveLock:
                item.Promise.TrySetResult(new(response.TryAcquireExclusiveLock));
                break;

            case GrpcServerBatchType.ServerTryAcquireExclusivePrefixLock:
                item.Promise.TrySetResult(new(response.TryAcquireExclusivePrefixLock));
                break;

            case GrpcServerBatchType.ServerTryAcquireExclusiveRangeLock:
                item.Promise.TrySetResult(new(response.TryAcquireExclusiveRangeLock));
                break;

            case GrpcServerBatchType.ServerTryAcquireManyExclusiveLocks:
                item.Promise.TrySetResult(new(response.TryAcquireManyExclusiveLocks));
                break;

            case GrpcServerBatchType.ServerTryReleaseExclusiveLock:
                item.Promise.TrySetResult(new(response.TryReleaseExclusiveLock));
                break;

            case GrpcServerBatchType.ServerTryReleaseExclusivePrefixLock:
                item.Promise.TrySetResult(new(response.TryReleaseExclusivePrefixLock));
                break;

            case GrpcServerBatchType.ServerTryReleaseExclusiveRangeLock:
                item.Promise.TrySetResult(new(response.TryReleaseExclusiveRangeLock));
                break;

            case GrpcServerBatchType.ServerTryReleaseManyExclusiveLocks:
                item.Promise.TrySetResult(new(response.TryReleaseManyExclusiveLocks));
                break;

            case GrpcServerBatchType.ServerTryPrepareMutations:
                item.Promise.TrySetResult(new(response.TryPrepareMutations));
                break;

            case GrpcServerBatchType.ServerTryPrepareManyMutations:
                item.Promise.TrySetResult(new(response.TryPrepareManyMutations));
                break;

            case GrpcServerBatchType.ServerTryCommitMutations:
                item.Promise.TrySetResult(new(response.TryCommitMutations));
                break;

            case GrpcServerBatchType.ServerTryCommitManyMutations:
                item.Promise.TrySetResult(new(response.TryCommitManyMutations));
                break;

            case GrpcServerBatchType.ServerTryRollbackMutations:
                item.Promise.TrySetResult(new(response.TryRollbackMutations));
                break;

            case GrpcServerBatchType.ServerTryRollbackManyMutations:
                item.Promise.TrySetResult(new(response.TryRollbackManyMutations));
                break;

            case GrpcServerBatchType.ServerTryStartTransaction:
                item.Promise.TrySetResult(new(response.StartTransaction));
                break;

            case GrpcServerBatchType.ServerTryCommitTransaction:
                item.Promise.TrySetResult(new(response.CommitTransaction));
                break;

            case GrpcServerBatchType.ServerTryRollbackTransaction:
                item.Promise.TrySetResult(new(response.RollbackTransaction));
                break;

            case GrpcServerBatchType.ServerBeginOperation:
                item.Promise.TrySetResult(new(response.BeginOperation));
                break;

            case GrpcServerBatchType.ServerCompleteOperation:
                item.Promise.TrySetResult(new(response.CompleteOperation));
                break;

            case GrpcServerBatchType.ServerGetTransactionWorkingSet:
                item.Promise.TrySetResult(new(response.GetTransactionWorkingSet));
                break;

            case GrpcServerBatchType.ServerCloseTransaction:
                item.Promise.TrySetResult(new(response.CloseTransaction));
                break;

            case GrpcServerBatchType.ServerTryEnsureKeyRangeSeeded:
                item.Promise.TrySetResult(new(response.EnsureKeyRangeSeeded));
                break;

            case GrpcServerBatchType.ServerTryEnsureKeyRangeRemoved:
                item.Promise.TrySetResult(new(response.EnsureKeyRangeRemoved));
                break;

            case GrpcServerBatchType.ServerTryGetRangeLocks:
                item.Promise.TrySetResult(new(response.GetRangeLocks));
                break;

            case GrpcServerBatchType.ServerTryImportRangeLocks:
                item.Promise.TrySetResult(new(response.ImportRangeLocks));
                break;

            case GrpcServerBatchType.ServerImportCompletionReceipts:
                item.Promise.TrySetResult(new(response.ImportCompletionReceipts));
                break;

            case GrpcServerBatchType.ServerDurableOperation:
                item.Promise.TrySetResult(new(response.DurableOperation));
                break;

            case GrpcServerBatchType.ServerDurableBundle:
                item.Promise.TrySetResult(new(response.DurableBundle));
                break;

            case GrpcServerBatchType.ServerDurableDecision:
                item.Promise.TrySetResult(new(response.DurableDecision));
                break;

            case GrpcServerBatchType.ServerDurableOnePhase:
                item.Promise.TrySetResult(new(response.DurableOnePhase));
                break;

            case GrpcServerBatchType.ServerLookupTransactionRecord:
                item.Promise.TrySetResult(new(response.LookupTransactionRecord));
                break;

            case GrpcServerBatchType.ServerReplicateKeyValueRangePage:
                item.Promise.TrySetResult(new(response.ReplicateKeyValueRangePage));
                break;

            case GrpcServerBatchType.ServerGetRangeTransactionState:
                item.Promise.TrySetResult(new(response.GetRangeTransactionState));
                break;

            case GrpcServerBatchType.ServerGetStagedBaseVerdicts:
                item.Promise.TrySetResult(new(response.GetStagedBaseVerdicts));
                break;

            case GrpcServerBatchType.ServerTryAcquireSnapshotHold:
                item.Promise.TrySetResult(new(response.AcquireSnapshotHold));
                break;

            case GrpcServerBatchType.ServerTryRenewSnapshotHold:
                item.Promise.TrySetResult(new(response.RenewSnapshotHold));
                break;

            case GrpcServerBatchType.ServerTryReleaseSnapshotHold:
                item.Promise.TrySetResult(new(response.ReleaseSnapshotHold));
                break;

            case GrpcServerBatchType.ServerTryGetSnapshotFloor:
                item.Promise.TrySetResult(new(response.GetSnapshotFloor));
                break;

            // The peer answered the request but could not express an outcome in its payload
            // (several inter-node payloads carry only a Success/Found flag, where a false
            // would be indistinguishable from a real negative answer): no definitive answer
            // was produced, which is exactly the retryable condition.
            case GrpcServerBatchType.ServerTypeNone:
                item.Promise.TrySetException(new RpcException(new(
                    StatusCode.Unavailable, "The remote node could not answer the key-value request.")));
                break;

            default:
                item.Promise.TrySetException(new KahunaServerException("Unknown response type: " + response.Type));
                break;
        }
    }

    // streamings is process-global (static) keyed by URL; the background read loops and their
    // captured logger belong to whichever GrpcServerBatcher instance first touches a URL.
    // In production there is one GrpcInterNodeCommunication singleton so this is a non-issue.
    private List<GrpcServerSharedStreaming> GetSharedStreamingList()
    {
        Lazy<List<GrpcServerSharedStreaming>> lazyStreamings = streamings.GetOrAdd(url, static (u, self) => self.GetSharedStreamings(), this);

        return lazyStreamings.Value;
    }

    private Lazy<List<GrpcServerSharedStreaming>> GetSharedStreamings()
    {
        return new(() => CreateSharedStreamings());
    }

    private List<GrpcServerSharedStreaming> CreateSharedStreamings()
    {
        List<GrpcChannel> nodeChannels = SharedChannels.GetAllChannels(url);

        List<GrpcServerSharedStreaming> nodeStreamings = new(nodeChannels.Count);

        foreach (GrpcChannel channel in nodeChannels)
        {
            Locker.LockerClient lockClient = new(channel);
            KeyValuer.KeyValuerClient keyValueClient = new(channel);

            AsyncDuplexStreamingCall<GrpcBatchServerLockRequest, GrpcBatchServerLockResponse>? locksStreaming = lockClient.BatchServerLockRequests();
            AsyncDuplexStreamingCall<GrpcBatchServerKeyValueRequest, GrpcBatchServerKeyValueResponse>? keyValueStreaming = keyValueClient.BatchServerKeyValueRequests();

            long id = Interlocked.Increment(ref streamingId);

            _ = ReadLockMessages(url, id, locksStreaming, logger);
            _ = ReadKeyValueMessages(url, id, keyValueStreaming, logger);

            nodeStreamings.Add(new(id, locksStreaming, keyValueStreaming));
        }

        return nodeStreamings;
    }
}
