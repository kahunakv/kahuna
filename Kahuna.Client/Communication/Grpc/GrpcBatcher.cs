
/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;
using System.Net.Security;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Grpc.Core;
using Grpc.Net.Client;
using Grpc.Net.Client.Configuration;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Locks;
using Microsoft.Extensions.Logging;

namespace Kahuna.Client.Communication;

/// <summary>
/// It tries to batch as many concurrent requests as possible to a specific host and uses gRPC bidirectional streaming to reduce the number
/// of HTTP/2 streams needed at any given time.
///
/// This approach improves throughput and reduces connection overhead by allowing multiple operations to be multiplexed
/// over a single long-lived stream, rather than opening a new stream for each request.
/// </summary>
internal sealed class GrpcBatcher
{
    /// <summary>
    /// Represents a thread-safe collection of gRPC channels categorized by a unique URL.
    /// The channels are lazily initialized and used to manage connections in a way that optimizes resource utilization.
    /// </summary>
    private static readonly ConcurrentDictionary<string, Lazy<List<GrpcChannel>>> channels = new();

    /// <summary>
    /// A thread-safe dictionary that maps a unique host URL to a lazily initialized collection of gRPC shared streaming objects.
    /// The collection allows batching and multiplexing of gRPC requests to optimize resource utilization and reduce the overhead of creating new streams.
    /// </summary>
    private static readonly ConcurrentDictionary<string, Lazy<List<GrpcSharedStreaming>>> streamings = new();

    /// <summary>
    /// Represents a thread-safe dictionary mapping unique request IDs to their corresponding batcher items.
    /// This dictionary is used to track and manage the lifecycle of batched gRPC requests.
    /// </summary>
    private static readonly ConcurrentDictionary<int, GrpcBatcherItem> requestRefs = new();

    private static readonly ConcurrentDictionary<int, long> requestStreamRefs = new();

    /// <summary>
    /// Represents a static counter used to generate unique request identifiers in a thread-safe manner.
    /// Each request processed by the GrpcBatcher is assigned an incrementing value from this counter
    /// to ensure consistent tracking of individual operations.
    /// </summary>
    private static int requestId;

    private static long streamingId;

    /// <summary>
    /// Represents the URL associated with the gRPC communication endpoint.
    /// This is used to establish a connection with the specified host for batching and streaming requests.
    /// </summary>
    private readonly string url;

    /// <summary>
    /// Default deadline applied to batched operations whose caller supplied no
    /// <see cref="CancellationToken"/>.  <see cref="TimeSpan.Zero"/> means no deadline.
    /// </summary>
    private readonly TimeSpan operationTimeout;

    /// <summary>
    /// Transport security options used when creating gRPC channels for this batcher's URL.
    /// </summary>
    private readonly KahunaOptions? securityOptions;

    private readonly ILogger? logger;

    private readonly int coalescingThreshold;
    private readonly int coalescingDelayMs;

    /// <summary>
    /// Represents a thread-safe queue used to temporarily store instances of <see cref="GrpcBatcherItem"/>
    /// for batched processing within the gRPC communication layer. The queue ensures efficient message
    /// handling and processing by maintaining the order of incoming items and supporting concurrent operations.
    /// </summary>
    private readonly ConcurrentQueue<GrpcBatcherItem> inbox = new();

    /// <summary>
    /// Indicates whether the batching process is active or idle.
    /// Used in conjunction with interlocked operations to ensure thread-safe state management during batch processing.
    /// </summary>
    private int processing = 1;

    /// <summary>
    /// Efficiently batches and processes gRPC requests to a specified host using bidirectional streaming.
    /// Reduces connection and HTTP/2 stream overhead by multiplexing multiple operations over a single stream.
    /// </summary>
    /// <param name="url">Server endpoint URL.</param>
    /// <param name="operationTimeout">
    /// Default deadline for batched ops with no caller-supplied token.
    /// <see cref="TimeSpan.Zero"/> disables the deadline (hang-forever on wedged streams).
    /// </param>
    public GrpcBatcher(string url, TimeSpan operationTimeout = default, KahunaOptions? securityOptions = null, ILogger? logger = null)
    {
        this.url = url;
        this.operationTimeout = operationTimeout;
        this.securityOptions = securityOptions;
        this.logger = logger;
        this.coalescingThreshold = securityOptions?.BatchCoalescingThreshold ?? 10;
        this.coalescingDelayMs = securityOptions?.BatchCoalescingDelayMs ?? 2;
    }

    /// <summary>
    /// Adds a new lock request to the batch processing queue and processes the queue as needed.
    /// </summary>
    /// <param name="message">The lock request message to be enqueued.</param>
    /// <returns>A task that represents the asynchronous operation, containing the batcher response for the lock request.</returns>
    public Task<GrpcBatcherResponse> Enqueue(GrpcTryLockRequest message, CancellationToken cancellationToken = default)
    {
        TaskCompletionSource<GrpcBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcBatcherItem grpcBatcherItem = new(GrpcBatcherItemType.Locks, Interlocked.Increment(ref requestId), new(message), promise, cancellationToken);

        return TryProcessQueue(grpcBatcherItem, promise, cancellationToken);
    }

    /// <summary>
    /// Adds an unlock request to the batch processing queue and initiates queue processing as needed.
    /// </summary>
    /// <param name="message">The unlock request message to be added to the queue.</param>
    /// <returns>A task representing the asynchronous operation, containing the batcher response for the unlock request.</returns>
    public Task<GrpcBatcherResponse> Enqueue(GrpcUnlockRequest message, CancellationToken cancellationToken = default)
    {
        TaskCompletionSource<GrpcBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcBatcherItem grpcBatcherItem = new(GrpcBatcherItemType.Locks, Interlocked.Increment(ref requestId), new(message), promise, cancellationToken);

        return TryProcessQueue(grpcBatcherItem, promise, cancellationToken);
    }

    /// <summary>
    /// Adds a lock extension request to the batch queue for processing and handles the request as necessary.
    /// </summary>
    /// <param name="message">The lock extension request to be enqueued.</param>
    /// <returns>A task that represents the asynchronous operation, containing the batcher response for the lock extension request.</returns>
    public Task<GrpcBatcherResponse> Enqueue(GrpcExtendLockRequest message, CancellationToken cancellationToken = default)
    {
        TaskCompletionSource<GrpcBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcBatcherItem grpcBatcherItem = new(GrpcBatcherItemType.Locks, Interlocked.Increment(ref requestId), new(message), promise, cancellationToken);

        return TryProcessQueue(grpcBatcherItem, promise, cancellationToken);
    }

    /// <summary>
    /// Adds a lock request to the batch processing queue and processes the queue as needed.
    /// </summary>
    /// <param name="message">The lock request message to be enqueued.</param>
    /// <returns>A task that represents the asynchronous operation, containing the batcher response for the lock request.</returns>
    public Task<GrpcBatcherResponse> Enqueue(GrpcGetLockRequest message, CancellationToken cancellationToken = default)
    {
        TaskCompletionSource<GrpcBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcBatcherItem grpcBatcherItem = new(GrpcBatcherItemType.Locks, Interlocked.Increment(ref requestId), new(message), promise, cancellationToken);

        return TryProcessQueue(grpcBatcherItem, promise, cancellationToken);
    }
    
    /// <summary>
    /// Adds a key-value set request to the batch queue and processes it accordingly.
    /// </summary>
    /// <param name="message">The key-value set request to be enqueued.</param>
    /// <returns>A task representing the asynchronous operation, containing the response for the processed request.</returns>
    public Task<GrpcBatcherResponse> Enqueue(GrpcTrySetKeyValueRequest message, CancellationToken cancellationToken = default)
    {
        TaskCompletionSource<GrpcBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcBatcherItem grpcBatcherItem = new(GrpcBatcherItemType.KeyValues, Interlocked.Increment(ref requestId), new(message), promise, cancellationToken);

        return TryProcessQueue(grpcBatcherItem, promise, cancellationToken);
    }

    /// <summary>
    /// Adds a key-value set request to the batch queue and processes it accordingly.
    /// </summary>
    /// <param name="message">The key-value set request to be enqueued.</param>
    /// <returns>A task representing the asynchronous operation, containing the response for the processed request.</returns>
    public Task<GrpcBatcherResponse> Enqueue(GrpcTrySetManyKeyValueRequest message, CancellationToken cancellationToken = default)
    {
        TaskCompletionSource<GrpcBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcBatcherItem grpcBatcherItem = new(GrpcBatcherItemType.KeyValues, Interlocked.Increment(ref requestId), new(message), promise, cancellationToken);

        return TryProcessQueue(grpcBatcherItem, promise, cancellationToken);
    }

    public Task<GrpcBatcherResponse> Enqueue(GrpcTryDeleteManyKeyValueRequest message, CancellationToken cancellationToken = default)
    {
        TaskCompletionSource<GrpcBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcBatcherItem grpcBatcherItem = new(GrpcBatcherItemType.KeyValues, Interlocked.Increment(ref requestId), new(message), promise, cancellationToken);

        return TryProcessQueue(grpcBatcherItem, promise, cancellationToken);
    }

    /// <summary>
    /// Adds a key-value request to the batch processing queue and processes the queue as needed.
    /// </summary>
    /// <param name="message">The key-value request message to be enqueued.</param>
    /// <returns>A task representing the asynchronous operation, containing the batcher response for the key-value request.</returns>
    public Task<GrpcBatcherResponse> Enqueue(GrpcTryGetKeyValueRequest message, CancellationToken cancellationToken = default)
    {
        TaskCompletionSource<GrpcBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcBatcherItem grpcBatcherItem = new(GrpcBatcherItemType.KeyValues, Interlocked.Increment(ref requestId), new(message), promise, cancellationToken);

        return TryProcessQueue(grpcBatcherItem, promise, cancellationToken);
    }
    
    public Task<GrpcBatcherResponse> Enqueue(GrpcTryDeleteKeyValueRequest message, CancellationToken cancellationToken = default)
    {
        TaskCompletionSource<GrpcBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcBatcherItem grpcBatcherItem = new(GrpcBatcherItemType.KeyValues, Interlocked.Increment(ref requestId), new(message), promise, cancellationToken);

        return TryProcessQueue(grpcBatcherItem, promise, cancellationToken);
    }
    
    public Task<GrpcBatcherResponse> Enqueue(GrpcTryExtendKeyValueRequest message, CancellationToken cancellationToken = default)
    {
        TaskCompletionSource<GrpcBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcBatcherItem grpcBatcherItem = new(GrpcBatcherItemType.KeyValues, Interlocked.Increment(ref requestId), new(message), promise, cancellationToken);

        return TryProcessQueue(grpcBatcherItem, promise, cancellationToken);
    }
    
    public Task<GrpcBatcherResponse> Enqueue(GrpcTryExistsKeyValueRequest message, CancellationToken cancellationToken = default)
    {
        TaskCompletionSource<GrpcBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcBatcherItem grpcBatcherItem = new(GrpcBatcherItemType.KeyValues, Interlocked.Increment(ref requestId), new(message), promise, cancellationToken);

        return TryProcessQueue(grpcBatcherItem, promise, cancellationToken);
    }
    
    public Task<GrpcBatcherResponse> Enqueue(GrpcTryExecuteTransactionScriptRequest message, CancellationToken cancellationToken = default)
    {
        TaskCompletionSource<GrpcBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcBatcherItem grpcBatcherItem = new(GrpcBatcherItemType.KeyValues, Interlocked.Increment(ref requestId), new(message), promise, cancellationToken);

        return TryProcessQueue(grpcBatcherItem, promise, cancellationToken);
    }
    
    public Task<GrpcBatcherResponse> Enqueue(GrpcTryAcquireExclusiveLockRequest message, CancellationToken cancellationToken = default)
    {
        TaskCompletionSource<GrpcBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcBatcherItem grpcBatcherItem = new(GrpcBatcherItemType.KeyValues, Interlocked.Increment(ref requestId), new(message), promise, cancellationToken);

        return TryProcessQueue(grpcBatcherItem, promise, cancellationToken);
    }
    
    public Task<GrpcBatcherResponse> Enqueue(GrpcGetByBucketRequest message, CancellationToken cancellationToken = default)
    {
        TaskCompletionSource<GrpcBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcBatcherItem grpcBatcherItem = new(GrpcBatcherItemType.KeyValues, Interlocked.Increment(ref requestId), new(message), promise, cancellationToken);

        return TryProcessQueue(grpcBatcherItem, promise, cancellationToken);
    }
    
    public Task<GrpcBatcherResponse> Enqueue(GrpcScanAllByPrefixRequest message, CancellationToken cancellationToken = default)
    {
        TaskCompletionSource<GrpcBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcBatcherItem grpcBatcherItem = new(GrpcBatcherItemType.KeyValues, Interlocked.Increment(ref requestId), new(message), promise, cancellationToken);

        return TryProcessQueue(grpcBatcherItem, promise, cancellationToken);
    }

    public Task<GrpcBatcherResponse> Enqueue(GrpcStartTransactionRequest message, CancellationToken cancellationToken = default)
    {
        TaskCompletionSource<GrpcBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcBatcherItem grpcBatcherItem = new(GrpcBatcherItemType.KeyValues, Interlocked.Increment(ref requestId), new(message), promise, cancellationToken);

        return TryProcessQueue(grpcBatcherItem, promise, cancellationToken);
    }
    
    public Task<GrpcBatcherResponse> Enqueue(GrpcCommitTransactionRequest message, CancellationToken cancellationToken = default)
    {
        TaskCompletionSource<GrpcBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcBatcherItem grpcBatcherItem = new(GrpcBatcherItemType.KeyValues, Interlocked.Increment(ref requestId), new(message), promise, cancellationToken);

        return TryProcessQueue(grpcBatcherItem, promise, cancellationToken);
    }
    
    public Task<GrpcBatcherResponse> Enqueue(GrpcRollbackTransactionRequest message, CancellationToken cancellationToken = default)
    {
        TaskCompletionSource<GrpcBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcBatcherItem grpcBatcherItem = new(GrpcBatcherItemType.KeyValues, Interlocked.Increment(ref requestId), new(message), promise, cancellationToken);

        return TryProcessQueue(grpcBatcherItem, promise, cancellationToken);
    }

    private Task<GrpcBatcherResponse> TryProcessQueue(GrpcBatcherItem grpcBatcherItem, TaskCompletionSource<GrpcBatcherResponse> promise, CancellationToken cancellationToken)
    {
        // CT3: when the caller passes no token, apply the configured default deadline so that a
        // wedged (non-faulting) stream doesn't hang the caller forever.  This reuses the CT1
        // cleanup path — the deadline CTS fires the same callback that removes refs and cancels
        // the promise.  The CTS is disposed when the promise completes (normal, faulted, or
        // cancelled).  Server-side cancellation is not propagated (best-effort local abandon).
        CancellationTokenSource? deadlineCts = null;
        if (!cancellationToken.CanBeCanceled && operationTimeout > TimeSpan.Zero)
        {
            deadlineCts = new(operationTimeout);
            cancellationToken = deadlineCts.Token;
            // Rebuild the item so that CT2's Semaphore.WaitAsync(request.CancellationToken) also
            // observes the deadline — GrpcBatcherItem is a readonly struct so we reassign.
            grpcBatcherItem = new(grpcBatcherItem.Type, grpcBatcherItem.RequestId, grpcBatcherItem.Request, grpcBatcherItem.Promise, cancellationToken);
        }

        inbox.Enqueue(grpcBatcherItem);

        if (cancellationToken.CanBeCanceled)
        {
            // Register a local-cleanup callback so that cancelling the caller's token (or the
            // deadline firing) removes the item from the tracking dicts and cancels the promise.
            // The registration AND the deadline CTS (if any) are disposed when the promise
            // completes to release all resources promptly.
            CancellationTokenRegistration reg = cancellationToken.Register(static state =>
            {
                (GrpcBatcherItem item, CancellationToken ct) = ((GrpcBatcherItem, CancellationToken))state!;
                requestRefs.TryRemove(item.RequestId, out _);
                requestStreamRefs.TryRemove(item.RequestId, out _);
                item.Promise.TrySetCanceled(ct);
            }, (grpcBatcherItem, cancellationToken));

            promise.Task.ContinueWith(
                static (_, state) =>
                {
                    (CancellationTokenRegistration reg, CancellationTokenSource? cts) = ((CancellationTokenRegistration, CancellationTokenSource?))state!;
                    reg.Dispose();
                    cts?.Dispose();
                },
                (reg, deadlineCts),
                CancellationToken.None,
                TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default);
        }

        if (1 == Interlocked.Exchange(ref processing, 0))
            _ = DeliverMessages();

        return promise.Task;
    }

    /// <summary>
    /// It retrieves a message from the inbox and invokes the actor by passing one message 
    /// at a time until the pending message list is cleared.
    /// </summary>
    /// <returns></returns>
    private async Task DeliverMessages()
    {
        try
        {
            do
            {
                do
                {
                    List<GrpcBatcherItem> messages = GrpcBatcherPool.Rent(2);
                    
                    while (inbox.TryDequeue(out GrpcBatcherItem message))
                        messages.Add(message);

                    if (messages.Count > 0)
                        await Receive(messages);

                } while (!inbox.IsEmpty);
                
            } while (Interlocked.CompareExchange(ref processing, 1, 0) != 0);
        }
        catch (Exception ex)
        {
            logger?.LogError(ex, "GrpcBatcher ({Url}): unexpected exception in batch dispatch loop", url);
        }
    }

    private async Task Receive(List<GrpcBatcherItem> requests)
    {
        await RunBatch(requests);

        if (coalescingThreshold > 1 && requests.Count < coalescingThreshold && coalescingDelayMs > 0)
            await Task.Delay(Random.Shared.Next(1, coalescingDelayMs + 1));
    }

    /// <summary>
    /// Number of in-flight requests currently tracked in <see cref="requestRefs"/>.
    /// Exposed internal for test assertions; not for production use.
    /// </summary>
    internal static int PendingRequestCount => requestRefs.Count;

    /// <summary>
    /// Replaces the streaming pool for <paramref name="url"/> with a single pre-built
    /// <see cref="GrpcSharedStreaming"/>. Call <see cref="RemoveTestSharedStreaming"/> in test
    /// teardown. For test use only — never call in production.
    /// </summary>
    internal static void InjectTestSharedStreaming(string url, GrpcSharedStreaming streaming)
    {
        streamings[url] = new Lazy<List<GrpcSharedStreaming>>(() => [streaming]);
    }

    /// <summary>Removes the injected test streaming for <paramref name="url"/>.</summary>
    internal static void RemoveTestSharedStreaming(string url)
    {
        streamings.TryRemove(url, out _);
    }

    /// <summary>
    /// Processes a batch of requests, delegating them to specific handling mechanisms based on their type.
    /// </summary>
    /// <param name="requests">The list of batcher items to be processed.</param>
    /// <returns>A task that represents the asynchronous batch processing operation.</returns>
    internal async Task RunBatch(List<GrpcBatcherItem> requests)
    {
        try
        {
            // Skip the entire network path when every item was cancelled before dispatch
            // (e.g. a synchronously pre-cancelled token fires the callback in TryProcessQueue
            // before DeliverMessages picks the item up).
            bool anyPending = false;
            foreach (GrpcBatcherItem r in requests)
            {
                if (!r.Promise.Task.IsCompleted) { anyPending = true; break; }
            }
            if (!anyPending)
                return;

            for (int attempt = 0; attempt < 2; attempt++)
            {
                try
                {
                    GrpcSharedStreaming sharedStreaming = GetSharedStreaming();

                    foreach (GrpcBatcherItem request in requests)
                    {
                        // Skip items whose promise was completed between inbox drain and now
                        // (cancelled mid-flight or synchronously pre-cancelled).
                        if (request.Promise.Task.IsCompleted)
                            continue;

                        requestRefs.TryAdd(request.RequestId, request);
                        requestStreamRefs[request.RequestId] = sharedStreaming.Id;

                        try
                        {
                            switch (request.Type)
                            {
                                case GrpcBatcherItemType.Locks:
                                    await RunLocksBatch(sharedStreaming, request);
                                    break;

                                case GrpcBatcherItemType.KeyValues:
                                    await RunKeyValueBatch(sharedStreaming, request);
                                    break;

                                case GrpcBatcherItemType.Sequences:
                                default:
                                    throw new KahunaException("Unknown batch type", LockResponseType.Errored);
                            }
                        }
                        catch (OperationCanceledException)
                        {
                            // This request's token fired during its own semaphore wait. CT1's
                            // callback already cancelled its promise and removed its refs; the
                            // TrySetCanceled/TryRemove calls below are no-ops in that case.
                            // When RunBatch is called directly (e.g. in tests) without going
                            // through TryProcessQueue the CT1 registration is absent, so we must
                            // set the promise here to avoid leaving it permanently pending.
                            requestRefs.TryRemove(request.RequestId, out _);
                            requestStreamRefs.TryRemove(request.RequestId, out _);
                            request.Promise.TrySetCanceled(request.CancellationToken);
                        }
                    }

                    return;
                }
                catch (RpcException ex) when (ex.StatusCode is StatusCode.Unavailable or StatusCode.Cancelled)
                {
                    InvalidateSharedConnections(MakeCacheKey(url, securityOptions));
                    RemoveRequestRefs(requests);

                    if (attempt == 0 && requests.Count == 1)
                        continue;

                    FailRequests(requests, ex);
                    return;
                }
                catch (Exception ex)
                {
                    RemoveRequestRefs(requests);
                    FailRequests(requests, ex);
                    return;
                }
            }
        }
        finally
        {
            GrpcBatcherPool.Return(requests);
        }
    }

    /// <summary>
    /// Processes a batch of lock-related requests and sends them to the shared streaming service.
    /// </summary>
    /// <param name="sharedStreaming">The streaming client used for sending lock batch requests.</param>
    /// <param name="request">The batcher item containing the lock-related request information.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    /// <exception cref="KahunaException">Thrown when the request type is unknown or invalid.</exception>
    private static async Task RunLocksBatch(GrpcSharedStreaming sharedStreaming, GrpcBatcherItem request)
    {
        GrpcBatchClientLockRequest batchRequest = new()
        {
            RequestId = request.RequestId
        };
        
        GrpcBatcherRequest itemRequest = request.Request;

        if (itemRequest.TryLock is not null)
        {
            batchRequest.Type = GrpcLockClientBatchType.TypeTryLock;
            batchRequest.TryLock = itemRequest.TryLock;
        } 
        else if (itemRequest.Unlock is not null)
        {
            batchRequest.Type = GrpcLockClientBatchType.TypeUnlock;
            batchRequest.Unlock = itemRequest.Unlock;
        }
        else if (itemRequest.ExtendLock is not null)
        {
            batchRequest.Type = GrpcLockClientBatchType.TypeExtendLock;
            batchRequest.ExtendLock = itemRequest.ExtendLock;
        }
        else if (itemRequest.GetLock is not null)
        {
            batchRequest.Type = GrpcLockClientBatchType.TypeGetLock;
            batchRequest.GetLock = itemRequest.GetLock;
        }
        else        
            throw new KahunaException("Unknown request type", LockResponseType.Errored);        

        bool lockTaken = false;

        try
        {
            await sharedStreaming.Semaphore.WaitAsync(request.CancellationToken);
            lockTaken = true;

            await sharedStreaming.LockStreaming.RequestStream.WriteAsync(batchRequest);
        }
        finally
        {
            if (lockTaken)
            {
                try
                {
                    sharedStreaming.Semaphore.Release();
                }
                catch (ObjectDisposedException)
                {
                }
            }
        }
    }

    /// <summary>
    /// Processes a batch of key-value operations based on the request type and sends it through the shared streaming instance.
    /// </summary>
    /// <param name="sharedStreaming">The shared streaming instance for sending the batched request.</param>
    /// <param name="request">The batcher item containing the key-value request details to be processed.</param>
    /// <returns>A task that represents the asynchronous operation of processing the key-value batch.</returns>
    /// <exception cref="KahunaException">Thrown if the request type is unknown or invalid.</exception>
    private static async Task RunKeyValueBatch(GrpcSharedStreaming sharedStreaming, GrpcBatcherItem request)
    {
        GrpcBatchClientKeyValueRequest batchRequest = new()
        {
            RequestId = request.RequestId
        };

        GrpcBatcherRequest itemRequest = request.Request;

        if (itemRequest.TrySetKeyValue is not null)
        {
            batchRequest.Type = GrpcClientBatchType.TrySetKeyValue;
            batchRequest.TrySetKeyValue = itemRequest.TrySetKeyValue;
        }
        else if (itemRequest.TrySetManyKeyValues is not null)
        {
            batchRequest.Type = GrpcClientBatchType.TrySetManyKeyValue;
            batchRequest.TrySetManyKeyValue = itemRequest.TrySetManyKeyValues;
        }
        else if (itemRequest.TryDeleteManyKeyValues is not null)
        {
            batchRequest.Type = GrpcClientBatchType.TryDeleteManyKeyValue;
            batchRequest.TryDeleteManyKeyValue = itemRequest.TryDeleteManyKeyValues;
        }
        else if (itemRequest.TryGetKeyValue is not null)
        {
            batchRequest.Type = GrpcClientBatchType.TryGetKeyValue;
            batchRequest.TryGetKeyValue = itemRequest.TryGetKeyValue;
        }
        else if (itemRequest.TryDeleteKeyValue is not null)
        {
            batchRequest.Type = GrpcClientBatchType.TryDeleteKeyValue;
            batchRequest.TryDeleteKeyValue = itemRequest.TryDeleteKeyValue;
        } 
        else if (itemRequest.TryExtendKeyValue is not null)
        {
            batchRequest.Type = GrpcClientBatchType.TryExtendKeyValue;
            batchRequest.TryExtendKeyValue = itemRequest.TryExtendKeyValue;
        } 
        else if (itemRequest.TryExistsKeyValue is not null)
        {
            batchRequest.Type = GrpcClientBatchType.TryExistsKeyValue;
            batchRequest.TryExistsKeyValue = itemRequest.TryExistsKeyValue;
        }
        else if (itemRequest.TryAcquireExclusiveLock is not null)
        {
            batchRequest.Type = GrpcClientBatchType.TryAcquireExclusiveLock;
            batchRequest.TryAcquireExclusiveLock = itemRequest.TryAcquireExclusiveLock;
        }
        else if (itemRequest.TryExecuteTransactionScript is not null)
        {
            batchRequest.Type = GrpcClientBatchType.TryExecuteTransactionScript;
            batchRequest.TryExecuteTransactionScript = itemRequest.TryExecuteTransactionScript;
        }
        else if (itemRequest.GetByBucket is not null)
        {
            batchRequest.Type = GrpcClientBatchType.TryGetByBucket;
            batchRequest.GetByBucket = itemRequest.GetByBucket;
        }
        else if (itemRequest.ScanByPrefix is not null)
        {
            batchRequest.Type = GrpcClientBatchType.TryScanByPrefix;
            batchRequest.ScanByPrefix = itemRequest.ScanByPrefix;
        }
        else if (itemRequest.StartTransaction is not null)
        {
            batchRequest.Type = GrpcClientBatchType.TryStartTransaction;
            batchRequest.StartTransaction = itemRequest.StartTransaction;
        }
        else if (itemRequest.CommitTransaction is not null)
        {
            batchRequest.Type = GrpcClientBatchType.TryCommitTransaction;
            batchRequest.CommitTransaction = itemRequest.CommitTransaction;
        }
        else if (itemRequest.RollbackTransaction is not null)
        {
            batchRequest.Type = GrpcClientBatchType.TryRollbackTransaction;
            batchRequest.RollbackTransaction = itemRequest.RollbackTransaction;
        }
        else        
            throw new KahunaException("Unknown request type", KeyValueResponseType.Errored);        

        bool lockTaken = false;

        try
        {
            await sharedStreaming.Semaphore.WaitAsync(request.CancellationToken);
            lockTaken = true;

            await sharedStreaming.KeyValueStreaming.RequestStream.WriteAsync(batchRequest);
        }
        finally
        {
            if (lockTaken)
            {
                try
                {
                    sharedStreaming.Semaphore.Release();
                }
                catch (ObjectDisposedException)
                {
                }
            }
        }
    }

    /// <summary>
    /// Reads key-value response messages from the provided streaming call and processes them asynchronously.
    /// </summary>
    /// <param name="streaming">The asynchronous duplex streaming call that delivers key-value request and response messages.</param>
    /// <returns>A task that represents the asynchronous operation of reading and processing the key-value messages.</returns>
    private static async Task ReadKeyValueMessages(string cacheKey, long sharedStreamingId, AsyncDuplexStreamingCall<GrpcBatchClientKeyValueRequest, GrpcBatchClientKeyValueResponse> streaming)
    {
        try
        {
            await foreach (GrpcBatchClientKeyValueResponse response in streaming.ResponseStream.ReadAllAsync())
            {
                // TryRemove wins the race against the cancellation callback's TryRemove; whoever
                // removes the item first gets to complete the promise via TrySetResult/TrySetCanceled.
                if (!requestRefs.TryRemove(response.RequestId, out GrpcBatcherItem item))
                    continue;

                requestStreamRefs.TryRemove(response.RequestId, out _);

                switch (response.Type)
                {
                    case GrpcClientBatchType.TrySetKeyValue:
                        item.Promise.TrySetResult(new(response.TrySetKeyValue));
                        break;

                    case GrpcClientBatchType.TrySetManyKeyValue:
                        item.Promise.TrySetResult(new(response.TrySetManyKeyValue));
                        break;

                    case GrpcClientBatchType.TryDeleteManyKeyValue:
                        item.Promise.TrySetResult(new(response.TryDeleteManyKeyValue));
                        break;

                    case GrpcClientBatchType.TryGetKeyValue:
                        item.Promise.TrySetResult(new(response.TryGetKeyValue));
                        break;

                    case GrpcClientBatchType.TryDeleteKeyValue:
                        item.Promise.TrySetResult(new(response.TryDeleteKeyValue));
                        break;

                    case GrpcClientBatchType.TryExtendKeyValue:
                        item.Promise.TrySetResult(new(response.TryExtendKeyValue));
                        break;

                    case GrpcClientBatchType.TryExistsKeyValue:
                        item.Promise.TrySetResult(new(response.TryExistsKeyValue));
                        break;

                    case GrpcClientBatchType.TryAcquireExclusiveLock:
                        item.Promise.TrySetResult(new(response.TryAcquireExclusiveLock));
                        break;

                    case GrpcClientBatchType.TryExecuteTransactionScript:
                        item.Promise.TrySetResult(new(response.TryExecuteTransactionScript));
                        break;

                    case GrpcClientBatchType.TryGetByBucket:
                        item.Promise.TrySetResult(new(response.GetByBucket));
                        break;

                    case GrpcClientBatchType.TryScanByPrefix:
                        item.Promise.TrySetResult(new(response.ScanByPrefix));
                        break;

                    case GrpcClientBatchType.TryStartTransaction:
                        item.Promise.TrySetResult(new(response.StartTransaction));
                        break;

                    case GrpcClientBatchType.TryCommitTransaction:
                        item.Promise.TrySetResult(new(response.CommitTransaction));
                        break;

                    case GrpcClientBatchType.TryRollbackTransaction:
                        item.Promise.TrySetResult(new(response.RollbackTransaction));
                        break;

                    case GrpcClientBatchType.TypeNone:
                    default:
                        item.Promise.TrySetException(new KahunaException("Unknown response type: " + response.Type, KeyValueResponseType.Errored));
                        break;
                }
            }

            RpcException ex = new(new(StatusCode.Unavailable, "gRPC key-value stream closed."));
            InvalidateSharedConnections(cacheKey);
            FailPendingRequests(sharedStreamingId, ex);
        }
        catch (RpcException ex) when (ex.StatusCode is StatusCode.Unavailable or StatusCode.Cancelled)
        {
            InvalidateSharedConnections(cacheKey);
            FailPendingRequests(sharedStreamingId, ex);
        }
    }

    /// <summary>
    /// Reads lock messages asynchronously from a duplex streaming call, processes the responses,
    /// and resolves or rejects associated tasks based on the response type.
    /// </summary>
    /// <param name="streaming">The asynchronous duplex streaming call containing lock request and response messages.</param>
    private static async Task ReadLockMessages(string cacheKey, long sharedStreamingId, AsyncDuplexStreamingCall<GrpcBatchClientLockRequest, GrpcBatchClientLockResponse> streaming)
    {
        try
        {
            await foreach (GrpcBatchClientLockResponse response in streaming.ResponseStream.ReadAllAsync())
            {
                if (!requestRefs.TryRemove(response.RequestId, out GrpcBatcherItem item))
                    continue;

                requestStreamRefs.TryRemove(response.RequestId, out _);

                switch (response.Type)
                {
                    case GrpcLockClientBatchType.TypeTryLock:
                        item.Promise.TrySetResult(new(response.TryLock));
                        break;

                    case GrpcLockClientBatchType.TypeUnlock:
                        item.Promise.TrySetResult(new(response.Unlock));
                        break;

                    case GrpcLockClientBatchType.TypeExtendLock:
                        item.Promise.TrySetResult(new(response.ExtendLock));
                        break;

                    case GrpcLockClientBatchType.TypeGetLock:
                        item.Promise.TrySetResult(new(response.GetLock));
                        break;

                    case GrpcLockClientBatchType.TypeNone:
                    default:
                        item.Promise.TrySetException(new KahunaException("Unknown response type: " + response.Type, LockResponseType.Errored));
                        break;
                }
            }

            RpcException ex = new(new(StatusCode.Unavailable, "gRPC lock stream closed."));
            InvalidateSharedConnections(cacheKey);
            FailPendingRequests(sharedStreamingId, ex);
        }
        catch (RpcException ex) when (ex.StatusCode is StatusCode.Unavailable or StatusCode.Cancelled)
        {
            InvalidateSharedConnections(cacheKey);
            FailPendingRequests(sharedStreamingId, ex);
        }
    }

    /// <summary>
    /// Retrieves a shared gRPC channel for the specified URL from the shared channel pool.
    /// </summary>
    /// <param name="url">The URL for which the shared gRPC channel is requested.</param>
    /// <returns>A shared gRPC channel corresponding to the specified URL.</returns>
    /// <summary>
    /// Derives a dict cache key that encodes the server URL, TLS security policy, and channel pool
    /// size so that callers with different configurations for the same URL never share channel pools.
    /// </summary>
    internal static string MakeCacheKey(string url, KahunaOptions? opts)
    {
        if (opts is null) return url;

        int poolSize = Math.Max(1, opts.GrpcChannelPoolSize);
        bool isDefault = poolSize == 2
            && !opts.AllowInsecureCertificateValidation
            && opts.TrustedServerCertificateThumbprints.Count == 0;

        if (isDefault) return url;

        string poolSuffix = poolSize != 2 ? $"\0pool:{poolSize}" : "";

        string tlsSuffix;
        if (opts.AllowInsecureCertificateValidation)
            tlsSuffix = "\0insecure";
        else if (opts.TrustedServerCertificateThumbprints.Count > 0)
        {
            string pins = string.Join(",", opts.TrustedServerCertificateThumbprints
                .Select(t => t.ToUpperInvariant())
                .OrderBy(t => t, StringComparer.Ordinal));
            tlsSuffix = $"\0pin:{pins}";
        }
        else
            tlsSuffix = "";

        return $"{url}{poolSuffix}{tlsSuffix}";
    }

    public static GrpcChannel GetSharedChannel(string url, KahunaOptions? opts = null)
    {
        string cacheKey = MakeCacheKey(url, opts);
        Lazy<List<GrpcChannel>> lazyChannels = channels.GetOrAdd(
            cacheKey,
            static (_, arg) => new(() => CreateSharedChannels(arg.url, arg.opts)),
            (url, opts));

        List<GrpcChannel> nodeChannels = lazyChannels.Value;

        return nodeChannels[Random.Shared.Next(0, nodeChannels.Count)];
    }

    private GrpcSharedStreaming GetSharedStreaming()
    {
        string cacheKey = MakeCacheKey(url, securityOptions);
        Lazy<List<GrpcSharedStreaming>> lazyStreamings = streamings.GetOrAdd(
            cacheKey,
            static (_, arg) => new(() => CreateSharedStreamings(arg.url, arg.cacheKey, arg.opts)),
            (url, cacheKey, opts: securityOptions));

        List<GrpcSharedStreaming> nodeStreamings = lazyStreamings.Value;

        return nodeStreamings[Random.Shared.Next(0, nodeStreamings.Count)];
    }

    private static void InvalidateSharedConnections(string cacheKey)
    {
        if (streamings.TryRemove(cacheKey, out Lazy<List<GrpcSharedStreaming>>? lazyStreamings) && lazyStreamings.IsValueCreated)
        {
            foreach (GrpcSharedStreaming streaming in lazyStreamings.Value)
                streaming.Dispose();
        }

        if (channels.TryRemove(cacheKey, out Lazy<List<GrpcChannel>>? lazyChannels) && lazyChannels.IsValueCreated)
        {
            foreach (GrpcChannel channel in lazyChannels.Value)
                channel.Dispose();
        }
    }

    private static void FailPendingRequests(long sharedStreamingId, RpcException ex)
    {
        foreach (KeyValuePair<int, long> requestStreamRef in requestStreamRefs.ToArray())
        {
            if (requestStreamRef.Value != sharedStreamingId)
                continue;

            requestStreamRefs.TryRemove(requestStreamRef.Key, out _);

            if (requestRefs.TryRemove(requestStreamRef.Key, out GrpcBatcherItem item))
                item.Promise.TrySetException(ex);
        }
    }

    private static void RemoveRequestRefs(List<GrpcBatcherItem> requests)
    {
        foreach (GrpcBatcherItem request in requests)
        {
            requestRefs.TryRemove(request.RequestId, out _);
            requestStreamRefs.TryRemove(request.RequestId, out _);
        }
    }

    private static void FailRequests(List<GrpcBatcherItem> requests, Exception ex)
    {
        foreach (GrpcBatcherItem request in requests)
            request.Promise.TrySetException(ex);
    }

    private static List<GrpcSharedStreaming> CreateSharedStreamings(string url, string cacheKey, KahunaOptions? opts)
    {
        Lazy<List<GrpcChannel>> lazyChannels = channels.GetOrAdd(
            cacheKey,
            static (_, arg) => new(() => CreateSharedChannels(arg.url, arg.opts)),
            (url, opts));

        List<GrpcChannel> nodeChannels = lazyChannels.Value;

        List<GrpcSharedStreaming> nodeStreamings = new(nodeChannels.Count);

        foreach (GrpcChannel channel in nodeChannels)
        {
            Locker.LockerClient lockClient = new(channel);
            KeyValuer.KeyValuerClient keyValueClient = new(channel);

            AsyncDuplexStreamingCall<GrpcBatchClientLockRequest, GrpcBatchClientLockResponse>? lockStreaming = lockClient.BatchClientLockRequests();
            AsyncDuplexStreamingCall<GrpcBatchClientKeyValueRequest, GrpcBatchClientKeyValueResponse>? keyValueStreaming = keyValueClient.BatchClientKeyValueRequests();

            long id = Interlocked.Increment(ref streamingId);

            _ = ReadLockMessages(cacheKey, id, lockStreaming);
            _ = ReadKeyValueMessages(cacheKey, id, keyValueStreaming);

            nodeStreamings.Add(new(id, lockStreaming, keyValueStreaming));
        }

        return nodeStreamings;
    }

    private static List<GrpcChannel> CreateSharedChannels(string url, KahunaOptions? opts)
    {
        int poolSize = Math.Max(1, opts?.GrpcChannelPoolSize ?? 2);
        List<GrpcChannel> urlChannels = new(poolSize);

        for (int i = 0; i < poolSize; i++)
            urlChannels.Add(CreateChannelInternal(url, opts));

        return urlChannels;
    }

    /// <summary>
    /// Returns a <see cref="RemoteCertificateValidationCallback"/> appropriate for the supplied
    /// options: <see langword="null"/> for standard OS chain validation (the default),
    /// always-true for insecure dev mode, or a SHA-256 thumbprint-pinning callback.
    /// </summary>
    internal static RemoteCertificateValidationCallback? BuildCertValidationCallback(KahunaOptions? opts)
    {
        if (opts is null || (!opts.AllowInsecureCertificateValidation && opts.TrustedServerCertificateThumbprints.Count == 0))
            return null;

        if (opts.AllowInsecureCertificateValidation)
            return static (_, _, _, _) => true;

        IReadOnlyList<string> thumbprints = opts.TrustedServerCertificateThumbprints;
        return (_, certificate, _, _) =>
        {
            if (certificate is null) return false;
            byte[] hash = SHA256.HashData(certificate.GetRawCertData());
            string thumbprint = Convert.ToHexString(hash);
            return thumbprints.Any(t => string.Equals(t, thumbprint, StringComparison.OrdinalIgnoreCase));
        };
    }

    private static GrpcChannel CreateChannelInternal(string url, KahunaOptions? opts)
    {
        SslClientAuthenticationOptions sslOptions = new()
        {
            RemoteCertificateValidationCallback = BuildCertValidationCallback(opts)
        };

        SocketsHttpHandler handler = new()
        {
            SslOptions = sslOptions,
            ConnectTimeout = TimeSpan.FromSeconds(10),
            PooledConnectionIdleTimeout = Timeout.InfiniteTimeSpan,
            KeepAlivePingDelay = TimeSpan.FromSeconds(30),
            KeepAlivePingTimeout = TimeSpan.FromSeconds(10),
            EnableMultipleHttp2Connections = true,            
        };
        
        MethodConfig defaultMethodConfig = new()
        {
            Names = { MethodName.Default },
            RetryPolicy = new RetryPolicy
            {
                MaxAttempts = 5,
                InitialBackoff = TimeSpan.FromSeconds(1),
                MaxBackoff = TimeSpan.FromSeconds(5),
                BackoffMultiplier = 1.5,
                RetryableStatusCodes = { StatusCode.Unavailable }
            }
        };

        return GrpcChannel.ForAddress(url, new()
        {
            HttpHandler = handler,
            ServiceConfig = new() { MethodConfigs = { defaultMethodConfig } }
        });
    }
}
