
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
/// </summary>
internal sealed class GrpcServerBatcher
{
    private static readonly ConcurrentDictionary<string, Lazy<List<GrpcServerSharedStreaming>>> streamings = new();

    private static readonly ConcurrentDictionary<int, GrpcServerBatcherItem> requestRefs = new();

    private static readonly ConcurrentDictionary<int, long> requestStreamRefs = new();

    private static int requestId;

    private static long streamingId;

    private readonly string url;

    private readonly ILogger logger;

    private readonly ConcurrentQueue<GrpcServerBatcherItem> inbox = new();

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
        inbox.Enqueue(grpcBatcherItem);

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
                    List<GrpcServerBatcherItem> messages = GrpcServerBatcherPool.Rent(2);
                    
                    while (inbox.TryDequeue(out GrpcServerBatcherItem message))
                        messages.Add(message);

                    if (messages.Count > 0)
                        await Receive(messages);

                } while (!inbox.IsEmpty);
                
            } while (Interlocked.CompareExchange(ref processing, 1, 0) != 0);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "GrpcServerBatcher DeliverMessages failed: {ExType}: {Message}", ex.GetType().Name, ex.Message);
        }
    }

    private async Task Receive(List<GrpcServerBatcherItem> requests)
    {
        await RunBatch(requests);
    }

    private async Task RunBatch(List<GrpcServerBatcherItem> requests)
    {
        try
        {
            GrpcServerSharedStreaming sharedStreaming = GetSharedStreaming();

            foreach (GrpcServerBatcherItem request in requests)
            {
                requestRefs.TryAdd(request.RequestId, request);
                requestStreamRefs[request.RequestId] = sharedStreaming.Id;

                switch (request.Type)
                {
                    case GrpcServerBatcherItemType.Locks:
                        await RunLockBatch(sharedStreaming, request);
                        break;

                    case GrpcServerBatcherItemType.KeyValues:
                        await RunKeyValueBatch(sharedStreaming, request);
                        break;

                    default:
                        throw new KahunaServerException("Unknown request type: " + request.Type);
                }
            }
        }
        catch (Exception ex)
        {
            foreach (GrpcServerBatcherItem request in requests)
            {
                requestRefs.TryRemove(request.RequestId, out _);
                requestStreamRefs.TryRemove(request.RequestId, out _);
                request.Promise.TrySetException(ex);
            }

            logger.LogError(ex, "GrpcServerBatcher RunBatch failed: {ExType}: {Message}", ex.GetType().Name, ex.Message);
        }
        finally
        {
            GrpcServerBatcherPool.Return(requests);
        }
    }

    private static void FailPendingRequests(long sharedStreamingId, Exception ex)
    {
        foreach (KeyValuePair<int, long> entry in requestStreamRefs.ToArray())
        {
            if (entry.Value != sharedStreamingId)
                continue;

            requestStreamRefs.TryRemove(entry.Key, out _);

            if (requestRefs.TryRemove(entry.Key, out GrpcServerBatcherItem item))
                item.Promise.TrySetException(ex);
        }
    }

    private static async Task RunLockBatch(GrpcServerSharedStreaming sharedStreaming, GrpcServerBatcherItem request)
    {
        GrpcBatchServerLockRequest batchRequest = new()
        {
            RequestId = request.RequestId
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

        try
        {
            await sharedStreaming.Semaphore.WaitAsync();

            await sharedStreaming.LockStreaming.RequestStream.WriteAsync(batchRequest);
        }
        finally
        {
            sharedStreaming.Semaphore.Release();
        }
    }

    private static async Task RunKeyValueBatch(GrpcServerSharedStreaming sharedStreaming, GrpcServerBatcherItem request)
    {
        GrpcBatchServerKeyValueRequest batchRequest = new()
        {
            RequestId = request.RequestId
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

        try
        {
            await sharedStreaming.Semaphore.WaitAsync();

            await sharedStreaming.KeyValueStreaming.RequestStream.WriteAsync(batchRequest);
        }
        finally
        {
            sharedStreaming.Semaphore.Release();
        }
    }
    
    private static async Task ReadLockMessages(long sharedStreamingId, AsyncDuplexStreamingCall<GrpcBatchServerLockRequest, GrpcBatchServerLockResponse> streaming, ILogger logger)
    {
        try
        {
            await foreach (GrpcBatchServerLockResponse response in streaming.ResponseStream.ReadAllAsync())
            {
                if (!requestRefs.TryRemove(response.RequestId, out GrpcServerBatcherItem item))
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

                    case GrpcLockServerBatchType.ServerTypeNone:
                    default:
                        item.Promise.TrySetException(new KahunaServerException("Unknown response type: " + response.Type));
                        break;
                }
            }

            RpcException streamClosed = new(new(StatusCode.Unavailable, "gRPC inter-node lock stream closed."));
            FailPendingRequests(sharedStreamingId, streamClosed);
        }
        catch (RpcException ex) when (ex.StatusCode is StatusCode.Unavailable or StatusCode.Cancelled)
        {
            logger.LogWarning("GrpcServerBatcher lock stream closed: {Status}", ex.Status);
            FailPendingRequests(sharedStreamingId, ex);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "GrpcServerBatcher ReadLockMessages failed: {ExType}: {Message}", ex.GetType().Name, ex.Message);
            FailPendingRequests(sharedStreamingId, ex);
        }
    }

    private static async Task ReadKeyValueMessages(long sharedStreamingId, AsyncDuplexStreamingCall<GrpcBatchServerKeyValueRequest, GrpcBatchServerKeyValueResponse> streaming, ILogger logger)
    {
        try
        {
            await foreach (GrpcBatchServerKeyValueResponse response in streaming.ResponseStream.ReadAllAsync())
            {
                if (!requestRefs.TryRemove(response.RequestId, out GrpcServerBatcherItem item))
                {
                    logger.LogWarning("GrpcServerBatcher key-value response: request not found {RequestId}", response.RequestId);
                    continue;
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

                    case GrpcServerBatchType.ServerTypeNone:
                    default:
                        item.Promise.TrySetException(new KahunaServerException("Unknown response type: " + response.Type));
                        break;
                }
            }

            RpcException streamClosed = new(new(StatusCode.Unavailable, "gRPC inter-node key-value stream closed."));
            FailPendingRequests(sharedStreamingId, streamClosed);
        }
        catch (RpcException ex) when (ex.StatusCode is StatusCode.Unavailable or StatusCode.Cancelled)
        {
            logger.LogWarning("GrpcServerBatcher key-value stream closed: {Status}", ex.Status);
            FailPendingRequests(sharedStreamingId, ex);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "GrpcServerBatcher ReadKeyValueMessages failed: {ExType}: {Message}", ex.GetType().Name, ex.Message);
            FailPendingRequests(sharedStreamingId, ex);
        }
    }

    // streamings is process-global (static) keyed by URL; the background read loops and their
    // captured logger belong to whichever GrpcServerBatcher instance first touches a URL.
    // In production there is one GrpcInterNodeCommunication singleton so this is a non-issue.
    private GrpcServerSharedStreaming GetSharedStreaming()
    {
        Lazy<List<GrpcServerSharedStreaming>> lazyStreamings = streamings.GetOrAdd(url, static (u, self) => self.GetSharedStreamings(), this);

        List<GrpcServerSharedStreaming> nodeStreamings = lazyStreamings.Value;

        return nodeStreamings[Random.Shared.Next(0, nodeStreamings.Count)];
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

            _ = ReadLockMessages(id, locksStreaming, logger);
            _ = ReadKeyValueMessages(id, keyValueStreaming, logger);

            nodeStreamings.Add(new(id, locksStreaming, keyValueStreaming));
        }

        return nodeStreamings;
    }
}
