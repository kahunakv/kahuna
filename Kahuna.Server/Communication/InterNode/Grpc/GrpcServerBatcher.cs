
using System.Collections.Concurrent;

using Grpc.Core;
using Grpc.Net.Client;
using Kommander.Communication.Grpc;

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
    
    private static int requestId;

    private readonly string url;
    
    private readonly ConcurrentQueue<GrpcServerBatcherItem> inbox = new();
    
    private int processing = 1;

    public GrpcServerBatcher(string url)
    {
        this.url = url;
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
    
    public Task<GrpcServerBatcherResponse> Enqueue(GrpcTryGetKeyValueRequest message)
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
    
    public Task<GrpcServerBatcherResponse> Enqueue(GrpcTryCommitManyMutationsRequest message)
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
                    List<GrpcServerBatcherItem> messages = [];
                    
                    while (inbox.TryDequeue(out GrpcServerBatcherItem? message))
                        messages.Add(message);

                    if (messages.Count > 0)
                        await Receive(messages);

                } while (!inbox.IsEmpty);
                
            } while (Interlocked.CompareExchange(ref processing, 1, 0) != 0);
        }
        catch (Exception ex)
        {
            Console.WriteLine("Exception: " + ex.Message);
            //manager.Logger.LogError("[{Actor}] {Exception}: {Message}\n{StackTrace}", manager.LocalEndpoint, ex.GetType().Name, ex.Message, ex.StackTrace);
        }
    }

    private async Task Receive(List<GrpcServerBatcherItem> requests)
    {
        //Console.WriteLine("Request count: " + requests.Count);
        
        await RunBatch(requests);
        
        if (requests.Count < 10)
            await Task.Delay(Random.Shared.Next(1, 2)); // Force large batches
    }

    private async Task RunBatch(List<GrpcServerBatcherItem> requests)
    {
        try
        {
            GrpcServerSharedStreaming sharedStreaming = GetSharedStreaming(url);

            foreach (GrpcServerBatcherItem request in requests)
            {
                requestRefs.TryAdd(request.RequestId, request);

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
                request.Promise.SetException(ex);
            
            Console.WriteLine("{0}", ex.Message);
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
        else if (itemRequest.TryGetKeyValue is not null)
        {
            batchRequest.Type = GrpcServerBatchType.ServerTryGetKeyValue;
            batchRequest.TryGetKeyValue = itemRequest.TryGetKeyValue;
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
    
    private static async Task ReadLockMessages(AsyncDuplexStreamingCall<GrpcBatchServerLockRequest, GrpcBatchServerLockResponse> streaming)
    {
        try
        {
            await foreach (GrpcBatchServerLockResponse response in streaming.ResponseStream.ReadAllAsync())
            {
                if (!requestRefs.TryGetValue(response.RequestId, out GrpcServerBatcherItem? item))
                {
                    Console.WriteLine("Request not found " + response.RequestId);
                    continue;
                }

                switch (response.Type)
                {
                    case GrpcLockServerBatchType.ServerTypeTryLock:
                        item.Promise.SetResult(new(response.TryLock));
                        break;
                    
                    case GrpcLockServerBatchType.ServerTypeUnlock:
                        item.Promise.SetResult(new(response.Unlock));
                        break;
                    
                    case GrpcLockServerBatchType.ServerTypeExtendLock:
                        item.Promise.SetResult(new(response.ExtendLock));
                        break;
                    
                    case GrpcLockServerBatchType.ServerTypeGetLock:
                        item.Promise.SetResult(new(response.GetLock));
                        break;

                    case GrpcLockServerBatchType.ServerTypeNone:
                    default:
                        item.Promise.SetException(new KahunaServerException("Unknown response type: " + response.Type));
                        break;
                }

                requestRefs.TryRemove(response.RequestId, out _);
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine("Exception: " + ex.Message);
        }
    }

    private static async Task ReadKeyValueMessages(AsyncDuplexStreamingCall<GrpcBatchServerKeyValueRequest, GrpcBatchServerKeyValueResponse> streaming)
    {
        try
        {
            await foreach (GrpcBatchServerKeyValueResponse response in streaming.ResponseStream.ReadAllAsync())
            {
                if (!requestRefs.TryGetValue(response.RequestId, out GrpcServerBatcherItem? item))
                {
                    Console.WriteLine("Request not found " + response.RequestId);
                    continue;
                }

                switch (response.Type)
                {
                    case GrpcServerBatchType.ServerTrySetKeyValue:
                        item.Promise.SetResult(new(response.TrySetKeyValue));
                        break;

                    case GrpcServerBatchType.ServerTryGetKeyValue:
                        item.Promise.SetResult(new(response.TryGetKeyValue));
                        break;

                    case GrpcServerBatchType.ServerTryDeleteKeyValue:
                        item.Promise.SetResult(new(response.TryDeleteKeyValue));
                        break;

                    case GrpcServerBatchType.ServerTryExtendKeyValue:
                        item.Promise.SetResult(new(response.TryExtendKeyValue));
                        break;

                    case GrpcServerBatchType.ServerTryExistsKeyValue:
                        item.Promise.SetResult(new(response.TryExistsKeyValue));
                        break;

                    case GrpcServerBatchType.ServerTryExecuteTransactionScript:
                        item.Promise.SetResult(new(response.TryExecuteTransactionScript));
                        break;
                    
                    case GrpcServerBatchType.ServerTryAcquireExclusiveLock:
                        item.Promise.SetResult(new(response.TryAcquireExclusiveLock));
                        break;
                    
                    case GrpcServerBatchType.ServerTryAcquireManyExclusiveLocks:
                        item.Promise.SetResult(new(response.TryAcquireManyExclusiveLocks));
                        break;
                    
                    case GrpcServerBatchType.ServerTryReleaseExclusiveLock:
                        item.Promise.SetResult(new(response.TryReleaseExclusiveLock));
                        break;
                    
                    case GrpcServerBatchType.ServerTryReleaseManyExclusiveLocks:
                        item.Promise.SetResult(new(response.TryReleaseManyExclusiveLocks));
                        break;
                    
                    case GrpcServerBatchType.ServerTryPrepareMutations:
                        item.Promise.SetResult(new(response.TryPrepareMutations));
                        break;
                    
                    case GrpcServerBatchType.ServerTryPrepareManyMutations:
                        item.Promise.SetResult(new(response.TryPrepareManyMutations));
                        break;
                    
                    case GrpcServerBatchType.ServerTryCommitMutations:
                        item.Promise.SetResult(new(response.TryCommitMutations));
                        break;
                    
                    case GrpcServerBatchType.ServerTryCommitManyMutations:
                        item.Promise.SetResult(new(response.TryCommitManyMutations));
                        break;

                    case GrpcServerBatchType.ServerTypeNone:
                    default:
                        item.Promise.SetException(new KahunaServerException("Unknown response type: " + response.Type));
                        break;
                }

                requestRefs.TryRemove(response.RequestId, out _);
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine("Exception: " + ex.Message);
        }
    }

    private static GrpcServerSharedStreaming GetSharedStreaming(string url)
    {
        Lazy<List<GrpcServerSharedStreaming>> lazyStreamings = streamings.GetOrAdd(url, GetSharedStreamings);

        List<GrpcServerSharedStreaming> nodeStreamings = lazyStreamings.Value;
        
        return nodeStreamings[Random.Shared.Next(0, nodeStreamings.Count)];
    }
    
    private static Lazy<List<GrpcServerSharedStreaming>> GetSharedStreamings(string url)
    {
        return new(() => CreateSharedStreamings(url));
    }

    private static List<GrpcServerSharedStreaming> CreateSharedStreamings(string url)
    {
        List<GrpcChannel> nodeChannels = SharedChannels.GetAllChannels(url);
        
        List<GrpcServerSharedStreaming> nodeStreamings = new(nodeChannels.Count);

        foreach (GrpcChannel channel in nodeChannels)
        {
            Locker.LockerClient lockClient = new(channel);
            KeyValuer.KeyValuerClient keyValueClient = new(channel);

            AsyncDuplexStreamingCall<GrpcBatchServerLockRequest, GrpcBatchServerLockResponse>? locksStreaming = lockClient.BatchServerLockRequests();
            AsyncDuplexStreamingCall<GrpcBatchServerKeyValueRequest, GrpcBatchServerKeyValueResponse>? keyValueStreaming = keyValueClient.BatchServerKeyValueRequests();
            
            _ = ReadLockMessages(locksStreaming);
            _ = ReadKeyValueMessages(keyValueStreaming);
            
            nodeStreamings.Add(new(locksStreaming, keyValueStreaming));
        }

        return nodeStreamings;
    }
}