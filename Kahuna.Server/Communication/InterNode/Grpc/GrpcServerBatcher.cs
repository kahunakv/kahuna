
using System.Collections.Concurrent;

using Grpc.Core;
using Grpc.Net.Client;
using Kommander.Communication.Grpc;

namespace Kahuna.Server.Communication.Internode.Grpc;

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
    
    public Task<GrpcServerBatcherResponse> Enqueue(GrpcTrySetKeyValueRequest message)
    {
        TaskCompletionSource<GrpcServerBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcServerBatcherItem grpcBatcherItem = new(Interlocked.Increment(ref requestId), new(message), promise);

        return TryProcessQueue(grpcBatcherItem, promise);
    }
    
    public Task<GrpcServerBatcherResponse> Enqueue(GrpcTryGetKeyValueRequest message)
    {
        TaskCompletionSource<GrpcServerBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcServerBatcherItem grpcBatcherItem = new(Interlocked.Increment(ref requestId), new(message), promise);

        return TryProcessQueue(grpcBatcherItem, promise);
    }
    
    public Task<GrpcServerBatcherResponse> Enqueue(GrpcTryDeleteKeyValueRequest message)
    {
        TaskCompletionSource<GrpcServerBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcServerBatcherItem grpcBatcherItem = new(Interlocked.Increment(ref requestId), new(message), promise);

        return TryProcessQueue(grpcBatcherItem, promise);
    }
    
    public Task<GrpcServerBatcherResponse> Enqueue(GrpcTryExtendKeyValueRequest message)
    {
        TaskCompletionSource<GrpcServerBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcServerBatcherItem grpcBatcherItem = new(Interlocked.Increment(ref requestId), new(message), promise);

        return TryProcessQueue(grpcBatcherItem, promise);
    }
    
    public Task<GrpcServerBatcherResponse> Enqueue(GrpcTryExistsKeyValueRequest message)
    {
        TaskCompletionSource<GrpcServerBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcServerBatcherItem grpcBatcherItem = new(Interlocked.Increment(ref requestId), new(message), promise);

        return TryProcessQueue(grpcBatcherItem, promise);
    }
    
    public Task<GrpcServerBatcherResponse> Enqueue(GrpcTryExecuteTransactionRequest message)
    {
        TaskCompletionSource<GrpcServerBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcServerBatcherItem grpcBatcherItem = new(Interlocked.Increment(ref requestId), new(message), promise);

        return TryProcessQueue(grpcBatcherItem, promise);
    }
    
     public Task<GrpcServerBatcherResponse> Enqueue(GrpcTryAcquireExclusiveLockRequest message)
    {
        TaskCompletionSource<GrpcServerBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcServerBatcherItem grpcBatcherItem = new(Interlocked.Increment(ref requestId), new(message), promise);

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
                else if (itemRequest.TryExecuteTransaction is not null)
                {
                    batchRequest.Type = GrpcServerBatchType.ServerTryExecuteTransaction;
                    batchRequest.TryExecuteTransaction = itemRequest.TryExecuteTransaction;
                } 
                else if (itemRequest.TryAcquireExclusiveLock is not null)
                {
                    batchRequest.Type = GrpcServerBatchType.ServerTryAcquireExclusiveLock;
                    batchRequest.TryAcquireExclusiveLock = itemRequest.TryAcquireExclusiveLock;
                }
                else
                {
                    throw new KahunaServerException("Unknown request type");
                }

                try
                {
                    await sharedStreaming.Semaphore.WaitAsync();

                    await sharedStreaming.Streaming.RequestStream.WriteAsync(batchRequest);
                }
                finally
                {
                    sharedStreaming.Semaphore.Release();
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

    private static async Task ReadMessages(AsyncDuplexStreamingCall<GrpcBatchServerKeyValueRequest, GrpcBatchServerKeyValueResponse> streaming)
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

                    case GrpcServerBatchType.ServerTryExecuteTransaction:
                        item.Promise.SetResult(new(response.TryExecuteTransaction));
                        break;
                    
                    case GrpcServerBatchType.ServerTryAcquireExclusiveLock:
                        item.Promise.SetResult(new(response.TryAcquireExclusiveLock));
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
            KeyValuer.KeyValuerClient client = new(channel);

            AsyncDuplexStreamingCall<GrpcBatchServerKeyValueRequest, GrpcBatchServerKeyValueResponse>? streaming = client.BatchServerKeyValueRequests();
            
            _ = ReadMessages(streaming);
            
            nodeStreamings.Add(new(streaming));
        }

        return nodeStreamings;
    }
}