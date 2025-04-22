
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

                GrpcBatchClientKeyValueRequest batchRequest = new()
                {
                    RequestId = request.RequestId
                };

                GrpcServerBatcherRequest itemRequest = request.Request;

                if (itemRequest.TrySetKeyValue is not null)
                {
                    batchRequest.Type = GrpcBatchClientType.TrySetKeyValue;
                    batchRequest.TrySetKeyValue = itemRequest.TrySetKeyValue;
                }
                else if (itemRequest.TryGetKeyValue is not null)
                {
                    batchRequest.Type = GrpcBatchClientType.TryGetKeyValue;
                    batchRequest.TryGetKeyValue = itemRequest.TryGetKeyValue;
                }
                else if (itemRequest.TryDeleteKeyValue is not null)
                {
                    batchRequest.Type = GrpcBatchClientType.TryDeleteKeyValue;
                    batchRequest.TryDeleteKeyValue = itemRequest.TryDeleteKeyValue;
                } 
                else if (itemRequest.TryExtendKeyValue is not null)
                {
                    batchRequest.Type = GrpcBatchClientType.TryExtendKeyValue;
                    batchRequest.TryExtendKeyValue = itemRequest.TryExtendKeyValue;
                } 
                else if (itemRequest.TryExistsKeyValue is not null)
                {
                    batchRequest.Type = GrpcBatchClientType.TryExistsKeyValue;
                    batchRequest.TryExistsKeyValue = itemRequest.TryExistsKeyValue;
                }
                else if (itemRequest.TryExecuteTransaction is not null)
                {
                    batchRequest.Type = GrpcBatchClientType.TryExecuteTransaction;
                    batchRequest.TryExecuteTransaction = itemRequest.TryExecuteTransaction;
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
            Console.WriteLine("{0}", ex.Message);
        }
    }

    private static async Task ReadMessages(AsyncDuplexStreamingCall<GrpcBatchClientKeyValueRequest, GrpcBatchClientKeyValueResponse> streaming)
    {
        await foreach (GrpcBatchClientKeyValueResponse response in streaming.ResponseStream.ReadAllAsync())
        {
            if (!requestRefs.TryGetValue(response.RequestId, out GrpcServerBatcherItem? item))
            {
                Console.WriteLine("Request not found " + response.RequestId);
                continue;
            }
            
            switch (response.Type)
            {
                case GrpcBatchClientType.TrySetKeyValue:
                    item.Promise.SetResult(new(response.TrySetKeyValue));
                    break;
                        
                case GrpcBatchClientType.TryGetKeyValue:
                    item.Promise.SetResult(new(response.TryGetKeyValue));
                    break;
                        
                case GrpcBatchClientType.TryDeleteKeyValue:
                    item.Promise.SetResult(new(response.TryDeleteKeyValue));
                    break;
                        
                case GrpcBatchClientType.TryExtendKeyValue:
                    item.Promise.SetResult(new(response.TryExtendKeyValue));
                    break;
                        
                case GrpcBatchClientType.TryExistsKeyValue:
                    item.Promise.SetResult(new(response.TryExistsKeyValue));
                    break;
                
                case GrpcBatchClientType.TryExecuteTransaction:
                    item.Promise.SetResult(new(response.TryExecuteTransaction));
                    break;
                        
                case GrpcBatchClientType.TypeNone:
                default:
                    item.Promise.SetException(new KahunaServerException("Unknown response type: " + response.Type));
                    break;
            }

            requestRefs.TryRemove(response.RequestId, out _);
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

            AsyncDuplexStreamingCall<GrpcBatchClientKeyValueRequest, GrpcBatchClientKeyValueResponse>? streaming = client.BatchClientKeyValueRequests();
            
            _ = ReadMessages(streaming);
            
            nodeStreamings.Add(new(streaming));
        }

        return nodeStreamings;
    }
}