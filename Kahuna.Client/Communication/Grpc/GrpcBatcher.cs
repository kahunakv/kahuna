
/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;
using System.Net.Security;
using Grpc.Core;
using Grpc.Net.Client;
using Grpc.Net.Client.Configuration;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Locks;

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
    private static readonly ConcurrentDictionary<string, Lazy<List<GrpcChannel>>> channels = new();
    
    private static readonly ConcurrentDictionary<string, Lazy<List<GrpcSharedStreaming>>> streamings = new();
    
    private static readonly ConcurrentDictionary<int, GrpcBatcherItem> requestRefs = new();
    
    private static int requestId;

    private readonly string url;
    
    private readonly ConcurrentQueue<GrpcBatcherItem> inbox = new();
    
    private int processing = 1;

    public GrpcBatcher(string url)
    {
        this.url = url;
    }

    /// <summary>
    /// Adds a new lock request to the batch processing queue and processes the queue as needed.
    /// </summary>
    /// <param name="message">The lock request message to be enqueued.</param>
    /// <returns>A task that represents the asynchronous operation, containing the batcher response for the lock request.</returns>
    public Task<GrpcBatcherResponse> Enqueue(GrpcTryLockRequest message)
    {
        TaskCompletionSource<GrpcBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcBatcherItem grpcBatcherItem = new(GrpcBatcherItemType.Locks, Interlocked.Increment(ref requestId), new(message), promise);

        return TryProcessQueue(grpcBatcherItem, promise);
    }

    /// <summary>
    /// Adds an unlock request to the batch processing queue and initiates queue processing as needed.
    /// </summary>
    /// <param name="message">The unlock request message to be added to the queue.</param>
    /// <returns>A task representing the asynchronous operation, containing the batcher response for the unlock request.</returns>
    public Task<GrpcBatcherResponse> Enqueue(GrpcUnlockRequest message)
    {
        TaskCompletionSource<GrpcBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcBatcherItem grpcBatcherItem = new(GrpcBatcherItemType.Locks, Interlocked.Increment(ref requestId), new(message), promise);

        return TryProcessQueue(grpcBatcherItem, promise);
    }

    /// <summary>
    /// Adds a lock extension request to the batch queue for processing and handles the request as necessary.
    /// </summary>
    /// <param name="message">The lock extension request to be enqueued.</param>
    /// <returns>A task that represents the asynchronous operation, containing the batcher response for the lock extension request.</returns>
    public Task<GrpcBatcherResponse> Enqueue(GrpcExtendLockRequest message)
    {
        TaskCompletionSource<GrpcBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcBatcherItem grpcBatcherItem = new(GrpcBatcherItemType.Locks, Interlocked.Increment(ref requestId), new(message), promise);

        return TryProcessQueue(grpcBatcherItem, promise);
    }

    /// <summary>
    /// Adds a lock request to the batch processing queue and processes the queue as needed.
    /// </summary>
    /// <param name="message">The lock request message to be enqueued.</param>
    /// <returns>A task that represents the asynchronous operation, containing the batcher response for the lock request.</returns>
    public Task<GrpcBatcherResponse> Enqueue(GrpcGetLockRequest message)
    {
        TaskCompletionSource<GrpcBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcBatcherItem grpcBatcherItem = new(GrpcBatcherItemType.Locks, Interlocked.Increment(ref requestId), new(message), promise);

        return TryProcessQueue(grpcBatcherItem, promise);
    }

    /// <summary>
    /// Adds a key-value set request to the batch queue and processes it accordingly.
    /// </summary>
    /// <param name="message">The key-value set request to be enqueued.</param>
    /// <returns>A task representing the asynchronous operation, containing the response for the processed request.</returns>
    public Task<GrpcBatcherResponse> Enqueue(GrpcTrySetKeyValueRequest message)
    {
        TaskCompletionSource<GrpcBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcBatcherItem grpcBatcherItem = new(GrpcBatcherItemType.KeyValues, Interlocked.Increment(ref requestId), new(message), promise);

        return TryProcessQueue(grpcBatcherItem, promise);
    }
    
    public Task<GrpcBatcherResponse> Enqueue(GrpcTryGetKeyValueRequest message)
    {
        TaskCompletionSource<GrpcBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcBatcherItem grpcBatcherItem = new(GrpcBatcherItemType.KeyValues, Interlocked.Increment(ref requestId), new(message), promise);

        return TryProcessQueue(grpcBatcherItem, promise);
    }
    
    public Task<GrpcBatcherResponse> Enqueue(GrpcTryDeleteKeyValueRequest message)
    {
        TaskCompletionSource<GrpcBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcBatcherItem grpcBatcherItem = new(GrpcBatcherItemType.KeyValues, Interlocked.Increment(ref requestId), new(message), promise);

        return TryProcessQueue(grpcBatcherItem, promise);
    }
    
    public Task<GrpcBatcherResponse> Enqueue(GrpcTryExtendKeyValueRequest message)
    {
        TaskCompletionSource<GrpcBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcBatcherItem grpcBatcherItem = new(GrpcBatcherItemType.KeyValues, Interlocked.Increment(ref requestId), new(message), promise);

        return TryProcessQueue(grpcBatcherItem, promise);
    }
    
    public Task<GrpcBatcherResponse> Enqueue(GrpcTryExistsKeyValueRequest message)
    {
        TaskCompletionSource<GrpcBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcBatcherItem grpcBatcherItem = new(GrpcBatcherItemType.KeyValues, Interlocked.Increment(ref requestId), new(message), promise);

        return TryProcessQueue(grpcBatcherItem, promise);
    }
    
    public Task<GrpcBatcherResponse> Enqueue(GrpcTryExecuteTransactionScriptRequest message)
    {
        TaskCompletionSource<GrpcBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcBatcherItem grpcBatcherItem = new(GrpcBatcherItemType.KeyValues, Interlocked.Increment(ref requestId), new(message), promise);

        return TryProcessQueue(grpcBatcherItem, promise);
    }
    
    public Task<GrpcBatcherResponse> Enqueue(GrpcGetByPrefixRequest message)
    {
        TaskCompletionSource<GrpcBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcBatcherItem grpcBatcherItem = new(GrpcBatcherItemType.KeyValues, Interlocked.Increment(ref requestId), new(message), promise);

        return TryProcessQueue(grpcBatcherItem, promise);
    }
    
    public Task<GrpcBatcherResponse> Enqueue(GrpcScanAllByPrefixRequest message)
    {
        TaskCompletionSource<GrpcBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcBatcherItem grpcBatcherItem = new(GrpcBatcherItemType.KeyValues, Interlocked.Increment(ref requestId), new(message), promise);

        return TryProcessQueue(grpcBatcherItem, promise);
    }

    private Task<GrpcBatcherResponse> TryProcessQueue(GrpcBatcherItem grpcBatcherItem, TaskCompletionSource<GrpcBatcherResponse> promise)
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
                    List<GrpcBatcherItem> messages = [];
                    
                    while (inbox.TryDequeue(out GrpcBatcherItem? message))
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

    private async Task Receive(List<GrpcBatcherItem> requests)
    {
        //Console.WriteLine("Request count: " + requests.Count);
        
        await RunBatch(requests);
        
        if (requests.Count < 10)
            await Task.Delay(Random.Shared.Next(1, 3)); // Force large batches
    }

    /// <summary>
    /// Processes a batch of requests, delegating them to specific handling mechanisms based on their type.
    /// </summary>
    /// <param name="requests">The list of batcher items to be processed.</param>
    /// <returns>A task that represents the asynchronous batch processing operation.</returns>
    private async Task RunBatch(List<GrpcBatcherItem> requests)
    {
        try
        {
            GrpcSharedStreaming sharedStreaming = GetSharedStreaming(url);

            foreach (GrpcBatcherItem request in requests)
            {
                requestRefs.TryAdd(request.RequestId, request);

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
        }
        catch (Exception ex)
        {
            foreach (GrpcBatcherItem request in requests)
            {
                requestRefs.TryRemove(request.RequestId, out _);
                
                request.Promise.SetException(ex);
            }
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
        else if (itemRequest.TryExecuteTransactionScript is not null)
        {
            batchRequest.Type = GrpcClientBatchType.TryExecuteTransactionScript;
            batchRequest.TryExecuteTransactionScript = itemRequest.TryExecuteTransactionScript;
        }
        else if (itemRequest.GetByPrefix is not null)
        {
            batchRequest.Type = GrpcClientBatchType.TryGetByPrefix;
            batchRequest.GetByPrefix = itemRequest.GetByPrefix;
        }
        else if (itemRequest.ScanByPrefix is not null)
        {
            batchRequest.Type = GrpcClientBatchType.TryScanByPrefix;
            batchRequest.ScanByPrefix = itemRequest.ScanByPrefix;
        }
        else        
            throw new KahunaException("Unknown request type", KeyValueResponseType.Errored);        

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

    /// <summary>
    /// Reads key-value response messages from the provided streaming call and processes them asynchronously.
    /// </summary>
    /// <param name="streaming">The asynchronous duplex streaming call that delivers key-value request and response messages.</param>
    /// <returns>A task that represents the asynchronous operation of reading and processing the key-value messages.</returns>
    private static async Task ReadKeyValueMessages(AsyncDuplexStreamingCall<GrpcBatchClientKeyValueRequest, GrpcBatchClientKeyValueResponse> streaming)
    {
        await foreach (GrpcBatchClientKeyValueResponse response in streaming.ResponseStream.ReadAllAsync())
        {
            if (!requestRefs.TryGetValue(response.RequestId, out GrpcBatcherItem? item))
            {
                Console.WriteLine("Request not found " + response.RequestId);
                continue;
            }
            
            switch (response.Type)
            {
                case GrpcClientBatchType.TrySetKeyValue:
                    item.Promise.SetResult(new(response.TrySetKeyValue));
                    break;
                        
                case GrpcClientBatchType.TryGetKeyValue:
                    item.Promise.SetResult(new(response.TryGetKeyValue));
                    break;
                        
                case GrpcClientBatchType.TryDeleteKeyValue:
                    item.Promise.SetResult(new(response.TryDeleteKeyValue));
                    break;
                        
                case GrpcClientBatchType.TryExtendKeyValue:
                    item.Promise.SetResult(new(response.TryExtendKeyValue));
                    break;
                        
                case GrpcClientBatchType.TryExistsKeyValue:
                    item.Promise.SetResult(new(response.TryExistsKeyValue));
                    break;
                
                case GrpcClientBatchType.TryExecuteTransactionScript:
                    item.Promise.SetResult(new(response.TryExecuteTransactionScript));
                    break;
                
                case GrpcClientBatchType.TryGetByPrefix:
                    item.Promise.SetResult(new(response.GetByPrefix));
                    break;
                
                case GrpcClientBatchType.TryScanByPrefix:
                    item.Promise.SetResult(new(response.ScanByPrefix));
                    break;
                        
                case GrpcClientBatchType.TypeNone:
                default:
                    item.Promise.SetException(new KahunaException("Unknown response type: " + response.Type,LockResponseType.Errored));
                    break;
            }

            requestRefs.TryRemove(response.RequestId, out _);
        }
    }

    /// <summary>
    /// Reads lock messages asynchronously from a duplex streaming call, processes the responses,
    /// and resolves or rejects associated tasks based on the response type.
    /// </summary>
    /// <param name="streaming">The asynchronous duplex streaming call containing lock request and response messages.</param>
    private static async Task ReadLockMessages(AsyncDuplexStreamingCall<GrpcBatchClientLockRequest, GrpcBatchClientLockResponse> streaming)
    {
        await foreach (GrpcBatchClientLockResponse response in streaming.ResponseStream.ReadAllAsync())
        {
            if (!requestRefs.TryGetValue(response.RequestId, out GrpcBatcherItem? item))
            {
                Console.WriteLine("Request not found " + response.RequestId);
                continue;
            }
            
            switch (response.Type)
            {
                case GrpcLockClientBatchType.TypeTryLock:
                    item.Promise.SetResult(new(response.TryLock));
                    break;
                
                case GrpcLockClientBatchType.TypeUnlock:
                    item.Promise.SetResult(new(response.Unlock));
                    break;
                
                case GrpcLockClientBatchType.TypeExtendLock:
                    item.Promise.SetResult(new(response.ExtendLock));
                    break;
                
                case GrpcLockClientBatchType.TypeGetLock:
                    item.Promise.SetResult(new(response.GetLock));
                    break;
                        
                case GrpcLockClientBatchType.TypeNone:
                default:
                    item.Promise.SetException(new KahunaException("Unknown response type: " + response.Type,LockResponseType.Errored));
                    break;
            }

            requestRefs.TryRemove(response.RequestId, out _);
        }
    }

    /// <summary>
    /// Retrieves a shared gRPC channel for the specified URL from the shared channel pool.
    /// </summary>
    /// <param name="url">The URL for which the shared gRPC channel is requested.</param>
    /// <returns>A shared gRPC channel corresponding to the specified URL.</returns>
    public static GrpcChannel GetSharedChannel(string url)
    {
        Lazy<List<GrpcChannel>> lazyChannels = channels.GetOrAdd(url, GetSharedChannels);

        List<GrpcChannel> nodeChannels = lazyChannels.Value;
        
        return nodeChannels[Random.Shared.Next(0, nodeChannels.Count)];
    }

    private static GrpcSharedStreaming GetSharedStreaming(string url)
    {
        Lazy<List<GrpcSharedStreaming>> lazyStreamings = streamings.GetOrAdd(url, GetSharedStreamings);

        List<GrpcSharedStreaming> nodeStreamings = lazyStreamings.Value;
        
        return nodeStreamings[Random.Shared.Next(0, nodeStreamings.Count)];
    }

    private static Lazy<List<GrpcChannel>> GetSharedChannels(string url)
    {
        return new(() => CreateSharedChannels(url));
    }
    
    private static Lazy<List<GrpcSharedStreaming>> GetSharedStreamings(string url)
    {
        return new(() => CreateSharedStreamings(url));
    }

    private static List<GrpcSharedStreaming> CreateSharedStreamings(string url)
    {
        Lazy<List<GrpcChannel>> lazyChannels = channels.GetOrAdd(url, GetSharedChannels);

        List<GrpcChannel> nodeChannels = lazyChannels.Value;
        
        List<GrpcSharedStreaming> nodeStreamings = new(nodeChannels.Count);

        foreach (GrpcChannel channel in nodeChannels)
        {
            Locker.LockerClient lockClient = new(channel);
            KeyValuer.KeyValuerClient keyValueClient = new(channel);            

            AsyncDuplexStreamingCall<GrpcBatchClientLockRequest, GrpcBatchClientLockResponse>? lockStreaming = lockClient.BatchClientLockRequests();
            AsyncDuplexStreamingCall<GrpcBatchClientKeyValueRequest, GrpcBatchClientKeyValueResponse>? keyValueStreaming = keyValueClient.BatchClientKeyValueRequests();
            
            _ = ReadLockMessages(lockStreaming);
            _ = ReadKeyValueMessages(keyValueStreaming);
                       
            nodeStreamings.Add(new(lockStreaming, keyValueStreaming));
        }

        return nodeStreamings;
    }

    private static List<GrpcChannel> CreateSharedChannels(string url)
    {
        SslClientAuthenticationOptions sslOptions = new()
        {
            RemoteCertificateValidationCallback = delegate { return true; }
        };

        SocketsHttpHandler handler = new()
        {
            SslOptions = sslOptions,
            ConnectTimeout = TimeSpan.FromSeconds(10),
            PooledConnectionIdleTimeout = Timeout.InfiniteTimeSpan,
            KeepAlivePingDelay = TimeSpan.FromSeconds(30),
            KeepAlivePingTimeout = TimeSpan.FromSeconds(10),
            EnableMultipleHttp2Connections = true
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
        
        List<GrpcChannel> urlChannels = new(2);
        
        for (int i = 0; i < 2; i++)
        {
            urlChannels.Add(GrpcChannel.ForAddress(url, new() {
                HttpHandler = handler,
                ServiceConfig = new() { MethodConfigs = { defaultMethodConfig } }
            }));
        }

        return urlChannels;
    }
}