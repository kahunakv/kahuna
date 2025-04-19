
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

    private readonly string url;
    
    private readonly ConcurrentQueue<GrpcBatcherItem> inbox = new();

    private readonly ConcurrentDictionary<int, GrpcBatcherItem> requestRefs = new();
    
    private int processing = 1;
    
    private static int requestId;

    public GrpcBatcher(string url)
    {
        this.url = url;
    }
    
    public Task<GrpcBatcherResponse> Enqueue(GrpcTrySetKeyValueRequest message)
    {
        TaskCompletionSource<GrpcBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcBatcherItem grpcBatcherItem = new(Interlocked.Increment(ref requestId), new(message), promise);

        return TryProcessQueue(grpcBatcherItem, promise);
    }
    
    public Task<GrpcBatcherResponse> Enqueue(GrpcTryGetKeyValueRequest message)
    {
        TaskCompletionSource<GrpcBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcBatcherItem grpcBatcherItem = new(Interlocked.Increment(ref requestId), new(message), promise);

        return TryProcessQueue(grpcBatcherItem, promise);
    }
    
    public Task<GrpcBatcherResponse> Enqueue(GrpcTryDeleteKeyValueRequest message)
    {
        TaskCompletionSource<GrpcBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcBatcherItem grpcBatcherItem = new(Interlocked.Increment(ref requestId), new(message), promise);

        return TryProcessQueue(grpcBatcherItem, promise);
    }
    
    public Task<GrpcBatcherResponse> Enqueue(GrpcTryExtendKeyValueRequest message)
    {
        TaskCompletionSource<GrpcBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcBatcherItem grpcBatcherItem = new(Interlocked.Increment(ref requestId), new(message), promise);

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
                    
                    while (inbox.TryDequeue(out GrpcBatcherItem message))
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
        
        _ = RunBatch(requests);
        
        if (requests.Count < 10)
            await Task.Delay(Random.Shared.Next(1, 3)); // Force large batches
    }

    private async Task RunBatch(List<GrpcBatcherItem> requests)
    {
        try
        {
            GrpcChannel channel = GetSharedChannel(url);

            KeyValuer.KeyValuerClient client = new(channel);

            if (requests.Count == 1) // Prevent streaming for single requests
            {
                GrpcBatcherRequest itemRequest = requests[0].Request;
                TaskCompletionSource<GrpcBatcherResponse> promise = requests[0].Promise;

                if (itemRequest.TrySetKeyValue is not null)
                    promise.SetResult(new(await client.TrySetKeyValueAsync(itemRequest.TrySetKeyValue)));
                else if (itemRequest.TryGetKeyValue is not null)
                    promise.SetResult(new(await client.TryGetKeyValueAsync(itemRequest.TryGetKeyValue)));
                else if (itemRequest.TryDeleteKeyValue is not null)
                    promise.SetResult(new(await client.TryDeleteKeyValueAsync(itemRequest.TryDeleteKeyValue)));
                else if (itemRequest.TryExtendKeyValue is not null)
                    promise.SetResult(new(await client.TryExtendKeyValueAsync(itemRequest.TryExtendKeyValue)));
                else
                    throw new KahunaException("Unknown request type", LockResponseType.Errored);
                
                return;
            }

            using AsyncDuplexStreamingCall<GrpcBatchClientKeyValueRequest, GrpcBatchClientKeyValueResponse> streaming = client.BatchClientKeyValueRequests();

            Task readTask = Task.Run(async () =>
            {
                // ReSharper disable once AccessToDisposedClosure
                await foreach (GrpcBatchClientKeyValueResponse response in streaming.ResponseStream.ReadAllAsync())
                {
                    if (requestRefs.TryGetValue(response.RequestId, out GrpcBatcherItem item))
                    {
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
                            
                            case GrpcBatchClientType.TypeNone:
                            default:                                
                                throw new KahunaException("Unknown response type: " + response.Type,LockResponseType.Errored);
                        }

                        requestRefs.TryRemove(response.RequestId, out _);
                    }
                    else
                        Console.WriteLine("Request not found " + response.RequestId);
                }
            });

            foreach (GrpcBatcherItem request in requests)
            {
                requestRefs.TryAdd(request.RequestId, request);

                GrpcBatchClientKeyValueRequest batchRequest = new()
                {
                    RequestId = request.RequestId
                };

                GrpcBatcherRequest itemRequest = request.Request;

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
                else
                {
                    throw new KahunaException("Unknown request type", LockResponseType.Errored);
                }

                await streaming.RequestStream.WriteAsync(batchRequest);
            }

            await streaming.RequestStream.CompleteAsync();
            await readTask;
        }
        catch (Exception ex)
        {
            Console.WriteLine("{0}", ex.Message);
        }
    }
    
    public static GrpcChannel GetSharedChannel(string url)
    {
        Lazy<List<GrpcChannel>> lazyChannels = channels.GetOrAdd(url, GetSharedChannels);

        List<GrpcChannel> nodeChannels = lazyChannels.Value;
        
        return nodeChannels[Random.Shared.Next(0, nodeChannels.Count)];
    }

    private static Lazy<List<GrpcChannel>> GetSharedChannels(string url)
    {
        return new(() => CreateSharedChannels(url));
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