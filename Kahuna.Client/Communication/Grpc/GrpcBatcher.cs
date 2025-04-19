using System.Collections.Concurrent;
using System.Net.Security;
using Grpc.Core;
using Grpc.Net.Client;
using Grpc.Net.Client.Configuration;
using Kahuna.Shared.Locks;

/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace Kahuna.Client.Communication;

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

        inbox.Enqueue(grpcBatcherItem);

        if (1 == Interlocked.Exchange(ref processing, 0))
            _ = DeliverMessages();

        return promise.Task;
    }
    
    public Task<GrpcBatcherResponse> Enqueue(GrpcTryGetKeyValueRequest message)
    {
        TaskCompletionSource<GrpcBatcherResponse> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        GrpcBatcherItem grpcBatcherItem = new(Interlocked.Increment(ref requestId), new(message), promise);

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
            //manager.Logger.LogError("[{Actor}] {Exception}: {Message}\n{StackTrace}", manager.LocalEndpoint, ex.GetType().Name, ex.Message, ex.StackTrace);
        }
    }

    private async Task Receive(List<GrpcBatcherItem> requests)
    {
        Console.WriteLine("Request count: " + requests.Count);
        
        _ = RunBatch(requests);
        
        if (requests.Count < 10)
            await Task.Delay(1);
    }

    private async Task RunBatch(List<GrpcBatcherItem> requests)
    {
        GrpcChannel channel = GetSharedChannel(url);
        
        KeyValuer.KeyValuerClient client = new(channel);
        
        if (requests.Count == 1)
        {
            GrpcBatcherRequest b = requests[0].Request;
            TaskCompletionSource<GrpcBatcherResponse> promise = requests[0].Promise;

            if (b.TrySetKeyValue is not null)
                promise.SetResult(new(await client.TrySetKeyValueAsync(b.TrySetKeyValue)));
            else if (b.TryGetKeyValue is not null)
                promise.SetResult(new(await client.TryGetKeyValueAsync(b.TryGetKeyValue)));
            else 
                throw new KahunaException("Unknown request type", LockResponseType.Errored);
            
            return;
        }
        
        AsyncDuplexStreamingCall<GrpcBatchClientRequest, GrpcBatchClientResponse> streaming = client.BatchClientRequests();
        
        Task readTask = Task.Run(async () =>
        {
            await foreach (GrpcBatchClientResponse response in streaming.ResponseStream.ReadAllAsync())
            {
                if (requestRefs.TryGetValue(response.RequestId, out GrpcBatcherItem item))
                {
                    if (response.Type == GrpcBatchClientType.TrySetKeyValue)
                        item.Promise.SetResult(new(response.TrySetKeyValue));
                    else if (response.Type == GrpcBatchClientType.TryGetKeyValue)
                        item.Promise.SetResult(new(response.TryGetKeyValue));
                    else
                        throw new KahunaException("Unknown response type: " + response.Type, LockResponseType.Errored);
                    
                    requestRefs.TryRemove(response.RequestId, out _);
                }
                else
                    Console.WriteLine("Request not found " + response.RequestId);
            }
        });

        foreach (GrpcBatcherItem request in requests)
        {
            requestRefs.TryAdd(request.RequestId, request);
            
            GrpcBatchClientRequest requestx = new()
            {
                RequestId = request.RequestId
            };
            
            GrpcBatcherRequest b = request.Request;

            if (b.TrySetKeyValue is not null)
            {
                requestx.Type = GrpcBatchClientType.TrySetKeyValue;
                requestx.TrySetKeyValue = b.TrySetKeyValue;
            }
            else if (b.TryGetKeyValue is not null)
            {
                requestx.Type = GrpcBatchClientType.TryGetKeyValue;
                requestx.TryGetKeyValue = b.TryGetKeyValue;
            } 
            else 
            {
                throw new KahunaException("Unknown request type", LockResponseType.Errored);
            }

            await streaming.RequestStream.WriteAsync(requestx);
        }
        
        await streaming.RequestStream.CompleteAsync();
        await readTask;
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
        
        List<GrpcChannel> urlChannels = new(4);
        
        for (int i = 0; i < 4; i++)
        {
            urlChannels.Add(GrpcChannel.ForAddress(url, new() {
                HttpHandler = handler,
                ServiceConfig = new() { MethodConfigs = { defaultMethodConfig } }
            }));
        }

        return urlChannels;
    }
}