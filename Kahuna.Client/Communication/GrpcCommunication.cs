
using System.Collections.Concurrent;
using Grpc.Net.Client;
using Kahuna.Shared.Locks;
using Microsoft.Extensions.Logging;

namespace Kahuna.Client.Communication;

internal sealed class GrpcCommunication
{
    private static readonly ConcurrentDictionary<string, GrpcChannel> channels = new();
    
    private readonly ILogger? logger;
    
    public GrpcCommunication(ILogger? logger)
    {
        this.logger = logger;
    }
    
    internal async Task<(KahunaLockAcquireResult, long)> TryAcquireLock(string url, string key, string lockId, int expiryTime, LockConsistency consistency)
    {
        GrpcTryLockRequest request = new() { LockName = key, LockId = lockId, ExpiresMs = expiryTime, Consistency = (GrpcLockConsistency)consistency };
        
        GrpcTryLockResponse? response;
        
        do
        {
            if (!channels.TryGetValue(url, out GrpcChannel? channel))
            {
                channel = GrpcChannel.ForAddress(url, new() { 
                    HttpHandler = new SocketsHttpHandler
                    {
                        EnableMultipleHttp2Connections = true
                    } 
                });
                
                channels.TryAdd(url, channel);
            }
        
            Locker.LockerClient client = new(channel);
        
            response = await client.TryLockAsync(request).ConfigureAwait(false);

            if (response is null)
                throw new KahunaException("Response is null", LockResponseType.Errored);

            if (response.Type == GrpcLockResponseType.LockResponseTypeLocked)
                return (KahunaLockAcquireResult.Success, response.FencingToken);
            
            if (response.Type == GrpcLockResponseType.LockResponseTypeBusy)
                return (KahunaLockAcquireResult.Conflicted, -1);

        } while (response.Type == GrpcLockResponseType.LockResponseTypeMustRetry);
            
        throw new KahunaException("Failed to lock", (LockResponseType)response.Type);
    }
    
    internal async Task<bool> TryUnlock(string url, string resource, string lockId, LockConsistency consistency)
    {
        GrpcUnlockRequest request = new() { LockName = resource, LockId = lockId, Consistency = (GrpcLockConsistency)consistency };
        
        GrpcUnlockResponse? response;
        
        do
        {
            if (!channels.TryGetValue(url, out GrpcChannel? channel))
            {
                channel = GrpcChannel.ForAddress(url, new() { 
                    HttpHandler = new SocketsHttpHandler
                    {
                        EnableMultipleHttp2Connections = true
                    } 
                });
                channels.TryAdd(url, channel);
            }
        
            Locker.LockerClient client = new(channel);
        
            response = await client.UnlockAsync(request).ConfigureAwait(false);

            if (response is null)
                throw new KahunaException("Response is null", LockResponseType.Errored);
                
            if (response.Type == GrpcLockResponseType.LockResponseTypeUnlocked)
                return true;
            
            if (response.Type == GrpcLockResponseType.LockResponseTypeInvalidOwner)
                return false;

        } while (response.Type == GrpcLockResponseType.LockResponseTypeMustRetry);
        
        throw new KahunaException("Failed to unlock", (LockResponseType)response.Type);
    }
    
    internal async Task<bool> TryExtend(string url, string resource, string lockId, int expiryTime, LockConsistency consistency)
    {
        /*KahunaLockRequest request = new() { LockName = resource, LockId = lockId, ExpiresMs = expiryTime, Consistency = consistency };
        string payload = JsonSerializer.Serialize(request, KahunaJsonContext.Default.KahunaLockRequest);

        KahunaLockResponse? response;
        
        do
        {
            AsyncRetryPolicy retryPolicy = BuildRetryPolicy(null);
            
            response = await retryPolicy.ExecuteAsync(() => 
                url
                    .WithOAuthBearerToken("xxx")
                    .AppendPathSegments("v1/kahuna/extend-lock")
                    .WithHeader("Accept", "application/json")
                    .WithHeader("Content-Type", "application/json")
                    .WithTimeout(5)
                    .WithSettings(o => o.HttpVersion = "2.0")
                    .PostStringAsync(payload)
                    .ReceiveJson<KahunaLockResponse>())
            .ConfigureAwait(false);
            
            if (response is null)
                throw new KahunaException("Response is null", LockResponseType.Errored);
            
            if (response.Type == LockResponseType.Extended)
                return true;

        } while (response.Type == LockResponseType.MustRetry);
        
        throw new KahunaException("Failed to extend lock", response.Type);*/
        
        await Task.Yield();
        
        throw new KahunaException("Failed to get lock information", LockResponseType.Errored);
    }
    
    internal async Task<KahunaLockInfo?> Get(string url, string resource, LockConsistency consistency)
    {
        /*KahunaGetLockRequest request = new() { LockName = resource, Consistency = consistency };
        string payload = JsonSerializer.Serialize(request, KahunaJsonContext.Default.KahunaGetLockRequest);

        KahunaGetLockResponse? response;

        do
        {
            AsyncRetryPolicy retryPolicy = BuildRetryPolicy(null);

            response = await retryPolicy.ExecuteAsync(() =>
                    url
                        .WithOAuthBearerToken("xxx")
                        .AppendPathSegments("v1/kahuna/get-lock")
                        .WithHeader("Accept", "application/json")
                        .WithHeader("Content-Type", "application/json")
                        .WithTimeout(5)
                        .WithSettings(o => o.HttpVersion = "2.0")
                        .PostStringAsync(payload)
                        .ReceiveJson<KahunaGetLockResponse>())
                .ConfigureAwait(false);

            if (response is null)
                throw new KahunaException("Response is null", LockResponseType.Errored);

            if (response.Type == LockResponseType.Got)
                return new(response.Owner ?? "", response.Expires, response.FencingToken);
            
        } while (response.Type == LockResponseType.MustRetry);
        
        throw new KahunaException("Failed to get lock information", response.Type);*/
        
        await Task.Yield();

        throw new KahunaException("Failed to get lock information", LockResponseType.Errored);
    }
}