
using System.Collections.Concurrent;
using Grpc.Net.Client;
using Kahuna.Shared.KeyValue;
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
        GrpcExtendLockRequest request = new() { LockName = resource, LockId = lockId, ExpiresMs = expiryTime, Consistency = (GrpcLockConsistency)consistency };
        
        GrpcExtendLockResponse? response;
        
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
        
            response = await client.TryExtendLockAsync(request).ConfigureAwait(false);

            if (response is null)
                throw new KahunaException("Response is null", LockResponseType.Errored);
                
            if (response.Type == GrpcLockResponseType.LockResponseTypeExtended)
                return true;

        } while (response.Type == GrpcLockResponseType.LockResponseTypeMustRetry);
        
        throw new KahunaException("Failed to extend", (LockResponseType)response.Type);
    }
    
    internal async Task<KahunaLockInfo?> Get(string url, string resource, LockConsistency consistency)
    {
        GrpcGetLockRequest request = new() { LockName = resource, Consistency = (GrpcLockConsistency)consistency };
        
        GrpcGetLockResponse? response;
        
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
        
            response = await client.GetLockAsync(request).ConfigureAwait(false);

            if (response is null)
                throw new KahunaException("Response is null", LockResponseType.Errored);
                
            if (response.Type == GrpcLockResponseType.LockResponseTypeGot)
                return new(response.Owner, new(response.ExpiresPhysical, response.ExpiresCounter), response.FencingToken);

        } while (response.Type == GrpcLockResponseType.LockResponseTypeMustRetry);
        
        throw new KahunaException("Failed to get lock information", (LockResponseType)response.Type);
    }
    
    internal async Task<bool> TrySetKeyValue(string url, string key, string? value, int expiryTime, KeyValueConsistency consistency)
    {
        GrpcTrySetKeyValueRequest request = new() { Key = key, Value = value, ExpiresMs = expiryTime, Consistency = (GrpcKeyValueConsistency)consistency };
        
        GrpcTrySetKeyValueResponse? response;
        
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
        
            KeyValuer.KeyValuerClient client = new(channel);
        
            response = await client.TrySetKeyValueAsync(request).ConfigureAwait(false);

            if (response is null)
                throw new KahunaException("Response is null", LockResponseType.Errored);

            if (response.Type == GrpcKeyValueResponseType.KeyvalueResponseTypeSet)
                return true;
            
            //if (response.Type == GrpcLockResponseType.LockResponseTypeBusy)
            //    return (KahunaLockAcquireResult.Conflicted, -1);

        } while (response.Type == GrpcKeyValueResponseType.KeyvalueResponseTypeMustRetry);
            
        throw new KahunaException("Failed to set key/value", (LockResponseType)response.Type);
    }
}