
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
    
    internal async Task<(bool, long)> TryExtend(string url, string resource, string lockId, int expiryTime, LockConsistency consistency)
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
                return (true, response.FencingToken);

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
    
    internal async Task<(bool, long)> TrySetKeyValue(string url, string key, string? value, int expiryTime, KeyValueConsistency consistency)
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
                return (true, response.Revision);
            
            //if (response.Type == GrpcLockResponseType.LockResponseTypeBusy)
            //    return (KahunaLockAcquireResult.Conflicted, -1);

        } while (response.Type == GrpcKeyValueResponseType.KeyvalueResponseTypeMustRetry);
            
        throw new KahunaException("Failed to set key/value", (LockResponseType)response.Type);
    }
    
    internal async Task<(string?, long)> TryGetKeyValue(string url, string key, KeyValueConsistency consistency)
    {
        GrpcTryGetKeyValueRequest request = new() { Key = key, Consistency = (GrpcKeyValueConsistency)consistency };
        
        GrpcTryGetKeyValueResponse? response;
        
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
        
            response = await client.TryGetKeyValueAsync(request).ConfigureAwait(false);

            if (response is null)
                throw new KahunaException("Response is null", LockResponseType.Errored);

            switch (response.Type)
            {
                case GrpcKeyValueResponseType.KeyvalueResponseTypeGot:
                    return (response.Value, response.Revision);
                
                case GrpcKeyValueResponseType.KeyvalueResponseTypeDoesNotExist:
                    return (null, 0);
            }
            
        } while (response.Type == GrpcKeyValueResponseType.KeyvalueResponseTypeMustRetry);
            
        throw new KahunaException("Failed to set key/value", (LockResponseType)response.Type);
    }
    
    internal async Task<bool> TryDeleteKeyValue(string url, string key, KeyValueConsistency consistency)
    {
        GrpcTryDeleteKeyValueRequest request = new() { Key = key, Consistency = (GrpcKeyValueConsistency)consistency };
        
        GrpcTryDeleteKeyValueResponse? response;
        
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
        
            response = await client.TryDeleteKeyValueAsync(request).ConfigureAwait(false);

            if (response is null)
                throw new KahunaException("Response is null", LockResponseType.Errored);

            switch (response.Type)
            {
                case GrpcKeyValueResponseType.KeyvalueResponseTypeDeleted:
                    return true;
                
                case GrpcKeyValueResponseType.KeyvalueResponseTypeDoesNotExist:
                    return false;
            }
            
        } while (response.Type == GrpcKeyValueResponseType.KeyvalueResponseTypeMustRetry);
            
        throw new KahunaException("Failed to delete key/value", (LockResponseType)response.Type);
    }
    
    internal async Task<(bool, long)> TryExtendKeyValue(string url, string key, int expiresMs, KeyValueConsistency consistency)
    {
        GrpcTryExtendKeyValueRequest request = new() { Key = key, ExpiresMs = expiresMs, Consistency = (GrpcKeyValueConsistency)consistency };
        
        GrpcTryExtendKeyValueResponse? response;
        
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
        
            response = await client.TryExtendKeyValueAsync(request).ConfigureAwait(false);

            if (response is null)
                throw new KahunaException("Response is null", LockResponseType.Errored);

            switch (response.Type)
            {
                case GrpcKeyValueResponseType.KeyvalueResponseTypeExtended:
                    return (true, response.Revision);
                
                case GrpcKeyValueResponseType.KeyvalueResponseTypeDoesNotExist:
                    return (false, 0);
            }
            
        } while (response.Type == GrpcKeyValueResponseType.KeyvalueResponseTypeMustRetry);
            
        throw new KahunaException("Failed to extend key/value", (LockResponseType)response.Type);
    }
}