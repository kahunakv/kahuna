
/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;
using Grpc.Net.Client;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Locks;
using Microsoft.Extensions.Logging;

namespace Kahuna.Client.Communication;

public class GrpcCommunication : IKahunaCommunication
{
    private static readonly ConcurrentDictionary<string, GrpcChannel> channels = new();
    
    private readonly ILogger? logger;
    
    public GrpcCommunication(ILogger? logger)
    {
        this.logger = logger;
    }
    
    public async Task<(KahunaLockAcquireResult, long)> TryAcquireLock(string url, string key, string owner, int expiryTime, LockConsistency consistency)
    {
        GrpcTryLockRequest request = new()
        {
            LockName = key, 
            LockId = owner, 
            ExpiresMs = expiryTime, 
            Consistency = (GrpcLockConsistency)consistency
        };
        
        GrpcTryLockResponse? response;
        
        do
        {
            GrpcChannel channel = GetSharedChannel(url);
        
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
    
    public async Task<bool> TryUnlock(string url, string resource, string owner, LockConsistency consistency)
    {
        GrpcUnlockRequest request = new()
        {
            LockName = resource, 
            LockId = owner, 
            Consistency = (GrpcLockConsistency)consistency
        };
        
        GrpcUnlockResponse? response;
        
        do
        {
            GrpcChannel channel = GetSharedChannel(url);
        
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
    
    public async Task<(bool, long)> TryExtend(string url, string resource, string owner, int expiryTime, LockConsistency consistency)
    {
        GrpcExtendLockRequest request = new()
        {
            LockName = resource, 
            LockId = owner, 
            ExpiresMs = expiryTime, 
            Consistency = (GrpcLockConsistency)consistency
        };
        
        GrpcExtendLockResponse? response;
        
        do
        {
            GrpcChannel channel = GetSharedChannel(url);
        
            Locker.LockerClient client = new(channel);
        
            response = await client.TryExtendLockAsync(request).ConfigureAwait(false);

            if (response is null)
                throw new KahunaException("Response is null", LockResponseType.Errored);
                
            if (response.Type == GrpcLockResponseType.LockResponseTypeExtended)
                return (true, response.FencingToken);

        } while (response.Type == GrpcLockResponseType.LockResponseTypeMustRetry);
        
        throw new KahunaException("Failed to extend", (LockResponseType)response.Type);
    }
    
    public async Task<KahunaLockInfo?> Get(string url, string resource, LockConsistency consistency)
    {
        GrpcGetLockRequest request = new()
        {
            LockName = resource, 
            Consistency = (GrpcLockConsistency)consistency
        };
        
        GrpcGetLockResponse? response;
        
        do
        {
            GrpcChannel channel = GetSharedChannel(url);
        
            Locker.LockerClient client = new(channel);
        
            response = await client.GetLockAsync(request).ConfigureAwait(false);

            if (response is null)
                throw new KahunaException("Response is null", LockResponseType.Errored);
                
            if (response.Type == GrpcLockResponseType.LockResponseTypeGot)
                return new(response.Owner, new(response.ExpiresPhysical, response.ExpiresCounter), response.FencingToken);

        } while (response.Type == GrpcLockResponseType.LockResponseTypeMustRetry);
        
        throw new KahunaException("Failed to get lock information", (LockResponseType)response.Type);
    }
    
    public async Task<(bool, long)> TrySetKeyValue(string url, string key, string? value, int expiryTime, KeyValueFlags flags, KeyValueConsistency consistency)
    {
        GrpcTrySetKeyValueRequest request = new()
        {
            Key = key, 
            Value = value,
            Flags = (GrpcKeyValueFlags)flags,
            ExpiresMs = expiryTime, 
            Consistency = (GrpcKeyValueConsistency)consistency
        };
        
        GrpcTrySetKeyValueResponse? response;
        
        do
        {
            GrpcChannel channel = GetSharedChannel(url);
        
            KeyValuer.KeyValuerClient client = new(channel);
        
            response = await client.TrySetKeyValueAsync(request).ConfigureAwait(false);

            if (response is null)
                throw new KahunaException("Response is null", LockResponseType.Errored);

            if (response.Type == GrpcKeyValueResponseType.KeyvalueResponseTypeSet)
                return (true, response.Revision);
            
            if (response.Type == GrpcKeyValueResponseType.KeyvalueResponseTypeNotset)
                return (false, response.Revision);

        } while (response.Type == GrpcKeyValueResponseType.KeyvalueResponseTypeMustRetry);
            
        throw new KahunaException("Failed to set key/value: " + response.Type, (LockResponseType)response.Type);
    }
    
    public async Task<(bool, long)> TryCompareValueAndSetKeyValue(string url, string key, string? value, string? compareValue, int expiryTime, KeyValueConsistency consistency)
    {
        GrpcTrySetKeyValueRequest request = new()
        {
            Key = key, 
            Value = value,
            CompareValue = compareValue,
            Flags = GrpcKeyValueFlags.KeyvalueFlagsSetIfEqualToValue,
            ExpiresMs = expiryTime, 
            Consistency = (GrpcKeyValueConsistency)consistency
        };
        
        GrpcTrySetKeyValueResponse? response;
        
        do
        {
            GrpcChannel channel = GetSharedChannel(url);
        
            KeyValuer.KeyValuerClient client = new(channel);
        
            response = await client.TrySetKeyValueAsync(request).ConfigureAwait(false);

            if (response is null)
                throw new KahunaException("Response is null", LockResponseType.Errored);

            if (response.Type == GrpcKeyValueResponseType.KeyvalueResponseTypeSet)
                return (true, response.Revision);
            
            if (response.Type == GrpcKeyValueResponseType.KeyvalueResponseTypeNotset)
                return (false, response.Revision);

        } while (response.Type == GrpcKeyValueResponseType.KeyvalueResponseTypeMustRetry);
            
        throw new KahunaException("Failed to set key/value: " + response.Type, (LockResponseType)response.Type);
    }
    
    public async Task<(bool, long)> TryCompareRevisionAndSetKeyValue(string url, string key, string? value, long compareRevision, int expiryTime, KeyValueConsistency consistency)
    {
        GrpcTrySetKeyValueRequest request = new()
        {
            Key = key, 
            Value = value,
            CompareRevision = compareRevision,
            Flags = GrpcKeyValueFlags.KeyvalueFlagsSetIfEqualToRevision,
            ExpiresMs = expiryTime, 
            Consistency = (GrpcKeyValueConsistency)consistency
        };
        
        GrpcTrySetKeyValueResponse? response;
        
        do
        {
            GrpcChannel channel = GetSharedChannel(url);
        
            KeyValuer.KeyValuerClient client = new(channel);
        
            response = await client.TrySetKeyValueAsync(request).ConfigureAwait(false);

            if (response is null)
                throw new KahunaException("Response is null", LockResponseType.Errored);

            if (response.Type == GrpcKeyValueResponseType.KeyvalueResponseTypeSet)
                return (true, response.Revision);
            
            if (response.Type == GrpcKeyValueResponseType.KeyvalueResponseTypeNotset)
                return (false, response.Revision);

        } while (response.Type == GrpcKeyValueResponseType.KeyvalueResponseTypeMustRetry);
            
        throw new KahunaException("Failed to set key/value:" + response.Type, (LockResponseType)response.Type);
    }
    
    public async Task<(string?, long)> TryGetKeyValue(string url, string key, KeyValueConsistency consistency)
    {
        GrpcTryGetKeyValueRequest request = new()
        {
            Key = key, 
            Consistency = (GrpcKeyValueConsistency)consistency
        };
        
        GrpcTryGetKeyValueResponse? response;
        
        do
        {
            GrpcChannel channel = GetSharedChannel(url);
        
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
            
        throw new KahunaException("Failed to get key/value:" + response.Type, (LockResponseType)response.Type);
    }
    
    public async Task<(bool, long)> TryDeleteKeyValue(string url, string key, KeyValueConsistency consistency)
    {
        GrpcTryDeleteKeyValueRequest request = new()
        {
            Key = key, 
            Consistency = (GrpcKeyValueConsistency)consistency
        };
        
        GrpcTryDeleteKeyValueResponse? response;
        
        do
        {
            GrpcChannel channel = GetSharedChannel(url);
        
            KeyValuer.KeyValuerClient client = new(channel);
        
            response = await client.TryDeleteKeyValueAsync(request).ConfigureAwait(false);

            if (response is null)
                throw new KahunaException("Response is null", LockResponseType.Errored);

            switch (response.Type)
            {
                case GrpcKeyValueResponseType.KeyvalueResponseTypeDeleted:
                    return (true, response.Revision);
                
                case GrpcKeyValueResponseType.KeyvalueResponseTypeDoesNotExist:
                    return (false, response.Revision);
            }
            
        } while (response.Type == GrpcKeyValueResponseType.KeyvalueResponseTypeMustRetry);
            
        throw new KahunaException("Failed to delete key/value: " + response.Type, (LockResponseType)response.Type);
    }
    
    public async Task<(bool, long)> TryExtendKeyValue(string url, string key, int expiresMs, KeyValueConsistency consistency)
    {
        GrpcTryExtendKeyValueRequest request = new() { Key = key, ExpiresMs = expiresMs, Consistency = (GrpcKeyValueConsistency)consistency };
        
        GrpcTryExtendKeyValueResponse? response;
        
        do
        {
            GrpcChannel channel = GetSharedChannel(url);
        
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

    private static GrpcChannel GetSharedChannel(string url)
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

        return channel;
    }
}