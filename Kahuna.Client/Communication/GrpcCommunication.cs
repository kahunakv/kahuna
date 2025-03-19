
/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;
using Google.Protobuf;
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
    
    public async Task<(KahunaLockAcquireResult, long)> TryAcquireLock(string url, string resource, byte[] owner, int expiryTime, LockConsistency consistency)
    {
        GrpcTryLockRequest request = new()
        {
            Resource = resource,
            Owner = UnsafeByteOperations.UnsafeWrap(owner), 
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
    
    public async Task<bool> TryUnlock(string url, string resource, byte[] owner, LockConsistency consistency)
    {
        GrpcUnlockRequest request = new()
        {
            Resource = resource, 
            Owner = UnsafeByteOperations.UnsafeWrap(owner), 
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
    
    public async Task<(bool, long)> TryExtend(string url, string resource, byte[] owner, int expiryTime, LockConsistency consistency)
    {
        GrpcExtendLockRequest request = new()
        {
            Resource = resource,
            Owner = UnsafeByteOperations.UnsafeWrap(owner),
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
            Resource = resource, 
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
                return new(response.Owner?.ToByteArray(), new(response.ExpiresPhysical, response.ExpiresCounter), response.FencingToken);

        } while (response.Type == GrpcLockResponseType.LockResponseTypeMustRetry);
        
        throw new KahunaException("Failed to get lock information", (LockResponseType)response.Type);
    }
    
    public async Task<(bool, long)> TrySetKeyValue(string url, string key, byte[]? value, int expiryTime, KeyValueFlags flags, KeyValueConsistency consistency)
    {
        GrpcTrySetKeyValueRequest request = new()
        {
            Key = key, 
            Value = value is not null ? UnsafeByteOperations.UnsafeWrap(value) : null,
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
            
        throw new KahunaException("Failed to set key/value: " + (KeyValueResponseType)response.Type, (KeyValueResponseType)response.Type);
    }
    
    public async Task<(bool, long)> TryCompareValueAndSetKeyValue(string url, string key, byte[]? value, byte[]? compareValue, int expiryTime, KeyValueConsistency consistency)
    {
        GrpcTrySetKeyValueRequest request = new()
        {
            Key = key, 
            Value = value is not null ? UnsafeByteOperations.UnsafeWrap(value) : null,
            CompareValue = compareValue is not null ? UnsafeByteOperations.UnsafeWrap(compareValue) : null,
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
            
        throw new KahunaException("Failed to set key/value: " + (KeyValueResponseType)response.Type, (KeyValueResponseType)response.Type);
    }
    
    public async Task<(bool, long)> TryCompareRevisionAndSetKeyValue(string url, string key, byte[]? value, long compareRevision, int expiryTime, KeyValueConsistency consistency)
    {
        GrpcTrySetKeyValueRequest request = new()
        {
            Key = key, 
            Value = value is not null ? UnsafeByteOperations.UnsafeWrap(value) : null,
            CompareRevision = compareRevision,
            Flags = GrpcKeyValueFlags.KeyvalueFlagsSetIfEqualToRevision,
            ExpiresMs = expiryTime, 
            Consistency = (GrpcKeyValueConsistency)consistency
        };

        int retries = 0;
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
            
            if (response.Type == GrpcKeyValueResponseType.KeyvalueResponseTypeMustRetry)
                logger?.LogDebug("Server asked to retry set key/value");
            
            if (++retries >= 5)
                throw new KahunaException("Retries exhauested", KeyValueResponseType.Errored);

        } while (response.Type == GrpcKeyValueResponseType.KeyvalueResponseTypeMustRetry);
            
        throw new KahunaException("Failed to set key/value:" + (KeyValueResponseType)response.Type, (KeyValueResponseType)response.Type);
    }
    
    public async Task<(byte[]?, long)> TryGetKeyValue(string url, string key, KeyValueConsistency consistency)
    {
        GrpcTryGetKeyValueRequest request = new()
        {
            Key = key, 
            Consistency = (GrpcKeyValueConsistency)consistency
        };

        int retries = 0;
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
                    return (response.Value.ToArray(), response.Revision);
                
                case GrpcKeyValueResponseType.KeyvalueResponseTypeDoesNotExist:
                    return (null, 0);
            }
            
            if (response.Type == GrpcKeyValueResponseType.KeyvalueResponseTypeMustRetry)
                logger?.LogDebug("Server asked to retry get key/value");
            
            if (++retries >= 5)
                throw new KahunaException("Retries exhauested", KeyValueResponseType.Errored);
            
        } while (response.Type == GrpcKeyValueResponseType.KeyvalueResponseTypeMustRetry);
            
        throw new KahunaException("Failed to get key/value:" + (KeyValueResponseType)response.Type, (KeyValueResponseType)response.Type);
    }
    
    public async Task<(bool, long)> TryDeleteKeyValue(string url, string key, KeyValueConsistency consistency)
    {
        GrpcTryDeleteKeyValueRequest request = new()
        {
            Key = key, 
            Consistency = (GrpcKeyValueConsistency)consistency
        };
        
        int retries = 0;
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
            
            if (response.Type == GrpcKeyValueResponseType.KeyvalueResponseTypeMustRetry)
                logger?.LogDebug("Server asked to retry delete key/value");
            
            if (++retries >= 5)
                throw new KahunaException("Retries exhauested", KeyValueResponseType.Errored);
            
        } while (response.Type == GrpcKeyValueResponseType.KeyvalueResponseTypeMustRetry);
            
        throw new KahunaException("Failed to delete key/value: " + (KeyValueResponseType)response.Type, (KeyValueResponseType)response.Type);
    }
    
    public async Task<(bool, long)> TryExtendKeyValue(string url, string key, int expiresMs, KeyValueConsistency consistency)
    {
        GrpcTryExtendKeyValueRequest request = new()
        {
            Key = key, 
            ExpiresMs = expiresMs, 
            Consistency = (GrpcKeyValueConsistency)consistency
        };

        int retries = 0;
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
            
            if (response.Type == GrpcKeyValueResponseType.KeyvalueResponseTypeMustRetry)
                logger?.LogDebug("Server asked to retry extend key/value");
            
            if (++retries >= 5)
                throw new KahunaException("Retries exhauested", KeyValueResponseType.Errored);
            
        } while (response.Type == GrpcKeyValueResponseType.KeyvalueResponseTypeMustRetry);
            
        throw new KahunaException("Failed to extend key/value: " + (KeyValueResponseType)response.Type, (KeyValueResponseType)response.Type);
    }

    public async Task<KahunaKeyValueTransactionResult> TryExecuteKeyValueTransaction(string url, byte[] script, string? hash)
    {
        GrpcTryExecuteTransactionRequest request = new()
        {
            Script = UnsafeByteOperations.UnsafeWrap(script)
        };
        
        if (hash is not null)
            request.Hash = hash;

        int retries = 0;
        GrpcTryExecuteTransactionResponse? response;
        
        do
        {
            GrpcChannel channel = GetSharedChannel(url);
        
            KeyValuer.KeyValuerClient client = new(channel);
        
            response = await client.TryExecuteTransactionAsync(request).ConfigureAwait(false);

            if (response is null)
                throw new KahunaException("Response is null", LockResponseType.Errored);

            if (response.Type is <= GrpcKeyValueResponseType.KeyvalueResponseTypeErrored or GrpcKeyValueResponseType.KeyvalueResponseTypeDoesNotExist)
                return new()
                {
                    Type = (KeyValueResponseType)response.Type,
                    Value = response.Value?.ToByteArray(),
                    Revision = response.Revision
                };
            
            if (response.Type == GrpcKeyValueResponseType.KeyvalueResponseTypeMustRetry)
                logger?.LogDebug("Server asked to retry transaction");
            
            if (++retries >= 5)
                throw new KahunaException("Retries exhauested", KeyValueResponseType.Errored);

        } while (response.Type == GrpcKeyValueResponseType.KeyvalueResponseTypeMustRetry);
            
        if (response.Type == GrpcKeyValueResponseType.KeyvalueResponseTypeAborted)
            throw new KahunaException("Transaction aborted", (KeyValueResponseType)response.Type);
        
        throw new KahunaException("Failed to execute key/value transaction:" + (KeyValueResponseType)response.Type, (KeyValueResponseType)response.Type);
    }

    private static GrpcChannel GetSharedChannel(string url)
    {
        if (!channels.TryGetValue(url, out GrpcChannel? channel))
        {
            channel = GrpcChannel.ForAddress(url, new() { 
                HttpHandler = new SocketsHttpHandler
                {
                    EnableMultipleHttp2Connections = true,
                    PooledConnectionIdleTimeout = TimeSpan.FromMinutes(10),
                    KeepAlivePingDelay = TimeSpan.FromSeconds(30),
                    KeepAlivePingTimeout = TimeSpan.FromSeconds(5)
                } 
            });
                
            channels.TryAdd(url, channel);
        }

        return channel;
    }
}