
/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;
using System.Runtime.InteropServices;
using Google.Protobuf;
using Google.Protobuf.Collections;
using Grpc.Net.Client;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Locks;
using Microsoft.Extensions.Logging;

namespace Kahuna.Client.Communication;

/// <summary>
/// Provides an implementation of the IKahunaCommunication interface for gRPC-based communication.
/// This class offers methods to perform distributed locking and manage key-value storage in a gRPC context.
/// </summary>
public class GrpcCommunication : IKahunaCommunication
{
    private static readonly ConcurrentDictionary<string, Lazy<GrpcBatcher>> batchers = new();

    private readonly KahunaOptions? options;
    
    private readonly ILogger? logger;
    
    public GrpcCommunication(KahunaOptions? options, ILogger? logger)
    {
        this.options = options;
        this.logger = logger;
    }
    
    public async Task<(KahunaLockAcquireResult, long, string?)> TryAcquireLock(string url, string resource, byte[] owner, int expiryTime, LockDurability durability, CancellationToken cancellationToken)
    {
        GrpcTryLockRequest request = new()
        {
            Resource = resource,
            Owner = UnsafeByteOperations.UnsafeWrap(owner), 
            ExpiresMs = expiryTime,
            Durability = (GrpcLockDurability)durability
        };
        
        int retries = 0;
        GrpcTryLockResponse? response;
        
        GrpcBatcher batcher = GetSharedBatcher(url);
        
        do
        {
            GrpcBatcherResponse batchResponse = await batcher.Enqueue(request).ConfigureAwait(false);
            response = batchResponse.TryLock;

            if (response is null)
                throw new KahunaException("Response is null", LockResponseType.Errored);

            if (response.Type == GrpcLockResponseType.LockResponseTypeLocked)
                return (KahunaLockAcquireResult.Success, response.FencingToken, response.ServedFrom);
            
            if (response.Type == GrpcLockResponseType.LockResponseTypeBusy)
                return (KahunaLockAcquireResult.Conflicted, -1, null);
            
            if (++retries >= 5)
                throw new KahunaException("Retries exhausted.", LockResponseType.Errored);

        } while (response.Type == GrpcLockResponseType.LockResponseTypeMustRetry);
            
        throw new KahunaException("Failed to lock", (LockResponseType)response.Type);
    }
    
    public async Task<bool> TryUnlock(string url, string resource, byte[] owner, LockDurability durability, CancellationToken cancellationToken)
    {
        GrpcUnlockRequest request = new()
        {
            Resource = resource, 
            Owner = UnsafeByteOperations.UnsafeWrap(owner), 
            Durability = (GrpcLockDurability)durability
        };
        
        int retries = 0;
        GrpcUnlockResponse? response;
        
        GrpcBatcher batcher = GetSharedBatcher(url);
        
        do
        {
            if (cancellationToken.IsCancellationRequested)
                throw new KahunaException("Operation cancelled", LockResponseType.Errored);
            
            GrpcBatcherResponse batchResponse = await batcher.Enqueue(request).ConfigureAwait(false);
            response = batchResponse.Unlock;

            if (response is null)
                throw new KahunaException("Response is null", LockResponseType.Errored);
                
            if (response.Type == GrpcLockResponseType.LockResponseTypeUnlocked)
                return true;
            
            if (response.Type is GrpcLockResponseType.LockResponseTypeInvalidOwner or GrpcLockResponseType.LockResponseTypeLockDoesNotExist)
                return false;
            
            if (++retries >= 5)
                throw new KahunaException("Retries exhausted.", LockResponseType.Errored);

        } while (response.Type == GrpcLockResponseType.LockResponseTypeMustRetry);
        
        throw new KahunaException("Failed to unlock: " + response.Type, (LockResponseType)response.Type);
    }
    
    public async Task<(bool, long)> TryExtendLock(string url, string resource, byte[] owner, int expiryTime, LockDurability durability, CancellationToken cancellationToken)
    {
        GrpcExtendLockRequest request = new()
        {
            Resource = resource,
            Owner = UnsafeByteOperations.UnsafeWrap(owner),
            ExpiresMs = expiryTime,
            Durability = (GrpcLockDurability)durability
        };
        
        int retries = 0;
        GrpcExtendLockResponse? response;
        
        GrpcBatcher batcher = GetSharedBatcher(url);
        
        do
        {
            if (cancellationToken.IsCancellationRequested)
                throw new KahunaException("Operation cancelled", LockResponseType.Errored);
            
            GrpcBatcherResponse batchResponse = await batcher.Enqueue(request).ConfigureAwait(false);
            response = batchResponse.ExtendLock;

            if (response is null)
                throw new KahunaException("Response is null", LockResponseType.Errored);
                
            if (response.Type == GrpcLockResponseType.LockResponseTypeExtended)
                return (true, response.FencingToken);
            
            if (++retries >= 5)
                throw new KahunaException("Retries exhausted.", LockResponseType.Errored);

        } while (response.Type == GrpcLockResponseType.LockResponseTypeMustRetry);
        
        throw new KahunaException("Failed to extend", (LockResponseType)response.Type);
    }
    
    public async Task<KahunaLockInfo?> Get(string url, string resource, LockDurability durability, CancellationToken cancellationToken)
    {
        GrpcGetLockRequest request = new()
        {
            Resource = resource, 
            Durability = (GrpcLockDurability)durability
        };
        
        int retries = 0;
        GrpcGetLockResponse? response;
        
        GrpcChannel channel = GrpcBatcher.GetSharedChannel(url);
        Locker.LockerClient client = new(channel);
        
        do
        {
            if (cancellationToken.IsCancellationRequested)
                throw new KahunaException("Operation cancelled", LockResponseType.Errored);
        
            response = await client.GetLockAsync(request, cancellationToken: cancellationToken).ConfigureAwait(false);

            if (response is null)
                throw new KahunaException("Response is null", LockResponseType.Errored);
                
            if (response.Type == GrpcLockResponseType.LockResponseTypeGot)
                return new(response.Owner?.ToByteArray(), new(response.ExpiresPhysical, response.ExpiresCounter), response.FencingToken);
            
            if (++retries >= 5)
                throw new KahunaException("Retries exhausted.", LockResponseType.Errored);

        } while (response.Type == GrpcLockResponseType.LockResponseTypeMustRetry);
        
        throw new KahunaException("Failed to get lock information", (LockResponseType)response.Type);
    }
    
    public async Task<(bool, long, int)> TrySetKeyValue(string url, string key, byte[]? value, int expiryTime, KeyValueFlags flags, KeyValueDurability durability, CancellationToken cancellationToken)
    {                
        GrpcTrySetKeyValueRequest request = new()
        {
            Key = key, 
            Value = value is not null ? UnsafeByteOperations.UnsafeWrap(value) : null,
            Flags = (GrpcKeyValueFlags)flags,
            ExpiresMs = expiryTime, 
            Durability = (GrpcKeyValueDurability)durability
        };
        
        int retries = 0;
        GrpcTrySetKeyValueResponse? response;
        
        GrpcBatcher batcher = GetSharedBatcher(url);

        do
        {
            if (cancellationToken.IsCancellationRequested)
                throw new KahunaException("Operation cancelled", KeyValueResponseType.Aborted);

            GrpcBatcherResponse batchResponse = await batcher.Enqueue(request);
            response = batchResponse.TrySetKeyValue;

            if (response is null)
                throw new KahunaException("Response is null", KeyValueResponseType.Errored);

            if (response.Type == GrpcKeyValueResponseType.TypeSet)
                return (true, response.Revision, response.TimeElapsedMs);
            
            if (response.Type == GrpcKeyValueResponseType.TypeNotset)
                return (false, response.Revision, response.TimeElapsedMs);

            if (++retries >= 5)
                throw new KahunaException("Retries exhausted.", KeyValueResponseType.Errored);

        } while (response.Type == GrpcKeyValueResponseType.TypeMustRetry);
            
        throw new KahunaException("Failed to set key/value: " + (KeyValueResponseType)response.Type, (KeyValueResponseType)response.Type);
    }
    
    public async Task<(bool, long, int)> TryCompareValueAndSetKeyValue(string url, string key, byte[]? value, byte[]? compareValue, int expiryTime, KeyValueDurability durability, CancellationToken cancellationToken)
    {
        GrpcTrySetKeyValueRequest request = new()
        {
            Key = key, 
            Value = value is not null ? UnsafeByteOperations.UnsafeWrap(value) : null,
            CompareValue = compareValue is not null ? UnsafeByteOperations.UnsafeWrap(compareValue) : null,
            Flags = GrpcKeyValueFlags.SetIfEqualToValue,
            ExpiresMs = expiryTime, 
            Durability = (GrpcKeyValueDurability)durability
        };
        
        int retries = 0;
        GrpcTrySetKeyValueResponse? response;
                
        GrpcBatcher batcher = GetSharedBatcher(url);
        
        do
        {
            if (cancellationToken.IsCancellationRequested)
                throw new KahunaException("Operation cancelled", KeyValueResponseType.Aborted);                   
            
            GrpcBatcherResponse batchResponse = await batcher.Enqueue(request);
            response = batchResponse.TrySetKeyValue;

            if (response is null)
                throw new KahunaException("Response is null", KeyValueResponseType.Errored);

            if (response.Type == GrpcKeyValueResponseType.TypeSet)
                return (true, response.Revision, response.TimeElapsedMs);
            
            if (response.Type == GrpcKeyValueResponseType.TypeNotset)
                return (false, response.Revision, response.TimeElapsedMs);
            
            if (++retries >= 5)
                throw new KahunaException("Retries exhausted.", KeyValueResponseType.Errored);

        } while (response.Type == GrpcKeyValueResponseType.TypeMustRetry);
            
        throw new KahunaException("Failed to set key/value: " + (KeyValueResponseType)response.Type, (KeyValueResponseType)response.Type);
    }
    
    public async Task<(bool, long, int)> TryCompareRevisionAndSetKeyValue(string url, string key, byte[]? value, long compareRevision, int expiryTime, KeyValueDurability durability, CancellationToken cancellationToken)
    {
        GrpcTrySetKeyValueRequest request = new()
        {
            Key = key, 
            Value = value is not null ? UnsafeByteOperations.UnsafeWrap(value) : null,
            CompareRevision = compareRevision,
            Flags = GrpcKeyValueFlags.SetIfEqualToRevision,
            ExpiresMs = expiryTime, 
            Durability = (GrpcKeyValueDurability)durability
        };

        int retries = 0;
        GrpcTrySetKeyValueResponse? response;
        
        //GrpcChannel channel = GetSharedChannel(url);
        //KeyValuer.KeyValuerClient client = new(channel);
        
        GrpcBatcher batcher = GetSharedBatcher(url);
        
        do
        {
            if (cancellationToken.IsCancellationRequested)
                throw new KahunaException("Operation cancelled", KeyValueResponseType.Aborted);
        
            //response = await client.TrySetKeyValueAsync(request, cancellationToken: cancellationToken).ConfigureAwait(false);
            
            GrpcBatcherResponse batchResponse = await batcher.Enqueue(request);
            response = batchResponse.TrySetKeyValue;

            if (response is null)
                throw new KahunaException("Response is null", KeyValueResponseType.Errored);

            if (response.Type == GrpcKeyValueResponseType.TypeSet)
                return (true, response.Revision, response.TimeElapsedMs);
            
            if (response.Type == GrpcKeyValueResponseType.TypeNotset)
                return (false, response.Revision, response.TimeElapsedMs);
            
            if (response.Type == GrpcKeyValueResponseType.TypeMustRetry)
                logger?.LogDebug("Server asked to retry set key/value");
            
            if (++retries >= 5)
                throw new KahunaException("Retries exhausted.", KeyValueResponseType.Errored);

        } while (response.Type == GrpcKeyValueResponseType.TypeMustRetry);
            
        throw new KahunaException("Failed to set key/value:" + (KeyValueResponseType)response.Type, (KeyValueResponseType)response.Type);
    }
    
    public async Task<(bool, byte[]?, long, int)> TryGetKeyValue(string url, string key, long revision, KeyValueDurability durability, CancellationToken cancellationToken)
    {
        GrpcTryGetKeyValueRequest request = new()
        {
            Key = key, 
            Revision = revision,
            Durability = (GrpcKeyValueDurability)durability
        };

        int retries = 0;
        GrpcTryGetKeyValueResponse? response;
               
        GrpcBatcher batcher = GetSharedBatcher(url);
        
        do
        {
            if (cancellationToken.IsCancellationRequested)
                throw new KahunaException("Operation cancelled", KeyValueResponseType.Aborted);                   
            
            GrpcBatcherResponse batchResponse = await batcher.Enqueue(request);
            response = batchResponse.TryGetKeyValue;

            if (response is null)
                throw new KahunaException("Response is null", KeyValueResponseType.Errored);

            switch (response.Type)
            {
                case GrpcKeyValueResponseType.TypeGot:
                {
                    byte[]? value;

                    if (MemoryMarshal.TryGetArray(response.Value.Memory, out ArraySegment<byte> segment))
                        value = segment.Array;
                    else
                        value = response.Value.ToByteArray();
                    
                    return (true, value, response.Revision, response.TimeElapsedMs);
                }

                case GrpcKeyValueResponseType.TypeDoesNotExist:
                    return (false, null, 0, response.TimeElapsedMs);
            }
            
            if (response.Type == GrpcKeyValueResponseType.TypeMustRetry)
                logger?.LogDebug("Server asked to retry get key/value");
            
            if (++retries >= 5)
                throw new KahunaException("Retries exhausted.", KeyValueResponseType.Errored);
            
        } while (response.Type == GrpcKeyValueResponseType.TypeMustRetry);
            
        throw new KahunaException("Failed to get key/value:" + (KeyValueResponseType)response.Type, (KeyValueResponseType)response.Type);
    }

    public async Task<(bool, long, int)> TryExistsKeyValue(string url, string key, long revision, KeyValueDurability durability, CancellationToken cancellationToken)
    {
        GrpcTryExistsKeyValueRequest request = new()
        {
            Key = key, 
            Revision = revision,
            Durability = (GrpcKeyValueDurability)durability
        };

        int retries = 0;
        GrpcTryExistsKeyValueResponse? response;
        
        GrpcBatcher batcher = GetSharedBatcher(url);
        
        do
        {
            if (cancellationToken.IsCancellationRequested)
                throw new KahunaException("Operation cancelled", KeyValueResponseType.Aborted);
        
            GrpcBatcherResponse batchResponse = await batcher.Enqueue(request);
            response = batchResponse.TryExistsKeyValue;

            if (response is null)
                throw new KahunaException("Response is null", KeyValueResponseType.Errored);

            switch (response.Type)
            {
                case GrpcKeyValueResponseType.TypeExists:
                    return (true, response.Revision, response.TimeElapsedMs);
                
                case GrpcKeyValueResponseType.TypeDoesNotExist:
                    return (false, 0, response.TimeElapsedMs);
            }
            
            if (response.Type == GrpcKeyValueResponseType.TypeMustRetry)
                logger?.LogDebug("Server asked to retry exists key/value");
            
            if (++retries >= 5)
                throw new KahunaException("Retries exhausted.", KeyValueResponseType.Errored);
            
        } while (response.Type == GrpcKeyValueResponseType.TypeMustRetry);
            
        throw new KahunaException("Failed to check if exists key/value:" + (KeyValueResponseType)response.Type, (KeyValueResponseType)response.Type);
    }
    
    public async Task<(bool, long, int)> TryDeleteKeyValue(string url, string key, KeyValueDurability durability, CancellationToken cancellationToken)
    {
        GrpcTryDeleteKeyValueRequest request = new()
        {
            Key = key, 
            Durability = (GrpcKeyValueDurability)durability
        };
        
        int retries = 0;
        GrpcTryDeleteKeyValueResponse? response;               
        
        GrpcBatcher batcher = GetSharedBatcher(url);
        
        do
        {
            if (cancellationToken.IsCancellationRequested)
                throw new KahunaException("Operation cancelled", KeyValueResponseType.Aborted);                   
            
            GrpcBatcherResponse batchResponse = await batcher.Enqueue(request);
            response = batchResponse.TryDeleteKeyValue;

            if (response is null)
                throw new KahunaException("Response is null", KeyValueResponseType.Errored);

            switch (response.Type)
            {
                case GrpcKeyValueResponseType.TypeDeleted:
                    return (true, response.Revision, response.TimeElapsedMs);
                
                case GrpcKeyValueResponseType.TypeDoesNotExist:
                    return (false, response.Revision, response.TimeElapsedMs);
            }
            
            if (response.Type == GrpcKeyValueResponseType.TypeMustRetry)
                logger?.LogDebug("Server asked to retry delete key/value");
            
            if (++retries >= 5)
                throw new KahunaException("Retries exhausted.", KeyValueResponseType.Errored);
            
        } while (response.Type == GrpcKeyValueResponseType.TypeMustRetry);
            
        throw new KahunaException("Failed to delete key/value: " + (KeyValueResponseType)response.Type, (KeyValueResponseType)response.Type);
    }
    
    public async Task<(bool, long, int)> TryExtendKeyValue(string url, string key, int expiresMs, KeyValueDurability durability, CancellationToken cancellationToken)
    {
        GrpcTryExtendKeyValueRequest request = new()
        {
            Key = key, 
            ExpiresMs = expiresMs, 
            Durability = (GrpcKeyValueDurability)durability
        };

        int retries = 0;
        GrpcTryExtendKeyValueResponse? response;               
        
        GrpcBatcher batcher = GetSharedBatcher(url);
        
        do
        {
            if (cancellationToken.IsCancellationRequested)
                throw new KahunaException("Operation cancelled", KeyValueResponseType.Aborted);
        
            GrpcBatcherResponse batchResponse = await batcher.Enqueue(request).ConfigureAwait(false);
            response = batchResponse.TryExtendKeyValue;

            if (response is null)
                throw new KahunaException("Response is null", KeyValueResponseType.Errored);

            switch (response.Type)
            {
                case GrpcKeyValueResponseType.TypeExtended:
                    return (true, response.Revision, response.TimeElapsedMs);
                
                case GrpcKeyValueResponseType.TypeDoesNotExist:
                    return (false, 0, response.TimeElapsedMs);
            }
            
            if (response.Type == GrpcKeyValueResponseType.TypeMustRetry)
                logger?.LogDebug("Server asked to retry extend key/value");
            
            if (++retries >= 5)
                throw new KahunaException("Retries exhausted.", KeyValueResponseType.Errored);
            
        } while (response.Type == GrpcKeyValueResponseType.TypeMustRetry);
            
        throw new KahunaException("Failed to extend key/value: " + (KeyValueResponseType)response.Type, (KeyValueResponseType)response.Type);
    }

    public async Task<KahunaKeyValueTransactionResult> TryExecuteKeyValueTransactionScript(string url, byte[] script, string? hash, List<KeyValueParameter>? parameters, CancellationToken cancellationToken)
    {
        GrpcTryExecuteTransactionScriptRequest request = new()
        {
            Script = UnsafeByteOperations.UnsafeWrap(script)
        };
        
        if (hash is not null)
            request.Hash = hash;
        
        if (parameters is not null)
            request.Parameters.AddRange(GetTransactionParameters(parameters));

        int retries = 0;
        GrpcTryExecuteTransactionScriptResponse? response;
        
        GrpcBatcher batcher = GetSharedBatcher(url);
        
        do
        {
            if (cancellationToken.IsCancellationRequested)
                throw new KahunaException("Operation cancelled", KeyValueResponseType.Aborted);
            
            GrpcBatcherResponse batchResponse = await batcher.Enqueue(request).ConfigureAwait(false);
            response = batchResponse.TryExecuteTransactionScript;

            if (response is null)
                throw new KahunaException("Response is null", KeyValueResponseType.Errored);

            if (response.Type is < GrpcKeyValueResponseType.TypeErrored or GrpcKeyValueResponseType.TypeDoesNotExist)
                return new()
                {
                    Type = (KeyValueResponseType)response.Type,
                    Values = GetTransactionValues(response.Values),
                    TimeElapsedMs = response.TimeElapsedMs
                };
            
            if (response.Type == GrpcKeyValueResponseType.TypeMustRetry)
                logger?.LogDebug("Server asked to retry transaction");
            
            if (++retries >= 5)
                throw new KahunaException("Retries exhausted.", KeyValueResponseType.Errored);

        } while (response.Type == GrpcKeyValueResponseType.TypeMustRetry);
        
        if (!string.IsNullOrEmpty(response.Reason))
            throw new KahunaException(response.Reason, (KeyValueResponseType)response.Type);

        if (response.Type == GrpcKeyValueResponseType.TypeAborted)
            throw new KahunaException("Transaction aborted", (KeyValueResponseType)response.Type);

        throw new KahunaException("Failed to execute key/value transaction: " + (KeyValueResponseType)response.Type, (KeyValueResponseType)response.Type);
    }

    private static List<KahunaKeyValueTransactionResultValue> GetTransactionValues(RepeatedField<GrpcTryExecuteTransactionResponseValue> responseValues)
    {
        List<KahunaKeyValueTransactionResultValue> values = new(responseValues.Count);
        
        foreach (GrpcTryExecuteTransactionResponseValue response in responseValues)
        {
            byte[]? value;

            if (MemoryMarshal.TryGetArray(response.Value.Memory, out ArraySegment<byte> segment))
                value = segment.Array;
            else
                value = response.Value.ToByteArray();
            
            KahunaKeyValueTransactionResultValue responseValue = new()
            {
                Key = response.Key,
                Value = value,
                Revision = response.Revision,
                Expires = new(response.ExpiresPhysical, response.ExpiresCounter),
                LastModified = new(response.LastModifiedPhysical, response.LastModifiedCounter)
            };
            
            values.Add(responseValue);
        }
        
        return values;
    }

    public async Task<(bool, List<string>)> GetByPrefix(string url, string prefixKey, KeyValueDurability durability, CancellationToken cancellationToken)
    {
        GrpcGetByPrefixRequest request = new()
        {
            PrefixKey = prefixKey, 
            Durability = (GrpcKeyValueDurability)durability
        };

        int retries = 0;
        GrpcGetByPrefixResponse? response;
        
        GrpcBatcher batcher = GetSharedBatcher(url);
        
        do
        {
            if (cancellationToken.IsCancellationRequested)
                throw new KahunaException("Operation cancelled", KeyValueResponseType.Aborted);
        
            GrpcBatcherResponse batchResponse = await batcher.Enqueue(request).ConfigureAwait(false);
            response = batchResponse.GetByPrefix;

            if (response is null)
                throw new KahunaException("Response is null", KeyValueResponseType.Errored);
            
            if (response.Type == GrpcKeyValueResponseType.TypeGot)
                return (true, response.Items.Select(x => x.Key).ToList());
            
            if (response.Type == GrpcKeyValueResponseType.TypeDoesNotExist)
                return (false, []);
            
            if (response.Type == GrpcKeyValueResponseType.TypeMustRetry)
                logger?.LogDebug("Server asked to retry get key/value by prefix");
            
            if (++retries >= 5)
                throw new KahunaException("Retries exhausted.", KeyValueResponseType.Errored);
            
        } while (response.Type == GrpcKeyValueResponseType.TypeMustRetry);
            
        throw new KahunaException("Failed to get key/value by prefix: " + (KeyValueResponseType)response.Type, (KeyValueResponseType)response.Type);
    }
    
    public async Task<(bool, List<string>)> ScanAllByPrefix(string url, string prefixKey, KeyValueDurability durability, CancellationToken cancellationToken)
    {
        GrpcScanAllByPrefixRequest request = new()
        {
            PrefixKey = prefixKey, 
            Durability = (GrpcKeyValueDurability)durability
        };

        int retries = 0;
        GrpcScanAllByPrefixResponse? response;               
        
        GrpcBatcher batcher = GetSharedBatcher(url);
        
        do
        {
            if (cancellationToken.IsCancellationRequested)
                throw new KahunaException("Operation cancelled", KeyValueResponseType.Aborted);                   
            
            GrpcBatcherResponse batchResponse = await batcher.Enqueue(request).ConfigureAwait(false);
            response = batchResponse.ScanByPrefix;

            if (response is null)
                throw new KahunaException("Response is null", KeyValueResponseType.Errored);
            
            if (response.Type == GrpcKeyValueResponseType.TypeGot)
                return (true, response.Items.Select(x => x.Key).ToList());
            
            if (response.Type == GrpcKeyValueResponseType.TypeDoesNotExist)
                return (false, []);
            
            if (response.Type == GrpcKeyValueResponseType.TypeMustRetry)
                logger?.LogDebug("Server asked to retry scan key/value by prefix");
            
            if (++retries >= 5)
                throw new KahunaException("Retries exhausted.", KeyValueResponseType.Errored);
            
        } while (response.Type == GrpcKeyValueResponseType.TypeMustRetry);
            
        throw new KahunaException("Failed to scan key/value by prefix: " + (KeyValueResponseType)response.Type, (KeyValueResponseType)response.Type);
    }

    private static IEnumerable<GrpcKeyValueParameter> GetTransactionParameters(List<KeyValueParameter> parameters)
    {
        foreach (KeyValueParameter parameter in parameters)
        {
            GrpcKeyValueParameter grpcParameter = new()
            {
                Key = parameter.Key
            };

            if (parameter.Value is not null)
                grpcParameter.Value = parameter.Value;
            
            yield return grpcParameter;
        }
    }

    private static GrpcBatcher GetSharedBatcher(string url)
    {
        Lazy<GrpcBatcher> lazyBatchers = batchers.GetOrAdd(url, GetSharedBatchers);
        return lazyBatchers.Value;
    }
    
    private static Lazy<GrpcBatcher> GetSharedBatchers(string url)
    {
        return new(() => new(url));
    }
}