
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
using Kommander.Time;
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

    /// <summary>
    /// Attempts to acquire a lock on a specified resource using the provided settings.
    /// </summary>
    /// <param name="url">The endpoint URL of the server where the lock request will be executed.</param>
    /// <param name="resource">The name of the resource to lock.</param>
    /// <param name="owner">A unique identifier representing the owner of the lock.</param>
    /// <param name="expiryTime">The duration, in milliseconds, for which the lock will remain valid.</param>
    /// <param name="durability">Specifies the durability type of the lock (e.g., ephemeral or persistent).</param>
    /// <param name="cancellationToken">A token to observe for cancellation requests while attempting to acquire the lock.</param>
    /// <returns>
    /// A tuple containing the result of the lock acquisition (<see cref="KahunaLockAcquireResult"/>),
    /// the remaining TTL (time-to-live) for the lock, and an optional string error message.
    /// </returns>
    /// <exception cref="KahunaException">Thrown if the lock acquisition process encounters an error or fails.</exception>
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
            GrpcBatcherResponse batchResponse;
                              
            if (cancellationToken == CancellationToken.None)
               batchResponse = await batcher.Enqueue(request).ConfigureAwait(false);
            else
               batchResponse = await batcher.Enqueue(request).WaitAsync(cancellationToken).ConfigureAwait(false);
            
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

    /// <summary>
    /// Attempts to release a lock on a specified resource with the given settings.
    /// </summary>
    /// <param name="url">The endpoint URL of the server to interact with.</param>
    /// <param name="resource">The name of the resource for which the lock release is requested.</param>
    /// <param name="owner">The identifier for the owner of the lock.</param>
    /// <param name="durability">The durability type of the lock (e.g., ephemeral or persistent).</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>
    /// A boolean indicating whether the lock was successfully released.
    /// </returns>
    /// <exception cref="KahunaException">Thrown if the operation fails or encounters an unrecoverable error.</exception>
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
            
            GrpcBatcherResponse batchResponse;
                              
            if (cancellationToken == CancellationToken.None)
               batchResponse = await batcher.Enqueue(request).ConfigureAwait(false);
            else
               batchResponse = await batcher.Enqueue(request).WaitAsync(cancellationToken).ConfigureAwait(false);
            
            response = batchResponse.Unlock;

            if (response is null)
                throw new KahunaException("Response is null", LockResponseType.Errored);
                
            if (response.Type == GrpcLockResponseType.LockResponseTypeUnlocked)
                return true;
            
            if (response.Type is GrpcLockResponseType.LockResponseTypeInvalidOwner or GrpcLockResponseType.LockResponseTypeLockDoesNotExist)
                return false;
            
            if (++retries >= 5)
                throw new KahunaException("Retries exhausted.", LockResponseType.Aborted);

        } while (response.Type == GrpcLockResponseType.LockResponseTypeMustRetry);
        
        throw new KahunaException("Failed to unlock: " + response.Type, (LockResponseType)response.Type);
    }

    /// <summary>
    /// Attempts to extend the lock on a specified resource with updated expiry and durability settings.
    /// </summary>
    /// <param name="url">The endpoint URL of the server to interact with.</param>
    /// <param name="resource">The name of the resource for which the lock extension is requested.</param>
    /// <param name="owner">The identifier for the owner of the lock.</param>
    /// <param name="expiryTime">The new expiry time in milliseconds to set for the lock extension.</param>
    /// <param name="durability">The durability type of the lock (e.g., ephemeral or persistent).</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>
    /// A tuple containing a boolean indicating whether the lock was successfully extended and the updated lock expiry timestamp.
    /// </returns>
    /// <exception cref="KahunaException">Thrown if the operation fails or encounters an unrecoverable error.</exception>
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
            
            GrpcBatcherResponse batchResponse;
                              
            if (cancellationToken == CancellationToken.None)
               batchResponse = await batcher.Enqueue(request).ConfigureAwait(false);
            else
               batchResponse = await batcher.Enqueue(request).WaitAsync(cancellationToken).ConfigureAwait(false);
            
            response = batchResponse.ExtendLock;

            if (response is null)
                throw new KahunaException("Response is null", LockResponseType.Errored);
                
            if (response.Type == GrpcLockResponseType.LockResponseTypeExtended)
                return (true, response.FencingToken);
            
            if (++retries >= 5)
                throw new KahunaException("Retries exhausted.", LockResponseType.Aborted);

        } while (response.Type == GrpcLockResponseType.LockResponseTypeMustRetry);
        
        throw new KahunaException("Failed to extend", (LockResponseType)response.Type);
    }

    /// <summary>
    /// Attempts to get information about a lock for a specified resource using the given parameters.
    /// </summary>
    /// <param name="url">The endpoint URL of the server to communicate with.</param>
    /// <param name="resource">The name of the resource to be locked.</param>
    /// <param name="durability">The durability type of the lock (e.g., ephemeral or persistent).</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>
    /// An object of type <see cref="KahunaLockInfo"/> if the lock is successfully acquired, otherwise null.
    /// </returns>
    /// <exception cref="KahunaException">Thrown if the operation fails after retries or encounters an error.</exception>
    public async Task<KahunaLockInfo?> GetLock(string url, string resource, LockDurability durability, CancellationToken cancellationToken)
    {
        GrpcGetLockRequest request = new()
        {
            Resource = resource,
            Durability = (GrpcLockDurability)durability
        };
        
        int retries = 0;
        GrpcGetLockResponse? response;
        
        GrpcBatcher batcher = GetSharedBatcher(url);
        
        do
        {
            if (cancellationToken.IsCancellationRequested)
                throw new KahunaException("Operation cancelled", LockResponseType.Errored);
        
            //response = await client.GetLockAsync(request, cancellationToken: cancellationToken).ConfigureAwait(false);
            
            GrpcBatcherResponse batchResponse;
                              
            if (cancellationToken == CancellationToken.None)
               batchResponse = await batcher.Enqueue(request).ConfigureAwait(false);
            else
               batchResponse = await batcher.Enqueue(request).WaitAsync(cancellationToken).ConfigureAwait(false);
            
            response = batchResponse.GetLock;

            if (response is null)
                throw new KahunaException("Response is null", LockResponseType.Errored);
                
            if (response.Type == GrpcLockResponseType.LockResponseTypeGot)
                return new(response.Owner?.ToByteArray(), new(response.ExpiresNode, response.ExpiresPhysical, response.ExpiresCounter), response.FencingToken);
            
            if (++retries >= 5)
                throw new KahunaException("Retries exhausted.", LockResponseType.Aborted);

        } while (response.Type == GrpcLockResponseType.LockResponseTypeMustRetry);
        
        throw new KahunaException("Failed to get lock information", (LockResponseType)response.Type);
    }

    /// <summary>
    /// Tries to set a key-value pair in a distributed key-value store with specified parameters.
    /// </summary>
    /// <param name="url">The address of the server or service endpoint.</param>
    /// <param name="transactionId">The transaction identifier used to associate the operation with a logical timeline.</param>
    /// <param name="key">The key to be set or updated in the key-value store.</param>
    /// <param name="value">The value to be associated with the key. Can be null if the intention is to set an empty value.</param>
    /// <param name="expiryTime">The expiration time in milliseconds for the key-value pair.</param>
    /// <param name="flags">The flags indicating the conditions or modes of the set operation.</param>
    /// <param name="durability">The durability guarantee of the operation (e.g., ephemeral or persistent).</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>
    /// A tuple where:
    /// - The first item represents whether the operation succeeded.
    /// - The second item represents the new revision number of the key.
    /// - The third item represents the time taken for the operation in milliseconds.
    /// </returns>
    /// <exception cref="KahunaException">Thrown if the operation fails after retries.</exception>
    public async Task<(bool, long, int)> TrySetKeyValue(
        string url,
        HLCTimestamp transactionId,
        string key,
        byte[]? value,
        int expiryTime,
        KeyValueFlags flags,
        KeyValueDurability durability,
        CancellationToken cancellationToken
    )
    {
        GrpcTrySetKeyValueRequest request = new()
        {
            TransactionIdNode = transactionId.N,
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C,
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

            GrpcBatcherResponse batchResponse;
                              
            if (cancellationToken == CancellationToken.None)
               batchResponse = await batcher.Enqueue(request).ConfigureAwait(false);
            else
               batchResponse = await batcher.Enqueue(request).WaitAsync(cancellationToken).ConfigureAwait(false);
            
            response = batchResponse.TrySetKeyValue;

            if (response is null)
                throw new KahunaException("Response is null", KeyValueResponseType.Errored);

            if (response.Type == GrpcKeyValueResponseType.TypeSet)
                return (true, response.Revision, response.TimeElapsedMs);
            
            if (response.Type == GrpcKeyValueResponseType.TypeNotset)
                return (false, response.Revision, response.TimeElapsedMs);

            if (++retries >= 5)
                throw new KahunaException("Retries exhausted.", KeyValueResponseType.Aborted);

        } while (transactionId == HLCTimestamp.Zero && response.Type == GrpcKeyValueResponseType.TypeMustRetry);
            
        throw new KahunaException("Failed to set key/value: " + (KeyValueResponseType)response.Type, (KeyValueResponseType)response.Type);
    }

    public async Task<(List<KahunaSetKeyValueResponseItem>, int)> TrySetManyKeyValues(
        string url, 
        IEnumerable<KahunaSetKeyValueRequestItem> requestItems, 
        CancellationToken cancellationToken
    )
    {
        GrpcTrySetManyKeyValueRequest request = new();
        
        request.Items.AddRange(GetSetManyKeyValueRequestItems(requestItems));
        
        GrpcBatcher batcher = GetSharedBatcher(url);
        
        if (cancellationToken.IsCancellationRequested)
            throw new KahunaException("Operation cancelled", KeyValueResponseType.Aborted);

        GrpcBatcherResponse batchResponse;
                              
        if (cancellationToken == CancellationToken.None)
           batchResponse = await batcher.Enqueue(request).ConfigureAwait(false);
        else
           batchResponse = await batcher.Enqueue(request).WaitAsync(cancellationToken).ConfigureAwait(false);
            
        GrpcTrySetManyKeyValueResponse? response = batchResponse.TrySetManyKeyValues;

        if (response is null)
            throw new KahunaException("Response is null", KeyValueResponseType.Errored);
            
        return (GetSetManyKeyValueResponseItems(response.Items), response.TimeElapsedMs);
    }

    private static IEnumerable<GrpcTrySetManyKeyValueRequestItem> GetSetManyKeyValueRequestItems(IEnumerable<KahunaSetKeyValueRequestItem> requestItems)
    {                
        foreach (KahunaSetKeyValueRequestItem item in requestItems)
        {
            yield return new()
            {
                Key = item.Key,
                Value = item.Value is not null ? UnsafeByteOperations.UnsafeWrap(item.Value) : null,
                ExpiresMs = item.ExpiresMs,
                Flags = (GrpcKeyValueFlags)item.Flags,
                Durability = (GrpcKeyValueDurability)item.Durability
            };                       
        }        
    }
    
    private static List<KahunaSetKeyValueResponseItem> GetSetManyKeyValueResponseItems(RepeatedField<GrpcTrySetManyKeyValueResponseItem> grpcReponseItems)
    {                
        List<KahunaSetKeyValueResponseItem> responseItems = new(grpcReponseItems.Count);
        
        foreach (GrpcTrySetManyKeyValueResponseItem? item in grpcReponseItems)
        {
            responseItems.Add(new()
            {
                Key = item.Key,
                Revision = item.Revision,
                LastModified = new(item.LastModifiedNode, item.LastModifiedPhysical, item.LastModifiedCounter),
                Durability = (KeyValueDurability)item.Durability
            });
        }

        return responseItems;
    }

    /// <summary>
    /// Attempts to compare the current value associated with a key in a distributed key-value store and set it to a new value if the comparison matches.
    /// </summary>
    /// <param name="url">The address of the server or service endpoint.</param>
    /// <param name="transactionId">The transaction identifier used to associate the operation with a logical timeline.</param> 
    /// <param name="key">The key whose value needs to be compared and possibly updated.</param>
    /// <param name="value">The new value to set if the current value matches the compare value. Can be null to represent an empty value.</param>
    /// <param name="compareValue">The value to compare against the current value. If the current value matches this, the update is performed. Can be null to represent an empty comparison value.</param>
    /// <param name="expiryTime">The expiration time in milliseconds for the key-value pair after it is updated.</param>
    /// <param name="durability">The durability guarantee of the operation (e.g., ephemeral or persistent).</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>
    /// A tuple where:
    /// - The first item indicates whether the operation succeeded.
    /// - The second item represents the new revision number of the key.
    /// - The third item represents the time taken for the operation in milliseconds.
    /// </returns>
    /// <exception cref="KahunaException">Thrown if the operation fails due to retries being exhausted, cancellation, or other errors.</exception>
    public async Task<(bool, long, int)> TryCompareValueAndSetKeyValue(
        string url, 
        HLCTimestamp transactionId, 
        string key, 
        byte[]? value, 
        byte[]? compareValue, 
        int expiryTime, 
        KeyValueDurability durability, 
        CancellationToken cancellationToken
    )
    {
        GrpcTrySetKeyValueRequest request = new()
        {
            TransactionIdNode = transactionId.N,
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C,
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
            
            GrpcBatcherResponse batchResponse;
                              
            if (cancellationToken == CancellationToken.None)
               batchResponse = await batcher.Enqueue(request).ConfigureAwait(false);
            else
               batchResponse = await batcher.Enqueue(request).WaitAsync(cancellationToken).ConfigureAwait(false);
            
            response = batchResponse.TrySetKeyValue;

            if (response is null)
                throw new KahunaException("Response is null", KeyValueResponseType.Errored);

            if (response.Type == GrpcKeyValueResponseType.TypeSet)
                return (true, response.Revision, response.TimeElapsedMs);
            
            if (response.Type == GrpcKeyValueResponseType.TypeNotset)
                return (false, response.Revision, response.TimeElapsedMs);
            
            if (++retries >= 5)
                throw new KahunaException("Retries exhausted.", KeyValueResponseType.Aborted);

        } while (transactionId == HLCTimestamp.Zero && response.Type == GrpcKeyValueResponseType.TypeMustRetry);
            
        throw new KahunaException("Failed to set key/value: " + (KeyValueResponseType)response.Type, (KeyValueResponseType)response.Type);
    }

    /// <summary>
    /// Attempts to update a key-value pair in a distributed key-value store only if the current revision number matches a specified value.
    /// </summary>
    /// <param name="url">The address of the server or service endpoint.</param>
    /// <param name="transactionId">The transaction identifier used to associate the operation with a logical timeline.</param> 
    /// <param name="key">The key to be updated in the key-value store.</param>
    /// <param name="value">The new value to associate with the key. Can be null to set an empty value.</param>
    /// <param name="compareRevision">The expected current revision number of the key. The update will only occur if this value matches the actual revision number.</param>
    /// <param name="expiryTime">The expiration time in milliseconds for the key-value pair.</param>
    /// <param name="durability">The durability guarantee of the operation (e.g., ephemeral or persistent).</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A tuple where:
    /// - The first item indicates whether the operation succeeded.
    /// - The second item provides the new revision number of the key.
    /// - The third item represents the time taken for the operation in milliseconds.</returns>
    /// <exception cref="KahunaException">Thrown if the operation fails or retries are exhausted.</exception>
    public async Task<(bool, long, int)> TryCompareRevisionAndSetKeyValue(
        string url,
        HLCTimestamp transactionId,
        string key,
        byte[]? value,
        long compareRevision,
        int expiryTime,
        KeyValueDurability durability,
        CancellationToken cancellationToken
    )
    {
        GrpcTrySetKeyValueRequest request = new()
        {
            TransactionIdNode = transactionId.N,
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C,
            Key = key,
            Value = value is not null ? UnsafeByteOperations.UnsafeWrap(value) : null,
            CompareRevision = compareRevision,
            Flags = GrpcKeyValueFlags.SetIfEqualToRevision,
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
            
            GrpcBatcherResponse batchResponse;
                              
            if (cancellationToken == CancellationToken.None)
               batchResponse = await batcher.Enqueue(request).ConfigureAwait(false);
            else
               batchResponse = await batcher.Enqueue(request).WaitAsync(cancellationToken).ConfigureAwait(false);
            
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
                throw new KahunaException("Retries exhausted.", KeyValueResponseType.Aborted);

        } while (transactionId == HLCTimestamp.Zero && response.Type == GrpcKeyValueResponseType.TypeMustRetry);
            
        throw new KahunaException("Failed to set key/value:" + (KeyValueResponseType)response.Type, (KeyValueResponseType)response.Type);
    }

    /// <summary>
    /// Tries to retrieve a key-value pair from a distributed key-value store with the specified parameters.
    /// </summary>
    /// <param name="url">The address of the server or service endpoint.</param>
    /// <param name="transactionId">The transaction identifier used to associate the operation with a logical timeline.</param>
    /// <param name="key">The key to be retrieved from the key-value store.</param>
    /// <param name="revision">The specific revision number of the key to retrieve. Use -1 to retrieve the latest revision.</param>
    /// <param name="durability">The durability guarantee of the operation (e.g., ephemeral or persistent).</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>
    /// A tuple where:
    /// - The first item indicates whether the operation succeeded.
    /// - The second item is the value associated with the key, if found, represented as a byte array. Null if the key is not found.
    /// - The third item is the last revision number of the key retrieved.
    /// - The fourth item represents the time taken for the operation in milliseconds.
    /// </returns>
    /// <exception cref="KahunaException">Thrown if the operation fails after retries.</exception>
    public async Task<(bool, byte[]?, long, int)> TryGetKeyValue(
        string url,
        HLCTimestamp transactionId,
        string key,
        long revision,
        KeyValueDurability durability,
        CancellationToken cancellationToken
    )
    {
        GrpcTryGetKeyValueRequest request = new()
        {
            TransactionIdNode = transactionId.N,
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C,
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
            
            GrpcBatcherResponse batchResponse;
                
            if (cancellationToken == CancellationToken.None)
               batchResponse = await batcher.Enqueue(request).ConfigureAwait(false);
            else
               batchResponse = await batcher.Enqueue(request).WaitAsync(cancellationToken).ConfigureAwait(false);
            
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
                throw new KahunaException("Retries exhausted.", KeyValueResponseType.Aborted);
            
        } while (transactionId == HLCTimestamp.Zero && response.Type == GrpcKeyValueResponseType.TypeMustRetry);
            
        throw new KahunaException("Failed to get key/value:" + (KeyValueResponseType)response.Type, (KeyValueResponseType)response.Type);
    }

    /// <summary>
    /// Attempts to check the existence of a key-value pair in the storage system with a specific revision, durability, and transaction ID.
    /// </summary>
    /// <param name="url">The endpoint URL of the server to interact with.</param>
    /// <param name="transactionId">The ID of the transaction under which the key-value existence check is performed.</param>
    /// <param name="key">The key for the key-value pair to check.</param>
    /// <param name="revision">The specific revision of the key-value pair to verify existence.</param>
    /// <param name="durability">The durability level of the key-value pair (e.g., ephemeral or persistent).</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests during the operation.</param>
    /// <returns>A tuple containing a boolean indicating whether the key-value pair exists, the current revision timestamp, and the response type as an integer.</returns>
    /// <exception cref="KahunaException">Thrown if the operation fails or encounters a retryable or non-recoverable error from the server.</exception>
    public async Task<(bool, long, int)> TryExistsKeyValue(
        string url,
        HLCTimestamp transactionId,
        string key,
        long revision,
        KeyValueDurability durability,
        CancellationToken cancellationToken
    )
    {
        GrpcTryExistsKeyValueRequest request = new()
        {
            TransactionIdNode = transactionId.N,
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C,
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
        
            GrpcBatcherResponse batchResponse;
                
            if (cancellationToken == CancellationToken.None)
               batchResponse = await batcher.Enqueue(request).ConfigureAwait(false);
            else
                batchResponse = await batcher.Enqueue(request).WaitAsync(cancellationToken).ConfigureAwait(false);
            
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
                throw new KahunaException("Retries exhausted.", KeyValueResponseType.Aborted);
            
        } while (transactionId == HLCTimestamp.Zero && response.Type == GrpcKeyValueResponseType.TypeMustRetry);
            
        throw new KahunaException("Failed to check if exists key/value:" + (KeyValueResponseType)response.Type, (KeyValueResponseType)response.Type);
    }

    /// <summary>
    /// Attempts to delete a key-value pair from the storage with the specified transaction ID and durability settings.
    /// </summary>
    /// <param name="url">The endpoint URL of the server to interact with.</param>
    /// <param name="transactionId">The hybrid logical clock timestamp used to track the transaction.</param>
    /// <param name="key">The key of the key-value pair to be deleted.</param>
    /// <param name="durability">The durability type of the key-value pair (e.g., ephemeral or persistent).</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>
    /// A tuple containing a boolean indicating if the deletion was successful, a long representing the logical timestamp of the operation, and an integer indicating the number of retries performed.
    /// </returns>
    /// <exception cref="KahunaException">Thrown if the deletion operation fails or encounters an unrecoverable error.</exception>
    public async Task<(bool, long, int)> TryDeleteKeyValue(string url, HLCTimestamp transactionId, string key, KeyValueDurability durability, CancellationToken cancellationToken)
    {
        GrpcTryDeleteKeyValueRequest request = new()
        {
            TransactionIdNode = transactionId.N,
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C,
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
            
            GrpcBatcherResponse batchResponse;
                
            if (cancellationToken == CancellationToken.None)
               batchResponse = await batcher.Enqueue(request).ConfigureAwait(false);
            else
                batchResponse = await batcher.Enqueue(request).WaitAsync(cancellationToken).ConfigureAwait(false);
            
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
                throw new KahunaException("Retries exhausted.", KeyValueResponseType.Aborted);
            
        } while (transactionId == HLCTimestamp.Zero && response.Type == GrpcKeyValueResponseType.TypeMustRetry);
            
        throw new KahunaException("Failed to delete key/value: " + (KeyValueResponseType)response.Type, (KeyValueResponseType)response.Type);
    }

    /// <summary>
    /// Attempts to extend the expiry of a key in a key-value store with updated settings for expiry and durability.
    /// </summary>
    /// <param name="url">The endpoint URL of the server that manages the key-value store.</param>
    /// <param name="transactionId">The transaction ID associated with the operation.</param>
    /// <param name="key">The key in the key-value store whose expiry should be extended.</param>
    /// <param name="expiresMs">The new expiry duration in milliseconds to set for the key.</param>
    /// <param name="durability">The durability option for the key-value operation, indicating how it should be persisted (e.g., ephemeral or persistent).</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests during the operation.</param>
    /// <returns>
    /// A tuple containing a boolean that indicates whether the operation was successful,
    /// the updated expiry timestamp in ticks, and the time taken for the operation in milliseconds.
    /// </returns>
    /// <exception cref="KahunaException">Thrown when the operation fails or encounters an unrecoverable error.</exception>
    public async Task<(bool, long, int)> TryExtendKeyValue(string url, HLCTimestamp transactionId, string key, int expiresMs, KeyValueDurability durability, CancellationToken cancellationToken)
    {
        GrpcTryExtendKeyValueRequest request = new()
        {
            TransactionIdNode = transactionId.N,
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C,
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
        
            GrpcBatcherResponse batchResponse;
                
            if (cancellationToken == CancellationToken.None)
               batchResponse = await batcher.Enqueue(request).ConfigureAwait(false);
            else
                batchResponse = await batcher.Enqueue(request).WaitAsync(cancellationToken).ConfigureAwait(false);
            
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
                throw new KahunaException("Retries exhausted.", KeyValueResponseType.Aborted);
            
        } while (transactionId == HLCTimestamp.Zero && response.Type == GrpcKeyValueResponseType.TypeMustRetry);
            
        throw new KahunaException("Failed to extend key/value: " + (KeyValueResponseType)response.Type, (KeyValueResponseType)response.Type);
    }

    /// <summary>
    /// Attempts to execute a key-value transaction script on a specified server with optional parameters and hash validation.
    /// </summary>
    /// <param name="url">The endpoint URL of the server to execute the transaction script on.</param>
    /// <param name="script">The byte array containing the transaction script to be executed.</param>
    /// <param name="hash">An optional hash string used for script validation, ensuring integrity.</param>
    /// <param name="parameters">An optional list of key-value parameters to be passed to the transaction script.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>
    /// An instance of <see cref="KahunaKeyValueTransactionResult"/> representing the outcome of the transaction execution.
    /// </returns>
    /// <exception cref="KahunaException">Thrown if the operation fails, the transaction is aborted, or an unrecoverable error is encountered.</exception>
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
            
            GrpcBatcherResponse batchResponse;
                
            if (cancellationToken == CancellationToken.None)
               batchResponse = await batcher.Enqueue(request).ConfigureAwait(false);
            else
                batchResponse = await batcher.Enqueue(request).WaitAsync(cancellationToken).ConfigureAwait(false);
            
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
                throw new KahunaException("Retries exhausted.", KeyValueResponseType.Aborted);

        } while (response.Type == GrpcKeyValueResponseType.TypeMustRetry);
        
        if (!string.IsNullOrEmpty(response.Reason))
            throw new KahunaException(response.Reason, (KeyValueResponseType)response.Type);

        if (response.Type == GrpcKeyValueResponseType.TypeAborted)
            throw new KahunaException("Transaction aborted", (KeyValueResponseType)response.Type);

        throw new KahunaException("Failed to execute key/value transaction: " + (KeyValueResponseType)response.Type, (KeyValueResponseType)response.Type);
    }

    /// <summary>
    /// Attempts to acquire an exclusive key-value lock using the provided parameters.
    /// </summary>
    /// <param name="url">The endpoint URL of the server where the lock request will be executed.</param>
    /// <param name="transactionId">The high-level consistent timestamp associated with the ongoing transaction.</param>
    /// <param name="key">The key representing the resource to lock.</param>
    /// <param name="durability">The durability type of the lock, indicating whether it is ephemeral or persistent.</param>
    /// <param name="cancellationToken">A token to observe for cancellation requests while attempting to acquire the lock.</param>
    /// <returns>
    /// A task that represents the asynchronous operation. The task result is a boolean indicating whether the lock was successfully acquired.
    /// </returns>
    /// <exception cref="KahunaException">Thrown if the lock acquisition process fails or encounters an error.</exception>
    public async Task<bool> TryAcquireExclusiveKeyValueLock(string url, HLCTimestamp transactionId, string key, KeyValueDurability durability, CancellationToken cancellationToken)
    {
        GrpcTryAcquireExclusiveLockRequest request = new()
        {
            TransactionIdNode = transactionId.N,
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C,
            Key = key,             
            Durability = (GrpcKeyValueDurability)durability
        };

        int retries = 0;
        GrpcTryAcquireExclusiveLockResponse? response;               
        
        GrpcBatcher batcher = GetSharedBatcher(url);
        
        do
        {
            if (cancellationToken.IsCancellationRequested)
                throw new KahunaException("Operation cancelled", KeyValueResponseType.Aborted);
        
            GrpcBatcherResponse batchResponse;
                
            if (cancellationToken == CancellationToken.None)
               batchResponse = await batcher.Enqueue(request).ConfigureAwait(false);
            else
                batchResponse = await batcher.Enqueue(request).WaitAsync(cancellationToken).ConfigureAwait(false);
            
            response = batchResponse.TryAcquireExclusiveLock;

            if (response is null)
                throw new KahunaException("Response is null", KeyValueResponseType.Errored);

            if (response.Type == GrpcKeyValueResponseType.TypeLocked)            
                return true;
            
            if (response.Type == GrpcKeyValueResponseType.TypeMustRetry)
                logger?.LogDebug("Server asked to retry acquire key/value lock");
            
            if (++retries >= 5)
                throw new KahunaException("Retries exhausted.", KeyValueResponseType.Aborted);
            
        } while (transactionId == HLCTimestamp.Zero && response.Type == GrpcKeyValueResponseType.TypeMustRetry);
            
        throw new KahunaException("Failed to acquire key/value lock: " + (KeyValueResponseType)response.Type, (KeyValueResponseType)response.Type);
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
                Expires = new(response.ExpiresNode, response.ExpiresPhysical, response.ExpiresCounter),
                LastModified = new(response.LastModifiedNode, response.LastModifiedPhysical, response.LastModifiedCounter)
            };
            
            values.Add(responseValue);
        }
        
        return values;
    }

    /// <summary>
    /// Retrieves a list of keys that match the specified prefix from the key-value store.
    /// </summary>
    /// <param name="url">The endpoint URL of the key-value store server.</param>
    /// <param name="prefixKey">The prefix used to filter keys stored in the system.</param>
    /// <param name="durability">Specifies the durability type (e.g., ephemeral or persistent) for the request.</param>
    /// <param name="cancellationToken">A token to observe for cancellation requests during the execution of the operation.</param>
    /// <returns>
    /// A tuple where the first element is a boolean indicating the success of the operation,
    /// and the second element is a list of string keys matching the specified prefix.
    /// </returns>
    /// <exception cref="KahunaException">
    /// Thrown if the operation fails or encounters an error while attempting to retrieve keys by prefix.
    /// </exception>
    public async Task<List<KeyValueGetByPrefixItem>> GetByPrefix(string url, string prefixKey, KeyValueDurability durability, CancellationToken cancellationToken)
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
        
            GrpcBatcherResponse batchResponse;
                
            if (cancellationToken == CancellationToken.None)
               batchResponse = await batcher.Enqueue(request).ConfigureAwait(false);
            else
                batchResponse = await batcher.Enqueue(request).WaitAsync(cancellationToken).ConfigureAwait(false);
            
            response = batchResponse.GetByPrefix;

            if (response is null)
                throw new KahunaException("Response is null", KeyValueResponseType.Errored);
            
            if (response.Type == GrpcKeyValueResponseType.TypeGot)
                return response.Items.Select(x => new KeyValueGetByPrefixItem()
                {
                    Key = x.Key,
                    Value = x.Value.ToByteArray(),
                    Revision = x.Revision,
                    LastModified = new(x.LastModifiedNode, x.LastModifiedPhysical, x.LastModifiedCounter)
                }).ToList();
            
            if (response.Type == GrpcKeyValueResponseType.TypeDoesNotExist)
                return [];
            
            if (response.Type == GrpcKeyValueResponseType.TypeMustRetry)
                logger?.LogDebug("Server asked to retry get key/value by prefix");
            
            if (++retries >= 5)
                throw new KahunaException("Retries exhausted.", KeyValueResponseType.Errored);
            
        } while (response.Type == GrpcKeyValueResponseType.TypeMustRetry);
            
        throw new KahunaException("Failed to get key/value by prefix: " + (KeyValueResponseType)response.Type, (KeyValueResponseType)response.Type);
    }

    /// <summary>
    /// Scans and retrieves all key/value pairs that match a specified prefix from the target server.
    /// </summary>
    /// <param name="url">The endpoint URL of the server to query.</param>
    /// <param name="prefixKey">The prefix used to filter the keys for the scan operation.</param>
    /// <param name="durability">Specifies the durability type for the keys being scanned (e.g., ephemeral or persistent).</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests while scanning.</param>
    /// <returns>
    /// A tuple containing a boolean indicating success or failure, and a list of keys that match the specified prefix.
    /// </returns>
    /// <exception cref="KahunaException">Thrown if the scan operation encounters an error or fails to complete successfully.</exception>
    public async Task<List<KeyValueGetByPrefixItem>> ScanAllByPrefix(string url, string prefixKey, KeyValueDurability durability, CancellationToken cancellationToken)
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
            
            GrpcBatcherResponse batchResponse;
                
            if (cancellationToken == CancellationToken.None)
               batchResponse = await batcher.Enqueue(request).ConfigureAwait(false);
            else
                batchResponse = await batcher.Enqueue(request).WaitAsync(cancellationToken).ConfigureAwait(false);
            
            response = batchResponse.ScanByPrefix;

            if (response is null)
                throw new KahunaException("Response is null", KeyValueResponseType.Errored);
            
            if (response.Type == GrpcKeyValueResponseType.TypeGot)
                return response.Items.Select(x => new KeyValueGetByPrefixItem()
                {
                    Key = x.Key,
                    Value = x.Value.ToByteArray(),
                    Revision = x.Revision,
                    LastModified = new(x.LastModifiedNode, x.LastModifiedPhysical, x.LastModifiedCounter)
                }).ToList();
            
            if (response.Type == GrpcKeyValueResponseType.TypeDoesNotExist)
                return [];
            
            if (response.Type == GrpcKeyValueResponseType.TypeMustRetry)
                logger?.LogDebug("Server asked to retry scan key/value by prefix");
            
            if (++retries >= 5)
                throw new KahunaException("Retries exhausted.", KeyValueResponseType.Errored);
            
        } while (response.Type == GrpcKeyValueResponseType.TypeMustRetry);
            
        throw new KahunaException("Failed to scan key/value by prefix: " + (KeyValueResponseType)response.Type, (KeyValueResponseType)response.Type);
    }

    /// <summary>
    /// Initiates a new transactional session for key-value operations using the specified parameters.
    /// </summary>
    /// <param name="url">The endpoint URL of the server where the transaction session will be started.</param>
    /// <param name="uniqueId">A unique identifier for the transaction session.</param>
    /// <param name="txOptions">Configuration options for the transaction, such as timeout and locking type.</param>
    /// <param name="cancellationToken">A token to observe for cancellation requests while attempting to start the transaction session.</param>
    /// <returns>
    /// A tuple containing the session identifier as a string and the Hybrid Logical Clock (HLC) timestamp for the transaction.
    /// </returns>
    /// <exception cref="KahunaException">
    /// Thrown if the transaction session initiation fails or encounters an error.
    /// </exception>
    public async Task<(string, HLCTimestamp transactionId)> StartTransactionSession(string url, string uniqueId, KahunaTransactionOptions txOptions, CancellationToken cancellationToken)
    {
        GrpcStartTransactionRequest request = new()
        {
            UniqueId = uniqueId,
            Timeout = txOptions.Timeout,
            LockingType = (GrpcLockingType)txOptions.Locking,
            AsyncRelease = txOptions.AsyncRelease,
            AutoCommit = txOptions.AutoCommit
        };

        int retries = 0;
        
        GrpcBatcher batcher = GetSharedBatcher(url);
        GrpcStartTransactionResponse? response;
        
        do
        {
            if (cancellationToken.IsCancellationRequested)
                throw new KahunaException("Operation cancelled", KeyValueResponseType.Aborted);                   
            
            GrpcBatcherResponse batchResponse;
                
            if (cancellationToken == CancellationToken.None)
               batchResponse = await batcher.Enqueue(request).ConfigureAwait(false);
            else
                batchResponse = await batcher.Enqueue(request).WaitAsync(cancellationToken).ConfigureAwait(false);
            
            response = batchResponse.StartTransaction;

            if (response is null)
                throw new KahunaException("Response is null", KeyValueResponseType.Errored);

            if (response.Type == GrpcKeyValueResponseType.TypeSet)
                return new(url, new(response.TransactionIdNode, response.TransactionIdPhysical, response.TransactionIdCounter)); 
                        
            if (response.Type == GrpcKeyValueResponseType.TypeMustRetry)
                logger?.LogDebug("Server asked to retry start key/value transaction");
            
            if (++retries >= 5)
                throw new KahunaException("Retries exhausted.", KeyValueResponseType.Errored);
            
        } while (response.Type == GrpcKeyValueResponseType.TypeMustRetry);
            
        throw new KahunaException("Failed to start key/value transaction: " + (KeyValueResponseType)response.Type, (KeyValueResponseType)response.Type);
    }

    /// <summary>
    /// Commits a transaction session to the server for the specified unique identifier and transaction ID.
    /// </summary>
    /// <param name="url">The endpoint URL of the server where the transaction will be committed.</param>
    /// <param name="uniqueId">A unique identifier for the session or request being committed.</param>
    /// <param name="transactionId">The hybrid logical clock timestamp representing the transaction to be committed.</param>
    /// <param name="acquiredLocks">Acquired locks during the transaction execution</param> 
    /// <param name="modifiedKeys">Modified keys to commit</param>
    /// <param name="cancellationToken">A token to observe for cancellation requests during the transaction commit operation.</param>
    /// <returns>
    /// A boolean value indicating whether the transaction was successfully committed.
    /// </returns>
    /// <exception cref="KahunaException">
    /// Thrown if the transaction commit process encounters an error, fails, or exceeds retry limits.
    /// </exception>
    public async Task<bool> CommitTransactionSession(
        string url, 
        string uniqueId, 
        HLCTimestamp transactionId,
        List<KeyValueTransactionModifiedKey> acquiredLocks, 
        List<KeyValueTransactionModifiedKey> modifiedKeys, 
        CancellationToken cancellationToken
    )
    {
        GrpcCommitTransactionRequest request = new()
        {
            UniqueId = uniqueId,
            TransactionIdNode = transactionId.N,
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C,
        };
        
        if (acquiredLocks.Count > 0)
            request.AcquiredLocks.AddRange(GetTransactionAcquiredOrModifiedKeys(acquiredLocks));
        
        if (modifiedKeys.Count > 0)
            request.ModifiedKeys.AddRange(GetTransactionAcquiredOrModifiedKeys(modifiedKeys));

        int retries = 0;
        
        GrpcBatcher batcher = GetSharedBatcher(url);
        GrpcCommitTransactionResponse? response;
        
        do
        {
            if (cancellationToken.IsCancellationRequested)
                throw new KahunaException("Operation cancelled", KeyValueResponseType.Aborted);                   
            
            GrpcBatcherResponse batchResponse;
                
            if (cancellationToken == CancellationToken.None)
               batchResponse = await batcher.Enqueue(request).ConfigureAwait(false);
            else
                batchResponse = await batcher.Enqueue(request).WaitAsync(cancellationToken).ConfigureAwait(false);
            
            response = batchResponse.CommitTransaction;

            if (response is null)
                throw new KahunaException("Response is null", KeyValueResponseType.Errored);

            if (response.Type == GrpcKeyValueResponseType.TypeCommitted)
                return true; 
                        
            if (response.Type == GrpcKeyValueResponseType.TypeMustRetry)
                logger?.LogDebug("Server asked to retry commit key/value transaction");
            
            if (++retries >= 5)
                throw new KahunaException("Retries exhausted.", KeyValueResponseType.Errored);
            
        } while (response.Type == GrpcKeyValueResponseType.TypeMustRetry);
            
        throw new KahunaException("Failed to commit key/value transaction: " + (KeyValueResponseType)response.Type, (KeyValueResponseType)response.Type);
    }

    /// <summary>
    /// Attempts to rollback a transaction session with the specified transaction details.
    /// </summary>
    /// <param name="url">The endpoint URL of the server where the rollback request will be executed.</param>
    /// <param name="uniqueId">A unique identifier associated with the session or transaction.</param>
    /// <param name="transactionId">The HLCTimestamp representing the transaction to be rolled back.</param>
    /// <param name="modifiedKeys">Acquired locks during the transaction execution</param>
    /// <param name="modifiedKeys">Modified keys to rollback</param> 
    /// <param name="cancellationToken">A token to observe for cancellation requests during the rollback operation.</param>
    /// <returns>
    /// A boolean value indicating whether the rollback operation was successful.
    /// </returns>
    /// <exception cref="KahunaException">
    /// Thrown if the rollback operation encounters an error, retries are exhausted, or the operation is explicitly cancelled.
    /// </exception>
    public async Task<bool> RollbackTransactionSession(
        string url, 
        string uniqueId, 
        HLCTimestamp transactionId, 
        List<KeyValueTransactionModifiedKey> acquiredLocks, 
        List<KeyValueTransactionModifiedKey> modifiedKeys, 
        CancellationToken cancellationToken
    )
    {
        GrpcRollbackTransactionRequest request = new()
        {
            UniqueId = uniqueId,
            TransactionIdNode = transactionId.N,
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C,
        };
        
        if (acquiredLocks.Count > 0)
            request.AcquiredLocks.AddRange(GetTransactionAcquiredOrModifiedKeys(acquiredLocks));
        
        if (modifiedKeys.Count > 0)
            request.ModifiedKeys.AddRange(GetTransactionAcquiredOrModifiedKeys(modifiedKeys));

        int retries = 0;
        
        GrpcBatcher batcher = GetSharedBatcher(url);
        GrpcRollbackTransactionResponse? response;
        
        do
        {
            if (cancellationToken.IsCancellationRequested)
                throw new KahunaException("Operation cancelled", KeyValueResponseType.Aborted);                   
            
            GrpcBatcherResponse batchResponse;
                
            if (cancellationToken == CancellationToken.None)
               batchResponse = await batcher.Enqueue(request).ConfigureAwait(false);
            else
                batchResponse = await batcher.Enqueue(request).WaitAsync(cancellationToken).ConfigureAwait(false);
            
            response = batchResponse.RollbackTransaction;

            if (response is null)
                throw new KahunaException("Response is null", KeyValueResponseType.Errored);

            if (response.Type == GrpcKeyValueResponseType.TypeRolledback)
                return true; 
                        
            if (response.Type == GrpcKeyValueResponseType.TypeMustRetry)
                logger?.LogDebug("Server asked to retry rollback key/value transaction");
            
            if (++retries >= 5)
                throw new KahunaException("Retries exhausted.", KeyValueResponseType.Errored);
            
        } while (response.Type == GrpcKeyValueResponseType.TypeMustRetry);
            
        throw new KahunaException("Failed to rollback key/value transaction: " + (KeyValueResponseType)response.Type, (KeyValueResponseType)response.Type);
    }
    
    private static IEnumerable<GrpcTransactionModifiedKey> GetTransactionAcquiredOrModifiedKeys(List<KeyValueTransactionModifiedKey> modifiedKeys)
    {
        foreach (KeyValueTransactionModifiedKey modifiedKey in modifiedKeys)
            yield return new()
            {
                Key = modifiedKey.Key, 
                Durability = (GrpcKeyValueDurability)modifiedKey.Durability
            };
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