
using Kommander.Time;
using Kommander.Communication.Grpc;

using Google.Protobuf;
using Grpc.Net.Client;
using System.Runtime.InteropServices;
using System.Collections.Concurrent;
using Google.Protobuf.Collections;

using Kahuna.Shared.Locks;
using Kahuna.Shared.KeyValue;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Server.Locks;
using Kahuna.Server.Communication.Internode.Grpc;
using Kahuna.Server.KeyValues.Transactions.Data;

namespace Kahuna.Server.Communication.Internode;

/// <summary>
/// Provides gRPC-based inter-node communication functionalities within the Kahuna Server.
/// This implementation facilitates distributed locking, key-value operations, and other
/// inter-node coordination mechanisms.
/// </summary>
public class GrpcInterNodeCommunication : IInterNodeCommunication
{
    private static readonly ConcurrentDictionary<string, Lazy<GrpcServerBatcher>> batchers = new();
    
    private readonly KahunaConfiguration configuration;
    
    public GrpcInterNodeCommunication(KahunaConfiguration configuration)
    {
        this.configuration = configuration;
    }

    /// <summary>
    /// Attempts to acquire a distributed lock on a specified resource using gRPC communication.
    /// </summary>
    /// <param name="node">The target node to coordinate the lock request.</param>
    /// <param name="resource">The name or identifier of the resource to lock.</param>
    /// <param name="owner">A unique identifier representing the lock owner, typically in byte format.</param>
    /// <param name="expiresMs">The expiration time for the lock, in milliseconds.</param>
    /// <param name="durability">Specifies the durability level of the lock, either ephemeral or persistent.</param>
    /// <param name="cancellationToken">A token to observe cancellation requests for the operation.</param>
    /// <returns>A tuple consisting of the lock response type and a fencing token to validate lock state.</returns>
    public async Task<(LockResponseType, long)> TryLock(
        string node, 
        string resource, 
        byte[] owner, 
        int expiresMs, 
        LockDurability durability, 
        CancellationToken cancellationToken
    )
    {
        GrpcTryLockRequest request = new()
        {
            Resource = resource,
            Owner = UnsafeByteOperations.UnsafeWrap(owner),
            ExpiresMs = expiresMs,
            Durability = (GrpcLockDurability)durability
        };
        
        GrpcServerBatcher batcher = GetSharedBatcher(node);
        
        GrpcServerBatcherResponse response = await batcher.Enqueue(request).WaitAsync(cancellationToken);
        
        GrpcTryLockResponse remoteResponse = response.TryLock!;
        
        remoteResponse.ServedFrom = $"https://{node}";
        
        return ((LockResponseType)remoteResponse.Type, remoteResponse.FencingToken);
    }

    /// <summary>
    /// Attempts to extend the duration of an existing distributed lock on a specified resource using gRPC communication.
    /// </summary>
    /// <param name="node">The target node responsible for coordinating the lock extension request.</param>
    /// <param name="resource">The name or identifier of the resource whose lock is being extended.</param>
    /// <param name="owner">A unique identifier representing the lock owner, provided in byte format.</param>
    /// <param name="expiresMs">The new expiration time, in milliseconds, for the lock's validity.</param>
    /// <param name="durability">Specifies the durability level of the lock, either ephemeral or persistent.</param>
    /// <param name="cancellationToken">A token to observe cancellation requests for the operation.</param>
    /// <returns>A tuple containing the lock response type and an updated fencing token to validate the extended lock's state.</returns>
    public async Task<(LockResponseType, long)> TryExtendLock(
        string node, 
        string resource, 
        byte[] owner, 
        int expiresMs, 
        LockDurability durability, 
        CancellationToken cancellationToken
    )
    {
        GrpcExtendLockRequest request = new()
        {
            Resource = resource,
            Owner = UnsafeByteOperations.UnsafeWrap(owner),
            ExpiresMs = expiresMs,
            Durability = (GrpcLockDurability)durability
        };
        
        GrpcServerBatcher batcher = GetSharedBatcher(node);
        
        GrpcServerBatcherResponse response = await batcher.Enqueue(request).WaitAsync(cancellationToken);
        GrpcExtendLockResponse remoteResponse = response.ExtendLock!;
        
        remoteResponse.ServedFrom = $"https://{node}";
        
        return ((LockResponseType)remoteResponse.Type, remoteResponse.FencingToken);
    }

    /// <summary>
    /// Attempts to release a distributed lock on a specified resource using gRPC communication.
    /// </summary>
    /// <param name="node">The target node to coordinate the unlock request.</param>
    /// <param name="resource">The name or identifier of the resource to unlock.</param>
    /// <param name="owner">A unique identifier representing the lock owner, typically in byte format.</param>
    /// <param name="durability">Specifies the durability level of the lock, either ephemeral or persistent.</param>
    /// <param name="cancellationToken">A token to observe cancellation requests for the operation.</param>
    /// <returns>The response type indicating the result of the unlock operation.</returns>
    public async Task<LockResponseType> TryUnlock(
        string node, 
        string resource, 
        byte[] owner, 
        LockDurability durability, 
        CancellationToken cancellationToken
    )
    {
        GrpcUnlockRequest request = new()
        {
            Resource = resource,
            Owner = UnsafeByteOperations.UnsafeWrap(owner),           
            Durability = (GrpcLockDurability)durability
        };
        
        GrpcServerBatcher batcher = GetSharedBatcher(node);
        
        GrpcServerBatcherResponse response = await batcher.Enqueue(request).WaitAsync(cancellationToken);
        GrpcUnlockResponse remoteResponse = response.Unlock!;
        
        remoteResponse.ServedFrom = $"https://{node}";
        
        return (LockResponseType)remoteResponse.Type;
    }

    /// <summary>
    /// Requests a distributed lock from the specified node for the given resource.
    /// </summary>
    /// <param name="node">The target node to coordinate the lock request.</param>
    /// <param name="resource">The name or identifier of the resource to be locked.</param>
    /// <param name="durability">Specifies the durability level of the lock, either ephemeral or persistent.</param>
    /// <param name="cancellationToken">A token to observe cancellation requests for the operation.</param>
    /// <returns>A tuple consisting of the lock response type and an optional lock context containing ownership and lock metadata.</returns>
    public async Task<(LockResponseType, ReadOnlyLockContext?)> GetLock(
        string node, 
        string resource, 
        LockDurability durability, 
        CancellationToken cancellationToken
    )
    {
        GrpcGetLockRequest request = new()
        {
            Resource = resource,
            Durability = (GrpcLockDurability)durability
        };               
        
        GrpcServerBatcher batcher = GetSharedBatcher(node);
        
        GrpcServerBatcherResponse response = await batcher.Enqueue(request).WaitAsync(cancellationToken);
        GrpcGetLockResponse remoteResponse = response.GetLock!;
        
        if (remoteResponse.Type != GrpcLockResponseType.LockResponseTypeGot)
            return ((LockResponseType)remoteResponse.Type, null);
        
        byte[]? owner;
            
        if (MemoryMarshal.TryGetArray(remoteResponse.Owner.Memory, out ArraySegment<byte> segment))
            owner = segment.Array;
        else
            owner = remoteResponse.Owner.ToByteArray();

        return ((LockResponseType)remoteResponse.Type,
            new(
                owner, 
                remoteResponse.FencingToken,
                new(remoteResponse.ExpiresNode, remoteResponse.ExpiresPhysical, remoteResponse.ExpiresCounter)
            )
        );
    }

    /// <summary>
    /// Redirects a set key/value operation to the specified node.
    /// </summary>
    /// <param name="node">The target node managing the key-value operation.</param>
    /// <param name="transactionId">The unique transaction ID represented as an HLC timestamp.</param>
    /// <param name="key">The key to be set or modified in the distributed store.</param>
    /// <param name="value">The value to be associated with the specified key, or null if no value is to be set.</param>
    /// <param name="compareValue">Optional value to compare with the current value for conditional set operations.</param>
    /// <param name="compareRevision">Optional revision number to compare with the current revision for conditional set operations.</param>
    /// <param name="flags">Flags specifying the operation type or conditional rules.</param>
    /// <param name="expiresMs">The time-to-live (TTL) for the key-value pair, in milliseconds.</param>
    /// <param name="durability">The durability level for the operation, either ephemeral or persistent.</param>
    /// <param name="cancellationToken">A token to observe cancellation requests for the operation.</param>
    /// <returns>A tuple consisting of the operation's response type, the revision number of the key-value pair, and the timestamp of the last modification.</returns>
    public async Task<(KeyValueResponseType, long, HLCTimestamp)> TrySetKeyValue(
        string node,
        HLCTimestamp transactionId,
        string key,
        byte[]? value,
        byte[]? compareValue,
        long compareRevision,
        KeyValueFlags flags,
        int expiresMs,
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
            CompareRevision = compareRevision,
            Flags = (GrpcKeyValueFlags) flags,
            ExpiresMs = expiresMs,
            Durability = (GrpcKeyValueDurability) durability,
        };
        
        if (value is not null)
            request.Value = UnsafeByteOperations.UnsafeWrap(value);
        
        if (compareValue is not null)
            request.CompareValue = UnsafeByteOperations.UnsafeWrap(compareValue);               
        
        GrpcServerBatcher batcher = GetSharedBatcher(node);
                       
        GrpcServerBatcherResponse response = await batcher.Enqueue(request).WaitAsync(cancellationToken);
        GrpcTrySetKeyValueResponse remoteResponse = response.TrySetKeyValue!;
        
        remoteResponse.ServedFrom = $"https://{node}";
        
        return (
            (KeyValueResponseType)remoteResponse.Type, 
            remoteResponse.Revision, 
            new(remoteResponse.LastModifiedNode, remoteResponse.LastModifiedPhysical, remoteResponse.LastModifiedCounter)
        );
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="node"></param>
    /// <param name="items"></param>
    /// <param name="lockSync"></param>
    /// <param name="responses"></param>
    /// <param name="cancellationToken"></param>
    public async Task TrySetManyNodeKeyValue(
        string node, 
        List<KahunaSetKeyValueRequestItem> items, 
        Lock lockSync, 
        List<KahunaSetKeyValueResponseItem> responses, 
        CancellationToken cancellationToken
    )
    {
        GrpcTrySetManyKeyValueRequest request = new();
            
        request.Items.Add(GetSetManyRequestItems(items));
        
        GrpcServerBatcher batcher = GetSharedBatcher(node);
        
        GrpcServerBatcherResponse response = await batcher.Enqueue(request).WaitAsync(cancellationToken);
        GrpcTrySetManyKeyValueResponse remoteResponse = response.TrySetManyKeyValue!;

        lock (lockSync)
        {
            foreach (GrpcTrySetManyKeyValueResponseItem item in remoteResponse.Items)
                responses.Add(new()
                {
                    Key = item.Key, 
                    Type = (KeyValueResponseType)item.Type, 
                    Revision = item.Revision, 
                    LastModified = new(item.LastModifiedNode, item.LastModifiedPhysical, item.LastModifiedCounter),
                    Durability = (KeyValueDurability)item.Durability,
                });
        }
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="items"></param>
    /// <returns></returns>
    private static IEnumerable<GrpcTrySetManyKeyValueRequestItem> GetSetManyRequestItems(List<KahunaSetKeyValueRequestItem> items)
    {
        foreach (KahunaSetKeyValueRequestItem item in items)
        {
            GrpcTrySetManyKeyValueRequestItem grpcItem = new()
            {
                TransactionIdNode = item.TransactionId.N,
                TransactionIdPhysical = item.TransactionId.L,
                TransactionIdCounter = item.TransactionId.C,
                Key = item.Key,
                CompareRevision = item.CompareRevision,
                Flags = (GrpcKeyValueFlags) item.Flags,
                ExpiresMs = item.ExpiresMs,
                Durability = (GrpcKeyValueDurability) item.Durability
            };

            if (item.Value is not null)
                grpcItem.Value = UnsafeByteOperations.UnsafeWrap(item.Value);

            if (item.CompareValue is not null)
                grpcItem.CompareValue = UnsafeByteOperations.UnsafeWrap(item.CompareValue);

            yield return grpcItem;
        }
    }

    /// <summary>
    /// Redirects a delete key/value operation to the specified node.
    /// </summary>
    /// <param name="node">The target node where the key-value deletion request is directed.</param>
    /// <param name="transactionId">The unique transaction identifier for maintaining atomicity across operations.</param>
    /// <param name="key">The key identifying the specific key-value pair to be deleted.</param>
    /// <param name="durability">Specifies the durability level of the key-value operation, either ephemeral or persistent.</param>
    /// <param name="cancellationToken">A token to observe cancellation requests for the operation.</param>
    /// <returns>A tuple containing the response type of the operation, the updated revision number, and the last modified timestamp for the key.</returns>
    public async Task<(KeyValueResponseType, long, HLCTimestamp)> TryDeleteKeyValue(
        string node,
        HLCTimestamp transactionId,
        string key,
        KeyValueDurability durability,
        CancellationToken cancellationToken
    )
    {
        GrpcTryDeleteKeyValueRequest request = new()
        {
            TransactionIdNode = transactionId.N,
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C,
            Key = key,
            Durability = (GrpcKeyValueDurability)durability,
        };               
        
        GrpcServerBatcher batcher = GetSharedBatcher(node);
        
        GrpcServerBatcherResponse response = await batcher.Enqueue(request).WaitAsync(cancellationToken);
        GrpcTryDeleteKeyValueResponse remoteResponse = response.TryDeleteKeyValue!;
        
        remoteResponse.ServedFrom = $"https://{node}";
        
        return (
            (KeyValueResponseType)remoteResponse.Type, 
            remoteResponse.Revision, 
            new(remoteResponse.LastModifiedNode, remoteResponse.LastModifiedPhysical, remoteResponse.LastModifiedCounter)
        );
    }

    /// <summary>
    /// Redirects an extend key/value operation to the specified node.
    /// </summary>
    /// <param name="node">The target node where the key-value pair resides.</param>
    /// <param name="transactionId">The unique transaction identifier, represented by a hybrid logical clock timestamp.</param>
    /// <param name="key">The key of the key-value pair to be extended.</param>
    /// <param name="expiresMs">The new expiration time for the key-value pair, in milliseconds.</param>
    /// <param name="durability">Specifies the durability level of the extension, either ephemeral or persistent.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests during the operation.</param>
    /// <returns>A tuple containing the key-value response type, the latest revision of the key, and the last modification timestamp.</returns>
    public async Task<(KeyValueResponseType, long, HLCTimestamp)> TryExtendKeyValue(
        string node,
        HLCTimestamp transactionId,
        string key,
        int expiresMs,
        KeyValueDurability durability,
        CancellationToken cancellationToken
    )
    {
        GrpcTryExtendKeyValueRequest request = new()
        {
            TransactionIdNode = transactionId.N,
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C,
            Key = key,
            ExpiresMs = expiresMs,
            Durability = (GrpcKeyValueDurability)durability,
        };               
        
        GrpcServerBatcher batcher = GetSharedBatcher(node);
        
        GrpcServerBatcherResponse response = await batcher.Enqueue(request).WaitAsync(cancellationToken);
        GrpcTryExtendKeyValueResponse remoteResponse = response.TryExtendKeyValue!;
        
        remoteResponse.ServedFrom = $"https://{node}";
        
        return (
            (KeyValueResponseType)remoteResponse.Type, 
            remoteResponse.Revision, 
            new(remoteResponse.LastModifiedNode, remoteResponse.LastModifiedPhysical, remoteResponse.LastModifiedCounter)
        );
    }

    /// <summary>
    /// Redirects a "get" key/value operation to the specified node.
    /// </summary>
    /// <param name="node">The target node from which the key's value will be retrieved.</param>
    /// <param name="transactionId">The transaction identifier used to maintain consistency and ordering.</param>
    /// <param name="key">The key corresponding to the value being retrieved.</param>
    /// <param name="revision">The specific revision of the key to fetch.</param>
    /// <param name="durability">The durability type indicating whether the operation is ephemeral or persistent.</param>
    /// <param name="cancellationToken">A token to observe any cancellation requests for the operation.</param>
    /// <returns>A tuple containing the type of key-value response and an optional read-only key-value context with the retrieved value and metadata.</returns>
    public async Task<(KeyValueResponseType, ReadOnlyKeyValueContext?)> TryGetValue(
        string node,
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
            Durability = (GrpcKeyValueDurability) durability,
        };
        
        GrpcServerBatcher batcher = GetSharedBatcher(node);               
        
        GrpcServerBatcherResponse response = await batcher.Enqueue(request).WaitAsync(cancellationToken);
        GrpcTryGetKeyValueResponse remoteResponse = response.TryGetKeyValue!;
        
        remoteResponse.ServedFrom = $"https://{node}";
        
        byte[]? value;
            
        if (MemoryMarshal.TryGetArray(remoteResponse.Value.Memory, out ArraySegment<byte> segment))
            value = segment.Array;
        else
            value = remoteResponse.Value.ToByteArray();
        
        return ((KeyValueResponseType)remoteResponse.Type, new(
            value,
            remoteResponse.Revision,
            new(remoteResponse.ExpiresNode, remoteResponse.ExpiresPhysical, remoteResponse.ExpiresCounter),
            new(remoteResponse.LastUsedNode, remoteResponse.LastUsedPhysical, remoteResponse.LastUsedCounter),
            new(remoteResponse.LastModifiedNode, remoteResponse.LastModifiedPhysical, remoteResponse.LastModifiedCounter),
            (KeyValueState)remoteResponse.State
        ));
    }

    /// <summary>
    /// Redirects an "exists" key/value operation to the specified node.
    /// </summary>
    /// <param name="node">The target node where the operation is to be performed.</param>
    /// <param name="transactionId">The hybrid logical clock timestamp associated with the transaction.</param>
    /// <param name="key">The key of the key-value pair to check for existence.</param>
    /// <param name="revision">The specific revision number of the key-value pair to verify.</param>
    /// <param name="durability">The durability level to determine if the operation should be ephemeral or persistent.</param>
    /// <param name="cancellationToken">A cancellation token to signal the operation should be aborted.</param>
    /// <returns>A tuple containing the key-value response type and an optional read-only context if the key-value pair exists.</returns>
    public async Task<(KeyValueResponseType, ReadOnlyKeyValueContext?)> TryExistsValue(
        string node,
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
            Durability = (GrpcKeyValueDurability) durability,
        };               
        
        GrpcServerBatcher batcher = GetSharedBatcher(node);
        
        GrpcServerBatcherResponse response = await batcher.Enqueue(request).WaitAsync(cancellationToken);
        GrpcTryExistsKeyValueResponse remoteResponse = response.TryExistsKeyValue!;
        
        remoteResponse.ServedFrom = $"https://{node}";
        
        return ((KeyValueResponseType)remoteResponse.Type, new(
            null,
            remoteResponse.Revision,
            new(remoteResponse.ExpiresNode, remoteResponse.ExpiresPhysical, remoteResponse.ExpiresCounter),
            new(remoteResponse.LastUsedNode, remoteResponse.LastUsedPhysical, remoteResponse.LastUsedCounter),
            new(remoteResponse.LastModifiedNode, remoteResponse.LastModifiedPhysical, remoteResponse.LastModifiedCounter),
            (KeyValueState)remoteResponse.State
        ));
    }

    /// <summary>
    /// Redirects an "acquire-exclusive-lock" key/value operation to the specified node.
    /// </summary>
    /// <param name="node">The target node to initiate the lock coordination.</param>
    /// <param name="transactionId">A high-precision logical clock timestamp identifying the transaction.</param>
    /// <param name="key">The key or identifier for the resource to be exclusively locked.</param>
    /// <param name="expiresMs">The duration in milliseconds for which the lock will remain valid unless explicitly released.</param>
    /// <param name="durability">The desired durability level of the lock, either ephemeral or persistent.</param>
    /// <param name="cancellationToken">A token to observe cancellation requests for the operation.</param>
    /// <returns>A tuple containing the type of the response, the key, and the lock's durability level.</returns>
    public async Task<(KeyValueResponseType, string, KeyValueDurability)> TryAcquireExclusiveLock(
        string node,
        HLCTimestamp transactionId,
        string key,
        int expiresMs,
        KeyValueDurability durability,
        CancellationToken cancellationToken
    )
    {        
        GrpcServerBatcher batcher = GetSharedBatcher(node);               
        
        GrpcTryAcquireExclusiveLockRequest request = new()
        {
            TransactionIdNode = transactionId.N,
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C,
            Key = key,
            ExpiresMs = expiresMs,
            Durability = (GrpcKeyValueDurability)durability,
        };
        
        GrpcServerBatcherResponse response = await batcher.Enqueue(request).WaitAsync(cancellationToken);
        GrpcTryAcquireExclusiveLockResponse remoteResponse = response.TryAcquireExclusiveLock!;
        
        remoteResponse.ServedFrom = $"https://{node}";
        
        return ((KeyValueResponseType)remoteResponse.Type, key, durability);
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="node"></param>
    /// <param name="transactionId"></param>
    /// <param name="prefixKey"></param>
    /// <param name="expiresMs"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    /// <exception cref="NotImplementedException"></exception>
    public async Task<KeyValueResponseType> TryAcquireExclusivePrefixLock(
        string node, 
        HLCTimestamp transactionId, 
        string prefixKey, 
        int expiresMs, 
        KeyValueDurability durability, 
        CancellationToken cancellationToken
    )
    {
        GrpcServerBatcher batcher = GetSharedBatcher(node);               
        
        GrpcTryAcquireExclusivePrefixLockRequest request = new()
        {
            TransactionIdNode = transactionId.N,
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C,
            PrefixKey = prefixKey,
            ExpiresMs = expiresMs,
            Durability = (GrpcKeyValueDurability)durability,
        };
        
        GrpcServerBatcherResponse response = await batcher.Enqueue(request).WaitAsync(cancellationToken);
        GrpcTryAcquireExclusivePrefixLockResponse remoteResponse = response.TryAcquireExclusivePrefixLock!;
        
        remoteResponse.ServedFrom = $"https://{node}";

        return (KeyValueResponseType)remoteResponse.Type;
    }

    /// <summary>
    /// Attempts to acquire exclusive locks on a set of specified keys at a given node.
    /// </summary>
    /// <param name="node">The identifier of the target node on which locks need to be acquired.</param>
    /// <param name="transactionId">The unique transaction ID used to associate the locking operation.</param>
    /// <param name="xkeys">A list of keys to be locked, each with its expiration time and durability level.</param>
    /// <param name="lockSync">The synchronization mechanism to ensure thread safety for lock-related operations.</param>
    /// <param name="responses">A collection used to capture the response types and metadata for each attempted lock.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests during the operation.</param>
    /// <returns>A task that represents the asynchronous operation of attempting to acquire the specified locks.</returns>
    public async Task TryAcquireNodeExclusiveLocks(
        string node,
        HLCTimestamp transactionId,
        List<(string key, int expiresMs, KeyValueDurability durability)> xkeys,
        Lock lockSync,
        List<(KeyValueResponseType type, string key, KeyValueDurability durability)> responses,
        CancellationToken cancellationToken
    )
    {
        GrpcServerBatcher batcher = GetSharedBatcher(node);
            
        GrpcTryAcquireManyExclusiveLocksRequest request = new()
        {
            TransactionIdNode = transactionId.N,
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C
        };
            
        request.Items.Add(GetAcquireLockRequestItems(xkeys));
        
        GrpcServerBatcherResponse response = await batcher.Enqueue(request).WaitAsync(cancellationToken);
        GrpcTryAcquireManyExclusiveLocksResponse remoteResponse = response.TryAcquireManyExclusiveLocks!;

        lock (lockSync)
        {
            foreach (GrpcTryAcquireManyExclusiveLocksResponseItem item in remoteResponse.Items)
                responses.Add(((KeyValueResponseType)item.Type, item.Key, (KeyValueDurability)item.Durability));
        }
    }
    
    private static IEnumerable<GrpcTryAcquireManyExclusiveLocksRequestItem> GetAcquireLockRequestItems(List<(string key, int expiresMs, KeyValueDurability durability)> xkeys)
    {
        foreach ((string key, int expiresMs, KeyValueDurability durability) key in xkeys)
            yield return new()
            {
                Key = key.key,
                ExpiresMs = key.expiresMs,
                Durability = (GrpcKeyValueDurability)key.durability
            };
    }

    public async Task<(KeyValueResponseType, string)> TryReleaseExclusiveLock(
        string node, 
        HLCTimestamp transactionId, 
        string key, 
        KeyValueDurability durability, 
        CancellationToken cancellationToken
    )
    {
        GrpcServerBatcher batcher = GetSharedBatcher(node);
        
        GrpcTryReleaseExclusiveLockRequest request = new()
        {
            TransactionIdNode = transactionId.N,
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C,
            Key = key,
            Durability = (GrpcKeyValueDurability)durability,
        };
        
        GrpcServerBatcherResponse response = await batcher.Enqueue(request).WaitAsync(cancellationToken);
        GrpcTryReleaseExclusiveLockResponse remoteResponse = response.TryReleaseExclusiveLock!;
        
        remoteResponse.ServedFrom = $"https://{node}";
        
        return ((KeyValueResponseType)remoteResponse.Type, key);
    }
    
    /// <summary>
    /// 
    /// </summary>
    /// <param name="node"></param>
    /// <param name="transactionId"></param>
    /// <param name="prefixKey"></param>
    /// <param name="expiresMs"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public async Task<KeyValueResponseType> TryReleaseExclusivePrefixLock(
        string node, 
        HLCTimestamp transactionId, 
        string prefixKey, 
        KeyValueDurability durability, 
        CancellationToken cancellationToken
    )
    {
        GrpcServerBatcher batcher = GetSharedBatcher(node);               
        
        GrpcTryReleaseExclusivePrefixLockRequest request = new()
        {
            TransactionIdNode = transactionId.N,
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C,
            PrefixKey = prefixKey,
            Durability = (GrpcKeyValueDurability)durability,
        };
        
        GrpcServerBatcherResponse response = await batcher.Enqueue(request).WaitAsync(cancellationToken);
        GrpcTryReleaseExclusivePrefixLockResponse remoteResponse = response.TryReleaseExclusivePrefixLock!;
        
        remoteResponse.ServedFrom = $"https://{node}";

        return (KeyValueResponseType)remoteResponse.Type;
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="node"></param>
    /// <param name="transactionId"></param>
    /// <param name="xkeys"></param>
    /// <param name="lockSync"></param>
    /// <param name="responses"></param>
    /// <param name="cancellationToken"></param>
    public async Task TryReleaseNodeExclusiveLocks(
        string node, 
        HLCTimestamp transactionId, 
        List<(string key, KeyValueDurability durability)> xkeys, 
        Lock lockSync, 
        List<(KeyValueResponseType type, string key, KeyValueDurability durability)> responses, 
        CancellationToken cancellationToken
    )
    {
        GrpcServerBatcher batcher = GetSharedBatcher(node);
            
        GrpcTryReleaseManyExclusiveLocksRequest request = new()
        {
            TransactionIdNode = transactionId.N,
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C
        };
            
        request.Items.Add(GetReleaseLockRequestItems(xkeys));
        
        GrpcServerBatcherResponse response = await batcher.Enqueue(request);
        GrpcTryReleaseManyExclusiveLocksResponse remoteResponse = response.TryReleaseManyExclusiveLocks!;

        lock (lockSync)
        {
            foreach (GrpcTryReleaseManyExclusiveLocksResponseItem item in remoteResponse.Items)
                responses.Add(((KeyValueResponseType)item.Type, item.Key, (KeyValueDurability)item.Durability));
        }
    }
    
    private static IEnumerable<GrpcTryReleaseManyExclusiveLocksRequestItem> GetReleaseLockRequestItems(List<(string key, KeyValueDurability durability)> xkeys)
    {
        foreach ((string key, KeyValueDurability durability) key in xkeys)
            yield return new()
            {
                Key = key.key,
                Durability = (GrpcKeyValueDurability)key.durability
            };
    }
    
    public async Task<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)> TryPrepareMutations(
        string node, HLCTimestamp transactionId, 
        HLCTimestamp commitId, 
        string key, 
        KeyValueDurability durability, 
        CancellationToken cancellationToken
    )
    {
        //GrpcChannel channel = SharedChannels.GetChannel(node);
        //KeyValuer.KeyValuerClient client = new(channel);
        
        GrpcServerBatcher batcher = GetSharedBatcher(node);
        
        GrpcTryPrepareMutationsRequest request = new()
        {
            TransactionIdNode = transactionId.N,
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C,
            CommitIdNode = commitId.N,
            CommitIdPhysical = commitId.L,
            CommitIdCounter = commitId.C,
            Key = key,
            Durability = (GrpcKeyValueDurability)durability
        };
        
        GrpcServerBatcherResponse response = await batcher.Enqueue(request);
        GrpcTryPrepareMutationsResponse remoteResponse = response.TryPrepareMutations!;
        
        remoteResponse.ServedFrom = $"https://{node}";
        
        return (
            (KeyValueResponseType)remoteResponse.Type, 
            new(remoteResponse.ProposalTicketNode, remoteResponse.ProposalTicketPhysical, remoteResponse.ProposalTicketCounter), 
            key, 
            durability
        );
    }

    public async Task TryPrepareNodeMutations(
        string node, 
        HLCTimestamp transactionId,
        HLCTimestamp commitId,
        List<(string key, KeyValueDurability durability)> xkeys, 
        Lock lockSync, 
        List<(KeyValueResponseType type, HLCTimestamp, string key, KeyValueDurability durability)> responses, 
        CancellationToken cancellationToken
    )
    {
        GrpcServerBatcher batcher = GetSharedBatcher(node);
            
        GrpcTryPrepareManyMutationsRequest request = new()
        {
            TransactionIdNode = transactionId.N,
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C,
            CommitIdNode = commitId.N,
            CommitIdPhysical = commitId.L,
            CommitIdCounter = commitId.C,
        };
            
        request.Items.Add(GetPrepareRequestItems(xkeys));
        
        GrpcServerBatcherResponse response = await batcher.Enqueue(request);
        GrpcTryPrepareManyMutationsResponse remoteResponse = response.TryPrepareManyMutations!;

        lock (lockSync)
        {
            foreach (GrpcTryPrepareManyMutationsResponseItem item in remoteResponse.Items)
                responses.Add((
                    (KeyValueResponseType)item.Type, 
                    new(item.ProposalTicketNode, item.ProposalTicketPhysical, item.ProposalTicketCounter), 
                    item.Key, 
                    (KeyValueDurability)item.Durability
                ));
        }
    }
    
    private static IEnumerable<GrpcTryPrepareManyMutationsRequestItem> GetPrepareRequestItems(List<(string key, KeyValueDurability durability)> xkeys)
    {
        foreach ((string key, KeyValueDurability durability) key in xkeys)
            yield return new()
            {
                Key = key.key,
                Durability = (GrpcKeyValueDurability)key.durability
            };
    }

    public async Task<(KeyValueResponseType, long)> TryCommitMutations(string node, HLCTimestamp transactionId, string key, HLCTimestamp ticketId, KeyValueDurability durability, CancellationToken cancellationToken)
    {
        GrpcServerBatcher batcher = GetSharedBatcher(node);
        
        GrpcTryCommitMutationsRequest request = new()
        {
            TransactionIdNode = transactionId.N,
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C,
            Key = key,
            ProposalTicketNode = ticketId.N,
            ProposalTicketPhysical = ticketId.L,
            ProposalTicketCounter = ticketId.C,
            Durability = (GrpcKeyValueDurability)durability,
        };
        
        GrpcServerBatcherResponse response = await batcher.Enqueue(request);
        GrpcTryCommitMutationsResponse remoteResponse = response.TryCommitMutations!;
        
        remoteResponse.ServedFrom = $"https://{node}";
        
        return ((KeyValueResponseType)remoteResponse.Type, remoteResponse.ProposalIndex);
    }

    public async Task TryCommitNodeMutations(string node, HLCTimestamp transactionId, List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)> xkeys, Lock lockSync, List<(KeyValueResponseType type, string key, long, KeyValueDurability durability)> responses, CancellationToken cancellationToken)
    {
        GrpcServerBatcher batcher = GetSharedBatcher(node);
            
        GrpcTryCommitManyMutationsRequest request = new()
        {
            TransactionIdNode = transactionId.N,
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C
        };
            
        request.Items.Add(GetCommitRequestItems(xkeys));
            
        GrpcServerBatcherResponse response = await batcher.Enqueue(request);
        GrpcTryCommitManyMutationsResponse remoteResponse = response.TryCommitManyMutations!;

        lock (lockSync)
        {
            foreach (GrpcTryCommitManyMutationsResponseItem item in remoteResponse.Items)
                responses.Add(((KeyValueResponseType)item.Type, item.Key, item.ProposalIndex, (KeyValueDurability)item.Durability));
        }
    }
    
    private static IEnumerable<GrpcTryCommitManyMutationsRequestItem> GetCommitRequestItems(List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)> xkeys)
    {
        foreach ((string key, HLCTimestamp ticketId, KeyValueDurability durability) key in xkeys)
            yield return new()
            {
                Key = key.key,
                ProposalTicketNode = key.ticketId.N,
                ProposalTicketPhysical = key.ticketId.L,
                ProposalTicketCounter = key.ticketId.C,
                Durability = (GrpcKeyValueDurability)key.durability
            };
    }

    public async Task<(KeyValueResponseType, long)> TryRollbackMutations(string node, HLCTimestamp transactionId, string key, HLCTimestamp ticketId, KeyValueDurability durability, CancellationToken cancellationToken)
    {
        GrpcServerBatcher batcher = GetSharedBatcher(node);
        
        GrpcTryRollbackMutationsRequest request = new()
        {
            TransactionIdNode = transactionId.N,
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C,
            Key = key,
            ProposalTicketNode = ticketId.N,
            ProposalTicketPhysical = ticketId.L,
            ProposalTicketCounter = ticketId.C,
            Durability = (GrpcKeyValueDurability)durability,
        };
        
        GrpcServerBatcherResponse response = await batcher.Enqueue(request);
        GrpcTryRollbackMutationsResponse remoteResponse = response.TryRollbackMutations!;
        
        remoteResponse.ServedFrom = $"https://{node}";
        
        return ((KeyValueResponseType)remoteResponse.Type, remoteResponse.ProposalIndex);
    }
    
    public async Task TryRollbackNodeMutations(string node, HLCTimestamp transactionId, List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)> xkeys, Lock lockSync, List<(KeyValueResponseType type, string key, long, KeyValueDurability durability)> responses, CancellationToken cancellationToken)
    {
        GrpcServerBatcher batcher = GetSharedBatcher(node);
            
        GrpcTryRollbackManyMutationsRequest request = new()
        {
            TransactionIdNode = transactionId.N,
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C
        };
            
        request.Items.Add(GetRollbackRequestItems(xkeys));
            
        GrpcServerBatcherResponse response = await batcher.Enqueue(request);
        GrpcTryRollbackManyMutationsResponse remoteResponse = response.TryRollbackManyMutations!;

        lock (lockSync)
        {
            foreach (GrpcTryRollbackManyMutationsResponseItem item in remoteResponse.Items)
                responses.Add(((KeyValueResponseType)item.Type, item.Key, item.ProposalIndex, (KeyValueDurability)item.Durability));
        }
    }
    
    private static IEnumerable<GrpcTryRollbackManyMutationsRequestItem> GetRollbackRequestItems(List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)> xkeys)
    {
        foreach ((string key, HLCTimestamp ticketId, KeyValueDurability durability) key in xkeys)
            yield return new()
            {
                Key = key.key,
                ProposalTicketNode = key.ticketId.N,
                ProposalTicketPhysical = key.ticketId.L,
                ProposalTicketCounter = key.ticketId.C,
                Durability = (GrpcKeyValueDurability)key.durability
            };
    }
    
    /// <summary>
    /// 
    /// </summary>
    /// <param name="node"></param>
    /// <param name="transactionId"></param>
    /// <param name="prefixedKey"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public async Task<KeyValueGetByPrefixResult> GetByPrefix(string node, HLCTimestamp transactionId, string prefixedKey, KeyValueDurability durability, CancellationToken cancellationToken)
    {
        GrpcServerBatcher batcher = GetSharedBatcher(node);
        
        GrpcGetByPrefixRequest request = new()
        {
            TransactionIdNode = transactionId.N,
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C,
            PrefixKey = prefixedKey,
            Durability = (GrpcKeyValueDurability)durability,
        };
        
        GrpcServerBatcherResponse batchResponse;
                              
        if (cancellationToken == CancellationToken.None)
           batchResponse = await batcher.Enqueue(request).ConfigureAwait(false);
        else
           batchResponse = await batcher.Enqueue(request).WaitAsync(cancellationToken).ConfigureAwait(false);
       
        GrpcGetByPrefixResponse remoteResponse = batchResponse.GetByPrefix!;
        
        remoteResponse.ServedFrom = $"https://{node}";
        
        return new((KeyValueResponseType)remoteResponse.Type, GetReadOnlyItem(remoteResponse.Items));
    }
    
    /// <summary>
    /// 
    /// </summary>
    /// <param name="node"></param>    
    /// <param name="prefixedKey"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public async Task<KeyValueGetByPrefixResult> ScanByPrefix(string node, string prefixedKey, KeyValueDurability durability, CancellationToken cancellationToken)
    {
        GrpcServerBatcher batcher = GetSharedBatcher(node);
        
        GrpcScanByPrefixRequest request = new()
        {            
            PrefixKey = prefixedKey,
            Durability = (GrpcKeyValueDurability)durability,
        };
        
        GrpcServerBatcherResponse batchResponse;
                              
        if (cancellationToken == CancellationToken.None)
           batchResponse = await batcher.Enqueue(request).ConfigureAwait(false);
        else
           batchResponse = await batcher.Enqueue(request).WaitAsync(cancellationToken).ConfigureAwait(false);
       
        GrpcScanByPrefixResponse remoteResponse = batchResponse.ScanByPrefix!;
        
        remoteResponse.ServedFrom = $"https://{node}";
        
        return new((KeyValueResponseType)remoteResponse.Type, GetReadOnlyItem(remoteResponse.Items));
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="node"></param>
    /// <param name="options"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, HLCTimestamp)> StartTransaction(string node, KeyValueTransactionOptions options, CancellationToken cancellationToken)
    {
        GrpcServerBatcher batcher = GetSharedBatcher(node);
        
        GrpcStartTransactionRequest request = new()
        {
            UniqueId = options.UniqueId,        
            LockingType = (GrpcLockingType)options.Locking,
            Timeout = options.Timeout,
            AsyncRelease = options.AsyncRelease,
            AutoCommit = options.AutoCommit,
        };
        
        GrpcServerBatcherResponse response = await batcher.Enqueue(request);
        GrpcStartTransactionResponse remoteResponse = response.StartTransaction!;
        
        remoteResponse.ServedFrom = $"https://{node}";
        
        return (
            (KeyValueResponseType)remoteResponse.Type, 
            new(remoteResponse.TransactionIdNode, remoteResponse.TransactionIdPhysical, remoteResponse.TransactionIdCounter)
        );
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="node"></param>
    /// <param name="uniqueId"></param>
    /// <param name="timestamp"></param>
    /// <param name="acquiredLocks"></param>
    /// <param name="modifiedKeys"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public async Task<KeyValueResponseType> CommitTransaction(
        string node, 
        string uniqueId, 
        HLCTimestamp timestamp, 
        List<KeyValueTransactionModifiedKey> acquiredLocks, 
        List<KeyValueTransactionModifiedKey> modifiedKeys, 
        CancellationToken cancellationToken
    )
    {
        GrpcServerBatcher batcher = GetSharedBatcher(node);
        
        GrpcCommitTransactionRequest request = new()
        {
            UniqueId = uniqueId,
            TransactionIdNode = timestamp.N,
            TransactionIdPhysical = timestamp.L,
            TransactionIdCounter = timestamp.C            
        };
        
        if (acquiredLocks.Count > 0)
            request.AcquiredLocks.AddRange(GetArquiredOrModifiedItems(acquiredLocks));
        
        if (modifiedKeys.Count > 0)
            request.ModifiedKeys.AddRange(GetArquiredOrModifiedItems(modifiedKeys));
        
        GrpcServerBatcherResponse response = await batcher.Enqueue(request);
        GrpcCommitTransactionResponse remoteResponse = response.CommitTransaction!;
        
        remoteResponse.ServedFrom = $"https://{node}";
        
        return (KeyValueResponseType)remoteResponse.Type;
    }    

    /// <summary>
    /// 
    /// </summary>
    /// <param name="node"></param>
    /// <param name="uniqueId"></param>
    /// <param name="timestamp"></param>
    /// <param name="acquiredLocks"></param>
    /// <param name="modifiedKeys"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public async Task<KeyValueResponseType> RollbackTransaction(
        string node, 
        string uniqueId, 
        HLCTimestamp timestamp, 
        List<KeyValueTransactionModifiedKey> acquiredLocks, 
        List<KeyValueTransactionModifiedKey> modifiedKeys, 
        CancellationToken cancellationToken
    )
    {
        GrpcServerBatcher batcher = GetSharedBatcher(node);
        
        GrpcRollbackTransactionRequest request = new()
        {
            UniqueId = uniqueId,
            TransactionIdNode = timestamp.N,
            TransactionIdPhysical = timestamp.L,
            TransactionIdCounter = timestamp.C            
        };
        
        if (acquiredLocks.Count > 0)
            request.AcquiredLocks.AddRange(GetArquiredOrModifiedItems(acquiredLocks));
        
        if (modifiedKeys.Count > 0)
            request.ModifiedKeys.AddRange(GetArquiredOrModifiedItems(modifiedKeys));
        
        GrpcServerBatcherResponse response = await batcher.Enqueue(request);
        GrpcRollbackTransactionResponse remoteResponse = response.RollbackTransaction!;
        
        remoteResponse.ServedFrom = $"https://{node}";
        
        return (KeyValueResponseType)remoteResponse.Type;
    }
    
    private static IEnumerable<GrpcTransactionModifiedKey> GetArquiredOrModifiedItems(List<KeyValueTransactionModifiedKey> items)
    {
        foreach (KeyValueTransactionModifiedKey item in items)
        {
            yield return new()
            {
                Key = item.Key,
                Durability = (GrpcKeyValueDurability) item.Durability,
            };
        }
    }

    private static List<(string, ReadOnlyKeyValueContext)> GetReadOnlyItem(RepeatedField<GrpcKeyValueByPrefixItemResponse> remoteResponseItems)
    {
        List<(string, ReadOnlyKeyValueContext)> responses = new(remoteResponseItems.Count);
        
        foreach (GrpcKeyValueByPrefixItemResponse? kv in remoteResponseItems)
        {
            byte[]? value;
            
            if (MemoryMarshal.TryGetArray(kv.Value.Memory, out ArraySegment<byte> segment))
                value = segment.Array;
            else
                value = kv.Value.ToByteArray();
            
            responses.Add((kv.Key, new(
                value, 
                kv.Revision, 
                new(kv.ExpiresNode, kv.ExpiresPhysical, kv.ExpiresCounter),
                new(kv.LastUsedNode, kv.LastUsedPhysical, kv.LastUsedCounter),
                new(kv.LastModifiedNode, kv.LastModifiedPhysical, kv.LastModifiedCounter),
                (KeyValueState)kv.State
            )));
        }

        return responses;
    }
    
    private static GrpcServerBatcher GetSharedBatcher(string url)
    {
        Lazy<GrpcServerBatcher> lazyBatchers = batchers.GetOrAdd(url, GetSharedBatchers);
        return lazyBatchers.Value;
    }
    
    private static Lazy<GrpcServerBatcher> GetSharedBatchers(string url)
    {
        return new(() => new(url));
    }
}