
using Kommander.Time;
using Kommander.Communication.Grpc;
using Microsoft.Extensions.Logging;

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
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Locks.Data;

namespace Kahuna.Server.Communication.Internode;

/// <summary>
/// Provides gRPC-based inter-node communication functionalities within the Kahuna Server.
/// This implementation facilitates distributed locking, key-value operations, and other
/// inter-node coordination mechanisms.
/// </summary>
public class GrpcInterNodeCommunication : IInterNodeCommunication
{
    private readonly ConcurrentDictionary<string, Lazy<GrpcServerBatcher>> batchers = new();

    private readonly KahunaConfiguration configuration;

    private readonly ILogger<GrpcInterNodeCommunication> logger;

    public GrpcInterNodeCommunication(KahunaConfiguration configuration, ILogger<GrpcInterNodeCommunication> logger)
    {
        this.configuration = configuration;
        this.logger = logger;
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
    public async Task<(LockResponseType, ReadOnlyLockEntry?)> GetLock(
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
        long routedGeneration,
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
            RoutedGeneration = routedGeneration,
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

    public async Task TryDeleteManyNodeKeyValue(
        string node,
        List<KahunaDeleteKeyValueRequestItem> items,
        Lock lockSync,
        List<KahunaDeleteKeyValueResponseItem> responses,
        CancellationToken cancellationToken
    )
    {
        GrpcTryDeleteManyKeyValueRequest request = new();

        request.Items.Add(GetDeleteManyRequestItems(items));

        GrpcServerBatcher batcher = GetSharedBatcher(node);

        GrpcServerBatcherResponse response = await batcher.Enqueue(request).WaitAsync(cancellationToken);
        GrpcTryDeleteManyKeyValueResponse remoteResponse = response.TryDeleteManyKeyValue!;

        lock (lockSync)
        {
            foreach (GrpcTryDeleteManyKeyValueResponseItem item in remoteResponse.Items)
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
                Durability = (GrpcKeyValueDurability) item.Durability,
                RoutedGeneration = item.RoutedGeneration,
            };

            if (item.Value is not null)
                grpcItem.Value = UnsafeByteOperations.UnsafeWrap(item.Value);

            if (item.CompareValue is not null)
                grpcItem.CompareValue = UnsafeByteOperations.UnsafeWrap(item.CompareValue);

            yield return grpcItem;
        }
    }

    private static IEnumerable<GrpcTryDeleteManyKeyValueRequestItem> GetDeleteManyRequestItems(List<KahunaDeleteKeyValueRequestItem> items)
    {
        foreach (KahunaDeleteKeyValueRequestItem item in items)
        {
            yield return new()
            {
                TransactionIdNode = item.TransactionId.N,
                TransactionIdPhysical = item.TransactionId.L,
                TransactionIdCounter = item.TransactionId.C,
                Key = item.Key,
                Durability = (GrpcKeyValueDurability)item.Durability
            };
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
    public async Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> TryGetValue(
        string node,
        HLCTimestamp transactionId,
        string key,
        long revision,
        HLCTimestamp readTimestamp,
        KeyValueDurability durability,
        CancellationToken cancellationToken
    )
    {
        GrpcTryGetKeyValueRequest request = new()
        {
            TransactionIdNode     = transactionId.N,
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter  = transactionId.C,
            Key                   = key,
            Revision              = revision,
            Durability            = (GrpcKeyValueDurability)durability,
            ReadTimestampNode     = readTimestamp.N,
            ReadTimestampPhysical = readTimestamp.L,
            ReadTimestampCounter  = readTimestamp.C,
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
    public async Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> TryExistsValue(
        string node,
        HLCTimestamp transactionId,
        string key,
        long revision,
        HLCTimestamp readTimestamp,
        KeyValueDurability durability,
        CancellationToken cancellationToken
    )
    {
        GrpcTryExistsKeyValueRequest request = new()
        {
            TransactionIdNode     = transactionId.N,
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter  = transactionId.C,
            Key                   = key,
            Revision              = revision,
            Durability            = (GrpcKeyValueDurability)durability,
            ReadTimestampNode     = readTimestamp.N,
            ReadTimestampPhysical = readTimestamp.L,
            ReadTimestampCounter  = readTimestamp.C,
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

    public async Task TryGetManyNodeValues(
        string node,
        HLCTimestamp transactionId,
        HLCTimestamp readTimestamp,
        List<(string key, long revision, KeyValueDurability durability)> keys,
        Lock lockSync,
        List<(KeyValueResponseType type, string key, KeyValueDurability durability, ReadOnlyKeyValueEntry? entry)> responses,
        CancellationToken cancellationToken
    )
    {
        GrpcServerBatcher batcher = GetSharedBatcher(node);

        GrpcTryGetManyValuesRequest request = new()
        {
            TransactionIdNode = transactionId.N,
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C,
            ReadTimestampNode = readTimestamp.N,
            ReadTimestampPhysical = readTimestamp.L,
            ReadTimestampCounter = readTimestamp.C
        };

        request.Items.Add(GetTryManyValuesRequestItems(keys));

        GrpcServerBatcherResponse response = await batcher.Enqueue(request).WaitAsync(cancellationToken);
        GrpcTryGetManyValuesResponse remoteResponse = response.TryGetManyValues!;

        lock (lockSync)
        {
            foreach (GrpcTryGetManyValuesResponseItem item in remoteResponse.Items)
                responses.Add(((KeyValueResponseType)item.Type, item.Key, (KeyValueDurability)item.Durability, GetReadOnlyKeyValueEntry(item)));
        }
    }

    public async Task TryExistsManyNodeValues(
        string node,
        HLCTimestamp transactionId,
        HLCTimestamp readTimestamp,
        List<(string key, long revision, KeyValueDurability durability)> keys,
        Lock lockSync,
        List<(KeyValueResponseType type, string key, KeyValueDurability durability, ReadOnlyKeyValueEntry? entry)> responses,
        CancellationToken cancellationToken
    )
    {
        GrpcServerBatcher batcher = GetSharedBatcher(node);

        GrpcTryExistsManyValuesRequest request = new()
        {
            TransactionIdNode = transactionId.N,
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C,
            ReadTimestampNode = readTimestamp.N,
            ReadTimestampPhysical = readTimestamp.L,
            ReadTimestampCounter = readTimestamp.C
        };

        request.Items.Add(GetTryManyValuesRequestItems(keys));

        GrpcServerBatcherResponse response = await batcher.Enqueue(request).WaitAsync(cancellationToken);
        GrpcTryExistsManyValuesResponse remoteResponse = response.TryExistsManyValues!;

        lock (lockSync)
        {
            foreach (GrpcTryExistsManyValuesResponseItem item in remoteResponse.Items)
                responses.Add(((KeyValueResponseType)item.Type, item.Key, (KeyValueDurability)item.Durability, GetReadOnlyKeyValueEntry(item)));
        }
    }

    private static IEnumerable<GrpcTryManyValuesRequestItem> GetTryManyValuesRequestItems(
        List<(string key, long revision, KeyValueDurability durability)> keys
    )
    {
        foreach ((string key, long revision, KeyValueDurability durability) item in keys)
        {
            yield return new()
            {
                Key = item.key,
                Revision = item.revision,
                Durability = (GrpcKeyValueDurability)item.durability
            };
        }
    }

    private static ReadOnlyKeyValueEntry? GetReadOnlyKeyValueEntry(GrpcTryGetManyValuesResponseItem item)
    {
        if ((KeyValueResponseType)item.Type is not (KeyValueResponseType.Get or KeyValueResponseType.Exists))
            return null;

        return new(
            item.Value.IsEmpty ? null : item.Value.ToByteArray(),
            item.Revision,
            new(item.ExpiresNode, item.ExpiresPhysical, item.ExpiresCounter),
            new(item.LastUsedNode, item.LastUsedPhysical, item.LastUsedCounter),
            new(item.LastModifiedNode, item.LastModifiedPhysical, item.LastModifiedCounter),
            (KeyValueState)item.State
        );
    }

    private static ReadOnlyKeyValueEntry? GetReadOnlyKeyValueEntry(GrpcTryExistsManyValuesResponseItem item)
    {
        if ((KeyValueResponseType)item.Type is not (KeyValueResponseType.Get or KeyValueResponseType.Exists))
            return null;

        return new(
            null,
            item.Revision,
            new(item.ExpiresNode, item.ExpiresPhysical, item.ExpiresCounter),
            new(item.LastUsedNode, item.LastUsedPhysical, item.LastUsedCounter),
            new(item.LastModifiedNode, item.LastModifiedPhysical, item.LastModifiedCounter),
            (KeyValueState)item.State
        );
    }

    /// <summary>
    /// Redirects a "check-write-intent" probe to the specified node.
    /// Used at commit time by optimistic transactions to detect concurrent writers (write-skew guard).
    /// Returns Aborted when a conflicting write intent is found; DoesNotExist otherwise.
    /// </summary>
    public async Task<KeyValueResponseType> TryCheckWriteIntentValue(
        string node,
        HLCTimestamp transactionId,
        string key,
        KeyValueDurability durability,
        CancellationToken cancellationToken
    )
    {
        GrpcTryCheckWriteIntentRequest request = new()
        {
            TransactionIdNode = transactionId.N,
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C,
            Key = key,
            Durability = (GrpcKeyValueDurability)durability,
        };

        GrpcServerBatcher batcher = GetSharedBatcher(node);

        GrpcServerBatcherResponse response = await batcher.Enqueue(request).WaitAsync(cancellationToken);
        GrpcTryCheckWriteIntentResponse remoteResponse = response.TryCheckWriteIntent!;

        return (KeyValueResponseType)remoteResponse.Type;
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
    public async Task<(KeyValueResponseType, string, KeyValueDurability, HLCTimestamp HolderTransactionId)> TryAcquireExclusiveLock(
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

        HLCTimestamp holder = new(remoteResponse.HolderTransactionIdNode, remoteResponse.HolderTransactionIdPhysical, remoteResponse.HolderTransactionIdCounter);
        return ((KeyValueResponseType)remoteResponse.Type, key, durability, holder);
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
        List<(KeyValueResponseType type, string key, KeyValueDurability durability, HLCTimestamp holder)> responses,
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
            {
                HLCTimestamp holder = new(item.HolderTransactionIdNode, item.HolderTransactionIdPhysical, item.HolderTransactionIdCounter);
                responses.Add(((KeyValueResponseType)item.Type, item.Key, (KeyValueDurability)item.Durability, holder));
            }
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

    public async Task<(KeyValueResponseType, HLCTimestamp HolderTransactionId)> TryAcquireRangeLock(
        string node,
        HLCTimestamp transactionId,
        string prefix,
        string? startKey, bool startInclusive,
        string? endKey,   bool endInclusive,
        int expiresMs,
        KeyValueDurability durability,
        RangeLockMode mode,
        CancellationToken cancellationToken
    )
    {
        GrpcServerBatcher batcher = GetSharedBatcher(node);

        GrpcTryAcquireExclusiveRangeLockRequest request = new()
        {
            TransactionIdNode     = transactionId.N,
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter  = transactionId.C,
            Prefix                = prefix,
            StartInclusive        = startInclusive,
            EndInclusive          = endInclusive,
            ExpiresMs             = expiresMs,
            Durability            = (GrpcKeyValueDurability)durability,
            Mode                  = (GrpcRangeLockMode)mode,
        };

        if (startKey is not null) request.StartKey = startKey;
        if (endKey   is not null) request.EndKey   = endKey;

        GrpcServerBatcherResponse response = await batcher.Enqueue(request).WaitAsync(cancellationToken);
        GrpcTryAcquireExclusiveRangeLockResponse remoteResponse = response.TryAcquireExclusiveRangeLock!;
        remoteResponse.ServedFrom = $"https://{node}";
        HLCTimestamp holder = new(remoteResponse.HolderTransactionIdNode, remoteResponse.HolderTransactionIdPhysical, remoteResponse.HolderTransactionIdCounter);
        return ((KeyValueResponseType)remoteResponse.Type, holder);
    }

    public Task<(KeyValueResponseType, HLCTimestamp HolderTransactionId)> TryAcquireExclusiveRangeLock(
        string node,
        HLCTimestamp transactionId,
        string prefix,
        string? startKey, bool startInclusive,
        string? endKey,   bool endInclusive,
        int expiresMs,
        KeyValueDurability durability,
        CancellationToken cancellationToken
    ) => TryAcquireRangeLock(node, transactionId, prefix, startKey, startInclusive, endKey, endInclusive, expiresMs, durability, RangeLockMode.Exclusive, cancellationToken);

    public async Task<KeyValueResponseType> TryReleaseExclusiveRangeLock(
        string node,
        HLCTimestamp transactionId,
        string prefix,
        string? startKey, bool startInclusive,
        string? endKey,   bool endInclusive,
        KeyValueDurability durability,
        CancellationToken cancellationToken
    )
    {
        GrpcServerBatcher batcher = GetSharedBatcher(node);

        GrpcTryReleaseExclusiveRangeLockRequest request = new()
        {
            TransactionIdNode     = transactionId.N,
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter  = transactionId.C,
            Prefix                = prefix,
            StartInclusive        = startInclusive,
            EndInclusive          = endInclusive,
            Durability            = (GrpcKeyValueDurability)durability,
        };

        if (startKey is not null) request.StartKey = startKey;
        if (endKey   is not null) request.EndKey   = endKey;

        GrpcServerBatcherResponse response = await batcher.Enqueue(request).WaitAsync(cancellationToken);
        GrpcTryReleaseExclusiveRangeLockResponse remoteResponse = response.TryReleaseExclusiveRangeLock!;
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
        long routedGeneration,
        CancellationToken cancellationToken,
        string? recordAnchorKey = null
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
            Durability = (GrpcKeyValueDurability)durability,
            RoutedGeneration = routedGeneration,
        };

        if (recordAnchorKey is not null)
            request.RecordAnchorKey = recordAnchorKey;

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
        CancellationToken cancellationToken,
        string? recordAnchorKey = null
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

        if (recordAnchorKey is not null)
            request.RecordAnchorKey = recordAnchorKey;

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
    public async Task<KeyValueGetByBucketResult> GetByBucket(string node, HLCTimestamp transactionId, string prefixedKey, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken)
    {
        GrpcServerBatcher batcher = GetSharedBatcher(node);

        GrpcGetByBucketRequest request = new()
        {
            TransactionIdNode     = transactionId.N,
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter  = transactionId.C,
            PrefixKey             = prefixedKey,
            Durability            = (GrpcKeyValueDurability)durability,
            ReadTimestampNode     = readTimestamp.N,
            ReadTimestampPhysical = readTimestamp.L,
            ReadTimestampCounter  = readTimestamp.C,
        };
        
        GrpcServerBatcherResponse batchResponse;
                              
        if (cancellationToken == CancellationToken.None)
           batchResponse = await batcher.Enqueue(request).ConfigureAwait(false);
        else
           batchResponse = await batcher.Enqueue(request).WaitAsync(cancellationToken).ConfigureAwait(false);
       
        GrpcGetByBucketResponse remoteResponse = batchResponse.GetByBucket!;
        
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
    /// <summary>
    /// Forwards a single-page GetByRange request to a remote leader node via the batch channel.
    /// </summary>
    public async Task<KeyValueGetByRangeResult> GetByRange(
        string node,
        HLCTimestamp transactionId,
        string prefix,
        string? startKey,
        bool startInclusive,
        string? endKey,
        bool endInclusive,
        int limit,
        HLCTimestamp readTimestamp,
        KeyValueDurability durability,
        CancellationToken cancellationToken)
    {
        GrpcServerBatcher batcher = GetSharedBatcher(node);

        GrpcGetByRangeRequest request = new()
        {
            TransactionIdNode     = transactionId.N,
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter  = transactionId.C,
            Prefix                = prefix,
            StartInclusive        = startInclusive,
            EndInclusive          = endInclusive,
            Limit                 = limit,
            ReadTimestampNode     = readTimestamp.N,
            ReadTimestampPhysical = readTimestamp.L,
            ReadTimestampCounter  = readTimestamp.C,
            Durability            = (GrpcKeyValueDurability)durability,
        };

        if (startKey is not null) request.StartKey = startKey;
        if (endKey   is not null) request.EndKey   = endKey;

        GrpcServerBatcherResponse batchResponse;

        if (cancellationToken == CancellationToken.None)
            batchResponse = await batcher.Enqueue(request).ConfigureAwait(false);
        else
            batchResponse = await batcher.Enqueue(request).WaitAsync(cancellationToken).ConfigureAwait(false);

        GrpcGetByRangeResponse remoteResponse = batchResponse.GetByRange!;
        remoteResponse.ServedFrom = $"https://{node}";

        return new(
            (KeyValueResponseType)remoteResponse.Type,
            GetReadOnlyItem(remoteResponse.Items),
            remoteResponse.HasNextCursor ? remoteResponse.NextCursor : null,
            remoteResponse.HasMore);
    }

    public async Task<KeyValueGetByBucketResult> ScanByPrefix(string node, string prefixedKey, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken)
    {
        GrpcServerBatcher batcher = GetSharedBatcher(node);

        GrpcScanByPrefixRequest request = new()
        {
            PrefixKey = prefixedKey,
            Durability = (GrpcKeyValueDurability)durability,
            ReadTimestampNode = readTimestamp.N,
            ReadTimestampPhysical = readTimestamp.L,
            ReadTimestampCounter = readTimestamp.C,
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
    public async Task<(KeyValueResponseType, TransactionHandle)> StartTransaction(string node, KeyValueTransactionOptions options, CancellationToken cancellationToken)
    {
        GrpcServerBatcher batcher = GetSharedBatcher(node);

        GrpcStartTransactionRequest request = new()
        {
            CoordinatorKey = options.CoordinatorKey,
            LockingType = (GrpcLockingType)options.Locking,
            Timeout = options.Timeout,
            AsyncRelease = options.AsyncRelease,
            AutoCommit = options.AutoCommit,
            ReadValidation = (GrpcReadValidation)options.ReadValidation,
            DecisionDurability = (GrpcDecisionDurability)options.DecisionDurability,
            ReadTimestampNode = options.ReadTimestamp.N,
            ReadTimestampPhysical = options.ReadTimestamp.L,
            ReadTimestampCounter = options.ReadTimestamp.C,
        };

        GrpcServerBatcherResponse response = await batcher.Enqueue(request);
        GrpcStartTransactionResponse remoteResponse = response.StartTransaction!;

        remoteResponse.ServedFrom = $"https://{node}";

        HLCTimestamp transactionId = new(remoteResponse.TransactionIdNode, remoteResponse.TransactionIdPhysical, remoteResponse.TransactionIdCounter);
        return (
            (KeyValueResponseType)remoteResponse.Type,
            new TransactionHandle(transactionId, options.CoordinatorKey)
        );
    }

    public async Task<(KeyValueResponseType, string?)> CommitTransaction(string node, TransactionHandle handle, CancellationToken cancellationToken)
    {
        GrpcServerBatcher batcher = GetSharedBatcher(node);

        GrpcCommitTransactionRequest request = new()
        {
            CoordinatorKey = handle.CoordinatorKey,
            TransactionIdNode = handle.TransactionId.N,
            TransactionIdPhysical = handle.TransactionId.L,
            TransactionIdCounter = handle.TransactionId.C
        };

        // Forward the record anchor so a commit routed to the anchor's node reaches the durable decision even
        // after the coordinating session was evicted or its node failed.
        if (handle.RecordAnchorKey is not null)
            request.RecordAnchorKey = handle.RecordAnchorKey;

        GrpcServerBatcherResponse response = await batcher.Enqueue(request);
        GrpcCommitTransactionResponse remoteResponse = response.CommitTransaction!;

        remoteResponse.ServedFrom = $"https://{node}";

        return ((KeyValueResponseType)remoteResponse.Type, remoteResponse.HasRecordAnchorKey ? remoteResponse.RecordAnchorKey : null);
    }

    public async Task<KeyValueResponseType> RollbackTransaction(string node, TransactionHandle handle, CancellationToken cancellationToken)
    {
        GrpcServerBatcher batcher = GetSharedBatcher(node);

        GrpcRollbackTransactionRequest request = new()
        {
            CoordinatorKey = handle.CoordinatorKey,
            TransactionIdNode = handle.TransactionId.N,
            TransactionIdPhysical = handle.TransactionId.L,
            TransactionIdCounter = handle.TransactionId.C
        };

        // Forward the record anchor so a rollback routed to the anchor's node can consult the durable decision.
        if (handle.RecordAnchorKey is not null)
            request.RecordAnchorKey = handle.RecordAnchorKey;

        GrpcServerBatcherResponse response = await batcher.Enqueue(request);
        GrpcRollbackTransactionResponse remoteResponse = response.RollbackTransaction!;

        remoteResponse.ServedFrom = $"https://{node}";

        return (KeyValueResponseType)remoteResponse.Type;
    }
    
    public async Task<(OperationRegistrationOutcome outcome, KeyValueResponseType cachedType, long cachedRevision, HLCTimestamp cachedTimestamp, string? recordAnchorKey)> BeginOperation(string node, string coordinatorKey, HLCTimestamp transactionId, TransactionOperationId operationId, OperationKind kind, byte[]? payloadDigest, CancellationToken cancellationToken)
    {
        GrpcServerBatcher batcher = GetSharedBatcher(node);

        GrpcBeginOperationRequest request = new()
        {
            CoordinatorKey = coordinatorKey,
            TransactionIdNode = transactionId.N,
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C,
            OperationIdHigh = operationId.High,
            OperationIdLow = operationId.Low,
            Kind = (GrpcOperationKind)kind
        };

        if (payloadDigest is not null)
            request.PayloadDigest = ByteString.CopyFrom(payloadDigest);

        GrpcServerBatcherResponse response = await batcher.Enqueue(request);
        GrpcBeginOperationResponse remoteResponse = response.BeginOperation!;

        HLCTimestamp cachedTimestamp = new(remoteResponse.CachedTimestampNode, remoteResponse.CachedTimestampPhysical, remoteResponse.CachedTimestampCounter);

        return (
            (OperationRegistrationOutcome)remoteResponse.Outcome,
            (KeyValueResponseType)remoteResponse.CachedType,
            remoteResponse.CachedRevision,
            cachedTimestamp,
            remoteResponse.HasRecordAnchorKey ? remoteResponse.RecordAnchorKey : null
        );
    }

    public async Task<(KeyValueResponseType outcome, string? anchor)> CompleteOperation(string node, string coordinatorKey, HLCTimestamp transactionId, TransactionOperationId operationId, OperationCompletionPayload payload, CancellationToken cancellationToken)
    {
        GrpcServerBatcher batcher = GetSharedBatcher(node);

        GrpcCompleteOperationRequest request = new()
        {
            CoordinatorKey = coordinatorKey,
            TransactionIdNode = transactionId.N,
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C,
            OperationIdHigh = operationId.High,
            OperationIdLow = operationId.Low,
            Durability = (GrpcKeyValueDurability)payload.Durability,
            CachedType = (GrpcKeyValueResponseType)payload.CachedType,
            CachedRevision = payload.CachedRevision,
            CachedTimestampNode = payload.CachedTimestamp.N,
            CachedTimestampPhysical = payload.CachedTimestamp.L,
            CachedTimestampCounter = payload.CachedTimestamp.C
        };

        if (payload.ModifiedKey is not null) request.ModifiedKey = payload.ModifiedKey;
        if (payload.AcquiredPointLock is not null) request.AcquiredPointLock = payload.AcquiredPointLock;
        if (payload.ReleasedPointLock is not null) request.ReleasedPointLock = payload.ReleasedPointLock;
        if (payload.AcquiredPrefixLock is not null) request.AcquiredPrefixLock = payload.AcquiredPrefixLock;
        if (payload.ReleasedPrefixLock is not null) request.ReleasedPrefixLock = payload.ReleasedPrefixLock;
        if (payload.AcquiredRangeLock is { } acquiredRange) request.AcquiredRangeLock = ToGrpcRangeLock(acquiredRange.Range, acquiredRange.Mode);
        if (payload.ReleasedRangeLock is { } releasedRange) request.ReleasedRangeLock = ToGrpcRangeLock(releasedRange, RangeLockMode.Exclusive);
        if (payload.Read is not null) request.Read = ToGrpcReadKey(payload.Read);
        if (payload.ReadObservations is not null)
            request.ReadObservations.AddRange(payload.ReadObservations.Select(ToGrpcReadKey));
        if (payload.ModifiedKeys is not null)
            request.ModifiedKeys.AddRange(payload.ModifiedKeys.Select(m => new GrpcTransactionModifiedKey { Key = m.Key, Durability = (GrpcKeyValueDurability)m.Durability }));

        GrpcServerBatcherResponse response = await batcher.Enqueue(request);
        GrpcCompleteOperationResponse remoteResponse = response.CompleteOperation!;
        KeyValueResponseType outcome = remoteResponse.Acknowledged ? KeyValueResponseType.Set : KeyValueResponseType.MustRetry;
        return (outcome, remoteResponse.HasRecordAnchorKey ? remoteResponse.RecordAnchorKey : null);
    }

    public async Task<TransactionWorkingSet?> GetTransactionWorkingSet(string node, string coordinatorKey, HLCTimestamp transactionId, CancellationToken cancellationToken)
    {
        GrpcServerBatcher batcher = GetSharedBatcher(node);

        GrpcGetTransactionWorkingSetRequest request = new()
        {
            CoordinatorKey = coordinatorKey,
            TransactionIdNode = transactionId.N,
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C
        };

        GrpcServerBatcherResponse response = await batcher.Enqueue(request);
        GrpcGetTransactionWorkingSetResponse remoteResponse = response.GetTransactionWorkingSet!;

        return remoteResponse.Found ? FromGrpcWorkingSet(remoteResponse.WorkingSet) : null;
    }

    public async Task<(KeyValueResponseType, TransactionWorkingSet?)> CloseTransaction(string node, string coordinatorKey, HLCTimestamp transactionId, CancellationToken cancellationToken)
    {
        GrpcServerBatcher batcher = GetSharedBatcher(node);

        GrpcCloseTransactionRequest request = new()
        {
            CoordinatorKey = coordinatorKey,
            TransactionIdNode = transactionId.N,
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C
        };

        GrpcServerBatcherResponse response = await batcher.Enqueue(request);
        GrpcCloseTransactionResponse remoteResponse = response.CloseTransaction!;

        return (
            (KeyValueResponseType)remoteResponse.Type,
            remoteResponse.HasWorkingSet ? FromGrpcWorkingSet(remoteResponse.WorkingSet) : null
        );
    }

    private static GrpcTransactionRangeLock ToGrpcRangeLock(RangeLockKey range, RangeLockMode mode)
    {
        GrpcTransactionRangeLock grpc = new()
        {
            Prefix = range.Prefix,
            StartInclusive = range.StartInclusive,
            EndInclusive = range.EndInclusive,
            Durability = (GrpcKeyValueDurability)range.Durability,
            Mode = (GrpcRangeLockMode)mode
        };

        if (range.StartKey is not null) grpc.StartKey = range.StartKey;
        if (range.EndKey is not null) grpc.EndKey = range.EndKey;

        return grpc;
    }

    private static GrpcTransactionReadKey ToGrpcReadKey(KeyValueTransactionReadKey read) => new()
    {
        Key = read.Key ?? "",
        Durability = (GrpcKeyValueDurability)read.Durability,
        Exists = read.Exists,
        Revision = read.Revision
    };

    private static TransactionWorkingSet FromGrpcWorkingSet(GrpcTransactionWorkingSet grpc)
    {
        return new()
        {
            ModifiedKeys = grpc.ModifiedKeys.Select(FromGrpcModifiedKey).ToList(),
            AcquiredLocks = grpc.AcquiredLocks.Select(FromGrpcModifiedKey).ToList(),
            AcquiredPrefixLocks = grpc.AcquiredPrefixLocks.Select(FromGrpcModifiedKey).ToList(),
            AcquiredRangeLocks = grpc.AcquiredRangeLocks.Select(FromGrpcRangeLock).ToList(),
            ReadKeys = grpc.ReadKeys.Select(FromGrpcReadKey).ToList(),
            RecordAnchorKey = grpc.HasRecordAnchorKey ? grpc.RecordAnchorKey : null,
            PendingOperationCount = grpc.PendingOperationCount
        };
    }

    private static KeyValueTransactionModifiedKey FromGrpcModifiedKey(GrpcTransactionModifiedKey g) =>
        new() { Key = g.Key, Durability = (KeyValueDurability)g.Durability };

    private static KeyValueTransactionReadKey FromGrpcReadKey(GrpcTransactionReadKey g) =>
        new() { Key = g.Key, Durability = (KeyValueDurability)g.Durability, Exists = g.Exists, Revision = g.Revision };

    private static KeyValueTransactionRangeLock FromGrpcRangeLock(GrpcTransactionRangeLock g) =>
        new()
        {
            Prefix = g.Prefix,
            StartKey = g.HasStartKey ? g.StartKey : null,
            StartInclusive = g.StartInclusive,
            EndKey = g.HasEndKey ? g.EndKey : null,
            EndInclusive = g.EndInclusive,
            Durability = (KeyValueDurability)g.Durability,
            Mode = (RangeLockMode)g.Mode
        };

    private static List<(string, ReadOnlyKeyValueEntry)> GetReadOnlyItem(RepeatedField<GrpcKeyValueByPrefixItemResponse> remoteResponseItems)
    {
        List<(string, ReadOnlyKeyValueEntry)> responses = new(remoteResponseItems.Count);
        
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
    
    public async Task<bool> EnsureKeyRangeSeeded(string node, string keySpace, CancellationToken cancellationToken)
    {
        GrpcServerBatcher batcher = GetSharedBatcher(node);

        GrpcEnsureKeyRangeSeededRequest request = new()
        {
            KeySpace = keySpace,
        };

        GrpcServerBatcherResponse batchResponse;

        if (cancellationToken == CancellationToken.None)
            batchResponse = await batcher.Enqueue(request).ConfigureAwait(false);
        else
            batchResponse = await batcher.Enqueue(request).WaitAsync(cancellationToken).ConfigureAwait(false);

        GrpcEnsureKeyRangeSeededResponse remoteResponse = batchResponse.EnsureKeyRangeSeeded!;
        return remoteResponse.Success;
    }

    public async Task<bool> EnsureKeyRangeRemoved(string node, string keySpace, CancellationToken cancellationToken)
    {
        GrpcServerBatcher batcher = GetSharedBatcher(node);

        GrpcEnsureKeyRangeRemovedRequest request = new()
        {
            KeySpace = keySpace,
        };

        GrpcServerBatcherResponse batchResponse;

        if (cancellationToken == CancellationToken.None)
            batchResponse = await batcher.Enqueue(request).ConfigureAwait(false);
        else
            batchResponse = await batcher.Enqueue(request).WaitAsync(cancellationToken).ConfigureAwait(false);

        GrpcEnsureKeyRangeRemovedResponse remoteResponse = batchResponse.EnsureKeyRangeRemoved!;
        return remoteResponse.Success;
    }

    public async Task<List<KeyValueRangeLock>> GetRangeLocks(string node, string keySpace, CancellationToken cancellationToken)
    {
        GrpcServerBatcher batcher = GetSharedBatcher(node);

        GrpcGetRangeLocksRequest request = new()
        {
            KeySpace = keySpace,
        };

        GrpcServerBatcherResponse batchResponse;

        if (cancellationToken == CancellationToken.None)
            batchResponse = await batcher.Enqueue(request).ConfigureAwait(false);
        else
            batchResponse = await batcher.Enqueue(request).WaitAsync(cancellationToken).ConfigureAwait(false);

        GrpcGetRangeLocksResponse remoteResponse = batchResponse.GetRangeLocks!;

        List<KeyValueRangeLock> locks = new(remoteResponse.Locks.Count);
        foreach (GrpcRangeLockEntry entry in remoteResponse.Locks)
        {
            locks.Add(new KeyValueRangeLock
            {
                TransactionId  = new HLCTimestamp(entry.TransactionIdNode, entry.TransactionIdPhysical, entry.TransactionIdCounter),
                StartKey       = entry.HasStartKey ? entry.StartKey : null,
                StartInclusive = entry.StartInclusive,
                EndKey         = entry.HasEndKey ? entry.EndKey : null,
                EndInclusive   = entry.EndInclusive,
                Mode           = (RangeLockMode)entry.Mode,
                Expires        = new HLCTimestamp(entry.ExpiresNode, entry.ExpiresPhysical, entry.ExpiresCounter),
            });
        }
        return locks;
    }

    public async Task ImportRangeLocks(string node, string keySpace, List<KeyValueRangeLock> locks, CancellationToken cancellationToken)
    {
        GrpcServerBatcher batcher = GetSharedBatcher(node);

        GrpcImportRangeLocksRequest request = new()
        {
            KeySpace = keySpace,
        };

        foreach (KeyValueRangeLock rl in locks)
        {
            GrpcRangeLockEntry entry = new()
            {
                TransactionIdNode     = rl.TransactionId.N,
                TransactionIdPhysical = rl.TransactionId.L,
                TransactionIdCounter  = rl.TransactionId.C,
                StartInclusive        = rl.StartInclusive,
                EndInclusive          = rl.EndInclusive,
                Mode                  = (GrpcRangeLockMode)rl.Mode,
                ExpiresNode           = rl.Expires.N,
                ExpiresPhysical       = rl.Expires.L,
                ExpiresCounter        = rl.Expires.C,
            };

            if (rl.StartKey is not null) entry.StartKey = rl.StartKey;
            if (rl.EndKey is not null) entry.EndKey = rl.EndKey;

            request.Locks.Add(entry);
        }

        GrpcServerBatcherResponse batchResponse;

        if (cancellationToken == CancellationToken.None)
            batchResponse = await batcher.Enqueue(request).ConfigureAwait(false);
        else
            batchResponse = await batcher.Enqueue(request).WaitAsync(cancellationToken).ConfigureAwait(false);

        _ = batchResponse.ImportRangeLocks;
    }

    public async Task<bool> ImportCompletionReceipts(string node, int partitionId, IReadOnlyCollection<CompletionReceiptRecord> receipts, CancellationToken cancellationToken, bool forget = false)
    {
        GrpcServerBatcher batcher = GetSharedBatcher(node);

        GrpcImportCompletionReceiptsRequest request = new() { DestinationPartitionId = partitionId, Forget = forget };

        foreach (CompletionReceiptRecord receipt in receipts)
        {
            GrpcCompletionReceiptEntry entry = new()
            {
                TransactionIdNode     = receipt.TransactionId.N,
                TransactionIdPhysical = receipt.TransactionId.L,
                TransactionIdCounter  = receipt.TransactionId.C,
                Key                   = receipt.Key,
                Durability            = (int)receipt.Durability,
            };

            if (receipt.RecordAnchorKey is not null) entry.RecordAnchorKey = receipt.RecordAnchorKey;

            request.Receipts.Add(entry);
        }

        GrpcServerBatcherResponse batchResponse;

        if (cancellationToken == CancellationToken.None)
            batchResponse = await batcher.Enqueue(request).ConfigureAwait(false);
        else
            batchResponse = await batcher.Enqueue(request).WaitAsync(cancellationToken).ConfigureAwait(false);

        return batchResponse.ImportCompletionReceipts?.Success ?? false;
    }

    public async Task<bool> DurableOperation(string node, int partitionId, int kind, string logType, byte[] payload, CancellationToken cancellationToken)
    {
        GrpcServerBatcher batcher = GetSharedBatcher(node);

        GrpcDurableOperationRequest request = new()
        {
            PartitionId = partitionId,
            Kind = kind,
            LogType = logType,
            Payload = UnsafeByteOperations.UnsafeWrap(payload)
        };

        GrpcServerBatcherResponse batchResponse;

        if (cancellationToken == CancellationToken.None)
            batchResponse = await batcher.Enqueue(request).ConfigureAwait(false);
        else
            batchResponse = await batcher.Enqueue(request).WaitAsync(cancellationToken).ConfigureAwait(false);

        return batchResponse.DurableOperation?.Committed ?? false;
    }

    public async Task<byte[]?> LookupTransactionRecord(string node, int partitionId, HLCTimestamp transactionId, long epoch, string anchorKey, CancellationToken cancellationToken)
    {
        GrpcServerBatcher batcher = GetSharedBatcher(node);

        GrpcLookupTransactionRecordRequest request = new()
        {
            PartitionId = partitionId,
            TransactionIdNode = transactionId.N,
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C,
            Epoch = epoch,
            AnchorKey = anchorKey
        };

        GrpcServerBatcherResponse batchResponse;

        if (cancellationToken == CancellationToken.None)
            batchResponse = await batcher.Enqueue(request).ConfigureAwait(false);
        else
            batchResponse = await batcher.Enqueue(request).WaitAsync(cancellationToken).ConfigureAwait(false);

        GrpcLookupTransactionRecordResponse? response = batchResponse.LookupTransactionRecord;
        return response is { Found: true } ? response.Record.ToByteArray() : null;
    }

    public async Task<(KeyValueResponseType Type, string HoldId, HLCTimestamp LeaseExpiry)>
        AcquireSnapshotHold(string node, string holderId, HLCTimestamp timestamp, int leaseMs, CancellationToken cancellationToken)
    {
        GrpcAcquireSnapshotHoldRequest request = new()
        {
            HolderId          = holderId,
            TimestampNode     = timestamp.N,
            TimestampPhysical = timestamp.L,
            TimestampCounter  = (uint)timestamp.C,
            LeaseMs           = leaseMs
        };

        GrpcServerBatcher batcher = GetSharedBatcher(node);

        GrpcServerBatcherResponse batchResponse;
        if (cancellationToken == CancellationToken.None)
            batchResponse = await batcher.Enqueue(request).ConfigureAwait(false);
        else
            batchResponse = await batcher.Enqueue(request).WaitAsync(cancellationToken).ConfigureAwait(false);

        GrpcAcquireSnapshotHoldResponse r = batchResponse.AcquireSnapshotHold!;
        return (
            (KeyValueResponseType)r.Type,
            r.HoldId,
            new HLCTimestamp(r.LeaseExpiryNode, r.LeaseExpiryPhysical, r.LeaseExpiryCounter)
        );
    }

    public async Task<(KeyValueResponseType Type, HLCTimestamp LeaseExpiry)>
        RenewSnapshotHold(string node, string holdId, int leaseMs, CancellationToken cancellationToken)
    {
        GrpcRenewSnapshotHoldRequest request = new()
        {
            HoldId  = holdId,
            LeaseMs = leaseMs
        };

        GrpcServerBatcher batcher = GetSharedBatcher(node);

        GrpcServerBatcherResponse batchResponse;
        if (cancellationToken == CancellationToken.None)
            batchResponse = await batcher.Enqueue(request).ConfigureAwait(false);
        else
            batchResponse = await batcher.Enqueue(request).WaitAsync(cancellationToken).ConfigureAwait(false);

        GrpcRenewSnapshotHoldResponse r = batchResponse.RenewSnapshotHold!;
        return (
            (KeyValueResponseType)r.Type,
            new HLCTimestamp(r.LeaseExpiryNode, r.LeaseExpiryPhysical, r.LeaseExpiryCounter)
        );
    }

    public async Task<KeyValueResponseType>
        ReleaseSnapshotHold(string node, string holdId, CancellationToken cancellationToken)
    {
        GrpcReleaseSnapshotHoldRequest request = new() { HoldId = holdId };

        GrpcServerBatcher batcher = GetSharedBatcher(node);

        GrpcServerBatcherResponse batchResponse;
        if (cancellationToken == CancellationToken.None)
            batchResponse = await batcher.Enqueue(request).ConfigureAwait(false);
        else
            batchResponse = await batcher.Enqueue(request).WaitAsync(cancellationToken).ConfigureAwait(false);

        return (KeyValueResponseType)batchResponse.ReleaseSnapshotHold!.Type;
    }

    public async Task<(HLCTimestamp Floor, int LiveHolds)>
        GetSnapshotFloor(string node, CancellationToken cancellationToken)
    {
        GrpcGetSnapshotFloorRequest request = new();

        GrpcServerBatcher batcher = GetSharedBatcher(node);

        GrpcServerBatcherResponse batchResponse;
        if (cancellationToken == CancellationToken.None)
            batchResponse = await batcher.Enqueue(request).ConfigureAwait(false);
        else
            batchResponse = await batcher.Enqueue(request).WaitAsync(cancellationToken).ConfigureAwait(false);

        GrpcGetSnapshotFloorResponse r = batchResponse.GetSnapshotFloor!;
        return (
            new HLCTimestamp(r.EffectiveFloorNode, r.EffectiveFloorPhysical, r.EffectiveFloorCounter),
            r.LiveHolds
        );
    }

    private GrpcServerBatcher GetSharedBatcher(string url)
    {
        Lazy<GrpcServerBatcher> lazyBatcher = batchers.GetOrAdd(url, static (u, self) => self.GetSharedBatchers(u), this);
        return lazyBatcher.Value;
    }

    private Lazy<GrpcServerBatcher> GetSharedBatchers(string url)
    {
        return new(() => new(url, logger));
    }
}
