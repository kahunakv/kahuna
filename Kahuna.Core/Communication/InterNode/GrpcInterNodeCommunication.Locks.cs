
using Kommander;
using Kahuna.Server.KeyValues.Data;
using Kommander.Time;
using Kommander.Communication.Grpc;
using Microsoft.Extensions.Logging;

using Google.Protobuf;
using Grpc.Core;
using Grpc.Net.Client;
using System.Collections.Concurrent;
using Google.Protobuf.Collections;

using Kahuna.Shared.Locks;
using Kahuna.Shared.Sequences;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Communication.Grpc;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Server.Locks;
using Kahuna.Server.Communication.Internode.Grpc;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.KeyValues.Writes;
using Kahuna.Server.Locks.Data;
using Kahuna.Server.Persistence.Pitr;


namespace Kahuna.Server.Communication.Internode;

/// <summary>Lock forwarding: acquire, extend, release and read of a distributed lock.</summary>
public partial class GrpcInterNodeCommunication
{
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
        
        GrpcServerBatcherResponse response = await ForwardAsync(batcher.Enqueue(request).WaitAsync(cancellationToken), "TryLock", node).ConfigureAwait(false);
        if (!response.IsAnswered)
            return (LockResponseType.MustRetry, 0);
        
        GrpcTryLockResponse remoteResponse = response.TryLock!;
        
        
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
        
        GrpcServerBatcherResponse response = await ForwardAsync(batcher.Enqueue(request).WaitAsync(cancellationToken), "TryExtendLock", node).ConfigureAwait(false);
        if (!response.IsAnswered)
            return (LockResponseType.MustRetry, 0);

        GrpcExtendLockResponse remoteResponse = response.ExtendLock!;
        
        
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
        
        GrpcServerBatcherResponse response = await ForwardAsync(batcher.Enqueue(request).WaitAsync(cancellationToken), "TryUnlock", node).ConfigureAwait(false);
        if (!response.IsAnswered)
            return LockResponseType.MustRetry;

        GrpcUnlockResponse remoteResponse = response.Unlock!;
        
        
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
        
        GrpcServerBatcherResponse response = await ForwardAsync(batcher.Enqueue(request).WaitAsync(cancellationToken), "GetLock", node).ConfigureAwait(false);
        if (!response.IsAnswered)
            return (LockResponseType.MustRetry, null);

        GrpcGetLockResponse remoteResponse = response.GetLock!;
        
        if (remoteResponse.Type != GrpcLockResponseType.LockResponseTypeGot)
            return ((LockResponseType)remoteResponse.Type, null);
        
        byte[]? owner;
            
        owner = ByteStringPayload.GetArray(remoteResponse.Owner);

        return ((LockResponseType)remoteResponse.Type,
            new(
                owner, 
                remoteResponse.FencingToken,
                new(remoteResponse.ExpiresNode, remoteResponse.ExpiresPhysical, remoteResponse.ExpiresCounter)
            )
        );
    }
}
