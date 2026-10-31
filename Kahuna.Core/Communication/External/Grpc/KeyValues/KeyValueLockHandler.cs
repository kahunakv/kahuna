
/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Runtime.CompilerServices;
using Kahuna.Server.KeyValues.Data;
using Kommander.Time;
using Google.Protobuf;
using Google.Protobuf.Collections;
using Grpc.Core;
using Kahuna.Communication.External.Grpc.KeyValues;
using Kahuna.Server.Communication.Internode.Grpc;
using Kahuna.Server.Communication;
using Kahuna.Server.Configuration;
using Kahuna.Server.Communication.Internode;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.KeyValues.Writes;
using Kahuna.Server.Routing;
using Kahuna.Shared.Routing;
using Kahuna.Shared.Communication.Grpc;
using Kahuna.Shared.Communication.Rest;
using Kahuna.Shared.KeyValue;
using Kommander.Diagnostics;
using static Kahuna.Communication.External.Grpc.KeyValues.KeyValueGrpcWire;

namespace Kahuna.Communication.External.Grpc.KeyValues;

/// <summary>
/// Exclusive, prefix and range lock acquisition and release.
/// </summary>
internal sealed class KeyValueLockHandler
{
    private readonly IKahuna keyValues;

    public KeyValueLockHandler(IKahuna keyValues)
    {
        this.keyValues = keyValues;
    }

    /// <summary>
    /// Attempts to acquire an exclusive lock for a given key with the specified expiration and durability settings.
    /// </summary>
    /// <param name="request">The request containing the key, transaction details, expiration time, and durability level.</param>
    /// <param name="context">The server call context that provides access to information about the RPC call.</param>
    /// <returns>A task that represents the asynchronous operation. The task result contains a response indicating whether the lock acquisition attempt succeeded or failed.</returns>
    internal async Task<GrpcTryAcquireExclusiveLockResponse> TryAcquireExclusiveLockCore(GrpcTryAcquireExclusiveLockRequest request, ServerCallContext context)
    {
        if (string.IsNullOrEmpty(request.Key))
            return new()
            {
                Type = GrpcKeyValueResponseType.TypeInvalidInput
            };
        
        using YieldingIntentPolicyScope.Scope lockPolicyScope = YieldingIntentPolicyScope.Enter(TransactionConflictPolicyWire.FromGrpc(request.ConflictPolicy));
        using LockGrantScope.Scope grantScope = LockGrantScope.Begin(out LockGrantCapture grants);
        (KeyValueResponseType type, _, _, HLCTimestamp holder, long baseRevision) = await keyValues.LocateAndTryAcquireExclusiveLockObserved(
            new(request.TransactionIdNode, request.TransactionIdPhysical, request.TransactionIdCounter),
            request.Key,
            request.ExpiresMs,
            (KeyValueDurability)request.Durability,
            context.CancellationToken,
            request.CoordinatorKey,
            new TransactionOperationId(request.OperationIdHigh, request.OperationIdLow)
        );

        bool baseObserved = baseRevision != PointLockBase.None;

        GrpcTryAcquireExclusiveLockResponse response = new()
        {
            Type = (GrpcKeyValueResponseType)type,
            HolderTransactionIdNode     = holder.N,
            HolderTransactionIdPhysical = holder.L,
            HolderTransactionIdCounter  = holder.C,
            BaseObserved = baseObserved,
            BaseRevision = baseObserved ? baseRevision : 0
        };

        AddLockGrantTerms(response.GrantTerms, grants);

        return response;
    }

    /// <summary>
    /// Attempts to acquire an exclusive lock on a group of key-value pairs prefixed by the given key with the specified expiration and durability settings.
    /// </summary>
    /// <param name="request">The request containing the key, transaction details, expiration time, and durability level.</param>
    /// <param name="context">The server call context that provides access to information about the RPC call.</param>
    /// <returns>A task that represents the asynchronous operation. The task result contains a response indicating whether the lock acquisition attempt succeeded or failed.</returns>
    internal async Task<GrpcTryAcquireExclusivePrefixLockResponse> TryAcquireExclusivePrefixLockCore(GrpcTryAcquireExclusivePrefixLockRequest request, ServerCallContext context)
    {
        if (string.IsNullOrEmpty(request.PrefixKey))
            return new()
            {
                Type = GrpcKeyValueResponseType.TypeInvalidInput
            };
        
        using LockGrantScope.Scope grantScope = LockGrantScope.Begin(out LockGrantCapture grants);
        KeyValueResponseType type = await keyValues.LocateAndTryAcquireExclusivePrefixLock(
            new(request.TransactionIdNode, request.TransactionIdPhysical, request.TransactionIdCounter),
            request.PrefixKey,
            request.ExpiresMs,
            (KeyValueDurability)request.Durability,
            context.CancellationToken,
            request.CoordinatorKey,
            new TransactionOperationId(request.OperationIdHigh, request.OperationIdLow)
        );

        GrpcTryAcquireExclusivePrefixLockResponse response = new()
        {
            Type = (GrpcKeyValueResponseType)type
        };

        AddLockGrantTerms(response.GrantTerms, grants);

        return response;
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="request"></param>
    /// <param name="context"></param>
    /// <returns></returns>
    internal async Task<GrpcTryAcquireManyExclusiveLocksResponse> TryAcquireManyExclusiveLocksCore(GrpcTryAcquireManyExclusiveLocksRequest request, ServerCallContext context)
    {
        using YieldingIntentPolicyScope.Scope manyLockPolicyScope = YieldingIntentPolicyScope.Enter(TransactionConflictPolicyWire.FromGrpc(request.ConflictPolicy));
        using LockGrantScope.Scope grantScope = LockGrantScope.Begin(out LockGrantCapture grants);
        List<(KeyValueResponseType, string, KeyValueDurability, HLCTimestamp, long)> responses = await keyValues.LocateAndTryAcquireManyExclusiveLocksObserved(
            new(request.TransactionIdNode, request.TransactionIdPhysical, request.TransactionIdCounter),
            GetRequestLocksItems(request.Items),
            context.CancellationToken,
            request.CoordinatorKey,
            new TransactionOperationId(request.OperationIdHigh, request.OperationIdLow)
        );

        GrpcTryAcquireManyExclusiveLocksResponse response = new();

        AddResponseLocksItems(response.Items, responses);
        AddLockGrantTerms(response.GrantTerms, grants);

        return response;
    }

    /// <summary>
    /// Converts a collection of gRPC exclusive lock request items into a list of tuples
    /// containing key, expiration time in milliseconds, and durability type.
    /// </summary>
    /// <param name="items">A collection of gRPC exclusive lock request items to be processed.</param>
    /// <returns>A list of tuples with each tuple containing a key, expiration time in milliseconds,
    /// and durability type derived from the input collection.</returns>
    private static List<(string key, int expiresMs, KeyValueDurability durability)> GetRequestLocksItems(
        RepeatedField<GrpcTryAcquireManyExclusiveLocksRequestItem> items
    )
    {
        List<(string key, int expiresMs, KeyValueDurability durability)> rItems = new(items.Count);
        
        foreach (GrpcTryAcquireManyExclusiveLocksRequestItem item in items)
            rItems.Add((item.Key, item.ExpiresMs, (KeyValueDurability)item.Durability));
        
        return rItems;
    }

    /// <summary>
    /// Converts a list of key-value response tuples into a collection of gRPC response items.
    /// </summary>
    /// <param name="responses">A list of tuples containing the key-value response type, key, and durability information.</param>
    /// <returns>An enumerable collection of gRPC response items corresponding to the provided key-value responses.</returns>
    private static void AddResponseLocksItems(
        RepeatedField<GrpcTryAcquireManyExclusiveLocksResponseItem> target,
        List<(KeyValueResponseType, string, KeyValueDurability, HLCTimestamp, long)> responses
    )
    {
        ReserveItemCapacity(target, responses.Count);

        foreach ((KeyValueResponseType response, string key, KeyValueDurability durability, HLCTimestamp holder, long baseRevision) in responses)
        {
            bool baseObserved = baseRevision != PointLockBase.None;

            target.Add(new GrpcTryAcquireManyExclusiveLocksResponseItem
            {
                Type = (GrpcKeyValueResponseType)response,
                Key = key,
                Durability = (GrpcKeyValueDurability)durability,
                HolderTransactionIdNode     = holder.N,
                HolderTransactionIdPhysical = holder.L,
                HolderTransactionIdCounter  = holder.C,
                BaseObserved = baseObserved,
                BaseRevision = baseObserved ? baseRevision : 0
            });
        }
    }

    /// <summary>
    /// Attempts to release an exclusive lock for the specified key and transaction.
    /// </summary>
    /// <param name="request">The request containing the key, transaction details, and durability level.</param>
    /// <param name="context">The context for the server call.</param>
    /// <returns>A response indicating the result of the release lock attempt.</returns>
    internal async Task<GrpcTryReleaseExclusiveLockResponse> TryReleaseExclusiveLockCore(GrpcTryReleaseExclusiveLockRequest request, ServerCallContext context)
    {
        if (string.IsNullOrEmpty(request.Key))
            return new()
            {
                Type = GrpcKeyValueResponseType.TypeInvalidInput
            };
        
        (KeyValueResponseType type, string _) = await keyValues.LocateAndTryReleaseExclusiveLock(
            new(request.TransactionIdNode, request.TransactionIdPhysical, request.TransactionIdCounter), 
            request.Key, 
            (KeyValueDurability)request.Durability, 
            context.CancellationToken
        );

        return new()
        {
            Type = (GrpcKeyValueResponseType)type
        };
    }

    /// <summary>
    /// Attempts to release an exclusive lock on a group of key-value pairs prefixed by the given key with the specified expiration and durability settings.
    /// </summary>
    /// <param name="request">The request containing the key, transaction details, expiration time, and durability level.</param>
    /// <param name="context">The server call context that provides access to information about the RPC call.</param>
    /// <returns>A task that represents the asynchronous operation. The task result contains a response indicating whether the lock acquisition attempt succeeded or failed.</returns>
    internal async Task<GrpcTryReleaseExclusivePrefixLockResponse> TryReleaseExclusivePrefixLockCore(GrpcTryReleaseExclusivePrefixLockRequest request, ServerCallContext context)
    {
        if (string.IsNullOrEmpty(request.PrefixKey))
            return new()
            {
                Type = GrpcKeyValueResponseType.TypeInvalidInput
            };
        
        KeyValueResponseType type = await keyValues.LocateAndTryReleaseExclusivePrefixLock(
            new(request.TransactionIdNode, request.TransactionIdPhysical, request.TransactionIdCounter), 
            request.PrefixKey,
            (KeyValueDurability)request.Durability, 
            context.CancellationToken
        );

        return new()
        {
            Type = (GrpcKeyValueResponseType)type
        };
    }

    internal async Task<GrpcTryAcquireExclusiveRangeLockResponse> TryAcquireExclusiveRangeLockCore(GrpcTryAcquireExclusiveRangeLockRequest request, ServerCallContext context)
    {
        if (string.IsNullOrEmpty(request.Prefix))
            return new() { Type = GrpcKeyValueResponseType.TypeInvalidInput };

        string? startKey = request.HasStartKey ? request.StartKey : null;
        string? endKey   = request.HasEndKey   ? request.EndKey   : null;

        using LockGrantScope.Scope grantScope = LockGrantScope.Begin(out LockGrantCapture grants);
        (KeyValueResponseType type, HLCTimestamp holder) = await keyValues.LocateAndTryAcquireRangeLock(
            new(request.TransactionIdNode, request.TransactionIdPhysical, request.TransactionIdCounter),
            request.Prefix,
            startKey, request.StartInclusive,
            endKey,   request.EndInclusive,
            request.ExpiresMs,
            (KeyValueDurability)request.Durability,
            (RangeLockMode)request.Mode,
            context.CancellationToken,
            request.CoordinatorKey,
            new TransactionOperationId(request.OperationIdHigh, request.OperationIdLow)
        );

        GrpcTryAcquireExclusiveRangeLockResponse response = new()
        {
            Type = (GrpcKeyValueResponseType)type,
            HolderTransactionIdNode     = holder.N,
            HolderTransactionIdPhysical = holder.L,
            HolderTransactionIdCounter  = holder.C
        };

        AddLockGrantTerms(response.GrantTerms, grants);

        return response;
    }

    internal async Task<GrpcTryReleaseExclusiveRangeLockResponse> TryReleaseExclusiveRangeLockCore(GrpcTryReleaseExclusiveRangeLockRequest request, ServerCallContext context)
    {
        if (string.IsNullOrEmpty(request.Prefix))
            return new() { Type = GrpcKeyValueResponseType.TypeInvalidInput };

        string? startKey = request.HasStartKey ? request.StartKey : null;
        string? endKey   = request.HasEndKey   ? request.EndKey   : null;

        // A partition-pinned release executes on this node's own actor state: the sender targeted
        // the node where the lock was acquired (a split/merge quiesce release), and re-routing
        // through the locator would misdirect the release after a cutover moved the bounds to a
        // new partition — stranding the lock's per-key write intents until their lease expires.
        if (request.HasTargetPartitionId)
        {
            KeyValueResponseType pinnedType = await keyValues.TryReleaseExclusiveRangeLock(
                new(request.TransactionIdNode, request.TransactionIdPhysical, request.TransactionIdCounter),
                request.Prefix,
                startKey, request.StartInclusive,
                endKey,   request.EndInclusive,
                (KeyValueDurability)request.Durability
            );

            return new() { Type = (GrpcKeyValueResponseType)pinnedType };
        }

        KeyValueResponseType type = await keyValues.LocateAndTryReleaseExclusiveRangeLock(
            new(request.TransactionIdNode, request.TransactionIdPhysical, request.TransactionIdCounter),
            request.Prefix,
            startKey, request.StartInclusive,
            endKey,   request.EndInclusive,
            (KeyValueDurability)request.Durability,
            context.CancellationToken
        );

        return new() { Type = (GrpcKeyValueResponseType)type };
    }

    /// <summary>
    /// Attempts to release multiple exclusive locks based on the provided request and context.
    /// </summary>
    /// <param name="request">The request containing transaction details and items to release.</param>
    /// <param name="context">The gRPC server call context for the current operation.</param>
    /// <returns>A task representing the asynchronous operation, containing a response with the release results.</returns>
    internal async Task<GrpcTryReleaseManyExclusiveLocksResponse> TryReleaseManyExclusiveLocksCore(GrpcTryReleaseManyExclusiveLocksRequest request, ServerCallContext context)
    {
        List<(KeyValueResponseType, string, KeyValueDurability)> responses = await keyValues.LocateAndTryReleaseManyExclusiveLocks(
            new(request.TransactionIdNode, request.TransactionIdPhysical, request.TransactionIdCounter), 
            GetRequestReleaseItems(request.Items), 
            context.CancellationToken
        );

        GrpcTryReleaseManyExclusiveLocksResponse response = new();

        AddResponseReleaseItems(response.Items, responses);

        return response;
    }

    /// <summary>
    /// Converts a collection of gRPC request items into a list of tuples containing the key and its associated durability.
    /// </summary>
    /// <param name="items">The collection of gRPC request items containing the key and durability information.</param>
    /// <returns>A list of tuples, where each tuple contains a key as a string and a durability type.</returns>
    private static List<(string key, KeyValueDurability durability)> GetRequestReleaseItems(RepeatedField<GrpcTryReleaseManyExclusiveLocksRequestItem> items)
    {
        List<(string key, KeyValueDurability durability)> rItems = new(items.Count);
        
        foreach (GrpcTryReleaseManyExclusiveLocksRequestItem item in items)
            rItems.Add((item.Key, (KeyValueDurability)item.Durability));
        
        return rItems;
    }

    private static void AddResponseReleaseItems(RepeatedField<GrpcTryReleaseManyExclusiveLocksResponseItem> target, List<(KeyValueResponseType, string, KeyValueDurability)> responses)
    {
        ReserveItemCapacity(target, responses.Count);

        foreach ((KeyValueResponseType response, string key, KeyValueDurability durability) in responses)
            target.Add(new GrpcTryReleaseManyExclusiveLocksResponseItem
            {
                Type = (GrpcKeyValueResponseType)response,
                Key = key,
                Durability = (GrpcKeyValueDurability)durability
            });
    }
}
