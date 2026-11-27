
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

/// <summary>Transactional exclusive, prefix and range lock forwarding.</summary>
public partial class GrpcInterNodeCommunication
{
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
    public async Task<(KeyValueResponseType, string, KeyValueDurability, HLCTimestamp HolderTransactionId, long BaseRevision)> TryAcquireExclusiveLock(
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
            ConflictPolicy = TransactionConflictPolicyWire.ToGrpc(YieldingIntentPolicyScope.Current),
        };

        GrpcServerBatcherResponse response = await ForwardAsync(batcher.Enqueue(request).WaitAsync(cancellationToken), "TryAcquireExclusiveLock", node).ConfigureAwait(false);
        if (!response.IsAnswered)
            return UnansweredResults.TryAcquireExclusiveLock(key, durability);

        GrpcTryAcquireExclusiveLockResponse remoteResponse = response.TryAcquireExclusiveLock!;
        RecordLockGrantTerms(remoteResponse.GrantTerms);

        HLCTimestamp holder = new(remoteResponse.HolderTransactionIdNode, remoteResponse.HolderTransactionIdPhysical, remoteResponse.HolderTransactionIdCounter);
        long baseRevision = remoteResponse.BaseObserved ? remoteResponse.BaseRevision : PointLockBase.None;
        return ((KeyValueResponseType)remoteResponse.Type, key, durability, holder, baseRevision);
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
        
        GrpcServerBatcherResponse response = await ForwardAsync(batcher.Enqueue(request).WaitAsync(cancellationToken), "TryAcquireExclusivePrefixLock", node).ConfigureAwait(false);
        if (!response.IsAnswered)
            return UnansweredResults.TryAcquireExclusivePrefixLock;

        GrpcTryAcquireExclusivePrefixLockResponse remoteResponse = response.TryAcquireExclusivePrefixLock!;
        RecordLockGrantTerms(remoteResponse.GrantTerms);

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
        List<(KeyValueResponseType type, string key, KeyValueDurability durability, HLCTimestamp holder, long baseRevision)> responses,
        CancellationToken cancellationToken
    )
    {
        GrpcServerBatcher batcher = GetSharedBatcher(node);

        GrpcTryAcquireManyExclusiveLocksRequest request = new()
        {
            TransactionIdNode = transactionId.N,
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C,
            ConflictPolicy = TransactionConflictPolicyWire.ToGrpc(YieldingIntentPolicyScope.Current)
        };

        AddAcquireLockRequestItems(request.Items, xkeys);

        GrpcServerBatcherResponse response = await ForwardAsync(batcher.Enqueue(request).WaitAsync(cancellationToken), "TryAcquireNodeExclusiveLocks", node).ConfigureAwait(false);
        if (!response.IsAnswered)
        {
            UnansweredResults.TryAcquireNodeExclusiveLocks(xkeys, lockSync, responses);
            return;
        }

        GrpcTryAcquireManyExclusiveLocksResponse remoteResponse = response.TryAcquireManyExclusiveLocks!;
        RecordLockGrantTerms(remoteResponse.GrantTerms);

        lock (lockSync)
        {
            foreach (GrpcTryAcquireManyExclusiveLocksResponseItem item in remoteResponse.Items)
            {
                HLCTimestamp holder = new(item.HolderTransactionIdNode, item.HolderTransactionIdPhysical, item.HolderTransactionIdCounter);
                long baseRevision = item.BaseObserved ? item.BaseRevision : PointLockBase.None;
                responses.Add(((KeyValueResponseType)item.Type, item.Key, (KeyValueDurability)item.Durability, holder, baseRevision));
            }
        }
    }

    /// <summary>
    /// Replays the leaderships a remote node granted locks under into the capture the caller opened, as the
    /// local grant path writes them directly. A node that predates the field answers none, and the locks it
    /// granted are then not checked at commit.
    /// </summary>
    internal static void RecordLockGrantTerms(RepeatedField<GrpcLockGrantTerm> grantTerms)
    {
        foreach (GrpcLockGrantTerm grant in grantTerms)
            LockGrantScope.Record(grant.PartitionId, grant.Term, grant.RoutingKey);
    }
    
    internal static void AddAcquireLockRequestItems(RepeatedField<GrpcTryAcquireManyExclusiveLocksRequestItem> target, List<(string key, int expiresMs, KeyValueDurability durability)> xkeys)
    {
        ReserveItemCapacity(target, xkeys.Count);

        foreach ((string key, int expiresMs, KeyValueDurability durability) key in xkeys)
            target.Add(new GrpcTryAcquireManyExclusiveLocksRequestItem
            {
                Key = key.key,
                ExpiresMs = key.expiresMs,
                Durability = (GrpcKeyValueDurability)key.durability
            });
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
        
        GrpcServerBatcherResponse response = await ForwardAsync(batcher.Enqueue(request).WaitAsync(cancellationToken), "TryReleaseExclusiveLock", node).ConfigureAwait(false);
        if (!response.IsAnswered)
            return UnansweredResults.TryReleaseExclusiveLock(key);

        GrpcTryReleaseExclusiveLockResponse remoteResponse = response.TryReleaseExclusiveLock!;
        
        
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

        GrpcServerBatcherResponse response = await ForwardAsync(batcher.Enqueue(request).WaitAsync(cancellationToken), "TryReleaseExclusivePrefixLock", node).ConfigureAwait(false);
        if (!response.IsAnswered)
            return UnansweredResults.TryReleaseExclusivePrefixLock;

        GrpcTryReleaseExclusivePrefixLockResponse remoteResponse = response.TryReleaseExclusivePrefixLock!;


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

        GrpcServerBatcherResponse response = await ForwardAsync(batcher.Enqueue(request).WaitAsync(cancellationToken), "TryAcquireRangeLock", node).ConfigureAwait(false);
        if (!response.IsAnswered)
            return UnansweredResults.TryAcquireRangeLock;

        GrpcTryAcquireExclusiveRangeLockResponse remoteResponse = response.TryAcquireExclusiveRangeLock!;
        RecordLockGrantTerms(remoteResponse.GrantTerms);

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
        CancellationToken cancellationToken,
        int? targetPartitionId = null
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
        if (targetPartitionId is not null) request.TargetPartitionId = targetPartitionId.Value;

        GrpcServerBatcherResponse response = await ForwardAsync(batcher.Enqueue(request).WaitAsync(cancellationToken), "TryReleaseExclusiveRangeLock", node).ConfigureAwait(false);
        if (!response.IsAnswered)
            return UnansweredResults.TryReleaseExclusiveRangeLock;

        GrpcTryReleaseExclusiveRangeLockResponse remoteResponse = response.TryReleaseExclusiveRangeLock!;
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
            
        AddReleaseLockRequestItems(request.Items, xkeys);
        
        GrpcServerBatcherResponse response = await ForwardAsync(batcher.Enqueue(request), "TryReleaseNodeExclusiveLocks", node).ConfigureAwait(false);
        if (!response.IsAnswered)
        {
            UnansweredResults.TryReleaseNodeExclusiveLocks(xkeys, lockSync, responses);
            return;
        }

        GrpcTryReleaseManyExclusiveLocksResponse remoteResponse = response.TryReleaseManyExclusiveLocks!;

        lock (lockSync)
        {
            foreach (GrpcTryReleaseManyExclusiveLocksResponseItem item in remoteResponse.Items)
                responses.Add(((KeyValueResponseType)item.Type, item.Key, (KeyValueDurability)item.Durability));
        }
    }
    
    internal static void AddReleaseLockRequestItems(RepeatedField<GrpcTryReleaseManyExclusiveLocksRequestItem> target, List<(string key, KeyValueDurability durability)> xkeys)
    {
        ReserveItemCapacity(target, xkeys.Count);

        foreach ((string key, KeyValueDurability durability) key in xkeys)
            target.Add(new GrpcTryReleaseManyExclusiveLocksRequestItem
            {
                Key = key.key,
                Durability = (GrpcKeyValueDurability)key.durability
            });
    }
}
