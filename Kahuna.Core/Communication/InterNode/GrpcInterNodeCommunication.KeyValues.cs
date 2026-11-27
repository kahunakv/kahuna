
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

/// <summary>Key-value forwarding: single and batched set, delete, extend, get, exists, probe and write-intent checks.</summary>
public partial class GrpcInterNodeCommunication
{
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
            ConflictPolicy = TransactionConflictPolicyWire.ToGrpc(YieldingIntentPolicyScope.Current),
        };
        
        if (value is not null)
            request.Value = UnsafeByteOperations.UnsafeWrap(value);
        
        if (compareValue is not null)
            request.CompareValue = UnsafeByteOperations.UnsafeWrap(compareValue);               
        
        GrpcServerBatcher batcher = GetSharedBatcher(node);
                       
        GrpcServerBatcherResponse response = await ForwardAsync(batcher.Enqueue(request).WaitAsync(cancellationToken), "TrySetKeyValue", node).ConfigureAwait(false);
        if (!response.IsAnswered)
            return UnansweredResults.TrySetKeyValue;

        GrpcTrySetKeyValueResponse remoteResponse = response.TrySetKeyValue!;
        
        
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
        bool checkOnly,
        Lock lockSync, 
        List<KahunaSetKeyValueResponseItem> responses, 
        CancellationToken cancellationToken
    )
    {
        GrpcTrySetManyKeyValueRequest request = new() { CheckOnly = checkOnly };

        AddSetManyRequestItems(request.Items, items);
        
        GrpcServerBatcher batcher = GetSharedBatcher(node);
        
        GrpcServerBatcherResponse response = await ForwardAsync(batcher.Enqueue(request).WaitAsync(cancellationToken), "TrySetManyNodeKeyValue", node).ConfigureAwait(false);
        if (!response.IsAnswered)
        {
            UnansweredResults.TrySetManyNodeKeyValue(items, lockSync, responses);
            return;
        }

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
                    HolderTransactionId = new(item.HolderTransactionIdNode, item.HolderTransactionIdPhysical, item.HolderTransactionIdCounter),
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

        AddDeleteManyRequestItems(request.Items, items);

        GrpcServerBatcher batcher = GetSharedBatcher(node);

        GrpcServerBatcherResponse response = await ForwardAsync(batcher.Enqueue(request).WaitAsync(cancellationToken), "TryDeleteManyNodeKeyValue", node).ConfigureAwait(false);
        if (!response.IsAnswered)
        {
            UnansweredResults.TryDeleteManyNodeKeyValue(items, lockSync, responses);
            return;
        }

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
    /// Reserves room for a batch of known size before its items are added one by one. Without the
    /// reservation the repeated field grows its backing array several times per batch.
    /// </summary>
    private static void ReserveItemCapacity<T>(RepeatedField<T> target, int count)
    {
        if (count == 0)
            return;

        // The field is empty at every current call site, but a later caller may append to a
        // populated one, so the existing items are part of the requirement.
        int required = checked(target.Count + count);

        if (required > target.Capacity)
            target.Capacity = required;
    }

    internal static void AddSetManyRequestItems(RepeatedField<GrpcTrySetManyKeyValueRequestItem> target, List<KahunaSetKeyValueRequestItem> items)
    {
        ReserveItemCapacity(target, items.Count);

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
                ConflictPolicy = TransactionConflictPolicyWire.ToGrpc(item.ConflictPolicy),
            };

            if (item.Value is not null)
                grpcItem.Value = UnsafeByteOperations.UnsafeWrap(item.Value);

            if (item.CompareValue is not null)
                grpcItem.CompareValue = UnsafeByteOperations.UnsafeWrap(item.CompareValue);

            target.Add(grpcItem);
        }
    }

    internal static void AddDeleteManyRequestItems(RepeatedField<GrpcTryDeleteManyKeyValueRequestItem> target, List<KahunaDeleteKeyValueRequestItem> items)
    {
        ReserveItemCapacity(target, items.Count);

        foreach (KahunaDeleteKeyValueRequestItem item in items)
        {
            target.Add(new GrpcTryDeleteManyKeyValueRequestItem
            {
                TransactionIdNode = item.TransactionId.N,
                TransactionIdPhysical = item.TransactionId.L,
                TransactionIdCounter = item.TransactionId.C,
                Key = item.Key,
                Durability = (GrpcKeyValueDurability)item.Durability,
                ConflictPolicy = TransactionConflictPolicyWire.ToGrpc(item.ConflictPolicy),
                Flags = (GrpcKeyValueFlags)item.Flags
            });
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
            ConflictPolicy = TransactionConflictPolicyWire.ToGrpc(YieldingIntentPolicyScope.Current),
        };               
        
        GrpcServerBatcher batcher = GetSharedBatcher(node);
        
        GrpcServerBatcherResponse response = await ForwardAsync(batcher.Enqueue(request).WaitAsync(cancellationToken), "TryDeleteKeyValue", node).ConfigureAwait(false);
        if (!response.IsAnswered)
            return UnansweredResults.TryDeleteKeyValue;

        GrpcTryDeleteKeyValueResponse remoteResponse = response.TryDeleteKeyValue!;
        
        
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
            ConflictPolicy = TransactionConflictPolicyWire.ToGrpc(YieldingIntentPolicyScope.Current),
        };               
        
        GrpcServerBatcher batcher = GetSharedBatcher(node);
        
        GrpcServerBatcherResponse response = await ForwardAsync(batcher.Enqueue(request).WaitAsync(cancellationToken), "TryExtendKeyValue", node).ConfigureAwait(false);
        if (!response.IsAnswered)
            return UnansweredResults.TryExtendKeyValue;

        GrpcTryExtendKeyValueResponse remoteResponse = response.TryExtendKeyValue!;
        
        
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
        CancellationToken cancellationToken,
        KeyValueReadMode readMode = KeyValueReadMode.Default,
        KeyValueIntentWait intentWait = default
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
            ReadMode              = (GrpcKeyValueReadMode)readMode,
            IntentWaitMs          = intentWait.LimitMs,
            IntentWaitHoldsLocks  = intentWait.HoldsLocks,
        };
        
        GrpcServerBatcher batcher = GetSharedBatcher(node);               
        
        GrpcServerBatcherResponse response = await ForwardAsync(batcher.Enqueue(request).WaitAsync(cancellationToken), "TryGetValue", node).ConfigureAwait(false);
        if (!response.IsAnswered)
            return UnansweredResults.TryGetValue;

        GrpcTryGetKeyValueResponse remoteResponse = response.TryGetKeyValue!;
        
        
        // The remote leader leaves the field unset for a key that holds no value; decoding the field alone
        // would make a routed read report zero bytes where a local read of the same key reports null.
        byte[]? value = ByteStringPayload.GetArrayOrNull(remoteResponse.HasValue, remoteResponse.Value);
        
        return ((KeyValueResponseType)remoteResponse.Type, new(
            value,
            remoteResponse.Revision,
            new(remoteResponse.ExpiresNode, remoteResponse.ExpiresPhysical, remoteResponse.ExpiresCounter),
            new(remoteResponse.LastUsedNode, remoteResponse.LastUsedPhysical, remoteResponse.LastUsedCounter),
            new(remoteResponse.LastModifiedNode, remoteResponse.LastModifiedPhysical, remoteResponse.LastModifiedCounter),
            (KeyValueState)remoteResponse.State
        )
        {
            HeadSeenTicks = remoteResponse.OwnStagedWrite ? ReadOnlyKeyValueEntry.OwnStagedWrite : 0
        });
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
        
        GrpcServerBatcherResponse response = await ForwardAsync(batcher.Enqueue(request).WaitAsync(cancellationToken), "TryExistsValue", node).ConfigureAwait(false);
        if (!response.IsAnswered)
            return UnansweredResults.TryExistsValue;

        GrpcTryExistsKeyValueResponse remoteResponse = response.TryExistsKeyValue!;
        
        
        return ((KeyValueResponseType)remoteResponse.Type, new(
            null,
            remoteResponse.Revision,
            new(remoteResponse.ExpiresNode, remoteResponse.ExpiresPhysical, remoteResponse.ExpiresCounter),
            new(remoteResponse.LastUsedNode, remoteResponse.LastUsedPhysical, remoteResponse.LastUsedCounter),
            new(remoteResponse.LastModifiedNode, remoteResponse.LastModifiedPhysical, remoteResponse.LastModifiedCounter),
            (KeyValueState)remoteResponse.State
        )
        {
            HeadSeenTicks = remoteResponse.OwnStagedWrite ? ReadOnlyKeyValueEntry.OwnStagedWrite : 0
        });
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

        AddTryManyValuesRequestItems(request.Items, keys);

        GrpcServerBatcherResponse response = await ForwardAsync(batcher.Enqueue(request).WaitAsync(cancellationToken), "TryGetManyNodeValues", node).ConfigureAwait(false);
        if (!response.IsAnswered)
        {
            UnansweredResults.TryGetManyNodeValues(keys, lockSync, responses);
            return;
        }

        GrpcTryGetManyValuesResponse remoteResponse = response.TryGetManyValues!;

        lock (lockSync)
        {
            foreach (GrpcTryGetManyValuesResponseItem item in remoteResponse.Items)
                responses.Add(((KeyValueResponseType)item.Type, item.Key, (KeyValueDurability)item.Durability, GetReadOnlyKeyValueEntry(item)));
        }
    }

    public async Task<KeyValueProbeResult[]> TryProbeMany(
        string node,
        HLCTimestamp transactionId,
        HLCTimestamp readTimestamp,
        string[] keys,
        KeyValueProbeLock lockMode,
        int expiresMs,
        KeyValueDurability durability,
        CancellationToken cancellationToken,
        KeyValueReadMode readMode = KeyValueReadMode.Default,
        KeyValueIntentWait intentWait = default
    )
    {
        GrpcServerBatcher batcher = GetSharedBatcher(node);

        GrpcTryProbeManyRequest request = ProbeManyWire.ToRequest(transactionId, readTimestamp, keys, lockMode, expiresMs, durability, readMode, intentWait);

        GrpcServerBatcherResponse response = await ForwardAsync(batcher.Enqueue(request).WaitAsync(cancellationToken), "TryProbeMany", node).ConfigureAwait(false);
        if (!response.IsAnswered)
            return UnansweredResults.TryProbeMany(keys.Length);

        GrpcTryProbeManyResponse remoteResponse = response.TryProbeMany!;
        RecordLockGrantTerms(remoteResponse.GrantTerms);

        return ProbeManyWire.FromItems(remoteResponse.Items, keys.Length);
    }

    public async Task<KeyValueProbeRangeResult[]> TryProbeManyRanges(
        string node,
        HLCTimestamp transactionId,
        HLCTimestamp readTimestamp,
        string prefix,
        KeyValueProbeRange[] seeks,
        KeyValueProbeLock lockMode,
        int expiresMs,
        KeyValueDurability durability,
        CancellationToken cancellationToken,
        KeyValueReadMode readMode = KeyValueReadMode.Default
    )
    {
        GrpcServerBatcher batcher = GetSharedBatcher(node);

        GrpcTryProbeManyRangesRequest request = ProbeManyRangesWire.ToRequest(transactionId, readTimestamp, prefix, seeks, lockMode, expiresMs, durability, readMode);

        GrpcServerBatcherResponse response = await ForwardAsync(batcher.Enqueue(request).WaitAsync(cancellationToken), "TryProbeManyRanges", node).ConfigureAwait(false);
        if (!response.IsAnswered)
            return UnansweredResults.TryProbeManyRanges(seeks.Length);

        GrpcTryProbeManyRangesResponse remoteResponse = response.TryProbeManyRanges!;
        RecordLockGrantTerms(remoteResponse.GrantTerms);

        return ProbeManyRangesWire.FromItems(remoteResponse.Items, seeks.Length);
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

        AddTryManyValuesRequestItems(request.Items, keys);

        GrpcServerBatcherResponse response = await ForwardAsync(batcher.Enqueue(request).WaitAsync(cancellationToken), "TryExistsManyNodeValues", node).ConfigureAwait(false);
        if (!response.IsAnswered)
        {
            UnansweredResults.TryExistsManyNodeValues(keys, lockSync, responses);
            return;
        }

        GrpcTryExistsManyValuesResponse remoteResponse = response.TryExistsManyValues!;

        lock (lockSync)
        {
            foreach (GrpcTryExistsManyValuesResponseItem item in remoteResponse.Items)
                responses.Add(((KeyValueResponseType)item.Type, item.Key, (KeyValueDurability)item.Durability, GetReadOnlyKeyValueEntry(item)));
        }
    }

    internal static void AddTryManyValuesRequestItems(
        RepeatedField<GrpcTryManyValuesRequestItem> target,
        List<(string key, long revision, KeyValueDurability durability)> keys
    )
    {
        ReserveItemCapacity(target, keys.Count);

        foreach ((string key, long revision, KeyValueDurability durability) item in keys)
        {
            target.Add(new GrpcTryManyValuesRequestItem
            {
                Key = item.key,
                Revision = item.revision,
                Durability = (GrpcKeyValueDurability)item.durability
            });
        }
    }

    /// <summary>
    /// Decodes one item of a routed batch read. The remote leader leaves the value field unset for a key that
    /// holds no value and writes a present, empty field for a key that holds zero bytes, so a decoder that reads
    /// the field alone reports null where a local read of the same key reports an empty array. The bytes are
    /// borrowed from the parsed message instead of copied, which is the contract the point read above follows.
    /// </summary>
    internal static ReadOnlyKeyValueEntry? GetReadOnlyKeyValueEntry(GrpcTryGetManyValuesResponseItem item)
    {
        if ((KeyValueResponseType)item.Type is not (KeyValueResponseType.Get or KeyValueResponseType.Exists))
            return null;

        return new(
            ByteStringPayload.GetArrayOrNull(item.HasValue, item.Value),
            item.Revision,
            new(item.ExpiresNode, item.ExpiresPhysical, item.ExpiresCounter),
            new(item.LastUsedNode, item.LastUsedPhysical, item.LastUsedCounter),
            new(item.LastModifiedNode, item.LastModifiedPhysical, item.LastModifiedCounter),
            (KeyValueState)item.State
        );
    }

    /// <summary>
    /// Decodes one item of a routed batch existence probe. An existence probe carries no payload, so the entry
    /// holds a null value for every item, the same as the point probe above.
    /// </summary>
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

        GrpcServerBatcherResponse response = await ForwardAsync(batcher.Enqueue(request).WaitAsync(cancellationToken), "TryCheckWriteIntentValue", node).ConfigureAwait(false);
        if (!response.IsAnswered)
            return UnansweredResults.TryCheckWriteIntentValue;

        GrpcTryCheckWriteIntentResponse remoteResponse = response.TryCheckWriteIntent!;

        return (KeyValueResponseType)remoteResponse.Type;
    }

    /// <summary>
    /// Probes every key of one owning node for concurrent write intents in a single call, so validating a read
    /// set costs one round trip per node rather than one per key.
    /// </summary>
    public async Task<List<(KeyValueResponseType type, string key, KeyValueDurability durability)>> TryCheckManyWriteIntents(
        string node,
        HLCTimestamp transactionId,
        List<KeyValueConflictProbe> keys,
        CancellationToken cancellationToken
    )
    {
        GrpcTryCheckManyWriteIntentsRequest request = new()
        {
            TransactionIdNode = transactionId.N,
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C
        };

        foreach (KeyValueConflictProbe probe in keys)
            request.Items.Add(new GrpcTryCheckManyWriteIntentsRequestItem
            {
                Key = probe.Key,
                Durability = (GrpcKeyValueDurability)probe.Durability,
                Checks = (uint)probe.Checks,
                BaseRevision = probe.BaseRevision
            });

        GrpcServerBatcher batcher = GetSharedBatcher(node);

        GrpcServerBatcherResponse response = await ForwardAsync(batcher.Enqueue(request).WaitAsync(cancellationToken), "TryCheckManyWriteIntents", node).ConfigureAwait(false);
        if (!response.IsAnswered)
            return UnansweredResults.TryCheckManyWriteIntents(keys);

        GrpcTryCheckManyWriteIntentsResponse remoteResponse = response.TryCheckManyWriteIntents!;

        // The terms the remote leader confirmed the caller's staged intents under, replayed into the caller's
        // capture exactly as a remote lock grant's are; the commit floor those intents carry travels with them.
        RecordLockGrantTerms(remoteResponse.GrantTerms);
        LockGrantScope.RaiseCommitFloor(new HLCTimestamp(remoteResponse.CommitFloorNode, remoteResponse.CommitFloorPhysical, remoteResponse.CommitFloorCounter));

        List<(KeyValueResponseType type, string key, KeyValueDurability durability)> responses = new(remoteResponse.Items.Count);

        foreach (GrpcTryCheckManyWriteIntentsResponseItem item in remoteResponse.Items)
            responses.Add(((KeyValueResponseType)item.Type, item.Key, (KeyValueDurability)item.Durability));

        return responses;
    }
}
