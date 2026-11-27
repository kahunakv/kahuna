
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

/// <summary>Scan forwarding: bucket, range and prefix reads.</summary>
public partial class GrpcInterNodeCommunication
{
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
        
        GrpcServerBatcherResponse batchResponse = await ForwardAsync(batcher.Enqueue(request).WaitAsync(cancellationToken), "GetByBucket", node).ConfigureAwait(false);
        if (!batchResponse.IsAnswered)
            return UnansweredResults.GetByBucket;
       
        GrpcGetByBucketResponse remoteResponse = batchResponse.GetByBucket!;
        
        
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
        CancellationToken cancellationToken,
        bool snapshotAtLeader = false,
        KeyValueReadMode readMode = KeyValueReadMode.Default)
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
            SnapshotAtLeader      = snapshotAtLeader,
            ReadMode              = (GrpcKeyValueReadMode)readMode,
        };

        if (startKey is not null) request.StartKey = startKey;
        if (endKey   is not null) request.EndKey   = endKey;

        GrpcServerBatcherResponse batchResponse = await ForwardAsync(batcher.Enqueue(request).WaitAsync(cancellationToken), "GetByRange", node).ConfigureAwait(false);
        if (!batchResponse.IsAnswered)
            return UnansweredResults.GetByRange;

        GrpcGetByRangeResponse remoteResponse = batchResponse.GetByRange!;

        return new(
            (KeyValueResponseType)remoteResponse.Type,
            GetReadOnlyItem(remoteResponse.Items),
            remoteResponse.HasNextCursor ? remoteResponse.NextCursor : null,
            remoteResponse.HasMore,
            ScanObservationWire.FromGrpc(remoteResponse.Observation));
    }

    public async Task<KeyValueGetByBucketResult> ScanByPrefix(string node, string prefixedKey, HLCTimestamp readTimestamp, KeyValueDurability durability, bool includeTombstones, CancellationToken cancellationToken)
    {
        GrpcServerBatcher batcher = GetSharedBatcher(node);

        GrpcScanByPrefixRequest request = new()
        {
            PrefixKey = prefixedKey,
            Durability = (GrpcKeyValueDurability)durability,
            ReadTimestampNode = readTimestamp.N,
            ReadTimestampPhysical = readTimestamp.L,
            ReadTimestampCounter = readTimestamp.C,
            IncludeTombstones = includeTombstones,
        };
        
        GrpcServerBatcherResponse batchResponse = await ForwardAsync(batcher.Enqueue(request).WaitAsync(cancellationToken), "ScanByPrefix", node).ConfigureAwait(false);
        if (!batchResponse.IsAnswered)
            return UnansweredResults.ScanByPrefix;
       
        GrpcScanByPrefixResponse remoteResponse = batchResponse.ScanByPrefix!;
        
        
        return new((KeyValueResponseType)remoteResponse.Type, GetReadOnlyItem(remoteResponse.Items));
    }

    private static List<(string, ReadOnlyKeyValueEntry)> GetReadOnlyItem(RepeatedField<GrpcKeyValueByPrefixItemResponse> remoteResponseItems)
    {
        List<(string, ReadOnlyKeyValueEntry)> responses = new(remoteResponseItems.Count);
        
        foreach (GrpcKeyValueByPrefixItemResponse? kv in remoteResponseItems)
        {
            byte[]? value = ByteStringPayload.GetArrayOrNull(kv.HasValue, kv.Value);
            
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
}
