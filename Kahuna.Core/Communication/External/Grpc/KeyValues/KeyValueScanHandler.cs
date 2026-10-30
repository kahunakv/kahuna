
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
/// Bucket, range and prefix scans.
/// </summary>
internal sealed class KeyValueScanHandler
{
    private readonly IKahuna keyValues;

    public KeyValueScanHandler(IKahuna keyValues)
    {
        this.keyValues = keyValues;
    }

    /// <summary>
    /// Handles the logic to retrieve key-value pairs based on a specified prefix.
    /// </summary>
    /// <param name="request">The request containing the prefix key and additional parameters for the operation.</param>
    /// <param name="context">The server call context for managing RPC settings and cancellation.</param>
    /// <returns>A task that represents the asynchronous operation, returning a response with the matching key-value pairs or an error type.</returns>
    internal async Task<GrpcGetByBucketResponse> GetByBucketCore(GrpcGetByBucketRequest request, ServerCallContext context)
    {
        if (request.PrefixKey is null)
            return new()
            {
                Type = GrpcKeyValueResponseType.TypeInvalidInput
            };
            
        KeyValueGetByBucketResult result = await keyValues.LocateAndGetByBucket(
            new(request.TransactionIdNode, request.TransactionIdPhysical, request.TransactionIdCounter),
            request.PrefixKey,
            new(request.ReadTimestampNode, request.ReadTimestampPhysical, request.ReadTimestampCounter),
            (KeyValueDurability)request.Durability,
            context.CancellationToken,
            request.CoordinatorKey,
            new TransactionOperationId(request.OperationIdHigh, request.OperationIdLow)
        );

        GrpcGetByBucketResponse response = new()
        {
            Type = (GrpcKeyValueResponseType)result.Type
        };
        
        AddKeyValueItems(response.Items, result.Items);
        
        return response;
    }

    /// <summary>
    /// Batch-batcher entry point: handles a single GetByRange page request, matching the pattern
    /// used by <see cref="GetByBucketInternal"/>.
    /// </summary>
    internal async Task<GrpcGetByRangeResponse> GetByRangeCore(GrpcGetByRangeRequest request, ServerCallContext context)
    {
        if (string.IsNullOrEmpty(request.Prefix))
            return new() { Type = GrpcKeyValueResponseType.TypeInvalidInput };

        string? startKey = request.HasStartKey ? request.StartKey : null;
        string? endKey   = request.HasEndKey   ? request.EndKey   : null;

        KeyValueGetByRangeResult result = await keyValues.LocateAndGetByRange(
            new(request.TransactionIdNode, request.TransactionIdPhysical, request.TransactionIdCounter),
            request.Prefix,
            startKey,
            startKey is null || request.StartInclusive,
            endKey,
            endKey is not null && request.EndInclusive,
            request.Limit,
            new(request.ReadTimestampNode, request.ReadTimestampPhysical, request.ReadTimestampCounter),
            (KeyValueDurability)request.Durability,
            context.CancellationToken,
            request.CoordinatorKey,
            new TransactionOperationId(request.OperationIdHigh, request.OperationIdLow),
            request.SnapshotAtLeader,
            (KeyValueReadMode)request.ReadMode);

        GrpcGetByRangeResponse response = new()
        {
            Type    = (GrpcKeyValueResponseType)result.Type,
            HasMore = result.HasMore,
        };

        if (result.NextCursor is not null)
            response.NextCursor = result.NextCursor;

        if (result.Observation is { } observation)
            response.Observation = ScanObservationWire.ToGrpc(observation);

        AddKeyValueItems(response.Items, result.Items);

        return response;
    }

    /// <summary>
    /// Handles the internal logic for scanning key-value items by a given prefix.
    /// </summary>
    /// <param name="request">The request containing the prefix key and other parameters for the scan operation.</param>
    /// <param name="context">The server call context for the gRPC operation.</param>
    /// <returns>A task representing the asynchronous operation, containing the response with the scanned key-value items.</returns>
    internal async Task<GrpcScanByPrefixResponse> ScanByPrefixCore(GrpcScanByPrefixRequest request, ServerCallContext context)
    {
        if (request.PrefixKey is null)
            return new()
            {
                Type = GrpcKeyValueResponseType.TypeInvalidInput
            };
            
        HLCTimestamp readTimestamp = new(request.ReadTimestampNode, request.ReadTimestampPhysical, request.ReadTimestampCounter);

        KeyValueGetByBucketResult result = await keyValues.ScanByPrefix(request.PrefixKey, readTimestamp, (KeyValueDurability) request.Durability, request.IncludeTombstones);

        GrpcScanByPrefixResponse response = new()
        {
            Type = GrpcKeyValueResponseType.TypeGot,
        };
        
        AddKeyValueItems(response.Items, result.Items);
        
        return response;
    }

    /// <summary>
    /// Processes a request to scan all key-value pairs with a specified prefix internally.
    /// </summary>
    /// <param name="request">The gRPC request containing the prefix key and additional scan criteria.</param>
    /// <param name="context">The server call context providing details about the request context.</param>
    /// <returns>A task that represents the asynchronous operation, returning a response with the scan results or an error type.</returns>
    internal async Task<GrpcScanAllByPrefixResponse> ScanAllByPrefixCore(GrpcScanAllByPrefixRequest request, ServerCallContext context)
    {
        if (request.PrefixKey is null)
            return new()
            {
                Type = GrpcKeyValueResponseType.TypeInvalidInput
            };
            
        HLCTimestamp readTimestamp = request.ReadTimestampNode == 0 && request.ReadTimestampPhysical == 0
            ? HLCTimestamp.Zero
            : new HLCTimestamp(request.ReadTimestampNode, request.ReadTimestampPhysical, request.ReadTimestampCounter);

        KeyValueGetByBucketResult result = await keyValues.ScanAllByPrefix(request.PrefixKey, readTimestamp, (KeyValueDurability) request.Durability, context.CancellationToken);

        GrpcScanAllByPrefixResponse response = new()
        {
            Type = GrpcKeyValueResponseType.TypeGot,
        };
        
        if (result.Items.Count > 0)
            AddKeyValueItems(response.Items, result.Items);
        
        return response;
    }
}
