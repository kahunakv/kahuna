
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

namespace Kahuna.Communication.External.Grpc;

public sealed partial class KeyValuesService
{
    public override async Task<GrpcGetByRangeResponse> GetByRange(
        GrpcGetByRangeRequest request,
        ServerCallContext context
    )
    {
        // The client's page size is bounded here and not in GetByRangeInternal: the inter-node batcher shares
        // that path, and it forwards pages whose size the forwarding node already resolved.
        if (!KeyValueScanLimits.TryBoundRangePageLimit(request.Limit, out int limit))
            return new() { Type = GrpcKeyValueResponseType.TypeInvalidInput };

        request.Limit = limit;

        return await GetByRangeInternal(request, context);
    }

    /// <summary>
    /// Retrieves key-value pairs matching a specific prefix.
    /// </summary>
    /// <param name="request">The request containing the prefix to match against.</param>
    /// <param name="context">The gRPC server call context.</param>
    /// <returns>A response containing the key-value pairs that match the specified prefix.</returns>
    public override async Task<GrpcGetByBucketResponse> GetByBucket(GrpcGetByBucketRequest request, ServerCallContext context)
    {
        return await GetByBucketInternal(request, context);
    }

    /// <summary>
    /// Server-streaming RPC: pages through all items in the given range, emitting one
    /// <see cref="GrpcGetByRangePageResponse"/> per page so the leader never buffers the full table.
    /// The snapshot timestamp captured on the first page is carried in each cursor and re-used on
    /// subsequent pages to guarantee a consistent view across the stream.
    /// </summary>
    public override async Task GetByRangeStream(
        GrpcGetByRangeRequest request,
        IServerStreamWriter<GrpcGetByRangePageResponse> responseStream,
        ServerCallContext context)
    {
        if (string.IsNullOrEmpty(request.Prefix) || !KeyValueScanLimits.TryBoundRangePageLimit(request.Limit, out int limit))
            return;

        string? startKey    = request.HasStartKey ? request.StartKey : null;
        string? endKey      = request.HasEndKey   ? request.EndKey   : null;
        bool startInclusive = !request.HasStartKey || request.StartInclusive;
        HLCTimestamp readTimestamp = new(request.ReadTimestampNode, request.ReadTimestampPhysical, request.ReadTimestampCounter);

        while (true)
        {
            KeyValueGetByRangeResult result = await keyValues.LocateAndGetByRange(
                new(request.TransactionIdNode, request.TransactionIdPhysical, request.TransactionIdCounter),
                request.Prefix,
                startKey,
                startInclusive,
                endKey,
                endKey is not null && request.EndInclusive,
                limit,
                readTimestamp,
                (KeyValueDurability)request.Durability,
                context.CancellationToken,
                snapshotAtLeader: request.SnapshotAtLeader,
                readMode: (KeyValueReadMode)request.ReadMode);

            GrpcGetByRangePageResponse pageResponse = new()
            {
                Type    = (GrpcKeyValueResponseType)result.Type,
                HasMore = result.HasMore,
            };

            if (result.NextCursor is not null)
                pageResponse.NextCursor = result.NextCursor;

            if (result.Observation is { } pageObservation)
                pageResponse.Observation = ScanObservationWire.ToGrpc(pageObservation);

            AddKeyValueItems(pageResponse.Items, result.Items);

            await responseStream.WriteAsync(pageResponse);

            if (!result.HasMore || result.NextCursor is null)
                break;

            // Cursor resume: exclusive start on the bare lastKey so no key in
            // (lastKey, nextKey) is skipped — in particular, keys where lastKey
            // is a strict prefix of the candidate (e.g. variable-length index keys).
            if (!KeyValueRangeCursor.TryDecode(result.NextCursor, out string lastKey, out _, out _, out HLCTimestamp cursorTs))
                break;

            startKey       = lastKey;
            startInclusive = false;
            readTimestamp  = cursorTs;
        }
    }

    /// <summary>
    /// Scans for key-value pairs based on a specified prefix.
    /// </summary>
    /// <param name="request">The request containing the prefix and relevant parameters for the scan operation.</param>
    /// <param name="context">The context for the server-side call, providing information such as deadlines and cancellation details.</param>
    /// <returns>A task representing the asynchronous operation, which resolves to a <see cref="GrpcScanByPrefixResponse"/> containing the scan results.</returns>
    public override async Task<GrpcScanByPrefixResponse> ScanByPrefix(GrpcScanByPrefixRequest request, ServerCallContext context)
    {
        return await ScanByPrefixInternal(request, context);
    }

    /// <summary>
    /// Scans all nodes in the cluster and returns key/value pairs by prefix
    /// </summary>
    /// <param name="request"></param>
    /// <param name="context"></param>
    /// <returns></returns>
    public override async Task<GrpcScanAllByPrefixResponse> ScanAllByPrefix(GrpcScanAllByPrefixRequest request, ServerCallContext context)
    {
        return await ScanAllByPrefixInternal(request, context);
    }

    internal Task<GrpcGetByBucketResponse> GetByBucketInternal(GrpcGetByBucketRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.scans.GetByBucketCore(r, c), static _ => KeyValueMustRetry.GetByBucket());

    internal Task<GrpcGetByRangeResponse> GetByRangeInternal(GrpcGetByRangeRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.scans.GetByRangeCore(r, c), static _ => KeyValueMustRetry.GetByRange());

    internal Task<GrpcScanByPrefixResponse> ScanByPrefixInternal(GrpcScanByPrefixRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.scans.ScanByPrefixCore(r, c), static _ => KeyValueMustRetry.ScanByPrefix());

    internal Task<GrpcScanAllByPrefixResponse> ScanAllByPrefixInternal(GrpcScanAllByPrefixRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.scans.ScanAllByPrefixCore(r, c), static _ => KeyValueMustRetry.ScanAllByPrefix());
}
