
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
/// Single-key and batched writes: set, extend and delete.
/// </summary>
internal sealed class KeyValueWriteHandler
{
    private readonly IKahuna keyValues;

    public KeyValueWriteHandler(IKahuna keyValues)
    {
        this.keyValues = keyValues;
    }

    /// <summary>
    /// Attempts to set a key-value pair within the key-value store with additional parameters such as expiration, flags, and durability.
    /// </summary>
    /// <param name="request">The request containing the key, value, comparison value, flags, expiration time, durability level, and transaction details.</param>
    /// <param name="context">The server call context associated with the operation, including cancellation tokens and call metadata.</param>
    /// <returns>A response indicating the result of the operation, including the response type, revision number, last modified timestamp, and elapsed time in milliseconds.</returns>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    internal async ValueTask<GrpcTrySetKeyValueResponse> TrySetKeyValueCore(GrpcTrySetKeyValueRequest request, ServerCallContext context)
    {
        if (string.IsNullOrEmpty(request.Key) || request.ExpiresMs < 0)
            return new()
            {
                Type = GrpcKeyValueResponseType.TypeInvalidInput
            };

        ValueStopwatch stopwatch = ValueStopwatch.StartNew();

        // Presence, not content: the client leaves the field unset for a null payload, so decoding the field
        // alone would store zero bytes for a key the caller set to no value. The REST transport carries the
        // same call as an explicit null, and the two must reach the store as the same write.
        byte[]? value = ByteStringPayload.GetArrayOrNull(request.HasValue, request.Value);

        byte[]? compareValue = ByteStringPayload.GetArrayOrNull(request.HasCompareValue, request.CompareValue);
        
        using RouteCaptureScope.Scope routeScope = RouteCaptureScope.Begin(out RouteCapture? capture);

        using YieldingIntentPolicyScope.Scope setPolicyScope = YieldingIntentPolicyScope.Enter(TransactionConflictPolicyWire.FromGrpc(request.ConflictPolicy));
        (KeyValueResponseType response, long revision, HLCTimestamp lastModified) = await keyValues.LocateAndTrySetKeyValue(
            new(request.TransactionIdNode, request.TransactionIdPhysical, request.TransactionIdCounter),
            request.Key,
            value,
            compareValue,
            request.CompareRevision,
            (KeyValueFlags)request.Flags,
            request.ExpiresMs,
            (KeyValueDurability)request.Durability,
            context.CancellationToken,
            request.RoutedGeneration,
            request.CoordinatorKey,
            new TransactionOperationId(request.OperationIdHigh, request.OperationIdLow)
        );

        GrpcTrySetKeyValueResponse setResponse = new()
        {
            Type = (GrpcKeyValueResponseType)response,
            Revision = revision,
            LastModifiedNode = lastModified.N,
            LastModifiedPhysical = lastModified.L,
            LastModifiedCounter = lastModified.C,
            TimeElapsedMs = (int)stopwatch.GetElapsedMilliseconds()
        };

        // A conditional write that failed its compare is a terminal answer about a key whose owner
        // was resolved, so it carries the hint like a success does.
        GrpcRouteHint? setRoute = RouteHintWriter.Grpc(capture, KahunaRoutingDomain.KeyValue, request.Key);
        if (setRoute is not null)
            setResponse.Route = setRoute;

        return setResponse;
    }

    /// <summary>
    /// Attempts to set many key-value pairs within the key-value store with additional parameters such as expiration, flags, and durability.
    /// </summary>
    /// <param name="request">The request containing the key, value, comparison value, flags, expiration time, durability level, and transaction details.</param>
    /// <param name="context">The server call context associated with the operation, including cancellation tokens and call metadata.</param>
    /// <returns>A response indicating the result of the operation, including the response type, revision number, last modified timestamp, and elapsed time in milliseconds.</returns>
    internal async Task<GrpcTrySetManyKeyValueResponse> TrySetManyKeyValueCore(GrpcTrySetManyKeyValueRequest request, ServerCallContext context)
    {       
        ValueStopwatch stopwatch = ValueStopwatch.StartNew();
   
        using RouteCaptureScope.Scope routeScope = RouteCaptureScope.Begin(out RouteCapture? capture);

        List<KahunaSetKeyValueResponseItem> responses = await keyValues.LocateAndTrySetManyKeyValue(
            GetRequestSetManyItems(request.Items),
            context.CancellationToken
        );
               
        GrpcTrySetManyKeyValueResponse response = new()        
        {
            TimeElapsedMs = (int)stopwatch.GetElapsedMilliseconds()
        };

        RouteHintWriter.Table routes = new(capture);

        AddResponseSetManyItems(response.Items, responses, routes);

        if (routes.HasRoutes)
            routes.WriteGrpc(response.Routes);

        return response;
    }

    private static List<KahunaSetKeyValueRequestItem> GetRequestSetManyItems(RepeatedField<GrpcTrySetManyKeyValueRequestItem> items)
    {                 
        List<KahunaSetKeyValueRequestItem> requestItems = new(items.Count);
        
        foreach (GrpcTrySetManyKeyValueRequestItem item in items)
        {
            // An absent value is a null payload, not an empty one — see TrySetKeyValueCore.
            byte[]? value = ByteStringPayload.GetArrayOrNull(item.HasValue, item.Value);

            byte[]? compareValue = ByteStringPayload.GetArrayOrNull(item.HasCompareValue, item.CompareValue);
            
            requestItems.Add(new()
            {
                TransactionId = new(item.TransactionIdNode, item.TransactionIdPhysical, item.TransactionIdCounter),
                Key = item.Key,
                Value = value,
                CompareValue = compareValue,
                CompareRevision = item.CompareRevision,
                ExpiresMs = item.ExpiresMs,
                Flags = (KeyValueFlags)item.Flags,
                Durability = (KeyValueDurability)item.Durability,
                RoutedGeneration = item.RoutedGeneration,
                ConflictPolicy = TransactionConflictPolicyWire.FromGrpc(item.ConflictPolicy),
            });
        }

        return requestItems;
    }

    private static void AddResponseSetManyItems(RepeatedField<GrpcTrySetManyKeyValueResponseItem> target, List<KahunaSetKeyValueResponseItem> responses, RouteHintWriter.Table routes)
    {
        ReserveItemCapacity(target, responses.Count);

        foreach (KahunaSetKeyValueResponseItem response in responses)
        {
            target.Add(new GrpcTrySetManyKeyValueResponseItem
            {
                Type = (GrpcKeyValueResponseType)response.Type,
                Key = response.Key,
                Revision = response.Revision,
                LastModifiedNode = response.LastModified.N,
                LastModifiedPhysical = response.LastModified.L,
                LastModifiedCounter = response.LastModified.C,
                Durability = (GrpcKeyValueDurability)response.Durability,
                RouteIndex = routes.IndexOf(KahunaRoutingDomain.KeyValue, response.Key ?? ""),
                HolderTransactionIdNode = response.HolderTransactionId.N,
                HolderTransactionIdPhysical = response.HolderTransactionId.L,
                HolderTransactionIdCounter = response.HolderTransactionId.C
            });
        }
    }

    internal async Task<GrpcTryDeleteManyKeyValueResponse> TryDeleteManyKeyValueCore(GrpcTryDeleteManyKeyValueRequest request, ServerCallContext context)
    {
        ValueStopwatch stopwatch = ValueStopwatch.StartNew();

        string coordinatorKey = request.HasCoordinatorKey ? request.CoordinatorKey : "";
        TransactionOperationId operationId = request is { HasOperationIdHigh: true, HasOperationIdLow: true }
            ? new(request.OperationIdHigh, request.OperationIdLow)
            : default;

        using RouteCaptureScope.Scope routeScope = RouteCaptureScope.Begin(out RouteCapture? capture);

        List<KahunaDeleteKeyValueResponseItem> responses = await keyValues.LocateAndTryDeleteManyKeyValue(
            GetRequestDeleteManyItems(request.Items),
            context.CancellationToken,
            coordinatorKey,
            operationId
        );

        GrpcTryDeleteManyKeyValueResponse response = new()
        {
            TimeElapsedMs = (int)stopwatch.GetElapsedMilliseconds()
        };

        RouteHintWriter.Table routes = new(capture);

        AddResponseDeleteManyItems(response.Items, responses, routes);

        if (routes.HasRoutes)
            routes.WriteGrpc(response.Routes);

        return response;
    }

    private static List<KahunaDeleteKeyValueRequestItem> GetRequestDeleteManyItems(RepeatedField<GrpcTryDeleteManyKeyValueRequestItem> items)
    {
        List<KahunaDeleteKeyValueRequestItem> requestItems = new(items.Count);

        foreach (GrpcTryDeleteManyKeyValueRequestItem item in items)
        {
            requestItems.Add(new()
            {
                TransactionId = new(item.TransactionIdNode, item.TransactionIdPhysical, item.TransactionIdCounter),
                Key = item.Key,
                Durability = (KeyValueDurability)item.Durability,
                ConflictPolicy = TransactionConflictPolicyWire.FromGrpc(item.ConflictPolicy)
            });
        }

        return requestItems;
    }

    private static void AddResponseDeleteManyItems(RepeatedField<GrpcTryDeleteManyKeyValueResponseItem> target, List<KahunaDeleteKeyValueResponseItem> responses, RouteHintWriter.Table routes)
    {
        ReserveItemCapacity(target, responses.Count);

        foreach (KahunaDeleteKeyValueResponseItem response in responses)
        {
            target.Add(new GrpcTryDeleteManyKeyValueResponseItem
            {
                Type = (GrpcKeyValueResponseType)response.Type,
                Key = response.Key,
                Revision = response.Revision,
                LastModifiedNode = response.LastModified.N,
                LastModifiedPhysical = response.LastModified.L,
                LastModifiedCounter = response.LastModified.C,
                Durability = (GrpcKeyValueDurability)response.Durability,
                RouteIndex = routes.IndexOf(KahunaRoutingDomain.KeyValue, response.Key ?? "")
            });
        }
    }

    /// <summary>
    /// Attempts to extend the expiration time for a key-value pair.
    /// </summary>
    /// <param name="request">The request containing the key, expiration time, and other parameters for the operation.</param>
    /// <param name="context">The server call context for the gRPC operation.</param>
    /// <returns>A response containing the result of the extension operation, including the type, revision, and last modified timestamp.</returns>
    internal async Task<GrpcTryExtendKeyValueResponse> TryExtendKeyValueCore(GrpcTryExtendKeyValueRequest request, ServerCallContext context)
    {
        if (string.IsNullOrEmpty(request.Key) || request.ExpiresMs < 0)
            return new()
            {
                Type = GrpcKeyValueResponseType.TypeInvalidInput
            };

        ValueStopwatch stopwatch = ValueStopwatch.StartNew();
        
        using RouteCaptureScope.Scope routeScope = RouteCaptureScope.Begin(out RouteCapture? capture);

        using YieldingIntentPolicyScope.Scope extendPolicyScope = YieldingIntentPolicyScope.Enter(TransactionConflictPolicyWire.FromGrpc(request.ConflictPolicy));
        (KeyValueResponseType type, long revision, HLCTimestamp lastModified) = await keyValues.LocateAndTryExtendKeyValue(
            new(request.TransactionIdNode, request.TransactionIdPhysical, request.TransactionIdCounter),
            request.Key,
            request.ExpiresMs,
            (KeyValueDurability)request.Durability,
            context.CancellationToken,
            request.CoordinatorKey,
            new TransactionOperationId(request.OperationIdHigh, request.OperationIdLow)
        );

        GrpcTryExtendKeyValueResponse extendResponse = new()
        {
            Type = (GrpcKeyValueResponseType)type,
            Revision = revision,
            LastModifiedNode = lastModified.N,
            LastModifiedPhysical = lastModified.L,
            LastModifiedCounter = lastModified.C,
            TimeElapsedMs = (int)stopwatch.GetElapsedMilliseconds()
        };

        GrpcRouteHint? extendRoute = RouteHintWriter.Grpc(capture, KahunaRoutingDomain.KeyValue, request.Key);
        if (extendRoute is not null)
            extendResponse.Route = extendRoute;

        return extendResponse;
    }

    /// <summary>
    /// Attempts to delete a key-value pair based on the provided request and returns the outcome.
    /// </summary>
    /// <param name="request">The request containing the key to be deleted and associated parameters.</param>
    /// <param name="context">The gRPC server call context for the current operation.</param>
    /// <returns>A task that represents the asynchronous operation. The task result contains a GrpcTryDeleteKeyValueResponse object which indicates the result of the delete operation.</returns>
    internal async Task<GrpcTryDeleteKeyValueResponse> TryDeleteKeyValueCore(GrpcTryDeleteKeyValueRequest request, ServerCallContext context)
    {
        if (string.IsNullOrEmpty(request.Key))
            return new()
            {
                Type = GrpcKeyValueResponseType.TypeInvalidInput
            };
        
        ValueStopwatch stopwatch = ValueStopwatch.StartNew();
        
        using RouteCaptureScope.Scope routeScope = RouteCaptureScope.Begin(out RouteCapture? capture);

        using YieldingIntentPolicyScope.Scope deletePolicyScope = YieldingIntentPolicyScope.Enter(TransactionConflictPolicyWire.FromGrpc(request.ConflictPolicy));
        (KeyValueResponseType type, long revision, HLCTimestamp lastModified) = await keyValues.LocateAndTryDeleteKeyValue(
            new(request.TransactionIdNode, request.TransactionIdPhysical, request.TransactionIdCounter),
            request.Key,
            (KeyValueDurability)request.Durability,
            context.CancellationToken,
            request.CoordinatorKey,
            new TransactionOperationId(request.OperationIdHigh, request.OperationIdLow)
        );

        GrpcTryDeleteKeyValueResponse deleteResponse = new()
        {
            Type = (GrpcKeyValueResponseType)type,
            Revision = revision,
            LastModifiedNode = lastModified.N,
            LastModifiedPhysical = lastModified.L,
            LastModifiedCounter = lastModified.C,
            TimeElapsedMs = (int)stopwatch.GetElapsedMilliseconds()
        };

        GrpcRouteHint? deleteRoute = RouteHintWriter.Grpc(capture, KahunaRoutingDomain.KeyValue, request.Key);
        if (deleteRoute is not null)
            deleteResponse.Route = deleteRoute;

        return deleteResponse;
    }
}
