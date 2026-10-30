
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
/// Reads, existence checks, conflict probes and write-intent checks, including the routed inter-node batch forms.
/// </summary>
internal sealed class KeyValueReadHandler
{
    private readonly IKahuna keyValues;

    public KeyValueReadHandler(IKahuna keyValues)
    {
        this.keyValues = keyValues;
    }

    internal async Task<GrpcTryGetManyValuesResponse> TryGetManyValuesRoutedCore(GrpcTryGetManyValuesRequest request, ServerCallContext context)
    {
        // The get-many wire carries no registration identity: a transactional batch read from a client
        // would skip read-set registration and silently weaken isolation, so it is rejected outright.
        if (request.TransactionIdNode != 0 || request.TransactionIdPhysical != 0 || request.TransactionIdCounter != 0)
        {
            GrpcTryGetManyValuesResponse rejected = new();
            ReserveItemCapacity(rejected.Items, request.Items.Count);
            foreach (GrpcTryManyValuesRequestItem item in request.Items)
                rejected.Items.Add(new GrpcTryGetManyValuesResponseItem
                {
                    Type = GrpcKeyValueResponseType.TypeInvalidInput,
                    Key = item.Key,
                    Durability = item.Durability
                });
            return rejected;
        }

        // A client sends the whole batch to one node as a unary RPC without routing per key, so this endpoint must
        // route each key to its partition leader itself — exactly as the single-key TryGetKeyValue does. Reading
        // the local actors directly returns this node's own state, which for a key whose partition this node does
        // not lead is stale or absent, so a batched read would miss a committed value a routed single-key read (or
        // a same-transaction scan) sees. The inter-node handler (TryGetManyValuesInternal) routes on arrival for
        // the same reason: the sender's belief about who leads is not proof.
        using RouteCaptureScope.Scope routeScope = RouteCaptureScope.Begin(out RouteCapture? capture);

        List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)> responses = await keyValues.LocateAndTryGetManyValues(
            new(request.TransactionIdNode, request.TransactionIdPhysical, request.TransactionIdCounter),
            new(request.ReadTimestampNode, request.ReadTimestampPhysical, request.ReadTimestampCounter),
            GetRequestManyValuesItems(request.Items),
            context.CancellationToken
        );

        GrpcTryGetManyValuesResponse response = new();
        RouteHintWriter.Table routes = new(capture);
        AddResponseGetManyValuesItems(response.Items, responses, routes);

        if (routes.HasRoutes)
            routes.WriteGrpc(response.Routes);

        return response;
    }

    /// <summary>
    /// Attempts to retrieve a key-value pair based on the specified request.
    /// </summary>
    /// <param name="request">The request containing details such as key, transaction IDs, revision, and durability settings.</param>
    /// <param name="context">The gRPC server call context providing runtime information about the request.</param>
    /// <returns>Returns a <see cref="GrpcTryGetKeyValueResponse"/> object indicating the result of the retrieval operation.</returns>
    internal async Task<GrpcTryGetKeyValueResponse> TryGetKeyValueCore(GrpcTryGetKeyValueRequest request, ServerCallContext context)
    {
        if (string.IsNullOrEmpty(request.Key))
            return new()
            {
                Type = GrpcKeyValueResponseType.TypeInvalidInput
            };
        
        ValueStopwatch stopwatch = ValueStopwatch.StartNew();
        
        using RouteCaptureScope.Scope routeScope = RouteCaptureScope.Begin(out RouteCapture? capture);

        (KeyValueResponseType type, ReadOnlyKeyValueEntry? keyValueContext) = await keyValues.LocateAndTryGetValue(
            new(request.TransactionIdNode, request.TransactionIdPhysical, request.TransactionIdCounter),
            request.Key,
            request.Revision,
            new(request.ReadTimestampNode, request.ReadTimestampPhysical, request.ReadTimestampCounter),
            (KeyValueDurability)request.Durability,
            context.CancellationToken,
            request.CoordinatorKey,
            new TransactionOperationId(request.OperationIdHigh, request.OperationIdLow),
            (KeyValueReadMode)request.ReadMode
        );
        
        if (keyValueContext is not null)
        {
            GrpcTryGetKeyValueResponse response = new()
            {
                ServedFrom = "",
                Type = (GrpcKeyValueResponseType)type,
                Revision = keyValueContext.Revision,
                ExpiresNode = keyValueContext.Expires.N,
                ExpiresPhysical = keyValueContext.Expires.L,
                ExpiresCounter = keyValueContext.Expires.C,
                LastUsedNode = keyValueContext.LastUsed.N,
                LastUsedPhysical = keyValueContext.LastUsed.L,
                LastUsedCounter = keyValueContext.LastUsed.C,
                LastModifiedNode = keyValueContext.LastModified.N,
                LastModifiedPhysical = keyValueContext.LastModified.L,
                LastModifiedCounter = keyValueContext.LastModified.C,
                State = (GrpcKeyValueState)keyValueContext.State,
                TimeElapsedMs = (int)stopwatch.GetElapsedMilliseconds()
            };

            if (keyValueContext.Value is not null)
                response.Value = UnsafeByteOperations.UnsafeWrap(keyValueContext.Value);

            GrpcRouteHint? foundRoute = RouteHintWriter.Grpc(capture, KahunaRoutingDomain.KeyValue, request.Key);
            if (foundRoute is not null)
                response.Route = foundRoute;

            return response;
        }

        GrpcTryGetKeyValueResponse missing = new()
        {
            Type = (GrpcKeyValueResponseType)type,
            TimeElapsedMs = (int)stopwatch.GetElapsedMilliseconds()
        };

        // "The key does not exist" is a terminal answer about a key whose owner was resolved. Left
        // without a hint, a read-mostly workload over absent keys would never learn a route.
        GrpcRouteHint? missingRoute = RouteHintWriter.Grpc(capture, KahunaRoutingDomain.KeyValue, request.Key);
        if (missingRoute is not null)
            missing.Route = missingRoute;

        return missing;
    }

    /// <summary>
    /// Inter-node batch read (<c>KeyValueServerBatcher</c>): the sending node grouped the keys by the leader it
    /// believes in and forwarded each group here. That belief is not proof. A gateway that hosts none of the
    /// partition's replicas resolves the leader from a gossiped hint, and on a hint miss it picks any voter of the
    /// committed replica set, so a group can land on a follower, or on a leader the partition has since moved
    /// away from. The keys are therefore routed again on arrival, exactly as the single-key handler does: a
    /// partition this node leads is served from local state after a quorum-confirmed leadership check, and a
    /// partition it does not lead is forwarded to its leader within the hop budget (the one extra hop
    /// <c>ForwardedRequestScope.MaxForwardHops</c> keeps for this case) or answered <c>MustRetry</c>. Serving the
    /// group from this node's actors on the sender's word alone returned a follower's state as a committed
    /// read, and a read-modify-write computed over it erased the writes the follower had not applied yet.
    /// </summary>
    /// <summary>
    /// Inter-node batched probe: the sender grouped the keys by the leader it believes in. That belief is not
    /// proof, so the keys are routed again on arrival exactly as the batch read above does, and the leaderships the
    /// probe's locks were granted under travel back with the answers.
    /// </summary>
    internal async Task<GrpcTryProbeManyResponse> TryProbeManyCore(GrpcTryProbeManyRequest request, ServerCallContext context)
    {
        string[] keys = new string[request.Keys.Count];
        request.Keys.CopyTo(keys, 0);

        using LockGrantScope.Scope grantScope = LockGrantScope.Begin(out LockGrantCapture grants);

        KeyValueProbeResult[] results = await keyValues.LocateAndTryProbeMany(
            new(request.TransactionIdNode, request.TransactionIdPhysical, request.TransactionIdCounter),
            new(request.ReadTimestampNode, request.ReadTimestampPhysical, request.ReadTimestampCounter),
            keys,
            (KeyValueProbeLock)request.LockMode,
            request.ExpiresMs,
            (KeyValueDurability)request.Durability,
            context.CancellationToken,
            readMode: (KeyValueReadMode)request.ReadMode
        );

        GrpcTryProbeManyResponse response = new();
        ProbeManyWire.AddItems(response.Items, results);
        AddLockGrantTerms(response.GrantTerms, grants);
        return response;
    }

    /// <summary>
    /// Inter-node batched range probe: the sender grouped the seeks by the leader it believes in. That belief is not
    /// proof, so the seeks are routed again on arrival, and the leaderships the probe's locks were granted under travel
    /// back with the answers.
    /// </summary>
    internal async Task<GrpcTryProbeManyRangesResponse> TryProbeManyRangesCore(GrpcTryProbeManyRangesRequest request, ServerCallContext context)
    {
        using LockGrantScope.Scope grantScope = LockGrantScope.Begin(out LockGrantCapture grants);

        KeyValueProbeRangeResult[] results = await keyValues.LocateAndTryProbeManyRanges(
            new(request.TransactionIdNode, request.TransactionIdPhysical, request.TransactionIdCounter),
            new(request.ReadTimestampNode, request.ReadTimestampPhysical, request.ReadTimestampCounter),
            request.Prefix,
            ProbeManyRangesWire.FromRequest(request),
            (KeyValueProbeLock)request.LockMode,
            request.ExpiresMs,
            (KeyValueDurability)request.Durability,
            context.CancellationToken,
            readMode: (KeyValueReadMode)request.ReadMode
        );

        GrpcTryProbeManyRangesResponse response = new();
        ProbeManyRangesWire.AddItems(response.Items, results);
        AddLockGrantTerms(response.GrantTerms, grants);
        return response;
    }

    internal async Task<GrpcTryGetManyValuesResponse> TryGetManyValuesCore(GrpcTryGetManyValuesRequest request, ServerCallContext context)
    {
        List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)> responses = await keyValues.LocateAndTryGetManyValues(
            new(request.TransactionIdNode, request.TransactionIdPhysical, request.TransactionIdCounter),
            new(request.ReadTimestampNode, request.ReadTimestampPhysical, request.ReadTimestampCounter),
            GetRequestManyValuesItems(request.Items),
            context.CancellationToken
        );

        GrpcTryGetManyValuesResponse response = new();
        AddResponseGetManyValuesItems(response.Items, responses);
        return response;
    }

    internal async Task<GrpcTryExistsManyValuesResponse> TryExistsManyValuesRoutedCore(GrpcTryExistsManyValuesRequest request, ServerCallContext context)
    {
        // Same rejection as TryGetManyValues: no identity on the wire means a transactional batch
        // existence check cannot register its read set.
        if (request.TransactionIdNode != 0 || request.TransactionIdPhysical != 0 || request.TransactionIdCounter != 0)
        {
            GrpcTryExistsManyValuesResponse rejected = new();
            ReserveItemCapacity(rejected.Items, request.Items.Count);
            foreach (GrpcTryManyValuesRequestItem item in request.Items)
                rejected.Items.Add(new GrpcTryExistsManyValuesResponseItem
                {
                    Type = GrpcKeyValueResponseType.TypeInvalidInput,
                    Key = item.Key,
                    Durability = item.Durability
                });
            return rejected;
        }

        // Route per key for the same reason as TryGetManyValues: the non-locating internal reads only this node's
        // actors and is correct only on the already-routed inter-node path.
        using RouteCaptureScope.Scope routeScope = RouteCaptureScope.Begin(out RouteCapture? capture);

        List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)> responses = await keyValues.LocateAndTryExistsManyValues(
            new(request.TransactionIdNode, request.TransactionIdPhysical, request.TransactionIdCounter),
            new(request.ReadTimestampNode, request.ReadTimestampPhysical, request.ReadTimestampCounter),
            GetRequestManyValuesItems(request.Items),
            context.CancellationToken
        );

        GrpcTryExistsManyValuesResponse response = new();
        RouteHintWriter.Table routes = new(capture);
        AddResponseExistsManyValuesItems(response.Items, responses, routes);

        if (routes.HasRoutes)
            routes.WriteGrpc(response.Routes);

        return response;
    }

    /// <summary>
    /// Attempts to check the existence of a key-value entry within a specified context.
    /// </summary>
    /// <param name="request">The request containing the details required to check the key-value existence, such as the key, transaction identifiers, revision, and durability settings.</param>
    /// <param name="context">The server call context used for managing RPC communication and cancellation tokens.</param>
    /// <returns>Returns a <see cref="GrpcTryExistsKeyValueResponse"/> representing the result of the existence check, including the response type and time elapsed during execution.</returns>
    internal async Task<GrpcTryExistsKeyValueResponse> TryExistsKeyValueCore(GrpcTryExistsKeyValueRequest request, ServerCallContext context)
    {
        if (string.IsNullOrEmpty(request.Key))
            return new()
            {
                Type = GrpcKeyValueResponseType.TypeInvalidInput
            };
        
        ValueStopwatch stopwatch = ValueStopwatch.StartNew();
        
        using RouteCaptureScope.Scope routeScope = RouteCaptureScope.Begin(out RouteCapture? capture);

        (KeyValueResponseType type, ReadOnlyKeyValueEntry? keyValueContext) = await keyValues.LocateAndTryExistsValue(
            new(request.TransactionIdNode, request.TransactionIdPhysical, request.TransactionIdCounter),
            request.Key,
            request.Revision,
            new(request.ReadTimestampNode, request.ReadTimestampPhysical, request.ReadTimestampCounter),
            (KeyValueDurability)request.Durability,
            context.CancellationToken,
            request.CoordinatorKey,
            new TransactionOperationId(request.OperationIdHigh, request.OperationIdLow)
        );
        
        if (keyValueContext is not null)
        {
            GrpcTryExistsKeyValueResponse response = new()
            {
                ServedFrom = "",
                Type = (GrpcKeyValueResponseType)type,
                Revision = keyValueContext.Revision,
                ExpiresNode = keyValueContext.Expires.N,
                ExpiresPhysical = keyValueContext.Expires.L,
                ExpiresCounter = keyValueContext.Expires.C,
                LastUsedNode = keyValueContext.LastUsed.N,
                LastUsedPhysical = keyValueContext.LastUsed.L,
                LastUsedCounter = keyValueContext.LastUsed.C,
                LastModifiedNode = keyValueContext.LastModified.N,
                LastModifiedPhysical = keyValueContext.LastModified.L,
                LastModifiedCounter = keyValueContext.LastModified.C,
                State = (GrpcKeyValueState)keyValueContext.State,
                TimeElapsedMs = (int)stopwatch.GetElapsedMilliseconds()
            };

            GrpcRouteHint? presentRoute = RouteHintWriter.Grpc(capture, KahunaRoutingDomain.KeyValue, request.Key);
            if (presentRoute is not null)
                response.Route = presentRoute;

            return response;
        }

        GrpcTryExistsKeyValueResponse absent = new()
        {
            Type = (GrpcKeyValueResponseType)type,
            TimeElapsedMs = (int)stopwatch.GetElapsedMilliseconds()
        };

        GrpcRouteHint? absentRoute = RouteHintWriter.Grpc(capture, KahunaRoutingDomain.KeyValue, request.Key);
        if (absentRoute is not null)
            absent.Route = absentRoute;

        return absent;
    }

    /// <summary>
    /// Inter-node batch exists: the counterpart of <see cref="TryGetManyValuesInternal"/>, with the same routing on
    /// arrival and for the same reason. A commit-time base check that reaches a follower through a stale forward
    /// would otherwise confirm a base the leader has already moved past.
    /// </summary>
    internal async Task<GrpcTryExistsManyValuesResponse> TryExistsManyValuesCore(GrpcTryExistsManyValuesRequest request, ServerCallContext context)
    {
        List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)> responses = await keyValues.LocateAndTryExistsManyValues(
            new(request.TransactionIdNode, request.TransactionIdPhysical, request.TransactionIdCounter),
            new(request.ReadTimestampNode, request.ReadTimestampPhysical, request.ReadTimestampCounter),
            GetRequestManyValuesItems(request.Items),
            context.CancellationToken
        );

        GrpcTryExistsManyValuesResponse response = new();
        AddResponseExistsManyValuesItems(response.Items, responses);
        return response;
    }

    private static List<(string key, long revision, KeyValueDurability durability)> GetRequestManyValuesItems(
        RepeatedField<GrpcTryManyValuesRequestItem> items
    )
    {
        List<(string key, long revision, KeyValueDurability durability)> rItems = new(items.Count);

        foreach (GrpcTryManyValuesRequestItem item in items)
            rItems.Add((item.Key, item.Revision, (KeyValueDurability)item.Durability));

        return rItems;
    }

    private static void AddResponseGetManyValuesItems(
        RepeatedField<GrpcTryGetManyValuesResponseItem> target,
        List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)> responses,
        RouteHintWriter.Table? routes = null
    )
    {
        ReserveItemCapacity(target, responses.Count);

        foreach ((KeyValueResponseType type, string key, KeyValueDurability durability, ReadOnlyKeyValueEntry? entry) in responses)
        {
            GrpcTryGetManyValuesResponseItem item = new()
            {
                Type = (GrpcKeyValueResponseType)type,
                Key = key,
                Durability = (GrpcKeyValueDurability)durability
            };

            if (entry is not null)
            {
                item.Revision = entry.Revision;
                item.ExpiresNode = entry.Expires.N;
                item.ExpiresPhysical = entry.Expires.L;
                item.ExpiresCounter = entry.Expires.C;
                item.LastUsedNode = entry.LastUsed.N;
                item.LastUsedPhysical = entry.LastUsed.L;
                item.LastUsedCounter = entry.LastUsed.C;
                item.LastModifiedNode = entry.LastModified.N;
                item.LastModifiedPhysical = entry.LastModified.L;
                item.LastModifiedCounter = entry.LastModified.C;
                item.State = (GrpcKeyValueState)entry.State;

                if (entry.Value is not null)
                    item.Value = UnsafeByteOperations.UnsafeWrap(entry.Value);
            }

            if (routes is not null)
                item.RouteIndex = routes.IndexOf(KahunaRoutingDomain.KeyValue, key);

            target.Add(item);
        }
    }

    private static void AddResponseExistsManyValuesItems(
        RepeatedField<GrpcTryExistsManyValuesResponseItem> target,
        List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)> responses,
        RouteHintWriter.Table? routes = null
    )
    {
        ReserveItemCapacity(target, responses.Count);

        foreach ((KeyValueResponseType type, string key, KeyValueDurability durability, ReadOnlyKeyValueEntry? entry) in responses)
        {
            GrpcTryExistsManyValuesResponseItem item = new()
            {
                Type = (GrpcKeyValueResponseType)type,
                Key = key,
                Durability = (GrpcKeyValueDurability)durability
            };

            if (entry is not null)
            {
                item.Revision = entry.Revision;
                item.ExpiresNode = entry.Expires.N;
                item.ExpiresPhysical = entry.Expires.L;
                item.ExpiresCounter = entry.Expires.C;
                item.LastUsedNode = entry.LastUsed.N;
                item.LastUsedPhysical = entry.LastUsed.L;
                item.LastUsedCounter = entry.LastUsed.C;
                item.LastModifiedNode = entry.LastModified.N;
                item.LastModifiedPhysical = entry.LastModified.L;
                item.LastModifiedCounter = entry.LastModified.C;
                item.State = (GrpcKeyValueState)entry.State;
            }

            if (routes is not null)
                item.RouteIndex = routes.IndexOf(KahunaRoutingDomain.KeyValue, key);

            target.Add(item);
        }
    }

    internal async Task<GrpcTryCheckWriteIntentResponse> TryCheckWriteIntentCore(GrpcTryCheckWriteIntentRequest request, ServerCallContext context)
    {
        if (string.IsNullOrEmpty(request.Key))
            return new() { Type = GrpcKeyValueResponseType.TypeInvalidInput };

        KeyValueResponseType type = await keyValues.LocateAndTryCheckWriteIntent(
            new(request.TransactionIdNode, request.TransactionIdPhysical, request.TransactionIdCounter),
            request.Key,
            (KeyValueDurability)request.Durability,
            context.CancellationToken
        );

        return new() { ServedFrom = "", Type = (GrpcKeyValueResponseType)type };
    }

    /// <summary>
    /// Probes a group of keys this node leads for concurrent write intents. The sender already grouped the keys
    /// by owning node, so — as with the batched many-key reads — this serves them locally rather than routing
    /// each one again, after confirming this node's leadership of the keys' partitions. The leadership terms
    /// the held staged intents were confirmed under travel back on the response, as lock grants' terms do.
    /// </summary>
    internal async Task<GrpcTryCheckManyWriteIntentsResponse> TryCheckManyWriteIntentsCore(GrpcTryCheckManyWriteIntentsRequest request, ServerCallContext context)
    {
        List<KeyValueConflictProbe> keys = new(request.Items.Count);

        foreach (GrpcTryCheckManyWriteIntentsRequestItem item in request.Items)
            keys.Add(new(item.Key, (KeyValueDurability)item.Durability, (KeyValueConflictChecks)item.Checks, item.BaseRevision));

        using LockGrantScope.Scope grantScope = LockGrantScope.Begin(out LockGrantCapture grants);

        List<(KeyValueResponseType type, string key, KeyValueDurability durability)> responses =
            await keyValues.TryCheckManyWriteIntentValuesConfirmed(
                new(request.TransactionIdNode, request.TransactionIdPhysical, request.TransactionIdCounter),
                keys,
                context.CancellationToken);

        GrpcTryCheckManyWriteIntentsResponse response = new() { ServedFrom = "" };

        foreach ((KeyValueResponseType type, string key, KeyValueDurability durability) in responses)
            response.Items.Add(new GrpcTryCheckManyWriteIntentsResponseItem
            {
                Type = (GrpcKeyValueResponseType)type,
                Key = key,
                Durability = (GrpcKeyValueDurability)durability
            });

        AddLockGrantTerms(response.GrantTerms, grants);

        // The highest commit floor over the caller's staged intents this node confirmed held, beside the terms.
        HLCTimestamp commitFloor = grants.CommitFloor;
        if (!commitFloor.IsNull())
        {
            response.CommitFloorNode = commitFloor.N;
            response.CommitFloorPhysical = commitFloor.L;
            response.CommitFloorCounter = commitFloor.C;
        }

        return response;
    }
}
