
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
    internal GrpcTryAcquireExclusiveLockResponse? RejectClientLock(GrpcTryAcquireExclusiveLockRequest request) =>
        inputLimits.KeyFits(request.Key) && ClientLockIdentity.IsRegistered(request.CoordinatorKey, request.OperationIdHigh, request.OperationIdLow)
            ? null
            : new() { Type = GrpcKeyValueResponseType.TypeInvalidInput };

    private GrpcTryAcquireManyExclusiveLocksResponse? RejectClientManyLocks(GrpcTryAcquireManyExclusiveLocksRequest request)
    {
        bool fits = inputLimits.BatchFits(request.Items.Count)
            && ClientLockIdentity.IsRegistered(request.CoordinatorKey, request.OperationIdHigh, request.OperationIdLow);
        for (int i = 0; fits && i < request.Items.Count; i++)
            fits = inputLimits.KeyFits(request.Items[i].Key);

        if (fits)
            return null;

        GrpcTryAcquireManyExclusiveLocksResponse response = new();
        ReserveItemCapacity(response.Items, request.Items.Count);
        foreach (GrpcTryAcquireManyExclusiveLocksRequestItem item in request.Items)
            response.Items.Add(new GrpcTryAcquireManyExclusiveLocksResponseItem
            {
                Type = GrpcKeyValueResponseType.TypeInvalidInput,
                Key = item.Key,
                Durability = item.Durability
            });
        return response;
    }

    /// <summary>
    /// Attempts to acquire an exclusive lock on a key-value resource.
    /// </summary>
    /// <param name="request">The request containing details about the lock acquisition operation.</param>
    /// <param name="context">The context of the server call.</param>
    /// <returns>A response indicating the result of the lock acquisition attempt.</returns>
    public override async Task<GrpcTryAcquireExclusiveLockResponse> TryAcquireExclusiveLock(GrpcTryAcquireExclusiveLockRequest request, ServerCallContext context)
    {
        if (RejectClientLock(request) is { } rejected)
            return rejected;

        return await TryAcquireExclusiveLockInternal(request, context);
    }

    /// <summary>
    /// Attempts to acquire an exclusive lock on group of key-value resource prefixed by the given prefix key.
    /// </summary>
    /// <param name="request">The request containing details about the lock acquisition operation.</param>
    /// <param name="context">The context of the server call.</param>
    /// <returns>A response indicating the result of the lock acquisition attempt.</returns>
    public override async Task<GrpcTryAcquireExclusivePrefixLockResponse> TryAcquireExclusivePrefixLock(
        GrpcTryAcquireExclusivePrefixLockRequest request, 
        ServerCallContext context
    )
    {
        if (!inputLimits.KeyFits(request.PrefixKey) || !ClientLockIdentity.IsRegistered(request.CoordinatorKey, request.OperationIdHigh, request.OperationIdLow))
            return new() { Type = GrpcKeyValueResponseType.TypeInvalidInput };

        return await TryAcquireExclusivePrefixLockInternal(request, context);
    }

    /// <summary>
    /// Attempts to acquire multiple exclusive locks based on the provided request.
    /// </summary>
    /// <param name="request">The request containing the details for acquiring multiple exclusive locks.</param>
    /// <param name="context">The server call context for the gRPC method.</param>
    /// <returns>A response indicating the result of the attempt to acquire the exclusive locks.</returns>
    public override async Task<GrpcTryAcquireManyExclusiveLocksResponse> TryAcquireManyExclusiveLocks(GrpcTryAcquireManyExclusiveLocksRequest request, ServerCallContext context)
    {
        if (RejectClientManyLocks(request) is { } rejected)
            return rejected;

        return await TryAcquireManyExclusiveLocksInternal(request, context);
    }

    /// <summary>
    /// Receives requests for the key/value "ReleaseExclusiveLock" service 
    /// </summary>
    /// <param name="request"></param>
    /// <param name="context"></param>
    /// <returns></returns>
    public override async Task<GrpcTryReleaseExclusiveLockResponse> TryReleaseExclusiveLock(GrpcTryReleaseExclusiveLockRequest request, ServerCallContext context)
    {
        return await TryReleaseExclusiveLockInternal(request, context);   
    }

    /// <summary>
    /// Attempts to release an exclusive lock on group of key-value resource prefixed by the given prefix key.
    /// </summary>
    /// <param name="request">The request containing details about the lock acquisition operation.</param>
    /// <param name="context">The context of the server call.</param>
    /// <returns>A response indicating the result of the lock acquisition attempt.</returns>
    public override async Task<GrpcTryReleaseExclusivePrefixLockResponse> TryReleaseExclusivePrefixLock(
        GrpcTryReleaseExclusivePrefixLockRequest request, 
        ServerCallContext context
    )
    {
        return await TryReleaseExclusivePrefixLockInternal(request, context);
    }

    public override async Task<GrpcTryAcquireExclusiveRangeLockResponse> TryAcquireExclusiveRangeLock(
        GrpcTryAcquireExclusiveRangeLockRequest request,
        ServerCallContext context
    )
    {
        if (!inputLimits.KeyFits(request.Prefix)
            || (request.HasStartKey && !inputLimits.KeyFits(request.StartKey))
            || (request.HasEndKey && !inputLimits.KeyFits(request.EndKey))
            || !ClientLockIdentity.IsRegistered(request.CoordinatorKey, request.OperationIdHigh, request.OperationIdLow))
            return new() { Type = GrpcKeyValueResponseType.TypeInvalidInput };

        return await TryAcquireExclusiveRangeLockInternal(request, context);
    }

    public override async Task<GrpcTryReleaseExclusiveRangeLockResponse> TryReleaseExclusiveRangeLock(
        GrpcTryReleaseExclusiveRangeLockRequest request,
        ServerCallContext context
    )
    {
        return await TryReleaseExclusiveRangeLockInternal(request, context);
    }

    /// <summary>
    /// Receives requests for the key/value "ReleaseManyExclusiveLocks" service
    /// </summary>
    /// <param name="request"></param>
    /// <param name="context"></param>
    /// <returns></returns>
    public override async Task<GrpcTryReleaseManyExclusiveLocksResponse> TryReleaseManyExclusiveLocks(
        GrpcTryReleaseManyExclusiveLocksRequest request,
        ServerCallContext context
    )
    {
        return await TryReleaseManyExclusiveLocksInternal(request, context);
    }

    internal Task<GrpcTryAcquireExclusiveLockResponse> TryAcquireExclusiveLockInternal(GrpcTryAcquireExclusiveLockRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.locks.TryAcquireExclusiveLockCore(r, c), static _ => KeyValueMustRetry.TryAcquireExclusiveLock());

    internal Task<GrpcTryAcquireExclusivePrefixLockResponse> TryAcquireExclusivePrefixLockInternal(GrpcTryAcquireExclusivePrefixLockRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.locks.TryAcquireExclusivePrefixLockCore(r, c), static _ => KeyValueMustRetry.TryAcquireExclusivePrefixLock());

    internal Task<GrpcTryAcquireManyExclusiveLocksResponse> TryAcquireManyExclusiveLocksInternal(GrpcTryAcquireManyExclusiveLocksRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.locks.TryAcquireManyExclusiveLocksCore(r, c), static r => KeyValueMustRetry.TryAcquireManyExclusiveLocks(r));

    internal Task<GrpcTryReleaseExclusiveLockResponse> TryReleaseExclusiveLockInternal(GrpcTryReleaseExclusiveLockRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.locks.TryReleaseExclusiveLockCore(r, c), static _ => KeyValueMustRetry.TryReleaseExclusiveLock());

    internal Task<GrpcTryReleaseExclusivePrefixLockResponse> TryReleaseExclusivePrefixLockInternal(GrpcTryReleaseExclusivePrefixLockRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.locks.TryReleaseExclusivePrefixLockCore(r, c), static _ => KeyValueMustRetry.TryReleaseExclusivePrefixLock());

    internal Task<GrpcTryAcquireExclusiveRangeLockResponse> TryAcquireExclusiveRangeLockInternal(GrpcTryAcquireExclusiveRangeLockRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.locks.TryAcquireExclusiveRangeLockCore(r, c), static _ => KeyValueMustRetry.TryAcquireExclusiveRangeLock());

    internal Task<GrpcTryReleaseExclusiveRangeLockResponse> TryReleaseExclusiveRangeLockInternal(GrpcTryReleaseExclusiveRangeLockRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.locks.TryReleaseExclusiveRangeLockCore(r, c), static _ => KeyValueMustRetry.TryReleaseExclusiveRangeLock());

    internal Task<GrpcTryReleaseManyExclusiveLocksResponse> TryReleaseManyExclusiveLocksInternal(GrpcTryReleaseManyExclusiveLocksRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.locks.TryReleaseManyExclusiveLocksCore(r, c), static r => KeyValueMustRetry.TryReleaseManyExclusiveLocks(r));
}
