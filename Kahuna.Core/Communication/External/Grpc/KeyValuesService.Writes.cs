
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
    /// <summary>
    /// Attempts to set a key-value pair.
    /// </summary>
    /// <param name="request">The request containing the key-value data to be set.</param>
    /// <param name="context">The server call context providing information about the call.</param>
    /// <returns>A task representing the asynchronous operation, with a response indicating the result of the set operation.</returns>
    public override Task<GrpcTrySetKeyValueResponse> TrySetKeyValue(GrpcTrySetKeyValueRequest request, ServerCallContext context)
    {
        if (RejectOversizedSet(request) is { } rejected)
            return Task.FromResult(rejected);

        return TrySetKeyValueInternal(request, context).AsTask();
    }

    /// <summary>
    /// Attempts to set many key-value pairs.
    /// </summary>
    /// <param name="request">The request containing the key-value data to be set.</param>
    /// <param name="context">The server call context providing information about the call.</param>
    /// <returns>A task representing the asynchronous operation, with a response indicating the result of the set operation.</returns>
    public override async Task<GrpcTrySetManyKeyValueResponse> TrySetManyKeyValue(GrpcTrySetManyKeyValueRequest request, ServerCallContext context)
    {
        if ((RejectUnregisteredTransactionalSetMany(request) ?? RejectOversizedSetMany(request)) is { } rejected)
            return rejected;

        return await TrySetManyKeyValueInternal(request, context);
    }

    public override async Task<GrpcTryDeleteManyKeyValueResponse> TryDeleteManyKeyValue(GrpcTryDeleteManyKeyValueRequest request, ServerCallContext context)
    {
        if ((RejectUnregisteredTransactionalDeleteMany(request) ?? RejectOversizedDeleteMany(request)) is { } rejected)
            return rejected;

        return await TryDeleteManyKeyValueInternal(request, context);
    }

    // A transactional batch operation arriving from a client without coordinator registration identity can
    // never commit: the server owns the transaction working set, so an unregistered effect is invisible to
    // commit, which would report Committed while writing nothing (and an unregistered read skips read-set
    // validation). Rejecting at the client boundary turns that silent loss into a visible InvalidInput.
    // The already-routed inter-node fan-out (KeyValueServerBatcher → *Internal) legitimately carries
    // transactional items without identity — registration lives on the origin node — so only the
    // client-facing entry points apply these checks.

    internal static GrpcTrySetManyKeyValueResponse? RejectUnregisteredTransactionalSetMany(GrpcTrySetManyKeyValueRequest request)
    {
        // The check round of a batch is an inter-node request: a client never sends one.
        if (request.CheckOnly)
            return InvalidSetMany(request);

        // The set-many wire carries no registration identity, so any transactional item is unregistered.
        bool transactional = false;
        foreach (GrpcTrySetManyKeyValueRequestItem item in request.Items)
        {
            if (item.TransactionIdNode != 0 || item.TransactionIdPhysical != 0 || item.TransactionIdCounter != 0)
            {
                transactional = true;
                break;
            }
        }

        return transactional ? InvalidSetMany(request) : null;
    }

    private static GrpcTrySetManyKeyValueResponse InvalidSetMany(GrpcTrySetManyKeyValueRequest request)
    {
        GrpcTrySetManyKeyValueResponse response = new();
        ReserveItemCapacity(response.Items, request.Items.Count);
        foreach (GrpcTrySetManyKeyValueRequestItem item in request.Items)
            response.Items.Add(new GrpcTrySetManyKeyValueResponseItem
            {
                Type = GrpcKeyValueResponseType.TypeInvalidInput,
                Key = item.Key,
                Durability = item.Durability
            });
        return response;
    }

    // Key and value sizes and batch item counts are checked at the client entry points only, for the same reason as
    // the registration checks above: the inter-node batcher forwards requests a client entry point already admitted,
    // and a node must not refuse a forwarded request a larger bound on another node let in. A refused batch runs none
    // of its items. The gRPC batch responses carry no envelope outcome, so a refusal answers every item; that answer
    // is no larger than the request, and costs no routing or actor work.

    internal GrpcTrySetKeyValueResponse? RejectOversizedSet(GrpcTrySetKeyValueRequest request) =>
        inputLimits.KeyFits(request.Key) && inputLimits.ValueFits(request.Value.Length) && inputLimits.ValueFits(request.CompareValue.Length)
            ? null
            : new() { Type = GrpcKeyValueResponseType.TypeInvalidInput };

    internal GrpcTrySetManyKeyValueResponse? RejectOversizedSetMany(GrpcTrySetManyKeyValueRequest request)
    {
        if (!inputLimits.BatchFits(request.Items.Count))
            return InvalidSetMany(request);

        foreach (GrpcTrySetManyKeyValueRequestItem item in request.Items)
        {
            if (!inputLimits.KeyFits(item.Key) || !inputLimits.ValueFits(item.Value.Length) || !inputLimits.ValueFits(item.CompareValue.Length))
                return InvalidSetMany(request);
        }

        return null;
    }

    internal GrpcTryDeleteManyKeyValueResponse? RejectOversizedDeleteMany(GrpcTryDeleteManyKeyValueRequest request)
    {
        if (inputLimits.BatchFits(request.Items.Count))
            return null;

        GrpcTryDeleteManyKeyValueResponse response = new();
        ReserveItemCapacity(response.Items, request.Items.Count);
        foreach (GrpcTryDeleteManyKeyValueRequestItem item in request.Items)
            response.Items.Add(new GrpcTryDeleteManyKeyValueResponseItem
            {
                Type = GrpcKeyValueResponseType.TypeInvalidInput,
                Key = item.Key,
                Durability = item.Durability
            });
        return response;
    }

    internal static GrpcTryDeleteManyKeyValueResponse? RejectUnregisteredTransactionalDeleteMany(GrpcTryDeleteManyKeyValueRequest request)
    {
        // Delete-many can register (batch-level identity); reject only when identity is absent.
        if (request is { HasCoordinatorKey: true, HasOperationIdHigh: true, HasOperationIdLow: true } && !string.IsNullOrEmpty(request.CoordinatorKey))
            return null;

        bool transactional = false;
        foreach (GrpcTryDeleteManyKeyValueRequestItem item in request.Items)
        {
            if (item.TransactionIdNode != 0 || item.TransactionIdPhysical != 0 || item.TransactionIdCounter != 0)
            {
                transactional = true;
                break;
            }
        }

        if (!transactional)
            return null;

        GrpcTryDeleteManyKeyValueResponse response = new();
        ReserveItemCapacity(response.Items, request.Items.Count);
        foreach (GrpcTryDeleteManyKeyValueRequestItem item in request.Items)
            response.Items.Add(new GrpcTryDeleteManyKeyValueResponseItem
            {
                Type = GrpcKeyValueResponseType.TypeInvalidInput,
                Key = item.Key,
                Durability = item.Durability
            });
        return response;
    }

    /// <summary>
    /// Attempts to extend the expiration of a key-value pair in the store.
    /// </summary>
    /// <param name="request">The request containing key and expiration data to extend the key-value pair.</param>
    /// <param name="context">The context of the server call.</param>
    /// <returns>A task that represents the asynchronous operation, containing the response of the key-value extension attempt.</returns>
    public override async Task<GrpcTryExtendKeyValueResponse> TryExtendKeyValue(GrpcTryExtendKeyValueRequest request, ServerCallContext context)
    {
        return await TryExtendKeyValueInternal(request, context);
    }

    /// <summary>
    /// Attempts to delete the specified key-value pair.
    /// </summary>
    /// <param name="request">The request containing the key to be deleted.</param>
    /// <param name="context">The server call context for the gRPC call.</param>
    /// <returns>Returns a response indicating the success or failure of the deletion operation.</returns>
    public override async Task<GrpcTryDeleteKeyValueResponse> TryDeleteKeyValue(GrpcTryDeleteKeyValueRequest request, ServerCallContext context)
    {
        return await TryDeleteKeyValueInternal(request, context);
    }

    internal ValueTask<GrpcTrySetKeyValueResponse> TrySetKeyValueInternal(GrpcTrySetKeyValueRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.writes.TrySetKeyValueCore(r, c), static _ => KeyValueMustRetry.TrySetKeyValue());

    internal Task<GrpcTrySetManyKeyValueResponse> TrySetManyKeyValueInternal(GrpcTrySetManyKeyValueRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.writes.TrySetManyKeyValueCore(r, c), static r => KeyValueMustRetry.TrySetManyKeyValue(r));

    internal Task<GrpcTryDeleteManyKeyValueResponse> TryDeleteManyKeyValueInternal(GrpcTryDeleteManyKeyValueRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.writes.TryDeleteManyKeyValueCore(r, c), static r => KeyValueMustRetry.TryDeleteManyKeyValue(r));

    internal Task<GrpcTryExtendKeyValueResponse> TryExtendKeyValueInternal(GrpcTryExtendKeyValueRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.writes.TryExtendKeyValueCore(r, c), static _ => KeyValueMustRetry.TryExtendKeyValue());

    internal Task<GrpcTryDeleteKeyValueResponse> TryDeleteKeyValueInternal(GrpcTryDeleteKeyValueRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.writes.TryDeleteKeyValueCore(r, c), static _ => KeyValueMustRetry.TryDeleteKeyValue());
}
