
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

/// <summary>
/// Provides gRPC services for managing key-value pairs, locks, and mutations.
/// Implements functionality for setting, extending, deleting, and retrieving
/// key-value pairs, as well as lock management and mutation operations.
/// </summary>
public sealed class KeyValuesService : KeyValuer.KeyValuerBase
{
    private readonly IKahuna keyValues;

    private readonly NodeTransportGate gate;

    private readonly ILogger<IKahuna> logger;

    private readonly KeyValueWriteHandler writes;

    private readonly KeyValueReadHandler reads;

    private readonly KeyValueLockHandler locks;

    private readonly KeyValueMutationHandler mutations;

    private readonly KeyValueScanHandler scans;

    private readonly KeyValueTransactionHandler transactions;

    private readonly KeyValueRangeAdminHandler rangeAdmin;

    private readonly KeyValueDurableHandler durable;

    private readonly KeyValueSnapshotHandler snapshots;

    private readonly KeyValueClientBatcher clientBatcher;

    private readonly KeyValueServerBatcher serverBatcher;

    // The most watches one Watch call may hold open; zero for no limit.
    private readonly int maxWatchesPerCall;

    // The key and value sizes the client entry points accept; the inter-node batcher is not bound by them.
    private readonly KeyValueInputLimits inputLimits;

    private static readonly KahunaConfiguration DefaultConfiguration = new();

    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="keyValues"></param>
    /// <param name="gate"></param>
    /// <param name="logger"></param>
    /// <param name="configuration">The node's configuration, for the watch limits; null takes the defaults.</param>
    public KeyValuesService(IKahuna keyValues, NodeTransportGate gate, ILogger<IKahuna> logger, KahunaConfiguration? configuration = null)
    {
        this.keyValues = keyValues;
        this.gate = gate;
        this.logger = logger;
        maxWatchesPerCall = (configuration ?? DefaultConfiguration).WatchMaxPerCall;
        inputLimits = KeyValueInputLimits.From(configuration ?? DefaultConfiguration);

        writes = new(keyValues);
        reads = new(keyValues);
        locks = new(keyValues);
        mutations = new(keyValues);
        scans = new(keyValues);
        transactions = new(keyValues);
        rangeAdmin = new(keyValues);
        durable = new(keyValues);
        snapshots = new(keyValues);

        clientBatcher = new(this, logger);
        serverBatcher = new(this, logger);
    }

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

    internal GrpcTryAcquireExclusiveLockResponse? RejectClientLock(GrpcTryAcquireExclusiveLockRequest request) =>
        inputLimits.KeyFits(request.Key) && ClientLockIdentity.IsRegistered(request.CoordinatorKey, request.OperationIdHigh, request.OperationIdLow)
            ? null
            : new() { Type = GrpcKeyValueResponseType.TypeInvalidInput };

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

    private static GrpcTryGetManyValuesResponse InvalidGetMany(GrpcTryGetManyValuesRequest request)
    {
        GrpcTryGetManyValuesResponse response = new();
        ReserveItemCapacity(response.Items, request.Items.Count);
        foreach (GrpcTryManyValuesRequestItem item in request.Items)
            response.Items.Add(new GrpcTryGetManyValuesResponseItem
            {
                Type = GrpcKeyValueResponseType.TypeInvalidInput,
                Key = item.Key,
                Durability = item.Durability
            });
        return response;
    }

    private static GrpcTryExistsManyValuesResponse InvalidExistsMany(GrpcTryExistsManyValuesRequest request)
    {
        GrpcTryExistsManyValuesResponse response = new();
        ReserveItemCapacity(response.Items, request.Items.Count);
        foreach (GrpcTryManyValuesRequestItem item in request.Items)
            response.Items.Add(new GrpcTryExistsManyValuesResponseItem
            {
                Type = GrpcKeyValueResponseType.TypeInvalidInput,
                Key = item.Key,
                Durability = item.Durability
            });
        return response;
    }

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

    /// <summary>
    /// Retrieves the value associated with the specified key if it exists.
    /// </summary>
    /// <param name="request">The request containing the key to get the associated value for.</param>
    /// <param name="context">The server call context for managing metadata, timing, and cancellation tokens.</param>
    /// <returns>A <see cref="GrpcTryGetKeyValueResponse"/> containing the retrieved value if the key exists, or an appropriate response indicating failure.</returns>
    public override async Task<GrpcTryGetKeyValueResponse> TryGetKeyValue(GrpcTryGetKeyValueRequest request, ServerCallContext context)
    {
        return await TryGetKeyValueInternal(request, context);
    }

    public override Task<GrpcTryGetManyValuesResponse> TryGetManyValues(GrpcTryGetManyValuesRequest request, ServerCallContext context)
    {
        if (!inputLimits.BatchFits(request.Items.Count))
            return Task.FromResult(InvalidGetMany(request));

        return Guard(request, context, static (s, r, c) => s.reads.TryGetManyValuesRoutedCore(r, c), static r => KeyValueMustRetry.TryGetManyValues(r));
    }

    /// <summary>
    /// Handles the gRPC request to check if a specific key exists in the key-value store.
    /// </summary>
    /// <param name="request">The request object containing the key information.</param>
    /// <param name="context">The server call context for the operation.</param>
    /// <returns>A task that represents the asynchronous operation, returning a response indicating whether the key exists.</returns>
    public override async Task<GrpcTryExistsKeyValueResponse> TryExistsKeyValue(GrpcTryExistsKeyValueRequest request, ServerCallContext context)
    {
        return await TryExistsKeyValueInternal(request, context);
    }

    public override Task<GrpcTryExistsManyValuesResponse> TryExistsManyValues(GrpcTryExistsManyValuesRequest request, ServerCallContext context)
    {
        if (!inputLimits.BatchFits(request.Items.Count))
            return Task.FromResult(InvalidExistsMany(request));

        return Guard(request, context, static (s, r, c) => s.reads.TryExistsManyValuesRoutedCore(r, c), static r => KeyValueMustRetry.TryExistsManyValues(r));
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

    /// <summary>
    /// Handles the request to prepare mutations for a set of key-value changes.
    /// </summary>
    /// <param name="request">The incoming request containing key-value mutation details.</param>
    /// <param name="context">The context for the server call providing runtime details and metadata.</param>
    /// <returns>A task that represents the asynchronous operation, containing the response for the mutation preparation request.</returns>
    public override async Task<GrpcTryPrepareMutationsResponse> TryPrepareMutations(GrpcTryPrepareMutationsRequest request, ServerCallContext context)
    {
        // The unary 2PC participant methods are node-only: no client calls them, and they stay on the
        // wire only because other-language clients ship this proto.
        gate.RequirePeer(context);

        return await TryPrepareMutationsInternal(request, context); 
    }

    /// <summary>
    /// Attempts to prepare multiple mutations in a single operation.
    /// </summary>
    /// <param name="request">The request object containing details of the mutations to prepare.</param>
    /// <param name="context">The server call context for the operation.</param>
    /// <returns>A task that represents the asynchronous operation. The task result contains the response object with the outcome of the mutation preparation.</returns>
    public override async Task<GrpcTryPrepareManyMutationsResponse> TryPrepareManyMutations(GrpcTryPrepareManyMutationsRequest request, ServerCallContext context)
    {
        gate.RequirePeer(context);

        return await TryPrepareManyMutationsInternal(request, context);
    }

    /// <summary>
    /// Attempts to commit a series of mutations represented by the request.
    /// </summary>
    /// <param name="request">The request containing the mutations to be committed.</param>
    /// <param name="context">The context for the server call.</param>
    /// <returns>A response indicating the result of the commit attempt.</returns>
    public override async Task<GrpcTryCommitMutationsResponse> TryCommitMutations(GrpcTryCommitMutationsRequest request, ServerCallContext context)
    {
        gate.RequirePeer(context);

        return await TryCommitMutationsInternal(request, context);
    }

    /// <summary>
    /// Attempts to commit multiple mutations atomically.
    /// </summary>
    /// <param name="request">The request containing the mutations to be committed.</param>
    /// <param name="context">The server call context.</param>
    /// <returns>A task representing the asynchronous operation, with a response indicating the result of the commit.</returns>
    public override async Task<GrpcTryCommitManyMutationsResponse> TryCommitManyMutations(GrpcTryCommitManyMutationsRequest request, ServerCallContext context)
    {
        gate.RequirePeer(context);

        return await TryCommitManyMutationsInternal(request, context);
    }

    /// <summary>
    /// Receives requests for the key/value "RollbackMutations" service 
    /// </summary>
    /// <param name="request"></param>
    /// <param name="context"></param>
    /// <returns></returns>
    public override async Task<GrpcTryRollbackMutationsResponse> TryRollbackMutations(GrpcTryRollbackMutationsRequest request, ServerCallContext context)
    {
        gate.RequirePeer(context);

        return await TryRollbackMutationsInternal(request, context);
    }

    /// <summary>
    /// Receives requests for the key/value "RollbackManyMutations" service 
    /// </summary>
    /// <param name="request"></param>
    /// <param name="context"></param>
    /// <returns></returns>
    public override async Task<GrpcTryRollbackManyMutationsResponse> TryRollbackManyMutations(GrpcTryRollbackManyMutationsRequest request, ServerCallContext context)
    {
        gate.RequirePeer(context);

        return await TryRollbackManyMutationsInternal(request, context);
    }

    /// <summary>
    /// Executes a transaction script on the key/value store
    /// </summary>
    /// <param name="request"></param>
    /// <param name="context"></param>
    /// <returns></returns>
    public override async Task<GrpcTryExecuteTransactionScriptResponse> TryExecuteTransactionScript(GrpcTryExecuteTransactionScriptRequest request, ServerCallContext context)
    {
        return await TryExecuteTransactionScriptInternal(request, context);
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

    /// <summary>
    /// Initiates an interactive transaction
    /// </summary>
    /// <param name="request">The request object containing transaction details.</param>
    /// <param name="context">The server call context for the request.</param>
    /// <returns>A task that represents the asynchronous operation, containing the transaction response.</returns>
    public override async Task<GrpcStartTransactionResponse> StartTransaction(GrpcStartTransactionRequest request, ServerCallContext context)
    {
        StampClientSession(request);
        return await StartTransactionInternal(request, context);
    }

    /// <summary>
    /// Sets the mutation limit of a session a client starts from this node's configuration, overwriting whatever the
    /// client sent: the field exists so a forwarded start carries the entry node's limit, and a client must not choose
    /// its own. Not called on the inter-node path, which carries the limit the entry node set.
    /// </summary>
    internal void StampClientSession(GrpcStartTransactionRequest request) =>
        request.MutationLimit = inputLimits.MaxTransactionMutations;

    /// <summary>
    /// Commits a transaction in the key-value store.
    /// </summary>
    /// <param name="request">The request containing transaction details to be committed.</param>
    /// <param name="context">The server call context for the current request.</param>
    /// <returns>A response indicating the result of the commit operation.</returns>
    public override async Task<GrpcCommitTransactionResponse> CommitTransaction(GrpcCommitTransactionRequest request, ServerCallContext context)
    {
        return await CommitTransactionInternal(request, context);
    }

    /// <summary>
    /// Rolls back a transaction in the key-value store operation.
    /// </summary>
    /// <param name="request">The rollback transaction request containing the operation details.</param>
    /// <param name="context">The server call context providing contextual information about the request.</param>
    /// <returns>A response indicating the outcome of the rollback transaction operation.</returns>
    public override async Task<GrpcRollbackTransactionResponse> RollbackTransaction(GrpcRollbackTransactionRequest request, ServerCallContext context)
    {
        return await RollbackTransactionInternal(request, context);
    }

    internal Task<GrpcBeginOperationResponse> BeginOperationInternal(GrpcBeginOperationRequest request, ServerCallContext context)
        => transactions.BeginOperationInternal(request, context);

    internal Task<GrpcCompleteOperationResponse> CompleteOperationInternal(GrpcCompleteOperationRequest request, ServerCallContext context)
        => transactions.CompleteOperationInternal(request, context);

    internal static OperationCompletionPayload FromGrpcCompleteOperationRequest(GrpcCompleteOperationRequest request)
        => KeyValueTransactionHandler.FromGrpcCompleteOperationRequest(request);

    internal Task<GrpcGetTransactionWorkingSetResponse> GetTransactionWorkingSetInternal(GrpcGetTransactionWorkingSetRequest request, ServerCallContext context)
        => transactions.GetTransactionWorkingSetInternal(request, context);

    internal static StagedMutationEffect FromGrpcStagedMutation(GrpcStagedMutationEffect g)
        => KeyValueTransactionHandler.FromGrpcStagedMutation(g);

    /// <summary>
    /// Processes a batch of client key-value requests using a gRPC bidirectional stream
    /// </summary>
    /// <param name="requestStream">The stream of incoming key-value requests from the client.</param>
    /// <param name="responseStream">The stream for sending key-value responses back to the client.</param>
    /// <param name="context">The context of the server call.</param>
    /// <returns>A task that represents the asynchronous processing of the batch of requests.</returns>
    public override async Task BatchClientKeyValueRequests(
        IAsyncStreamReader<GrpcBatchClientKeyValueRequest> requestStream,
        IServerStreamWriter<GrpcBatchClientKeyValueResponse> responseStream,
        ServerCallContext context
    )
    {
        await clientBatcher.BatchClientKeyValueRequests(requestStream, responseStream, context);
    }

    /// <summary>
    /// Processes a batch of server key-value requests using a gRPC bidirectional stream 
    /// </summary>
    /// <param name="requestStream"></param>
    /// <param name="responseStream"></param>
    /// <param name="context"></param>
    public override async Task BatchServerKeyValueRequests(
        IAsyncStreamReader<GrpcBatchServerKeyValueRequest> requestStream,
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
        ServerCallContext context
    )
    {
        // Only peers open this stream: it carries forwarded key-value operations.
        gate.RequirePeer(context);

        await serverBatcher.BatchServerKeyValueRequests(requestStream, responseStream, context);
    }

    /// <summary>
    /// Bidirectional watch streams: one call carries any number of watches (see <see cref="KeyValueWatchGrpcSession"/>).
    /// A refused watch is answered with a typed refusal frame for its watch id. A retryable failure that ends the
    /// whole call is mapped to <see cref="StatusCode.Unavailable"/>, which a client retries, rather than to an
    /// unclassifiable <see cref="StatusCode.Unknown"/>.
    /// </summary>
    public override async Task Watch(
        IAsyncStreamReader<GrpcWatchRequest> requestStream,
        IServerStreamWriter<GrpcWatchResponse> responseStream,
        ServerCallContext context)
    {
        try
        {
            await new KeyValueWatchGrpcSession(keyValues, logger, maxWatchesPerCall).RunAsync(requestStream, responseStream, context.CancellationToken);
        }
        catch (Exception ex) when (RetryableFailureClassifier.IsRetryable(ex) && !context.CancellationToken.IsCancellationRequested)
        {
            logger.LogWarning("Mapping retryable {ExceptionType} on Watch to Unavailable: {Message}", ex.GetType().Name, ex.Message);
            throw new RpcException(new Status(StatusCode.Unavailable, ex.Message));
        }
    }

    /// <summary>
    /// Registers a key space as key-range routed and seeds its whole-space descriptor. Mirrors REST
    /// <c>POST /v1/ranges/register</c> 1:1.
    /// <para>
    /// The routing-mode flip is node-local, so this must be called on <b>every</b> node; the seed is
    /// a replicated meta-partition write that the manager forwards to that partition's leader, so a
    /// follower answering here succeeds rather than refusing.
    /// </para>
    /// <para>
    /// This used to flip the routing mode alone and answer <c>Success = true</c> unconditionally,
    /// which left the space routed by key range with no descriptor to route to — every subsequent
    /// write to it threw "no range descriptor covers key". Seeding is what makes the call usable.
    /// </para>
    /// </summary>
    public override async Task<GrpcRegisterKeyRangeResponse> RegisterKeyRange(
        GrpcRegisterKeyRangeRequest request,
        ServerCallContext context)
    {
        KahunaRegisterKeyRangeResponse outcome = await keyValues
            .RegisterKeyRangeWithOutcomeAsync(request.KeySpace, context.CancellationToken)
            .ConfigureAwait(false);

        return new()
        {
            Success = outcome.Success,
            Status = outcome.Status,
            Seeded = outcome.Seeded,
            RoutingMode = outcome.RoutingMode,
            DescriptorCount = outcome.DescriptorCount,
            Reason = outcome.Reason ?? ""
        };
    }

    /// <summary>
    /// Drops a key space's descriptors from the replicated range map — the inverse of
    /// <see cref="RegisterKeyRange"/>, used for teardown. Mirrors REST
    /// <c>POST /v1/ranges/unregister</c> 1:1, including the retryable refusal while a split holds
    /// the quiesce window open.
    /// </summary>
    public override async Task<GrpcRemoveKeyRangeResponse> RemoveKeyRange(
        GrpcRemoveKeyRangeRequest request,
        ServerCallContext context)
    {
        KahunaRemoveKeyRangeResponse outcome = await keyValues
            .RemoveKeyRangeWithOutcomeAsync(request.KeySpace, context.CancellationToken)
            .ConfigureAwait(false);

        return new()
        {
            Success = outcome.Success,
            Status = outcome.Status,
            RoutingMode = outcome.RoutingMode,
            DescriptorCount = outcome.DescriptorCount,
            Reason = outcome.Reason ?? ""
        };
    }

    internal Task<GrpcEnsureKeyRangeSeededResponse> EnsureKeyRangeSeededInternal(GrpcEnsureKeyRangeSeededRequest request, ServerCallContext context)
        => rangeAdmin.EnsureKeyRangeSeededInternal(request, context);

    internal Task<GrpcEnsureKeyRangeRemovedResponse> EnsureKeyRangeRemovedInternal(GrpcEnsureKeyRangeRemovedRequest request, ServerCallContext context)
        => rangeAdmin.EnsureKeyRangeRemovedInternal(request, context);

    internal Task<GrpcGetRangeLocksResponse> GetRangeLocksInternal(GrpcGetRangeLocksRequest request, ServerCallContext context)
        => rangeAdmin.GetRangeLocksInternal(request, context);

    internal Task<GrpcImportRangeLocksResponse> ImportRangeLocksInternal(GrpcImportRangeLocksRequest request, ServerCallContext context)
        => rangeAdmin.ImportRangeLocksInternal(request, context);

    internal Task<GrpcImportCompletionReceiptsResponse> ImportCompletionReceiptsInternal(GrpcImportCompletionReceiptsRequest request, ServerCallContext context)
        => rangeAdmin.ImportCompletionReceiptsInternal(request, context);

    internal Task<GrpcReplicateKeyValueRangePageResponse> ReplicateKeyValueRangePageInternal(GrpcReplicateKeyValueRangePageRequest request, ServerCallContext context)
        => rangeAdmin.ReplicateKeyValueRangePageInternal(request, context);

    internal Task<GrpcGetRangeTransactionStateResponse> GetRangeTransactionStateInternal(GrpcGetRangeTransactionStateRequest request, ServerCallContext context)
        => rangeAdmin.GetRangeTransactionStateInternal(request, context);

    internal Task<GrpcGetStagedBaseVerdictsResponse> GetStagedBaseVerdictsInternal(GrpcGetStagedBaseVerdictsRequest request, ServerCallContext context)
        => rangeAdmin.GetStagedBaseVerdictsInternal(request, context);

    internal Task<GrpcDurableOperationResponse> DurableOperationInternal(GrpcDurableOperationRequest request, ServerCallContext context)
        => durable.DurableOperationInternal(request, context);

    internal Task<GrpcLookupTransactionRecordResponse> LookupTransactionRecordInternal(GrpcLookupTransactionRecordRequest request, ServerCallContext context)
        => durable.LookupTransactionRecordInternal(request, context);

    internal Task<GrpcDurableBundleResponse> DurableBundleInternal(GrpcDurableBundleRequest request, ServerCallContext context)
        => durable.DurableBundleInternal(request, context);

    internal Task<GrpcDurableDecisionResponse> DurableDecisionInternal(GrpcDurableDecisionRequest request, ServerCallContext context)
        => durable.DurableDecisionInternal(request, context);

    internal Task<GrpcDurableOnePhaseResponse> DurableOnePhaseInternal(GrpcDurableOnePhaseRequest request, ServerCallContext context)
        => durable.DurableOnePhaseInternal(request, context);

    public override Task<GrpcAcquireSnapshotHoldResponse> AcquireSnapshotHold(GrpcAcquireSnapshotHoldRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.snapshots.AcquireSnapshotHoldCore(r, c), static _ => KeyValueMustRetry.AcquireSnapshotHold());

    public override Task<GrpcRenewSnapshotHoldResponse> RenewSnapshotHold(GrpcRenewSnapshotHoldRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.snapshots.RenewSnapshotHoldCore(r, c), static _ => KeyValueMustRetry.RenewSnapshotHold());

    public override Task<GrpcReleaseSnapshotHoldResponse> ReleaseSnapshotHold(GrpcReleaseSnapshotHoldRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.snapshots.ReleaseSnapshotHoldCore(r, c), static _ => KeyValueMustRetry.ReleaseSnapshotHold());

    public override Task<GrpcGetSnapshotFloorResponse> GetSnapshotFloor(GrpcGetSnapshotFloorRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.snapshots.GetSnapshotFloorCore(r, c), static _ => KeyValueMustRetry.GetSnapshotFloor());

    public override Task<GrpcHistoryHoldResponse> HistoryHold(GrpcHistoryHoldRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.snapshots.HistoryHoldCore(r, c), static _ => KeyValueMustRetry.HistoryHold());

    public override Task<GrpcGetPartitionApplyFingerprintResponse> GetPartitionApplyFingerprint(GrpcGetPartitionApplyFingerprintRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.snapshots.GetPartitionApplyFingerprintCore(r, c), static _ => KeyValueMustRetry.GetPartitionApplyFingerprint());

    public override Task<GrpcGetPreparedIntentPresenceResponse> GetPreparedIntentPresence(GrpcGetPreparedIntentPresenceRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.snapshots.GetPreparedIntentPresenceCore(r, c), static _ => KeyValueMustRetry.GetPreparedIntentPresence());

    // ── Retryable-failure guards ────────────────────────────────────────────────────────────────
    //
    // A retry loop must always receive a classifiable answer. Any escape that means "no definitive
    // answer was produced" — a Raft resolution failure, an inter-node transport failure — would
    // otherwise reach a unary caller as gRPC status Unknown, which clients do not retry, and would
    // leave a batched caller waiting on a response that never comes. Each handler is answered with
    // its own MustRetry response instead. Genuine bugs are not retryable and keep propagating, so a
    // real server fault stays visible as an error.
    //
    // Handlers answering with a bare Success/Found flag or a collection are absent below on purpose:
    // they have no field that can express "retry", and a fabricated negative would be
    // indistinguishable from a real answer.

    private async Task<TResponse> Guard<TRequest, TResponse>(
        TRequest request,
        ServerCallContext context,
        Func<KeyValuesService, TRequest, ServerCallContext, Task<TResponse>> handler,
        Func<TRequest, TResponse> refusal,
        [CallerMemberName] string handlerName = ""
    )
    {
        try
        {
            return await handler(this, request, context);
        }
        catch (Exception ex) when (RetryableFailureClassifier.IsRetryable(ex))
        {
            logger.LogWarning(
                "Mapping retryable {ExceptionType} on {Handler} to MustRetry: {Message}",
                ex.GetType().Name, handlerName, ex.Message);

            return refusal(request);
        }
    }

    // ValueTask twin of the guard above for hot handlers converted to pooled async frames: same
    // classification and refusal contract, but the state machine is recycled instead of allocated
    // per request. The returned ValueTask must be awaited exactly once.
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<TResponse> Guard<TRequest, TResponse>(
        TRequest request,
        ServerCallContext context,
        Func<KeyValuesService, TRequest, ServerCallContext, ValueTask<TResponse>> handler,
        Func<TRequest, TResponse> refusal,
        [CallerMemberName] string handlerName = ""
    )
    {
        try
        {
            return await handler(this, request, context);
        }
        catch (Exception ex) when (RetryableFailureClassifier.IsRetryable(ex))
        {
            logger.LogWarning(
                "Mapping retryable {ExceptionType} on {Handler} to MustRetry: {Message}",
                ex.GetType().Name, handlerName, ex.Message);

            return refusal(request);
        }
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

    internal Task<GrpcTryGetKeyValueResponse> TryGetKeyValueInternal(GrpcTryGetKeyValueRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.reads.TryGetKeyValueCore(r, c), static _ => KeyValueMustRetry.TryGetKeyValue());

    internal Task<GrpcTryGetManyValuesResponse> TryGetManyValuesInternal(GrpcTryGetManyValuesRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.reads.TryGetManyValuesCore(r, c), static r => KeyValueMustRetry.TryGetManyValues(r));

    internal Task<GrpcTryProbeManyResponse> TryProbeManyInternal(GrpcTryProbeManyRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.reads.TryProbeManyCore(r, c), static r => ProbeManyWire.MustRetry(r));

    internal Task<GrpcTryProbeManyRangesResponse> TryProbeManyRangesInternal(GrpcTryProbeManyRangesRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.reads.TryProbeManyRangesCore(r, c), static r => ProbeManyRangesWire.MustRetry(r));

    internal Task<GrpcTryExistsKeyValueResponse> TryExistsKeyValueInternal(GrpcTryExistsKeyValueRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.reads.TryExistsKeyValueCore(r, c), static _ => KeyValueMustRetry.TryExistsKeyValue());

    internal Task<GrpcTryExistsManyValuesResponse> TryExistsManyValuesInternal(GrpcTryExistsManyValuesRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.reads.TryExistsManyValuesCore(r, c), static r => KeyValueMustRetry.TryExistsManyValues(r));

    internal Task<GrpcTryCheckWriteIntentResponse> TryCheckWriteIntentInternal(GrpcTryCheckWriteIntentRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.reads.TryCheckWriteIntentCore(r, c), static _ => KeyValueMustRetry.TryCheckWriteIntent());

    internal Task<GrpcTryCheckManyWriteIntentsResponse> TryCheckManyWriteIntentsInternal(GrpcTryCheckManyWriteIntentsRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.reads.TryCheckManyWriteIntentsCore(r, c), static r => KeyValueMustRetry.TryCheckManyWriteIntents(r));

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

    internal Task<GrpcTryPrepareMutationsResponse> TryPrepareMutationsInternal(GrpcTryPrepareMutationsRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.mutations.TryPrepareMutationsCore(r, c), static _ => KeyValueMustRetry.TryPrepareMutations());

    internal Task<GrpcTryPrepareManyMutationsResponse> TryPrepareManyMutationsInternal(GrpcTryPrepareManyMutationsRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.mutations.TryPrepareManyMutationsCore(r, c), static r => KeyValueMustRetry.TryPrepareManyMutations(r));

    internal Task<GrpcTryCommitMutationsResponse> TryCommitMutationsInternal(GrpcTryCommitMutationsRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.mutations.TryCommitMutationsCore(r, c), static _ => KeyValueMustRetry.TryCommitMutations());

    internal Task<GrpcTryCommitManyMutationsResponse> TryCommitManyMutationsInternal(GrpcTryCommitManyMutationsRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.mutations.TryCommitManyMutationsCore(r, c), static r => KeyValueMustRetry.TryCommitManyMutations(r));

    internal Task<GrpcTryRollbackMutationsResponse> TryRollbackMutationsInternal(GrpcTryRollbackMutationsRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.mutations.TryRollbackMutationsCore(r, c), static _ => KeyValueMustRetry.TryRollbackMutations());

    internal Task<GrpcTryRollbackManyMutationsResponse> TryRollbackManyMutationsInternal(GrpcTryRollbackManyMutationsRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.mutations.TryRollbackManyMutationsCore(r, c), static r => KeyValueMustRetry.TryRollbackManyMutations(r));

    internal Task<GrpcTryExecuteTransactionScriptResponse> TryExecuteTransactionScriptInternal(GrpcTryExecuteTransactionScriptRequest request, ServerCallContext context)
        // Every path here is a client's: no node forwards a script to another, so each run takes the client limit.
        => Guard(request, context, static (s, r, c) => s.transactions.TryExecuteTransactionScriptCore(r, c, s.inputLimits.MaxTransactionMutations), static _ => KeyValueMustRetry.TryExecuteTransactionScript());

    internal Task<GrpcGetByBucketResponse> GetByBucketInternal(GrpcGetByBucketRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.scans.GetByBucketCore(r, c), static _ => KeyValueMustRetry.GetByBucket());

    internal Task<GrpcGetByRangeResponse> GetByRangeInternal(GrpcGetByRangeRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.scans.GetByRangeCore(r, c), static _ => KeyValueMustRetry.GetByRange());

    internal Task<GrpcScanByPrefixResponse> ScanByPrefixInternal(GrpcScanByPrefixRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.scans.ScanByPrefixCore(r, c), static _ => KeyValueMustRetry.ScanByPrefix());

    internal Task<GrpcScanAllByPrefixResponse> ScanAllByPrefixInternal(GrpcScanAllByPrefixRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.scans.ScanAllByPrefixCore(r, c), static _ => KeyValueMustRetry.ScanAllByPrefix());

    internal Task<GrpcStartTransactionResponse> StartTransactionInternal(GrpcStartTransactionRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.transactions.StartTransactionCore(r, c), static _ => KeyValueMustRetry.StartTransaction());

    internal Task<GrpcCommitTransactionResponse> CommitTransactionInternal(GrpcCommitTransactionRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.transactions.CommitTransactionCore(r, c), static _ => KeyValueMustRetry.CommitTransaction());

    internal Task<GrpcRollbackTransactionResponse> RollbackTransactionInternal(GrpcRollbackTransactionRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.transactions.RollbackTransactionCore(r, c), static _ => KeyValueMustRetry.RollbackTransaction());

    internal Task<GrpcCloseTransactionResponse> CloseTransactionInternal(GrpcCloseTransactionRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.transactions.CloseTransactionCore(r, c), static _ => KeyValueMustRetry.CloseTransaction());

    internal Task<GrpcAcquireSnapshotHoldResponse> AcquireSnapshotHoldInternal(GrpcAcquireSnapshotHoldRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.snapshots.AcquireSnapshotHoldCore(r, c), static _ => KeyValueMustRetry.AcquireSnapshotHold());

    internal Task<GrpcRenewSnapshotHoldResponse> RenewSnapshotHoldInternal(GrpcRenewSnapshotHoldRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.snapshots.RenewSnapshotHoldCore(r, c), static _ => KeyValueMustRetry.RenewSnapshotHold());

    internal Task<GrpcReleaseSnapshotHoldResponse> ReleaseSnapshotHoldInternal(GrpcReleaseSnapshotHoldRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.snapshots.ReleaseSnapshotHoldCore(r, c), static _ => KeyValueMustRetry.ReleaseSnapshotHold());

    internal Task<GrpcGetSnapshotFloorResponse> GetSnapshotFloorInternal(GrpcGetSnapshotFloorRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.snapshots.GetSnapshotFloorCore(r, c), static _ => KeyValueMustRetry.GetSnapshotFloor());
}
