
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
public sealed partial class KeyValuesService : KeyValuer.KeyValuerBase
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
}
