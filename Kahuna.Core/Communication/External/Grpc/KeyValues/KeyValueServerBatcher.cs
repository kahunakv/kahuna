
using System.Collections.Concurrent;

using Grpc.Core;
using Kahuna.Communication.External.Grpc.Logging;

namespace Kahuna.Communication.External.Grpc.KeyValues;

/// <summary>
/// Provides batching functionality for handling server key-value requests in a gRPC server environment.
/// </summary>
internal sealed class KeyValueServerBatcher
{
    private readonly KeyValuesService service;

    private readonly ILogger<IKahuna> logger;

    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="service"></param>
    /// <param name="logger"></param>
    public KeyValueServerBatcher(KeyValuesService service, ILogger<IKahuna> logger)
    {
        this.service = service;
        this.logger = logger;
    }

    /// <summary>
    /// Per-stream in-flight accounting: the read loop holds one entry for itself, every dispatched
    /// handler enters before it starts, and the last exit completes the drain. Handlers report their
    /// completion here directly (instead of being awaited by a per-request observer) so a dispatched
    /// request costs a single async frame.
    /// </summary>
    private sealed class StreamDrain
    {
        private int inFlight = 1;

        private readonly TaskCompletionSource completed = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task Completed => completed.Task;

        public void Enter() => Interlocked.Increment(ref inFlight);

        public void Exit()
        {
            if (Interlocked.Decrement(ref inFlight) == 0)
                completed.TrySetResult();
        }
    }

    /// <summary>
    /// Per-stream FIFO for the order-sensitive placement/replication handlers. The read loop
    /// allocates turns in arrival order; each handler waits for its predecessor's turn before it
    /// executes. Those handlers therefore run serialized per stream, in arrival order, without
    /// holding the response-write gate across the service call — a slow maintenance call must
    /// not block the write of an already-completed unrelated response on the same stream. A turn
    /// always completes (in the handler's finally), so a faulted or cancelled handler never wedges
    /// its successors.
    /// </summary>
    private sealed class OrderedLane
    {
        private Task tail = Task.CompletedTask;

        /// <summary>
        /// Allocates the next turn. Called only from the stream's read loop, which dispatches
        /// requests one at a time, so the tail needs no synchronization.
        /// </summary>
        public LaneTurn Next()
        {
            Task previous = tail;
            TaskCompletionSource turn = new(TaskCreationOptions.RunContinuationsAsynchronously);
            tail = turn.Task;
            return new LaneTurn(previous, turn);
        }
    }

    /// <summary>
    /// One position in a stream's ordered lane: await <see cref="PreviousCompleted"/>, execute,
    /// then call <see cref="Complete"/> unconditionally so the successors can run.
    /// </summary>
    private readonly struct LaneTurn(Task previous, TaskCompletionSource turn)
    {
        public Task PreviousCompleted => previous;

        public void Complete() => turn.TrySetResult();
    }

    /// <summary>
    /// Processes and handles batch server key-value requests received via gRPC streams.
    /// </summary>
    /// <param name="requestStream">The asynchronous stream of incoming key-value requests.</param>
    /// <param name="responseStream">The asynchronous stream for outgoing key-value responses.</param>
    /// <param name="context">The <see cref="ServerCallContext"/> providing metadata and control over the gRPC method being executed.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    public async Task BatchServerKeyValueRequests(
        IAsyncStreamReader<GrpcBatchServerKeyValueRequest> requestStream,
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
        ServerCallContext context
    )
    {
        StreamDrain drain = new();

        OrderedLane lane = new();

        CoalescingResponseWriter responseWriter = new(responseStream, context.CancellationToken);

        try
        {
            await foreach (GrpcBatchServerKeyValueRequest request in requestStream.ReadAllAsync())
            {
                if (request.Type == GrpcServerBatchType.ServerCoalesced)
                {
                    // Each inner request dispatches exactly like a directly-read one, in carrier
                    // order — the ordered lane hands out its turns in that same order.
                    foreach (GrpcBatchServerKeyValueRequest inner in request.Coalesced)
                    {
                        drain.Enter();

                        using (Kahuna.Server.ForwardedRequestScope.EnterAt(inner.ForwardHops))
                            DispatchServerRequest(responseWriter, lane, inner, context, drain);
                    }

                    continue;
                }

                drain.Enter();

                // Serve each request under the hop count its sender stamped, so the forward budget
                // spans the whole chain instead of restarting at this process boundary. Each
                // handler captures the marker when it is created inside the scope; the next
                // request on this shared stream may carry a different count.
                using (Kahuna.Server.ForwardedRequestScope.EnterAt(request.ForwardHops))
                    DispatchServerRequest(responseWriter, lane, request, context, drain);
            }
        }
        catch (IOException ex)
        {
            logger.LogCommunicationIoException(ex);
        }
        finally
        {
            drain.Exit();
            await drain.Completed;
        }
    }

    /// <summary>
    /// Starts the handler for one request read off the shared inter-node stream. Handlers are
    /// started, not awaited: the stream stays readable while they run, and the drain tracks them
    /// to completion.
    /// </summary>
    private void DispatchServerRequest(
        CoalescingResponseWriter responseWriter,
        OrderedLane lane,
        GrpcBatchServerKeyValueRequest request,
        ServerCallContext context,
        StreamDrain drain
    )
    {
        switch (request.Type)
        {
            case GrpcServerBatchType.ServerTrySetKeyValue:
                _ = TrySetKeyValueServerDelayed(responseWriter, request, context, drain);
                break;

            case GrpcServerBatchType.ServerTrySetManyKeyValue:
                _ = TrySetManyKeyValueServerDelayed(responseWriter, request, context, drain);
                break;

            case GrpcServerBatchType.ServerTryDeleteManyKeyValue:
                _ = TryDeleteManyKeyValueServerDelayed(responseWriter, request, context, drain);
                break;

            case GrpcServerBatchType.ServerTryGetKeyValue:
                _ = TryGetKeyValueServerDelayed(responseWriter, request, context, drain);
                break;

            case GrpcServerBatchType.ServerTryGetManyValues:
                _ = TryGetManyValuesDelayed(responseWriter, request, context, drain);
                break;

            case GrpcServerBatchType.ServerTryDeleteKeyValue:
                _ = TryDeleteKeyValueServerDelayed(responseWriter, request, context, drain);
                break;

            case GrpcServerBatchType.ServerTryExtendKeyValue:
                _ = TryExtendKeyValueServerDelayed(responseWriter, request, context, drain);
                break;

            case GrpcServerBatchType.ServerTryExistsKeyValue:
                _ = TryExistsKeyValueServerDelayed(responseWriter, request, context, drain);
                break;

            case GrpcServerBatchType.ServerTryExistsManyValues:
                _ = TryExistsManyValuesDelayed(responseWriter, request, context, drain);
                break;

            case GrpcServerBatchType.ServerTryCheckWriteIntent:
                _ = TryCheckWriteIntentServerDelayed(responseWriter, request, context, drain);
                break;

            case GrpcServerBatchType.ServerTryCheckManyWriteIntents:
                _ = TryCheckManyWriteIntentsServerDelayed(responseWriter, request, context, drain);
                break;

            case GrpcServerBatchType.ServerTryExecuteTransactionScript:
                _ = TryExecuteTransactionServerDelayed(responseWriter, request, context, drain);
                break;

            case GrpcServerBatchType.ServerTryAcquireExclusiveLock:
                _ = TryAcquireExclusiveLockDelayed(responseWriter, request, context, drain);
                break;

            case GrpcServerBatchType.ServerTryAcquireExclusivePrefixLock:
                _ = TryAcquireExclusivePrefixLockDelayed(responseWriter, request, context, drain);
                break;

            case GrpcServerBatchType.ServerTryAcquireManyExclusiveLocks:
                _ = TryAcquireManyExclusiveLocksDelayed(responseWriter, request, context, drain);
                break;

            case GrpcServerBatchType.ServerTryReleaseExclusiveLock:
                _ = TryReleaseExclusiveLockDelayed(responseWriter, request, context, drain);
                break;

            case GrpcServerBatchType.ServerTryReleaseExclusivePrefixLock:
                _ = TryReleaseExclusivePrefixLockDelayed(responseWriter, request, context, drain);
                break;

            case GrpcServerBatchType.ServerTryAcquireExclusiveRangeLock:
                _ = TryAcquireExclusiveRangeLockDelayed(responseWriter, request, context, drain);
                break;

            case GrpcServerBatchType.ServerTryReleaseExclusiveRangeLock:
                _ = TryReleaseExclusiveRangeLockDelayed(responseWriter, request, context, drain);
                break;

            case GrpcServerBatchType.ServerTryEnsureKeyRangeSeeded:
                _ = EnsureKeyRangeSeededDelayed(responseWriter, lane.Next(), request, context, drain);
                break;

            case GrpcServerBatchType.ServerTryEnsureKeyRangeRemoved:
                _ = EnsureKeyRangeRemovedDelayed(responseWriter, lane.Next(), request, context, drain);
                break;

            case GrpcServerBatchType.ServerTryGetRangeLocks:
                _ = GetRangeLocksDelayed(responseWriter, lane.Next(), request, context, drain);
                break;

            case GrpcServerBatchType.ServerTryImportRangeLocks:
                _ = ImportRangeLocksDelayed(responseWriter, lane.Next(), request, context, drain);
                break;

            case GrpcServerBatchType.ServerImportCompletionReceipts:
                _ = ImportCompletionReceiptsDelayed(responseWriter, lane.Next(), request, context, drain);
                break;

            case GrpcServerBatchType.ServerDurableOperation:
                _ = DurableOperationDelayed(responseWriter, lane.Next(), request, context, drain);
                break;

            case GrpcServerBatchType.ServerDurableBundle:
                _ = DurableBundleDelayed(responseWriter, lane.Next(), request, context, drain);
                break;

            case GrpcServerBatchType.ServerDurableOnePhase:
                _ = DurableOnePhaseDelayed(responseWriter, lane.Next(), request, context, drain);
                break;

            case GrpcServerBatchType.ServerDurableDecision:
                _ = DurableDecisionDelayed(responseWriter, lane.Next(), request, context, drain);
                break;

            case GrpcServerBatchType.ServerLookupTransactionRecord:
                _ = LookupTransactionRecordDelayed(responseWriter, lane.Next(), request, context, drain);
                break;

            case GrpcServerBatchType.ServerReplicateKeyValueRangePage:
                _ = ReplicateKeyValueRangePageDelayed(responseWriter, lane.Next(), request, context, drain);
                break;

            case GrpcServerBatchType.ServerGetRangeTransactionState:
                _ = GetRangeTransactionStateDelayed(responseWriter, lane.Next(), request, context, drain);
                break;

            case GrpcServerBatchType.ServerGetStagedBaseVerdicts:
                _ = GetStagedBaseVerdictsDelayed(responseWriter, request, context, drain);
                break;

            case GrpcServerBatchType.ServerTryReleaseManyExclusiveLocks:
                _ = TryReleaseManyExclusiveLocksDelayed(responseWriter, request, context, drain);
                break;

            case GrpcServerBatchType.ServerTryPrepareMutations:
                _ = TryPrepareMutationsDelayed(responseWriter, request, context, drain);
                break;

            case GrpcServerBatchType.ServerTryPrepareManyMutations:
                _ = TryPrepareManyMutationsDelayed(responseWriter, request, context, drain);
                break;

            case GrpcServerBatchType.ServerTryCommitMutations:
                _ = TryCommitMutationsDelayed(responseWriter, request, context, drain);
                break;

            case GrpcServerBatchType.ServerTryCommitManyMutations:
                _ = TryCommitManyMutationsDelayed(responseWriter, request, context, drain);
                break;

            case GrpcServerBatchType.ServerTryRollbackMutations:
                _ = TryRollbackMutationsDelayed(responseWriter, request, context, drain);
                break;

            case GrpcServerBatchType.ServerTryRollbackManyMutations:
                _ = TryRollbackManyMutationsDelayed(responseWriter, request, context, drain);
                break;

            case GrpcServerBatchType.ServerTryGetByBucket:
                _ = GetByBucketDelayed(responseWriter, request, context, drain);
                break;

            case GrpcServerBatchType.ServerTryGetByRange:
                _ = GetByRangeDelayed(responseWriter, request, context, drain);
                break;

            case GrpcServerBatchType.ServerTryScanByPrefix:
                _ = ScanByPrefixDelayed(responseWriter, request, context, drain);
                break;

            case GrpcServerBatchType.ServerTryStartTransaction:
                _ = StartTransactionDelayed(responseWriter, request, context, drain);
                break;

            case GrpcServerBatchType.ServerTryCommitTransaction:
                _ = CommitTransactionDelayed(responseWriter, request, context, drain);
                break;

            case GrpcServerBatchType.ServerTryRollbackTransaction:
                _ = RollbackTransactionDelayed(responseWriter, request, context, drain);
                break;

            case GrpcServerBatchType.ServerBeginOperation:
                _ = BeginOperationDelayed(responseWriter, request, context, drain);
                break;

            case GrpcServerBatchType.ServerCompleteOperation:
                _ = CompleteOperationDelayed(responseWriter, request, context, drain);
                break;

            case GrpcServerBatchType.ServerGetTransactionWorkingSet:
                _ = GetTransactionWorkingSetDelayed(responseWriter, request, context, drain);
                break;

            case GrpcServerBatchType.ServerCloseTransaction:
                _ = CloseTransactionDelayed(responseWriter, request, context, drain);
                break;

            case GrpcServerBatchType.ServerTryAcquireSnapshotHold:
                _ = AcquireSnapshotHoldDelayed(responseWriter, request, context, drain);
                break;

            case GrpcServerBatchType.ServerTryRenewSnapshotHold:
                _ = RenewSnapshotHoldDelayed(responseWriter, request, context, drain);
                break;

            case GrpcServerBatchType.ServerTryReleaseSnapshotHold:
                _ = ReleaseSnapshotHoldDelayed(responseWriter, request, context, drain);
                break;

            case GrpcServerBatchType.ServerTryGetSnapshotFloor:
                _ = GetSnapshotFloorDelayed(responseWriter, request, context, drain);
                break;

            case GrpcServerBatchType.ServerTypeNone:
            default:
                logger.LogError("Unknown batch Server request type: {Type}", request.Type);
                drain.Exit();
                break;
        }
    }

    /// <summary>
    /// A handler that throws must still answer its own RequestId: the caller matches responses by
    /// id, so an unanswered request hangs until its deadline while every other request on the
    /// shared stream keeps flowing. Refusing just that one request with MustRetry leaves the
    /// stream and its neighbours untouched.
    /// </summary>
    private async Task ObserveFault(
        CoalescingResponseWriter responseWriter,
        GrpcBatchServerKeyValueRequest request,
        ServerCallContext context,
        Exception ex
    )
    {
        if (ex is IOException or OperationCanceledException)
        {
            // The stream is already gone or the caller left; there is nobody left to answer.
            logger.LogCommunicationIoException(ex);
            return;
        }

        logger.LogError(ex, "Batch key-value server handler faulted");

        try
        {
            await responseWriter.WriteAsync(BatchRefusalResponses.ForServerKeyValue(request));
        }
        catch (Exception writeEx)
        {
            logger.LogCommunicationIoException(writeEx);
        }
    }

    private async Task TrySetKeyValueServerDelayed(
        CoalescingResponseWriter responseWriter,
        GrpcBatchServerKeyValueRequest request,
        ServerCallContext context,
        StreamDrain drain
    )
    {
        try
        {
            GrpcTrySetKeyValueResponse trySetResponse = await service.TrySetKeyValueInternal(request.TrySetKeyValue, context);

            GrpcBatchServerKeyValueResponse response = new()
            {
                Type = GrpcServerBatchType.ServerTrySetKeyValue,
                RequestId = request.RequestId,
                TrySetKeyValue = trySetResponse
            };

            await responseWriter.WriteAsync(response);
        }
        catch (Exception ex)
        {
            await ObserveFault(responseWriter, request, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task TrySetManyKeyValueServerDelayed(
        CoalescingResponseWriter responseWriter,
        GrpcBatchServerKeyValueRequest request,
        ServerCallContext context,
        StreamDrain drain
    )
    {
        try
        {
            GrpcTrySetManyKeyValueResponse trySetManyResponse = await service.TrySetManyKeyValueInternal(request.TrySetManyKeyValue, context);

            GrpcBatchServerKeyValueResponse response = new()
            {
                Type = GrpcServerBatchType.ServerTrySetManyKeyValue,
                RequestId = request.RequestId,
                TrySetManyKeyValue = trySetManyResponse
            };

            await responseWriter.WriteAsync(response);
        }
        catch (Exception ex)
        {
            await ObserveFault(responseWriter, request, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task TryDeleteManyKeyValueServerDelayed(
        CoalescingResponseWriter responseWriter,
        GrpcBatchServerKeyValueRequest request,
        ServerCallContext context,
        StreamDrain drain
    )
    {
        try
        {
            GrpcTryDeleteManyKeyValueResponse tryDeleteManyResponse = await service.TryDeleteManyKeyValueInternal(request.TryDeleteManyKeyValue, context);

            GrpcBatchServerKeyValueResponse response = new()
            {
                Type = GrpcServerBatchType.ServerTryDeleteManyKeyValue,
                RequestId = request.RequestId,
                TryDeleteManyKeyValue = tryDeleteManyResponse
            };

            await responseWriter.WriteAsync(response);
        }
        catch (Exception ex)
        {
            await ObserveFault(responseWriter, request, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task TryGetKeyValueServerDelayed(
        CoalescingResponseWriter responseWriter,
        GrpcBatchServerKeyValueRequest request,
        ServerCallContext context,
        StreamDrain drain
    )
    {
        try
        {
            GrpcTryGetKeyValueResponse tryGetResponse = await service.TryGetKeyValueInternal(request.TryGetKeyValue, context);

            GrpcBatchServerKeyValueResponse response = new()
            {
                Type = GrpcServerBatchType.ServerTryGetKeyValue,
                RequestId = request.RequestId,
                TryGetKeyValue = tryGetResponse
            };

            await responseWriter.WriteAsync(response);
        }
        catch (Exception ex)
        {
            await ObserveFault(responseWriter, request, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task TryGetManyValuesDelayed(
        CoalescingResponseWriter responseWriter,
        GrpcBatchServerKeyValueRequest request,
        ServerCallContext context,
        StreamDrain drain
    )
    {
        try
        {
            GrpcTryGetManyValuesResponse tryGetManyResponse = await service.TryGetManyValuesInternal(request.TryGetManyValues, context);

            GrpcBatchServerKeyValueResponse response = new()
            {
                Type = GrpcServerBatchType.ServerTryGetManyValues,
                RequestId = request.RequestId,
                TryGetManyValues = tryGetManyResponse
            };

            await responseWriter.WriteAsync(response);
        }
        catch (Exception ex)
        {
            await ObserveFault(responseWriter, request, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task TryDeleteKeyValueServerDelayed(
        CoalescingResponseWriter responseWriter,
        GrpcBatchServerKeyValueRequest request,
        ServerCallContext context,
        StreamDrain drain
    )
    {
        try
        {
            GrpcTryDeleteKeyValueResponse tryDeleteResponse = await service.TryDeleteKeyValueInternal(request.TryDeleteKeyValue, context);

            GrpcBatchServerKeyValueResponse response = new()
            {
                Type = GrpcServerBatchType.ServerTryDeleteKeyValue,
                RequestId = request.RequestId,
                TryDeleteKeyValue = tryDeleteResponse
            };

            await responseWriter.WriteAsync(response);
        }
        catch (Exception ex)
        {
            await ObserveFault(responseWriter, request, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task TryExtendKeyValueServerDelayed(
        CoalescingResponseWriter responseWriter,
        GrpcBatchServerKeyValueRequest request,
        ServerCallContext context,
        StreamDrain drain
    )
    {
        try
        {
            GrpcTryExtendKeyValueResponse tryExtendResponse = await service.TryExtendKeyValueInternal(request.TryExtendKeyValue, context);

            GrpcBatchServerKeyValueResponse response = new()
            {
                Type = GrpcServerBatchType.ServerTryExtendKeyValue,
                RequestId = request.RequestId,
                TryExtendKeyValue = tryExtendResponse
            };

            await responseWriter.WriteAsync(response);
        }
        catch (Exception ex)
        {
            await ObserveFault(responseWriter, request, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task TryExistsKeyValueServerDelayed(
        CoalescingResponseWriter responseWriter,
        GrpcBatchServerKeyValueRequest request,
        ServerCallContext context,
        StreamDrain drain
    )
    {
        try
        {
            GrpcTryExistsKeyValueResponse tryExistsResponse = await service.TryExistsKeyValueInternal(request.TryExistsKeyValue, context);

            GrpcBatchServerKeyValueResponse response = new()
            {
                Type = GrpcServerBatchType.ServerTryExistsKeyValue,
                RequestId = request.RequestId,
                TryExistsKeyValue = tryExistsResponse
            };

            await responseWriter.WriteAsync(response);
        }
        catch (Exception ex)
        {
            await ObserveFault(responseWriter, request, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task TryExistsManyValuesDelayed(
        CoalescingResponseWriter responseWriter,
        GrpcBatchServerKeyValueRequest request,
        ServerCallContext context,
        StreamDrain drain
    )
    {
        try
        {
            GrpcTryExistsManyValuesResponse tryExistsManyResponse = await service.TryExistsManyValuesInternal(request.TryExistsManyValues, context);

            GrpcBatchServerKeyValueResponse response = new()
            {
                Type = GrpcServerBatchType.ServerTryExistsManyValues,
                RequestId = request.RequestId,
                TryExistsManyValues = tryExistsManyResponse
            };

            await responseWriter.WriteAsync(response);
        }
        catch (Exception ex)
        {
            await ObserveFault(responseWriter, request, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task TryCheckWriteIntentServerDelayed(
        CoalescingResponseWriter responseWriter,
        GrpcBatchServerKeyValueRequest request,
        ServerCallContext context,
        StreamDrain drain
    )
    {
        try
        {
            GrpcTryCheckWriteIntentResponse tryCheckWriteIntentResponse = await service.TryCheckWriteIntentInternal(request.TryCheckWriteIntent, context);

            GrpcBatchServerKeyValueResponse response = new()
            {
                Type = GrpcServerBatchType.ServerTryCheckWriteIntent,
                RequestId = request.RequestId,
                TryCheckWriteIntent = tryCheckWriteIntentResponse
            };

            await responseWriter.WriteAsync(response);
        }
        catch (Exception ex)
        {
            await ObserveFault(responseWriter, request, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task TryCheckManyWriteIntentsServerDelayed(
        CoalescingResponseWriter responseWriter,
        GrpcBatchServerKeyValueRequest request,
        ServerCallContext context,
        StreamDrain drain
    )
    {
        try
        {
            GrpcTryCheckManyWriteIntentsResponse tryCheckManyWriteIntentsResponse =
                await service.TryCheckManyWriteIntentsInternal(request.TryCheckManyWriteIntents, context);

            GrpcBatchServerKeyValueResponse response = new()
            {
                Type = GrpcServerBatchType.ServerTryCheckManyWriteIntents,
                RequestId = request.RequestId,
                TryCheckManyWriteIntents = tryCheckManyWriteIntentsResponse
            };

            await responseWriter.WriteAsync(response);
        }
        catch (Exception ex)
        {
            await ObserveFault(responseWriter, request, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task TryExecuteTransactionServerDelayed(
        CoalescingResponseWriter responseWriter,
        GrpcBatchServerKeyValueRequest request,
        ServerCallContext context,
        StreamDrain drain
    )
    {
        try
        {
            GrpcTryExecuteTransactionScriptResponse tryExecuteTransactionScriptResponse = await service.TryExecuteTransactionScriptInternal(request.TryExecuteTransactionScript, context);

            GrpcBatchServerKeyValueResponse response = new()
            {
                Type = GrpcServerBatchType.ServerTryExecuteTransactionScript,
                RequestId = request.RequestId,
                TryExecuteTransactionScript = tryExecuteTransactionScriptResponse
            };

            await responseWriter.WriteAsync(response);
        }
        catch (Exception ex)
        {
            await ObserveFault(responseWriter, request, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task TryAcquireExclusiveLockDelayed(
        CoalescingResponseWriter responseWriter,
        GrpcBatchServerKeyValueRequest request,
        ServerCallContext context,
        StreamDrain drain
    )
    {
        try
        {
            GrpcTryAcquireExclusiveLockResponse tryAcquireExclusiveLockResponse = await service.TryAcquireExclusiveLockInternal(request.TryAcquireExclusiveLock, context);

            GrpcBatchServerKeyValueResponse response = new()
            {
                Type = GrpcServerBatchType.ServerTryAcquireExclusiveLock,
                RequestId = request.RequestId,
                TryAcquireExclusiveLock = tryAcquireExclusiveLockResponse
            };

            await responseWriter.WriteAsync(response);
        }
        catch (Exception ex)
        {
            await ObserveFault(responseWriter, request, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task TryAcquireExclusivePrefixLockDelayed(
        CoalescingResponseWriter responseWriter,
        GrpcBatchServerKeyValueRequest request,
        ServerCallContext context,
        StreamDrain drain
    )
    {
        try
        {
            GrpcTryAcquireExclusivePrefixLockResponse tryAcquireExclusivePrefixLockResponse = await service.TryAcquireExclusivePrefixLockInternal(request.TryAcquireExclusivePrefixLock, context);

            GrpcBatchServerKeyValueResponse response = new()
            {
                Type = GrpcServerBatchType.ServerTryAcquireExclusivePrefixLock,
                RequestId = request.RequestId,
                TryAcquireExclusivePrefixLock = tryAcquireExclusivePrefixLockResponse
            };

            await responseWriter.WriteAsync(response);
        }
        catch (Exception ex)
        {
            await ObserveFault(responseWriter, request, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task TryAcquireManyExclusiveLocksDelayed(
        CoalescingResponseWriter responseWriter,
        GrpcBatchServerKeyValueRequest request,
        ServerCallContext context,
        StreamDrain drain
    )
    {
        try
        {
            GrpcTryAcquireManyExclusiveLocksResponse tryAcquireManyExclusiveLocksResponse = await service.TryAcquireManyExclusiveLocksInternal(request.TryAcquireManyExclusiveLocks, context);

            GrpcBatchServerKeyValueResponse response = new()
            {
                Type = GrpcServerBatchType.ServerTryAcquireManyExclusiveLocks,
                RequestId = request.RequestId,
                TryAcquireManyExclusiveLocks = tryAcquireManyExclusiveLocksResponse
            };

            await responseWriter.WriteAsync(response);
        }
        catch (Exception ex)
        {
            await ObserveFault(responseWriter, request, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task TryReleaseExclusiveLockDelayed(
        CoalescingResponseWriter responseWriter,
        GrpcBatchServerKeyValueRequest request,
        ServerCallContext context,
        StreamDrain drain
    )
    {
        try
        {
            GrpcTryReleaseExclusiveLockResponse tryReleaseExclusiveLockResponse = await service.TryReleaseExclusiveLockInternal(request.TryReleaseExclusiveLock, context);

            GrpcBatchServerKeyValueResponse response = new()
            {
                Type = GrpcServerBatchType.ServerTryReleaseExclusiveLock,
                RequestId = request.RequestId,
                TryReleaseExclusiveLock = tryReleaseExclusiveLockResponse
            };

            await responseWriter.WriteAsync(response);
        }
        catch (Exception ex)
        {
            await ObserveFault(responseWriter, request, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task TryReleaseExclusivePrefixLockDelayed(
        CoalescingResponseWriter responseWriter,
        GrpcBatchServerKeyValueRequest request,
        ServerCallContext context,
        StreamDrain drain
    )
    {
        try
        {
            GrpcTryReleaseExclusivePrefixLockResponse tryReleaseExclusivePrefixLockResponse = await service.TryReleaseExclusivePrefixLockInternal(request.TryReleaseExclusivePrefixLock, context);

            GrpcBatchServerKeyValueResponse response = new()
            {
                Type = GrpcServerBatchType.ServerTryReleaseExclusivePrefixLock,
                RequestId = request.RequestId,
                TryReleaseExclusivePrefixLock = tryReleaseExclusivePrefixLockResponse
            };

            await responseWriter.WriteAsync(response);
        }
        catch (Exception ex)
        {
            await ObserveFault(responseWriter, request, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task TryAcquireExclusiveRangeLockDelayed(
        CoalescingResponseWriter responseWriter,
        GrpcBatchServerKeyValueRequest request,
        ServerCallContext context,
        StreamDrain drain
    )
    {
        try
        {
            GrpcTryAcquireExclusiveRangeLockResponse resp = await service.TryAcquireExclusiveRangeLockInternal(request.TryAcquireExclusiveRangeLock, context);

            GrpcBatchServerKeyValueResponse response = new()
            {
                Type = GrpcServerBatchType.ServerTryAcquireExclusiveRangeLock,
                RequestId = request.RequestId,
                TryAcquireExclusiveRangeLock = resp
            };

            await responseWriter.WriteAsync(response);
        }
        catch (Exception ex)
        {
            await ObserveFault(responseWriter, request, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task TryReleaseExclusiveRangeLockDelayed(
        CoalescingResponseWriter responseWriter,
        GrpcBatchServerKeyValueRequest request,
        ServerCallContext context,
        StreamDrain drain
    )
    {
        try
        {
            GrpcTryReleaseExclusiveRangeLockResponse resp = await service.TryReleaseExclusiveRangeLockInternal(request.TryReleaseExclusiveRangeLock, context);

            GrpcBatchServerKeyValueResponse response = new()
            {
                Type = GrpcServerBatchType.ServerTryReleaseExclusiveRangeLock,
                RequestId = request.RequestId,
                TryReleaseExclusiveRangeLock = resp
            };

            await responseWriter.WriteAsync(response);
        }
        catch (Exception ex)
        {
            await ObserveFault(responseWriter, request, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    // The handlers below run on the stream's ordered lane: the read loop assigns each a turn in
    // arrival order and a handler waits for its predecessor's turn before executing, so these
    // placement/replication operations stay serialized per stream in arrival order. Keep that
    // shape: their effects are order-sensitive (seed before import, import before receipts). The
    // lane deliberately does not involve the response-write gate during execution — holding
    // that across a slow service call would block the write of every already-completed unrelated
    // response on this shared stream. The write gate is taken only for the write itself.

    private async Task EnsureKeyRangeSeededDelayed(
        CoalescingResponseWriter responseWriter,
        LaneTurn turn,
        GrpcBatchServerKeyValueRequest request,
        ServerCallContext context,
        StreamDrain drain
    )
    {
        try
        {
            await turn.PreviousCompleted;
            context.CancellationToken.ThrowIfCancellationRequested();

            GrpcEnsureKeyRangeSeededResponse resp = await service.EnsureKeyRangeSeededInternal(request.EnsureKeyRangeSeeded, context);

            await responseWriter.WriteAsync(new GrpcBatchServerKeyValueResponse
            {
                Type = GrpcServerBatchType.ServerTryEnsureKeyRangeSeeded,
                RequestId = request.RequestId,
                EnsureKeyRangeSeeded = resp
            });
        }
        catch (Exception ex)
        {
            await ObserveFault(responseWriter, request, context, ex);
        }
        finally
        {
            turn.Complete();
            drain.Exit();
        }
    }

    private async Task EnsureKeyRangeRemovedDelayed(
        CoalescingResponseWriter responseWriter,
        LaneTurn turn,
        GrpcBatchServerKeyValueRequest request,
        ServerCallContext context,
        StreamDrain drain
    )
    {
        try
        {
            await turn.PreviousCompleted;
            context.CancellationToken.ThrowIfCancellationRequested();

            GrpcEnsureKeyRangeRemovedResponse resp = await service.EnsureKeyRangeRemovedInternal(request.EnsureKeyRangeRemoved, context);

            await responseWriter.WriteAsync(new GrpcBatchServerKeyValueResponse
            {
                Type = GrpcServerBatchType.ServerTryEnsureKeyRangeRemoved,
                RequestId = request.RequestId,
                EnsureKeyRangeRemoved = resp
            });
        }
        catch (Exception ex)
        {
            await ObserveFault(responseWriter, request, context, ex);
        }
        finally
        {
            turn.Complete();
            drain.Exit();
        }
    }

    private async Task GetRangeLocksDelayed(
        CoalescingResponseWriter responseWriter,
        LaneTurn turn,
        GrpcBatchServerKeyValueRequest request,
        ServerCallContext context,
        StreamDrain drain
    )
    {
        try
        {
            await turn.PreviousCompleted;
            context.CancellationToken.ThrowIfCancellationRequested();

            GrpcGetRangeLocksResponse resp = await service.GetRangeLocksInternal(request.GetRangeLocks, context);

            await responseWriter.WriteAsync(new GrpcBatchServerKeyValueResponse
            {
                Type = GrpcServerBatchType.ServerTryGetRangeLocks,
                RequestId = request.RequestId,
                GetRangeLocks = resp
            });
        }
        catch (Exception ex)
        {
            await ObserveFault(responseWriter, request, context, ex);
        }
        finally
        {
            turn.Complete();
            drain.Exit();
        }
    }

    private async Task ImportRangeLocksDelayed(
        CoalescingResponseWriter responseWriter,
        LaneTurn turn,
        GrpcBatchServerKeyValueRequest request,
        ServerCallContext context,
        StreamDrain drain
    )
    {
        try
        {
            await turn.PreviousCompleted;
            context.CancellationToken.ThrowIfCancellationRequested();

            GrpcImportRangeLocksResponse resp = await service.ImportRangeLocksInternal(request.ImportRangeLocks, context);

            await responseWriter.WriteAsync(new GrpcBatchServerKeyValueResponse
            {
                Type = GrpcServerBatchType.ServerTryImportRangeLocks,
                RequestId = request.RequestId,
                ImportRangeLocks = resp
            });
        }
        catch (Exception ex)
        {
            await ObserveFault(responseWriter, request, context, ex);
        }
        finally
        {
            turn.Complete();
            drain.Exit();
        }
    }

    private async Task ImportCompletionReceiptsDelayed(
        CoalescingResponseWriter responseWriter,
        LaneTurn turn,
        GrpcBatchServerKeyValueRequest request,
        ServerCallContext context,
        StreamDrain drain
    )
    {
        try
        {
            await turn.PreviousCompleted;
            context.CancellationToken.ThrowIfCancellationRequested();

            GrpcImportCompletionReceiptsResponse resp = await service.ImportCompletionReceiptsInternal(request.ImportCompletionReceipts, context);

            await responseWriter.WriteAsync(new GrpcBatchServerKeyValueResponse
            {
                Type = GrpcServerBatchType.ServerImportCompletionReceipts,
                RequestId = request.RequestId,
                ImportCompletionReceipts = resp
            });
        }
        catch (Exception ex)
        {
            await ObserveFault(responseWriter, request, context, ex);
        }
        finally
        {
            turn.Complete();
            drain.Exit();
        }
    }

    private async Task DurableOperationDelayed(
        CoalescingResponseWriter responseWriter,
        LaneTurn turn,
        GrpcBatchServerKeyValueRequest request,
        ServerCallContext context,
        StreamDrain drain
    )
    {
        try
        {
            await turn.PreviousCompleted;
            context.CancellationToken.ThrowIfCancellationRequested();

            GrpcDurableOperationResponse resp = await service.DurableOperationInternal(request.DurableOperation, context);

            await responseWriter.WriteAsync(new GrpcBatchServerKeyValueResponse
            {
                Type = GrpcServerBatchType.ServerDurableOperation,
                RequestId = request.RequestId,
                DurableOperation = resp
            });
        }
        catch (Exception ex)
        {
            await ObserveFault(responseWriter, request, context, ex);
        }
        finally
        {
            turn.Complete();
            drain.Exit();
        }
    }

    private async Task DurableBundleDelayed(
        CoalescingResponseWriter responseWriter,
        LaneTurn turn,
        GrpcBatchServerKeyValueRequest request,
        ServerCallContext context,
        StreamDrain drain
    )
    {
        try
        {
            await turn.PreviousCompleted;
            context.CancellationToken.ThrowIfCancellationRequested();

            GrpcDurableBundleResponse resp = await service.DurableBundleInternal(request.DurableBundle, context);

            await responseWriter.WriteAsync(new GrpcBatchServerKeyValueResponse
            {
                Type = GrpcServerBatchType.ServerDurableBundle,
                RequestId = request.RequestId,
                DurableBundle = resp
            });
        }
        catch (Exception ex)
        {
            await ObserveFault(responseWriter, request, context, ex);
        }
        finally
        {
            turn.Complete();
            drain.Exit();
        }
    }

    private async Task DurableOnePhaseDelayed(
        CoalescingResponseWriter responseWriter,
        LaneTurn turn,
        GrpcBatchServerKeyValueRequest request,
        ServerCallContext context,
        StreamDrain drain
    )
    {
        try
        {
            await turn.PreviousCompleted;
            context.CancellationToken.ThrowIfCancellationRequested();

            GrpcDurableOnePhaseResponse resp = await service.DurableOnePhaseInternal(request.DurableOnePhase, context);

            await responseWriter.WriteAsync(new GrpcBatchServerKeyValueResponse
            {
                Type = GrpcServerBatchType.ServerDurableOnePhase,
                RequestId = request.RequestId,
                DurableOnePhase = resp
            });
        }
        catch (Exception ex)
        {
            await ObserveFault(responseWriter, request, context, ex);
        }
        finally
        {
            turn.Complete();
            drain.Exit();
        }
    }

    private async Task DurableDecisionDelayed(
        CoalescingResponseWriter responseWriter,
        LaneTurn turn,
        GrpcBatchServerKeyValueRequest request,
        ServerCallContext context,
        StreamDrain drain
    )
    {
        try
        {
            await turn.PreviousCompleted;
            context.CancellationToken.ThrowIfCancellationRequested();

            GrpcDurableDecisionResponse resp = await service.DurableDecisionInternal(request.DurableDecision, context);

            await responseWriter.WriteAsync(new GrpcBatchServerKeyValueResponse
            {
                Type = GrpcServerBatchType.ServerDurableDecision,
                RequestId = request.RequestId,
                DurableDecision = resp
            });
        }
        catch (Exception ex)
        {
            await ObserveFault(responseWriter, request, context, ex);
        }
        finally
        {
            turn.Complete();
            drain.Exit();
        }
    }

    private async Task ReplicateKeyValueRangePageDelayed(
        CoalescingResponseWriter responseWriter,
        LaneTurn turn,
        GrpcBatchServerKeyValueRequest request,
        ServerCallContext context,
        StreamDrain drain
    )
    {
        try
        {
            await turn.PreviousCompleted;
            context.CancellationToken.ThrowIfCancellationRequested();

            GrpcReplicateKeyValueRangePageResponse resp = await service.ReplicateKeyValueRangePageInternal(request.ReplicateKeyValueRangePage, context);

            await responseWriter.WriteAsync(new GrpcBatchServerKeyValueResponse
            {
                Type = GrpcServerBatchType.ServerReplicateKeyValueRangePage,
                RequestId = request.RequestId,
                ReplicateKeyValueRangePage = resp
            });
        }
        catch (Exception ex)
        {
            await ObserveFault(responseWriter, request, context, ex);
        }
        finally
        {
            turn.Complete();
            drain.Exit();
        }
    }

    private async Task GetRangeTransactionStateDelayed(
        CoalescingResponseWriter responseWriter,
        LaneTurn turn,
        GrpcBatchServerKeyValueRequest request,
        ServerCallContext context,
        StreamDrain drain
    )
    {
        try
        {
            await turn.PreviousCompleted;
            context.CancellationToken.ThrowIfCancellationRequested();

            GrpcGetRangeTransactionStateResponse resp = await service.GetRangeTransactionStateInternal(request.GetRangeTransactionState, context);

            await responseWriter.WriteAsync(new GrpcBatchServerKeyValueResponse
            {
                Type = GrpcServerBatchType.ServerGetRangeTransactionState,
                RequestId = request.RequestId,
                GetRangeTransactionState = resp
            });
        }
        catch (Exception ex)
        {
            await ObserveFault(responseWriter, request, context, ex);
        }
        finally
        {
            turn.Complete();
            drain.Exit();
        }
    }

    private async Task GetStagedBaseVerdictsDelayed(
        CoalescingResponseWriter responseWriter,
        GrpcBatchServerKeyValueRequest request,
        ServerCallContext context,
        StreamDrain drain
    )
    {
        try
        {
            // The verdict read may wait (bounded) for the prepare to apply locally, so it must run
            // OUTSIDE the stream write gate — holding it would stall every other forwarded
            // request on this shared stream for the duration of that wait.
            GrpcGetStagedBaseVerdictsResponse resp = await service.GetStagedBaseVerdictsInternal(request.GetStagedBaseVerdicts, context);

            GrpcBatchServerKeyValueResponse response = new()
            {
                Type = GrpcServerBatchType.ServerGetStagedBaseVerdicts,
                RequestId = request.RequestId,
                GetStagedBaseVerdicts = resp
            };

            await responseWriter.WriteAsync(response);
        }
        catch (Exception ex)
        {
            await ObserveFault(responseWriter, request, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task LookupTransactionRecordDelayed(
        CoalescingResponseWriter responseWriter,
        LaneTurn turn,
        GrpcBatchServerKeyValueRequest request,
        ServerCallContext context,
        StreamDrain drain
    )
    {
        try
        {
            await turn.PreviousCompleted;
            context.CancellationToken.ThrowIfCancellationRequested();

            GrpcLookupTransactionRecordResponse resp = await service.LookupTransactionRecordInternal(request.LookupTransactionRecord, context);

            await responseWriter.WriteAsync(new GrpcBatchServerKeyValueResponse
            {
                Type = GrpcServerBatchType.ServerLookupTransactionRecord,
                RequestId = request.RequestId,
                LookupTransactionRecord = resp
            });
        }
        catch (Exception ex)
        {
            await ObserveFault(responseWriter, request, context, ex);
        }
        finally
        {
            turn.Complete();
            drain.Exit();
        }
    }

    private async Task TryReleaseManyExclusiveLocksDelayed(
        CoalescingResponseWriter responseWriter,
        GrpcBatchServerKeyValueRequest request,
        ServerCallContext context,
        StreamDrain drain
    )
    {
        try
        {
            GrpcTryReleaseManyExclusiveLocksResponse tryReleaseManyExclusiveLocksResponse = await service.TryReleaseManyExclusiveLocksInternal(request.TryReleaseManyExclusiveLocks, context);

            GrpcBatchServerKeyValueResponse response = new()
            {
                Type = GrpcServerBatchType.ServerTryReleaseManyExclusiveLocks,
                RequestId = request.RequestId,
                TryReleaseManyExclusiveLocks = tryReleaseManyExclusiveLocksResponse
            };

            await responseWriter.WriteAsync(response);
        }
        catch (Exception ex)
        {
            await ObserveFault(responseWriter, request, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task TryPrepareMutationsDelayed(
        CoalescingResponseWriter responseWriter,
        GrpcBatchServerKeyValueRequest request,
        ServerCallContext context,
        StreamDrain drain
    )
    {
        try
        {
            GrpcTryPrepareMutationsResponse tryPrepareMutationsResponse = await service.TryPrepareMutationsInternal(request.TryPrepareMutations, context);

            GrpcBatchServerKeyValueResponse response = new()
            {
                Type = GrpcServerBatchType.ServerTryPrepareMutations,
                RequestId = request.RequestId,
                TryPrepareMutations = tryPrepareMutationsResponse
            };

            await responseWriter.WriteAsync(response);
        }
        catch (Exception ex)
        {
            await ObserveFault(responseWriter, request, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task TryPrepareManyMutationsDelayed(
        CoalescingResponseWriter responseWriter,
        GrpcBatchServerKeyValueRequest request,
        ServerCallContext context,
        StreamDrain drain
    )
    {
        try
        {
            GrpcTryPrepareManyMutationsResponse tryPrepareManyMutationsResponse = await service.TryPrepareManyMutationsInternal(request.TryPrepareManyMutations, context);

            GrpcBatchServerKeyValueResponse response = new()
            {
                Type = GrpcServerBatchType.ServerTryPrepareManyMutations,
                RequestId = request.RequestId,
                TryPrepareManyMutations = tryPrepareManyMutationsResponse
            };

            await responseWriter.WriteAsync(response);
        }
        catch (Exception ex)
        {
            await ObserveFault(responseWriter, request, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task TryCommitMutationsDelayed(
        CoalescingResponseWriter responseWriter,
        GrpcBatchServerKeyValueRequest request,
        ServerCallContext context,
        StreamDrain drain
    )
    {
        try
        {
            GrpcTryCommitMutationsResponse tryCommitMutationsResponse = await service.TryCommitMutationsInternal(request.TryCommitMutations, context);

            GrpcBatchServerKeyValueResponse response = new()
            {
                Type = GrpcServerBatchType.ServerTryCommitMutations,
                RequestId = request.RequestId,
                TryCommitMutations = tryCommitMutationsResponse
            };

            await responseWriter.WriteAsync(response);
        }
        catch (Exception ex)
        {
            await ObserveFault(responseWriter, request, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task TryCommitManyMutationsDelayed(
        CoalescingResponseWriter responseWriter,
        GrpcBatchServerKeyValueRequest request,
        ServerCallContext context,
        StreamDrain drain
    )
    {
        try
        {
            GrpcTryCommitManyMutationsResponse tryCommitManyMutationsResponse = await service.TryCommitManyMutationsInternal(request.TryCommitManyMutations, context);

            GrpcBatchServerKeyValueResponse response = new()
            {
                Type = GrpcServerBatchType.ServerTryCommitManyMutations,
                RequestId = request.RequestId,
                TryCommitManyMutations = tryCommitManyMutationsResponse
            };

            await responseWriter.WriteAsync(response);
        }
        catch (Exception ex)
        {
            await ObserveFault(responseWriter, request, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task TryRollbackMutationsDelayed(
        CoalescingResponseWriter responseWriter,
        GrpcBatchServerKeyValueRequest request,
        ServerCallContext context,
        StreamDrain drain
    )
    {
        try
        {
            GrpcTryRollbackMutationsResponse tryRollbackMutationsResponse = await service.TryRollbackMutationsInternal(request.TryRollbackMutations, context);

            GrpcBatchServerKeyValueResponse response = new()
            {
                Type = GrpcServerBatchType.ServerTryRollbackMutations,
                RequestId = request.RequestId,
                TryRollbackMutations = tryRollbackMutationsResponse
            };

            await responseWriter.WriteAsync(response);
        }
        catch (Exception ex)
        {
            await ObserveFault(responseWriter, request, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task TryRollbackManyMutationsDelayed(
        CoalescingResponseWriter responseWriter,
        GrpcBatchServerKeyValueRequest request,
        ServerCallContext context,
        StreamDrain drain
    )
    {
        try
        {
            GrpcTryRollbackManyMutationsResponse tryRollbackManyMutationsResponse = await service.TryRollbackManyMutationsInternal(request.TryRollbackManyMutations, context);

            GrpcBatchServerKeyValueResponse response = new()
            {
                Type = GrpcServerBatchType.ServerTryRollbackManyMutations,
                RequestId = request.RequestId,
                TryRollbackManyMutations = tryRollbackManyMutationsResponse
            };

            await responseWriter.WriteAsync(response);
        }
        catch (Exception ex)
        {
            await ObserveFault(responseWriter, request, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task GetByBucketDelayed(
        CoalescingResponseWriter responseWriter,
        GrpcBatchServerKeyValueRequest request,
        ServerCallContext context,
        StreamDrain drain
    )
    {
        try
        {
            GrpcGetByBucketResponse getByBucketResponse = await service.GetByBucketInternal(request.GetByBucket, context);

            GrpcBatchServerKeyValueResponse response = new()
            {
                Type = GrpcServerBatchType.ServerTryGetByBucket,
                RequestId = request.RequestId,
                GetByBucket = getByBucketResponse
            };

            await responseWriter.WriteAsync(response);
        }
        catch (Exception ex)
        {
            await ObserveFault(responseWriter, request, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task GetByRangeDelayed(
        CoalescingResponseWriter responseWriter,
        GrpcBatchServerKeyValueRequest request,
        ServerCallContext context,
        StreamDrain drain
    )
    {
        try
        {
            GrpcGetByRangeResponse getByRangeResponse = await service.GetByRangeInternal(request.GetByRange, context);

            GrpcBatchServerKeyValueResponse response = new()
            {
                Type = GrpcServerBatchType.ServerTryGetByRange,
                RequestId = request.RequestId,
                GetByRange = getByRangeResponse
            };

            await responseWriter.WriteAsync(response);
        }
        catch (Exception ex)
        {
            await ObserveFault(responseWriter, request, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task ScanByPrefixDelayed(
        CoalescingResponseWriter responseWriter,
        GrpcBatchServerKeyValueRequest request,
        ServerCallContext context,
        StreamDrain drain
    )
    {
        try
        {
            GrpcScanByPrefixResponse scanByPrefixResponse = await service.ScanByPrefixInternal(request.ScanByPrefix, context);

            GrpcBatchServerKeyValueResponse response = new()
            {
                Type = GrpcServerBatchType.ServerTryScanByPrefix,
                RequestId = request.RequestId,
                ScanByPrefix = scanByPrefixResponse
            };

            await responseWriter.WriteAsync(response);
        }
        catch (Exception ex)
        {
            await ObserveFault(responseWriter, request, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task StartTransactionDelayed(
        CoalescingResponseWriter responseWriter,
        GrpcBatchServerKeyValueRequest request,
        ServerCallContext context,
        StreamDrain drain
    )
    {
        try
        {
            GrpcStartTransactionResponse startTransactionResponse = await service.StartTransactionInternal(request.StartTransaction, context);

            GrpcBatchServerKeyValueResponse response = new()
            {
                Type = GrpcServerBatchType.ServerTryStartTransaction,
                RequestId = request.RequestId,
                StartTransaction = startTransactionResponse
            };

            await responseWriter.WriteAsync(response);
        }
        catch (Exception ex)
        {
            await ObserveFault(responseWriter, request, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task CommitTransactionDelayed(
        CoalescingResponseWriter responseWriter,
        GrpcBatchServerKeyValueRequest request,
        ServerCallContext context,
        StreamDrain drain
    )
    {
        try
        {
            GrpcCommitTransactionResponse commitTransactionResponse = await service.CommitTransactionInternal(request.CommitTransaction, context);

            GrpcBatchServerKeyValueResponse response = new()
            {
                Type = GrpcServerBatchType.ServerTryCommitTransaction,
                RequestId = request.RequestId,
                CommitTransaction = commitTransactionResponse
            };

            await responseWriter.WriteAsync(response);
        }
        catch (Exception ex)
        {
            await ObserveFault(responseWriter, request, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task RollbackTransactionDelayed(
        CoalescingResponseWriter responseWriter,
        GrpcBatchServerKeyValueRequest request,
        ServerCallContext context,
        StreamDrain drain
    )
    {
        try
        {
            GrpcRollbackTransactionResponse rollbackTransactionResponse = await service.RollbackTransactionInternal(request.RollbackTransaction, context);

            GrpcBatchServerKeyValueResponse response = new()
            {
                Type = GrpcServerBatchType.ServerTryRollbackTransaction,
                RequestId = request.RequestId,
                RollbackTransaction = rollbackTransactionResponse
            };

            await responseWriter.WriteAsync(response);
        }
        catch (Exception ex)
        {
            await ObserveFault(responseWriter, request, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task BeginOperationDelayed(
        CoalescingResponseWriter responseWriter,
        GrpcBatchServerKeyValueRequest request,
        ServerCallContext context,
        StreamDrain drain
    )
    {
        try
        {
            GrpcBeginOperationResponse beginOperationResponse = await service.BeginOperationInternal(request.BeginOperation, context);

            GrpcBatchServerKeyValueResponse response = new()
            {
                Type = GrpcServerBatchType.ServerBeginOperation,
                RequestId = request.RequestId,
                BeginOperation = beginOperationResponse
            };

            await responseWriter.WriteAsync(response);
        }
        catch (Exception ex)
        {
            await ObserveFault(responseWriter, request, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task CompleteOperationDelayed(
        CoalescingResponseWriter responseWriter,
        GrpcBatchServerKeyValueRequest request,
        ServerCallContext context,
        StreamDrain drain
    )
    {
        try
        {
            GrpcCompleteOperationResponse completeOperationResponse = await service.CompleteOperationInternal(request.CompleteOperation, context);

            GrpcBatchServerKeyValueResponse response = new()
            {
                Type = GrpcServerBatchType.ServerCompleteOperation,
                RequestId = request.RequestId,
                CompleteOperation = completeOperationResponse
            };

            await responseWriter.WriteAsync(response);
        }
        catch (Exception ex)
        {
            await ObserveFault(responseWriter, request, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task GetTransactionWorkingSetDelayed(
        CoalescingResponseWriter responseWriter,
        GrpcBatchServerKeyValueRequest request,
        ServerCallContext context,
        StreamDrain drain
    )
    {
        try
        {
            GrpcGetTransactionWorkingSetResponse getWorkingSetResponse = await service.GetTransactionWorkingSetInternal(request.GetTransactionWorkingSet, context);

            GrpcBatchServerKeyValueResponse response = new()
            {
                Type = GrpcServerBatchType.ServerGetTransactionWorkingSet,
                RequestId = request.RequestId,
                GetTransactionWorkingSet = getWorkingSetResponse
            };

            await responseWriter.WriteAsync(response);
        }
        catch (Exception ex)
        {
            await ObserveFault(responseWriter, request, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task CloseTransactionDelayed(
        CoalescingResponseWriter responseWriter,
        GrpcBatchServerKeyValueRequest request,
        ServerCallContext context,
        StreamDrain drain
    )
    {
        try
        {
            GrpcCloseTransactionResponse closeTransactionResponse = await service.CloseTransactionInternal(request.CloseTransaction, context);

            GrpcBatchServerKeyValueResponse response = new()
            {
                Type = GrpcServerBatchType.ServerCloseTransaction,
                RequestId = request.RequestId,
                CloseTransaction = closeTransactionResponse
            };

            await responseWriter.WriteAsync(response);
        }
        catch (Exception ex)
        {
            await ObserveFault(responseWriter, request, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task AcquireSnapshotHoldDelayed(
        CoalescingResponseWriter responseWriter,
        GrpcBatchServerKeyValueRequest request,
        ServerCallContext context,
        StreamDrain drain
    )
    {
        try
        {
            GrpcAcquireSnapshotHoldResponse holdResponse = await service.AcquireSnapshotHoldInternal(request.AcquireSnapshotHold, context);

            GrpcBatchServerKeyValueResponse response = new()
            {
                Type                = GrpcServerBatchType.ServerTryAcquireSnapshotHold,
                RequestId           = request.RequestId,
                AcquireSnapshotHold = holdResponse
            };

            await responseWriter.WriteAsync(response);
        }
        catch (Exception ex)
        {
            await ObserveFault(responseWriter, request, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task RenewSnapshotHoldDelayed(
        CoalescingResponseWriter responseWriter,
        GrpcBatchServerKeyValueRequest request,
        ServerCallContext context,
        StreamDrain drain
    )
    {
        try
        {
            GrpcRenewSnapshotHoldResponse holdResponse = await service.RenewSnapshotHoldInternal(request.RenewSnapshotHold, context);

            GrpcBatchServerKeyValueResponse response = new()
            {
                Type              = GrpcServerBatchType.ServerTryRenewSnapshotHold,
                RequestId         = request.RequestId,
                RenewSnapshotHold = holdResponse
            };

            await responseWriter.WriteAsync(response);
        }
        catch (Exception ex)
        {
            await ObserveFault(responseWriter, request, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task ReleaseSnapshotHoldDelayed(
        CoalescingResponseWriter responseWriter,
        GrpcBatchServerKeyValueRequest request,
        ServerCallContext context,
        StreamDrain drain
    )
    {
        try
        {
            GrpcReleaseSnapshotHoldResponse holdResponse = await service.ReleaseSnapshotHoldInternal(request.ReleaseSnapshotHold, context);

            GrpcBatchServerKeyValueResponse response = new()
            {
                Type                = GrpcServerBatchType.ServerTryReleaseSnapshotHold,
                RequestId           = request.RequestId,
                ReleaseSnapshotHold = holdResponse
            };

            await responseWriter.WriteAsync(response);
        }
        catch (Exception ex)
        {
            await ObserveFault(responseWriter, request, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task GetSnapshotFloorDelayed(
        CoalescingResponseWriter responseWriter,
        GrpcBatchServerKeyValueRequest request,
        ServerCallContext context,
        StreamDrain drain
    )
    {
        try
        {
            GrpcGetSnapshotFloorResponse floorResponse = await service.GetSnapshotFloorInternal(request.GetSnapshotFloor, context);

            GrpcBatchServerKeyValueResponse response = new()
            {
                Type             = GrpcServerBatchType.ServerTryGetSnapshotFloor,
                RequestId        = request.RequestId,
                GetSnapshotFloor = floorResponse
            };

            await responseWriter.WriteAsync(response);
        }
        catch (Exception ex)
        {
            await ObserveFault(responseWriter, request, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    /// <summary>
    /// Serializes response writes for one inter-node stream: gRPC allows only one write at a
    /// time, and handlers complete out of order. It is a write gate only — never hold a write
    /// across a service call. Responses that queued up while a write was in flight leave
    /// together in one coalesced carrier, under the item and byte caps; a response with an idle
    /// writer leaves alone and immediately. Every caller awaits its own response's write
    /// completion, so the stream drain still guarantees all responses are written before the
    /// call returns.
    /// </summary>
    internal sealed class CoalescingResponseWriter
    {
        /// <summary>
        /// Largest number of responses one coalesced carrier may bundle. Mutable only so tests
        /// can shrink it.
        /// </summary>
        internal static int MaxResponsesPerCoalescedEnvelope = 64;

        /// <summary>
        /// Upper bound on the serialized bytes one coalesced carrier may bundle. The client runs
        /// the gRPC default 4 MB receive limit, so the bound keeps a bundle far from it; a single
        /// response larger than the bound still travels, alone, exactly as it does today.
        /// Mutable only so tests can shrink it.
        /// </summary>
        internal static long MaxCoalescedResponseBytes = 1024L * 1024;

        private readonly IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream;

        private readonly CancellationToken cancellationToken;

        private readonly ConcurrentQueue<PendingResponse> pending = new();

        private int writing;

        private readonly record struct PendingResponse(GrpcBatchServerKeyValueResponse Response, TaskCompletionSource Written);

        public CoalescingResponseWriter(IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream, CancellationToken cancellationToken = default)
        {
            this.responseStream = responseStream;
            this.cancellationToken = cancellationToken;
        }

        /// <summary>
        /// Queues one response and returns when it is written to the stream. Whichever caller
        /// claims the writer flushes on behalf of everyone queued behind it; the rest await
        /// their completion sources.
        /// </summary>
        public async Task WriteAsync(GrpcBatchServerKeyValueResponse response)
        {
            // The caller of a cancelled call is gone: refusing here keeps a dead stream from
            // receiving writes, exactly as the cancellable gate acquisition used to.
            cancellationToken.ThrowIfCancellationRequested();

            TaskCompletionSource written = new(TaskCreationOptions.RunContinuationsAsynchronously);
            pending.Enqueue(new(response, written));

            if (Interlocked.Exchange(ref writing, 1) == 0)
            {
                try
                {
                    await Flush().ConfigureAwait(false);
                }
                catch
                {
                    // The flush already faulted every parked completion source, this caller's
                    // included; the await below surfaces the same failure to this caller.
                }
            }

            await written.Task.ConfigureAwait(false);
        }

        /// <summary>
        /// Drains the queue while this caller owns the writer. The release-and-recheck at the
        /// bottom closes the race with a producer that enqueued after the drain saw an empty
        /// queue but before the ownership flag was released: without it that response would sit
        /// unwritten until the next unrelated response arrived.
        /// </summary>
        private async Task Flush()
        {
            while (true)
            {
                while (pending.TryDequeue(out PendingResponse first))
                {
                    GrpcBatchServerKeyValueResponse toWrite = first.Response;
                    List<TaskCompletionSource>? bundled = null;

                    if (!pending.IsEmpty)
                    {
                        GrpcBatchServerKeyValueResponse? carrier = null;
                        long bytes = toWrite.CalculateSize();
                        int count = 1;

                        // Only this flusher dequeues, so peek-then-dequeue cannot lose a race.
                        while (count < MaxResponsesPerCoalescedEnvelope && pending.TryPeek(out PendingResponse next))
                        {
                            long nextBytes = next.Response.CalculateSize();

                            if (bytes + nextBytes > MaxCoalescedResponseBytes)
                                break;

                            pending.TryDequeue(out next);

                            if (carrier is null)
                            {
                                carrier = new() { Type = GrpcServerBatchType.ServerCoalesced };
                                carrier.Coalesced.Add(first.Response);
                                bundled = [first.Written];
                            }

                            carrier.Coalesced.Add(next.Response);
                            bundled!.Add(next.Written);
                            bytes += nextBytes;
                            count++;
                        }

                        if (carrier is not null)
                            toWrite = carrier;
                    }

                    try
                    {
                        cancellationToken.ThrowIfCancellationRequested();
                        await responseStream.WriteAsync(toWrite).ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        if (bundled is not null)
                        {
                            foreach (TaskCompletionSource waiter in bundled)
                                waiter.TrySetException(ex);
                        }
                        else
                            first.Written.TrySetException(ex);

                        // A parked response behind a dead stream can never be written; its
                        // caller must observe the failure instead of waiting forever.
                        while (pending.TryDequeue(out PendingResponse orphan))
                            orphan.Written.TrySetException(ex);

                        Interlocked.Exchange(ref writing, 0);
                        throw;
                    }

                    if (bundled is not null)
                    {
                        foreach (TaskCompletionSource waiter in bundled)
                            waiter.TrySetResult();
                    }
                    else
                        first.Written.TrySetResult();
                }

                Interlocked.Exchange(ref writing, 0);

                if (pending.IsEmpty || Interlocked.Exchange(ref writing, 1) == 1)
                    return;
            }
        }
    }
}
