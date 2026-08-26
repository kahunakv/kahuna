
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
        // This stream only ever carries requests forwarded by another Kahuna node, so every handler
        // dispatched from it serves under the forwarded-request marker: a non-hosting receiver
        // answers MustRetry instead of forwarding onward (replica-placement loop safety).
        using Kahuna.Server.ForwardedRequestScope.Scope forwardedScope = Kahuna.Server.ForwardedRequestScope.Enter();

        StreamDrain drain = new();

        using SemaphoreSlim semaphore = new(1, 1);

        try
        {
            await foreach (GrpcBatchServerKeyValueRequest request in requestStream.ReadAllAsync())
            {
                drain.Enter();

                switch (request.Type)
                {
                    case GrpcServerBatchType.ServerTrySetKeyValue:
                        _ = TrySetKeyValueServerDelayed(semaphore, request, responseStream, context, drain);
                        break;

                    case GrpcServerBatchType.ServerTrySetManyKeyValue:
                        _ = TrySetManyKeyValueServerDelayed(semaphore, request, responseStream, context, drain);
                        break;

                    case GrpcServerBatchType.ServerTryDeleteManyKeyValue:
                        _ = TryDeleteManyKeyValueServerDelayed(semaphore, request, responseStream, context, drain);
                        break;

                    case GrpcServerBatchType.ServerTryGetKeyValue:
                        _ = TryGetKeyValueServerDelayed(semaphore, request, responseStream, context, drain);
                        break;

                    case GrpcServerBatchType.ServerTryGetManyValues:
                        _ = TryGetManyValuesDelayed(semaphore, request, responseStream, context, drain);
                        break;

                    case GrpcServerBatchType.ServerTryDeleteKeyValue:
                        _ = TryDeleteKeyValueServerDelayed(semaphore, request, responseStream, context, drain);
                        break;

                    case GrpcServerBatchType.ServerTryExtendKeyValue:
                        _ = TryExtendKeyValueServerDelayed(semaphore, request, responseStream, context, drain);
                        break;

                    case GrpcServerBatchType.ServerTryExistsKeyValue:
                        _ = TryExistsKeyValueServerDelayed(semaphore, request, responseStream, context, drain);
                        break;

                    case GrpcServerBatchType.ServerTryExistsManyValues:
                        _ = TryExistsManyValuesDelayed(semaphore, request, responseStream, context, drain);
                        break;

                    case GrpcServerBatchType.ServerTryCheckWriteIntent:
                        _ = TryCheckWriteIntentServerDelayed(semaphore, request, responseStream, context, drain);
                        break;

                    case GrpcServerBatchType.ServerTryCheckManyWriteIntents:
                        _ = TryCheckManyWriteIntentsServerDelayed(semaphore, request, responseStream, context, drain);
                        break;

                    case GrpcServerBatchType.ServerTryExecuteTransactionScript:
                        _ = TryExecuteTransactionServerDelayed(semaphore, request, responseStream, context, drain);
                        break;

                    case GrpcServerBatchType.ServerTryAcquireExclusiveLock:
                        _ = TryAcquireExclusiveLockDelayed(semaphore, request, responseStream, context, drain);
                        break;

                    case GrpcServerBatchType.ServerTryAcquireExclusivePrefixLock:
                        _ = TryAcquireExclusivePrefixLockDelayed(semaphore, request, responseStream, context, drain);
                        break;

                    case GrpcServerBatchType.ServerTryAcquireManyExclusiveLocks:
                        _ = TryAcquireManyExclusiveLocksDelayed(semaphore, request, responseStream, context, drain);
                        break;

                    case GrpcServerBatchType.ServerTryReleaseExclusiveLock:
                        _ = TryReleaseExclusiveLockDelayed(semaphore, request, responseStream, context, drain);
                        break;

                    case GrpcServerBatchType.ServerTryReleaseExclusivePrefixLock:
                        _ = TryReleaseExclusivePrefixLockDelayed(semaphore, request, responseStream, context, drain);
                        break;

                    case GrpcServerBatchType.ServerTryAcquireExclusiveRangeLock:
                        _ = TryAcquireExclusiveRangeLockDelayed(semaphore, request, responseStream, context, drain);
                        break;

                    case GrpcServerBatchType.ServerTryReleaseExclusiveRangeLock:
                        _ = TryReleaseExclusiveRangeLockDelayed(semaphore, request, responseStream, context, drain);
                        break;

                    case GrpcServerBatchType.ServerTryEnsureKeyRangeSeeded:
                        _ = EnsureKeyRangeSeededDelayed(semaphore, request, responseStream, context, drain);
                        break;

                    case GrpcServerBatchType.ServerTryEnsureKeyRangeRemoved:
                        _ = EnsureKeyRangeRemovedDelayed(semaphore, request, responseStream, context, drain);
                        break;

                    case GrpcServerBatchType.ServerTryGetRangeLocks:
                        _ = GetRangeLocksDelayed(semaphore, request, responseStream, context, drain);
                        break;

                    case GrpcServerBatchType.ServerTryImportRangeLocks:
                        _ = ImportRangeLocksDelayed(semaphore, request, responseStream, context, drain);
                        break;

                    case GrpcServerBatchType.ServerImportCompletionReceipts:
                        _ = ImportCompletionReceiptsDelayed(semaphore, request, responseStream, context, drain);
                        break;

                    case GrpcServerBatchType.ServerDurableOperation:
                        _ = DurableOperationDelayed(semaphore, request, responseStream, context, drain);
                        break;

                    case GrpcServerBatchType.ServerLookupTransactionRecord:
                        _ = LookupTransactionRecordDelayed(semaphore, request, responseStream, context, drain);
                        break;

                    case GrpcServerBatchType.ServerReplicateKeyValueRangePage:
                        _ = ReplicateKeyValueRangePageDelayed(semaphore, request, responseStream, context, drain);
                        break;

                    case GrpcServerBatchType.ServerGetRangeTransactionState:
                        _ = GetRangeTransactionStateDelayed(semaphore, request, responseStream, context, drain);
                        break;

                    case GrpcServerBatchType.ServerTryReleaseManyExclusiveLocks:
                        _ = TryReleaseManyExclusiveLocksDelayed(semaphore, request, responseStream, context, drain);
                        break;

                    case GrpcServerBatchType.ServerTryPrepareMutations:
                        _ = TryPrepareMutationsDelayed(semaphore, request, responseStream, context, drain);
                        break;

                    case GrpcServerBatchType.ServerTryPrepareManyMutations:
                        _ = TryPrepareManyMutationsDelayed(semaphore, request, responseStream, context, drain);
                        break;

                    case GrpcServerBatchType.ServerTryCommitMutations:
                        _ = TryCommitMutationsDelayed(semaphore, request, responseStream, context, drain);
                        break;

                    case GrpcServerBatchType.ServerTryCommitManyMutations:
                        _ = TryCommitManyMutationsDelayed(semaphore, request, responseStream, context, drain);
                        break;

                    case GrpcServerBatchType.ServerTryRollbackMutations:
                        _ = TryRollbackMutationsDelayed(semaphore, request, responseStream, context, drain);
                        break;

                    case GrpcServerBatchType.ServerTryRollbackManyMutations:
                        _ = TryRollbackManyMutationsDelayed(semaphore, request, responseStream, context, drain);
                        break;

                    case GrpcServerBatchType.ServerTryGetByBucket:
                        _ = GetByBucketDelayed(semaphore, request, responseStream, context, drain);
                        break;

                    case GrpcServerBatchType.ServerTryGetByRange:
                        _ = GetByRangeDelayed(semaphore, request, responseStream, context, drain);
                        break;

                    case GrpcServerBatchType.ServerTryScanByPrefix:
                        _ = ScanByPrefixDelayed(semaphore, request, responseStream, context, drain);
                        break;

                    case GrpcServerBatchType.ServerTryStartTransaction:
                        _ = StartTransactionDelayed(semaphore, request, responseStream, context, drain);
                        break;

                    case GrpcServerBatchType.ServerTryCommitTransaction:
                        _ = CommitTransactionDelayed(semaphore, request, responseStream, context, drain);
                        break;

                    case GrpcServerBatchType.ServerTryRollbackTransaction:
                        _ = RollbackTransactionDelayed(semaphore, request, responseStream, context, drain);
                        break;

                    case GrpcServerBatchType.ServerBeginOperation:
                        _ = BeginOperationDelayed(semaphore, request, responseStream, context, drain);
                        break;

                    case GrpcServerBatchType.ServerCompleteOperation:
                        _ = CompleteOperationDelayed(semaphore, request, responseStream, context, drain);
                        break;

                    case GrpcServerBatchType.ServerGetTransactionWorkingSet:
                        _ = GetTransactionWorkingSetDelayed(semaphore, request, responseStream, context, drain);
                        break;

                    case GrpcServerBatchType.ServerCloseTransaction:
                        _ = CloseTransactionDelayed(semaphore, request, responseStream, context, drain);
                        break;

                    case GrpcServerBatchType.ServerTryAcquireSnapshotHold:
                        _ = AcquireSnapshotHoldDelayed(semaphore, request, responseStream, context, drain);
                        break;

                    case GrpcServerBatchType.ServerTryRenewSnapshotHold:
                        _ = RenewSnapshotHoldDelayed(semaphore, request, responseStream, context, drain);
                        break;

                    case GrpcServerBatchType.ServerTryReleaseSnapshotHold:
                        _ = ReleaseSnapshotHoldDelayed(semaphore, request, responseStream, context, drain);
                        break;

                    case GrpcServerBatchType.ServerTryGetSnapshotFloor:
                        _ = GetSnapshotFloorDelayed(semaphore, request, responseStream, context, drain);
                        break;

                    case GrpcServerBatchType.ServerTypeNone:
                    default:
                        logger.LogError("Unknown batch Server request type: {Type}", request.Type);
                        drain.Exit();
                        break;
                }
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
    /// A handler that throws must still answer its own RequestId: the caller matches responses by
    /// id, so an unanswered request hangs until its deadline while every other request on the
    /// shared stream keeps flowing. Refusing just that one request with MustRetry leaves the
    /// stream and its neighbours untouched.
    /// </summary>
    private async Task ObserveFault(
        SemaphoreSlim semaphore,
        GrpcBatchServerKeyValueRequest request,
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
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
            await WriteResponseToStream(semaphore, responseStream, BatchRefusalResponses.ForServerKeyValue(request), context);
        }
        catch (Exception writeEx)
        {
            logger.LogCommunicationIoException(writeEx);
        }
    }

    private async Task TrySetKeyValueServerDelayed(
        SemaphoreSlim semaphore,
        GrpcBatchServerKeyValueRequest request,
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
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

            await WriteResponseToStream(semaphore, responseStream, response, context);
        }
        catch (Exception ex)
        {
            await ObserveFault(semaphore, request, responseStream, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task TrySetManyKeyValueServerDelayed(
        SemaphoreSlim semaphore,
        GrpcBatchServerKeyValueRequest request,
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
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

            await WriteResponseToStream(semaphore, responseStream, response, context);
        }
        catch (Exception ex)
        {
            await ObserveFault(semaphore, request, responseStream, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task TryDeleteManyKeyValueServerDelayed(
        SemaphoreSlim semaphore,
        GrpcBatchServerKeyValueRequest request,
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
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

            await WriteResponseToStream(semaphore, responseStream, response, context);
        }
        catch (Exception ex)
        {
            await ObserveFault(semaphore, request, responseStream, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task TryGetKeyValueServerDelayed(
        SemaphoreSlim semaphore,
        GrpcBatchServerKeyValueRequest request,
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
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

            await WriteResponseToStream(semaphore, responseStream, response, context);
        }
        catch (Exception ex)
        {
            await ObserveFault(semaphore, request, responseStream, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task TryGetManyValuesDelayed(
        SemaphoreSlim semaphore,
        GrpcBatchServerKeyValueRequest request,
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
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

            await WriteResponseToStream(semaphore, responseStream, response, context);
        }
        catch (Exception ex)
        {
            await ObserveFault(semaphore, request, responseStream, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task TryDeleteKeyValueServerDelayed(
        SemaphoreSlim semaphore,
        GrpcBatchServerKeyValueRequest request,
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
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

            await WriteResponseToStream(semaphore, responseStream, response, context);
        }
        catch (Exception ex)
        {
            await ObserveFault(semaphore, request, responseStream, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task TryExtendKeyValueServerDelayed(
        SemaphoreSlim semaphore,
        GrpcBatchServerKeyValueRequest request,
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
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

            await WriteResponseToStream(semaphore, responseStream, response, context);
        }
        catch (Exception ex)
        {
            await ObserveFault(semaphore, request, responseStream, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task TryExistsKeyValueServerDelayed(
        SemaphoreSlim semaphore,
        GrpcBatchServerKeyValueRequest request,
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
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

            await WriteResponseToStream(semaphore, responseStream, response, context);
        }
        catch (Exception ex)
        {
            await ObserveFault(semaphore, request, responseStream, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task TryExistsManyValuesDelayed(
        SemaphoreSlim semaphore,
        GrpcBatchServerKeyValueRequest request,
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
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

            await WriteResponseToStream(semaphore, responseStream, response, context);
        }
        catch (Exception ex)
        {
            await ObserveFault(semaphore, request, responseStream, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task TryCheckWriteIntentServerDelayed(
        SemaphoreSlim semaphore,
        GrpcBatchServerKeyValueRequest request,
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
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

            await WriteResponseToStream(semaphore, responseStream, response, context);
        }
        catch (Exception ex)
        {
            await ObserveFault(semaphore, request, responseStream, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task TryCheckManyWriteIntentsServerDelayed(
        SemaphoreSlim semaphore,
        GrpcBatchServerKeyValueRequest request,
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
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

            await WriteResponseToStream(semaphore, responseStream, response, context);
        }
        catch (Exception ex)
        {
            await ObserveFault(semaphore, request, responseStream, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task TryExecuteTransactionServerDelayed(
        SemaphoreSlim semaphore,
        GrpcBatchServerKeyValueRequest request,
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
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

            await WriteResponseToStream(semaphore, responseStream, response, context);
        }
        catch (Exception ex)
        {
            await ObserveFault(semaphore, request, responseStream, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task TryAcquireExclusiveLockDelayed(
        SemaphoreSlim semaphore,
        GrpcBatchServerKeyValueRequest request,
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
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

            await WriteResponseToStream(semaphore, responseStream, response, context);
        }
        catch (Exception ex)
        {
            await ObserveFault(semaphore, request, responseStream, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task TryAcquireExclusivePrefixLockDelayed(
        SemaphoreSlim semaphore,
        GrpcBatchServerKeyValueRequest request,
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
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

            await WriteResponseToStream(semaphore, responseStream, response, context);
        }
        catch (Exception ex)
        {
            await ObserveFault(semaphore, request, responseStream, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task TryAcquireManyExclusiveLocksDelayed(
        SemaphoreSlim semaphore,
        GrpcBatchServerKeyValueRequest request,
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
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

            await WriteResponseToStream(semaphore, responseStream, response, context);
        }
        catch (Exception ex)
        {
            await ObserveFault(semaphore, request, responseStream, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task TryReleaseExclusiveLockDelayed(
        SemaphoreSlim semaphore,
        GrpcBatchServerKeyValueRequest request,
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
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

            await WriteResponseToStream(semaphore, responseStream, response, context);
        }
        catch (Exception ex)
        {
            await ObserveFault(semaphore, request, responseStream, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task TryReleaseExclusivePrefixLockDelayed(
        SemaphoreSlim semaphore,
        GrpcBatchServerKeyValueRequest request,
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
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

            await WriteResponseToStream(semaphore, responseStream, response, context);
        }
        catch (Exception ex)
        {
            await ObserveFault(semaphore, request, responseStream, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task TryAcquireExclusiveRangeLockDelayed(
        SemaphoreSlim semaphore,
        GrpcBatchServerKeyValueRequest request,
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
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

            await WriteResponseToStream(semaphore, responseStream, response, context);
        }
        catch (Exception ex)
        {
            await ObserveFault(semaphore, request, responseStream, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task TryReleaseExclusiveRangeLockDelayed(
        SemaphoreSlim semaphore,
        GrpcBatchServerKeyValueRequest request,
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
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

            await WriteResponseToStream(semaphore, responseStream, response, context);
        }
        catch (Exception ex)
        {
            await ObserveFault(semaphore, request, responseStream, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    // The handlers below hold the write semaphore across the whole service call, not just the
    // response write, so these placement/replication operations execute serialized per stream in
    // arrival order. Keep that shape: their effects are order-sensitive (seed before import,
    // import before receipts).

    private async Task EnsureKeyRangeSeededDelayed(
        SemaphoreSlim semaphore,
        GrpcBatchServerKeyValueRequest request,
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
        ServerCallContext context,
        StreamDrain drain
    )
    {
        try
        {
            await semaphore.WaitAsync(context.CancellationToken);
            try
            {
                GrpcEnsureKeyRangeSeededResponse resp = await service.EnsureKeyRangeSeededInternal(request.EnsureKeyRangeSeeded, context);
                await responseStream.WriteAsync(new GrpcBatchServerKeyValueResponse
                {
                    Type = GrpcServerBatchType.ServerTryEnsureKeyRangeSeeded,
                    RequestId = request.RequestId,
                    EnsureKeyRangeSeeded = resp
                });
            }
            finally
            {
                semaphore.Release();
            }
        }
        catch (Exception ex)
        {
            await ObserveFault(semaphore, request, responseStream, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task EnsureKeyRangeRemovedDelayed(
        SemaphoreSlim semaphore,
        GrpcBatchServerKeyValueRequest request,
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
        ServerCallContext context,
        StreamDrain drain
    )
    {
        try
        {
            await semaphore.WaitAsync(context.CancellationToken);
            try
            {
                GrpcEnsureKeyRangeRemovedResponse resp = await service.EnsureKeyRangeRemovedInternal(request.EnsureKeyRangeRemoved, context);
                await responseStream.WriteAsync(new GrpcBatchServerKeyValueResponse
                {
                    Type = GrpcServerBatchType.ServerTryEnsureKeyRangeRemoved,
                    RequestId = request.RequestId,
                    EnsureKeyRangeRemoved = resp
                });
            }
            finally
            {
                semaphore.Release();
            }
        }
        catch (Exception ex)
        {
            await ObserveFault(semaphore, request, responseStream, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task GetRangeLocksDelayed(
        SemaphoreSlim semaphore,
        GrpcBatchServerKeyValueRequest request,
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
        ServerCallContext context,
        StreamDrain drain
    )
    {
        try
        {
            await semaphore.WaitAsync(context.CancellationToken);
            try
            {
                GrpcGetRangeLocksResponse resp = await service.GetRangeLocksInternal(request.GetRangeLocks, context);
                await responseStream.WriteAsync(new GrpcBatchServerKeyValueResponse
                {
                    Type = GrpcServerBatchType.ServerTryGetRangeLocks,
                    RequestId = request.RequestId,
                    GetRangeLocks = resp
                });
            }
            finally
            {
                semaphore.Release();
            }
        }
        catch (Exception ex)
        {
            await ObserveFault(semaphore, request, responseStream, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task ImportRangeLocksDelayed(
        SemaphoreSlim semaphore,
        GrpcBatchServerKeyValueRequest request,
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
        ServerCallContext context,
        StreamDrain drain
    )
    {
        try
        {
            await semaphore.WaitAsync(context.CancellationToken);
            try
            {
                GrpcImportRangeLocksResponse resp = await service.ImportRangeLocksInternal(request.ImportRangeLocks, context);
                await responseStream.WriteAsync(new GrpcBatchServerKeyValueResponse
                {
                    Type = GrpcServerBatchType.ServerTryImportRangeLocks,
                    RequestId = request.RequestId,
                    ImportRangeLocks = resp
                });
            }
            finally
            {
                semaphore.Release();
            }
        }
        catch (Exception ex)
        {
            await ObserveFault(semaphore, request, responseStream, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task ImportCompletionReceiptsDelayed(
        SemaphoreSlim semaphore,
        GrpcBatchServerKeyValueRequest request,
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
        ServerCallContext context,
        StreamDrain drain
    )
    {
        try
        {
            await semaphore.WaitAsync(context.CancellationToken);
            try
            {
                GrpcImportCompletionReceiptsResponse resp = await service.ImportCompletionReceiptsInternal(request.ImportCompletionReceipts, context);
                await responseStream.WriteAsync(new GrpcBatchServerKeyValueResponse
                {
                    Type = GrpcServerBatchType.ServerImportCompletionReceipts,
                    RequestId = request.RequestId,
                    ImportCompletionReceipts = resp
                });
            }
            finally
            {
                semaphore.Release();
            }
        }
        catch (Exception ex)
        {
            await ObserveFault(semaphore, request, responseStream, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task DurableOperationDelayed(
        SemaphoreSlim semaphore,
        GrpcBatchServerKeyValueRequest request,
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
        ServerCallContext context,
        StreamDrain drain
    )
    {
        try
        {
            await semaphore.WaitAsync(context.CancellationToken);
            try
            {
                GrpcDurableOperationResponse resp = await service.DurableOperationInternal(request.DurableOperation, context);
                await responseStream.WriteAsync(new GrpcBatchServerKeyValueResponse
                {
                    Type = GrpcServerBatchType.ServerDurableOperation,
                    RequestId = request.RequestId,
                    DurableOperation = resp
                });
            }
            finally
            {
                semaphore.Release();
            }
        }
        catch (Exception ex)
        {
            await ObserveFault(semaphore, request, responseStream, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task ReplicateKeyValueRangePageDelayed(
        SemaphoreSlim semaphore,
        GrpcBatchServerKeyValueRequest request,
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
        ServerCallContext context,
        StreamDrain drain
    )
    {
        try
        {
            await semaphore.WaitAsync(context.CancellationToken);
            try
            {
                GrpcReplicateKeyValueRangePageResponse resp = await service.ReplicateKeyValueRangePageInternal(request.ReplicateKeyValueRangePage, context);
                await responseStream.WriteAsync(new GrpcBatchServerKeyValueResponse
                {
                    Type = GrpcServerBatchType.ServerReplicateKeyValueRangePage,
                    RequestId = request.RequestId,
                    ReplicateKeyValueRangePage = resp
                });
            }
            finally
            {
                semaphore.Release();
            }
        }
        catch (Exception ex)
        {
            await ObserveFault(semaphore, request, responseStream, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task GetRangeTransactionStateDelayed(
        SemaphoreSlim semaphore,
        GrpcBatchServerKeyValueRequest request,
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
        ServerCallContext context,
        StreamDrain drain
    )
    {
        try
        {
            await semaphore.WaitAsync(context.CancellationToken);
            try
            {
                GrpcGetRangeTransactionStateResponse resp = await service.GetRangeTransactionStateInternal(request.GetRangeTransactionState, context);
                await responseStream.WriteAsync(new GrpcBatchServerKeyValueResponse
                {
                    Type = GrpcServerBatchType.ServerGetRangeTransactionState,
                    RequestId = request.RequestId,
                    GetRangeTransactionState = resp
                });
            }
            finally
            {
                semaphore.Release();
            }
        }
        catch (Exception ex)
        {
            await ObserveFault(semaphore, request, responseStream, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task LookupTransactionRecordDelayed(
        SemaphoreSlim semaphore,
        GrpcBatchServerKeyValueRequest request,
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
        ServerCallContext context,
        StreamDrain drain
    )
    {
        try
        {
            await semaphore.WaitAsync(context.CancellationToken);
            try
            {
                GrpcLookupTransactionRecordResponse resp = await service.LookupTransactionRecordInternal(request.LookupTransactionRecord, context);
                await responseStream.WriteAsync(new GrpcBatchServerKeyValueResponse
                {
                    Type = GrpcServerBatchType.ServerLookupTransactionRecord,
                    RequestId = request.RequestId,
                    LookupTransactionRecord = resp
                });
            }
            finally
            {
                semaphore.Release();
            }
        }
        catch (Exception ex)
        {
            await ObserveFault(semaphore, request, responseStream, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task TryReleaseManyExclusiveLocksDelayed(
        SemaphoreSlim semaphore,
        GrpcBatchServerKeyValueRequest request,
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
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

            await WriteResponseToStream(semaphore, responseStream, response, context);
        }
        catch (Exception ex)
        {
            await ObserveFault(semaphore, request, responseStream, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task TryPrepareMutationsDelayed(
        SemaphoreSlim semaphore,
        GrpcBatchServerKeyValueRequest request,
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
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

            await WriteResponseToStream(semaphore, responseStream, response, context);
        }
        catch (Exception ex)
        {
            await ObserveFault(semaphore, request, responseStream, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task TryPrepareManyMutationsDelayed(
        SemaphoreSlim semaphore,
        GrpcBatchServerKeyValueRequest request,
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
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

            await WriteResponseToStream(semaphore, responseStream, response, context);
        }
        catch (Exception ex)
        {
            await ObserveFault(semaphore, request, responseStream, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task TryCommitMutationsDelayed(
        SemaphoreSlim semaphore,
        GrpcBatchServerKeyValueRequest request,
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
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

            await WriteResponseToStream(semaphore, responseStream, response, context);
        }
        catch (Exception ex)
        {
            await ObserveFault(semaphore, request, responseStream, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task TryCommitManyMutationsDelayed(
        SemaphoreSlim semaphore,
        GrpcBatchServerKeyValueRequest request,
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
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

            await WriteResponseToStream(semaphore, responseStream, response, context);
        }
        catch (Exception ex)
        {
            await ObserveFault(semaphore, request, responseStream, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task TryRollbackMutationsDelayed(
        SemaphoreSlim semaphore,
        GrpcBatchServerKeyValueRequest request,
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
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

            await WriteResponseToStream(semaphore, responseStream, response, context);
        }
        catch (Exception ex)
        {
            await ObserveFault(semaphore, request, responseStream, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task TryRollbackManyMutationsDelayed(
        SemaphoreSlim semaphore,
        GrpcBatchServerKeyValueRequest request,
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
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

            await WriteResponseToStream(semaphore, responseStream, response, context);
        }
        catch (Exception ex)
        {
            await ObserveFault(semaphore, request, responseStream, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task GetByBucketDelayed(
        SemaphoreSlim semaphore,
        GrpcBatchServerKeyValueRequest request,
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
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

            await WriteResponseToStream(semaphore, responseStream, response, context);
        }
        catch (Exception ex)
        {
            await ObserveFault(semaphore, request, responseStream, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task GetByRangeDelayed(
        SemaphoreSlim semaphore,
        GrpcBatchServerKeyValueRequest request,
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
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

            await WriteResponseToStream(semaphore, responseStream, response, context);
        }
        catch (Exception ex)
        {
            await ObserveFault(semaphore, request, responseStream, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task ScanByPrefixDelayed(
        SemaphoreSlim semaphore,
        GrpcBatchServerKeyValueRequest request,
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
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

            await WriteResponseToStream(semaphore, responseStream, response, context);
        }
        catch (Exception ex)
        {
            await ObserveFault(semaphore, request, responseStream, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task StartTransactionDelayed(
        SemaphoreSlim semaphore,
        GrpcBatchServerKeyValueRequest request,
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
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

            await WriteResponseToStream(semaphore, responseStream, response, context);
        }
        catch (Exception ex)
        {
            await ObserveFault(semaphore, request, responseStream, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task CommitTransactionDelayed(
        SemaphoreSlim semaphore,
        GrpcBatchServerKeyValueRequest request,
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
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

            await WriteResponseToStream(semaphore, responseStream, response, context);
        }
        catch (Exception ex)
        {
            await ObserveFault(semaphore, request, responseStream, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task RollbackTransactionDelayed(
        SemaphoreSlim semaphore,
        GrpcBatchServerKeyValueRequest request,
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
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

            await WriteResponseToStream(semaphore, responseStream, response, context);
        }
        catch (Exception ex)
        {
            await ObserveFault(semaphore, request, responseStream, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task BeginOperationDelayed(
        SemaphoreSlim semaphore,
        GrpcBatchServerKeyValueRequest request,
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
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

            await WriteResponseToStream(semaphore, responseStream, response, context);
        }
        catch (Exception ex)
        {
            await ObserveFault(semaphore, request, responseStream, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task CompleteOperationDelayed(
        SemaphoreSlim semaphore,
        GrpcBatchServerKeyValueRequest request,
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
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

            await WriteResponseToStream(semaphore, responseStream, response, context);
        }
        catch (Exception ex)
        {
            await ObserveFault(semaphore, request, responseStream, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task GetTransactionWorkingSetDelayed(
        SemaphoreSlim semaphore,
        GrpcBatchServerKeyValueRequest request,
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
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

            await WriteResponseToStream(semaphore, responseStream, response, context);
        }
        catch (Exception ex)
        {
            await ObserveFault(semaphore, request, responseStream, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task CloseTransactionDelayed(
        SemaphoreSlim semaphore,
        GrpcBatchServerKeyValueRequest request,
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
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

            await WriteResponseToStream(semaphore, responseStream, response, context);
        }
        catch (Exception ex)
        {
            await ObserveFault(semaphore, request, responseStream, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task AcquireSnapshotHoldDelayed(
        SemaphoreSlim semaphore,
        GrpcBatchServerKeyValueRequest request,
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
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

            await WriteResponseToStream(semaphore, responseStream, response, context);
        }
        catch (Exception ex)
        {
            await ObserveFault(semaphore, request, responseStream, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task RenewSnapshotHoldDelayed(
        SemaphoreSlim semaphore,
        GrpcBatchServerKeyValueRequest request,
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
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

            await WriteResponseToStream(semaphore, responseStream, response, context);
        }
        catch (Exception ex)
        {
            await ObserveFault(semaphore, request, responseStream, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task ReleaseSnapshotHoldDelayed(
        SemaphoreSlim semaphore,
        GrpcBatchServerKeyValueRequest request,
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
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

            await WriteResponseToStream(semaphore, responseStream, response, context);
        }
        catch (Exception ex)
        {
            await ObserveFault(semaphore, request, responseStream, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task GetSnapshotFloorDelayed(
        SemaphoreSlim semaphore,
        GrpcBatchServerKeyValueRequest request,
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
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

            await WriteResponseToStream(semaphore, responseStream, response, context);
        }
        catch (Exception ex)
        {
            await ObserveFault(semaphore, request, responseStream, context, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private static async Task WriteResponseToStream(
        SemaphoreSlim semaphore,
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
        GrpcBatchServerKeyValueResponse response,
        ServerCallContext context
    )
    {
        bool acquired = false;
        try
        {
            await semaphore.WaitAsync(context.CancellationToken);
            acquired = true;
            await responseStream.WriteAsync(response);
        }
        finally
        {
            if (acquired) semaphore.Release();
        }
    }
}
