
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
        int inFlight = 1;
        TaskCompletionSource drain = new(TaskCreationOptions.RunContinuationsAsynchronously);

        void Track(Task task)
        {
            Interlocked.Increment(ref inFlight);
            _ = task.ContinueWith(completed =>
            {
                if (completed.IsFaulted)
                {
                    Exception? e = completed.Exception?.InnerException;
                    if (e is IOException or OperationCanceledException)
                        logger.LogCommunicationIoException(e);
                    else
                        logger.LogError(completed.Exception, "Batch key-value server handler faulted");
                }
                if (Interlocked.Decrement(ref inFlight) == 0)
                    drain.TrySetResult();
            }, TaskScheduler.Default);
        }

        using SemaphoreSlim semaphore = new(1, 1);

        try
        {
            await foreach (GrpcBatchServerKeyValueRequest request in requestStream.ReadAllAsync())
            {
                switch (request.Type)
                {
                    case GrpcServerBatchType.ServerTrySetKeyValue:
                    {
                        GrpcTrySetKeyValueRequest? setKeyRequest = request.TrySetKeyValue;

                        Track(TrySetKeyValueServerDelayed(semaphore, request.RequestId, setKeyRequest, responseStream, context));
                    }
                    break;
                    
                    case GrpcServerBatchType.ServerTrySetManyKeyValue:
                    {
                        GrpcTrySetManyKeyValueRequest? setKeyRequest = request.TrySetManyKeyValue;

                        Track(TrySetManyKeyValueServerDelayed(semaphore, request.RequestId, setKeyRequest, responseStream, context));
                    }
                    break;

                    case GrpcServerBatchType.ServerTryDeleteManyKeyValue:
                    {
                        GrpcTryDeleteManyKeyValueRequest? deleteManyKeyRequest = request.TryDeleteManyKeyValue;

                        Track(TryDeleteManyKeyValueServerDelayed(semaphore, request.RequestId, deleteManyKeyRequest, responseStream, context));
                    }
                    break;

                    case GrpcServerBatchType.ServerTryGetKeyValue:
                    {
                        GrpcTryGetKeyValueRequest? getKeyRequest = request.TryGetKeyValue;

                        Track(TryGetKeyValueServerDelayed(semaphore, request.RequestId, getKeyRequest, responseStream, context));
                    }
                    break;

                    case GrpcServerBatchType.ServerTryGetManyValues:
                    {
                        GrpcTryGetManyValuesRequest? getManyValuesRequest = request.TryGetManyValues;

                        Track(TryGetManyValuesDelayed(semaphore, request.RequestId, getManyValuesRequest, responseStream, context));
                    }
                    break;

                    case GrpcServerBatchType.ServerTryDeleteKeyValue:
                    {
                        GrpcTryDeleteKeyValueRequest? deleteKeyRequest = request.TryDeleteKeyValue;

                        Track(TryDeleteKeyValueServerDelayed(semaphore, request.RequestId, deleteKeyRequest, responseStream, context));
                    }
                    break;

                    case GrpcServerBatchType.ServerTryExtendKeyValue:
                    {
                        GrpcTryExtendKeyValueRequest? extendKeyRequest = request.TryExtendKeyValue;

                        Track(TryExtendKeyValueServerDelayed(semaphore, request.RequestId, extendKeyRequest, responseStream, context));
                    }
                    break;

                    case GrpcServerBatchType.ServerTryExistsKeyValue:
                    {
                        GrpcTryExistsKeyValueRequest? extendKeyRequest = request.TryExistsKeyValue;

                        Track(TryExistsKeyValueServerDelayed(semaphore, request.RequestId, extendKeyRequest, responseStream, context));
                    }
                    break;

                    case GrpcServerBatchType.ServerTryExistsManyValues:
                    {
                        GrpcTryExistsManyValuesRequest? existsManyValuesRequest = request.TryExistsManyValues;

                        Track(TryExistsManyValuesDelayed(semaphore, request.RequestId, existsManyValuesRequest, responseStream, context));
                    }
                    break;

                    case GrpcServerBatchType.ServerTryCheckWriteIntent:
                    {
                        GrpcTryCheckWriteIntentRequest? checkWriteIntentRequest = request.TryCheckWriteIntent;

                        Track(TryCheckWriteIntentServerDelayed(semaphore, request.RequestId, checkWriteIntentRequest, responseStream, context));
                    }
                    break;

                    case GrpcServerBatchType.ServerTryExecuteTransactionScript:
                    {
                        GrpcTryExecuteTransactionScriptRequest? tryExecuteTransactionScriptRequest = request.TryExecuteTransactionScript;

                        Track(TryExecuteTransactionServerDelayed(semaphore, request.RequestId, tryExecuteTransactionScriptRequest, responseStream, context));
                    }
                    break;
                    
                    case GrpcServerBatchType.ServerTryAcquireExclusiveLock:
                    {
                        GrpcTryAcquireExclusiveLockRequest? tryAcquireExclusiveLockRequest = request.TryAcquireExclusiveLock;

                        Track(TryAcquireExclusiveLockDelayed(semaphore, request.RequestId, tryAcquireExclusiveLockRequest, responseStream, context));
                    }
                    break;
                    
                    case GrpcServerBatchType.ServerTryAcquireExclusivePrefixLock:
                    {
                        GrpcTryAcquireExclusivePrefixLockRequest? tryAcquireExclusivePrefixLockRequest = request.TryAcquireExclusivePrefixLock;

                        Track(TryAcquireExclusivePrefixLockDelayed(semaphore, request.RequestId, tryAcquireExclusivePrefixLockRequest, responseStream, context));
                    }
                    break;
                    
                    case GrpcServerBatchType.ServerTryAcquireManyExclusiveLocks:
                    {
                        GrpcTryAcquireManyExclusiveLocksRequest? tryAcquireManyExclusiveLocksRequest = request.TryAcquireManyExclusiveLocks;

                        Track(TryAcquireManyExclusiveLocksDelayed(semaphore, request.RequestId, tryAcquireManyExclusiveLocksRequest, responseStream, context));
                    }
                    break;
                    
                    case GrpcServerBatchType.ServerTryReleaseExclusiveLock:
                    {
                        GrpcTryReleaseExclusiveLockRequest? tryReleaseExclusiveLockRequest = request.TryReleaseExclusiveLock;

                        Track(TryReleaseExclusiveLockDelayed(semaphore, request.RequestId, tryReleaseExclusiveLockRequest, responseStream, context));
                    }
                    break;
                    
                    case GrpcServerBatchType.ServerTryReleaseExclusivePrefixLock:
                    {
                        GrpcTryReleaseExclusivePrefixLockRequest? tryReleaseExclusivePrefixLockRequest = request.TryReleaseExclusivePrefixLock;

                        Track(TryReleaseExclusivePrefixLockDelayed(semaphore, request.RequestId, tryReleaseExclusivePrefixLockRequest, responseStream, context));
                    }
                    break;

                    case GrpcServerBatchType.ServerTryAcquireExclusiveRangeLock:
                    {
                        GrpcTryAcquireExclusiveRangeLockRequest? req = request.TryAcquireExclusiveRangeLock;
                        Track(TryAcquireExclusiveRangeLockDelayed(semaphore, request.RequestId, req, responseStream, context));
                    }
                    break;

                    case GrpcServerBatchType.ServerTryReleaseExclusiveRangeLock:
                    {
                        GrpcTryReleaseExclusiveRangeLockRequest? req = request.TryReleaseExclusiveRangeLock;
                        Track(TryReleaseExclusiveRangeLockDelayed(semaphore, request.RequestId, req, responseStream, context));
                    }
                    break;

                    case GrpcServerBatchType.ServerTryEnsureKeyRangeSeeded:
                    {
                        GrpcEnsureKeyRangeSeededRequest? req = request.EnsureKeyRangeSeeded;
                        Track(EnsureKeyRangeSeededDelayed(semaphore, request.RequestId, req, responseStream, context));
                    }
                    break;

                    case GrpcServerBatchType.ServerTryEnsureKeyRangeRemoved:
                    {
                        GrpcEnsureKeyRangeRemovedRequest? req = request.EnsureKeyRangeRemoved;
                        Track(EnsureKeyRangeRemovedDelayed(semaphore, request.RequestId, req, responseStream, context));
                    }
                    break;

                    case GrpcServerBatchType.ServerTryGetRangeLocks:
                    {
                        GrpcGetRangeLocksRequest? req = request.GetRangeLocks;
                        Track(GetRangeLocksDelayed(semaphore, request.RequestId, req, responseStream, context));
                    }
                    break;

                    case GrpcServerBatchType.ServerTryImportRangeLocks:
                    {
                        GrpcImportRangeLocksRequest? req = request.ImportRangeLocks;
                        Track(ImportRangeLocksDelayed(semaphore, request.RequestId, req, responseStream, context));
                    }
                    break;

                    case GrpcServerBatchType.ServerImportCompletionReceipts:
                    {
                        GrpcImportCompletionReceiptsRequest? req = request.ImportCompletionReceipts;
                        Track(ImportCompletionReceiptsDelayed(semaphore, request.RequestId, req, responseStream, context));
                    }
                    break;

                    case GrpcServerBatchType.ServerImportCoordinatorDecisions:
                    {
                        GrpcImportCoordinatorDecisionsRequest? req = request.ImportCoordinatorDecisions;
                        Track(ImportCoordinatorDecisionsDelayed(semaphore, request.RequestId, req, responseStream, context));
                    }
                    break;

                    case GrpcServerBatchType.ServerTryReleaseManyExclusiveLocks:
                    {
                        GrpcTryReleaseManyExclusiveLocksRequest? tryReleaseManyExclusiveLocksRequest = request.TryReleaseManyExclusiveLocks;

                        Track(TryReleaseManyExclusiveLocksDelayed(semaphore, request.RequestId, tryReleaseManyExclusiveLocksRequest, responseStream, context));
                    }
                    break;
                    
                    case GrpcServerBatchType.ServerTryPrepareMutations:
                    {
                        GrpcTryPrepareMutationsRequest? tryPrepareMutationsRequest = request.TryPrepareMutations;

                        Track(TryPrepareMutationsDelayed(semaphore, request.RequestId, tryPrepareMutationsRequest, responseStream, context));
                    }
                    break;
                    
                    case GrpcServerBatchType.ServerTryPrepareManyMutations:
                    {
                        GrpcTryPrepareManyMutationsRequest? tryPrepareManyMutationsRequest = request.TryPrepareManyMutations;

                        Track(TryPrepareManyMutationsDelayed(semaphore, request.RequestId, tryPrepareManyMutationsRequest, responseStream, context));
                    }
                    break;
                    
                    case GrpcServerBatchType.ServerTryCommitMutations:
                    {
                        GrpcTryCommitMutationsRequest? tryCommitMutationsRequest = request.TryCommitMutations;

                        Track(TryCommitMutationsDelayed(semaphore, request.RequestId, tryCommitMutationsRequest, responseStream, context));
                    }
                    break;
                    
                    case GrpcServerBatchType.ServerTryCommitManyMutations:
                    {
                        GrpcTryCommitManyMutationsRequest? tryCommitManyMutationsRequest = request.TryCommitManyMutations;

                        Track(TryCommitManyMutationsDelayed(semaphore, request.RequestId, tryCommitManyMutationsRequest, responseStream, context));
                    }
                    break;
                    
                    case GrpcServerBatchType.ServerTryRollbackMutations:
                    {
                        GrpcTryRollbackMutationsRequest? tryRollbackMutationsRequest = request.TryRollbackMutations;

                        Track(TryRollbackMutationsDelayed(semaphore, request.RequestId, tryRollbackMutationsRequest, responseStream, context));
                    }
                    break;
                    
                    case GrpcServerBatchType.ServerTryRollbackManyMutations:
                    {
                        GrpcTryRollbackManyMutationsRequest? tryRollbackManyMutationsRequest = request.TryRollbackManyMutations;

                        Track(TryRollbackManyMutationsDelayed(semaphore, request.RequestId, tryRollbackManyMutationsRequest, responseStream, context));
                    }
                    break;
                    
                    case GrpcServerBatchType.ServerTryGetByBucket:
                    {
                        GrpcGetByBucketRequest? tryGetByBucketRequest = request.GetByBucket;

                        Track(GetByBucketDelayed(semaphore, request.RequestId, tryGetByBucketRequest, responseStream, context));
                    }
                    break;

                    case GrpcServerBatchType.ServerTryGetByRange:
                    {
                        GrpcGetByRangeRequest? getByRangeRequest = request.GetByRange;

                        Track(GetByRangeDelayed(semaphore, request.RequestId, getByRangeRequest, responseStream, context));
                    }
                    break;
                    
                    case GrpcServerBatchType.ServerTryScanByPrefix:
                    {
                        GrpcScanByPrefixRequest? tryScanByPrefixRequest = request.ScanByPrefix;

                        Track(ScanByPrefixDelayed(semaphore, request.RequestId, tryScanByPrefixRequest, responseStream, context));
                    }
                    break;
                    
                    case GrpcServerBatchType.ServerTryStartTransaction:
                    {
                        GrpcStartTransactionRequest? startTransactionRequest = request.StartTransaction;

                        Track(StartTransactionDelayed(semaphore, request.RequestId, startTransactionRequest, responseStream, context));
                    }
                    break;
                    
                    case GrpcServerBatchType.ServerTryCommitTransaction:
                    {
                        GrpcCommitTransactionRequest? commitTransactionRequest = request.CommitTransaction;

                        Track(CommitTransactionDelayed(semaphore, request.RequestId, commitTransactionRequest, responseStream, context));
                    }
                    break;
                    
                    case GrpcServerBatchType.ServerTryRollbackTransaction:
                    {
                        GrpcRollbackTransactionRequest? rollbackTransactionRequest = request.RollbackTransaction;

                        Track(RollbackTransactionDelayed(semaphore, request.RequestId, rollbackTransactionRequest, responseStream, context));
                    }
                    break;

                    case GrpcServerBatchType.ServerBeginOperation:
                    {
                        GrpcBeginOperationRequest? beginOperationRequest = request.BeginOperation;

                        Track(BeginOperationDelayed(semaphore, request.RequestId, beginOperationRequest, responseStream, context));
                    }
                    break;

                    case GrpcServerBatchType.ServerCompleteOperation:
                    {
                        GrpcCompleteOperationRequest? completeOperationRequest = request.CompleteOperation;

                        Track(CompleteOperationDelayed(semaphore, request.RequestId, completeOperationRequest, responseStream, context));
                    }
                    break;

                    case GrpcServerBatchType.ServerGetTransactionWorkingSet:
                    {
                        GrpcGetTransactionWorkingSetRequest? getWorkingSetRequest = request.GetTransactionWorkingSet;

                        Track(GetTransactionWorkingSetDelayed(semaphore, request.RequestId, getWorkingSetRequest, responseStream, context));
                    }
                    break;

                    case GrpcServerBatchType.ServerCloseTransaction:
                    {
                        GrpcCloseTransactionRequest? closeTransactionRequest = request.CloseTransaction;

                        Track(CloseTransactionDelayed(semaphore, request.RequestId, closeTransactionRequest, responseStream, context));
                    }
                    break;

                    case GrpcServerBatchType.ServerTryAcquireSnapshotHold:
                    {
                        GrpcAcquireSnapshotHoldRequest? req = request.AcquireSnapshotHold;
                        Track(AcquireSnapshotHoldDelayed(semaphore, request.RequestId, req, responseStream, context));
                    }
                    break;

                    case GrpcServerBatchType.ServerTryRenewSnapshotHold:
                    {
                        GrpcRenewSnapshotHoldRequest? req = request.RenewSnapshotHold;
                        Track(RenewSnapshotHoldDelayed(semaphore, request.RequestId, req, responseStream, context));
                    }
                    break;

                    case GrpcServerBatchType.ServerTryReleaseSnapshotHold:
                    {
                        GrpcReleaseSnapshotHoldRequest? req = request.ReleaseSnapshotHold;
                        Track(ReleaseSnapshotHoldDelayed(semaphore, request.RequestId, req, responseStream, context));
                    }
                    break;

                    case GrpcServerBatchType.ServerTryGetSnapshotFloor:
                    {
                        GrpcGetSnapshotFloorRequest? req = request.GetSnapshotFloor;
                        Track(GetSnapshotFloorDelayed(semaphore, request.RequestId, req, responseStream, context));
                    }
                    break;

                    case GrpcServerBatchType.ServerTypeNone:
                    default:
                        logger.LogError("Unknown batch Server request type: {Type}", request.Type);
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
            if (Interlocked.Decrement(ref inFlight) == 0) drain.TrySetResult();
            await drain.Task;
        }
    }

    private async Task TrySetKeyValueServerDelayed(
        SemaphoreSlim semaphore, 
        int requestId, 
        GrpcTrySetKeyValueRequest setKeyRequest, 
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
        ServerCallContext context
    )
    {
        GrpcTrySetKeyValueResponse trySetResponse = await service.TrySetKeyValueInternal(setKeyRequest, context);
        
        GrpcBatchServerKeyValueResponse response = new()
        {
            Type = GrpcServerBatchType.ServerTrySetKeyValue,
            RequestId = requestId,
            TrySetKeyValue = trySetResponse
        };

        await WriteResponseToStream(semaphore, responseStream, response, context);
    }
    
    private async Task TrySetManyKeyValueServerDelayed(
        SemaphoreSlim semaphore, 
        int requestId, 
        GrpcTrySetManyKeyValueRequest setKeyRequest, 
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
        ServerCallContext context
    )
    {
        GrpcTrySetManyKeyValueResponse trySetManyResponse = await service.TrySetManyKeyValueInternal(setKeyRequest, context);
        
        GrpcBatchServerKeyValueResponse response = new()
        {
            Type = GrpcServerBatchType.ServerTrySetManyKeyValue,
            RequestId = requestId,
            TrySetManyKeyValue = trySetManyResponse
        };

        await WriteResponseToStream(semaphore, responseStream, response, context);
    }

    private async Task TryDeleteManyKeyValueServerDelayed(
        SemaphoreSlim semaphore,
        int requestId,
        GrpcTryDeleteManyKeyValueRequest deleteManyKeyRequest,
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
        ServerCallContext context
    )
    {
        GrpcTryDeleteManyKeyValueResponse tryDeleteManyResponse = await service.TryDeleteManyKeyValueInternal(deleteManyKeyRequest, context);

        GrpcBatchServerKeyValueResponse response = new()
        {
            Type = GrpcServerBatchType.ServerTryDeleteManyKeyValue,
            RequestId = requestId,
            TryDeleteManyKeyValue = tryDeleteManyResponse
        };

        await WriteResponseToStream(semaphore, responseStream, response, context);
    }

    private async Task TryGetKeyValueServerDelayed(
        SemaphoreSlim semaphore, 
        int requestId, 
        GrpcTryGetKeyValueRequest getKeyRequest, 
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
        ServerCallContext context
    )
    {
        GrpcTryGetKeyValueResponse tryGetResponse = await service.TryGetKeyValueInternal(getKeyRequest, context);
        
        GrpcBatchServerKeyValueResponse response = new()
        {
            Type = GrpcServerBatchType.ServerTryGetKeyValue,
            RequestId = requestId,
            TryGetKeyValue = tryGetResponse
        };

        await WriteResponseToStream(semaphore, responseStream, response, context);
    }

    private async Task TryGetManyValuesDelayed(
        SemaphoreSlim semaphore,
        int requestId,
        GrpcTryGetManyValuesRequest getManyValuesRequest,
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
        ServerCallContext context
    )
    {
        GrpcTryGetManyValuesResponse tryGetManyResponse = await service.TryGetManyValuesInternal(getManyValuesRequest, context);

        GrpcBatchServerKeyValueResponse response = new()
        {
            Type = GrpcServerBatchType.ServerTryGetManyValues,
            RequestId = requestId,
            TryGetManyValues = tryGetManyResponse
        };

        await WriteResponseToStream(semaphore, responseStream, response, context);
    }

    private async Task TryDeleteKeyValueServerDelayed(
        SemaphoreSlim semaphore, 
        int requestId, 
        GrpcTryDeleteKeyValueRequest deleteKeyRequest, 
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
        ServerCallContext context
    )
    {
        GrpcTryDeleteKeyValueResponse tryDeleteResponse = await service.TryDeleteKeyValueInternal(deleteKeyRequest, context);
        
        GrpcBatchServerKeyValueResponse response = new()
        {
            Type = GrpcServerBatchType.ServerTryDeleteKeyValue,
            RequestId = requestId,
            TryDeleteKeyValue = tryDeleteResponse
        };

        await WriteResponseToStream(semaphore, responseStream, response, context);
    }

    private async Task TryExtendKeyValueServerDelayed(
        SemaphoreSlim semaphore, 
        int requestId, 
        GrpcTryExtendKeyValueRequest extendKeyRequest, 
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
        ServerCallContext context
    )
    {
        GrpcTryExtendKeyValueResponse tryExtendResponse = await service.TryExtendKeyValueInternal(extendKeyRequest, context);
        
        GrpcBatchServerKeyValueResponse response = new()
        {
            Type = GrpcServerBatchType.ServerTryExtendKeyValue,
            RequestId = requestId,
            TryExtendKeyValue = tryExtendResponse
        };

        await WriteResponseToStream(semaphore, responseStream, response, context);
    }

    private async Task TryExistsKeyValueServerDelayed(
        SemaphoreSlim semaphore, 
        int requestId, 
        GrpcTryExistsKeyValueRequest existKeyRequest, 
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
        ServerCallContext context
    )
    {
        GrpcTryExistsKeyValueResponse tryExistsResponse = await service.TryExistsKeyValueInternal(existKeyRequest, context);
        
        GrpcBatchServerKeyValueResponse response = new()
        {
            Type = GrpcServerBatchType.ServerTryExistsKeyValue,
            RequestId = requestId,
            TryExistsKeyValue = tryExistsResponse
        };

        await WriteResponseToStream(semaphore, responseStream, response, context);
    }

    private async Task TryExistsManyValuesDelayed(
        SemaphoreSlim semaphore,
        int requestId,
        GrpcTryExistsManyValuesRequest existsManyValuesRequest,
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
        ServerCallContext context
    )
    {
        GrpcTryExistsManyValuesResponse tryExistsManyResponse = await service.TryExistsManyValuesInternal(existsManyValuesRequest, context);

        GrpcBatchServerKeyValueResponse response = new()
        {
            Type = GrpcServerBatchType.ServerTryExistsManyValues,
            RequestId = requestId,
            TryExistsManyValues = tryExistsManyResponse
        };

        await WriteResponseToStream(semaphore, responseStream, response, context);
    }

    private async Task TryCheckWriteIntentServerDelayed(
        SemaphoreSlim semaphore,
        int requestId,
        GrpcTryCheckWriteIntentRequest checkWriteIntentRequest,
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
        ServerCallContext context
    )
    {
        GrpcTryCheckWriteIntentResponse tryCheckWriteIntentResponse = await service.TryCheckWriteIntentInternal(checkWriteIntentRequest, context);

        GrpcBatchServerKeyValueResponse response = new()
        {
            Type = GrpcServerBatchType.ServerTryCheckWriteIntent,
            RequestId = requestId,
            TryCheckWriteIntent = tryCheckWriteIntentResponse
        };

        await WriteResponseToStream(semaphore, responseStream, response, context);
    }

    private async Task TryExecuteTransactionServerDelayed(
        SemaphoreSlim semaphore,
        int requestId,
        GrpcTryExecuteTransactionScriptRequest tryExecuteTransactionRequest,
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
        ServerCallContext context
    )
    {
        GrpcTryExecuteTransactionScriptResponse tryExecuteTransactionScriptResponse = await service.TryExecuteTransactionScriptInternal(tryExecuteTransactionRequest, context);
        
        GrpcBatchServerKeyValueResponse response = new()
        {
            Type = GrpcServerBatchType.ServerTryExecuteTransactionScript,
            RequestId = requestId,
            TryExecuteTransactionScript = tryExecuteTransactionScriptResponse
        };

        await WriteResponseToStream(semaphore, responseStream, response, context);
    }
    
    private async Task TryAcquireExclusiveLockDelayed(
        SemaphoreSlim semaphore, 
        int requestId, 
        GrpcTryAcquireExclusiveLockRequest tryExecuteTransactionRequest, 
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
        ServerCallContext context
    )
    {
        GrpcTryAcquireExclusiveLockResponse tryExecuteTransactionResponse = await service.TryAcquireExclusiveLockInternal(tryExecuteTransactionRequest, context);
        
        GrpcBatchServerKeyValueResponse response = new()
        {
            Type = GrpcServerBatchType.ServerTryAcquireExclusiveLock,
            RequestId = requestId,
            TryAcquireExclusiveLock = tryExecuteTransactionResponse
        };

        await WriteResponseToStream(semaphore, responseStream, response, context);
    }
    
    private async Task TryAcquireExclusivePrefixLockDelayed(
        SemaphoreSlim semaphore, 
        int requestId, 
        GrpcTryAcquireExclusivePrefixLockRequest tryExecuteTransactionRequest, 
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
        ServerCallContext context
    )
    {
        GrpcTryAcquireExclusivePrefixLockResponse tryExecuteTransactionResponse = await service.TryAcquireExclusivePrefixLockInternal(tryExecuteTransactionRequest, context);
        
        GrpcBatchServerKeyValueResponse response = new()
        {
            Type = GrpcServerBatchType.ServerTryAcquireExclusivePrefixLock,
            RequestId = requestId,
            TryAcquireExclusivePrefixLock = tryExecuteTransactionResponse
        };

        await WriteResponseToStream(semaphore, responseStream, response, context);
    }
    
    private async Task TryAcquireManyExclusiveLocksDelayed(
        SemaphoreSlim semaphore, 
        int requestId, 
        GrpcTryAcquireManyExclusiveLocksRequest tryAcquireManyExclusiveLocksnRequest, 
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
        ServerCallContext context
    )
    {
        GrpcTryAcquireManyExclusiveLocksResponse tryAcquireManyExclusiveLocksResponse = await service.TryAcquireManyExclusiveLocksInternal(tryAcquireManyExclusiveLocksnRequest, context);
        
        GrpcBatchServerKeyValueResponse response = new()
        {
            Type = GrpcServerBatchType.ServerTryAcquireManyExclusiveLocks,
            RequestId = requestId,
            TryAcquireManyExclusiveLocks = tryAcquireManyExclusiveLocksResponse
        };

        await WriteResponseToStream(semaphore, responseStream, response, context);
    }
    
    private async Task TryReleaseExclusiveLockDelayed(
        SemaphoreSlim semaphore, 
        int requestId, 
        GrpcTryReleaseExclusiveLockRequest tryReleaseExclusiveLockRequest, 
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
        ServerCallContext context
    )
    {
        GrpcTryReleaseExclusiveLockResponse tryReleaseExclusiveLockResponse = await service.TryReleaseExclusiveLockInternal(tryReleaseExclusiveLockRequest, context);
        
        GrpcBatchServerKeyValueResponse response = new()
        {
            Type = GrpcServerBatchType.ServerTryReleaseExclusiveLock,
            RequestId = requestId,
            TryReleaseExclusiveLock = tryReleaseExclusiveLockResponse
        };

        await WriteResponseToStream(semaphore, responseStream, response, context);
    }
    
    private async Task TryReleaseExclusivePrefixLockDelayed(
        SemaphoreSlim semaphore, 
        int requestId, 
        GrpcTryReleaseExclusivePrefixLockRequest tryReleaseExclusivePrefixLockRequest, 
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
        ServerCallContext context
    )
    {
        GrpcTryReleaseExclusivePrefixLockResponse tryReleaseExclusivePrefixLockResponse = await service.TryReleaseExclusivePrefixLockInternal(tryReleaseExclusivePrefixLockRequest, context);
        
        GrpcBatchServerKeyValueResponse response = new()
        {
            Type = GrpcServerBatchType.ServerTryReleaseExclusivePrefixLock,
            RequestId = requestId,
            TryReleaseExclusivePrefixLock = tryReleaseExclusivePrefixLockResponse
        };

        await WriteResponseToStream(semaphore, responseStream, response, context);
    }
    
    private async Task TryAcquireExclusiveRangeLockDelayed(
        SemaphoreSlim semaphore,
        int requestId,
        GrpcTryAcquireExclusiveRangeLockRequest request,
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
        ServerCallContext context
    )
    {
        GrpcTryAcquireExclusiveRangeLockResponse resp = await service.TryAcquireExclusiveRangeLockInternal(request, context);

        GrpcBatchServerKeyValueResponse response = new()
        {
            Type = GrpcServerBatchType.ServerTryAcquireExclusiveRangeLock,
            RequestId = requestId,
            TryAcquireExclusiveRangeLock = resp
        };

        await WriteResponseToStream(semaphore, responseStream, response, context);
    }

    private async Task TryReleaseExclusiveRangeLockDelayed(
        SemaphoreSlim semaphore,
        int requestId,
        GrpcTryReleaseExclusiveRangeLockRequest request,
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
        ServerCallContext context
    )
    {
        GrpcTryReleaseExclusiveRangeLockResponse resp = await service.TryReleaseExclusiveRangeLockInternal(request, context);

        GrpcBatchServerKeyValueResponse response = new()
        {
            Type = GrpcServerBatchType.ServerTryReleaseExclusiveRangeLock,
            RequestId = requestId,
            TryReleaseExclusiveRangeLock = resp
        };

        await WriteResponseToStream(semaphore, responseStream, response, context);
    }

    private async Task EnsureKeyRangeSeededDelayed(
        SemaphoreSlim semaphore,
        int requestId,
        GrpcEnsureKeyRangeSeededRequest request,
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
        ServerCallContext context)
    {
        await semaphore.WaitAsync(context.CancellationToken);
        try
        {
            GrpcEnsureKeyRangeSeededResponse resp = await service.EnsureKeyRangeSeededInternal(request, context);
            await responseStream.WriteAsync(new GrpcBatchServerKeyValueResponse
            {
                Type = GrpcServerBatchType.ServerTryEnsureKeyRangeSeeded,
                RequestId = requestId,
                EnsureKeyRangeSeeded = resp
            });
        }
        finally
        {
            semaphore.Release();
        }
    }

    private async Task EnsureKeyRangeRemovedDelayed(
        SemaphoreSlim semaphore,
        int requestId,
        GrpcEnsureKeyRangeRemovedRequest request,
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
        ServerCallContext context)
    {
        await semaphore.WaitAsync(context.CancellationToken);
        try
        {
            GrpcEnsureKeyRangeRemovedResponse resp = await service.EnsureKeyRangeRemovedInternal(request, context);
            await responseStream.WriteAsync(new GrpcBatchServerKeyValueResponse
            {
                Type = GrpcServerBatchType.ServerTryEnsureKeyRangeRemoved,
                RequestId = requestId,
                EnsureKeyRangeRemoved = resp
            });
        }
        finally
        {
            semaphore.Release();
        }
    }

    private async Task GetRangeLocksDelayed(
        SemaphoreSlim semaphore,
        int requestId,
        GrpcGetRangeLocksRequest request,
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
        ServerCallContext context)
    {
        await semaphore.WaitAsync(context.CancellationToken);
        try
        {
            GrpcGetRangeLocksResponse resp = await service.GetRangeLocksInternal(request, context);
            await responseStream.WriteAsync(new GrpcBatchServerKeyValueResponse
            {
                Type = GrpcServerBatchType.ServerTryGetRangeLocks,
                RequestId = requestId,
                GetRangeLocks = resp
            });
        }
        finally
        {
            semaphore.Release();
        }
    }

    private async Task ImportRangeLocksDelayed(
        SemaphoreSlim semaphore,
        int requestId,
        GrpcImportRangeLocksRequest request,
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
        ServerCallContext context)
    {
        await semaphore.WaitAsync(context.CancellationToken);
        try
        {
            GrpcImportRangeLocksResponse resp = await service.ImportRangeLocksInternal(request, context);
            await responseStream.WriteAsync(new GrpcBatchServerKeyValueResponse
            {
                Type = GrpcServerBatchType.ServerTryImportRangeLocks,
                RequestId = requestId,
                ImportRangeLocks = resp
            });
        }
        finally
        {
            semaphore.Release();
        }
    }

    private async Task ImportCompletionReceiptsDelayed(
        SemaphoreSlim semaphore,
        int requestId,
        GrpcImportCompletionReceiptsRequest request,
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
        ServerCallContext context)
    {
        await semaphore.WaitAsync(context.CancellationToken);
        try
        {
            GrpcImportCompletionReceiptsResponse resp = await service.ImportCompletionReceiptsInternal(request, context);
            await responseStream.WriteAsync(new GrpcBatchServerKeyValueResponse
            {
                Type = GrpcServerBatchType.ServerImportCompletionReceipts,
                RequestId = requestId,
                ImportCompletionReceipts = resp
            });
        }
        finally
        {
            semaphore.Release();
        }
    }

    private async Task ImportCoordinatorDecisionsDelayed(
        SemaphoreSlim semaphore,
        int requestId,
        GrpcImportCoordinatorDecisionsRequest request,
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
        ServerCallContext context)
    {
        await semaphore.WaitAsync(context.CancellationToken);
        try
        {
            GrpcImportCoordinatorDecisionsResponse resp = await service.ImportCoordinatorDecisionsInternal(request, context);
            await responseStream.WriteAsync(new GrpcBatchServerKeyValueResponse
            {
                Type = GrpcServerBatchType.ServerImportCoordinatorDecisions,
                RequestId = requestId,
                ImportCoordinatorDecisions = resp
            });
        }
        finally
        {
            semaphore.Release();
        }
    }

    private async Task TryReleaseManyExclusiveLocksDelayed(
        SemaphoreSlim semaphore,
        int requestId,
        GrpcTryReleaseManyExclusiveLocksRequest tryReleaseManyExclusiveLocksnRequest,
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
        ServerCallContext context
    )
    {
        GrpcTryReleaseManyExclusiveLocksResponse tryReleaseManyExclusiveLocksResponse = await service.TryReleaseManyExclusiveLocksInternal(tryReleaseManyExclusiveLocksnRequest, context);
        
        GrpcBatchServerKeyValueResponse response = new()
        {
            Type = GrpcServerBatchType.ServerTryReleaseManyExclusiveLocks,
            RequestId = requestId,
            TryReleaseManyExclusiveLocks = tryReleaseManyExclusiveLocksResponse
        };

        await WriteResponseToStream(semaphore, responseStream, response, context);
    }
    
    private async Task TryPrepareMutationsDelayed(
        SemaphoreSlim semaphore, 
        int requestId, 
        GrpcTryPrepareMutationsRequest tryPrepareMutationsRequest, 
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
        ServerCallContext context
    )
    {
        GrpcTryPrepareMutationsResponse tryPrepareMutationsResponse = await service.TryPrepareMutationsInternal(tryPrepareMutationsRequest, context);
        
        GrpcBatchServerKeyValueResponse response = new()
        {
            Type = GrpcServerBatchType.ServerTryPrepareMutations,
            RequestId = requestId,
            TryPrepareMutations = tryPrepareMutationsResponse
        };

        await WriteResponseToStream(semaphore, responseStream, response, context);
    }
    
    private async Task TryPrepareManyMutationsDelayed(
        SemaphoreSlim semaphore, 
        int requestId, 
        GrpcTryPrepareManyMutationsRequest tryPrepareManyMutationsRequest, 
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
        ServerCallContext context
    )
    {
        GrpcTryPrepareManyMutationsResponse tryPrepareManyMutationsResponse = await service.TryPrepareManyMutationsInternal(tryPrepareManyMutationsRequest, context);
        
        GrpcBatchServerKeyValueResponse response = new()
        {
            Type = GrpcServerBatchType.ServerTryPrepareManyMutations,
            RequestId = requestId,
            TryPrepareManyMutations = tryPrepareManyMutationsResponse
        };

        await WriteResponseToStream(semaphore, responseStream, response, context);
    }
    
    private async Task TryCommitMutationsDelayed(
        SemaphoreSlim semaphore, 
        int requestId, 
        GrpcTryCommitMutationsRequest tryCommitMutationsRequest, 
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
        ServerCallContext context
    )
    {
        GrpcTryCommitMutationsResponse tryCommitMutationsResponse = await service.TryCommitMutationsInternal(tryCommitMutationsRequest, context);
        
        GrpcBatchServerKeyValueResponse response = new()
        {
            Type = GrpcServerBatchType.ServerTryCommitMutations,
            RequestId = requestId,
            TryCommitMutations = tryCommitMutationsResponse
        };

        await WriteResponseToStream(semaphore, responseStream, response, context);
    }
    
    private async Task TryCommitManyMutationsDelayed(
        SemaphoreSlim semaphore, 
        int requestId, 
        GrpcTryCommitManyMutationsRequest tryCommitManyMutationsRequest, 
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
        ServerCallContext context
    )
    {
        GrpcTryCommitManyMutationsResponse tryCommitManyMutationsResponse = await service.TryCommitManyMutationsInternal(tryCommitManyMutationsRequest, context);
        
        GrpcBatchServerKeyValueResponse response = new()
        {
            Type = GrpcServerBatchType.ServerTryCommitManyMutations,
            RequestId = requestId,
            TryCommitManyMutations = tryCommitManyMutationsResponse
        };

        await WriteResponseToStream(semaphore, responseStream, response, context);
    }
    
    private async Task TryRollbackMutationsDelayed(
        SemaphoreSlim semaphore, 
        int requestId, 
        GrpcTryRollbackMutationsRequest tryRollbackMutationsRequest, 
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
        ServerCallContext context
    )
    {
        GrpcTryRollbackMutationsResponse tryRollbackMutationsResponse = await service.TryRollbackMutationsInternal(tryRollbackMutationsRequest, context);
        
        GrpcBatchServerKeyValueResponse response = new()
        {
            Type = GrpcServerBatchType.ServerTryRollbackMutations,
            RequestId = requestId,
            TryRollbackMutations = tryRollbackMutationsResponse
        };

        await WriteResponseToStream(semaphore, responseStream, response, context);
    }
    
    private async Task TryRollbackManyMutationsDelayed(
        SemaphoreSlim semaphore, 
        int requestId, 
        GrpcTryRollbackManyMutationsRequest tryRollbackManyMutationsRequest, 
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
        ServerCallContext context
    )
    {
        GrpcTryRollbackManyMutationsResponse tryRollbackManyMutationsResponse = await service.TryRollbackManyMutationsInternal(tryRollbackManyMutationsRequest, context);
        
        GrpcBatchServerKeyValueResponse response = new()
        {
            Type = GrpcServerBatchType.ServerTryRollbackManyMutations,
            RequestId = requestId,
            TryRollbackManyMutations = tryRollbackManyMutationsResponse
        };

        await WriteResponseToStream(semaphore, responseStream, response, context);
    }
    
    private async Task GetByBucketDelayed(
        SemaphoreSlim semaphore, 
        int requestId, 
        GrpcGetByBucketRequest GetByBucketRequest, 
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
        ServerCallContext context
    )
    {
        GrpcGetByBucketResponse GetByBucketResponse = await service.GetByBucketInternal(GetByBucketRequest, context);
        
        GrpcBatchServerKeyValueResponse response = new()
        {
            Type = GrpcServerBatchType.ServerTryGetByBucket,
            RequestId = requestId,
            GetByBucket = GetByBucketResponse
        };

        await WriteResponseToStream(semaphore, responseStream, response, context);
    }
    
    private async Task GetByRangeDelayed(
        SemaphoreSlim semaphore,
        int requestId,
        GrpcGetByRangeRequest getByRangeRequest,
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
        ServerCallContext context
    )
    {
        GrpcGetByRangeResponse getByRangeResponse = await service.GetByRangeInternal(getByRangeRequest, context);

        GrpcBatchServerKeyValueResponse response = new()
        {
            Type = GrpcServerBatchType.ServerTryGetByRange,
            RequestId = requestId,
            GetByRange = getByRangeResponse
        };

        await WriteResponseToStream(semaphore, responseStream, response, context);
    }

    private async Task ScanByPrefixDelayed(
        SemaphoreSlim semaphore, 
        int requestId, 
        GrpcScanByPrefixRequest scanByPrefixRequest, 
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
        ServerCallContext context
    )
    {
        GrpcScanByPrefixResponse scanByPrefixResponse = await service.ScanByPrefixInternal(scanByPrefixRequest, context);
        
        GrpcBatchServerKeyValueResponse response = new()
        {
            Type = GrpcServerBatchType.ServerTryScanByPrefix,
            RequestId = requestId,
            ScanByPrefix = scanByPrefixResponse
        };

        await WriteResponseToStream(semaphore, responseStream, response, context);
    }
       
    private async Task StartTransactionDelayed(
        SemaphoreSlim semaphore, 
        int requestId, 
        GrpcStartTransactionRequest startTransactionRequest, 
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
        ServerCallContext context
    )
    {
        GrpcStartTransactionResponse startTransactionResponse = await service.StartTransactionInternal(startTransactionRequest, context);
        
        GrpcBatchServerKeyValueResponse response = new()
        {
            Type = GrpcServerBatchType.ServerTryStartTransaction,
            RequestId = requestId,
            StartTransaction = startTransactionResponse
        };

        await WriteResponseToStream(semaphore, responseStream, response, context);
    }
    
    private async Task CommitTransactionDelayed(
        SemaphoreSlim semaphore, 
        int requestId, 
        GrpcCommitTransactionRequest commitTransactionRequest, 
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
        ServerCallContext context
    )
    {
        GrpcCommitTransactionResponse commitTransactionResponse = await service.CommitTransactionInternal(commitTransactionRequest, context);
        
        GrpcBatchServerKeyValueResponse response = new()
        {
            Type = GrpcServerBatchType.ServerTryCommitTransaction,
            RequestId = requestId,
            CommitTransaction = commitTransactionResponse
        };

        await WriteResponseToStream(semaphore, responseStream, response, context);
    }
    
    private async Task RollbackTransactionDelayed(
        SemaphoreSlim semaphore, 
        int requestId, 
        GrpcRollbackTransactionRequest rollbackTransactionRequest, 
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
        ServerCallContext context
    )
    {
        GrpcRollbackTransactionResponse rollbackTransactionResponse = await service.RollbackTransactionInternal(rollbackTransactionRequest, context);

        GrpcBatchServerKeyValueResponse response = new()
        {
            Type = GrpcServerBatchType.ServerTryRollbackTransaction,
            RequestId = requestId,
            RollbackTransaction = rollbackTransactionResponse
        };

        await WriteResponseToStream(semaphore, responseStream, response, context);
    }

    private async Task BeginOperationDelayed(
        SemaphoreSlim semaphore,
        int requestId,
        GrpcBeginOperationRequest beginOperationRequest,
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
        ServerCallContext context
    )
    {
        GrpcBeginOperationResponse beginOperationResponse = await service.BeginOperationInternal(beginOperationRequest, context);

        GrpcBatchServerKeyValueResponse response = new()
        {
            Type = GrpcServerBatchType.ServerBeginOperation,
            RequestId = requestId,
            BeginOperation = beginOperationResponse
        };

        await WriteResponseToStream(semaphore, responseStream, response, context);
    }

    private async Task CompleteOperationDelayed(
        SemaphoreSlim semaphore,
        int requestId,
        GrpcCompleteOperationRequest completeOperationRequest,
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
        ServerCallContext context
    )
    {
        GrpcCompleteOperationResponse completeOperationResponse = await service.CompleteOperationInternal(completeOperationRequest, context);

        GrpcBatchServerKeyValueResponse response = new()
        {
            Type = GrpcServerBatchType.ServerCompleteOperation,
            RequestId = requestId,
            CompleteOperation = completeOperationResponse
        };

        await WriteResponseToStream(semaphore, responseStream, response, context);
    }

    private async Task GetTransactionWorkingSetDelayed(
        SemaphoreSlim semaphore,
        int requestId,
        GrpcGetTransactionWorkingSetRequest getWorkingSetRequest,
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
        ServerCallContext context
    )
    {
        GrpcGetTransactionWorkingSetResponse getWorkingSetResponse = await service.GetTransactionWorkingSetInternal(getWorkingSetRequest, context);

        GrpcBatchServerKeyValueResponse response = new()
        {
            Type = GrpcServerBatchType.ServerGetTransactionWorkingSet,
            RequestId = requestId,
            GetTransactionWorkingSet = getWorkingSetResponse
        };

        await WriteResponseToStream(semaphore, responseStream, response, context);
    }

    private async Task CloseTransactionDelayed(
        SemaphoreSlim semaphore,
        int requestId,
        GrpcCloseTransactionRequest closeTransactionRequest,
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
        ServerCallContext context
    )
    {
        GrpcCloseTransactionResponse closeTransactionResponse = await service.CloseTransactionInternal(closeTransactionRequest, context);

        GrpcBatchServerKeyValueResponse response = new()
        {
            Type = GrpcServerBatchType.ServerCloseTransaction,
            RequestId = requestId,
            CloseTransaction = closeTransactionResponse
        };

        await WriteResponseToStream(semaphore, responseStream, response, context);
    }

    private async Task AcquireSnapshotHoldDelayed(
        SemaphoreSlim semaphore,
        int requestId,
        GrpcAcquireSnapshotHoldRequest request,
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
        ServerCallContext context
    )
    {
        GrpcAcquireSnapshotHoldResponse holdResponse = await service.AcquireSnapshotHoldInternal(request, context);

        GrpcBatchServerKeyValueResponse response = new()
        {
            Type                = GrpcServerBatchType.ServerTryAcquireSnapshotHold,
            RequestId           = requestId,
            AcquireSnapshotHold = holdResponse
        };

        await WriteResponseToStream(semaphore, responseStream, response, context);
    }

    private async Task RenewSnapshotHoldDelayed(
        SemaphoreSlim semaphore,
        int requestId,
        GrpcRenewSnapshotHoldRequest request,
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
        ServerCallContext context
    )
    {
        GrpcRenewSnapshotHoldResponse holdResponse = await service.RenewSnapshotHoldInternal(request, context);

        GrpcBatchServerKeyValueResponse response = new()
        {
            Type              = GrpcServerBatchType.ServerTryRenewSnapshotHold,
            RequestId         = requestId,
            RenewSnapshotHold = holdResponse
        };

        await WriteResponseToStream(semaphore, responseStream, response, context);
    }

    private async Task ReleaseSnapshotHoldDelayed(
        SemaphoreSlim semaphore,
        int requestId,
        GrpcReleaseSnapshotHoldRequest request,
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
        ServerCallContext context
    )
    {
        GrpcReleaseSnapshotHoldResponse holdResponse = await service.ReleaseSnapshotHoldInternal(request, context);

        GrpcBatchServerKeyValueResponse response = new()
        {
            Type                = GrpcServerBatchType.ServerTryReleaseSnapshotHold,
            RequestId           = requestId,
            ReleaseSnapshotHold = holdResponse
        };

        await WriteResponseToStream(semaphore, responseStream, response, context);
    }

    private async Task GetSnapshotFloorDelayed(
        SemaphoreSlim semaphore,
        int requestId,
        GrpcGetSnapshotFloorRequest request,
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
        ServerCallContext context
    )
    {
        GrpcGetSnapshotFloorResponse floorResponse = await service.GetSnapshotFloorInternal(request, context);

        GrpcBatchServerKeyValueResponse response = new()
        {
            Type             = GrpcServerBatchType.ServerTryGetSnapshotFloor,
            RequestId        = requestId,
            GetSnapshotFloor = floorResponse
        };

        await WriteResponseToStream(semaphore, responseStream, response, context);
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
