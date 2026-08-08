
using Grpc.Core;
using Kahuna.Communication.External.Grpc.Logging;

namespace Kahuna.Communication.External.Grpc.KeyValues;

/// <summary>
/// Provides batching functionality for handling client key-value requests in a gRPC server environment.
/// </summary>
internal sealed class KeyValueClientBatcher
{
    private readonly KeyValuesService service;
           
    private readonly ILogger<IKahuna> logger;
    
    public KeyValueClientBatcher(KeyValuesService service, ILogger<IKahuna> logger)
    {
        this.service = service;        
        this.logger = logger;
    }
    
    public async Task BatchClientKeyValueRequests(
        IAsyncStreamReader<GrpcBatchClientKeyValueRequest> requestStream,
        IServerStreamWriter<GrpcBatchClientKeyValueResponse> responseStream, 
        ServerCallContext context
    )
    {
        int inFlight = 1;
        TaskCompletionSource drain = new(TaskCreationOptions.RunContinuationsAsynchronously);

        using SemaphoreSlim semaphore = new(1, 1);

        void Track(GrpcBatchClientKeyValueRequest request, Task task)
        {
            Interlocked.Increment(ref inFlight);
            _ = Observe(request, task);
        }

        // A handler that throws must still answer its own RequestId: the SDK matches responses by id,
        // so an unanswered request hangs until its deadline while every other request on the shared
        // stream keeps flowing. Refusing just that one request with MustRetry leaves the stream and
        // its neighbours untouched, and gives the SDK's retry loop something it can classify.
        async Task Observe(GrpcBatchClientKeyValueRequest request, Task task)
        {
            try
            {
                await task;
            }
            catch (Exception ex) when (ex is IOException or OperationCanceledException)
            {
                // The stream is already gone or the client left; there is nobody left to answer.
                logger.LogCommunicationIoException(ex);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Batch key-value client handler faulted");

                try
                {
                    await WriteResponseToStream(semaphore, responseStream, BatchRefusalResponses.ForClientKeyValue(request), context);
                }
                catch (Exception writeEx)
                {
                    logger.LogCommunicationIoException(writeEx);
                }
            }
            finally
            {
                if (Interlocked.Decrement(ref inFlight) == 0)
                    drain.TrySetResult();
            }
        }

        try
        {
            await foreach (GrpcBatchClientKeyValueRequest request in requestStream.ReadAllAsync())
            {
                switch (request.Type)
                {
                    case GrpcClientBatchType.TrySetKeyValue:
                    {
                        GrpcTrySetKeyValueRequest? setKeyRequest = request.TrySetKeyValue;

                        Track(request, TrySetKeyValueDelayed(semaphore, request.RequestId, setKeyRequest, responseStream, context));
                    }
                    break;
                    
                    case GrpcClientBatchType.TrySetManyKeyValue:
                    {
                        GrpcTrySetManyKeyValueRequest? setKeyRequest = request.TrySetManyKeyValue;

                        Track(request, TrySetManyKeyValueDelayed(semaphore, request.RequestId, setKeyRequest, responseStream, context));
                    }
                    break;

                    case GrpcClientBatchType.TryDeleteManyKeyValue:
                    {
                        GrpcTryDeleteManyKeyValueRequest? deleteManyKeyRequest = request.TryDeleteManyKeyValue;

                        Track(request, TryDeleteManyKeyValueDelayed(semaphore, request.RequestId, deleteManyKeyRequest, responseStream, context));
                    }
                    break;

                    case GrpcClientBatchType.TryGetKeyValue:
                    {
                        GrpcTryGetKeyValueRequest? getKeyRequest = request.TryGetKeyValue;

                        Track(request, TryGetKeyValueDelayed(semaphore, request.RequestId, getKeyRequest, responseStream, context));
                    }
                    break;

                    case GrpcClientBatchType.TryDeleteKeyValue:
                    {
                        GrpcTryDeleteKeyValueRequest? deleteKeyRequest = request.TryDeleteKeyValue;

                        Track(request, TryDeleteKeyValueDelayed(semaphore, request.RequestId, deleteKeyRequest, responseStream, context));
                    }
                    break;

                    case GrpcClientBatchType.TryExtendKeyValue:
                    {
                        GrpcTryExtendKeyValueRequest? extendKeyRequest = request.TryExtendKeyValue;

                        Track(request, TryExtendKeyValueDelayed(semaphore, request.RequestId, extendKeyRequest, responseStream, context));
                    }
                    break;

                    case GrpcClientBatchType.TryExistsKeyValue:
                    {
                        GrpcTryExistsKeyValueRequest? extendKeyRequest = request.TryExistsKeyValue;

                        Track(request, TryExistsKeyValueDelayed(semaphore, request.RequestId, extendKeyRequest, responseStream, context));
                    }
                    break;
                    
                    case GrpcClientBatchType.TryExecuteTransactionScript:
                    {
                        GrpcTryExecuteTransactionScriptRequest? tryExecuteTransactionScriptRequest = request.TryExecuteTransactionScript;

                        Track(request, TryExecuteTransactionScriptDelayed(semaphore, request.RequestId, tryExecuteTransactionScriptRequest, responseStream, context));
                    }
                    break;
                    
                    case GrpcClientBatchType.TryAcquireExclusiveLock:
                    {
                        GrpcTryAcquireExclusiveLockRequest? tryAcquireExclusiveLockRequest = request.TryAcquireExclusiveLock;

                        Track(request, TryAcquireExclusiveLockDelayed(semaphore, request.RequestId, tryAcquireExclusiveLockRequest, responseStream, context));
                    }
                    break;
                    
                    case GrpcClientBatchType.TryGetByBucket:
                    {
                        GrpcGetByBucketRequest? GetByBucketRequest = request.GetByBucket;

                        Track(request, TryGetByBucketDelayed(semaphore, request.RequestId, GetByBucketRequest, responseStream, context));
                    }
                    break;
                    
                    case GrpcClientBatchType.TryScanByPrefix:
                    {
                        GrpcScanAllByPrefixRequest? scanByPrefixRequest = request.ScanByPrefix;

                        Track(request, TryScanAllByPrefixDelayed(semaphore, request.RequestId, scanByPrefixRequest, responseStream, context));
                    }
                    break;
                    
                    case GrpcClientBatchType.TryStartTransaction:
                    {
                        GrpcStartTransactionRequest? startTransactionRequest = request.StartTransaction;

                        Track(request, TryStartTransactionDelayed(semaphore, request.RequestId, startTransactionRequest, responseStream, context));
                    }
                    break;
                    
                    case GrpcClientBatchType.TryCommitTransaction:
                    {
                        GrpcCommitTransactionRequest? commitTransactionRequest = request.CommitTransaction;

                        Track(request, TryCommitTransactionDelayed(semaphore, request.RequestId, commitTransactionRequest, responseStream, context));
                    }
                    break;
                    
                    case GrpcClientBatchType.TryRollbackTransaction:
                    {
                        GrpcRollbackTransactionRequest? rollbackTransactionRequest = request.RollbackTransaction;

                        Track(request, TryRollbackTransactionDelayed(semaphore, request.RequestId, rollbackTransactionRequest, responseStream, context));
                    }
                    break;

                    case GrpcClientBatchType.TypeNone:
                    default:
                        logger.LogError("Unknown batch client request type: {Type}", request.Type);
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

    private async Task TrySetKeyValueDelayed(
        SemaphoreSlim semaphore, 
        int requestId, 
        GrpcTrySetKeyValueRequest setKeyRequest, 
        IServerStreamWriter<GrpcBatchClientKeyValueResponse> responseStream,
        ServerCallContext context
    )
    {
        GrpcTrySetKeyValueResponse trySetResponse = await service.TrySetKeyValueInternal(setKeyRequest, context);
        
        GrpcBatchClientKeyValueResponse response = new()
        {
            Type = GrpcClientBatchType.TrySetKeyValue,
            RequestId = requestId,
            TrySetKeyValue = trySetResponse
        };

        await WriteResponseToStream(semaphore, responseStream, response, context);
    }
    
    private async Task TrySetManyKeyValueDelayed(
        SemaphoreSlim semaphore, 
        int requestId, 
        GrpcTrySetManyKeyValueRequest setKeyRequest, 
        IServerStreamWriter<GrpcBatchClientKeyValueResponse> responseStream,
        ServerCallContext context
    )
    {
        // Client-facing entry: a transactional batch without registration identity can never commit, so
        // reject it here exactly as the unary endpoint does (the inter-node batcher stays unguarded —
        // its already-routed fan-out legitimately carries transactional items without identity).
        GrpcTrySetManyKeyValueResponse trySetResponse =
            KeyValuesService.RejectUnregisteredTransactionalSetMany(setKeyRequest)
            ?? await service.TrySetManyKeyValueInternal(setKeyRequest, context);
        
        GrpcBatchClientKeyValueResponse response = new()
        {
            Type = GrpcClientBatchType.TrySetManyKeyValue,
            RequestId = requestId,
            TrySetManyKeyValue = trySetResponse
        };

        await WriteResponseToStream(semaphore, responseStream, response, context);
    }

    private async Task TryDeleteManyKeyValueDelayed(
        SemaphoreSlim semaphore,
        int requestId,
        GrpcTryDeleteManyKeyValueRequest deleteManyKeyRequest,
        IServerStreamWriter<GrpcBatchClientKeyValueResponse> responseStream,
        ServerCallContext context
    )
    {
        // Client-facing entry: reject an unregistered transactional batch exactly as the unary endpoint does.
        GrpcTryDeleteManyKeyValueResponse tryDeleteManyResponse =
            KeyValuesService.RejectUnregisteredTransactionalDeleteMany(deleteManyKeyRequest)
            ?? await service.TryDeleteManyKeyValueInternal(deleteManyKeyRequest, context);

        GrpcBatchClientKeyValueResponse response = new()
        {
            Type = GrpcClientBatchType.TryDeleteManyKeyValue,
            RequestId = requestId,
            TryDeleteManyKeyValue = tryDeleteManyResponse
        };

        await WriteResponseToStream(semaphore, responseStream, response, context);
    }
    
    private async Task TryGetKeyValueDelayed(
        SemaphoreSlim semaphore, 
        int requestId, 
        GrpcTryGetKeyValueRequest getKeyRequest, 
        IServerStreamWriter<GrpcBatchClientKeyValueResponse> responseStream,
        ServerCallContext context
    )
    {
        GrpcTryGetKeyValueResponse tryGetResponse = await service.TryGetKeyValueInternal(getKeyRequest, context);
        
        GrpcBatchClientKeyValueResponse response = new()
        {
            Type = GrpcClientBatchType.TryGetKeyValue,
            RequestId = requestId,
            TryGetKeyValue = tryGetResponse
        };

        await WriteResponseToStream(semaphore, responseStream, response, context);
    }
    
    private async Task TryDeleteKeyValueDelayed(
        SemaphoreSlim semaphore, 
        int requestId, 
        GrpcTryDeleteKeyValueRequest deleteKeyRequest, 
        IServerStreamWriter<GrpcBatchClientKeyValueResponse> responseStream,
        ServerCallContext context
    )
    {
        GrpcTryDeleteKeyValueResponse tryDeleteResponse = await service.TryDeleteKeyValueInternal(deleteKeyRequest, context);
        
        GrpcBatchClientKeyValueResponse response = new()
        {
            Type = GrpcClientBatchType.TryDeleteKeyValue,
            RequestId = requestId,
            TryDeleteKeyValue = tryDeleteResponse
        };

        await WriteResponseToStream(semaphore, responseStream, response, context);
    }
    
    private async Task TryExtendKeyValueDelayed(
        SemaphoreSlim semaphore, 
        int requestId, 
        GrpcTryExtendKeyValueRequest extendKeyRequest, 
        IServerStreamWriter<GrpcBatchClientKeyValueResponse> responseStream,
        ServerCallContext context
    )
    {
        GrpcTryExtendKeyValueResponse tryExtendResponse = await service.TryExtendKeyValueInternal(extendKeyRequest, context);
        
        GrpcBatchClientKeyValueResponse response = new()
        {
            Type = GrpcClientBatchType.TryExtendKeyValue,
            RequestId = requestId,
            TryExtendKeyValue = tryExtendResponse
        };

        await WriteResponseToStream(semaphore, responseStream, response, context);
    }
    
    private async Task TryExistsKeyValueDelayed(
        SemaphoreSlim semaphore, 
        int requestId, 
        GrpcTryExistsKeyValueRequest existKeyRequest, 
        IServerStreamWriter<GrpcBatchClientKeyValueResponse> responseStream,
        ServerCallContext context
    )
    {
        GrpcTryExistsKeyValueResponse tryExistsResponse = await service.TryExistsKeyValueInternal(existKeyRequest, context);
        
        GrpcBatchClientKeyValueResponse response = new()
        {
            Type = GrpcClientBatchType.TryExistsKeyValue,
            RequestId = requestId,
            TryExistsKeyValue = tryExistsResponse
        };

        await WriteResponseToStream(semaphore, responseStream, response, context);
    }
    
    private async Task TryAcquireExclusiveLockDelayed(
        SemaphoreSlim semaphore, 
        int requestId, 
        GrpcTryAcquireExclusiveLockRequest existKeyRequest, 
        IServerStreamWriter<GrpcBatchClientKeyValueResponse> responseStream,
        ServerCallContext context
    )
    {
        GrpcTryAcquireExclusiveLockResponse tryAcquireExclusiveLockResponse = await service.TryAcquireExclusiveLockInternal(existKeyRequest, context);
        
        GrpcBatchClientKeyValueResponse response = new()
        {
            Type = GrpcClientBatchType.TryAcquireExclusiveLock,
            RequestId = requestId,
            TryAcquireExclusiveLock = tryAcquireExclusiveLockResponse
        };

        await WriteResponseToStream(semaphore, responseStream, response, context);
    }
    
    private async Task TryExecuteTransactionScriptDelayed(
        SemaphoreSlim semaphore, 
        int requestId, 
        GrpcTryExecuteTransactionScriptRequest tryExecuteTransactionRequest, 
        IServerStreamWriter<GrpcBatchClientKeyValueResponse> responseStream,
        ServerCallContext context
    )
    {
        GrpcTryExecuteTransactionScriptResponse tryExecuteTransactionScriptResponse = await service.TryExecuteTransactionScriptInternal(tryExecuteTransactionRequest, context);
        
        GrpcBatchClientKeyValueResponse response = new()
        {
            Type = GrpcClientBatchType.TryExecuteTransactionScript,
            RequestId = requestId,
            TryExecuteTransactionScript = tryExecuteTransactionScriptResponse
        };

        await WriteResponseToStream(semaphore, responseStream, response, context);              
    }
    
    private async Task TryGetByBucketDelayed(
        SemaphoreSlim semaphore, 
        int requestId, 
        GrpcGetByBucketRequest GetByBucketRequest, 
        IServerStreamWriter<GrpcBatchClientKeyValueResponse> responseStream,
        ServerCallContext context
    )
    {
        GrpcGetByBucketResponse GetByBucketResponse = await service.GetByBucketInternal(GetByBucketRequest, context);
        
        GrpcBatchClientKeyValueResponse response = new()
        {
            Type = GrpcClientBatchType.TryGetByBucket,
            RequestId = requestId,
            GetByBucket = GetByBucketResponse
        };

        await WriteResponseToStream(semaphore, responseStream, response, context);             
    }
    
    private async Task TryScanAllByPrefixDelayed(
        SemaphoreSlim semaphore, 
        int requestId, 
        GrpcScanAllByPrefixRequest scanAllByPrefixRequest, 
        IServerStreamWriter<GrpcBatchClientKeyValueResponse> responseStream,
        ServerCallContext context
    )
    {
        GrpcScanAllByPrefixResponse scanAllByPrefixResponse = await service.ScanAllByPrefixInternal(scanAllByPrefixRequest, context);
        
        GrpcBatchClientKeyValueResponse response = new()
        {
            Type = GrpcClientBatchType.TryScanByPrefix,
            RequestId = requestId,
            ScanByPrefix = scanAllByPrefixResponse
        };

        await WriteResponseToStream(semaphore, responseStream, response, context);              
    }
    
    private async Task TryStartTransactionDelayed(
        SemaphoreSlim semaphore, 
        int requestId, 
        GrpcStartTransactionRequest startTransactionRequest, 
        IServerStreamWriter<GrpcBatchClientKeyValueResponse> responseStream,
        ServerCallContext context
    )
    {
        GrpcStartTransactionResponse startTransactionResponse = await service.StartTransactionInternal(startTransactionRequest, context);
        
        GrpcBatchClientKeyValueResponse response = new()
        {
            Type = GrpcClientBatchType.TryStartTransaction,
            RequestId = requestId,
            StartTransaction = startTransactionResponse
        };

        await WriteResponseToStream(semaphore, responseStream, response, context);
    }
    
    private async Task TryCommitTransactionDelayed(
        SemaphoreSlim semaphore, 
        int requestId, 
        GrpcCommitTransactionRequest commitTransactionRequest, 
        IServerStreamWriter<GrpcBatchClientKeyValueResponse> responseStream,
        ServerCallContext context
    )
    {
        GrpcCommitTransactionResponse commitTransactionResponse = await service.CommitTransactionInternal(commitTransactionRequest, context);
        
        GrpcBatchClientKeyValueResponse response = new()
        {
            Type = GrpcClientBatchType.TryCommitTransaction,
            RequestId = requestId,
            CommitTransaction = commitTransactionResponse
        };

        await WriteResponseToStream(semaphore, responseStream, response, context);
    }
    
    private async Task TryRollbackTransactionDelayed(
        SemaphoreSlim semaphore, 
        int requestId, 
        GrpcRollbackTransactionRequest rollbackTransactionRequest, 
        IServerStreamWriter<GrpcBatchClientKeyValueResponse> responseStream,
        ServerCallContext context
    )
    {
        GrpcRollbackTransactionResponse rollbackTransactionResponse = await service.RollbackTransactionInternal(rollbackTransactionRequest, context);
        
        GrpcBatchClientKeyValueResponse response = new()
        {
            Type = GrpcClientBatchType.TryRollbackTransaction,
            RequestId = requestId,
            RollbackTransaction = rollbackTransactionResponse
        };

        await WriteResponseToStream(semaphore, responseStream, response, context);
    }

    private static async Task WriteResponseToStream(
        SemaphoreSlim semaphore, 
        IServerStreamWriter<GrpcBatchClientKeyValueResponse> responseStream, 
        GrpcBatchClientKeyValueResponse response, 
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
