
using Grpc.Core;

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
        try
        {
            List<Task> tasks = [];

            using SemaphoreSlim semaphore = new(1, 1);

            await foreach (GrpcBatchServerKeyValueRequest request in requestStream.ReadAllAsync())
            {
                switch (request.Type)
                {
                    case GrpcServerBatchType.ServerTrySetKeyValue:
                    {
                        GrpcTrySetKeyValueRequest? setKeyRequest = request.TrySetKeyValue;

                        tasks.Add(TrySetKeyValueServerDelayed(semaphore, request.RequestId, setKeyRequest, responseStream, context));
                    }
                    break;
                    
                    case GrpcServerBatchType.ServerTrySetManyKeyValue:
                    {
                        GrpcTrySetManyKeyValueRequest? setKeyRequest = request.TrySetManyKeyValue;

                        tasks.Add(TrySetManyKeyValueServerDelayed(semaphore, request.RequestId, setKeyRequest, responseStream, context));
                    }
                    break;

                    case GrpcServerBatchType.ServerTryGetKeyValue:
                    {
                        GrpcTryGetKeyValueRequest? getKeyRequest = request.TryGetKeyValue;

                        tasks.Add(TryGetKeyValueServerDelayed(semaphore, request.RequestId, getKeyRequest, responseStream, context));
                    }
                    break;

                    case GrpcServerBatchType.ServerTryDeleteKeyValue:
                    {
                        GrpcTryDeleteKeyValueRequest? deleteKeyRequest = request.TryDeleteKeyValue;

                        tasks.Add(TryDeleteKeyValueServerDelayed(semaphore, request.RequestId, deleteKeyRequest, responseStream, context));
                    }
                    break;

                    case GrpcServerBatchType.ServerTryExtendKeyValue:
                    {
                        GrpcTryExtendKeyValueRequest? extendKeyRequest = request.TryExtendKeyValue;

                        tasks.Add(TryExtendKeyValueServerDelayed(semaphore, request.RequestId, extendKeyRequest, responseStream, context));
                    }
                    break;

                    case GrpcServerBatchType.ServerTryExistsKeyValue:
                    {
                        GrpcTryExistsKeyValueRequest? extendKeyRequest = request.TryExistsKeyValue;

                        tasks.Add(TryExistsKeyValueServerDelayed(semaphore, request.RequestId, extendKeyRequest, responseStream, context));
                    }
                    break;
                    
                    case GrpcServerBatchType.ServerTryExecuteTransactionScript:
                    {
                        GrpcTryExecuteTransactionScriptRequest? tryExecuteTransactionScriptRequest = request.TryExecuteTransactionScript;

                        tasks.Add(TryExecuteTransactionServerDelayed(semaphore, request.RequestId, tryExecuteTransactionScriptRequest, responseStream, context));
                    }
                    break;
                    
                    case GrpcServerBatchType.ServerTryAcquireExclusiveLock:
                    {
                        GrpcTryAcquireExclusiveLockRequest? tryAcquireExclusiveLockRequest = request.TryAcquireExclusiveLock;

                        tasks.Add(TryAcquireExclusiveLockDelayed(semaphore, request.RequestId, tryAcquireExclusiveLockRequest, responseStream, context));
                    }
                    break;
                    
                    case GrpcServerBatchType.ServerTryAcquireExclusivePrefixLock:
                    {
                        GrpcTryAcquireExclusivePrefixLockRequest? tryAcquireExclusivePrefixLockRequest = request.TryAcquireExclusivePrefixLock;

                        tasks.Add(TryAcquireExclusivePrefixLockDelayed(semaphore, request.RequestId, tryAcquireExclusivePrefixLockRequest, responseStream, context));
                    }
                    break;
                    
                    case GrpcServerBatchType.ServerTryAcquireManyExclusiveLocks:
                    {
                        GrpcTryAcquireManyExclusiveLocksRequest? tryAcquireManyExclusiveLocksRequest = request.TryAcquireManyExclusiveLocks;

                        tasks.Add(TryAcquireManyExclusiveLocksDelayed(semaphore, request.RequestId, tryAcquireManyExclusiveLocksRequest, responseStream, context));
                    }
                    break;
                    
                    case GrpcServerBatchType.ServerTryReleaseExclusiveLock:
                    {
                        GrpcTryReleaseExclusiveLockRequest? tryReleaseExclusiveLockRequest = request.TryReleaseExclusiveLock;

                        tasks.Add(TryReleaseExclusiveLockDelayed(semaphore, request.RequestId, tryReleaseExclusiveLockRequest, responseStream, context));
                    }
                    break;
                    
                    case GrpcServerBatchType.ServerTryReleaseExclusivePrefixLock:
                    {
                        GrpcTryReleaseExclusivePrefixLockRequest? tryReleaseExclusivePrefixLockRequest = request.TryReleaseExclusivePrefixLock;

                        tasks.Add(TryReleaseExclusivePrefixLockDelayed(semaphore, request.RequestId, tryReleaseExclusivePrefixLockRequest, responseStream, context));
                    }
                        break;
                    
                    case GrpcServerBatchType.ServerTryReleaseManyExclusiveLocks:
                    {
                        GrpcTryReleaseManyExclusiveLocksRequest? tryReleaseManyExclusiveLocksRequest = request.TryReleaseManyExclusiveLocks;

                        tasks.Add(TryReleaseManyExclusiveLocksDelayed(semaphore, request.RequestId, tryReleaseManyExclusiveLocksRequest, responseStream, context));
                    }
                    break;
                    
                    case GrpcServerBatchType.ServerTryPrepareMutations:
                    {
                        GrpcTryPrepareMutationsRequest? tryPrepareMutationsRequest = request.TryPrepareMutations;

                        tasks.Add(TryPrepareMutationsDelayed(semaphore, request.RequestId, tryPrepareMutationsRequest, responseStream, context));
                    }
                    break;
                    
                    case GrpcServerBatchType.ServerTryPrepareManyMutations:
                    {
                        GrpcTryPrepareManyMutationsRequest? tryPrepareManyMutationsRequest = request.TryPrepareManyMutations;

                        tasks.Add(TryPrepareManyMutationsDelayed(semaphore, request.RequestId, tryPrepareManyMutationsRequest, responseStream, context));
                    }
                    break;
                    
                    case GrpcServerBatchType.ServerTryCommitMutations:
                    {
                        GrpcTryCommitMutationsRequest? tryCommitMutationsRequest = request.TryCommitMutations;

                        tasks.Add(TryCommitMutationsDelayed(semaphore, request.RequestId, tryCommitMutationsRequest, responseStream, context));
                    }
                    break;
                    
                    case GrpcServerBatchType.ServerTryCommitManyMutations:
                    {
                        GrpcTryCommitManyMutationsRequest? tryCommitManyMutationsRequest = request.TryCommitManyMutations;

                        tasks.Add(TryCommitManyMutationsDelayed(semaphore, request.RequestId, tryCommitManyMutationsRequest, responseStream, context));
                    }
                    break;
                    
                    case GrpcServerBatchType.ServerTryRollbackMutations:
                    {
                        GrpcTryRollbackMutationsRequest? tryRollbackMutationsRequest = request.TryRollbackMutations;

                        tasks.Add(TryRollbackMutationsDelayed(semaphore, request.RequestId, tryRollbackMutationsRequest, responseStream, context));
                    }
                    break;
                    
                    case GrpcServerBatchType.ServerTryRollbackManyMutations:
                    {
                        GrpcTryRollbackManyMutationsRequest? tryRollbackManyMutationsRequest = request.TryRollbackManyMutations;

                        tasks.Add(TryRollbackManyMutationsDelayed(semaphore, request.RequestId, tryRollbackManyMutationsRequest, responseStream, context));
                    }
                    break;
                    
                    case GrpcServerBatchType.ServerTryGetByBucket:
                    {
                        GrpcGetByBucketRequest? tryGetByBucketRequest = request.GetByBucket;

                        tasks.Add(GetByBucketDelayed(semaphore, request.RequestId, tryGetByBucketRequest, responseStream, context));
                    }
                    break;
                    
                    case GrpcServerBatchType.ServerTryScanByPrefix:
                    {
                        GrpcScanByPrefixRequest? tryScanByPrefixRequest = request.ScanByPrefix;

                        tasks.Add(ScanByPrefixDelayed(semaphore, request.RequestId, tryScanByPrefixRequest, responseStream, context));
                    }
                    break;
                    
                    case GrpcServerBatchType.ServerTryStartTransaction:
                    {
                        GrpcStartTransactionRequest? startTransactionRequest = request.StartTransaction;

                        tasks.Add(StartTransactionDelayed(semaphore, request.RequestId, startTransactionRequest, responseStream, context));
                    }
                    break;
                    
                    case GrpcServerBatchType.ServerTryCommitTransaction:
                    {
                        GrpcCommitTransactionRequest? commitTransactionRequest = request.CommitTransaction;

                        tasks.Add(CommitTransactionDelayed(semaphore, request.RequestId, commitTransactionRequest, responseStream, context));
                    }
                    break;
                    
                    case GrpcServerBatchType.ServerTryRollbackTransaction:
                    {
                        GrpcRollbackTransactionRequest? rollbackTransactionRequest = request.RollbackTransaction;

                        tasks.Add(RollbackTransactionDelayed(semaphore, request.RequestId, rollbackTransactionRequest, responseStream, context));
                    }
                    break;

                    case GrpcServerBatchType.ServerTypeNone:
                    default:
                        logger.LogError("Unknown batch Server request type: {Type}", request.Type);
                        break;
                }
            }

            await Task.WhenAll(tasks);
        }
        catch (IOException ex)
        {
            logger.LogDebug("IOException: {Message}", ex.Message);
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
    
    private static async Task WriteResponseToStream(
        SemaphoreSlim semaphore, 
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream, 
        GrpcBatchServerKeyValueResponse response, 
        ServerCallContext context
    )
    {
        try
        {
            await semaphore.WaitAsync(context.CancellationToken);

            await responseStream.WriteAsync(response);
        }
        finally
        {
            semaphore.Release();
        }
    }   
}