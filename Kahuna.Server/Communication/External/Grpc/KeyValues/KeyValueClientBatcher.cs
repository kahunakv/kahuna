
using Grpc.Core;

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
        try
        {
            List<Task> tasks = [];

            using SemaphoreSlim semaphore = new(1, 1);

            await foreach (GrpcBatchClientKeyValueRequest request in requestStream.ReadAllAsync())
            {
                switch (request.Type)
                {
                    case GrpcClientBatchType.TrySetKeyValue:
                    {
                        GrpcTrySetKeyValueRequest? setKeyRequest = request.TrySetKeyValue;

                        tasks.Add(TrySetKeyValueDelayed(semaphore, request.RequestId, setKeyRequest, responseStream, context));
                    }
                    break;

                    case GrpcClientBatchType.TryGetKeyValue:
                    {
                        GrpcTryGetKeyValueRequest? getKeyRequest = request.TryGetKeyValue;

                        tasks.Add(TryGetKeyValueDelayed(semaphore, request.RequestId, getKeyRequest, responseStream, context));
                    }
                    break;

                    case GrpcClientBatchType.TryDeleteKeyValue:
                    {
                        GrpcTryDeleteKeyValueRequest? deleteKeyRequest = request.TryDeleteKeyValue;

                        tasks.Add(TryDeleteKeyValueDelayed(semaphore, request.RequestId, deleteKeyRequest, responseStream, context));
                    }
                    break;

                    case GrpcClientBatchType.TryExtendKeyValue:
                    {
                        GrpcTryExtendKeyValueRequest? extendKeyRequest = request.TryExtendKeyValue;

                        tasks.Add(TryExtendKeyValueDelayed(semaphore, request.RequestId, extendKeyRequest, responseStream, context));
                    }
                    break;

                    case GrpcClientBatchType.TryExistsKeyValue:
                    {
                        GrpcTryExistsKeyValueRequest? extendKeyRequest = request.TryExistsKeyValue;

                        tasks.Add(TryExistsKeyValueDelayed(semaphore, request.RequestId, extendKeyRequest, responseStream, context));
                    }
                    break;
                    
                    case GrpcClientBatchType.TryExecuteTransactionScript:
                    {
                        GrpcTryExecuteTransactionScriptRequest? tryExecuteTransactionScriptRequest = request.TryExecuteTransactionScript;

                        tasks.Add(TryExecuteTransactionScriptDelayed(semaphore, request.RequestId, tryExecuteTransactionScriptRequest, responseStream, context));
                    }
                    break;
                    
                    case GrpcClientBatchType.TryGetByPrefix:
                    {
                        GrpcGetByPrefixRequest? getByPrefixRequest = request.GetByPrefix;

                        tasks.Add(TryGetByPrefixDelayed(semaphore, request.RequestId, getByPrefixRequest, responseStream, context));
                    }
                    break;
                    
                    case GrpcClientBatchType.TryScanByPrefix:
                    {
                        GrpcScanAllByPrefixRequest? scanByPrefixRequest = request.ScanByPrefix;

                        tasks.Add(TryScanAllByPrefixDelayed(semaphore, request.RequestId, scanByPrefixRequest, responseStream, context));
                    }
                    break;
                    
                    case GrpcClientBatchType.TryStartTransaction:
                    {
                        GrpcStartTransactionRequest? startTransactionRequest = request.StartTransaction;

                        tasks.Add(TryStartTransactionDelayed(semaphore, request.RequestId, startTransactionRequest, responseStream, context));
                    }
                    break;
                    
                    case GrpcClientBatchType.TryCommitTransaction:
                    {
                        GrpcCommitTransactionRequest? commitTransactionRequest = request.CommitTransaction;

                        tasks.Add(TryCommitTransactionDelayed(semaphore, request.RequestId, commitTransactionRequest, responseStream, context));
                    }
                    break;
                    
                    case GrpcClientBatchType.TryRollbackTransaction:
                    {
                        GrpcRollbackTransactionRequest? rollbackTransactionRequest = request.RollbackTransaction;

                        tasks.Add(TryRollbackTransactionDelayed(semaphore, request.RequestId, rollbackTransactionRequest, responseStream, context));
                    }
                    break;

                    case GrpcClientBatchType.TypeNone:
                    default:
                        logger.LogError("Unknown batch client request type: {Type}", request.Type);
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
    
    private async Task TryGetByPrefixDelayed(
        SemaphoreSlim semaphore, 
        int requestId, 
        GrpcGetByPrefixRequest getByPrefixRequest, 
        IServerStreamWriter<GrpcBatchClientKeyValueResponse> responseStream,
        ServerCallContext context
    )
    {
        GrpcGetByPrefixResponse getByPrefixResponse = await service.GetByPrefixInternal(getByPrefixRequest, context);
        
        GrpcBatchClientKeyValueResponse response = new()
        {
            Type = GrpcClientBatchType.TryGetByPrefix,
            RequestId = requestId,
            GetByPrefix = getByPrefixResponse
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