
using System.Threading.Channels;

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

        // Handlers complete concurrently, but the HTTP/2 stream admits one writer. Funneling the
        // responses through a single-reader channel (instead of a per-response semaphore hand-off)
        // lets the drain loop below coalesce every response completed during the previous write
        // into one buffered burst with a single pipe flush, rather than one socket flush each.
        Channel<GrpcBatchClientKeyValueResponse> responses = Channel.CreateUnbounded<GrpcBatchClientKeyValueResponse>(
            new UnboundedChannelOptions { SingleReader = true, SingleWriter = false });

        ChannelWriter<GrpcBatchClientKeyValueResponse> writer = responses.Writer;

        Task writerTask = WriteResponsesToStream(responses.Reader, responseStream, context);

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

                writer.TryWrite(BatchRefusalResponses.ForClientKeyValue(request));
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

                        Track(request, TrySetKeyValueDelayed(request.RequestId, setKeyRequest, writer, context));
                    }
                    break;

                    case GrpcClientBatchType.TrySetManyKeyValue:
                    {
                        GrpcTrySetManyKeyValueRequest? setKeyRequest = request.TrySetManyKeyValue;

                        Track(request, TrySetManyKeyValueDelayed(request.RequestId, setKeyRequest, writer, context));
                    }
                    break;

                    case GrpcClientBatchType.TryDeleteManyKeyValue:
                    {
                        GrpcTryDeleteManyKeyValueRequest? deleteManyKeyRequest = request.TryDeleteManyKeyValue;

                        Track(request, TryDeleteManyKeyValueDelayed(request.RequestId, deleteManyKeyRequest, writer, context));
                    }
                    break;

                    case GrpcClientBatchType.TryGetKeyValue:
                    {
                        GrpcTryGetKeyValueRequest? getKeyRequest = request.TryGetKeyValue;

                        Track(request, TryGetKeyValueDelayed(request.RequestId, getKeyRequest, writer, context));
                    }
                    break;

                    case GrpcClientBatchType.TryDeleteKeyValue:
                    {
                        GrpcTryDeleteKeyValueRequest? deleteKeyRequest = request.TryDeleteKeyValue;

                        Track(request, TryDeleteKeyValueDelayed(request.RequestId, deleteKeyRequest, writer, context));
                    }
                    break;

                    case GrpcClientBatchType.TryExtendKeyValue:
                    {
                        GrpcTryExtendKeyValueRequest? extendKeyRequest = request.TryExtendKeyValue;

                        Track(request, TryExtendKeyValueDelayed(request.RequestId, extendKeyRequest, writer, context));
                    }
                    break;

                    case GrpcClientBatchType.TryExistsKeyValue:
                    {
                        GrpcTryExistsKeyValueRequest? extendKeyRequest = request.TryExistsKeyValue;

                        Track(request, TryExistsKeyValueDelayed(request.RequestId, extendKeyRequest, writer, context));
                    }
                    break;

                    case GrpcClientBatchType.TryExecuteTransactionScript:
                    {
                        GrpcTryExecuteTransactionScriptRequest? tryExecuteTransactionScriptRequest = request.TryExecuteTransactionScript;

                        Track(request, TryExecuteTransactionScriptDelayed(request.RequestId, tryExecuteTransactionScriptRequest, writer, context));
                    }
                    break;

                    case GrpcClientBatchType.TryAcquireExclusiveLock:
                    {
                        GrpcTryAcquireExclusiveLockRequest? tryAcquireExclusiveLockRequest = request.TryAcquireExclusiveLock;

                        Track(request, TryAcquireExclusiveLockDelayed(request.RequestId, tryAcquireExclusiveLockRequest, writer, context));
                    }
                    break;

                    case GrpcClientBatchType.TryGetByBucket:
                    {
                        GrpcGetByBucketRequest? GetByBucketRequest = request.GetByBucket;

                        Track(request, TryGetByBucketDelayed(request.RequestId, GetByBucketRequest, writer, context));
                    }
                    break;

                    case GrpcClientBatchType.TryScanByPrefix:
                    {
                        GrpcScanAllByPrefixRequest? scanByPrefixRequest = request.ScanByPrefix;

                        Track(request, TryScanAllByPrefixDelayed(request.RequestId, scanByPrefixRequest, writer, context));
                    }
                    break;

                    case GrpcClientBatchType.TryStartTransaction:
                    {
                        GrpcStartTransactionRequest? startTransactionRequest = request.StartTransaction;

                        Track(request, TryStartTransactionDelayed(request.RequestId, startTransactionRequest, writer, context));
                    }
                    break;

                    case GrpcClientBatchType.TryCommitTransaction:
                    {
                        GrpcCommitTransactionRequest? commitTransactionRequest = request.CommitTransaction;

                        Track(request, TryCommitTransactionDelayed(request.RequestId, commitTransactionRequest, writer, context));
                    }
                    break;

                    case GrpcClientBatchType.TryRollbackTransaction:
                    {
                        GrpcRollbackTransactionRequest? rollbackTransactionRequest = request.RollbackTransaction;

                        Track(request, TryRollbackTransactionDelayed(request.RequestId, rollbackTransactionRequest, writer, context));
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

            // Every handler has answered; close the channel so the drain loop flushes the tail
            // and exits, then wait for it so no write races the call teardown.
            writer.TryComplete();
            await writerTask;
        }
    }

    /// <summary>
    /// A write carrying <see cref="WriteFlags.BufferHint"/> only appends to the response pipe;
    /// a write without it also flushes the pipe to the socket.
    /// </summary>
    private static readonly WriteOptions BufferedWrite = new(WriteFlags.BufferHint);

    private static readonly WriteOptions FlushingWrite = new();

    /// <summary>
    /// Single writer for the shared response stream. Drains every response available in one pass,
    /// buffering each write while more are already waiting and letting the pass's last write carry
    /// the flush — so a burst of concurrent completions costs one socket flush instead of one each.
    /// A pass of one response degenerates to exactly the old write-then-flush behavior. Exits when
    /// the channel completes or the call dies; in the latter case remaining responses are dropped,
    /// matching the old per-response writer, because there is nobody left to read them.
    /// </summary>
    private async Task WriteResponsesToStream(
        ChannelReader<GrpcBatchClientKeyValueResponse> responses,
        IServerStreamWriter<GrpcBatchClientKeyValueResponse> responseStream,
        ServerCallContext context
    )
    {
        try
        {
            while (await responses.WaitToReadAsync(context.CancellationToken))
            {
                while (responses.TryRead(out GrpcBatchClientKeyValueResponse? response))
                {
                    responseStream.WriteOptions = responses.TryPeek(out _) ? BufferedWrite : FlushingWrite;

                    await responseStream.WriteAsync(response);
                }
            }
        }
        catch (Exception ex) when (ex is IOException or OperationCanceledException or InvalidOperationException)
        {
            // The client went away or the call was cancelled mid-write.
            logger.LogCommunicationIoException(ex);
        }
    }

    private async Task TrySetKeyValueDelayed(
        int requestId,
        GrpcTrySetKeyValueRequest setKeyRequest,
        ChannelWriter<GrpcBatchClientKeyValueResponse> responses,
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

        responses.TryWrite(response);
    }

    private async Task TrySetManyKeyValueDelayed(
        int requestId,
        GrpcTrySetManyKeyValueRequest setKeyRequest,
        ChannelWriter<GrpcBatchClientKeyValueResponse> responses,
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

        responses.TryWrite(response);
    }

    private async Task TryDeleteManyKeyValueDelayed(
        int requestId,
        GrpcTryDeleteManyKeyValueRequest deleteManyKeyRequest,
        ChannelWriter<GrpcBatchClientKeyValueResponse> responses,
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

        responses.TryWrite(response);
    }

    private async Task TryGetKeyValueDelayed(
        int requestId,
        GrpcTryGetKeyValueRequest getKeyRequest,
        ChannelWriter<GrpcBatchClientKeyValueResponse> responses,
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

        responses.TryWrite(response);
    }

    private async Task TryDeleteKeyValueDelayed(
        int requestId,
        GrpcTryDeleteKeyValueRequest deleteKeyRequest,
        ChannelWriter<GrpcBatchClientKeyValueResponse> responses,
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

        responses.TryWrite(response);
    }

    private async Task TryExtendKeyValueDelayed(
        int requestId,
        GrpcTryExtendKeyValueRequest extendKeyRequest,
        ChannelWriter<GrpcBatchClientKeyValueResponse> responses,
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

        responses.TryWrite(response);
    }

    private async Task TryExistsKeyValueDelayed(
        int requestId,
        GrpcTryExistsKeyValueRequest existKeyRequest,
        ChannelWriter<GrpcBatchClientKeyValueResponse> responses,
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

        responses.TryWrite(response);
    }

    private async Task TryAcquireExclusiveLockDelayed(
        int requestId,
        GrpcTryAcquireExclusiveLockRequest existKeyRequest,
        ChannelWriter<GrpcBatchClientKeyValueResponse> responses,
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

        responses.TryWrite(response);
    }

    private async Task TryExecuteTransactionScriptDelayed(
        int requestId,
        GrpcTryExecuteTransactionScriptRequest tryExecuteTransactionRequest,
        ChannelWriter<GrpcBatchClientKeyValueResponse> responses,
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

        responses.TryWrite(response);
    }

    private async Task TryGetByBucketDelayed(
        int requestId,
        GrpcGetByBucketRequest GetByBucketRequest,
        ChannelWriter<GrpcBatchClientKeyValueResponse> responses,
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

        responses.TryWrite(response);
    }

    private async Task TryScanAllByPrefixDelayed(
        int requestId,
        GrpcScanAllByPrefixRequest scanAllByPrefixRequest,
        ChannelWriter<GrpcBatchClientKeyValueResponse> responses,
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

        responses.TryWrite(response);
    }

    private async Task TryStartTransactionDelayed(
        int requestId,
        GrpcStartTransactionRequest startTransactionRequest,
        ChannelWriter<GrpcBatchClientKeyValueResponse> responses,
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

        responses.TryWrite(response);
    }

    private async Task TryCommitTransactionDelayed(
        int requestId,
        GrpcCommitTransactionRequest commitTransactionRequest,
        ChannelWriter<GrpcBatchClientKeyValueResponse> responses,
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

        responses.TryWrite(response);
    }

    private async Task TryRollbackTransactionDelayed(
        int requestId,
        GrpcRollbackTransactionRequest rollbackTransactionRequest,
        ChannelWriter<GrpcBatchClientKeyValueResponse> responses,
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

        responses.TryWrite(response);
    }
}
