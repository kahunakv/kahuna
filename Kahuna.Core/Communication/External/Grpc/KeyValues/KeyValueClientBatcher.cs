
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

    public async Task BatchClientKeyValueRequests(
        IAsyncStreamReader<GrpcBatchClientKeyValueRequest> requestStream,
        IServerStreamWriter<GrpcBatchClientKeyValueResponse> responseStream,
        ServerCallContext context
    )
    {
        StreamDrain drain = new();

        // Handlers complete concurrently, but the HTTP/2 stream admits one writer. Funneling the
        // responses through a single-reader channel (instead of a per-response semaphore hand-off)
        // lets the drain loop below coalesce every response completed during the previous write
        // into one buffered burst with a single pipe flush, rather than one socket flush each.
        Channel<GrpcBatchClientKeyValueResponse> responses = Channel.CreateUnbounded<GrpcBatchClientKeyValueResponse>(
            new UnboundedChannelOptions { SingleReader = true, SingleWriter = false });

        ChannelWriter<GrpcBatchClientKeyValueResponse> writer = responses.Writer;

        Task writerTask = WriteResponsesToStream(responses.Reader, responseStream, context);

        try
        {
            await foreach (GrpcBatchClientKeyValueRequest request in requestStream.ReadAllAsync())
            {
                drain.Enter();

                switch (request.Type)
                {
                    case GrpcClientBatchType.TrySetKeyValue:
                        _ = TrySetKeyValueDelayed(request, writer, context, drain);
                        break;

                    case GrpcClientBatchType.TrySetManyKeyValue:
                        _ = TrySetManyKeyValueDelayed(request, writer, context, drain);
                        break;

                    case GrpcClientBatchType.TryDeleteManyKeyValue:
                        _ = TryDeleteManyKeyValueDelayed(request, writer, context, drain);
                        break;

                    case GrpcClientBatchType.TryGetKeyValue:
                        _ = TryGetKeyValueDelayed(request, writer, context, drain);
                        break;

                    case GrpcClientBatchType.TryDeleteKeyValue:
                        _ = TryDeleteKeyValueDelayed(request, writer, context, drain);
                        break;

                    case GrpcClientBatchType.TryExtendKeyValue:
                        _ = TryExtendKeyValueDelayed(request, writer, context, drain);
                        break;

                    case GrpcClientBatchType.TryExistsKeyValue:
                        _ = TryExistsKeyValueDelayed(request, writer, context, drain);
                        break;

                    case GrpcClientBatchType.TryExecuteTransactionScript:
                        _ = TryExecuteTransactionScriptDelayed(request, writer, context, drain);
                        break;

                    case GrpcClientBatchType.TryAcquireExclusiveLock:
                        _ = TryAcquireExclusiveLockDelayed(request, writer, context, drain);
                        break;

                    case GrpcClientBatchType.TryGetByBucket:
                        _ = TryGetByBucketDelayed(request, writer, context, drain);
                        break;

                    case GrpcClientBatchType.TryScanByPrefix:
                        _ = TryScanAllByPrefixDelayed(request, writer, context, drain);
                        break;

                    case GrpcClientBatchType.TryStartTransaction:
                        _ = TryStartTransactionDelayed(request, writer, context, drain);
                        break;

                    case GrpcClientBatchType.TryCommitTransaction:
                        _ = TryCommitTransactionDelayed(request, writer, context, drain);
                        break;

                    case GrpcClientBatchType.TryRollbackTransaction:
                        _ = TryRollbackTransactionDelayed(request, writer, context, drain);
                        break;

                    case GrpcClientBatchType.TypeNone:
                    default:
                        logger.LogError("Unknown batch client request type: {Type}", request.Type);
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

            // Every handler has answered; close the channel so the drain loop flushes the tail
            // and exits, then wait for it so no write races the call teardown.
            writer.TryComplete();
            await writerTask;
        }
    }

    /// <summary>
    /// A handler that throws must still answer its own RequestId: the SDK matches responses by id,
    /// so an unanswered request hangs until its deadline while every other request on the shared
    /// stream keeps flowing. Refusing just that one request with MustRetry leaves the stream and
    /// its neighbours untouched, and gives the SDK's retry loop something it can classify.
    /// </summary>
    private void ObserveFault(
        GrpcBatchClientKeyValueRequest request,
        ChannelWriter<GrpcBatchClientKeyValueResponse> responses,
        Exception ex
    )
    {
        if (ex is IOException or OperationCanceledException)
        {
            // The stream is already gone or the client left; there is nobody left to answer.
            logger.LogCommunicationIoException(ex);
            return;
        }

        logger.LogError(ex, "Batch key-value client handler faulted");

        responses.TryWrite(BatchRefusalResponses.ForClientKeyValue(request));
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
        GrpcBatchClientKeyValueRequest request,
        ChannelWriter<GrpcBatchClientKeyValueResponse> responses,
        ServerCallContext context,
        StreamDrain drain
    )
    {
        try
        {
            GrpcTrySetKeyValueResponse trySetResponse = await service.TrySetKeyValueInternal(request.TrySetKeyValue, context);

            responses.TryWrite(new GrpcBatchClientKeyValueResponse
            {
                Type = GrpcClientBatchType.TrySetKeyValue,
                RequestId = request.RequestId,
                TrySetKeyValue = trySetResponse
            });
        }
        catch (Exception ex)
        {
            ObserveFault(request, responses, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task TrySetManyKeyValueDelayed(
        GrpcBatchClientKeyValueRequest request,
        ChannelWriter<GrpcBatchClientKeyValueResponse> responses,
        ServerCallContext context,
        StreamDrain drain
    )
    {
        try
        {
            // Client-facing entry: a transactional batch without registration identity can never commit, so
            // reject it here exactly as the unary endpoint does (the inter-node batcher stays unguarded —
            // its already-routed fan-out legitimately carries transactional items without identity).
            GrpcTrySetManyKeyValueResponse trySetResponse =
                KeyValuesService.RejectUnregisteredTransactionalSetMany(request.TrySetManyKeyValue)
                ?? await service.TrySetManyKeyValueInternal(request.TrySetManyKeyValue, context);

            responses.TryWrite(new GrpcBatchClientKeyValueResponse
            {
                Type = GrpcClientBatchType.TrySetManyKeyValue,
                RequestId = request.RequestId,
                TrySetManyKeyValue = trySetResponse
            });
        }
        catch (Exception ex)
        {
            ObserveFault(request, responses, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task TryDeleteManyKeyValueDelayed(
        GrpcBatchClientKeyValueRequest request,
        ChannelWriter<GrpcBatchClientKeyValueResponse> responses,
        ServerCallContext context,
        StreamDrain drain
    )
    {
        try
        {
            // Client-facing entry: reject an unregistered transactional batch exactly as the unary endpoint does.
            GrpcTryDeleteManyKeyValueResponse tryDeleteManyResponse =
                KeyValuesService.RejectUnregisteredTransactionalDeleteMany(request.TryDeleteManyKeyValue)
                ?? await service.TryDeleteManyKeyValueInternal(request.TryDeleteManyKeyValue, context);

            responses.TryWrite(new GrpcBatchClientKeyValueResponse
            {
                Type = GrpcClientBatchType.TryDeleteManyKeyValue,
                RequestId = request.RequestId,
                TryDeleteManyKeyValue = tryDeleteManyResponse
            });
        }
        catch (Exception ex)
        {
            ObserveFault(request, responses, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task TryGetKeyValueDelayed(
        GrpcBatchClientKeyValueRequest request,
        ChannelWriter<GrpcBatchClientKeyValueResponse> responses,
        ServerCallContext context,
        StreamDrain drain
    )
    {
        try
        {
            GrpcTryGetKeyValueResponse tryGetResponse = await service.TryGetKeyValueInternal(request.TryGetKeyValue, context);

            responses.TryWrite(new GrpcBatchClientKeyValueResponse
            {
                Type = GrpcClientBatchType.TryGetKeyValue,
                RequestId = request.RequestId,
                TryGetKeyValue = tryGetResponse
            });
        }
        catch (Exception ex)
        {
            ObserveFault(request, responses, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task TryDeleteKeyValueDelayed(
        GrpcBatchClientKeyValueRequest request,
        ChannelWriter<GrpcBatchClientKeyValueResponse> responses,
        ServerCallContext context,
        StreamDrain drain
    )
    {
        try
        {
            GrpcTryDeleteKeyValueResponse tryDeleteResponse = await service.TryDeleteKeyValueInternal(request.TryDeleteKeyValue, context);

            responses.TryWrite(new GrpcBatchClientKeyValueResponse
            {
                Type = GrpcClientBatchType.TryDeleteKeyValue,
                RequestId = request.RequestId,
                TryDeleteKeyValue = tryDeleteResponse
            });
        }
        catch (Exception ex)
        {
            ObserveFault(request, responses, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task TryExtendKeyValueDelayed(
        GrpcBatchClientKeyValueRequest request,
        ChannelWriter<GrpcBatchClientKeyValueResponse> responses,
        ServerCallContext context,
        StreamDrain drain
    )
    {
        try
        {
            GrpcTryExtendKeyValueResponse tryExtendResponse = await service.TryExtendKeyValueInternal(request.TryExtendKeyValue, context);

            responses.TryWrite(new GrpcBatchClientKeyValueResponse
            {
                Type = GrpcClientBatchType.TryExtendKeyValue,
                RequestId = request.RequestId,
                TryExtendKeyValue = tryExtendResponse
            });
        }
        catch (Exception ex)
        {
            ObserveFault(request, responses, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task TryExistsKeyValueDelayed(
        GrpcBatchClientKeyValueRequest request,
        ChannelWriter<GrpcBatchClientKeyValueResponse> responses,
        ServerCallContext context,
        StreamDrain drain
    )
    {
        try
        {
            GrpcTryExistsKeyValueResponse tryExistsResponse = await service.TryExistsKeyValueInternal(request.TryExistsKeyValue, context);

            responses.TryWrite(new GrpcBatchClientKeyValueResponse
            {
                Type = GrpcClientBatchType.TryExistsKeyValue,
                RequestId = request.RequestId,
                TryExistsKeyValue = tryExistsResponse
            });
        }
        catch (Exception ex)
        {
            ObserveFault(request, responses, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task TryAcquireExclusiveLockDelayed(
        GrpcBatchClientKeyValueRequest request,
        ChannelWriter<GrpcBatchClientKeyValueResponse> responses,
        ServerCallContext context,
        StreamDrain drain
    )
    {
        try
        {
            GrpcTryAcquireExclusiveLockResponse tryAcquireExclusiveLockResponse = await service.TryAcquireExclusiveLockInternal(request.TryAcquireExclusiveLock, context);

            responses.TryWrite(new GrpcBatchClientKeyValueResponse
            {
                Type = GrpcClientBatchType.TryAcquireExclusiveLock,
                RequestId = request.RequestId,
                TryAcquireExclusiveLock = tryAcquireExclusiveLockResponse
            });
        }
        catch (Exception ex)
        {
            ObserveFault(request, responses, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task TryExecuteTransactionScriptDelayed(
        GrpcBatchClientKeyValueRequest request,
        ChannelWriter<GrpcBatchClientKeyValueResponse> responses,
        ServerCallContext context,
        StreamDrain drain
    )
    {
        try
        {
            GrpcTryExecuteTransactionScriptResponse tryExecuteTransactionScriptResponse = await service.TryExecuteTransactionScriptInternal(request.TryExecuteTransactionScript, context);

            responses.TryWrite(new GrpcBatchClientKeyValueResponse
            {
                Type = GrpcClientBatchType.TryExecuteTransactionScript,
                RequestId = request.RequestId,
                TryExecuteTransactionScript = tryExecuteTransactionScriptResponse
            });
        }
        catch (Exception ex)
        {
            ObserveFault(request, responses, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task TryGetByBucketDelayed(
        GrpcBatchClientKeyValueRequest request,
        ChannelWriter<GrpcBatchClientKeyValueResponse> responses,
        ServerCallContext context,
        StreamDrain drain
    )
    {
        try
        {
            GrpcGetByBucketResponse getByBucketResponse = await service.GetByBucketInternal(request.GetByBucket, context);

            responses.TryWrite(new GrpcBatchClientKeyValueResponse
            {
                Type = GrpcClientBatchType.TryGetByBucket,
                RequestId = request.RequestId,
                GetByBucket = getByBucketResponse
            });
        }
        catch (Exception ex)
        {
            ObserveFault(request, responses, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task TryScanAllByPrefixDelayed(
        GrpcBatchClientKeyValueRequest request,
        ChannelWriter<GrpcBatchClientKeyValueResponse> responses,
        ServerCallContext context,
        StreamDrain drain
    )
    {
        try
        {
            GrpcScanAllByPrefixResponse scanAllByPrefixResponse = await service.ScanAllByPrefixInternal(request.ScanByPrefix, context);

            responses.TryWrite(new GrpcBatchClientKeyValueResponse
            {
                Type = GrpcClientBatchType.TryScanByPrefix,
                RequestId = request.RequestId,
                ScanByPrefix = scanAllByPrefixResponse
            });
        }
        catch (Exception ex)
        {
            ObserveFault(request, responses, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task TryStartTransactionDelayed(
        GrpcBatchClientKeyValueRequest request,
        ChannelWriter<GrpcBatchClientKeyValueResponse> responses,
        ServerCallContext context,
        StreamDrain drain
    )
    {
        try
        {
            GrpcStartTransactionResponse startTransactionResponse = await service.StartTransactionInternal(request.StartTransaction, context);

            responses.TryWrite(new GrpcBatchClientKeyValueResponse
            {
                Type = GrpcClientBatchType.TryStartTransaction,
                RequestId = request.RequestId,
                StartTransaction = startTransactionResponse
            });
        }
        catch (Exception ex)
        {
            ObserveFault(request, responses, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task TryCommitTransactionDelayed(
        GrpcBatchClientKeyValueRequest request,
        ChannelWriter<GrpcBatchClientKeyValueResponse> responses,
        ServerCallContext context,
        StreamDrain drain
    )
    {
        try
        {
            GrpcCommitTransactionResponse commitTransactionResponse = await service.CommitTransactionInternal(request.CommitTransaction, context);

            responses.TryWrite(new GrpcBatchClientKeyValueResponse
            {
                Type = GrpcClientBatchType.TryCommitTransaction,
                RequestId = request.RequestId,
                CommitTransaction = commitTransactionResponse
            });
        }
        catch (Exception ex)
        {
            ObserveFault(request, responses, ex);
        }
        finally
        {
            drain.Exit();
        }
    }

    private async Task TryRollbackTransactionDelayed(
        GrpcBatchClientKeyValueRequest request,
        ChannelWriter<GrpcBatchClientKeyValueResponse> responses,
        ServerCallContext context,
        StreamDrain drain
    )
    {
        try
        {
            GrpcRollbackTransactionResponse rollbackTransactionResponse = await service.RollbackTransactionInternal(request.RollbackTransaction, context);

            responses.TryWrite(new GrpcBatchClientKeyValueResponse
            {
                Type = GrpcClientBatchType.TryRollbackTransaction,
                RequestId = request.RequestId,
                RollbackTransaction = rollbackTransactionResponse
            });
        }
        catch (Exception ex)
        {
            ObserveFault(request, responses, ex);
        }
        finally
        {
            drain.Exit();
        }
    }
}
