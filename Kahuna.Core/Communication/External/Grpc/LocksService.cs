
using Kommander;
using Google.Protobuf;
using System.Runtime.CompilerServices;

using Grpc.Core;
using Kahuna.Server.Communication.Internode;
using Kahuna.Server.Configuration;
using Kahuna.Server.Locks;
using Kahuna.Shared.Communication.Grpc;
using Kahuna.Shared.Locks;
using System.Runtime.InteropServices;
using Kahuna.Server.Locks.Data;
using Kahuna.Communication.External.Grpc.Logging;

namespace Kahuna.Communication.External.Grpc;

/// <summary>
/// Provides gRPC service for managing distributed locks. This service allows creating, extending,
/// releasing, and retrieving locks using gRPC requests and responses.
/// </summary>
/// <remarks>
/// The LocksService class extends `Locker.LockerBase` to implement the required gRPC functionality for managing locks.
/// It interacts with the lock management system and provides functionality to both individual
/// and batch operations. The service is used for distributed environments where coordination is needed.
/// </remarks>
public sealed class LocksService : Locker.LockerBase
{
    private readonly IKahuna locks;

    private readonly KahunaConfiguration configuration;

    private readonly IRaft raft; 
    
    private readonly ILogger<IKahuna> logger;
    
    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="locks"></param>
    /// <param name="configuration"></param>
    /// <param name="raft"></param>
    /// <param name="logger"></param>
    public LocksService(IKahuna locks, KahunaConfiguration configuration, IRaft raft, ILogger<IKahuna> logger)
    {
        this.locks = locks;
        this.configuration = configuration;
        this.raft = raft;
        this.logger = logger;
    }
    
    // The unary overrides return the guarded task directly: an async wrapper here would only
    // re-await it, adding one state machine and one wrapper task per call.
    public override Task<GrpcTryLockResponse> TryLock(GrpcTryLockRequest request, ServerCallContext context)
    {
        return TryLockInternal(request, context);
    }

    private async Task<GrpcTryLockResponse> TryLockCore(GrpcTryLockRequest request, ServerCallContext context)
    {
        if (string.IsNullOrEmpty(request.Resource) || request.Owner is null || request.ExpiresMs <= 0)
            return new()
            {
                Type = GrpcLockResponseType.LockResponseTypeInvalidInput
            };

        byte[] owner;

        owner = ByteStringPayload.GetArray(request.Owner);
        
        (LockResponseType response, long fencingToken)  = await locks.LocateAndTryLock(
            request.Resource, 
            owner, 
            request.ExpiresMs, 
            (LockDurability) request.Durability, 
            context.CancellationToken
        );

        return new()
        {
            Type = (GrpcLockResponseType)response,
            FencingToken = fencingToken,
            ServedFrom = ""
        };
    }
    
    public override Task<GrpcExtendLockResponse> TryExtendLock(GrpcExtendLockRequest request, ServerCallContext context)
    {
        return TryExtendLockInternal(request, context);
    }

    private async Task<GrpcExtendLockResponse> TryExtendLockCore(GrpcExtendLockRequest request, ServerCallContext context)
    {
        if (string.IsNullOrEmpty(request.Resource) || request.Owner is null || request.ExpiresMs <= 0)
            return new()
            {
                Type = GrpcLockResponseType.LockResponseTypeInvalidInput
            };

        byte[] owner;

        owner = ByteStringPayload.GetArray(request.Owner);
        
        (LockResponseType response, long fencingToken) = await locks.LocateAndTryExtendLock(
            request.Resource, 
            owner, 
            request.ExpiresMs, 
            (LockDurability)request.Durability, 
            context.CancellationToken
        );

        return new()
        {
            Type = (GrpcLockResponseType)response,
            FencingToken = fencingToken,
            ServedFrom = ""
        };
    }
    
    public override Task<GrpcUnlockResponse> Unlock(GrpcUnlockRequest request, ServerCallContext context)
    {
        return UnlockInternal(request, context);
    }

    private async Task<GrpcUnlockResponse> UnlockCore(GrpcUnlockRequest request, ServerCallContext context)
    {
        if (string.IsNullOrEmpty(request.Resource) || request.Owner is null)
            return new()
            {
                Type = GrpcLockResponseType.LockResponseTypeInvalidInput
            };

        byte[] owner;

        owner = ByteStringPayload.GetArray(request.Owner);
        
        LockResponseType response = await locks.LocateAndTryUnlock(
            request.Resource, 
            owner, 
            (LockDurability)request.Durability, 
            context.CancellationToken
        );

        return new()
        {
            Type = (GrpcLockResponseType)response,
            ServedFrom = ""
        };
    }
    
    public override Task<GrpcGetLockResponse> GetLock(GrpcGetLockRequest request, ServerCallContext context)
    {
        return GetLockInternal(request, context);
    }

    private async Task<GrpcGetLockResponse> GetLockCore(GrpcGetLockRequest request, ServerCallContext context)
    {
        if (string.IsNullOrEmpty(request.Resource))
            return new()
            {
                Type = GrpcLockResponseType.LockResponseTypeInvalidInput
            };
        
        (LockResponseType type, ReadOnlyLockEntry? lockContext) = await locks.LocateAndGetLock(
            request.Resource, 
            (LockDurability)request.Durability, 
            context.CancellationToken
        );
        
        if (type != LockResponseType.Got)
            return new()
            {
                Type = (GrpcLockResponseType)type
            };

        return new()
        {
            Type = (GrpcLockResponseType)type,
            Owner = lockContext?.Owner is not null ? UnsafeByteOperations.UnsafeWrap(lockContext.Owner) : null,
            FencingToken = lockContext?.FencingToken ?? 0,
            ExpiresNode = lockContext?.Expires.N ?? 0,
            ExpiresPhysical = lockContext?.Expires.L ?? 0,
            ExpiresCounter = lockContext?.Expires.C ?? 0,
            ServedFrom = ""
        };
    }
    
    public override async Task BatchClientLockRequests(
        IAsyncStreamReader<GrpcBatchClientLockRequest> requestStream,
        IServerStreamWriter<GrpcBatchClientLockResponse> responseStream, 
        ServerCallContext context
    )
    {
        int inFlight = 1;
        TaskCompletionSource drain = new(TaskCreationOptions.RunContinuationsAsynchronously);

        using SemaphoreSlim semaphore = new(1, 1);

        void Track(GrpcBatchClientLockRequest request, Task task)
        {
            Interlocked.Increment(ref inFlight);
            _ = Observe(request, task);
        }

        // A handler that throws must still answer its own RequestId: the SDK matches responses by id,
        // so an unanswered request hangs until its deadline while every other request on the shared
        // stream keeps flowing. Refusing just that one request with MustRetry leaves the stream and
        // its neighbours untouched.
        async Task Observe(GrpcBatchClientLockRequest request, Task task)
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
                logger.LogError(ex, "Batch lock client handler faulted");

                try
                {
                    await WriteResponseToStream(semaphore, responseStream, BatchRefusalResponses.ForClientLock(request), context);
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
            await foreach (GrpcBatchClientLockRequest request in requestStream.ReadAllAsync())
            {
                switch (request.Type)
                {
                    case GrpcLockClientBatchType.TypeTryLock:
                    {
                        GrpcTryLockRequest? lockRequest = request.TryLock;

                        Track(request, TryLockDelayed(semaphore, request.RequestId, lockRequest, responseStream, context));
                    }
                    break;
                    
                    case GrpcLockClientBatchType.TypeUnlock:
                    {
                        GrpcUnlockRequest? unlockRequest = request.Unlock;

                        Track(request, TryUnlockDelayed(semaphore, request.RequestId, unlockRequest, responseStream, context));
                    }
                    break;
                    
                    case GrpcLockClientBatchType.TypeExtendLock:
                    {
                        GrpcExtendLockRequest? extendLockRequest = request.ExtendLock;

                        Track(request, TryExtendLockDelayed(semaphore, request.RequestId, extendLockRequest, responseStream, context));
                    }
                    break;
                    
                    case GrpcLockClientBatchType.TypeGetLock:
                    {
                        GrpcGetLockRequest? getLockRequest = request.GetLock;

                        Track(request, TryGetLockDelayed(semaphore, request.RequestId, getLockRequest, responseStream, context));
                    }
                    break;

                    case GrpcLockClientBatchType.TypeNone:
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

    /// <summary>Serializes a write onto the shared client response stream: gRPC allows only one
    /// write at a time, and batched handlers complete out of order.</summary>
    private static async Task WriteResponseToStream(
        SemaphoreSlim semaphore,
        IServerStreamWriter<GrpcBatchClientLockResponse> responseStream,
        GrpcBatchClientLockResponse response,
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

    /// <summary>Serializes a write onto the shared inter-node response stream.</summary>
    private static async Task WriteResponseToStream(
        SemaphoreSlim semaphore,
        IServerStreamWriter<GrpcBatchServerLockResponse> responseStream,
        GrpcBatchServerLockResponse response,
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

    private async Task TryLockDelayed(
        SemaphoreSlim semaphore,
        int requestId,
        GrpcTryLockRequest lockRequest, 
        IServerStreamWriter<GrpcBatchClientLockResponse> responseStream,
        ServerCallContext context
    )
    {
        GrpcTryLockResponse tryLockResponse = await TryLockInternal(lockRequest, context);
        
        GrpcBatchClientLockResponse response = new()
        {
            Type = GrpcLockClientBatchType.TypeTryLock,
            RequestId = requestId,
            TryLock = tryLockResponse
        };

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
    
    private async Task TryUnlockDelayed(
        SemaphoreSlim semaphore, 
        int requestId, 
        GrpcUnlockRequest unlockRequest, 
        IServerStreamWriter<GrpcBatchClientLockResponse> responseStream,
        ServerCallContext context
    )
    {
        GrpcUnlockResponse unlockResponse = await UnlockInternal(unlockRequest, context);
        
        GrpcBatchClientLockResponse response = new()
        {
            Type = GrpcLockClientBatchType.TypeUnlock,
            RequestId = requestId,
            Unlock = unlockResponse
        };

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
    
    private async Task TryExtendLockDelayed(
        SemaphoreSlim semaphore, 
        int requestId, 
        GrpcExtendLockRequest extendLockRequest, 
        IServerStreamWriter<GrpcBatchClientLockResponse> responseStream,
        ServerCallContext context
    )
    {
        GrpcExtendLockResponse extendLockResponse = await TryExtendLockInternal(extendLockRequest, context);
        
        GrpcBatchClientLockResponse response = new()
        {
            Type = GrpcLockClientBatchType.TypeExtendLock,
            RequestId = requestId,
            ExtendLock = extendLockResponse
        };

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
    
    private async Task TryGetLockDelayed(
        SemaphoreSlim semaphore, 
        int requestId, 
        GrpcGetLockRequest getLockRequest, 
        IServerStreamWriter<GrpcBatchClientLockResponse> responseStream,
        ServerCallContext context
    )
    {
        GrpcGetLockResponse getLockResponse = await GetLockInternal(getLockRequest, context);
        
        GrpcBatchClientLockResponse response = new()
        {
            Type = GrpcLockClientBatchType.TypeGetLock,
            RequestId = requestId,
            GetLock = getLockResponse
        };

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
    
    public override async Task BatchServerLockRequests(
        IAsyncStreamReader<GrpcBatchServerLockRequest> requestStream,
        IServerStreamWriter<GrpcBatchServerLockResponse> responseStream,
        ServerCallContext context
    )
    {
        int inFlight = 1;
        TaskCompletionSource drain = new(TaskCreationOptions.RunContinuationsAsynchronously);

        using SemaphoreSlim semaphore = new(1, 1);

        void Track(GrpcBatchServerLockRequest request, Task task)
        {
            Interlocked.Increment(ref inFlight);
            _ = Observe(request, task);
        }

        // A handler that throws must still answer its own RequestId: the caller matches responses by id,
        // so an unanswered request hangs until its deadline while every other request on the shared
        // stream keeps flowing. Refusing just that one request with MustRetry leaves the stream and
        // its neighbours untouched.
        async Task Observe(GrpcBatchServerLockRequest request, Task task)
        {
            try
            {
                await task;
            }
            catch (Exception ex) when (ex is IOException or OperationCanceledException)
            {
                // The stream is already gone or the caller left; there is nobody left to answer.
                logger.LogCommunicationIoException(ex);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Batch lock server handler faulted");

                try
                {
                    await WriteResponseToStream(semaphore, responseStream, BatchRefusalResponses.ForServerLock(request), context);
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
            await foreach (GrpcBatchServerLockRequest request in requestStream.ReadAllAsync())
            {
                // Serve each request under the hop count its sender stamped, so the forward budget
                // spans the whole chain instead of restarting at this process boundary. Each
                // handler captures the marker when it is created inside the scope; the next
                // request on this shared stream may carry a different count.
                using (Kahuna.Server.ForwardedRequestScope.EnterAt(request.ForwardHops))
                {
                    switch (request.Type)
                    {
                        case GrpcLockServerBatchType.ServerTypeTryLock:
                        {
                            GrpcTryLockRequest? lockRequest = request.TryLock;

                            Track(request, TryLockServerDelayed(semaphore, request.RequestId, lockRequest, responseStream, context));
                        }
                        break;
                    
                        case GrpcLockServerBatchType.ServerTypeUnlock:
                        {
                            GrpcUnlockRequest? unlockRequest = request.Unlock;

                            Track(request, TryUnlockServerDelayed(semaphore, request.RequestId, unlockRequest, responseStream, context));
                        }
                        break;
                    
                        case GrpcLockServerBatchType.ServerTypeExtendLock:
                        {
                            GrpcExtendLockRequest? extendLockRequest = request.ExtendLock;

                            Track(request, ExtendLockServerDelayed(semaphore, request.RequestId, extendLockRequest, responseStream, context));
                        }
                        break;
                    
                        case GrpcLockServerBatchType.ServerTypeGetLock:
                        {
                            GrpcGetLockRequest? getLockRequest = request.GetLock;

                            Track(request, GetLockServerDelayed(semaphore, request.RequestId, getLockRequest, responseStream, context));
                        }
                        break;

                        case GrpcLockServerBatchType.ServerTypeNone:
                        default:
                            logger.LogError("Unknown batch server request type: {Type}", request.Type);
                            break;
                    }
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
    
    private async Task TryLockServerDelayed(
        SemaphoreSlim semaphore, 
        int requestId, 
        GrpcTryLockRequest lockRequest, 
        IServerStreamWriter<GrpcBatchServerLockResponse> responseStream,
        ServerCallContext context
    )
    {
        GrpcTryLockResponse tryLockResponse = await TryLockInternal(lockRequest, context);
        
        GrpcBatchServerLockResponse response = new()
        {
            Type = GrpcLockServerBatchType.ServerTypeTryLock,
            RequestId = requestId,
            TryLock = tryLockResponse
        };

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
    
    private async Task ExtendLockServerDelayed(
        SemaphoreSlim semaphore, 
        int requestId, 
        GrpcExtendLockRequest lockRequest, 
        IServerStreamWriter<GrpcBatchServerLockResponse> responseStream,
        ServerCallContext context
    )
    {
        GrpcExtendLockResponse extendLockResponse = await TryExtendLockInternal(lockRequest, context);
        
        GrpcBatchServerLockResponse response = new()
        {
            Type = GrpcLockServerBatchType.ServerTypeExtendLock,
            RequestId = requestId,
            ExtendLock = extendLockResponse
        };

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
    
    private async Task TryUnlockServerDelayed(
        SemaphoreSlim semaphore, 
        int requestId, 
        GrpcUnlockRequest setKeyRequest, 
        IServerStreamWriter<GrpcBatchServerLockResponse> responseStream,
        ServerCallContext context
    )
    {
        GrpcUnlockResponse unlockResponse = await UnlockInternal(setKeyRequest, context);
        
        GrpcBatchServerLockResponse response = new()
        {
            Type = GrpcLockServerBatchType.ServerTypeUnlock,
            RequestId = requestId,
            Unlock = unlockResponse
        };

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
    
    private async Task GetLockServerDelayed(
        SemaphoreSlim semaphore, 
        int requestId, 
        GrpcGetLockRequest lockRequest, 
        IServerStreamWriter<GrpcBatchServerLockResponse> responseStream,
        ServerCallContext context
    )
    {
        GrpcGetLockResponse getLockResponse = await GetLockInternal(lockRequest, context);
        
        GrpcBatchServerLockResponse response = new()
        {
            Type = GrpcLockServerBatchType.ServerTypeGetLock,
            RequestId = requestId,
            GetLock = getLockResponse
        };

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

    // ── Retryable-failure guards ────────────────────────────────────────────────────────────────
    //
    // A retry loop must always receive a classifiable answer. A Raft resolution failure or an
    // inter-node transport failure means no definitive answer was produced; left unguarded it
    // reaches a unary caller as gRPC status Unknown (which clients do not retry) and leaves a
    // batched caller waiting for a response that never comes. Both entry points — the unary
    // overrides and the batchers' delayed handlers — go through these guards. Genuine bugs are not
    // retryable and keep propagating.

    private async Task<TResponse> Guard<TRequest, TResponse>(
        TRequest request,
        ServerCallContext context,
        Func<LocksService, TRequest, ServerCallContext, Task<TResponse>> handler,
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

    private Task<GrpcTryLockResponse> TryLockInternal(GrpcTryLockRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.TryLockCore(r, c), static _ => LockMustRetry.TryLock());

    private Task<GrpcExtendLockResponse> TryExtendLockInternal(GrpcExtendLockRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.TryExtendLockCore(r, c), static _ => LockMustRetry.ExtendLock());

    private Task<GrpcUnlockResponse> UnlockInternal(GrpcUnlockRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.UnlockCore(r, c), static _ => LockMustRetry.Unlock());

    private Task<GrpcGetLockResponse> GetLockInternal(GrpcGetLockRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.GetLockCore(r, c), static _ => LockMustRetry.GetLock());
}
