
using Kommander;
using Google.Protobuf;
using Grpc.Core;
using Kahuna.Server.Configuration;
using Kahuna.Server.Locks;
using Kahuna.Shared.Locks;
using System.Runtime.InteropServices;
using Kahuna.Server.Locks.Data;

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
    
    public override async Task<GrpcTryLockResponse> TryLock(GrpcTryLockRequest request, ServerCallContext context)
    {
        return await TryLockInternal(request, context);
    }

    private async Task<GrpcTryLockResponse> TryLockInternal(GrpcTryLockRequest request, ServerCallContext context)
    {
        if (string.IsNullOrEmpty(request.Resource) || request.Owner is null || request.ExpiresMs <= 0)
            return new()
            {
                Type = GrpcLockResponseType.LockResponseTypeInvalidInput
            };

        byte[] owner;

        if (MemoryMarshal.TryGetArray(request.Owner.Memory, out ArraySegment<byte> segment))
            owner = segment.Array ?? request.Owner.ToByteArray();
        else
            owner = request.Owner.ToByteArray();
        
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
    
    public override async Task<GrpcExtendLockResponse> TryExtendLock(GrpcExtendLockRequest request, ServerCallContext context)
    {
        return await TryExtendLockInternal(request, context);
    }

    private async Task<GrpcExtendLockResponse> TryExtendLockInternal(GrpcExtendLockRequest request, ServerCallContext context)
    {
        if (string.IsNullOrEmpty(request.Resource) || request.Owner is null || request.ExpiresMs <= 0)
            return new()
            {
                Type = GrpcLockResponseType.LockResponseTypeInvalidInput
            };

        byte[] owner;

        if (MemoryMarshal.TryGetArray(request.Owner.Memory, out ArraySegment<byte> segment))
            owner = segment.Array ?? request.Owner.ToByteArray();
        else
            owner = request.Owner.ToByteArray();
        
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
    
    public override async Task<GrpcUnlockResponse> Unlock(GrpcUnlockRequest request, ServerCallContext context)
    {
        return await UnlockInternal(request, context);
    }

    private async Task<GrpcUnlockResponse> UnlockInternal(GrpcUnlockRequest request, ServerCallContext context)
    {
        if (string.IsNullOrEmpty(request.Resource) || request.Owner is null)
            return new()
            {
                Type = GrpcLockResponseType.LockResponseTypeInvalidInput
            };

        byte[] owner;

        if (MemoryMarshal.TryGetArray(request.Owner.Memory, out ArraySegment<byte> segment))
            owner = segment.Array ?? request.Owner.ToByteArray();
        else
            owner = request.Owner.ToByteArray();
        
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
    
    public override async Task<GrpcGetLockResponse> GetLock(GrpcGetLockRequest request, ServerCallContext context)
    {
        return await GetLockInternal(request, context);
    }

    private async Task<GrpcGetLockResponse> GetLockInternal(GrpcGetLockRequest request, ServerCallContext context)
    {
        if (string.IsNullOrEmpty(request.Resource))
            return new()
            {
                Type = GrpcLockResponseType.LockResponseTypeInvalidInput
            };
        
        (LockResponseType type, ReadOnlyLockContext? lockContext) = await locks.LocateAndGetLock(
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
        try
        {
            List<Task> tasks = [];

            using SemaphoreSlim semaphore = new(1, 1);

            await foreach (GrpcBatchClientLockRequest request in requestStream.ReadAllAsync())
            {
                switch (request.Type)
                {
                    case GrpcLockClientBatchType.TypeTryLock:
                    {
                        GrpcTryLockRequest? lockRequest = request.TryLock;

                        tasks.Add(TryLockDelayed(semaphore, request.RequestId, lockRequest, responseStream, context));
                    }
                    break;
                    
                    case GrpcLockClientBatchType.TypeUnlock:
                    {
                        GrpcUnlockRequest? unlockRequest = request.Unlock;

                        tasks.Add(TryUnlockDelayed(semaphore, request.RequestId, unlockRequest, responseStream, context));
                    }
                    break;
                    
                    case GrpcLockClientBatchType.TypeExtendLock:
                    {
                        GrpcExtendLockRequest? extendLockRequest = request.ExtendLock;

                        tasks.Add(TryExtendLockDelayed(semaphore, request.RequestId, extendLockRequest, responseStream, context));
                    }
                    break;
                    
                    case GrpcLockClientBatchType.TypeGetLock:
                    {
                        GrpcGetLockRequest? getLockRequest = request.GetLock;

                        tasks.Add(TryGetLockDelayed(semaphore, request.RequestId, getLockRequest, responseStream, context));
                    }
                    break;

                    case GrpcLockClientBatchType.TypeNone:
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
    
    public override async Task BatchServerLockRequests(
        IAsyncStreamReader<GrpcBatchServerLockRequest> requestStream,
        IServerStreamWriter<GrpcBatchServerLockResponse> responseStream, 
        ServerCallContext context
    )
    {
        try
        {
            List<Task> tasks = [];

            using SemaphoreSlim semaphore = new(1, 1);

            await foreach (GrpcBatchServerLockRequest request in requestStream.ReadAllAsync())
            {
                switch (request.Type)
                {
                    case GrpcLockServerBatchType.ServerTypeTryLock:
                    {
                        GrpcTryLockRequest? lockRequest = request.TryLock;

                        tasks.Add(TryLockServerDelayed(semaphore, request.RequestId, lockRequest, responseStream, context));
                    }
                    break;
                    
                    case GrpcLockServerBatchType.ServerTypeUnlock:
                    {
                        GrpcUnlockRequest? unlockRequest = request.Unlock;

                        tasks.Add(TryUnlockServerDelayed(semaphore, request.RequestId, unlockRequest, responseStream, context));
                    }
                    break;
                    
                    case GrpcLockServerBatchType.ServerTypeExtendLock:
                    {
                        GrpcExtendLockRequest? extendLockRequest = request.ExtendLock;

                        tasks.Add(ExtendLockServerDelayed(semaphore, request.RequestId, extendLockRequest, responseStream, context));
                    }
                    break;
                    
                    case GrpcLockServerBatchType.ServerTypeGetLock:
                    {
                        GrpcGetLockRequest? getLockRequest = request.GetLock;

                        tasks.Add(GetLockServerDelayed(semaphore, request.RequestId, getLockRequest, responseStream, context));
                    }
                    break;

                    case GrpcLockServerBatchType.ServerTypeNone:
                    default:
                        logger.LogError("Unknown batch server request type: {Type}", request.Type);
                        break;
                }
            }

            await Task.WhenAll(tasks);
        }
        catch (IOException ex)
        {
            logger.LogTrace("IOException: {Message}", ex.Message);
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
