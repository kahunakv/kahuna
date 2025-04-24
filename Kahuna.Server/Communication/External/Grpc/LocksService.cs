
using Kommander;
using Google.Protobuf;
using Grpc.Core;
using Kahuna.Server.Configuration;
using Kahuna.Server.Locks;
using Kahuna.Shared.Locks;
using System.Runtime.InteropServices;

namespace Kahuna.Communication.External.Grpc;

public sealed class LocksService : Locker.LockerBase
{
    private readonly IKahuna locks;

    private readonly KahunaConfiguration configuration;

    private readonly IRaft raft; 
    
    private readonly ILogger<IKahuna> logger;
    
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

    public async Task<GrpcUnlockResponse> UnlockInternal(GrpcUnlockRequest request, ServerCallContext context)
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
        GrpcTryLockRequest setKeyRequest, 
        IServerStreamWriter<GrpcBatchClientLockResponse> responseStream,
        ServerCallContext context
    )
    {
        GrpcTryLockResponse tryLockResponse = await TryLockInternal(setKeyRequest, context);
        
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
}
