
using System.Runtime.InteropServices;
using Google.Protobuf;
using Grpc.Core;
using Grpc.Net.Client;
using Kahuna.Communication.Common.Grpc;
using Kahuna.Server.Configuration;
using Kahuna.Server.Locks;
using Kahuna.Shared.Locks;
using Kommander;

namespace Kahuna.Communication.External.Grpc;

public class LocksService : Locker.LockerBase
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
        if (string.IsNullOrEmpty(request.Resource))
            return new()
            {
                Type = GrpcLockResponseType.LockResponseTypeInvalidInput
            };
        
        if (request.Owner is null)
            return new()
            {
                Type = GrpcLockResponseType.LockResponseTypeInvalidInput
            };
        
        if (request.ExpiresMs <= 0)
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
        if (string.IsNullOrEmpty(request.Resource))
            return new()
            {
                Type = GrpcLockResponseType.LockResponseTypeInvalidInput
            };
        
        if (request.Owner is null)
            return new()
            {
                Type = GrpcLockResponseType.LockResponseTypeInvalidInput
            };
        
        if (request.ExpiresMs <= 0)
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
        if (string.IsNullOrEmpty(request.Resource))
            return new()
            {
                Type = GrpcLockResponseType.LockResponseTypeInvalidInput
            };
        
        if (request.Owner is null)
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
}
