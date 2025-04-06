
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
        
        (LockResponseType response, long fencingToken)  = await locks.LocateAndTryLock(
            request.Resource, 
            request.Owner?.ToByteArray()!, 
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
        
        (LockResponseType response, long fencingToken)  = await locks.LocateAndTryExtendLock(
            request.Resource, 
            request.Owner?.ToByteArray()!, 
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
        
        int partitionId = raft.GetPartitionKey(request.Resource);

        if (!raft.Joined || await raft.AmILeader(partitionId, context.CancellationToken))
        {
            LockResponseType response = await locks.TryUnlock(request.Resource, request.Owner?.ToByteArray() ?? [], (LockDurability)request.Durability);

            return new()
            {
                Type = (GrpcLockResponseType)response
            };
        }
            
        string leader = await raft.WaitForLeader(partitionId, context.CancellationToken);
        if (leader == raft.GetLocalEndpoint())
            return new()
            {
                Type = GrpcLockResponseType.LockResponseTypeMustRetry
            };
        
        logger.LogDebug("UNLOCK Redirect {LockName} to leader partition {Partition} at {Leader}", request.Resource, partitionId, leader);
        
        GrpcChannel channel = SharedChannels.GetChannel(leader, configuration);
        
        Locker.LockerClient client = new(channel);
        
        GrpcUnlockResponse? remoteResponse = await client.UnlockAsync(request);
        remoteResponse.ServedFrom = $"https://{leader}";
        return remoteResponse;
    }
    
    public override async Task<GrpcGetLockResponse> GetLock(GrpcGetLockRequest request, ServerCallContext context)
    {
        if (string.IsNullOrEmpty(request.Resource))
            return new()
            {
                Type = GrpcLockResponseType.LockResponseTypeInvalidInput
            };
        
        int partitionId = raft.GetPartitionKey(request.Resource);

        if (!raft.Joined || await raft.AmILeader(partitionId, context.CancellationToken))
        {
            (LockResponseType type, ReadOnlyLockContext? lockContext) = await locks.GetLock(request.Resource, (LockDurability)request.Durability);
            if (type != LockResponseType.Got)
                return new()
                {
                    Type = (GrpcLockResponseType)type
                };

            return new()
            {
                ServedFrom = "",
                Type = (GrpcLockResponseType)type,
                Owner = lockContext?.Owner is not null ? UnsafeByteOperations.UnsafeWrap(lockContext.Owner) : null,
                FencingToken = lockContext?.FencingToken ?? 0,
                ExpiresPhysical = lockContext?.Expires.L ?? 0,
                ExpiresCounter = lockContext?.Expires.C ?? 0,
            };
        }
            
        string leader = await raft.WaitForLeader(partitionId, context.CancellationToken);
        if (leader == raft.GetLocalEndpoint())
            return new()
            {
                Type = GrpcLockResponseType.LockResponseTypeMustRetry
            };
        
        logger.LogDebug("GET-LOCK Redirect {LockName} to leader partition {Partition} at {Leader}", request.Resource, partitionId, leader);
        
        GrpcChannel channel = SharedChannels.GetChannel(leader, configuration);
        
        Locker.LockerClient client = new(channel);
        
        GrpcGetLockResponse? remoteResponse = await client.GetLockAsync(request);
        remoteResponse.ServedFrom = $"https://{leader}";
        return remoteResponse;
    }
}
