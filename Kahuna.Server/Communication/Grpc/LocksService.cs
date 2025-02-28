
using Grpc.Core;
using Kahuna.Locks;
using Kommander;

namespace Kahuna.Communication.Grpc;

public class LocksService : Locker.LockerBase
{
    private readonly IKahuna locks;

    private readonly IRaft raft; 
    
    private readonly ILogger<IKahuna> logger;
    
    public LocksService(IKahuna locks, IRaft raft, ILogger<IKahuna> logger)
    {
        this.locks = locks;
        this.raft = raft;
        this.logger = logger;
    }
    
    public override async Task<TryLockResponse> TryLock(TryLockRequest request, ServerCallContext context)
    {
        int partitionId = raft.GetPartitionKey(request.LockName);

        if (!raft.Joined || await raft.AmILeader(partitionId, CancellationToken.None))
        {
            (LockResponseType response, long fencingToken) = await locks.TryLock(request.LockName, request.LockId, request.ExpiresMs, (LockConsistency)request.Consistency);

            return new()
            {
                Type = (GrpcLockResponseType)response,
                FencingToken = fencingToken
            };
        }
            
        string leader = await raft.WaitForLeader(partitionId, CancellationToken.None);
        if (leader == raft.GetLocalEndpoint())
            return new()
            {
                Type = GrpcLockResponseType.LockResponseTypeMustRetry
            };
        
        logger.LogInformation("LOCK Redirect {LockName} to leader partition {Partition} at {Leader}", request.LockName, partitionId, leader);

        return new()
        {
            Type = GrpcLockResponseType.LockResponseTypeMustRetry
        };
    }
}