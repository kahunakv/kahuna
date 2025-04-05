
using Kahuna.Server.Communication.Internode;
using Kommander;
using Kahuna.Server.Configuration;
using Kahuna.Shared.Locks;

namespace Kahuna.Server.Locks;

internal sealed class LockLocator
{
    private readonly LockManager manager;

    private readonly KahunaConfiguration configuration;

    private readonly IRaft raft;
    
    private readonly IInterNodeCommunication interNodeCommunication;

    private readonly ILogger<IKahuna> logger;

    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="manager"></param>
    /// <param name="configuration"></param>
    /// <param name="raft"></param>
    /// <param name="interNodeCommunication"></param>
    /// <param name="logger"></param>
    public LockLocator(LockManager manager, KahunaConfiguration configuration, IRaft raft, IInterNodeCommunication interNodeCommunication, ILogger<IKahuna> logger)
    {
        this.manager = manager;
        this.configuration = configuration;
        this.raft = raft;
        this.interNodeCommunication = interNodeCommunication;
        this.logger = logger;
    }
    
    /// <summary>
    /// Locates the leader node for the given key and passes a TryLock request to the locker actor for the given lock name.
    /// </summary>
    /// <param name="resource"></param>
    /// <param name="owner"></param>
    /// <param name="expiresMs"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public async Task<(LockResponseType, long)> LocateAndTryLock(string resource, byte[] owner, int expiresMs, LockDurability durability, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(resource))
            return (LockResponseType.InvalidInput, 0);
        
        if (string.IsNullOrEmpty(resource))
            return (LockResponseType.InvalidInput, 0);
        
        if (expiresMs <= 0)
            return (LockResponseType.InvalidInput, 0);
        
        int partitionId = raft.GetPartitionKey(resource);

        if (!raft.Joined || await raft.AmILeader(partitionId, cancellationToken))
            return await manager.TryLock(resource, owner, expiresMs, durability);
            
        string leader = await raft.WaitForLeader(partitionId, cancellationToken);
        if (leader == raft.GetLocalEndpoint())
            return (LockResponseType.MustRetry, 0);
        
        logger.LogDebug("LOCK Redirect {LockName} to leader partition {Partition} at {Leader}", resource, partitionId, leader);

        return await interNodeCommunication.TryLock(leader, resource, owner, expiresMs, durability, cancellationToken);
    }
}