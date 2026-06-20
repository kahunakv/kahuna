
using Kahuna.Server.Communication.Internode;
using Kommander;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.Locks.Data;
using Kahuna.Server.Locks.Logging;
using Kahuna.Shared.Locks;

namespace Kahuna.Server.Locks;

/// <summary>
/// A core class responsible for locating the appropriate node or partition to handle lock-related operations.
/// </summary>
internal sealed class LockLocator
{
    private readonly LockManager manager;

    private readonly KahunaConfiguration configuration;

    private readonly IRaft raft;

    private readonly DataPartitionRouter dataPartitionRouter;

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
        this.dataPartitionRouter = new DataPartitionRouter(raft);
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
        if (string.IsNullOrEmpty(resource) || owner.Length == 0 || expiresMs <= 0)
            return (LockResponseType.InvalidInput, 0);

        int partitionId = dataPartitionRouter.Locate(resource);

        if (!raft.Joined)
            return (LockResponseType.MustRetry, 0);

        if (await raft.AmILeader(partitionId, cancellationToken))
            return await manager.TryLock(resource, owner, expiresMs, durability);

        string leader = await raft.WaitForLeader(partitionId, cancellationToken);
        if (leader == raft.GetLocalEndpoint())
            return await manager.TryLock(resource, owner, expiresMs, durability);

        logger.LogLockRedirect(resource, partitionId, leader);

        return await interNodeCommunication.TryLock(leader, resource, owner, expiresMs, durability, cancellationToken);
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
    public async Task<(LockResponseType, long)> LocateAndTryExtendLock(string resource, byte[] owner, int expiresMs, LockDurability durability, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(resource) || owner.Length == 0 || expiresMs <= 0)
            return (LockResponseType.InvalidInput, 0);

        int partitionId = dataPartitionRouter.Locate(resource);

        if (!raft.Joined)
            return (LockResponseType.MustRetry, 0);

        if (await raft.AmILeader(partitionId, cancellationToken))
            return await manager.TryExtendLock(resource, owner, expiresMs, durability);

        string leader = await raft.WaitForLeader(partitionId, cancellationToken);
        if (leader == raft.GetLocalEndpoint())
            return await manager.TryExtendLock(resource, owner, expiresMs, durability);

        logger.LogExtendLockRedirect(resource, partitionId, leader);

        return await interNodeCommunication.TryExtendLock(leader, resource, owner, expiresMs, durability, cancellationToken);
    }

    /// <summary>
    /// Locates the leader node for the given key and passes a TryUnlock request to the locker actor for the given lock name.
    /// </summary>
    /// <param name="resource"></param>
    /// <param name="owner"></param>
    /// <param name="expiresMs"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public async Task<LockResponseType> LocateAndTryUnlock(string resource, byte[] owner, LockDurability durability, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(resource) || owner.Length == 0)
            return LockResponseType.InvalidInput;

        int partitionId = dataPartitionRouter.Locate(resource);

        if (!raft.Joined)
            return LockResponseType.MustRetry;

        if (await raft.AmILeader(partitionId, cancellationToken))
            return await manager.TryUnlock(resource, owner, durability);

        string leader = await raft.WaitForLeader(partitionId, cancellationToken);
        if (leader == raft.GetLocalEndpoint())
            return await manager.TryUnlock(resource, owner, durability);

        logger.LogExtendLockRedirect(resource, partitionId, leader);

        return await interNodeCommunication.TryUnlock(leader, resource, owner, durability, cancellationToken);
    }

    /// <summary>
    /// Locates the leader node for the given key and passes a TryGetLock request to the locker actor for the given lock name.
    /// </summary>
    /// <param name="resource"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public async Task<(LockResponseType, ReadOnlyLockEntry?)> LocateAndGetLock(string resource, LockDurability durability, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(resource))
            return (LockResponseType.InvalidInput, null);

        int partitionId = dataPartitionRouter.Locate(resource);

        if (!raft.Joined)
            return (LockResponseType.MustRetry, null);

        if (await raft.AmILeader(partitionId, cancellationToken))
            return await manager.GetLock(resource, durability);

        string leader = await raft.WaitForLeader(partitionId, cancellationToken);
        if (leader == raft.GetLocalEndpoint())
            return await manager.GetLock(resource, durability);

        logger.LogGetLockRedirect(resource, partitionId, leader);

        return await interNodeCommunication.GetLock(leader, resource, durability, cancellationToken);
    }
}