using System.Runtime.InteropServices;
using Kahuna.Server.Locks.Data;
using Kahuna.Server.Persistence;
using Kahuna.Server.Replication;
using Kahuna.Server.Replication.Protos;
using Kahuna.Shared.Communication.Grpc;
using Kahuna.Shared.Locks;
using Kommander;
using Kommander.Data;
using Kommander.Time;
using Nixie;
using Nixie.Routers;

namespace Kahuna.Server.Locks;

/// <summary>
/// The LockReplicator class handles the replication of lock-related requests within a distributed system
/// using a Raft consensus protocol. This includes processing various types of lock operations such as
/// acquiring locks, releasing locks, and extending lock durations.
/// </summary>
internal sealed class LockReplicator
{
    private readonly IActorRef<BackgroundWriterActor, BackgroundWriteRequest> backgroundWriter;

    private readonly IActorRef<ConsistentHashActor<LockActor, LockRequest, LockResponse>, LockRequest, LockResponse>? persistentLocksRouter;

    private readonly IRaft raft;

    private readonly UnflushedLockWritesIndex? unflushedLockWrites;

    private readonly PartitionDurabilityTracker? durabilityTracker;

    private readonly ILogger<IKahuna> logger;

    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="backgroundWriter"></param>
    /// <param name="raft"></param>
    /// <param name="logger"></param>
    /// <param name="unflushedLockWrites"></param>
    /// <param name="durabilityTracker"></param>
    /// <param name="persistentLocksRouter"></param>
    public LockReplicator(
        IActorRef<BackgroundWriterActor, BackgroundWriteRequest> backgroundWriter,
        IRaft raft,
        ILogger<IKahuna> logger,
        UnflushedLockWritesIndex? unflushedLockWrites = null,
        PartitionDurabilityTracker? durabilityTracker = null,
        IActorRef<ConsistentHashActor<LockActor, LockRequest, LockResponse>, LockRequest, LockResponse>? persistentLocksRouter = null)
    {
        this.backgroundWriter = backgroundWriter;
        this.raft = raft;
        this.logger = logger;
        this.unflushedLockWrites = unflushedLockWrites;
        this.durabilityTracker = durabilityTracker;
        this.persistentLocksRouter = persistentLocksRouter;
    }

    /// <summary>
    /// Routes an <c>InvalidateOrApply</c> message to the owning actor in the persistent pool so a
    /// resident in-memory entry advances to this committed mutation. Without it, the actor cache of
    /// a node that lost leadership stays frozen at its last tenure, and a later re-promotion would
    /// mint fencing tokens (or answer reads) from that stale state. Ephemeral locks are never
    /// replicated via Raft, so every entry seen here belongs to the persistent pool.
    /// </summary>
    private void SendInvalidateOrApply(
        int partitionId,
        string resource,
        byte[]? owner,
        long fencingToken,
        HLCTimestamp expires,
        HLCTimestamp lastUsed,
        HLCTimestamp lastModified,
        LockState state)
    {
        persistentLocksRouter?.Send(new(
            LockRequestType.InvalidateOrApply,
            resource,
            owner,
            0,
            LockDurability.Persistent,
            0,
            partitionId,
            null,
            invalidateOrApplyData: new(fencingToken, expires, lastUsed, lastModified, state)
        ));
    }

    /// <summary>
    /// Replicates a lock-related operation within the distributed system by processing
    /// the provided log data and invoking the appropriate action based on the lock request type.
    /// </summary>
    /// <param name="partitionId">
    /// The identifier of the partition to which the lock operation belongs.
    /// </param>
    /// <param name="log">
    /// The Raft log entry containing the lock operation data to be replicated.
    /// </param>
    /// <returns>
    /// A boolean value indicating whether the replication operation was successful.
    /// Returns true if the replication was executed successfully or skipped due to
    /// empty log data. Returns false if an error occurs during processing.
    /// </returns>
    public bool Replicate(int partitionId, RaftLog log)
    {
        if (log.LogData is null || log.LogData.Length == 0)
            return true;
        
        try
        {
            LockMessage lockMessage = ReplicationSerializer.UnserializeLockMessage(log.LogData);

            switch ((LockRequestType)lockMessage.Type)
            {
                case LockRequestType.TryLock:
                {
                    /*PersistenceResponse? response = await persistenceActorRouter.Ask(new(
                        PersistenceRequestType.StoreLock,
                        [
                            new(
                                lockMessage.Resource,
                                lockMessage.Owner?.ToByteArray(),
                                lockMessage.FencingToken,
                                lockMessage.ExpireLogical,
                                lockMessage.ExpireCounter,
                                (int)LockState.Locked
                            )
                        ]
                    ));
                    
                    if (response is null)
                        return false;

                    return response.Type == PersistenceResponseType.Success;*/
                    
                    byte[] owner;

                    owner = ByteStringPayload.GetArray(lockMessage.Owner);
                    
                    // Register before enqueueing: the partition's durability floor must not pass
                    // this entry until its flush lands. Applies run in log-id order here, so the
                    // registration always precedes any watermark advance over this index.
                    durabilityTracker?.RegisterPending(partitionId, log.Id, DurabilityChannel.Flush);

                    // Record before enqueueing so a lock read that misses the actor table observes this
                    // committed mutation even before the background flush lands it in the backend.
                    unflushedLockWrites?.Record(lockMessage.Resource, owner, lockMessage.FencingToken,
                        new(lockMessage.ExpireNode, lockMessage.ExpirePhysical, lockMessage.ExpireCounter),
                        new(lockMessage.LastUsedNode, lockMessage.LastUsedPhysical, lockMessage.LastUsedCounter),
                        new(lockMessage.LastModifiedNode, lockMessage.LastModifiedPhysical, lockMessage.LastModifiedCounter),
                        LockState.Locked);

                    backgroundWriter.Send(BackgroundWriteRequestPool.Rent(
            BackgroundWriteType.QueueStoreLock,
                        partitionId,
                        lockMessage.Resource,
                        owner,
                        lockMessage.FencingToken,
                        new(lockMessage.ExpireNode, lockMessage.ExpirePhysical, lockMessage.ExpireCounter),
                        new(lockMessage.LastUsedNode, lockMessage.LastUsedPhysical, lockMessage.LastUsedCounter),
                        new(lockMessage.LastModifiedNode, lockMessage.LastModifiedPhysical, lockMessage.LastModifiedCounter),
                        (int)LockState.Locked,
                        logIndex: log.Id
                    ));

                    SendInvalidateOrApply(partitionId, lockMessage.Resource, owner, lockMessage.FencingToken,
                        new(lockMessage.ExpireNode, lockMessage.ExpirePhysical, lockMessage.ExpireCounter),
                        new(lockMessage.LastUsedNode, lockMessage.LastUsedPhysical, lockMessage.LastUsedCounter),
                        new(lockMessage.LastModifiedNode, lockMessage.LastModifiedPhysical, lockMessage.LastModifiedCounter),
                        LockState.Locked);

                    return true;
                }

                case LockRequestType.TryUnlock:
                {
                    /*PersistenceResponse? response = await persistenceActorRouter.Ask(new(
                        PersistenceRequestType.StoreLock,
                        [
                            new(
                                lockMessage.Resource,
                                lockMessage.Owner?.ToByteArray(),
                                lockMessage.FencingToken,
                                lockMessage.ExpireLogical,
                                lockMessage.ExpireCounter,
                                (int)LockState.Unlocked
                            )
                        ]
                    ));
                    
                    if (response is null)
                        return false;

                    return response.Type == PersistenceResponseType.Success;*/
                    
                    byte[] owner;

                    owner = ByteStringPayload.GetArray(lockMessage.Owner);
                    
                    // Register before enqueueing: the partition's durability floor must not pass
                    // this entry until its flush lands. Applies run in log-id order here, so the
                    // registration always precedes any watermark advance over this index.
                    durabilityTracker?.RegisterPending(partitionId, log.Id, DurabilityChannel.Flush);

                    // Record before enqueueing so a lock read that misses the actor table observes this
                    // committed mutation even before the background flush lands it in the backend.
                    unflushedLockWrites?.Record(lockMessage.Resource, owner, lockMessage.FencingToken,
                        new(lockMessage.ExpireNode, lockMessage.ExpirePhysical, lockMessage.ExpireCounter),
                        new(lockMessage.LastUsedNode, lockMessage.LastUsedPhysical, lockMessage.LastUsedCounter),
                        new(lockMessage.LastModifiedNode, lockMessage.LastModifiedPhysical, lockMessage.LastModifiedCounter),
                        LockState.Unlocked);

                    backgroundWriter.Send(BackgroundWriteRequestPool.Rent(
            BackgroundWriteType.QueueStoreLock,
                        partitionId,
                        lockMessage.Resource,
                        owner,
                        lockMessage.FencingToken,
                        new(lockMessage.ExpireNode, lockMessage.ExpirePhysical, lockMessage.ExpireCounter),
                        new(lockMessage.LastUsedNode, lockMessage.LastUsedPhysical, lockMessage.LastUsedCounter),
                        new(lockMessage.LastModifiedNode, lockMessage.LastModifiedPhysical, lockMessage.LastModifiedCounter),
                        (int)LockState.Unlocked,
                        logIndex: log.Id
                    ));

                    // Owner is null for the cache apply: an unlock clears the holder, matching the
                    // entry state CompleteProposal installs on the proposing leader.
                    SendInvalidateOrApply(partitionId, lockMessage.Resource, null, lockMessage.FencingToken,
                        new(lockMessage.ExpireNode, lockMessage.ExpirePhysical, lockMessage.ExpireCounter),
                        new(lockMessage.LastUsedNode, lockMessage.LastUsedPhysical, lockMessage.LastUsedCounter),
                        new(lockMessage.LastModifiedNode, lockMessage.LastModifiedPhysical, lockMessage.LastModifiedCounter),
                        LockState.Unlocked);

                    return true;
                }

                case LockRequestType.TryExtendLock:
                {
                    /*PersistenceResponse? response = await persistenceActorRouter.Ask(new(
                        PersistenceRequestType.StoreLock,
                        [
                            new(
                                lockMessage.Resource,
                                lockMessage.Owner?.ToByteArray(),
                                lockMessage.FencingToken,
                                lockMessage.ExpireLogical,
                                lockMessage.ExpireCounter,
                                (int)LockState.Locked
                            )
                        ]
                    ));

                    if (response is null)
                        return false;

                    return response.Type == PersistenceResponseType.Success;*/
                    
                    byte[] owner;

                    owner = ByteStringPayload.GetArray(lockMessage.Owner);
                    
                    // Register before enqueueing: the partition's durability floor must not pass
                    // this entry until its flush lands. Applies run in log-id order here, so the
                    // registration always precedes any watermark advance over this index.
                    durabilityTracker?.RegisterPending(partitionId, log.Id, DurabilityChannel.Flush);

                    // Record before enqueueing so a lock read that misses the actor table observes this
                    // committed mutation even before the background flush lands it in the backend.
                    unflushedLockWrites?.Record(lockMessage.Resource, owner, lockMessage.FencingToken,
                        new(lockMessage.ExpireNode, lockMessage.ExpirePhysical, lockMessage.ExpireCounter),
                        new(lockMessage.LastUsedNode, lockMessage.LastUsedPhysical, lockMessage.LastUsedCounter),
                        new(lockMessage.LastModifiedNode, lockMessage.LastModifiedPhysical, lockMessage.LastModifiedCounter),
                        LockState.Locked);

                    backgroundWriter.Send(BackgroundWriteRequestPool.Rent(
            BackgroundWriteType.QueueStoreLock,
                        partitionId,
                        lockMessage.Resource,
                        owner,
                        lockMessage.FencingToken,
                        new(lockMessage.ExpireNode, lockMessage.ExpirePhysical, lockMessage.ExpireCounter),
                        new(lockMessage.LastUsedNode, lockMessage.LastUsedPhysical, lockMessage.LastUsedCounter),
                        new(lockMessage.LastModifiedNode, lockMessage.LastModifiedPhysical, lockMessage.LastModifiedCounter),
                        (int)LockState.Locked,
                        logIndex: log.Id
                    ));

                    SendInvalidateOrApply(partitionId, lockMessage.Resource, owner, lockMessage.FencingToken,
                        new(lockMessage.ExpireNode, lockMessage.ExpirePhysical, lockMessage.ExpireCounter),
                        new(lockMessage.LastUsedNode, lockMessage.LastUsedPhysical, lockMessage.LastUsedCounter),
                        new(lockMessage.LastModifiedNode, lockMessage.LastModifiedPhysical, lockMessage.LastModifiedCounter),
                        LockState.Locked);

                    return true;
                }

                case LockRequestType.Get:
                case LockRequestType.CompleteProposal:
                case LockRequestType.ReleaseProposal:
                    break;
                
                default:
                    logger.LogError("Unknown replication message type: {Type}", lockMessage.Type);
                    break;
            }
        } 
        catch (Exception ex)
        {
            logger.LogError("{Type}: {Message}\n{StackTrace}", ex.GetType().Name, ex.Message, ex.StackTrace);

            return false;
        }

        return true;
    }
}