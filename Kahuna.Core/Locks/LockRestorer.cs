
using Nixie;
using Nixie.Routers;

using Kommander;
using Kommander.Data;
using Kommander.Time;

using System.Runtime.InteropServices;
using Kahuna.Server.Locks.Data;
using Kahuna.Server.Persistence;
using Kahuna.Server.Replication;
using Kahuna.Server.Replication.Protos;
using Kahuna.Shared.Communication.Grpc;
using Kahuna.Shared.Locks;

namespace Kahuna.Server.Locks;

/// <summary>
/// The LockRestorer class is responsible for restoring lock state based on persisted log data.
/// It deserializes log entries and performs the necessary actions to restore the distributed lock state.
/// </summary>
internal sealed class LockRestorer
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
    public LockRestorer(
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
    /// resident in-memory entry advances to this replayed committed mutation. Restore replays run
    /// on live nodes during a partition leader change, where an actor can hold a warm entry from a
    /// previous leadership tenure; leaving it stale would let a re-promoted leader mint fencing
    /// tokens from state older than the replicated log.
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
    /// Restores the distributed lock state based on the provided partition ID and log entry.
    /// It processes the log data to apply the appropriate lock state changes.
    /// </summary>
    /// <param name="partitionId">The identifier of the partition to restore.</param>
    /// <param name="log">The log containing serialized lock state data.</param>
    /// <returns>True if restoration succeeds or the log contains no data; false otherwise.</returns>
    public bool Restore(int partitionId, RaftLog log)
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
                    // this replayed entry until its flush lands. Replay runs in log-id order, so
                    // the registration always precedes any watermark advance over this index.
                    durabilityTracker?.RegisterPending(partitionId, log.Id, DurabilityChannel.Flush);

                    // Record before enqueueing so a lock read that misses the actor table observes this
                    // replayed committed mutation even before the background flush lands it.
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
                    // this replayed entry until its flush lands. Replay runs in log-id order, so
                    // the registration always precedes any watermark advance over this index.
                    durabilityTracker?.RegisterPending(partitionId, log.Id, DurabilityChannel.Flush);

                    // Record before enqueueing so a lock read that misses the actor table observes this
                    // replayed committed mutation even before the background flush lands it.
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
                    // this replayed entry until its flush lands. Replay runs in log-id order, so
                    // the registration always precedes any watermark advance over this index.
                    durabilityTracker?.RegisterPending(partitionId, log.Id, DurabilityChannel.Flush);

                    // Record before enqueueing so a lock read that misses the actor table observes this
                    // replayed committed mutation even before the background flush lands it.
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