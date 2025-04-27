using System.Runtime.InteropServices;
using Kahuna.Server.Persistence;
using Kahuna.Server.Replication;
using Kahuna.Server.Replication.Protos;
using Kahuna.Shared.Locks;
using Kommander;
using Kommander.Data;
using Kommander.Time;
using Nixie;

namespace Kahuna.Server.Locks;

/// <summary>
/// The LockReplicator class handles the replication of lock-related requests within a distributed system
/// using a Raft consensus protocol. This includes processing various types of lock operations such as
/// acquiring locks, releasing locks, and extending lock durations.
/// </summary>
internal sealed class LockReplicator
{
    private readonly IActorRef<BackgroundWriterActor, BackgroundWriteRequest> backgroundWriter;

    private readonly IRaft raft;

    private readonly ILogger<IKahuna> logger;

    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="backgroundWriter"></param>
    /// <param name="raft"></param>
    /// <param name="logger"></param>
    public LockReplicator(IActorRef<BackgroundWriterActor, BackgroundWriteRequest> backgroundWriter, IRaft raft, ILogger<IKahuna> logger)
    {
        this.backgroundWriter = backgroundWriter;
        this.raft = raft;
        this.logger = logger;
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

                    if (MemoryMarshal.TryGetArray(lockMessage.Owner.Memory, out ArraySegment<byte> segment))
                        owner = segment.Array ?? lockMessage.Owner.ToByteArray();
                    else
                        owner = lockMessage.Owner.ToByteArray();
                    
                    backgroundWriter.Send(new(
                        BackgroundWriteType.QueueStoreLock,
                        partitionId,
                        lockMessage.Resource,
                        owner,
                        lockMessage.FencingToken,
                        new(lockMessage.ExpireNode, lockMessage.ExpirePhysical, lockMessage.ExpireCounter),
                        new(lockMessage.LastUsedNode, lockMessage.LastUsedPhysical, lockMessage.LastUsedCounter),
                        new(lockMessage.LastModifiedNode, lockMessage.LastModifiedPhysical, lockMessage.LastModifiedCounter),
                        (int)LockState.Locked
                    ));

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

                    if (MemoryMarshal.TryGetArray(lockMessage.Owner.Memory, out ArraySegment<byte> segment))
                        owner = segment.Array ?? lockMessage.Owner.ToByteArray();
                    else
                        owner = lockMessage.Owner.ToByteArray();
                    
                    backgroundWriter.Send(new(
                        BackgroundWriteType.QueueStoreLock,
                        partitionId,
                        lockMessage.Resource,
                        owner,
                        lockMessage.FencingToken,
                        new(lockMessage.ExpireNode, lockMessage.ExpirePhysical, lockMessage.ExpireCounter),
                        new(lockMessage.LastUsedNode, lockMessage.LastUsedPhysical, lockMessage.LastUsedCounter),
                        new(lockMessage.LastModifiedNode, lockMessage.LastModifiedPhysical, lockMessage.LastModifiedCounter),
                        (int)LockState.Unlocked
                    ));
                    
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

                    if (MemoryMarshal.TryGetArray(lockMessage.Owner.Memory, out ArraySegment<byte> segment))
                        owner = segment.Array ?? lockMessage.Owner.ToByteArray();
                    else
                        owner = lockMessage.Owner.ToByteArray();
                    
                    backgroundWriter.Send(new(
                        BackgroundWriteType.QueueStoreLock,
                        partitionId,
                        lockMessage.Resource,
                        owner,
                        lockMessage.FencingToken,
                        new(lockMessage.ExpireNode, lockMessage.ExpirePhysical, lockMessage.ExpireCounter),
                        new(lockMessage.LastUsedNode, lockMessage.LastUsedPhysical, lockMessage.LastUsedCounter),
                        new(lockMessage.LastModifiedNode, lockMessage.LastModifiedPhysical, lockMessage.LastModifiedCounter),
                        (int)LockState.Locked
                    ));

                    return true;
                }

                case LockRequestType.Get:
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