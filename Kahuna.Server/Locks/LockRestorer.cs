
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

internal sealed class LockRestorer
{
    private readonly IActorRef<BackgroundWriterActor, BackgroundWriteRequest> backgroundWriter;

    private readonly IRaft raft;

    private readonly ILogger<IKahuna> logger;

    public LockRestorer(IActorRef<BackgroundWriterActor, BackgroundWriteRequest> backgroundWriter, IRaft raft, ILogger<IKahuna> logger)
    {
        this.backgroundWriter = backgroundWriter;
        this.raft = raft;
        this.logger = logger;
    }

    public bool Restore(RaftLog log)
    {
        if (log.LogData is null || log.LogData.Length == 0)
            return true;
        
        try
        {
            LockMessage lockMessage = ReplicationSerializer.UnserializeLockMessage(log.LogData);

            HLCTimestamp eventTime = new(lockMessage.TimeLogical, lockMessage.TimeCounter);

            raft.HybridLogicalClock.ReceiveEvent(eventTime);

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
                        -1,
                        lockMessage.Resource,
                        owner,
                        lockMessage.FencingToken,
                        new(lockMessage.ExpireLogical, lockMessage.ExpireCounter),
                        new(lockMessage.LastUsedLogical, lockMessage.LastUsedCounter),
                        new(lockMessage.LastModifiedLogical, lockMessage.LastModifiedCounter),
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
                        -1,
                        lockMessage.Resource,
                        owner,
                        lockMessage.FencingToken,
                        new(lockMessage.ExpireLogical, lockMessage.ExpireCounter),
                        new(lockMessage.LastUsedLogical, lockMessage.LastUsedCounter),
                        new(lockMessage.LastModifiedLogical, lockMessage.LastModifiedCounter),
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
                        -1,
                        lockMessage.Resource,
                        owner,
                        lockMessage.FencingToken,
                        new(lockMessage.ExpireLogical, lockMessage.ExpireCounter),
                        new(lockMessage.LastUsedLogical, lockMessage.LastUsedCounter),
                        new(lockMessage.LastModifiedLogical, lockMessage.LastModifiedCounter),
                        (int)LockState.Unlocked
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