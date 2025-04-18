
using System.Runtime.InteropServices;
using Nixie;

using Kommander;
using Kommander.Data;
using Kommander.Time;

using Kahuna.Server.Persistence;
using Kahuna.Server.Replication;
using Kahuna.Server.Replication.Protos;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues;

internal sealed class KeyValueReplicator
{
    private readonly IActorRef<BackgroundWriterActor, BackgroundWriteRequest> backgroundWriter;
    
    private readonly IRaft raft;

    private readonly ILogger<IKahuna> logger;
    
    public KeyValueReplicator(IActorRef<BackgroundWriterActor, BackgroundWriteRequest> backgroundWriter, IRaft raft, ILogger<IKahuna> logger)
    {
        this.backgroundWriter = backgroundWriter;
        this.raft = raft;
        this.logger = logger;
    }

    public bool Replicate(int partitionId, RaftLog log)
    {
        if (log.LogData is null || log.LogData.Length == 0)
            return true;
        
        try
        {
            KeyValueMessage keyValueMessage = ReplicationSerializer.UnserializeKeyValueMessage(log.LogData);

            switch ((KeyValueRequestType)keyValueMessage.Type)
            {
                case KeyValueRequestType.TrySet:
                {
                    /*PersistenceResponse? response = await persistenceActorRouter.Ask(new(
                        PersistenceRequestType.StoreKeyValue,
                        [new(
                            keyValueMessage.Key,
                            keyValueMessage.Value?.ToByteArray(),
                            keyValueMessage.Revision,
                            keyValueMessage.ExpireLogical,
                            keyValueMessage.ExpireCounter,
                            (int)KeyValueState.Set
                        )]
                    ));
                    
                    if (response is null)
                        return false;

                    if (response.Type == PersistenceResponseType.Success)
                        logger.LogDebug("Replicated key/value set {Key} {Revision} to {Node}", keyValueMessage.Key, keyValueMessage.Revision, raft.GetLocalNodeId());*/
                    
                    byte[]? messageValue;
            
                    if (MemoryMarshal.TryGetArray(keyValueMessage.Value.Memory, out ArraySegment<byte> segment))
                        messageValue = segment.Array;
                    else
                        messageValue = keyValueMessage.Value.ToByteArray();

                    backgroundWriter.Send(new(
                        BackgroundWriteType.QueueStoreKeyValue,
                        partitionId,
                        keyValueMessage.Key,
                        messageValue,
                        keyValueMessage.Revision,
                        new(keyValueMessage.ExpirePhysical, keyValueMessage.ExpireCounter),
                        new(keyValueMessage.LastUsedPhysical, keyValueMessage.LastUsedCounter),
                        new(keyValueMessage.LastModifiedPhysical, keyValueMessage.LastModifiedCounter),
                        (int)KeyValueState.Set
                    ));

                    return true;
                }

                case KeyValueRequestType.TryDelete:
                {
                    /*PersistenceResponse? response = await persistenceActorRouter.Ask(new(
                        PersistenceRequestType.StoreKeyValue,
                        [new(
                            keyValueMessage.Key,
                            keyValueMessage.Value?.ToByteArray(),
                            keyValueMessage.Revision,
                            keyValueMessage.ExpireLogical,
                            keyValueMessage.ExpireCounter,
                            (int)KeyValueState.Deleted
                        )]
                    ));
                    
                    if (response is null)
                        return false;

                    if (response.Type == PersistenceResponseType.Success)
                        logger.LogDebug("Replicated key/value delete {Key} {Revision} to {Node}", keyValueMessage.Key, keyValueMessage.Revision, raft.GetLocalNodeId());*/
                    
                    byte[]? messageValue;
            
                    if (MemoryMarshal.TryGetArray(keyValueMessage.Value.Memory, out ArraySegment<byte> segment))
                        messageValue = segment.Array;
                    else
                        messageValue = keyValueMessage.Value.ToByteArray();
                    
                    backgroundWriter.Send(new(
                        BackgroundWriteType.QueueStoreKeyValue,
                        partitionId,
                        keyValueMessage.Key,
                        messageValue,
                        keyValueMessage.Revision,
                        new(keyValueMessage.ExpirePhysical, keyValueMessage.ExpireCounter),
                        new(keyValueMessage.LastUsedPhysical, keyValueMessage.LastUsedCounter),
                        new(keyValueMessage.LastModifiedPhysical, keyValueMessage.LastModifiedCounter),
                        (int)KeyValueState.Deleted
                    ));

                    return true;
                }

                case KeyValueRequestType.TryExtend:
                {
                    /*PersistenceResponse? response = await persistenceActorRouter.Ask(new(
                        PersistenceRequestType.StoreKeyValue,
                        [new(
                            keyValueMessage.Key,
                            keyValueMessage.Value?.ToByteArray(),
                            keyValueMessage.Revision,
                            keyValueMessage.ExpireLogical,
                            keyValueMessage.ExpireCounter,
                            (int)KeyValueState.Set
                        )]
                    ));

                    if (response is null)
                        return false;

                    if (response.Type == PersistenceResponseType.Success)
                        logger.LogDebug("Replicated key/value extend {Key} {Revision} to {Node}", keyValueMessage.Key, keyValueMessage.Revision, raft.GetLocalNodeId());*/
                    
                    byte[]? messageValue;
            
                    if (MemoryMarshal.TryGetArray(keyValueMessage.Value.Memory, out ArraySegment<byte> segment))
                        messageValue = segment.Array;
                    else
                        messageValue = keyValueMessage.Value.ToByteArray();
                    
                    backgroundWriter.Send(new(
                        BackgroundWriteType.QueueStoreKeyValue,
                        partitionId,
                        keyValueMessage.Key,
                        messageValue,
                        keyValueMessage.Revision,
                        new(keyValueMessage.ExpirePhysical, keyValueMessage.ExpireCounter),
                        new(keyValueMessage.LastUsedPhysical, keyValueMessage.LastUsedCounter),
                        new(keyValueMessage.LastModifiedPhysical, keyValueMessage.LastModifiedCounter),
                        (int)KeyValueState.Set
                    ));

                    return true;
                }

                case KeyValueRequestType.TryGet:
                case KeyValueRequestType.TryExists:
                case KeyValueRequestType.TryAcquireExclusiveLock:
                case KeyValueRequestType.TryReleaseExclusiveLock:
                case KeyValueRequestType.TryPrepareMutations:
                case KeyValueRequestType.TryCommitMutations:
                case KeyValueRequestType.TryRollbackMutations:
                case KeyValueRequestType.ScanByPrefix:
                case KeyValueRequestType.GetByPrefix:
                    break;
                
                default:
                    logger.LogError("KeyValueReplicator: Unknown replication message type: {Type}", keyValueMessage.Type);
                    break;
            }
        } 
        catch (Exception ex)
        {
            logger.LogError(ex, "KeyValueReplicator: Error processing replication message");
            return false;
        }

        return true;
    }
}