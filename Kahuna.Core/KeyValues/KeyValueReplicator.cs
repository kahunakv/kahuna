
using System.Runtime.InteropServices;
using Nixie;

using Kommander;
using Kommander.Data;

using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.Persistence;
using Kahuna.Server.Replication;
using Kahuna.Server.Replication.Protos;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// Responsible for handling the replication of key-value operations in a distributed system.
/// Processes replication requests received from the Raft log and commits the replication
/// operations as appropriate.
/// </summary>
/// <remarks>
/// This class plays a critical role in maintaining consistency in a distributed key-value store
/// by executing replication messages. It interacts with the Raft consensus module to process
/// log entries that represent key-value operations such as setting, deleting, or extending keys.
/// The replication ensures distributed state is properly synchronized across nodes.
/// </remarks>
internal sealed class KeyValueReplicator
{
    private readonly IActorRef<BackgroundWriterActor, BackgroundWriteRequest> backgroundWriter;

    private readonly IRaft raft;

    private readonly KeyWriteFrequencyRegistry writeFrequencyRegistry;

    private readonly KeySpaceRegistry keySpaceRegistry;

    private readonly ILogger<IKahuna> logger;

    public KeyValueReplicator(
        IActorRef<BackgroundWriterActor, BackgroundWriteRequest> backgroundWriter,
        IRaft raft,
        KeyWriteFrequencyRegistry writeFrequencyRegistry,
        KeySpaceRegistry keySpaceRegistry,
        ILogger<IKahuna> logger)
    {
        this.backgroundWriter       = backgroundWriter;
        this.raft                   = raft;
        this.writeFrequencyRegistry = writeFrequencyRegistry;
        this.keySpaceRegistry       = keySpaceRegistry;
        this.logger                 = logger;
    }

    /// <summary>
    /// Replicates the specified log entry for the given partition.
    /// </summary>
    /// <param name="partitionId">The unique identifier of the partition where the log entry should be replicated.</param>
    /// <param name="log">The log entry containing the data to be replicated.</param>
    /// <returns>Returns <c>true</c> if replication succeeded or the log data was empty; otherwise, <c>false</c> if an error occurred during replication.</returns>
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
                        new(keyValueMessage.ExpireNode, keyValueMessage.ExpirePhysical, keyValueMessage.ExpireCounter),
                        new(keyValueMessage.LastUsedNode, keyValueMessage.LastUsedPhysical, keyValueMessage.LastUsedCounter),
                        new(keyValueMessage.LastModifiedNode, keyValueMessage.LastModifiedPhysical, keyValueMessage.LastModifiedCounter),
                        (int)KeyValueState.Set
                    ));

                    // K1b: record the committed write into the local histogram.
                    // Running on every node (leader + followers) so the P0/meta leader — which
                    // runs the split trigger — always has warm data regardless of where the
                    // partition leader sits.
                    // Guard: only key-range spaces are load-split; skip hash-routed writes to
                    // avoid building 4096-entry trackers for partitions the trigger never reads.
                    if (RangeRouting.IsKeyRange(keySpaceRegistry, keyValueMessage.Key))
                        writeFrequencyRegistry.GetOrCreate(partitionId).RecordWrite(keyValueMessage.Key);

                    return true;
                }

                case KeyValueRequestType.TryDelete:
                {
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
                        new(keyValueMessage.ExpireNode, keyValueMessage.ExpirePhysical, keyValueMessage.ExpireCounter),
                        new(keyValueMessage.LastUsedNode, keyValueMessage.LastUsedPhysical, keyValueMessage.LastUsedCounter),
                        new(keyValueMessage.LastModifiedNode, keyValueMessage.LastModifiedPhysical, keyValueMessage.LastModifiedCounter),
                        (int)KeyValueState.Deleted
                    ));

                    if (RangeRouting.IsKeyRange(keySpaceRegistry, keyValueMessage.Key))
                        writeFrequencyRegistry.GetOrCreate(partitionId).RecordWrite(keyValueMessage.Key);

                    return true;
                }

                case KeyValueRequestType.TryExtend:
                {
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
                        new(keyValueMessage.ExpireNode, keyValueMessage.ExpirePhysical, keyValueMessage.ExpireCounter),
                        new(keyValueMessage.LastUsedNode, keyValueMessage.LastUsedPhysical, keyValueMessage.LastUsedCounter),
                        new(keyValueMessage.LastModifiedNode, keyValueMessage.LastModifiedPhysical, keyValueMessage.LastModifiedCounter),
                        (int)KeyValueState.Set
                    ));

                    if (RangeRouting.IsKeyRange(keySpaceRegistry, keyValueMessage.Key))
                        writeFrequencyRegistry.GetOrCreate(partitionId).RecordWrite(keyValueMessage.Key);

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
                case KeyValueRequestType.GetByBucket:
                case KeyValueRequestType.GetByRange:
                case KeyValueRequestType.TryAcquireExclusivePrefixLock:
                case KeyValueRequestType.TryReleaseExclusivePrefixLock:
                case KeyValueRequestType.ScanByPrefixFromDisk:
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