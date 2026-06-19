
using Nixie;

using Kommander;
using Kommander.Data;

using Kahuna.Server.Persistence;
using Kahuna.Server.Replication;
using Kahuna.Server.Replication.Protos;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// The KeyValueRestorer class is responsible for restoring key-value data from a Raft log during
/// the state recovery process. It processes and interprets the log entries to update the
/// key-value storage accordingly, ensuring system consistency.
/// </summary>
internal sealed class KeyValueRestorer
{
    private readonly IActorRef<BackgroundWriterActor, BackgroundWriteRequest> backgroundWriter;

    private readonly IRaft raft;

    private readonly ILogger<IKahuna> logger;

    public KeyValueRestorer(IActorRef<BackgroundWriterActor, BackgroundWriteRequest> backgroundWriter, IRaft raft, ILogger<IKahuna> logger)
    {
        this.backgroundWriter = backgroundWriter;
        this.raft = raft;
        this.logger = logger;
    }

    /// <summary>
    /// Restores key-value data from the provided Raft log for a given partition.
    /// It processes the log to ensure the key-value storage is updated correctly and system consistency is maintained.
    /// </summary>
    /// <param name="partitionId">The ID of the partition where the log data is being restored.</param>
    /// <param name="log">The Raft log containing key-value data to be restored.</param>
    /// <returns>
    /// Returns <c>true</c> if the restoration succeeds or if the log is empty;
    /// otherwise, returns <c>false</c> if an error occurs during restoration.
    /// </returns>
    public bool Restore(int partitionId, RaftLog log)
    {
        if (log.LogData is null || log.LogData.Length == 0)
            return true;

        try
        {
            KeyValueMessage keyValueMessage = ReplicationSerializer.UnserializeKeyValueMessage(log.LogData);

            (KeyValueState state, byte[]? messageValue) = KeyValueMessageDecoder.Decode(keyValueMessage);

            if (state == KeyValueState.Undefined)
            {
                logger.LogError("KeyValueRestorer: Unknown restore message type: {Type}", keyValueMessage.Type);
                return true;
            }

            backgroundWriter.Send(new(
                BackgroundWriteType.QueueStoreKeyValue,
                partitionId,
                keyValueMessage.Key,
                messageValue,
                keyValueMessage.Revision,
                new(keyValueMessage.ExpireNode, keyValueMessage.ExpirePhysical, keyValueMessage.ExpireCounter),
                new(keyValueMessage.LastUsedNode, keyValueMessage.LastUsedPhysical, keyValueMessage.LastUsedCounter),
                new(keyValueMessage.LastModifiedNode, keyValueMessage.LastModifiedPhysical, keyValueMessage.LastModifiedCounter),
                (int)state
            ));

            return true;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "KeyValueRestorer: Error processing replication message");
            return false;
        }
    }
}
