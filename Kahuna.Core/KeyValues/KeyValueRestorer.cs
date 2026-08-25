
using Nixie;

using Kommander;
using Kommander.Data;
using Kommander.Time;

using Kahuna.Server.KeyValues.Transactions.Data;
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

    private readonly CompletionReceiptStore completionReceiptStore;

    private readonly UnflushedKeyValueWritesIndex? unflushedWrites;

    private readonly PartitionDurabilityTracker? durabilityTracker;

    private readonly ILogger<IKahuna> logger;

    public KeyValueRestorer(IActorRef<BackgroundWriterActor, BackgroundWriteRequest> backgroundWriter, IRaft raft, CompletionReceiptStore completionReceiptStore, ILogger<IKahuna> logger, UnflushedKeyValueWritesIndex? unflushedWrites = null, PartitionDurabilityTracker? durabilityTracker = null)
    {
        this.backgroundWriter = backgroundWriter;
        this.raft = raft;
        this.completionReceiptStore = completionReceiptStore;
        this.logger = logger;
        this.unflushedWrites = unflushedWrites;
        this.durabilityTracker = durabilityTracker;
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

            HLCTimestamp expires      = new(keyValueMessage.ExpireNode, keyValueMessage.ExpirePhysical, keyValueMessage.ExpireCounter);
            HLCTimestamp lastUsed     = new(keyValueMessage.LastUsedNode, keyValueMessage.LastUsedPhysical, keyValueMessage.LastUsedCounter);
            HLCTimestamp lastModified = new(keyValueMessage.LastModifiedNode, keyValueMessage.LastModifiedPhysical, keyValueMessage.LastModifiedCounter);

            // A replayed transactional entry re-derives a completion receipt below, so it must
            // register on Flush AND Receipts — the floor may not pass it until the flushed row and
            // a receipt snapshot covering the rebuilt receipt are both durable. A single-shot entry
            // (zero transaction id) derives no receipt and registers on Flush alone.
            bool derivesReceipt = keyValueMessage.TransactionIdNode != 0
                || keyValueMessage.TransactionIdPhysical != 0
                || keyValueMessage.TransactionIdCounter != 0;

            // Register before enqueueing: the partition's durability floor must not pass this
            // replayed entry until its durable artifacts land. Replay runs in log-id order, so the
            // registration always precedes any watermark advance over this index.
            if (derivesReceipt)
                durabilityTracker?.RegisterPending(partitionId, log.Id, DurabilityChannel.Flush, DurabilityChannel.Receipts);
            else
                durabilityTracker?.RegisterPending(partitionId, log.Id, DurabilityChannel.Flush);

            // Record before enqueueing so reads observe the replayed committed write even before the
            // background flush lands it in the backend.
            unflushedWrites?.Record(keyValueMessage.Key, messageValue, keyValueMessage.Revision,
                expires, lastUsed, lastModified, state, keyValueMessage.NoRevision);

            backgroundWriter.Send(BackgroundWriteRequestPool.Rent(
            BackgroundWriteType.QueueStoreKeyValue,
                partitionId,
                keyValueMessage.Key,
                messageValue,
                keyValueMessage.Revision,
                expires,
                lastUsed,
                lastModified,
                (int)state,
                keyValueMessage.NoRevision,
                logIndex: log.Id
            ));

            // Rebuild the completion receipt from the replayed committed record so a re-commit after a
            // cold restart / leader change resolves Committed rather than MustRetry. Then raise the
            // Receipts resolve ceiling over this entry — Record precedes MarkApplied so a snapshot
            // capture that samples the raised ceiling always finds the receipt already in the store.
            if (derivesReceipt)
            {
                HLCTimestamp transactionId = new(keyValueMessage.TransactionIdNode, keyValueMessage.TransactionIdPhysical, keyValueMessage.TransactionIdCounter);
                completionReceiptStore.Record(
                    transactionId,
                    keyValueMessage.Key,
                    keyValueMessage.HasRecordAnchorKey ? keyValueMessage.RecordAnchorKey : null,
                    KeyValueDurability.Persistent);

                durabilityTracker?.MarkApplied(partitionId, log.Id, DurabilityChannel.Receipts);
            }

            return true;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "KeyValueRestorer: Error processing replication message");
            return false;
        }
    }
}
