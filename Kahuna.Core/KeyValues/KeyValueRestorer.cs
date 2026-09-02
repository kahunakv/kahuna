
using Nixie;

using Kommander;
using Kommander.Data;
using Kommander.Time;

using Kahuna.Server.KeyValues.Transactions;
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

    // The node's prepared-intent store, restored from its own snapshot and from the replayed prepare deltas
    // that precede a by-reference materialization record in the same partition's log. Null (bare
    // direct-construction tests) makes every by-reference record a reported miss.
    private readonly PreparedIntentStore? preparedIntentStore;

    public KeyValueRestorer(IActorRef<BackgroundWriterActor, BackgroundWriteRequest> backgroundWriter, IRaft raft, CompletionReceiptStore completionReceiptStore, ILogger<IKahuna> logger, UnflushedKeyValueWritesIndex? unflushedWrites = null, PartitionDurabilityTracker? durabilityTracker = null, PreparedIntentStore? preparedIntentStore = null)
    {
        this.preparedIntentStore = preparedIntentStore;
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

            KeyValueState state;
            byte[]? messageValue;

            if ((KeyValueRequestType)keyValueMessage.Type == KeyValueRequestType.MaterializeIntent)
            {
                // A by-reference record carries no value: the mutation comes from the prepared intent it
                // names. Replay reaches it in the same order a live replica does — the prepare delta applies
                // first on this partition, and the settle that removes the intent applies later — and the
                // checkpoint that bounds this replay is appended AFTER the intent snapshot is written, so an
                // intent removed at or below the checkpoint had its materialization below the checkpoint too
                // and is never replayed. That is why the intent is here.
                if (!TryResolveIntentForRestore(keyValueMessage, log.Id, out PreparedIntent? intent))
                    return true;

                state = intent!.State;
                messageValue = intent.Value;
            }
            else
            {
                (state, messageValue) = KeyValueMessageDecoder.Decode(keyValueMessage);

                if (state == KeyValueState.Undefined)
                {
                    logger.LogError("KeyValueRestorer: Unknown restore message type: {Type}", keyValueMessage.Type);
                    return true;
                }
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

    /// <summary>
    /// Resolves the prepared intent a by-reference record names, and reports whether the replay may apply it.
    /// False means the record contributes nothing here — either because the value is already durable (a second
    /// producer's duplicate record, whose first copy this replay already recorded in the overlay) or because
    /// the intent is genuinely absent, which is the correctness alarm.
    ///
    /// <para>The proof available during replay is the unflushed overlay alone. It covers every materialization
    /// replayed so far, which is the whole duplicate case that arises inside one replay. The narrow case it
    /// cannot dismiss is a duplicate whose first copy sits below the checkpoint: the row is durable, but this
    /// path may not read the backend to prove it. Such a record is reported, so the alarm names it as
    /// possibly-already-durable rather than claiming a lost write outright.</para>
    /// </summary>
    private bool TryResolveIntentForRestore(KeyValueMessage keyValueMessage, long logIndex, out PreparedIntent? intent)
    {
        HLCTimestamp transactionId = new(
            keyValueMessage.TransactionIdNode, keyValueMessage.TransactionIdPhysical, keyValueMessage.TransactionIdCounter);

        intent = preparedIntentStore?.GetByIdentity(transactionId, keyValueMessage.Epoch, keyValueMessage.Key);

        if (intent is not null && intent.Revision == keyValueMessage.Revision)
            return true;

        if (intent is not null)
        {
            // The record and the intent name two different mutations; applying the intent would restore the
            // wrong revision.
            Transactions.DurableTransactionMetrics.MaterializationIntentMissing.Add(1);
            logger.LogError(
                "KeyValueRestorer: by-reference record for key {Key} (transaction {TransactionId} epoch {Epoch}) names revision {Revision}, but the restored intent stands at revision {IntentRevision} (log entry {LogIndex})",
                keyValueMessage.Key, transactionId, keyValueMessage.Epoch, keyValueMessage.Revision, intent.Revision, logIndex);

            intent = null;
            return false;
        }

        if (unflushedWrites is not null
            && unflushedWrites.TryGet(keyValueMessage.Key, out UnflushedKeyValueWrite pending)
            && pending.Revision >= keyValueMessage.Revision)
            return false; // Already replayed by an earlier copy of the same materialization.

        Transactions.DurableTransactionMetrics.MaterializationIntentMissing.Add(1);
        logger.LogError(
            "KeyValueRestorer: by-reference record for key {Key} at revision {Revision} found no restored intent for transaction {TransactionId} epoch {Epoch} (log entry {LogIndex}); the value is missing here unless an earlier copy of this materialization is already flushed",
            keyValueMessage.Key, keyValueMessage.Revision, transactionId, keyValueMessage.Epoch, logIndex);

        return false;
    }
}
