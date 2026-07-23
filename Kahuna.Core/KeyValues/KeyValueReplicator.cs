
using System.Runtime.InteropServices;
using Nixie;
using Nixie.Routers;

using Kommander;
using Kommander.Data;
using Kommander.Time;

using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.KeyValues.Transactions.Data;
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

    private readonly IActorRef<ConsistentHashActor<KeyValueActor, KeyValueRequest, KeyValueResponse>, KeyValueRequest, KeyValueResponse> persistentRouter;

    private readonly IRaft raft;

    private readonly KeyWriteFrequencyRegistry writeFrequencyRegistry;

    private readonly KeySpaceRegistry keySpaceRegistry;

    private readonly CompletionReceiptStore completionReceiptStore;

    private readonly ILogger<IKahuna> logger;

    public KeyValueReplicator(
        IActorRef<BackgroundWriterActor, BackgroundWriteRequest> backgroundWriter,
        IActorRef<ConsistentHashActor<KeyValueActor, KeyValueRequest, KeyValueResponse>, KeyValueRequest, KeyValueResponse> persistentRouter,
        IRaft raft,
        KeyWriteFrequencyRegistry writeFrequencyRegistry,
        KeySpaceRegistry keySpaceRegistry,
        CompletionReceiptStore completionReceiptStore,
        ILogger<IKahuna> logger)
    {
        this.backgroundWriter         = backgroundWriter;
        this.persistentRouter         = persistentRouter;
        this.raft                     = raft;
        this.writeFrequencyRegistry   = writeFrequencyRegistry;
        this.keySpaceRegistry         = keySpaceRegistry;
        this.completionReceiptStore   = completionReceiptStore;
        this.logger                   = logger;
    }

    /// <summary>
    /// Applies the transactional commit metadata carried on a committed persistent mutation as a follower
    /// replicates the log record: records a durable completion receipt (so a re-commit that lands here after
    /// the write intent / MVCC entry are gone answers <c>Committed</c> instead of <c>MustRetry</c>). A
    /// non-transactional (single-shot) write carries a zero transaction id, which is harmless to record.
    /// </summary>
    private void RecordCompletionReceipt(KeyValueMessage keyValueMessage)
    {
        HLCTimestamp transactionId = new(keyValueMessage.TransactionIdNode, keyValueMessage.TransactionIdPhysical, keyValueMessage.TransactionIdCounter);

        completionReceiptStore.Record(
            transactionId,
            keyValueMessage.Key,
            keyValueMessage.HasRecordAnchorKey ? keyValueMessage.RecordAnchorKey : null,
            KeyValueDurability.Persistent);
    }

    /// <summary>
    /// Routes an <c>InvalidateOrApply</c> message to the owning actor in the persistent pool.
    /// Ephemeral writes are never replicated via Raft (all three write handlers gate
    /// <c>CreateProposal</c> behind <c>Durability == Persistent</c>), so every entry this
    /// replicator sees is a persistent commit — sending to the ephemeral pool would be both
    /// wrong (it could corrupt an ephemeral entry for the same key name) and useless.
    /// </summary>
    private void SendInvalidateOrApply(
        int partitionId,
        string key,
        byte[]? value,
        long revision,
        HLCTimestamp expires,
        HLCTimestamp lastUsed,
        HLCTimestamp lastModified,
        KeyValueState state,
        HLCTimestamp transactionId,
        bool noRevision)
    {
        persistentRouter.Send(
            KeyValueRequest.ForInvalidateOrApply(
                key, revision, value, expires, lastUsed, lastModified, state,
                transactionId: transactionId, partitionId: partitionId, noRevision: noRevision));
    }

    /// <summary>
    /// Applies a durable-intent resolution's committed value on the leader by routing a commit-apply to the owning
    /// persistent actor: unlike the ordinary follower cache-coherence path, it carries the committing transaction id
    /// so the actor can clear that transaction's staged write intent and MVCC snapshot and apply the value to the
    /// base entry. The returned acknowledgement means the actor has
    /// completed that work; routing/enqueueing alone is not sufficient to settle the durable intent.
    /// </summary>
    public async Task<bool> ApplyDurableCommit(int partitionId, PreparedIntent intent)
    {
        try
        {
            KeyValueResponse? response = await persistentRouter.Ask(KeyValueRequest.ForInvalidateOrApply(
                intent.Key, intent.Revision, intent.Value,
                intent.Expires, intent.CommitTimestamp, intent.CommitTimestamp, intent.State,
                forceResident: true, transactionId: intent.TransactionId, partitionId: partitionId, noRevision: intent.NoRevision)).ConfigureAwait(false);
            return response?.Type == KeyValueResponseType.Committed;
        }
        catch
        {
            return false;
        }
    }

    /// <summary>
    /// Routes a durable-intent ABORT cleanup to the owning persistent actor: clears the transaction's staged write
    /// intent and MVCC snapshot for the key so an aborted transaction does not leave it blocked until the write
    /// intent lease expires (the durable analog of ApplyConfirmedRollback). The returned acknowledgement is
    /// positive only after the actor has processed the cleanup.
    /// </summary>
    public async Task<bool> ApplyDurableRollback(int partitionId, PreparedIntent intent)
    {
        try
        {
            KeyValueResponse? response = await persistentRouter.Ask(KeyValueRequest.ForInvalidateOrApply(
                intent.Key, intent.Revision, intent.Value,
                intent.Expires, intent.CommitTimestamp, intent.CommitTimestamp, intent.State,
                forceResident: true, transactionId: intent.TransactionId, partitionId: partitionId, noRevision: intent.NoRevision, isRollback: true)).ConfigureAwait(false);
            return response?.Type == KeyValueResponseType.RolledBack;
        }
        catch
        {
            return false;
        }
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

                    HLCTimestamp expires      = new(keyValueMessage.ExpireNode, keyValueMessage.ExpirePhysical, keyValueMessage.ExpireCounter);
                    HLCTimestamp lastUsed     = new(keyValueMessage.LastUsedNode, keyValueMessage.LastUsedPhysical, keyValueMessage.LastUsedCounter);
                    HLCTimestamp lastModified = new(keyValueMessage.LastModifiedNode, keyValueMessage.LastModifiedPhysical, keyValueMessage.LastModifiedCounter);

                    backgroundWriter.Send(BackgroundWriteRequestPool.Rent(
            BackgroundWriteType.QueueStoreKeyValue,
                        partitionId,
                        keyValueMessage.Key,
                        messageValue,
                        keyValueMessage.Revision,
                        expires,
                        lastUsed,
                        lastModified,
                        (int)KeyValueState.Set,
                        keyValueMessage.NoRevision
                    ));

                    SendInvalidateOrApply(partitionId, keyValueMessage.Key, messageValue, keyValueMessage.Revision,
                        expires, lastUsed, lastModified, KeyValueState.Set,
                        new(keyValueMessage.TransactionIdNode, keyValueMessage.TransactionIdPhysical, keyValueMessage.TransactionIdCounter),
                        keyValueMessage.NoRevision);

                    RecordCompletionReceipt(keyValueMessage);

                    // Record the committed write into the local histogram.
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

                    HLCTimestamp expires      = new(keyValueMessage.ExpireNode, keyValueMessage.ExpirePhysical, keyValueMessage.ExpireCounter);
                    HLCTimestamp lastUsed     = new(keyValueMessage.LastUsedNode, keyValueMessage.LastUsedPhysical, keyValueMessage.LastUsedCounter);
                    HLCTimestamp lastModified = new(keyValueMessage.LastModifiedNode, keyValueMessage.LastModifiedPhysical, keyValueMessage.LastModifiedCounter);

                    backgroundWriter.Send(BackgroundWriteRequestPool.Rent(
            BackgroundWriteType.QueueStoreKeyValue,
                        partitionId,
                        keyValueMessage.Key,
                        messageValue,
                        keyValueMessage.Revision,
                        expires,
                        lastUsed,
                        lastModified,
                        (int)KeyValueState.Deleted,
                        keyValueMessage.NoRevision
                    ));

                    SendInvalidateOrApply(partitionId, keyValueMessage.Key, messageValue, keyValueMessage.Revision,
                        expires, lastUsed, lastModified, KeyValueState.Deleted,
                        new(keyValueMessage.TransactionIdNode, keyValueMessage.TransactionIdPhysical, keyValueMessage.TransactionIdCounter),
                        keyValueMessage.NoRevision);

                    RecordCompletionReceipt(keyValueMessage);

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

                    HLCTimestamp expires      = new(keyValueMessage.ExpireNode, keyValueMessage.ExpirePhysical, keyValueMessage.ExpireCounter);
                    HLCTimestamp lastUsed     = new(keyValueMessage.LastUsedNode, keyValueMessage.LastUsedPhysical, keyValueMessage.LastUsedCounter);
                    HLCTimestamp lastModified = new(keyValueMessage.LastModifiedNode, keyValueMessage.LastModifiedPhysical, keyValueMessage.LastModifiedCounter);

                    backgroundWriter.Send(BackgroundWriteRequestPool.Rent(
            BackgroundWriteType.QueueStoreKeyValue,
                        partitionId,
                        keyValueMessage.Key,
                        messageValue,
                        keyValueMessage.Revision,
                        expires,
                        lastUsed,
                        lastModified,
                        (int)KeyValueState.Set,
                        keyValueMessage.NoRevision
                    ));

                    SendInvalidateOrApply(partitionId, keyValueMessage.Key, messageValue, keyValueMessage.Revision,
                        expires, lastUsed, lastModified, KeyValueState.Set,
                        new(keyValueMessage.TransactionIdNode, keyValueMessage.TransactionIdPhysical, keyValueMessage.TransactionIdCounter),
                        keyValueMessage.NoRevision);

                    RecordCompletionReceipt(keyValueMessage);

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
