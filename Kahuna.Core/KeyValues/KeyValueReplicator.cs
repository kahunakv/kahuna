
using System.Runtime.InteropServices;
using Nixie;

using Kommander;
using Kommander.Data;
using Kommander.Time;

using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Persistence;
using Kahuna.Server.Replication;
using Kahuna.Server.Replication.Protos;
using Kahuna.Shared.Communication.Grpc;
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

    private readonly KeyValueActorRing persistentRouter;

    private readonly IRaft raft;

    private readonly KeyWriteFrequencyRegistry writeFrequencyRegistry;

    private readonly KeySpaceRegistry keySpaceRegistry;

    private readonly CompletionReceiptStore completionReceiptStore;

    private readonly UnflushedKeyValueWritesIndex? unflushedWrites;

    private readonly PartitionDurabilityTracker? durabilityTracker;

    // Performs the authoritative backend point read for a key OFF the owning actor, so the actor's message
    // loop never awaits queued I/O. Consulted by ApplyDurableCommit when the target actor answers that the
    // key is not resident; null (bare unit-test construction) keeps the un-hydrated single-ask behavior.
    private readonly Func<int, string, Task<KeyValueEntry?>>? hydrateFromBackend;

    private readonly ILogger<IKahuna> logger;

    public KeyValueReplicator(
        IActorRef<BackgroundWriterActor, BackgroundWriteRequest> backgroundWriter,
        KeyValueActorRing persistentRouter,
        IRaft raft,
        KeyWriteFrequencyRegistry writeFrequencyRegistry,
        KeySpaceRegistry keySpaceRegistry,
        CompletionReceiptStore completionReceiptStore,
        ILogger<IKahuna> logger,
        UnflushedKeyValueWritesIndex? unflushedWrites = null,
        PartitionDurabilityTracker? durabilityTracker = null,
        Func<int, string, Task<KeyValueEntry?>>? hydrateFromBackend = null)
    {
        this.backgroundWriter         = backgroundWriter;
        this.persistentRouter         = persistentRouter;
        this.raft                     = raft;
        this.writeFrequencyRegistry   = writeFrequencyRegistry;
        this.keySpaceRegistry         = keySpaceRegistry;
        this.completionReceiptStore   = completionReceiptStore;
        this.unflushedWrites          = unflushedWrites;
        this.durabilityTracker        = durabilityTracker;
        this.hydrateFromBackend       = hydrateFromBackend;
        this.logger                   = logger;
    }

    /// <summary>
    /// Applies the transactional commit metadata carried on a committed persistent mutation as a follower
    /// replicates the log record: records a durable completion receipt (so a re-commit that lands here after
    /// the write intent / MVCC entry are gone answers <c>Committed</c> instead of <c>MustRetry</c>), then
    /// raises the Receipts resolve ceiling over this entry so the next durable receipt snapshot certifies
    /// it. A non-transactional (single-shot) write carries a zero transaction id and derives no receipt,
    /// so it neither records nor touches the Receipts channel.
    /// </summary>
    private void RecordCompletionReceipt(int partitionId, long logIndex, KeyValueMessage keyValueMessage)
    {
        HLCTimestamp transactionId = new(keyValueMessage.TransactionIdNode, keyValueMessage.TransactionIdPhysical, keyValueMessage.TransactionIdCounter);

        if (transactionId == HLCTimestamp.Zero)
            return;

        completionReceiptStore.Record(
            transactionId,
            keyValueMessage.Key,
            keyValueMessage.HasRecordAnchorKey ? keyValueMessage.RecordAnchorKey : null,
            KeyValueDurability.Persistent
        );

        // Record precedes MarkApplied so a snapshot capture that samples the raised ceiling always
        // finds the receipt already in the store.
        durabilityTracker?.MarkApplied(partitionId, logIndex, DurabilityChannel.Receipts);
    }

    /// <summary>
    /// Registers a committed key-value entry with the durability tracker before its effects are
    /// enqueued. A transactional entry (non-zero transaction id) registers on Flush AND Receipts:
    /// its apply produces two durable artifacts — the flushed row and the derived completion
    /// receipt — and the floor passing the index with only the row durable would lose the receipt
    /// on a floor-narrowed restart replay (a post-restart re-commit would answer MustRetry instead
    /// of Committed). A single-shot entry derives no receipt and registers on Flush alone.
    /// </summary>
    private void RegisterPendingApply(int partitionId, long logIndex, KeyValueMessage keyValueMessage)
    {
        if (durabilityTracker is null)
            return;

        if (keyValueMessage.TransactionIdNode != 0 || keyValueMessage.TransactionIdPhysical != 0 || keyValueMessage.TransactionIdCounter != 0)
            durabilityTracker.RegisterPending(partitionId, logIndex, DurabilityChannel.Flush, DurabilityChannel.Receipts);
        else
            durabilityTracker.RegisterPending(partitionId, logIndex, DurabilityChannel.Flush);
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
        // Fire-and-forget with ownership transfer: the actor returns the pooled request after
        // handling it, so no reference may be kept past the send.
        persistentRouter.Send(
            KeyValueRequestPool.RentInvalidateOrApply(
                key,
                revision,
                value,
                expires,
                lastUsed,
                lastModified,
                state,
                forceResident: false,
                transactionId: transactionId,
                partitionId: partitionId,
                noRevision: noRevision,
                isRollback: false,
                returnToPoolOnReceive: true
            )
        );
    }

    /// <summary>
    /// Applies a durable-intent resolution's committed value on the leader by routing a commit-apply to the owning
    /// persistent actor: unlike the ordinary follower cache-coherence path, it carries the committing transaction id
    /// so the actor can clear that transaction's staged write intent and MVCC snapshot and apply the value to the
    /// base entry. The returned acknowledgement means the actor has
    /// completed that work; routing/enqueueing alone is not sufficient to settle the durable intent.
    ///
    /// <para>Two-step hydration: the actor's message loop never performs backend I/O, so when the key is not
    /// resident the first ask answers MustRetry, the persisted row is read HERE — off the actor, on the queued
    /// read scheduler — and a second ask hands the result in. The resident hot path stays a single ask with no
    /// read at all. The point read is needed for correctness on the cold path: a commit-apply can land late
    /// (after a snapshot install or un-host purge evicted the entry), and installing over a fabricated empty
    /// base would shadow newer persisted rows.</para>
    /// </summary>
    public async Task<bool> ApplyDurableCommit(int partitionId, PreparedIntent intent)
    {
        KeyValueResponseType first = await AskDurableCommit(partitionId, intent, hydratedEntry: null, backendHydrated: false).ConfigureAwait(false);

        if (first == KeyValueResponseType.Committed)
            return true;

        if (first != KeyValueResponseType.MustRetry || hydrateFromBackend is null)
            return false;

        ReadOnlyKeyValueEntry? persisted;
        try
        {
            KeyValueEntry? row = await hydrateFromBackend(partitionId, intent.Key).ConfigureAwait(false);
            persisted = row is null
                ? null
                : new ReadOnlyKeyValueEntry(row.Value, row.Revision, row.Expires, row.LastUsed, row.LastModified, row.State);
        }
        catch
        {
            return false;
        }

        return await AskDurableCommit(partitionId, intent, persisted, backendHydrated: true).ConfigureAwait(false)
            == KeyValueResponseType.Committed;
    }

    private async Task<KeyValueResponseType> AskDurableCommit(
        int partitionId, PreparedIntent intent, ReadOnlyKeyValueEntry? hydratedEntry, bool backendHydrated)
    {
        KeyValueRequest request = KeyValueRequestPool.RentInvalidateOrApply(
            intent.Key,
            intent.Revision,
            intent.Value,
            intent.Expires,
            intent.CommitTimestamp,
            intent.CommitTimestamp,
            intent.State,
            forceResident: true,
            transactionId: intent.TransactionId,
            partitionId: partitionId,
            noRevision: intent.NoRevision,
            isRollback: false,
            returnToPoolOnReceive: false,
            backendHydrated: backendHydrated,
            hydratedEntry: hydratedEntry
        );

        try
        {
            KeyValueResponse? response = await persistentRouter.Ask(request).ConfigureAwait(false);
            return response?.Type ?? KeyValueResponseType.Errored;
        }
        catch
        {
            return KeyValueResponseType.Errored;
        }
        finally
        {
            KeyValueRequestPool.Return(request);
        }
    }

    /// <summary>
    /// Detached coherence reconcile for a key whose resident entry stopped converging with this node's own
    /// durable state — detected as a fence-refusal streak at a frozen (validated base, committed head) pair.
    /// Reads the durable row off the actor (backend or unflushed overlay; the value is durable here even when
    /// the actor dropped its one coherence notification) and hands it to the owning actor as a reconcile
    /// message, which adopts it when strictly newer and clears the blocking write intent. Fire-and-forget:
    /// the caller sits on the replicated prepare-apply path and must not block; a missed reconcile re-arms on
    /// the continuing refusal streak.
    /// </summary>
    public void ScheduleCoherenceReconcile(int partitionId, string key)
    {
        Func<int, string, Task<KeyValueEntry?>>? hydrate = hydrateFromBackend;
        if (hydrate is null)
            return;

        _ = Task.Run(async () =>
        {
            try
            {
                KeyValueEntry? row = await hydrate(partitionId, key).ConfigureAwait(false);
                if (row is null)
                    return;

                persistentRouter.Send(
                    KeyValueRequestPool.RentInvalidateOrApply(
                        key,
                        row.Revision,
                        row.Value,
                        row.Expires,
                        row.LastModified,
                        row.LastModified,
                        row.State,
                        forceResident: false,
                        transactionId: HLCTimestamp.Zero,
                        partitionId: partitionId,
                        noRevision: false,
                        isRollback: false,
                        returnToPoolOnReceive: true,
                        backendHydrated: true,
                        hydratedEntry: new ReadOnlyKeyValueEntry(row.Value, row.Revision, row.Expires, row.LastUsed, row.LastModified, row.State),
                        reconcile: true
                    )
                );
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex,
                    "Coherence reconcile for key {Key} failed; the refusal streak re-arms it",
                    key);
            }
        });
    }

    /// <summary>
    /// Detached convergence repair for a committed durable intent whose materialization never applied on this
    /// node. The caller sits on the replicated settle-apply path and must not block, so the repair runs the
    /// full <see cref="ApplyDurableCommit"/> flow (including its off-actor hydration read) on a background
    /// task. The apply is idempotent (head guards turn a re-apply into a no-op), so a spurious repair is
    /// harmless; an unconfirmed one is logged and left to the recovery sweep, which remains the backstop.
    /// </summary>
    public void ScheduleDurableCommitRepair(int partitionId, PreparedIntent intent)
    {
        _ = Task.Run(async () =>
        {
            try
            {
                if (!await ApplyDurableCommit(partitionId, intent).ConfigureAwait(false))
                    logger.LogWarning(
                        "Materialization repair for key {Key} of transaction {TransactionId} did not confirm; the recovery sweep remains the backstop",
                        intent.Key, intent.TransactionId);
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex,
                    "Materialization repair for key {Key} of transaction {TransactionId} failed; the recovery sweep remains the backstop",
                    intent.Key, intent.TransactionId);
            }
        });
    }

    /// <summary>
    /// Routes a durable-intent ABORT cleanup to the owning persistent actor: clears the transaction's staged write
    /// intent and MVCC snapshot for the key so an aborted transaction does not leave it blocked until the write
    /// intent lease expires (the durable analog of ApplyConfirmedRollback). The returned acknowledgement is
    /// positive only after the actor has processed the cleanup.
    /// </summary>
    public async Task<bool> ApplyDurableRollback(int partitionId, PreparedIntent intent)
    {
        KeyValueRequest request = KeyValueRequestPool.RentInvalidateOrApply(
            intent.Key, 
            intent.Revision, 
            intent.Value,
            intent.Expires, 
            intent.CommitTimestamp, 
            intent.CommitTimestamp, 
            intent.State,
            forceResident: true, 
            transactionId: intent.TransactionId, 
            partitionId: partitionId, 
            noRevision: intent.NoRevision, 
            isRollback: true
        );

        try
        {
            KeyValueResponse? response = await persistentRouter.Ask(request).ConfigureAwait(false);
            return response?.Type == KeyValueResponseType.RolledBack;
        }
        catch
        {
            return false;
        }
        finally
        {
            KeyValueRequestPool.Return(request);
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
            // Thread-cached shell: valid only within this synchronous call — every field is copied out
            // below before the next entry on this thread reuses it. The extracted value array belongs
            // to this parse's ByteString, not to the shell, so it is safe to hand onward.
            KeyValueMessage keyValueMessage = ReplicationSerializer.UnserializeKeyValueMessageThreadCached(log.LogData);

            switch ((KeyValueRequestType)keyValueMessage.Type)
            {
                case KeyValueRequestType.TrySet:
                {
                    byte[]? messageValue;

                    messageValue = ByteStringPayload.GetArray(keyValueMessage.Value);

                    HLCTimestamp expires      = new(keyValueMessage.ExpireNode, keyValueMessage.ExpirePhysical, keyValueMessage.ExpireCounter);
                    HLCTimestamp lastUsed     = new(keyValueMessage.LastUsedNode, keyValueMessage.LastUsedPhysical, keyValueMessage.LastUsedCounter);
                    HLCTimestamp lastModified = new(keyValueMessage.LastModifiedNode, keyValueMessage.LastModifiedPhysical, keyValueMessage.LastModifiedCounter);

                    // Register before enqueueing: the partition's durability floor must not pass
                    // this entry until every durable artifact of its apply lands (see
                    // RegisterPendingApply). Applies arrive in log-id order (leaders deliver their
                    // own committed proposals through this path too), so the registration always
                    // precedes any watermark advance over this index.
                    RegisterPendingApply(partitionId, log.Id, keyValueMessage);

                    // Record before enqueueing so a read that misses the actor cache observes this
                    // committed write even before the background flush lands it in the backend.
                    unflushedWrites?.Record(
                        keyValueMessage.Key, 
                        messageValue, 
                        keyValueMessage.Revision,
                        expires, 
                        lastUsed, 
                        lastModified, 
                        KeyValueState.Set, 
                        keyValueMessage.NoRevision
                    );

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
                        keyValueMessage.NoRevision,
                        logIndex: log.Id
                    ));

                    SendInvalidateOrApply(
                        partitionId, 
                        keyValueMessage.Key, 
                        messageValue, 
                        keyValueMessage.Revision,
                        expires, 
                        lastUsed, 
                        lastModified, 
                        KeyValueState.Set,
                        new(keyValueMessage.TransactionIdNode, keyValueMessage.TransactionIdPhysical, keyValueMessage.TransactionIdCounter),
                        keyValueMessage.NoRevision
                    );

                    RecordCompletionReceipt(partitionId, log.Id, keyValueMessage);

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

                    messageValue = ByteStringPayload.GetArray(keyValueMessage.Value);

                    HLCTimestamp expires      = new(keyValueMessage.ExpireNode, keyValueMessage.ExpirePhysical, keyValueMessage.ExpireCounter);
                    HLCTimestamp lastUsed     = new(keyValueMessage.LastUsedNode, keyValueMessage.LastUsedPhysical, keyValueMessage.LastUsedCounter);
                    HLCTimestamp lastModified = new(keyValueMessage.LastModifiedNode, keyValueMessage.LastModifiedPhysical, keyValueMessage.LastModifiedCounter);

                    RegisterPendingApply(partitionId, log.Id, keyValueMessage);

                    unflushedWrites?.Record(keyValueMessage.Key, messageValue, keyValueMessage.Revision,
                        expires, lastUsed, lastModified, KeyValueState.Deleted, keyValueMessage.NoRevision);

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
                        keyValueMessage.NoRevision,
                        logIndex: log.Id
                    ));

                    SendInvalidateOrApply(
                        partitionId, 
                        keyValueMessage.Key, 
                        messageValue, 
                        keyValueMessage.Revision,
                        expires, 
                        lastUsed, 
                        lastModified, 
                        KeyValueState.Deleted,
                        new(keyValueMessage.TransactionIdNode, keyValueMessage.TransactionIdPhysical, keyValueMessage.TransactionIdCounter),
                        keyValueMessage.NoRevision
                    );

                    RecordCompletionReceipt(partitionId, log.Id, keyValueMessage);

                    if (RangeRouting.IsKeyRange(keySpaceRegistry, keyValueMessage.Key))
                        writeFrequencyRegistry.GetOrCreate(partitionId).RecordWrite(keyValueMessage.Key);

                    return true;
                }

                case KeyValueRequestType.TryExtend:
                {
                    byte[]? messageValue;

                    messageValue = ByteStringPayload.GetArray(keyValueMessage.Value);

                    HLCTimestamp expires      = new(keyValueMessage.ExpireNode, keyValueMessage.ExpirePhysical, keyValueMessage.ExpireCounter);
                    HLCTimestamp lastUsed     = new(keyValueMessage.LastUsedNode, keyValueMessage.LastUsedPhysical, keyValueMessage.LastUsedCounter);
                    HLCTimestamp lastModified = new(keyValueMessage.LastModifiedNode, keyValueMessage.LastModifiedPhysical, keyValueMessage.LastModifiedCounter);

                    RegisterPendingApply(partitionId, log.Id, keyValueMessage);

                    unflushedWrites?.Record(keyValueMessage.Key, messageValue, keyValueMessage.Revision,
                        expires, lastUsed, lastModified, KeyValueState.Set, keyValueMessage.NoRevision);

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
                        keyValueMessage.NoRevision,
                        logIndex: log.Id
                    ));

                    SendInvalidateOrApply(
                        partitionId, 
                        keyValueMessage.Key, 
                        messageValue, 
                        keyValueMessage.Revision,
                        expires,
                        lastUsed,
                        lastModified,
                        KeyValueState.Set,
                        new(keyValueMessage.TransactionIdNode, keyValueMessage.TransactionIdPhysical, keyValueMessage.TransactionIdCounter),
                        keyValueMessage.NoRevision
                    );

                    RecordCompletionReceipt(partitionId, log.Id, keyValueMessage);

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
