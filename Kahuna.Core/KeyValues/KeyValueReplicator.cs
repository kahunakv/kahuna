
using System.Collections.Concurrent;
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
    /// The settle-time convergence gate: decides whether this node's local durable state is missing a
    /// commit's materialization at the moment its settlement applies. The unflushed overlay is the witness
    /// that this node's replicator processed the commit's kv record — the replicator records the head there
    /// before anything else, and the entry stays until the flush lands — so an overlay entry at or above the
    /// intent's revision proves the local durable read path already serves the value. An absent or older
    /// entry means the record's apply never ran here (a skipped consumer apply at a leadership boundary) and
    /// the settled intent, still carrying the full mutation, must re-drive it. A false miss — the flush
    /// landed inside the materialize→settle gap and removed the entry — costs one idempotent no-op ask.
    /// Without an overlay (raw-backend configurations) absence cannot be proven either way, so the repair
    /// always runs and degrades to the same no-op.
    /// </summary>
    internal static bool LocalMaterializationMissing(UnflushedKeyValueWritesIndex? overlay, PreparedIntent intent) =>
        overlay is null
        || !overlay.TryGet(intent.Key, out UnflushedKeyValueWrite pending)
        || pending.Revision < intent.Revision;

    /// <summary>
    /// Detached coherence reconcile for a key whose resident entry stopped converging with this node's own
    /// durable state — detected as a fence-refusal streak at a frozen (validated base, committed head) pair.
    /// Reads the durable row off the actor (backend or unflushed overlay; the value is durable here even when
    /// the actor dropped its one coherence notification) and hands it to the owning actor as a reconcile
    /// message, which adopts it when strictly newer and clears the blocking write intent. Fire-and-forget:
    /// the caller sits on the replicated prepare-apply path and must not block; a missed reconcile re-arms on
    /// the continuing refusal streak.
    /// </summary>
    public void ScheduleCoherenceReconcile(int partitionId, string key, long committedHeadRevision)
    {
        Func<int, string, Task<KeyValueEntry?>>? hydrate = hydrateFromBackend;
        if (hydrate is null)
            return;

        _ = Task.Run(async () =>
        {
            try
            {
                KeyValueEntry? row = await hydrate(partitionId, key).ConfigureAwait(false);

                // The reconcile can only heal from what this node durably holds. A local row below the
                // committed head means the head commit's value never reached this node's durable state at
                // all — the reconcile cannot converge the key from here, and staying silent about that made
                // a 46-minute wedge diagnosable only by inference. Say it plainly; the settle-time repair
                // owns re-driving the mutation, and this alarm firing means that repair did not land either.
                if (row is null || row.Revision < committedHeadRevision)
                    logger.LogError(
                        "Coherence reconcile for key {Key} cannot heal: the local durable row is at revision {LocalRevision} but the committed head is {HeadRevision} — this node's durable state is missing the head commit",
                        key, row?.Revision ?? -1, committedHeadRevision);

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
    /// harmless.
    ///
    /// <para>The repair is armed-until-confirmed, never one-shot: the intent — the Raft-delivered copy of the
    /// committed mutation, and the only copy this node holds once the settle removed it from the intent store
    /// (the recovery sweep only sees LIVE intents, so no sweep ever backstops a settled one) — is PARKED
    /// before the first drive. The drive itself retries on a short backoff ladder to ride out the disturbed
    /// window that typically caused the miss (a peer pause backing up the read scheduler, actor churn); if it
    /// still cannot confirm, the parked mutation stays armed and the fence's refusal-streak hook re-drives it
    /// (<see cref="RetryPendingCommitRepair"/>) for as long as the key keeps refusing. Without this, a repair
    /// lost in the same pause window that caused the skip left the key read-only to run end.</para>
    /// </summary>
    public void ScheduleDurableCommitRepair(int partitionId, PreparedIntent intent)
    {
        ParkCommitRepair(intent);
        DriveCommitRepairDetached(partitionId, intent);
    }

    /// <summary>
    /// Re-drives a parked commit repair for <paramref name="key"/>, if one is armed — invoked by the fence's
    /// refusal-streak hook, so a repair that failed its initial drives keeps being retried for as long as the
    /// wedge it would heal keeps refusing writes. Returns whether a parked mutation existed.
    /// </summary>
    public bool RetryPendingCommitRepair(int partitionId, string key)
    {
        if (!pendingCommitRepairs.TryGetValue(key, out PreparedIntent? parked))
            return false;

        DriveCommitRepairDetached(partitionId, parked);
        return true;
    }

    /// <summary>
    /// Drops any parked commit repair for <paramref name="key"/> at or below <paramref name="upToRevision"/>:
    /// a later settle whose materialization is locally proven (the overlay witness passed) supersedes an older
    /// parked mutation, which could otherwise retain its value bytes for the process lifetime.
    /// </summary>
    public void DiscardPendingCommitRepair(string key, long upToRevision)
    {
        if (pendingCommitRepairs.TryGetValue(key, out PreparedIntent? parked) && parked.Revision <= upToRevision)
            pendingCommitRepairs.TryRemove(new KeyValuePair<string, PreparedIntent>(key, parked));
    }

    /// <summary>Test observability: the parked repair for <paramref name="key"/>, or null.</summary>
    internal PreparedIntent? TryGetPendingCommitRepair(string key) =>
        pendingCommitRepairs.TryGetValue(key, out PreparedIntent? parked) ? parked : null;

    // The armed repairs: one per key, keeping the newest revision. Entries are added only when the settle-time
    // witness detected a locally missing materialization (rare), removed on a confirmed drive or a superseding
    // proven settle, and capped as a memory backstop — at the cap, new arrivals are still driven, just not
    // parked (the refusal streak then has nothing to re-drive, which the drive-failure log records).
    private readonly ConcurrentDictionary<string, PreparedIntent> pendingCommitRepairs = new(StringComparer.Ordinal);

    private const int MaxPendingCommitRepairs = 4_096;

    // Collapses concurrent drives per key: the streak hook re-fires every few seconds while a drive ladder can
    // still be mid-backoff; stacking identical idempotent drives is harmless but pointless.
    private readonly ConcurrentDictionary<string, byte> commitRepairsInFlight = new(StringComparer.Ordinal);

    // Backoff ladder for one drive: rides out the short disturbed window (a peer pause, a backed-up read
    // scheduler) that typically caused both the original miss and a failed first attempt.
    private static readonly TimeSpan[] CommitRepairBackoff =
        [TimeSpan.Zero, TimeSpan.FromMilliseconds(250), TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(4), TimeSpan.FromSeconds(15)];

    private void ParkCommitRepair(PreparedIntent intent)
    {
        if (pendingCommitRepairs.Count >= MaxPendingCommitRepairs && !pendingCommitRepairs.ContainsKey(intent.Key))
        {
            logger.LogWarning(
                "Pending commit-repair registry is full; the repair for key {Key} of transaction {TransactionId} runs un-parked and cannot be re-driven if it fails",
                intent.Key, intent.TransactionId);
            return;
        }

        pendingCommitRepairs.AddOrUpdate(
            intent.Key,
            intent,
            (_, existing) => existing.Revision > intent.Revision ? existing : intent);
    }

    private void DriveCommitRepairDetached(int partitionId, PreparedIntent intent)
    {
        if (!commitRepairsInFlight.TryAdd(intent.Key, 0))
            return;

        _ = Task.Run(async () =>
        {
            try
            {
                foreach (TimeSpan delay in CommitRepairBackoff)
                {
                    if (delay > TimeSpan.Zero)
                        await Task.Delay(delay).ConfigureAwait(false);

                    bool confirmed;
                    try
                    {
                        confirmed = await ApplyDurableCommit(partitionId, intent).ConfigureAwait(false);
                    }
                    catch
                    {
                        confirmed = false;
                    }

                    if (confirmed)
                    {
                        DiscardPendingCommitRepair(intent.Key, intent.Revision);
                        return;
                    }
                }

                logger.LogWarning(
                    "Materialization repair for key {Key} of transaction {TransactionId} did not confirm after retries; the mutation stays armed and the refusal streak re-drives it",
                    intent.Key, intent.TransactionId);
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex,
                    "Materialization repair for key {Key} of transaction {TransactionId} failed; the mutation stays armed and the refusal streak re-drives it",
                    intent.Key, intent.TransactionId);
            }
            finally
            {
                commitRepairsInFlight.TryRemove(intent.Key, out _);
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
