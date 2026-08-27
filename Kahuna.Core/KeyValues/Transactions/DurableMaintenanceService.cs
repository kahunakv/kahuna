using System.Diagnostics;

using Nixie;
using Nixie.Routers;

using Kommander;
using Kommander.Time;

using Kahuna.Server.Communication.Internode;
using Kahuna.Server.Replication;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Transactions;

/// <summary>
/// The background upkeep of the durable-2PC metadata: recovering prepared intents whose decision never
/// landed, retiring terminal transaction records and the participant receipts they release, and settling
/// intents that sit in a range about to move.
///
/// Two retention rules are deliberate. A terminal record is kept for a retention window before its
/// receipts are reclaimed, and the receipt store additionally applies an age backstop independent of
/// that record-driven release — without the backstop, a receipt whose record was already gone leaked
/// forever. Reclaimed receipts are batched into one replicated forget per participant partition rather
/// than one per record, and chunked so a single entry cannot outgrow the transport's message limit.
/// </summary>
internal sealed class DurableMaintenanceService
{
    // Receipts per replicated forget entry. The sweep batches a whole pass's receipts per participant partition,
    // which without a bound could put thousands of them — an unbounded number of keys — into one Raft log entry
    // and past the transport's message limit (gRPC defaults to 4 MB, and several entries may share a frame).
    // Chunking costs one extra replication per chunk, still far below the one-per-record it replaces.
    private const int ReceiptForgetBatchMax = 512;

    private readonly KeyValuesRuntime runtime;

    private readonly KeyValuesManager manager;

    private readonly TransactionCoordinator txCoordinator;

    private readonly Ranges.RangeStateTransferService rangeStateTransfer;

    private readonly LocalLockOperations localLocks;

    // Retention GC of durable-2PC metadata (records + participant receipts): the window a terminal record is kept
    // before it and its receipts are reclaimed, and the cap that bounds one reclamation batch inside a sweep. A
    // sweep drains every eligible record; the cap only sizes its batches.
    private readonly TimeSpan durableRecordRetentionTtl;

    private readonly int durableRecordGcMaxPerPass;

    // Age backstop for the receipt store, applied independently of the record-driven release above.
    private readonly TimeSpan completionReceiptRetentionTtl;

    private readonly int durableRecoveryMaxPartitionsPerPass;

    internal DurableMaintenanceService(
        KeyValuesRuntime runtime,
        KeyValuesManager manager,
        TransactionCoordinator txCoordinator,
        Ranges.RangeStateTransferService rangeStateTransfer,
        LocalLockOperations localLocks)
    {
        this.runtime = runtime;
        this.manager = manager;
        this.txCoordinator = txCoordinator;
        this.rangeStateTransfer = rangeStateTransfer;
        this.localLocks = localLocks;

        durableRecordRetentionTtl = runtime.Configuration.TransactionOutcomeRetentionTtl;
        durableRecordGcMaxPerPass = runtime.Configuration.DurableRecordGcMaxPerPass;
        completionReceiptRetentionTtl = runtime.Configuration.CompletionReceiptRetentionTtl;
        durableRecoveryMaxPartitionsPerPass = runtime.Configuration.DurableRecoveryMaxPartitionsPerPass;
    }

    // Aliases matching the field names the moved bodies use, so those bodies stay byte-for-byte as they were.
    private IRaft raft => runtime.Raft;

    private ILogger<IKahuna> logger => runtime.Logger;

    private KahunaConfiguration configuration => runtime.Configuration;

    private IInterNodeCommunication interNodeCommunication => runtime.InterNodeCommunication;

    private KeyValueLocator locator => runtime.Locator;

    private RangeMapStore rangeMapStore => runtime.RangeMapStore;

    private CompletionReceiptStore completionReceiptStore => runtime.CompletionReceiptStore;

    private TransactionRecordStore transactionRecordStore => runtime.TransactionRecordStore;

    private PreparedIntentStore preparedIntentStore => runtime.PreparedIntentStore;

    private Writes.DurableReplicationGateway durableReplication => runtime.DurableReplication;

    private Task<bool> ForgetCompletionReceiptsReplicated(int partitionId, IReadOnlyList<CompletionReceiptRecord> receipts, CancellationToken cancellationToken) =>
        rangeStateTransfer.ForgetCompletionReceiptsReplicated(partitionId, receipts, cancellationToken);

    private Task<bool> ForgetCompletionReceiptsToPartitionLeaderAsync(int partitionId, IReadOnlyList<CompletionReceiptRecord> receipts, CancellationToken cancellationToken) =>
        rangeStateTransfer.ForgetCompletionReceiptsToPartitionLeaderAsync(partitionId, receipts, cancellationToken);

    // The settle barrier consumes only the intents, so the gather skips receipts and records: they
    // add nothing to the settle decision and only inflate the response toward the transport's limit.
    private Task<(bool Ok, IReadOnlyCollection<CompletionReceiptRecord> Receipts, IReadOnlyList<TransactionRecord> Records, IReadOnlyList<PreparedIntent> Intents)> GetRangeIntentsFromPartitionLeaderAsync(
        int sourcePartitionId, string? startKey, string? endKey, CancellationToken cancellationToken) =>
        rangeStateTransfer.GetRangeTransactionStateFromPartitionLeaderAsync(sourcePartitionId, startKey, endKey, KeyValueRangeStateKinds.Intents, cancellationToken);

    private Task<bool> ReplicateDurableThroughScheduler(int partitionId, string logType, byte[] data, Writes.WriteAdmissionClass admissionClass, CancellationToken cancellationToken) =>
        durableReplication.ReplicateDurableThroughScheduler(partitionId, logType, data, admissionClass, cancellationToken);

    private Task<TransactionRecord?> LookupDurableRecordRouted(HLCTimestamp transactionId, long epoch, string anchorKey, CancellationToken cancellationToken) =>
        durableReplication.LookupDurableRecordRouted(transactionId, epoch, anchorKey, cancellationToken);

    private Task<bool> ApplyDurableCommit(int partitionId, PreparedIntent intent, CancellationToken cancellationToken) =>
        durableReplication.ApplyDurableCommit(partitionId, intent, cancellationToken);

    private Task<bool> ApplyDurableRollback(int partitionId, PreparedIntent intent, CancellationToken cancellationToken) =>
        durableReplication.ApplyDurableRollback(partitionId, intent, cancellationToken);

    private Task<KeyValueResponseType> TryReleaseExclusiveRangeLock(HLCTimestamp transactionId, string keySpace, string? startKey, bool startInclusive, string? endKey, bool endInclusive, KeyValueDurability durability) =>
        localLocks.TryReleaseExclusiveRangeLock(transactionId, keySpace, startKey, startInclusive, endKey, endInclusive, durability);



    /// <summary>
    /// Participant-side recovery for the durable-intent path: on each partition this node leads, resolves due
    /// unresolved prepared intents to their canonical decision (presuming abort only past the decision deadline,
    /// for anchors this node also leads). No-op unless the durable-intent path is enabled. Runs off the request
    /// path; idempotent with a concurrent finalize.
    /// </summary>
    internal async Task RecoverPreparedIntents(CancellationToken cancellationToken)
    {
        if (preparedIntentStore.Count == 0)
            return;

        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        IReadOnlyList<PreparedIntent> due = preparedIntentStore.DueForRecovery(now);
        if (due.Count == 0)
            return;

        // Cap the cross-partition fan-out per pass so a large backlog spread over many partitions is drained
        // across successive collection ticks rather than fanning out to every partition (and its recovery
        // lookups) at once. Deferred partitions' intents stay due and are picked up next pass.
        int partitionCap = durableRecoveryMaxPartitionsPerPass;

        HashSet<int> partitions = [];
        foreach (PreparedIntent intent in due)
        {
            if (cancellationToken.IsCancellationRequested)
                return;
            if (partitionCap > 0 && partitions.Count >= partitionCap)
                break;

            int partitionId = locator.LocateRange(intent.Key).PartitionId;
            if (partitions.Contains(partitionId))
                continue;
            if (raft.Joined && !await raft.AmILeaderIfHosted(partitionId, cancellationToken).ConfigureAwait(false))
                continue;

            partitions.Add(partitionId);
        }

        if (partitions.Count == 0)
            return;

        DurableTransactionRecovery recovery = BuildPreparedIntentRecovery();
        foreach (int partitionId in partitions)
        {
            try
            {
                await recovery.SweepAsync(partitionId, now, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Prepared-intent recovery sweep failed for partition {Partition}", partitionId);
            }
        }
    }

    /// <summary>
    /// Retention GC for durable-2PC metadata: on the anchor partitions this node leads, reclaims terminal
    /// transaction records whose retention window has elapsed, releasing each transaction's participant completion
    /// receipts first and then purging the record. Both stores grow one entry per persistent write / per
    /// transaction and are otherwise never reclaimed, so without this sweep they retain for the node's lifetime.
    ///
    /// <para>The retention window is the safety gate: a completion receipt answers a re-delivered commit's
    /// idempotency check (<c>Committed</c> vs. ambiguous <c>MustRetry</c>) after the prepare state is gone, so a
    /// receipt is only released once no such re-delivery can still arrive — the window (
    /// <see cref="KahunaConfiguration.TransactionOutcomeRetentionTtl"/>) is far longer than the write-intent
    /// lease and any leader-change replay. A record is purged only after every one of its participants' receipts
    /// was released durably; a failed release retains the record for the next pass (missing proof ⇒ retain).</para>
    ///
    /// <para>The sweep runs in three stages — select every eligible record, then release <b>all</b> their receipts
    /// with one replicated forget per participant partition, then purge. Batching the release is what lets
    /// reclamation keep pace with commit inflow: a batch costs one round trip per partition it touches rather
    /// than one per record it reclaims, so a backlog drains in partition-count replications instead of
    /// thousands.</para>
    ///
    /// <para>One sweep drains the <b>whole</b> eligible backlog, processed in batches of at most
    /// <see cref="KahunaConfiguration.DurableRecordGcMaxPerPass"/> records each. The cap bounds a batch's
    /// receipt/purge structures and each replicated entry's size — it must not bound the sweep's total: paced at
    /// cap-per-tick the sweep reclaims at most cap ÷ collection-interval records per second, and any workload
    /// committing faster than that grows the store without bound while every checkpoint re-serializes the
    /// growing set.</para>
    /// </summary>
    internal async Task CollectDurableTransactionRecords(CancellationToken cancellationToken)
    {
        if (transactionRecordStore.Count == 0)
            return;

        TimeSpan retentionTtl = durableRecordRetentionTtl;
        if (retentionTtl <= TimeSpan.Zero)
            return; // age-based GC disabled

        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        // Transactions whose local prepared intents have not settled yet: their settlement is still in flight, so
        // do not GC their record even if the (generous) retention window has nominally elapsed.
        //
        // This guard is NODE-LOCAL: an unsettled leg on a partition this node does not replicate is invisible
        // here, and a completion receipt cannot stand in for it (receipts are recorded at materialization, and
        // an unmaterialized committed leg has none). A record purged past such a leg strands it without its
        // authority — which is why the recovery sweep refuses to presume abort for a recordless intent older
        // than the retention horizon and holds it instead (see DurableTransactionRecovery). A cluster-wide
        // settlement acknowledgment on the record would let the purge wait for proof instead; until then the
        // hold is the backstop.
        HashSet<(HLCTimestamp, long)> settlementPending = [];
        foreach (PreparedIntent intent in preparedIntentStore.Snapshot())
            settlementPending.Add((intent.TransactionId, intent.Epoch));

        int cap = durableRecordGcMaxPerPass;

        // Stage 1 — select. The records eligible in the current batch, each paired with its anchor partition, plus
        // the batch-wide receipt set (keyed by the participant partition that must forget each) those partitions
        // forget in stage 2. A record's own participant partitions are not stored per-record: they are needed only
        // to hold a record back when a forget fails (rare), and that dependency is reconstructed once in stage 3
        // from the receipt batch.
        List<(TransactionRecord Record, int AnchorPartition)> eligible = [];
        Dictionary<int, List<CompletionReceiptRecord>> receiptsByPartition = [];

        // Anchor leadership is asked once per partition rather than once per record: a backlog is typically many
        // records over few partitions, and each miss would otherwise be an await on the request path of the sweep.
        Dictionary<int, bool> anchorLeadership = [];

        // Participant partitions whose receipt forget already failed during this sweep. Later batches treat them
        // as failed without a new round trip — their dependent records stay retained for the next tick — instead
        // of re-issuing a doomed replication per batch against a partition that is down or mid-election.
        HashSet<int> failedForgetPartitions = [];

        foreach (TransactionRecord record in transactionRecordStore.Snapshot())
        {
            if (cancellationToken.IsCancellationRequested)
                break;

            if (!record.IsTerminal || record.DecidedAt == HLCTimestamp.Zero)
                continue; // undecided records belong to recovery, never GC
            if (now - record.DecidedAt < retentionTtl)
                continue; // retention window not elapsed
            if (settlementPending.Contains((record.TransactionId, record.Epoch)))
                continue; // settlement still in progress locally

            int anchorPartition = locator.LocateRange(record.RecordAnchorKey).PartitionId;

            if (!anchorLeadership.TryGetValue(anchorPartition, out bool leadsAnchor))
            {
                leadsAnchor = !raft.Joined || await raft.AmILeaderIfHosted(anchorPartition, cancellationToken).ConfigureAwait(false);
                anchorLeadership[anchorPartition] = leadsAnchor;
            }

            if (!leadsAnchor)
                continue; // only the anchor leader drives this record's GC

            AppendCompletionReceiptsForRecord(record, receiptsByPartition);
            eligible.Add((record, anchorPartition));

            if (cap > 0 && eligible.Count >= cap)
            {
                await ReclaimBatchAsync(eligible, receiptsByPartition, failedForgetPartitions, cancellationToken).ConfigureAwait(false);
                eligible.Clear();
                receiptsByPartition.Clear();
            }
        }

        if (eligible.Count > 0 && !cancellationToken.IsCancellationRequested)
            await ReclaimBatchAsync(eligible, receiptsByPartition, failedForgetPartitions, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Stages 2 and 3 of <see cref="CollectDurableTransactionRecords"/> for one selected batch:
    /// release the batch's completion receipts, then purge the records whose receipts all released durably.</summary>
    private async Task ReclaimBatchAsync(
        List<(TransactionRecord Record, int AnchorPartition)> eligible,
        Dictionary<int, List<CompletionReceiptRecord>> receiptsByPartition,
        HashSet<int> failedForgetPartitions,
        CancellationToken cancellationToken)
    {
        // Stage 2 — release. One replicated forget per participant partition, carrying every receipt this batch
        // releases on it (chunked, see ReceiptForgetBatchMax). A partition whose forget was not durable is
        // remembered so the records that depend on it stay retained; other partitions' records still purge.
        HashSet<int> unreleasedPartitions = [];
        int receiptsReleased = 0;

        foreach ((int partitionId, List<CompletionReceiptRecord> receipts) in receiptsByPartition)
        {
            if (failedForgetPartitions.Contains(partitionId))
            {
                unreleasedPartitions.Add(partitionId);
                continue;
            }

            bool partitionReleased = true;

            for (int offset = 0; offset < receipts.Count; offset += ReceiptForgetBatchMax)
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    partitionReleased = false;
                    break;
                }

                List<CompletionReceiptRecord> chunk = receipts.GetRange(
                    offset, Math.Min(ReceiptForgetBatchMax, receipts.Count - offset));

                // A chunk that fails abandons the rest for this partition. Earlier chunks stay forgotten, which is
                // safe: forget is idempotent, and every record here is retained and re-attempted on a later sweep.
                if (!await ForgetCompletionReceiptsToPartitionLeaderAsync(partitionId, chunk, cancellationToken).ConfigureAwait(false))
                {
                    partitionReleased = false;
                    break;
                }

                receiptsReleased += chunk.Count;
            }

            if (!partitionReleased)
            {
                unreleasedPartitions.Add(partitionId);
                failedForgetPartitions.Add(partitionId);
            }
        }

        if (receiptsReleased > 0)
            DurableTransactionMetrics.ReceiptsReleased(receiptsReleased);

        // Stage 3 — purge, grouped by anchor partition. A record is purged only once every partition holding one of
        // its receipts forgot it durably, so a partial failure narrows what this batch reclaims instead of purging a
        // record while a proof of it still exists somewhere.
        //
        // The set of transactions blocked by a failed forget is reconstructed here, once, from the receipt batch —
        // rather than storing each record's participant partitions in stage 1 (an allocation per record, on the hot
        // common path where nothing fails). A receipt carries only its transaction id, but within a single batch a
        // terminal record's transaction id identifies it uniquely, so keying the block set on transaction id is
        // exact. When no forget failed (the common case) the block set is empty and every eligible record purges.
        HashSet<HLCTimestamp> blockedTransactions = [];
        foreach (int partitionId in unreleasedPartitions)
        {
            foreach (CompletionReceiptRecord receipt in receiptsByPartition[partitionId])
                blockedTransactions.Add(receipt.TransactionId);
        }

        Dictionary<int, List<PurgeTransactionCommand>> purgesByAnchor = [];

        foreach ((TransactionRecord record, int anchorPartition) in eligible)
        {
            if (blockedTransactions.Contains(record.TransactionId))
                continue;

            if (!purgesByAnchor.TryGetValue(anchorPartition, out List<PurgeTransactionCommand>? purges))
                purgesByAnchor[anchorPartition] = purges = [];
            purges.Add(new PurgeTransactionCommand(record.TransactionId, record.Epoch));
        }

        // Replicate the purges per anchor partition (one delta each) through the ordered durable seam. Terminal
        // class: GC cleanup must land and must not be starved by ordinary write pressure on the anchor partition.
        foreach ((int partitionId, List<PurgeTransactionCommand> purges) in purgesByAnchor)
        {
            try
            {
                byte[] delta = TransactionRecordStore.SerializeDelta(purges);
                await ReplicateDurableThroughScheduler(partitionId, ReplicationTypes.TransactionRecord, delta,
                    Writes.WriteAdmissionClass.Terminal, cancellationToken).ConfigureAwait(false);
                DurableTransactionMetrics.RecordsReclaimed(purges.Count);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Durable transaction-record GC purge failed for partition {Partition}", partitionId);
            }
        }
    }

    /// <summary>
    /// Age backstop for the node-local completion-receipt store, run after the record sweep above and independent
    /// of it. <see cref="CollectDurableTransactionRecords"/> can only release a receipt whose transaction record
    /// still exists, but a receipt outlives its record whenever a committed persistent mutation is replayed after
    /// that record was reclaimed — which every cold restart and partition leader change does, replaying the whole
    /// retained log. Those receipts are orphans no acknowledgement will ever release, so this drops them once they
    /// are older than <see cref="KahunaConfiguration.CompletionReceiptRetentionTtl"/> and no re-delivered commit
    /// can still need them to answer <c>Committed</c>.
    ///
    /// <para>Purely node-local: receipts are derived state every replica rebuilds from its own log, so unlike the
    /// acknowledgement-driven release there is nothing to replicate — each node ages out its own copy. The sweep
    /// is in-memory only and needs no per-pass cap.</para>
    /// </summary>
    internal void CollectExpiredCompletionReceipts()
    {
        TimeSpan retentionTtl = completionReceiptRetentionTtl;
        if (retentionTtl <= TimeSpan.Zero)
            return; // backstop disabled

        if (completionReceiptStore.Count == 0)
            return;

        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        int expired = completionReceiptStore.CollectExpired(now, retentionTtl);
        if (expired == 0)
            return;

        DurableTransactionMetrics.ReceiptsExpired(expired);
        if (logger.IsEnabled(LogLevel.Debug))
            logger.LogDebug("Dropped {Count} completion receipts past the retention backstop", expired);
    }

    /// <summary>
    /// Appends a record's persistent participants' completion receipts into <paramref name="receiptsByPartition"/>,
    /// the pass-wide batch keyed by the partition that must forget each. Only persistent participants ever recorded
    /// a receipt; a manifestless tombstone or an ephemeral-only transaction wrote none and contributes nothing — it
    /// has no proof to release and is immediately purgeable. The record's own participant partitions are not
    /// returned: they are needed only to hold a record back on a failed forget (rare), which stage 3 reconstructs
    /// from this batch rather than paying a per-record allocation on every pass.
    /// </summary>
    private void AppendCompletionReceiptsForRecord(
        TransactionRecord record,
        Dictionary<int, List<CompletionReceiptRecord>> receiptsByPartition)
    {
        foreach (TransactionParticipantRef participant in record.Participants)
        {
            if (participant.Durability != KeyValueDurability.Persistent)
                continue;

            int partitionId = locator.LocateRange(participant.Key).PartitionId;

            if (!receiptsByPartition.TryGetValue(partitionId, out List<CompletionReceiptRecord>? receipts))
                receiptsByPartition[partitionId] = receipts = [];
            receipts.Add(new CompletionReceiptRecord(record.TransactionId, participant.Key, record.RecordAnchorKey, KeyValueDurability.Persistent));
        }
    }

    // One cached instance serves both the periodic sweep and the finalizer's helping pass: the recovery object
    // holds only immutable delegates, so sharing it across concurrent callers is safe.
    private DurableTransactionRecovery? durableBlockerRecovery;

    /// <summary>
    /// Finalizer seam for the prepare-conflict helping pass: settles foreign intents blocking the given keys on
    /// <paramref name="partitionId"/> when their canonical record is already terminal (settlement lag). Gated on
    /// partition leadership exactly like the recovery sweep — a non-leader's local intent store is not
    /// authoritative, and its replicate seam could not apply the settle delta anyway.
    /// </summary>
    internal async Task<int> TryResolveDecidedDurableBlockersAsync(
        int partitionId, IReadOnlyList<PreparedIntent> intents, HLCTimestamp transactionId, long epoch, CancellationToken cancellationToken)
    {
        if (raft.Joined && !await raft.AmILeaderIfHosted(partitionId, cancellationToken).ConfigureAwait(false))
            return 0;

        DurableTransactionRecovery recovery = durableBlockerRecovery ??= BuildPreparedIntentRecovery();
        return await recovery.TryResolveDecidedBlockersAsync(partitionId, intents, transactionId, epoch, cancellationToken).ConfigureAwait(false);
    }

    // The drain below must leave the caller's 30-second quiesce window enough room for the catch-up
    // copy, the state handoff and the cutover that follow it, whatever the operator configured.
    private const long MovingIntentDrainMaxMs = 15_000;

    // Delay between drain passes. Undecided intents belong to in-flight coordinators whose decisions
    // land within tens to hundreds of milliseconds; polling faster only re-gathers an unchanged set.
    private const int MovingIntentDrainDelayMs = 100;

    /// <summary>
    /// The pre-cutover settlement barrier of a range split/merge: gathers the moving range's prepared intents
    /// from the source partition's leader (the authoritative store) and settles every decided one through the
    /// recovery path, so the data copy that follows carries materialized rows instead of values that exist only
    /// as intents. Without this barrier a cutover races deferred settlement: the copied rows predate the commit
    /// and the child range serves the prior revision.
    ///
    /// <para>The barrier drains rather than gates. The caller holds the quiesce, so no new prepare can enter
    /// the moving range and the intent set can only shrink: each pass settles every decided intent, and an
    /// intent still undecided inside its window belongs to an in-flight coordinator whose decision lands
    /// shortly — so the loop waits briefly and re-gathers instead of refusing outright. A refusal on first
    /// contact would starve the move: a range under sustained writes always carries a few just-prepared
    /// intents, so a barrier that never waits refuses every attempt and the split lands only after the load
    /// stops. The wait is bounded by <see cref="KahunaConfiguration.RangeMoveSettleTimeout"/> (clamped so the
    /// quiesce window keeps room for the copy and cutover that follow).</para>
    ///
    /// <para>Returns true when a gather confirms the moving range holds no durable intent at all; false — the
    /// caller must refuse this move attempt retryably — when an intent is still unsettled at the deadline, the
    /// gather could not reach the source leader, or a gather or settle failed.</para>
    /// </summary>
    internal async Task<bool> SettleMovingRangeIntentsAsync(
        int sourcePartitionId, string? startKey, string? endKey, CancellationToken cancellationToken)
    {
        double budgetMs = Math.Min(configuration.RangeMoveSettleTimeout.TotalMilliseconds, MovingIntentDrainMaxMs);
        long startTick = Stopwatch.GetTimestamp();

        while (true)
        {
            bool ok;
            IReadOnlyList<PreparedIntent> intents;
            int unsettled;

            try
            {
                (ok, _, _, intents) =
                    await GetRangeIntentsFromPartitionLeaderAsync(sourcePartitionId, startKey, endKey, cancellationToken).ConfigureAwait(false);

                if (!ok)
                    return false;

                if (intents.Count == 0)
                    return true;

                DurableTransactionRecovery recovery = durableBlockerRecovery ??= BuildPreparedIntentRecovery();
                HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

                unsettled = await recovery.SettleSuppliedIntentsAsync(sourcePartitionId, intents, now, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                // A transport failure (an oversized gather response, a dropped call) refuses this attempt
                // retryably; it must not propagate and take down the caller's whole trigger pass.
                logger.LogWarning(ex,
                    "Settle barrier: gather/settle failed for partition {Partition} [{Start},{End}); refusing this move attempt",
                    sourcePartitionId, startKey ?? "-inf", endKey ?? "+inf");
                return false;
            }

            // The deadline bounds the whole loop, not only the undecided case: a pass whose settles all
            // landed still needs its confirming re-gather to come back empty, and if that confirmation
            // keeps lagging past the deadline the attempt refuses rather than spinning inside the quiesce.
            double elapsedMs = (Stopwatch.GetTimestamp() - startTick) * 1000.0 / Stopwatch.Frequency;

            if (elapsedMs >= budgetMs)
            {
                logger.LogWarning(
                    "Settle barrier: partition {Partition} [{Start},{End}) still gathered {Gathered} durable intents ({Unsettled} unsettled) after {Elapsed:F0} ms; refusing this move attempt",
                    sourcePartitionId, startKey ?? "-inf", endKey ?? "+inf", intents.Count, unsettled, elapsedMs);
                return false;
            }

            // Unsettled intents are undecided coordinators: give their decisions real time to land before
            // the re-gather. A fully settled pass re-gathers after the same short delay, which lets the
            // settle deltas' ordered apply land so the confirming gather reads them back as absent.
            await Task.Delay(MovingIntentDrainDelayMs, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// The zero-impact admission gate a split runs before it invests in an attempt: one settle pass
    /// over the moving range's intents with no quiesce held, followed by an age check on whatever
    /// could not settle. Returns true when the quiesced drain that follows is expected to finish
    /// quickly; false — the caller should refuse the attempt retryably and back off — when it is not.
    ///
    /// <para>Why it exists: everything after this point costs the cluster real work — a bulk copy of
    /// the whole moving half, and then a quiesce whose exclusive range lock stamps write intents on
    /// every resident key and refuses the range's writes for up to the full drain budget. An attempt
    /// that ends refused at the in-quiesce barrier pays all of that for nothing, and under sustained
    /// load those refused attempts — not the completed splits — are what halves client throughput.
    /// This gate moves the common refusal to a point where it disturbs nothing.</para>
    ///
    /// <para>The verdict is a heuristic on intent age, not a zero-intent requirement — requiring zero
    /// without a quiesce would re-create the starvation this machinery exists to avoid, because new
    /// prepares are still flowing. Decided intents settle here (useful work in any outcome). A
    /// survivor is an undecided coordinator: a young one (inside the drain budget) is expected to
    /// decide within the quiesced drain, an old one has already out-waited a full budget without a
    /// decision and would very likely stall the quiesced drain to its deadline too.</para>
    /// </summary>
    internal async Task<bool> PreSettleMovingRangeIntentsAsync(
        int sourcePartitionId, string? startKey, string? endKey, CancellationToken cancellationToken)
    {
        double budgetMs = Math.Min(configuration.RangeMoveSettleTimeout.TotalMilliseconds, MovingIntentDrainMaxMs);

        try
        {
            (bool ok, _, _, IReadOnlyList<PreparedIntent> intents) =
                await GetRangeIntentsFromPartitionLeaderAsync(sourcePartitionId, startKey, endKey, cancellationToken).ConfigureAwait(false);

            if (!ok)
                return false;

            if (intents.Count == 0)
                return true;

            DurableTransactionRecovery recovery = durableBlockerRecovery ??= BuildPreparedIntentRecovery();
            HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

            if (await recovery.SettleSuppliedIntentsAsync(sourcePartitionId, intents, now, cancellationToken).ConfigureAwait(false) == 0)
                return true;

            // Survivors of the settle pass are undecided coordinators (or settles that must retry).
            // Re-gather so the aged set reflects what actually remains, then judge by age.
            (ok, _, _, intents) =
                await GetRangeIntentsFromPartitionLeaderAsync(sourcePartitionId, startKey, endKey, cancellationToken).ConfigureAwait(false);

            if (!ok)
                return false;

            now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

            long oldestAgeMs = 0;
            foreach (PreparedIntent intent in intents)
                oldestAgeMs = Math.Max(oldestAgeMs, now.L - intent.CommitTimestamp.L);

            if (oldestAgeMs <= budgetMs)
                return true;

            logger.LogInformation(
                "Pre-settle gate: partition {Partition} [{Start},{End}) holds {Count} unsettled durable intents, oldest {OldestMs} ms; refusing before the quiesce",
                sourcePartitionId, startKey ?? "-inf", endKey ?? "+inf", intents.Count, oldestAgeMs);

            return false;
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex,
                "Pre-settle gate: gather/settle failed for partition {Partition} [{Start},{End}); refusing this move attempt",
                sourcePartitionId, startKey ?? "-inf", endKey ?? "+inf");
            return false;
        }
    }

    private DurableTransactionRecovery BuildPreparedIntentRecovery() => new(
        preparedIntentStore,
        // The scheduler seam is the single ordered apply owner: recovery's settle/materialize deltas apply in Raft
        // order alongside any concurrent finalizer decision for the same record, so the two cannot diverge.
        ReplicateDurableThroughScheduler,
        // Anchor record lookup routed to the anchor partition leader: a participant recovering an orphan intent
        // whose anchor lives on another node now reads the authoritative decision there instead of missing locally
        // and leaving it for the anchor's own sweep. A remote-anchor commit/abort is resolved directly; only a
        // genuinely absent/undecided record falls through to the leadership-gated drive-abort path.
        (transactionId, epoch, anchorKey, cancellationToken) => LookupDurableRecordRouted(transactionId, epoch, anchorKey, cancellationToken),
        DriveDurableAbortAsync,
        // Apply the recovered committed value to the leader's own KV state. Materialization replication converges
        // followers and makes the value durable, but the leader materializes into its in-memory MVCC through this
        // dedicated apply path — without it a recovered commit is invisible on the recovering leader until restart.
        (partitionId, intent) => ApplyDurableCommit(partitionId, intent, CancellationToken.None),
        // The record retention horizon bounds when record absence can still be read as "never initialized":
        // past it, the absent record may be a reclaimed commit and the sweep holds the intent instead of
        // presuming abort — the guard against discarding a committed leg whose settlement kept failing.
        runtime.Configuration.TransactionOutcomeRetentionTtl,
        logger);

    private async Task<TransactionRecord?> DriveDurableAbortAsync(AbortTransactionCommand abort, string anchorKey, CancellationToken cancellationToken)
    {
        int anchorPartition = locator.LocateRange(anchorKey).PartitionId;

        // Only drive/read the decision when this node leads the anchor partition; otherwise the local record store
        // is not authoritative and applying an abort locally could diverge from the real remote decision.
        if (raft.Joined && !await raft.AmILeaderIfHosted(anchorPartition, cancellationToken).ConfigureAwait(false))
            return null;

        // Drive the abort through the ordered scheduler seam (this node leads the anchor partition), which applies
        // it in Raft order — never overwriting a commit that won earlier in the log. Then read back the winner.
        byte[] delta = TransactionRecordStore.SerializeDelta([abort]);
        // A recovery-driven abort is terminal work resolving an already-prepared transaction — admit as Terminal.
        await ReplicateDurableThroughScheduler(anchorPartition, ReplicationTypes.TransactionRecord, delta, Writes.WriteAdmissionClass.Terminal, cancellationToken).ConfigureAwait(false);

        return transactionRecordStore.Get(abort.TransactionId, abort.Epoch);
    }

    /// <summary>
    /// Releases an exclusive range lock on the leader of <paramref name="partitionId"/>, forwarding
    /// via IPC if this node is not the leader. Used by <see cref="RangeSplitter"/> to release the
    /// quiesce lock on the <em>original</em> partition after cutover, bypassing the locator which
    /// would otherwise route to the newly-created partition.
    /// </summary>
    internal async Task<KeyValueResponseType> ReleaseExclusiveRangeLockOnPartitionLeaderAsync(
        int partitionId,
        HLCTimestamp transactionId,
        string keySpace,
        string? startKey, bool startInclusive,
        string? endKey, bool endInclusive,
        KeyValueDurability durability,
        CancellationToken cancellationToken)
    {
        if (!raft.Joined || await raft.AmILeaderIfHosted(partitionId, cancellationToken).ConfigureAwait(false))
            return await TryReleaseExclusiveRangeLock(transactionId, keySpace, startKey, startInclusive, endKey, endInclusive, durability).ConfigureAwait(false);

        // Placement-safe resolution: the split/merge driver may not host the source partition.
        // An unroutable target reports MustRetry — the lock has a TTL, so a missed release only
        // delays direct writes on the old range until the quiesce lease expires.
        string? leader = await raft.TryResolveLeader(partitionId, cancellationToken).ConfigureAwait(false);
        if (leader is null)
            return KeyValueResponseType.MustRetry;
        if (leader == raft.GetLocalEndpoint())
            return await TryReleaseExclusiveRangeLock(transactionId, keySpace, startKey, startInclusive, endKey, endInclusive, durability).ConfigureAwait(false);

        // The receiver must run the release on its own actor state, not re-route it through the
        // locator: this call targets the node where the lock was acquired, and after a split or
        // merge cutover the receiver's range map routes these bounds to the NEW partition — a
        // re-locate then misdirects (or refuses) the release and strands the lock's per-key write
        // intents on this partition's leader for the rest of their lease, refusing every snapshot
        // scan of the moved range that lands there. Pinning the target partition tells the
        // receiver to execute locally.
        return await interNodeCommunication.TryReleaseExclusiveRangeLock(leader, transactionId, keySpace, startKey, startInclusive, endKey, endInclusive, durability, cancellationToken, targetPartitionId: partitionId).ConfigureAwait(false);
    }
}
