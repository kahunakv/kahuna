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
///
/// The retention window is bounded in <b>memory</b> as well as time. Records and receipts are retained on
/// every replica, so a purely time-bounded window grows linearly with the commit rate — at a few thousand
/// commits per second a five-minute window is more than a gigabyte of gen2 on every node, and the node
/// that runs out of heap first does so inside the Raft WAL write. The resident-metadata budget
/// (<see cref="KahunaConfiguration.DurableRecordRetentionMax"/>, <c>…MaxBytes</c>) makes the window yield:
/// above the budget the sweep reclaims the oldest terminal records ahead of their TTL, never below
/// <see cref="KahunaConfiguration.DurableRecordRetentionFloor"/>, and a heap-pressure valve reclaims
/// everything past the floor when the managed heap nears its limit. The floor is the retention horizon
/// recovery reasons with (<see cref="EffectiveMinimumRetention"/>), which keeps early reclaim safe.
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

    // Resident-metadata budget (see the class remarks): count and estimated-byte bounds on the records plus
    // receipts resident on this node, the managed-heap load above which the pressure valve opens, and the
    // floor below which no record is reclaimed early whatever the budget says.
    private readonly int retentionMaxRecords;

    private readonly long retentionMaxBytes;

    private readonly double retentionHeapPressure;

    private readonly TimeSpan retentionFloor;

    // The sweep reclaims down to this fraction of a budget rather than exactly to it, so a steady inflow does
    // not put every tick back over the line by the time it runs.
    private const double BudgetLowWaterFraction = 0.9;

    // One warning per streak of budget-driven reclaim, then a reminder at this interval while it continues: the
    // condition is the steady state under sustained load (the 1.7.8 soaks sat 1.8-1.9x over the record budget for
    // 45 minutes on every node, bounded by the floor) and must not become a line per tick or per minute. The
    // continuous signal is the kahuna.durable_tx.retention_over_budget gauge and the gc_budget_sweeps counter.
    internal static readonly TimeSpan BudgetLogInterval = TimeSpan.FromMinutes(10);

    private readonly RetentionBudgetLogGate budgetLogGate = new(BudgetLogInterval);

    /// <summary>True while the last sweep found the resident-metadata budget exceeded (the
    /// <c>kahuna.durable_tx.retention_over_budget</c> gauge). Written by the sweep, read by the gauge callback.</summary>
    internal bool RetentionOverBudget => Volatile.Read(ref retentionOverBudget);

    private bool retentionOverBudget;

    /// <summary>
    /// The shortest age at which a terminal record may be reclaimed on any node — the floor while a budget or
    /// the pressure valve is enabled, otherwise the full TTL. Prepared-intent recovery uses this as the
    /// horizon past which an absent record can no longer be read as "never initialized": with early reclaim
    /// possible, an intent older than the floor with no record may be a reclaimed commit and is held rather
    /// than presumed aborted. Early reclaim is safe exactly because this horizon moves with it.
    /// </summary>
    internal TimeSpan EffectiveMinimumRetention { get; }

    /// <summary>Whether any memory bound on the retained metadata is enabled.</summary>
    internal bool RetentionBudgetEnabled => retentionMaxRecords > 0 || retentionMaxBytes > 0 || HeapPressureValveEnabled;

    private bool HeapPressureValveEnabled => retentionHeapPressure > 0 && retentionHeapPressure < 1;

    /// <summary>Set by the last record sweep when it ran under heap pressure, so the receipt backstop that follows
    /// it in the same tick runs at the floor instead of waiting for its own interval.</summary>
    internal bool HeapPressureObserved { get; private set; }

    /// <summary>Test seam: overrides the managed-heap load reading (0..1) the pressure valve compares against.</summary>
    internal Func<double>? HeapLoadProbe { get; set; }

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

        KahunaConfiguration configuration = runtime.Configuration;

        durableRecordRetentionTtl = configuration.TransactionOutcomeRetentionTtl;
        durableRecordGcMaxPerPass = configuration.DurableRecordGcMaxPerPass;
        completionReceiptRetentionTtl = configuration.CompletionReceiptRetentionTtl;
        durableRecoveryMaxPartitionsPerPass = configuration.DurableRecoveryMaxPartitionsPerPass;

        retentionMaxRecords = configuration.DurableRecordRetentionMax;
        retentionMaxBytes = configuration.DurableRecordRetentionMaxBytes;
        retentionHeapPressure = configuration.DurableRecordRetentionHeapPressure;
        retentionFloor = ResolveRetentionFloor(configuration, runtime.Logger);

        EffectiveMinimumRetention = RetentionBudgetEnabled && durableRecordRetentionTtl > TimeSpan.Zero
            ? (retentionFloor < durableRecordRetentionTtl ? retentionFloor : durableRecordRetentionTtl)
            : durableRecordRetentionTtl;
    }

    /// <summary>
    /// The floor the budget honors: the configured value, raised to the longest an orphaned prepared intent can
    /// take to be swept — its decision-deadline ceiling plus two maintenance ticks (one to become due, one of
    /// rotation slack). Below that, a genuine orphan could reach the hold horizon before recovery presumes it
    /// aborted and would be held instead, blocking its key space; the floor is the one knob that must never
    /// undercut recovery, so a misconfiguration is corrected with a warning rather than honored.
    /// </summary>
    internal static TimeSpan ResolveRetentionFloor(KahunaConfiguration configuration, ILogger<IKahuna>? logger)
    {
        TimeSpan tick = MaintenanceTick(configuration);
        TimeSpan orphanHorizon = TimeSpan.FromMilliseconds(Math.Max(0, configuration.DurableDecisionDeadlineCeilingMs)) + tick + tick;

        TimeSpan configured = configuration.DurableRecordRetentionFloor;
        if (configured >= orphanHorizon)
            return configured;

        logger?.LogWarning(
            "DurableRecordRetentionFloor {Configured} is below the orphan-recovery horizon (decision-deadline ceiling {Ceiling} + 2 × maintenance tick {Tick}); raising it to {Effective} so a memory-budget reclaim can never precede the recovery of an orphaned prepared intent",
            configured, TimeSpan.FromMilliseconds(configuration.DurableDecisionDeadlineCeilingMs), tick, orphanHorizon);

        return orphanHorizon;
    }

    /// <summary>The maintenance actor's tick: <see cref="KahunaConfiguration.DurableMaintenanceInterval"/> clamped
    /// to the collection interval, which it falls back to when non-positive.</summary>
    internal static TimeSpan MaintenanceTick(KahunaConfiguration configuration)
    {
        TimeSpan collection = configuration.CollectionInterval;
        TimeSpan maintenance = configuration.DurableMaintenanceInterval;

        if (maintenance <= TimeSpan.Zero || (collection > TimeSpan.Zero && maintenance > collection))
            return collection;

        return maintenance;
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

    private Task<bool> ReplicateDurableThroughScheduler(int partitionId, string logType, byte[] data, Writes.WriteAdmissionClass admissionClass, Writes.WriteSubmissionStage stage, CancellationToken cancellationToken) =>
        durableReplication.ReplicateDurableThroughScheduler(partitionId, logType, data, admissionClass, stage, cancellationToken);

    private Task<TransactionRecord?> LookupDurableRecordRouted(HLCTimestamp transactionId, long epoch, string anchorKey, CancellationToken cancellationToken) =>
        durableReplication.LookupDurableRecordRouted(transactionId, epoch, anchorKey, cancellationToken);

    private Task<bool> ApplyDurableCommit(int partitionId, PreparedIntent intent, CancellationToken cancellationToken) =>
        durableReplication.ApplyDurableCommit(partitionId, intent, cancellationToken);

    private Task<bool> ApplyDurableRollback(int partitionId, PreparedIntent intent, CancellationToken cancellationToken) =>
        durableReplication.ApplyDurableRollback(partitionId, intent, cancellationToken);

    private Task<KeyValueResponseType> TryReleaseExclusiveRangeLock(HLCTimestamp transactionId, string keySpace, string? startKey, bool startInclusive, string? endKey, bool endInclusive, KeyValueDurability durability) =>
        localLocks.TryReleaseExclusiveRangeLock(transactionId, keySpace, startKey, startInclusive, endKey, endInclusive, durability);



    // The partition after which the next capped recovery pass resumes its rotation. Without the
    // rotation, a backlog concentrated on the store's first-enumerated partitions would fill the
    // per-pass cap on every tick and a later partition's due intents would never be swept — an
    // orphan there would silently cross the record-retention horizon un-aborted and wedge forever.
    private int recoverySweepResumeAfterPartition = -1;

    /// <summary>
    /// Participant-side recovery for the durable-intent path: on each partition this node leads, resolves due
    /// unresolved prepared intents to their canonical decision, presuming abort only past the decision deadline
    /// (the abort drive routes to the anchor partition's leader when it is remote). No-op unless the
    /// durable-intent path is enabled. Runs off the request path; idempotent with a concurrent finalize.
    /// </summary>
    internal async Task RecoverPreparedIntents(CancellationToken cancellationToken)
    {
        if (preparedIntentStore.Count == 0)
            return;

        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        IReadOnlyList<PreparedIntent> due = preparedIntentStore.DueForRecovery(now);
        if (due.Count == 0)
            return;

        // Every led partition with due intents, deduplicated; sorted so the rotation below is stable.
        SortedSet<int> ledDuePartitions = [];
        foreach (PreparedIntent intent in due)
        {
            if (cancellationToken.IsCancellationRequested)
                return;

            int partitionId = locator.LocateRange(intent.Key).PartitionId;
            if (ledDuePartitions.Contains(partitionId))
                continue;
            if (raft.Joined && !await raft.AmILeaderIfHosted(partitionId, cancellationToken).ConfigureAwait(false))
                continue;

            ledDuePartitions.Add(partitionId);
        }

        if (ledDuePartitions.Count == 0)
            return;

        // Cap the cross-partition fan-out per pass so a large backlog spread over many partitions is drained
        // across successive collection ticks rather than fanning out to every partition (and its recovery
        // lookups) at once. The cap rotates: each pass resumes after the last partition the previous pass
        // swept, so a persistent backlog on the low partitions cannot starve the high ones out of recovery.
        int partitionCap = durableRecoveryMaxPartitionsPerPass;

        List<int> partitions = SelectRotated([.. ledDuePartitions], recoverySweepResumeAfterPartition, partitionCap);
        recoverySweepResumeAfterPartition = partitions[^1];

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
    /// Takes up to <paramref name="cap"/> items from <paramref name="sorted"/>, resuming strictly after
    /// <paramref name="resumeAfter"/> and wrapping to the front — the rotation that keeps a capped sweep
    /// fair across passes. A non-positive cap takes everything. Pure, so the fairness is directly testable.
    /// </summary>
    internal static List<int> SelectRotated(List<int> sorted, int resumeAfter, int cap)
    {
        if (cap <= 0 || sorted.Count <= cap)
            return sorted;

        int start = 0;
        while (start < sorted.Count && sorted[start] <= resumeAfter)
            start++;

        List<int> taken = new(cap);
        for (int i = 0; i < cap; i++)
            taken.Add(sorted[(start + i) % sorted.Count]);

        return taken;
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
    /// <para>The window yields to memory. Records and receipts are resident on every replica, so a window bounded
    /// only in time grows linearly with the commit rate with no ceiling. When the resident count or estimated
    /// bytes exceed the budget (<see cref="KahunaConfiguration.DurableRecordRetentionMax"/>, <c>…MaxBytes</c>),
    /// the sweep also reclaims the oldest terminal records that are past
    /// <see cref="KahunaConfiguration.DurableRecordRetentionFloor"/> but not yet past their TTL — oldest decision
    /// first, this leader's proportional share of the overage, down to a low-water mark — and when the managed
    /// heap is above <see cref="KahunaConfiguration.DurableRecordRetentionHeapPressure"/> it reclaims every such
    /// record at once. The floor is what recovery treats as the retention horizon, so a record reclaimed early
    /// is never mistaken for one that never existed.</para>
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
        HeapPressureObserved = false;

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

        // The memory budget is judged once per sweep, from the stores' running counters and the heap, before
        // the scan: the scan then collects early-reclaim candidates only when there is an overage to cover.
        RetentionPressure pressure = AssessRetentionPressure();
        HeapPressureObserved = pressure.HeapPressure;

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

        // Early-reclaim candidates: led, settled, terminal records past the floor but inside their TTL. Collected
        // only under budget pressure; ordered and trimmed after the scan, since which are oldest is only known
        // once all are seen.
        List<(TransactionRecord Record, int AnchorPartition)>? budgetCandidates = pressure.OverBudget ? [] : null;

        IReadOnlyCollection<TransactionRecord> snapshot = transactionRecordStore.Snapshot();

        // For the proportional share: among the terminal records old enough to reclaim at all (past the floor),
        // how many this node leads vs. how many are resident here. Records inside the floor are skipped before
        // any routing or leadership lookup — the common case on a healthy node must stay a couple of field reads
        // per record — and they are distributed across leaders like the older ones, so the ratio is unaffected.
        int reclaimableTotal = 0;
        int reclaimableLed = 0;
        int expiredSelected = 0;

        foreach (TransactionRecord record in snapshot)
        {
            if (cancellationToken.IsCancellationRequested)
                break;

            if (!record.IsTerminal || record.DecidedAt == HLCTimestamp.Zero)
                continue; // undecided records belong to recovery, never GC

            TimeSpan age = now - record.DecidedAt;
            bool expired = age >= retentionTtl;

            // Inside the floor nothing reclaims it; inside the TTL only a budget overage can.
            if (!expired && (budgetCandidates is null || age < retentionFloor))
                continue; // retention window not elapsed

            reclaimableTotal++;

            int anchorPartition = locator.LocateRange(record.RecordAnchorKey).PartitionId;

            if (!anchorLeadership.TryGetValue(anchorPartition, out bool leadsAnchor))
            {
                leadsAnchor = !raft.Joined || await raft.AmILeaderIfHosted(anchorPartition, cancellationToken).ConfigureAwait(false);
                anchorLeadership[anchorPartition] = leadsAnchor;
            }

            if (!leadsAnchor)
                continue; // only the anchor leader drives this record's GC

            reclaimableLed++;

            if (settlementPending.Contains((record.TransactionId, record.Epoch)))
                continue; // settlement still in progress locally

            if (!expired)
            {
                budgetCandidates!.Add((record, anchorPartition));
                continue;
            }

            AppendCompletionReceiptsForRecord(record, receiptsByPartition);
            eligible.Add((record, anchorPartition));
            expiredSelected++;

            if (cap > 0 && eligible.Count >= cap)
            {
                await ReclaimBatchAsync(eligible, receiptsByPartition, failedForgetPartitions, cancellationToken).ConfigureAwait(false);
                eligible.Clear();
                receiptsByPartition.Clear();
            }
        }

        if (eligible.Count > 0 && !cancellationToken.IsCancellationRequested)
        {
            await ReclaimBatchAsync(eligible, receiptsByPartition, failedForgetPartitions, cancellationToken).ConfigureAwait(false);
            eligible.Clear();
            receiptsByPartition.Clear();
        }

        // Stage 1b — the budget's share. Oldest decisions first: they are the records with the least remaining
        // value as an idempotency answer and the ones the TTL would have reclaimed next anyway.
        if (budgetCandidates is null || cancellationToken.IsCancellationRequested)
        {
            NoteBudgetState(pressure, reclaimed: 0, eligible: 0);
            return;
        }

        int take = ChooseEarlyReclaimCount(pressure, budgetCandidates, reclaimableTotal, reclaimableLed, expiredSelected);
        NoteBudgetState(pressure, reclaimed: take, eligible: budgetCandidates.Count);

        if (take <= 0)
            return;

        budgetCandidates.Sort(static (a, b) =>
        {
            int byDecision = a.Record.DecidedAt.CompareTo(b.Record.DecidedAt);
            return byDecision != 0 ? byDecision : a.Record.TransactionId.CompareTo(b.Record.TransactionId);
        });

        DurableTransactionMetrics.RecordsReclaimedEarly(take, pressure.HeapPressure);
        if (pressure.HeapPressure)
            DurableTransactionMetrics.HeapPressureSweep();

        for (int i = 0; i < take; i++)
        {
            if (cancellationToken.IsCancellationRequested)
                break;

            (TransactionRecord record, int anchorPartition) = budgetCandidates[i];
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

    /// <summary>The budget verdict for one sweep: how far the resident metadata is over each bound, and whether the
    /// heap-pressure valve is open.</summary>
    internal readonly record struct RetentionPressure(long RecordOverage, long ByteOverage, bool HeapPressure, double HeapLoad)
    {
        public bool OverBudget => HeapPressure || RecordOverage > 0 || ByteOverage > 0;
    }

    /// <summary>
    /// Reads the budget inputs — the stores' running count and byte estimates, and the managed-heap load — and
    /// reports the overage above each bound's low-water mark. Pure with respect to the stores; reading it costs
    /// two counters and one <see cref="GC.GetGCMemoryInfo()"/>.
    /// </summary>
    internal RetentionPressure AssessRetentionPressure()
    {
        long recordOverage = 0;
        if (retentionMaxRecords > 0)
        {
            long lowWater = (long)(retentionMaxRecords * BudgetLowWaterFraction);
            long resident = transactionRecordStore.Count;
            if (resident > retentionMaxRecords)
                recordOverage = resident - lowWater;
        }

        long byteOverage = 0;
        if (retentionMaxBytes > 0)
        {
            long lowWater = (long)(retentionMaxBytes * BudgetLowWaterFraction);
            long resident = transactionRecordStore.EstimatedBytes + completionReceiptStore.EstimatedBytes;
            if (resident > retentionMaxBytes)
                byteOverage = resident - lowWater;
        }

        double heapLoad = 0;
        bool heapPressure = false;
        if (HeapPressureValveEnabled)
        {
            heapLoad = HeapLoadProbe?.Invoke() ?? ReadManagedHeapLoad();
            heapPressure = heapLoad >= retentionHeapPressure;
        }

        return new RetentionPressure(recordOverage, byteOverage, heapPressure, heapLoad);
    }

    /// <summary>Post-GC managed heap size over the runtime's available memory (the heap hard limit when one is
    /// configured, the machine's or container's memory otherwise); 0 when the runtime reports no bound.</summary>
    private static double ReadManagedHeapLoad()
    {
        GCMemoryInfo info = GC.GetGCMemoryInfo();
        if (info.TotalAvailableMemoryBytes <= 0)
            return 0;

        return (double)info.HeapSizeBytes / info.TotalAvailableMemoryBytes;
    }

    /// <summary>
    /// How many of the (unsorted) early-reclaim candidates this sweep takes. Under heap pressure, all of them.
    /// Otherwise this leader's proportional share of the overage: every anchor leader in the cluster sees the
    /// same resident total (records replicate to every replica of their anchor partition) and each can only
    /// reclaim what it leads, so each takes <c>overage × led ÷ total</c> and the cluster converges to the
    /// low-water mark in one round instead of every leader draining a full overage's worth. The records the
    /// TTL already reclaimed this sweep count against the overage first. Pure, so the share is testable.
    /// </summary>
    internal static int ChooseEarlyReclaimCount(
        RetentionPressure pressure,
        List<(TransactionRecord Record, int AnchorPartition)> candidates,
        int terminalTotal,
        int terminalLed,
        int expiredSelected)
    {
        if (candidates.Count == 0)
            return 0;

        if (pressure.HeapPressure)
            return candidates.Count;

        double share = terminalTotal <= 0 ? 1.0 : Math.Clamp((double)terminalLed / terminalTotal, 0.0, 1.0);

        long byCount = 0;
        if (pressure.RecordOverage > 0)
        {
            long remaining = pressure.RecordOverage - expiredSelected;
            if (remaining > 0)
                byCount = (long)Math.Ceiling(remaining * share);
        }

        long byBytes = 0;
        if (pressure.ByteOverage > 0)
        {
            // Bytes are attributed per candidate (record plus the receipts it releases), so the count needed to
            // cover the share of the byte overage depends on which candidates are taken; since the oldest are
            // taken and sizes are roughly uniform, an average over the candidates is a fair conversion.
            long candidateBytes = 0;
            foreach ((TransactionRecord record, _) in candidates)
                candidateBytes += EstimateReclaimableBytes(record);

            double averageBytes = candidateBytes / (double)candidates.Count;
            if (averageBytes > 0)
                byBytes = (long)Math.Ceiling(pressure.ByteOverage * share / averageBytes) - expiredSelected;
        }

        return (int)Math.Clamp(Math.Max(byCount, byBytes), 0, candidates.Count);
    }

    // The heap a reclaimed record frees on this node: the record itself plus the receipts its persistent
    // participants hold (one per participant, mirrored on every replica of the participant partition).
    private static long EstimateReclaimableBytes(TransactionRecord record)
    {
        long bytes = record.EstimateBytes();
        foreach (TransactionParticipantRef participant in record.Participants)
        {
            if (participant.Durability == KeyValueDurability.Persistent)
                bytes += 64 + 26 + 2L * participant.Key.Length + 26 + 2L * record.RecordAnchorKey.Length;
        }

        return bytes;
    }

    // Operator signal for the budget: one warning when a streak starts (memory, not time, is now the retention
    // bound — or, with nothing past the floor to reclaim, the floor rather than the budget is sizing the heap and
    // the rate × floor product is too large for it), one reminder per BudgetLogInterval carrying the current
    // numbers, and one line when the sweep finds the node back under budget. Every over-budget sweep also counts
    // on gc_budget_sweeps by outcome, so the cadence of the condition stays visible without the log.
    private void NoteBudgetState(RetentionPressure pressure, int reclaimed, int eligible)
    {
        long nowTicks = Stopwatch.GetTimestamp();

        Volatile.Write(ref retentionOverBudget, pressure.OverBudget);

        if (pressure.OverBudget)
            DurableTransactionMetrics.BudgetSweep(pressure.HeapPressure ? RetentionBudgetSweepOutcome.HeapPressure
                : eligible == 0 ? RetentionBudgetSweepOutcome.FloorBound
                : RetentionBudgetSweepOutcome.Reclaimed);

        RetentionBudgetLogAction action = budgetLogGate.Observe(pressure.OverBudget, nowTicks);

        switch (action)
        {
            case RetentionBudgetLogAction.None:
                return;

            case RetentionBudgetLogAction.StreakEnded:
                if (logger.IsEnabled(LogLevel.Information))
                    logger.LogInformation(
                        "Durable-2PC retention is back under its memory budget: {Records} records / {Bytes} estimated bytes resident; the full TTL window applies again",
                        transactionRecordStore.Count, transactionRecordStore.EstimatedBytes + completionReceiptStore.EstimatedBytes);
                return;
        }

        long residentBytes = transactionRecordStore.EstimatedBytes + completionReceiptStore.EstimatedBytes;
        TimeSpan streak = budgetLogGate.StreakDuration(nowTicks);

        if (pressure.HeapPressure)
            logger.LogWarning(
                "Durable-2PC retention under managed-heap pressure (heap load {HeapLoad:P0} ≥ {Threshold:P0}): reclaiming all {Reclaimed} terminal records past the {Floor} floor ahead of their TTL; {Records} records / {Bytes} estimated bytes resident; over budget for {Streak}. The retention budgets are undersized for this node's heap. Next reminder in {Interval}",
                pressure.HeapLoad, retentionHeapPressure, reclaimed, retentionFloor, transactionRecordStore.Count, residentBytes, streak, BudgetLogInterval);
        else if (eligible == 0)
            logger.LogWarning(
                "Durable-2PC retention over its memory budget ({Records} records / {Bytes} estimated bytes resident; budget {MaxRecords} records / {MaxBytes} bytes; over budget for {Streak}) but every terminal record this node leads is younger than the {Floor} floor: nothing can be reclaimed early. The commit rate times the floor exceeds the budget — lower the floor (and the decision-deadline ceiling it must cover) or raise the budget. Next reminder in {Interval}; watch kahuna.durable_tx.retention_over_budget and resident_records meanwhile",
                transactionRecordStore.Count, residentBytes, retentionMaxRecords, retentionMaxBytes, streak, retentionFloor, BudgetLogInterval);
        else
            logger.LogWarning(
                "Durable-2PC retention over its memory budget ({Records} records / {Bytes} estimated bytes resident; budget {MaxRecords} records / {MaxBytes} bytes; over budget for {Streak}): reclaiming {Reclaimed} of {Eligible} terminal records ahead of their TTL, none younger than the {Floor} floor. The idempotency window is the floor, not the TTL, while this continues. Next reminder in {Interval}; watch kahuna.durable_tx.retention_over_budget and gc_records_reclaimed_early meanwhile",
                transactionRecordStore.Count, residentBytes, retentionMaxRecords, retentionMaxBytes, streak, reclaimed, eligible, retentionFloor, BudgetLogInterval);
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
                    Writes.WriteAdmissionClass.Terminal, Writes.WriteSubmissionStage.Other, cancellationToken).ConfigureAwait(false);
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
    internal void CollectExpiredCompletionReceipts() => CollectExpiredCompletionReceipts(heapPressure: false);

    /// <summary>
    /// The receipt age backstop, run at the floor instead of its TTL when <paramref name="heapPressure"/> is set:
    /// under pressure the node's survival outranks the idempotency answers the receipts still hold — the same
    /// trade the record sweep makes for the records, and it needs no replication either way.
    /// </summary>
    internal void CollectExpiredCompletionReceipts(bool heapPressure)
    {
        TimeSpan retentionTtl = completionReceiptRetentionTtl;
        if (retentionTtl <= TimeSpan.Zero)
            return; // backstop disabled

        if (heapPressure && HeapPressureValveEnabled && retentionFloor > TimeSpan.Zero && retentionFloor < retentionTtl)
            retentionTtl = retentionFloor;

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

            if (logger.IsEnabled(LogLevel.Information))
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
        // presuming abort — the guard against discarding a committed leg whose settlement kept failing. With
        // a memory budget enabled the horizon is the retention floor, the earliest any leader may reclaim.
        EffectiveMinimumRetention,
        logger,
        // The abort fence: a locally visible terminal Abort is definitive, so a commit-direction settle
        // must never push that transaction's value into the log whatever decision its caller read.
        locallyAborted: (transactionId, epoch) =>
            transactionRecordStore.Get(transactionId, epoch) is { Decision: TransactionDecision.Abort },
        // The recovery sweep materializes through the same record shape the finalizer produces, so it follows
        // the same by-reference setting; otherwise a sweep would keep copying values the finalizer stopped
        // copying.
        materializeByReference: runtime.Configuration.DurableMaterializeByReference,
        // The same materialization window caps and local-apply bound the finalizer's resolution honors, so a
        // recovery or helping pass coalesces like a finalize and cannot out-fan it.
        maxMaterializationBatchItems: Math.Min(
            runtime.Configuration.KeyValueWriteMaxBatchItems,
            runtime.Configuration.KeyValueWriteMaxQueuedItemsPerPartition),
        maxMaterializationBatchBytes: Math.Min(
            runtime.Configuration.KeyValueWriteMaxBatchBytes,
            runtime.Configuration.KeyValueWriteMaxQueuedBytesPerPartition),
        localApplyGate: runtime.DurableLocalApplyGate);

    private async Task<TransactionRecord?> DriveDurableAbortAsync(AbortTransactionCommand abort, string anchorKey, CancellationToken cancellationToken)
    {
        int anchorPartition = locator.LocateRange(anchorKey).PartitionId;

        // Drive the abort through the durable gateway, which forwards to the anchor partition's leader when it
        // is remote; the leader's scheduler applies it in Raft order and the record state machine never lets an
        // abort overwrite a commit that already won. The drive must NOT be gated on this node leading the anchor
        // partition: the recovery sweep runs on the intent's key-partition leader, and when the anchor partition
        // is led elsewhere no node would ever be authorized to resolve the intent — an abandoned cross-partition
        // transaction would then stay undecided forever and its intent would refuse every scan of its key space.
        //
        // projectRecordLocally: false — this abort can lose at the anchor, and projecting a losing abort into
        // this node's record store would mint a divergent local tombstone over the canonical commit.
        byte[] delta = TransactionRecordStore.SerializeDelta([abort]);
        // A recovery-driven abort is terminal work resolving an already-prepared transaction — admit as Terminal.
        await durableReplication.ReplicateDurableThroughScheduler(
            anchorPartition, ReplicationTypes.TransactionRecord, delta, Writes.WriteAdmissionClass.Terminal,
            Writes.WriteSubmissionStage.Decision, cancellationToken, projectRecordLocally: false).ConfigureAwait(false);

        // Read back the winner — never assume the abort won. The local store is authoritative only when this
        // node leads the anchor partition; otherwise ask the anchor leader.
        if (!raft.Joined || await raft.AmILeaderIfHosted(anchorPartition, cancellationToken).ConfigureAwait(false))
            return transactionRecordStore.Get(abort.TransactionId, abort.Epoch);

        return await LookupDurableRecordRouted(abort.TransactionId, abort.Epoch, anchorKey, cancellationToken).ConfigureAwait(false);
    }

    // Transactions this node has already vetoed, so the finalizer's prepare-retry (which re-proposes the same
    // delta up to its retry budget) costs one veto instead of one per retry. Bounded by a wholesale clear at the
    // cap — vetoes are rare by construction, so the map staying tiny is the norm and a clear only risks one
    // duplicate veto per entry, which the record state machine absorbs idempotently.
    private readonly System.Collections.Concurrent.ConcurrentDictionary<(HLCTimestamp TransactionId, long Epoch), byte> vetoedTransactions = new();

    private const int VetoedTransactionsMaxTracked = 4_096;

    /// <summary>
    /// Drives a replica stale-base veto: proposes an Abort for the prepare's transaction at its anchor and
    /// classifies the outcome. Invoked (detached) when THIS node's fence memory proves a replicated
    /// validated-base prepare's base moved while the acknowledging leader admitted it — the leader-local
    /// enforcement hole behind the fsync-gate lost-update forks, where the healthy majority computed the
    /// refusal and had no way to make it count.
    ///
    /// <para>Safety rests on two facts. The verdict is deterministically correct at the prepare's apply
    /// position: committed heads record only real settled commits and advance in the same log order the
    /// prepare applied in, so a node can be behind (admits, never vetoes) but never wrongly ahead. And the
    /// abort is harmless when it loses: the record state machine never lets an abort overwrite a commit, so a
    /// veto that arrives after the decision degrades to a counted, logged no-op.</para>
    ///
    /// <para>Outcomes: abort won — a lost update was prevented (counted as upheld); commit already recorded —
    /// a confirmed stale-base commit exists (counted as late, logged as an error: this is a fork the veto
    /// missed, or one found retroactively during catch-up); still undecided — the drive itself could not
    /// resolve the record (counted as sent only; the recovery sweep owns the transaction from here).</para>
    /// </summary>
    internal async Task VetoStaleBasePrepareAsync(PreparedIntent intent, long committedHeadRevision, CancellationToken cancellationToken = default)
    {
        if (!vetoedTransactions.TryAdd((intent.TransactionId, intent.Epoch), 0))
            return;

        if (vetoedTransactions.Count > VetoedTransactionsMaxTracked)
            vetoedTransactions.Clear();

        DurableTransactionMetrics.StaleBaseVetoSent();
        logger.LogWarning(
            "Stale-base veto for transaction {TransactionId} (epoch {Epoch}): this node's fence memory proves key {Key} moved past the validated base (base revision {BaseRevision}, committed head {HeadRevision}); driving an abort at anchor {AnchorKey}",
            intent.TransactionId, intent.Epoch, intent.Key, intent.BaseRevision, committedHeadRevision, intent.RecordAnchorKey);

        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        AbortTransactionCommand abort = new(
            intent.TransactionId, intent.Epoch, intent.ManifestHash, TransactionAbortClass.Conflict,
            OpId: now, AttemptHlc: now,
            intent.RecordAnchorKey,
            CommitTimestamp: intent.CommitTimestamp,
            DecisionDeadline: intent.RecoveryDeadline,
            CreatedAt: now);

        TransactionRecord? after = await DriveDurableAbortAsync(abort, intent.RecordAnchorKey, cancellationToken).ConfigureAwait(false);

        switch (after?.Decision)
        {
            case TransactionDecision.Abort:
                DurableTransactionMetrics.StaleBaseVetoUpheld();
                logger.LogWarning(
                    "Stale-base veto upheld for transaction {TransactionId}: the abort won at the anchor; a lost update on key {Key} was prevented (base revision {BaseRevision}, committed head {HeadRevision})",
                    intent.TransactionId, intent.Key, intent.BaseRevision, committedHeadRevision);
                break;

            case TransactionDecision.Commit:
                DurableTransactionMetrics.StaleBaseVetoLate();
                logger.LogError(
                    "Stale-base veto arrived late for transaction {TransactionId}: the commit is already recorded, so key {Key} carries a committed write validated against revision {BaseRevision} while this node's committed head was {HeadRevision} — an acknowledged stale-base commit",
                    intent.TransactionId, intent.Key, intent.BaseRevision, committedHeadRevision);
                break;

            default:
                logger.LogWarning(
                    "Stale-base veto for transaction {TransactionId} did not resolve the record (key {Key}); the recovery sweep owns the transaction from here",
                    intent.TransactionId, intent.Key);
                break;
        }
    }

    // ── Replica fence confirmation ───────────────────────────────────────────────
    //
    // The staged-base fence's committed-head memory is per-node, and the prepare acknowledgement folds only
    // the LEADER's verdict — a leader whose memory is frozen or freshly restored admits exactly the prepares
    // the healthy replicas refuse. The detached stale-base veto made those replica verdicts count, but it
    // RACED the commit at the anchor and a veto that lost the race only logged (the acknowledged stale-base
    // commit stood — the fsync-gate lost updates). This pass collects the same verdicts synchronously, before
    // the finalizer proposes the commit, so a refusal is ordered ahead of the decision instead of raced
    // against it. A replica that cannot answer contributes nothing: a down node cannot veto either, so the
    // commit never blocks on absence — the veto remains the backstop for that residual, and its late counter
    // stays the loss witness.

    // How long a replica may hold the verdict request while waiting for the prepare to apply locally. The
    // prepare already committed on the leader before the confirmation starts, so a healthy follower applies
    // it within roughly one commit-broadcast hop; this bound only pays off when the follower lags. The wait
    // is event-driven — the intent store wakes it on apply — so the bound is a lag ceiling, not a poll tick.
    private const int ReplicaFenceApplyWaitMs = 400;

    // Caller-side cap per verdict call, above the server-side wait so a served answer is never abandoned
    // mid-wait, but far below the transport deadline so an unreachable node cannot stall the commit path.
    private const int ReplicaFenceCallBudgetMs = 1500;

    /// <summary>
    /// Answers THIS node's staged-base fence verdict for one transaction's validated-base prepares.
    /// Deliberately not leader-gated: the verdict is about this node's own memory, and a follower's refusal
    /// is exactly the evidence the confirmation collects. Waits (bounded, woken by the intent store's apply
    /// pulse) for keys whose prepare has not applied here yet, then answers
    /// <see cref="KeyValueStagedBaseVerdict.NotApplied"/> for the remainder.
    /// </summary>
    internal async Task<(bool Serviced, IReadOnlyList<KeyValueStagedBaseVerdictEntry> Verdicts)> GetStagedBaseVerdictsLocal(
        int partitionId, HLCTimestamp transactionId, long epoch, IReadOnlyList<string> keys, int waitMs, CancellationToken cancellationToken)
    {
        if (keys.Count == 0)
            return (true, []);

        // A node that does not host the partition never applies its prepares, so a wait cannot be satisfied:
        // answer with the instant evaluation instead of holding the request against the full budget. Under
        // legacy full replication every node hosts every partition, so this only short-circuits asks that
        // reached a non-replica (the cluster-wide endpoint fallback for an unplaced partition).
        if (!raft.HostsPartition(partitionId))
            return (true, preparedIntentStore.EvaluateReplicaFenceVerdicts(transactionId, epoch, keys));

        KeyValueStagedBaseVerdictEntry[] verdicts = await preparedIntentStore.EvaluateReplicaFenceVerdictsAsync(
            transactionId, epoch, keys, Math.Clamp(waitMs, 0, ReplicaFenceApplyWaitMs), cancellationToken).ConfigureAwait(false);

        return (true, verdicts);
    }

    /// <summary>
    /// The pre-decision replica fence confirmation, run by the finalizer between the prepare barrier and the
    /// commit decision. For every participant partition carrying validated-base intents it reads the
    /// staged-base fence verdict of each replica (this node inline, the rest over the wire). Any
    /// <see cref="KeyValueStagedBaseVerdict.StaleBase"/> answer returns false — the caller aborts with a
    /// truthful conflict instead of acknowledging a lost update. Missing verdicts (unreachable node, apply
    /// lag past the wait budget, transport error) never block the commit; they are counted so the residual
    /// window stays observable. Never throws.
    /// </summary>
    internal async Task<bool> ConfirmReplicaFenceForCommitAsync(DurableFinalizeInput input, CancellationToken cancellationToken)
    {
        long fenceStart = Stopwatch.GetTimestamp();

        try
        {
            if (!raft.Joined)
                return true;

            bool unattested = false;
            string localEndpoint = raft.GetLocalEndpoint();

            // Gather the validated-base work and answer from this node's own verdicts first: they cost
            // nothing, need no wait, and a local refusal must not leave remote calls in flight unobserved.
            List<(int PartitionId, List<string> Keys, List<PreparedIntent> Intents, bool LocalAttested)>? fenced = null;

            foreach (DurablePartitionPrepare partition in input.Partitions)
            {
                List<string>? fencedKeys = null;
                List<PreparedIntent>? fencedIntents = null;

                foreach (PreparedIntent intent in partition.Intents)
                {
                    if (!intent.HasValidatedBase)
                        continue;

                    (fencedKeys ??= []).Add(intent.Key);
                    (fencedIntents ??= []).Add(intent);
                }

                if (fencedKeys is null || fencedIntents is null)
                    continue;

                KeyValueStagedBaseVerdictEntry[] local =
                    preparedIntentStore.EvaluateReplicaFenceVerdicts(input.TransactionId, input.Epoch, fencedKeys);
                if (AnyStaleBaseVerdict(input, localEndpoint, fencedIntents, local))
                {
                    DurableTransactionMetrics.ReplicaFenceRefused();
                    return false;
                }

                // While a same-identity intent is live, the single-live-intent rule freezes its key's
                // committed head, so an instant answer other than NotApplied cannot change: when every key
                // answered, the wait-based local round below would only repeat this evaluation.
                bool localAttested = true;
                foreach (KeyValueStagedBaseVerdictEntry verdict in local)
                {
                    if (verdict.Verdict == KeyValueStagedBaseVerdict.NotApplied)
                    {
                        localAttested = false;
                        break;
                    }
                }

                (fenced ??= []).Add((partition.PartitionId, fencedKeys, fencedIntents, localAttested));
            }

            if (fenced is null)
                return true;

            List<Task<bool>>? calls = null;

            foreach ((int partitionId, List<string> fencedKeys, List<PreparedIntent> fencedIntents, bool localAttested) in fenced)
            {
                // When this node hosts the partition and its instant verdicts left any key unattested, its
                // own verdict joins the wait-based round too: the instant read above may have run before the
                // prepare applied here, and this node can be the only current-memory replica left.
                if (!localAttested && raft.HostsPartition(partitionId))
                    (calls ??= []).Add(AskLocalFenceVerdictAsync(partitionId, input, fencedKeys, fencedIntents, localEndpoint, cancellationToken));

                foreach (string endpoint in ResolveReplicaEndpoints(partitionId, localEndpoint))
                {
                    (calls ??= []).Add(AskReplicaFenceVerdictAsync(
                        endpoint, partitionId, input, fencedKeys, fencedIntents, cancellationToken));
                }
            }

            if (calls is null)
                return true;

            Task<bool?>[] wrapped = new Task<bool?>[calls.Count];
            for (int i = 0; i < calls.Count; i++)
                wrapped[i] = WrapReplicaCall(calls[i]);

            bool refused = false;
            foreach (bool? answer in await Task.WhenAll(wrapped).ConfigureAwait(false))
            {
                if (answer is null)
                    unattested = true;
                else if (answer.Value)
                    refused = true;
            }

            if (refused)
                DurableTransactionMetrics.ReplicaFenceRefused();
            else if (unattested)
                DurableTransactionMetrics.ReplicaFenceProceededUnattested();

            return !refused;
        }
        catch (Exception ex)
        {
            // The confirmation is an extra guard ahead of the decision; a broken confirmation path must
            // degrade to the pre-confirmation behavior (veto backstop), never block or fail commits.
            logger.LogWarning(ex, "Replica fence confirmation failed for transaction {TransactionId}; proceeding unattested", input.TransactionId);
            DurableTransactionMetrics.ReplicaFenceProceededUnattested();
            return true;
        }
        finally
        {
            DurableTransactionMetrics.FinalizeReplicaFenceMs.Record(Stopwatch.GetElapsedTime(fenceStart).TotalMilliseconds);
        }
    }

    /// <summary>Maps one replica call to its three-way outcome: true = refused (stale base proven), false =
    /// clear, null = no verdict (timeout or transport failure — never an objection).</summary>
    private static async Task<bool?> WrapReplicaCall(Task<bool> call)
    {
        try
        {
            return await call.WaitAsync(TimeSpan.FromMilliseconds(ReplicaFenceCallBudgetMs)).ConfigureAwait(false);
        }
        catch
        {
            // A timeout abandons the underlying call still in flight; observe its eventual fault so an
            // unreachable replica cannot surface as an unobserved-task exception later.
            _ = call.ContinueWith(static t => _ = t.Exception, TaskContinuationOptions.OnlyOnFaulted);
            return null;
        }
    }

    /// <summary>The endpoints hosting <paramref name="partitionId"/> other than this node: the partition's
    /// placed replica set when one exists, every peer under legacy full replication.</summary>
    private List<string> ResolveReplicaEndpoints(int partitionId, string localEndpoint)
    {
        IReadOnlyList<Kommander.System.RaftReplica> replicas = raft.GetPartitionReplicas(partitionId);

        List<string> endpoints = [];

        if (replicas.Count == 0)
        {
            foreach (RaftNode node in raft.GetNodes())
                endpoints.Add(node.Endpoint);
            return endpoints;
        }

        foreach (Kommander.System.RaftReplica replica in replicas)
        {
            if (!string.Equals(replica.Endpoint, localEndpoint, StringComparison.Ordinal))
                endpoints.Add(replica.Endpoint);
        }

        return endpoints;
    }

    private async Task<bool> AskLocalFenceVerdictAsync(
        int partitionId, DurableFinalizeInput input,
        List<string> fencedKeys, List<PreparedIntent> fencedIntents, string localEndpoint, CancellationToken cancellationToken)
    {
        (_, IReadOnlyList<KeyValueStagedBaseVerdictEntry> verdicts) = await GetStagedBaseVerdictsLocal(
            partitionId, input.TransactionId, input.Epoch, fencedKeys, ReplicaFenceApplyWaitMs, cancellationToken).ConfigureAwait(false);

        return AnyStaleBaseVerdict(input, localEndpoint, fencedIntents, verdicts);
    }

    private async Task<bool> AskReplicaFenceVerdictAsync(
        string endpoint, int partitionId, DurableFinalizeInput input,
        List<string> fencedKeys, List<PreparedIntent> fencedIntents, CancellationToken cancellationToken)
    {
        bool serviced;
        IReadOnlyList<KeyValueStagedBaseVerdictEntry> verdicts;
        try
        {
            (serviced, verdicts) = await interNodeCommunication.GetStagedBaseVerdicts(
                endpoint, partitionId, input.TransactionId, input.Epoch, fencedKeys, ReplicaFenceApplyWaitMs, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            DurableTransactionMetrics.ReplicaFenceRequestThrew();
            throw;
        }

        bool answered = serviced && verdicts.Count == fencedKeys.Count;
        DurableTransactionMetrics.ReplicaFenceRequested(answered);

        if (!answered)
            throw new KahunaServerException($"Node {endpoint} did not answer the staged-base verdict request.");

        return AnyStaleBaseVerdict(input, endpoint, fencedIntents, verdicts);
    }

    private bool AnyStaleBaseVerdict(
        DurableFinalizeInput input, string endpoint,
        List<PreparedIntent> fencedIntents, IReadOnlyList<KeyValueStagedBaseVerdictEntry> verdicts)
    {
        bool refused = false;

        for (int i = 0; i < verdicts.Count && i < fencedIntents.Count; i++)
        {
            if (verdicts[i].Verdict != KeyValueStagedBaseVerdict.StaleBase)
                continue;

            refused = true;

            logger.LogWarning(
                "Replica fence refused the commit of transaction {TransactionId}: node {Endpoint} proves key {Key} moved past the validated base (base revision {BaseRevision}, committed head {HeadRevision}); aborting before the decision",
                input.TransactionId, endpoint, fencedIntents[i].Key, fencedIntents[i].BaseRevision, verdicts[i].HeadRevision);
        }

        return refused;
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
