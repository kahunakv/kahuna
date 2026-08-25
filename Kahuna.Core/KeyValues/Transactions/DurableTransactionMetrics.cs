using System.Diagnostics.Metrics;

namespace Kahuna.Server.KeyValues.Transactions;

/// <summary>
/// <see cref="System.Diagnostics.Metrics"/> instruments for the durable-intent 2PC finalize path. The decision
/// deadline is safety-critical: it decides whether a slow-but-alive coordinator's commit is honored or presumed
/// aborted by recovery. A deadline set too low spuriously aborts healthy transactions under load; one set too
/// high delays recovery of genuinely dead coordinators. These instruments make a mis-tuned deadline visible
/// rather than letting it silently convert live commits into aborts.
///
/// <para>Instrument naming follows the OpenTelemetry semantic conventions (dot-separated lowercase); Prometheus
/// exporters typically translate dots to underscores automatically. Counters are cumulative and thread-safe.</para>
/// </summary>
internal static class DurableTransactionMetrics
{
    internal static readonly Meter Meter = new("Kahuna", "1.0");

    /// <summary>
    /// One count per backoff sleep in a KeyValuesManager retry loop, tagged by call site (method + line).
    /// Diagnostic instrument for localizing latency tails: a statement path that never sleeps shows zero;
    /// whichever site dominates during a stall names the loop responsible.
    /// </summary>
    internal static readonly Counter<long> KvRetryWaits =
        Meter.CreateCounter<long>(
            "kahuna.kv.retry_waits",
            description: "Backoff sleeps in key/value manager retry loops, by site.");

    internal static void AddKvRetryWait(string site) => KvRetryWaits.Add(1, new KeyValuePair<string, object?>("site", site));

    /// <summary>
    /// Commits rejected because the attempt's HLC passed the transaction's frozen decision deadline, so the
    /// canonical record stayed <c>Undecided</c> and the transaction yields to presumed-abort recovery. A rising
    /// rate is the signal that the deadline is too tight for the current finalize latency — healthy transactions
    /// are being converted into aborts. Alert on any sustained non-zero rate.
    /// </summary>
    internal static readonly Counter<long> LateCommitRejections =
        Meter.CreateCounter<long>(
            "kahuna.durable_tx.late_commit_rejections",
            description: "Durable commits rejected because the attempt passed the frozen decision deadline.");

    /// <summary>
    /// Transactions aborted at the commit barrier because a foreign range lock covered a key they had written —
    /// a range lock acquired after the write was staged, which the write-time fence cannot see. Each occurrence
    /// is a prevented phantom write. A sustained rate means real contention between writers and range-locking
    /// readers (or a long-running range split, whose quiesce window is a range lock), not a defect.
    /// </summary>
    internal static readonly Counter<long> RangeLockFenceAborts =
        Meter.CreateCounter<long>(
            "kahuna.transactions.range_lock_fence_aborts",
            description: "Transactions aborted because a foreign range lock covered one of their written keys.");

    /// <summary>
    /// Transactions aborted because a commit-conflict probe answered "staged base compare failed". The local
    /// coordinator no longer asks that check (the prepare-apply fence below owns it); a nonzero count means a
    /// mixed-version remote peer still probes the retired way, or the defensive branch caught an unexpected
    /// answer. Kept for wire compatibility with peers that still send staged-base probes.
    /// </summary>
    internal static readonly Counter<long> StagedBasePostPrepareAborts =
        Meter.CreateCounter<long>(
            "kahuna.transactions.staged_base_post_prepare_aborts",
            description: "Transactions aborted because a written key's committed base moved between validation and prepare.");

    /// <summary>
    /// Validated-base prepares whose acknowledgement the intent store's staged-base fence refused: at the
    /// prepare's own apply position, the key's last transactionally committed head no longer matched the base
    /// the write was validated against — a competitor committed the same base between the pre-propose
    /// staged-base validation and this prepare landing. On the 2PC path each refusal becomes a truthful
    /// conflict abort and is a prevented lost update (before this fence existed, exactly this interleaving
    /// silently dropped committed writes under a paused coordinator — the bank-soak conservation loss). In a
    /// one-phase bundle the refusal cannot withhold the bundled decision; the bundle's guard is its own
    /// pre-propose re-validation, so a bundle-path occurrence only marks the accepted residual window.
    /// </summary>
    internal static readonly Counter<long> StagedBasePrepareRejections =
        Meter.CreateCounter<long>(
            "kahuna.transactions.staged_base_prepare_rejections",
            description: "Prepare acknowledgements refused because the written key's committed base moved before the prepare applied.");

    /// <summary>
    /// Fence-wedge watchdog escalations: a key refused a run of consecutive validated-base prepares at an
    /// unchanged (validated base, committed head) pair, meaning this node's visible entry stopped converging
    /// with its committed head — the key is effectively read-only until the entry reconciles. Healthy refusals
    /// are transient (the client re-reads the moved base and passes), so any occurrence is a convergence
    /// failure worth an operator's attention even though the refusals themselves lose no data.
    /// </summary>
    internal static readonly Counter<long> StagedBaseFenceWedgedKeys =
        Meter.CreateCounter<long>(
            "kahuna.transactions.staged_base_fence_wedged_keys",
            description: "Watchdog escalations for keys stuck refusing validated-base prepares at a frozen validated/head pair.");

    /// <summary>
    /// Commit settlements that applied on this node without a local completion receipt for the settled key —
    /// proof the committed mutation never materialized into this node's visible state (a dropped or misrouted
    /// commit-apply) — and were repaired by re-driving the force-resident apply from the intent still in hand.
    /// Zero on a healthy node: the materialization always precedes its settlement in log order.
    /// </summary>
    internal static readonly Counter<long> MaterializationRepairs =
        Meter.CreateCounter<long>(
            "kahuna.transactions.materialization_repairs",
            description: "Committed mutations re-applied locally because their settlement applied without a local completion receipt.");

    /// <summary>
    /// Recovery passes that HELD a due prepared intent instead of resolving it, because its canonical record is
    /// absent and the intent is older than the record retention horizon — absence can then mean a committed
    /// record the retention GC reclaimed while this leg's settlement kept failing, and presuming abort would
    /// discard the only durable copy of a committed value. A sustained nonzero rate means an intent is wedged:
    /// it cannot resolve without its record, and it blocks writers to its key (single live intent per key).
    /// Surface it to an operator; the safe manual resolutions are re-materializing the value or an explicit,
    /// audited discard.
    /// </summary>
    internal static readonly Counter<long> RecordlessIntentHolds =
        Meter.CreateCounter<long>(
            "kahuna.transactions.recordless_intent_holds",
            description: "Due prepared intents held by recovery because their record is absent past the retention horizon.");

    /// <summary>
    /// Transactions committed through the one-phase fast path: a single durable batch carrying
    /// [record init + anchor prepare + commit decision], taken when the participant set collapses to the
    /// locally-led anchor partition, no foreign durable intent holds any written key, and read-set
    /// validation passes up front. Compare with <see cref="OnePhaseFallbacks"/> for the hit rate.
    /// </summary>
    internal static readonly Counter<long> OnePhaseCommits =
        Meter.CreateCounter<long>(
            "kahuna.durable_tx.one_phase_commits",
            description: "Durable transactions committed via the single-batch one-phase fast path.");

    /// <summary>
    /// A committed transaction's resolution failed to materialize or leader-apply an intent's value, leaving
    /// it committed-but-unsettled until the recovery sweep retries. Each failure extends the window in which
    /// the committed value exists only as a prepared intent — visible solely through the intent overlay, and
    /// the state a range move must settle before it may cut over. A sustained rate means settlement is being
    /// refused somewhere (scheduler backpressure, a quiesced range, a forwarding failure) and the deferred
    /// path is silently leaning on recovery.
    /// </summary>
    internal static readonly Counter<long> ResolutionSettleFailures =
        Meter.CreateCounter<long>(
            "kahuna.durable_tx.resolution_settle_failures",
            description: "Intents a commit resolution could not materialize/apply; recovery completes them.");

    /// <summary>
    /// One-phase-eligible transactions that fell back to the standard 2PC flow (remote anchor leader, a
    /// foreign durable intent on a written key, failed up-front validation, or a scheduler rejection).
    /// A high rate relative to <see cref="OnePhaseCommits"/> means the fast path's gate rarely opens and
    /// the workload still pays both barriers.
    /// </summary>
    internal static readonly Counter<long> OnePhaseFallbacks =
        Meter.CreateCounter<long>(
            "kahuna.durable_tx.one_phase_fallbacks",
            description: "One-phase-eligible finalizes that fell back to the standard 2PC flow.");

    /// <summary>
    /// A one-phase bundle whose prepare was rejected even though the pre-flight foreign-intent check passed —
    /// another transaction took a key between the check and the batch's ordered apply. Reachable when the
    /// in-memory write intents that normally exclude conflicting writers are lost while the proposal is in
    /// flight (a stalled proposal surfacing after a partition heals, a killed node's wiped locks, an expired
    /// intent lease). The bundled commit decision is rejected with the prepare by the record store's
    /// bundled-prepare gate, so the transaction stays Undecided and retries truthfully.
    /// </summary>
    internal static readonly Counter<long> OnePhasePrepareRejections =
        Meter.CreateCounter<long>(
            "kahuna.durable_tx.one_phase_prepare_rejections",
            description: "One-phase bundles whose prepare was rejected after the decision was already proposed.");

    /// <summary>
    /// One-phase bundled commit decisions rejected by the record store's bundled-prepare gate: the commit
    /// transition applied without a live same-transaction prepared intent at every bundled key, so the record
    /// was kept Undecided instead of durably committing a mutation that was never durably prepared. Expected to
    /// track <see cref="OnePhasePrepareRejections"/>; each occurrence is a prevented lost update.
    /// </summary>
    internal static readonly Counter<long> OnePhaseGatedCommitRejections =
        Meter.CreateCounter<long>(
            "kahuna.durable_tx.one_phase_gated_commit_rejections",
            description: "One-phase bundled commits rejected because their bundled prepare did not take ownership of every key.");

    /// <summary>
    /// Wall time of the finalize's prepare stage: record init + every participant prepare (anchor-bundled when
    /// available), including the bounded conflict-retry loop. Ends when the prepare barrier resolves, before
    /// read-set validation. Compare against <see cref="FinalizeValidateMs"/>/<see cref="FinalizeDecisionMs"/> to
    /// localize where a slow finalize spends its time under load.
    /// </summary>
    internal static readonly Histogram<double> FinalizePrepareMs =
        Meter.CreateHistogram<double>(
            "kahuna.durable_tx.finalize_prepare_ms", unit: "ms",
            description: "Finalize prepare-stage wall time (record init + all prepares + conflict retries).");

    /// <summary>
    /// Wall time of the finalize's optimistic read-set validation: re-probing every tracked read to confirm no
    /// committed writer invalidated it. Runs only when every prepare was durable. Scales with read-set size (see
    /// <see cref="FinalizeReadSetKeys"/>) and with key-actor load, since each probe is an actor ask.
    /// </summary>
    internal static readonly Histogram<double> FinalizeValidateMs =
        Meter.CreateHistogram<double>(
            "kahuna.durable_tx.finalize_validate_ms", unit: "ms",
            description: "Finalize read-set validation wall time.");

    /// <summary>
    /// Wall time of the finalize's decision stage: replicating the terminal commit/abort transition at the anchor
    /// and reading back the winner. One durable round trip plus the record read.
    /// </summary>
    internal static readonly Histogram<double> FinalizeDecisionMs =
        Meter.CreateHistogram<double>(
            "kahuna.durable_tx.finalize_decision_ms", unit: "ms",
            description: "Finalize decision-stage wall time (terminal transition + winner read-back).");

    /// <summary>
    /// Number of tracked read-set keys a finalize validated. Interpreted together with
    /// <see cref="FinalizeValidateMs"/>: a large set explains a slow validation; a small set with slow validation
    /// points at key-actor queueing instead.
    /// </summary>
    internal static readonly Histogram<long> FinalizeReadSetKeys =
        Meter.CreateHistogram<long>(
            "kahuna.durable_tx.finalize_read_set_keys", unit: "{key}",
            description: "Read-set keys validated per finalize.");

    /// <summary>
    /// Blocking prepared intents settled inline by a finalize's prepare-conflict "helping" pass: the blocker's
    /// canonical record was already terminal (committed or aborted) but its deferred settlement had not run yet,
    /// so the blocked finalize resolved it directly instead of backing off and re-preparing. A high rate means
    /// deferred settlement is lagging the commit rate — the convoy this pass exists to break — and is worth
    /// correlating with prepare-retry counts and commit latency.
    /// </summary>
    internal static readonly Counter<long> PrepareConflictBlockersSettled =
        Meter.CreateCounter<long>(
            "kahuna.durable_tx.prepare_conflict_blockers_settled",
            description: "Decided-but-unsettled blocking intents settled inline by a blocked finalize's helping pass.");

    /// <summary>
    /// Recovery aborts attributed to decision-deadline expiry: a canonical record still <c>Undecided</c> past its
    /// deadline that recovery drove to a presumed abort. Distinguishes deadline-expiry aborts from orphan-prepare
    /// aborts (a remote prepare that outlived a failed anchor initialization). A rising rate corroborates
    /// <see cref="LateCommitRejections"/>: the deadline is expiring before healthy coordinators can decide.
    /// </summary>
    internal static readonly Counter<long> DeadlineExpiryAborts =
        Meter.CreateCounter<long>(
            "kahuna.durable_tx.deadline_expiry_aborts",
            description: "Recovery presumed-aborts of records left Undecided past their decision deadline.");

    /// <summary>
    /// The decision-deadline margin (ms past the commit timestamp) chosen for each finalize. Recorded at freeze
    /// so dashboards can correlate the derived margin with the observed finalize latency and the late-commit /
    /// deadline-expiry rates when tuning the floor, ceiling, and multiplier.
    /// </summary>
    internal static readonly Histogram<long> DecisionDeadlineMarginMs =
        Meter.CreateHistogram<long>(
            "kahuna.durable_tx.decision_deadline_margin_ms",
            unit: "ms",
            description: "Decision-deadline margin (ms past commit timestamp) frozen for each durable finalize.");

    /// <summary>
    /// Terminal transaction records reclaimed by the retention GC sweep (removed after their retention window
    /// elapsed and their participants' receipts were released). Its rate against admitted durable transactions
    /// shows whether reclamation keeps pace with inflow; a persistently lagging value is the early signal of the
    /// metadata growth this GC exists to bound, visible long before a heap dump.
    /// </summary>
    internal static readonly Counter<long> GcRecordsReclaimed =
        Meter.CreateCounter<long>(
            "kahuna.durable_tx.gc_records_reclaimed",
            description: "Terminal transaction records removed by the retention GC sweep.");

    /// <summary>
    /// Participant completion receipts released by the retention GC sweep. Receipts are otherwise never evicted,
    /// so this is the counter that proves the completion-receipt store returns to a steady-state floor rather
    /// than growing for the node's lifetime.
    /// </summary>
    internal static readonly Counter<long> GcReceiptsReleased =
        Meter.CreateCounter<long>(
            "kahuna.durable_tx.gc_receipts_released",
            description: "Participant completion receipts released by the retention GC sweep.");

    /// <summary>
    /// Durable transactions refused admission because the node was at <c>DurableDecisionOutstandingMax</c>
    /// outstanding durable finalizes. Each is a retryable <c>MustRetry</c> that prepared nothing. A sustained
    /// non-zero rate means inflow exceeds the admission bound — the backpressure that keeps prepared state, and
    /// the write scheduler's terminal-class reserve, within their budgets.
    /// </summary>
    internal static readonly Counter<long> AdmissionRejections =
        Meter.CreateCounter<long>(
            "kahuna.durable_tx.admission_rejections",
            description: "Durable transactions refused admission at the outstanding-decision cap.");

    /// <summary>
    /// Completion receipts dropped by the age backstop rather than by a coordinator acknowledgement. These are
    /// receipts no surviving transaction record owns — re-recorded by a log replay after their record was already
    /// reclaimed — so a sustained non-zero rate is expected after restarts and leader changes, and is the signal
    /// that the backstop, not the ordinary release path, is what keeps the store bounded.
    /// </summary>
    internal static readonly Counter<long> GcReceiptsExpired =
        Meter.CreateCounter<long>(
            "kahuna.durable_tx.gc_receipts_expired",
            description: "Completion receipts dropped by the age backstop with no owning transaction record.");

    internal static void RecordsReclaimed(int count) => GcRecordsReclaimed.Add(count);

    internal static void ReceiptsReleased(int count) => GcReceiptsReleased.Add(count);

    internal static void ReceiptsExpired(int count) => GcReceiptsExpired.Add(count);

    /// <summary>
    /// Durable record/intent applies skipped by the write scheduler's completion because the consumer apply of that
    /// exact log entry already ran and left its result. Each skip avoids a full re-deserialization of a delta whose
    /// effect is already in the store. A value that stays near zero on a busy leader means the two apply paths are no
    /// longer agreeing on log identity — the redundant parse is back.
    /// </summary>
    internal static readonly Counter<long> RedundantAppliesSkipped =
        Meter.CreateCounter<long>(
            "kahuna.durable_tx.redundant_applies_skipped",
            description: "Durable applies skipped because that log entry's apply already ran.");

    private static long redundantAppliesSkipped;

    /// <summary>Process-wide count behind <see cref="RedundantAppliesSkipped"/>, readable so the skip can be asserted
    /// as actually happening rather than assumed.</summary>
    internal static long RedundantAppliesSkippedCount => Interlocked.Read(ref redundantAppliesSkipped);

    internal static void RedundantApplySkipped()
    {
        Interlocked.Increment(ref redundantAppliesSkipped);
        RedundantAppliesSkipped.Add(1);
    }

    /// <summary>
    /// Registers resident-state observable gauges — canonical record count, completion-receipt count, resident
    /// prepared-intent count and bytes, and outstanding durable transactions — on a fresh, <b>instance-owned</b>
    /// <see cref="Meter"/> returned to the caller. The callbacks capture the stores/coordinator, so the caller
    /// must dispose the returned meter on teardown or a disposed node's state stays reachable (mirrors the write
    /// aggregator's instance-meter ownership). These gauges make the retained metadata this GC bounds visible
    /// continuously, long before a heap dump; the counters above stay on the shared static meter.
    /// </summary>
    internal static Meter RegisterGauges(
        Func<long> recordCount,
        Func<long> receiptCount,
        Func<long> preparedIntentCount,
        Func<long> preparedIntentBytes,
        Func<long> outstandingDurable)
    {
        Meter gaugeMeter = new("Kahuna", "1.0");
        gaugeMeter.CreateObservableGauge("kahuna.durable_tx.resident_records", recordCount,
            description: "Canonical transaction records resident on this node (awaiting retention GC).");
        gaugeMeter.CreateObservableGauge("kahuna.durable_tx.resident_receipts", receiptCount,
            description: "Completion receipts resident on this node (released by GC after retention).");
        gaugeMeter.CreateObservableGauge("kahuna.durable_tx.resident_prepared_intents", preparedIntentCount,
            description: "Prepared intents resident on this node (bounded by durable admission).");
        gaugeMeter.CreateObservableGauge("kahuna.durable_tx.resident_prepared_intent_bytes", preparedIntentBytes,
            unit: "By", description: "Resident prepared-intent value bytes on this node (bounded by durable admission).");
        gaugeMeter.CreateObservableGauge("kahuna.durable_tx.outstanding", outstandingDurable,
            description: "Durable transactions currently being driven through finalize (admission-gated).");
        return gaugeMeter;
    }
}
