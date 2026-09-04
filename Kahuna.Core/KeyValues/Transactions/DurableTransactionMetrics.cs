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
    /// Cache-miss hydrations refused because the loaded persistent row (or its absence) sits strictly below the
    /// staged-base fence's committed-head memory for the key. That state is provably stale: every recorded head
    /// is a real durable-transaction commit, so a lower local row means this node's visible state lost committed
    /// history — the exact precondition of a lost update on a freshly promoted leader whose resident cache is
    /// cold. Each refusal answers MustRetry and schedules the convergence repair instead of installing the stale
    /// row as the key's base. Any sustained rate means local durable state is not converging with settles.
    /// </summary>
    internal static readonly Counter<long> StaleHydrationsRefused =
        Meter.CreateCounter<long>(
            "kahuna.durable_tx.stale_hydrations_refused",
            description: "Cache-miss reads refused because the hydrated row was below the key's remembered committed head.");

    private static long staleHydrationsRefused;

    /// <summary>Process-wide count behind <see cref="StaleHydrationsRefused"/>, readable so tests can assert the
    /// refusal actually fired rather than assume it.</summary>
    internal static long StaleHydrationsRefusedCount => Interlocked.Read(ref staleHydrationsRefused);

    internal static void StaleHydrationRefused()
    {
        Interlocked.Increment(ref staleHydrationsRefused);
        StaleHydrationsRefused.Add(1);
    }

    /// <summary>
    /// Committed key-value materialization records that applied at a revision strictly below the key's
    /// remembered committed head. A late re-driven materialization that the head guards no-op is the benign
    /// producer; anything else is a fork witness — a committed record entering the log below history this node
    /// already saw settle, which is how a stale-base commit permanently overwrites acknowledged writes. Each
    /// occurrence is logged with both revisions and the transaction id so a conserved-total drift in a soak run
    /// attributes to its producer from the log alone.
    /// </summary>
    internal static readonly Counter<long> BelowHeadMaterializations =
        Meter.CreateCounter<long>(
            "kahuna.durable_tx.below_head_materializations",
            description: "Committed key-value records applied at a revision below the key's remembered committed head.");

    /// <summary>
    /// Validated-base prepares the staged-base fence admitted because it held NO committed-head memory for the
    /// key. Admission on absence is correct when the key genuinely never had a durable-transaction commit within
    /// retention — but it is also the only silent path around the fence: a node whose settle applies lagged (or
    /// whose memory was lost) admits a stale base here without any refusal. The counter separates "fence proved
    /// the base current" from "fence had nothing to check", which a loss investigation needs to tell apart.
    /// </summary>
    internal static readonly Counter<long> FenceAdmissionsAbsentHead =
        Meter.CreateCounter<long>(
            "kahuna.durable_tx.fence_admissions_absent_head",
            description: "Validated-base prepares admitted because the fence held no committed head for the key.");

    /// <summary>
    /// Stale-base vetoes dispatched by replicas. When a node applies a replicated validated-base prepare and its
    /// own fence memory proves the base moved, that verdict is deterministically correct (heads record only real
    /// commits, in log order) even when the acknowledging leader's memory is frozen and admitted the prepare —
    /// the exact hole behind the fsync-gate lost-update forks. Each veto drives a best-effort Abort at the
    /// transaction's anchor; the record state machine makes it safe (an abort never overwrites a commit).
    /// </summary>
    internal static readonly Counter<long> StaleBaseVetoesSent =
        Meter.CreateCounter<long>(
            "kahuna.durable_tx.stale_base_vetoes_sent",
            description: "Replica-side aborts driven for prepares whose validated base this node's fence proved stale.");

    private static long staleBaseVetoesSent;

    /// <summary>Process-wide count behind <see cref="StaleBaseVetoesSent"/>, readable for tests.</summary>
    internal static long StaleBaseVetoesSentCount => Interlocked.Read(ref staleBaseVetoesSent);

    internal static void StaleBaseVetoSent()
    {
        Interlocked.Increment(ref staleBaseVetoesSent);
        StaleBaseVetoesSent.Add(1);
    }

    /// <summary>Vetoes whose abort won at the anchor: a lost update was prevented by a replica's verdict after
    /// the leader had already admitted the stale base. Any occurrence means the leader-local fence was blind.</summary>
    internal static readonly Counter<long> StaleBaseVetoesUpheld =
        Meter.CreateCounter<long>(
            "kahuna.durable_tx.stale_base_vetoes_upheld",
            description: "Replica stale-base vetoes whose abort won at the transaction's anchor.");

    private static long staleBaseVetoesUpheld;

    /// <summary>Process-wide count behind <see cref="StaleBaseVetoesUpheld"/>, readable for tests.</summary>
    internal static long StaleBaseVetoesUpheldCount => Interlocked.Read(ref staleBaseVetoesUpheld);

    internal static void StaleBaseVetoUpheld()
    {
        Interlocked.Increment(ref staleBaseVetoesUpheld);
        StaleBaseVetoesUpheld.Add(1);
    }

    /// <summary>Vetoes that found the commit already recorded. Each is a confirmed acknowledged stale-base
    /// commit that got past every fence — the residual race window, or a fork discovered retroactively during a
    /// catch-up replay. Always investigated; the paired log line names the key and both revisions.</summary>
    internal static readonly Counter<long> StaleBaseVetoesLate =
        Meter.CreateCounter<long>(
            "kahuna.durable_tx.stale_base_vetoes_late",
            description: "Replica stale-base vetoes that found the transaction already committed.");

    private static long staleBaseVetoesLate;

    /// <summary>Process-wide count behind <see cref="StaleBaseVetoesLate"/>, readable for tests.</summary>
    internal static long StaleBaseVetoesLateCount => Interlocked.Read(ref staleBaseVetoesLate);

    internal static void StaleBaseVetoLate()
    {
        Interlocked.Increment(ref staleBaseVetoesLate);
        StaleBaseVetoesLate.Add(1);
    }

    /// <summary>
    /// Commits refused by the pre-decision replica fence confirmation: a replica's staged-base fence proved a
    /// validated base moved while the acknowledging leader admitted the prepare, and the finalizer read that
    /// verdict BEFORE proposing the commit — the ordered form of the stale-base veto, which raced the commit
    /// and lost in the fsync-gate runs. Each refusal is a prevented lost update; the transaction aborts with a
    /// truthful conflict and the client retries on the moved base.
    /// </summary>
    internal static readonly Counter<long> ReplicaFenceRefusals =
        Meter.CreateCounter<long>(
            "kahuna.durable_tx.replica_fence_refusals",
            description: "Commits aborted before the decision because a replica's staged-base fence proved a validated base moved.");

    private static long replicaFenceRefusals;

    /// <summary>Process-wide count behind <see cref="ReplicaFenceRefusals"/>, readable for tests.</summary>
    internal static long ReplicaFenceRefusalsCount => Interlocked.Read(ref replicaFenceRefusals);

    internal static void ReplicaFenceRefused()
    {
        Interlocked.Increment(ref replicaFenceRefusals);
        ReplicaFenceRefusals.Add(1);
    }

    /// <summary>
    /// Replica fence confirmations that proceeded to the commit with at least one replica verdict missing —
    /// the node was unreachable, answered too slowly, or had not applied the prepare within the wait budget.
    /// The commit is not blocked on an absent verdict (a down replica cannot veto either), so each tick marks
    /// a window where only the reachable verdicts protected the base; the detached veto remains the backstop
    /// there, and a rising rate alongside stale_base_vetoes_late localises that residual.
    /// </summary>
    internal static readonly Counter<long> ReplicaFenceUnattested =
        Meter.CreateCounter<long>(
            "kahuna.durable_tx.replica_fence_unattested",
            description: "Replica fence confirmations that proceeded with at least one replica verdict unavailable.");

    private static long replicaFenceUnattested;

    /// <summary>Process-wide count behind <see cref="ReplicaFenceUnattested"/>, readable for tests.</summary>
    internal static long ReplicaFenceUnattestedCount => Interlocked.Read(ref replicaFenceUnattested);

    internal static void ReplicaFenceProceededUnattested()
    {
        Interlocked.Increment(ref replicaFenceUnattested);
        ReplicaFenceUnattested.Add(1);
    }

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
    /// Committed mutations VERIFIED missing from this node's durable state at settlement — the settle-time
    /// overlay witness missed AND the off-actor verification against the flushed backend confirmed the row is
    /// genuinely absent (not merely flushed-and-removed from the overlay, the common benign race) — and
    /// re-driven from the settled intent. Zero on a healthy node; each tick is a locally-skipped record apply
    /// being repaired, the event behind the frozen validated/head wedge.
    /// </summary>
    internal static readonly Counter<long> MaterializationRepairs =
        Meter.CreateCounter<long>(
            "kahuna.transactions.materialization_repairs",
            description: "Committed mutations verified missing from local durable state at settlement and re-driven.");

    /// <summary>
    /// Coherence reconciles scheduled by the fence-wedge repair: a refusal streak at a frozen
    /// (validated base, committed head) pair re-drove the key's resident entry from this node's own durable
    /// row. Each one is a dropped coherence notification being repaired; a sustained rate on the same key
    /// (with the wedged-keys alarm firing) means the reconcile is not converging and needs investigation.
    /// </summary>
    internal static readonly Counter<long> CoherenceReconciles =
        Meter.CreateCounter<long>(
            "kahuna.transactions.coherence_reconciles",
            description: "Resident-entry reconciles from local durable state, triggered by fence-refusal streaks.");

    /// <summary>
    /// Committed heads recovered from local retained revision history because the durable current row
    /// was below the committed head when a coherence reconcile read it. Zero on a healthy node; each
    /// tick is a durable current-head regression (or a lost head flush) being healed by re-promoting
    /// the exact head revision through the persistence path.
    /// </summary>
    internal static readonly Counter<long> CoherenceHeadRecoveries =
        Meter.CreateCounter<long>(
            "kahuna.transactions.coherence_head_recoveries",
            description: "Committed heads re-promoted from local revision history after a below-head durable read.");

    /// <summary>
    /// Recovery passes that HELD a due prepared intent instead of resolving it, because its canonical record is
    /// absent and the intent is older than the record retention horizon — absence can then mean a committed
    /// record the retention GC reclaimed while this leg's settlement kept failing, and presuming abort would
    /// discard the only durable copy of a committed value. A sustained nonzero rate means an intent is wedged:
    /// it cannot resolve without its record, and it blocks writers to its key (single live intent per key).
    /// Surface it to an operator; the safe manual resolutions are re-materializing the value or an explicit,
    /// audited discard.
    /// </summary>
    /// <summary>
    /// A replicated key/value apply carried the same revision as the newest write already recorded for the
    /// key but a DIFFERENT value. Revisions are supposed to identify a mutation uniquely, but an aborted
    /// attempt and its client replay both stage base+1, so a stale record of the aborted attempt proposed
    /// around the abort/replay boundary collides with the replay's committed record at the same revision —
    /// and a revision-monotonic durable head cannot tell them apart. Any non-zero count is a correctness
    /// alarm: the paired error log names both transactions and the log index, which attributes the
    /// conserved-total drift this collision produces.
    /// </summary>
    /// <summary>
    /// Durable-commit applies or materialization proposals refused because a terminal Abort for the
    /// transaction is locally visible. A local Abort is definitive (an abort can never overwrite a
    /// commit, and terminal records replicate only through the canonical log), so each refusal is a
    /// materialization of an aborted leg that was about to happen — the conserved-total drift.
    /// The paired error log's call path names the producer.
    /// </summary>
    internal static readonly Counter<long> AbortFencedCommitApplies =
        Meter.CreateCounter<long>(
            "kahuna.kv.abort_fenced_commit_applies",
            description: "Durable commit applies refused because the transaction's record is a terminal Abort.");

    /// <summary>
    /// By-reference materialization records that found no matching prepared intent on this node AND could not
    /// be proven redundant — the key's newest durable write is still below the record's revision. Every other
    /// miss is the benign duplicate (a second producer's record arriving after the settle removed the intent)
    /// and is not counted. A non-zero count means one replica is missing a committed value the rest of the
    /// cluster has: the paired error log names the transaction, the epoch, the key and the log index.
    /// </summary>
    internal static readonly Counter<long> MaterializationIntentMissing =
        Meter.CreateCounter<long>(
            "kahuna.kv.materialization_intent_missing",
            description: "By-reference materialization records whose prepared intent was absent and whose value is not durable here.");

    internal static readonly Counter<long> SameRevisionDivergentApplies =
        Meter.CreateCounter<long>(
            "kahuna.kv.same_revision_divergent_applies",
            description: "Replicated key/value applies whose revision equals the newest recorded write but whose value differs.");

    internal static readonly Counter<long> RecordlessIntentHolds =
        Meter.CreateCounter<long>(
            "kahuna.transactions.recordless_intent_holds",
            description: "Due prepared intents held by recovery because their record is absent past the retention horizon.");

    /// <summary>
    /// Same-id resends of an already-completed many-key batch that were refused instead of re-executed.
    /// The first drive's detached completion folded the batch's confirmed effects after the caller stopped
    /// waiting for it, so the caller resent a batch the coordinator already owns. Re-executing it would
    /// mutate participants invisibly to the session freeze; the refusal answers transient and lets the
    /// caller's bounded retry budget resolve the stale view. A firing marks the ack-loss race, not an error.
    /// </summary>
    internal static readonly Counter<long> CompletedBatchRedriveRefusals =
        Meter.CreateCounter<long>(
            "kahuna.kv.completed_batch_redrive_refusals",
            description: "Same-id resends of a completed many-key batch refused instead of re-executed.");

    /// <summary>
    /// Scans that exhausted the per-page retry budget: one page kept answering
    /// MustRetry/WaitingForReplication for the whole budget, so the scan failed loudly instead of
    /// hanging. The paired error log names the range and the cursor. A firing means some key in the
    /// page cannot serve — typically a foreign write intent whose commit timestamp never resolves
    /// (an orphaned session-owned intent is one durable producer) — and the range stays unscannable
    /// until that state clears; the counter makes the wedge visible instead of silent.
    /// </summary>
    internal static readonly Counter<long> ScanPageRetryBudgetExhausted =
        Meter.CreateCounter<long>(
            "kahuna.kv.scan_page_retry_budget_exhausted",
            description: "Range scans failed loudly after one page answered transient for the whole retry budget.");

    /// <summary>
    /// Session-owned locks dropped because they outlived the liveness ceiling: a write intent or range lock
    /// requested with no deadline whose owning session never released it. Past the ceiling the session is
    /// provably finalized or reaped, so the lock is orphaned and the key would otherwise stay unservable to
    /// snapshot scans for the life of the process. The <c>kind</c> tag separates point and prefix intents
    /// from range locks; the paired warning names the key and the owning transaction, which is the only
    /// record of who left it behind. A firing marks a wedge that healed, not a failed operation.
    /// </summary>
    internal static readonly Counter<long> SessionOwnedIntentCeilingExpiries =
        Meter.CreateCounter<long>(
            "kahuna.kv.session_owned_intent_ceiling_expiries",
            description: "Session-owned write intents and range locks dropped after outliving the liveness ceiling.");

    /// <summary>
    /// Operation completions carrying at least one confirmed working-set effect (a modified key, a staged
    /// mutation, an acquired lock, or a read observation) that arrived for an operation record that is
    /// absent or no longer pending — so the effect was applied at a participant but can never enter the
    /// coordinator's working set. The transaction may then finalize without a mutation a participant
    /// holds. Any firing is a correctness alarm: the paired error log names the transaction and operation.
    /// </summary>
    internal static readonly Counter<long> DiscardedOperationEffects =
        Meter.CreateCounter<long>(
            "kahuna.kv.discarded_operation_effects",
            description: "Effect-bearing operation completions discarded because their registration was absent or not pending.");

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
    /// One-phase bundled commits rejected at apply because a co-bundled intent's validated base had been moved
    /// past by a settled commit before the bundle applied (or the transaction outlived the ledger's retention
    /// horizon) — the lost-update shape of a stalled bundle, caught in log order against the partition's
    /// replicated committed-head ledger. Each occurrence is a prevented lost update; the proposing finalizer
    /// drives a truthful conflict abort from it. Counted on every replica that applies the rejection.
    /// </summary>
    internal static readonly Counter<long> OnePhaseGatedCommitStaleBaseRejections =
        Meter.CreateCounter<long>(
            "kahuna.durable_tx.one_phase_gated_commit_stale_base_rejections",
            description: "One-phase bundled commits rejected at apply because a validated base moved before the bundle applied.");

    /// <summary>
    /// One-phase bundled commits rejected at apply because a carried read-only dependency no longer held: a
    /// foreign undecided or committed intent held the read key, or the ledger's head had moved past the observed
    /// state — the write-skew shape of a stalled bundle, caught in log order. Each occurrence is a prevented
    /// write skew; the proposing finalizer drives a truthful conflict abort from it.
    /// </summary>
    internal static readonly Counter<long> OnePhaseGatedCommitStaleReadRejections =
        Meter.CreateCounter<long>(
            "kahuna.durable_tx.one_phase_gated_commit_stale_read_rejections",
            description: "One-phase bundled commits rejected at apply because a read-only dependency moved before the bundle applied.");

    private static long onePhaseGatedCommitStaleBaseRejections;

    private static long onePhaseGatedCommitStaleReadRejections;

    /// <summary>Process-wide count behind <see cref="OnePhaseGatedCommitStaleBaseRejections"/>, readable for tests.</summary>
    internal static long OnePhaseGatedCommitStaleBaseRejectionsCount => Interlocked.Read(ref onePhaseGatedCommitStaleBaseRejections);

    /// <summary>Process-wide count behind <see cref="OnePhaseGatedCommitStaleReadRejections"/>, readable for tests.</summary>
    internal static long OnePhaseGatedCommitStaleReadRejectionsCount => Interlocked.Read(ref onePhaseGatedCommitStaleReadRejections);

    internal static void OnePhaseGatedCommitStaleBaseRejected()
    {
        Interlocked.Increment(ref onePhaseGatedCommitStaleBaseRejections);
        OnePhaseGatedCommitStaleBaseRejections.Add(1);
    }

    internal static void OnePhaseGatedCommitStaleReadRejected()
    {
        Interlocked.Increment(ref onePhaseGatedCommitStaleReadRejections);
        OnePhaseGatedCommitStaleReadRejections.Add(1);
    }

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
    /// Wall time of the finalize's validate stage. The stage runs the optimistic read-set validation
    /// (re-probing every tracked read to confirm no committed writer invalidated it) and the pre-decision
    /// replica fence confirmation concurrently, so this records the MAX of the two, never their sum — the
    /// faster half is shadowed. Break a slow stage apart with <see cref="FinalizeReplicaFenceMs"/> (the fence
    /// alone) and <see cref="FinalizeReadSetKeys"/> (read-set size): a slow stage with a fast fence points at
    /// read-set probes or key-actor queueing; a slow fence points at replica apply lag or an unreachable
    /// replica. Runs only when every prepare was durable.
    /// </summary>
    internal static readonly Histogram<double> FinalizeValidateMs =
        Meter.CreateHistogram<double>(
            "kahuna.durable_tx.finalize_validate_ms", unit: "ms",
            description: "Finalize validate-stage wall time (max of read-set validation and replica fence confirmation).");

    /// <summary>
    /// Wall time of the pre-decision replica fence confirmation alone, recorded on every confirmation call —
    /// near zero when the transaction carries no validated-base intents or the node has not joined the
    /// cluster. Kept separate from <see cref="FinalizeValidateMs"/> because that stage records the max of its
    /// two concurrent halves, which hides fence cost whenever read-set probes run longer.
    /// </summary>
    internal static readonly Histogram<double> FinalizeReplicaFenceMs =
        Meter.CreateHistogram<double>(
            "kahuna.durable_tx.finalize_replica_fence_ms", unit: "ms",
            description: "Pre-decision replica fence confirmation wall time.");

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
        Func<long> outstandingDurable,
        Func<IReadOnlyList<(int PartitionId, long Entries, long Bytes)>>? committedHeadLedgerSizes = null)
    {
        Meter gaugeMeter = new("Kahuna", "1.0");

        if (committedHeadLedgerSizes is not null)
        {
            // Tagged by partition: cardinality is the node's hosted partition count, which is small and bounded.
            gaugeMeter.CreateObservableGauge("kahuna.durable_tx.committed_head_ledger_entries",
                () => LedgerMeasurements(committedHeadLedgerSizes(), static size => size.Entries),
                description: "Keys retained by the committed-head ledger, per partition (bounded by the staged-base fence retention).");
            gaugeMeter.CreateObservableGauge("kahuna.durable_tx.committed_head_ledger_bytes",
                () => LedgerMeasurements(committedHeadLedgerSizes(), static size => size.Bytes),
                unit: "By", description: "Approximate bytes retained by the committed-head ledger, per partition.");
        }
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

    private static IEnumerable<Measurement<long>> LedgerMeasurements(
        IReadOnlyList<(int PartitionId, long Entries, long Bytes)> sizes,
        Func<(int PartitionId, long Entries, long Bytes), long> select)
    {
        List<Measurement<long>> measurements = new(sizes.Count);
        foreach ((int PartitionId, long Entries, long Bytes) size in sizes)
            measurements.Add(new Measurement<long>(select(size), new KeyValuePair<string, object?>("partition", size.PartitionId)));
        return measurements;
    }
}
