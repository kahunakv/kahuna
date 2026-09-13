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
/// <summary>How a finalize's prepare retry loop ended; the tag of <see cref="DurableTransactionMetrics.PrepareRetryLoops"/>.</summary>
internal enum PrepareRetryLoopOutcome
{
    Prepared,
    Exhausted,
    Cancelled,
    StaleBase,
    RangeMoved
}

/// <summary>What an over-budget retention sweep could do; the tag of <see cref="DurableTransactionMetrics.GcBudgetSweeps"/>.</summary>
internal enum RetentionBudgetSweepOutcome
{
    /// <summary>Records past the floor were reclaimed ahead of their TTL.</summary>
    Reclaimed,

    /// <summary>Every led terminal record is younger than the floor: nothing could be reclaimed.</summary>
    FloorBound,

    /// <summary>The managed-heap pressure valve opened and everything past the floor was reclaimed.</summary>
    HeapPressure
}

/// <summary>What the one-phase eligibility gate decided for a finalize; the tag of
/// <see cref="DurableTransactionMetrics.OnePhaseGateDecisions"/>. Only <see cref="Entered"/> attempts can
/// later count as a commit or a fallback. Every exclusion names the shape that closed the bundle, so an
/// operator can tell a workload the bundle cannot serve from a flag that did not take effect.</summary>
internal enum OnePhaseGateOutcome
{
    Entered,

    /// <summary>The node has no bundle path at all (no one-phase replicator was wired).</summary>
    Disabled,

    /// <summary>Apply-time validation off, multi-process group: a read-only dependency (a point read of an
    /// unwritten key, or a prefix/range lock) that nothing would re-check at apply.</summary>
    ReadSetBeyondWrites,

    /// <summary>Apply-time validation off, multi-process group: a read-then-written key whose validated base
    /// the bundle could not fence at apply.</summary>
    ValidatedBase,

    /// <summary>Apply-time validation on, multi-process group: a prefix or range lock — a predicate, not a
    /// key, so no deterministic apply-time check exists for it.</summary>
    PredicateRead,

    /// <summary>Apply-time validation on, multi-process group: a read-only key that routes to a partition
    /// other than the anchor — no cross-partition state exists at apply. Under hash routing this is the
    /// placement of the read's key space relative to the written key's; co-locate them (a shared placement
    /// group) to open the bundle.</summary>
    OffPartitionRead,

    /// <summary>Apply-time validation on, multi-process group: a read-only key of a non-persistent durability,
    /// whose writes never feed the committed-head ledger.</summary>
    NonPersistentRead,

    MultiPartition,
    AnchorOffPartition
}

/// <summary>Why an entered one-phase attempt fell back to the two-phase flow; the tag of
/// <see cref="DurableTransactionMetrics.OnePhaseFallbacks"/>.</summary>
internal enum OnePhaseFallbackReason
{
    None,
    ForeignIntent,
    ValidationFailed,
    RemoteLeader
}

/// <summary>Which resolution path materialized or settled an intent; the tag of
/// <see cref="DurableTransactionMetrics.Materializations"/> and <see cref="DurableTransactionMetrics.SettledIntents"/>.</summary>
internal enum ResolutionSource
{
    Finalize,
    Recovery,
    Helping,
    RangeMove
}

/// <summary>Why a resolution pass over one transaction's intents settled nothing; the tag of
/// <see cref="DurableTransactionMetrics.HelpingSettledNothing"/>. <see cref="None"/> means something was settled.</summary>
internal enum ResolveFailureCause
{
    None,
    Fenced,
    MaterializeFailed,
    ApplyFailed,
    SettleFailed
}

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
    /// Commit-repair drives where the owning actor answered Committed while verification still read the
    /// durable state below the intent's revision, so the drive re-promoted the mutation from the parked
    /// intent through the persistence path itself. This is the resident-head-applied / durable-apply-skipped
    /// state (the leader's own one-phase materialization persists nothing and the replicator's durable apply
    /// for the log entry never ran): before the re-promotion existed, the actor's word discarded the parked
    /// intent — the node's last copy of the mutation — and one acknowledged commit was durably lost. Zero on
    /// a healthy node; each tick is that loss being healed instead.
    /// </summary>
    internal static readonly Counter<long> MaterializationRepairRepromotions =
        Meter.CreateCounter<long>(
            "kahuna.transactions.materialization_repair_repromotions",
            description: "Commit repairs re-promoted from the parked mutation after the actor confirmed an apply the durable state does not hold.");

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
    /// Scans that failed loudly because a page answered a non-retryable, non-Get response type
    /// (for example Errored, Aborted, or InvalidInput), or returned a continuation cursor that
    /// could not be decoded. An empty range still answers Get with zero items, so every firing is
    /// a genuine page failure, never emptiness. Before the loud failure existed the scan ended the
    /// stream silently here, and the caller received a truncated result indistinguishable from a
    /// completed scan. The paired error log names the range, the cursor, and the response type.
    /// </summary>
    internal static readonly Counter<long> ScanPageFailed =
        Meter.CreateCounter<long>(
            "kahuna.kv.scan_page_failed",
            description: "Range scans failed loudly after one page answered a non-retryable failure type.");

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
    /// One-phase-eligible transactions that fell back to the standard 2PC flow, tagged by <c>reason</c>: a
    /// foreign durable intent on a written key, failed up-front validation, or a remote anchor leader. Counts
    /// only attempts the gate admitted (see <see cref="OnePhaseGateDecisions"/> for the ones it excluded). A
    /// high rate relative to <see cref="OnePhaseCommits"/> means the fast path rarely completes and the
    /// workload still pays both barriers.
    /// </summary>
    internal static readonly Counter<long> OnePhaseFallbacks =
        Meter.CreateCounter<long>(
            "kahuna.durable_tx.one_phase_fallbacks",
            description: "One-phase-eligible finalizes that fell back to the standard 2PC flow, tagged by reason.");

    /// <summary>
    /// Every finalize's one-phase eligibility verdict, tagged by <c>outcome</c>: <c>entered</c>, or the reason
    /// the gate kept it on the two-phase path — <c>disabled</c>; with apply-time validation off,
    /// <c>read_set_beyond_writes</c> or <c>validated_base</c>; with it on, <c>predicate_read</c>,
    /// <c>off_partition_read</c> or <c>non_persistent_read</c>; and for any mode <c>multi_partition</c> or
    /// <c>anchor_off_partition</c> (see <see cref="OnePhaseGateOutcome"/> for what each names). Together with
    /// <see cref="OnePhaseCommits"/> and <see cref="OnePhaseFallbacks"/> this closes the accounting: every
    /// finalize is exactly one of excluded, fell back, or committed one-phase.
    /// </summary>
    internal static readonly Counter<long> OnePhaseGateDecisions =
        Meter.CreateCounter<long>(
            "kahuna.durable_tx.one_phase_gate",
            description: "One-phase eligibility verdicts per finalize, tagged by outcome.");

    private static readonly KeyValuePair<string, object?> GateEntered = new("outcome", "entered");
    private static readonly KeyValuePair<string, object?> GateDisabled = new("outcome", "disabled");
    private static readonly KeyValuePair<string, object?> GateReadSet = new("outcome", "read_set_beyond_writes");
    private static readonly KeyValuePair<string, object?> GateValidatedBase = new("outcome", "validated_base");
    private static readonly KeyValuePair<string, object?> GatePredicateRead = new("outcome", "predicate_read");
    private static readonly KeyValuePair<string, object?> GateOffPartitionRead = new("outcome", "off_partition_read");
    private static readonly KeyValuePair<string, object?> GateNonPersistentRead = new("outcome", "non_persistent_read");
    private static readonly KeyValuePair<string, object?> GateMultiPartition = new("outcome", "multi_partition");
    private static readonly KeyValuePair<string, object?> GateAnchorOff = new("outcome", "anchor_off_partition");
    private static readonly KeyValuePair<string, object?> FallbackForeignIntent = new("reason", "foreign_intent");
    private static readonly KeyValuePair<string, object?> FallbackValidationFailed = new("reason", "validation_failed");
    private static readonly KeyValuePair<string, object?> FallbackRemoteLeader = new("reason", "remote_leader");
    private static readonly KeyValuePair<string, object?> FallbackOther = new("reason", "other");

    internal static void OnePhaseGateDecided(OnePhaseGateOutcome outcome) =>
        OnePhaseGateDecisions.Add(1, outcome switch
        {
            OnePhaseGateOutcome.Entered => GateEntered,
            OnePhaseGateOutcome.Disabled => GateDisabled,
            OnePhaseGateOutcome.ReadSetBeyondWrites => GateReadSet,
            OnePhaseGateOutcome.ValidatedBase => GateValidatedBase,
            OnePhaseGateOutcome.PredicateRead => GatePredicateRead,
            OnePhaseGateOutcome.OffPartitionRead => GateOffPartitionRead,
            OnePhaseGateOutcome.NonPersistentRead => GateNonPersistentRead,
            OnePhaseGateOutcome.MultiPartition => GateMultiPartition,
            _ => GateAnchorOff
        });

    internal static void OnePhaseFellBack(OnePhaseFallbackReason reason) =>
        OnePhaseFallbacks.Add(1, reason switch
        {
            OnePhaseFallbackReason.ForeignIntent => FallbackForeignIntent,
            OnePhaseFallbackReason.ValidationFailed => FallbackValidationFailed,
            OnePhaseFallbackReason.RemoteLeader => FallbackRemoteLeader,
            _ => FallbackOther
        });

    /// <summary>
    /// Wall time of a one-phase commit's bundled proposal: from the bundle's hand-off to the partition write
    /// aggregator to its completed acknowledgement — measured on the leader that enqueued it (this node, or the
    /// remote anchor leader, whose measurement travels back on the typed reply) and recorded at the origin. The
    /// one-phase equivalent of <see cref="FinalizeFirstPrepareMs"/>: the single durable round that replaces
    /// prepare + decision. Recorded once per one-phase attempt whose bundle was enqueued, whatever the outcome;
    /// an attempt that fell back before proposing records no sample here.
    /// </summary>
    internal static readonly Histogram<double> OnePhaseBundleMs =
        Meter.CreateHistogram<double>(
            "kahuna.durable_tx.one_phase_bundle_ms", unit: "ms",
            description: "One-phase bundled proposal wall time (submission to committed acknowledgement).");

    /// <summary>
    /// Wall time a one-phase attempt spent before its bundle reached an aggregator: the pre-flight foreign-intent
    /// check, up-front read-set validation, the late staged-base re-validation, decision-delta construction,
    /// anchor leader resolution, and — on the <c>route=forwarded</c> series — both wire directions of the forward
    /// to a remote anchor leader. Recorded once per attempt the gate admitted, including attempts that fall back
    /// (their time must not also be charged to the 2PC <c>finalize_*</c> stages, which restart their clocks at
    /// the fallback).
    /// <para>Reconciliation: for an attempt whose bundle was enqueued, this sample plus the matching
    /// <see cref="OnePhaseBundleMs"/> sample equals the attempt's span from its entry to the acknowledged reply,
    /// by construction. The residual outside both, against the commit's total finalize latency, is the shared
    /// pre-gate work (the staged-base preflight of <see cref="FinalizePreflightMs"/> and the prepare-delta
    /// serialization) plus the post-acknowledgement leader-local apply and verdict handling.</para>
    /// </summary>
    internal static readonly Histogram<double> OnePhasePreBundleMs =
        Meter.CreateHistogram<double>(
            "kahuna.durable_tx.one_phase_pre_bundle_ms", unit: "ms",
            description: "One-phase pre-submission wall time (eligibility, bundle construction, anchor routing), tagged by route.");

    /// <summary>One one-phase attempt's pre-submission time, on the series of the route it took (the shared
    /// <c>route</c> tag values defined beside <see cref="SessionRegistrationForwards"/>). An attempt that fell
    /// back before resolving the anchor route is local by construction: every measured moment ran on this
    /// node.</summary>
    internal static void OnePhasePreBundle(double milliseconds, bool forwarded) =>
        OnePhasePreBundleMs.Record(milliseconds < 0 ? 0 : milliseconds, forwarded ? RouteForwarded : RouteLocal);

    /// <summary>
    /// Committed intents whose value was materialized (its key/value record replicated), tagged by
    /// <c>source</c>: the finalize's own resolution, the recovery sweep, the prepare-conflict helping pass, or a
    /// range move's pre-cutover settle. The finalize share is the healthy rate; every other share is settlement
    /// the deferred path did not finish on its own, and the helping share in particular is work paid inside a
    /// successor's prepare stage.
    /// </summary>
    internal static readonly Counter<long> Materializations =
        Meter.CreateCounter<long>(
            "kahuna.durable_tx.materializations",
            description: "Committed intents materialized, tagged by the resolution path that did it.");

    /// <summary>
    /// Prepared intents settled (resolved and removed by a committed settle delta), tagged by <c>source</c> as
    /// <see cref="Materializations"/> is. Its rate against admitted durable transactions shows whether
    /// settlement keeps pace with commits; a widening gap is the backlog that turns into helping and retries.
    /// </summary>
    internal static readonly Counter<long> SettledIntents =
        Meter.CreateCounter<long>(
            "kahuna.durable_tx.settled_intents",
            description: "Prepared intents settled, tagged by the resolution path that did it.");

    private static readonly KeyValuePair<string, object?> SourceFinalize = new("source", "finalize");
    private static readonly KeyValuePair<string, object?> SourceRecovery = new("source", "recovery");
    private static readonly KeyValuePair<string, object?> SourceHelping = new("source", "helping");
    private static readonly KeyValuePair<string, object?> SourceRangeMove = new("source", "range_move");

    private static KeyValuePair<string, object?> SourceTag(ResolutionSource source) => source switch
    {
        ResolutionSource.Recovery => SourceRecovery,
        ResolutionSource.Helping => SourceHelping,
        ResolutionSource.RangeMove => SourceRangeMove,
        _ => SourceFinalize
    };

    internal static void Materialized(ResolutionSource source, int count)
    {
        if (count > 0)
            Materializations.Add(count, SourceTag(source));
    }

    internal static void Settled(ResolutionSource source, int count)
    {
        if (count > 0)
            SettledIntents.Add(count, SourceTag(source));
    }

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
    /// and reading back the winner. One durable round trip plus the record read. The two halves are recorded
    /// separately as <see cref="FinalizeDecisionReplicateMs"/> and <see cref="FinalizeDecisionLookupMs"/>.
    /// </summary>
    internal static readonly Histogram<double> FinalizeDecisionMs =
        Meter.CreateHistogram<double>(
            "kahuna.durable_tx.finalize_decision_ms", unit: "ms",
            description: "Finalize decision-stage wall time (terminal transition + winner read-back).");

    /// <summary>
    /// The decision stage's replication half alone: from proposing the terminal transition (locally through the
    /// scheduler, or forwarded to a remote anchor leader) to its committed acknowledgement.
    /// </summary>
    internal static readonly Histogram<double> FinalizeDecisionReplicateMs =
        Meter.CreateHistogram<double>(
            "kahuna.durable_tx.finalize_decision_replicate_ms", unit: "ms",
            description: "Decision-stage replication wall time (terminal transition proposal to acknowledgement).");

    /// <summary>
    /// The decision stage's read-back half alone: the canonical record lookup that names the winner. Local when
    /// this node leads the anchor partition; one inter-node call otherwise, always a cache miss for the record
    /// being decided.
    /// </summary>
    internal static readonly Histogram<double> FinalizeDecisionLookupMs =
        Meter.CreateHistogram<double>(
            "kahuna.durable_tx.finalize_decision_lookup_ms", unit: "ms",
            description: "Decision-stage canonical record read-back wall time.");

    /// <summary>
    /// Wall time of the pre-propose staged-base validation (the write-side compare-and-set that runs before
    /// anything durable is proposed). The first slice of <see cref="FinalizePrepareMs"/>; recorded only when the
    /// check is wired.
    /// </summary>
    internal static readonly Histogram<double> FinalizePreflightMs =
        Meter.CreateHistogram<double>(
            "kahuna.durable_tx.finalize_preflight_ms", unit: "ms",
            description: "Pre-propose staged-base validation wall time.");

    /// <summary>
    /// Wall time of the first prepare barrier alone: the anchor bundle (or record init then prepare) and every
    /// other participant's prepare, awaited together, before any conflict retry. Subtracting this and
    /// <see cref="FinalizePreflightMs"/> from <see cref="FinalizePrepareMs"/> leaves the retry loop's cost
    /// (helping plus backoff plus re-prepares). Recorded only when the barrier resolved (not on a failed init).
    /// </summary>
    internal static readonly Histogram<double> FinalizeFirstPrepareMs =
        Meter.CreateHistogram<double>(
            "kahuna.durable_tx.finalize_first_prepare_ms", unit: "ms",
            description: "First prepare barrier wall time, before any conflict retry.");

    /// <summary>
    /// Prepare re-proposal rounds a finalize ran after its first barrier left a participant unprepared. Zero for
    /// the uncontended path; bounded by the finalizer's retry budget. Every re-proposal round re-submits every
    /// participant, so this multiplies directly into proposals per commit under contention.
    /// </summary>
    internal static readonly Histogram<int> FinalizePrepareRetries =
        Meter.CreateHistogram<int>(
            "kahuna.durable_tx.finalize_prepare_retries", unit: "{round}",
            description: "Prepare re-proposal rounds per finalize.");

    /// <summary>
    /// Helping-pass invocations per finalize that entered the prepare retry loop (one per participant partition
    /// per round). Recorded only for finalizes that retried, so the distribution is not diluted by the
    /// uncontended majority.
    /// </summary>
    internal static readonly Histogram<int> FinalizeHelperCalls =
        Meter.CreateHistogram<int>(
            "kahuna.durable_tx.finalize_helper_calls", unit: "{call}",
            description: "Prepare-conflict helping invocations per retrying finalize.");

    /// <summary>
    /// Total wall time a retrying finalize spent inside the helping pass (materializing and settling
    /// decided-but-unsettled blockers inline). Recorded only for finalizes that entered the retry loop.
    /// </summary>
    internal static readonly Histogram<double> FinalizeHelperMs =
        Meter.CreateHistogram<double>(
            "kahuna.durable_tx.finalize_helper_ms", unit: "ms",
            description: "Total helping-pass wall time per retrying finalize.");

    /// <summary>
    /// Total wall time a retrying finalize slept in prepare-retry backoff (the rounds where helping made no
    /// progress). Recorded only for finalizes that entered the retry loop.
    /// </summary>
    internal static readonly Histogram<double> FinalizeBackoffMs =
        Meter.CreateHistogram<double>(
            "kahuna.durable_tx.finalize_backoff_ms", unit: "ms",
            description: "Total prepare-retry backoff wall time per retrying finalize.");

    /// <summary>
    /// Prepare retry loops entered (the first barrier left a participant unprepared), tagged by how the loop
    /// ended: <c>prepared</c> (a later round acknowledged every participant), <c>exhausted</c> (the budget ran
    /// out and the finalize aborts as a retryable failure), <c>stale_base</c> (a refusal named a moved base and
    /// the finalize aborted as a conflict at once), <c>range_moved</c> (a refused participant's range moved since
    /// freeze; a clean retry), or <c>cancelled</c>. The exhausted share is the fraction of contended finalizes
    /// whose retry work bought nothing.
    /// </summary>
    internal static readonly Counter<long> PrepareRetryLoops =
        Meter.CreateCounter<long>(
            "kahuna.durable_tx.prepare_retry_loops",
            description: "Prepare retry loops entered, tagged by how they ended.");

    private static readonly KeyValuePair<string, object?> LoopOutcomePrepared = new("outcome", "prepared");
    private static readonly KeyValuePair<string, object?> LoopOutcomeExhausted = new("outcome", "exhausted");
    private static readonly KeyValuePair<string, object?> LoopOutcomeCancelled = new("outcome", "cancelled");
    private static readonly KeyValuePair<string, object?> LoopOutcomeStaleBase = new("outcome", "stale_base");
    private static readonly KeyValuePair<string, object?> LoopOutcomeRangeMoved = new("outcome", "range_moved");

    internal static void PrepareRetryLoopEnded(PrepareRetryLoopOutcome outcome) =>
        PrepareRetryLoops.Add(1, outcome switch
        {
            PrepareRetryLoopOutcome.Prepared => LoopOutcomePrepared,
            PrepareRetryLoopOutcome.Cancelled => LoopOutcomeCancelled,
            PrepareRetryLoopOutcome.StaleBase => LoopOutcomeStaleBase,
            PrepareRetryLoopOutcome.RangeMoved => LoopOutcomeRangeMoved,
            _ => LoopOutcomeExhausted
        });

    /// <summary>
    /// Transactions that saw at least one late-commit rejection, counted once per transaction on the first
    /// rejection. <see cref="LateCommitRejections"/> counts rejection events, and one transaction whose client
    /// retries the commit can produce several; the ratio of the two is the retry amplification of the deadline
    /// gate, and this counter is the number of transactions whose committed work was actually lost to it.
    /// </summary>
    internal static readonly Counter<long> LateCommitRejectedTransactions =
        Meter.CreateCounter<long>(
            "kahuna.durable_tx.late_commit_rejected_transactions",
            description: "Distinct transactions that saw at least one late-commit rejection.");

    private static long lateCommitRejectedTransactions;

    /// <summary>Process-wide count behind <see cref="LateCommitRejectedTransactions"/>, readable for tests.</summary>
    internal static long LateCommitRejectedTransactionsCount => Interlocked.Read(ref lateCommitRejectedTransactions);

    internal static void LateCommitRejectedTransaction()
    {
        Interlocked.Increment(ref lateCommitRejectedTransactions);
        LateCommitRejectedTransactions.Add(1);
    }

    /// <summary>
    /// Durable operations this node forwarded to a remote partition leader (a 2PC leg whose partition it does
    /// not lead), tagged by <c>kind</c> (<c>replicate</c> for a record/intent/value delta, <c>commit</c> and
    /// <c>rollback</c> for a leader-state apply) and <c>result</c> (<c>ok</c>, <c>refused</c> for a false
    /// reply, <c>threw</c>). A remote-anchor commit costs several of these per transaction; the count per
    /// committed transaction is the transport cost the local-leader path never pays.
    /// </summary>
    internal static readonly Counter<long> DurableOperationForwards =
        Meter.CreateCounter<long>(
            "kahuna.durable_tx.durable_operation_forwards",
            description: "Durable operations forwarded to a remote partition leader, tagged by kind and result.");

    /// <summary>
    /// Canonical transaction-record lookups this node sent to a remote anchor leader, tagged by <c>result</c>
    /// (<c>found</c>, <c>absent</c>, <c>threw</c>). Terminal answers are cached, so a steady rate under a
    /// steady commit rate measures the lookups the cache cannot serve: the decision read-back, which is always
    /// cold for the record being decided.
    /// </summary>
    internal static readonly Counter<long> RecordLookupForwards =
        Meter.CreateCounter<long>(
            "kahuna.durable_tx.record_lookup_forwards",
            description: "Canonical record lookups sent to a remote anchor leader, tagged by result.");

    /// <summary>
    /// Pre-decision replica fence requests sent to other replicas, tagged by <c>result</c> (<c>ok</c>,
    /// <c>unserviced</c> for a reply that carried no verdicts, <c>threw</c>). Roughly participant partitions
    /// times replicas-minus-one per validated-base commit; the exact count per commit is what this measures.
    /// </summary>
    internal static readonly Counter<long> ReplicaFenceRequests =
        Meter.CreateCounter<long>(
            "kahuna.durable_tx.replica_fence_requests",
            description: "Replica fence verdict requests sent to other replicas, tagged by result.");

    /// <summary>
    /// Forwarded durable operations and record lookups that reached a node which had to redirect them to the
    /// actual leader (the sender routed on a stale or guessed leader), tagged by <c>op</c>. Each redirect is a
    /// second hop the sender's routing did not predict.
    /// </summary>
    internal static readonly Counter<long> ForwardRedirects =
        Meter.CreateCounter<long>(
            "kahuna.durable_tx.forward_redirects",
            description: "Forwarded durable operations and lookups redirected by the receiver to the actual leader.");

    private static readonly KeyValuePair<string, object?> KindReplicate = new("kind", "replicate");
    private static readonly KeyValuePair<string, object?> KindCommit = new("kind", "commit");
    private static readonly KeyValuePair<string, object?> KindRollback = new("kind", "rollback");
    private static readonly KeyValuePair<string, object?> KindBundle = new("kind", "bundle");
    private static readonly KeyValuePair<string, object?> KindDecision = new("kind", "decision");
    private static readonly KeyValuePair<string, object?> KindOnePhase = new("kind", "one_phase");
    private static readonly KeyValuePair<string, object?> KindOther = new("kind", "other");

    /// <summary>A typed multi-entry bundle forward (one call carrying an anchor's record init and prepare).</summary>
    internal static void DurableBundleForwarded(bool ok) =>
        DurableOperationForwards.Add(1, KindBundle, ok ? ResultOk : ResultRefused);

    internal static void DurableBundleForwardThrew() =>
        DurableOperationForwards.Add(1, KindBundle, ResultThrew);

    /// <summary>A typed decision forward that returns the canonical outcome with the replication result.</summary>
    internal static void DurableDecisionForwarded(bool ok) =>
        DurableOperationForwards.Add(1, KindDecision, ok ? ResultOk : ResultRefused);

    internal static void DurableDecisionForwardThrew() =>
        DurableOperationForwards.Add(1, KindDecision, ResultThrew);

    /// <summary>A typed one-phase bundle forward (record init + prepare + commit decision in one call), answered
    /// with the canonical outcome read on the anchor leader after the ordered apply.</summary>
    internal static void DurableOnePhaseForwarded(bool ok) =>
        DurableOperationForwards.Add(1, KindOnePhase, ok ? ResultOk : ResultRefused);

    internal static void DurableOnePhaseForwardThrew() =>
        DurableOperationForwards.Add(1, KindOnePhase, ResultThrew);
    private static readonly KeyValuePair<string, object?> ResultOk = new("result", "ok");
    private static readonly KeyValuePair<string, object?> ResultRefused = new("result", "refused");
    private static readonly KeyValuePair<string, object?> ResultThrew = new("result", "threw");
    private static readonly KeyValuePair<string, object?> ResultFound = new("result", "found");
    private static readonly KeyValuePair<string, object?> ResultAbsent = new("result", "absent");
    private static readonly KeyValuePair<string, object?> ResultUnserviced = new("result", "unserviced");
    private static readonly KeyValuePair<string, object?> OpDurableOperation = new("op", "durable_operation");
    private static readonly KeyValuePair<string, object?> OpRecordLookup = new("op", "record_lookup");

    private static KeyValuePair<string, object?> KindTag(int kind) => kind switch
    {
        0 => KindReplicate,
        1 => KindCommit,
        2 => KindRollback,
        _ => KindOther
    };

    /// <summary><paramref name="kind"/> is the durable-operation wire kind (0 replicate, 1 commit, 2 rollback).</summary>
    internal static void DurableOperationForwarded(int kind, bool ok) =>
        DurableOperationForwards.Add(1, KindTag(kind), ok ? ResultOk : ResultRefused);

    internal static void DurableOperationForwardThrew(int kind) =>
        DurableOperationForwards.Add(1, KindTag(kind), ResultThrew);

    internal static void RecordLookupForwarded(bool found) =>
        RecordLookupForwards.Add(1, found ? ResultFound : ResultAbsent);

    internal static void RecordLookupForwardThrew() => RecordLookupForwards.Add(1, ResultThrew);

    internal static void ReplicaFenceRequested(bool serviced) =>
        ReplicaFenceRequests.Add(1, serviced ? ResultOk : ResultUnserviced);

    internal static void ReplicaFenceRequestThrew() => ReplicaFenceRequests.Add(1, ResultThrew);

    internal static void DurableOperationRedirected() => ForwardRedirects.Add(1, OpDurableOperation);

    internal static void RecordLookupRedirected() => ForwardRedirects.Add(1, OpRecordLookup);

    /// <summary>
    /// Session-registration calls this node made for a transaction whose session lives on the node that leads
    /// the coordinator partition, tagged by <c>op</c> (<c>begin</c>, <c>complete</c>, <c>working_set</c>),
    /// <c>route</c> (<c>local</c> when this node holds the session, <c>forwarded</c> when the call left the
    /// node) and <c>result</c> (<c>ok</c>, <c>refused</c> for a rejection or a not-delivered answer,
    /// <c>threw</c>, <c>unrouted</c> for an attempt that found no reachable session leader and so never left
    /// the node). An operation executed away from its session node costs one <c>begin</c> and one
    /// <c>complete</c> forward; a working-set query made away from the session node costs one
    /// <c>working_set</c> forward. The forwarded count per committed transaction is the registration cost that
    /// a session-local transaction never pays, and the local count is the denominator that turns it into a
    /// share.
    /// </summary>
    internal static readonly Counter<long> SessionRegistrationForwards =
        Meter.CreateCounter<long>(
            "kahuna.durable_tx.session_registration_forwards",
            description: "Session-registration calls, tagged by op, route and result.");

    private static readonly KeyValuePair<string, object?> OpBegin = new("op", "begin");
    private static readonly KeyValuePair<string, object?> OpComplete = new("op", "complete");
    private static readonly KeyValuePair<string, object?> OpWorkingSet = new("op", "working_set");
    private static readonly KeyValuePair<string, object?> RouteLocal = new("route", "local");
    private static readonly KeyValuePair<string, object?> RouteForwarded = new("route", "forwarded");
    private static readonly KeyValuePair<string, object?> ResultUnrouted = new("result", "unrouted");

    private static KeyValuePair<string, object?> OpTag(SessionRegistrationOp op) => op switch
    {
        SessionRegistrationOp.Begin => OpBegin,
        SessionRegistrationOp.Complete => OpComplete,
        _ => OpWorkingSet
    };

    /// <summary>A registration served on this node because it holds the session.</summary>
    internal static void SessionRegistrationLocal(SessionRegistrationOp op, bool ok) =>
        SessionRegistrationForwards.Add(1, OpTag(op), RouteLocal, ok ? ResultOk : ResultRefused);

    /// <summary>A registration this node sent to the session owner, and the answer it came back with.</summary>
    internal static void SessionRegistrationForwarded(SessionRegistrationOp op, bool ok) =>
        SessionRegistrationForwards.Add(1, OpTag(op), RouteForwarded, ok ? ResultOk : ResultRefused);

    /// <summary>A registration this node sent to the session owner whose transport threw.</summary>
    internal static void SessionRegistrationForwardThrew(SessionRegistrationOp op) =>
        SessionRegistrationForwards.Add(1, OpTag(op), RouteForwarded, ResultThrew);

    /// <summary>
    /// A registration that had to be forwarded but found no session leader to forward it to, so no call left
    /// the node. Counted apart from the forwards, because a hop that never happened must not inflate the hop
    /// count a batching decision divides by.
    /// </summary>
    internal static void SessionRegistrationUnrouted(SessionRegistrationOp op) =>
        SessionRegistrationForwards.Add(1, OpTag(op), RouteForwarded, ResultUnrouted);

    /// <summary>
    /// Bucket boundaries for <see cref="SessionRegistrationForwardMs"/>, in milliseconds. The default
    /// OpenTelemetry boundaries start at 0, 5, 10 ms, which drops every forwarded registration into the first
    /// bucket and answers nothing: a container-to-container round trip is a small fraction of a millisecond,
    /// and the question this histogram exists to settle — whether registration hops explain a material share
    /// of commit latency — turns on values near a quarter of a millisecond. These boundaries resolve that
    /// region and still separate a stalled forward from a slow one.
    /// </summary>
    private static readonly InstrumentAdvice<double> SessionRegistrationForwardAdvice = new()
    {
        HistogramBucketBoundaries = [0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 25, 50, 100]
    };

    /// <summary>
    /// Wall time of a session-registration call this node forwarded to the session owner, from the sender's
    /// call to the answer it came back with, tagged by <c>op</c> (<c>begin</c>, <c>complete</c>,
    /// <c>working_set</c>). The batcher wait is inside it deliberately: what a consumer needs is the cost a
    /// forwarded registration adds to its transaction, which includes queueing behind the coalescing window,
    /// not just the wire.
    ///
    /// <para>Only the forwarded branch is timed. The local branch is a method call on this node's own session
    /// table, and timing it would mix a number three orders of magnitude smaller into a distribution whose
    /// whole purpose is the remote cost; the <c>route</c> tag of
    /// <see cref="SessionRegistrationForwards"/> already separates the two populations.</para>
    ///
    /// <para>A forward whose transport threw is recorded with the time it consumed. A forward that failed
    /// after 40 ms cost its transaction 40 ms, and dropping it would flatter the distribution.</para>
    /// </summary>
    internal static readonly Histogram<double> SessionRegistrationForwardMs =
        Meter.CreateHistogram<double>(
            "kahuna.durable_tx.session_registration_forward_ms",
            unit: "ms",
            description: "Duration of a forwarded session-registration call, tagged by op.",
            tags: null,
            advice: SessionRegistrationForwardAdvice);

    /// <summary>Records the wall time one forwarded session-registration call took.</summary>
    internal static void SessionRegistrationForwardTimed(SessionRegistrationOp op, double elapsedMs) =>
        SessionRegistrationForwardMs.Record(elapsedMs, OpTag(op));

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
    /// Helping passes that found a decided-but-unsettled blocker and settled none of its intents, tagged by
    /// <c>cause</c>: the abort fence refused the group, no materialization committed, the leader-local apply
    /// did not confirm, or the settle delta did not replicate. Each is a retry round the blocked finalize spent
    /// on work that bought no progress; the finalize then backs off exactly as if helping were disabled. A
    /// sustained <c>settle_failed</c> or <c>materialize_failed</c> rate points at scheduler backpressure, a
    /// quiesced range, or a forwarding failure on the blocker's partition.
    /// </summary>
    internal static readonly Counter<long> HelpingSettledNothing =
        Meter.CreateCounter<long>(
            "kahuna.durable_tx.helping_settled_nothing",
            description: "Helping passes over a decided blocker that settled none of its intents, tagged by cause.");

    private static readonly KeyValuePair<string, object?> CauseFenced = new("cause", "fenced");
    private static readonly KeyValuePair<string, object?> CauseMaterializeFailed = new("cause", "materialize_failed");
    private static readonly KeyValuePair<string, object?> CauseApplyFailed = new("cause", "apply_failed");
    private static readonly KeyValuePair<string, object?> CauseSettleFailed = new("cause", "settle_failed");
    private static readonly KeyValuePair<string, object?> CauseOther = new("cause", "other");

    internal static void HelpingSettledNone(ResolveFailureCause cause) =>
        HelpingSettledNothing.Add(1, cause switch
        {
            ResolveFailureCause.Fenced => CauseFenced,
            ResolveFailureCause.MaterializeFailed => CauseMaterializeFailed,
            ResolveFailureCause.ApplyFailed => CauseApplyFailed,
            ResolveFailureCause.SettleFailed => CauseSettleFailed,
            _ => CauseOther
        });

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

    /// <summary>
    /// Terminal transaction records reclaimed by the retention sweep <b>before</b> their TTL because the node's
    /// resident-metadata budget (<c>DurableRecordRetentionMax</c> / <c>DurableRecordRetentionMaxBytes</c>) was
    /// exceeded, or — tagged <c>reason=heap_pressure</c> — because the managed heap crossed the pressure
    /// threshold. Counted in addition to <see cref="GcRecordsReclaimed"/>. A sustained non-zero rate means the
    /// commit rate times the retention floor exceeds the budget: the memory bound is doing its job, and the
    /// idempotency window is the floor rather than the TTL.
    /// </summary>
    internal static readonly Counter<long> GcRecordsReclaimedEarly =
        Meter.CreateCounter<long>(
            "kahuna.durable_tx.gc_records_reclaimed_early",
            description: "Terminal transaction records reclaimed before their TTL by the resident-metadata budget or the heap-pressure valve.");

    /// <summary>Retention sweeps that found the managed heap above the pressure threshold and reclaimed every
    /// record past the floor. Any non-zero value means the budgets are undersized for the node's heap.</summary>
    internal static readonly Counter<long> GcHeapPressureSweeps =
        Meter.CreateCounter<long>(
            "kahuna.durable_tx.gc_heap_pressure_sweeps",
            description: "Retention sweeps run under managed-heap pressure (every terminal record past the floor reclaimed).");

    internal static void RecordsReclaimed(int count) => GcRecordsReclaimed.Add(count);

    internal static void RecordsReclaimedEarly(int count, bool heapPressure) =>
        GcRecordsReclaimedEarly.Add(count, new KeyValuePair<string, object?>("reason", heapPressure ? "heap_pressure" : "budget"));

    internal static void HeapPressureSweep() => GcHeapPressureSweeps.Add(1);

    /// <summary>
    /// Retention sweeps that found the resident-metadata budget exceeded, by what the sweep could do about it:
    /// <c>reclaimed</c> (records past the floor were reclaimed ahead of their TTL), <c>floor_bound</c> (every
    /// record this node leads is younger than the floor, so nothing could be reclaimed — the rate × floor
    /// product, not the budget, is sizing the heap), or <c>heap_pressure</c> (the valve opened). Being over
    /// budget is the steady state under sustained load, so the log carries it at most once per ten minutes;
    /// this counter and the <c>retention_over_budget</c> gauge carry it continuously.
    /// </summary>
    internal static readonly Counter<long> GcBudgetSweeps =
        Meter.CreateCounter<long>("kahuna.durable_tx.gc_budget_sweeps",
            description: "Retention sweeps that ran over the resident-metadata budget, tagged by outcome (reclaimed, floor_bound, heap_pressure).");

    private static readonly KeyValuePair<string, object?> BudgetOutcomeReclaimed = new("outcome", "reclaimed");
    private static readonly KeyValuePair<string, object?> BudgetOutcomeFloorBound = new("outcome", "floor_bound");
    private static readonly KeyValuePair<string, object?> BudgetOutcomeHeapPressure = new("outcome", "heap_pressure");

    internal static void BudgetSweep(RetentionBudgetSweepOutcome outcome) =>
        GcBudgetSweeps.Add(1, outcome switch
        {
            RetentionBudgetSweepOutcome.HeapPressure => BudgetOutcomeHeapPressure,
            RetentionBudgetSweepOutcome.FloorBound => BudgetOutcomeFloorBound,
            _ => BudgetOutcomeReclaimed
        });

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
        Func<IReadOnlyList<(int PartitionId, long Entries, long Bytes)>>? committedHeadLedgerSizes = null,
        Func<long>? recordBytes = null,
        Func<long>? receiptBytes = null)
    {
        Meter gaugeMeter = new("Kahuna", "1.0");

        // The two byte gauges are what the resident-metadata budget compares against its byte bound; exposing
        // them next to the counts lets an operator size DurableRecordRetentionMaxBytes from a live node.
        if (recordBytes is not null)
            gaugeMeter.CreateObservableGauge("kahuna.durable_tx.resident_record_bytes", recordBytes,
                unit: "By", description: "Estimated heap bytes retained by resident canonical transaction records (the retention byte budget's input).");
        if (receiptBytes is not null)
            gaugeMeter.CreateObservableGauge("kahuna.durable_tx.resident_receipt_bytes", receiptBytes,
                unit: "By", description: "Estimated heap bytes retained by resident completion receipts (the retention byte budget's input).");

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
