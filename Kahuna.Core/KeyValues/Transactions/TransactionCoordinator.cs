
using System.Collections.Concurrent;
using System.Diagnostics;
using DotNext;
using Kommander;
using Kommander.Time;

using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues.Logging;
using Kahuna.Server.Replication;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using TransactionHandle = Kahuna.Shared.KeyValue.TransactionHandle;

namespace Kahuna.Server.KeyValues.Transactions;

/// <summary>
/// Manages interactive transaction sessions and the generic 2PC lifecycle.
/// Has no dependency on the script parser or AST. Script execution uses
/// <see cref="ScriptTransactionExecutor"/> which composes this coordinator.
/// </summary>
internal sealed class TransactionCoordinator : IDisposable
{
    /// <summary>
    /// Grace added on top of a session's own timeout before the reaper reclaims it. Kept at or above
    /// the write-intent TTL (DefaultTxCompleteTimeout, 15 s) so that by the time a session is reaped
    /// any commit it could still attempt would already fail on expired intents. A session-owned intent
    /// carries no deadline of its own; the liveness ceiling built from this constant is what makes the
    /// same statement true for it (see <see cref="SessionMaxLifespanMs"/>).
    /// </summary>
    internal const int ReapGraceMs = 15_000;

    /// <summary>
    /// Upper bound, in milliseconds, on how long a participant retains an effect a dispatched operation may
    /// have placed (the write-intent lease, DefaultTxCompleteTimeout, 15 s). While a reaped session still has
    /// unresolved dispatched operations, the reaper waits at least this long past the session deadline before
    /// expiring it, so it never abandons a session whose in-flight effect could still land at a participant.
    /// </summary>
    internal const int MaxParticipantEffectTtlMs = 15_000;

    /// <summary>
    /// Longest wall-clock span, in milliseconds, that any interactive session can still matter: its clamped
    /// maximum timeout, plus the reaper's grace window, plus the maximum time a dispatched effect can still
    /// land at a participant after the session is reaped. Past this span from a session's start, that session
    /// is provably gone and anything it left behind is orphaned. Both consumers of that fact — the MVCC
    /// snapshot trim and the session-owned lock ceiling — read the span from here so they cannot drift apart.
    /// </summary>
    internal static int SessionMaxLifespanMs(KahunaConfiguration configuration) =>
        configuration.MaxTransactionTimeout + ReapGraceMs + MaxParticipantEffectTtlMs;

    /// <summary>
    /// Maximum number of phase-two (commit/rollback) retry attempts when a participant returns
    /// MustRetry due to a transient Raft condition (e.g. NodeIsNotLeader after a leader election).
    /// With Phase2RetryDelayMs = 250 this allows up to 5 s for leadership to stabilise.
    /// </summary>
    internal const int MaxPhase2Retries = 20;

    /// <summary>Delay between phase-two retry attempts, in milliseconds.</summary>
    internal const int Phase2RetryDelayMs = 250;

    private readonly KeyValuesManager manager;

    private readonly KahunaConfiguration configuration;

    private readonly IRaft raft;

    private readonly ILogger<IKahuna> logger;

    internal readonly ConcurrentDictionary<HLCTimestamp, TransactionContext> sessions = new();

    /// <summary>
    /// Count of durable transactions currently being driven through finalize on this node — reserved before a
    /// transaction prepares and released when its attempt ends (decision installed, or the attempt gave up). It
    /// bounds concurrent durable inflow to <c>DurableDecisionOutstandingMax</c>, so a burst backpressures with
    /// <see cref="KeyValueResponseType.MustRetry"/> instead of admitting unbounded prepared state. Sized so the
    /// write scheduler's terminal-class reserve (per-partition and global) always has room for every admitted
    /// transaction's eventual decision/settle; a value &lt;= 0 in configuration disables the gate.
    /// </summary>
    private int outstandingDurable;

    /// <summary>
    /// Best-effort retention of finalized outcomes after their session leaves <see cref="sessions"/>. A
    /// duplicate commit/rollback that arrives once the session is gone replays the recorded terminal answer
    /// (Committed/RolledBack/expired) instead of an unknown result. Bounded by size and HLC age (configuration
    /// <c>TransactionOutcomeRetentionMax</c> / <c>TransactionOutcomeRetentionTtl</c>); after eviction a
    /// duplicate falls through to the durable-record consult: with a routable anchor it receives the
    /// record's true outcome or a retryable <see cref="KeyValueResponseType.MustRetry"/> while the record
    /// is absent, and only an unanchored handle receives the unknown
    /// <see cref="KeyValueResponseType.Errored"/> — never a conflict Aborted.
    /// Lookups are lock-free; inserts and eviction are O(1) — see <see cref="TerminalOutcomeWindow"/> for the
    /// structure and why a scanning implementation is not acceptable on this path.
    /// </summary>
    private readonly TerminalOutcomeWindow terminalOutcomes = new();

    /// <summary>
    /// Admission gate for interactive sessions. Deliberately a different instance from the script executor's:
    /// a session holds its slot for as long as the client keeps it open, so sharing one pool would let idle
    /// client-paced sessions occupy every slot and stall script transactions node-wide.
    /// </summary>
    private readonly TransactionPriorityOrderer orderer;

    public TransactionCoordinator(KeyValuesManager manager, KahunaConfiguration configuration, IRaft raft, ILogger<IKahuna> logger, TransactionPriorityOrderer orderer)
    {
        this.manager = manager;
        this.configuration = configuration;
        this.raft = raft;
        this.logger = logger;
        this.orderer = orderer;
    }

    /// <summary>
    /// Starts a new interactive transaction session and returns a handle that pins the session to
    /// the coordinator partition. The handle must be supplied on every subsequent commit/rollback.
    /// </summary>
    public async Task<(KeyValueResponseType, TransactionHandle)> StartTransaction(KeyValueTransactionOptions options)
    {
        string coordinatorKey = string.IsNullOrEmpty(options.CoordinatorKey)
            ? Guid.NewGuid().ToString("N")
            : options.CoordinatorKey;

        // Reject an out-of-range policy value up front. External Begin casts numeric wire values straight to
        // these enums, so an unknown ordinal would otherwise slip through and fall into whichever equality
        // branch happens to match instead of being reported as malformed input.
        if (!Enum.IsDefined(options.Locking) ||
            !Enum.IsDefined(options.ReadValidation) ||
            !Enum.IsDefined(options.DecisionDurability))
            return (KeyValueResponseType.InvalidInput, new TransactionHandle(HLCTimestamp.Zero, coordinatorKey));

        // Priority is normalized rather than rejected, unlike the policies above. Those change what the
        // transaction *means* — isolation, durability — so an unrecognized value must never be silently
        // defaulted. Priority only influences the order in which work starts, so an unknown ordinal (a raw
        // REST number, a cast enum, a newer peer's value) is treated as ordinary work rather than failing an
        // otherwise valid transaction. This matches the gRPC wire contract and keeps an out-of-range value
        // from being read as Critical.
        TransactionPriority priority = TransactionPriorityOrderer.Normalize(options.Priority);

        // A fixed read snapshot and write-skew validation are mutually exclusive: pinning reads to a past
        // timestamp cannot detect concurrent writes that land after that snapshot, so the two together would
        // silently promise a guarantee the engine cannot honor. Reject the combination up front.
        if (options.ReadTimestamp != HLCTimestamp.Zero && options.ReadValidation == ReadValidation.TrackAndValidate)
            return (KeyValueResponseType.InvalidInput, new TransactionHandle(HLCTimestamp.Zero, coordinatorKey));

        // Clamp to the server's hard maximum so no admitted session can outlive it — the bound that
        // makes age-based reclamation of a transaction's orphaned MVCC read snapshots correct.
        int timeout = Math.Min(
            options.Timeout <= 0 ? configuration.DefaultTransactionTimeout : options.Timeout,
            configuration.MaxTransactionTimeout);

        // How long this caller will queue at the door, which is deliberately not the session timeout above.
        // Those are unrelated quantities: the session timeout is the reaper window — how long a transaction may
        // live — while this bounds how long the caller waits to start one, and a long-running transaction is not
        // thereby a patient one. Reusing the session timeout here let a caller that asked for a long-lived
        // session occupy a queue slot for that whole span. Clamped so no caller can hold a slot longer than the
        // operator allows.
        int admissionWait = Math.Min(
            options.AdmissionWaitMs <= 0 ? configuration.DefaultAdmissionWaitMs : options.AdmissionWaitMs,
            configuration.MaxAdmissionWaitMs);

        // Take a session slot before the session exists. Below the ceiling this completes synchronously.
        AdmissionLease? lease;

        using (CancellationTokenSource admissionCts = new(TimeSpan.FromMilliseconds(admissionWait)))
        {
            try
            {
                lease = await orderer.AdmitAsync(priority, admissionCts.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // The admission budget expired while queued. No session was created, so there is nothing to clean
                // up, but the node is saturated — the caller should back off rather than re-queue immediately,
                // which is what separates this from the transient MustRetry conditions below.
                return (KeyValueResponseType.AdmissionRefused, new TransactionHandle(HLCTimestamp.Zero, coordinatorKey));
            }
        }

        // Refused outright because the wait queue is full — the gate shedding load. Same contract as a budget
        // expiry: nothing was started, so a retry is safe, but it should follow a back-off.
        if (lease is null)
            return (KeyValueResponseType.AdmissionRefused, new TransactionHandle(HLCTimestamp.Zero, coordinatorKey));

        bool added = false;
        HLCTimestamp transactionId = HLCTimestamp.Zero;

        try
        {
            do
            {
                transactionId = raft.HybridLogicalClock.SendOrLocalEvent(raft.GetLocalNodeId());

                TransactionContext context = new()
                {
                    CoordinatorKey     = coordinatorKey,
                    TransactionId      = transactionId,
                    Locking            = options.Locking,
                    ReadValidation     = options.ReadValidation,
                    DecisionDurability = options.DecisionDurability,
                    ReadTimestamp      = options.ReadTimestamp,
                    Priority           = priority,
                    Action             = KeyValueTransactionAction.Commit,
                    AsyncRelease       = options.AsyncRelease,
                    Timeout            = timeout,
                    // The slot travels with the session and is returned by whichever path finalizes it.
                    AdmissionLease     = lease
                };

                added = sessions.TryAdd(transactionId, context);

            } while (!added);
        }
        finally
        {
            // Ownership of the slot transfers to the published session and is released by whichever path
            // finalizes it. Until that publication succeeds the slot belongs to nobody, so anything that goes
            // wrong in between — a clock fault while minting the id, teardown, an unexpected throw — must give
            // it back here or the node silently loses capacity for the rest of its life.
            if (!added)
                lease.Dispose();
        }

        logger.LogStartedInteractiveTransaction(transactionId);

        return (KeyValueResponseType.Set, new TransactionHandle(transactionId, coordinatorKey));
    }

    /// <summary>
    /// Retires a finalized session and returns the admission slot it held. Every path that ends a session —
    /// commit, rollback, and the reaper reclaiming an abandoned one — goes through here, so node capacity
    /// recovers even when the client never cooperates. The lease's own release is idempotent, so a racing
    /// second finalize that finds the session already gone changes nothing.
    /// </summary>
    private void FinalizeSession(HLCTimestamp transactionId)
    {
        if (sessions.TryRemove(transactionId, out TransactionContext? context))
            context.AdmissionLease?.Dispose();
    }

    /// <summary>
    /// Reads the decision-durability policy recorded for an active session, or null when no active session
    /// with that id exists. Reflects exactly what <see cref="StartTransaction"/> captured from the caller's
    /// options, so a caller can confirm the policy was carried through Begin.
    /// </summary>
    internal DecisionDurability? GetRecordedDecisionDurability(HLCTimestamp transactionId)
        => sessions.TryGetValue(transactionId, out TransactionContext? context) ? context.DecisionDurability : null;

    /// <summary>
    /// Reads the clamped session timeout (milliseconds) recorded for an active session, or null when no
    /// active session with that id exists. Reflects the value after the <c>MaxTransactionTimeout</c> clamp
    /// applied in <see cref="StartTransaction"/>.
    /// </summary>
    internal int? GetRecordedSessionTimeout(HLCTimestamp transactionId)
        => sessions.TryGetValue(transactionId, out TransactionContext? context) ? context.Timeout : null;

    // The durable-intent finalize epoch is stable per transaction id: a retried finalize re-drives the same
    // canonical record (record CAS and intent idempotency converge), so it never mints a second record.
    private const long DurableFinalizeEpoch = 1;

    /// <summary>
    /// Consults the durable-intent canonical transaction record for a handle whose in-memory session is gone,
    /// so a lost interactive durable transaction reports its true outcome instead of <c>Errored</c>: a committed
    /// record answers <c>Committed</c>; an abort answers <c>Aborted</c> (terminal whatever its class); an undecided
    /// record answers <c>MustRetry</c> (recovery finishes it). The lookup is routed by the handle's anchor key to
    /// the anchor partition leader, so the record is found even when it lives on another node — closing the
    /// cross-node lost-session gap.
    ///
    /// <para>A routed lookup that comes back with no resident record does <b>not</b> answer Found=false: an
    /// absent record is <b>not</b> proof the transaction never committed. A durable finalize still in flight, and
    /// a commit record that has not yet propagated to the leader this read resolved to, both present as a
    /// momentary absence. Reporting a definite <c>Errored</c> for that absence told a client that a transaction
    /// which actually committed had applied nothing — observed downstream as an uncounted durable write (a
    /// conserved-total drift). So absence answers the retryable <c>MustRetry</c> instead: a finalize retry
    /// re-observes the record once it settles, and a genuinely, permanently unknown transaction surfaces through
    /// the caller's own bounded-retry exhaustion as an unresolved/indeterminate outcome — never a fabricated
    /// terminal failure. Found=false is returned only when there is no anchor to consult at all.</para>
    /// </summary>
    private async Task<(bool Found, KeyValueResponseType Response)> TryConsultDurableTransactionRecord(TransactionHandle handle)
    {
        // No anchor ⇒ the handle carries no route to any canonical record, so the outcome cannot be consulted
        // here at all. The caller decides how to report an unconsultable lost session.
        if (string.IsNullOrEmpty(handle.RecordAnchorKey))
            return (false, KeyValueResponseType.Errored);

        TransactionRecord? record;
        try
        {
            record = await manager
                .LookupDurableRecordRouted(handle.TransactionId, DurableFinalizeEpoch, handle.RecordAnchorKey, CancellationToken.None)
                .ConfigureAwait(false);
        }
        catch (PartitionNotHostedException)
        {
            // The anchor leader cannot be resolved from here right now, so the outcome cannot be
            // proven either way — answer the retryable unknown rather than a terminal Errored.
            return (true, KeyValueResponseType.MustRetry);
        }

        // No resident record on the routed anchor leader: indeterminate, not terminal. See the summary — a
        // finalize that is in flight or a not-yet-propagated commit both look like this, and a definite Errored
        // would mislabel a committed transaction as having done nothing. Retry re-observes the truth.
        if (record is null)
            return (true, KeyValueResponseType.MustRetry);

        KeyValueResponseType response = record.Decision switch
        {
            TransactionDecision.Commit => KeyValueResponseType.Committed,
            TransactionDecision.Abort => KeyValueResponseType.Aborted, // terminal whatever the class: it can never commit
            _ => KeyValueResponseType.MustRetry // still undecided → recovery completes it
        };

        return (true, response);
    }

    /// <summary>
    /// Commits the transaction identified by the supplied handle.
    /// The handle carries both the transaction ID (used to locate the session) and the
    /// coordinator key (used by the caller to route this request to the right node).
    /// </summary>
    public async Task<(KeyValueResponseType, string?)> CommitTransaction(TransactionHandle handle)
    {
        // The commit may have reached this coordinator via placement forwarding, but everything it
        // does from here — staged-base validation reads, durable prepare legs, the decision
        // proposal, settlement — is new work this node initiates against other partitions' leaders.
        // Lift the forwarded-request loop guard so that fan-out routes; without this a forwarded
        // commit wedges on MustRetry because none of its sub-operations may leave the node.
        using ForwardedRequestScope.SuppressScope _ = ForwardedRequestScope.Suppress();

        HLCTimestamp transactionId = handle.TransactionId;

        if (!sessions.TryGetValue(transactionId, out TransactionContext? context))
        {
            // The session is gone. If its outcome is still within the idempotency window, a duplicate commit
            // replays the recorded terminal answer; otherwise the durable record is consulted below, and only
            // a handle with no routable anchor is reported as the unknown Errored — never a conflict Aborted.
            if (terminalOutcomes.TryGet(transactionId, out TerminalOutcomeWindow.RetainedOutcome retained))
                return (retained.Outcome.Type, retained.Outcome.RecordAnchorKey);

            // The in-memory session is gone (evicted, restarted, or it lived on a node that failed), but an
            // all-persistent transaction's outcome survives as its canonical durable record anchored on the
            // data partition. Consult that record — routed to the anchor leader so a remote anchor is found:
            // committed → Committed; undecided → MustRetry (recovery finishes it); absent → MustRetry too,
            // because absence proves nothing (a finalize still in flight and a not-yet-propagated commit both
            // look absent). Only a handle with no routable anchor stays unknown Errored.
            (bool found, KeyValueResponseType durableRecord) = await TryConsultDurableTransactionRecord(handle);
            if (found)
                return (durableRecord, handle.RecordAnchorKey);

            logger.LogWarning("Trying to commit unknown transaction {TransactionId}", transactionId);

            return (KeyValueResponseType.Errored, null);
        }

        // Serialize against a concurrent commit, rollback, or reaper on the same session. The owner runs the
        // finalize once; any concurrent finalizer mirrors the owner's published outcome rather than racing a
        // second two-phase commit against the same working set.
        // Rejected means the reaper claimed the session and its reap is between attempts (a retrying sweep):
        // the outcome is genuinely undecided at this instant, so answer the retryable MustRetry — never a
        // fabricated definite Aborted, which a stalled commit decision could later contradict. A retry mirrors
        // the reap once it re-arms, or replays the retained outcome once the reap turns terminal.
        FinalizeAdmission admission = context.EnterFinalize(out FinalizeAttempt? attempt);
        if (admission == FinalizeAdmission.Rejected)
            return (KeyValueResponseType.MustRetry, null);

        if (admission == FinalizeAdmission.Mirror)
        {
            FinalizeOutcome mirrored = await attempt!.Completion;
            return (mirrored.Type, mirrored.RecordAnchorKey);
        }

        FinalizeOutcome outcome = new(KeyValueResponseType.Aborted, null);
        try
        {
            outcome = await ExecuteCommit(context, transactionId);

            // Any terminal outcome — a successful Committed or a definite Aborted/Errored (read conflict,
            // permanent 2PC failure) — finalizes the session: retain the outcome, then remove the session from
            // the active map. Retaining first closes the idempotency window: at every instant a duplicate
            // finalize finds either the live session (mirrors this attempt's outcome) or the retained outcome,
            // never a gap where it sees neither and reports unknown. A non-terminal MustRetry (drain timeout)
            // leaves the session live to retry.
            if (outcome.IsTerminal)
            {
                RetainTerminalOutcome(transactionId, outcome, raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId()));
                FinalizeSession(transactionId);
            }

            return (outcome.Type, outcome.RecordAnchorKey);
        }
        finally
        {
            // Always publish, even on an unexpected throw, so no mirroring waiter can hang. A terminal
            // outcome is retained for later duplicate finalizes; a MustRetry releases the slot for a retry.
            context.CompleteFinalize(attempt!, outcome);
        }
    }

    /// <summary>
    /// Runs the commit finalize for the session that already owns the finalize slot: freezes the
    /// server-owned working set, drives two-phase commit, and cleans up. Returns the outcome the owner
    /// publishes to any mirroring finalizer.
    /// </summary>
    private async Task<FinalizeOutcome> ExecuteCommit(TransactionContext context, HLCTimestamp transactionId)
    {
        // Wait for any in-flight operations to complete, so the coordinator-owned working set is frozen
        // before two-phase commit reads it. Operations that registered before finalization began are part
        // of the commit; any that arrive after are rejected. A drain timeout is a non-terminal MustRetry.
        if (!await FreezeForFinalize(context))
            return new(KeyValueResponseType.MustRetry, null);

        // The working set is now frozen: capture the immutable record anchor under the finalize fence and
        // return it from the commit itself. This is the race-free anchor source — a concurrent operation
        // can no longer assign the anchor after a pre-commit read but before the fence closes.
        string? recordAnchorKey = context.RecordAnchorKey;

        try
        {
            context.Result = new() { Type = KeyValueResponseType.Set, Reason = null };

            // Two-phase commit and the cleanup below read only the server-owned working set — every confirmed
            // effect was folded from its operation completion, so the coordinator, not the client, decides.
            // An empty read-only transaction has no modified keys: two-phase commit is a no-op and the
            // transaction commits validly. Its read MVCC snapshots are still cleaned in the finally.
            await TwoPhaseCommit(context, CancellationToken.None);

            if (context.Result is null)
                return new(KeyValueResponseType.Errored, recordAnchorKey);

            // A durable finalize that recorded its canonical decision but left some participant resolution to the
            // background reports MustRetry, not a definite outcome: the decision is durable, so recovery finishes
            // the rest. Non-terminal — the session stays live and a retry re-observes MustRetry until it completes.
            if (context.Result.Type is KeyValueResponseType.MustRetry)
                return new(KeyValueResponseType.MustRetry, recordAnchorKey);

            if (context.Result.Type is KeyValueResponseType.Aborted or KeyValueResponseType.Errored)
                return new(KeyValueResponseType.Aborted, recordAnchorKey);

            logger.LogCommittedInteractiveTransaction(transactionId);

            // The caller (CommitTransaction) removes the session and retains the outcome for every terminal
            // result uniformly, so a definite Aborted is finalized the same way a Committed is.
            return new(KeyValueResponseType.Committed, recordAnchorKey);
        }
        catch (KahunaAbortedException ex)
        {
            logger.LogKahunaAbortedException(ex);

            return new(KeyValueResponseType.Aborted, recordAnchorKey);
        }
        catch (TaskCanceledException ex)
        {
            logger.LogTaskCanceledException(ex);

            return new(KeyValueResponseType.Aborted, recordAnchorKey);
        }
        catch (OperationCanceledException ex)
        {
            logger.LogOperationCanceledException(ex);

            return new(KeyValueResponseType.Aborted, recordAnchorKey);
        }
        catch (Exception ex)
        {
            logger.LogOperationCanceledException(ex);

            return new(KeyValueResponseType.Aborted, recordAnchorKey);
        }
        finally
        {
            // Post-commit cleanup is best-effort: the mutations (if any) are already durably committed, so a
            // Committed result never depends on it. Release every confirmed lock shape not finalized by 2PC
            // and clean the transaction's read MVCC snapshots. Because no terminal promise rides on its
            // completion, AsyncRelease may run it detached without over-promising cleanup.
            //
            // EXCEPT while a durable finalize attempt is unresolved (a MustRetry with a decision proposal
            // possibly still in flight): releasing the locks then would let a conflicting writer interleave
            // ahead of a commit that can still land, so the working set is held until a retry resolves the
            // attempt or the reaper/rollback fences it through the record CAS.
            if (context.UnresolvedDurableFinalize is null)
            {
                if (context.AsyncRelease)
                    _ = ReleaseWorkingSet(context);
                else
                    await ReleaseWorkingSet(context);
            }
        }
    }

    /// <summary>
    /// Rolls back the transaction identified by the supplied handle.
    /// </summary>
    public async Task<KeyValueResponseType> RollbackTransaction(TransactionHandle handle)
    {
        // Same as CommitTransaction: a forwarded rollback's compensating work (mutation rollbacks,
        // lock releases on participant leaders) is new fan-out this node initiates, not a re-forward
        // of the rollback itself — lift the loop guard so it can route.
        using ForwardedRequestScope.SuppressScope _ = ForwardedRequestScope.Suppress();

        HLCTimestamp transactionId = handle.TransactionId;

        if (!sessions.TryGetValue(transactionId, out TransactionContext? context))
        {
            // Replay a retained terminal outcome within the idempotency window; otherwise unknown → Errored.
            if (terminalOutcomes.TryGet(transactionId, out TerminalOutcomeWindow.RetainedOutcome retained))
                return retained.Outcome.Type;

            // A rollback cannot override a durably decided commit: if the canonical transaction record exists for
            // this lost session (routed to the anchor leader), return its outcome (committed → Committed, undecided
            // → MustRetry) instead of rolling back a transaction whose commit may already be durable.
            (bool found, KeyValueResponseType durableRecord) = await TryConsultDurableTransactionRecord(handle);
            if (found)
                return durableRecord;

            logger.LogWarning("Trying to rollback unknown transaction {TransactionId}", transactionId);

            return KeyValueResponseType.Errored;
        }

        // Rollback contends for the same finalize slot as commit and the reaper, so a concurrent finalize is
        // never run twice on one session; a mirroring caller returns the owner's outcome. A Rejected admission
        // (the reaper owns the session, between attempts) answers the retryable MustRetry — the reap decides the
        // real outcome, which for a fenced durable transaction can even be Committed.
        FinalizeAdmission admission = context.EnterFinalize(out FinalizeAttempt? attempt);
        if (admission == FinalizeAdmission.Rejected)
            return KeyValueResponseType.MustRetry;

        if (admission == FinalizeAdmission.Mirror)
        {
            FinalizeOutcome mirrored = await attempt!.Completion;
            return mirrored.Type;
        }

        FinalizeOutcome outcome = new(KeyValueResponseType.Aborted, null);
        try
        {
            outcome = await ExecuteRollback(context, transactionId);

            // A terminal RolledBack finalizes the session: retain the outcome, then remove the session (retain
            // first so a duplicate rollback never sees a gap where neither the session nor the record exists).
            // A non-terminal MustRetry (drain timeout or incomplete mandatory release) leaves the session live
            // so a later call — or the reaper, if the caller disappears — retries the cleanup.
            if (outcome.IsTerminal)
            {
                RetainTerminalOutcome(transactionId, outcome, raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId()));
                FinalizeSession(transactionId);
            }

            return outcome.Type;
        }
        finally
        {
            context.CompleteFinalize(attempt!, outcome);
        }
    }

    /// <summary>
    /// Runs the rollback finalize for the session that already owns the finalize slot: freezes the
    /// server-owned working set, marks the transaction for abort, and cleans up every confirmed effect.
    /// </summary>
    private async Task<FinalizeOutcome> ExecuteRollback(TransactionContext context, HLCTimestamp transactionId)
    {
        // Freeze the coordinator-owned working set before rolling back its operations.
        if (!await FreezeForFinalize(context))
            return new(KeyValueResponseType.MustRetry, null);

        context.Action = KeyValueTransactionAction.Abort;

        // A prior commit attempt on this session may have left a durable decision proposal in flight (its
        // replication timed out — indeterminate, not failed). Before this rollback may claim RolledBack, install
        // a durable Abort through the record CAS: if the stalled commit already won, the transaction is committed
        // and that is the only truthful answer; if the abort wins, any late commit is permanently fenced out.
        DurableFinalizeInput? unresolved = context.UnresolvedDurableFinalize;
        if (unresolved is not null)
        {
            DurableFinalizeOutcome fenced = await DurableFinalizer.FenceAbandonedAsync(
                unresolved, raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId()), CancellationToken.None);

            if (fenced.Result == DurableFinalizeResult.MustRetry)
                return new(KeyValueResponseType.MustRetry, null);

            context.UnresolvedDurableFinalize = null;

            if (fenced.Result == DurableFinalizeResult.Committed)
            {
                await ReleaseWorkingSet(context);
                return new(KeyValueResponseType.Committed, context.RecordAnchorKey);
            }
            // Durable Abort installed: the rollback below is safe to report.
        }

        // Rollback runs no prepare: it clears the transaction's staged (un-prepared) writes, releases every
        // confirmed lock shape, and cleans its read MVCC — all from the server-owned working set. Cleanup is
        // synchronous and RolledBack is reported only when every mandatory (intent-bearing) release
        // acknowledged; otherwise the caller retries (the released finalize slot lets a later call re-run).
        // RolledBack must never promise cleanup that did not complete, so it is never dispatched via
        // AsyncRelease.
        if (!await ReleaseWorkingSet(context))
            return new(KeyValueResponseType.MustRetry, null);

        logger.LogRolledBackInteractiveTransaction(transactionId);

        // The caller (RollbackTransaction) removes the session and retains the outcome for the terminal
        // RolledBack result.
        return new(KeyValueResponseType.RolledBack, null);
    }

    // ---- operation registry façade ----

    /// <summary>
    /// Registers an operation under the session identified by <paramref name="transactionId"/> for
    /// idempotent tracking. Returns <see cref="OperationRegistrationResult"/> describing whether
    /// the operation is new, already in flight, or already completed with a cached response.
    /// </summary>
    public OperationRegistrationResult BeginOperation(
        HLCTimestamp transactionId,
        TransactionOperationId operationId,
        OperationKind kind,
        byte[]? payloadDigest)
    {
        if (!sessions.TryGetValue(transactionId, out TransactionContext? context))
            return new(OperationRegistrationOutcome.RejectedSessionClosed);

        return context.BeginOperation(operationId, kind, payloadDigest);
    }

    /// <summary>
    /// Marks the operation as completed and stores the response so that duplicate submissions
    /// can receive the same answer without re-executing the operation.
    /// </summary>
    /// <summary>
    /// Folds the confirmed effect into the coordinator working set and returns the transaction's record
    /// anchor (null if no persistent write yet). Throws when the session no longer exists — the effect
    /// cannot be folded, so this completion is not acknowledged and the caller must not report success.
    /// </summary>
    public string? CompleteOperation(HLCTimestamp transactionId, TransactionOperationId operationId, OperationCompletionPayload? payload, object? response)
    {
        if (sessions.TryGetValue(transactionId, out TransactionContext? context))
        {
            string? anchor = context.CompleteOperation(operationId, payload, response, out bool discardedEffects);

            // A completion carrying confirmed effects whose registration no longer exists: the participant
            // applied a mutation this transaction can never own (a transient result cancelled the
            // registration while the apply was still in flight). The fold is gone for good — the session
            // could finalize without it — so say it loudly; the metric pairs with this line for attribution.
            if (discardedEffects)
                logger.LogError(
                    "Discarding effect-bearing completion of operation {OperationId} for transaction {TransactionId}: its registration was cancelled while the participant apply was in flight",
                    operationId, transactionId);

            return anchor;
        }

        // The session is gone (reaped/aborted, or never existed). Signalling rather than returning a null
        // anchor — which reads as success — keeps a participant that already applied the operation from
        // claiming a write that never entered the coordinator working set; it surfaces MustRetry and a
        // retry then observes the closed/absent session and aborts.
        throw new KahunaServerException($"No transaction session {transactionId} to complete operation {operationId}.");
    }

    /// <summary>
    /// Cancels the specified operation if it is still in the pending state.
    /// </summary>
    public bool CancelOperation(HLCTimestamp transactionId, TransactionOperationId operationId)
    {
        if (!sessions.TryGetValue(transactionId, out TransactionContext? context))
            return false;

        return context.TryCancelOperation(operationId);
    }

    // ---- working-set query and close snapshot ----

    /// <summary>
    /// Returns a snapshot of the working set for the specified transaction, or null if the
    /// session does not exist.
    /// </summary>
    public WorkingSetSnapshot? GetTransactionWorkingSet(HLCTimestamp transactionId)
    {
        if (!sessions.TryGetValue(transactionId, out TransactionContext? context))
            return null;

        return context.GetWorkingSetSnapshot();
    }

    /// <summary>
    /// Closes a session to new operations and waits for in-flight ones to drain, so a subsequent
    /// two-phase commit / rollback reads a stable coordinator-owned working set. Returns false when the
    /// drain deadline elapses (the session stays closed so the caller retries and rejoins the same drain).
    /// </summary>
    private async Task<bool> FreezeForFinalize(TransactionContext context)
    {
        // The caller already owns the finalize slot, which closed the session to new operations on install
        // (Accepting→Finalizing) — no operation can join once we reach here. Just wait for the in-flight
        // ones to drain; a retry after a prior drain timeout rejoins this same still-closed drain.
        using CancellationTokenSource cts = new(context.Timeout <= 0 ? configuration.DefaultTransactionTimeout : context.Timeout);
        try
        {
            await context.WaitForPendingOperations(cts.Token);
            return true;
        }
        catch (OperationCanceledException)
        {
            // Drain deadline hit. Keep the session closed — do NOT reopen it to new operations — so no
            // operation can join after finalization began; the caller retries and rejoins this same drain.
            return false;
        }
    }

    /// <summary>
    /// Closes a session to new operations, drains in-flight ones, and captures the immutable working-set
    /// snapshot that a later commit or rollback finalizes against (CamusDB calls Close to publish cache
    /// keyspaces before committing). Close shares the single finalize slot with commit/rollback so a Close and
    /// a finalize never run over the same transaction concurrently, but — unlike commit/rollback — Close does
    /// <b>not</b> decide the transaction: it stores the snapshot, leaves the session
    /// <see cref="SessionLifecycle.Finalizing"/> (still finalizable, never terminal), and releases the slot so
    /// a subsequent commit/rollback owns it and finalizes the frozen working set. A repeat Close returns the
    /// same stored snapshot.
    /// </summary>
    public async Task<(KeyValueResponseType, WorkingSetSnapshot?)> CloseTransaction(
        HLCTimestamp transactionId,
        CancellationToken cancellationToken)
    {
        if (!sessions.TryGetValue(transactionId, out TransactionContext? context))
            return (KeyValueResponseType.Errored, null);

        // Already closed and snapshotted: return the same frozen snapshot without re-capturing.
        WorkingSetSnapshot? stored = context.CloseSnapshot;
        if (stored is not null)
            return (KeyValueResponseType.Set, stored);

        // Contend for the single finalize slot so Close and a commit/rollback never touch the working set at
        // once. Rejected means the reaper (or a terminal state) owns the session — nothing to close.
        FinalizeAdmission admission = context.EnterFinalize(out FinalizeAttempt? attempt);
        if (admission == FinalizeAdmission.Rejected)
            return context.CloseSnapshot is { } published
                ? (KeyValueResponseType.Set, published)
                : (KeyValueResponseType.MustRetry, null);

        if (admission == FinalizeAdmission.Mirror)
        {
            // Another finalize owns the slot. Wait for it: if it was a concurrent Close, its stored snapshot is
            // now available; if it was a deciding commit/rollback, there is no snapshot and the caller retries.
            await attempt!.Completion;
            return context.CloseSnapshot is { } published
                ? (KeyValueResponseType.Set, published)
                : (KeyValueResponseType.MustRetry, null);
        }

        // Owner: drain, snapshot, store, then release the slot with a non-terminal outcome so the session
        // stays Finalizing (closed to new operations, still finalizable) and a later commit/rollback owns it.
        try
        {
            using CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            cts.CancelAfter(context.Timeout <= 0 ? configuration.DefaultTransactionTimeout : context.Timeout);

            try
            {
                await context.WaitForPendingOperations(cts.Token);
            }
            catch (OperationCanceledException)
            {
                // Drain deadline hit. Release the slot (MustRetry keeps the session Finalizing and closed) so a
                // later Close rejoins the same drain; do not snapshot a set that has not finished draining.
                context.CompleteFinalize(attempt!, new FinalizeOutcome(KeyValueResponseType.MustRetry, null));
                return (KeyValueResponseType.MustRetry, null);
            }

            WorkingSetSnapshot snapshot = context.GetWorkingSetSnapshot();
            context.StoreCloseSnapshot(snapshot);

            // Release the slot without deciding the transaction. The non-terminal outcome frees the slot and
            // bounces any commit/rollback that mirrored this Close with MustRetry so it retries and then owns
            // the finalize itself; a mirroring Close instead observes the now-stored snapshot.
            context.CompleteFinalize(attempt!, new FinalizeOutcome(KeyValueResponseType.MustRetry, null));

            return (KeyValueResponseType.Set, context.CloseSnapshot ?? snapshot);
        }
        catch
        {
            context.CompleteFinalize(attempt!, new FinalizeOutcome(KeyValueResponseType.MustRetry, null));
            throw;
        }
    }

    // ---- terminal outcome retention (idempotency window) ----

    /// <summary>
    /// Records a finalized outcome so a duplicate finalize arriving after the session is removed replays the
    /// same answer. Only terminal outcomes are retained — a non-terminal MustRetry leaves the session live, so
    /// there is nothing to replay. <c>TransactionOutcomeRetentionMax</c> is a <b>strict</b> upper bound: the
    /// window evicts oldest-retained-first before the insert is observable above the cap (see
    /// <see cref="TerminalOutcomeWindow.Retain"/>). A non-positive max <b>disables retention entirely</b> —
    /// nothing is retained, so a duplicate after removal reports an unknown
    /// <see cref="KeyValueResponseType.Errored"/> (never a conflict Aborted).
    /// </summary>
    private void RetainTerminalOutcome(HLCTimestamp transactionId, FinalizeOutcome outcome, HLCTimestamp now)
    {
        if (!outcome.IsTerminal)
            return;

        int max = configuration.TransactionOutcomeRetentionMax;
        if (max <= 0)
            return;

        terminalOutcomes.Retain(transactionId, outcome, now, max);
    }

    /// <summary>
    /// Prunes retained outcomes older than the configured idempotency window. Called on each reaper sweep so
    /// the window is bounded by age in addition to size. A non-positive TTL disables age pruning (size alone
    /// bounds the window). Serialized against retention inserts/evictions inside
    /// <see cref="TerminalOutcomeWindow.PruneExpired"/>.
    /// </summary>
    private void PruneRetainedOutcomes(HLCTimestamp now)
    {
        TimeSpan ttl = configuration.TransactionOutcomeRetentionTtl;
        if (ttl <= TimeSpan.Zero || terminalOutcomes.IsEmpty)
            return;

        terminalOutcomes.PruneExpired(now, ttl);
    }

    // ---- session range-lock renewal ----

    /// <summary>
    /// Server-owned lease, in milliseconds, granted to a live session's range locks on each renewal tick.
    /// Set to twice the renewal cadence (the reaper's <see cref="KahunaConfiguration.CollectionInterval"/>) so a
    /// single missed tick cannot let a lock lapse while its session is alive. This is the server policy that
    /// replaces client heartbeats: the coordinator, not the client, owns range-lock liveness for the session's
    /// lifetime. A client acquires a range lock once with any TTL and never touches it again.
    /// </summary>
    private int RangeLockRenewalTtlMs => (int)Math.Max(configuration.CollectionInterval.TotalMilliseconds * 2, 1);

    /// <summary>
    /// Re-extends the range locks held by every live session so they outlive their original acquire TTL without
    /// any client heartbeat. For each session not yet past its reap deadline, re-acquires each recorded range
    /// lock over its identical bounds/mode with the server renewal TTL — the participant treats a
    /// same-transaction, same-bounds re-acquire as a renewal that refreshes the expiry, not a second lock.
    /// A session past <c>Timeout + grace</c> is skipped so its locks lapse and the reaper reclaims it.
    /// A session whose cleanup has started (<see cref="TransactionContext.MarkRenewalExcluded"/> called)
    /// contributes an empty snapshot. A renewal whose snapshot was taken just before that flip can still
    /// land its re-acquire after cleanup released the lock, briefly resurrecting it; that hold is bounded
    /// by one renewal TTL (nothing renews it again) and errs toward over-protecting the range — the safe
    /// direction, never an early lapse.
    /// The sweep runs under a self-imposed deadline (<c>RangeLockRenewalTtlMs / 2</c>) with bounded
    /// concurrency, and the deadline token is passed into each acquire so a slow or stuck participant is
    /// cancelled at the budget rather than blocking the reaper or overflowing the next tick.
    /// </summary>
    public async Task RenewSessionRangeLocks()
    {
        if (sessions.IsEmpty)
            return;

        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());
        int renewalTtlMs = RangeLockRenewalTtlMs;

        // Flatten (context, range, mode) tuples for all sessions not past their reap deadline.
        List<(TransactionContext Context, RangeLockKey Range, RangeLockMode Mode)> workList = [];

        foreach (KeyValuePair<HLCTimestamp, TransactionContext> pair in sessions)
        {
            TransactionContext context = pair.Value;

            // Past its reap deadline: leave it to the reaper. Renewing a doomed session would only extend
            // locks the reaper is about to release, so stop renewing once the abandonment window has elapsed.
            HLCTimestamp deadline = pair.Key + (context.Timeout + ReapGraceMs);
            if ((deadline - now) <= TimeSpan.Zero)
                continue;

            // The snapshot is empty once MarkRenewalExcluded has been called (cleanup owns the locks).
            // A Finalizing session whose drain is still in progress still returns its locks — renewal
            // continues through the drain so the predicate lock does not lapse mid-finalize.
            List<(RangeLockKey Range, RangeLockMode Mode)> renewable = context.SnapshotRenewableRangeLocks();
            foreach ((RangeLockKey range, RangeLockMode mode) in renewable)
                workList.Add((context, range, mode));
        }

        if (workList.Count == 0)
            return;

        // Self-imposed sweep deadline: half the renewal TTL. A lock last renewed at the start of the prior
        // tick has ~half the TTL remaining; renewing it within this budget keeps it alive. Locks that do not
        // finish within the budget are abandoned this tick (they lapse only if already near expiry); the next
        // tick retries. Abandoned count is logged so silent truncation is visible.
        int sweepDeadlineMs = Math.Max(1, renewalTtlMs / 2);
        int concurrencyLimit = Math.Min(16, Environment.ProcessorCount);

        using CancellationTokenSource sweepCts = new(sweepDeadlineMs);
        CancellationToken sweepToken = sweepCts.Token;
        using SemaphoreSlim concurrencyGate = new(concurrencyLimit, concurrencyLimit);
        int[] abandoned = [0];

        async Task RenewOne(TransactionContext ctx, RangeLockKey range, RangeLockMode mode)
        {
            bool acquired = false;
            try
            {
                await concurrencyGate.WaitAsync(sweepToken).ConfigureAwait(false);
                acquired = true;

                // Pass the sweep deadline token into the acquire so a slow/stuck participant is cancelled at
                // the budget instead of blocking Task.WhenAll (and therefore the reaper) indefinitely.
                await manager.LocateAndTryAcquireRangeLock(
                    ctx.TransactionId, 
                    range.Prefix, 
                    range.StartKey, 
                    range.StartInclusive,
                    range.EndKey, 
                    range.EndInclusive, 
                    renewalTtlMs, 
                    range.Durability, 
                    mode, 
                    sweepToken
                );
            }
            catch (OperationCanceledException)
            {
                // Deadline elapsed while queued or mid-acquire: this lock is abandoned for this tick and
                // retried on the next one (it only lapses if it was already near expiry).
                Interlocked.Increment(ref abandoned[0]);
            }
            catch (Exception ex)
            {
                logger.LogError("RenewSessionRangeLocks {Prefix}: {Type} {Message}", range.Prefix, ex.GetType().Name, ex.Message);
            }
            finally
            {
                if (acquired)
                    concurrencyGate.Release();
            }
        }

        List<Task> tasks = new(workList.Count);
        foreach ((TransactionContext context, RangeLockKey range, RangeLockMode mode) in workList)
            tasks.Add(RenewOne(context, range, mode));

        await Task.WhenAll(tasks);

        int totalAbandoned = Volatile.Read(ref abandoned[0]);
        if (totalAbandoned > 0)
            logger.LogWarning("RenewSessionRangeLocks: {Abandoned}/{Total} locks abandoned to sweep budget", totalAbandoned, workList.Count);
    }

    // ---- session reaper ----

    /// <summary>
    /// Reclaims interactive sessions that were started but never committed or rolled back —
    /// e.g. the client crashed, timed out, or dropped its connection.
    /// </summary>
    public async Task ReapAbandonedSessions()
    {
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        // Bound the idempotency window by age on the same sweep, independent of whether any session is reaped.
        PruneRetainedOutcomes(now);

        if (sessions.IsEmpty)
            return;

        foreach (KeyValuePair<HLCTimestamp, TransactionContext> pair in sessions)
        {
            TransactionContext context = pair.Value;

            if (context.State is KeyValueTransactionState.Preparing
                or KeyValueTransactionState.Committing
                or KeyValueTransactionState.RollingBack)
                continue;

            // A session already claimed by a prior reap whose cleanup could not fully release retries every
            // sweep — regardless of the deadline — until every mandatory release acknowledges. Resume it with
            // a fresh attempt; a null resume means the slot is still held by an in-flight reap, so skip.
            if (context.Lifecycle == SessionLifecycle.Reaping)
            {
                FinalizeAttempt? resume = context.TryResumeReap();
                if (resume is not null)
                    await ReapSession(pair.Key, context, resume, now);

                continue;
            }

            // A session with unresolved dispatched operations is not abandoned until the extended deadline —
            // base timeout + grace + the maximum participant effect TTL — elapses, so the reaper never cancels
            // an operation that could still land an effect at a participant.
            int window = context.Timeout + ReapGraceMs;
            if (context.HasPendingOperations)
                window += MaxParticipantEffectTtlMs;

            HLCTimestamp deadline = pair.Key + window;

            if ((deadline - now) > TimeSpan.Zero)
                continue;

            // Claim the finalize slot and close the session to new operations before removing it from the
            // map. This reclaims both a still-accepting abandoned session and an abandoned finalization — one
            // left Finalizing (closed to new ops) after a commit/rollback/Close returned MustRetry or stored a
            // Close snapshot and the caller then disappeared, so no finalize owner remains to decide it. A
            // BeginOperation that already captured this context observes the closed lifecycle and is rejected
            // rather than registering on a vanishing session. TryEnterReap returns null when a commit or
            // rollback still owns the slot (that finalize, not the reaper, decides the outcome).
            FinalizeAttempt? attempt = context.TryEnterReap();
            if (attempt is null)
                continue;

            await ReapSession(pair.Key, context, attempt, now);
        }
    }

    /// <summary>
    /// Reclaims one abandoned session that this reaper tick owns the finalize slot for. Releases its confirmed
    /// working set and decides the outcome published to any racing finalizer:
    /// <list type="bullet">
    /// <item>every mandatory release acknowledged and no operation still pending → terminal
    /// <see cref="KeyValueResponseType.RolledBack"/>; the session is removed and its outcome retained.</item>
    /// <item>an operation is still unresolved after the extended deadline → the session is <b>expired</b>
    /// without reporting RolledBack: it is removed and the outcome retained as an unknown
    /// <see cref="KeyValueResponseType.Errored"/>. The dispatched operation is never cancelled; if its effect
    /// lands later it finds no session, is not acknowledged, and expires at the participant on its intent
    /// lease.</item>
    /// <item>a mandatory release could not complete (transient) → non-terminal
    /// <see cref="KeyValueResponseType.MustRetry"/>: the session stays <see cref="SessionLifecycle.Reaping"/>
    /// and a later sweep retries the cleanup.</item>
    /// </list>
    /// </summary>
    private async Task ReapSession(HLCTimestamp transactionId, TransactionContext context, FinalizeAttempt attempt, HLCTimestamp now)
    {
        FinalizeOutcome outcome = new(KeyValueResponseType.MustRetry, null);
        try
        {
            context.Action = KeyValueTransactionAction.Abort;

            logger.LogWarning("Reaping abandoned interactive transaction {TransactionId}", transactionId);

            // An abandoned durable finalize may have a decision proposal still in flight — a replication timeout
            // is indeterminate, and a stalled [record init + prepare + commit] bundle can apply after a partition
            // heals. Before this reap may claim RolledBack, install a durable Abort through the record CAS and
            // take whatever the record answers: an abort (including a tombstone from absence) permanently rejects
            // any late commit, while a commit that already won means this transaction COMMITTED and the reap must
            // report that, not fabricate a rollback the record contradicts.
            DurableFinalizeInput? unresolved = context.UnresolvedDurableFinalize;
            if (unresolved is not null)
            {
                DurableFinalizeOutcome fenced = await DurableFinalizer.FenceAbandonedAsync(unresolved, now, CancellationToken.None);

                if (fenced.Result == DurableFinalizeResult.MustRetry)
                    // The fence itself could not be installed (replication unavailable): the outcome is still
                    // indeterminate. Do not release the working set — that would widen the window in which a
                    // stalled commit could interleave with new writers — and retry on a later sweep.
                    return;

                context.UnresolvedDurableFinalize = null;

                if (fenced.Result == DurableFinalizeResult.Committed)
                {
                    // The stalled decision won: the transaction is durably committed and its resolution has run.
                    // Release the remaining working set (idempotent for keys the commit apply already cleaned)
                    // and retain the truthful outcome so a late duplicate commit/rollback replays Committed.
                    await ReleaseWorkingSet(context);
                    outcome = new(KeyValueResponseType.Committed, context.RecordAnchorKey);
                    RetainTerminalOutcome(transactionId, outcome, now);
                    FinalizeSession(transactionId);
                    return;
                }
                // A durable Abort is installed: any late commit is fenced out, so the rollback below is safe.
            }

            bool cleaned = await ReleaseWorkingSet(context);

            if (context.HasPendingOperations)
                // Unresolved dispatched operation(s) after the extended deadline: give up on this session
                // without ever reporting a rollback of work whose fate we do not know. Late effects get no
                // acknowledgement and lapse at their participant.
                outcome = new(KeyValueResponseType.Errored, null);
            else if (cleaned)
                outcome = new(KeyValueResponseType.RolledBack, null);
            // else: cleanup incomplete — keep the session Reaping and retry on a later sweep (MustRetry).

            // Retain the terminal outcome before removing the session so a duplicate finalize never observes a
            // gap where neither the session nor the record exists. A non-terminal MustRetry retains nothing and
            // leaves the session in the map for the next sweep.
            if (outcome.IsTerminal)
            {
                RetainTerminalOutcome(transactionId, outcome, now);
                FinalizeSession(transactionId);
            }
        }
        catch (Exception ex)
        {
            logger.LogError("ReapSession {TransactionId}: {Type} {Message}", transactionId, ex.GetType().Name, ex.Message);
            // Leave the session Reaping so a later sweep retries.
            outcome = new(KeyValueResponseType.MustRetry, null);
        }
        finally
        {
            // Publish so a commit/rollback that raced in and mirrored this attempt learns the outcome.
            context.CompleteFinalize(attempt, outcome);
        }
    }

    /// <summary>
    /// Releases every confirmed lock shape held by the transaction and cleans its per-key MVCC state from the
    /// coordinator-owned working set. Called from commit, rollback, the reaper, and the script executor.
    /// </summary>
    /// <remarks>
    /// The ticket-free exclusive-lock release removes the transaction's MVCC entry <b>and</b> clears any write
    /// intent it owns, so one per-key call cleans a point lock, a staged (un-prepared) write, and a read
    /// snapshot alike. Point locks and staged writes are <b>mandatory</b> cleanup — both carry a write intent
    /// that blocks other writers until cleared; read-only MVCC snapshots are <b>best-effort</b> (no intent;
    /// lazily trimmed). On a committed transaction the modified keys were already finalized by two-phase
    /// commit, so their locks are not released a second time. Every cleanup item is attempted even after an
    /// individual failure. Returns true when every mandatory release acknowledged — the caller uses this to
    /// decide whether a rollback may report <c>RolledBack</c> or must retry.
    /// </remarks>
    internal async Task<bool> ReleaseWorkingSet(TransactionContext context)
    {
        // Exclude from renewal before releasing any lock: from this point cleanup owns the range locks,
        // and no renewal snapshot will include this session. This closes the gap where renewal has stopped
        // (lifecycle fence) but cleanup has not yet run, which would leave the predicate lock unprotected.
        context.MarkRenewalExcluded();

        bool committed = context.State == KeyValueTransactionState.Committed;
        bool allMandatoryAcked = true;

        // Intent-bearing per-key cleanup: point locks and, when not committed, staged (un-prepared) writes.
        HashSet<(string, KeyValueDurability)> mandatoryKeys = [];

        if (context.LocksAcquired is not null)
        {
            foreach ((string, KeyValueDurability) lockKey in context.LocksAcquired)
            {
                // A committed modified key was already unlocked by the commit phase; don't release it twice.
                if (committed && context.ModifiedKeys is not null && context.ModifiedKeys.Contains(lockKey))
                    continue;

                mandatoryKeys.Add(lockKey);
            }
        }

        // Clean staged (un-prepared) writes only when no prepare ran on this transaction. When the state is
        // still Pending, every modified key holds only the write intent placed at write time — no proposal
        // ticket exists, so the ticket-free release is the correct and only cleanup. Once a prepare has run
        // (Preparing/Prepared/Committing/RollingBack/RolledBack) two-phase commit owns those keys' proposals
        // and cleans them via their tickets; re-releasing them here could clear an intent out from under an
        // in-flight rollback, so leave them to 2PC.
        if (context.State == KeyValueTransactionState.Pending && context.ModifiedKeys is not null)
        {
            foreach ((string, KeyValueDurability) modified in context.ModifiedKeys)
                mandatoryKeys.Add(modified);
        }

        // Best-effort read MVCC cleanup for keys not already covered by a mandatory release or a committed
        // mutation (which the commit phase already cleaned).
        HashSet<(string, KeyValueDurability)> readKeys = [];

        if (context.ReadKeys is not null)
        {
            foreach ((string, KeyValueDurability) readKey in context.ReadKeys.Keys)
            {
                if (mandatoryKeys.Contains(readKey))
                    continue;

                if (committed && context.ModifiedKeys is not null && context.ModifiedKeys.Contains(readKey))
                    continue;

                readKeys.Add(readKey);
            }
        }

        // One batched round-trip cleans every per-key entry; the release handler is idempotent, so a key
        // already cleaned is a harmless no-op.
        List<(string, KeyValueDurability)> perKey = [.. mandatoryKeys, .. readKeys];

        if (perKey.Count > 0)
        {
            try
            {
                if (perKey.Count == 1)
                {
                    (string key, KeyValueDurability durability) = perKey[0];

                    (KeyValueResponseType type, string _) = await manager.LocateAndTryReleaseExclusiveLock(context.TransactionId, key, durability, CancellationToken.None);

                    if (mandatoryKeys.Contains((key, durability)) && !IsReleaseAcked(type))
                        allMandatoryAcked = false;
                }
                else
                {
                    List<(KeyValueResponseType, string, KeyValueDurability)> results =
                        await manager.LocateAndTryReleaseManyExclusiveLocks(context.TransactionId, perKey, CancellationToken.None);

                    // Index the responses by key so every mandatory key can be verified against the batch. A
                    // mandatory key that is absent from the results — or present with a non-proof response — is
                    // not acknowledged: the batch must positively account for the full mandatory set.
                    Dictionary<(string, KeyValueDurability), KeyValueResponseType> releasedByKey = new(results.Count);
                    foreach ((KeyValueResponseType type, string key, KeyValueDurability durability) in results)
                        releasedByKey[(key, durability)] = type;

                    foreach ((string, KeyValueDurability) mandatory in mandatoryKeys)
                    {
                        if (!releasedByKey.TryGetValue(mandatory, out KeyValueResponseType type) || !IsReleaseAcked(type))
                            allMandatoryAcked = false;
                    }
                }
            }
            catch (Exception ex)
            {
                logger.LogError("ReleaseWorkingSet keys: {Type} {Message}\n{StackTrace}", ex.GetType().Name, ex.Message, ex.StackTrace);
                allMandatoryAcked = false;
            }
        }

        // Prefix locks (mandatory).
        if (context.PrefixLocksAcquired is not null)
        {
            foreach ((string prefixKey, KeyValueDurability durability) in context.PrefixLocksAcquired)
            {
                try
                {
                    KeyValueResponseType type = await manager.LocateAndTryReleaseExclusivePrefixLock(context.TransactionId, prefixKey, durability, CancellationToken.None);
                    if (!IsReleaseAcked(type))
                        allMandatoryAcked = false;
                }
                catch (Exception ex)
                {
                    logger.LogError("ReleaseWorkingSet prefix {Prefix}: {Type} {Message}", prefixKey, ex.GetType().Name, ex.Message);
                    allMandatoryAcked = false;
                }
            }
        }

        // Range locks (mandatory).
        if (context.RangeLocksAcquired is not null)
        {
            foreach (RangeLockKey range in context.RangeLocksAcquired.Keys)
            {
                try
                {
                    KeyValueResponseType type = await manager.LocateAndTryReleaseExclusiveRangeLock(
                        context.TransactionId, range.Prefix, range.StartKey, range.StartInclusive,
                        range.EndKey, range.EndInclusive, range.Durability, CancellationToken.None);
                    if (!IsReleaseAcked(type))
                        allMandatoryAcked = false;
                }
                catch (Exception ex)
                {
                    logger.LogError("ReleaseWorkingSet range {Prefix}: {Type} {Message}", range.Prefix, ex.GetType().Name, ex.Message);
                    allMandatoryAcked = false;
                }
            }
        }

        return allMandatoryAcked;
    }

    /// <summary>
    /// Whether a lock/MVCC release response is positive <b>proof</b> that this transaction's effect on the key
    /// is gone. Only an explicit allowlist qualifies, because <c>RolledBack</c> must never be reported while an
    /// intent may remain:
    /// <list type="bullet">
    /// <item><c>Unlocked</c> — the handler removed this transaction's MVCC entry and cleared its write intent.</item>
    /// <item><c>DoesNotExist</c> — the entry is absent, so no effect of this transaction can be on it.</item>
    /// <item><c>AlreadyLocked</c> — the entry's current write intent belongs to <em>another</em> transaction, so
    /// this transaction's intent is not present and its MVCC entry was removed; not treating this as proof would
    /// livelock the rollback against a lock it does not own.</item>
    /// </list>
    /// Every other response — <c>MustRetry</c>/<c>WaitingForReplication</c> (transient), <c>Errored</c>/
    /// <c>InvalidInput</c> (the release did not run), or a missing batch result — is <b>not</b> proof and forces
    /// the rollback to retry rather than falsely claiming the effect released.
    /// </summary>
    internal static bool IsReleaseAcked(KeyValueResponseType type) =>
        type is KeyValueResponseType.Unlocked
             or KeyValueResponseType.DoesNotExist
             or KeyValueResponseType.AlreadyLocked;

    /// <summary>
    /// Executes the 2PC protocol for the transaction.
    /// </summary>
    internal async Task TwoPhaseCommit(TransactionContext context, CancellationToken cancellationToken)
    {
        if (context.ModifiedKeys is null || context.ModifiedKeys.Count == 0)
        {
            // A transaction with no writes still has to prove its reads form one consistent cut of committed
            // state: each read was served at a different moment against the latest committed version, so a
            // concurrent commit landing between two of them yields a snapshot no serial order can explain — an
            // anti-dependency cycle closed by this very transaction. The concurrent-writer probe runs first
            // (it answers immediately for an undecided foreign intent, and a writer that validated its reads
            // but has not yet decided is invisible to the revision check); the revision re-check then catches
            // any read a decided commit has already made stale. Failure sets the Aborted result.
            if (RequiresReadSetValidation(context) && (context.ReadObservationConflict || context.ReadKeys is { Count: > 0 }))
            {
                if (!await CheckCommitConflicts(context, cancellationToken))
                    return;

                await ValidateReadSet(context, cancellationToken);
            }

            return;
        }

        // A transaction that modified keys but holds no lock set cannot prepare its mutations. Committing
        // it as an empty no-op would silently drop the writes (the caller would see Committed), so the
        // only honest outcome is a retryable abort.
        if (context.LocksAcquired is null)
        {
            context.Result = new() { Type = KeyValueResponseType.Aborted, Reason = "Transaction modified keys but holds no lock set" };
            return;
        }

        // A Durable-mode transaction promises crash-atomicity, which an ephemeral (in-memory) mutation cannot
        // provide — so a Durable transaction that modified any ephemeral key is rejected outright, before the
        // split below (which would otherwise commit those ephemeral keys in memory keyed on the persistent
        // decision — correct only for the default BestEffort mode). A Durable transaction must also have an anchor.
        if (context.DecisionDurability == DecisionDurability.Durable)
        {
            if (context.ModifiedKeys.Any(static k => k.Item2 != KeyValueDurability.Persistent))
            {
                context.Result = new() { Type = KeyValueResponseType.Aborted, Reason = "A durable transaction cannot modify an ephemeral key" };
                return;
            }

            if (context.RecordAnchorKey is null)
            {
                context.Result = new() { Type = KeyValueResponseType.Aborted, Reason = "A durable transaction with modifications must have a record anchor" };
                return;
            }
        }

        // Split the working set: the persistent (crash-atomic) keys are finalized through the durable-intent path
        // (canonical decision record + prepared intents, no manual Raft ticket); the ephemeral keys are committed
        // or rolled back in memory keyed on that decision, never touching CommitLogs. An all-persistent transaction
        // takes the durable path alone; an all-ephemeral one has no persistent subset and falls through to the
        // (ticket-free, in-memory) ephemeral path below.
        List<(string Key, KeyValueDurability Durability)> persistentKeys = [];
        List<(string Key, KeyValueDurability Durability)> ephemeralKeys = [];
        foreach ((string, KeyValueDurability) modified in context.ModifiedKeys)
            (modified.Item2 == KeyValueDurability.Persistent ? persistentKeys : ephemeralKeys).Add(modified);

        if (TryBuildDurableFinalizeInput(context, persistentKeys, out DurableFinalizeInput? durableInput, out HLCTimestamp durableOpId))
        {
            if (ephemeralKeys.Count == 0)
            {
                await DurableFinalize(context, durableInput!, durableOpId, cancellationToken).ConfigureAwait(false);
                return;
            }

            // Mixed transaction. Prepare the ephemeral subset first, so an ephemeral prepare failure aborts the
            // whole transaction before anything persistent is decided — preserving all-or-nothing on the abort
            // side. Then finalize the persistent subset durably and let its decision drive the ephemeral commit.
            (bool ephemeralOk, List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)>? ephemeralMutations) =
                await PrepareMutations(context, cancellationToken, ephemeralKeys).ConfigureAwait(false);

            if (ephemeralMutations is null)
                return;

            if (!ephemeralOk)
            {
                if (context.AsyncRelease)
                    _ = RollbackMutations(context, ephemeralMutations, CancellationToken.None);
                else
                    await RollbackMutations(context, ephemeralMutations, cancellationToken).ConfigureAwait(false);
                return; // context.Result already set to Aborted by PrepareMutations
            }

            DurableFinalizeResult durableResult = await DurableFinalize(context, durableInput!, durableOpId, cancellationToken).ConfigureAwait(false);

            // The durable decision is the transaction's outcome. On a durable commit, apply the ephemeral writes in
            // memory (near-always succeeds; the persistent commit is already durable and truthful regardless); on a
            // conflict abort or retryable result, discard them.
            if (durableResult == DurableFinalizeResult.Committed)
                await CommitMutations(context, ephemeralMutations, cancellationToken).ConfigureAwait(false);
            else if (context.AsyncRelease)
                _ = RollbackMutations(context, ephemeralMutations, CancellationToken.None);
            else
                await RollbackMutations(context, ephemeralMutations, cancellationToken).ConfigureAwait(false);

            return;
        }

        // A transaction with persistent modifications that could not be finalized durably — a persistent key with
        // no staged value the coordinator can replay losslessly — must not commit its persistent writes through the
        // manual ticket path, which is being retired. It aborts (retryable at the caller) rather than committing
        // persistence through a soon-to-be-removed mechanism. Only the all-ephemeral case falls through below to the
        // in-memory (ticket-free) commit path; every persistent-bearing transaction is finalized durably or aborts.
        if (persistentKeys.Count > 0)
        {
            context.Result = new() { Type = KeyValueResponseType.Aborted, Reason = "Persistent mutation could not be staged for a durable commit" };
            return;
        }

        {
            if (!await ValidateReadSet(context, cancellationToken))
                return;

            // Place write intents before probing for commit conflicts so that a racing peer's own probe sees
            // them and aborts — preventing write-skew anomalies. The probe also fences these writes against a
            // foreign range lock acquired since they were staged; the ephemeral prepare handler does not check
            // range locks either, so this path carries the same hole as the durable one.
            (bool success, List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)>? mutationsPrepared) = await PrepareMutations(
                context,
                cancellationToken
            );

            if (mutationsPrepared is null)
                return;

            if (!success)
            {
                if (context.AsyncRelease)
                    _ = RollbackMutations(context, mutationsPrepared, CancellationToken.None);
                else
                    await RollbackMutations(context, mutationsPrepared, cancellationToken);
                return;
            }

            if (!await CheckCommitConflicts(context, cancellationToken))
            {
                if (context.AsyncRelease)
                    _ = RollbackMutations(context, mutationsPrepared, CancellationToken.None);
                else
                    await RollbackMutations(context, mutationsPrepared, cancellationToken);
                return;
            }

            await CommitMutations(context, mutationsPrepared, cancellationToken);
        }
    }

    // ── Durable prepared-intent finalize path ─────────────────────────────

    // Rolling p99 of durable-finalize latency, used to size each transaction's decision-deadline margin so the
    // deadline tracks real finalize cost instead of a fixed guess (too low spuriously aborts slow-but-alive
    // coordinators; too high delays recovery of dead ones). Latency is a local elapsed-time measurement, not a
    // distributed-event ordering, so it is sampled with a monotonic stopwatch; the deadline itself is an HLC offset.
    private readonly FinalizeLatencyEstimator finalizeLatency = new();

    private DurableTransactionFinalizer? durableFinalizer;

    /// <summary>Test access to the lazily built finalizer, for installing its interleaving hooks.</summary>
    internal DurableTransactionFinalizer DurableFinalizerForTests => DurableFinalizer;

    // Deferred-settlement background resolutions in flight. Tracked so Dispose can drain them (bounded) before
    // teardown and cancel any straggler; a lost run is finished by recovery regardless, so this is best-effort.
    private readonly CancellationTokenSource deferredResolutionCts = new();

    private readonly ConcurrentDictionary<Task, byte> deferredResolutions = new();

    // Resolution (materialize committed values, settle intents) runs synchronously before the finalizer returns
    // (a null scheduler = await inline). Deferred settlement would return after the decision barrier and materialize
    // in the background, but that requires the cross-node anchor decision lookup: a latest read on another node that
    // meets a committed-but-unmaterialized foreign intent must resolve it against the remote canonical record.
    // Without that lookup a back-to-back transaction on a different node reads the stale prior value, violating
    // read-your-writes, so settlement is synchronous until the cross-node lookup lands. The finalizer
    // still supports deferred settlement via its scheduler seam for when it does.
    private DurableTransactionFinalizer DurableFinalizer => durableFinalizer ??= new DurableTransactionFinalizer(
        manager.DurableTransactionRecordStore, manager.DurablePreparedIntentStore, ReplicateDurableAsync,
        // Deferred settlement runs resolution off the commit critical path when configured; otherwise resolution is
        // awaited inline (synchronous). Reads/writes that meet a committed-but-unsettled intent resolve it through
        // the durable-intent visibility path (local record, or routed to the anchor leader), so no stale value is
        // served; a lost background run is completed by recovery.
        resolutionScheduler: configuration.DurableDeferredSettlement ? ScheduleDeferredResolution : null,
        ApplyDurableCommitLocally, ApplyDurableRollbackLocally,
        // Fresh attempt HLC at the decision so a slow prepare can trip the deadline; record latency only to the
        // decision so post-decision settlement never inflates the deadline the next finalize must fit inside.
        attemptClock: () => raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId()),
        recordDecisionLatencyMs: finalizeLatency.Record,
        // Re-fence the pre-decision record/prepare submissions at dispatch, so a split/merge between freeze and
        // dispatch releases them retryably instead of appending to a retired partition.
        replicateFenced: ReplicateDurableFencedAsync,
        // Bundle the anchor partition's record init with its own prepare into one proposal, removing a pre-decision
        // Raft barrier from the one-partition critical path.
        replicateAnchorBundle: ReplicateDurableAnchorBundleAsync,
        maxMaterializationBatchItems: Math.Min(
            configuration.KeyValueWriteMaxBatchItems,
            configuration.KeyValueWriteMaxQueuedItemsPerPartition),
        maxMaterializationBatchBytes: Math.Min(
            configuration.KeyValueWriteMaxBatchBytes,
            configuration.KeyValueWriteMaxQueuedBytesPerPartition),
        // Prepare-conflict helping: a prepare blocked by a decided-but-unsettled foreign intent settles that
        // intent inline (through the recovery path's idempotent resolution) and re-prepares immediately, instead
        // of backing off while deferred settlement catches up. Breaks the settlement-lag convoy under load.
        resolveDecidedBlockers: manager.TryResolveDecidedDurableBlockersAsync,
        // One-phase commit: a single-participant transaction whose anchor partition is led locally decides in
        // one durable barrier ([init + prepare + decision] in one atomic batch) instead of two.
        replicateOnePhaseBundle: manager.ReplicateDurableOnePhaseBundleThroughSchedulerFenced,
        // Write-side compare-and-set before anything durable: a staged base that moved while the in-memory
        // write-intent lease lapsed aborts truthfully instead of silently discarding the other writer's commit.
        validateStagedBases: ValidateStagedBasesAsync,
        // Re-resolve each intent's current data partition at resolution time: a range split/merge between the
        // decision and a (deferred or retried) materialization moves the key's ownership, and materializing
        // into the frozen partition would hide the committed value from every reader of the new range.
        resolveCurrentPartition: key => manager.LocateDurablePartition(key).PartitionId,
        // The terminal decision replicates WITHOUT the sender-side record projection: a decision forwarded to
        // a remote anchor leader can lose to one that already won there (a routed presumed abort racing this
        // commit, or the reverse), and projecting the losing delta would diverge this node's record store from
        // the canonical record for good — the divergent answer then mis-directs scans and resolution.
        replicateDecision: (partitionId, delta, fenceKey, fenceGeneration, cancellationToken) =>
            manager.ReplicateDurableThroughSchedulerFenced(
                partitionId, ReplicationTypes.TransactionRecord, delta, fenceKey, fenceGeneration,
                Writes.WriteAdmissionClass.Terminal, cancellationToken, projectRecordLocally: false),
        // The decision winner and the resolution direction read the CANONICAL record: locally exactly when
        // this node leads the anchor partition, routed to the anchor leader otherwise.
        lookupRecordRouted: manager.LookupDurableRecordRouted,
        // Pre-decision replica fence confirmation: the prepare acknowledgement folds only the leader's
        // staged-base verdict, and a leader whose fence memory is frozen or freshly restored admits exactly
        // the prepares the healthy replicas refuse. Reading the replicas' verdicts before the decision turns
        // the racing stale-base veto into an ordered, truthful conflict abort. A single-process group skips
        // it: every replica shares this process's stores, so there is no divergent memory to consult.
        confirmReplicaFence: configuration.SingleProcessRaftGroup ? null : manager.ConfirmReplicaFenceForCommitAsync,
        // Replicate the post-decision materialization by reference when the cluster is known to apply it: the
        // record then names the prepared intent every replica already holds instead of copying the committed
        // value through the log a second time.
        materializeByReference: configuration.DurableMaterializeByReference);

    /// <summary>Schedules a durable transaction's post-decision resolution to run off the commit critical path.
    /// Exceptions are swallowed — the decision is already durable and recovery finishes any lost run — and the task
    /// is tracked so <see cref="Dispose"/> can drain it before teardown.</summary>
    private void ScheduleDeferredResolution(Func<CancellationToken, Task> resolution)
    {
        Task task = Task.Run(async () =>
        {
            try
            {
                await resolution(deferredResolutionCts.Token).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "Deferred durable resolution failed; recovery will complete it");
            }
        });

        deferredResolutions.TryAdd(task, 0);
        _ = task.ContinueWith(static (t, state) => ((ConcurrentDictionary<Task, byte>)state!).TryRemove(t, out _),
            deferredResolutions, TaskScheduler.Default);
    }

    public void Dispose()
    {
        // Give in-flight deferred resolutions a bounded window to finish, then cancel any straggler (recovery
        // completes a cancelled/lost run on restart), before disposing.
        try { Task.WhenAll(deferredResolutions.Keys).Wait(TimeSpan.FromSeconds(5)); } catch { /* best-effort drain */ }
        try { deferredResolutionCts.Cancel(); } catch (ObjectDisposedException) { /* already disposed */ }
        deferredResolutionCts.Dispose();
        durableFinalizer?.Dispose();
    }

    // Durable-intent 2PC records replicate through the shared partition write scheduler so concurrent
    // transactions' records to the same partition coalesce into one ReplicateEntries proposal (cross-transaction
    // batching). The finalizer applies committed record/intent deltas to its local stores, and the resolution's
    // committed value to the leader's KV state via ApplyDurableCommitLocally.
    private Task<bool> ReplicateDurableAsync(int partitionId, string logType, byte[] data, Writes.WriteAdmissionClass admissionClass, CancellationToken cancellationToken) =>
        manager.ReplicateDurableThroughScheduler(partitionId, logType, data, admissionClass, cancellationToken);

    // Fenced variant for the pre-decision record/prepare path: re-resolves the partition against the frozen
    // generation at dispatch (local aggregator path) so a topology change since freeze releases it retryably.
    private Task<bool> ReplicateDurableFencedAsync(int partitionId, string logType, byte[] data, string fenceKey, long fenceGeneration, Writes.WriteAdmissionClass admissionClass, CancellationToken cancellationToken) =>
        manager.ReplicateDurableThroughSchedulerFenced(partitionId, logType, data, fenceKey, fenceGeneration, admissionClass, cancellationToken);

    // Bundles the anchor partition's [record init, prepare] into one fenced proposal, removing a pre-decision
    // barrier. Returns the record-durable and prepare-acknowledged signals separately so the finalizer can tell a
    // clean retry (nothing durable) from a committed-but-rejected prepare (record durable, drive a truthful abort).
    private Task<(bool BatchCommitted, bool PrepareAcknowledged)> ReplicateDurableAnchorBundleAsync(int partitionId, byte[] recordInitDelta, byte[] anchorPrepareDelta, string fenceKey, long fenceGeneration, CancellationToken cancellationToken) =>
        manager.ReplicateDurableBundleThroughSchedulerFenced(partitionId, recordInitDelta, anchorPrepareDelta, fenceKey, fenceGeneration, cancellationToken);

    // Applies a committed intent's value on the leader (clears the staged write intent/MVCC and applies the value —
    // the durable-path commit apply), invoked by the finalizer's resolution after the intent commits.
    // Routes to the participant partition's leader node on a cluster.
    private Task<bool> ApplyDurableCommitLocally(int partitionId, PreparedIntent intent) =>
        manager.ApplyDurableCommit(partitionId, intent, CancellationToken.None);

    // Clears an aborted transaction's staged write intent/MVCC on the leader (the durable analog of
    // ApplyConfirmedRollback), invoked by the finalizer's resolution when the decision is abort.
    private Task<bool> ApplyDurableRollbackLocally(int partitionId, PreparedIntent intent) =>
        manager.ApplyDurableRollback(partitionId, intent, CancellationToken.None);

    /// <summary>
    /// Derives this finalize's decision-deadline margin (ms past the commit timestamp) as
    /// <c>clamp(multiplier × observed-finalize-p99, floor, ceiling)</c>, giving a healthy commit headroom above
    /// typical finalize latency while capping how long a dead coordinator's undecided record can block recovery.
    /// During warmup (no p99 yet) the floor applies. The chosen margin is recorded for tuning observability.
    /// </summary>
    private long DeriveDecisionDeadlineMarginMs()
    {
        long floor = configuration.DurableDecisionDeadlineFloorMs;
        long ceiling = Math.Max(configuration.DurableDecisionDeadlineCeilingMs, floor);
        long derived = (long)configuration.DurableDecisionDeadlineMultiplier * finalizeLatency.P99Ms;
        long margin = Math.Clamp(derived, floor, ceiling);

        DurableTransactionMetrics.DecisionDeadlineMarginMs.Record(margin);
        return margin;
    }

    /// <summary>
    /// Builds the frozen finalize input from the transaction's staged persistent mutations, grouping intents by
    /// their current data partition. Returns false — so the caller falls back to the ticket path — when the
    /// transaction is not all-persistent, has no anchor, or a modified key has no staged value to prepare.
    /// </summary>
    // Builds a durable finalize input from the transaction's PERSISTENT modified keys (the crash-atomic subset).
    // For an all-persistent transaction this is every modified key; for a mixed transaction it is the persistent
    // subset only — the ephemeral keys are committed/rolled back in memory keyed on this decision. Returns false
    // (fall back) when there is no persistent key, no anchor, or a persistent key with no staged value.
    private bool TryBuildDurableFinalizeInput(TransactionContext context,
        IReadOnlyCollection<(string Key, KeyValueDurability Durability)> persistentKeys,
        out DurableFinalizeInput? input, out HLCTimestamp opId)
    {
        input = null;
        opId = HLCTimestamp.Zero;

        // A prior durable attempt on this session proposed its canonical record without reaching a record-backed
        // outcome. Its identity — commit timestamp, decision deadline, manifest hash — is frozen in the record
        // CAS, so a retry must re-drive that same frozen input under a fresh operation id (exactly as the
        // reaper's and rollback's fences do): rebuilding would mint a new identity that the existing record
        // rejects on every transition (init, prepare, decision alike), leaving the transaction permanently
        // retryable instead of ever resolving it.
        if (context.UnresolvedDurableFinalize is { } unresolved)
        {
            input = unresolved;
            opId = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());
            return true;
        }

        if (persistentKeys.Count == 0 || context.RecordAnchorKey is null)
            return false;

        // Staged values are accumulated per key across every mutation command (not from ModifiedResult, which
        // reflects only the last command). A modified key missing here — an extend, a TTL set, or any mutation the
        // coordinator cannot stage losslessly — makes TryBuild fall back to the ticket path.
        Dictionary<string, StagedValue>? staged = context.StagedMutations;
        if (staged is null || staged.Count == 0)
            return false;

        HLCTimestamp txId = context.TransactionId;

        const long epoch = DurableFinalizeEpoch;

        HLCTimestamp commitTimestamp = raft.HybridLogicalClock.ReceiveEvent(raft.GetLocalNodeId(), txId);
        HLCTimestamp decisionDeadline = new(commitTimestamp.N, commitTimestamp.L + DeriveDecisionDeadlineMarginMs(), commitTimestamp.C);

        Dictionary<string, StagedMutation> stagedByKey = new(staged.Count);
        foreach ((string key, StagedValue value) in staged)
            stagedByKey[key] = new StagedMutation(value.Value, value.Revision, value.ExpiresMs, value.NoRevision);

        // Each read-then-written key's pre-write observation is the exact committed base its mutation is
        // conditioned on; frozen into the intents so the staged-base compare-and-set can enforce it.
        Dictionary<string, KeyValueTransactionReadKey>? writtenBases = null;
        if (context.WrittenBaseObservations is { Count: > 0 } observations)
        {
            writtenBases = new Dictionary<string, KeyValueTransactionReadKey>(observations.Count);
            foreach (((string key, KeyValueDurability durability), KeyValueTransactionReadKey observed) in observations)
                if (durability == KeyValueDurability.Persistent)
                    writtenBases[key] = observed;
        }

        if (!DurableFinalizeInputBuilder.TryBuild(
                txId, epoch, context.CoordinatorKey ?? context.RecordAnchorKey, context.RecordAnchorKey,
                commitTimestamp, decisionDeadline, persistentKeys, stagedByKey,
                manager.LocateDurablePartition, out input, writtenBases))
            return false;

        opId = commitTimestamp;
        return true;
    }

    // Finalizes the persistent (durable) subset of a transaction and returns its terminal decision, so a mixed
    // transaction's caller can commit or roll back the ephemeral subset accordingly. Sets context.Result too.
    private async Task<DurableFinalizeResult> DurableFinalize(TransactionContext context, DurableFinalizeInput input, HLCTimestamp opId, CancellationToken cancellationToken)
    {
        // Admission gate 1 — resident prepared-intent count/bytes: refuse before preparing if this transaction's
        // intents would push resident prepared-intent state past its node bound, so slow settlement cannot let it
        // grow without limit. A read-only check (no reservation to unwind), so it runs before the slot reserve.
        if (!AdmitPreparedIntents(input))
        {
            DurableTransactionMetrics.AdmissionRejections.Add(1);
            context.Result = new KeyValueTransactionResult
            {
                Type = KeyValueResponseType.MustRetry,
                Reason = "Durable prepared-intent capacity reached; retry"
            };
            return DurableFinalizeResult.MustRetry;
        }

        // Admission gate 2 — reserve one outstanding-durable slot before prepare. At capacity the transaction is
        // refused with a retryable MustRetry (nothing prepared, no state installed), so a burst applies
        // backpressure instead of admitting unbounded prepared intents / canonical records.
        if (!TryReserveDurableSlot())
        {
            DurableTransactionMetrics.AdmissionRejections.Add(1);
            context.Result = new KeyValueTransactionResult
            {
                Type = KeyValueResponseType.MustRetry,
                Reason = "Durable admission at capacity; retry"
            };
            return DurableFinalizeResult.MustRetry;
        }

        // Post-prepare commit validation: the batched probe catches a concurrent in-flight writer on a read key
        // and a foreign range lock covering a written key, and the revision-comparison check (intent-aware — it
        // reads current committed state through the durable-intent-aware read path) catches a read that a
        // concurrent commit made stale. Both must pass to commit.
        //
        // This is where the range-lock fence has to run, and post-prepare is the point: the anomaly it exists to
        // stop is a lock acquired *after* the write was staged, so a write-time check is blind to it by
        // construction. It is also the one barrier both finalize protocols share — the one-phase bundle validates
        // here before proposing, and a failed validation there falls back to the standard 2PC flow, which
        // re-validates after the prepares are durable.
        //
        // The concurrent-writer probe runs FIRST, and the order is load-bearing: it answers immediately for an
        // undecided foreign intent, while the revision check's read waits for one to resolve. Two transactions that
        // prepared intents on each other's read-set keys would otherwise each wait for the other to decide — a
        // deadlock broken only by the read's own deadline. Probing for the concurrent writer first turns that
        // standoff into the immediate mutual abort the probe exists to produce.
        // The finalizer records its own decision-scoped latency into finalizeLatency.
        // From here on the attempt drives real replication, and a proposal that times out is indeterminate — it
        // can still commit after a partition heals, carrying a bundled commit decision whose propose-time attempt
        // HLC passes the record's deadline gate whenever it applies. Record the frozen input so any finalizer that
        // later gives up on this session (the reaper, an explicit rollback) must fence through the record CAS
        // before reporting a definite abort. Cleared below only on a record-backed terminal outcome.
        context.UnresolvedDurableFinalize = input;

        DurableFinalizeOutcome outcome;
        try
        {
            outcome = await DurableFinalizer.FinalizeAsync(
                input,
                validateReadSet: async ct =>
                {
                    DurableTransactionMetrics.FinalizeReadSetKeys.Record(context.ReadKeys?.Count ?? 0);
                    return await CheckCommitConflicts(context, ct).ConfigureAwait(false)
                        && await ValidateReadSet(context, ct).ConfigureAwait(false);
                },
                opId,
                cancellationToken,
                // A read set reaching beyond the written keys disqualifies the one-phase bundle because
                // apply-time re-checks cover only written keys — a stalled bundle surfacing after a
                // leader change would decide on a long-stale read validation. In a single-process Raft
                // group that stall cannot exist (no remote replica can resurrect an in-flight proposal;
                // a committed bundle replays during restore ahead of any later conflicting write), so
                // read-carrying transactions stay one-phase eligible there. See
                // <see cref="KahunaConfiguration.SingleProcessRaftGroup"/> for the full argument.
                //
                // A validated base (a read-then-written key) disqualifies the bundle for the same shape of
                // reason: on the 2PC path a competitor that commits the same base between the pre-propose
                // probe and this transaction's prepare landing (the bank-soak lost update) is caught by the
                // intent store's staged-base fence at prepare apply, but the bundle's decision shares the
                // prepare's atomic batch, so a refused acknowledgement cannot withhold it — the bundle's only
                // guard is its pre-propose re-validation, whose unguarded tail is the validate→apply gap of
                // the proposal. Multi-process only, matching the read-set rule: a multi-process stalled bundle
                // can apply arbitrarily late, making that tail unbounded; in a single-process group it is a
                // sub-millisecond in-process window behind a lease lapse, accepted as a residual in exchange
                // for the embedded fast path.
                readSetExtendsBeyondWrites: !configuration.SingleProcessRaftGroup
                    && (HasReadDependenciesBeyondWrites(context)
                        || context.WrittenBaseObservations is { Count: > 0 })).ConfigureAwait(false);
        }
        catch (Exception)
        {
            // An exception during finalize does not itself decide anything. Classify from the canonical record:
            // if the decision is already durable it is the truth (Committed or Abort); otherwise the outcome is
            // unknown and retryable. This is what stops a post-decision infrastructure failure from telling the
            // caller it is safe to replay a transaction that actually committed.
            outcome = ClassifyFromCanonicalRecord(input);
        }
        finally
        {
            // The attempt has ended (decision installed or given up); free the slot for the next transaction.
            ReleaseDurableSlot();
        }

        // A terminal outcome is record-backed (classified from the canonical record), so no stalled proposal can
        // contradict it; only a MustRetry leaves the attempt unresolved and keeps the fence obligation in place.
        if (outcome.Result is DurableFinalizeResult.Committed or DurableFinalizeResult.Aborted)
            context.UnresolvedDurableFinalize = null;

        context.Result = outcome.Result switch
        {
            // On commit, preserve the last statement's result exactly as the ticket path does (its CommitMutations
            // sets no result on success), so an auto-commit script ending in a read returns that read — not a
            // synthetic Set. A bare commit with no prior result still reports Set.
            //
            // Never preserve a failure, though: a discarded earlier attempt can have written one here — the
            // one-phase fast path's pre-propose validation sets a conflict Aborted when its write-skew probe
            // finds a concurrent intent, then falls back to the standard flow, whose re-validation can pass
            // (the intent's owner resolved meanwhile) and commit. The transaction durably committed and its
            // writes are visible; answering that stale Aborted would tell the client a committed write had
            // no effect — an aborted-read anomaly for every reader that then observes it.
            DurableFinalizeResult.Committed =>
                context.Result is { Type: not (KeyValueResponseType.Aborted or KeyValueResponseType.Errored or KeyValueResponseType.MustRetry) }
                    ? context.Result
                    : new KeyValueTransactionResult { Type = KeyValueResponseType.Set, Reason = null },
            DurableFinalizeResult.Aborted => new KeyValueTransactionResult
            {
                Type = KeyValueResponseType.Aborted,
                Reason = outcome.AbortClass == TransactionAbortClass.Conflict
                    ? "Transaction conflict"
                    : $"Transaction aborted: {outcome.AbortClass}"
            },
            _ => new KeyValueTransactionResult { Type = KeyValueResponseType.MustRetry, Reason = "Durable finalize could not complete; retry" }
        };

        return outcome.Result;
    }

    /// <summary>
    /// Maps the canonical transaction record to a finalize outcome after a finalize threw. A durable Commit is
    /// Committed and a durable Abort is Aborted (terminal whatever its class); an undecided record or no resident
    /// record is the retryable MustRetry — never a fabricated abort. A remote anchor's record is read from this
    /// node's local projection; a nonresident record stays MustRetry until recovery or the anchor-routed lookup
    /// resolves it.
    /// </summary>
    /// <summary>Reserves one outstanding-durable admission slot, or returns false if the node is at
    /// <c>DurableDecisionOutstandingMax</c>. A non-positive cap disables the gate (always admits). The CAS loop
    /// makes the reservation atomic under concurrent admissions, so the count can never exceed the cap.</summary>
    private bool TryReserveDurableSlot()
    {
        int max = configuration.DurableDecisionOutstandingMax;
        if (max <= 0)
            return true;

        while (true)
        {
            int current = Volatile.Read(ref outstandingDurable);
            if (current >= max)
                return false;
            if (Interlocked.CompareExchange(ref outstandingDurable, current + 1, current) == current)
                return true;
        }
    }

    /// <summary>Releases a slot reserved by <see cref="TryReserveDurableSlot"/>. A no-op when the gate is
    /// disabled, so a disabled-then-enabled reconfiguration cannot underflow the counter.</summary>
    private void ReleaseDurableSlot()
    {
        if (configuration.DurableDecisionOutstandingMax <= 0)
            return;

        Interlocked.Decrement(ref outstandingDurable);
    }

    /// <summary>Current count of durable transactions being driven through finalize (test/observability).</summary>
    internal int OutstandingDurableCount => Volatile.Read(ref outstandingDurable);

    /// <summary>Whether admitting this transaction's prepared intents keeps resident prepared-intent count and
    /// bytes within their node bounds. A non-positive cap disables that dimension. The check reads the current
    /// resident totals without reserving, so a rare concurrent admission can overshoot slightly — acceptable for
    /// a soft memory backpressure bound whose transaction count is already capped by the outstanding gate.</summary>
    private bool AdmitPreparedIntents(DurableFinalizeInput input)
    {
        int maxCount = configuration.DurablePreparedIntentMaxCount;
        long maxBytes = configuration.DurablePreparedIntentMaxBytes;
        if (maxCount <= 0 && maxBytes <= 0)
            return true;

        int txCount = 0;
        long txBytes = 0;
        foreach (DurablePartitionPrepare partition in input.Partitions)
            foreach (PreparedIntent intent in partition.Intents)
            {
                txCount++;
                txBytes += intent.Value?.Length ?? 0;
            }

        PreparedIntentStore store = manager.DurablePreparedIntentStore;
        if (maxCount > 0 && store.Count + txCount > maxCount)
            return false;
        if (maxBytes > 0 && store.TotalBytes + txBytes > maxBytes)
            return false;

        return true;
    }

    private DurableFinalizeOutcome ClassifyFromCanonicalRecord(DurableFinalizeInput input)
    {
        TransactionRecord? record = manager.DurableTransactionRecordStore.Get(input.TransactionId, input.Epoch);

        return record?.Decision switch
        {
            TransactionDecision.Commit => new DurableFinalizeOutcome(DurableFinalizeResult.Committed, TransactionAbortClass.None),
            TransactionDecision.Abort => new DurableFinalizeOutcome(DurableFinalizeResult.Aborted, record.AbortClass),
            _ => new DurableFinalizeOutcome(DurableFinalizeResult.MustRetry, TransactionAbortClass.RetryableFailure)
        };
    }

    /// <summary>
    /// Whether the transaction's read-set must be validated for write-skew before prepare. Optimistic locking
    /// is validate-at-commit by definition, so it always checks its read set: an optimistic transaction that
    /// skipped validation would provide no isolation and silently admit lost updates and write skew. An
    /// explicit <see cref="ReadValidation.TrackAndValidate"/> policy additionally requests validation for a
    /// pessimistic transaction, independent of locking mode.
    /// </summary>
    private static bool RequiresReadSetValidation(TransactionContext context)
    {
        return context.ReadValidation == ReadValidation.TrackAndValidate
            || context.Locking == KeyValueTransactionLocking.Optimistic;
    }

    /// <summary>
    /// Whether the transaction's validated read footprint reaches beyond the keys it writes: a point read of a
    /// key it never modifies, or any prefix/range lock (a scan's footprint). Written keys carrying a validated
    /// base are re-fenced at prepare apply (the intent store's staged-base fence refuses the prepare
    /// acknowledgement when the base moved), but a read-only dependency is protected solely by commit-time
    /// validation — so the finalize path must not choose a protocol that lets the decision apply after that
    /// validation can no longer be trusted. The bundled-prepare gate verifies intent presence only, and
    /// decision-apply gates only on the frozen deadline. Always false when the read set is not validated at
    /// all, since no read-stability promise exists to protect.
    /// </summary>
    private static bool HasReadDependenciesBeyondWrites(TransactionContext context)
    {
        if (!RequiresReadSetValidation(context))
            return false;

        if (context.PrefixLocksAcquired is { Count: > 0 } || context.RangeLocksAcquired is { Count: > 0 })
            return true;

        if (context.ReadKeys is null || context.ReadKeys.Count == 0)
            return false;

        foreach (KeyValueTransactionReadKey readKey in context.ReadKeys.Values)
        {
            if (string.IsNullOrEmpty(readKey.Key))
                continue;

            if (context.ModifiedKeys is not null && context.ModifiedKeys.Contains((readKey.Key, readKey.Durability)))
                continue;

            return true;
        }

        return false;
    }

    /// <summary>
    /// Validates optimistic read dependencies against the current committed state before prepare.
    /// </summary>
    private async Task<bool> ValidateReadSet(TransactionContext context, CancellationToken cancellationToken)
    {
        if (!RequiresReadSetValidation(context))
            return true;

        // The transaction observed the same key at two different base states — an unstable read snapshot that
        // cannot be validated as consistent. Abort rather than commit on a self-contradictory read set.
        if (context.ReadObservationConflict)
        {
            context.Result = new()
            {
                Type = KeyValueResponseType.Aborted,
                Reason = "Read observation instability: a key was observed at two different base revisions"
            };

            return false;
        }

        if (context.ReadKeys is null || context.ReadKeys.Count == 0)
            return true;

        List<KeyValueTransactionReadKey> toValidate = [];

        foreach (KeyValueTransactionReadKey readKey in context.ReadKeys.Values)
        {
            if (string.IsNullOrEmpty(readKey.Key))
                continue;

            if (context.ModifiedKeys is not null && context.ModifiedKeys.Contains((readKey.Key, readKey.Durability)))
                continue;

            toValidate.Add(readKey);
        }

        if (toValidate.Count == 0)
            return true;

        List<(string key, long revision, KeyValueDurability durability)> probes = new(toValidate.Count);

        for (int i = 0; i < toValidate.Count; i++)
            probes.Add((toValidate[i].Key!, -1, toValidate[i].Durability));

        List<(KeyValueResponseType type, string key, KeyValueDurability durability, ReadOnlyKeyValueEntry? entry)> results =
            await manager.LocateAndTryExistsManyValues(HLCTimestamp.Zero, HLCTimestamp.Zero, probes, cancellationToken);

        Dictionary<(string key, KeyValueDurability durability), (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry)> currentByKey = new(results.Count);

        foreach ((KeyValueResponseType type, string key, KeyValueDurability durability, ReadOnlyKeyValueEntry? entry) result in results)
            currentByKey[(result.key, result.durability)] = (result.type, result.entry);

        foreach (KeyValueTransactionReadKey readKey in toValidate)
        {
            if (!currentByKey.TryGetValue((readKey.Key!, readKey.Durability), out (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) current))
            {
                context.Result = new()
                {
                    Type = KeyValueResponseType.Aborted,
                    Reason = $"Read dependency validation failed for {readKey.Key}: {KeyValueResponseType.Errored}"
                };

                logger.LogWarning(
                    "Read dependency validation failed for {Key} {Durability}: {Response}",
                    readKey.Key,
                    readKey.Durability,
                    KeyValueResponseType.Errored
                );

                return false;
            }

            ReadValidationFailure? failure = ValidateReadKey(readKey, current.type, current.entry);

            if (failure is null)
                continue;

            context.Result = new()
            {
                Type = KeyValueResponseType.Aborted,
                Reason = failure.Value.AbortReason
            };

            failure.Value.Log();

            return false;
        }

        return true;
    }

    /// <summary>
    /// Validates a single optimistic read dependency against the current committed state.
    /// </summary>
    private ReadValidationFailure? ValidateReadKey(
        KeyValueTransactionReadKey readKey,
        KeyValueResponseType response,
        ReadOnlyKeyValueEntry? current
    )
    {
        if (response is KeyValueResponseType.MustRetry or KeyValueResponseType.Errored or KeyValueResponseType.Aborted or KeyValueResponseType.WaitingForReplication)
            return new(
                $"Read dependency validation failed for {readKey.Key}: {response}",
                () => logger.LogWarning(
                    "Read dependency validation failed for {Key} {Durability}: {Response}",
                    readKey.Key,
                    readKey.Durability,
                    response
                )
            );

        bool existsNow = response == KeyValueResponseType.Exists && current is not null;

        if (readKey.Exists != existsNow)
            return new(
                $"Read dependency changed for {readKey.Key}",
                () => logger.LogReadDependencyChanged(readKey.Key!, readKey.Durability, readKey.Exists, existsNow)
            );

        if (readKey.Exists && current!.Revision != readKey.Revision)
            return new(
                $"Read dependency revision changed for {readKey.Key}",
                () => logger.LogReadDependencyRevisionChanged(readKey.Key!, readKey.Durability, readKey.Revision, current.Revision)
            );

        return null;
    }

    private readonly record struct ReadValidationFailure(string AbortReason, Action Log);

    /// <summary>
    /// The write-side compare-and-set, run by the finalizer before anything durable is proposed: checks each
    /// frozen intent's validated base revision against the key's current committed state, so a
    /// base that moved after staging — the in-memory write-intent lease lapsed mid-finalize and another
    /// transaction committed the same base first — aborts truthfully instead of silently discarding that
    /// writer's commit. Each key resolves in three tiers:
    /// <list type="bullet">
    /// <item>A live durable intent owned by this transaction (a retry of a prepared-but-undecided attempt):
    /// the base is frozen beneath that intent by the single-live-intent rule, so it is valid by construction —
    /// the check that admitted the prepare already ran before it was proposed.</item>
    /// <item>A foreign durable intent: its canonical record decides. A committed winner's revision that is not
    /// this transaction's validated base is the moved-base conflict, even before its value materializes into
    /// MVCC (the ordinary read paths cannot answer this: they either overlay that intent behind a wait, or
    /// serve this transaction's own staged view). An aborted holder never materializes and falls through to
    /// the committed read; an undecided one is a retryable unknown.</item>
    /// <item>No intent: the plain committed read compares revision and existence directly.</item>
    /// </list>
    /// </summary>
    private async Task<StagedBaseValidation> ValidateStagedBasesAsync(DurableFinalizeInput input, CancellationToken cancellationToken)
    {
        List<(string key, long revision, KeyValueDurability durability)> probes = [];
        List<PreparedIntent> toProbe = [];

        foreach (DurablePartitionPrepare partition in input.Partitions)
        {
            foreach (PreparedIntent staged in partition.Intents)
            {
                // A blind write carries no validated base: it is last-writer-wins by design, and a foreign
                // holder on its key is the prepare apply's conflict to resolve, not this check's.
                if (!staged.HasValidatedBase)
                    continue;

                PreparedIntent? holder = manager.DurablePreparedIntentStore.Get(staged.Key);

                if (holder is not null)
                {
                    if (holder.TransactionId == input.TransactionId && holder.Epoch == input.Epoch)
                        continue;

                    TransactionRecord? holderRecord;
                    try
                    {
                        holderRecord = await manager.LookupDurableRecordRouted(
                            holder.TransactionId, holder.Epoch, holder.RecordAnchorKey, cancellationToken);
                    }
                    catch (PartitionNotHostedException)
                    {
                        // The holder's decision cannot be consulted right now: a retryable unknown,
                        // never a silent fallback to the intent's possibly-stale local resolution.
                        return StagedBaseValidation.Unknown;
                    }

                    // The routed record is authoritative; a missing record (already purged after settlement)
                    // falls back to the resolution stamped on the intent itself.
                    TransactionDecision holderDecision = holderRecord?.Decision ?? holder.Resolution switch
                    {
                        PreparedIntentResolution.Committed => TransactionDecision.Commit,
                        PreparedIntentResolution.Aborted => TransactionDecision.Abort,
                        _ => TransactionDecision.Undecided
                    };

                    if (holderDecision == TransactionDecision.Commit)
                    {
                        // The holder's mutation is this key's next committed state whether or not it has
                        // materialized yet. It is only a valid base if it is exactly the base this transaction
                        // validated against — possible when the holder settled before staging but its intent
                        // has not been garbage-collected yet. Revisions are the exact part of the frozen base
                        // (the builder's BaseState is nominal), so they alone decide.
                        if (holder.Revision == staged.BaseRevision)
                            continue;

                        logger.LogWarning(
                            "Staged base for {Key} was overtaken by committed transaction {HolderTransactionId} at revision {HolderRevision} (validated base was {BaseRevision})",
                            staged.Key, holder.TransactionId, holder.Revision, staged.BaseRevision);
                        return StagedBaseValidation.Conflict;
                    }

                    if (holderDecision == TransactionDecision.Undecided)
                        return StagedBaseValidation.Unknown;

                    // Aborted holder: its value never materializes; the committed state beneath it decides.
                }

                probes.Add((staged.Key, -1, KeyValueDurability.Persistent));
                toProbe.Add(staged);
            }
        }

        if (probes.Count == 0)
            return StagedBaseValidation.Valid;

        // The unconfirmed variant reads locally-led keys without a read-index round: this check only
        // decides whether to drive the durable decision proposal, and that proposal's own
        // replication fences a deposed leader — a stale answer here can produce a failed proposal
        // or a conservative abort, never a durably wrong outcome. See the variant's contract before
        // pointing any other caller at it.
        List<(KeyValueResponseType type, string key, KeyValueDurability durability, ReadOnlyKeyValueEntry? entry)> results =
            await manager.LocateAndTryExistsManyValuesUnconfirmed(HLCTimestamp.Zero, HLCTimestamp.Zero, probes, cancellationToken);

        Dictionary<string, (KeyValueResponseType Type, ReadOnlyKeyValueEntry? Entry)> byKey = new(results.Count);
        foreach ((KeyValueResponseType type, string key, KeyValueDurability _, ReadOnlyKeyValueEntry? entry) in results)
            byKey[key] = (type, entry);

        foreach (PreparedIntent staged in toProbe)
        {
            if (!byKey.TryGetValue(staged.Key, out (KeyValueResponseType Type, ReadOnlyKeyValueEntry? Entry) current))
                return StagedBaseValidation.Unknown;

            if (current.Type is KeyValueResponseType.MustRetry or KeyValueResponseType.Errored
                or KeyValueResponseType.Aborted or KeyValueResponseType.WaitingForReplication)
                return StagedBaseValidation.Unknown;

            // The frozen base is the transaction's own pre-write read observation — existence and revision are
            // both exact. Existence must match; and when the key exists on both sides, so must the revision.
            // (A DoesNotExist probe carries a default revision for a never-written key, so only a positive
            // existence answer's counter is compared.)
            bool baseExisted = staged.BaseState == KeyValueState.Set;
            bool existsNow = current.Type == KeyValueResponseType.Exists && current.Entry is not null;

            if (baseExisted != existsNow)
            {
                logger.LogWarning(
                    "Staged base changed for {Key}: observed exists={BaseExisted} rev={BaseRevision}, exists now={ExistsNow}",
                    staged.Key, baseExisted, staged.BaseRevision, existsNow);
                return StagedBaseValidation.Conflict;
            }

            if (existsNow && current.Entry!.Revision != staged.BaseRevision)
            {
                logger.LogWarning(
                    "Staged base revision changed for {Key}: validated against {BaseRevision}, committed is now {CurrentRevision}",
                    staged.Key, staged.BaseRevision, current.Entry.Revision);
                return StagedBaseValidation.Conflict;
            }
        }

        return StagedBaseValidation.Valid;
    }

    /// <summary>
    /// Probes for the two conflicts that must not exist when the transaction commits, in one batched call:
    ///
    /// <list type="bullet">
    /// <item>a concurrent writer on any key in the <b>read set</b> — the write-skew guard, which only optimistic
    /// (or explicitly read-validating) transactions need, since exclusive key locks already serialize the rest;</item>
    /// <item>a foreign range lock covering any key in the <b>write set</b> — the fence against a range lock
    /// acquired after the write was staged. The write-time fence cannot see it (no lock existed then) and the
    /// acquire deliberately does not conflict with an existing write intent, deferring to exactly this check.
    /// It is ungated by locking mode: a range lock steps around a held key lock just as it does a write intent,
    /// so a pessimistic transaction is no less exposed.</item>
    /// </list>
    ///
    /// The two sets are disjoint — a read key that was also written is validated as a write — so a flagged
    /// answer is attributed by which set the key came from.
    /// </summary>
    private async Task<bool> CheckCommitConflicts(TransactionContext context, CancellationToken cancellationToken)
    {
        List<KeyValueConflictProbe> probeKeys = [];

        // Read keys ask only about concurrent write intents: a range lock covering a key this transaction
        // merely read is not a conflict for it.
        if (RequiresReadSetValidation(context) && context.ReadKeys is { Count: > 0 })
        {
            foreach (KeyValueTransactionReadKey readKey in context.ReadKeys.Values)
            {
                if (string.IsNullOrEmpty(readKey.Key))
                    continue;

                if (context.ModifiedKeys is not null && context.ModifiedKeys.Contains((readKey.Key, readKey.Durability)))
                    continue;

                probeKeys.Add(new(readKey.Key!, readKey.Durability, KeyValueConflictChecks.WriteIntent));
            }
        }

        // Write keys ask only about foreign range locks. Deliberately not about write intents: a foreign
        // in-memory intent on a key this transaction writes is the durable prepare's conflict to resolve —
        // it retries and helps the blocker settle — and flagging it here would turn that retryable contention
        // into a hard abort. Deliberately not about staged bases either: a moved base on a read-then-written
        // key is caught at the prepare's own apply position by the intent store's staged-base fence, which
        // refuses the prepare acknowledgement and drives the truthful abort — asking here again would spend a
        // per-key entry load inside the post-prepare barrier, while the transaction's durable intents freeze
        // the hottest keys, for an answer the apply path already gives for free.
        if (context.ModifiedKeys is not null)
        {
            foreach ((string key, KeyValueDurability durability) in context.ModifiedKeys)
            {
                if (string.IsNullOrEmpty(key))
                    continue;

                probeKeys.Add(new(key, durability, KeyValueConflictChecks.ForeignRangeLock));
            }
        }

        if (probeKeys.Count == 0)
            return true;

        // One probe per node owning part of the set, rather than one per key: a working set spread over remote
        // partitions otherwise costs a network round trip per key.
        List<(KeyValueResponseType type, string key, KeyValueDurability durability)> results =
            await manager.LocateAndTryCheckManyWriteIntents(context.TransactionId, probeKeys, cancellationToken);

        Dictionary<(string, KeyValueDurability), KeyValueResponseType> byKey = new(results.Count);

        foreach ((KeyValueResponseType type, string key, KeyValueDurability durability) in results)
        {
            // Two results for one key can only differ if one of them found a conflict, and a conflict is the
            // answer that matters: never let a second, cleaner answer overwrite it. NotSet is the staged-base
            // fence's "compare failed" — a conflict answer just like Aborted.
            if (byKey.TryGetValue((key, durability), out KeyValueResponseType existing)
                && existing is KeyValueResponseType.Aborted or KeyValueResponseType.NotSet)
                continue;

            byKey[(key, durability)] = type;
        }

        foreach ((string key, KeyValueDurability durability, KeyValueConflictChecks checks, _) in probeKeys)
        {
            // Which set the key came from, read off the question that was asked about it rather than off its
            // position in the list: only the read set asks about write intents.
            bool isReadKey = checks == KeyValueConflictChecks.WriteIntent;

            // A key that was probed but has no answer means the probe did not actually cover the set. That is a
            // broken contract rather than a transient outcome, and treating an unanswered key as "no conflict"
            // would silently disable the guard it was asked about, so it aborts like a detected conflict.
            if (!byKey.TryGetValue((key, durability), out KeyValueResponseType type))
            {
                context.Result = new()
                {
                    Type = KeyValueResponseType.Aborted,
                    Reason = isReadKey
                        ? $"Write intent probe returned no result for read key {key}"
                        : $"Range lock probe returned no result for written key {key}"
                };

                logger.LogWriteSkewGuardAborted(context.TransactionId, key);

                return false;
            }

            // Defensive: NotSet is the staged-base probe's "compare failed" answer. This coordinator no longer
            // asks for that check (the apply-time fence owns it), but a mixed-version peer serving part of the
            // probe set may still answer it — and a compare failure must abort, never read as clean.
            if (type == KeyValueResponseType.NotSet)
            {
                DurableTransactionMetrics.StagedBasePostPrepareAborts.Add(1);

                context.Result = new()
                {
                    Type = KeyValueResponseType.Aborted,
                    Reason = $"Committed base for written key {key} changed after validation"
                };

                logger.LogWarning(
                    "Staged base for {Key} moved between validation and prepare of transaction {TransactionId}; aborting to prevent a lost update",
                    key, context.TransactionId);

                return false;
            }

            if (type != KeyValueResponseType.Aborted)
                continue;

            if (!isReadKey)
                DurableTransactionMetrics.RangeLockFenceAborts.Add(1);

            context.Result = new()
            {
                Type = KeyValueResponseType.Aborted,
                Reason = isReadKey
                    ? $"Concurrent write intent detected on read key {key}"
                    : $"Foreign range lock covers written key {key}"
            };

            logger.LogWriteSkewGuardAborted(context.TransactionId, key);

            return false;
        }

        return true;
    }

    /// <summary>
    /// Places write intents on every confirmed modified key (prepare phase of 2PC).
    /// </summary>
    internal async Task<(bool, List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)>?)> PrepareMutations(
        TransactionContext context,
        CancellationToken cancellationToken,
        IReadOnlyList<(string Key, KeyValueDurability Durability)>? keysToPrepare = null
    )
    {
        if (context.LocksAcquired is null || context.ModifiedKeys is null || context.ModifiedKeys.Count == 0)
            return (false, null);

        // Prepare a caller-supplied subset (the ephemeral keys of a mixed transaction, prepared separately from the
        // durable persistent subset) or, by default, every modified key (the legacy all-in-one ticket path).
        IReadOnlyList<(string Key, KeyValueDurability Durability)> keys = keysToPrepare ?? [.. context.ModifiedKeys];
        if (keys.Count == 0)
            return (true, []);

        if (!context.SetState(KeyValueTransactionState.Preparing, KeyValueTransactionState.Pending))
            throw new KahunaAbortedException("Failed to set transaction state to Preparing");

        HLCTimestamp highestModifiedTime = context.TransactionId;

        if (context.ModifiedResult?.Type is KeyValueResponseType.Set or KeyValueResponseType.Extended or KeyValueResponseType.Deleted)
        {
            if (context.ModifiedResult.Values is not null)
            {
                foreach (KeyValueTransactionResultValue result in context.ModifiedResult.Values)
                {
                    if (result.LastModified != HLCTimestamp.Zero && result.LastModified > highestModifiedTime)
                        highestModifiedTime = result.LastModified;
                }
            }
        }

        HLCTimestamp commitId = raft.HybridLogicalClock.ReceiveEvent(raft.GetLocalNodeId(), highestModifiedTime);

        if (keys.Count == 1)
        {
            (string key, KeyValueDurability durability) = keys[0];

            (KeyValueResponseType type, HLCTimestamp ticketId, string _, KeyValueDurability _) = await manager.LocateAndTryPrepareMutations(
                context.TransactionId,
                commitId,
                key, durability,
                cancellationToken,
                recordAnchorKey: context.RecordAnchorKey
            );

            if (!context.SetState(KeyValueTransactionState.Prepared, KeyValueTransactionState.Preparing))
                throw new KahunaAbortedException("Failed to set transaction state to Prepared");

            if (type != KeyValueResponseType.Prepared)
            {
                context.Result = new() { Type = KeyValueResponseType.Aborted, Reason = "Couldn't prepare mutations" };

                logger.LogWarning("Couldn't propose {Key} {Response}", key, type);

                return (false, null);
            }

            return (true, [(key, ticketId, durability)]);
        }

        List<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)> proposalResponses = await manager.LocateAndTryPrepareManyMutations(
            context.TransactionId,
            commitId,
            [.. keys],
            cancellationToken,
            context.RecordAnchorKey
        );

        if (!context.SetState(KeyValueTransactionState.Prepared, KeyValueTransactionState.Preparing))
            throw new KahunaAbortedException("Failed to set transaction state to Prepared");

        if (proposalResponses.Any(r => r.Item1 != KeyValueResponseType.Prepared))
        {
            List<(string, HLCTimestamp, KeyValueDurability)> preparedMutations = [];

            foreach ((KeyValueResponseType, HLCTimestamp, string, KeyValueDurability) proposalResponse in proposalResponses)
            {
                if (proposalResponse.Item1 == KeyValueResponseType.Prepared)
                    preparedMutations.Add((proposalResponse.Item3, proposalResponse.Item2, proposalResponse.Item4));

                logger.LogWarning("Couldn't propose {Key} {Response}", proposalResponse.Item3, proposalResponse.Item1);
            }

            context.Result = new() { Type = KeyValueResponseType.Aborted, Reason = "Couldn't prepare mutations" };

            return (false, preparedMutations);
        }

        return (true, proposalResponses.Select(r => (r.Item3, r.Item2, r.Item4)).ToList());
    }

    /// <summary>
    /// Commits the prepared mutations to the key-value store, retrying transient MustRetry responses.
    /// </summary>
    internal async Task CommitMutations(TransactionContext context, List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)> mutationsPrepared, CancellationToken cancellationToken)
    {
        if (mutationsPrepared.Count == 0)
            return;

        if (!context.SetState(KeyValueTransactionState.Committing, KeyValueTransactionState.Prepared))
            throw new KahunaAbortedException("Failed to set transaction state to Committing");

        if (mutationsPrepared.Count == 1)
        {
            (string key, HLCTimestamp ticketId, KeyValueDurability durability) = mutationsPrepared.First();

            bool committed = false;
            for (int attempt = 0; attempt <= MaxPhase2Retries; attempt++)
            {
                if (attempt > 0 && !await DelayBetweenRetries(cancellationToken))
                    break;

                (KeyValueResponseType response, long _) = await manager.LocateAndTryCommitMutations(
                    context.TransactionId,
                    key,
                    ticketId,
                    durability,
                    CancellationToken.None
                );

                if (response == KeyValueResponseType.Committed)
                {
                    committed = true;
                    break;
                }

                if (response == KeyValueResponseType.MustRetry)
                {
                    logger.LogWarning("CommitMutations: transient failure on {Key} attempt {Attempt}, retrying", key, attempt + 1);
                    continue;
                }

                string reason = $"Failed to commit mutation {key}: {response}";
                context.Result = new() { Type = KeyValueResponseType.Aborted, Reason = reason };
                logger.LogWarning("CommitMutations: {Type} {Key} {TicketId}", response, key, ticketId);
                throw new KahunaAbortedException(reason);
            }

            if (!committed)
            {
                string reason = $"Exhausted retries committing mutation {key}";
                context.Result = new() { Type = KeyValueResponseType.Aborted, Reason = reason };
                logger.LogError("CommitMutations: exhausted {MaxRetries} retries for {Key}", MaxPhase2Retries, key);
                throw new KahunaAbortedException(reason);
            }

            if (!context.SetState(KeyValueTransactionState.Committed, KeyValueTransactionState.Committing))
                throw new KahunaAbortedException("Failed to set transaction state to Committed");

            return;
        }

        Dictionary<string, (HLCTimestamp ticketId, KeyValueDurability durability)> pendingCommits =
            mutationsPrepared.ToDictionary(x => x.key, x => (x.ticketId, x.durability));

        // Result-truth rule for phase two: the transaction may report a definite Aborted only when EVERY
        // participant provably did not commit. A participant provably did not commit only when its group's
        // CommitLogs returned a hard error (Errored). A MustRetry is in-doubt — the shared commit may have
        // landed while only the local apply is uncertain, or a transient may clear on re-drive — so a pending
        // MustRetry participant, like any committed participant, forbids Aborted. A shared partition ticket
        // commits its whole group at once, so this boundary is per group, not per key.
        bool anyCommitted = false;
        HashSet<string> permanentlyFailed = [];

        for (int attempt = 0; attempt <= MaxPhase2Retries && pendingCommits.Count > 0; attempt++)
        {
            if (attempt > 0 && !await DelayBetweenRetries(cancellationToken))
                break;

            List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)> batch =
                [.. pendingCommits.Select(kvp => (kvp.Key, kvp.Value.ticketId, kvp.Value.durability))];

            List<(KeyValueResponseType, string, long, KeyValueDurability)> responses =
                await manager.LocateAndTryCommitManyMutations(context.TransactionId, batch, CancellationToken.None);

            bool hasTransientFailure = false;
            bool hasPermanentFailure = false;
            foreach ((KeyValueResponseType response, string key, long commitIndex, KeyValueDurability durability) in responses)
            {
                if (response == KeyValueResponseType.Committed)
                {
                    pendingCommits.Remove(key);
                    permanentlyFailed.Remove(key);
                    anyCommitted = true;
                    continue;
                }

                if (response == KeyValueResponseType.MustRetry)
                {
                    hasTransientFailure = true;
                    permanentlyFailed.Remove(key);
                    logger.LogWarning("CommitMutations: transient failure on {Key} attempt {Attempt}, retrying", key, attempt + 1);
                    continue;
                }

                // A hard failure of this participant's group — its CommitLogs never took effect. Record it, but
                // decide abort vs in-doubt only after the whole run: a sibling group may have committed, and a
                // key that fails hard this round could still be re-driven if it is not the last word.
                hasPermanentFailure = true;
                permanentlyFailed.Add(key);
                logger.LogError("CommitMutations: permanent failure {Type} on {Key} attempt {Attempt}", response, key, attempt + 1);
            }

            if (hasPermanentFailure || !hasTransientFailure)
                break;
        }

        if (pendingCommits.Count > 0)
        {
            // In-doubt unless every still-pending participant provably failed (hard Errored) AND none committed.
            bool everyPendingProvablyFailed = pendingCommits.Keys.All(permanentlyFailed.Contains);

            if (anyCommitted || !everyPendingProvablyFailed)
            {
                // At least one participant committed, or at least one is in-doubt (pending MustRetry). Never
                // abort — leave the transaction Committing and return MustRetry so a retry (or, for Durable,
                // recovery) re-drives the rest. A definite failure here would lie about a possibly-committed
                // participant.
                logger.LogWarning(
                    "CommitMutations: {Count} participant(s) not confirmed committed; in-doubt, returning MustRetry: {Keys}",
                    pendingCommits.Count, string.Join(", ", pendingCommits.Keys));

                context.Result = new() { Type = KeyValueResponseType.MustRetry, Reason = "Commit in-doubt: not every participant confirmed, remainder pending re-drive" };
                return;
            }

            // Every participant provably failed to commit and none committed — nothing took effect anywhere, so
            // a definite abort is truthful and gives the caller a terminal answer instead of endless MustRetry.
            logger.LogError("CommitMutations: all {Count} participant(s) hard-failed to commit, none committed: {Keys}",
                pendingCommits.Count, string.Join(", ", pendingCommits.Keys));

            string reason = $"All {pendingCommits.Count} participant(s) failed to commit and none committed";
            context.Result = new() { Type = KeyValueResponseType.Aborted, Reason = reason };
            throw new KahunaAbortedException(reason);
        }

        if (!context.SetState(KeyValueTransactionState.Committed, KeyValueTransactionState.Committing))
            throw new KahunaAbortedException("Failed to set transaction state to Committed");
    }

    /// <summary>
    /// Rolls back a collection of prepared mutations, retrying MustRetry participants.
    /// </summary>
    internal async Task RollbackMutations(TransactionContext context, List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)> mutationsPrepared, CancellationToken cancellationToken)
    {
        if (mutationsPrepared.Count == 0)
            return;

        // Pre-decision rollback may begin from Prepared (a prepare or read-set failure rolls back before commit)
        // or from Committing (a Durable anchor that never committed must roll its prepared set back after
        // CommitMutations already advanced the state). Both are pre-commit states in which every prepared ticket
        // is still uncommitted, so settle them from whichever one we are in — anchoring the CAS on Prepared alone
        // would throw from Committing and orphan the tickets instead of rolling them back.
        if (!context.SetState(KeyValueTransactionState.RollingBack, KeyValueTransactionState.Prepared) &&
            !context.SetState(KeyValueTransactionState.RollingBack, KeyValueTransactionState.Committing))
            throw new KahunaAbortedException("Failed to set transaction state to RollingBack");

        if (mutationsPrepared.Count == 1)
        {
            (string key, HLCTimestamp ticketId, KeyValueDurability durability) = mutationsPrepared.First();

            bool rolledBackConfirmed = false;
            for (int attempt = 0; attempt <= MaxPhase2Retries; attempt++)
            {
                if (attempt > 0 && !await DelayBetweenRetries(cancellationToken))
                    break;

                (KeyValueResponseType response, long _) = await manager.LocateAndTryRollbackMutations(
                    context.TransactionId,
                    key,
                    ticketId,
                    durability,
                    CancellationToken.None
                );

                if (response == KeyValueResponseType.RolledBack)
                {
                    rolledBackConfirmed = true;
                    break;
                }

                if (response == KeyValueResponseType.MustRetry)
                {
                    logger.LogWarning("RollbackMutations: transient failure on {Key} attempt {Attempt}, retrying", key, attempt + 1);
                    continue;
                }

                logger.LogWarning("RollbackMutations: {Type} {Key} {TicketId}", response, key, ticketId);
                rolledBackConfirmed = true;
                break;
            }

            if (!rolledBackConfirmed)
            {
                logger.LogError("RollbackMutations: exhausted {MaxRetries} retries for {Key}; participant retains write intent",
                    MaxPhase2Retries, key);
                return;
            }

            if (!context.SetState(KeyValueTransactionState.RolledBack, KeyValueTransactionState.RollingBack))
                throw new KahunaAbortedException("Failed to set transaction state to RolledBack");

            return;
        }

        Dictionary<string, (HLCTimestamp ticketId, KeyValueDurability durability)> pendingRollbacks =
            mutationsPrepared.ToDictionary(x => x.key, x => (x.ticketId, x.durability));

        for (int attempt = 0; attempt <= MaxPhase2Retries && pendingRollbacks.Count > 0; attempt++)
        {
            if (attempt > 0 && !await DelayBetweenRetries(cancellationToken))
                break;

            List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)> batch =
                [.. pendingRollbacks.Select(kvp => (kvp.Key, kvp.Value.ticketId, kvp.Value.durability))];

            List<(KeyValueResponseType, string, long, KeyValueDurability)> responses =
                await manager.LocateAndTryRollbackManyMutations(context.TransactionId, batch, CancellationToken.None);

            bool hasTransientFailure = false;
            foreach ((KeyValueResponseType response, string key, long commitIndex, KeyValueDurability durability) in responses)
            {
                if (response == KeyValueResponseType.RolledBack)
                {
                    // A settled rollback is the only positive proof the participant's prepare state is gone.
                    pendingRollbacks.Remove(key);
                    continue;
                }

                // Anything else — an explicit MustRetry or an unsettled Errored — is NOT proof the write intent
                // or proposal is gone, so retain and retry rather than abandon a possibly-live intent. On
                // exhaustion the pending set is logged and the lingering state expires via its write-intent
                // lease.
                hasTransientFailure = true;
                logger.LogWarning("RollbackMutations: unsettled {Type} on {Key} attempt {Attempt}, retaining", response, key, attempt + 1);
            }

            if (!hasTransientFailure)
                break;
        }

        if (pendingRollbacks.Count > 0)
        {
            logger.LogError("RollbackMutations: exhausted {MaxRetries} retries, {Count} participants still pending: {Keys}",
                MaxPhase2Retries, pendingRollbacks.Count, string.Join(", ", pendingRollbacks.Keys));
            return;
        }

        if (!context.SetState(KeyValueTransactionState.RolledBack, KeyValueTransactionState.RollingBack))
            throw new KahunaAbortedException("Failed to set transaction state to RolledBack");
    }

    /// <summary>
    /// Waits the inter-retry backoff, honouring cancellation.
    /// Returns true when the delay elapsed normally; false when cancellation fired.
    /// </summary>
    internal static async Task<bool> DelayBetweenRetries(CancellationToken cancellationToken)
    {
        try
        {
            await Task.Delay(Phase2RetryDelayMs, cancellationToken);
            return true;
        }
        catch (OperationCanceledException)
        {
            return false;
        }
    }
}
