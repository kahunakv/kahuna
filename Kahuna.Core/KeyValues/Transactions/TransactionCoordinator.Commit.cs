
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
using Kahuna.Shared.Communication.Grpc;
using TransactionHandle = Kahuna.Shared.KeyValue.TransactionHandle;
using Kahuna.Server.KeyValues.Handlers;

namespace Kahuna.Server.KeyValues.Transactions;

// Commit and rollback entry points, working-set release after a decision, and thrown-commit classification.
internal sealed partial class TransactionCoordinator
{

    /// <summary>
    /// Reads the canonical durable transaction record for a handle whose in-memory session is gone, so a lost
    /// interactive durable transaction reports its true outcome instead of <c>Errored</c>: commit answers
    /// <c>Committed</c>; abort answers <c>Aborted</c> (terminal whatever its class); undecided answers
    /// <c>MustRetry</c> (recovery finishes it). The lookup is routed by the handle's anchor key to the anchor
    /// partition leader, so a record on another node is found.
    ///
    /// <para>No resident record does <b>not</b> answer Found=false: absence is not proof the transaction never
    /// committed. An in-flight durable finalize, or a commit record not yet propagated to the resolved leader,
    /// both look absent. <c>Errored</c> would tell the client a committed transaction applied nothing (an
    /// uncounted durable write). So absence answers <c>MustRetry</c>: a retry re-observes the settled record, and
    /// a permanently unknown transaction surfaces as indeterminate when the caller's bounded retries run out,
    /// never as a fabricated terminal failure. Found=false is returned only when there is no anchor.</para>
    /// </summary>
    private async Task<(bool Found, KeyValueResponseType Response, CommitRefusalClass Refusal)> TryConsultDurableTransactionRecord(TransactionHandle handle)
    {
        // No anchor: no route to a canonical record. The caller decides how to report the lost session.
        if (string.IsNullOrEmpty(handle.RecordAnchorKey))
            return (false, KeyValueResponseType.Errored, CommitRefusalClass.None);

        TransactionRecord? record;
        try
        {
            record = await manager
                .LookupDurableRecordRouted(handle.TransactionId, DurableFinalizeEpoch, handle.RecordAnchorKey, CancellationToken.None)
                .ConfigureAwait(false);
        }
        catch (PartitionNotHostedException)
        {
            // The anchor leader cannot be resolved right now, so the outcome is unproven: answer the
            // retryable unknown, not a terminal Errored.
            return (true, KeyValueResponseType.MustRetry, CommitRefusalClass.Unresolved);
        }

        // No resident record on the anchor leader: indeterminate, not terminal (see the summary). A retry
        // re-observes the truth.
        if (record is null)
            return (true, KeyValueResponseType.MustRetry, CommitRefusalClass.Unresolved);

        return record.Decision switch
        {
            TransactionDecision.Commit => (true, KeyValueResponseType.Committed, CommitRefusalClass.None),
            // Terminal whatever the class: it can never commit.
            TransactionDecision.Abort => (true, KeyValueResponseType.Aborted, CommitRefusals.FromAbortClass(record.AbortClass)),
            // Still undecided → recovery completes it.
            _ => (true, KeyValueResponseType.MustRetry, CommitRefusalClass.Unresolved)
        };
    }

    /// <summary>
    /// Commits the transaction identified by the supplied handle.
    /// The handle carries both the transaction ID (used to locate the session) and the
    /// coordinator key (used by the caller to route this request to the right node).
    /// </summary>
    public async Task<TransactionCommitAnswer> CommitTransaction(TransactionHandle handle)
    {
        // The commit may arrive by placement forwarding, but its work (staged-base validation reads, durable
        // prepare legs, the decision proposal, settlement) is new fan-out this node starts against other
        // partitions' leaders. Lift the forwarded-request loop guard so it routes; otherwise a forwarded commit
        // wedges on MustRetry because no sub-operation may leave the node.
        using ForwardedRequestScope.SuppressScope _ = ForwardedRequestScope.Suppress();

        HLCTimestamp transactionId = handle.TransactionId;

        if (!sessions.TryGetValue(transactionId, out TransactionContext? context))
        {
            // The session is gone. Within the idempotency window, a duplicate commit replays the retained
            // terminal answer.
            if (terminalOutcomes.TryGet(transactionId, out TerminalOutcomeWindow.RetainedOutcome retained))
                return retained.Outcome.ToAnswer();

            // The session was evicted, restarted, or lived on a failed node, but an all-persistent transaction's
            // outcome survives as its canonical durable record on the data partition. Consult it via the anchor
            // leader: committed → Committed; undecided or absent → MustRetry (absence proves nothing). Only a
            // handle with no routable anchor is the unknown Errored, never a conflict Aborted.
            (bool found, KeyValueResponseType durableRecord, CommitRefusalClass durableRefusal) = await TryConsultDurableTransactionRecord(handle);
            if (found)
                return new TransactionCommitAnswer(durableRecord, handle.RecordAnchorKey, durableRefusal,
                    durableRefusal == CommitRefusalClass.None ? null : "Outcome consulted from the canonical record after the session was lost");

            logger.LogWarning("Trying to commit unknown transaction {TransactionId}", transactionId);

            return new TransactionCommitAnswer(KeyValueResponseType.Errored, null, CommitRefusalClass.UnknownTransaction, "Unknown transaction: no session and no routable record anchor");
        }

        // Serialize against a concurrent commit, rollback, or reaper on the session. The owner runs the finalize
        // once; concurrent finalizers mirror its published outcome instead of racing a second 2PC.
        // Rejected means the reaper claimed the session and is between attempts: the outcome is undecided, so
        // answer MustRetry, never a definite Aborted that a stalled commit decision could later contradict. A
        // retry mirrors the reap once it re-arms, or replays the retained outcome once the reap is terminal.
        FinalizeAdmission admission = context.EnterFinalize(out FinalizeAttempt? attempt);
        if (admission == FinalizeAdmission.Rejected)
            return new TransactionCommitAnswer(KeyValueResponseType.MustRetry, null, CommitRefusalClass.Unresolved, "The session is being reaped between attempts");

        if (admission == FinalizeAdmission.Mirror)
        {
            FinalizeOutcome mirrored = await attempt!.Completion;
            return mirrored.ToAnswer();
        }

        FinalizeOutcome outcome = new(KeyValueResponseType.Aborted, null, CommitRefusalClass.Faulted, "The finalize did not publish an outcome");
        try
        {
            outcome = await ExecuteCommit(context, transactionId);

            // A terminal outcome (Committed, or a definite Aborted/Errored) finalizes the session. Retain the
            // outcome before removing the session, so a duplicate finalize always finds one of them and never
            // reports unknown. A non-terminal MustRetry (drain timeout) leaves the session live to retry.
            if (outcome.IsTerminal)
            {
                RetainTerminalOutcome(transactionId, outcome, raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId()));
                FinalizeSession(transactionId);
            }

            return outcome.ToAnswer();
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
        // Drain in-flight operations so the working set is frozen before 2PC reads it. Operations registered
        // before finalization are part of the commit; later ones are rejected. A drain timeout is a non-terminal
        // MustRetry.
        if (!await FreezeForFinalize(context))
            return new(KeyValueResponseType.MustRetry, null, CommitRefusalClass.Unresolved, "In-flight operations did not drain before the finalize");

        // The working set is frozen: capture the record anchor under the finalize fence and return it from the
        // commit. No concurrent operation can now assign the anchor between a pre-commit read and the fence.
        string? recordAnchorKey = context.RecordAnchorKey;

        try
        {
            context.Result = new() { Type = KeyValueResponseType.Set, Reason = null };

            // 2PC and cleanup read only the server-owned working set, folded from operation completions, so the
            // coordinator decides, not the client. An empty read-only transaction has no modified keys: 2PC is a
            // no-op and it commits. Its read MVCC snapshots are still cleaned in the finally.
            await TwoPhaseCommit(context, CancellationToken.None);

            if (context.Result is null)
                return new(KeyValueResponseType.Errored, recordAnchorKey, CommitRefusalClass.Faulted, "The finalize produced no result");

            // A durable finalize that recorded its decision but left participant resolution to the background
            // reports MustRetry: the decision is durable and recovery finishes the rest. Non-terminal: the session
            // stays live and retries see MustRetry until it completes.
            if (context.Result.Type is KeyValueResponseType.MustRetry)
                return new(KeyValueResponseType.MustRetry, recordAnchorKey,
                    context.Result.Refusal == CommitRefusalClass.None ? CommitRefusalClass.Unresolved : context.Result.Refusal,
                    context.Result.Reason);

            // The refusal class names the arm that refused; a result that set none threw or errored before any
            // arm could name itself.
            if (context.Result.Type is KeyValueResponseType.Aborted or KeyValueResponseType.Errored)
                return new(KeyValueResponseType.Aborted, recordAnchorKey,
                    context.Result.Refusal == CommitRefusalClass.None ? CommitRefusalClass.Faulted : context.Result.Refusal,
                    context.Result.Reason);

            logger.LogCommittedInteractiveTransaction(transactionId);

            // CommitTransaction removes the session and retains the outcome for every terminal result, Aborted
            // included.
            return new(KeyValueResponseType.Committed, recordAnchorKey, CommitTimestamp: context.DurableCommitTimestamp);
        }
        catch (KahunaAbortedException ex)
        {
            logger.LogKahunaAbortedException(ex);

            return ClassifyThrownCommit(context, recordAnchorKey, ex.Message);
        }
        catch (TaskCanceledException ex)
        {
            logger.LogTaskCanceledException(ex);

            return ClassifyThrownCommit(context, recordAnchorKey, ex.Message);
        }
        catch (OperationCanceledException ex)
        {
            logger.LogOperationCanceledException(ex);

            return ClassifyThrownCommit(context, recordAnchorKey, ex.Message);
        }
        catch (Exception ex)
        {
            logger.LogOperationCanceledException(ex);

            return ClassifyThrownCommit(context, recordAnchorKey, ex.Message);
        }
        finally
        {
            // Post-commit cleanup is best-effort: committed mutations are already durable, so Committed never
            // depends on it. Release every confirmed lock shape not finalized by 2PC and clean read MVCC
            // snapshots. No terminal answer depends on it, so AsyncRelease may run it detached.
            //
            // EXCEPT while a durable finalize attempt is unresolved (MustRetry with a decision proposal possibly
            // in flight): releasing the locks would let a conflicting writer get ahead of a commit that can still
            // land. Hold the working set until a retry resolves the attempt or the reaper/rollback fences it
            // through the record CAS.
            //
            // And EXCEPT when the commit was refused, before anything durable, because a written key is in a
            // range quiesced for a data move: that answer promises a retry under the same handle once the window
            // ends, which needs the staged writes and their locks. The staged-intent lease and the session
            // deadline bound a client that never returns; a retry after the range cut over fails its lock proof
            // and intent probe like any moved range.
            bool heldForRetry = context.UnresolvedDurableFinalize is not null
                || context.Result is { Type: KeyValueResponseType.MustRetry, Refusal: CommitRefusalClass.RangeQuiesced };

            if (!heldForRetry)
            {
                if (context.AsyncRelease)
                    _ = ReleaseWorkingSetAfterCommit(context);
                else
                    await ReleaseWorkingSetAfterCommit(context);
            }
        }
    }

    /// <summary>
    /// The commit's working-set cleanup. No commit outcome depends on it; the session is finalized whatever it
    /// answers. A release is served for locks the node holds even without quorum-confirmed leadership
    /// (<c>LocatorRouting.AdmitLockRelease</c>), so the missing confirmation that refused the commit as a lost
    /// lock does not also block cleanup of the locks left behind. A mandatory release still unacknowledged (no
    /// node could vouch for it during an election, or its leader was unreachable) is retried detached by
    /// <see cref="RetryUnacknowledgedReleases"/> until acknowledged, the lock lease ends, or the node shuts down.
    /// A lock the retry never reaches lapses with its lease; the retry's final warning explains the writers that
    /// waited.
    /// </summary>
    private async Task ReleaseWorkingSetAfterCommit(TransactionContext context)
    {
        bool acknowledged;

        try
        {
            acknowledged = await ReleaseWorkingSet(context);
        }
        catch (Exception ex)
        {
            logger.LogError("ReleaseWorkingSet after commit {TransactionId}: {Type} {Message}", context.TransactionId, ex.GetType().Name, ex.Message);
            return;
        }

        if (acknowledged)
            return;

        List<PendingLockRelease>? pending = context.TakeUnacknowledgedReleases();

        if (pending is null || pending.Count == 0)
        {
            // The release did not name what it could not acknowledge (a best-effort item failed): nothing to retry.
            logger.LogWarning(
                "Transaction {TransactionId}: the working-set release after the commit finalize was not fully acknowledged, and no mandatory lock release is left to retry",
                context.TransactionId);
            return;
        }

        _ = RetryUnacknowledgedReleases(context, pending);
    }

    /// <summary>
    /// Retries a finalized transaction's unacknowledged mandatory lock releases, backing off from
    /// <see cref="LockReleaseRetryInitialDelayMs"/>, doubling up to <see cref="LockReleaseRetryMaxDelayMs"/>.
    /// Each attempt re-resolves admission and leader through the locator, so a newly elected leader is found,
    /// and a release this node cannot vouch for is carried to the other voters. Ends when every release is
    /// acknowledged, when the transaction's latest lock lease ends (<see cref="TransactionContext.LockLeaseEndTicks"/>;
    /// the participant effect window if no lock had a lease), or on shutdown. Runs detached: the commit already
    /// answered and the session stays finalized.
    /// </summary>
    private async Task RetryUnacknowledgedReleases(TransactionContext context, List<PendingLockRelease> pending)
    {
        // The releases a retry sends are new fan-out this node starts, as the commit's own cleanup is.
        using ForwardedRequestScope.SuppressScope _ = ForwardedRequestScope.Suppress();

        long startedTicks = Environment.TickCount64;

        // A lock granted with a lease is gone once the lease ends, so the retry ends there too. A lock that
        // carried no lease (a write's intent) is bounded by the participant effect window instead.
        long retryUntil = context.LockLeaseEndTicks > 0 ? context.LockLeaseEndTicks : startedTicks + MaxParticipantEffectTtlMs;
        int delayMs = LockReleaseRetryInitialDelayMs;
        int attempts = 0;
        string ended = "the lock lease ended";

        try
        {
            while (pending.Count > 0)
            {
                if (shutdown.IsCancellationRequested)
                {
                    ended = "the node is shutting down";
                    break;
                }

                long remainingMs = retryUntil - Environment.TickCount64;
                if (remainingMs <= 0)
                    break;

                try
                {
                    await Task.Delay((int)Math.Min(delayMs, remainingMs), shutdown).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    ended = "the node is shutting down";
                    break;
                }

                delayMs = Math.Min(delayMs * 2, LockReleaseRetryMaxDelayMs);
                attempts++;

                for (int i = pending.Count - 1; i >= 0; i--)
                {
                    DurableTransactionMetrics.LockReleaseRetried();

                    if (!await TryReleasePending(context.TransactionId, pending[i]).ConfigureAwait(false))
                        continue;

                    pending.RemoveAt(i);
                    DurableTransactionMetrics.LockReleasedLate();
                }
            }
        }
        catch (Exception ex)
        {
            ended = "the retry failed";
            logger.LogError("Retrying the lock releases of transaction {TransactionId}: {Type} {Message}", context.TransactionId, ex.GetType().Name, ex.Message);
        }

        long elapsedMs = Environment.TickCount64 - startedTicks;

        if (pending.Count == 0)
        {
            logger.LogLockReleasesAcknowledgedOnRetry(context.TransactionId, attempts, elapsedMs);
            return;
        }

        DurableTransactionMetrics.CountLockReleasesLeftToLease(pending.Count);

        logger.LogLockReleasesLeftToLease(context.TransactionId, pending.Count, attempts, elapsedMs, ended);
    }

    /// <summary>One retry of one unacknowledged release, over the locator; true when the answer is proof.</summary>
    private async Task<bool> TryReleasePending(HLCTimestamp transactionId, PendingLockRelease release)
    {
        try
        {
            KeyValueResponseType type;

            switch (release.Shape)
            {
                case PendingLockShape.Point:
                    (type, _) = await manager.LocateAndTryReleaseExclusiveLock(transactionId, release.Key, release.Durability, CancellationToken.None).ConfigureAwait(false);
                    break;

                case PendingLockShape.Prefix:
                    type = await manager.LocateAndTryReleaseExclusivePrefixLock(transactionId, release.Key, release.Durability, CancellationToken.None).ConfigureAwait(false);
                    break;

                default:
                    type = await manager.LocateAndTryReleaseExclusiveRangeLock(
                        transactionId, release.Range.Prefix, release.Range.StartKey, release.Range.StartInclusive,
                        release.Range.EndKey, release.Range.EndInclusive, release.Range.Durability, CancellationToken.None).ConfigureAwait(false);
                    break;
            }

            return IsReleaseAcked(type);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogWarning("Retrying a lock release of transaction {TransactionId}: {Type} {Message}", transactionId, ex.GetType().Name, ex.Message);
            return false;
        }
    }

    /// <summary>
    /// Rolls back the transaction identified by the supplied handle.
    /// </summary>
    public async Task<KeyValueResponseType> RollbackTransaction(TransactionHandle handle)
    {
        // As in CommitTransaction: a forwarded rollback's compensating work (mutation rollbacks, lock releases
        // on participant leaders) is new fan-out from this node, not a re-forward. Lift the loop guard.
        using ForwardedRequestScope.SuppressScope _ = ForwardedRequestScope.Suppress();

        HLCTimestamp transactionId = handle.TransactionId;

        if (!sessions.TryGetValue(transactionId, out TransactionContext? context))
        {
            // Replay a retained terminal outcome within the idempotency window; otherwise unknown → Errored.
            if (terminalOutcomes.TryGet(transactionId, out TerminalOutcomeWindow.RetainedOutcome retained))
                return retained.Outcome.Type;

            // A rollback cannot override a durable commit: for a lost session with a record anchor, return the
            // canonical record's outcome (routed to the anchor leader) instead of rolling back a commit that may
            // already be durable.
            (bool found, KeyValueResponseType durableRecord, CommitRefusalClass _) = await TryConsultDurableTransactionRecord(handle);
            if (found)
                return durableRecord;

            logger.LogWarning("Trying to rollback unknown transaction {TransactionId}", transactionId);

            return KeyValueResponseType.Errored;
        }

        // Rollback contends for the same finalize slot as commit and the reaper, so a finalize never runs twice
        // on one session; a mirroring caller returns the owner's outcome. Rejected (the reaper owns the session,
        // between attempts) answers MustRetry: the reap decides the real outcome, which for a fenced durable
        // transaction can be Committed.
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

            // A terminal outcome finalizes the session. Retain it before removing the session, so a duplicate
            // rollback always finds one of them. A non-terminal MustRetry (drain timeout or incomplete mandatory
            // release) leaves the session live for a later call, or the reaper, to retry the cleanup.
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
    /// The outcome of a commit finalize that threw. A throw decides nothing. If a durable attempt is unresolved
    /// (its decision proposal was submitted but never reached a record-backed outcome), a definite Aborted would
    /// be fabricated: the proposal may already have committed on a quorum (the coordinator was cut off before
    /// the reply arrived), and that answer would be retained and replayed to every duplicate finalize, including
    /// a survivor probing this node for the session it still owns. Such a session answers from the canonical
    /// record: durable commit → Committed, durable abort → Aborted, anything else → MustRetry, which keeps the
    /// session and working set for a retry, the reaper, or a rollback to fence through the record CAS. With no
    /// unresolved durable attempt nothing durable is in flight, and the throw is a definite Aborted.
    /// </summary>
    private FinalizeOutcome ClassifyThrownCommit(TransactionContext context, string? recordAnchorKey, string thrown)
    {
        // A result an arm already named (a refused mutation, a lost lock) outranks the throw that carried it out.
        if (context.UnresolvedDurableFinalize is not { } unresolved)
            return context.Result is { Type: KeyValueResponseType.Aborted, Refusal: not CommitRefusalClass.None } named
                ? new(KeyValueResponseType.Aborted, recordAnchorKey, named.Refusal, named.Reason)
                : new(KeyValueResponseType.Aborted, recordAnchorKey, CommitRefusalClass.Faulted, "The finalize threw: " + thrown);

        DurableFinalizeResult classified = ApplyDurableOutcome(context, ClassifyFromCanonicalRecord(unresolved));

        return classified switch
        {
            DurableFinalizeResult.Committed => new(KeyValueResponseType.Committed, recordAnchorKey, CommitTimestamp: unresolved.CommitTimestamp),
            DurableFinalizeResult.Aborted => new(KeyValueResponseType.Aborted, recordAnchorKey, context.Result?.Refusal ?? CommitRefusalClass.None, context.Result?.Reason),
            _ => new(KeyValueResponseType.MustRetry, recordAnchorKey, CommitRefusalClass.Unresolved, "The finalize threw with a durable attempt unresolved: " + thrown)
        };
    }

    /// <summary>
    /// Fences an unresolved durable finalize attempt through the record CAS, answering only what the canonical
    /// record supports. The fence is replication work itself: on a cut-off coordinator that lost leadership and
    /// cannot reach the anchor leader, resolving that leader throws. That throw decides nothing, since the
    /// proposal being overruled may already be durable (a bundled commit applied on a quorum before the cut-off),
    /// so it is classified from the canonical record like a throw inside the finalize. <c>Fenced</c> reports
    /// whether the fence itself reached a record-backed outcome, so a caller counting deadline-gate rejections
    /// does not count a classified throw.
    /// </summary>
    private async Task<(DurableFinalizeOutcome Outcome, bool Fenced)> FenceUnresolvedDurableFinalize(
        DurableFinalizeInput input, HLCTimestamp opId, CancellationToken cancellationToken)
    {
        try
        {
            return (await DurableFinalizer.FenceAbandonedAsync(input, opId, cancellationToken).ConfigureAwait(false), true);
        }
        catch (Exception ex)
        {
            DurableFinalizeOutcome classified = ClassifyFromCanonicalRecord(input);

            logger.LogWarning(ex,
                "Fencing the unresolved durable finalize of transaction {TransactionId} threw; classified from the canonical record as {Outcome}",
                input.TransactionId, classified.Result);

            return (classified, false);
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

        // A prior commit attempt may have left a durable decision proposal in flight (replication timed out:
        // indeterminate, not failed). Before claiming RolledBack, install a durable Abort through the record CAS.
        // If the stalled commit already won, Committed is the only truthful answer; if the abort wins, any late
        // commit is fenced out for good.
        DurableFinalizeInput? unresolved = context.UnresolvedDurableFinalize;
        if (unresolved is not null)
        {
            (DurableFinalizeOutcome fenced, _) = await FenceUnresolvedDurableFinalize(
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

        // Rollback runs no prepare: from the server-owned working set it clears staged (un-prepared) writes,
        // releases every confirmed lock shape, and cleans read MVCC. RolledBack must not promise cleanup that
        // did not complete, so cleanup is synchronous (never AsyncRelease) and RolledBack is reported only when
        // every mandatory (intent-bearing) release acknowledged. Otherwise the caller retries; the released
        // finalize slot lets a later call re-run.
        if (!await ReleaseWorkingSet(context))
            return new(KeyValueResponseType.MustRetry, null);

        logger.LogRolledBackInteractiveTransaction(transactionId);

        AccountStagedChainBreakNotCommitted(context, transactionId, "rolled back");

        // RollbackTransaction removes the session and retains the terminal RolledBack outcome.
        return new(KeyValueResponseType.RolledBack, null);
    }

    /// <summary>
    /// A transaction that recorded a staging loss and ends without a commit attempt has no abort to match the
    /// loss counter. Count and log it once per session at its terminal rollback or reap, so metrics can pair
    /// every break with its outcome.
    /// </summary>
    private void AccountStagedChainBreakNotCommitted(TransactionContext context, HLCTimestamp transactionId, string outcome)
    {
        if (context.StagedChainBreak is not { } stagedChainBreak)
            return;

        DurableTransactionMetrics.StagedChainBreakRollbacks.Add(1);
        logger.LogWarning("Transaction {TransactionId} was {Outcome} after it lost a staging: {Break}", transactionId, outcome, stagedChainBreak);
    }
}
