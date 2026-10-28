
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

// Session range-lock renewal and the reaper for abandoned sessions.
internal sealed partial class TransactionCoordinator
{

    /// <summary>
    /// Extends the range locks of every live session past their acquire TTL without client heartbeats. For each
    /// session not past its reap deadline, re-acquires each recorded range lock with the same bounds and mode and
    /// the server renewal TTL; the participant treats a same-transaction, same-bounds re-acquire as a renewal of
    /// the expiry, not a second lock. A session past <c>Timeout + grace</c> is skipped so its locks lapse and the
    /// reaper reclaims it. A session whose cleanup has started yields an empty snapshot. Each dispatched acquire
    /// registers with its session; cleanup closes that admission and drains admitted renewals before releasing
    /// locks, so a delayed renewal cannot re-create a finalized transaction's lock. The sweep has a deadline
    /// (<c>RangeLockRenewalTtlMs / 2</c>) and bounded concurrency; the deadline token goes into each acquire so a
    /// slow participant is cancelled at the budget instead of blocking the reaper or overrunning the next tick.
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

            // Past its reap deadline: leave it to the reaper. Renewing would only extend locks about to be released.
            HLCTimestamp deadline = pair.Key + (context.Timeout + ReapGraceMs);
            if ((deadline - now) <= TimeSpan.Zero)
                continue;

            // Empty once cleanup closed renewal admission (cleanup owns the locks). A Finalizing session still
            // draining returns its locks, so the predicate lock does not lapse mid-finalize.
            List<(RangeLockKey Range, RangeLockMode Mode)> renewable = context.SnapshotRenewableRangeLocks();
            foreach ((RangeLockKey range, RangeLockMode mode) in renewable)
                workList.Add((context, range, mode));
        }

        if (workList.Count == 0)
            return;

        // Sweep deadline: half the renewal TTL. A lock renewed at the start of the prior tick has about half its
        // TTL left, so renewing within this budget keeps it alive. Unfinished renewals are abandoned this tick
        // (lapsing only if already near expiry) and retried next tick. The abandoned count is logged so
        // truncation is visible.
        int sweepDeadlineMs = Math.Max(1, renewalTtlMs / 2);
        int concurrencyLimit = Math.Min(16, Environment.ProcessorCount);

        using CancellationTokenSource sweepCts = new(sweepDeadlineMs);
        CancellationToken sweepToken = sweepCts.Token;
        using SemaphoreSlim concurrencyGate = new(concurrencyLimit, concurrencyLimit);
        int[] abandoned = [0];

        async Task RenewOne(TransactionContext ctx, RangeLockKey range, RangeLockMode mode)
        {
            bool acquired = false;
            bool renewalStarted = false;
            try
            {
                await concurrencyGate.WaitAsync(sweepToken).ConfigureAwait(false);
                acquired = true;
                if (!ctx.TryBeginRangeRenewal())
                    return;
                renewalStarted = true;

                // The sweep token cancels a slow participant at the budget instead of blocking Task.WhenAll (and
                // the reaper) indefinitely.
                //
                // A renewal that meets a new leader is answered as a fresh grant: the old lock was dropped with
                // the old leadership, and this request silently plants a new one. The grant's term exposes that,
                // so it is folded like any other grant and the session's commit is refused (see
                // TransactionContext.LockGrantTermChange).
                LockGrantCapture grants;
                using (LockGrantScope.Begin(out grants))
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

                ctx.RecordLockGrantTerms(grants.Take());
            }
            catch (OperationCanceledException)
            {
                // Deadline elapsed while queued or mid-acquire: abandoned this tick, retried next (lapses only
                // if already near expiry).
                Interlocked.Increment(ref abandoned[0]);
            }
            catch (Exception ex)
            {
                logger.LogError("RenewSessionRangeLocks {Prefix}: {Type} {Message}", range.Prefix, ex.GetType().Name, ex.Message);
            }
            finally
            {
                if (renewalStarted)
                    ctx.EndRangeRenewal();
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

            // A session claimed by a prior reap whose cleanup did not fully release retries every sweep,
            // regardless of deadline, until every mandatory release acknowledges. A null resume means an
            // in-flight reap still holds the slot, so skip.
            if (context.Lifecycle == SessionLifecycle.Reaping)
            {
                FinalizeAttempt? resume = context.TryResumeReap();
                if (resume is not null)
                    await ReapSession(pair.Key, context, resume, now);

                continue;
            }

            // With unresolved dispatched operations, a session is not abandoned until the extended deadline
            // (timeout + grace + max participant effect TTL), so the reaper never cancels an operation that
            // could still land an effect at a participant.
            int window = context.Timeout + ReapGraceMs;
            if (context.HasPendingOperations)
                window += MaxParticipantEffectTtlMs;

            HLCTimestamp deadline = pair.Key + window;

            if ((deadline - now) > TimeSpan.Zero)
                continue;

            // Claim the finalize slot and close the session to new operations before removing it from the map.
            // This reclaims both an abandoned session still accepting operations and an abandoned finalization:
            // one left Finalizing after a commit/rollback/Close returned MustRetry or stored a Close snapshot and
            // the caller vanished, leaving no finalize owner. A BeginOperation that already captured this context
            // sees the closed lifecycle and is rejected instead of registering on a vanishing session.
            // TryEnterReap returns null while a commit or rollback owns the slot; that finalize decides.
            FinalizeAttempt? attempt = context.TryEnterReap();
            if (attempt is null)
                continue;

            await ReapSession(pair.Key, context, attempt, now);
        }
    }

    /// <summary>
    /// Reclaims one abandoned session whose finalize slot this tick owns. Releases its confirmed working set and
    /// decides the outcome published to any racing finalizer:
    /// <list type="bullet">
    /// <item>all mandatory releases acknowledged, no pending operation → terminal
    /// <see cref="KeyValueResponseType.RolledBack"/>; session removed, outcome retained.</item>
    /// <item>an operation still unresolved after the extended deadline → session <b>expired</b> without
    /// reporting RolledBack: removed, with an unknown <see cref="KeyValueResponseType.Errored"/> retained. The
    /// operation is never cancelled; a late effect finds no session, is not acknowledged, and expires at the
    /// participant on its intent lease.</item>
    /// <item>a mandatory release could not complete (transient) → non-terminal
    /// <see cref="KeyValueResponseType.MustRetry"/>: the session stays <see cref="SessionLifecycle.Reaping"/>
    /// and a later sweep retries cleanup.</item>
    /// <item>an unresolved durable finalize whose record CAS fence finds the commit won → terminal
    /// <see cref="KeyValueResponseType.Committed"/>; session removed, outcome retained.</item>
    /// <item>the fence cannot be installed → MustRetry with the working set kept, so a stalled commit cannot
    /// interleave with new writers.</item>
    /// </list>
    /// </summary>
    private async Task ReapSession(HLCTimestamp transactionId, TransactionContext context, FinalizeAttempt attempt, HLCTimestamp now)
    {
        FinalizeOutcome outcome = new(KeyValueResponseType.MustRetry, null);
        try
        {
            context.Action = KeyValueTransactionAction.Abort;

            logger.LogWarning("Reaping abandoned interactive transaction {TransactionId}", transactionId);

            // An abandoned durable finalize may still have a decision proposal in flight: a replication timeout is
            // indeterminate, and a stalled [record init + prepare + commit] bundle can apply after a partition
            // heals. Before claiming RolledBack, install a durable Abort through the record CAS and accept the
            // record's answer. An abort (including a tombstone from absence) permanently rejects any late commit.
            // A commit that already won means the transaction COMMITTED; report that, not a contradicting rollback.
            DurableFinalizeInput? unresolved = context.UnresolvedDurableFinalize;
            if (unresolved is not null)
            {
                (DurableFinalizeOutcome fenced, _) = await FenceUnresolvedDurableFinalize(unresolved, now, CancellationToken.None);

                if (fenced.Result == DurableFinalizeResult.MustRetry)
                    // The fence could not be installed (replication unavailable), so the outcome is still
                    // indeterminate. Keep the working set (releasing it would widen the window for a stalled
                    // commit to interleave with new writers) and retry on a later sweep.
                    return;

                context.UnresolvedDurableFinalize = null;

                if (fenced.Result == DurableFinalizeResult.Committed)
                {
                    // The stalled decision won: the transaction is durably committed and resolved. Release the
                    // rest of the working set (idempotent for keys the commit apply cleaned) and retain Committed
                    // so a late duplicate commit/rollback replays it.
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
                // Operations still unresolved after the extended deadline: give up without reporting a rollback
                // of work whose fate is unknown. Late effects get no acknowledgement and lapse at their participant.
                outcome = new(KeyValueResponseType.Errored, null);
            else if (cleaned)
                outcome = new(KeyValueResponseType.RolledBack, null);
            // else: cleanup incomplete — keep the session Reaping and retry on a later sweep (MustRetry).

            // Retain the terminal outcome before removing the session, so a duplicate finalize never sees a gap
            // with neither session nor record. MustRetry retains nothing and leaves the session for the next sweep.
            if (outcome.IsTerminal)
            {
                AccountStagedChainBreakNotCommitted(context, transactionId, "reaped");
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
}
