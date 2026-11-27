
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text;
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

// Per-operation registration and completion, working-set snapshots, session close, and retained terminal outcomes.
internal sealed partial class TransactionCoordinator
{

    /// <summary>
    /// Registers an operation under the session <paramref name="transactionId"/> for idempotent tracking.
    /// The <see cref="OperationRegistrationResult"/> says whether it is new, in flight, or already completed
    /// with a cached response.
    /// </summary>
    public OperationRegistrationResult BeginOperation(
        HLCTimestamp transactionId,
        TransactionOperationId operationId,
        OperationKind kind,
        byte[]? payloadDigest,
        int itemCount = 0)
    {
        if (!sessions.TryGetValue(transactionId, out TransactionContext? context))
            return new(OperationRegistrationOutcome.RejectedSessionClosed);

        return context.BeginOperation(operationId, kind, payloadDigest, itemCount);
    }

    /// <summary>
    /// Marks the operation completed, storing the response so duplicates get the same answer without
    /// re-executing, and folds its confirmed effect into the coordinator working set. Returns the record anchor
    /// (null if no persistent write yet). Throws when the session is gone: the effect cannot be folded, so the
    /// caller must not report success.
    /// </summary>
    public string? CompleteOperation(HLCTimestamp transactionId, TransactionOperationId operationId, OperationCompletionPayload? payload, object? response)
    {
        if (sessions.TryGetValue(transactionId, out TransactionContext? context))
        {
            string? anchor = context.CompleteOperation(operationId, payload, response, out bool discardedEffects);

            // Confirmed effects for a registration a transient result cancelled while the apply was in flight.
            // The participant applied a mutation this transaction can never own, and the session may finalize
            // without it, so log an error. The metric pairs with this line for attribution.
            if (discardedEffects)
                logger.LogError(
                    "Discarding effect-bearing completion of operation {OperationId} for transaction {TransactionId}: its registration was cancelled while the participant apply was in flight",
                    operationId, transactionId);

            return anchor;
        }

        // The session is gone (reaped, aborted, or never existed). Throw rather than return a null anchor,
        // which reads as success: a participant that applied the operation must not claim a write the working
        // set never got. The caller sees MustRetry; the retry finds no session and aborts.
        throw new KahunaServerException($"No transaction session {transactionId} to complete operation {operationId}.");
    }

    /// <summary>
    /// Cancels the specified operation if it is still in the pending state.
    /// </summary>
    public bool CancelOperation(HLCTimestamp transactionId, TransactionOperationId operationId) =>
        CancelPendingOperation(transactionId, operationId, onlyOnceClosed: false) == OperationCancelOutcome.Cancelled;

    /// <summary>
    /// Cancels the specified operation if it is still pending, and says what it did; see
    /// <see cref="TransactionContext.CancelPendingOperation"/>. <see cref="OperationCancelOutcome.NoSession"/> when
    /// this node holds no session for the transaction.
    /// </summary>
    internal OperationCancelOutcome CancelPendingOperation(HLCTimestamp transactionId, TransactionOperationId operationId, bool onlyOnceClosed)
    {
        if (!sessions.TryGetValue(transactionId, out TransactionContext? context))
            return OperationCancelOutcome.NoSession;

        return context.CancelPendingOperation(operationId, onlyOnceClosed);
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
    /// Waits for a closed session's in-flight operations to drain, so a later two-phase commit / rollback
    /// reads a stable working set. Returns false on the drain deadline; the session stays closed so the
    /// caller retries and rejoins the same drain.
    /// </summary>
    private async Task<bool> FreezeForFinalize(TransactionContext context, FinalizeDrainPhase phase)
    {
        // The caller owns the finalize slot, which closed the session to new operations on install
        // (Accepting→Finalizing). Only wait for in-flight ones; a retry after a timeout rejoins this drain.
        using CancellationTokenSource cts = new(context.Timeout <= 0 ? configuration.DefaultTransactionTimeout : context.Timeout);
        try
        {
            await DrainPendingOperations(context, phase, cts.Token);
            return true;
        }
        catch (OperationCanceledException)
        {
            // Drain deadline hit. Do NOT reopen the session: no operation may join once finalization began.
            // The caller retries and rejoins this drain.
            return false;
        }
    }

    /// <summary>
    /// Closes a session to new operations, drains in-flight ones, and stores the immutable working-set snapshot
    /// a later commit or rollback finalizes against (CamusDB calls Close to publish cache keyspaces before
    /// committing). Close shares the single finalize slot with commit/rollback, so they never run concurrently
    /// on one transaction. Unlike them, Close does <b>not</b> decide the transaction: the session stays
    /// <see cref="SessionLifecycle.Finalizing"/> (finalizable, never terminal) and the slot is released for a
    /// later commit/rollback. A repeat Close returns the same snapshot.
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

        // Take the finalize slot so Close and commit/rollback never touch the working set at once.
        // Rejected means the reaper or a terminal state owns the session: nothing to close.
        FinalizeAdmission admission = context.EnterFinalize(out FinalizeAttempt? attempt);
        if (admission == FinalizeAdmission.Rejected)
            return context.CloseSnapshot is { } published
                ? (KeyValueResponseType.Set, published)
                : (KeyValueResponseType.MustRetry, null);

        if (admission == FinalizeAdmission.Mirror)
        {
            // Another finalize owns the slot; wait for it. A concurrent Close leaves its snapshot; a deciding
            // commit/rollback leaves none, so the caller retries.
            await attempt!.Completion;
            return context.CloseSnapshot is { } published
                ? (KeyValueResponseType.Set, published)
                : (KeyValueResponseType.MustRetry, null);
        }

        // Owner: drain, snapshot, store, then release the slot with a non-terminal outcome. The session stays
        // Finalizing (closed, still finalizable) for a later commit/rollback.
        try
        {
            using CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            cts.CancelAfter(context.Timeout <= 0 ? configuration.DefaultTransactionTimeout : context.Timeout);

            try
            {
                await DrainPendingOperations(context, FinalizeDrainPhase.Close, cts.Token);
            }
            catch (OperationCanceledException)
            {
                // Drain deadline hit. Release the slot (MustRetry keeps the session Finalizing and closed) so a
                // later Close rejoins the drain. Never snapshot an undrained set.
                context.CompleteFinalize(attempt!, new FinalizeOutcome(KeyValueResponseType.MustRetry, null));
                return (KeyValueResponseType.MustRetry, null);
            }

            WorkingSetSnapshot snapshot = context.GetWorkingSetSnapshot();
            context.StoreCloseSnapshot(snapshot);

            // Release the slot without deciding. A commit/rollback that mirrored this Close gets MustRetry and
            // retries as owner; a mirroring Close sees the stored snapshot.
            context.CompleteFinalize(attempt!, new FinalizeOutcome(KeyValueResponseType.MustRetry, null));

            return (KeyValueResponseType.Set, context.CloseSnapshot ?? snapshot);
        }
        catch
        {
            context.CompleteFinalize(attempt!, new FinalizeOutcome(KeyValueResponseType.MustRetry, null));
            throw;
        }
    }

    // ---- slow finalize drains ----

    /// <summary>A finalize drain that still waits after this many milliseconds logs the operations it waits for.</summary>
    internal const int SlowDrainReportMs = 2_000;

    /// <summary>The shortest interval, in milliseconds, between two slow-drain logs of this node.</summary>
    private const int SlowDrainLogIntervalMs = 1_000;

    /// <summary>The most pending operations one slow-drain log names.</summary>
    private const int SlowDrainLogMaxOperations = 8;

    private long nextSlowDrainLogTicks;

    private int suppressedSlowDrainLogs;

    /// <summary>
    /// Waits for the session's pending operations, as <see cref="TransactionContext.WaitForPendingOperations"/> does.
    /// A drain that still waits after <see cref="SlowDrainReportMs"/> is counted, and its pending operations (id,
    /// kind, age) are logged once, at most once a second for this node. A pending operation that nobody completes
    /// holds the drain until its deadline, and the log names it, so it can be matched with the participant's
    /// warning for the same operation id. A drain with nothing pending returns at once and allocates nothing.
    /// </summary>
    private async Task DrainPendingOperations(TransactionContext context, FinalizeDrainPhase phase, CancellationToken cancellationToken)
    {
        Task drained = context.WaitForPendingOperations(cancellationToken);

        if (!drained.IsCompleted)
        {
            long started = Environment.TickCount64;

            try
            {
                await drained.WaitAsync(TimeSpan.FromMilliseconds(SlowDrainReportMs), CancellationToken.None).ConfigureAwait(false);
                return;
            }
            catch (TimeoutException)
            {
                ReportSlowDrain(context, phase, Environment.TickCount64 - started);
            }
        }

        await drained.ConfigureAwait(false);
    }

    private void ReportSlowDrain(TransactionContext context, FinalizeDrainPhase phase, long waitedMs)
    {
        DurableTransactionMetrics.CountSlowFinalizeDrain(phase);

        long now = Environment.TickCount64;
        long next = Volatile.Read(ref nextSlowDrainLogTicks);

        if (now < next || Interlocked.CompareExchange(ref nextSlowDrainLogTicks, now + SlowDrainLogIntervalMs, next) != next)
        {
            Interlocked.Increment(ref suppressedSlowDrainLogs);
            return;
        }

        int suppressed = Interlocked.Exchange(ref suppressedSlowDrainLogs, 0);
        (List<PendingOperationInfo> pending, int total) = context.SnapshotPendingOperations(SlowDrainLogMaxOperations);

        StringBuilder text = new();
        foreach (PendingOperationInfo operation in pending)
        {
            if (text.Length > 0)
                text.Append(", ");
            text.Append(operation.OperationId).Append(' ').Append(operation.Kind).Append(' ').Append(operation.AgeMs).Append(" ms");
        }

        if (total > pending.Count)
            text.Append(" and ").Append(total - pending.Count).Append(" more");

        logger.LogFinalizeDrainWaits(context.TransactionId, FinalizeDrainPhases.Name(phase), total, waitedMs, text.ToString(), suppressed);
    }

    // ---- terminal outcome retention (idempotency window) ----

    /// <summary>
    /// Records a terminal outcome so a duplicate finalize after the session is removed replays the same answer.
    /// A non-terminal MustRetry is skipped: the session stays live, so there is nothing to replay.
    /// <c>TransactionOutcomeRetentionMax</c> caps the window: each retain evicts oldest-first back to the cap
    /// before returning, so only retains in flight exceed it (see <see cref="TerminalOutcomeWindow.Retain"/>).
    /// A non-positive max <b>disables retention</b>: a duplicate after removal reports an unknown
    /// <see cref="KeyValueResponseType.Errored"/>, never a conflict Aborted.
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
    /// Prunes retained outcomes older than the idempotency window. Runs on each reaper sweep so age, not only
    /// size, bounds the window. A non-positive TTL disables age pruning. Does not block concurrent retains or
    /// evictions (see <see cref="TerminalOutcomeWindow.PruneExpired"/>).
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
    /// Server-owned lease, in ms, given to a live session's range locks on each renewal tick. Twice the renewal
    /// cadence (the reaper's <see cref="KahunaConfiguration.CollectionInterval"/>), so one missed tick cannot let
    /// a lock lapse while its session lives. The coordinator, not client heartbeats, owns range-lock liveness:
    /// a client acquires a range lock once with any TTL and never renews it.
    /// </summary>
    private int RangeLockRenewalTtlMs => (int)Math.Max(configuration.CollectionInterval.TotalMilliseconds * 2, 1);
}
