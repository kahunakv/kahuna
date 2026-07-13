
using System.Collections.Concurrent;
using DotNext;
using Kommander;
using Kommander.Time;

using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues.Logging;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using TransactionHandle = Kahuna.Shared.KeyValue.TransactionHandle;

namespace Kahuna.Server.KeyValues.Transactions;

/// <summary>
/// Manages interactive transaction sessions and the generic 2PC lifecycle.
/// Has no dependency on the script parser or AST. Script execution uses
/// <see cref="ScriptTransactionExecutor"/> which composes this coordinator.
/// </summary>
internal sealed class TransactionCoordinator
{
    /// <summary>
    /// Grace added on top of a session's own timeout before the reaper reclaims it. Kept at or above
    /// the write-intent TTL (DefaultTxCompleteTimeout, 15 s) so that by the time a session is reaped
    /// any commit it could still attempt would already fail on expired intents.
    /// </summary>
    private const int ReapGraceMs = 15_000;

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

    public TransactionCoordinator(KeyValuesManager manager, KahunaConfiguration configuration, IRaft raft, ILogger<IKahuna> logger)
    {
        this.manager = manager;
        this.configuration = configuration;
        this.raft = raft;
        this.logger = logger;
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

        // A fixed read snapshot and write-skew validation are mutually exclusive: pinning reads to a past
        // timestamp cannot detect concurrent writes that land after that snapshot, so the two together would
        // silently promise a guarantee the engine cannot honor. Reject the combination up front.
        if (options.ReadTimestamp != HLCTimestamp.Zero && options.ReadValidation == ReadValidation.TrackAndValidate)
            return (KeyValueResponseType.InvalidInput, new TransactionHandle(HLCTimestamp.Zero, coordinatorKey));

        bool added;
        HLCTimestamp transactionId;

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
                Action             = KeyValueTransactionAction.Commit,
                AsyncRelease       = options.AsyncRelease,
                Timeout            = options.Timeout <= 0 ? configuration.DefaultTransactionTimeout : options.Timeout
            };

            added = sessions.TryAdd(transactionId, context);

        } while (!added);

        logger.LogStartedInteractiveTransaction(transactionId);

        await Task.CompletedTask;

        return (KeyValueResponseType.Set, new TransactionHandle(transactionId, coordinatorKey));
    }

    /// <summary>
    /// Commits the transaction identified by the supplied handle.
    /// The handle carries both the transaction ID (used to locate the session) and the
    /// coordinator key (used by the caller to route this request to the right node).
    /// </summary>
    public async Task<(KeyValueResponseType, string?)> CommitTransaction(TransactionHandle handle)
    {
        HLCTimestamp transactionId = handle.TransactionId;

        if (!sessions.TryGetValue(transactionId, out TransactionContext? context))
        {
            logger.LogWarning("Trying to commit unknown transaction {TransactionId}", transactionId);

            return (KeyValueResponseType.Errored, null);
        }

        // Serialize against a concurrent commit, rollback, or reaper on the same session. The owner runs the
        // finalize once; any concurrent finalizer mirrors the owner's published outcome rather than racing a
        // second two-phase commit against the same working set.
        FinalizeAdmission admission = context.EnterFinalize(out FinalizeAttempt? attempt);
        if (admission == FinalizeAdmission.Rejected)
            return (KeyValueResponseType.Aborted, null);

        if (admission == FinalizeAdmission.Mirror)
        {
            FinalizeOutcome mirrored = await attempt!.Completion;
            return (mirrored.Type, mirrored.RecordAnchorKey);
        }

        FinalizeOutcome outcome = new(KeyValueResponseType.Aborted, null);
        try
        {
            outcome = await ExecuteCommit(context, transactionId);
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

            if (context.Result.Type is KeyValueResponseType.Aborted or KeyValueResponseType.Errored)
                return new(KeyValueResponseType.Aborted, recordAnchorKey);

            logger.LogCommittedInteractiveTransaction(transactionId);

            sessions.TryRemove(transactionId, out _);

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
            if (context.AsyncRelease)
                _ = ReleaseWorkingSet(context);
            else
                await ReleaseWorkingSet(context);
        }
    }

    /// <summary>
    /// Rolls back the transaction identified by the supplied handle.
    /// </summary>
    public async Task<KeyValueResponseType> RollbackTransaction(TransactionHandle handle)
    {
        HLCTimestamp transactionId = handle.TransactionId;

        if (!sessions.TryGetValue(transactionId, out TransactionContext? context))
        {
            logger.LogWarning("Trying to rollback unknown transaction {TransactionId}", transactionId);

            return KeyValueResponseType.Errored;
        }

        // Rollback contends for the same finalize slot as commit and the reaper, so a concurrent finalize is
        // never run twice on one session; a mirroring caller returns the owner's outcome.
        FinalizeAdmission admission = context.EnterFinalize(out FinalizeAttempt? attempt);
        if (admission == FinalizeAdmission.Rejected)
            return KeyValueResponseType.Aborted;

        if (admission == FinalizeAdmission.Mirror)
        {
            FinalizeOutcome mirrored = await attempt!.Completion;
            return mirrored.Type;
        }

        FinalizeOutcome outcome = new(KeyValueResponseType.Aborted, null);
        try
        {
            outcome = await ExecuteRollback(context, transactionId);
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

        // Rollback runs no prepare: it clears the transaction's staged (un-prepared) writes, releases every
        // confirmed lock shape, and cleans its read MVCC — all from the server-owned working set. Cleanup is
        // synchronous and RolledBack is reported only when every mandatory (intent-bearing) release
        // acknowledged; otherwise the caller retries (the released finalize slot lets a later call re-run).
        // RolledBack must never promise cleanup that did not complete, so it is never dispatched via
        // AsyncRelease.
        if (!await ReleaseWorkingSet(context))
            return new(KeyValueResponseType.MustRetry, null);

        logger.LogRolledBackInteractiveTransaction(transactionId);

        sessions.TryRemove(transactionId, out _);

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
    public string? CompleteOperation(HLCTimestamp transactionId, TransactionOperationId operationId, OperationEffect? effect, object? response)
    {
        if (sessions.TryGetValue(transactionId, out TransactionContext? context))
            return context.CompleteOperation(operationId, effect, response);

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
    /// Transitions the session to <see cref="SessionLifecycle.Finalizing"/>, waits for all
    /// in-flight operations to drain, captures a working-set snapshot, and transitions to
    /// <see cref="SessionLifecycle.Terminal"/>. Callers use the returned snapshot to drive the
    /// 2PC prepare/commit/rollback steps independently of the live context.
    /// </summary>
    /// <summary>
    /// Closes a session to new operations and waits for in-flight ones to drain, so a subsequent
    /// two-phase commit / rollback reads a stable coordinator-owned working set. Returns false (and
    /// reopens the session if this call was the one that closed it) when the drain deadline elapses.
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

    public async Task<(KeyValueResponseType, WorkingSetSnapshot?)> CloseTransaction(
        HLCTimestamp transactionId,
        CancellationToken cancellationToken)
    {
        if (!sessions.TryGetValue(transactionId, out TransactionContext? context))
            return (KeyValueResponseType.Errored, null);

        if (!context.TryBeginFinalizing())
        {
            // Not the first to close. If a finalize already completed, hand back its published snapshot.
            WorkingSetSnapshot? published = context.PublishedSnapshot;
            if (published is not null)
                return (KeyValueResponseType.Set, published);

            // A finalize is still in progress (this is a retry after a prior drain timeout, or a
            // concurrent close): the session stays Finalizing and closed to new operations, so fall
            // through and rejoin the same pending drain rather than reopening it. Any other state
            // (reaping/terminal-without-snapshot) is not ours to close — ask the caller to retry.
            if (context.Lifecycle != SessionLifecycle.Finalizing)
                return (KeyValueResponseType.MustRetry, null);
        }

        using CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(context.Timeout);

        try
        {
            await context.WaitForPendingOperations(cts.Token);
        }
        catch (OperationCanceledException)
        {
            // Drain deadline hit. Keep the session closed — do NOT reopen it to new operations — so a
            // later Close rejoins this same pre-close pending set instead of admitting new work.
            return (KeyValueResponseType.MustRetry, null);
        }

        WorkingSetSnapshot snapshot = context.GetWorkingSetSnapshot();
        context.PublishTerminal(snapshot);

        return (KeyValueResponseType.Set, snapshot);
    }

    // ---- session reaper ----

    /// <summary>
    /// Reclaims interactive sessions that were started but never committed or rolled back —
    /// e.g. the client crashed, timed out, or dropped its connection.
    /// </summary>
    public async Task ReapAbandonedSessions()
    {
        if (sessions.IsEmpty)
            return;

        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        foreach (KeyValuePair<HLCTimestamp, TransactionContext> pair in sessions)
        {
            TransactionContext context = pair.Value;

            if (context.State is KeyValueTransactionState.Preparing
                or KeyValueTransactionState.Committing
                or KeyValueTransactionState.RollingBack)
                continue;

            HLCTimestamp deadline = pair.Key + (context.Timeout + ReapGraceMs);

            if ((deadline - now) > TimeSpan.Zero)
                continue;

            // Claim the finalize slot and close the session to new operations before removing it from the
            // map. A BeginOperation that already captured this context reference then observes the closed
            // lifecycle and is rejected, instead of registering a New operation on a session that is about
            // to vanish (whose completion would find no session and leave an applied mutation with no
            // coordinator record). Skip a session already owned by an in-flight commit or rollback — that
            // finalize, not the reaper, decides its outcome.
            FinalizeAttempt? attempt = context.TryEnterReap();
            if (attempt is null)
                continue;

            // Publish the reap outcome to any commit/rollback that races in and mirrors this attempt, so it
            // learns the abandoned session was rolled back rather than finalizing a vanishing session.
            FinalizeOutcome outcome = new(KeyValueResponseType.RolledBack, null);
            try
            {
                if (!sessions.TryRemove(pair.Key, out _))
                {
                    outcome = new(KeyValueResponseType.Errored, null);
                    continue;
                }

                context.Action = KeyValueTransactionAction.Abort;

                logger.LogWarning("Reaping abandoned interactive transaction {TransactionId}", pair.Key);

                await ReleaseWorkingSet(context);
            }
            finally
            {
                context.CompleteFinalize(attempt, outcome);
            }
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

                    foreach ((KeyValueResponseType type, string key, KeyValueDurability durability) in results)
                    {
                        if (mandatoryKeys.Contains((key, durability)) && !IsReleaseAcked(type))
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
    /// Whether a lock/MVCC release response counts as an acknowledgement. Only transient conditions
    /// (a participant still catching up, or a not-yet-stable leader) leave state uncleared and warrant a
    /// retry; every other response — including "nothing was there" — means the release is settled.
    /// </summary>
    private static bool IsReleaseAcked(KeyValueResponseType type) =>
        type is not (KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication);

    /// <summary>
    /// Executes the 2PC protocol for the transaction.
    /// </summary>
    internal async Task TwoPhaseCommit(TransactionContext context, CancellationToken cancellationToken)
    {
        if (context.LocksAcquired is null || context.ModifiedKeys is null || context.ModifiedKeys.Count == 0)
            return;

        if (!await ValidateReadSet(context, cancellationToken))
            return;

        // Place write intents before checking for concurrent writers on the read set so that a
        // racing peer's conflict check sees them and aborts — preventing write-skew anomalies.
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

        if (!await CheckReadSetForConflicts(context, cancellationToken))
        {
            if (context.AsyncRelease)
                _ = RollbackMutations(context, mutationsPrepared, CancellationToken.None);
            else
                await RollbackMutations(context, mutationsPrepared, cancellationToken);
            return;
        }

        await CommitMutations(context, mutationsPrepared, cancellationToken);
    }

    /// <summary>
    /// Whether the transaction's read-set must be validated for write-skew before prepare. Optimistic locking
    /// implies validation for backward compatibility; an explicit <see cref="ReadValidation.TrackAndValidate"/>
    /// policy requests it regardless of locking mode.
    /// </summary>
    private static bool RequiresReadSetValidation(TransactionContext context)
    {
        return context.Locking == KeyValueTransactionLocking.Optimistic
            || context.ReadValidation == ReadValidation.TrackAndValidate;
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
    /// Probes the read-set for concurrent writers (write-skew guard) — optimistic transactions only.
    /// </summary>
    private async Task<bool> CheckReadSetForConflicts(TransactionContext context, CancellationToken cancellationToken)
    {
        if (!RequiresReadSetValidation(context) || context.ReadKeys is null || context.ReadKeys.Count == 0)
            return true;

        List<KeyValueTransactionReadKey> toCheck = [];

        foreach (KeyValueTransactionReadKey readKey in context.ReadKeys.Values)
        {
            if (string.IsNullOrEmpty(readKey.Key))
                continue;

            if (context.ModifiedKeys is not null && context.ModifiedKeys.Contains((readKey.Key, readKey.Durability)))
                continue;

            toCheck.Add(readKey);
        }

        if (toCheck.Count == 0)
            return true;

        Task<KeyValueResponseType>[] tasks = new Task<KeyValueResponseType>[toCheck.Count];

        for (int i = 0; i < toCheck.Count; i++)
        {
            KeyValueTransactionReadKey readKey = toCheck[i];
            tasks[i] = manager.LocateAndTryCheckWriteIntent(
                context.TransactionId,
                readKey.Key!,
                readKey.Durability,
                cancellationToken
            );
        }

        KeyValueResponseType[] results = await Task.WhenAll(tasks);

        for (int i = 0; i < results.Length; i++)
        {
            if (results[i] != KeyValueResponseType.Aborted)
                continue;

            string key = toCheck[i].Key!;

            context.Result = new()
            {
                Type = KeyValueResponseType.Aborted,
                Reason = $"Concurrent write intent detected on read key {key}"
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
        CancellationToken cancellationToken
    )
    {
        if (context.LocksAcquired is null || context.ModifiedKeys is null || context.ModifiedKeys.Count == 0)
            return (false, null);

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

        if (context.ModifiedKeys.Count == 1)
        {
            (string key, KeyValueDurability durability) = context.ModifiedKeys.First();

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
            context.ModifiedKeys.ToList(),
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

        for (int attempt = 0; attempt <= MaxPhase2Retries && pendingCommits.Count > 0; attempt++)
        {
            if (attempt > 0 && !await DelayBetweenRetries(cancellationToken))
                break;

            List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)> batch =
                [.. pendingCommits.Select(kvp => (kvp.Key, kvp.Value.ticketId, kvp.Value.durability))];

            List<(KeyValueResponseType, string, long, KeyValueDurability)> responses =
                await manager.LocateAndTryCommitManyMutations(context.TransactionId, batch, CancellationToken.None);

            bool hasTransientFailure = false;
            foreach ((KeyValueResponseType response, string key, long commitIndex, KeyValueDurability durability) in responses)
            {
                if (response == KeyValueResponseType.Committed)
                {
                    pendingCommits.Remove(key);
                    continue;
                }

                if (response == KeyValueResponseType.MustRetry)
                {
                    hasTransientFailure = true;
                    logger.LogWarning("CommitMutations: transient failure on {Key} attempt {Attempt}, retrying", key, attempt + 1);
                    continue;
                }

                string reason = $"Permanent commit failure on {key}: {response}";
                context.Result = new() { Type = KeyValueResponseType.Aborted, Reason = reason };
                logger.LogError("CommitMutations: permanent failure {Type} on {Key} attempt {Attempt}", response, key, attempt + 1);
                throw new KahunaAbortedException(reason);
            }

            if (!hasTransientFailure)
                break;
        }

        if (pendingCommits.Count > 0)
        {
            logger.LogError("CommitMutations: exhausted {MaxRetries} retries, {Count} participants still pending: {Keys}",
                MaxPhase2Retries, pendingCommits.Count, string.Join(", ", pendingCommits.Keys));

            string reason = $"Coordinator exhausted retries during commit ({pendingCommits.Count} participants still pending)";
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

        if (!context.SetState(KeyValueTransactionState.RollingBack, KeyValueTransactionState.Prepared))
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
                    pendingRollbacks.Remove(key);
                    continue;
                }

                if (response == KeyValueResponseType.MustRetry)
                {
                    hasTransientFailure = true;
                    logger.LogWarning("RollbackMutations: transient failure on {Key} attempt {Attempt}, retrying", key, attempt + 1);
                    continue;
                }

                logger.LogWarning("RollbackMutations {Type} {Key} {TicketId} {Durability}", response, key, commitIndex, durability);
                pendingRollbacks.Remove(key);
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
}
