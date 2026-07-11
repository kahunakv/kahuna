
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

        bool added;
        HLCTimestamp transactionId;

        do
        {
            transactionId = raft.HybridLogicalClock.SendOrLocalEvent(raft.GetLocalNodeId());

            TransactionContext context = new()
            {
                CoordinatorKey = coordinatorKey,
                TransactionId  = transactionId,
                Locking        = options.Locking,
                Action         = KeyValueTransactionAction.Commit,
                AsyncRelease   = options.AsyncRelease,
                Timeout        = options.Timeout <= 0 ? configuration.DefaultTransactionTimeout : options.Timeout
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
    public async Task<KeyValueResponseType> CommitTransaction(
        TransactionHandle handle,
        List<KeyValueTransactionModifiedKey> acquiredLocks,
        List<KeyValueTransactionModifiedKey> modifiedKeys,
        List<KeyValueTransactionReadKey> readKeys
    )
    {
        HLCTimestamp transactionId = handle.TransactionId;

        if (!sessions.TryGetValue(transactionId, out TransactionContext? context))
        {
            logger.LogWarning("Trying to commit unknown transaction {TransactionId}", transactionId);

            return KeyValueResponseType.Errored;
        }

        try
        {
            context.Result = new() { Type = KeyValueResponseType.Set, Reason = null };

            foreach (KeyValueTransactionModifiedKey acquiredLock in acquiredLocks)
            {
                context.LocksAcquired ??= [];
                context.LocksAcquired.Add((acquiredLock.Key ?? "", acquiredLock.Durability));
            }

            foreach (KeyValueTransactionModifiedKey modifiedKey in modifiedKeys)
            {
                context.ModifiedKeys ??= [];
                context.ModifiedKeys.Add((modifiedKey.Key ?? "", modifiedKey.Durability));
            }

            foreach (KeyValueTransactionReadKey readKey in readKeys)
            {
                if (string.IsNullOrEmpty(readKey.Key))
                    continue;

                context.ReadKeys ??= [];
                context.ReadKeys[(readKey.Key, readKey.Durability)] = readKey;
            }

            await TwoPhaseCommit(context, CancellationToken.None);

            if (context.Result is null)
                return KeyValueResponseType.Errored;

            if (context.Result.Type is KeyValueResponseType.Aborted or KeyValueResponseType.Errored)
                return KeyValueResponseType.Aborted;

            logger.LogCommittedInteractiveTransaction(transactionId);

            sessions.TryRemove(transactionId, out _);

            return KeyValueResponseType.Committed;
        }
        catch (KahunaAbortedException ex)
        {
            logger.LogKahunaAbortedException(ex);

            return KeyValueResponseType.Aborted;
        }
        catch (TaskCanceledException ex)
        {
            logger.LogTaskCanceledException(ex);

            return KeyValueResponseType.Aborted;
        }
        catch (OperationCanceledException ex)
        {
            logger.LogOperationCanceledException(ex);

            return KeyValueResponseType.Aborted;
        }
        catch (Exception ex)
        {
            logger.LogOperationCanceledException(ex);

            return KeyValueResponseType.Aborted;
        }
        finally
        {
            if (context.Locking == KeyValueTransactionLocking.Pessimistic || (context.State != KeyValueTransactionState.Committed && context.State != KeyValueTransactionState.RolledBack))
            {
                if (context.AsyncRelease)
                    _ = ReleaseAcquiredLocks(context);
                else
                    await ReleaseAcquiredLocks(context);
            }
        }
    }

    /// <summary>
    /// Rolls back the transaction identified by the supplied handle.
    /// </summary>
    public async Task<KeyValueResponseType> RollbackTransaction(
        TransactionHandle handle,
        List<KeyValueTransactionModifiedKey> acquiredLocks,
        List<KeyValueTransactionModifiedKey> modifiedKeys
    )
    {
        HLCTimestamp transactionId = handle.TransactionId;

        if (!sessions.TryGetValue(transactionId, out TransactionContext? context))
        {
            logger.LogWarning("Trying to rollback unknown transaction {TransactionId}", transactionId);

            return KeyValueResponseType.Errored;
        }

        try
        {
            foreach (KeyValueTransactionModifiedKey acquiredLock in acquiredLocks)
            {
                context.LocksAcquired ??= [];
                context.LocksAcquired.Add((acquiredLock.Key ?? "", acquiredLock.Durability));
            }

            foreach (KeyValueTransactionModifiedKey modifiedKey in modifiedKeys)
            {
                context.ModifiedKeys ??= [];
                context.ModifiedKeys.Add((modifiedKey.Key ?? "", modifiedKey.Durability));
            }

            context.Action = KeyValueTransactionAction.Abort;

            logger.LogRolledBackInteractiveTransaction(transactionId);

            sessions.TryRemove(transactionId, out _);

            return KeyValueResponseType.RolledBack;
        }
        finally
        {
            if (context.Locking == KeyValueTransactionLocking.Pessimistic || context.State != KeyValueTransactionState.Committed && context.State != KeyValueTransactionState.RolledBack)
            {
                await ReleaseAcquiredLocks(context);
            }
        }
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
    public void CompleteOperation(HLCTimestamp transactionId, TransactionOperationId operationId, OperationEffect? effect, object? response)
    {
        if (sessions.TryGetValue(transactionId, out TransactionContext? context))
            context.CompleteOperation(operationId, effect, response);
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
    public async Task<(KeyValueResponseType, WorkingSetSnapshot?)> CloseTransaction(
        HLCTimestamp transactionId,
        CancellationToken cancellationToken)
    {
        if (!sessions.TryGetValue(transactionId, out TransactionContext? context))
            return (KeyValueResponseType.Errored, null);

        if (!context.TryBeginFinalizing())
        {
            // Another finalize won the race or the session is terminal. Return the snapshot only once
            // it has actually been published; until then ask the caller to retry rather than hand back
            // a null set that reads as success.
            WorkingSetSnapshot? published = context.PublishedSnapshot;
            return published is not null
                ? (KeyValueResponseType.Set, published)
                : (KeyValueResponseType.MustRetry, null);
        }

        using CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(context.Timeout);

        try
        {
            await context.WaitForPendingOperations(cts.Token);
        }
        catch (OperationCanceledException)
        {
            // Drain deadline hit — reopen the session so the caller can retry the close.
            context.RevertFinalizing();
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

            if (!sessions.TryRemove(pair.Key, out _))
                continue;

            context.Action = KeyValueTransactionAction.Abort;

            logger.LogWarning("Reaping abandoned interactive transaction {TransactionId}", pair.Key);

            await ReleaseAcquiredLocks(context);
        }
    }

    /// <summary>
    /// Releases all locks held by the given context. Called from commit, rollback, and the reaper,
    /// as well as from the script executor after script-scoped transactions.
    /// </summary>
    internal async Task ReleaseAcquiredLocks(TransactionContext context)
    {
        try
        {
            if (context.PrefixLocksAcquired is not null && context.PrefixLocksAcquired.Count > 0)
            {
                foreach ((string prefixKey, KeyValueDurability durability) in context.PrefixLocksAcquired)
                    await manager.LocateAndTryReleaseExclusivePrefixLock(context.TransactionId, prefixKey, durability, CancellationToken.None);
            }

            if (context.LocksAcquired is not null && context.LocksAcquired.Count > 0)
            {
                List<(string, KeyValueDurability)> locksToRelease;

                if (context.ModifiedKeys is null || (context.State != KeyValueTransactionState.Committed && context.State != KeyValueTransactionState.RolledBack))
                    locksToRelease = context.LocksAcquired.ToList();
                else
                {
                    locksToRelease = [];

                    foreach ((string, KeyValueDurability) lockKey in context.LocksAcquired)
                    {
                        if (!context.ModifiedKeys.Contains(lockKey))
                            locksToRelease.Add(lockKey);
                    }
                }

                if (locksToRelease.Count == 1)
                {
                    (string lockKey, KeyValueDurability durability) = locksToRelease.First();

                    await manager.LocateAndTryReleaseExclusiveLock(context.TransactionId, lockKey, durability, CancellationToken.None);
                    return;
                }

                await manager.LocateAndTryReleaseManyExclusiveLocks(context.TransactionId, locksToRelease, CancellationToken.None);
            }
        }
        catch (Exception ex)
        {
            logger.LogError("ReleaseAcquiredLocks: {Type} {Message}\n{StackTrace}", ex.GetType().Name, ex.Message, ex.StackTrace);
        }
    }

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
    /// Validates optimistic read dependencies against the current committed state before prepare.
    /// </summary>
    private async Task<bool> ValidateReadSet(TransactionContext context, CancellationToken cancellationToken)
    {
        if (context.Locking != KeyValueTransactionLocking.Optimistic || context.ReadKeys is null || context.ReadKeys.Count == 0)
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
        if (context.Locking != KeyValueTransactionLocking.Optimistic || context.ReadKeys is null || context.ReadKeys.Count == 0)
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
                cancellationToken
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
            cancellationToken
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
