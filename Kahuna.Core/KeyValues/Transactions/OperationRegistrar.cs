using Kommander;
using Kommander.Time;

using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Transactions;

/// <summary>
/// The register-remote machinery behind every transaction-scoped operation: classifying a request by the
/// identity it carries, registering it on the coordinator before it is applied, and recording the confirmed
/// effect afterwards so a retry replays the cached response instead of applying the operation twice.
///
/// The outcome mapping is a contract, not a detail. A capacity rejection is <c>MustRetry</c> because retrying
/// is safe; a closed or over-budget session is <c>Aborted</c> because the transaction is genuinely finished;
/// a duplicate id is <c>Errored</c> because the caller is malformed. A request carrying exactly one of
/// {coordinator key, operation id} is rejected rather than degraded to the unregistered path — applying it
/// would mutate a participant outside the finalize fence.
/// </summary>
/// <summary>How a transaction-scoped request routes based on the register-remote identity it carries.</summary>
internal enum RegistrationRouting
{
    /// <summary>Non-transactional, or a script/REST transaction that owns its 2PC: take the legacy locator path.</summary>
    Legacy,

    /// <summary>An interactive request carrying only one of {coordinator key, operation id}: reject as malformed.</summary>
    Malformed,

    /// <summary>An interactive request carrying both identity components: register on the coordinator.</summary>
    Registered
}

internal sealed class OperationRegistrar
{
    private readonly KeyValuesRuntime runtime;

    private readonly TransactionCoordinator txCoordinator;

    /// <summary>
    /// Participant-side results of transaction operations whose actor executed but whose coordinator
    /// completion is not yet acknowledged, so a retry can recover the confirmed effect without reapplying.
    /// </summary>
    private readonly ParticipantOperationCache participantOperationCache = new();

    internal OperationRegistrar(KeyValuesRuntime runtime, TransactionCoordinator txCoordinator)
    {
        this.runtime = runtime;
        this.txCoordinator = txCoordinator;
    }

    /// <summary>The retry-recovery cache of confirmed-but-unacknowledged participant effects.</summary>
    internal ParticipantOperationCache ParticipantOperationCache => participantOperationCache;

    // Aliases matching the field names the moved bodies use, so those bodies stay byte-for-byte as they were.
    private IRaft raft => runtime.Raft;

    private ILogger<IKahuna> logger => runtime.Logger;

    private KeyValueLocator locator => runtime.Locator;

    private Kahuna.Server.Communication.Internode.IInterNodeCommunication interNodeCommunication => runtime.InterNodeCommunication;

    /// <summary>
    /// Classifies a transaction-scoped point/lock request by the register-remote identity it carries. A
    /// non-transactional request, or a script/REST transaction that manages its own 2PC, carries neither a
    /// coordinator key nor an operation id and takes the legacy locator path. An interactive request must
    /// carry <b>both</b>; carrying exactly one is malformed — applying it would mutate a participant outside
    /// the finalize fence, so it is rejected rather than silently degraded to the unregistered path.
    /// </summary>
    internal static RegistrationRouting ClassifyRegistration(HLCTimestamp transactionId, string coordinatorKey, TransactionOperationId operationId)
    {
        bool hasCoordinator = !string.IsNullOrEmpty(coordinatorKey);
        bool hasOperation = !operationId.IsEmpty;

        if (transactionId.IsNull() || (!hasCoordinator && !hasOperation))
            return RegistrationRouting.Legacy;

        return hasCoordinator && hasOperation ? RegistrationRouting.Registered : RegistrationRouting.Malformed;
    }

    /// <summary>
    /// Drives the coordinator completion for an operation whose actor just executed. On acknowledgement
    /// nothing was shared, so this frame recycles the rented payload shell. When the completion does not
    /// land, the confirmed response and payload are cached on this participant so a same-id retry
    /// recovers the result through <see cref="TryRecoverRegisteredOperation"/> without reapplying the
    /// operation — the cache then owns the payload for good (recovery can hand its reference to
    /// concurrent retries, so it is never recycled). Takes ownership of <paramref name="payload"/>:
    /// the caller must not touch it after this call. The completion runs detached from the caller's
    /// cancellation token: once the actor has mutated, abandoning the completion because the caller
    /// went away would strand the effect with the coordinator record stuck pending. Returns true when
    /// the coordinator acknowledged the fold; false means the caller must surface
    /// <see cref="KeyValueResponseType.MustRetry"/>.
    /// </summary>
    internal async Task<bool> CompleteRegisteredOperation(
        string coordinatorKey, HLCTimestamp transactionId, TransactionOperationId operationId,
        object response, OperationCompletionPayload payload)
    {
        try
        {
            (KeyValueResponseType outcome, _) = await LocateAndCompleteOperation(coordinatorKey, transactionId, operationId, payload, CancellationToken.None);

            if (outcome == KeyValueResponseType.Set)
            {
                // Acknowledged without ever entering the cache: this frame holds the sole reference,
                // and the coordinator fold copied everything it kept, so the shell can be recycled.
                OperationCompletionPayloadPool.Return(payload);
                return true;
            }

            // The routing resolved without an exception but the completion did not reach the coordinator
            // (e.g. a leadership flip between routing and landing). Cache the result so a same-id retry
            // re-drives the idempotent completion through TryRecoverRegisteredOperation. In the window
            // before this store, a concurrent same-id retry finds no entry and surfaces MustRetry, which
            // is safe — a later retry finds the entry.
            participantOperationCache.Store(transactionId, operationId, response, payload);
            logger.LogWarning("Completion of transaction operation {OperationId} was not acknowledged by coordinator; retaining participant result for retry", operationId);
            return false;
        }
        catch (Exception ex)
        {
            // RPC loss or transient fault — cache the result for same-id retry recovery.
            participantOperationCache.Store(transactionId, operationId, response, payload);
            logger.LogWarning(ex, "Completion of transaction operation {OperationId} was not acknowledged; retaining participant result for retry", operationId);
            return false;
        }
    }

    /// <summary>
    /// On an <see cref="OperationRegistrationOutcome.AlreadyPending"/> outcome, attempts to finish an
    /// operation whose actor already executed on this participant but whose completion was lost. Re-drives
    /// the coordinator completion from the cached effect (the coordinator fold is idempotent) instead of
    /// reapplying the operation. Returns the original boxed response when recovery succeeds; null means the
    /// caller should surface <see cref="KeyValueResponseType.MustRetry"/> (no local record, or the
    /// completion is still unreachable).
    /// </summary>
    internal async Task<object?> TryRecoverRegisteredOperation(
        string coordinatorKey, HLCTimestamp transactionId, TransactionOperationId operationId)
    {
        if (!participantOperationCache.TryGet(transactionId, operationId, out object? response, out OperationCompletionPayload? payload))
            return null;

        // The payload reference is cache-owned and may be held by concurrent same-id retries — it
        // must never be returned to the payload pool, on any path in this method.
        try
        {
            (KeyValueResponseType outcome, _) = await LocateAndCompleteOperation(coordinatorKey, transactionId, operationId, payload!, CancellationToken.None);

            if (outcome == KeyValueResponseType.Set)
            {
                participantOperationCache.Remove(transactionId, operationId);
                return response;
            }

            // Coordinator did not acknowledge — retain cache entry so the next same-id retry re-drives.
            logger.LogWarning("Retry completion of transaction operation {OperationId} was not acknowledged by coordinator; will retry again", operationId);
            return null;
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Retry completion of transaction operation {OperationId} was not acknowledged; will retry again", operationId);
            return null;
        }
    }

    /// <summary>Routes an operation registration to the coordinator node identified by <paramref name="coordinatorKey"/>.</summary>
    public Task<(OperationRegistrationOutcome outcome, KeyValueResponseType cachedType, long cachedRevision, HLCTimestamp cachedTimestamp, string? recordAnchorKey)> LocateAndBeginOperation(string coordinatorKey, HLCTimestamp transactionId, TransactionOperationId operationId, OperationKind kind, byte[]? payloadDigest, CancellationToken cancellationToken)
    {
        return locator.LocateAndBeginOperation(coordinatorKey, transactionId, operationId, kind, payloadDigest, cancellationToken);
    }

    /// <summary>Routes an operation completion to the coordinator node identified by <paramref name="coordinatorKey"/>. Returns Set+anchor on acknowledgement, MustRetry when routing did not deliver.</summary>
    public Task<(KeyValueResponseType outcome, string? anchor)> LocateAndCompleteOperation(string coordinatorKey, HLCTimestamp transactionId, TransactionOperationId operationId, OperationCompletionPayload payload, CancellationToken cancellationToken)
    {
        return locator.LocateAndCompleteOperation(coordinatorKey, transactionId, operationId, payload, cancellationToken);
    }

    /// <summary>Node-local registration: the session lives here (this node is the coordinator for the key).</summary>
    public (OperationRegistrationOutcome outcome, KeyValueResponseType cachedType, long cachedRevision, HLCTimestamp cachedTimestamp, string? recordAnchorKey) BeginOperation(HLCTimestamp transactionId, TransactionOperationId operationId, OperationKind kind, byte[]? payloadDigest)
    {
        OperationRegistrationResult result = txCoordinator.BeginOperation(transactionId, operationId, kind, payloadDigest);

        CachedOperationResponse cached = result.CachedResponse is CachedOperationResponse c
            ? c
            : new(KeyValueResponseType.Errored, 0, HLCTimestamp.Zero);

        return (result.Outcome, cached.Type, cached.Revision, cached.CommitTimestamp, result.RecordAnchorKey);
    }

    /// <summary>
    /// Node-local completion: folds the confirmed effect into the coordinator-owned working set. Returns
    /// the transaction's record anchor after the fold, or null when nothing durable-anchoring exists yet.
    /// </summary>
    public string? CompleteOperation(HLCTimestamp transactionId, TransactionOperationId operationId, OperationCompletionPayload payload)
    {
        // A transient outcome, or a WaitingForReplication registration that produced no effect, must not
        // be cached as terminal: cancel so a same-id retry re-registers as new and actually applies the
        // mutation. A WaitingForReplication that DID produce an effect keeps the cached-response replay
        // path (do not cancel) — a retry would see the cached response and skip reapplication.
        if (payload.CachedType == KeyValueResponseType.MustRetry ||
            (payload.CachedType == KeyValueResponseType.WaitingForReplication && !HasEffect(payload)))
        {
            txCoordinator.CancelOperation(transactionId, operationId);
            return null;
        }

        return txCoordinator.CompleteOperation(transactionId, operationId, payload, new CachedOperationResponse(payload.CachedType, payload.CachedRevision, payload.CachedTimestamp));
    }

    /// <summary>
    /// Inbound landing point for a remote completion: re-checks local leadership before folding, so a
    /// node that lost the coordinator partition between the caller's route decision and the RPC landing
    /// does not silently swallow the completion as a false acknowledgement. Returns <c>Set</c>+anchor
    /// when this node is still the leader and the fold succeeded; <c>MustRetry</c> when leadership was
    /// lost. Does not re-forward — the caller's retry re-routes through a fresh
    /// <see cref="LocateAndCompleteOperation"/>.
    /// </summary>
    public async Task<(KeyValueResponseType outcome, string? anchor)> CompleteOperationInbound(string coordinatorKey, HLCTimestamp transactionId, TransactionOperationId operationId, OperationCompletionPayload payload)
    {
        if (string.IsNullOrEmpty(coordinatorKey))
            return (KeyValueResponseType.MustRetry, null);

        int partitionId = locator.LocatePartition(coordinatorKey);

        if (raft.Joined && !await raft.AmILeaderIfHosted(partitionId, CancellationToken.None))
        {
            // Under replica placement the sender may have picked a coordinator-partition replica
            // that is not the leader; this node hosts the partition, so its own resolution is
            // accurate — redirect the completion once instead of refusing a sender that would
            // only retry against the same guessed replica. A genuinely unresolvable leader keeps
            // the retryable refusal below.
            string? actualLeader;
            try
            {
                actualLeader = await raft.TryResolveLeader(partitionId, CancellationToken.None).ConfigureAwait(false);
            }
            catch (RaftException)
            {
                actualLeader = null;
            }

            if (actualLeader is not null && actualLeader != raft.GetLocalEndpoint())
                return await interNodeCommunication
                    .CompleteOperation(actualLeader, coordinatorKey, transactionId, operationId, payload, CancellationToken.None)
                    .ConfigureAwait(false);

            return (KeyValueResponseType.MustRetry, null);
        }

        string? anchor = CompleteOperation(transactionId, operationId, payload);
        return (KeyValueResponseType.Set, anchor);
    }

    /// <summary>True when the payload records at least one working-set effect: a modified key, an acquired
    /// or released lock, a read observation, or a staged mutation. The fold itself reads the payload
    /// directly, so this predicate only drives the no-effect cancel decision above.</summary>
    private static bool HasEffect(OperationCompletionPayload payload) =>
        !string.IsNullOrEmpty(payload.ModifiedKey) ||
        (payload.ModifiedKeys is { Count: > 0 }) ||
        (payload.AcquiredPointLocks is { Count: > 0 }) ||
        !string.IsNullOrEmpty(payload.AcquiredPointLock) || !string.IsNullOrEmpty(payload.ReleasedPointLock) ||
        !string.IsNullOrEmpty(payload.AcquiredPrefixLock) || !string.IsNullOrEmpty(payload.ReleasedPrefixLock) ||
        payload.AcquiredRangeLock is not null || payload.ReleasedRangeLock is not null ||
        payload.Read is not null ||
        (payload.ReadObservations is { Count: > 0 }) ||
        (payload.StagedMutations is { Count: > 0 });
}
