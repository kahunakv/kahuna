using Kommander;
using Kommander.Time;

using Kahuna.Server.KeyValues.Logging;
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

    /// <summary>First delay of the detached retry of an unacknowledged completion; doubles each attempt up to
    /// <see cref="RedriveMaxDelayMs"/>.</summary>
    internal const int RedriveInitialDelayMs = 50;

    internal const int RedriveMaxDelayMs = 1_000;

    /// <summary>After this long without an acknowledgement, the retry waits up to <see cref="RedriveSlowMaxDelayMs"/>
    /// between attempts. A retry that runs this long is most likely sending a release that waits for the close of a
    /// session that still takes operations, or a completion for a session that is gone.</summary>
    internal const int RedriveSlowAfterMs = 30_000;

    internal const int RedriveSlowMaxDelayMs = 5_000;

    /// <summary>The most detached completion retries one node runs at once: one for each result the cache can keep.
    /// A leader change under load can leave many completions unacknowledged at the same moment.</summary>
    internal const int MaxRedrives = ParticipantOperationCache.MaxEntries;

    /// <summary>The most completions the detached retries of one node send at once, so a burst of them does not start
    /// as many owner probes at the same moment. An attempt that finds the limit reached waits for its next turn.</summary>
    private const int MaxConcurrentRedriveSends = 32;

    private int activeRedrives;

    private int redriveSendsInFlight;

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
    /// Drives the coordinator completion for an operation whose participant work just executed — a lock, a
    /// write, or a read. On acknowledgement nothing was shared, so this frame recycles the rented payload
    /// shell. When the completion does not land, the confirmed response and payload are cached on this
    /// participant so a same-id retry recovers the result through <see cref="TryRecoverRegisteredOperation"/>
    /// without reapplying the operation — the cache then owns the payload for good (recovery can hand its
    /// reference to concurrent retries, so it is never recycled). A detached retry also sends the completion
    /// again (<see cref="StartRedrive"/>), because the caller can stop retrying, and the record would then stay
    /// pending until the session timeout and hold every finalize of the transaction. Takes ownership of <paramref name="payload"/>:
    /// the caller must not touch it after this call. The completion runs detached from the caller's
    /// cancellation token: once the actor has mutated, abandoning the completion because the caller
    /// went away would strand the effect with the coordinator record stuck pending. Returns true when
    /// the coordinator acknowledged the fold; false means the caller must surface
    /// <see cref="KeyValueResponseType.MustRetry"/>. Generic in the response type so an acknowledged
    /// value-type response (the common case) is never boxed; boxing happens only when the response
    /// enters the retry cache on a lost completion.
    ///
    /// <para>A read passes only its response type, never the values it returned: its observations (keys and
    /// revisions) travel in the payload and are what must fold exactly once, while the values are re-read on
    /// recovery. That keeps a cached scan page to its key list, so the cache's entry cap bounds it in memory
    /// too. See <see cref="TryRecoverRegisteredRead"/>.</para>
    /// </summary>
    internal async Task<bool> CompleteRegisteredOperation<TResponse>(
        string coordinatorKey, HLCTimestamp transactionId, TransactionOperationId operationId,
        TResponse response, OperationCompletionPayload payload) where TResponse : notnull
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
            ParticipantOperationCache.Entry entry = participantOperationCache.Store(transactionId, operationId, response, payload);
            logger.LogWarning("Completion of transaction operation {OperationId} was not acknowledged by coordinator; retaining participant result for retry", operationId);
            StartRedrive(coordinatorKey, transactionId, operationId, payload, entry);
            return false;
        }
        catch (Exception ex)
        {
            // RPC loss or transient fault — cache the result for same-id retry recovery.
            ParticipantOperationCache.Entry entry = participantOperationCache.Store(transactionId, operationId, response, payload);
            logger.LogWarning(ex, "Completion of transaction operation {OperationId} was not acknowledged; retaining participant result for retry", operationId);
            StartRedrive(coordinatorKey, transactionId, operationId, payload, entry);
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

    /// <summary>
    /// The read-side recovery for an <see cref="OperationRegistrationOutcome.AlreadyPending"/> outcome: an
    /// earlier attempt executed the read but its completion did not land, so the record on the coordinator is
    /// pending and its observations are only in this participant's retry cache. Re-drives that completion.
    /// Returns true when the coordinator now holds the read as completed, so the caller may re-execute the
    /// read and return fresh values without folding again — the first-recorded observations stay authoritative
    /// for commit validation, exactly as for any re-read under an already-completed id. Returns false when
    /// the caller must surface <see cref="KeyValueResponseType.MustRetry"/>: no local record, the completion
    /// is still unreachable, or the recovered completion carried a transient read that the coordinator turns
    /// into a cancel — the record is then gone, and the next same-id retry registers afresh and folds.
    /// </summary>
    internal async Task<bool> TryRecoverRegisteredRead(string coordinatorKey, HLCTimestamp transactionId, TransactionOperationId operationId)
    {
        object? recovered = await TryRecoverRegisteredOperation(coordinatorKey, transactionId, operationId);

        return recovered is KeyValueResponseType type
            && type is not (KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication);
    }

    /// <summary>
    /// Releases a freshly registered operation whose participant work threw before this frame could complete
    /// it. The frame that registered an operation is its only completer, so without this the record stays
    /// pending for the life of the session and every commit or rollback waits out the full finalize drain
    /// deadline on it. The most common trigger is the caller's cancellation token firing inside the work (a
    /// leadership confirmation, a forward, an actor wait) after the registration itself succeeded.
    ///
    /// <para>The release is a completion with a transient outcome and no effect, which the coordinator turns
    /// into a cancel: the record is removed, the pending count drops, and a same-id retry registers as new and
    /// re-drives the operation. Nothing is folded into the working set. If the work did take effect on the
    /// participant before it threw, that effect stays unfolded — exactly as it would have with the record left
    /// pending — so it is bounded by its own lease and the session liveness ceiling, and a later commit never
    /// materializes it.</para>
    ///
    /// <para>Runs detached from the caller's token, for the same reason <see cref="CompleteRegisteredOperation"/>
    /// does: the caller going away is what got us here. Never throws — the caller is already propagating the
    /// original exception. A release that does not land is sent again by a detached retry
    /// (<see cref="StartRedrive"/>), in the form that cancels the record only once the session is closed.</para>
    /// </summary>
    internal async Task AbandonRegisteredOperation(string coordinatorKey, HLCTimestamp transactionId, TransactionOperationId operationId)
    {
        OperationCompletionPayload payload = OperationCompletionPayloadPool.Rent();
        payload.CachedType = KeyValueResponseType.MustRetry;

        try
        {
            (KeyValueResponseType outcome, _) = await LocateAndCompleteOperation(coordinatorKey, transactionId, operationId, payload, CancellationToken.None);

            // Recycle only on an acknowledged release: a failed remote completion may not have finished with the
            // shell, and a shell that misses its return is simply reclaimed by the GC.
            if (outcome == KeyValueResponseType.Set)
            {
                OperationCompletionPayloadPool.Return(payload);
                return;
            }

            logger.LogWarning("Release of failed transaction operation {OperationId} was not acknowledged by coordinator; a detached retry sends it again", operationId);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Release of failed transaction operation {OperationId} was not acknowledged; a detached retry sends it again", operationId);
        }

        StartRedrive(coordinatorKey, transactionId, operationId, payload, entry: null);
    }

    /// <summary>
    /// Test-only seam: when true, an unacknowledged completion or release starts no detached retry, so a test can
    /// observe the record that only a same-id retry completes. Never set in production.
    /// </summary>
    internal bool TestDisableRedrive { private get; set; }

    /// <summary>The detached completion retries this node runs now. Test access.</summary>
    internal int ActiveRedrives => Volatile.Read(ref activeRedrives);

    /// <summary>
    /// Starts the detached retry of a completion or release that the coordinator did not acknowledge. One retry runs
    /// for each kept result: <paramref name="entry"/> is the cache entry of a completion (null for a release, which
    /// keeps no result), and a newer result for the same operation settles it and ends its retry. The retry runs on
    /// the thread pool with a clean execution context, so no scope of the caller's request (a forward, a terminal
    /// probe, a lock-grant capture) reaches the completions it sends. Starts nothing past <see cref="MaxRedrives"/>.
    /// </summary>
    private void StartRedrive(string coordinatorKey, HLCTimestamp transactionId, TransactionOperationId operationId, OperationCompletionPayload payload, ParticipantOperationCache.Entry? entry)
    {
        if (TestDisableRedrive)
            return;

        if (Interlocked.Increment(ref activeRedrives) > MaxRedrives)
        {
            Interlocked.Decrement(ref activeRedrives);

            long count = DurableTransactionMetrics.CompletionRetryNotStarted();
            if (count == 1 || count % 1000 == 0)
                logger.LogCompletionRetryNotStarted(operationId, transactionId, MaxRedrives, count);

            return;
        }

        DurableTransactionMetrics.CompletionKeptForRetry();

        // A release is sent again only in the form that waits for the close of the session: its first copy can have
        // landed and freed the operation id for a new registration, which a plain copy would cancel. An effect is
        // sent again as it is: once it folds, its record is completed and stays so, and another copy is a no-op.
        OperationCompletionPayload sent = IsRelease(payload)
            ? new OperationCompletionPayload { CachedType = KeyValueResponseType.MustRetry, ReleaseOnceClosed = true }
            : payload;

        // SuppressFlow throws when the flow is already suppressed; the task then starts with no context anyway.
        if (ExecutionContext.IsFlowSuppressed())
        {
            _ = Task.Run(() => RedriveCompletion(coordinatorKey, transactionId, operationId, sent, entry));
            return;
        }

        using (ExecutionContext.SuppressFlow())
            _ = Task.Run(() => RedriveCompletion(coordinatorKey, transactionId, operationId, sent, entry));
    }

    /// <summary>
    /// Sends a completion again until the coordinator acknowledges it. The first wait is
    /// <see cref="RedriveInitialDelayMs"/>, doubled each attempt up to <see cref="RedriveMaxDelayMs"/>, and up to
    /// <see cref="RedriveSlowMaxDelayMs"/> after <see cref="RedriveSlowAfterMs"/>. Each attempt routes the completion
    /// again, so a new leader of the coordinator partition is found, and that leader probes its peers for the owner of
    /// the session. An answer that this node holds no session does not end the retry: the owner probe can miss the
    /// owner. The retry ends when the completion is acknowledged; when its cache entry is settled (a same-id retry of
    /// the caller completed it, or a newer result replaced it); at the end of the longest span a session can matter
    /// (<see cref="TransactionCoordinator.SessionMaxLifespanMs"/> from the transaction start, by this node's clock and
    /// configuration); or at the node shutdown. An effect that never folds stays on its participant until its lock
    /// lease or intent lease ends. The fold is idempotent, so this can run beside a same-id retry of the caller.
    /// Never throws. The count of live retries drops only after the retry counted and logged how it ended.
    /// </summary>
    private async Task RedriveCompletion(string coordinatorKey, HLCTimestamp transactionId, TransactionOperationId operationId, OperationCompletionPayload payload, ParticipantOperationCache.Entry? entry)
    {
        try
        {
            await RedriveUntilEnded(coordinatorKey, transactionId, operationId, payload, entry).ConfigureAwait(false);
        }
        finally
        {
            Interlocked.Decrement(ref activeRedrives);
        }
    }

    private async Task RedriveUntilEnded(string coordinatorKey, HLCTimestamp transactionId, TransactionOperationId operationId, OperationCompletionPayload payload, ParticipantOperationCache.Entry? entry)
    {
        long startedTicks = Environment.TickCount64;
        int delayMs = RedriveInitialDelayMs;
        int attempts = 0;
        string ended = "the session lifespan ended";

        try
        {
            CancellationToken shutdown = runtime.Shutdown.Token;
            long untilTicks = startedTicks + RemainingSessionLifespanMs(transactionId);

            while (true)
            {
                if (entry is { IsSettled: true })
                {
                    DurableTransactionMetrics.CompletionRecoveredFirst();
                    return;
                }

                if (shutdown.IsCancellationRequested)
                {
                    ended = "the node is shutting down";
                    break;
                }

                long remainingMs = untilTicks - Environment.TickCount64;
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

                int maxDelayMs = Environment.TickCount64 - startedTicks < RedriveSlowAfterMs ? RedriveMaxDelayMs : RedriveSlowMaxDelayMs;
                delayMs = Math.Min(delayMs * 2, maxDelayMs);

                if (entry is { IsSettled: true })
                {
                    DurableTransactionMetrics.CompletionRecoveredFirst();
                    return;
                }

                attempts++;

                if (!await TrySendRedrive(coordinatorKey, transactionId, operationId, payload).ConfigureAwait(false))
                    continue;

                if (entry is not null)
                    participantOperationCache.Remove(transactionId, operationId, entry);

                DurableTransactionMetrics.CompletionDrivenByRetry();
                logger.LogCompletionDrivenOnRetry(operationId, transactionId, attempts, Environment.TickCount64 - startedTicks);
                return;
            }
        }
        catch (Exception ex)
        {
            ended = "the retry failed";
            logger.LogError("Retrying the completion of transaction operation {OperationId} of transaction {TransactionId}: {Type} {Message}", operationId, transactionId, ex.GetType().Name, ex.Message);
        }

        DurableTransactionMetrics.CompletionRetryGivenUp();
        logger.LogCompletionRetryGivenUp(operationId, transactionId, attempts, Environment.TickCount64 - startedTicks, ended);
    }

    /// <summary>One attempt of a detached retry; true when the coordinator acknowledged the completion. An attempt
    /// past <see cref="MaxConcurrentRedriveSends"/> sends nothing and answers false.</summary>
    private async Task<bool> TrySendRedrive(string coordinatorKey, HLCTimestamp transactionId, TransactionOperationId operationId, OperationCompletionPayload payload)
    {
        if (Interlocked.Increment(ref redriveSendsInFlight) > MaxConcurrentRedriveSends)
        {
            Interlocked.Decrement(ref redriveSendsInFlight);
            return false;
        }

        try
        {
            (KeyValueResponseType outcome, _) = await LocateAndCompleteOperation(coordinatorKey, transactionId, operationId, payload, CancellationToken.None).ConfigureAwait(false);
            return outcome == KeyValueResponseType.Set;
        }
        catch (Exception ex)
        {
            logger.LogCompletionRetryAttemptFailed(operationId, ex);
            return false;
        }
        finally
        {
            Interlocked.Decrement(ref redriveSendsInFlight);
        }
    }

    /// <summary>
    /// Milliseconds left until <paramref name="transactionId"/> is older than the longest span a session can matter
    /// (<see cref="TransactionCoordinator.SessionMaxLifespanMs"/>), by this node's hybrid clock. The transaction id is
    /// the timestamp its coordinator began the session at.
    /// </summary>
    private long RemainingSessionLifespanMs(HLCTimestamp transactionId)
    {
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());
        TimeSpan remaining = (transactionId + TransactionCoordinator.SessionMaxLifespanMs(runtime.Configuration)) - now;

        return remaining <= TimeSpan.Zero ? 0 : (long)remaining.TotalMilliseconds;
    }

    /// <summary>Routes an operation registration to the coordinator node identified by <paramref name="coordinatorKey"/>.</summary>
    public ValueTask<(OperationRegistrationOutcome outcome, KeyValueResponseType cachedType, long cachedRevision, HLCTimestamp cachedTimestamp, string? recordAnchorKey, TransactionConflictPolicy conflictPolicy)> LocateAndBeginOperation(string coordinatorKey, HLCTimestamp transactionId, TransactionOperationId operationId, OperationKind kind, byte[]? payloadDigest, CancellationToken cancellationToken, int itemCount = 0)
    {
        return locator.LocateAndBeginOperation(coordinatorKey, transactionId, operationId, kind, payloadDigest, cancellationToken, itemCount);
    }

    /// <summary>
    /// Test-only seam: when set and it answers true for an operation, the routing of that operation's completion
    /// answers <c>(MustRetry, null)</c> without delivering it — the same answer the routing gives when the
    /// coordinator partition has no reachable leader, or when the node it forwarded to lost the partition before
    /// the completion landed. A healthy embedded node cannot be asked to produce that state on demand, so this is
    /// how a test drives the not-acknowledged completion path deterministically. It is consulted on every
    /// completion routed through this registrar, a retry's recovery included, so the delegate decides how often
    /// it fires. Null (the default) is a no-op; never wired in production.
    /// </summary>
    internal Func<HLCTimestamp, TransactionOperationId, bool>? TestCompletionRefusal { private get; set; }

    /// <summary>Routes an operation completion to the coordinator node identified by <paramref name="coordinatorKey"/>. Returns Set+anchor on acknowledgement, MustRetry when routing did not deliver.</summary>
    public ValueTask<(KeyValueResponseType outcome, string? anchor)> LocateAndCompleteOperation(string coordinatorKey, HLCTimestamp transactionId, TransactionOperationId operationId, OperationCompletionPayload payload, CancellationToken cancellationToken)
    {
        if (TestCompletionRefusal is not null && TestCompletionRefusal(transactionId, operationId))
            return ValueTask.FromResult<(KeyValueResponseType outcome, string? anchor)>((KeyValueResponseType.MustRetry, null));

        return locator.LocateAndCompleteOperation(coordinatorKey, transactionId, operationId, payload, cancellationToken);
    }

    /// <summary>Node-local registration: the session lives here (this node is the coordinator for the key).</summary>
    public (OperationRegistrationOutcome outcome, KeyValueResponseType cachedType, long cachedRevision, HLCTimestamp cachedTimestamp, string? recordAnchorKey, TransactionConflictPolicy conflictPolicy) BeginOperation(HLCTimestamp transactionId, TransactionOperationId operationId, OperationKind kind, byte[]? payloadDigest, int itemCount = 0)
    {
        OperationRegistrationResult result = txCoordinator.BeginOperation(transactionId, operationId, kind, payloadDigest, itemCount);

        CachedOperationResponse cached = result.CachedResponse is CachedOperationResponse c
            ? c
            : new(KeyValueResponseType.Errored, 0, HLCTimestamp.Zero);

        return (result.Outcome, cached.Type, cached.Revision, cached.CommitTimestamp, result.RecordAnchorKey, result.ConflictPolicy);
    }

    /// <summary>
    /// Node-local completion: folds the confirmed effect into the coordinator-owned working set. Returns
    /// the transaction's record anchor after the fold, or null when nothing durable-anchoring exists yet.
    /// Throws when the fold is not acknowledged (see <see cref="FoldCompletion"/>).
    /// </summary>
    public string? CompleteOperation(HLCTimestamp transactionId, TransactionOperationId operationId, OperationCompletionPayload payload)
    {
        (bool acknowledged, string? anchor) = FoldCompletion(transactionId, operationId, payload);

        if (!acknowledged)
            throw new KahunaServerException($"The release of operation {operationId} of transaction {transactionId} was not applied on this node.");

        return anchor;
    }

    /// <summary>
    /// Node-local completion that says whether it counts as acknowledged. A completion with an effect folds into the
    /// session and is acknowledged; it throws when this node holds no session, as
    /// <see cref="TransactionCoordinator.CompleteOperation"/> does. A release (a transient outcome, see
    /// <see cref="IsRelease"/>) cancels the pending record and is acknowledged, also when there was nothing left to
    /// cancel. It is not acknowledged in two cases:
    /// <list type="bullet">
    /// <item>This node holds no session and is a member of a cluster. The session can live on a node that led the
    /// coordinator partition before, and the owner probe can miss it; an acknowledgement would leave the record
    /// pending on the owner with no result kept to complete it. A node outside a cluster is the only node, so no
    /// session there means no record.</item>
    /// <item>The release waits for the close of a session that still takes operations
    /// (<see cref="OperationCompletionPayload.ReleaseOnceClosed"/>).</item>
    /// </list>
    /// </summary>
    internal (bool Acknowledged, string? Anchor) FoldCompletion(HLCTimestamp transactionId, TransactionOperationId operationId, OperationCompletionPayload payload)
    {
        // A transient outcome, or a WaitingForReplication registration that produced no effect, must not
        // be cached as terminal: cancel so a same-id retry re-registers as new and actually applies the
        // mutation. A WaitingForReplication that DID produce an effect keeps the cached-response replay
        // path (do not cancel) — a retry would see the cached response and skip reapplication.
        if (IsRelease(payload))
        {
            OperationCancelOutcome cancel = txCoordinator.CancelPendingOperation(transactionId, operationId, payload.ReleaseOnceClosed);

            return cancel switch
            {
                OperationCancelOutcome.NoSession => (!raft.Joined, null),
                OperationCancelOutcome.Deferred => (false, null),
                _ => (true, null)
            };
        }

        return (true, txCoordinator.CompleteOperation(transactionId, operationId, payload, new CachedOperationResponse(payload.CachedType, payload.CachedRevision, payload.CachedTimestamp)));
    }

    /// <summary>True when <paramref name="payload"/> is a release: the coordinator cancels the record instead of
    /// completing it, so a same-id retry registers again and runs the operation.</summary>
    private static bool IsRelease(OperationCompletionPayload payload) =>
        payload.CachedType == KeyValueResponseType.MustRetry ||
        (payload.CachedType == KeyValueResponseType.WaitingForReplication && !HasEffect(payload));

    /// <summary>
    /// Inbound landing point for a remote completion. The session's owner folds it whatever the current
    /// leader is: sessions live on the node that began them and do not move with the coordinator partition's
    /// leadership. Any other node re-checks local leadership before folding, so a node that lost the
    /// coordinator partition between the caller's route decision and the RPC landing does not silently
    /// swallow the completion as a false acknowledgement. Returns <c>Set</c>+anchor when the fold succeeded
    /// here or on the owner a leader without the session found among its peers; <c>MustRetry</c> when
    /// leadership was lost and no leader could be resolved. A non-leader that resolves the leader redirects
    /// the completion once; otherwise the caller's retry re-routes through a fresh
    /// <see cref="LocateAndCompleteOperation"/>.
    /// </summary>
    public async Task<(KeyValueResponseType outcome, string? anchor)> CompleteOperationInbound(string coordinatorKey, HLCTimestamp transactionId, TransactionOperationId operationId, OperationCompletionPayload payload)
    {
        if (string.IsNullOrEmpty(coordinatorKey))
            return (KeyValueResponseType.MustRetry, null);

        // The owner serves its own session even after the partition's leadership moved away from it: a leader
        // without the session offers the completion to its peers (below), and the owner is the one that folds it.
        if (txCoordinator.HasSession(transactionId))
        {
            (bool folded, string? ownedAnchor) = FoldCompletion(transactionId, operationId, payload);

            locator.CountInboundCompletionServedLocally(folded);
            return folded ? (KeyValueResponseType.Set, ownedAnchor) : (KeyValueResponseType.MustRetry, null);
        }

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
            {
                SessionRegistrationForward forward = locator.BeginInboundCompletionRedirect();

                try
                {
                    (KeyValueResponseType outcome, string? anchor) redirected = await interNodeCommunication
                        .CompleteOperation(actualLeader, coordinatorKey, transactionId, operationId, payload, CancellationToken.None)
                        .ConfigureAwait(false);

                    forward.Answered(redirected.outcome == KeyValueResponseType.Set);
                    return redirected;
                }
                catch
                {
                    forward.Threw();
                    throw;
                }
            }

            return (KeyValueResponseType.MustRetry, null);
        }

        // This node leads the coordinator partition but did not begin the session: leadership moved after the
        // session began. Offer the completion to the peers, one of which owns it; the fold below would find no
        // session and refuse a completion the participant already applied, which leaves the granted lock in the
        // participant's memory with no working set to release it from.
        (KeyValueResponseType outcome, string? anchor)? served =
            await locator.ProbePeersForCompletionAsync(coordinatorKey, transactionId, operationId, payload, CancellationToken.None).ConfigureAwait(false);

        if (served is { } answer)
            return answer;

        // No peer owns it either: the fold throws for the missing session, or answers a release as not
        // acknowledged, and the participant keeps its result for its retries.
        (bool acknowledged, string? anchor) = FoldCompletion(transactionId, operationId, payload);

        locator.CountInboundCompletionServedLocally(acknowledged);
        return acknowledged ? (KeyValueResponseType.Set, anchor) : (KeyValueResponseType.MustRetry, null);
    }

    /// <summary>True when the payload records at least one working-set effect: a modified key, an acquired
    /// or released lock, a read observation, or a staged mutation. The fold itself reads the payload
    /// directly, so this predicate only drives the no-effect cancel decision above.</summary>
    private static bool HasEffect(OperationCompletionPayload payload) => payload.HasWorkingSetEffect;
}
