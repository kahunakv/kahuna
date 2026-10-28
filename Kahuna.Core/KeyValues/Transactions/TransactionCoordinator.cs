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

/// <summary>
/// Manages interactive transaction sessions and the generic 2PC lifecycle.
/// Has no dependency on the script parser or AST. Script execution uses
/// <see cref="ScriptTransactionExecutor"/> which composes this coordinator.
/// </summary>
internal sealed partial class TransactionCoordinator : IDisposable
{
    /// <summary>The longest transaction origin label accepted at start.</summary>
    internal const int MaxOriginLength = 64;

    /// <summary>
    /// Grace past a session's timeout before the reaper reclaims it. At least the write-intent TTL
    /// (DefaultTxCompleteTimeout, 15 s), so a reaped session's late commit fails on expired intents.
    /// Session-owned intents have no deadline; <see cref="SessionMaxLifespanMs"/> gives them the same bound.
    /// </summary>
    internal const int ReapGraceMs = 15_000;

    /// <summary>
    /// How long, in milliseconds, a participant keeps the effect of a dispatched operation (the write-intent
    /// lease, DefaultTxCompleteTimeout, 15 s). A reaped session with unresolved dispatched operations is kept at
    /// least this long past its deadline, so the reaper never drops a session whose effect could still land.
    /// </summary>
    internal const int MaxParticipantEffectTtlMs = 15_000;

    /// <summary>
    /// Longest span, in milliseconds, that an interactive session can still matter: the maximum timeout, plus the
    /// reaper grace, plus the time a dispatched effect can still land. After this span from its start, a session
    /// is gone and anything it left is orphaned. The MVCC snapshot trim and the session-owned lock ceiling both
    /// read it from here so they cannot drift apart.
    /// </summary>
    internal static int SessionMaxLifespanMs(KahunaConfiguration configuration) =>
        configuration.MaxTransactionTimeout + ReapGraceMs + MaxParticipantEffectTtlMs;

    /// <summary>
    /// Maximum phase-two (commit/rollback) retries when a participant returns MustRetry for a transient Raft
    /// condition, such as NodeIsNotLeader after an election. With Phase2RetryDelayMs = 250 this gives
    /// leadership up to 5 s to settle.
    /// </summary>
    internal const int MaxPhase2Retries = 20;

    /// <summary>Delay between phase-two retry attempts, in milliseconds.</summary>
    internal const int Phase2RetryDelayMs = 250;

    private readonly KeyValuesManager manager;

    private readonly KahunaConfiguration configuration;

    private readonly IRaft raft;

    private readonly ILogger<IKahuna> logger;

    internal readonly ConcurrentDictionary<HLCTimestamp, TransactionContext> sessions = new();

    /// <summary>True while this node holds the interactive session for <paramref name="transactionId"/>. A session
    /// stays on the node that began it, even when coordinator partition leadership moves, so routed entry points
    /// serve a local session whoever the leader is.</summary>
    internal bool HasSession(HLCTimestamp transactionId) => sessions.ContainsKey(transactionId);

    /// <summary>
    /// Durable transactions in finalize on this node. Reserved before prepare, released when the attempt ends
    /// (decision installed or given up). Caps durable inflow at <c>DurableDecisionOutstandingMax</c>, so a burst
    /// gets <see cref="KeyValueResponseType.MustRetry"/> instead of unbounded prepared state. The cap keeps room in
    /// the write scheduler's terminal-class reserve (per partition and global) for every admitted transaction's
    /// decision and settle. A configured value &lt;= 0 disables the gate.
    /// </summary>
    private int outstandingDurable;

    /// <summary>
    /// Best-effort cache of finalized outcomes after the session leaves <see cref="sessions"/>. A duplicate
    /// commit/rollback replays the recorded answer (Committed/RolledBack/expired) instead of an unknown result.
    /// Bounded by size and HLC age (<c>TransactionOutcomeRetentionMax</c> / <c>TransactionOutcomeRetentionTtl</c>).
    /// After eviction, a duplicate consults the durable record: with a routable anchor it gets the record's
    /// outcome, or <see cref="KeyValueResponseType.MustRetry"/> while the record is absent. Only an unanchored
    /// handle gets <see cref="KeyValueResponseType.Errored"/>; never a conflict Aborted.
    /// Lock-free and O(1); see <see cref="TerminalOutcomeWindow"/> for why.
    /// </summary>
    private readonly TerminalOutcomeWindow terminalOutcomes = new();

    /// <summary>
    /// Admission gate for interactive sessions, separate from the script executor's. A session holds its slot
    /// while the client keeps it open, so a shared pool would let idle sessions starve script transactions.
    /// </summary>
    private readonly TransactionPriorityOrderer orderer;

    /// <summary>Signaled when the node begins to shut down; ends the detached retry of a commit's lock releases.</summary>
    private readonly CancellationToken shutdown;

    /// <summary>First delay of the detached retry of unacknowledged lock releases; doubles each attempt up to
    /// <see cref="LockReleaseRetryMaxDelayMs"/>.</summary>
    internal const int LockReleaseRetryInitialDelayMs = 50;

    internal const int LockReleaseRetryMaxDelayMs = 1_000;

    public TransactionCoordinator(KeyValuesManager manager, KahunaConfiguration configuration, IRaft raft, ILogger<IKahuna> logger, TransactionPriorityOrderer orderer, CancellationToken shutdown = default)
    {
        this.manager = manager;
        this.configuration = configuration;
        this.raft = raft;
        this.logger = logger;
        this.orderer = orderer;
        this.shutdown = shutdown;
    }

    /// <summary>
    /// Starts an interactive transaction session. The returned handle pins the session to the coordinator
    /// partition and must be passed on every later commit/rollback.
    /// </summary>
    public async Task<(KeyValueResponseType, TransactionHandle)> StartTransaction(KeyValueTransactionOptions options)
    {
        string coordinatorKey = string.IsNullOrEmpty(options.CoordinatorKey)
            ? Guid.NewGuid().ToString("N")
            : options.CoordinatorKey;

        // Reject out-of-range policy values. Begin casts wire numbers straight to these enums, so an unknown
        // value would otherwise fall into some branch instead of being reported as malformed input.
        if (!Enum.IsDefined(options.Locking) ||
            !Enum.IsDefined(options.ReadValidation) ||
            !Enum.IsDefined(options.DecisionDurability))
            return (KeyValueResponseType.InvalidInput, new TransactionHandle(HLCTimestamp.Zero, coordinatorKey));

        // Priority is normalized, not rejected. The policies above change what the transaction means, so an
        // unknown value must never be defaulted. Priority only orders when work starts, so an unknown value runs
        // as ordinary work (matching the gRPC contract) and is never read as Critical.
        TransactionPriority priority = TransactionPriorityOrderer.Normalize(options.Priority);

        // A fixed read snapshot cannot see writes that land after it, so it cannot back write-skew validation.
        // Reject the combination instead of promising a guarantee the engine cannot keep.
        if (options.ReadTimestamp != HLCTimestamp.Zero && options.ReadValidation == ReadValidation.TrackAndValidate)
            return (KeyValueResponseType.InvalidInput, new TransactionHandle(HLCTimestamp.Zero, coordinatorKey));

        // The origin travels on every prepared intent of the transaction, so it is kept short.
        if (options.Origin is { Length: > MaxOriginLength })
            return (KeyValueResponseType.InvalidInput, new TransactionHandle(HLCTimestamp.Zero, coordinatorKey));

        // Clamp to the server maximum. Age-based cleanup of orphaned MVCC read snapshots depends on this bound.
        int timeout = Math.Min(
            options.Timeout <= 0 ? configuration.DefaultTransactionTimeout : options.Timeout,
            configuration.MaxTransactionTimeout);

        // How long the caller may queue for admission. This is not the session timeout: that bounds how long a
        // transaction lives, this bounds how long the caller waits to start one. Clamped so no caller holds a
        // queue slot longer than the operator allows.
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
                // The admission wait expired. Nothing was created, but the node is saturated: unlike MustRetry,
                // the caller should back off before it retries.
                return (KeyValueResponseType.AdmissionRefused, new TransactionHandle(HLCTimestamp.Zero, coordinatorKey));
            }
        }

        // The wait queue is full. Same contract as an expired wait: a retry is safe, after a back-off.
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
                    ConflictPolicy     = TransactionConflictPolicyWire.Normalize(options.ConflictPolicy),
                    // Written into the intents only once every node reads it (see the setting).
                    Origin             = configuration.TransactionOriginTags && !string.IsNullOrEmpty(options.Origin) ? options.Origin : null,
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
            // The slot belongs to the session only once it is published. If anything throws before that (a clock
            // fault, teardown), release it here, or the node loses that capacity for good.
            if (!added)
                lease.Dispose();
        }

        logger.LogStartedInteractiveTransaction(transactionId);

        return (KeyValueResponseType.Set, new TransactionHandle(transactionId, coordinatorKey));
    }

    /// <summary>
    /// Removes a finalized session and returns its admission slot. Commit, rollback and the reaper all end
    /// sessions here, so capacity recovers even when the client never returns. Releasing the lease is
    /// idempotent, so a racing second finalize is harmless.
    /// </summary>
    private void FinalizeSession(HLCTimestamp transactionId)
    {
        if (sessions.TryRemove(transactionId, out TransactionContext? context))
            context.AdmissionLease?.Dispose();
    }

    /// <summary>
    /// The decision-durability policy <see cref="StartTransaction"/> recorded for an active session, or null
    /// when there is no such session.
    /// </summary>
    internal DecisionDurability? GetRecordedDecisionDurability(HLCTimestamp transactionId)
        => sessions.TryGetValue(transactionId, out TransactionContext? context) ? context.DecisionDurability : null;

    /// <summary>
    /// The session timeout (milliseconds, after the <c>MaxTransactionTimeout</c> clamp in
    /// <see cref="StartTransaction"/>) for an active session, or null when there is no such session.
    /// </summary>
    internal int? GetRecordedSessionTimeout(HLCTimestamp transactionId)
        => sessions.TryGetValue(transactionId, out TransactionContext? context) ? context.Timeout : null;

    // Fixed per transaction id: a retried finalize drives the same record (record CAS and intent idempotency
    // converge), so it never creates a second one.
    private const long DurableFinalizeEpoch = 1;

    public void Dispose()
    {
        // Give in-flight deferred resolutions a bounded time to finish, then cancel the rest (recovery completes
        // them on restart).
#if !KAHUNA_THREAD_FREE
        // Skipped in the thread-free build: the resolutions run on the thread this wait blocks, so it could only
        // time out.
        try { Task.WhenAll(deferredResolutions.Keys).Wait(TimeSpan.FromSeconds(5)); } catch { /* best-effort drain */ }
#endif
        try { deferredResolutionCts.Cancel(); } catch (ObjectDisposedException) { /* already disposed */ }
        deferredResolutionCts.Dispose();
        durableFinalizer?.Dispose();
    }
}
