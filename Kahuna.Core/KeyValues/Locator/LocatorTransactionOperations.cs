
using Kommander;
using Kommander.Time;
using Kommander.Diagnostics;

using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.CompilerServices;

using Kahuna.Server.Communication.Internode;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues.Logging;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;

using Kahuna.Server.Routing;
using Kahuna.Shared.Routing;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// Interactive transaction sessions and operation registration: start, commit, rollback, begin / complete operation, working set and close.
/// </summary>
internal sealed class LocatorTransactionOperations
{
    private readonly KeyValuesManager manager;

    private readonly IRaft raft;

    private readonly IInterNodeCommunication interNodeCommunication;

    private readonly DataPartitionRouter dataPartitionRouter;

    private readonly ILogger<IKahuna> logger;

    private readonly LocatorRouting routing;

    // Instance-owned session-registration counts, beside the process-wide meter counters: in-process test
    // clusters share the static meter across every node, so a per-node figure (how many registration calls ONE
    // node made for ONE transaction) is only readable here.
    private readonly SessionRegistrationTelemetry sessionRegistration = new();

    internal LocatorTransactionOperations(LocatorRouting routing)
    {
        this.manager = routing.Manager;
        this.raft = routing.Raft;
        this.interNodeCommunication = routing.InterNodeCommunication;
        this.dataPartitionRouter = routing.DataPartitionRouter;
        this.logger = routing.Logger;
        this.routing = routing;
    }

    /// <summary>
    /// Attempts to locate the appropriate partition leader and starts a transaction based on the provided options.
    /// </summary>
    /// <param name="options">The transaction options, including a unique identifier used to determine the partition.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A tuple containing the result of the transaction operation (<see cref="KeyValueResponseType"/>),
    /// and a <see cref="TransactionHandle"/> that must be used for all subsequent commit/rollback calls.</returns>
    public async Task<(KeyValueResponseType, TransactionHandle)> LocateAndStartTransaction(KeyValueTransactionOptions options, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(options.CoordinatorKey))
            options.CoordinatorKey = Guid.NewGuid().ToString("N");

        if (!TryPinCoordinator(options, out int partitionId))
            return new(KeyValueResponseType.InvalidInput, TransactionHandle.None);

        // A session begun on a stopping node would be coordinated by a node that is going away.
        if (routing.RefusesWhileStopping())
            return new(KeyValueResponseType.MustRetry, TransactionHandle.None);

        if (!raft.Joined || await routing.AmILeaderIfHosted(partitionId, cancellationToken))
            return await manager.StartTransaction(options);

        string? leader = await routing.TryWaitForLeader(partitionId, cancellationToken);
        if (leader is null || leader == raft.GetLocalEndpoint())
            return new(KeyValueResponseType.MustRetry, TransactionHandle.None);

        logger.LogStartTransactionRedirected(options.CoordinatorKey, partitionId, leader);

        return await interNodeCommunication.StartTransaction(leader, options, cancellationToken);
    }

    /// <summary>
    /// The partition whose leader hosts the session for <paramref name="coordinatorKey"/>: the partition a
    /// pinned key names, otherwise the hash rule on the key. A pinned partition is read from the key and
    /// never from the range map, so a split or move of the caller's range between start and commit cannot
    /// send a later call to a partition other than the session's.
    /// </summary>
    private int LocateCoordinator(string coordinatorKey) =>
        CoordinatorKeys.TryGetPinnedPartition(coordinatorKey, out int pinned) ? pinned : dataPartitionRouter.Locate(coordinatorKey);

    /// <summary>
    /// Resolves the partition a new session starts on and, when the caller chose one, rewrites
    /// <see cref="KeyValueTransactionOptions.CoordinatorKey"/> into the pinned form that carries it. A key that
    /// is already pinned (a start forwarded from another node, or a caller that composed the form) keeps its
    /// partition. False, with nothing started, for a malformed pinned key, for a pinned key that disagrees
    /// with <see cref="KeyValueTransactionOptions.CoordinatorPartitionId"/>, and for an id that names no data
    /// partition: a refused id must never fall back to the hash rule, because the caller asked for placement
    /// it would then silently not get.
    /// </summary>
    private bool TryPinCoordinator(KeyValueTransactionOptions options, out int partitionId)
    {
        string key = options.CoordinatorKey;
        int? requested = options.CoordinatorPartitionId;

        if (CoordinatorKeys.IsReserved(key))
        {
            if (!CoordinatorKeys.TryGetPinnedPartition(key, out partitionId))
            {
                logger.LogStartTransactionCoordinatorPartitionRefused(key, requested, "the key uses the reserved pinned form but does not parse");
                return false;
            }

            if (requested is { } explicitPartition && explicitPartition != partitionId)
            {
                logger.LogStartTransactionCoordinatorPartitionRefused(key, requested, "the pinned key names another partition");
                return false;
            }
        }
        else if (requested is { } chosen)
        {
            partitionId = chosen;
        }
        else
        {
            partitionId = dataPartitionRouter.Locate(key);
            return true;
        }

        if (!IsDataPartition(partitionId))
        {
            logger.LogStartTransactionCoordinatorPartitionRefused(key, requested, "no data partition has this id");
            return false;
        }

        if (!CoordinatorKeys.IsReserved(key))
            options.CoordinatorKey = CoordinatorKeys.Pin(partitionId, key);

        return true;
    }

    /// <summary>
    /// True when <paramref name="partitionId"/> names a user partition the committed partition map holds. A
    /// partition's generation starts at 1 and reads 0 once it is removed or was never created, so the
    /// generation answers without copying the map. Before this node joins a cluster no map is applied yet and
    /// only the system partition is refused; the start then runs locally whatever the id names.
    /// </summary>
    private bool IsDataPartition(int partitionId)
    {
        if (partitionId < DataPartitionRouter.FirstUserPartitionId)
            return false;

        return !raft.Joined || raft.GetPartitionGeneration(partitionId) > 0;
    }

    /// <summary>
    /// Locates the appropriate partition and commits the transaction identified by <paramref name="handle"/>.
    /// </summary>
    /// <param name="handle">The handle returned by <see cref="LocateAndStartTransaction"/>.</param>
    /// <param name="acquiredLocks">The list of keys that have been locked as part of the transaction.</param>
    /// <param name="modifiedKeys">The list of keys that have been modified as part of the transaction.</param>
    /// <param name="readKeys">The list of keys read during the transaction.</param>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <returns>
    /// A <see cref="KeyValueResponseType"/> indicating the outcome of the transaction operation.
    /// </returns>
    /// <summary>
    /// True when this node holds the interactive session for <paramref name="transactionId"/>. Sessions live on
    /// the node that began them and do not move with the coordinator partition's leadership: the session's own
    /// finalizer forwards its durable operations to whichever node leads the partition now. Routing a commit,
    /// rollback or operation for a session this node owns to the current leader — which has no such session —
    /// answered every commit in flight on a stepped-down leader with "No transaction session" until the client's
    /// deadline (~100 indeterminate commits per leader pause under a paused disk).
    /// </summary>
    private bool OwnsSession(HLCTimestamp transactionId) => manager?.Coordinator?.HasSession(transactionId) == true;


    /// <summary>
    /// Whether this leader may look for a session it does not hold on its peers: only for a request that
    /// arrived directly from a caller. A request that was already forwarded here (a non-leader routed it to
    /// the leader) is answered locally, so a transaction no node holds gets one answer instead of a round of
    /// probes per hop.
    /// </summary>
    private static bool MayProbePeersForOwner => ForwardedRequestScope.ChainedHops == 0;

    /// <summary>
    /// Offers a commit or rollback for a session this leader does not hold to each peer in turn and returns the
    /// first definitive answer: the peer that owns the session (it was begun on the previous leader and stayed
    /// there) serves it and answers Committed/Aborted/RolledBack; every other peer answers unknown (Errored) or
    /// MustRetry, which is skipped. Null when no peer served it, so the caller falls back to its local answer
    /// (a retained terminal outcome, the canonical record, or unknown). The membership roster cannot name the
    /// owner directly — peers' node ids are not carried on it — so the probe is the round trip that replaces
    /// it, and it runs only on the rare leader-but-not-owner path, never on a steady-state commit.
    /// </summary>
    private async Task<T?> ProbePeersForSessionAsync<T>(Func<string, Task<T>> ask, Func<T, KeyValueResponseType> status, CancellationToken cancellationToken) where T : struct
    {
        string local = raft.GetLocalEndpoint();

        foreach (RaftNode peer in raft.GetNodes())
        {
            if (peer.Endpoint == local)
                continue;

            try
            {
                T answer = await ask(peer.Endpoint);
                KeyValueResponseType type = status(answer);
                if (type is not (KeyValueResponseType.Errored or KeyValueResponseType.MustRetry))
                    return answer;
            }
            catch (KahunaServerException)
            {
                // The peer does not hold the session either.
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger.LogWarning(ex, "Probing {Peer} for the owner of a transaction session failed; trying the next peer", peer.Endpoint);
            }
        }

        return null;
    }

    public async Task<TransactionCommitAnswer> LocateAndCommitTransaction(TransactionHandle handle, CancellationToken cancellationToken)
    {
        if (handle.IsEmpty)
            return new TransactionCommitAnswer(KeyValueResponseType.Errored, null, CommitRefusalClass.UnknownTransaction, "The transaction handle is empty");

        // The session's owner serves it, whoever leads the coordinator partition now.
        if (OwnsSession(handle.TransactionId))
            return await manager.CommitTransaction(handle);

        // A commit of a session this node does not own is not taken on while the node stops: the sender
        // resolves the owner, or the record, through a node whose leadership view is live.
        if (routing.RefusesWhileStopping())
            return new TransactionCommitAnswer(KeyValueResponseType.MustRetry, null, CommitRefusalClass.Unresolved, "The node is shutting down");

        int partitionId = LocateCoordinator(handle.CoordinatorKey);

        if (!raft.Joined || await routing.AmILeaderIfHosted(partitionId, cancellationToken))
        {
            // This node leads the coordinator partition but did not begin the session: leadership moved after
            // the session began. Offer the commit to the peers, one of which owns it; if none does, the local
            // answer (a retained outcome, the canonical record, or unknown) stands.
            if (raft.Joined && MayProbePeersForOwner)
            {
                TransactionCommitAnswer? served = await ProbePeersForSessionAsync<TransactionCommitAnswer>(
                    peer => interNodeCommunication.CommitTransaction(peer, handle, cancellationToken), r => r.Type, cancellationToken);

                if (served is { } answer)
                    return answer;
            }

            return await manager.CommitTransaction(handle);
        }

        string? leader = await routing.TryWaitForLeader(partitionId, cancellationToken);
        if (leader is null || leader == raft.GetLocalEndpoint())
            return new TransactionCommitAnswer(KeyValueResponseType.MustRetry, null, CommitRefusalClass.Unresolved, "No leader is known for the coordinator partition");

        logger.LogCommitTransactionRedirected(handle.CoordinatorKey, partitionId, leader);

        return await interNodeCommunication.CommitTransaction(leader, handle, cancellationToken);
    }

    /// <summary>
    /// Locates and rolls back the transaction identified by <paramref name="handle"/>.
    /// </summary>
    /// <param name="handle">The handle returned by <see cref="LocateAndStartTransaction"/>.</param>
    /// <param name="cancellationToken">The token to monitor for cancellation requests.</param>
    /// <returns>A <see cref="KeyValueResponseType"/> indicating the result of the operation.</returns>
    public async Task<KeyValueResponseType> LocateAndRollbackTransaction(TransactionHandle handle, CancellationToken cancellationToken)
    {
        if (handle.IsEmpty)
            return KeyValueResponseType.Errored;

        if (OwnsSession(handle.TransactionId))
            return await manager.RollbackTransaction(handle);

        int partitionId = LocateCoordinator(handle.CoordinatorKey);

        if (!raft.Joined || await routing.AmILeaderIfHosted(partitionId, cancellationToken))
        {
            if (raft.Joined && MayProbePeersForOwner)
            {
                KeyValueResponseType? served = await ProbePeersForSessionAsync<KeyValueResponseType>(
                    peer => interNodeCommunication.RollbackTransaction(peer, handle, cancellationToken), r => r, cancellationToken);

                if (served is { } answer)
                    return answer;
            }

            return await manager.RollbackTransaction(handle);
        }

        string? leader = await routing.TryWaitForLeader(partitionId, cancellationToken);
        if (leader is null || leader == raft.GetLocalEndpoint())
            return KeyValueResponseType.MustRetry;

        logger.LogRollbackTransactionRedirected(handle.CoordinatorKey, partitionId, leader);

        return await interNodeCommunication.RollbackTransaction(leader, handle, cancellationToken);
    }

    /// <summary>
    /// Routes an operation registration to the node that leads the coordinator partition for
    /// <paramref name="coordinatorKey"/> — the node that holds the session — registering it locally
    /// when this node is that leader and forwarding otherwise.
    /// </summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<(OperationRegistrationOutcome outcome, KeyValueResponseType cachedType, long cachedRevision, HLCTimestamp cachedTimestamp, string? recordAnchorKey, TransactionConflictPolicy conflictPolicy)> LocateAndBeginOperation(string coordinatorKey, HLCTimestamp transactionId, TransactionOperationId operationId, OperationKind kind, byte[]? payloadDigest, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(coordinatorKey))
            return (OperationRegistrationOutcome.RejectedSessionClosed, KeyValueResponseType.Errored, 0, HLCTimestamp.Zero, null, TransactionConflictPolicy.Normal);

        int partitionId = LocateCoordinator(coordinatorKey);

        // The session's owner registers it, whoever leads the coordinator partition now (see OwnsSession).
        if (OwnsSession(transactionId) || !raft.Joined || await routing.AmILeaderIfHosted(partitionId, cancellationToken))
        {
            (OperationRegistrationOutcome outcome, KeyValueResponseType cachedType, long cachedRevision, HLCTimestamp cachedTimestamp, string? recordAnchorKey, TransactionConflictPolicy conflictPolicy) local =
                manager.BeginOperation(transactionId, operationId, kind, payloadDigest);

            sessionRegistration.Local(SessionRegistrationOp.Begin, IsRegistrationAccepted(local.outcome));
            return local;
        }

        string? leader = await routing.TryWaitForLeader(partitionId, cancellationToken);
        if (leader is null || leader == raft.GetLocalEndpoint())
        {
            sessionRegistration.Unrouted(SessionRegistrationOp.Begin);
            return (OperationRegistrationOutcome.AlreadyPending, KeyValueResponseType.MustRetry, 0, HLCTimestamp.Zero, null, TransactionConflictPolicy.Normal);
        }

        SessionRegistrationForward forward = sessionRegistration.BeginForward(SessionRegistrationOp.Begin);

        try
        {
            (OperationRegistrationOutcome outcome, KeyValueResponseType cachedType, long cachedRevision, HLCTimestamp cachedTimestamp, string? recordAnchorKey, TransactionConflictPolicy conflictPolicy) forwarded =
                await interNodeCommunication.BeginOperation(leader, coordinatorKey, transactionId, operationId, kind, payloadDigest, cancellationToken);

            forward.Answered(IsRegistrationAccepted(forwarded.outcome));
            return forwarded;
        }
        catch
        {
            forward.Threw();
            throw;
        }
    }

    /// <summary>Returns the partition id that owns the coordinator session for <paramref name="coordinatorKey"/>.</summary>
    public int LocatePartition(string coordinatorKey) => LocateCoordinator(coordinatorKey);

    /// <summary>The session-registration calls this node has made so far (per node, for tests and diagnostics).</summary>
    internal SessionRegistrationCounts SessionRegistrationCounts => sessionRegistration.Snapshot;

    /// <summary>The wall time this node's forwarded session-registration calls have taken so far (per node, for
    /// tests and diagnostics). Zero unless a listener asked for the duration histogram.</summary>
    internal SessionRegistrationForwardDurations SessionRegistrationForwardDurations => sessionRegistration.DurationSnapshot;

    /// <summary>
    /// A completion that arrived here and was folded into this node's own session table. The inbound landing
    /// point does not route through <see cref="LocateAndCompleteOperation"/>, so it reports the local service
    /// here; without it the local count would miss every completion a peer sent to this node, and the remote
    /// share computed from these counters would be wrong.
    /// </summary>
    internal void CountInboundCompletionServedLocally() => sessionRegistration.Local(SessionRegistrationOp.Complete, true);

    /// <summary>
    /// Opens a completion this node forwards as a receiver, after finding that the sender routed it to a
    /// replica that does not lead the coordinator partition. It is a second hop the sender's routing did not
    /// predict, so it is counted and timed on the node that actually sends it, through the same telemetry the
    /// routed exits use.
    /// </summary>
    internal SessionRegistrationForward BeginInboundCompletionRedirect() =>
        sessionRegistration.BeginForward(SessionRegistrationOp.Complete);

    /// <summary>
    /// True when a registration outcome is an answer from a live session (fresh, already pending, or already
    /// completed) rather than a rejection. Only rejections count as refused: an idempotent re-registration is
    /// a served call, and the counters measure transport cost, not logical operations.
    /// </summary>
    private static bool IsRegistrationAccepted(OperationRegistrationOutcome outcome) =>
        outcome is OperationRegistrationOutcome.New
            or OperationRegistrationOutcome.AlreadyPending
            or OperationRegistrationOutcome.AlreadyCompleted;

    /// <summary>Routes an operation completion to the coordinator-partition leader for <paramref name="coordinatorKey"/>. Returns the acknowledged outcome and the record anchor after the fold, or MustRetry when routing did not deliver the completion.</summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<(KeyValueResponseType outcome, string? anchor)> LocateAndCompleteOperation(string coordinatorKey, HLCTimestamp transactionId, TransactionOperationId operationId, OperationCompletionPayload payload, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(coordinatorKey))
            return (KeyValueResponseType.MustRetry, null);

        int partitionId = LocateCoordinator(coordinatorKey);

        if (OwnsSession(transactionId) || !raft.Joined || await routing.AmILeaderIfHosted(partitionId, cancellationToken))
        {
            string? localAnchor = manager.CompleteOperation(transactionId, operationId, payload);

            sessionRegistration.Local(SessionRegistrationOp.Complete, true);
            return (KeyValueResponseType.Set, localAnchor);
        }

        string? leader = await routing.TryWaitForLeader(partitionId, cancellationToken);
        if (leader is null || leader == raft.GetLocalEndpoint())
        {
            sessionRegistration.Unrouted(SessionRegistrationOp.Complete);
            return (KeyValueResponseType.MustRetry, null);
        }

        SessionRegistrationForward forward = sessionRegistration.BeginForward(SessionRegistrationOp.Complete);

        try
        {
            (KeyValueResponseType outcome, string? anchor) forwarded =
                await interNodeCommunication.CompleteOperation(leader, coordinatorKey, transactionId, operationId, payload, cancellationToken);

            forward.Answered(forwarded.outcome == KeyValueResponseType.Set);
            return forwarded;
        }
        catch
        {
            forward.Threw();
            throw;
        }
    }

    /// <summary>Routes a working-set query to the coordinator-partition leader for <paramref name="coordinatorKey"/>.</summary>
    public async Task<TransactionWorkingSet?> LocateAndGetTransactionWorkingSet(string coordinatorKey, HLCTimestamp transactionId, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(coordinatorKey))
            return null;

        int partitionId = LocateCoordinator(coordinatorKey);

        if (OwnsSession(transactionId) || !raft.Joined || await routing.AmILeaderIfHosted(partitionId, cancellationToken))
        {
            TransactionWorkingSet? local = manager.GetTransactionWorkingSet(transactionId);

            sessionRegistration.Local(SessionRegistrationOp.WorkingSet, local is not null);
            return local;
        }

        string? leader = await routing.TryWaitForLeader(partitionId, cancellationToken);
        if (leader is null || leader == raft.GetLocalEndpoint())
        {
            sessionRegistration.Unrouted(SessionRegistrationOp.WorkingSet);
            return null;
        }

        SessionRegistrationForward forward = sessionRegistration.BeginForward(SessionRegistrationOp.WorkingSet);

        try
        {
            TransactionWorkingSet? forwarded =
                await interNodeCommunication.GetTransactionWorkingSet(leader, coordinatorKey, transactionId, cancellationToken);

            forward.Answered(forwarded is not null);
            return forwarded;
        }
        catch
        {
            forward.Threw();
            throw;
        }
    }

    /// <summary>Routes a close-and-snapshot to the coordinator-partition leader for <paramref name="coordinatorKey"/>.</summary>
    public async Task<(KeyValueResponseType, TransactionWorkingSet?)> LocateAndCloseTransaction(string coordinatorKey, HLCTimestamp transactionId, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(coordinatorKey))
            return (KeyValueResponseType.Errored, null);

        int partitionId = LocateCoordinator(coordinatorKey);

        if (!raft.Joined || await routing.AmILeaderIfHosted(partitionId, cancellationToken))
            return await manager.CloseTransaction(transactionId, cancellationToken);

        string? leader = await routing.TryWaitForLeader(partitionId, cancellationToken);
        if (leader is null || leader == raft.GetLocalEndpoint())
            return (KeyValueResponseType.MustRetry, null);

        return await interNodeCommunication.CloseTransaction(leader, coordinatorKey, transactionId, cancellationToken);
    }
}
