
using Kommander.Data;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Handlers;

/// <summary>
/// Applies the outcome of an off-mailbox two-phase-commit Raft call (delivered by
/// <see cref="KeyValuePhaseTwoActor"/>) on the participant actor mailbox, keyed by the pending entry
/// the participant handler parked when it dispatched. Runs the same after-Raft state mutation the
/// inline participant handlers run today, then resolves the caller's promise.
/// </summary>
/// <remarks>
/// Idempotency has two layers. First: the completion removes its <see cref="PendingPhaseTwo"/> from the
/// registry, so a duplicate completion for the same id finds nothing and no-ops. Second: the apply
/// re-checks the live write intent, so even a completion that races a re-commit does not double-apply.
/// </remarks>
internal sealed class CompletePhaseTwoHandler : BaseHandler
{
    public CompletePhaseTwoHandler(KeyValueContext context) : base(context)
    {

    }

    public KeyValueResponse Execute(KeyValueRequest message)
    {
        PhaseTwoCompletionData? completion = message.PhaseTwoCompletionData;

        if (completion is null)
        {
            context.Logger.LogWarning("KeyValueActor/CompletePhaseTwo: missing completion payload");

            message.Promise?.TrySetResult(KeyValueStaticResponses.ErroredResponse);

            return KeyValueStaticResponses.ErroredResponse;
        }

        // A completion whose pending entry is already gone is a duplicate (or arrived after a retry
        // advanced the state); the promise was already resolved, so re-resolving is harmless.
        if (!context.PendingPhaseTwos.TryGetValue(completion.PhaseTwoId, out PendingPhaseTwo? pending))
        {
            KeyValueResponse duplicate = MapOutcome(completion);

            message.Promise?.TrySetResult(duplicate);

            return duplicate;
        }

        // Correlation guard: the completion's op kind must match the parked entry's. A mismatch is a
        // misrouted/malformed internal completion — surface a protocol error without consuming the pending
        // entry (its real completion still owns it) or applying under the wrong operation.
        if (completion.OpKind != pending.OpKind)
        {
            context.Logger.LogError(
                "KeyValueActor/CompletePhaseTwo: op-kind mismatch for id {Id} — completion {CompletionKind}, pending {PendingKind}",
                completion.PhaseTwoId, completion.OpKind, pending.OpKind);

            message.Promise?.TrySetResult(KeyValueStaticResponses.ErroredResponse);

            return KeyValueStaticResponses.ErroredResponse;
        }

        // Primary duplicate-completion guard: the first completion for this id removes the pending entry
        // and applies; any later completion for the same id finds nothing and no-ops.
        context.PendingPhaseTwos.Remove(completion.PhaseTwoId);

        KeyValueResponse response;
        try
        {
            response = pending.OpKind switch
            {
                PhaseTwoOpKind.Prepare => ApplyPrepare(completion),
                PhaseTwoOpKind.Commit => ApplyCommit(pending, completion),
                PhaseTwoOpKind.Rollback => ApplyRollback(pending, completion),
                _ => KeyValueStaticResponses.ErroredResponse
            };
        }
        catch (Exception ex)
        {
            // The after-Raft apply threw. Never leak the caller's promise: resolve it retryable so the
            // coordinator re-drives (idempotent — the receipt/recent-decision check and re-apply reconcile
            // any partial state). The pending entry is already removed; the re-drive registers a fresh one.
            context.Logger.LogError(ex, "KeyValueActor/CompletePhaseTwo: apply threw for {OpKind} {TxId}", pending.OpKind, pending.TxId);
            response = KeyValueStaticResponses.MustRetryResponse;
        }

        message.Promise?.TrySetResult(response);

        return response;
    }

    /// <summary>
    /// Prepare has no state apply on completion — the write intent was set before dispatch. Resolve the
    /// proposal ticket on success; on any replication failure resolve Errored (the write-intent lease
    /// self-heals and the coordinator rolls back), matching the inline prepare's terminal outcome.
    /// </summary>
    private static KeyValueResponse ApplyPrepare(PhaseTwoCompletionData completion)
    {
        return completion.Success
            ? new KeyValueResponse(KeyValueResponseType.Prepared, completion.TicketId)
            : KeyValueStaticResponses.ErroredResponse;
    }

    private KeyValueResponse ApplyCommit(PendingPhaseTwo pending, PhaseTwoCompletionData completion)
    {
        // Transient Raft failure: preserve MVCC + write intent so the coordinator can re-drive the
        // commit against the newly elected leader — idempotent once it commits. Permanent failure:
        // preserve state so the coordinator can roll back. Neither clears participant state.
        if (!completion.Success)
            return IsTransientRaftStatus(completion.Status)
                ? KeyValueStaticResponses.MustRetryResponse
                : KeyValueStaticResponses.ErroredResponse;

        // The tx's write intent is no longer active here (no resident entry, cleared, or owned by a
        // different tx): the local apply already happened, or state was lost. Positive identification —
        // report Committed only with local proof that this node applied the commit: the recent-decision
        // set, or a durable completion receipt (recorded by the replication apply that ran during
        // CommitLogs, or by an earlier retry's completion). Record the decision so this terminal path is
        // authoritative too. Without proof, do not fabricate a Committed over missing local state — return
        // retryable so the coordinator re-drives (idempotent); the apply/receipt reconciles it.
        if (!context.Store.TryGetValue(pending.Key, out KeyValueEntry? entry) || entry is null
            || entry.WriteIntent is null || entry.WriteIntent.TransactionId != pending.TxId)
        {
            if (context.WasCommittedHere(pending.TxId)
                || context.CompletionReceiptStore.Contains(pending.TxId, pending.Key, pending.Durability))
            {
                context.RecordCommitted(pending.TxId);
                return new KeyValueResponse(KeyValueResponseType.Committed, completion.CommitIndex);
            }

            if (context.WasRolledBackHere(pending.TxId))
                return KeyValueStaticResponses.ErroredResponse;

            return KeyValueStaticResponses.MustRetryResponse;
        }

        // Confirmed durable commit, first apply: the entry is pinned by its live write intent, so it is
        // resident. RecordCommitted (inside ApplyConfirmedCommit) makes this decision authoritative.
        ApplyConfirmedCommit(
            entry, pending.Proposal!, pending.TxId, pending.CurrentTime, pending.PartitionId,
            pending.RecordAnchorKey);

        return new KeyValueResponse(KeyValueResponseType.Committed, completion.CommitIndex);
    }

    private KeyValueResponse ApplyRollback(PendingPhaseTwo pending, PhaseTwoCompletionData completion)
    {
        if (!completion.Success)
            return IsTransientRaftStatus(completion.Status)
                ? KeyValueStaticResponses.MustRetryResponse
                : KeyValueStaticResponses.ErroredResponse;

        // The tx's write intent is no longer active here: it was already resolved. Positive identification —
        // report RolledBack only with local proof (the recent-rolled-back set); record it so this path is
        // authoritative. Never report RolledBack over a commit recorded here. Without proof, return retryable.
        if (!context.Store.TryGetValue(pending.Key, out KeyValueEntry? entry) || entry is null
            || entry.WriteIntent is null || entry.WriteIntent.TransactionId != pending.TxId)
        {
            if (context.WasRolledBackHere(pending.TxId))
            {
                context.RecordRolledBack(pending.TxId);
                return new KeyValueResponse(KeyValueResponseType.RolledBack, completion.CommitIndex);
            }

            if (context.WasCommittedHere(pending.TxId))
                return KeyValueStaticResponses.ErroredResponse;

            return KeyValueStaticResponses.MustRetryResponse;
        }

        // Confirmed rollback, first apply. RecordRolledBack (inside ApplyConfirmedRollback) makes this
        // decision authoritative.
        ApplyConfirmedRollback(entry, pending.TxId, pending.CurrentTime);

        return new KeyValueResponse(KeyValueResponseType.RolledBack, completion.CommitIndex);
    }

    /// <summary>
    /// Maps a completion outcome to the participant response without applying state — used for a
    /// duplicate completion whose pending entry is already gone (the promise is already resolved).
    /// </summary>
    private static KeyValueResponse MapOutcome(PhaseTwoCompletionData completion)
    {
        if (!completion.Success)
            return IsTransientRaftStatus(completion.Status)
                ? KeyValueStaticResponses.MustRetryResponse
                : KeyValueStaticResponses.ErroredResponse;

        return completion.OpKind switch
        {
            PhaseTwoOpKind.Prepare => new KeyValueResponse(KeyValueResponseType.Prepared, completion.TicketId),
            PhaseTwoOpKind.Commit => new KeyValueResponse(KeyValueResponseType.Committed, completion.CommitIndex),
            PhaseTwoOpKind.Rollback => new KeyValueResponse(KeyValueResponseType.RolledBack, completion.CommitIndex),
            _ => KeyValueStaticResponses.ErroredResponse
        };
    }

    /// <summary>
    /// Transient Raft statuses that warrant a retry rather than a terminal error, mirroring the
    /// participant handlers' classification of a re-drivable phase-two failure.
    /// </summary>
    private static bool IsTransientRaftStatus(RaftOperationStatus status) => status is
        RaftOperationStatus.NodeIsNotLeader or
        RaftOperationStatus.ProposalQueueFull or
        RaftOperationStatus.RestoreInProgress or
        RaftOperationStatus.ProposalTimeout or
        RaftOperationStatus.ReplicationFailed or
        RaftOperationStatus.OperationCancelled;
}
