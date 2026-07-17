
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

        // Primary duplicate-completion guard: the first completion for this id removes the pending entry
        // and applies; any later completion for the same id finds nothing and no-ops. The promise it
        // targets was already resolved by the first completion, so re-resolving is harmless.
        if (!context.PendingPhaseTwos.Remove(completion.PhaseTwoId, out PendingPhaseTwo? pending))
        {
            KeyValueResponse duplicate = MapOutcome(completion);

            message.Promise?.TrySetResult(duplicate);

            return duplicate;
        }

        KeyValueResponse response = pending.OpKind switch
        {
            PhaseTwoOpKind.Prepare => ApplyPrepare(completion),
            PhaseTwoOpKind.Commit => ApplyCommit(pending, completion),
            PhaseTwoOpKind.Rollback => ApplyRollback(pending, completion),
            _ => KeyValueStaticResponses.ErroredResponse
        };

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

        // Second line of defence behind the registry: if the tx's write intent is no longer active here
        // (no resident entry, cleared, or owned by a different tx), it was already resolved — a prior
        // completion for a coordinator retry, or an idempotent re-commit that raced ahead. Consult the
        // recent-decision set rather than re-applying, and never report a false Committed over a rollback.
        if (!context.Store.TryGetValue(pending.Key, out KeyValueEntry? entry) || entry is null
            || entry.WriteIntent is null || entry.WriteIntent.TransactionId != pending.TxId)
        {
            if (context.WasRolledBackHere(pending.TxId))
                return KeyValueStaticResponses.ErroredResponse;

            // Committed here already, or the CommitLogs succeeded durably with no conflicting decision.
            return new KeyValueResponse(KeyValueResponseType.Committed, completion.CommitIndex);
        }

        // Confirmed durable commit, first apply: the entry is pinned by its live write intent, so it is
        // resident. RecordCommitted (inside ApplyConfirmedCommit) makes this decision authoritative.
        ApplyConfirmedCommit(
            entry, pending.Proposal!, pending.TxId, pending.CurrentTime, pending.PartitionId,
            pending.RecordAnchorKey, pending.EmbeddedDecision);

        return new KeyValueResponse(KeyValueResponseType.Committed, completion.CommitIndex);
    }

    private KeyValueResponse ApplyRollback(PendingPhaseTwo pending, PhaseTwoCompletionData completion)
    {
        if (!completion.Success)
            return IsTransientRaftStatus(completion.Status)
                ? KeyValueStaticResponses.MustRetryResponse
                : KeyValueStaticResponses.ErroredResponse;

        // Second line of defence behind the registry: if the tx's write intent is no longer active here,
        // it was already resolved. Consult the recent-decision set rather than re-clearing state, and
        // never report a false RolledBack over a commit.
        if (!context.Store.TryGetValue(pending.Key, out KeyValueEntry? entry) || entry is null
            || entry.WriteIntent is null || entry.WriteIntent.TransactionId != pending.TxId)
        {
            if (context.WasCommittedHere(pending.TxId))
                return KeyValueStaticResponses.ErroredResponse;

            return new KeyValueResponse(KeyValueResponseType.RolledBack, completion.CommitIndex);
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
