
using Kommander.Data;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Handlers;

/// <summary>
/// Resolves the caller's promise from an off-mailbox two-phase-commit Raft outcome delivered by
/// <see cref="KeyValuePhaseTwoActor"/>. Runs on the participant actor mailbox so the after-Raft work
/// happens on the actor thread, never on the worker.
/// </summary>
/// <remarks>
/// This is the minimal completion path: it maps the Raft outcome to the participant response and
/// resolves the promise. The pending-op registry and the idempotent commit/rollback state apply are
/// layered on top of this by the phase-two dispatch refactor of the participant handlers.
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

        KeyValueResponse response;

        if (completion.Success)
        {
            response = completion.OpKind switch
            {
                PhaseTwoOpKind.Prepare => new KeyValueResponse(KeyValueResponseType.Prepared, completion.TicketId),
                PhaseTwoOpKind.Commit => new KeyValueResponse(KeyValueResponseType.Committed, completion.CommitIndex),
                PhaseTwoOpKind.Rollback => new KeyValueResponse(KeyValueResponseType.RolledBack, completion.CommitIndex),
                _ => KeyValueStaticResponses.ErroredResponse
            };
        }
        else if (IsTransientRaftStatus(completion.Status))
        {
            // A transient Raft failure (leadership moved, proposal timed out/queued, restore in
            // progress, deadline tripped) is retryable: surface MustRetry so the coordinator re-drives.
            response = KeyValueStaticResponses.MustRetryResponse;
        }
        else
        {
            response = KeyValueStaticResponses.ErroredResponse;
        }

        message.Promise?.TrySetResult(response);

        return response;
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
