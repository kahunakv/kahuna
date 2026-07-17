
using Nixie;
using Kommander;
using Kommander.Data;
using Kommander.Time;
using Kahuna.Server.Configuration;
using Kahuna.Server.Replication;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// Off-mailbox worker for two-phase-commit Raft round trips. A participant <c>KeyValueActor</c>
/// dispatches its prepare/commit/rollback replication here (via <c>.Send</c>) instead of awaiting the
/// Raft call inline, so an unrelated key owned by the same actor does not stall behind one
/// transaction's Raft round trip. The worker runs the matching <see cref="IRaft"/> call under a
/// bounded deadline and sends a <c>CompletePhaseTwo</c> message back to the originating actor carrying
/// the outcome and the caller's promise. It never touches the participant actor's state directly.
/// </summary>
internal sealed class KeyValuePhaseTwoActor : IActor<KeyValuePhaseTwoRequest>
{
    private readonly IRaft raft;

    private readonly KahunaConfiguration configuration;

    private readonly ILogger<IKahuna> logger;

    public KeyValuePhaseTwoActor(
        IActorContext<KeyValuePhaseTwoActor, KeyValuePhaseTwoRequest> context,
        IRaft raft,
        KahunaConfiguration configuration,
        ILogger<IKahuna> logger
    )
    {
        this.raft = raft;
        this.configuration = configuration;
        this.logger = logger;
    }

    public async Task Receive(KeyValuePhaseTwoRequest message)
    {
        // Not joined to a Raft cluster (embedded single-node without replication): the participant
        // handlers treat the Raft round trip as an immediate success, so mirror that here.
        if (!raft.Joined)
        {
            SendCompletion(message, success: true, RaftOperationStatus.Success, commitIndex: 0, HLCTimestamp.Zero);
            return;
        }

        // Bound the Raft wait so a stuck partition (no leader, quorum loss, WAL stall) cannot park
        // the worker indefinitely. On the deadline the call surfaces OperationCancelled, classified
        // transient by the participant's completion path, and the coordinator re-drives the same
        // ticket — idempotent once it commits. A non-positive timeout disables the bound.
        int timeoutMs = configuration.Phase2CommitTimeout;
        using CancellationTokenSource? cts = timeoutMs > 0 ? new CancellationTokenSource(timeoutMs) : null;
        CancellationToken token = cts?.Token ?? CancellationToken.None;

        bool success;
        RaftOperationStatus status;
        long commitIndex = -1;
        HLCTimestamp ticketId = HLCTimestamp.Zero;

        try
        {
            switch (message.OpKind)
            {
                case PhaseTwoOpKind.Prepare:
                {
                    RaftReplicationResult result = await raft.ReplicateLogs(
                        message.PartitionId,
                        ReplicationTypes.KeyValues,
                        message.SerializedMessage!,
                        autoCommit: false,
                        cancellationToken: token
                    );

                    success = result.Success;
                    status = result.Status;
                    ticketId = result.TicketId;
                    break;
                }

                case PhaseTwoOpKind.Commit:
                {
                    (success, status, commitIndex) = await raft.CommitLogs(message.PartitionId, message.TicketId, token);
                    break;
                }

                case PhaseTwoOpKind.Rollback:
                {
                    (success, status, commitIndex) = await raft.RollbackLogs(message.PartitionId, message.TicketId, token);
                    break;
                }

                default:
                    success = false;
                    status = RaftOperationStatus.Errored;
                    break;
            }
        }
        catch (OperationCanceledException)
        {
            // The deadline tripped. Report the retryable cancellation so the participant returns
            // MustRetry with its state retained and the coordinator re-drives the ticket.
            success = false;
            status = RaftOperationStatus.OperationCancelled;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "KeyValuePhaseTwoActor: {OpKind} on partition {Partition} threw", message.OpKind, message.PartitionId);
            success = false;
            status = RaftOperationStatus.Errored;
        }

        if (!success)
            logger.LogWarning("KeyValuePhaseTwoActor: {OpKind} on partition {Partition} failed Status={Status}", message.OpKind, message.PartitionId, status);

        SendCompletion(message, success, status, commitIndex, ticketId);
    }

    private static void SendCompletion(
        KeyValuePhaseTwoRequest message,
        bool success,
        RaftOperationStatus status,
        long commitIndex,
        HLCTimestamp ticketId
    )
    {
        message.KeyValueActor.Send(KeyValueRequest.ForCompletePhaseTwo(
            new PhaseTwoCompletionData(message.PhaseTwoId, message.OpKind, success, status, commitIndex, ticketId),
            message.Promise));
    }
}
