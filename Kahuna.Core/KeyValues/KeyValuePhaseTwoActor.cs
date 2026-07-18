
using Nixie;
using Kommander;
using Kommander.Data;
using Kommander.Time;
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

    private readonly ILogger<IKahuna> logger;

    /// <summary>
    /// Test seam: null in production. When set, the worker awaits it immediately before running the
    /// phase-two Raft call, letting a test hold the op "in flight" deterministically so a
    /// mailbox-not-parked assertion is not timing-dependent. Mirrors
    /// <see cref="Persistence.BackgroundWriterActor.BeforePruneSampleHook"/>.
    /// </summary>
    internal Func<Task>? BeforeRaftCallHook;

    /// <summary>Minimum interval between emitted failure warnings, per worker (see
    /// <see cref="LogFailureRateLimited"/>).</summary>
    private const long FailureLogIntervalMs = 1000;

    private long lastFailureLogTicks;

    private int suppressedFailures;

    public KeyValuePhaseTwoActor(
        IActorContext<KeyValuePhaseTwoActor, KeyValuePhaseTwoRequest> context,
        IRaft raft,
        ILogger<IKahuna> logger
    )
    {
        this.raft = raft;
        this.logger = logger;
    }

    /// <summary>Milliseconds remaining until the dispatch deadline; <see cref="int.MaxValue"/> when the
    /// request carries no deadline. Negative once the deadline has passed.</summary>
    private static long RemainingMs(long deadlineTicks) =>
        deadlineTicks == KeyValuePhaseTwoRequest.NoDeadline ? int.MaxValue : deadlineTicks - Environment.TickCount64;

    public async Task Receive(KeyValuePhaseTwoRequest message)
    {
        // Do NOT re-read the mutable raft.Joined here. The participant handler only dispatches to this
        // worker when the node was joined at dispatch time (a not-joined node applies inline, never
        // dispatches). Short-circuiting to a fabricated success because membership changed after the
        // request queued (e.g. LeaveCluster set Joined=false) would report a commit/rollback as durable
        // without ever settling the Raft ticket. Always run the Raft op and surface its real
        // leadership/cancellation/failure status.

        // Test seam (null in production): hold the op in flight before the Raft call.
        if (BeforeRaftCallHook is { } gate)
            await gate();

        // Bound the Raft wait by the time remaining to the dispatch deadline — NOT a fresh full timeout —
        // so queue time counts against the budget: an op that spent most of its window queued behind other
        // work gets only the leftover for its Raft call, tripping quickly to OperationCancelled (transient,
        // re-driven by the coordinator) instead of waiting a full timeout after dequeue. A request already
        // past its deadline still makes one minimal (1 ms) attempt rather than being skipped — the attempt
        // may still land (Kommander applies a cancelled commit late), which the idempotent retry needs. No
        // deadline (timeout disabled) leaves the wait unbounded.
        using CancellationTokenSource? cts = message.DeadlineTicks != KeyValuePhaseTwoRequest.NoDeadline
            ? new CancellationTokenSource((int)Math.Clamp(RemainingMs(message.DeadlineTicks), 1, int.MaxValue))
            : null;
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
                        expectedGeneration: message.RoutedGeneration,
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
            LogFailureRateLimited(message.OpKind, message.PartitionId, status);

        SendCompletion(message, success, status, commitIndex, ticketId);
    }

    /// <summary>
    /// Logs a phase-two failure at warning level at most once per <see cref="FailureLogIntervalMs"/> per
    /// worker, folding the count of suppressed failures into the next emitted line. A cluster-wide outage
    /// with many keys and retries would otherwise emit thousands of near-identical warnings; this caps
    /// the volume while preserving a sampled, counted diagnostic. Actor-thread only — no locking.
    /// </summary>
    private void LogFailureRateLimited(PhaseTwoOpKind opKind, int partitionId, RaftOperationStatus status)
    {
        long now = Environment.TickCount64;
        if (now - lastFailureLogTicks < FailureLogIntervalMs)
        {
            suppressedFailures++;
            return;
        }

        int suppressed = suppressedFailures;
        suppressedFailures = 0;
        lastFailureLogTicks = now;

        if (suppressed > 0)
            logger.LogWarning(
                "KeyValuePhaseTwoActor: {OpKind} on partition {Partition} failed Status={Status} (+{Suppressed} similar suppressed)",
                opKind, partitionId, status, suppressed);
        else
            logger.LogWarning(
                "KeyValuePhaseTwoActor: {OpKind} on partition {Partition} failed Status={Status}",
                opKind, partitionId, status);
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
