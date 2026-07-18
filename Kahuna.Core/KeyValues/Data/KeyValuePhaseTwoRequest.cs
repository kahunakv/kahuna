
using Nixie;
using Kommander.Time;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// A unit of two-phase-commit Raft work handed to the off-mailbox <see cref="KeyValuePhaseTwoActor"/>
/// so the participant's <c>KeyValueActor</c> is not parked on the Raft round trip. The worker runs the
/// matching Raft call under a deadline and sends a <c>CompletePhaseTwo</c> message back to
/// <see cref="KeyValueActor"/> carrying the outcome and the caller's <see cref="Promise"/>; it never
/// touches actor state directly.
/// </summary>
internal sealed class KeyValuePhaseTwoRequest
{
    /// <summary>Sentinel <see cref="DeadlineTicks"/> meaning "no deadline" (the timeout is disabled).</summary>
    public const long NoDeadline = long.MaxValue;

    /// <summary>Which Raft round trip to run: prepare proposal, commit, or rollback.</summary>
    public PhaseTwoOpKind OpKind { get; }

    /// <summary>Monotonic correlation id the completion carries back so the originating actor can
    /// match this outcome to its pending phase-two entry.</summary>
    public int PhaseTwoId { get; }

    /// <summary>The Raft partition the proposal/commit/rollback targets.</summary>
    public int PartitionId { get; }

    /// <summary>Serialized committed <c>KeyValueMessage</c> to propose. Non-null only for
    /// <see cref="PhaseTwoOpKind.Prepare"/>.</summary>
    public byte[]? SerializedMessage { get; }

    /// <summary>The prepared proposal ticket to commit or roll back. Zero for
    /// <see cref="PhaseTwoOpKind.Prepare"/> (the ticket is produced by the proposal).</summary>
    public HLCTimestamp TicketId { get; }

    /// <summary>The originating participant actor the completion message is sent back to.</summary>
    public IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> KeyValueActor { get; }

    /// <summary>The caller's promise, resolved by the actor-owned completion handler.</summary>
    public TaskCompletionSource<KeyValueResponse?> Promise { get; }

    /// <summary>Absolute deadline (in <see cref="Environment.TickCount64"/> monotonic ms) computed at
    /// dispatch, so the timeout covers dispatch-to-resolution — queue time plus the Raft call — not just
    /// the Raft call after dequeue. <see cref="NoDeadline"/> when the timeout is disabled.</summary>
    public long DeadlineTicks { get; }

    /// <summary>Key-range routing generation the prepare was resolved on, threaded into the worker's
    /// <c>ReplicateLogs(expectedGeneration:)</c> so a split/move during the queue delay is fenced (the
    /// proposal is rejected rather than landing on a stale partition). 0 for hash spaces and for
    /// commit/rollback (which target a ticket, not a routed proposal).</summary>
    public long RoutedGeneration { get; }

    private KeyValuePhaseTwoRequest(
        PhaseTwoOpKind opKind,
        int phaseTwoId,
        int partitionId,
        byte[]? serializedMessage,
        HLCTimestamp ticketId,
        long deadlineTicks,
        long routedGeneration,
        IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> keyValueActor,
        TaskCompletionSource<KeyValueResponse?> promise
    )
    {
        OpKind = opKind;
        PhaseTwoId = phaseTwoId;
        PartitionId = partitionId;
        SerializedMessage = serializedMessage;
        TicketId = ticketId;
        DeadlineTicks = deadlineTicks;
        RoutedGeneration = routedGeneration;
        KeyValueActor = keyValueActor;
        Promise = promise;
    }

    /// <summary>Computes the absolute dispatch deadline from a timeout in ms (≤ 0 disables it).</summary>
    public static long DeadlineFrom(int timeoutMs) =>
        timeoutMs > 0 ? Environment.TickCount64 + timeoutMs : NoDeadline;

    /// <summary>Builds a prepare request that proposes <paramref name="serializedMessage"/> to
    /// <paramref name="partitionId"/> with <c>autoCommit:false</c>; the proposal yields the ticket.</summary>
    public static KeyValuePhaseTwoRequest ForPrepare(
        int phaseTwoId,
        int partitionId,
        byte[] serializedMessage,
        long deadlineTicks,
        long routedGeneration,
        IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> keyValueActor,
        TaskCompletionSource<KeyValueResponse?> promise
    ) => new(PhaseTwoOpKind.Prepare, phaseTwoId, partitionId, serializedMessage, HLCTimestamp.Zero, deadlineTicks, routedGeneration, keyValueActor, promise);

    /// <summary>Builds a commit request for a previously prepared <paramref name="ticketId"/>.</summary>
    public static KeyValuePhaseTwoRequest ForCommit(
        int phaseTwoId,
        int partitionId,
        HLCTimestamp ticketId,
        long deadlineTicks,
        IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> keyValueActor,
        TaskCompletionSource<KeyValueResponse?> promise
    ) => new(PhaseTwoOpKind.Commit, phaseTwoId, partitionId, null, ticketId, deadlineTicks, 0, keyValueActor, promise);

    /// <summary>Builds a rollback request for a previously prepared <paramref name="ticketId"/>.</summary>
    public static KeyValuePhaseTwoRequest ForRollback(
        int phaseTwoId,
        int partitionId,
        HLCTimestamp ticketId,
        long deadlineTicks,
        IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> keyValueActor,
        TaskCompletionSource<KeyValueResponse?> promise
    ) => new(PhaseTwoOpKind.Rollback, phaseTwoId, partitionId, null, ticketId, deadlineTicks, 0, keyValueActor, promise);
}
