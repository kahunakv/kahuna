using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.KeyValues.Transactions.Data;

/// <summary>
/// Phase of a durable coordinator decision record. A committed transaction's decision is recorded as
/// <see cref="CommitDecided"/> and becomes <see cref="Completed"/> only once every participant mutation
/// and required cleanup effect has a durable acknowledgement.
/// </summary>
public enum CoordinatorDecisionStatus
{
    CommitDecided = 0,
    Completed = 1
}

/// <summary>
/// One participant mutation of a durable transaction record. The logical <see cref="Key"/> is the
/// routing authority for replay; <see cref="TicketId"/> is the participant's prepared manual proposal
/// (<see cref="HLCTimestamp.Zero"/> when none). <see cref="Acked"/> marks the participant commit durably
/// acknowledged on the anchored record; <see cref="ReceiptReleased"/> marks its completion receipt
/// released after that acknowledgement.
/// </summary>
public readonly record struct CoordinatorParticipant(
    string Key,
    KeyValueDurability Durability,
    HLCTimestamp TicketId,
    bool Acked,
    bool ReceiptReleased);

/// <summary>
/// One frozen non-modified-read/lock cleanup effect and its release progress.
/// </summary>
public readonly record struct CoordinatorCleanupEffect(string Effect, bool Released);

/// <summary>
/// A durable coordinator decision record, keyed by <see cref="TransactionId"/> and owned by the data
/// partition that currently routes <see cref="RecordAnchorKey"/>. The record is internal metadata — it
/// is not inserted into the user key-value namespace — and the participant/anchor keys are the routing
/// authority; a stored partition id would be diagnostic only and is deliberately not carried here.
/// </summary>
public sealed record CoordinatorDecisionRecord(
    HLCTimestamp TransactionId,
    string CoordinatorKey,
    string RecordAnchorKey,
    HLCTimestamp CommitTimestamp,
    CoordinatorDecisionStatus Status,
    IReadOnlyList<CoordinatorParticipant> Participants,
    IReadOnlyList<CoordinatorCleanupEffect> CleanupEffects,
    HLCTimestamp CreatedAt,
    HLCTimestamp CompletedAt)
{
    /// <summary>True once every participant is acknowledged and every cleanup effect released.</summary>
    public bool AllParticipantsAcked => Participants.All(static p => p.Acked);

    public bool AllCleanupReleased => CleanupEffects.All(static c => c.Released);
}
