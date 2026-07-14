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
/// The shape of a frozen cleanup effect, which selects how it is released on replay.
/// </summary>
public enum CoordinatorCleanupKind
{
    /// <summary>A point lock and/or read-MVCC snapshot on a single key: a ticket-free exclusive-lock release.</summary>
    KeyRelease = 0,

    /// <summary>A prefix lock: an exclusive prefix-lock release.</summary>
    PrefixLock = 1,

    /// <summary>A range lock: an exclusive range-lock release over the frozen bounds.</summary>
    RangeLock = 2
}

/// <summary>
/// One frozen non-modified-read/lock cleanup effect of a committed transaction and its release progress.
/// Modified keys are recorded as participants (their commit releases them) and are never cleanup effects;
/// these cover the non-modified point locks, read-MVCC snapshots, prefix locks, and range locks that the
/// live finalize would otherwise release only best-effort. <see cref="Key"/> is the point/read key for
/// <see cref="CoordinatorCleanupKind.KeyRelease"/> and the key-space prefix for the prefix/range kinds; the
/// range bounds apply only to <see cref="CoordinatorCleanupKind.RangeLock"/>. The key/prefix is the routing
/// authority for replay. <see cref="Released"/> marks the release durably acknowledged on the record.
/// </summary>
public readonly record struct CoordinatorCleanupEffect(
    CoordinatorCleanupKind Kind,
    string Key,
    KeyValueDurability Durability,
    string? StartKey,
    bool StartInclusive,
    string? EndKey,
    bool EndInclusive,
    bool Released)
{
    /// <summary>A non-modified point lock and/or read-MVCC snapshot on a single key.</summary>
    public static CoordinatorCleanupEffect ForKey(string key, KeyValueDurability durability) =>
        new(CoordinatorCleanupKind.KeyRelease, key, durability, null, false, null, false, false);

    /// <summary>A held prefix lock.</summary>
    public static CoordinatorCleanupEffect ForPrefix(string prefix, KeyValueDurability durability) =>
        new(CoordinatorCleanupKind.PrefixLock, prefix, durability, null, false, null, false, false);

    /// <summary>A held range lock over its frozen bounds.</summary>
    public static CoordinatorCleanupEffect ForRange(RangeLockKey range) =>
        new(CoordinatorCleanupKind.RangeLock, range.Prefix, range.Durability,
            range.StartKey, range.StartInclusive, range.EndKey, range.EndInclusive, false);
}

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
