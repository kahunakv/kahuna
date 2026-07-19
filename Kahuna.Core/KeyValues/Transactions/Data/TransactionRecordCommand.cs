using System.Collections.Generic;
using Kommander.Time;

namespace Kahuna.Server.KeyValues.Transactions.Data;

/// <summary>
/// A transition submitted to <see cref="TransactionRecordStateMachine.Apply"/>. Every field a transition needs
/// to be resolved deterministically on any replica — expected identity, operation id, and the attempt's HLC — is
/// carried here; apply never consults a process-local clock or hash.
/// </summary>
internal abstract record TransactionRecordCommand;

/// <summary>
/// Freezes the immutable identity of a transaction and creates it <see cref="TransactionDecision.Undecided"/>.
/// The first committed initialization wins; an exact replay is a no-op and a mismatched one for the same
/// <c>(TransactionId, Epoch)</c> is an invariant violation.
/// </summary>
internal sealed record InitializeTransactionCommand(
    HLCTimestamp TransactionId,
    long Epoch,
    string CoordinatorKey,
    string RecordAnchorKey,
    HLCTimestamp CommitTimestamp,
    HLCTimestamp DecisionDeadline,
    long ManifestHash,
    IReadOnlyList<TransactionParticipantRef> Participants,
    HLCTimestamp OpId,
    HLCTimestamp CreatedAt) : TransactionRecordCommand;

/// <summary>
/// Attempts the terminal <see cref="TransactionDecision.Commit"/> transition. Valid only from an existing
/// <see cref="TransactionDecision.Undecided"/> record whose manifest hash matches and whose decision deadline has
/// not passed (<see cref="AttemptHlc"/> &lt;= the record's frozen deadline). It can never create a record from
/// absence.
/// </summary>
internal sealed record CommitTransactionCommand(
    HLCTimestamp TransactionId,
    long Epoch,
    long ManifestHash,
    HLCTimestamp OpId,
    HLCTimestamp AttemptHlc) : TransactionRecordCommand;

/// <summary>
/// Attempts the terminal <see cref="TransactionDecision.Abort"/> transition. Valid from
/// <see cref="TransactionDecision.Undecided"/>, and — uniquely — able to create a terminal tombstone from
/// absence (the anchor fields are used only in that case), so an orphan prepare that outlived a failed anchor
/// initialization still has an authoritative outcome. Never overwrites a terminal record.
/// </summary>
internal sealed record AbortTransactionCommand(
    HLCTimestamp TransactionId,
    long Epoch,
    long ManifestHash,
    TransactionAbortClass AbortClass,
    HLCTimestamp OpId,
    HLCTimestamp AttemptHlc,
    string RecordAnchorKey,
    HLCTimestamp CommitTimestamp,
    HLCTimestamp DecisionDeadline,
    HLCTimestamp CreatedAt) : TransactionRecordCommand;
