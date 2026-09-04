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
///
/// <para><see cref="BundledPrepareKeys"/> is set only by the one-phase fast path, whose commit shares one atomic
/// batch with its own prepared-intent group and so cannot be withheld if that prepare is rejected. When non-empty,
/// the transition is additionally legal only if a live prepared intent owned by this (TransactionId, Epoch) exists
/// at every listed key at apply time — the prepare applies earlier in the same batch, so the check is
/// deterministic in log order on every replica. Without it, a bundle that applies after a competing transaction
/// took the key (a stalled proposal surfacing after a partition heals, with the in-memory locks that justified
/// deciding up front long gone) would durably record Commit for a mutation that was never durably prepared —
/// reporting a lost update as committed.</para>
///
/// <para><see cref="ApplyTimeValidation"/> extends that gate to the bundle's read-set. When set, the transition
/// additionally requires — at apply, in log order, against the partition's replicated committed-head ledger —
/// that every co-bundled intent's validated base still holds and that every entry of
/// <see cref="BundledReadDependencies"/> (the read-only point dependencies routed to the anchor partition)
/// is neither held by a foreign undecided intent nor moved past by a settled commit. A stalled bundle that
/// applies after a competitor committed the same base or read is then rejected instead of durably recording a
/// lost update or a write skew. The flag travels with the command so every current applier runs the same
/// checks whatever its local configuration; an applier that predates the field skips them, which is why the
/// producer sets it only once every node in the group applies it.</para>
/// </summary>
internal sealed record CommitTransactionCommand(
    HLCTimestamp TransactionId,
    long Epoch,
    long ManifestHash,
    HLCTimestamp OpId,
    HLCTimestamp AttemptHlc,
    IReadOnlyList<string>? BundledPrepareKeys = null,
    bool ApplyTimeValidation = false,
    IReadOnlyList<BundledReadDependency>? BundledReadDependencies = null) : TransactionRecordCommand;

/// <summary>
/// One read-only point dependency carried by a one-phase bundled commit: the key and the committed state the
/// transaction observed when it read it (<paramref name="ObservedExists"/> false means the key was absent, and
/// <paramref name="ObservedRevision"/> is then not compared). Judged at apply by the bundled commit gate.
/// </summary>
internal readonly record struct BundledReadDependency(string Key, long ObservedRevision, bool ObservedExists);

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

/// <summary>
/// Removes a <b>terminal</b> record once its retention window has elapsed and its participants' completion
/// receipts have been released — the retention GC transition. It is rejected against an <c>Undecided</c> record
/// (an in-flight transaction's record is never dropped) and is an idempotent no-op against an already-absent one,
/// so every replica converges to the same removed state in Raft log order.
/// </summary>
internal sealed record PurgeTransactionCommand(
    HLCTimestamp TransactionId,
    long Epoch) : TransactionRecordCommand;
