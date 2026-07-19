using Kahuna.Server.KeyValues.Transactions.Data;
using Kommander.Time;

namespace Kahuna.Server.KeyValues.Transactions;

/// <summary>What an <see cref="TransactionRecordStateMachine.Apply"/> transition did to the record.</summary>
internal enum TransactionApplyOutcome
{
    /// <summary>The transition changed the record (created it, or moved Undecided to a terminal decision).</summary>
    Applied,

    /// <summary>The transition was a valid exact replay of what the record already reflects; no change.</summary>
    IdempotentNoop,

    /// <summary>The transition is not permitted (would overwrite a terminal decision, violate identity, create a
    /// commit from absence, or arrive past the decision deadline). No change; the invariant is preserved.</summary>
    Rejected
}

/// <summary>The result of applying one transition: the outcome plus the record as it stands afterward.</summary>
/// <param name="Outcome">Whether the transition applied, was an idempotent no-op, or was rejected.</param>
/// <param name="Record">The record after the transition (unchanged on <see cref="TransactionApplyOutcome.IdempotentNoop"/>
/// / <see cref="TransactionApplyOutcome.Rejected"/>), or <see langword="null"/> only when a transition was
/// rejected against an absent record.</param>
/// <param name="RejectReason">A short reason, set only when rejected, for diagnostics/invariant assertions.</param>
internal readonly record struct TransactionRecordApplyResult(
    TransactionApplyOutcome Outcome,
    TransactionRecord? Record,
    string? RejectReason);

/// <summary>
/// The pure, deterministic compare-and-set state machine for the canonical <see cref="TransactionRecord"/>. It is
/// the linearization point of a transaction's outcome: leader, follower apply, WAL replay, and state-transfer
/// replay all call <see cref="Apply"/> with the same prior record and encoded transition and converge to the same
/// terminal record — with no wall-clock, hashing, or shared mutable state. The store layers durability, routing,
/// and Raft ordering on top; this type owns only the transition legality.
///
/// <para>Transition table:</para>
/// <code>
/// absent            --Initialize--------------> Undecided
/// Undecided         --Commit(opId,hash)-------> Commit
/// absent | Undecided--Abort(opId,hash,class)--> Abort            (Abort may create a tombstone from absence)
/// Commit            --Commit(same)------------> Commit           (idempotent)
/// Abort             --Abort(same)-------------> Abort            (idempotent, keeps original class + winner)
/// absent            --Commit-------------------> rejected        (commit needs an Undecided init as proof)
/// Commit            --Abort--------------------> rejected
/// Abort             --Commit-------------------> rejected
/// Undecided         --Commit(AttemptHlc>deadline)-> rejected     (a late commit yields to presumed-abort recovery)
/// any               --* with mismatched (TxnId,Epoch,ManifestHash)-> rejected (invariant violation)
/// </code>
/// </summary>
internal static class TransactionRecordStateMachine
{
    public static TransactionRecordApplyResult Apply(TransactionRecord? existing, TransactionRecordCommand command) =>
        command switch
        {
            InitializeTransactionCommand init => ApplyInitialize(existing, init),
            CommitTransactionCommand commit => ApplyCommit(existing, commit),
            AbortTransactionCommand abort => ApplyAbort(existing, abort),
            _ => Rejected(existing, "unknown command")
        };

    private static TransactionRecordApplyResult ApplyInitialize(TransactionRecord? existing, InitializeTransactionCommand init)
    {
        if (existing is null)
        {
            TransactionRecord created = new(
                init.TransactionId,
                init.Epoch,
                init.CoordinatorKey,
                init.RecordAnchorKey,
                init.CommitTimestamp,
                init.DecisionDeadline,
                init.ManifestHash,
                init.Participants,
                ManifestPresent: true,
                TransactionDecision.Undecided,
                TransactionAbortClass.None,
                WinningOpId: HLCTimestamp.Zero,
                init.CreatedAt,
                DecidedAt: HLCTimestamp.Zero);

            return Applied(created);
        }

        // Same-record identity is (TransactionId, Epoch, ManifestHash). A different manifest hash for the same
        // (TransactionId, Epoch) is a frozen-identity violation and must never replace the existing record — even
        // if the existing record is a manifestless abort tombstone (it still carries the manifest hash).
        if (!IdentityMatches(existing, init.TransactionId, init.Epoch, init.ManifestHash, out string? reason))
            return Rejected(existing, reason);

        // An exact re-initialization is a no-op regardless of the current decision: a terminal record (including an
        // absence-created abort tombstone) is never resurrected to Undecided; an already-Undecided record is
        // unchanged. Initialization only ever creates the record; it never advances or rewinds the decision.
        return IdempotentNoop(existing);
    }

    private static TransactionRecordApplyResult ApplyCommit(TransactionRecord? existing, CommitTransactionCommand commit)
    {
        if (existing is null)
            return Rejected(null, "commit cannot create a record from absence");

        if (!IdentityMatches(existing, commit.TransactionId, commit.Epoch, commit.ManifestHash, out string? reason))
            return Rejected(existing, reason);

        switch (existing.Decision)
        {
            case TransactionDecision.Commit:
                return IdempotentNoop(existing);

            case TransactionDecision.Abort:
                return Rejected(existing, "commit after abort");

            case TransactionDecision.Undecided:
            default:
                // A commit whose attempt is not authorized by the frozen deadline loses to presumed-abort recovery.
                // Compared by HLC (encoded on both sides), never a local clock.
                if (commit.AttemptHlc > existing.DecisionDeadline)
                    return Rejected(existing, "commit past decision deadline");

                TransactionRecord committed = existing with
                {
                    Decision = TransactionDecision.Commit,
                    AbortClass = TransactionAbortClass.None,
                    WinningOpId = commit.OpId,
                    DecidedAt = commit.AttemptHlc
                };

                return Applied(committed);
        }
    }

    private static TransactionRecordApplyResult ApplyAbort(TransactionRecord? existing, AbortTransactionCommand abort)
    {
        if (existing is null)
        {
            // Tombstone from absence: enough identity to reject every late commit and let each orphan intent
            // resolve itself, but no full participant manifest.
            TransactionRecord tombstone = new(
                abort.TransactionId,
                abort.Epoch,
                CoordinatorKey: string.Empty,
                abort.RecordAnchorKey,
                abort.CommitTimestamp,
                abort.DecisionDeadline,
                abort.ManifestHash,
                Participants: [],
                ManifestPresent: false,
                TransactionDecision.Abort,
                abort.AbortClass,
                WinningOpId: abort.OpId,
                abort.CreatedAt,
                DecidedAt: abort.AttemptHlc);

            return Applied(tombstone);
        }

        if (!IdentityMatches(existing, abort.TransactionId, abort.Epoch, abort.ManifestHash, out string? reason))
            return Rejected(existing, reason);

        switch (existing.Decision)
        {
            case TransactionDecision.Abort:
                // Idempotent: a conflicting class or op id can never rewrite the winner that already aborted.
                return IdempotentNoop(existing);

            case TransactionDecision.Commit:
                return Rejected(existing, "abort after commit");

            case TransactionDecision.Undecided:
            default:
                TransactionRecord aborted = existing with
                {
                    Decision = TransactionDecision.Abort,
                    AbortClass = abort.AbortClass,
                    WinningOpId = abort.OpId,
                    DecidedAt = abort.AttemptHlc
                };

                return Applied(aborted);
        }
    }

    private static bool IdentityMatches(TransactionRecord existing, HLCTimestamp txId, long epoch, long manifestHash, out string? reason)
    {
        if (existing.TransactionId != txId || existing.Epoch != epoch)
        {
            reason = "transition targets a different (TransactionId, Epoch) than the record";
            return false;
        }

        if (existing.ManifestHash != manifestHash)
        {
            reason = "manifest hash mismatch for the same (TransactionId, Epoch)";
            return false;
        }

        reason = null;
        return true;
    }

    private static TransactionRecordApplyResult Applied(TransactionRecord record) =>
        new(TransactionApplyOutcome.Applied, record, null);

    private static TransactionRecordApplyResult IdempotentNoop(TransactionRecord record) =>
        new(TransactionApplyOutcome.IdempotentNoop, record, null);

    private static TransactionRecordApplyResult Rejected(TransactionRecord? record, string? reason) =>
        new(TransactionApplyOutcome.Rejected, record, reason);
}
