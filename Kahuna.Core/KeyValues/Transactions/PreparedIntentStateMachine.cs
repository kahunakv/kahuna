using Kahuna.Server.KeyValues.Transactions.Data;

namespace Kahuna.Server.KeyValues.Transactions;

/// <summary>The result of applying one prepared-intent transition against the current intent at a key.</summary>
/// <param name="Outcome">Applied (installed/updated), an idempotent no-op, or rejected (invariant preserved).</param>
/// <param name="Intent">The intent as it stands afterward. On an <see cref="TransactionApplyOutcome.Applied"/>
/// removal this is <see langword="null"/>, which the store reads as "delete this key's intent". On a no-op or
/// rejection it is the unchanged current intent (or null when absent).</param>
/// <param name="RejectReason">Set only when rejected.</param>
internal readonly record struct PreparedIntentApplyResult(
    TransactionApplyOutcome Outcome,
    PreparedIntent? Intent,
    string? RejectReason);

/// <summary>
/// The pure, deterministic state machine for a single key's durable prepared intent. Enforces exactly one live
/// intent per key, idempotent prepare/resolve replay, and terminal-resolution immutability, with no wall clock or
/// shared state — so every replica converges. Callers pass the current intent at the key (or null); the store
/// layers the per-partition map, routing, and Raft ordering on top.
///
/// <para>The single-live-intent-per-key rule is enforced here: a prepare for a key already held by a different
/// transaction is rejected (never overwritten), whether that holder is pending or merely resolved-not-yet-removed
/// — the caller retries after the holder is cleaned up.</para>
/// </summary>
internal static class PreparedIntentStateMachine
{
    public static PreparedIntentApplyResult Apply(PreparedIntent? existing, PreparedIntentCommand command) =>
        command switch
        {
            PrepareIntentCommand prepare => ApplyPrepare(existing, prepare),
            ResolveIntentCommand resolve => ApplyResolve(existing, resolve),
            RemoveIntentCommand remove => ApplyRemove(existing, remove),
            _ => Rejected(existing, "unknown command")
        };

    private static PreparedIntentApplyResult ApplyPrepare(PreparedIntent? existing, PrepareIntentCommand prepare)
    {
        PreparedIntent intent = prepare.Intent;

        if (existing is null)
            return Applied(intent);

        // Same transaction attempt re-preparing this key: an exact-duplicate mutation is a no-op; a divergent
        // mutation for the same identity is corruption and must be rejected, never silently overwritten.
        if (existing.TransactionId == intent.TransactionId && existing.Epoch == intent.Epoch)
        {
            return PreparedIntentDigest.Matches(existing, intent)
                ? IdempotentNoop(existing)
                : Rejected(existing, "prepared-intent mutation digest mismatch for the same (TransactionId, Epoch, Key)");
        }

        // A different transaction already holds this key. It is never overwritten; the caller retries.
        return Rejected(existing, "key already has a prepared intent from another transaction");
    }

    private static PreparedIntentApplyResult ApplyResolve(PreparedIntent? existing, ResolveIntentCommand resolve)
    {
        // Resolving an absent intent is a no-op: it was already resolved and garbage-collected, and the decision
        // is replaying. The canonical record remains the authority.
        if (existing is null)
            return IdempotentNoop(null);

        if (existing.TransactionId != resolve.TransactionId || existing.Epoch != resolve.Epoch)
            return Rejected(existing, "resolve targets a different transaction than the live intent at this key");

        PreparedIntentResolution target = resolve.Commit ? PreparedIntentResolution.Committed : PreparedIntentResolution.Aborted;

        switch (existing.Resolution)
        {
            case PreparedIntentResolution.Pending:
                return Applied(existing with { Resolution = target });

            case PreparedIntentResolution.Committed:
                return resolve.Commit ? IdempotentNoop(existing) : Rejected(existing, "abort after committed resolution");

            case PreparedIntentResolution.Aborted:
            default:
                return resolve.Commit ? Rejected(existing, "commit after aborted resolution") : IdempotentNoop(existing);
        }
    }

    private static PreparedIntentApplyResult ApplyRemove(PreparedIntent? existing, RemoveIntentCommand remove)
    {
        if (existing is null)
            return IdempotentNoop(null);

        if (existing.TransactionId != remove.TransactionId || existing.Epoch != remove.Epoch)
            return Rejected(existing, "remove targets a different transaction than the live intent at this key");

        if (existing.IsPending)
            return Rejected(existing, "cannot remove a pending intent before it is resolved");

        // Applied with a null intent means "delete this key's intent from the store".
        return Applied(null);
    }

    private static PreparedIntentApplyResult Applied(PreparedIntent? intent) =>
        new(TransactionApplyOutcome.Applied, intent, null);

    private static PreparedIntentApplyResult IdempotentNoop(PreparedIntent? intent) =>
        new(TransactionApplyOutcome.IdempotentNoop, intent, null);

    private static PreparedIntentApplyResult Rejected(PreparedIntent? intent, string reason) =>
        new(TransactionApplyOutcome.Rejected, intent, reason);
}
