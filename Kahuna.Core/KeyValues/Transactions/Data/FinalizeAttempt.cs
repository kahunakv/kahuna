
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Transactions.Data;

/// <summary>
/// The result of a single finalize (commit / rollback / reap) attempt on a session. Carries the response
/// type the caller reports and, for a commit, the transaction's immutable record anchor. A terminal
/// outcome (anything other than <see cref="KeyValueResponseType.MustRetry"/>) is retained and mirrored to
/// concurrent finalizers; a <see cref="KeyValueResponseType.MustRetry"/> is non-terminal and releases the
/// finalize slot so a later call can run a fresh attempt.
/// </summary>
internal readonly record struct FinalizeOutcome(KeyValueResponseType Type, string? RecordAnchorKey)
{
    /// <summary>True when this outcome is a final answer that must be retained rather than retried.</summary>
    internal bool IsTerminal => Type != KeyValueResponseType.MustRetry;
}

/// <summary>
/// One in-flight finalize on a session. Only a single attempt may be active at a time: the caller that
/// installs it owns the finalize and must publish its outcome exactly once; every concurrent commit,
/// rollback, or reaper observes the same attempt and mirrors its published outcome instead of running a
/// second, racing finalize. The completion source runs its continuations asynchronously so publishing the
/// result never executes a waiter's continuation inline under the session registry lock.
/// </summary>
internal sealed class FinalizeAttempt
{
    private readonly TaskCompletionSource<FinalizeOutcome> completion = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>Completes with the outcome once the owning finalize publishes it.</summary>
    internal Task<FinalizeOutcome> Completion => completion.Task;

    /// <summary>Publishes the outcome to every waiter. Idempotent: a second publish is ignored.</summary>
    internal void Publish(FinalizeOutcome outcome) => completion.TrySetResult(outcome);
}
