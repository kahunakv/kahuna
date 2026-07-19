using Kahuna.Server.KeyValues.Transactions.Data;
using Kommander.Time;

namespace Kahuna.Server.KeyValues.Transactions;

/// <summary>What a read should do about a durable prepared intent that covers the key it is reading.</summary>
internal enum ReadVisibilityAction
{
    /// <summary>Serve the existing committed value (no intent, an aborted intent, or a snapshot below the intent's
    /// commit timestamp): the pending mutation is not visible to this read.</summary>
    UseExisting,

    /// <summary>Serve the intent's prepared value: the transaction is committed and this read must observe it even
    /// if the value has not yet materialized into local MVCC.</summary>
    UseIntentValue,

    /// <summary>The decision is not yet known to this read (undecided intent, at or after its commit timestamp):
    /// retry rather than risk returning a stale value that a concurrent commit is about to supersede.</summary>
    Retry
}

/// <summary>
/// The pure read-visibility rule for the durable-intent 2PC model: given the prepared intent covering a key (if
/// any) and the read's snapshot timestamp, decides whether the read sees the existing committed value, the
/// intent's prepared value, or must retry. It never returns a stale value across an undecided intent, and a
/// snapshot strictly below the intent's commit timestamp is always served the prior value with no decision needed.
/// </summary>
internal static class PreparedIntentVisibility
{
    /// <param name="intent">The prepared intent covering the key, or null when the key has none.</param>
    /// <param name="readTimestamp">The read's snapshot timestamp; <see cref="HLCTimestamp.Zero"/> means a latest
    /// (most-recent-committed) read, which conceptually sits at or after every commit timestamp.</param>
    public static ReadVisibilityAction Resolve(PreparedIntent? intent, HLCTimestamp readTimestamp)
    {
        if (intent is null)
            return ReadVisibilityAction.UseExisting;

        // A snapshot strictly before the intent's commit timestamp cannot see it regardless of the outcome — the
        // prior committed revision is visible and no decision lookup is required.
        bool latest = readTimestamp == HLCTimestamp.Zero;
        if (!latest && readTimestamp < intent.CommitTimestamp)
            return ReadVisibilityAction.UseExisting;

        // Latest read, or a snapshot at/after the commit timestamp: the outcome decides visibility.
        return intent.Resolution switch
        {
            PreparedIntentResolution.Committed => ReadVisibilityAction.UseIntentValue,
            PreparedIntentResolution.Aborted => ReadVisibilityAction.UseExisting,
            _ => ReadVisibilityAction.Retry
        };
    }
}
