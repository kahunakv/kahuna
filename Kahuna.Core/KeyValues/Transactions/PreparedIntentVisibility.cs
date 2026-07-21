using System;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kommander.Time;

namespace Kahuna.Server.KeyValues.Transactions;

// TransactionDecision lives in Transactions.Data; the using above brings it in.

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
    /// <param name="canonicalDecision">The intent transaction's canonical decision from its transaction record, used
    /// only while the intent's own resolution is still <see cref="PreparedIntentResolution.Pending"/> — under
    /// deferred settlement the intent stays pending until it settles, after the decision is already durable, so the
    /// record is the authority. <see cref="TransactionDecision.Undecided"/> (the default) when the record is not
    /// consulted or not locally available, which keeps a pending intent at <see cref="ReadVisibilityAction.Retry"/>.</param>
    public static ReadVisibilityAction Resolve(PreparedIntent? intent, HLCTimestamp readTimestamp,
        TransactionDecision canonicalDecision = TransactionDecision.Undecided)
    {
        if (intent is null)
            return ReadVisibilityAction.UseExisting;

        // A snapshot strictly before the intent's commit timestamp cannot see it regardless of the outcome — the
        // prior committed revision is visible and no decision lookup is required.
        bool latest = readTimestamp == HLCTimestamp.Zero;
        if (!latest && readTimestamp < intent.CommitTimestamp)
            return ReadVisibilityAction.UseExisting;

        // A terminal intent resolution is authoritative; a still-pending intent defers to the canonical transaction
        // decision (deferred settlement flips the intent's resolution only once it settles).
        bool committed = intent.Resolution == PreparedIntentResolution.Committed
            || (intent.Resolution == PreparedIntentResolution.Pending && canonicalDecision == TransactionDecision.Commit);
        bool aborted = intent.Resolution == PreparedIntentResolution.Aborted
            || (intent.Resolution == PreparedIntentResolution.Pending && canonicalDecision == TransactionDecision.Abort);

        if (committed)
            return ReadVisibilityAction.UseIntentValue;
        if (aborted)
            return ReadVisibilityAction.UseExisting;

        return ReadVisibilityAction.Retry;
    }

    /// <summary>The ordinary-read expiry predicate applied to a committed prepared intent's value: expired iff it
    /// carries an expiry that is strictly before "now" (<c>Expires != Zero &amp;&amp; (Expires - currentTime) &lt; 0</c>,
    /// matching every materialized-read exit). A committed-but-expired intent must not be served as live — the read
    /// treats it as does-not-exist (point) or excludes it (scan), exactly as an expired MVCC head entry. Applied by
    /// the read egress rather than folded into <see cref="Resolve"/> because expiry is compared against wall-clock
    /// "now", orthogonal to the commit-timestamp visibility ordering, and the by-revision/writer paths do not filter
    /// on it. A Zero <paramref name="currentTime"/> opts out (no filter).</summary>
    public static bool IsExpired(PreparedIntent intent, HLCTimestamp currentTime) =>
        currentTime != HLCTimestamp.Zero
        && intent.Expires != HLCTimestamp.Zero
        && intent.Expires - currentTime < TimeSpan.Zero;
}
