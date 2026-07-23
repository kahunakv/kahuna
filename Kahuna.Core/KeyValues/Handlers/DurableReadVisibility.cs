using Kommander.Time;

using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;

namespace Kahuna.Server.KeyValues.Handlers;

/// <summary>
/// Resolves the read-visibility action for a foreign prepared intent, consulting the canonical transaction record
/// when the intent is still pending. Under deferred settlement the finalizer returns as soon as the decision is
/// durable and settles the intent (which flips its own resolution) off the critical path, so between decision and
/// settlement the intent's <see cref="PreparedIntent.Resolution"/> is still <see cref="PreparedIntentResolution.Pending"/>
/// while the transaction is actually committed or aborted. The authority in that window is the transaction record
/// on the anchor partition: this helper reads it from the local <see cref="TransactionRecordStore"/> (a co-located
/// anchor) and passes its decision to <see cref="PreparedIntentVisibility"/>. When the record is not locally
/// available (a remote anchor is not fetched here), the decision stays undecided and the read retries.
/// </summary>
internal static class DurableReadVisibility
{
    public static ReadVisibilityAction Resolve(KeyValueContext context, PreparedIntent intent, HLCTimestamp readTimestamp,
        ForeignDecisionHint hint = default)
    {
        TransactionDecision decision = intent.Resolution == PreparedIntentResolution.Pending
            ? DecisionFor(context, intent, hint)
            : TransactionDecision.Undecided;

        return PreparedIntentVisibility.Resolve(intent, readTimestamp, decision);
    }

    /// <summary>The canonical decision for a pending intent: a routed hint for this exact intent wins (it was
    /// resolved against the remote anchor leader off the mailbox); otherwise the co-located record store; else
    /// Undecided (keeps the read at Retry, which triggers the off-mailbox routed lookup on a remote anchor).</summary>
    private static TransactionDecision DecisionFor(KeyValueContext context, PreparedIntent intent, ForeignDecisionHint hint)
    {
        if (hint.Applies(intent.TransactionId, intent.Epoch))
            return hint.Decision;

        return context.TransactionRecordStore?.Get(intent.TransactionId, intent.Epoch)?.Decision ?? TransactionDecision.Undecided;
    }

    /// <summary>The canonical decision for a still-pending intent met by a scan overlay: a decision routed off the
    /// mailbox for this exact intent identity wins (it was resolved against the intent's anchor-partition leader);
    /// otherwise the co-located record store; else Undecided (leaves the page at retry, which triggers the scan's
    /// off-mailbox routed resolution). The multi-intent analog of <see cref="DecisionFor"/> for the scan-merge path,
    /// where one page can straddle many foreign intents with different remote anchors.</summary>
    internal static TransactionDecision ScanDecision(
        KeyValueContext context,
        IReadOnlyDictionary<(HLCTimestamp TransactionId, long Epoch), TransactionDecision>? routedDecisions,
        PreparedIntent intent)
    {
        if (routedDecisions is not null
            && routedDecisions.TryGetValue((intent.TransactionId, intent.Epoch), out TransactionDecision routed)
            && routed is TransactionDecision.Commit or TransactionDecision.Abort)
            return routed;

        return context.TransactionRecordStore?.Get(intent.TransactionId, intent.Epoch)?.Decision ?? TransactionDecision.Undecided;
    }

    /// <summary>
    /// Whether a foreign prepared intent is a live, still-undecided concurrent writer — the durable analog of a
    /// live in-memory write intent for the commit-time conflict probe. A pending intent whose canonical decision is
    /// already commit or abort is not "in flight": commit-staleness is caught by revision-based read validation and
    /// an abort is no conflict, so neither is flagged here.
    /// </summary>
    public static bool IsUndecidedWriter(KeyValueContext context, PreparedIntent intent, ForeignDecisionHint hint = default)
    {
        if (intent.Resolution != PreparedIntentResolution.Pending)
            return false;

        return DecisionFor(context, intent, hint) == TransactionDecision.Undecided;
    }
}
