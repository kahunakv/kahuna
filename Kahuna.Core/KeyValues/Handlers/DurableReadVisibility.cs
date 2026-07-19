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
    public static ReadVisibilityAction Resolve(KeyValueContext context, PreparedIntent intent, HLCTimestamp readTimestamp)
    {
        TransactionDecision decision = intent.Resolution == PreparedIntentResolution.Pending
            ? context.TransactionRecordStore?.Get(intent.TransactionId, intent.Epoch)?.Decision ?? TransactionDecision.Undecided
            : TransactionDecision.Undecided;

        return PreparedIntentVisibility.Resolve(intent, readTimestamp, decision);
    }

    /// <summary>
    /// Whether a foreign prepared intent is a live, still-undecided concurrent writer — the durable analog of a
    /// live in-memory write intent for the commit-time conflict probe. A pending intent whose canonical decision is
    /// already commit or abort is not "in flight": commit-staleness is caught by revision-based read validation and
    /// an abort is no conflict, so neither is flagged here.
    /// </summary>
    public static bool IsUndecidedWriter(KeyValueContext context, PreparedIntent intent)
    {
        if (intent.Resolution != PreparedIntentResolution.Pending)
            return false;

        TransactionDecision decision =
            context.TransactionRecordStore?.Get(intent.TransactionId, intent.Epoch)?.Decision ?? TransactionDecision.Undecided;

        return decision == TransactionDecision.Undecided;
    }
}
