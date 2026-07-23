
using Kahuna.Server.Configuration;
using Nixie;

using Kommander;
using Kommander.Data;
using Kommander.Time;

using Kahuna.Shared.KeyValue;
using Kahuna.Server.KeyValues.Logging;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Utils;

namespace Kahuna.Server.KeyValues.Handlers;

/// <summary>
/// Handles the execution of mutation commit requests in the key-value store.
/// </summary>
/// <remarks>
/// This handler is responsible for committing prepared mutations to ensure
/// consistency and durability within the key-value store. It interacts with
/// the persistence backend, the Raft consensus module, and other components
/// required for distributed state management.
/// </remarks>
/// <seealso cref="BaseHandler"/>
internal sealed class TryCommitMutationsHandler : BaseHandler
{
    public TryCommitMutationsHandler(KeyValueContext context) : base(context)
    {

    }

    public async Task<KeyValueResponse> Execute(KeyValueRequest message)
    {
        (KeyValueResponse? terminal, KeyValueEntry? entryOrNull, KeyValueProposal? proposalOrNull,
            HLCTimestamp currentTime, string? recordAnchorKey)
            = await ValidateAndBuildCommit(message);

        if (terminal is not null)
            return terminal;

        // Non-terminal: the entry, its live write intent for this transaction, and the built proposal are
        // all present.
        KeyValueEntry entry = entryOrNull!;
        KeyValueProposal proposal = proposalOrNull!;

        if (message.Durability != KeyValueDurability.Persistent)
        {
            ApplyCommittedHead(entry, proposal, message.TransactionId);
            entry.FlushedRevision = entry.Revision;

            RemoveMvccEntry(entry, message.TransactionId);
            TrimExpiredMvccEntries(entry, currentTime);
            entry.WriteIntent = null;

            context.RecordCommitted(message.TransactionId);
            return new(KeyValueResponseType.Committed, 0);
        }

        // A crash-atomic (persistent) mutation commits through the durable-intent canonical-record path, and
        // the manual persistent 2PC commit is rejected at the manager boundary, so a persistent key must not
        // reach here. Fail loudly rather than silently mis-apply if the invariant is ever violated.
        context.Logger.LogError("Persistent commit reached the manual mutation handler for {TransactionId} {Key}", message.TransactionId, message.Key);

        return KeyValueStaticResponses.ErroredResponse;
    }

    /// <summary>
    /// The commit prologue for <see cref="Execute"/>: receipt-first idempotency (a completion receipt or
    /// recorded decision proves it already applied here), entry + write-intent + MVCC validation, capture of
    /// the anchor, and the built proposal. A non-null <c>Terminal</c> means the commit is already resolved or
    /// cannot proceed (Committed on proof it applied here, MustRetry when this node never prepared it, Errored
    /// on malformed/conflicting state); otherwise <c>Terminal</c> is null and the entry/proposal/anchor are
    /// handed back for <see cref="Execute"/> to apply the committed head.
    /// </summary>
    private async Task<(KeyValueResponse? Terminal, KeyValueEntry? Entry, KeyValueProposal? Proposal,
        HLCTimestamp CurrentTime, string? RecordAnchorKey)>
        ValidateAndBuildCommit(KeyValueRequest message)
    {
        if (message.TransactionId == HLCTimestamp.Zero)
        {
            context.Logger.LogWarning("Cannot commit mutations for missing transaction id");

            return (KeyValueStaticResponses.ErroredResponse, null, null, HLCTimestamp.Zero, null);
        }

        // A completion receipt is authoritative proof this persistent commit already applied here —
        // recorded when the committed record was applied on the leader, replicated to a follower, or
        // replayed on restore. It holds even across a leadership change that erased the prepare state,
        // and regardless of whether the committed value is currently resident or already flushed to
        // disk, so a re-delivered commit resolves Committed up front rather than racing entry loading.
        if (context.CompletionReceiptStore.Contains(message.TransactionId, message.Key, message.Durability))
            return (new(KeyValueResponseType.Committed, 0), null, null, HLCTimestamp.Zero, null);

        HLCTimestamp currentTime = context.Raft.HybridLogicalClock.TrySendOrLocalEvent(context.Raft.GetLocalNodeId());

        KeyValueEntry? entry = await GetKeyValueEntry(message.Key, message.Durability);

        if (entry is null)
        {
            context.Logger.LogWarning("Key/Value context is missing for {TransactionId}", message.TransactionId);

            return (KeyValueStaticResponses.ErroredResponse, null, null, HLCTimestamp.Zero, null);
        }

        if (entry.WriteIntent is null)
        {
            bool mvccGone = entry.MvccEntries is null || !entry.MvccEntries.ContainsKey(message.TransactionId);
            if (mvccGone)
            {
                // Distinguish ack-loss re-commit (this actor made the terminal decision) from a
                // request arriving at a node that never prepared the transaction. Prepare state
                // (WriteIntent + MVCC) lives only in the preparing leader's actor memory and is not
                // Raft-replicated, so after a leader change the new leader has no write intent and
                // no MVCC entry — indistinguishable from the ack-loss case without an explicit record.
                if (context.WasCommittedHere(message.TransactionId))
                    return (new(KeyValueResponseType.Committed, 0), null, null, HLCTimestamp.Zero, null);

                // A durable completion receipt (the top-of-handler check) has already resolved the
                // ack-loss-after-leader-change case to Committed; reaching here means no receipt exists.
                // Never prepared here: return MustRetry so the coordinator can re-route to the
                // node (original leader, if it recovered) that still holds the write intent.
                return (KeyValueStaticResponses.MustRetryResponse, null, null, HLCTimestamp.Zero, null);
            }

            context.Logger.LogWarning("Write intent is missing for {TransactionId}", message.TransactionId);

            return (KeyValueStaticResponses.ErroredResponse, null, null, HLCTimestamp.Zero, null);
        }

        if (entry.WriteIntent.TransactionId != message.TransactionId)
        {
            context.Logger.LogWarning("Write intent conflict between {CurrentTransactionId} and {TransactionId}", entry.WriteIntent.TransactionId, message.TransactionId);

            return (KeyValueStaticResponses.ErroredResponse, null, null, HLCTimestamp.Zero, null);
        }

        // Capture the record anchor before the write intent is cleared below; it rides the completion
        // receipt recorded on a confirmed persistent commit.
        string? recordAnchorKey = entry.WriteIntent.RecordAnchorKey;

        if (entry.MvccEntries is null)
        {
            context.Logger.LogWarning("Couldn't find MVCC entry for transaction {TransactionId} [1]", message.TransactionId);

            return (KeyValueStaticResponses.ErroredResponse, null, null, HLCTimestamp.Zero, null);
        }

        if (!entry.MvccEntries.TryGetValue(message.TransactionId, out KeyValueMvccEntry? mvccEntry))
        {
            context.Logger.LogWarning("Couldn't find MVCC entry for transaction {TransactionId} [2]", message.TransactionId);

            return (KeyValueStaticResponses.ErroredResponse, null, null, HLCTimestamp.Zero, null);
        }

        KeyValueProposal proposal = new(
            message.Type,
            message.Key,
            mvccEntry.Value,
            mvccEntry.Revision,
            mvccEntry.NoRevision,
            mvccEntry.Expires,
            mvccEntry.LastUsed,
            mvccEntry.LastModified,
            mvccEntry.State,
            message.Durability
        );

        return (null, entry, proposal, currentTime, recordAnchorKey);
    }
}
