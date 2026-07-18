
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
        if (message.TransactionId == HLCTimestamp.Zero)
        {
            context.Logger.LogWarning("Cannot commit mutations for missing transaction id");

            return KeyValueStaticResponses.ErroredResponse;
        }

        // A completion receipt is authoritative proof this persistent commit already applied here —
        // recorded when the committed record was applied on the leader, replicated to a follower, or
        // replayed on restore. It holds even across a leadership change that erased the prepare state,
        // and regardless of whether the committed value is currently resident or already flushed to
        // disk, so a re-delivered commit resolves Committed up front rather than racing entry loading.
        if (context.CompletionReceiptStore.Contains(message.TransactionId, message.Key, message.Durability))
            return new(KeyValueResponseType.Committed, 0);

        HLCTimestamp currentTime = context.Raft.HybridLogicalClock.TrySendOrLocalEvent(context.Raft.GetLocalNodeId());

        KeyValueEntry? entry = await GetKeyValueEntry(message.Key, message.Durability);

        if (entry is null)
        {
            context.Logger.LogWarning("Key/Value context is missing for {TransactionId}", message.TransactionId);

            return KeyValueStaticResponses.ErroredResponse;
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
                    return new(KeyValueResponseType.Committed, 0);

                // A durable completion receipt (the top-of-handler check) has already resolved the
                // ack-loss-after-leader-change case to Committed; reaching here means no receipt exists.
                // Never prepared here: return MustRetry so the coordinator can re-route to the
                // node (original leader, if it recovered) that still holds the write intent.
                return KeyValueStaticResponses.MustRetryResponse;
            }

            context.Logger.LogWarning("Write intent is missing for {TransactionId}", message.TransactionId);

            return KeyValueStaticResponses.ErroredResponse;
        }

        if (entry.WriteIntent.TransactionId != message.TransactionId)
        {
            context.Logger.LogWarning("Write intent conflict between {CurrentTransactionId} and {TransactionId}", entry.WriteIntent.TransactionId, message.TransactionId);

            return KeyValueStaticResponses.ErroredResponse;
        }

        // Capture the record anchor before the write intent is cleared below; it rides the completion
        // receipt recorded on a confirmed persistent commit.
        string? recordAnchorKey = entry.WriteIntent.RecordAnchorKey;

        // Capture the initial coordinator decision before the write intent is cleared. Present only on the
        // anchor key of a Durable transaction; installing it as this anchor mutation commits means the
        // anchor value, its completion receipt, and the CommitDecided record all land from one committed
        // proposal — no window where a secondary participant could observe the anchor value without a record.
        Transactions.Data.CoordinatorDecisionRecord? embeddedDecision = entry.WriteIntent.EmbeddedDecision;

        if (entry.MvccEntries is null)
        {
            context.Logger.LogWarning("Couldn't find MVCC entry for transaction {TransactionId} [1]", message.TransactionId);

            return KeyValueStaticResponses.ErroredResponse;
        }

        if (!entry.MvccEntries.TryGetValue(message.TransactionId, out KeyValueMvccEntry? mvccEntry))
        {
            context.Logger.LogWarning("Couldn't find MVCC entry for transaction {TransactionId} [2]", message.TransactionId);

            return KeyValueStaticResponses.ErroredResponse;
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

        if (message.Durability != KeyValueDurability.Persistent)
        {
            if (entry.Revisions is not null)
                RemoveExpiredRevisions(entry, proposal.Revision);

            if (!proposal.NoRevision)
            {
                bool revisionsCreatedEphemeral = entry.Revisions is null || entry.Revisions.Count == 0;
                entry.Revisions ??= new();
                // Idempotent archive (see the persistent path below): a revision can recur across a
                // delete→re-set cycle; Dictionary.Add would throw and corrupt the commit.
                entry.Revisions[entry.Revision] = new KeyValueRevisionEntry(entry.Value, entry.LastModified, entry.Expires, entry.State);
                context.AdjustEstimatedEntryBytes(entry, KeyValueStoreAccounting.EstimateRevisionAddedBytes(revisionsCreatedEphemeral, entry.Value));
            }

            int previousValueLength = entry.Value?.Length ?? 0;

            entry.Value = proposal.Value;
            entry.Expires = proposal.Expires;
            entry.Revision = proposal.Revision;
            context.TouchEntry(entry, proposal.LastUsed);
            entry.LastModified = proposal.LastModified;
            entry.State = proposal.State;

            context.AdjustEntryValueBytes(entry, previousValueLength, entry.Value?.Length ?? 0);
            context.EnqueueExpiry(message.Key, proposal.Expires);
            if (proposal.State is KeyValueState.Deleted or KeyValueState.Undefined)
                context.EnqueueTombstone(message.Key);

            RemoveMvccEntry(entry, message.TransactionId);
            TrimExpiredMvccEntries(entry, currentTime);
            entry.WriteIntent = null;

            context.RecordCommitted(message.TransactionId);
            return new(KeyValueResponseType.Committed, 0);
        }

        // Not joined to a Raft cluster (embedded single-node, no replication): there is nothing to
        // commit to, so the commit is an immediate success — apply inline.
        if (!context.Raft.Joined)
        {
            ApplyConfirmedCommit(entry, proposal, message.TransactionId, currentTime, -1, recordAnchorKey, embeddedDecision);

            return new(KeyValueResponseType.Committed, 0);
        }

        int partitionId = ResolvePartition(message.Key);

        // Joined but no off-mailbox worker wired (bare test contexts): commit the ticket inline so the
        // mutation is durably committed to Raft before it is applied — never skip CommitLogs, which would
        // silently drop durability. Mirrors the prepare fallback's inline replication.
        if (context.PhaseTwoRouter is null)
            return await CommitInline(message, entry, proposal, currentTime, partitionId, recordAnchorKey, embeddedDecision);

        // Otherwise dispatch the CommitLogs Raft round trip to the off-mailbox worker so this actor is
        // free while it runs; the completion applies the confirmed commit back on the mailbox.
        IActorContext<KeyValueActor, KeyValueRequest, KeyValueResponse> actorContext = context.ActorContext;
        if (!actorContext.Reply.HasValue)
            return KeyValueStaticResponses.ErroredResponse;

        // Park the after-Raft context on the actor, keyed by a monotonic id the completion carries back.
        // The entry stays pinned by its live write intent across the window, so it cannot be evicted.
        int phaseTwoId = context.NextPhaseTwoId();
        long deadlineTicks = KeyValuePhaseTwoRequest.DeadlineFrom(context.Configuration.Phase2CommitTimeout);

        PendingPhaseTwo pending = PendingPhaseTwo.ForCommit(
            message.TransactionId, message.Key, message.Durability, proposal, currentTime,
            message.ProposalTicketId, partitionId, recordAnchorKey, embeddedDecision);
        // Retain the promise + deadline so the collector's sweep can resolve the caller (retryable) if the
        // worker dies or its completion is dropped and never arrives.
        pending.Promise = actorContext.Reply.Value.Promise;
        pending.DeadlineTicks = deadlineTicks;
        context.PendingPhaseTwos[phaseTwoId] = pending;

        context.PhaseTwoRouter.Send(KeyValuePhaseTwoRequest.ForCommit(
            phaseTwoId, partitionId, message.ProposalTicketId,
            deadlineTicks, actorContext.Self, actorContext.Reply.Value.Promise));

        actorContext.ByPassReply = true;

        return KeyValueStaticResponses.WaitingForReplicationResponse;
    }

    /// <summary>
    /// Commits the prepared ticket inline (durable), bounded by the phase-two deadline, then applies —
    /// the fallback when no off-mailbox worker is wired. Transient failures return MustRetry with state
    /// retained; a confirmed commit applies exactly once (post-await idempotency re-checks the intent).
    /// </summary>
    private async Task<KeyValueResponse> CommitInline(
        KeyValueRequest message, KeyValueEntry entry, KeyValueProposal proposal, HLCTimestamp currentTime,
        int partitionId, string? recordAnchorKey, Transactions.Data.CoordinatorDecisionRecord? embeddedDecision)
    {
        int timeoutMs = context.Configuration.Phase2CommitTimeout;
        using CancellationTokenSource? cts = timeoutMs > 0 ? new CancellationTokenSource(timeoutMs) : null;

        (bool success, RaftOperationStatus status, long commitIndex) = await context.Raft.CommitLogs(
            partitionId, message.ProposalTicketId, cts?.Token ?? CancellationToken.None);

        if (!success)
            return IsTransientRaftStatus(status)
                ? KeyValueStaticResponses.MustRetryResponse
                : KeyValueStaticResponses.ErroredResponse;

        // Post-await: the intent may have been cleared by an idempotent re-commit while suspended.
        if (entry.WriteIntent is null || entry.WriteIntent.TransactionId != message.TransactionId)
            return new(KeyValueResponseType.Committed, commitIndex);

        ApplyConfirmedCommit(entry, proposal, message.TransactionId, currentTime, partitionId, recordAnchorKey, embeddedDecision);

        return new(KeyValueResponseType.Committed, commitIndex);
    }

    private static bool IsTransientRaftStatus(RaftOperationStatus status) => status is
        RaftOperationStatus.NodeIsNotLeader or
        RaftOperationStatus.ProposalQueueFull or
        RaftOperationStatus.RestoreInProgress or
        RaftOperationStatus.ProposalTimeout or
        RaftOperationStatus.ReplicationFailed or
        RaftOperationStatus.OperationCancelled;
}
