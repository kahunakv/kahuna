
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

        // Not joined to a Raft cluster (embedded single-node, no replication): there is nothing to
        // commit to, so the commit is an immediate success — apply inline.
        if (!context.Raft.Joined)
        {
            ApplyConfirmedCommit(entry, proposal, message.TransactionId, currentTime, -1, recordAnchorKey);

            return new(KeyValueResponseType.Committed, 0);
        }

        int partitionId = ResolvePartition(message.Key);

        // Joined but no off-mailbox worker wired (bare test contexts): commit the ticket inline so the
        // mutation is durably committed to Raft before it is applied — never skip CommitLogs, which would
        // silently drop durability. Mirrors the prepare fallback's inline replication.
        if (context.PhaseTwoRouter is null)
            return await CommitInline(message, entry, proposal, currentTime, partitionId, recordAnchorKey);

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
            message.ProposalTicketId, partitionId, recordAnchorKey);
        // Retain the promise + deadline so the collector's sweep can resolve the caller (retryable) if the
        // worker dies or its completion is dropped and never arrives.
        pending.Promise = actorContext.Reply.Value.Promise!;
        pending.DeadlineTicks = deadlineTicks;
        context.PendingPhaseTwos[phaseTwoId] = pending;

        context.PhaseTwoRouter.Send(KeyValuePhaseTwoRequest.ForCommit(
            phaseTwoId, partitionId, message.ProposalTicketId,
            deadlineTicks, actorContext.Self, actorContext.Reply.Value.Promise!));

        actorContext.ByPassReply = true;

        return KeyValueStaticResponses.WaitingForReplicationResponse;
    }

    /// <summary>
    /// Applies a mutation whose partition ticket the manager has already committed via a single batched
    /// <c>CommitLogs</c> — the per-key half of partition-batched commit, with no Raft round trip of its own.
    /// Persistent participants only; ephemeral keys keep the per-key inline commit.
    ///
    /// <para>Unlike <see cref="Execute"/> this is <b>intent-first, not receipt-first</b>, and the ordering is
    /// load-bearing. The manager's batched <c>CommitLogs</c> makes the leader's replicator apply the committed
    /// log — which, under the write-intent deferral, no-ops on the resident entry but still records a
    /// completion receipt — <em>before</em> this apply runs. A receipt-first short-circuit (as <see cref="Execute"/>
    /// uses for re-delivered commits) would therefore see that receipt and skip the real archive, leaving the
    /// entry frozen at its pre-commit value with the intent never cleared. So while our own intent is still
    /// live it always archives; only once the intent is gone or held by a later owner does it resolve from
    /// durable proof.</para>
    ///
    /// <para><b>Never returns Errored (except malformed input).</b> The caller only invokes this after the
    /// group's shared <c>CommitLogs</c> already succeeded, so the mutation is durable in Raft. A failure to
    /// archive locally (entry evicted, intent replaced by a later transaction, MVCC gone) is therefore
    /// in-doubt/recoverable, not an abort: it resolves to <c>Committed</c> on durable proof (completion receipt
    /// or recorded decision) and otherwise to <c>MustRetry</c> for re-drive/recovery — the atomicity rule that
    /// nothing may report a definite failure after a shared ticket has committed.</para>
    /// </summary>
    public async Task<KeyValueResponse> ApplyExecute(KeyValueRequest message)
    {
        if (message.TransactionId == HLCTimestamp.Zero)
        {
            context.Logger.LogWarning("Cannot apply committed mutations for missing transaction id");

            return KeyValueStaticResponses.ErroredResponse;
        }

        HLCTimestamp currentTime = context.Raft.HybridLogicalClock.TrySendOrLocalEvent(context.Raft.GetLocalNodeId());

        KeyValueEntry? entry = await GetKeyValueEntry(message.Key, message.Durability);

        // Fast path: our own write intent is still live and the staged MVCC is present → archive the confirmed
        // commit into the resident entry now. Intent-first, not receipt-first (see remarks): the replicator has
        // already recorded the receipt for this durable log, so keying idempotency on it here would skip the
        // real archive and freeze the entry at its pre-commit value.
        if (entry?.WriteIntent is not null &&
            entry.WriteIntent.TransactionId == message.TransactionId &&
            entry.MvccEntries is not null &&
            entry.MvccEntries.TryGetValue(message.TransactionId, out KeyValueMvccEntry? mvccEntry))
        {
            string? recordAnchorKey = entry.WriteIntent.RecordAnchorKey;

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

            ApplyConfirmedCommit(entry, proposal, message.TransactionId, currentTime,
                ResolvePartition(message.Key), recordAnchorKey);

            return new(KeyValueResponseType.Committed, entry.Revision);
        }

        // No own live intent to archive: the commit already applied here (a re-delivered apply cleared the
        // intent), or the entry is gone/held by a later transaction that took it after our commit applied and
        // cleared. Either way the shared ticket already committed, so the mutation is durable. Report Committed
        // on durable proof; otherwise it is in-doubt and re-driven — never Errored, never aborted.
        if (context.WasCommittedHere(message.TransactionId) ||
            context.CompletionReceiptStore.Contains(message.TransactionId, message.Key, message.Durability))
            return new(KeyValueResponseType.Committed, entry?.Revision ?? 0);

        return KeyValueStaticResponses.MustRetryResponse;
    }

    /// <summary>
    /// The per-key commit prologue for <see cref="Execute"/>: receipt-first idempotency (a completion receipt
    /// or recorded decision proves it already applied here), entry + write-intent + MVCC validation, capture
    /// of the anchor and embedded decision, and the built proposal. A non-null <c>Terminal</c> means the commit
    /// is already resolved or cannot proceed (Committed on proof it applied here, MustRetry when this node
    /// never prepared it, Errored on malformed/conflicting state); otherwise <c>Terminal</c> is null and the
    /// entry/proposal/anchor/decision are handed back for <see cref="Execute"/> to commit the ticket and apply.
    /// (<see cref="ApplyExecute"/> deliberately does not use this: after the manager's batched CommitLogs the
    /// receipt is recorded before the apply runs, so it must key idempotency on the live intent, not the receipt.)
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

    /// <summary>
    /// Commits the prepared ticket inline (durable), bounded by the phase-two deadline, then applies —
    /// the fallback when no off-mailbox worker is wired. Transient failures return MustRetry with state
    /// retained; a confirmed commit applies exactly once (post-await idempotency re-checks the intent).
    /// </summary>
    private async Task<KeyValueResponse> CommitInline(
        KeyValueRequest message, KeyValueEntry entry, KeyValueProposal proposal, HLCTimestamp currentTime,
        int partitionId, string? recordAnchorKey)
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

        ApplyConfirmedCommit(entry, proposal, message.TransactionId, currentTime, partitionId, recordAnchorKey);

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
