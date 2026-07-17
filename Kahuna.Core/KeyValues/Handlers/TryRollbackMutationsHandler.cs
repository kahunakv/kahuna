
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues.Logging;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Shared.KeyValue;
using Kahuna.Utils;
using Kommander;
using Kommander.Data;
using Kommander.Time;
using Nixie;

namespace Kahuna.Server.KeyValues.Handlers;

/// <summary>
/// Handles the rollback of mutations in the key-value store based on the given transaction ID and durability requirements.
/// </summary>
/// <remarks>
/// The <c>TryRollbackMutationsHandler</c> is responsible for ensuring that any mutations associated
/// with a given transaction are correctly rolled back if necessary. This handler validates the transaction ID,
/// checks the corresponding key-value context, and performs cleanup of write intents and MVCC entries as needed.
/// It also supports persistence operations for cases requiring data durability.
/// </remarks>
/// <example>
/// This class is typically invoked as part of the Two Phase Commit (2PC) workflow when a rollback request
/// is received.
/// </example>
internal sealed class TryRollbackMutationsHandler : BaseHandler
{
    public TryRollbackMutationsHandler(KeyValueContext context) : base(context)
    {

    }

    public async Task<KeyValueResponse> Execute(KeyValueRequest message)
    {
        if (message.TransactionId == HLCTimestamp.Zero)
        {
            context.Logger.LogWarning("Cannot rollback mutations for missing transaction id");

            return KeyValueStaticResponses.ErroredResponse;
        }

        HLCTimestamp currentTime = context.Raft.HybridLogicalClock.TrySendOrLocalEvent(context.Raft.GetLocalNodeId());

        KeyValueEntry? entry = await GetKeyValueEntry(message.Key, message.Durability);

        if (entry is null)
        {
            context.Logger.LogWarning("Key/Value context is missing for {TransactionId}", message.TransactionId);

            return KeyValueStaticResponses.ErroredResponse;
        }

        if (entry.ReplicationIntent is not null)
        {
            context.Logger.LogWarning("Replication intent is active on key {Key}", message.Key);

            return KeyValueStaticResponses.WaitingForReplicationResponse;
        }

        if (entry.WriteIntent is null)
        {
            bool mvccGone = entry.MvccEntries is null || !entry.MvccEntries.ContainsKey(message.TransactionId);
            if (mvccGone)
            {
                // Distinguish ack-loss re-rollback (this actor made the terminal decision) from a
                // request arriving at a node that never prepared the transaction. Prepare state
                // (WriteIntent + MVCC) lives only in the preparing leader's actor memory and is not
                // Raft-replicated, so after a leader change the new leader has neither — same surface
                // as a true re-rollback. Without an explicit record, returning RolledBack here would
                // silently succeed on a node that holds no state to roll back.
                if (context.WasRolledBackHere(message.TransactionId))
                    return new(KeyValueResponseType.RolledBack, 0);

                // Never prepared here: return MustRetry so the coordinator can re-route to the node
                // that still holds the write intent.
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

        if (entry.MvccEntries is null)
        {
            context.Logger.LogWarning("Couldn't find MVCC entry for transaction {TransactionId} [1]", message.TransactionId);

            return KeyValueStaticResponses.ErroredResponse;
        }

        if (!entry.MvccEntries.ContainsKey(message.TransactionId))
        {
            context.Logger.LogWarning("Couldn't find MVCC entry for transaction {TransactionId} [2]", message.TransactionId);

            return KeyValueStaticResponses.ErroredResponse;
        }

        if (message.Durability != KeyValueDurability.Persistent)
        {
            ApplyConfirmedRollback(entry, message.TransactionId, currentTime);
            return new(KeyValueResponseType.RolledBack);
        }

        // Not joined to a Raft cluster (embedded single-node, no replication): there is nothing to roll
        // back in Raft, so the rollback is an immediate success — apply inline.
        if (!context.Raft.Joined)
        {
            ApplyConfirmedRollback(entry, message.TransactionId, currentTime);
            return new(KeyValueResponseType.RolledBack, 0);
        }

        int partitionId = ResolvePartition(message.Key);

        // Joined but no off-mailbox worker wired (bare test contexts): roll the ticket back inline so it
        // is durably settled in Raft before the local state is cleared — never skip RollbackLogs.
        if (context.PhaseTwoRouter is null)
            return await RollbackInline(message, entry, currentTime, partitionId);

        // Otherwise dispatch the RollbackLogs Raft round trip to the off-mailbox worker so this actor is
        // free while it runs; the completion applies the confirmed rollback back on the mailbox.
        IActorContext<KeyValueActor, KeyValueRequest, KeyValueResponse> actorContext = context.ActorContext;
        if (!actorContext.Reply.HasValue)
            return KeyValueStaticResponses.ErroredResponse;

        // Park the after-Raft context on the actor, keyed by a monotonic id the completion carries back.
        // The entry stays pinned by its live write intent across the window, so it cannot be evicted.
        int phaseTwoId = context.NextPhaseTwoId();
        context.PendingPhaseTwos[phaseTwoId] = PendingPhaseTwo.ForRollback(
            message.TransactionId, message.Key, message.Durability, currentTime,
            message.ProposalTicketId, partitionId);

        context.PhaseTwoRouter.Send(KeyValuePhaseTwoRequest.ForRollback(
            phaseTwoId, partitionId, message.ProposalTicketId, actorContext.Self, actorContext.Reply.Value.Promise));

        actorContext.ByPassReply = true;

        return KeyValueStaticResponses.WaitingForReplicationResponse;
    }

    /// <summary>
    /// Rolls the prepared ticket back inline (durable), bounded by the phase-two deadline, then clears
    /// the local state — the fallback when no off-mailbox worker is wired. Transient failures return
    /// MustRetry with state retained.
    /// </summary>
    private async Task<KeyValueResponse> RollbackInline(
        KeyValueRequest message, KeyValueEntry entry, HLCTimestamp currentTime, int partitionId)
    {
        int timeoutMs = context.Configuration.Phase2CommitTimeout;
        using CancellationTokenSource? cts = timeoutMs > 0 ? new CancellationTokenSource(timeoutMs) : null;

        (bool success, RaftOperationStatus status, long logIndex) = await context.Raft.RollbackLogs(
            partitionId, message.ProposalTicketId, cts?.Token ?? CancellationToken.None);

        if (!success)
            return IsTransientRaftStatus(status)
                ? KeyValueStaticResponses.MustRetryResponse
                : KeyValueStaticResponses.ErroredResponse;

        if (entry.WriteIntent is null || entry.WriteIntent.TransactionId != message.TransactionId)
            return new(KeyValueResponseType.RolledBack, logIndex);

        ApplyConfirmedRollback(entry, message.TransactionId, currentTime);

        return new(KeyValueResponseType.RolledBack, logIndex);
    }

    private static bool IsTransientRaftStatus(RaftOperationStatus status) => status is
        RaftOperationStatus.NodeIsNotLeader or
        RaftOperationStatus.ProposalQueueFull or
        RaftOperationStatus.RestoreInProgress or
        RaftOperationStatus.ProposalTimeout or
        RaftOperationStatus.ReplicationFailed or
        RaftOperationStatus.OperationCancelled;
}
