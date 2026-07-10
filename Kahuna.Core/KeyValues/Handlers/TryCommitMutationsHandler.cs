
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

        int previousValueLength = 0;

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

            previousValueLength = entry.Value?.Length ?? 0;

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

            if (entry.MvccEntries.Remove(message.TransactionId, out KeyValueMvccEntry? removedMvccE))
                context.AdjustEstimatedEntryBytes(entry, -KeyValueStoreAccounting.MvccEntryRemovedBytes(entry.MvccEntries.Count == 0, removedMvccE.Value));
            TrimExpiredMvccEntries(entry, currentTime);
            entry.WriteIntent = null;

            context.RecordCommitted(message.TransactionId);
            return new(KeyValueResponseType.Committed, 0);
        }

        (bool success, int partitionId, long commitIndex, RaftOperationStatus raftStatus) = await CommitKeyValueMessage(message.Key, message.ProposalTicketId);

        // Transient Raft failure (e.g. NodeIsNotLeader, ProposalQueueFull): preserve MVCC + write intent
        // so the coordinator can retry the commit against the newly elected leader.
        if (!success && IsTransientRaftStatus(raftStatus))
            return KeyValueStaticResponses.MustRetryResponse;

        // Post-await: write intent may have been cleared by an idempotent re-commit that completed
        // while this handler was suspended at the Raft call. Return Committed for idempotency;
        // return Errored if success is false (unexpected — Raft returned success=false but no transient).
        if (entry.WriteIntent is null)
        {
            return success
                ? new(KeyValueResponseType.Committed, commitIndex)
                : KeyValueStaticResponses.ErroredResponse;
        }

        // Permanent Raft failure: MVCC + write intent are preserved so the coordinator can issue a
        // rollback to clean up. Propagate the error to abort the transaction.
        if (!success)
            return KeyValueStaticResponses.ErroredResponse;

        // Confirmed terminal commit: clear MVCC + write intent, then advance the entry.
        if (entry.MvccEntries.Remove(message.TransactionId, out KeyValueMvccEntry? removedMvccP))
            context.AdjustEstimatedEntryBytes(entry, -KeyValueStoreAccounting.MvccEntryRemovedBytes(entry.MvccEntries.Count == 0, removedMvccP.Value));
        TrimExpiredMvccEntries(entry, currentTime);
        entry.WriteIntent = null;

        if (entry.Revisions is not null)
            RemoveExpiredRevisions(entry, proposal.Revision);

        if (!proposal.NoRevision)
        {
            bool revisionsCreatedPersistent = entry.Revisions is null || entry.Revisions.Count == 0;
            entry.Revisions ??= new();
            // Idempotent archive: a revision number can recur across a delete→re-set cycle for the same
            // key. Dictionary.Add throws on a duplicate key, and that exception used to abort the index
            // entry's commit while the row commit had already succeeded — leaving the row persisted but
            // its unique-index entry stuck in the Deleted state (orphaned row → broken PK lookups and
            // duplicate inserts). Overwriting is safe: the same revision carries the same value.
            entry.Revisions[entry.Revision] = new KeyValueRevisionEntry(entry.Value, entry.LastModified, entry.Expires, entry.State);
            context.AdjustEstimatedEntryBytes(entry, KeyValueStoreAccounting.EstimateRevisionAddedBytes(revisionsCreatedPersistent, entry.Value));
        }

        previousValueLength = entry.Value?.Length ?? 0;

        entry.Value = proposal.Value;
        entry.Expires = proposal.Expires;
        entry.Revision = proposal.Revision;
        context.TouchEntry(entry, proposal.LastUsed);
        entry.LastModified = proposal.LastModified;
        entry.State = proposal.State;

        context.AdjustEntryValueBytes(entry, previousValueLength, entry.Value?.Length ?? 0);
        context.EnqueueExpiry(proposal.Key, proposal.Expires);
        if (proposal.State is KeyValueState.Deleted or KeyValueState.Undefined)
            context.EnqueueTombstone(proposal.Key);

        context.BackgroundWriter.Send(new(
            BackgroundWriteType.QueueStoreKeyValue,
            partitionId,
            proposal.Key,
            proposal.Value,
            proposal.Revision,
            proposal.Expires,
            proposal.LastUsed,
            proposal.LastModified,
            (int)proposal.State,
            proposal.NoRevision
        ));

        context.RecordCommitted(message.TransactionId);
        return new(KeyValueResponseType.Committed, commitIndex);
    }

    /// <summary>
    /// Returns true when the Raft status represents a transient condition that warrants preserving
    /// participant state and returning MustRetry so the coordinator can drive the same decision
    /// against the newly elected leader.
    /// </summary>
    private static bool IsTransientRaftStatus(RaftOperationStatus status) => status is
        RaftOperationStatus.NodeIsNotLeader or
        RaftOperationStatus.ProposalQueueFull or
        RaftOperationStatus.RestoreInProgress or
        RaftOperationStatus.ProposalTimeout or
        RaftOperationStatus.ReplicationFailed or
        RaftOperationStatus.OperationCancelled;

    /// <summary>
    /// Commits a previously proposed key value message
    /// </summary>
    private async Task<(bool, int, long, RaftOperationStatus)> CommitKeyValueMessage(string key, HLCTimestamp proposalTicketId)
    {
        if (!context.Raft.Joined)
            return (true, -1, 0, RaftOperationStatus.Success);

        int partitionId = ResolvePartition(key);

        (bool success, RaftOperationStatus status, long commitLogId) = await context.Raft.CommitLogs(
            partitionId,
            proposalTicketId
        );

        if (!success)
        {
            context.Logger.LogWarning("Failed to commit key/value {Key} Partition={Partition} Status={Status}", key, partitionId, status);

            return (false, -1, 0, status);
        }

        context.Logger.LogSuccessfullyCommittedKeyValue(key, partitionId, commitLogId);

        return (success, partitionId, commitLogId, status);
    }
}
