
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

        KeyValueEntry? entry = await GetKeyValueEntry(message.Key, message.Durability);

        if (entry is null)
        {
            context.Logger.LogWarning("Key/Value context is missing for {TransactionId}", message.TransactionId);

            return KeyValueStaticResponses.ErroredResponse;
        }

        if (entry.WriteIntent is null)
        {
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
            false,
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

            bool revisionsCreatedEphemeral = entry.Revisions is null || entry.Revisions.Count == 0;
            entry.Revisions ??= new();
            // Idempotent archive (see the persistent path below): a revision can recur across a
            // delete→re-set cycle; Dictionary.Add would throw and corrupt the commit.
            entry.Revisions[entry.Revision] = new KeyValueRevisionEntry(entry.Value, entry.LastModified, entry.Expires, entry.State);
            context.AdjustEstimatedEntryBytes(entry, KeyValueStoreAccounting.EstimateRevisionAddedBytes(revisionsCreatedEphemeral, entry.Value));

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
            entry.WriteIntent = null;

            return new(KeyValueResponseType.Committed, 0);
        }

        (bool success, int partitionId, long commitIndex) = await CommitKeyValueMessage(message.Key, message.ProposalTicketId);

        // Re-check after await: a concurrent commit for the same transaction may have
        // already committed this entry while we were suspended waiting for Raft.
        if (entry.WriteIntent is null)
        {
            context.Logger.LogWarning("Write intent already cleared for {TransactionId} — duplicate commit detected, aborting", message.TransactionId);
            return KeyValueStaticResponses.ErroredResponse;
        }

        if (entry.MvccEntries.Remove(message.TransactionId, out KeyValueMvccEntry? removedMvccP))
            context.AdjustEstimatedEntryBytes(entry, -KeyValueStoreAccounting.MvccEntryRemovedBytes(entry.MvccEntries.Count == 0, removedMvccP.Value));
        entry.WriteIntent = null;

        if (!success)
            return KeyValueStaticResponses.ErroredResponse;

        if (entry.Revisions is not null)
            RemoveExpiredRevisions(entry, proposal.Revision);

        bool revisionsCreatedPersistent = entry.Revisions is null || entry.Revisions.Count == 0;
        entry.Revisions ??= new();
        // Idempotent archive: a revision number can recur across a delete→re-set cycle for the same
        // key. Dictionary.Add throws on a duplicate key, and that exception used to abort the index
        // entry's commit while the row commit had already succeeded — leaving the row persisted but
        // its unique-index entry stuck in the Deleted state (orphaned row → broken PK lookups and
        // duplicate inserts). Overwriting is safe: the same revision carries the same value.
        entry.Revisions[entry.Revision] = new KeyValueRevisionEntry(entry.Value, entry.LastModified, entry.Expires, entry.State);
        context.AdjustEstimatedEntryBytes(entry, KeyValueStoreAccounting.EstimateRevisionAddedBytes(revisionsCreatedPersistent, entry.Value));

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
            (int)proposal.State
        ));                       

        return new(KeyValueResponseType.Committed, commitIndex);
    }

    /// <summary>
    /// Commits a previously proposed key value message
    /// </summary>
    /// <param name="key"></param>
    /// <param name="proposalTicketId"></param>
    /// <returns></returns>
    private async Task<(bool, int, long)> CommitKeyValueMessage(string key, HLCTimestamp proposalTicketId)
    {
        if (!context.Raft.Joined)
            return (true, -1, 0);

        int partitionId = ResolvePartition(key);

        (bool success, RaftOperationStatus status, long commitLogId) = await context.Raft.CommitLogs(
            partitionId,
            proposalTicketId
        );

        if (!success)
        {
            context.Logger.LogWarning("Failed to commit key/value {Key} Partition={Partition} Status={Status}", key, partitionId, status);

            return (false, -1, 0);
        }               

        context.Logger.LogSuccessfullyCommittedKeyValue(key, partitionId, commitLogId);

        return (success, partitionId, commitLogId);
    }
}