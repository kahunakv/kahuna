
using Kahuna.Server.Configuration;
using Nixie;

using Kommander;
using Kommander.Data;
using Kommander.Time;

using Kahuna.Shared.KeyValue;
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

        if (message.Durability != KeyValueDurability.Persistent)
        {
            if (entry.Revisions is not null)
                RemoveExpiredRevisions(entry, proposal.Revision);

            entry.Revisions ??= new();
            entry.Revisions.Add(entry.Revision, entry.Value);

            entry.Value = proposal.Value;
            entry.Expires = proposal.Expires;
            entry.Revision = proposal.Revision;
            entry.LastUsed = proposal.LastUsed;
            entry.LastModified = proposal.LastModified;
            entry.State = proposal.State;
            
            entry.MvccEntries.Remove(message.TransactionId);                    
            entry.WriteIntent = null;

            return new(KeyValueResponseType.Committed, 0);
        }

        (bool success, int partitionId, long commitIndex) = await CommitKeyValueMessage(message.Key, message.ProposalTicketId);
        
        entry.MvccEntries.Remove(message.TransactionId);                   
        entry.WriteIntent = null;
        
        if (!success)                                
            return KeyValueStaticResponses.ErroredResponse;        

        if (entry.Revisions is not null)
            RemoveExpiredRevisions(entry, proposal.Revision);
               
        entry.Revisions ??= new();
        entry.Revisions.Add(entry.Revision, entry.Value);

        entry.Value = proposal.Value;
        entry.Expires = proposal.Expires;
        entry.Revision = proposal.Revision;
        entry.LastUsed = proposal.LastUsed;
        entry.LastModified = proposal.LastModified;
        entry.State = proposal.State;
        
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

        int partitionId = context.Raft.GetPartitionKey(key);

        (bool success, RaftOperationStatus status, long commitLogId) = await context.Raft.CommitLogs(
            partitionId,
            proposalTicketId
        );

        if (!success)
        {
            context.Logger.LogWarning("Failed to commit key/value {Key} Partition={Partition} Status={Status}", key, partitionId, status);

            return (false, -1, 0);
        }               

        context.Logger.LogDebug("Successfully commmitted key/value {Key} Partition={Partition} ProposalIndex={ProposalIndex}", key, partitionId, commitLogId);

        return (success, partitionId, commitLogId);
    }
}