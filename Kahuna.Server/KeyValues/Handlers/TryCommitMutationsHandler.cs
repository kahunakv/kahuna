
using Nixie;

using Kommander;
using Kommander.Data;
using Kommander.Time;

using Kahuna.Shared.KeyValue;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;

namespace Kahuna.Server.KeyValues.Handlers;

internal sealed class TryCommitMutationsHandler : BaseHandler
{
    public TryCommitMutationsHandler(
        Dictionary<string, KeyValueContext> keyValuesStore,
        IActorRef<BackgroundWriterActor, BackgroundWriteRequest> backgroundWriter,
        IPersistenceBackend persistenceBackend,
        IRaft raft,
        ILogger<IKahuna> logger
    ) : base(keyValuesStore, backgroundWriter, persistenceBackend, raft, logger)
    {

    }

    public async Task<KeyValueResponse> Execute(KeyValueRequest message)
    {
        if (message.TransactionId == HLCTimestamp.Zero)
        {
            logger.LogWarning("Cannot commit mutations for missing transaction id");
            
            return KeyValueStaticResponses.ErroredResponse;
        }

        KeyValueContext? context = await GetKeyValueContext(message.Key, message.Durability);

        if (context is null)
        {
            logger.LogWarning("Key/Value context is missing for {TransactionId}", message.TransactionId);
            
            return KeyValueStaticResponses.ErroredResponse;;
        }

        if (context.WriteIntent is null)
        {
            logger.LogWarning("Write intent is missing for {TransactionId}", message.TransactionId);
            
            return KeyValueStaticResponses.ErroredResponse;
        }

        if (context.WriteIntent.TransactionId != message.TransactionId)
        {
            logger.LogWarning("Write intent conflict between {CurrentTransactionId} and {TransactionId}", context.WriteIntent.TransactionId, message.TransactionId);
            
            return KeyValueStaticResponses.ErroredResponse;
        }

        if (context.MvccEntries is null)
        {
            logger.LogWarning("Couldn't find MVCC entry for transaction {TransactionId} [1]", message.TransactionId);
            
            return KeyValueStaticResponses.ErroredResponse;
        }

        if (!context.MvccEntries.TryGetValue(message.TransactionId, out KeyValueMvccEntry? entry))
        {
            logger.LogWarning("Couldn't find MVCC entry for transaction {TransactionId} [2]", message.TransactionId);
            
            return KeyValueStaticResponses.ErroredResponse;
        }

        KeyValueProposal proposal = new(
            message.Key,
            entry.Value,
            entry.Revision,
            entry.Expires,
            entry.LastUsed,
            entry.LastModified,
            entry.State
        );

        if (message.Durability != KeyValueDurability.Persistent)
        {
            context.Value = proposal.Value;
            context.Expires = proposal.Expires;
            context.Revision = proposal.Revision;
            context.LastUsed = proposal.LastUsed;
            context.LastModified = proposal.LastModified;
            context.State = proposal.State;
            
            return new(KeyValueResponseType.Committed, 0);
        }
        
        (bool success, long commitIndex) = await CommitKeyValueMessage(message.Key, message.ProposalTicketId);
        if (!success)
            return KeyValueStaticResponses.ErroredResponse;
        
        if (message.Durability == KeyValueDurability.Persistent)
        {
            // Schedule store to be processed asynchronously in a background actor
            backgroundWriter.Send(new(
                BackgroundWriteType.QueueStoreKeyValue,
                -1,
                proposal.Key,
                proposal.Value,
                proposal.Revision,
                proposal.Expires,
                proposal.LastUsed,
                proposal.LastModified,
                (int)proposal.State
            ));
        }
        
        context.Value = proposal.Value;
        context.Expires = proposal.Expires;
        context.Revision = proposal.Revision;
        context.LastUsed = proposal.LastUsed;
        context.LastModified = proposal.LastModified;
        context.State = proposal.State;
        
        return new(KeyValueResponseType.Committed, commitIndex);
    }
    
    /// <summary>
    /// Commits a previously proposed key value message
    /// </summary>
    /// <param name="key"></param>
    /// <param name="proposalTicketId"></param>
    /// <returns></returns>
    private async Task<(bool, long)> CommitKeyValueMessage(string key, HLCTimestamp proposalTicketId)
    {
        if (!raft.Joined)
            return (true, 0);

        int partitionId = raft.GetPartitionKey(key);

        (bool success, RaftOperationStatus status, long commitLogId) = await raft.CommitLogs(
            partitionId,
            proposalTicketId
        );

        if (!success)
        {
            logger.LogWarning("Failed to commit key/value {Key} Partition={Partition} Status={Status}", key, partitionId, status);
            
            return (false, 0);
        }
        
        logger.LogDebug("Successfully commmitted key/value {Key} Partition={Partition} ProposalIndex={ProposalIndex}", key, partitionId, commitLogId);

        return (success, commitLogId);
    }
}