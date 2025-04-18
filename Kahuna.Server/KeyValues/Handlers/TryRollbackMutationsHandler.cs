
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Data;
using Kommander.Time;
using Nixie;

namespace Kahuna.Server.KeyValues.Handlers;

internal sealed class TryRollbackMutationsHandler : BaseHandler
{
    public TryRollbackMutationsHandler(
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
            logger.LogWarning("Cannot rollback mutations for missing transaction id");
            
            return KeyValueStaticResponses.ErroredResponse;
        }

        KeyValueContext? context = await GetKeyValueContext(message.Key, message.Durability);

        if (context is null)
        {
            logger.LogWarning("Key/Value context is missing for {TransactionId}", message.TransactionId);
            
            return KeyValueStaticResponses.ErroredResponse;
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

        if (message.Durability != KeyValueDurability.Persistent)
            return new(KeyValueResponseType.RolledBack);
        
        (bool success, long rollbackIndex) = await RollbackKeyValueMessage(message.Key, message.ProposalTicketId);
        
        if (!success)
            return KeyValueStaticResponses.ErroredResponse;
        
        return new(KeyValueResponseType.RolledBack, rollbackIndex);
    }
    
    /// <summary>
    /// Rollbacks a previously proposed key value message
    /// </summary>
    /// <param name="key"></param>
    /// <param name="proposalTicketId"></param>
    /// <returns></returns>
    private async Task<(bool, long)> RollbackKeyValueMessage(string key, HLCTimestamp proposalTicketId)
    {
        if (!raft.Joined)
            return (true, 0);

        int partitionId = raft.GetPartitionKey(key);

        (bool success, RaftOperationStatus status, long logIndex) = await raft.RollbackLogs(
            partitionId,
            proposalTicketId
        );

        if (!success)
        {
            logger.LogWarning("Failed to rollback key/value {Key} Partition={Partition} Status={Status}", key, partitionId, status);
            
            return (false, 0);
        }
        
        logger.LogDebug("Successfully rolled back key/value {Key} Partition={Partition} ProposalIndex={ProposalIndex}", key, partitionId, logIndex);

        return (success, logIndex);
    }
}