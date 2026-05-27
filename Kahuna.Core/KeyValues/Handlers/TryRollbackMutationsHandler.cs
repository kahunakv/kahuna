
using Kahuna.Server.Configuration;
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

        KeyValueEntry? entry = await GetKeyValueEntry(message.Key, message.Durability);

        if (entry is null)
        {
            context.Logger.LogWarning("Key/Value context is missing for {TransactionId}", message.TransactionId);
            
            return KeyValueStaticResponses.ErroredResponse;
        }

        if (entry.ReplicationIntent is null)
        {
            context.Logger.LogWarning("Replication intent is active on key {Key}", message.Key);
            
            return KeyValueStaticResponses.WaitingForReplicationResponse;;
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

        if (!entry.MvccEntries.ContainsKey(message.TransactionId))
        {
            context.Logger.LogWarning("Couldn't find MVCC entry for transaction {TransactionId} [2]", message.TransactionId);
            
            return KeyValueStaticResponses.ErroredResponse;
        }

        if (message.Durability != KeyValueDurability.Persistent)
        {
            entry.MvccEntries.Remove(message.TransactionId);                   
            entry.WriteIntent = null;
            
            return new(KeyValueResponseType.RolledBack);
        }

        (bool success, long rollbackIndex) = await RollbackKeyValueMessage(message.Key, message.ProposalTicketId);
        
        entry.MvccEntries.Remove(message.TransactionId);                   
        entry.WriteIntent = null;

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
        if (!context.Raft.Joined)
            return (true, 0);

        int partitionId = context.Raft.GetPartitionKey(key);

        (bool success, RaftOperationStatus status, long logIndex) = await context.Raft.RollbackLogs(
            partitionId,
            proposalTicketId
        );

        if (!success)
        {
            context.Logger.LogWarning("Failed to rollback key/value {Key} Partition={Partition} Status={Status}", key, partitionId, status);
            
            return (false, 0);
        }
        
        context.Logger.LogDebug("Successfully rolled back key/value {Key} Partition={Partition} ProposalIndex={ProposalIndex}", key, partitionId, logIndex);

        return (success, logIndex);
    }
}