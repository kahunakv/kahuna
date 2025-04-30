
using Google.Protobuf;
using Kahuna.Server.Configuration;
using Nixie;
using Kommander;
using Kommander.Time;

using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Server.Replication;
using Kahuna.Server.Replication.Protos;
using Kahuna.Shared.KeyValue;
using Kahuna.Utils;

namespace Kahuna.Server.KeyValues.Handlers;

/// <summary>
/// Handles the preparation of mutations for the key-value store.
/// Responsible for validating and preparing mutation requests before they are executed or persisted.
/// Ensures the mutations adhere to required conditions for proper operation within the key-value system.
/// </summary>
internal sealed class TryPrepareMutationsHandler : BaseHandler
{
    private const int DefaultTxCompleteTimeout = 15000;
    
    public TryPrepareMutationsHandler(BTree<string, KeyValueContext> keyValuesStore,
        Dictionary<string, KeyValueWriteIntent> locksByPrefix,
        IActorRef<BackgroundWriterActor, BackgroundWriteRequest> backgroundWriter,
        IPersistenceBackend persistenceBackend,
        IRaft raft,
        KahunaConfiguration configuration,
        ILogger<IKahuna> logger) : base(keyValuesStore, locksByPrefix, backgroundWriter, persistenceBackend, raft, configuration, logger)
    {
        
    }

    public async Task<KeyValueResponse> Execute(KeyValueRequest message)
    {
        if (message.TransactionId == HLCTimestamp.Zero)
        {
            logger.LogWarning("Cannot prepare mutations for missing transaction id");
            
            return KeyValueStaticResponses.ErroredResponse;
        }
        
        if (message.CommitId == HLCTimestamp.Zero)
        {
            logger.LogWarning("Cannot prepare mutations for missing commit id");
            
            return KeyValueStaticResponses.ErroredResponse;
        }

        KeyValueContext? context = await GetKeyValueContext(message.Key, message.Durability);
        if (context is null)
        {
            logger.LogWarning("Key/Value context is missing for {TransactionId}", message.TransactionId);
            
            return KeyValueStaticResponses.ErroredResponse;
        }

        if (context.WriteIntent is not null && context.WriteIntent.TransactionId != message.TransactionId)
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

        /// A new revision is available in the context which means the transaction was modified
        if (context.Revision > entry.Revision)
        {
            logger.LogWarning("Transaction CommitId={TransactionId} conflicts with Revision={Revision} NewRevision={NewRevision} [3]", message.CommitId, context.Revision, entry.Revision);
            
            return KeyValueStaticResponses.ErroredResponse;
        }
        
        /// Last modified is higher than the commit id which means the transaction was modified
        if (context.LastModified.CompareTo(message.CommitId) > 0)
        {
            logger.LogWarning("Transaction CommitId={TransactionId} conflicts with LastModified={LastModified} [4]", message.CommitId, context.LastModified);
            
            return KeyValueStaticResponses.ErroredResponse;
        }

        // Higher transactions has seen the committed value 
        foreach ((HLCTimestamp key, KeyValueMvccEntry _) in context.MvccEntries)
        {
            if (key.CompareTo(message.TransactionId) > 0)
            {
                logger.LogWarning("Transaction {TransactionId} conflicts with {ExistingTransactionId} [5]", message.TransactionId, key);
            
                return KeyValueStaticResponses.ErroredResponse;
            }
        }
        
        // Transaction queried a value that didn't exist
        if (entry.State == KeyValueState.Undefined)
            return new(KeyValueResponseType.Prepared);

        // In optimistic concurrency, we create the write intent if it doesn't exist
        // this is to ensure that the assigned transaction will win the race.
        // The write intent lease will by extended by DefaultTxCompleteTimeout
        // it will give the transaction enough time to commit or rollback
        if (context.WriteIntent is null)
        {
            context.WriteIntent = new()
            {
                TransactionId = message.TransactionId,
                Expires = message.TransactionId + DefaultTxCompleteTimeout
            };
        }
        else
        {
            context.WriteIntent.Expires = message.TransactionId + DefaultTxCompleteTimeout;
        }

        if (message.Durability != KeyValueDurability.Persistent)
            return new(KeyValueResponseType.Prepared);
        
        KeyValueProposal proposal = new(
            message.Key,
            entry.Value,
            entry.Revision,
            entry.Expires,
            entry.LastUsed,
            entry.LastModified,
            entry.State
        );

        (bool success, HLCTimestamp proposalTicket) = await PrepareKeyValueMessage(KeyValueRequestType.TrySet, proposal, message.TransactionId);
        if (!success)
        {
            logger.LogWarning("Failed to propose logs for {TransactionId}", message.TransactionId);
            
            return KeyValueStaticResponses.ErroredResponse;
        }

        return new(KeyValueResponseType.Prepared, proposalTicket);
    }
    
    /// <summary>
    /// Proposes a key value message to the partition
    /// </summary>
    /// <param name="type"></param>
    /// <param name="proposal"></param>
    /// <param name="currentTime"></param>
    /// <returns></returns>
    private async Task<(bool, HLCTimestamp)> PrepareKeyValueMessage(KeyValueRequestType type, KeyValueProposal proposal, HLCTimestamp currentTime)
    {
        if (!raft.Joined)
            return (true, HLCTimestamp.Zero);        

        int partitionId = raft.GetPartitionKey(proposal.Key);

        KeyValueMessage kvm = new()
        {
            Type = (int)type,
            Key = proposal.Key,
            Revision = proposal.Revision,
            ExpireNode = proposal.Expires.N,
            ExpirePhysical = proposal.Expires.L,
            ExpireCounter = proposal.Expires.C,
            LastUsedNode = proposal.LastUsed.N,
            LastUsedPhysical = proposal.LastUsed.L,
            LastUsedCounter = proposal.LastUsed.C,
            LastModifiedNode = proposal.LastModified.N,
            LastModifiedPhysical = proposal.LastModified.L,
            LastModifiedCounter = proposal.LastModified.C,
            TimePhysical = currentTime.L,
            TimeCounter = currentTime.C
        };
        
        if (proposal.Value is not null)
            kvm.Value = UnsafeByteOperations.UnsafeWrap(proposal.Value);

        RaftReplicationResult result = await raft.ReplicateLogs(
            partitionId,
            ReplicationTypes.KeyValues,
            ReplicationSerializer.Serialize(kvm),
            autoCommit: false
        );

        if (!result.Success)
        {
            logger.LogWarning("Failed to propose key/value {Key} Partition={Partition} Status={Status}", proposal.Key, partitionId, result.Status);
            
            return (false, HLCTimestamp.Zero);
        }
        
        logger.LogDebug("Successfully proposed key/value {Key} Partition={Partition} ProposalIndex={ProposalIndex}", proposal.Key, partitionId, result.LogIndex);

        return (result.Success, result.TicketId);
    }
}