
using Google.Protobuf;

using Kommander;
using Kommander.Time;

using Kahuna.Server.Replication;
using Kahuna.Server.Replication.Protos;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Handlers;

/// <summary>
/// Handles the preparation of mutations for the key-value store.
/// Responsible for validating and preparing mutation requests before they are executed or persisted.
/// Ensures the mutations adhere to required conditions for proper operation within the key-value system.
/// </summary>
internal sealed class TryPrepareMutationsHandler : BaseHandler
{
    private const int DefaultTxCompleteTimeout = 15000;
    
    public TryPrepareMutationsHandler(KeyValueContext context) : base(context)
    {
        
    }

    public async Task<KeyValueResponse> Execute(KeyValueRequest message)
    {
        if (message.TransactionId == HLCTimestamp.Zero)
        {
            context.Logger.LogWarning("Cannot prepare mutations for missing transaction id");
            
            return KeyValueStaticResponses.ErroredResponse;
        }
        
        if (message.CommitId == HLCTimestamp.Zero)
        {
            context.Logger.LogWarning("Cannot prepare mutations for missing commit id");
            
            return KeyValueStaticResponses.ErroredResponse;
        }

        KeyValueEntry? entry = await GetKeyValueEntry(message.Key, message.Durability);
        if (entry is null)
        {
            context.Logger.LogWarning("Key/Value context is missing for {TransactionId}", message.TransactionId);
            
            return KeyValueStaticResponses.ErroredResponse;
        }

        if (entry.WriteIntent is not null && entry.WriteIntent.TransactionId != message.TransactionId)
        {
            context.Logger.LogWarning("Write intent conflict between {CurrentTransactionId} and {TransactionId}", entry.WriteIntent.TransactionId, message.TransactionId);
        
            return KeyValueStaticResponses.ErroredResponse;
        }
        
        if (entry.Bucket is not null && context.LocksByPrefix.TryGetValue(entry.Bucket, out KeyValueWriteIntent? intent))
        {
            if (intent.TransactionId != message.TransactionId)
            {
                if (intent.Expires - message.CommitId > TimeSpan.Zero)
                    return new(KeyValueResponseType.MustRetry, 0);
            
                context.LocksByPrefix.Remove(entry.Bucket);
            }
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

        /// A new revision is available in the context which means the transaction was modified
        if (entry.Revision > mvccEntry.Revision)
        {
            context.Logger.LogWarning("Transaction CommitId={TransactionId} conflicts with Revision={Revision} NewRevision={NewRevision} [3]", message.CommitId, entry.Revision, mvccEntry.Revision);
            
            return KeyValueStaticResponses.ErroredResponse;
        }
        
        /// Last modified is higher than the commit id which means the transaction was modified
        if (entry.LastModified.CompareTo(message.CommitId) > 0)
        {
            context.Logger.LogWarning("Transaction CommitId={TransactionId} conflicts with LastModified={LastModified} [4]", message.CommitId, entry.LastModified);
            
            return KeyValueStaticResponses.ErroredResponse;
        }

        // Higher transactions has seen the committed value 
        foreach ((HLCTimestamp key, KeyValueMvccEntry _) in entry.MvccEntries)
        {
            if (key.CompareTo(message.TransactionId) > 0)
            {
                context.Logger.LogWarning("Transaction {TransactionId} conflicts with {ExistingTransactionId} [5]", message.TransactionId, key);
            
                return KeyValueStaticResponses.ErroredResponse;
            }
        }
        
        // Transaction queried a value that didn't exist
        if (mvccEntry.State == KeyValueState.Undefined)
            return KeyValueStaticResponses.PrepareResponse;

        // In optimistic concurrency, we create the write intent if it doesn't exist
        // this is to ensure that the assigned transaction will win the race.
        // The write intent lease will by extended by DefaultTxCompleteTimeout
        // it will give the transaction enough time to commit or rollback
        if (entry.WriteIntent is null)
        {
            entry.WriteIntent = new()
            {
                TransactionId = message.TransactionId,
                Expires = message.TransactionId + DefaultTxCompleteTimeout
            };
        }
        else
        {
            entry.WriteIntent.Expires = message.TransactionId + DefaultTxCompleteTimeout;
        }

        if (message.Durability != KeyValueDurability.Persistent)
            return KeyValueStaticResponses.PrepareResponse;
        
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

        (bool success, HLCTimestamp proposalTicket) = await PrepareKeyValueMessage(KeyValueRequestType.TrySet, proposal, message.TransactionId);
        if (!success)
        {
            context.Logger.LogWarning("Failed to propose logs for {TransactionId}", message.TransactionId);
            
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
        if (!context.Raft.Joined)
            return (true, HLCTimestamp.Zero);        

        int partitionId = context.Raft.GetPartitionKey(proposal.Key);

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

        RaftReplicationResult result = await context.Raft.ReplicateLogs(
            partitionId,
            ReplicationTypes.KeyValues,
            ReplicationSerializer.Serialize(kvm),
            autoCommit: false
        );

        if (!result.Success)
        {
            context.Logger.LogWarning("Failed to propose key/value {Key} Partition={Partition} Status={Status}", proposal.Key, partitionId, result.Status);
            
            return (false, HLCTimestamp.Zero);
        }
        
        context.Logger.LogDebug("Successfully proposed key/value {Key} Partition={Partition} ProposalIndex={ProposalIndex}", proposal.Key, partitionId, result.LogIndex);

        return (result.Success, result.TicketId);
    }
}