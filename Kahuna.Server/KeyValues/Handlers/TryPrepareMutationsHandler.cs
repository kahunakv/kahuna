
using Google.Protobuf;
using Nixie;
using Kommander;
using Kommander.Time;
using Kahuna.Server.Persistence;
using Kahuna.Server.Replication;
using Kahuna.Server.Replication.Protos;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Handlers;

internal sealed class TryPrepareMutationsHandler : BaseHandler
{
    public TryPrepareMutationsHandler(
        Dictionary<string, KeyValueContext> keyValuesStore,
        IActorRef<BackgroundWriterActor, BackgroundWriteRequest> backgroundWriter,
        IPersistence persistence,
        IRaft raft,
        ILogger<IKahuna> logger
    ) : base(keyValuesStore, backgroundWriter, persistence, raft, logger)
    {

    }

    public async Task<KeyValueResponse> Execute(KeyValueRequest message)
    {
        if (message.TransactionId == HLCTimestamp.Zero)
        {
            logger.LogWarning("Cannot prepare mutations for missing transaction id");
            
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
        
        if (context.LastUsed.CompareTo(message.TransactionId) > 0)
        {
            logger.LogWarning("Transaction {TransactionId} conflicts with {ExistingTransactionId} [3]", message.TransactionId, context.LastUsed);
            
            return KeyValueStaticResponses.ErroredResponse;
        }

        foreach ((HLCTimestamp key, KeyValueMvccEntry _) in context.MvccEntries)
        {
            if (key.CompareTo(message.TransactionId) > 0)
            {
                logger.LogWarning("Transaction {TransactionId} conflicts with {ExistingTransactionId} [4]", message.TransactionId, context.LastUsed);
            
                return KeyValueStaticResponses.ErroredResponse;
            }
        }
        
        // Transaction queried a value that didn't exist
        if (entry.State == KeyValueState.Undefined)
            return new(KeyValueResponseType.Prepared);

        // in optimistic concurrency, we create the write intent if it doesn't exist
        // this is to ensure that the assigned transaction will gain the race
        context.WriteIntent ??= new()
        {
            TransactionId = message.TransactionId,
            Expires = message.TransactionId + TimeSpan.FromMilliseconds(5000)
        };
        
        if (message.Durability != KeyValueDurability.Persistent)
            return new(KeyValueResponseType.Prepared);
        
        KeyValueProposal proposal = new(
            message.Key,
            entry.Value,
            entry.Revision,
            entry.Expires,
            entry.LastUsed,
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

        if (proposal.Key == "test")
            return (false, HLCTimestamp.Zero);

        int partitionId = raft.GetPartitionKey(proposal.Key);

        KeyValueMessage kvm = new()
        {
            Type = (int)type,
            Key = proposal.Key,
            Revision = proposal.Revision,
            ExpireLogical = proposal.Expires.L,
            ExpireCounter = proposal.Expires.C,
            TimeLogical = currentTime.L,
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