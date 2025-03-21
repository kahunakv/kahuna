
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
            
            return new(KeyValueResponseType.Errored);
        }

        KeyValueContext? context = await GetKeyValueContext(message.Key, message.Consistency);
        if (context is null)
        {
            logger.LogWarning("Key/Value context is missing for {TransactionId}", message.TransactionId);
            
            return new(KeyValueResponseType.Errored);
        }

        if (context.WriteIntent is null)
        {
            logger.LogWarning("Write intent is missing for {TransactionId}", message.TransactionId);
            
            return new(KeyValueResponseType.Errored);
        }

        if (context.WriteIntent.TransactionId != message.TransactionId)
        {
            logger.LogWarning("Write intent conflict between {CurrentTransactionId} and {TransactionId}", context.WriteIntent.TransactionId, message.TransactionId);
            
            return new(KeyValueResponseType.Errored);
        }

        if (context.MvccEntries is null)
        {
            logger.LogWarning("Couldn't find MVCC entry for transaction {TransactionId} [1]", message.TransactionId);
            
            return new(KeyValueResponseType.Errored);
        }

        if (!context.MvccEntries.TryGetValue(message.TransactionId, out KeyValueMvccEntry? entry))
        {
            logger.LogWarning("Couldn't find MVCC entry for transaction {TransactionId} [2]", message.TransactionId);
            
            return new(KeyValueResponseType.Errored);
        }
        
        if (message.Consistency != KeyValueConsistency.Linearizable)
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
            
            return new(KeyValueResponseType.Errored);
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
            TimeCounter = currentTime.C,
            Consistency = (int)KeyValueConsistency.LinearizableReplication
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