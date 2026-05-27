
using Google.Protobuf;
using Nixie;
using Kommander;
using Kahuna.Server.Configuration;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Server.Replication;
using Kahuna.Server.Replication.Protos;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.KeyValues;

internal sealed class KeyValueProposalActor : IActor<KeyValueProposalRequest>
{
    private readonly IRaft raft;

    private readonly IPersistenceBackend persistenceBackend;

    private readonly KahunaConfiguration configuration;

    private readonly ILogger<IKahuna> logger;

    public KeyValueProposalActor(
        IActorContext<KeyValueProposalActor, KeyValueProposalRequest> context,
        IRaft raft,
        IPersistenceBackend persistenceBackend,
        KahunaConfiguration configuration,
        ILogger<IKahuna> logger
    )
    {
        this.raft = raft;
        this.persistenceBackend = persistenceBackend;
        this.configuration = configuration;
        this.logger = logger;
    }

    public async Task Receive(KeyValueProposalRequest message)
    {
        if (!raft.Joined)
            return;
        
        KeyValueProposal proposal = message.Proposal;
        HLCTimestamp currentTime = message.Timestamp;
        int partitionId = raft.GetPartitionKey(proposal.Key);
        
        KeyValueMessage kvm = new()
        {
            Type = (int)message.Type,
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
            TimeNode = currentTime.N,
            TimePhysical = currentTime.L,
            TimeCounter = currentTime.C
        };
        
        if (proposal.Value is not null)
            kvm.Value = UnsafeByteOperations.UnsafeWrap(proposal.Value);
        
        IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> keyValueActor = message.KeyValueActor;

        RaftReplicationResult result = await raft.ReplicateLogs(
            partitionId,
            ReplicationTypes.KeyValues,
            ReplicationSerializer.Serialize(kvm)
        );

        if (!result.Success)
        {
            logger.LogWarning("Failed to replicate key/value {Key} Partition={Partition} Status={Status} Ticket={Ticket}", proposal.Key, partitionId, result.Status, result.TicketId);
            
            keyValueActor.Send(new(
                KeyValueRequestType.ReleaseProposal,
                HLCTimestamp.Zero,
                HLCTimestamp.Zero,
                proposal.Key, 
                null, 
                null,
                -1,
                KeyValueFlags.None,
                0, 
                HLCTimestamp.Zero,
                proposal.Durability,
                message.ProposalId,
                partitionId,
                message.Promise
            ));

            return;
        }
        
        keyValueActor.Send(new(
            KeyValueRequestType.CompleteProposal,
            HLCTimestamp.Zero,
            HLCTimestamp.Zero,
            proposal.Key, 
            null, 
            null,
            -1,
            KeyValueFlags.None,
            0, 
            HLCTimestamp.Zero,
            proposal.Durability,
            message.ProposalId,
            partitionId,
            message.Promise
        ));
    }
}