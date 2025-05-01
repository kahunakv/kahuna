
using Google.Protobuf;
using Nixie;
using Kommander;
using Kahuna.Server.Configuration;
using Kahuna.Server.Locks.Data;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Server.Replication;
using Kahuna.Server.Replication.Protos;
using Kahuna.Shared.Locks;

namespace Kahuna.Server.Locks;

internal sealed class LockProposalActor : IActor<LockProposalRequest>
{
    private readonly IRaft raft;

    private readonly IPersistenceBackend persistenceBackend;

    private readonly KahunaConfiguration configuration;

    private readonly ILogger<IKahuna> logger;
    
    public LockProposalActor(
        IActorContext<LockProposalActor, LockProposalRequest> context,
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
    
    public async Task Receive(LockProposalRequest message)
    {
        if (!raft.Joined)
            return;
        
        var proposal = message.Proposal;

        int partitionId = raft.GetPartitionKey(proposal.Resource);

        LockMessage lockMessage = new()
        {
            //Type = (int)type,
            Type = (int)LockRequestType.TryLock,
            Resource = proposal.Resource,
            FencingToken = proposal.FencingToken,
            ExpireNode = proposal.Expires.N,
            ExpirePhysical = proposal.Expires.L,
            ExpireCounter = proposal.Expires.C,
            LastUsedNode = proposal.LastUsed.N,
            LastUsedPhysical = proposal.LastUsed.L,
            LastUsedCounter = proposal.LastUsed.C,
            LastModifiedNode = proposal.LastModified.N,
            LastModifiedPhysical = proposal.LastModified.L,
            LastModifiedCounter = proposal.LastModified.C,
            //TimeNode = currentTime.N,
            //TimePhysical = currentTime.L,
            //TimeCounter = currentTime.C
        };

        if (proposal.Owner is not null)
            lockMessage.Owner = UnsafeByteOperations.UnsafeWrap(proposal.Owner);

        RaftReplicationResult result = await raft.ReplicateLogs(
            partitionId,
            ReplicationTypes.Locks,
            ReplicationSerializer.Serialize(lockMessage)
        );
               
        ActorMessageReply<LockRequest, LockResponse> reply = message.ActorContextReply;
        
        reply.Promise.SetResult(new LockResponse(LockResponseType.Locked, proposal.FencingToken));
        
        IActorRef<LockActor, LockRequest, LockResponse> lockActor = message.LockActor;
        
        lockActor.Send(new(
            LockRequestType.CompleteProposal, 
            proposal.Resource, 
            null, 
            0, 
            LockDurability.Ephemeral,
            message.ProposalId
        ));

        if (!result.Success)
        {
            logger.LogWarning("Failed to replicate lock {Resource} Partition={Partition} Status={Status} Ticket={Ticket}", proposal.Resource, partitionId, result.Status, result.TicketId);
            
            return;
        }
        
        Console.WriteLine("Lock proposal replicated successfully.");

        /*backgroundWriter.Send(new(
            BackgroundWriteType.QueueStoreLock,
            partitionId,
            proposal.Resource, 
            proposal.Owner, 
            proposal.FencingToken,
            proposal.Expires,
            proposal.LastUsed,
            proposal.LastModified,
            (int)proposal.State
        ));

        return result.Success;*/
    }        
}