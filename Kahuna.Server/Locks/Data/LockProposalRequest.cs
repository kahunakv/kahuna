
using Nixie;
using Kahuna.Server.Locks.Data;

namespace Kahuna.Server.Locks;

internal sealed class LockProposalRequest
{
    public int ProposalId { get; }
    
    public LockProposal Proposal { get; }
    
    public IActorRef<LockActor, LockRequest, LockResponse> LockActor { get; }

    public ActorMessageReply<LockRequest, LockResponse> ActorContextReply { get; }
    
    public LockProposalRequest(
        int proposalId, 
        LockProposal proposal, 
        IActorRef<LockActor, LockRequest, LockResponse> lockActor, 
        ActorMessageReply<LockRequest, LockResponse> actorContextReply
    )
    {
        ProposalId = proposalId;
        Proposal = proposal;
        LockActor = lockActor;
        ActorContextReply = actorContextReply;
    }
}