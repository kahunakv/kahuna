
using Nixie;
using Kahuna.Server.Locks.Data;
using Kahuna.Shared.Locks;
using Kommander.Time;

namespace Kahuna.Server.Locks;

internal sealed class LockProposalRequest
{
    public LockRequestType Type { get; }
    
    public int ProposalId { get; }
    
    public LockProposal Proposal { get; }
    
    public IActorRef<LockActor, LockRequest, LockResponse> LockActor { get; }

    public TaskCompletionSource<LockResponse?> Promise { get; }
    
    public HLCTimestamp Timestamp { get; }
    
    public LockProposalRequest(
        LockRequestType type,
        int proposalId, 
        LockProposal proposal, 
        IActorRef<LockActor, LockRequest, LockResponse> lockActor, 
        TaskCompletionSource<LockResponse?> promise,
        HLCTimestamp timestamp
    )
    {
        Type = type;
        ProposalId = proposalId;
        Proposal = proposal;
        LockActor = lockActor;
        Promise = promise;
        Timestamp = timestamp;
    }
}