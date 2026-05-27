
using Nixie;
using Kommander.Time;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues;

internal sealed class KeyValueProposalRequest
{
    public KeyValueRequestType Type { get; }
    
    public int ProposalId { get; }
    
    public KeyValueProposal Proposal { get; }
    
    public IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> KeyValueActor { get; }

    public TaskCompletionSource<KeyValueResponse?> Promise { get; }
    
    public HLCTimestamp Timestamp { get; }
    
    public KeyValueProposalRequest(
        KeyValueRequestType type,
        int proposalId, 
        KeyValueProposal proposal, 
        IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> keyValueActor, 
        TaskCompletionSource<KeyValueResponse?> promise,
        HLCTimestamp timestamp
    )
    {
        Type = type;
        ProposalId = proposalId;
        Proposal = proposal;
        KeyValueActor = keyValueActor;
        Promise = promise;
        Timestamp = timestamp;
    }
}