
using Kommander.Time;

namespace Kahuna.Server.Locks;

public readonly struct LockProposal
{
    public string Resource { get; } 
    
    public byte[]? Owner { get; } 
    
    public long FencingToken { get; }
    
    public HLCTimestamp Expires { get; } 
    
    public HLCTimestamp LastUsed { get; }
    
    public LockState State { get; }
    
    public LockProposal(
        string resource, 
        byte[]? owner, 
        long fencingToken,
        HLCTimestamp expires, 
        HLCTimestamp lastUsed,
        LockState state
    )
    {
        Resource = resource;
        Owner = owner;
        FencingToken = fencingToken;
        Expires = expires;
        LastUsed = lastUsed;
        State = state;
    }
}