
using Kahuna.Locks;
using Kommander.Time;

namespace Kahuna.Persistence;

public sealed class PersistenceItem
{
    public string Resource { get; }
    
    public string? Owner { get; }
    
    public long FencingToken { get; }
    
    public HLCTimestamp Expires { get; }
    
    public LockConsistency Consistency { get; }
    
    public LockState State { get; }
    
    public PersistenceItem(
        string resource, 
        string? owner, 
        long fencingToken, 
        HLCTimestamp expires, 
        LockConsistency consistency,
        LockState state
    )
    {
        Resource = resource;
        Owner = owner;
        FencingToken = fencingToken;
        Expires = expires;
        Consistency = consistency;
        State = state;
    }
}