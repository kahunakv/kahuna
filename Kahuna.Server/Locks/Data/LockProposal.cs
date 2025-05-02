
using Kommander.Time;
using Kahuna.Shared.Locks;

namespace Kahuna.Server.Locks.Data;

/// <summary>
/// Represents a proposal for a lock operation in the system.
/// This class encapsulates the state and metadata for a lock
/// associated with a resource, including ownership, expiration,
/// and fencing information.
/// </summary>
public sealed class LockProposal
{
    public LockRequestType Type { get; }
    
    public string Resource { get; } 
    
    public byte[]? Owner { get; } 
    
    public long FencingToken { get; }
    
    public HLCTimestamp Expires { get; } 
    
    public HLCTimestamp LastUsed { get; }
    
    public HLCTimestamp LastModified { get; }
    
    public LockState State { get; }
    
    public LockProposal(
        LockRequestType type,
        string resource, 
        byte[]? owner, 
        long fencingToken,
        HLCTimestamp expires, 
        HLCTimestamp lastUsed,
        HLCTimestamp lastModified,
        LockState state
    )
    {
        Type = type;
        Resource = resource;
        Owner = owner;
        FencingToken = fencingToken;
        Expires = expires;
        LastUsed = lastUsed;
        LastModified = lastModified;
        State = state;
    }
}