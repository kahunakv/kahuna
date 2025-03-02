
using Kahuna.Locks;
using Kahuna.Shared.Locks;

namespace Kahuna.Persistence;

public sealed class PersistenceRequest
{
    public PersistenceRequestType Type { get; }
    
    public string Resource { get; }
    
    public string? Owner { get; }
    
    public long FencingToken { get; }
    
    public long ExpiresLogical { get; }
    
    public uint ExpiresCounter { get; }
    
    public LockConsistency Consistency { get; }
    
    public LockState State { get; }
    
    public PersistenceRequest(
        PersistenceRequestType type,
        string resource, 
        string? owner, 
        long fencingToken, 
        long expiresLogical,
        uint expiresCounter, 
        LockConsistency consistency,
        LockState state
    )
    {
        Type = type;
        Resource = resource;
        Owner = owner;
        FencingToken = fencingToken;
        ExpiresLogical = expiresLogical;
        ExpiresCounter = expiresCounter;
        Consistency = consistency;
        State = state;
    }
}