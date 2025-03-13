
using Kahuna.Locks;
using Kahuna.Shared.Locks;
using Kommander;
using Nixie.Routers;

namespace Kahuna.Persistence;

public sealed class PersistenceRequest : IConsistentHashable
{
    public PersistenceRequestType Type { get; }
    
    public string Key { get; }
    
    public string? Value { get; }
    
    public long Revision { get; }
    
    public long ExpiresLogical { get; }
    
    public uint ExpiresCounter { get; }
    
    public int Consistency { get; }
    
    public int State { get; }
    
    public PersistenceRequest(
        PersistenceRequestType type,
        string key, 
        string? value, 
        long revision, 
        long expiresLogical,
        uint expiresCounter, 
        int consistency,
        int state
    )
    {
        Type = type;
        Key = key;
        Value = value;
        Revision = revision;
        ExpiresLogical = expiresLogical;
        ExpiresCounter = expiresCounter;
        Consistency = consistency;
        State = state;
    }

    public int GetHash()
    {
        return (int)HashUtils.ConsistentHash(Key, 4);
    }
}