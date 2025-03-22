
using Kommander;
using Nixie.Routers;

namespace Kahuna.Server.Persistence;

public sealed class PersistenceRequest : IConsistentHashable
{
    public PersistenceRequestType Type { get; }
    
    public string Key { get; }
    
    public byte[]? Value { get; }
    
    public long Revision { get; }
    
    public long ExpiresLogical { get; }
    
    public uint ExpiresCounter { get; }
    
    public int State { get; }
    
    public PersistenceRequest(
        PersistenceRequestType type,
        string key, 
        byte[]? value, 
        long revision, 
        long expiresLogical,
        uint expiresCounter, 
        int state
    )
    {
        Type = type;
        Key = key;
        Value = value;
        Revision = revision;
        ExpiresLogical = expiresLogical;
        ExpiresCounter = expiresCounter;
        State = state;
    }

    public int GetHash()
    {
        return (int)HashUtils.ConsistentHash(Key, 4);
    }
}