
using Kommander;
using Nixie.Routers;

namespace Kahuna.Server.Persistence;

public sealed class PersistenceRequestItem
{
    public string Key { get; }
    
    public byte[]? Value { get; }
    
    public long Revision { get; }
    
    public long ExpiresPhysical { get; }
    
    public uint ExpiresCounter { get; }
    
    public int State { get; }
    
    public PersistenceRequestItem(
        string key, 
        byte[]? value, 
        long revision, 
        long expiresPhysical,
        uint expiresCounter, 
        int state
    )
    {
        Key = key;
        Value = value;
        Revision = revision;
        ExpiresPhysical = expiresPhysical;
        ExpiresCounter = expiresCounter;
        State = state;
    }
}

public sealed class PersistenceRequest
{
    public PersistenceRequestType Type { get; }
    
    public List<PersistenceRequestItem> Items { get; }
    
    public PersistenceRequest(PersistenceRequestType type, List<PersistenceRequestItem> items)
    {
        Type = type;
        Items = items;
    }
}