
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
    
    public long LastUsedPhysical { get; }
    
    public uint LastUsedCounter { get; }
    
    public long LastModifiedPhysical { get; }
    
    public uint LastModifiedCounter { get; }
    
    public int State { get; }
    
    public PersistenceRequestItem(
        string key, 
        byte[]? value, 
        long revision, 
        long expiresPhysical,
        uint expiresCounter,
        long lastUsedPhysical,
        uint lastUsedCounter,
        long lastModifiedPhysical,
        uint lastModifiedCounter,
        int state
    )
    {
        Key = key;
        Value = value;
        Revision = revision;
        ExpiresPhysical = expiresPhysical;
        ExpiresCounter = expiresCounter;
        LastUsedPhysical = lastUsedPhysical;
        LastUsedCounter = lastUsedCounter;
        LastModifiedPhysical = lastModifiedPhysical;
        LastModifiedCounter = lastModifiedCounter;
        State = state;
    }
}
