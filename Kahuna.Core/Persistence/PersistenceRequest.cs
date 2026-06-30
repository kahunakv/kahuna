
namespace Kahuna.Server.Persistence;

/// <summary>
///
/// </summary>
public readonly struct PersistenceRequestItem
{
    public string Key { get; }
    
    public byte[]? Value { get; }
    
    public long Revision { get; }
    
    public int ExpiresNode { get; }
    
    public long ExpiresPhysical { get; }
    
    public uint ExpiresCounter { get; }
    
    public int LastUsedNode { get; }
    
    public long LastUsedPhysical { get; }
    
    public uint LastUsedCounter { get; }
    
    public int LastModifiedNode { get; }
    
    public long LastModifiedPhysical { get; }
    
    public uint LastModifiedCounter { get; }
    
    public int State { get; }

    public bool NoRevision { get; }

    public PersistenceRequestItem(
        string key,
        byte[]? value,
        long revision,
        int expiresNode,
        long expiresPhysical,
        uint expiresCounter,
        int lastUsedNode,
        long lastUsedPhysical,
        uint lastUsedCounter,
        int lastModifiedNode,
        long lastModifiedPhysical,
        uint lastModifiedCounter,
        int state,
        bool noRevision = false
    )
    {
        Key = key;
        Value = value;
        Revision = revision;
        ExpiresNode = expiresNode;
        ExpiresPhysical = expiresPhysical;
        ExpiresCounter = expiresCounter;
        LastUsedNode = lastUsedNode;
        LastUsedPhysical = lastUsedPhysical;
        LastUsedCounter = lastUsedCounter;
        LastModifiedNode = lastModifiedNode;
        LastModifiedPhysical = lastModifiedPhysical;
        LastModifiedCounter = lastModifiedCounter;
        State = state;
        NoRevision = noRevision;
    }
}
