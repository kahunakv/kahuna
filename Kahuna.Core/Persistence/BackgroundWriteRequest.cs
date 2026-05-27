
using Kommander.Time;

namespace Kahuna.Server.Persistence;

/// <summary>
/// Represents a background write request.
/// </summary>
public sealed class BackgroundWriteRequest
{
    public BackgroundWriteType Type { get; }
    
    public int PartitionId { get; }
    
    public string Key { get; }
    
    public byte[]? Value { get; }
    
    public long Revision { get; }
    
    public HLCTimestamp Expires { get; }
    
    public HLCTimestamp LastUsed { get; }
    
    public HLCTimestamp LastModified { get; }
    
    public int State { get; }
    
    public BackgroundWriteRequest(
        BackgroundWriteType type,
        int partitionId,
        string key, 
        byte[]? value, 
        long revision, 
        HLCTimestamp expires, 
        HLCTimestamp lastUsed,
        HLCTimestamp lastModified,
        int state
    )
    {
        Type = type;
        PartitionId = partitionId;
        Key = key;
        Value = value;
        Revision = revision;
        Expires = expires;
        LastUsed = lastUsed;
        LastModified = lastModified;
        State = state;
    }
    
    public BackgroundWriteRequest(BackgroundWriteType type)
    {
        Type = type;
        Key = "";
    }
}