
using Kommander.Time;

namespace Kahuna.Server.Persistence;

/// <summary>
/// Represents a lock background write request.
/// </summary>
public sealed class BackgroundWriteRequest
{
    public BackgroundWriteType Type { get; }
    
    public int PartitionId { get; }
    
    public string Key { get; }
    
    public byte[]? Value { get; }
    
    public long Revision { get; }
    
    public HLCTimestamp Expires { get; }
    
    public int State { get; }
    
    public BackgroundWriteRequest(
        BackgroundWriteType type,
        int partitionId,
        string key, 
        byte[]? value, 
        long revision, 
        HLCTimestamp expires, 
        int state
    )
    {
        Type = type;
        PartitionId = partitionId;
        Key = key;
        Value = value;
        Revision = revision;
        Expires = expires;
        State = state;
    }
    
    public BackgroundWriteRequest(BackgroundWriteType type)
    {
        Type = type;
        Key = "";
    }
}