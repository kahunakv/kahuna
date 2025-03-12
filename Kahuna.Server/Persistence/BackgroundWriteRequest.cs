
using Kahuna.Shared.Locks;
using Kommander.Time;

namespace Kahuna.Locks;

/// <summary>
/// Represents a lock background write request.
/// </summary>
public sealed class BackgroundWriteRequest
{
    public BackgroundWriteType Type { get; }
    
    public int PartitionId { get; }
    
    public string Key { get; }
    
    public string? Value { get; }
    
    public long Revision { get; }
    
    public HLCTimestamp Expires { get; }
    
    public int Consistency { get; }
    
    public int State { get; }
    
    public BackgroundWriteRequest(
        BackgroundWriteType type,
        int partitionId,
        string key, 
        string? value, 
        long revision, 
        HLCTimestamp expires, 
        int consistency,
        int state
    )
    {
        Type = type;
        PartitionId = partitionId;
        Key = key;
        Value = value;
        Revision = revision;
        Expires = expires;
        Consistency = consistency;
        State = state;
    }
    
    public BackgroundWriteRequest(BackgroundWriteType type)
    {
        Type = type;
        Key = "";
    }
}