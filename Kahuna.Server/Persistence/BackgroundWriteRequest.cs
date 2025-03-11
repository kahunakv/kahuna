
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
    
    public string Resource { get; }
    
    public string? Owner { get; }
    
    public long FencingToken { get; }
    
    public HLCTimestamp Expires { get; }
    
    public int Consistency { get; }
    
    public int State { get; }
    
    public BackgroundWriteRequest(
        BackgroundWriteType type,
        int partitionId,
        string resource, 
        string? owner, 
        long fencingToken, 
        HLCTimestamp expires, 
        int consistency,
        int state
    )
    {
        Type = type;
        PartitionId = partitionId;
        Resource = resource;
        Owner = owner;
        FencingToken = fencingToken;
        Expires = expires;
        Consistency = consistency;
        State = state;
    }
    
    public BackgroundWriteRequest(BackgroundWriteType type)
    {
        Type = type;
        Resource = "";
    }
}