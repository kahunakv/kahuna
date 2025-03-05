
using Kahuna.Shared.Locks;
using Kommander.Time;

namespace Kahuna.Locks;

/// <summary>
/// Represents a lock background write request.
/// </summary>
public sealed class LockBackgroundWriteRequest
{
    public LockBackgroundWriteType Type { get; }
    
    public int PartitionId { get; }
    
    public string Resource { get; }
    
    public string? Owner { get; }
    
    public long FencingToken { get; }
    
    public HLCTimestamp Expires { get; }
    
    public LockConsistency Consistency { get; }
    
    public LockState State { get; }
    
    public LockBackgroundWriteRequest(
        LockBackgroundWriteType type,
        int partitionId,
        string resource, 
        string? owner, 
        long fencingToken, 
        HLCTimestamp expires, 
        LockConsistency consistency,
        LockState state
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
    
    public LockBackgroundWriteRequest(LockBackgroundWriteType type)
    {
        Type = type;
        Resource = "";
    }
}