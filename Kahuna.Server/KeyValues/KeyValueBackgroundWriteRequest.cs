
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.KeyValues;

/// <summary>
/// Represents a keyValue background write request.
/// </summary>
public sealed class KeyValueBackgroundWriteRequest
{
    public KeyValueBackgroundWriteType Type { get; }
    
    public int PartitionId { get; }
    
    public string Key { get; }
    
    public string? Value { get; }
    
    public long Revision { get; }
    
    public HLCTimestamp Expires { get; }
    
    public KeyValueConsistency Consistency { get; }
    
    public KeyValueState State { get; }
    
    public KeyValueBackgroundWriteRequest(
        KeyValueBackgroundWriteType type,
        int partitionId,
        string key, 
        string? value, 
        long revision,
        HLCTimestamp expires, 
        KeyValueConsistency consistency,
        KeyValueState state
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
    
    public KeyValueBackgroundWriteRequest(KeyValueBackgroundWriteType type)
    {
        Type = type;
        Key = "";
    }
}