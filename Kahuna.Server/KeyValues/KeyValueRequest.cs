
using Kahuna.Shared.KeyValue;
using Nixie.Routers;
using Standart.Hash.xxHash;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// Represents a request to perform an action on a key-value actor
/// </summary>
public readonly struct KeyValueRequest : IConsistentHashable
{
    public KeyValueRequestType Type { get; }
    
    public string Key { get; }
    
    public byte[]? Value { get; }
    
    public byte[]? CompareValue { get; }
    
    public long CompareRevision { get; }
    
    public KeyValueFlags Flags { get; }
    
    public int ExpiresMs { get; }
    
    public KeyValueConsistency Consistency { get; }
    
    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="type"></param>
    /// <param name="key"></param>
    /// <param name="value"></param>
    /// <param name="expiresMs"></param>
    /// <param name="consistency"></param>
    public KeyValueRequest(
        KeyValueRequestType type, 
        string key, 
        byte[]? value, 
        byte[]? compareValue,
        long compareRevision,
        KeyValueFlags flags,
        int expiresMs, 
        KeyValueConsistency consistency
    )
    {
        Type = type;
        Key = key;
        Value = value;
        CompareValue = compareValue;
        CompareRevision = compareRevision;
        Flags = flags;
        ExpiresMs = expiresMs;
        Consistency = consistency;
    }

    public int GetHash()
    {
        return (int)xxHash32.ComputeHash(Key ?? "");
    }
}