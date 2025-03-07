
using Kahuna.Shared.KeyValue;
using Nixie.Routers;
using Standart.Hash.xxHash;

namespace Kahuna.KeyValues;

/// <summary>
/// Represents a request to perform an action on a key-value actor
/// </summary>
public readonly struct KeyValueRequest : IConsistentHashable
{
    public KeyValueRequestType Type { get; }
    
    public string Key { get; }
    
    public string? Value { get; }
    
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
    public KeyValueRequest(KeyValueRequestType type, string key, string? value, int expiresMs, KeyValueConsistency consistency)
    {
        Type = type;
        Key = key;
        Value = value;
        ExpiresMs = expiresMs;
        Consistency = consistency;
    }

    public int GetHash()
    {
        return (int)xxHash32.ComputeHash(Key ?? "");
    }
}