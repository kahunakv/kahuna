
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// Represents the result of a key-value retrieval operation using a bucket.
/// </summary>
public sealed class KeyValueGetByBucketResult
{
    public KeyValueResponseType Type { get; }
    
    public List<(string, ReadOnlyKeyValueEntry)> Items { get; }
    
    public KeyValueGetByBucketResult(KeyValueResponseType type, List<(string, ReadOnlyKeyValueEntry)> items)
    {
        Type = type;
        Items = items;
    }
}