
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// Represents the result of a key-value retrieval operation using a prefix.
/// </summary>
public sealed class KeyValueGetByPrefixResult
{
    public KeyValueResponseType Type { get; }
    
    public List<(string, ReadOnlyKeyValueContext)> Items { get; }
    
    public KeyValueGetByPrefixResult(KeyValueResponseType type, List<(string, ReadOnlyKeyValueContext)> items)
    {
        Type = type;
        Items = items;
    }
}