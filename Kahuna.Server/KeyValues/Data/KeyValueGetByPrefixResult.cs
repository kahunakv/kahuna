
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues;

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