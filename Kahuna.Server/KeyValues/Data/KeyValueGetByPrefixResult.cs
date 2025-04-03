namespace Kahuna.Server.KeyValues;

public sealed class KeyValueGetByPrefixResult
{
    public List<(string, ReadOnlyKeyValueContext)> Items { get; }
    
    public KeyValueGetByPrefixResult(List<(string, ReadOnlyKeyValueContext)> items)
    {
        Items = items;
    }
}