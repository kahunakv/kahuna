
using Kahuna.Shared.KeyValue;

namespace Kahuna.KeyValues;

public readonly struct KeyValueResponse
{
    public KeyValueResponseType Type { get; }
    
    public ReadOnlyKeyValueContext? Context { get; }
    
    public KeyValueResponse(KeyValueResponseType type)
    {
        Type = type;
    }
    
    public KeyValueResponse(KeyValueResponseType type, ReadOnlyKeyValueContext? context)
    {
        Type = type;
        Context = context;
    }
}