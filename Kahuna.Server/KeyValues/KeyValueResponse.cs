
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues;

public readonly struct KeyValueResponse
{
    public KeyValueResponseType Type { get; }
    
    public long Revision { get; } 
    
    public ReadOnlyKeyValueContext? Context { get; }
    
    public KeyValueResponse(KeyValueResponseType type)
    {
        Type = type;
    }
    
    public KeyValueResponse(KeyValueResponseType type, long revision)
    {
        Type = type;
        Revision = revision;
    }
    
    public KeyValueResponse(KeyValueResponseType type, ReadOnlyKeyValueContext? context)
    {
        Type = type;
        Context = context;
    }
}