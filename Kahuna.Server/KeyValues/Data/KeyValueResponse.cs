
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.KeyValues;

public sealed class KeyValueResponse
{
    public KeyValueResponseType Type { get; }
    
    public long Revision { get; } 
    
    public HLCTimestamp Ticket { get; }
    
    public ReadOnlyKeyValueContext? Context { get; }
    
    public List<(string, ReadOnlyKeyValueContext)>? Items { get; }
    
    public KeyValueResponse(KeyValueResponseType type)
    {
        Type = type;
    }
    
    public KeyValueResponse(KeyValueResponseType type, long revision)
    {
        Type = type;
        Revision = revision;
    }
    
    public KeyValueResponse(KeyValueResponseType type, HLCTimestamp ticket)
    {
        Type = type;
        Ticket = ticket;
    }
    
    public KeyValueResponse(KeyValueResponseType type, ReadOnlyKeyValueContext? context)
    {
        Type = type;
        Context = context;
    }
    
    public KeyValueResponse(KeyValueResponseType type, List<(string, ReadOnlyKeyValueContext)> items)
    {
        Type = type;
        Items = items;
    }
}