
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.KeyValues;

public sealed class KeyValueResponse
{
    public KeyValueResponseType Type { get; }
    
    public long Revision { get; } 
    
    public HLCTimestamp Ticket { get; }
    
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
}