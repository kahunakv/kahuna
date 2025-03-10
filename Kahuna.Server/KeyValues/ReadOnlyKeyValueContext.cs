
using Kommander.Time;

namespace Kahuna.KeyValues;

public sealed class ReadOnlyKeyValueContext
{
    public string? Value { get; }
    
    public long Revision { get; }
    
    public HLCTimestamp Expires { get; }
    
    public ReadOnlyKeyValueContext(string? value, long revision, HLCTimestamp expires)
    {
        Value = value;
        Revision = revision;
        Expires = expires;
    }
}