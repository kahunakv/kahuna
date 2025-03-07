
using Kommander.Time;

namespace Kahuna.KeyValues;

public sealed class ReadOnlyKeyValueContext
{
    public string? Value { get; }
    
    public HLCTimestamp Expires { get; }
    
    public ReadOnlyKeyValueContext(string? value, HLCTimestamp expires)
    {
        Value = value;
        Expires = expires;
    }
}