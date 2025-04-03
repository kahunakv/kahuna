
using Kommander.Time;

namespace Kahuna.Server.KeyValues;

public sealed class ReadOnlyKeyValueContext
{
    public byte[]? Value { get; }
    
    public long Revision { get; }
    
    public HLCTimestamp Expires { get; }
    
    public ReadOnlyKeyValueContext(byte[]? value, long revision, HLCTimestamp expires)
    {
        Value = value;
        Revision = revision;
        Expires = expires;
    }
}