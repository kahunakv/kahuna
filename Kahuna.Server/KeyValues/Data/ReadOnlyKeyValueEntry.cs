
using Kommander.Time;

namespace Kahuna.Server.KeyValues;

public sealed class ReadOnlyKeyValueEntry
{
    public byte[]? Value { get; }
    
    public long Revision { get; }
    
    public HLCTimestamp Expires { get; }
    
    public HLCTimestamp LastUsed { get; }
    
    public HLCTimestamp LastModified { get; }
    
    public KeyValueState State { get; }
    
    public ReadOnlyKeyValueEntry(byte[]? value, long revision, HLCTimestamp expires, HLCTimestamp lastUsed, HLCTimestamp lastModified, KeyValueState state)
    {
        Value = value;
        Revision = revision;
        Expires = expires;
        LastUsed = lastUsed;
        LastModified = lastModified;
        State = state;
    }
}