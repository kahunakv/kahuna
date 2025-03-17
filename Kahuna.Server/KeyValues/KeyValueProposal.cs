
using Kommander.Time;

namespace Kahuna.Server.KeyValues;

public readonly struct KeyValueProposal
{
    public string Key { get; } 
    
    public byte[]? Value { get; } 
    
    public long Revision { get; }
    
    public HLCTimestamp Expires { get; } 
    
    public HLCTimestamp LastUsed { get; }
    
    public KeyValueState State { get; }
    
    public KeyValueProposal(
        string key, 
        byte[]? value, 
        long revision,
        HLCTimestamp expires, 
        HLCTimestamp lastUsed,
        KeyValueState state
    )
    {
        Key = key;
        Value = value;
        Revision = revision;
        Expires = expires;
        LastUsed = lastUsed;
        State = state;
    }
}