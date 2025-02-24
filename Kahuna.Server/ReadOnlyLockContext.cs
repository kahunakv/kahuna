
using Kommander.Time;

namespace Kahuna;

public sealed class ReadOnlyLockContext
{
    public string? Owner { get; }
    
    public HLCTimestamp Expires { get; }
    
    public long FencingToken { get; }
    
    public ReadOnlyLockContext(string? owner, HLCTimestamp expires, long fencingToken)
    {
        Owner = owner;
        Expires = expires;
        FencingToken = fencingToken;
    }
}