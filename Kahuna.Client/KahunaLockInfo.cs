
using Kommander.Time;

namespace Kahuna.Client;

public sealed class KahunaLockInfo
{
    public string Owner { get; }
    
    public HLCTimestamp Expires { get; }
    
    public long FencingToken { get; }
    
    public KahunaLockInfo(string owner, HLCTimestamp expires, long fencingToken)
    {
        Owner = owner;
        Expires = expires;
        FencingToken = fencingToken;
    }
}