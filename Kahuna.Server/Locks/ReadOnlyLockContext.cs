
using Kommander.Time;

namespace Kahuna.Locks;

/// <summary>
/// A snapshot of a lock context that is read-only.
/// </summary>
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