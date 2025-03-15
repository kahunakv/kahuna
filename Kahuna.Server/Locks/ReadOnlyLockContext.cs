
using Kommander.Time;

namespace Kahuna.Locks;

/// <summary>
/// A snapshot of a lock context that is read-only.
/// </summary>
public readonly struct ReadOnlyLockContext
{
    public byte[]? Owner { get; }
    
    public long FencingToken { get; }
    
    public HLCTimestamp Expires { get; }
    
    public ReadOnlyLockContext(byte[]? owner, long fencingToken, HLCTimestamp expires)
    {
        Owner = owner;
        FencingToken = fencingToken;
        Expires = expires;
    }
}