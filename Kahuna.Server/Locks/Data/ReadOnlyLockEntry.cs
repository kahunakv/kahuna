
using Kommander.Time;

namespace Kahuna.Server.Locks.Data;

/// <summary>
/// A snapshot of a lock entry that is read-only.
/// </summary>
public sealed class ReadOnlyLockEntry
{
    public byte[]? Owner { get; }
    
    public long FencingToken { get; }
    
    public HLCTimestamp Expires { get; }
    
    public ReadOnlyLockEntry(byte[]? owner, long fencingToken, HLCTimestamp expires)
    {
        Owner = owner;
        FencingToken = fencingToken;
        Expires = expires;
    }
}