
using Kommander.Time;

namespace Kahuna.Server.Locks.Data;

/// <summary>
/// The persisted state of one lock, as a persistence backend stores and returns it. Immutable: the
/// lock actors keep their own mutable entries and copy a row into one when they load it.
/// </summary>
public sealed class LockRow
{
    /// <summary>The owner token, or null when the lock is free.</summary>
    public byte[]? Owner { get; }

    /// <summary>The fencing token of the current owner.</summary>
    public long FencingToken { get; }

    /// <summary>When the lock expires.</summary>
    public HLCTimestamp Expires { get; }

    /// <summary>When the lock was last read or written.</summary>
    public HLCTimestamp LastUsed { get; }

    /// <summary>When the lock was last written.</summary>
    public HLCTimestamp LastModified { get; }

    /// <summary>Whether the lock is held or released.</summary>
    public LockState State { get; }

    /// <summary>Creates a row from its fields.</summary>
    public LockRow(byte[]? owner, long fencingToken, HLCTimestamp expires, HLCTimestamp lastUsed, HLCTimestamp lastModified, LockState state)
    {
        Owner = owner;
        FencingToken = fencingToken;
        Expires = expires;
        LastUsed = lastUsed;
        LastModified = lastModified;
        State = state;
    }
}
