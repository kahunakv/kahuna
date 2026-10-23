
using Kommander.Time;

namespace Kahuna.Server.Locks.Data;

/// <summary>
/// Represents a lock entry, encapsulating the state, owner, expiration and other
/// relevant metadata associated with a specific lock resource.
/// </summary>
internal sealed class LockEntry
{
    /// <summary>
    /// The current owner of the lock.
    /// </summary>
    public byte[]? Owner { get; set; }
    
    /// <summary>
    /// HLC timestamp when the lock will expire
    /// </summary>
    public HLCTimestamp Expires { get; set; }
    
    /// <summary>
    /// Current fencing token
    /// </summary>
    public long FencingToken { get; set; }
    
    /// <summary>
    /// HLC timestamp of the last time the lock was used
    /// </summary>
    public HLCTimestamp LastUsed { get; set; }
    
    /// <summary>
    /// HLC timestamp of the last time the lock was used
    /// </summary>
    public HLCTimestamp LastModified { get; set; }
    
    /// <summary>
    /// Current state of the key
    /// </summary>
    public LockState State { get; set; } = LockState.Locked;
    
    /// <summary>
    /// Locks with persistent durability are temporarily marked for writing 
    /// </summary>
    public LockReplicationIntent? ReplicationIntent { get; set; }

    /// <summary>
    /// A new entry holding a persisted row, for a lock actor to own: the persistence backend returns
    /// immutable rows. Returns null for a null row.
    /// </summary>
    [return: System.Diagnostics.CodeAnalysis.NotNullIfNotNull(nameof(row))]
    public static LockEntry? FromRow(LockRow? row)
    {
        if (row is null)
            return null;

        return new()
        {
            Owner = row.Owner,
            FencingToken = row.FencingToken,
            Expires = row.Expires,
            LastUsed = row.LastUsed,
            LastModified = row.LastModified,
            State = row.State,
        };
    }

    /// <summary>The persisted fields of this entry as an immutable row.</summary>
    public LockRow ToRow() => new(Owner, FencingToken, Expires, LastUsed, LastModified, State);
}
