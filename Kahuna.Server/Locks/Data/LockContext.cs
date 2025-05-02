
using Kommander.Time;

namespace Kahuna.Server.Locks.Data;

/// <summary>
/// Represents the context of a lock, encapsulating the state, owner, expiration, and other
/// relevant metadata associated with a specific lock resource.
/// </summary>
internal sealed class LockContext
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
    public LockWriteIntent? WriteIntent { get; set; }
}