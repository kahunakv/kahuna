
using Kommander.Time;

namespace Kahuna.Locks;

/// <summary>
/// 
/// </summary>
public sealed class LockContext
{
    /// <summary>
    /// The current owner of the lock.
    /// </summary>
    public string? Owner { get; set; }
    
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
}