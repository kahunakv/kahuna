
using Kommander.Time;

namespace Kahuna.KeyValues;

/// <summary>
/// 
/// </summary>
public sealed class KeyValueContext
{
    /// <summary>
    /// The current value of the key.
    /// </summary>
    public string? Value { get; set; }
    
    /// <summary>
    /// HLC timestamp when the lock will expire
    /// </summary>
    public HLCTimestamp Expires { get; set; }
    
    /// <summary>
    /// Current modification revision
    /// </summary>
    public long Revision { get; set; }
    
    /// <summary>
    /// HLC timestamp of the last time the lock was used
    /// </summary>
    public HLCTimestamp LastUsed { get; set; }

    /// <summary>
    /// Current state of the key
    /// </summary>
    public KeyValueState State { get; set; } = KeyValueState.Set;
}