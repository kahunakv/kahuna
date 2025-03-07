
using Kommander.Time;

namespace Kahuna.KeyValues;

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
    /// HLC timestamp of the last time the lock was used
    /// </summary>
    public HLCTimestamp LastUsed { get; set; }
}