
using Kommander.Time;

namespace Kahuna.Server.KeyValues;

public sealed class KeyValueMvccEntry
{
    /// <summary>
    /// The current value of the key.
    /// </summary>
    public byte[]? Value { get; set; }
    
    /// <summary>
    /// HLC timestamp when the key/value will expire
    /// </summary>
    public HLCTimestamp Expires { get; set; }
    
    /// <summary>
    /// Current modification revision
    /// </summary>
    public long Revision { get; set; }
    
    /// <summary>
    /// HLC timestamp of the last time the key/value was used (read or written)
    /// </summary>
    public HLCTimestamp LastUsed { get; set; }
    
    /// <summary>
    /// HLC timestamp of the last time the key/value was modified
    /// </summary>
    public HLCTimestamp LastModified { get; set; }
    
    /// <summary>
    /// State of the key
    /// </summary>
    public KeyValueState State { get; set; }
}