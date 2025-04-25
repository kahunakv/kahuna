
using Kommander.Time;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// Represents a key-value entry in a multi-version concurrency control (MVCC) system.
/// </summary>
/// <remarks>
/// This class acts as a data structure for storing metadata and state related to
/// a key-value entry, supporting versioning, state tracking, expiration, and modification timestamps.
/// </remarks>
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