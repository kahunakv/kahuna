
using Kommander.Time;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// 
/// </summary>
public sealed class KeyValueContext
{
    /// <summary>
    /// The current value of the key.
    /// </summary>
    public byte[]? Value { get; set; }
    
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
    /// Represents a potential write intent to modify the key
    /// </summary>
    public KeyValueWriteIntent? WriteIntent { get; set; }
    
    /// <summary>
    /// Represents recently accessed revisions
    /// </summary>
    public Dictionary<long, byte[]?>? Revisions { get; set; }
    
    /// <summary>
    /// Multiversion Concurrency Control (MVCC) values per TransactionId
    /// </summary>
    public Dictionary<HLCTimestamp, KeyValueMvccEntry>? MvccEntries { get; set; }

    /// <summary>
    /// Current state of the key
    /// </summary>
    public KeyValueState State { get; set; } = KeyValueState.Set;
}