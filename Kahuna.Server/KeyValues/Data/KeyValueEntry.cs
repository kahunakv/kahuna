
using Kommander.Time;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// Represents a key/value entry that holds metadata, historical versions and state.
/// Used to manage the lifecycle and multi-version concurrency control (MVCC)
/// of key-value pairs, providing support for persistence, revisions, expiration and lock management.
/// </summary>
public sealed class KeyValueEntry
{
    /// <summary>
    /// The current bucket of the key if any
    /// </summary>
    public string? Bucket { get; set; }
    
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
    /// HLC timestamp of the last time the key/value was used
    /// </summary>
    public HLCTimestamp LastUsed { get; set; }
    
    /// <summary>
    /// HLC timestamp of the last time the key was modified
    /// </summary>
    public HLCTimestamp LastModified { get; set; }

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