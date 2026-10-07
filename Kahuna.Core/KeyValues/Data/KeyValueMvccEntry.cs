
using Kommander.Time;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// Represents a key-value entry in a multi-version concurrency control (MVCC) system.
/// </summary>
/// <remarks>
/// This class acts as a data structure for storing metadata and state related to
/// a key-value entry, supporting versioning, state tracking, expiration, and modification timestamps.
/// </remarks>
internal sealed class KeyValueMvccEntry
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

    /// <summary>
    /// When true this write must not archive a historical revision entry.
    /// </summary>
    public bool NoRevision { get; set; }

    /// <summary>
    /// The write intent that held the key when this transaction staged its first write here, or null while the
    /// transaction has only read the key. A snapshot read waits for the staged write only while that same intent
    /// is live, so the commit-time probe proves the transaction never lost the key by finding this exact intent
    /// still in place and never lapsed (see <see cref="KeyValueWriteIntent.Lapsed"/>).
    /// </summary>
    public KeyValueWriteIntent? StagedUnder { get; set; }
}