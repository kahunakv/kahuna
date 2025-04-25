
using Kommander.Time;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// Represents a proposal for a key-value operation in the system. This proposal encapsulates
/// all relevant metadata and state required to process or prepare a key-value mutation.
/// </summary>
public sealed class KeyValueProposal
{
    /// <summary>
    /// Gets the unique key associated with the key-value proposal.
    /// The key identifies the entry within the key-value data store and is used
    /// for partitioning, retrieval, or processing operations.
    /// </summary>
    public string Key { get; }

    /// <summary>
    /// Gets the value associated with the key-value proposal.
    /// This represents the data or payload to be stored or processed
    /// in the key-value store, which may be null if no value is provided.
    /// </summary>
    public byte[]? Value { get; }

    /// <summary>
    /// Gets the revision number associated with the key-value proposal.
    /// The revision represents the unique version of the key-value pair
    /// and is used to track changes over time for concurrency control
    /// or update resolution.
    /// </summary>
    public long Revision { get; }

    /// <summary>
    /// Gets the expiry timestamp associated with the key-value proposal.
    /// Represents the logical and physical hybrid timestamp indicating when
    /// the associated data entry is considered expired and may no longer be valid.
    /// </summary>
    public HLCTimestamp Expires { get; }

    /// <summary>
    /// Gets the timestamp representing the last access or use of the key-value entry.
    /// This property is updated to reflect the most recent time the entry was retrieved
    /// or interacted with, which can be used for access tracking or expiration logic.
    /// </summary>
    public HLCTimestamp LastUsed { get; }

    /// <summary>
    /// Gets the timestamp indicating the last modification time of the key-value proposal.
    /// This property is used to track when the associated entry was most recently updated.
    /// </summary>
    public HLCTimestamp LastModified { get; }

    /// <summary>
    /// Gets the state of the key-value proposal.
    /// Represents the current operational state of the proposal,
    /// indicating whether the associated key-value entry is undefined, set, or deleted.
    /// </summary>
    public KeyValueState State { get; }
    
    /// <summary>
    /// Constructor for creating a new instance of the KeyValueProposal class.
    /// </summary>
    /// <param name="key"></param>
    /// <param name="value"></param>
    /// <param name="revision"></param>
    /// <param name="expires"></param>
    /// <param name="lastUsed"></param>
    /// <param name="lastModified"></param>
    /// <param name="state"></param>
    public KeyValueProposal(
        string key, 
        byte[]? value, 
        long revision,
        HLCTimestamp expires, 
        HLCTimestamp lastUsed,
        HLCTimestamp lastModified,
        KeyValueState state
    )
    {
        Key = key;
        Value = value;
        Revision = revision;
        Expires = expires;
        LastUsed = lastUsed;
        LastModified = lastModified;
        State = state;
    }
}