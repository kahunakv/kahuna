
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// Represents a response object for the KeyValueActor operations
/// </summary>
/// <remarks>
/// The <see cref="KeyValueResponse"/> class encapsulates the result of operations performed within the
/// key-value actor. It is designed to handle various response scenarios, including success, errors,
/// locks, and other specific outcomes. The response may include additional metadata such as the revision
/// number, timestamp, or context items depending on the operation performed.
/// </remarks>
public sealed class KeyValueResponse
{
    /// <summary>
    /// Gets the response type of the key-value operation.
    /// </summary>
    /// <remarks>
    /// The <see cref="Type"/> property indicates the result or response type of the operation performed
    /// against the key-value system. It provides information on whether the operation was successful,
    /// encountered errors, involved locks, or resulted in other specific states. The value is derived from
    /// the <see cref="KeyValueResponseType"/> enumeration, which defines all possible response types.
    /// </remarks>
    public KeyValueResponseType Type { get; }

    /// <summary>
    /// Gets the revision number associated with the key-value operation.
    /// </summary>
    /// <remarks>
    /// The <see cref="Revision"/> property represents a unique, incremental value that indicates
    /// the specific version or state of the key-value store after a particular operation.
    /// It is used for concurrency control and tracking changes within the system, ensuring that
    /// modifications can be identified with precision based on their order of execution.
    /// </remarks>
    public long Revision { get; }

    /// <summary>
    /// Gets the hybrid logical clock (HLC) timestamp associated with preparation of the mutation
    /// when the key-value operation was last modified or processed as part of a transaction.
    /// </summary>
    public HLCTimestamp Ticket { get; }

    /// <summary>
    /// Gets the read-only key-value context associated with the response. Used in the 'get' operation.
    /// </summary>
    /// <remarks>
    /// The <see cref="Context"/> property provides additional context about the key-value operation if available.
    /// It encapsulates metadata or supplementary information relevant to the request, such as conditions
    /// or detailed operation state. This property can return null if no context is associated with the response.
    /// </remarks>
    public ReadOnlyKeyValueContext? Context { get; }
    
    /// <summary>
    /// Used in the 'get by prefix' operation to return all the found values.
    /// </summary>
    public List<(string, ReadOnlyKeyValueContext)>? Items { get; }
    
    /// <summary>
    /// Construtor
    /// </summary>
    /// <param name="type"></param>
    public KeyValueResponse(KeyValueResponseType type)
    {
        Type = type;
    }
    
    public KeyValueResponse(KeyValueResponseType type, long revision)
    {
        Type = type;
        Revision = revision;
    }
    
    public KeyValueResponse(KeyValueResponseType type, long revision, HLCTimestamp lastModified)
    {
        Type = type;
        Revision = revision;
        Ticket = lastModified;
    }
    
    public KeyValueResponse(KeyValueResponseType type, HLCTimestamp ticket)
    {
        Type = type;
        Ticket = ticket;
    }
    
    public KeyValueResponse(KeyValueResponseType type, ReadOnlyKeyValueContext? context)
    {
        Type = type;
        Context = context;
    }
    
    public KeyValueResponse(KeyValueResponseType type, List<(string, ReadOnlyKeyValueContext)> items)
    {
        Type = type;
        Items = items;
    }
}