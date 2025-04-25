
namespace Kahuna.Server.Persistence;

/// <summary>
/// Represents a response used by the persistence layer to indicate the type of operation result.
/// </summary>
public sealed class PersistenceResponse
{
    public PersistenceResponseType Type { get; }
    
    public PersistenceResponse(PersistenceResponseType type)
    {
        Type = type;
    }
}