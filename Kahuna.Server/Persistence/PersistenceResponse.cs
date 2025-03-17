
namespace Kahuna.Server.Persistence;

public sealed class PersistenceResponse
{
    public PersistenceResponseType Type { get; }
    
    public PersistenceResponse(PersistenceResponseType type)
    {
        Type = type;
    }
}