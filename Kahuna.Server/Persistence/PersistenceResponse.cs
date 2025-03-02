
using Kahuna.Locks;

namespace Kahuna.Persistence;

public sealed class PersistenceResponse
{
    public PersistenceResponseType Type { get; }
    
    public LockContext? LockContext { get; }
    
    public PersistenceResponse(PersistenceResponseType type)
    {
        Type = type;
    }

    public PersistenceResponse(PersistenceResponseType type, LockContext lockContext)
    {
        Type = type;
        LockContext = lockContext;
    }
}