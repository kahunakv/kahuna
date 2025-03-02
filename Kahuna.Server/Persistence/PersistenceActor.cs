
using Nixie;
using Kahuna.Locks;

namespace Kahuna.Persistence;

public sealed class PersistenceActor : IActor<PersistenceRequest, PersistenceResponse>
{
    private readonly IPersistence persistence;
    
    public PersistenceActor(
        IActorContext<PersistenceActor, PersistenceRequest, PersistenceResponse> _, 
        IPersistence persistence,
        ILogger<IKahuna> logger
    )
    {
        this.persistence = persistence;
    }
    
    public async Task<PersistenceResponse?> Receive(PersistenceRequest message)
    {
        switch (message.Type)
        {
            case PersistenceRequestType.Store:
                await persistence.StoreLock(
                    message.Resource, 
                    message.Owner ?? "", 
                    message.ExpiresLogical, 
                    message.ExpiresCounter, 
                    message.FencingToken, 
                    (long)message.Consistency, 
                    message.State
                );
                break;
            
            case PersistenceRequestType.Get:
                LockContext? lockContext = await persistence.GetLock(message.Resource);
                if (lockContext == null)
                    return new(PersistenceResponseType.NotFound);
                
                return new(PersistenceResponseType.Found);
        }

        return new(PersistenceResponseType.Success);
    }
}