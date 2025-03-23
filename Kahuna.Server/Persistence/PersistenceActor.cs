
using Nixie;
using Kahuna.Server.Locks;
using Kahuna.Server.KeyValues;

namespace Kahuna.Server.Persistence;

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
            case PersistenceRequestType.StoreLock:
            {
                bool success = await persistence.StoreLock(
                    message.Key,
                    message.Value,
                    message.ExpiresLogical,
                    message.ExpiresCounter,
                    message.Revision,
                    message.State
                );

                if (!success)
                    return PersistenceResponseStatic.FailedResponse;
            }
            break;

            case PersistenceRequestType.StoreKeyValue:
            {
                bool success = await persistence.StoreKeyValue(
                    message.Key,
                    message.Value,
                    message.ExpiresLogical,
                    message.ExpiresCounter,
                    message.Revision,
                    message.State
                );

                if (!success)
                    return PersistenceResponseStatic.FailedResponse;
            } 
            break;

            case PersistenceRequestType.GetLock:
                LockContext? lockContext = await persistence.GetLock(message.Key);
                if (lockContext == null)
                    return new(PersistenceResponseType.NotFound);
                
                return new(PersistenceResponseType.Found);
            
            case PersistenceRequestType.GetKeyValue:
                KeyValueContext? keyValueContext = await persistence.GetKeyValue(message.Key);
                if (keyValueContext == null)
                    return new(PersistenceResponseType.NotFound);
                
                return new(PersistenceResponseType.Found);
        }

        return PersistenceResponseStatic.SuccessResponse;
    }
}