
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
                bool success = await persistence.StoreLocks(message.Items);
                if (!success)
                    return PersistenceResponseStatic.FailedResponse;
            }
            break;

            case PersistenceRequestType.StoreKeyValue:
            {
                bool success = await persistence.StoreKeyValues(message.Items);
                if (!success)
                    return PersistenceResponseStatic.FailedResponse;
            } 
            break;
            
            default:
                throw new ArgumentOutOfRangeException();
        }

        return PersistenceResponseStatic.SuccessResponse;
    }
}