
using Nixie;

namespace Kahuna;

public sealed class LockActor : IActor<LockRequest, LockResponse>
{
    private string? owner;
    
    public LockActor(IActorContext<LockActor, LockRequest, LockResponse> _)
    {
        
    }

    public async Task<LockResponse?> Receive(LockRequest message)
    {
        //Console.WriteLine("Message: {0}", message.Type);
        
        await Task.CompletedTask;
        
        switch (message.Type)
        {
            case LockRequestType.TryLock:
                return TryLock(message);
            
            case LockRequestType.TryUnlock:
                return TryUnlock(message);
        }

        return new(LockResponseType.Errored);
    }
    
    private LockResponse TryLock(LockRequest message)
    {
        if (!string.IsNullOrEmpty(owner))
            return new(LockResponseType.Busy);

        owner = message.Owner;

        return new(LockResponseType.Locked);
    }
    
    private LockResponse TryUnlock(LockRequest message)
    {
        if (string.IsNullOrEmpty(owner))
            return new(LockResponseType.Errored);

        owner = null;

        return new(LockResponseType.Unlocked);
    }
}