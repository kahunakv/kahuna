
using Nixie;

namespace Kahuna;

public sealed class LockActor : IActorStruct<LockRequest, LockResponse>
{
    private string? owner;
    
    private DateTime expires;

    public LockActor(IActorContextStruct<LockActor, LockRequest, LockResponse> _)
    {
        
    }

    public async Task<LockResponse> Receive(LockRequest message)
    {
        Console.WriteLine("Message: {0} {1} {2}", message.Type, message.Owner, message.ExpiresMs);
        
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
        {
            if (expires - DateTime.UtcNow > TimeSpan.Zero)
                return new(LockResponseType.Busy);
        }

        owner = message.Owner;
        expires = DateTime.UtcNow.AddMilliseconds(message.ExpiresMs);

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