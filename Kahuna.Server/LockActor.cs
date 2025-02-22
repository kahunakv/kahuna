
using Nixie;

namespace Kahuna;

public sealed class LockActor : IActorStruct<LockRequest, LockResponse>
{
    private string? owner;
    
    private DateTime expires;

    public LockActor(IActorContextStruct<LockActor, LockRequest, LockResponse> _)
    {
        
    }

    /// <summary>
    /// Main entry point for the actor.
    /// Receives messages one at a time to prevent concurrency issues
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    public async Task<LockResponse> Receive(LockRequest message)
    {
        Console.WriteLine("Message: {0} {1} {2}", message.Type, message.Owner, message.ExpiresMs);
        
        await Task.CompletedTask;

        return message.Type switch
        {
            LockRequestType.TryLock => TryLock(message),
            LockRequestType.TryUnlock => TryUnlock(message),
            LockRequestType.TryExtendLock => TryExtendLock(message),
            _ => new(LockResponseType.Errored)
        };
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
    
    private LockResponse TryExtendLock(LockRequest message)
    {
        if (string.IsNullOrEmpty(owner))
            return new(LockResponseType.Errored);
        
        if (owner != message.Owner)
            return new(LockResponseType.Errored);
        
        expires = DateTime.UtcNow.AddMilliseconds(message.ExpiresMs);

        return new(LockResponseType.Extended);
    }
    
    private LockResponse TryUnlock(LockRequest message)
    {
        if (string.IsNullOrEmpty(owner))
            return new(LockResponseType.Errored);

        if (message.Owner != owner)
            return new(LockResponseType.Errored);
        
        owner = null;

        return new(LockResponseType.Unlocked);
    }
}