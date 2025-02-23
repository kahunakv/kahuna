
using Nixie;

namespace Kahuna;

/// <summary>
/// 
/// </summary>
public sealed class LockActor : IActorStruct<LockRequest, LockResponse>
{
    private readonly Dictionary<string, LockContext> locks = new();

    private readonly ILogger<IKahuna> logger;

    public LockActor(IActorContextStruct<LockActor, LockRequest, LockResponse> _, ILogger<IKahuna> logger)
    {
        this.logger = logger;
    }

    /// <summary>
    /// Main entry point for the actor.
    /// Receives messages one at a time to prevent concurrency issues
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    public async Task<LockResponse> Receive(LockRequest message)
    {
        logger.LogInformation("Message: {Type} {Resource} {Owner} {ExpiresMs}", message.Type, message.Resource, message.Owner, message.ExpiresMs);
        
        await Task.CompletedTask;

        return message.Type switch
        {
            LockRequestType.TryLock => TryLock(message),
            LockRequestType.TryUnlock => TryUnlock(message),
            LockRequestType.TryExtendLock => TryExtendLock(message),
            LockRequestType.Get => GetLock(message),
            _ => new(LockResponseType.Errored)
        };
    }
    
    /// <summary>
    /// Looks for a lock on the resource and tries to lock it
    /// Check for the owner and expiration time
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    private LockResponse TryLock(LockRequest message)
    {
        if (locks.TryGetValue(message.Resource, out LockContext? context))
        {
            if (!string.IsNullOrEmpty(context.Owner))
            {
                if (context.Expires - DateTime.UtcNow > TimeSpan.Zero)
                    return new(LockResponseType.Busy);
            }
            
            context.Owner = message.Owner;
            context.Expires = DateTime.UtcNow.AddMilliseconds(message.ExpiresMs);

            return new(LockResponseType.Locked, context.FencingToken++);
        }
        
        LockContext newContext = new()
        {
            Owner = message.Owner,
            Expires = DateTime.UtcNow.AddMilliseconds(message.ExpiresMs)
        };
        
        locks.Add(message.Resource, newContext);
        
        return new(LockResponseType.Locked);
    }
    
    /// <summary>
    /// Looks for a lock on the resource and tries to extend it
    /// If the lock doesn't exist or the owner is different, return an error
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    private LockResponse TryExtendLock(LockRequest message)
    {
        if (!locks.TryGetValue(message.Resource, out LockContext? context))
            return new(LockResponseType.Errored);
        
        if (context.Owner != message.Owner)
            return new(LockResponseType.Errored);
        
        context.Expires = DateTime.UtcNow.AddMilliseconds(message.ExpiresMs);

        return new(LockResponseType.Extended);
    }
    
    /// <summary>
    /// Looks for a lock on the resource and tries to unlock it
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    private LockResponse TryUnlock(LockRequest message)
    {
        if (!locks.TryGetValue(message.Resource, out LockContext? context))
            return new(LockResponseType.Errored);

        if (message.Owner != context.Owner)
            return new(LockResponseType.Errored);
        
        context.Owner = null;

        return new(LockResponseType.Unlocked);
    }
    
    /// <summary>
    /// Gets Information about an existing lock
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    private LockResponse GetLock(LockRequest message)
    {
        if (!locks.TryGetValue(message.Resource, out LockContext? context))
            return new(LockResponseType.Errored);
        
        if (context.Expires - DateTime.UtcNow > TimeSpan.Zero)
            return new(LockResponseType.Busy);

        ReadOnlyLockContext readOnlyLockContext = new(context.Owner, context.Expires, context.FencingToken);

        return new(LockResponseType.Got, readOnlyLockContext);
    }
}