
using Kommander;
using Kommander.Time;
using Nixie;

namespace Kahuna;

/// <summary>
/// Actor to manage lock operations on resources
/// It ensures linearizable and serializable access to the resources on the same bucket
/// </summary>
public sealed class LockActor : IActorStruct<LockRequest, LockResponse>
{
    private readonly IRaft raft;
    
    private readonly Dictionary<string, LockContext> locks = new();

    private readonly ILogger<IKahuna> logger;

    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="_"></param>
    /// <param name="raft"></param>
    /// <param name="logger"></param>
    public LockActor(IActorContextStruct<LockActor, LockRequest, LockResponse> _, IRaft raft, ILogger<IKahuna> logger)
    {
        this.raft = raft;
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
        try
        {
            logger.LogInformation("Message: {Type} {Resource} {Owner} {ExpiresMs}", message.Type, message.Resource, message.Owner, message.ExpiresMs);

            return message.Type switch
            {
                LockRequestType.TryLock => await TryLock(message),
                LockRequestType.TryUnlock => TryUnlock(message),
                LockRequestType.TryExtendLock => await TryExtendLock(message),
                LockRequestType.Get => await GetLock(message),
                _ => new(LockResponseType.Errored)
            };
        }
        catch (Exception ex)
        {
            logger.LogError("Error processing message: {Message}", ex.Message);
        }
        
        return new(LockResponseType.Errored);
    }
    
    /// <summary>
    /// Looks for a lock on the resource and tries to lock it
    /// Check for the owner and expiration time
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    private async Task<LockResponse> TryLock(LockRequest message)
    {
        HLCTimestamp currentTime = await ((RaftManager)raft).HybridLogicalClock.SendOrLocalEvent();
        
        if (locks.TryGetValue(message.Resource, out LockContext? context))
        {
            if (!string.IsNullOrEmpty(context.Owner))
            {
                bool isExpired = context.Expires - currentTime > TimeSpan.Zero;
                
                if (context.Owner == message.Owner && !isExpired)
                    return new(LockResponseType.Locked, context.FencingToken);
                
                if (isExpired)
                    return new(LockResponseType.Busy);
            }
            
            if (message.Consistency == LockConsistency.Consistent)
            {
                int partition = Math.Abs(message.Resource.GetHashCode()) % 3;
                await raft.ReplicateLogs(partition, Array.Empty<byte>());
            }
            
            context.Owner = message.Owner;
            context.Expires = currentTime + message.ExpiresMs;

            return new(LockResponseType.Locked, context.FencingToken++);
        }
        
        if (message.Consistency == LockConsistency.Consistent)
        {
            int partition = Math.Abs(message.Resource.GetHashCode()) % 3;
            await raft.ReplicateLogs(partition, Array.Empty<byte>());
        }
        
        LockContext newContext = new()
        {
            Owner = message.Owner,
            Expires = currentTime + message.ExpiresMs
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
    private async Task<LockResponse> TryExtendLock(LockRequest message)
    {
        if (!locks.TryGetValue(message.Resource, out LockContext? context))
            return new(LockResponseType.Errored);
        
        if (context.Owner != message.Owner)
            return new(LockResponseType.Errored);
        
        HLCTimestamp currentTime = await ((RaftManager)raft).HybridLogicalClock.SendOrLocalEvent();
        
        context.Expires = currentTime + message.ExpiresMs;

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
    private async Task<LockResponse> GetLock(LockRequest message)
    {
        if (!locks.TryGetValue(message.Resource, out LockContext? context))
            return new(LockResponseType.Errored);
        
        HLCTimestamp currentTime = await ((RaftManager)raft).HybridLogicalClock.SendOrLocalEvent();
        
        if (context.Expires - currentTime < TimeSpan.Zero)
            return new(LockResponseType.Busy);

        ReadOnlyLockContext readOnlyLockContext = new(context.Owner, context.Expires, context.FencingToken);

        return new(LockResponseType.Got, readOnlyLockContext);
    }
}