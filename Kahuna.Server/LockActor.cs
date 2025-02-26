
using Kahuna.Server.Protos;
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
    private readonly IActorContextStruct<LockActor, LockRequest, LockResponse> actorContext;
    
    private readonly IRaft raft;
    
    private readonly Dictionary<string, LockContext> locks = new();

    private readonly ILogger<IKahuna> logger;

    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="actorContext"></param>
    /// <param name="raft"></param>
    /// <param name="logger"></param>
    public LockActor(IActorContextStruct<LockActor, LockRequest, LockResponse> actorContext, IRaft raft, ILogger<IKahuna> logger)
    {
        this.actorContext = actorContext;
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
            logger.LogInformation("Message: {Actor} {Type} {Resource} {Owner} {ExpiresMs}", actorContext.Self.Runner.Name, message.Type, message.Resource, message.Owner, message.ExpiresMs);

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
        HLCTimestamp currentTime = await raft.HybridLogicalClock.SendOrLocalEvent();
        
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

            long fencingToken = context.FencingToken++;
            context.Owner = message.Owner;
            context.Expires = currentTime + message.ExpiresMs;
            
            if (message.Consistency == LockConsistency.Consistent)
            {
                bool success = await ReplicateMessage(message, currentTime);
                if (!success)
                    return new(LockResponseType.Errored);
            }

            return new(LockResponseType.Locked, fencingToken);
        }
        
        if (message.Consistency == LockConsistency.Consistent)
        {
            bool success = await ReplicateMessage(message, currentTime);
            if (!success)
                return new(LockResponseType.Errored);
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
        
        HLCTimestamp currentTime = await raft.HybridLogicalClock.SendOrLocalEvent();
        
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
        
        HLCTimestamp currentTime = await raft.HybridLogicalClock.SendOrLocalEvent();
        
        if (context.Expires - currentTime < TimeSpan.Zero)
            return new(LockResponseType.Busy);

        ReadOnlyLockContext readOnlyLockContext = new(context.Owner, context.Expires, context.FencingToken);

        return new(LockResponseType.Got, readOnlyLockContext);
    }
    
    private async Task<bool> ReplicateMessage(LockRequest message, HLCTimestamp currentTime)
    {
        if (!raft.Joined)
            return true;
        
        int partitionId = (int)raft.GetPartitionKey(message.Resource);
                
        (bool success, long _) = await raft.ReplicateLogs(
            partitionId, 
            ReplicationSerializer.Serialize(new LockMessage()
            {
                Type = (int)message.Type,
                Resource = message.Resource, 
                Owner = message.Owner,
                Expires = message.ExpiresMs,
                TimeLogical = currentTime.L,
                TimeCounter = currentTime.C,
                Consistency = (int)LockConsistency.ReplicationConsistent
            })
        );

        return success;
    }
}