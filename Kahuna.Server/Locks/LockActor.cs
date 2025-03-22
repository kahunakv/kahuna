
using Nixie;
using Kommander;
using Kommander.Time;
using Google.Protobuf;
using Kahuna.Server.Persistence;
using Kahuna.Server.Replication;
using Kahuna.Server.Replication.Protos;
using Kahuna.Shared.Locks;

namespace Kahuna.Server.Locks;

/// <summary>
/// Actor to manage lock operations on resources
/// It ensures linearizable and serializable access to the resources on the same bucket
/// </summary>
public sealed class LockActor : IActorStruct<LockRequest, LockResponse>
{
    private readonly IActorContextStruct<LockActor, LockRequest, LockResponse> actorContext;

    private readonly IActorRef<BackgroundWriterActor, BackgroundWriteRequest> backgroundWriter;

    private readonly IPersistence persistence;

    private readonly IRaft raft;

    private readonly Dictionary<string, LockContext> locks = new();
    
    private readonly HashSet<string> keysToEvict = [];

    private readonly ILogger<IKahuna> logger;

    private uint operations;

    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="actorContext"></param>
    /// <param name="raft"></param>
    /// <param name="logger"></param>
    public LockActor(
        IActorContextStruct<LockActor, LockRequest, LockResponse> actorContext,
        IActorRef<BackgroundWriterActor, BackgroundWriteRequest> backgroundWriter,
        IPersistence persistence,
        IRaft raft, 
        ILogger<IKahuna> logger
    )
    {
        this.actorContext = actorContext;
        this.backgroundWriter = backgroundWriter;
        this.persistence = persistence;
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
            logger.LogDebug(
                "LockActor Message: {Actor} {Type} {Resource} {Owner} {ExpiresMs} {Durability}", 
                actorContext.Self.Runner.Name, 
                message.Type, 
                message.Resource, 
                message.Owner?.Length, 
                message.ExpiresMs,
                message.Durability
            );

            if ((operations++) % 1000 == 0)
                await Collect();

            return message.Type switch
            {
                LockRequestType.TryLock => await TryLock(message),
                LockRequestType.TryUnlock => await TryUnlock(message),
                LockRequestType.TryExtendLock => await TryExtendLock(message),
                LockRequestType.Get => await GetLock(message),
                _ => new(LockResponseType.Errored)
            };
        }
        catch (Exception ex)
        {
            logger.LogError("LockActor: Error processing message: {Type} {Message}\n{Stacktrace}", ex.GetType().Name, ex.Message, ex.StackTrace);
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

        if (!locks.TryGetValue(message.Resource, out LockContext? context))
        {
            LockContext? newContext = null;

            /// Try to retrieve lock context from persistence
            if (message.Durability == LockDurability.Persistent)
                newContext = await persistence.GetLock(message.Resource);

            newContext ??= new() { FencingToken = -1 };
            
            context = newContext;
            
            locks.Add(message.Resource, newContext);
        }
        
        if (context.Owner is not null)
        {
            bool isExpired = context.Expires - currentTime < TimeSpan.Zero;

            if (((ReadOnlySpan<byte>)context.Owner).SequenceEqual(message.Owner) && !isExpired)
                return new(LockResponseType.Locked, context.FencingToken);

            if (!isExpired)
                return new(LockResponseType.Busy);
        }
        
        LockProposal proposal = new(
            message.Resource,
            message.Owner,
            context.FencingToken + 1,
            currentTime + message.ExpiresMs,
            currentTime,
            LockState.Locked
        );

        if (message.Durability == LockDurability.Persistent)
        {
            bool success = await PersistAndReplicateLockMessage(message.Type, proposal, currentTime);
            if (!success)
                return new(LockResponseType.Errored);
        }
        
        context.FencingToken = proposal.FencingToken;
        context.Owner = proposal.Owner;
        context.Expires = proposal.Expires;
        context.LastUsed = proposal.LastUsed;
        context.State = proposal.State;

        return new(LockResponseType.Locked, context.FencingToken);
    }

    /// <summary>
    /// Looks for a lock on the resource and tries to extend it
    /// If the lock doesn't exist or the owner is different, return an error
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    private async Task<LockResponse> TryExtendLock(LockRequest message)
    {
        LockContext? context = await GetLockContext(message.Resource, message.Durability);
        if (context is null || context.State == LockState.Unlocked)
            return new(LockResponseType.LockDoesNotExist);

        if (!((ReadOnlySpan<byte>)context.Owner).SequenceEqual(message.Owner))
            return new(LockResponseType.InvalidOwner);

        HLCTimestamp currentTime = await raft.HybridLogicalClock.SendOrLocalEvent();

        LockProposal proposal = new(
            message.Resource,
            context.Owner,
            context.FencingToken,
            currentTime + message.ExpiresMs,
            currentTime,
            context.State
        );

        if (message.Durability == LockDurability.Persistent)
        {
            bool success = await PersistAndReplicateLockMessage(message.Type, proposal, currentTime);
            if (!success)
                return new(LockResponseType.Errored);
        }
        
        context.Expires = proposal.Expires;
        context.LastUsed = proposal.LastUsed;

        return new(LockResponseType.Extended);
    }

    /// <summary>
    /// Looks for a lock on the resource and tries to unlock it
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    private async Task<LockResponse> TryUnlock(LockRequest message)
    {
        LockContext? context = await GetLockContext(message.Resource, message.Durability);
        if (context is null || context.State == LockState.Unlocked)
            return new(LockResponseType.LockDoesNotExist);

        if (!((ReadOnlySpan<byte>)context.Owner).SequenceEqual(message.Owner))
            return new(LockResponseType.InvalidOwner);
        
        HLCTimestamp currentTime = await raft.HybridLogicalClock.SendOrLocalEvent();

        LockProposal proposal = new(
            message.Resource,
            null,
            context.FencingToken,
            context.Expires,
            currentTime,
            LockState.Unlocked
        );

        if (message.Durability == LockDurability.Persistent)
        {
            bool success = await PersistAndReplicateLockMessage(message.Type, proposal, currentTime);
            if (!success)
                return new(LockResponseType.Errored);
        }
        
        context.Owner = proposal.Owner;
        context.LastUsed = proposal.LastUsed;
        context.State = proposal.State;

        return new(LockResponseType.Unlocked);
    }

    /// <summary>
    /// Gets Information about an existing lock
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    private async Task<LockResponse> GetLock(LockRequest message)
    {
        LockContext? context = await GetLockContext(message.Resource, message.Durability);
        if (context is null || context.State == LockState.Unlocked)
            return new(LockResponseType.LockDoesNotExist, new ReadOnlyLockContext(null, context?.FencingToken ?? 0, HLCTimestamp.Zero));

        HLCTimestamp currentTime = await raft.HybridLogicalClock.SendOrLocalEvent();

        if (context.Expires - currentTime < TimeSpan.Zero)
            return new(LockResponseType.LockDoesNotExist, new ReadOnlyLockContext(null, context?.FencingToken ?? 0, HLCTimestamp.Zero));
        
        context.LastUsed = currentTime;

        ReadOnlyLockContext readOnlyLockContext = new(context.Owner, context.FencingToken, context.Expires);

        return new(LockResponseType.Got, readOnlyLockContext);
    }

    /// <summary>
    /// Returns an existing lock context from memory or tries to retrieve it from disk
    /// </summary>
    /// <param name="resource"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    private async ValueTask<LockContext?> GetLockContext(string resource, LockDurability? durability)
    {
        if (!locks.TryGetValue(resource, out LockContext? context))
        {
            if (durability == LockDurability.Persistent)
            {
                context = await persistence.GetLock(resource);
                if (context is not null)
                {
                    context.LastUsed = await raft.HybridLogicalClock.SendOrLocalEvent();
                    locks.Add(resource, context);
                    return context;
                }
                
                return null;
            }
            
            return null;    
        }
        
        return context;
    }

    /// <summary>
    /// Persists and replicates lock message to the Raft cluster
    /// </summary>
    /// <param name="type"></param>
    /// <param name="proposal"></param>
    /// <param name="currentTime"></param>
    /// <returns></returns>
    private async Task<bool> PersistAndReplicateLockMessage(LockRequestType type, LockProposal proposal ,HLCTimestamp currentTime)
    {
        if (!raft.Joined)
            return true;

        int partitionId = raft.GetPartitionKey(proposal.Resource);

        LockMessage lockMessage = new()
        {
            Type = (int)type,
            Resource = proposal.Resource,
            FencingToken = proposal.FencingToken,
            ExpireLogical = proposal.Expires.L,
            ExpireCounter = proposal.Expires.C,
            TimeLogical = currentTime.L,
            TimeCounter = currentTime.C,
            Consistency = (int)LockDurability.ReplicationConsistent
        };

        if (proposal.Owner is not null)
            lockMessage.Owner = UnsafeByteOperations.UnsafeWrap(proposal.Owner);

        RaftReplicationResult result = await raft.ReplicateLogs(
            partitionId,
            ReplicationTypes.Locks,
            ReplicationSerializer.Serialize(lockMessage)
        );

        if (!result.Success)
        {
            logger.LogWarning("Failed to replicate lock {Resource} Partition={Partition} Status={Status}", proposal.Resource, partitionId, result.Status);
            
            return false;
        }

        backgroundWriter.Send(new(
            BackgroundWriteType.QueueStoreLock,
            partitionId,
            proposal.Resource, 
            proposal.Owner, 
            proposal.FencingToken,
            proposal.Expires,
            (int)proposal.State
        ));

        return result.Success;
    }

    private async ValueTask Collect()
    {
        if (locks.Count < 2000)
            return;
        
        int number = 0;
        TimeSpan range = TimeSpan.FromMinutes(30);
        HLCTimestamp currentTime = await raft.HybridLogicalClock.SendOrLocalEvent();

        foreach (KeyValuePair<string, LockContext> key in locks)
        {
            if ((currentTime - key.Value.LastUsed) < range)
                continue;
            
            keysToEvict.Add(key.Key);
            number++;
            
            if (number > 100)
                break;
        }

        foreach (string key in keysToEvict)
        {
            locks.Remove(key);
            
            Console.WriteLine("Removed {0}", key);
        }
        
        keysToEvict.Clear();
    }
}