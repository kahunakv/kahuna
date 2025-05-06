
using Nixie;
using Nixie.Routers;

using Kommander;
using Kommander.Time;

using System.Diagnostics;
using Kahuna.Server.Configuration;
using Kahuna.Server.Locks.Data;
using Kahuna.Server.Locks.Logging;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Shared.Locks;

namespace Kahuna.Server.Locks;

/// <summary>
/// Actor to manage lock operations on resources
/// It ensures linearizable and serializable access to the resources on the same bucket
/// </summary>
internal sealed class LockActor : IActor<LockRequest, LockResponse>
{
    private const int ProposalWaitTimeout = 10000;
    
    private static int proposalId = int.MinValue;
    
    /// <summary>
    /// 
    /// </summary>
    private const int CollectThreshold = 500;
    
    private readonly IActorContext<LockActor, LockRequest, LockResponse> actorContext;

    private readonly IActorRef<BackgroundWriterActor, BackgroundWriteRequest> backgroundWriter;

    private readonly IActorRef<BalancingActor<LockProposalActor, LockProposalRequest>, LockProposalRequest> proposalRouter;

    private readonly IPersistenceBackend persistenceBackend;

    private readonly IRaft raft;
    
    private readonly KahunaConfiguration configuration;

    private readonly Dictionary<string, LockEntry> locks = new();
    
    private readonly Dictionary<int, LockProposal> proposals = new();
    
    private readonly HashSet<string> keysToEvict = [];

    private readonly ILogger<IKahuna> logger;
    
    private readonly Stopwatch stopwatch = Stopwatch.StartNew();

    private int operations = CollectThreshold;

    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="actorContext"></param>
    /// <param name="backgroundWriter"></param>
    /// <param name="persistenceBackend"></param>
    /// <param name="raft"></param>
    /// <param name="configuration"></param>
    /// <param name="logger"></param>
    public LockActor(
        IActorContext<LockActor, LockRequest, LockResponse> actorContext,
        IActorRef<BackgroundWriterActor, BackgroundWriteRequest> backgroundWriter,
        IActorRef<BalancingActor<LockProposalActor, LockProposalRequest>, LockProposalRequest> proposalRouter,
        IPersistenceBackend persistenceBackend,
        IRaft raft, 
        KahunaConfiguration configuration,
        ILogger<IKahuna> logger
    )
    {
        this.actorContext = actorContext;
        this.backgroundWriter = backgroundWriter;
        this.proposalRouter = proposalRouter;
        this.persistenceBackend = persistenceBackend;
        this.raft = raft;
        this.configuration = configuration;
        this.logger = logger;
    }

    /// <summary>
    /// Main entry point for the actor.
    /// Receives messages one at a time to prevent concurrency issues
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    public async Task<LockResponse?> Receive(LockRequest message)
    {
        stopwatch.Restart();
        
        try
        {
            logger.LogLocksActorEnter( 
                actorContext.Self.Runner.Name, 
                message.Type, 
                message.Resource, 
                message.Owner?.Length, 
                message.ExpiresMs,
                message.Durability
            );
            
            if (--operations == 0)
            {
                Collect();
                operations = CollectThreshold;
            }

            return message.Type switch
            {
                LockRequestType.TryLock => await TryLock(message),
                LockRequestType.TryUnlock => await TryUnlock(message),
                LockRequestType.TryExtendLock => await TryExtendLock(message),
                LockRequestType.Get => await GetLock(message),
                LockRequestType.CompleteProposal => CompleteProposal(message),
                LockRequestType.ReleaseProposal => ReleaseProposal(message),
                _ => LockStaticResponses.ErroredResponse
            };
        }
        catch (Exception ex)
        {
            logger.LogError("LockActor: Error processing message: {Type} {Message}\n{Stacktrace}", ex.GetType().Name, ex.Message, ex.StackTrace);
        }
        finally
        {
            logger.LogLocksActorTook(                
                actorContext.Self.Runner.Name,
                message.Type,
                message.Resource,
                stopwatch.ElapsedMilliseconds
            );
        }

        return LockStaticResponses.ErroredResponse;
    }

    /// <summary>
    /// Looks for a lock on the resource and tries to lock it
    /// Check for the owner and expiration time
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    private async Task<LockResponse> TryLock(LockRequest message)
    {
        if (!locks.TryGetValue(message.Resource, out LockEntry? entry))
        {
            LockEntry? newEntry = null;

            /// Try to retrieve lock context from persistence
            if (message.Durability == LockDurability.Persistent)
                newEntry = await raft.ReadThreadPool.EnqueueTask(() => persistenceBackend.GetLock(message.Resource));

            newEntry ??= new() { FencingToken = -1 };
            
            entry = newEntry;
            
            locks.Add(message.Resource, newEntry);
        }
        
        if (entry.ReplicationIntent is not null)
            return LockStaticResponses.WaitingForReplication;
        
        HLCTimestamp currentTime = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());
        
        if (entry.Owner is not null)
        {
            bool isExpired = entry.Expires - currentTime < TimeSpan.Zero;

            if (((ReadOnlySpan<byte>)entry.Owner).SequenceEqual(message.Owner) && !isExpired)
                return new(LockResponseType.Locked, entry.FencingToken);

            if (!isExpired)
                return LockStaticResponses.BusyResponse;
        }
        
        LockProposal proposal = new(
            message.Type,
            message.Resource,
            message.Owner,
            entry.FencingToken + 1,
            currentTime + message.ExpiresMs,
            currentTime,
            currentTime,
            LockState.Locked,
            message.Durability
        );

        if (message.Durability == LockDurability.Persistent)
            return CreateProposal(message, entry, proposal, currentTime);
        
        entry.FencingToken = proposal.FencingToken;
        entry.Owner = proposal.Owner;
        entry.Expires = proposal.Expires;
        entry.LastUsed = proposal.LastUsed;
        entry.State = proposal.State;

        return new(LockResponseType.Locked, entry.FencingToken);
    }

    /// <summary>
    /// Looks for a lock on the resource and tries to extend it
    /// If the lock doesn't exist or the owner is different, return an error
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    private async Task<LockResponse> TryExtendLock(LockRequest message)
    {
        LockEntry? entry = await GetLockEntry(message.Resource, message.Durability);
        if (entry is null || entry.State == LockState.Unlocked)
            return LockStaticResponses.DoesNotExistResponse;
        
        if (entry.ReplicationIntent is not null)
            return LockStaticResponses.WaitingForReplication;

        HLCTimestamp currentTime = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());
        
        if (entry.Expires - currentTime < TimeSpan.Zero)
            return LockStaticResponses.DoesNotExistResponse;
        
        if (!((ReadOnlySpan<byte>)entry.Owner).SequenceEqual(message.Owner))
            return LockStaticResponses.InvalidOwnerResponse;

        LockProposal proposal = new(
            message.Type,
            message.Resource,
            entry.Owner,
            entry.FencingToken,
            currentTime + message.ExpiresMs,
            currentTime,
            currentTime,
            entry.State,
            message.Durability
        );

        if (message.Durability == LockDurability.Persistent)
            return CreateProposal(message, entry, proposal, currentTime);
        
        entry.Expires = proposal.Expires;
        entry.LastUsed = proposal.LastUsed;

        return new(LockResponseType.Extended, entry.FencingToken);
    }

    /// <summary>
    /// Looks for a lock on the resource and tries to unlock it
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    private async Task<LockResponse> TryUnlock(LockRequest message)
    {
        LockEntry? entry = await GetLockEntry(message.Resource, message.Durability);
        if (entry is null || entry.State == LockState.Unlocked)
            return LockStaticResponses.DoesNotExistResponse;
        
        if (entry.ReplicationIntent is not null)
            return LockStaticResponses.WaitingForReplication;

        if (!((ReadOnlySpan<byte>)entry.Owner).SequenceEqual(message.Owner))
            return LockStaticResponses.InvalidOwnerResponse;
        
        HLCTimestamp currentTime = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        LockProposal proposal = new(
            message.Type,
            message.Resource,
            null,
            entry.FencingToken,
            entry.Expires,
            currentTime,
            currentTime,
            LockState.Unlocked,
            message.Durability
        );

        if (message.Durability == LockDurability.Persistent)
            return CreateProposal(message, entry, proposal, currentTime);
        
        entry.Owner = proposal.Owner;
        entry.LastUsed = proposal.LastUsed;
        entry.State = proposal.State;

        return LockStaticResponses.UnlockedResponse;
    }

    /// <summary>
    /// Gets Information about an existing lock
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    private async Task<LockResponse> GetLock(LockRequest message)
    {
        LockEntry? entry = await GetLockEntry(message.Resource, message.Durability);
        
        if (entry is null || entry.State == LockState.Unlocked)
            return new(LockResponseType.LockDoesNotExist, new ReadOnlyLockEntry(null, entry?.FencingToken ?? 0, HLCTimestamp.Zero));
        
        if (entry.ReplicationIntent is not null)
            return LockStaticResponses.WaitingForReplication;

        HLCTimestamp currentTime = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        if (entry.Expires - currentTime < TimeSpan.Zero)
            return new(LockResponseType.LockDoesNotExist, new ReadOnlyLockEntry(null, entry.FencingToken, HLCTimestamp.Zero));
        
        entry.LastUsed = currentTime;

        ReadOnlyLockEntry readOnlyLockEntry = new(entry.Owner, entry.FencingToken, entry.Expires);

        return new(LockResponseType.Got, readOnlyLockEntry);
    }

    /// <summary>
    /// Returns an existing lock entry from memory or tries to retrieve it from disk
    /// </summary>
    /// <param name="resource"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    private async ValueTask<LockEntry?> GetLockEntry(string resource, LockDurability durability)
    {
        if (!locks.TryGetValue(resource, out LockEntry? entry))
        {
            if (durability == LockDurability.Persistent)
            {
                entry = await raft.ReadThreadPool.EnqueueTask(() => persistenceBackend.GetLock(resource));
                if (entry is not null)
                {
                    entry.LastUsed = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());
                    locks.Add(resource, entry);
                    return entry;
                }                               
            }
            
            return null;    
        }
        
        return entry;
    }

    private void Collect()
    {
        int count = locks.Count;
        if (count < 200)
            return;
        
        int number = 0;
        TimeSpan range = configuration.CacheEntryTtl;
        HLCTimestamp currentTime = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        foreach (KeyValuePair<string, LockEntry> key in locks)
        {
            if ((currentTime - key.Value.LastUsed) < range)
                continue;
            
            keysToEvict.Add(key.Key);
            number++;
            
            if (number > configuration.CacheEntriesToRemove)
                break;
        }

        foreach (string key in keysToEvict)
            locks.Remove(key);
        
        if (keysToEvict.Count > 0)
            logger.LogDebug("Evicted {Count} key/value pairs", keysToEvict.Count);
        
        keysToEvict.Clear();
    }

    /// <summary>
    /// Creates a proposal for a lock operation and sends it to the proposal actor for replication.
    /// </summary>
    /// <param name="message"></param>
    /// <param name="entry"></param>
    /// <param name="proposal"></param>
    /// <param name="currentTime"></param>
    /// <returns></returns>
    private LockResponse CreateProposal(LockRequest message, LockEntry entry, LockProposal proposal, HLCTimestamp currentTime)
    {
        if (!actorContext.Reply.HasValue)
            return LockStaticResponses.ErroredResponse;
            
        int currentProposalId = Interlocked.Increment(ref proposalId);

        entry.ReplicationIntent = new()
        {
            ProposalId = currentProposalId, 
            Expires = currentTime + ProposalWaitTimeout
        };
            
        proposals.Add(currentProposalId, proposal);
            
        proposalRouter.Send(new(
            message.Type,
            currentProposalId, 
            proposal, 
            actorContext.Self, 
            actorContext.Reply.Value.Promise,
            currentTime
        ));

        actorContext.ByPassReply = true;
            
        return LockStaticResponses.WaitingForReplication;
    }

    /// <summary>
    /// Completes a lock proposal by updating the lock entry with the proposal's state.
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    /// <exception cref="NotImplementedException"></exception>
    private LockResponse CompleteProposal(LockRequest message)
    {
        if (!locks.TryGetValue(message.Resource, out LockEntry? entry))
        {
            logger.LogWarning("LockActor/CompleteProposal: Lock not found for resource {Resource}", message.Resource);
            
            message.Promise?.TrySetResult(LockStaticResponses.ErroredResponse);

            return LockStaticResponses.DoesNotExistResponse;
        }

        if (entry.ReplicationIntent is null)
        {
            logger.LogWarning("LockActor/CompleteProposal: Couldn't find an active write intent on resource {Resource}", message.Resource);
            
            message.Promise?.TrySetResult(LockStaticResponses.ErroredResponse);

            return LockStaticResponses.DoesNotExistResponse;
        }

        if (entry.ReplicationIntent.ProposalId != message.ProposalId)
        {
            logger.LogWarning("LockActor/CompleteProposal: Current write intent on resource {Resource} doesn't match passed id {Current} {Passed}", message.Resource, entry.ReplicationIntent.ProposalId, message.ProposalId);
            
            message.Promise?.TrySetResult(LockStaticResponses.ErroredResponse);

            return LockStaticResponses.DoesNotExistResponse;
        }

        if (!proposals.TryGetValue(message.ProposalId, out LockProposal? proposal))
        {
            logger.LogWarning("LockActor/CompleteProposal: Proposal on resource {Resource} doesn't exist {ProposalId}", message.Resource, message.ProposalId);
            
            message.Promise?.TrySetResult(LockStaticResponses.ErroredResponse);

            return LockStaticResponses.DoesNotExistResponse;
        }

        entry.FencingToken = proposal.FencingToken;
        entry.Owner = proposal.Owner;
        entry.Expires = proposal.Expires;
        entry.LastUsed = proposal.LastUsed;
        entry.State = proposal.State;

        backgroundWriter.Send(new(
            BackgroundWriteType.QueueStoreLock,
            message.PartitionId,
            proposal.Resource,
            proposal.Owner,
            proposal.FencingToken,
            proposal.Expires,
            proposal.LastUsed,
            proposal.LastModified,
            (int)proposal.State
        ));

        entry.ReplicationIntent = null;
        proposals.Remove(message.ProposalId);

        if (message.Promise is null)
            return LockStaticResponses.LockedResponse;

        switch (proposal.Type)
        {
            case LockRequestType.TryLock:
                message.Promise.TrySetResult(new(LockResponseType.Locked, entry.FencingToken));
                break;

            case LockRequestType.TryExtendLock:
                message.Promise.TrySetResult(new(LockResponseType.Extended, entry.FencingToken));
                break;

            case LockRequestType.TryUnlock:
                message.Promise.TrySetResult(LockStaticResponses.UnlockedResponse);
                break;

            case LockRequestType.Get:
            case LockRequestType.CompleteProposal:
            case LockRequestType.ReleaseProposal:
            default:
                throw new NotImplementedException();
        }
        
        return LockStaticResponses.ErroredResponse;
    }
    
    /// <summary>
    /// Releases a failed lock proposal by removing the replication intent from the lock entry and the proposal list.
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>    
    private LockResponse ReleaseProposal(LockRequest message)
    {
        if (!locks.TryGetValue(message.Resource, out LockEntry? entry))
        {
            logger.LogWarning("LockActor/ReleaseProposal: Lock not found for resource {Resource}", message.Resource);
            
            message.Promise?.TrySetResult(LockStaticResponses.ErroredResponse);

            return LockStaticResponses.DoesNotExistResponse;
        }

        if (entry.ReplicationIntent is null)
        {
            logger.LogWarning("LockActor/ReleaseProposal: Couldn't find an active write intent on resource {Resource}", message.Resource);
            
            message.Promise?.TrySetResult(LockStaticResponses.ErroredResponse);

            return LockStaticResponses.DoesNotExistResponse;
        }

        if (entry.ReplicationIntent.ProposalId != message.ProposalId)
        {
            logger.LogWarning("LockActor/ReleaseProposal: Current write intent on resource {Resource} doesn't match passed id {Current} {Passed}", message.Resource, entry.ReplicationIntent.ProposalId, message.ProposalId);
            
            message.Promise?.TrySetResult(LockStaticResponses.ErroredResponse);

            return LockStaticResponses.DoesNotExistResponse;
        }

        if (!proposals.ContainsKey(message.ProposalId))
        {
            logger.LogWarning("LockActor/ReleaseProposal: Proposal on resource {Resource} doesn't exist {ProposalId}", message.Resource, message.ProposalId);

            message.Promise?.TrySetResult(LockStaticResponses.ErroredResponse);

            return LockStaticResponses.DoesNotExistResponse;
        }        

        entry.ReplicationIntent = null;
        proposals.Remove(message.ProposalId);

        if (message.Promise is null)
            return LockStaticResponses.LockedResponse;
        
        message.Promise.TrySetResult(LockStaticResponses.ErroredResponse);
                
        return LockStaticResponses.ErroredResponse;
    }
}