
using Nixie;
using Nixie.Routers;

using Kommander;
using Kommander.Time;
using Kommander.WAL.IO;

using System.Diagnostics;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues.Ranges;
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

    private readonly UnflushedLockWritesIndex? unflushedLockWrites;

    private readonly IRaft raft;

    private readonly IRaftReadScheduler backendReadScheduler;

    private readonly DataPartitionRouter dataPartitionRouter;

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
        IRaftReadScheduler backendReadScheduler,
        KahunaConfiguration configuration,
        ILogger<IKahuna> logger
    )
    {
        this.actorContext = actorContext;
        this.backgroundWriter = backgroundWriter;
        this.proposalRouter = proposalRouter;
        this.persistenceBackend = persistenceBackend;
        // Overlay of committed-but-unflushed lock mutations, carried by the decorated backend; the
        // commit path records into it so reads observe the mutation before the flush lands. Null for
        // ephemeral actors (no backend) and for tests constructed over a raw backend.
        this.unflushedLockWrites = (persistenceBackend as UnflushedOverlayPersistenceBackend)?.UnflushedLockWrites;
        this.raft = raft;
        this.backendReadScheduler = backendReadScheduler;
        this.dataPartitionRouter = new DataPartitionRouter(raft);
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
                LockRequestType.InvalidateOrApply => InvalidateOrApply(message),
                LockRequestType.EvictPartition => EvictPartition(message),
                _ => LockStaticResponses.ErroredResponse
            };
        }
        catch (ReadBackpressureExceededException ex)
        {
            // The backend read scheduler's per-partition queue is at its depth limit while loading a
            // persistent lock from disk. Transient: surface MustRetry so the caller retries rather than
            // faulting the actor. A stop-time InvalidOperationException falls through to the generic handler,
            // which faults the request deterministically instead of hanging its awaiter.
            logger.LogWarning("LockActor: backend read scheduler rejected {Type} for '{Resource}' (partition {Partition} depth {Depth}); returning MustRetry.",
                message.Type, message.Resource, ex.PartitionId, ex.CurrentDepth);
            return LockStaticResponses.MustRetryResponse;
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
                newEntry = await backendReadScheduler.EnqueueTask(message.PartitionId, () => persistenceBackend.GetLock(message.Resource));

            newEntry ??= new() { FencingToken = -1 };
            
            entry = newEntry;
            
            locks.Add(message.Resource, newEntry);
        }
        
        if (entry.ReplicationIntent is not null)
            return LockStaticResponses.WaitingForReplication;
        
        HLCTimestamp currentTime = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        // Gate the busy check on the state, not just the owner: an unlocked entry loaded from the
        // backend or the unflushed overlay retains the releasing holder's owner bytes and its
        // original (possibly unexpired) lease, but the lock is free — treating it as held would
        // refuse grants until that stale lease ran out.
        if (entry.State == LockState.Locked && entry.Owner is not null)
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
        entry.LastModified = proposal.LastModified;
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
        entry.LastModified = proposal.LastModified;

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
        entry.LastModified = proposal.LastModified;
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
                entry = await backendReadScheduler.EnqueueTask(dataPartitionRouter.Locate(resource), () => persistenceBackend.GetLock(resource));
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
            logger.LogLocksActorEviction(keysToEvict.Count);
        
        keysToEvict.Clear();
    }

    /// <summary>
    /// Applies a committed lock mutation delivered by the replication/restore path to a resident
    /// cache entry. Committed mutations from other leaders reach this node only through that path,
    /// so without this apply the in-memory entry of a former leader stays frozen at its last tenure
    /// and a re-promotion would mint fencing tokens from that stale state — replaying token values
    /// that were already granted. A non-resident resource is a no-op: a cold load reads the flushed
    /// backend plus the unflushed overlay, which the replicator records into before sending this
    /// message, so it always observes the mutation.
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    private LockResponse InvalidateOrApply(LockRequest message)
    {
        LockInvalidateOrApplyData data = message.InvalidateOrApplyData!;

        if (!locks.TryGetValue(message.Resource, out LockEntry? entry))
            return LockStaticResponses.DoesNotExistResponse;

        // A live replication intent means this actor owns an in-flight proposal for the resource:
        // CompleteProposal installs the committed values (or ReleaseProposal drops the entry)
        // exactly once, so this apply must not race it. An expired intent is abandoned state and
        // no longer owns the entry.
        if (entry.ReplicationIntent is not null)
        {
            HLCTimestamp currentTime = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

            if (entry.ReplicationIntent.Expires - currentTime > TimeSpan.Zero)
                return LockStaticResponses.WaitingForReplication;

            entry.ReplicationIntent = null;
        }

        // Applies can race the leader's own CompleteProposal (they arrive through different
        // senders), so only ever advance the entry: an older token — or the same token with an
        // older modification stamp (extend and unlock reuse the token) — is a stale or duplicate
        // delivery and must not regress the cache.
        if (entry.FencingToken > data.FencingToken
            || (entry.FencingToken == data.FencingToken && entry.LastModified >= data.LastModified))
            return LockStaticResponses.LockedResponse;

        entry.Owner = message.Owner;
        entry.FencingToken = data.FencingToken;
        entry.Expires = data.Expires;
        entry.LastUsed = data.LastUsed;
        entry.LastModified = data.LastModified;
        entry.State = data.State;

        return LockStaticResponses.LockedResponse;
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
            actorContext.Reply.Value.Promise!,
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

        // LastModified must be installed along with the committed values: the InvalidateOrApply
        // advance guard orders same-token mutations by it, so a stale stamp here would let the
        // replicator's late delivery of an older mutation (e.g. this entry's own grant) overwrite
        // a newer one (e.g. its release) and resurrect a held lock.
        entry.FencingToken = proposal.FencingToken;
        entry.Owner = proposal.Owner;
        entry.Expires = proposal.Expires;
        entry.LastUsed = proposal.LastUsed;
        entry.LastModified = proposal.LastModified;
        entry.State = proposal.State;

        // Record before enqueueing so a read that misses the actor table (e.g. on a later promoted
        // leader) observes this committed mutation even before the background flush lands it.
        unflushedLockWrites?.Record(proposal.Resource, proposal.Owner, proposal.FencingToken,
            proposal.Expires, proposal.LastUsed, proposal.LastModified, proposal.State);

        backgroundWriter.Send(BackgroundWriteRequestPool.Rent(
            BackgroundWriteType.QueueStoreLock,
            message.PartitionId,
            proposal.Resource,
            proposal.Owner,
            proposal.FencingToken,
            proposal.Expires,
            proposal.LastUsed,
            proposal.LastModified,
            (int)proposal.State,
            logIndex: message.ProposalLogIndex
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
    /// Removes every resident lock entry owned by the partition carried in the request: after the
    /// committed placement map stopped listing this node as one of its replicas, or after a
    /// whole-partition snapshot install replaced the partition's backend rows. The persistent
    /// lock rows are purged/installed in the backend separately; this drops the actor-resident
    /// copies — ephemeral locks included — so the node can never answer from a stale resident
    /// lease (whose fencing token could sit below the freshly seeded backend row, regressing the
    /// fencing contract). Lock resources route purely by key-space hash, so the classification is
    /// the same hash the locator applies.
    /// </summary>
    private LockResponse EvictPartition(LockRequest message)
    {
        List<string>? toEvict = null;

        foreach (string resource in locks.Keys)
        {
            if (dataPartitionRouter.Locate(resource) == message.PartitionId)
                (toEvict ??= []).Add(resource);
        }

        if (toEvict is not null)
        {
            foreach (string resource in toEvict)
                locks.Remove(resource);
        }

        return new(LockResponseType.Unlocked, toEvict?.Count ?? 0);
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

        // The proposal failed or its outcome is unknown (e.g. leadership was lost mid-replication),
        // so the cached entry can no longer be trusted as the latest committed state — the entry it
        // was minted from may already be behind another leader's committed grants. Drop it: the
        // next access cold-loads through the flushed backend plus the unflushed overlay, which
        // reflect every committed mutation this node has applied.
        locks.Remove(message.Resource);

        if (message.Promise is null)
            return LockStaticResponses.LockedResponse;

        message.Promise.TrySetResult(LockStaticResponses.ErroredResponse);

        return LockStaticResponses.ErroredResponse;
    }
}
