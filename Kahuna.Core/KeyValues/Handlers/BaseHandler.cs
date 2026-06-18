
using Google.Protobuf;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Server.Replication;
using Kahuna.Server.Replication.Protos;
using Kahuna.Shared.KeyValue;
using Kahuna.Utils;
using Kommander;
using Kommander.Time;
using Nixie;

namespace Kahuna.Server.KeyValues.Handlers;

/// <summary>
/// Base class for handling key/value operations.
/// </summary>
internal abstract class BaseHandler
{
    private const int ProposalWaitTimeout = 10000;
    
    private static int proposalId;
    
    /// <summary>
    /// Represents the background writer actor reference.
    /// </summary>
    protected readonly KeyValueContext context;
    
    private readonly HashSet<long> revisionsToRemove = [];
    
    protected BaseHandler(KeyValueContext context)
    {
        this.context = context;
    }

    /// <summary>
    /// Resolves <paramref name="key"/> to its owning partition id. Key-range spaces look up the
    /// live descriptor; hash spaces use <see cref="DataPartitionRouter"/> over the user partitions
    /// <c>[1, InitialPartitions]</c>. Both routing call sites (locator and handlers) must call
    /// <see cref="RangeRouting.Locate"/> so they cannot drift.
    /// </summary>
    protected int ResolvePartition(string key)
    {
        (int partitionId, _) = RangeRouting.Locate(
            context.KeySpaceRegistry,
            context.RangeMapStore.Current,
            new DataPartitionRouter(context.Raft),
            key);
        return partitionId;
    }
    
    /// <summary>
    /// Creates a proposal for a key/value operation and sends it to the proposal actor for replication.
    /// </summary>
    /// <param name="message"></param>
    /// <param name="entry"></param>
    /// <param name="proposal"></param>
    /// <param name="currentTime"></param>
    /// <returns></returns>
    protected KeyValueResponse CreateProposal(KeyValueRequest message, KeyValueEntry entry, KeyValueProposal proposal, HLCTimestamp currentTime)
    {
        IActorContext<KeyValueActor, KeyValueRequest, KeyValueResponse> actorContext = context.ActorContext;
        
        if (!actorContext.Reply.HasValue)
            return KeyValueStaticResponses.ErroredResponse;
            
        int currentProposalId = Interlocked.Increment(ref proposalId);

        // Carry the key-range routing generation from the request into the proposal so the proposal
        // actor's generation fence can reject a stale-routed write. 0 for hash spaces.
        proposal.RoutedGeneration = message.RoutedGeneration;

        entry.ReplicationIntent = new()
        {
            ProposalId = currentProposalId, 
            Expires = currentTime + ProposalWaitTimeout
        };
            
        context.Proposals.Add(currentProposalId, proposal);
            
        context.ProposalRouter.Send(new(
            message.Type,
            currentProposalId, 
            proposal,
            actorContext.Self, 
            actorContext.Reply.Value.Promise,
            currentTime
        ));

        actorContext.ByPassReply = true;
            
        return KeyValueStaticResponses.WaitingForReplicationResponse;
    }
    
    /// <summary>
    /// Persists and replicates the key/value messages to the Raft partition
    /// </summary>
    /// <param name="type"></param>
    /// <param name="proposal"></param>
    /// <param name="currentTime"></param>
    /// <returns></returns>
    protected async Task<bool> PersistAndReplicateKeyValueMessage(KeyValueRequestType type, KeyValueProposal proposal, HLCTimestamp currentTime)
    {
        if (!context.Raft.Joined)
            return true;

        int partitionId = ResolvePartition(proposal.Key);

        KeyValueMessage kvm = new()
        {
            Type = (int)type,
            Key = proposal.Key,
            Revision = proposal.Revision,
            ExpireNode = proposal.Expires.N,
            ExpirePhysical = proposal.Expires.L,
            ExpireCounter = proposal.Expires.C,
            LastUsedNode = proposal.LastUsed.N,
            LastUsedPhysical = proposal.LastUsed.L,
            LastUsedCounter = proposal.LastUsed.C,
            LastModifiedNode = proposal.LastModified.N,
            LastModifiedPhysical = proposal.LastModified.L,
            LastModifiedCounter = proposal.LastModified.C,
            TimeNode = currentTime.N,
            TimePhysical = currentTime.L,
            TimeCounter = currentTime.C
        };
        
        if (proposal.Value is not null)
            kvm.Value = UnsafeByteOperations.UnsafeWrap(proposal.Value);

        RaftReplicationResult result = await context.Raft.ReplicateLogs(
            partitionId,
            ReplicationTypes.KeyValues,
            ReplicationSerializer.Serialize(kvm)
        );

        if (!result.Success)
        {
            context.Logger.LogWarning("Failed to replicate key/value {Key} Partition={Partition} Status={Status} Ticket={Ticket}", proposal.Key, partitionId, result.Status, result.TicketId);
            
            return false;
        }
        
        context.BackgroundWriter.Send(new(
            BackgroundWriteType.QueueStoreKeyValue,
            partitionId,
            proposal.Key,
            proposal.Value,
            proposal.Revision,
            proposal.Expires,
            proposal.LastUsed,
            proposal.LastModified,
            (int)proposal.State
        ));

        return result.Success;
    }

    /// <summary>
    /// Returns an existing KeyValueEntry from memory or retrieves it from the persistence layer if there's a cache miss.
    /// </summary>
    /// <param name="key"></param>
    /// <param name="durability"></param>
    /// <param name="readKeyValueEntry"></param>
    /// <returns></returns>
    /// <param name="populateCache">
    /// When true (default) a disk-resident entry is inserted into the in-memory store as a
    /// read-through cache. Read-only range scans MUST pass false: they enumerate the store
    /// lazily, and inserting during that enumeration mutates the BTree mid-iteration (leaf
    /// splits invalidate the live cursor) and pollutes the cache with the entire scanned range.
    /// </param>
    protected async ValueTask<KeyValueEntry?> GetKeyValueEntry(string key, KeyValueDurability durability, ReadOnlyKeyValueEntry? readKeyValueEntry = null, bool populateCache = true)
    {
        if (!context.Store.TryGetValue(key, out KeyValueEntry? entry))
        {
            if (durability == KeyValueDurability.Persistent)
            {
                if (readKeyValueEntry is null)
                    entry = await context.Raft.ReadScheduler.EnqueueTask(ResolvePartition(key), () => context.PersistenceBackend.GetKeyValue(key));
                else
                    entry = new()
                    {
                        Bucket = GetBucket(key),
                        Value = readKeyValueEntry.Value,
                        Revision = readKeyValueEntry.Revision,
                        FlushedRevision = readKeyValueEntry.Revision,
                        Expires = readKeyValueEntry.Expires,
                        LastUsed = readKeyValueEntry.LastUsed,
                        LastModified = readKeyValueEntry.LastModified,
                        State = readKeyValueEntry.State
                    };

                if (entry is not null)
                {
                    entry.FlushedRevision = entry.Revision; // already on disk
                    entry.LastUsed = context.Raft.HybridLogicalClock.TrySendOrLocalEvent(context.Raft.GetLocalNodeId());
                    if (populateCache)
                        context.InsertStoreEntry(key, entry);
                    return entry;
                }
            }

            return null;
        }

        return entry;
    }

    /// <summary>
    /// Calculates the bucket name for the key.
    /// </summary>
    /// <param name="key"></param>
    /// <returns></returns>
    protected static string? GetBucket(string key)
    {
        int index = key.LastIndexOf('/');
        return index == -1 ? null : key[..index];
    }

    /// <summary>
    /// Removes expired revisions from the KeyValueEntry dictionary
    /// </summary>
    /// <param name="entry"></param>
    /// <param name="refRevision">Revisions older than the ref revision will be removed</param>
    protected void RemoveExpiredRevisions(KeyValueEntry entry, long refRevision)
    {
        if (entry.Revisions is null)
            return;

        int toBeKept = context.Configuration.RevisionsToKeepCached;

        foreach (KeyValuePair<long, KeyValueRevisionEntry> kv in entry.Revisions)
        {
            if (kv.Key < (refRevision - toBeKept))
                revisionsToRemove.Add(kv.Key);
        }

        if (revisionsToRemove.Count > 0)
        {
            long bytesFreed = 0;

            foreach (long revision in revisionsToRemove)
            {
                entry.Revisions.TryGetValue(revision, out KeyValueRevisionEntry removed);
                entry.Revisions.Remove(revision);
                bytesFreed += KeyValueStoreAccounting.EstimateRevisionRemovedBytes(entry.Revisions.Count == 0, removed.Value);
            }

            context.AdjustEstimatedEntryBytes(entry, -bytesFreed);
            revisionsToRemove.Clear();
        }
    }
}
