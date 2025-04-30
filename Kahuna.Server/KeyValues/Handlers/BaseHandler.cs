
using Google.Protobuf;
using Kahuna.Server.Configuration;
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
    /// <summary>
    /// Represents the background writer actor reference.
    /// </summary>
    protected readonly IActorRef<BackgroundWriterActor, BackgroundWriteRequest> backgroundWriter;
    
    protected readonly IRaft raft;

    protected readonly IPersistenceBackend PersistenceBackend;

    protected readonly BTree<string, KeyValueContext> keyValuesStore;

    protected readonly KahunaConfiguration configuration;

    protected readonly ILogger<IKahuna> logger;
    
    private readonly HashSet<long> revisionsToRemove = [];
    
    protected BaseHandler(
        BTree<string, KeyValueContext> keyValuesStore,
        IActorRef<BackgroundWriterActor, BackgroundWriteRequest> backgroundWriter,
        IPersistenceBackend persistenceBackend,
        IRaft raft,
        KahunaConfiguration configuration,
        ILogger<IKahuna> logger
    )
    {
        this.backgroundWriter = backgroundWriter;
        this.raft = raft;
        this.PersistenceBackend = persistenceBackend;
        this.keyValuesStore = keyValuesStore;
        this.configuration = configuration;
        this.logger = logger;
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
        if (!raft.Joined)
            return true;

        int partitionId = raft.GetPartitionKey(proposal.Key);

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

        RaftReplicationResult result = await raft.ReplicateLogs(
            partitionId,
            ReplicationTypes.KeyValues,
            ReplicationSerializer.Serialize(kvm)
        );

        if (!result.Success)
        {
            logger.LogWarning("Failed to replicate key/value {Key} Partition={Partition} Status={Status} Ticket={Ticket}", proposal.Key, partitionId, result.Status, result.TicketId);
            
            return false;
        }
        
        backgroundWriter.Send(new(
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
    /// Returns an existing KeyValueContext from memory or retrieves it from the persistence layer
    /// </summary>
    /// <param name="key"></param>
    /// <param name="durability"></param>
    /// <param name="readKeyValueContext"></param>
    /// <returns></returns>
    protected async ValueTask<KeyValueContext?> GetKeyValueContext(string key, KeyValueDurability durability, ReadOnlyKeyValueContext? readKeyValueContext = null)
    {
        if (!keyValuesStore.TryGetValue(key, out KeyValueContext? context))
        {
            if (durability == KeyValueDurability.Persistent)
            {
                if (readKeyValueContext is null)
                    context = await raft.ReadThreadPool.EnqueueTask(() => PersistenceBackend.GetKeyValue(key));
                else
                    context = new()
                    {
                        Value = readKeyValueContext.Value,
                        Revision = readKeyValueContext.Revision,
                        Expires = readKeyValueContext.Expires,
                        LastUsed = readKeyValueContext.LastUsed,
                        LastModified = readKeyValueContext.LastModified,
                        State = readKeyValueContext.State
                    };
                
                if (context is not null)
                {
                    context.LastUsed = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());
                    keyValuesStore.Insert(key, context);
                    return context;
                }
            }
            
            return null;    
        }
        
        return context;
    }
    
    protected void RemoveExpiredRevisions(KeyValueContext context, long refRevision)
    {
        if (context.Revisions is null)
            return;               
            
        foreach (KeyValuePair<long, byte[]?> kv in context.Revisions)
        {
            if (kv.Key < (refRevision - configuration.RevisionsToKeepCached))                
                revisionsToRemove.Add(kv.Key);
        }

        if (revisionsToRemove.Count > 0)
        {
            foreach (long revision in revisionsToRemove)                
                context.Revisions.Remove(revision);                
            
            revisionsToRemove.Clear();
        }
    }
}