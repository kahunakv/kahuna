
using Google.Protobuf;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Server.Replication;
using Kahuna.Server.Replication.Protos;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Time;
using Nixie;

namespace Kahuna.Server.KeyValues.Handlers;

internal abstract class BaseHandler
{
    protected readonly IActorRef<BackgroundWriterActor, BackgroundWriteRequest> backgroundWriter;
    
    protected readonly IRaft raft;

    protected readonly IPersistenceBackend PersistenceBackend;

    protected readonly Dictionary<string, KeyValueContext> keyValuesStore;

    protected readonly ILogger<IKahuna> logger;
    
    protected BaseHandler(
        Dictionary<string, KeyValueContext> keyValuesStore,
        IActorRef<BackgroundWriterActor, BackgroundWriteRequest> backgroundWriter,
        IPersistenceBackend persistenceBackend,
        IRaft raft,
        ILogger<IKahuna> logger
    )
    {
        this.backgroundWriter = backgroundWriter;
        this.raft = raft;
        this.PersistenceBackend = persistenceBackend;
        this.keyValuesStore = keyValuesStore;
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
            ExpirePhysical = proposal.Expires.L,
            ExpireCounter = proposal.Expires.C,
            LastUsedPhysical = proposal.LastUsed.L,
            LastUsedCounter = proposal.LastUsed.C,
            LastModifiedPhysical = proposal.LastModified.L,
            LastModifiedCounter = proposal.LastModified.C,
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

        // Schedule save to be saved asynchronously in a background actor
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
                    context.LastUsed = raft.HybridLogicalClock.TrySendOrLocalEvent();
                    keyValuesStore.Add(key, context);
                    return context;
                }
                
                return null;
            }
            
            return null;    
        }
        
        return context;
    }
}