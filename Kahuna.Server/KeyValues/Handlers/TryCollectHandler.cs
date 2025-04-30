
using Kahuna.Server.Configuration;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Utils;
using Kommander;
using Kommander.Time;
using Nixie;

namespace Kahuna.Server.KeyValues.Handlers;

/// <summary>
/// Handles the periodic collection and eviction of key-value pairs based on specific criteria.
/// This ensures efficient memory usage and optimal performance by removing expired, deleted,
/// or unused keys from the key-value store.
/// </summary>
internal sealed class TryCollectHandler : BaseHandler
{
    private readonly HashSet<string> keysToEvict = [];
    
    public TryCollectHandler(
        BTree<string, KeyValueContext> keyValuesStore,
        IActorRef<BackgroundWriterActor, BackgroundWriteRequest> backgroundWriter,
        IPersistenceBackend persistenceBackend,
        IRaft raft,
        KahunaConfiguration configuration,
        ILogger<IKahuna> logger
    ) : base(keyValuesStore, backgroundWriter, persistenceBackend, raft, configuration, logger)
    {
        
    }

    public void Execute()
    {
        int count = keyValuesStore.Count;
        if (count < 200)
            return;

        int number = 0;
        TimeSpan range = configuration.CacheEntryTtl;
        HLCTimestamp currentTime = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        // Step 1: Evict expired keys
        foreach (KeyValuePair<string, KeyValueContext> key in keyValuesStore.GetItems())
        {
            if (number >= configuration.CacheEntriesToRemove)
                break;
            
            if (key.Value.WriteIntent is not null)
                continue;
            
            if (key.Value.Expires == HLCTimestamp.Zero)
                continue;
            
            if ((key.Value.Expires - currentTime) > TimeSpan.Zero)
                continue;
            
            keysToEvict.Add(key.Key);
            number++;
        }
        
        // Step 2: Evict deleted keys
        foreach (KeyValuePair<string, KeyValueContext> key in keyValuesStore.GetItems())
        {
            if (number >= configuration.CacheEntriesToRemove)
                break;
            
            if (key.Value.WriteIntent is not null)
                continue;

            if (key.Value.State is KeyValueState.Deleted or KeyValueState.Undefined)
            {
                keysToEvict.Add(key.Key);
                number++;
            }
        }
        
        // Step 3: Evict keys that haven't been used in a while
        foreach (KeyValuePair<string, KeyValueContext> key in keyValuesStore.GetItems())
        {
            if (number >= configuration.CacheEntriesToRemove)
                break;
            
            if (key.Value.WriteIntent is not null)
                continue;
            
            if ((currentTime - key.Value.LastUsed) < range)
                continue;
            
            keysToEvict.Add(key.Key);
            number++;
        }

        foreach (string key in keysToEvict)
            keyValuesStore.Remove(key);
        
        if (keysToEvict.Count > 0)
            logger.LogDebug("Evicted {Count} key/value pairs", keysToEvict.Count);
        
        keysToEvict.Clear();
        
        // Ensure that the store has enough capacity for future writes
        // keyValuesStore.EnsureCapacity(keyValuesStore.Count + 16);
    }
}