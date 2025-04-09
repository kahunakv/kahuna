
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kommander;
using Kommander.Time;
using Nixie;

namespace Kahuna.Server.KeyValues.Handlers;

internal sealed class TryCollectHandler : BaseHandler
{
    private readonly HashSet<string> keysToEvict = [];
    
    public TryCollectHandler(
        Dictionary<string, KeyValueContext> keyValuesStore,
        IActorRef<BackgroundWriterActor, BackgroundWriteRequest> backgroundWriter,
        IPersistenceBackend persistenceBackend,
        IRaft raft,
        ILogger<IKahuna> logger
    ) : base(keyValuesStore, backgroundWriter, persistenceBackend, raft, logger)
    {

    }

    public void Execute()
    {
        if (keyValuesStore.Count < 200)
            return;
        
        int number = 0;
        TimeSpan range = TimeSpan.FromMinutes(30);
        HLCTimestamp currentTime = raft.HybridLogicalClock.TrySendOrLocalEvent();

        foreach (KeyValuePair<string, KeyValueContext> key in keyValuesStore)
        {
            if (key.Value.WriteIntent is not null)
                continue;
            
            if ((currentTime - key.Value.LastUsed) < range)
                continue;
            
            keysToEvict.Add(key.Key);
            number++;
            
            if (number > 100)
                break;
        }

        foreach (string key in keysToEvict)
        {
            keyValuesStore.Remove(key);
            
            logger.LogDebug("Evicted {Key}", key);
        }
        
        keysToEvict.Clear();
        
        // Ensure that the store has enough capacity for future writes
        keyValuesStore.EnsureCapacity(keyValuesStore.Count + 16);
    }
}