
using Kahuna.Server.Persistence;
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
        IPersistence persistence,
        IRaft raft,
        ILogger<IKahuna> logger
    ) : base(keyValuesStore, backgroundWriter, persistence, raft, logger)
    {

    }

    public async Task Execute()
    {
        if (keyValuesStore.Count < 2000)
            return;
        
        int number = 0;
        TimeSpan range = TimeSpan.FromMinutes(30);
        HLCTimestamp currentTime = await raft.HybridLogicalClock.TrySendOrLocalEvent();

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
    }
}