
using Kahuna.Server.Configuration;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Shared.KeyValue;
using Kahuna.Utils;
using Kommander;
using Kommander.Time;
using Nixie;

namespace Kahuna.Server.KeyValues.Handlers;

/// <summary>
/// Represents a handler that attempts to scan key-value entries from disk based on a specified prefix.
/// </summary>
/// <remarks>
/// This handler asks the backend persistence to scan the key-value store for entries that match the given prefix.
/// </remarks>
internal sealed class TryScanByPrefixFromDiskHandler : BaseHandler
{
    public TryScanByPrefixFromDiskHandler(BTree<string, KeyValueContext> keyValuesStore,
        Dictionary<string, KeyValueWriteIntent> locksByPrefix,
        IActorRef<BackgroundWriterActor, BackgroundWriteRequest> backgroundWriter,
        IPersistenceBackend persistenceBackend,
        IRaft raft,
        KahunaConfiguration configuration,
        ILogger<IKahuna> logger) : base(keyValuesStore, locksByPrefix, backgroundWriter, persistenceBackend, raft, configuration, logger)
    {
        
    }

    /// <summary>
    /// Executes the scan by prefix from disk request
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    public async Task<KeyValueResponse> Execute(KeyValueRequest message)
    {
        Dictionary<string, ReadOnlyKeyValueContext> items = new();
                
        HLCTimestamp currentTime = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());
        
        List<(string, ReadOnlyKeyValueContext)> itemsFromDisk = await raft.ReadThreadPool.EnqueueTask(() => PersistenceBackend.GetKeyValueByPrefix(message.Key));
        
        foreach ((string key, ReadOnlyKeyValueContext readOnlyKeyValueContext) in itemsFromDisk)
        {
            if (items.ContainsKey(key))
                continue;
            
            if (readOnlyKeyValueContext.State == KeyValueState.Deleted || readOnlyKeyValueContext.Expires != HLCTimestamp.Zero && readOnlyKeyValueContext.Expires - currentTime < TimeSpan.Zero)
                continue;
                        
            items.Add(key, readOnlyKeyValueContext);
        }                             
                
        return new(KeyValueResponseType.Get, items.Select(kv => (kv.Key, kv.Value)).ToList());
    }
}