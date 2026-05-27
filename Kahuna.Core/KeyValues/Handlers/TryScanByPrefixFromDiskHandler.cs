
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
    public TryScanByPrefixFromDiskHandler(KeyValueContext context) : base(context)
    {
        
    }

    /// <summary>
    /// Executes the scan by prefix from disk request
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    public async Task<KeyValueResponse> Execute(KeyValueRequest message)
    {
        Dictionary<string, ReadOnlyKeyValueEntry> items = new();
                
        HLCTimestamp currentTime = context.Raft.HybridLogicalClock.TrySendOrLocalEvent(context.Raft.GetLocalNodeId());
        
        List<(string, ReadOnlyKeyValueEntry)> itemsFromDisk = await context.Raft.ReadThreadPool.EnqueueTask(() => context.PersistenceBackend.GetKeyValueByPrefix(message.Key));
        
        foreach ((string key, ReadOnlyKeyValueEntry readOnlyKeyValueEntry) in itemsFromDisk)
        {
            if (items.ContainsKey(key))
                continue;
            
            if (readOnlyKeyValueEntry.State == KeyValueState.Deleted || readOnlyKeyValueEntry.Expires != HLCTimestamp.Zero && readOnlyKeyValueEntry.Expires - currentTime < TimeSpan.Zero)
                continue;
                        
            items.Add(key, readOnlyKeyValueEntry);
        }                             
                
        return new(KeyValueResponseType.Get, items.Select(kv => (kv.Key, kv.Value)).ToList());
    }
}