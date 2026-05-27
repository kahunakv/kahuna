
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
/// Represents a handler that attempts to scan key-value entries in a data store based on a specified prefix.
/// </summary>
/// <remarks>
/// This handler iterates over the key-value store (b+tree), filters entries that match the given prefix, discards expired entries,
/// and returns the filtered entries. 
/// </remarks>
internal sealed class TryScanByPrefixHandler : BaseHandler
{
    public TryScanByPrefixHandler(KeyValueContext context) : base(context)
    {
        
    }

    /// <summary>
    /// Executes the scan by prefix request
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    public async Task<KeyValueResponse> Execute(KeyValueRequest message)
    {
        List<(string, ReadOnlyKeyValueEntry)> items = [];
        HLCTimestamp currentTime = context.Raft.HybridLogicalClock.TrySendOrLocalEvent(context.Raft.GetLocalNodeId());
        
        foreach ((string key, KeyValueEntry? _) in context.Store.GetByBucket(message.Key))
        {
            KeyValueResponse response = await Get(currentTime, key, message.Durability);     
            
            if (response.Type != KeyValueResponseType.Get)
                continue;

            if (response is { Type: KeyValueResponseType.Get, Entry: not null })
                items.Add((key, response.Entry));
        }        
                
        return new(KeyValueResponseType.Get, items);
    }        

    private async Task<KeyValueResponse> Get(HLCTimestamp currentTime, string key, KeyValueDurability durability, ReadOnlyKeyValueEntry? keyValueEntry = null)
    {
        KeyValueEntry? entry = await GetKeyValueEntry(key, durability, keyValueEntry);

        if (entry is null || entry.State == KeyValueState.Deleted || entry.Expires != HLCTimestamp.Zero && entry.Expires - currentTime < TimeSpan.Zero)
            return KeyValueStaticResponses.DoesNotExistContextResponse;

        ReadOnlyKeyValueEntry readOnlyKeyValueEntry = new(
            entry.Value, 
            entry.Revision, 
            entry.Expires, 
            entry.LastUsed, 
            entry.LastModified, 
            entry.State
        );

        return new(KeyValueResponseType.Get, readOnlyKeyValueEntry);
    }       
}