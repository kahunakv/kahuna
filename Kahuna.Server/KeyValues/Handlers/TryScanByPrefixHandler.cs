
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
    public TryScanByPrefixHandler(
        BTree<string, KeyValueContext> keyValuesStore,
        IActorRef<BackgroundWriterActor, BackgroundWriteRequest> backgroundWriter,
        IPersistenceBackend persistenceBackend,
        IRaft raft,
        KahunaConfiguration configuration,
        ILogger<IKahuna> logger
    ) : base(keyValuesStore, backgroundWriter, persistenceBackend, raft, configuration, logger)
    {
        
    }

    /// <summary>
    /// Executes the scan by prefix request
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    public async Task<KeyValueResponse> Execute(KeyValueRequest message)
    {
        List<(string, ReadOnlyKeyValueContext)> items = [];
        HLCTimestamp currentTime = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());
        
        foreach ((string key, KeyValueContext? _) in keyValuesStore.GetByPrefix(message.Key))
        {
            KeyValueResponse response = await Get(currentTime, key, message.Durability);     
            
            if (response.Type != KeyValueResponseType.Get)
                continue;

            if (response is { Type: KeyValueResponseType.Get, Context: not null })
                items.Add((key, response.Context));
        }        
                
        return new(KeyValueResponseType.Get, items);
    }        

    private async Task<KeyValueResponse> Get(HLCTimestamp currentTime, string key, KeyValueDurability durability, ReadOnlyKeyValueContext? keyValueContext = null)
    {
        KeyValueContext? context = await GetKeyValueContext(key, durability, keyValueContext);

        if (context is null || context.State == KeyValueState.Deleted || context.Expires != HLCTimestamp.Zero && context.Expires - currentTime < TimeSpan.Zero)
            return KeyValueStaticResponses.DoesNotExistContextResponse;

        ReadOnlyKeyValueContext readOnlyKeyValueContext = new(
            context.Value, 
            context.Revision, 
            context.Expires, 
            context.LastUsed, 
            context.LastModified, 
            context.State
        );

        return new(KeyValueResponseType.Get, readOnlyKeyValueContext);
    }       
}