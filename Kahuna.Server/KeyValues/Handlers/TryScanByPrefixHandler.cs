
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
/// This handler iterates over the key-value store, filters entries that match the given prefix, discards expired entries,
/// and returns the filtered entries in lexicographical order. 
/// </remarks>
/// <threadsafety>
/// This class is intended for use in a multi-threaded actor context, ensuring encapsulated access to shared resources.
/// </threadsafety>
internal sealed class TryScanByPrefixHandler : BaseHandler
{
    public TryScanByPrefixHandler(
        BTree<string, KeyValueContext> keyValuesStore,
        IActorRef<BackgroundWriterActor, BackgroundWriteRequest> backgroundWriter,
        IPersistenceBackend persistenceBackend,
        IRaft raft,
        ILogger<IKahuna> logger
    ) : base(keyValuesStore, backgroundWriter, persistenceBackend, raft, logger)
    {

    }

    /// <summary>
    /// Executes the get by prefix request
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    public async Task<KeyValueResponse> Execute(KeyValueRequest message)
    {
        if (message.Durability == KeyValueDurability.Ephemeral)
            return await ScanByPrefixEphemeral(message);
        
        return await ScanByPrefixPersistent(message);
    }

    /// <summary>
    /// Queries the key-value store for entries matching the specified prefix in an ephemeral context.
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    private async Task<KeyValueResponse> ScanByPrefixEphemeral(KeyValueRequest message)
    {
        List<(string, ReadOnlyKeyValueContext)> items = [];
        HLCTimestamp currentTime = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());
        
        foreach ((string key, KeyValueContext? _) in keyValuesStore.GetByPrefix(message.Key))
        {
            KeyValueResponse response = await Get(currentTime, message.TransactionId, key, message.Durability);     
            
            if (response.Type != KeyValueResponseType.Get)
                continue;

            if (response is { Type: KeyValueResponseType.Get, Context: not null })
                items.Add((key, response.Context));
        }

        items.Sort(EnsureLexicographicalOrder);
                
        return new(KeyValueResponseType.Get, items);
    }

    /// <summary>
    /// Queries the key-value store for entries matching the specified prefix in a persistent context.
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    private async Task<KeyValueResponse> ScanByPrefixPersistent(KeyValueRequest message)
    {
        Dictionary<string, ReadOnlyKeyValueContext> items = new();
        
        HLCTimestamp currentTime = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        // step 1: we need to check the in-memory store to get the MVCC entry or the latest value
        foreach ((string key, KeyValueContext? _) in keyValuesStore.GetByPrefix(message.Key))
        {
            KeyValueResponse response = await Get(currentTime, message.TransactionId, key, message.Durability);
            
            if (response.Type != KeyValueResponseType.Get || response.Context is null)
                continue;

            items.Add(key, new(
                response.Context.Value, 
                response.Context.Revision, 
                response.Context.Expires, 
                response.Context.LastUsed,
                response.Context.LastModified,
                response.Context.State
            ));
        }

        // step 2: we join the in-memory store with the disk store
        List<(string, ReadOnlyKeyValueContext)> itemsFromDisk = await raft.ReadThreadPool.EnqueueTask(() => PersistenceBackend.GetKeyValueByPrefix(message.Key));
        
        foreach ((string key, ReadOnlyKeyValueContext readOnlyKeyValueContext) in itemsFromDisk)
        {
            if (items.ContainsKey(key))
                continue;
            
            KeyValueResponse response = await Get(currentTime, message.TransactionId, key, message.Durability, readOnlyKeyValueContext);

            if (response is { Type: KeyValueResponseType.Get, Context: not null })
                items.Add(key, response.Context);
        }

        // step 3: make sure the items are sorted in lexicographical order
        List<(string Key, ReadOnlyKeyValueContext Value)> itemsToReturn = items.Select(kv => (kv.Key, kv.Value)).ToList();
        
        itemsToReturn.Sort(EnsureLexicographicalOrder);
                
        return new(KeyValueResponseType.Get, itemsToReturn);
    }

    private async Task<KeyValueResponse> Get(HLCTimestamp currentTime, HLCTimestamp transactionId, string key, KeyValueDurability durability, ReadOnlyKeyValueContext? keyValueContext = null)
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
    
    private static int EnsureLexicographicalOrder((string, ReadOnlyKeyValueContext) x, (string, ReadOnlyKeyValueContext) y)
    {
        return string.Compare(x.Item1, y.Item1, StringComparison.Ordinal);
    }
}