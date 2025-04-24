
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

    public Task<KeyValueResponse> Execute(KeyValueRequest message)
    {
        List<(string, ReadOnlyKeyValueContext)> items = [];
        
        foreach ((string key, KeyValueContext keyValueContext) in keyValuesStore.GetByPrefix(message.Key))
        {            
            if (keyValueContext.State != KeyValueState.Set)
                continue;

            if (keyValueContext.Expires != HLCTimestamp.Zero && keyValueContext.Expires - message.TransactionId < TimeSpan.Zero)
                continue;

            items.Add((key, new(
                keyValueContext.Value, 
                keyValueContext.Revision, 
                keyValueContext.Expires, 
                keyValueContext.LastUsed,
                keyValueContext.LastModified,
                keyValueContext.State
            )));
        }
        
        items.Sort(EnsureLexicographicalOrder);
        
        return Task.FromResult<KeyValueResponse>(new(KeyValueResponseType.Get, items));
    }
    
    private static int EnsureLexicographicalOrder((string, ReadOnlyKeyValueContext) x, (string, ReadOnlyKeyValueContext) y)
    {
        return string.Compare(x.Item1, y.Item1, StringComparison.Ordinal);
    }
}