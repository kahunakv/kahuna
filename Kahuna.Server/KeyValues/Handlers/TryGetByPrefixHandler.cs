
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Time;
using Nixie;

namespace Kahuna.Server.KeyValues.Handlers;

internal sealed class TryGetByPrefixHandler : BaseHandler
{
    public TryGetByPrefixHandler(
        Dictionary<string, KeyValueContext> keyValuesStore,
        IActorRef<BackgroundWriterActor, BackgroundWriteRequest> backgroundWriter,
        IPersistenceBackend persistenceBackend,
        IRaft raft,
        ILogger<IKahuna> logger
    ) : base(keyValuesStore, backgroundWriter, persistenceBackend, raft, logger)
    {

    }

    public async Task<KeyValueResponse> Execute(KeyValueRequest message)
    {
        if (message.Durability == KeyValueDurability.Ephemeral)
            return await GetByPrefixEphemeral(message);
        
        return await GetByPrefixPersistent(message);
    }

    private async Task<KeyValueResponse> GetByPrefixEphemeral(KeyValueRequest message)
    {
        List<(string, ReadOnlyKeyValueContext)> items = [];
        HLCTimestamp currentTime = raft.HybridLogicalClock.TrySendOrLocalEvent();
        
        foreach ((string key, KeyValueContext? _) in keyValuesStore)
        {
            if (!key.StartsWith(message.Key, StringComparison.Ordinal))
                continue;

            KeyValueResponse response = await Get(currentTime, message.TransactionId, key, message.Durability);

            if (response is { Type: KeyValueResponseType.Get, Context: not null })
                items.Add((key, response.Context));
        }

        items.Sort(EnsureLexicographicalOrder);
        
        //foreach (var item in items)
        //    Console.WriteLine($"{item.Item1}");
        
        return new(KeyValueResponseType.Get, items);
    }

    private async Task<KeyValueResponse> GetByPrefixPersistent(KeyValueRequest message)
    {
        List<(string, ReadOnlyKeyValueContext)> items = [];
        HLCTimestamp currentTime = raft.HybridLogicalClock.TrySendOrLocalEvent();

        List<(string, ReadOnlyKeyValueContext)> itemsFromDisk = await raft.ReadThreadPool.EnqueueTask(() => PersistenceBackend.GetKeyValueByPrefix(message.Key));
        
        foreach ((string key, ReadOnlyKeyValueContext readOnlyKeyValueContext) in itemsFromDisk)
        {
            KeyValueResponse response = await Get(currentTime, message.TransactionId, key, message.Durability, readOnlyKeyValueContext);

            if (response is { Type: KeyValueResponseType.Get, Context: not null })
                items.Add((key, response.Context));
        }
        
        items.Sort(EnsureLexicographicalOrder);
        
        //foreach (var item in items)
        //    Console.WriteLine($"{item.Item1}");
        
        return new(KeyValueResponseType.Get, items);
    }

    private async Task<KeyValueResponse> Get(HLCTimestamp currentTime, HLCTimestamp transactionId, string key, KeyValueDurability durability, ReadOnlyKeyValueContext? keyValueContext = null)
    {
        KeyValueContext? context = await GetKeyValueContext(key, durability, keyValueContext);

        ReadOnlyKeyValueContext readOnlyKeyValueContext;
        
        if (context?.WriteIntent != null && context.WriteIntent.TransactionId != transactionId)
            return new(KeyValueResponseType.MustRetry, 0);

        // TransactionId is provided so we keep a MVCC entry for it
        if (transactionId != HLCTimestamp.Zero)
        {
            if (context is null)
            {
                context = new() { State = KeyValueState.Undefined, Revision = -1 };
                keyValuesStore.Add(key, context);
            }
            
            context.MvccEntries ??= new();

            if (!context.MvccEntries.TryGetValue(transactionId, out KeyValueMvccEntry? entry))
            {
                entry = new()
                {
                    Value = context.Value, 
                    Revision = context.Revision, 
                    Expires = context.Expires, 
                    LastUsed = context.LastUsed,
                    State = context.State
                };

                context.MvccEntries.Add(transactionId, entry);
            }
            
            if (entry.State is KeyValueState.Undefined or KeyValueState.Deleted)
                return KeyValueStaticResponses.DoesNotExistContextResponse;
            
            if (entry.Expires != HLCTimestamp.Zero && entry.Expires - currentTime < TimeSpan.Zero)
                return KeyValueStaticResponses.DoesNotExistContextResponse;
            
            readOnlyKeyValueContext = new(entry.Value, entry.Revision, entry.Expires, context.State);

            return new(KeyValueResponseType.Get, readOnlyKeyValueContext);
        }

        if (context is null)
            return KeyValueStaticResponses.DoesNotExistContextResponse;

        if (context.State == KeyValueState.Deleted)
            return KeyValueStaticResponses.DoesNotExistContextResponse;

        if (context.Expires != HLCTimestamp.Zero && context.Expires - currentTime < TimeSpan.Zero)
            return KeyValueStaticResponses.DoesNotExistContextResponse;

        context.LastUsed = currentTime;

        readOnlyKeyValueContext = new(context.Value, context.Revision, context.Expires, context.State);

        return new(KeyValueResponseType.Get, readOnlyKeyValueContext);
    } 
    
    private static int EnsureLexicographicalOrder((string, ReadOnlyKeyValueContext) x, (string, ReadOnlyKeyValueContext) y)
    {
        return string.Compare(x.Item1, y.Item1, StringComparison.Ordinal);
    }
}