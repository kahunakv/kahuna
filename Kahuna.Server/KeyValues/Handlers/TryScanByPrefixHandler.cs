
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Time;
using Nixie;

namespace Kahuna.Server.KeyValues.Handlers;

internal sealed class TryScanByPrefixHandler : BaseHandler
{
    public TryScanByPrefixHandler(
        Dictionary<string, KeyValueContext> keyValuesStore,
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
        
        foreach ((string? key, KeyValueContext? keyValueContext) in keyValuesStore)
        {
            if (!key.StartsWith(message.Key))
                continue;

            if (keyValueContext.State != KeyValueState.Set)
                continue;

            if (keyValueContext.Expires != HLCTimestamp.Zero && keyValueContext.Expires - message.TransactionId < TimeSpan.Zero)
                continue;

            items.Add((key, new(keyValueContext.Value, keyValueContext.Revision, keyValueContext.Expires, keyValueContext.State)));
        }
        
        items.Sort(EnsureLexicographicalOrder);
        
        return Task.FromResult<KeyValueResponse>(new(KeyValueResponseType.Get, items));
    }
    
    private static int EnsureLexicographicalOrder((string, ReadOnlyKeyValueContext) x, (string, ReadOnlyKeyValueContext) y)
    {
        return string.Compare(x.Item1, y.Item1, StringComparison.Ordinal);
    }
}