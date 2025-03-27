
using Kahuna.Server.Persistence;
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
        IPersistence persistence,
        IRaft raft,
        ILogger<IKahuna> logger
    ) : base(keyValuesStore, backgroundWriter, persistence, raft, logger)
    {

    }

    public Task<KeyValueResponse> Execute(KeyValueRequest message)
    {
        List<(string, ReadOnlyKeyValueContext)> items = [];
        
        foreach (KeyValuePair<string, KeyValueContext> keyValue in keyValuesStore)
        {
            if (!keyValue.Key.StartsWith(message.Key))
                continue;

            if (keyValue.Value.Expires != HLCTimestamp.Zero && keyValue.Value.Expires - message.TransactionId < TimeSpan.Zero)
                continue;

            KeyValueContext keyValueContext = keyValue.Value;
            
            items.Add((keyValue.Key, new(keyValueContext.Value, keyValueContext.Revision, keyValueContext.Expires)));
        }
        
        return Task.FromResult<KeyValueResponse>(new(KeyValueResponseType.Get, items));
    }
}