
using Nixie;
using Kommander;
using Kahuna.Server.Persistence;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.KeyValues.Handlers;

internal sealed class TryReleaseExclusiveLockHandler : BaseHandler
{
    public TryReleaseExclusiveLockHandler(
        Dictionary<string, KeyValueContext> keyValuesStore,
        IActorRef<BackgroundWriterActor, BackgroundWriteRequest> backgroundWriter,
        IPersistence persistence,
        IRaft raft,
        ILogger<IKahuna> logger
    ) : base(keyValuesStore, backgroundWriter, persistence, raft, logger)
    {

    }

    public async Task<KeyValueResponse> Execute(KeyValueRequest message)
    {
        if (message.TransactionId == HLCTimestamp.Zero)
            return new(KeyValueResponseType.Errored);
        
        KeyValueContext? context = await GetKeyValueContext(message.Key, message.Durability);
        
        if (context is null)
            return new(KeyValueResponseType.DoesNotExist);
        
        context.MvccEntries?.Remove(message.TransactionId);
        
        if (context.WriteIntent is not null && context.WriteIntent.TransactionId != message.TransactionId)
            return new(KeyValueResponseType.AlreadyLocked);

        context.WriteIntent = null;

        return new(KeyValueResponseType.Unlocked);
    }
}