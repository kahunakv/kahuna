
using Nixie;
using Kommander;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.KeyValues.Handlers;

internal sealed class TryReleaseExclusiveLockHandler : BaseHandler
{
    public TryReleaseExclusiveLockHandler(
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
        if (message.TransactionId == HLCTimestamp.Zero)
            return KeyValueStaticResponses.ErroredResponse;
        
        KeyValueContext? context = await GetKeyValueContext(message.Key, message.Durability);
        
        if (context is null)
            return new(KeyValueResponseType.DoesNotExist);
        
        if (context.MvccEntries is null)
            logger.LogWarning("Trying to release exclusive lock for {Key} but MVCC entries are null", message.Key);
        else
            context.MvccEntries.Remove(message.TransactionId);
        
        if (context.WriteIntent is not null && context.WriteIntent.TransactionId != message.TransactionId)
            return new(KeyValueResponseType.AlreadyLocked);

        context.WriteIntent = null;

        return new(KeyValueResponseType.Unlocked);
    }
}