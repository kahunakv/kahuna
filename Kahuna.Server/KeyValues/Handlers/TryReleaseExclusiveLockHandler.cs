
using Kahuna.Server.Configuration;
using Nixie;
using Kommander;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Shared.KeyValue;
using Kahuna.Utils;
using Kommander.Time;

namespace Kahuna.Server.KeyValues.Handlers;

internal sealed class TryReleaseExclusiveLockHandler : BaseHandler
{
    public TryReleaseExclusiveLockHandler(BTree<string, KeyValueContext> keyValuesStore,
        Dictionary<string, KeyValueWriteIntent> locksByPrefix,
        IActorRef<BackgroundWriterActor, BackgroundWriteRequest> backgroundWriter,
        IPersistenceBackend persistenceBackend,
        IRaft raft,
        KahunaConfiguration configuration,
        ILogger<IKahuna> logger) : base(keyValuesStore, locksByPrefix, backgroundWriter, persistenceBackend, raft, configuration, logger)
    {
        
    }

    public async Task<KeyValueResponse> Execute(KeyValueRequest message)
    {
        if (message.TransactionId == HLCTimestamp.Zero)
            return KeyValueStaticResponses.ErroredResponse;
        
        KeyValueContext? context = await GetKeyValueContext(message.Key, message.Durability);
        
        if (context is null)
            return KeyValueStaticResponses.DoesNotExistResponse;
        
        if (context.MvccEntries is null)
            logger.LogWarning("Trying to release exclusive lock for {Key} but MVCC entries are null", message.Key);
        else
            context.MvccEntries.Remove(message.TransactionId);
        
        if (context.WriteIntent is not null && context.WriteIntent.TransactionId != message.TransactionId)
            return KeyValueStaticResponses.AlreadyLockedResponse;

        context.WriteIntent = null;

        return KeyValueStaticResponses.UnlockedResponse;
    }
}