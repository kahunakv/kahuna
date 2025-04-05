
using Nixie;
using Kommander;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.KeyValues.Handlers;

internal sealed class TryAdquireExclusiveLockHandler : BaseHandler
{
    public TryAdquireExclusiveLockHandler(
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
        
        HLCTimestamp currentTime = raft.HybridLogicalClock.ReceiveEvent(message.TransactionId);

        if (!keyValuesStore.TryGetValue(message.Key, out KeyValueContext? context))
        {
            KeyValueContext? newContext = null;

            /// Try to retrieve KeyValue context from persistence
            if (message.Durability == KeyValueDurability.Persistent)
                newContext = await raft.ReadThreadPool.EnqueueTask(() => PersistenceBackend.GetKeyValue(message.Key));

            newContext ??= new() { State = KeyValueState.Undefined, Revision = -1 };
            
            context = newContext;

            keyValuesStore.Add(message.Key, newContext);
        }

        if (context.WriteIntent is not null)
        {
            // if the transactionId is the same owner no need to acquire the lock
            if (context.WriteIntent.TransactionId == message.TransactionId) 
                return KeyValueStaticResponses.LockedResponse;

            // Check if the lease is still active
            if (context.WriteIntent.Expires != HLCTimestamp.Zero && context.WriteIntent.Expires - currentTime > TimeSpan.Zero)
                return KeyValueStaticResponses.AlreadyLockedResponse;
        }

        context.WriteIntent = new()
        {
            TransactionId = message.TransactionId,
            Expires = message.TransactionId + message.ExpiresMs,
        };
        
        logger.LogDebug("Assigned {Key} write intent to TxId={TransactionId}", message.Key, message.TransactionId);
        
        return KeyValueStaticResponses.LockedResponse;
    }
}