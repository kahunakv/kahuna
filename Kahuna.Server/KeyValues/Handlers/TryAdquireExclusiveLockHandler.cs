
using Nixie;
using Kommander;
using Kahuna.Server.Persistence;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.KeyValues.Handlers;

internal sealed class TryAdquireExclusiveLockHandler : BaseHandler
{
    public TryAdquireExclusiveLockHandler(
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
        
        HLCTimestamp currentTime = await raft.HybridLogicalClock.ReceiveEvent(message.TransactionId);

        if (!keyValuesStore.TryGetValue(message.Key, out KeyValueContext? context))
        {
            KeyValueContext? newContext = null;

            /// Try to retrieve KeyValue context from persistence
            if (message.Consistency == KeyValueConsistency.Linearizable)
                newContext = await persistence.GetKeyValue(message.Key);

            newContext ??= new() { Revision = -1 };
            
            context = newContext;

            keyValuesStore.Add(message.Key, newContext);
        }

        if (context.WriteIntent is not null)
        {
            // if the transactionId is the same owner no need to acquire the lock
            if (context.WriteIntent.TransactionId == message.TransactionId) 
                return new(KeyValueResponseType.Locked);

            // Check if the lease is still active
            if (context.WriteIntent.Expires != HLCTimestamp.Zero && context.WriteIntent.Expires - currentTime > TimeSpan.Zero)
                return new(KeyValueResponseType.AlreadyLocked);
        }

        context.WriteIntent = new()
        {
            TransactionId = message.TransactionId,
            Expires = message.TransactionId + message.ExpiresMs,
        };
        
        return new(KeyValueResponseType.Locked);
    }
}