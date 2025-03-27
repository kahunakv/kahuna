
using Nixie;
using Kommander;
using Kahuna.Server.Persistence;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.KeyValues.Handlers;

internal sealed class TryExistsHandler : BaseHandler
{
    public TryExistsHandler(
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
        KeyValueContext? context = await GetKeyValueContext(message.Key, message.Durability);

        ReadOnlyKeyValueContext readOnlyKeyValueContext;
        
        if (message.CompareRevision > -1)
        {
            if (message.Durability == KeyValueDurability.Persistent)
            {
                KeyValueContext? revisionContext = await raft.ReadThreadPool.EnqueueTask(() => persistence.GetKeyValueRevision(message.Key, message.CompareRevision));
                if (revisionContext is null)
                    return KeyValueStaticResponses.DoesNotExistContextResponse;

                return new(KeyValueResponseType.Exists, new ReadOnlyKeyValueContext(null, message.CompareRevision, HLCTimestamp.Zero));
            }
            
            return KeyValueStaticResponses.DoesNotExistContextResponse; 
        }
        
        if (context?.WriteIntent != null && context.WriteIntent.TransactionId != message.TransactionId)
            return new(KeyValueResponseType.MustRetry, 0);

        HLCTimestamp currentTime = await raft.HybridLogicalClock.TrySendOrLocalEvent();

        if (message.TransactionId != HLCTimestamp.Zero)
        {
            if (context is null)
            {
                context = new() { State = KeyValueState.Undefined, Revision = -1 };
                keyValuesStore.Add(message.Key, context);
            }
            
            context.MvccEntries ??= new();

            if (!context.MvccEntries.TryGetValue(message.TransactionId, out KeyValueMvccEntry? entry))
            {
                entry = new()
                {
                    Value = context.Value, 
                    Revision = context.Revision, 
                    Expires = context.Expires, 
                    LastUsed = context.LastUsed,
                    State = context.State
                };

                context.MvccEntries.Add(message.TransactionId, entry);
            }
            
            if (entry.State is KeyValueState.Undefined or KeyValueState.Deleted)
                return KeyValueStaticResponses.DoesNotExistContextResponse;
            
            if (entry.Expires != HLCTimestamp.Zero && entry.Expires - currentTime < TimeSpan.Zero)
                return KeyValueStaticResponses.DoesNotExistContextResponse;
            
            readOnlyKeyValueContext = new(null, entry.Revision, entry.Expires);

            return new(KeyValueResponseType.Exists, readOnlyKeyValueContext);
        }
        
        if (context is null)
            return KeyValueStaticResponses.DoesNotExistContextResponse;
        
        if (context.State == KeyValueState.Deleted)
            return KeyValueStaticResponses.DoesNotExistContextResponse;

        if (context.Expires != HLCTimestamp.Zero && context.Expires - currentTime < TimeSpan.Zero)
            return KeyValueStaticResponses.DoesNotExistContextResponse;
        
        context.LastUsed = currentTime;

        readOnlyKeyValueContext = new(null, context.Revision, context.Expires);

        return new(KeyValueResponseType.Exists, readOnlyKeyValueContext);
    }
}