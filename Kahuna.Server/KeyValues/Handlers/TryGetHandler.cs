
using Nixie;
using Kommander;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.KeyValues.Handlers;

internal sealed class TryGetHandler : BaseHandler
{
    public TryGetHandler(
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
        KeyValueContext? context = await GetKeyValueContext(message.Key, message.Durability);

        ReadOnlyKeyValueContext readOnlyKeyValueContext;
        
        // Revision is provided so we need to fetch a specific revision from storage
        if (message.CompareRevision > -1)
        {
            if (context is not null && context.Revision == message.CompareRevision)
                return new(KeyValueResponseType.Get, new ReadOnlyKeyValueContext(
                    context.Value, 
                    message.CompareRevision, 
                    HLCTimestamp.Zero,
                    HLCTimestamp.Zero,
                    HLCTimestamp.Zero,
                    KeyValueState.Set
                ));
            
            if (context?.Revisions != null)
            {
                if (context.Revisions.TryGetValue(message.CompareRevision, out byte[]? revisionValue))
                {
                    Console.WriteLine("from memory");
                    
                    return new(KeyValueResponseType.Get, new ReadOnlyKeyValueContext(
                        revisionValue,
                        message.CompareRevision,
                        HLCTimestamp.Zero,
                        HLCTimestamp.Zero,
                        HLCTimestamp.Zero,
                        KeyValueState.Set
                    ));
                }
            }
            
            // Fallback to disk
            if (message.Durability == KeyValueDurability.Persistent)
            {
                KeyValueContext? revisionContext = await raft.ReadThreadPool.EnqueueTask(() => PersistenceBackend.GetKeyValueRevision(message.Key, message.CompareRevision));
                if (revisionContext is null)
                    return KeyValueStaticResponses.DoesNotExistContextResponse;
                
                Console.WriteLine("from disk");

                return new(KeyValueResponseType.Get, new ReadOnlyKeyValueContext(
                    revisionContext.Value, 
                    message.CompareRevision, 
                    HLCTimestamp.Zero,
                    HLCTimestamp.Zero,
                    HLCTimestamp.Zero,
                    KeyValueState.Set
                ));
            }
            
            return KeyValueStaticResponses.DoesNotExistContextResponse; 
        }
        
        if (context?.WriteIntent != null && context.WriteIntent.TransactionId != message.TransactionId)
            return new(KeyValueResponseType.MustRetry, 0);

        HLCTimestamp currentTime = raft.HybridLogicalClock.TrySendOrLocalEvent();

        // TransactionId is provided so we keep a MVCC entry for it
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
                    LastModified = context.LastModified,
                    State = context.State
                };

                context.MvccEntries.Add(message.TransactionId, entry);
            }
            
            if (entry.State is KeyValueState.Undefined or KeyValueState.Deleted)
                return KeyValueStaticResponses.DoesNotExistContextResponse;
            
            if (entry.Expires != HLCTimestamp.Zero && entry.Expires - currentTime < TimeSpan.Zero)
                return KeyValueStaticResponses.DoesNotExistContextResponse;                      
            
            readOnlyKeyValueContext = new(
                entry.Value, 
                entry.Revision, 
                entry.Expires, 
                entry.LastUsed, 
                entry.LastModified, 
                entry.State
            );

            return new(KeyValueResponseType.Get, readOnlyKeyValueContext);
        }
        
        if (context is null)
            return KeyValueStaticResponses.DoesNotExistContextResponse;
        
        if (context.State is KeyValueState.Undefined or KeyValueState.Deleted)
            return KeyValueStaticResponses.DoesNotExistContextResponse;

        if (context.Expires != HLCTimestamp.Zero && context.Expires - currentTime < TimeSpan.Zero)
            return KeyValueStaticResponses.DoesNotExistContextResponse;
        
        context.LastUsed = currentTime;

        readOnlyKeyValueContext = new(
            context.Value, 
            context.Revision, 
            context.Expires, 
            context.LastUsed, 
            context.LastModified, 
            context.State
        );

        return new(KeyValueResponseType.Get, readOnlyKeyValueContext);
    }
}