
using Kahuna.Server.Configuration;
using Nixie;
using Kommander;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Shared.KeyValue;
using Kahuna.Utils;
using Kommander.Time;

namespace Kahuna.Server.KeyValues.Handlers;

/// <summary>
/// Handles the processing of requests to check the existence of a key-value entry in the store.
/// </summary>
internal sealed class TryExistsHandler : BaseHandler
{
    public TryExistsHandler(BTree<string, KeyValueContext> keyValuesStore,
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
        KeyValueContext? context = await GetKeyValueContext(message.Key, message.Durability);

        ReadOnlyKeyValueContext readOnlyKeyValueContext;
        
        if (message.CompareRevision > -1)
        {
            if (context is not null && context.Revision == message.CompareRevision)
                return new(KeyValueResponseType.Exists, new ReadOnlyKeyValueContext(
                    null, 
                    message.CompareRevision, 
                    HLCTimestamp.Zero,
                    HLCTimestamp.Zero,
                    HLCTimestamp.Zero, 
                    KeyValueState.Set
                ));
            
            if (context?.Revisions != null)
            {
                if (context.Revisions.ContainsKey(message.CompareRevision))
                    return new(KeyValueResponseType.Exists, new ReadOnlyKeyValueContext(
                        null, 
                        message.CompareRevision, 
                        HLCTimestamp.Zero,
                        HLCTimestamp.Zero,
                        HLCTimestamp.Zero, 
                        KeyValueState.Set
                    ));
            }
            
            // Fallback to disk
            if (message.Durability == KeyValueDurability.Persistent)
            {
                KeyValueContext? revisionContext = await raft.ReadThreadPool.EnqueueTask(() => PersistenceBackend.GetKeyValueRevision(message.Key, message.CompareRevision));
                if (revisionContext is null)
                    return KeyValueStaticResponses.DoesNotExistContextResponse;

                return new(KeyValueResponseType.Exists, new ReadOnlyKeyValueContext(
                    null, 
                    message.CompareRevision, 
                    HLCTimestamp.Zero,
                    HLCTimestamp.Zero,
                    HLCTimestamp.Zero,
                    KeyValueState.Set
                ));
            }
            
            return KeyValueStaticResponses.DoesNotExistContextResponse; 
        }
        
        HLCTimestamp currentTime = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());
        
        // Validate if there's an exclusive key acquired on the lock and whether it is expired
        // if we find expired write intents we can remove it to allow new transactions to proceed
        if (context?.WriteIntent != null)
        {
            if (context.WriteIntent.TransactionId != message.TransactionId)
            {
                if (context.WriteIntent.Expires - currentTime > TimeSpan.Zero)                
                    return new(KeyValueResponseType.MustRetry, 0);
                
                context.WriteIntent = null;
            }
        }        

        if (message.TransactionId != HLCTimestamp.Zero)
        {
            if (context is null)
            {
                context = new() { Bucket = GetBucket(message.Key), State = KeyValueState.Undefined, Revision = -1 };
                keyValuesStore.Insert(message.Key, context);
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
            
            if (context.Revision > entry.Revision) // early conflict detection
                return KeyValueStaticResponses.AbortedResponse;
            
            readOnlyKeyValueContext = new(
                null, 
                entry.Revision, 
                entry.Expires, 
                entry.LastUsed, 
                entry.LastModified, 
                entry.State
            );

            return new(KeyValueResponseType.Exists, readOnlyKeyValueContext);
        }
        
        if (context is null)
            return KeyValueStaticResponses.DoesNotExistContextResponse;
        
        if (context.State is KeyValueState.Undefined or KeyValueState.Deleted)
            return KeyValueStaticResponses.DoesNotExistContextResponse;

        if (context.Expires != HLCTimestamp.Zero && context.Expires - currentTime < TimeSpan.Zero)
            return KeyValueStaticResponses.DoesNotExistContextResponse;
        
        context.LastUsed = currentTime;

        readOnlyKeyValueContext = new(
            null, 
            context.Revision, 
            context.Expires, 
            context.LastUsed, 
            context.LastModified, 
            context.State
        );

        return new(KeyValueResponseType.Exists, readOnlyKeyValueContext);
    }
}