
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
    public TryExistsHandler(KeyValueContext context) : base(context)
    {
        
    }

    public async Task<KeyValueResponse> Execute(KeyValueRequest message)
    {
        KeyValueEntry? entry = await GetKeyValueEntry(message.Key, message.Durability);

        ReadOnlyKeyValueEntry readOnlyKeyValueEntry;
        
        if (message.CompareRevision > -1)
        {
            if (entry is not null && entry.Revision == message.CompareRevision)
                return new(KeyValueResponseType.Exists, new ReadOnlyKeyValueEntry(
                    null, 
                    message.CompareRevision, 
                    HLCTimestamp.Zero,
                    HLCTimestamp.Zero,
                    HLCTimestamp.Zero, 
                    KeyValueState.Set
                ));
            
            if (entry?.Revisions != null)
            {
                if (entry.Revisions.ContainsKey(message.CompareRevision))
                    return new(KeyValueResponseType.Exists, new ReadOnlyKeyValueEntry(
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
                KeyValueEntry? revisionContext = await context.Raft.ReadThreadPool.EnqueueTask(() => context.PersistenceBackend.GetKeyValueRevision(message.Key, message.CompareRevision));
                if (revisionContext is null)
                    return KeyValueStaticResponses.DoesNotExistContextResponse;

                return new(KeyValueResponseType.Exists, new ReadOnlyKeyValueEntry(
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
        
        HLCTimestamp currentTime = context.Raft.HybridLogicalClock.TrySendOrLocalEvent(context.Raft.GetLocalNodeId());
        
        // Validate if there's an exclusive key acquired on the lock and whether it is expired
        // if we find expired write intents we can remove it to allow new transactions to proceed
        if (entry?.WriteIntent != null)
        {
            if (entry.WriteIntent.TransactionId != message.TransactionId)
            {
                if (entry.WriteIntent.Expires - currentTime > TimeSpan.Zero)                
                    return new(KeyValueResponseType.MustRetry, 0);
                
                entry.WriteIntent = null;
            }
        }
        
        // Validate if there's a prefix lock acquired on the bucket
        // if we find expired write intents we can remove it to allow new transactions to proceed
        
        string? bucket = entry is not null ? entry.Bucket : GetBucket(message.Key);
        
        if (bucket is not null && context.LocksByPrefix.TryGetValue(bucket, out KeyValueWriteIntent? intent))
        {
            if (intent.TransactionId != message.TransactionId)
            {
                if (intent.Expires - currentTime > TimeSpan.Zero)
                    return new(KeyValueResponseType.MustRetry, 0);
            
                context.LocksByPrefix.Remove(bucket);
            }
        }

        if (message.TransactionId != HLCTimestamp.Zero)
        {
            if (entry is null)
            {
                entry = new() { Bucket = GetBucket(message.Key), State = KeyValueState.Undefined, Revision = -1 };
                context.Store.Insert(message.Key, entry);
            }
            
            entry.MvccEntries ??= new();

            if (!entry.MvccEntries.TryGetValue(message.TransactionId, out KeyValueMvccEntry? mvccEntry))
            {
                mvccEntry = new()
                {
                    Value = entry.Value, 
                    Revision = entry.Revision, 
                    Expires = entry.Expires, 
                    LastUsed = entry.LastUsed,
                    LastModified = entry.LastModified,
                    State = entry.State
                };

                entry.MvccEntries.Add(message.TransactionId, mvccEntry);
            }
            
            if (mvccEntry.State is KeyValueState.Undefined or KeyValueState.Deleted || (mvccEntry.Expires != HLCTimestamp.Zero && mvccEntry.Expires - currentTime < TimeSpan.Zero))
                return KeyValueStaticResponses.DoesNotExistContextResponse;

            if (entry.Revision > mvccEntry.Revision) // early conflict detection
                return KeyValueStaticResponses.AbortedResponse;
            
            readOnlyKeyValueEntry = new(
                null, 
                mvccEntry.Revision, 
                mvccEntry.Expires, 
                mvccEntry.LastUsed, 
                mvccEntry.LastModified, 
                mvccEntry.State
            );

            return new(KeyValueResponseType.Exists, readOnlyKeyValueEntry);
        }
        
        if (entry is null || entry.State is KeyValueState.Undefined or KeyValueState.Deleted || (entry.Expires != HLCTimestamp.Zero && entry.Expires - currentTime < TimeSpan.Zero))
            return KeyValueStaticResponses.DoesNotExistContextResponse;

        entry.LastUsed = currentTime;

        readOnlyKeyValueEntry = new(
            null, 
            entry.Revision, 
            entry.Expires, 
            entry.LastUsed, 
            entry.LastModified, 
            entry.State
        );

        return new(KeyValueResponseType.Exists, readOnlyKeyValueEntry);
    }
}