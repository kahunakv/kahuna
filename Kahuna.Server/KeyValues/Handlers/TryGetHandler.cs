
using Kommander.Time;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Handlers;

/// <summary>
/// Represents a handler for attempting to retrieve key-value pairs.
/// </summary>
/// <remarks>
/// This handler is used within the KeyValue system to process requests for retrieving
/// key-value data. It operates on the basis of the provided key-value store, background
/// writer, persistence backend, and Raft consensus system for managing distributed state.
/// </remarks>
internal sealed class TryGetHandler : BaseHandler
{
    public TryGetHandler(KeyValueContext context) : base(context)
    {
        
    }

    public async Task<KeyValueResponse> Execute(KeyValueRequest message)
    {
        KeyValueEntry? entry = await GetKeyValueEntry(message.Key, message.Durability);

        ReadOnlyKeyValueEntry readOnlyKeyValueEntry;
        
        // Revision is provided so we need to fetch a specific revision from storage
        if (message.CompareRevision > -1)
        {
            if (entry is not null && entry.Revision == message.CompareRevision)
                return new(KeyValueResponseType.Get, new ReadOnlyKeyValueEntry(
                    entry.Value, 
                    message.CompareRevision, 
                    HLCTimestamp.Zero,
                    HLCTimestamp.Zero,
                    HLCTimestamp.Zero,
                    KeyValueState.Set
                ));
            
            if (entry?.Revisions != null)
            {
                if (entry.Revisions.TryGetValue(message.CompareRevision, out byte[]? revisionValue))
                {
                    return new(KeyValueResponseType.Get, new ReadOnlyKeyValueEntry(
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
                KeyValueEntry? revisionContext = await context.Raft.ReadThreadPool.EnqueueTask(() => context.PersistenceBackend.GetKeyValueRevision(message.Key, message.CompareRevision));
                if (revisionContext is null)
                    return KeyValueStaticResponses.DoesNotExistContextResponse;

                return new(KeyValueResponseType.Get, new ReadOnlyKeyValueEntry(
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

        // TransactionId is provided so we keep a MVCC entry for it
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
            
            if (entry.Revision > mvccEntry.Revision) // early conflict detection
                return KeyValueStaticResponses.AbortedResponse;
            
            if (mvccEntry.State is KeyValueState.Undefined or KeyValueState.Deleted || (mvccEntry.Expires != HLCTimestamp.Zero && mvccEntry.Expires - currentTime < TimeSpan.Zero))
                return KeyValueStaticResponses.DoesNotExistContextResponse;

            readOnlyKeyValueEntry = new(
                mvccEntry.Value, 
                mvccEntry.Revision, 
                mvccEntry.Expires, 
                mvccEntry.LastUsed, 
                mvccEntry.LastModified, 
                mvccEntry.State
            );

            return new(KeyValueResponseType.Get, readOnlyKeyValueEntry);
        }
        
        if (entry is null || entry.State is KeyValueState.Undefined or KeyValueState.Deleted || (entry.Expires != HLCTimestamp.Zero && entry.Expires - currentTime < TimeSpan.Zero))
            return KeyValueStaticResponses.DoesNotExistContextResponse;

        entry.LastUsed = currentTime;

        readOnlyKeyValueEntry = new(
            entry.Value, 
            entry.Revision, 
            entry.Expires, 
            entry.LastUsed, 
            entry.LastModified, 
            entry.State
        );

        return new(KeyValueResponseType.Get, readOnlyKeyValueEntry);
    }
}