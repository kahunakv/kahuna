
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
                KeyValueEntry? revisionContext = await context.Raft.ReadScheduler.EnqueueTask(message.PartitionId, () => context.PersistenceBackend.GetKeyValueRevision(message.Key, message.CompareRevision));
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
        
        // Validate if there's an active replication enty on the key/value entry
        // clients must retry operations to make sure the entry is fully replicated
        // before modifying the entry
        if (entry?.ReplicationIntent is not null)
        {
            if (entry.ReplicationIntent.Expires - currentTime > TimeSpan.Zero)                
                return KeyValueStaticResponses.WaitingForReplicationResponse;
                
            entry.ReplicationIntent = null;
        }
        
        // Mirror TryGetHandler safe-time logic: when a snapshot readTimestamp is in play, a live
        // foreign write intent whose pending commit could land at-or-before T must be awaited.
        if (entry?.WriteIntent != null)
        {
            if (entry.WriteIntent.TransactionId != message.TransactionId)
            {
                if (entry.WriteIntent.Expires - currentTime <= TimeSpan.Zero)
                    entry.WriteIntent = null;
                else if (!message.ReadTimestamp.IsNull())
                {
                    HLCTimestamp commitTs = entry.WriteIntent.CommitTimestamp;
                    if (commitTs.IsNull() || commitTs.CompareTo(message.ReadTimestamp) <= 0)
                        return KeyValueStaticResponses.WaitingForReplicationResponse;
                    // commitTs > readTimestamp: write won't land in our snapshot; fall through
                }
                // live write intent from another tx without snapshot: fall through to committed state
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
                    return KeyValueStaticResponses.MustRetryResponse;
            
                context.LocksByPrefix.Remove(bucket);
            }
        }

        // TransactionId is provided so we keep a MVCC entry for it.
        // Exception: when readTimestamp is also set, this is an AS OF snapshot read — skip MVCC
        // tracking and fall through to the snapshot visibility path below.
        if (message.TransactionId != HLCTimestamp.Zero && message.ReadTimestamp.IsNull())
        {
            if (entry is null)
            {
                entry = new() { Bucket = GetBucket(message.Key), State = KeyValueState.Undefined, Revision = -1 };
                context.InsertStoreEntry(message.Key, entry);
            }

            entry.MvccEntries ??= new();

            if (!entry.MvccEntries.TryGetValue(message.TransactionId, out KeyValueMvccEntry? mvccEntry))
            {
                bool mvccDictJustCreated = entry.MvccEntries.Count == 0;
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
                context.AdjustEstimatedEntryBytes(entry, KeyValueStoreAccounting.MvccEntryAddedBytes(mvccDictJustCreated, mvccEntry.Value));
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

        // Snapshot visibility (non-transactional or AS OF within transaction).
        if (!message.ReadTimestamp.IsNull() && entry is not null && entry.LastModified > message.ReadTimestamp)
        {
            if (!entry.TryGetRevisionAtOrBefore(message.ReadTimestamp, out long snapRevision, out KeyValueRevisionEntry snapshot))
                return KeyValueStaticResponses.DoesNotExistContextResponse;

            if (snapshot.State is KeyValueState.Deleted or KeyValueState.Undefined ||
                (snapshot.Expires != HLCTimestamp.Zero && snapshot.Expires - currentTime < TimeSpan.Zero))
                return KeyValueStaticResponses.DoesNotExistContextResponse;

            return new(KeyValueResponseType.Exists, new ReadOnlyKeyValueEntry(
                null,
                snapRevision,
                snapshot.Expires,
                currentTime,
                snapshot.LastModified,
                snapshot.State));
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
