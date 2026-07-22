
using Kahuna.Server.Configuration;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Shared.KeyValue;
using Kahuna.Utils;
using Kommander;
using Kommander.Time;
using Nixie;

namespace Kahuna.Server.KeyValues.Handlers;

/// <summary>
/// Handles the execution of key-value operations related to extending the expiration of a given key in the B-tree store.
/// </summary>
/// <seealso cref="BaseHandler"/>
internal sealed class TryExtendHandler : BaseHandler
{
    public TryExtendHandler(KeyValueContext context) : base(context)
    {
        
    }

    public async Task<KeyValueResponse> Execute(KeyValueRequest message)
    {
        KeyValueEntry? entry = await GetKeyValueEntry(message.Key, message.Durability);

        // Deferred-settlement writer visibility: a foreign durable prepared intent covering this key may hold a
        // committed value not yet materialized locally, so an intent-only committed key is not really missing.
        // Resolve its canonical outcome before treating the key as absent — a committed set materializes into a
        // resident entry to extend, an undecided intent retries, and an aborted or committed-delete intent leaves
        // the key absent. No-op off the durable-intent path.
        if (ForeignIntentWriteResolver.Resolve(
                context, message.Key, message.TransactionId, ref entry, ApplyCommittedHead)
            == ForeignIntentWriteDecision.MustRetry)
            return KeyValueStaticResponses.MustRetryResponse;

        if (entry is null)
            return KeyValueStaticResponses.DoesNotExistResponse;
        
        HLCTimestamp currentTime;
        
        if (message.TransactionId == HLCTimestamp.Zero)
            currentTime = context.Raft.HybridLogicalClock.TrySendOrLocalEvent(context.Raft.GetLocalNodeId());
        else
            currentTime = context.Raft.HybridLogicalClock.ReceiveEvent(context.Raft.GetLocalNodeId(), message.TransactionId);
        
        // Validate if there's an active replication enty on the key/value entry
        // clients must retry operations to make sure the entry is fully replicated
        // before modifying the entry
        if (entry.ReplicationIntent is not null)
        {
            if (entry.ReplicationIntent.Expires - currentTime > TimeSpan.Zero)                
                return KeyValueStaticResponses.WaitingForReplicationResponse;
                
            entry.ReplicationIntent = null;
        }
        
        // Validate if there's an exclusive key acquired on the lock and whether it is expired
        // if we find expired write intents we can remove it to allow new transactions to proceed
        if (entry.WriteIntent is not null)
        {
            if (entry.WriteIntent.TransactionId != message.TransactionId)
            {
                if (KeyValueWriteIntentLease.IsLive(entry.WriteIntent, currentTime))
                    return KeyValueStaticResponses.MustRetryResponse;
                
                entry.WriteIntent = null;
            }
        }
        
        // Validate if there's a prefix lock acquired on the bucket
        // if we find expired write intents we can remove it to allow new transactions to proceed
        if (entry.Bucket is not null && context.LocksByPrefix.TryGetValue(entry.Bucket, out KeyValueWriteIntent? intent))
        {
            if (intent.TransactionId != message.TransactionId)
            {
                if (KeyValueWriteIntentLease.IsLive(intent, currentTime))
                    return KeyValueStaticResponses.MustRetryResponse;
            
                context.LocksByPrefix.Remove(entry.Bucket);
            }
        }
        
        // Temporarily store the value in the MVCC entry if the transaction ID is set
        if (message.TransactionId != HLCTimestamp.Zero)
        {
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
            
            if (mvccEntry.State == KeyValueState.Deleted)
                return new(KeyValueResponseType.DoesNotExist, mvccEntry.Revision);
            
            if (mvccEntry.Expires != HLCTimestamp.Zero && mvccEntry.Expires - currentTime < TimeSpan.Zero)
                return new(KeyValueResponseType.DoesNotExist, entry.Revision);
            
            if (entry.Revision > mvccEntry.Revision) // early conflict detection
                return KeyValueStaticResponses.AbortedResponse;
            
            mvccEntry.Expires = currentTime + message.ExpiresMs;
            mvccEntry.LastUsed = currentTime;
            mvccEntry.LastModified = currentTime;
            
            return new(KeyValueResponseType.Extended, mvccEntry.Revision, mvccEntry.LastModified);
        }
        
        if (entry.State == KeyValueState.Deleted || (entry.Expires != HLCTimestamp.Zero && entry.Expires - currentTime < TimeSpan.Zero))
            return new(KeyValueResponseType.DoesNotExist, entry.Revision);

        KeyValueProposal proposal = new(
            message.Type,
            message.Key,
            entry.Value,
            entry.Revision,
            false,
            currentTime + message.ExpiresMs,
            currentTime,
            currentTime,
            entry.State,
            message.Durability
        );
        
        if (message.Durability == KeyValueDurability.Persistent)
            return CreateProposal(message, entry, proposal, currentTime);

        ApplyCommittedHead(entry, proposal, message.TransactionId);
        entry.FlushedRevision = entry.Revision;

        return new(KeyValueResponseType.Extended, entry.Revision, entry.LastModified);
    }
}
