
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
    public TryExtendHandler(BTree<string, KeyValueContext> keyValuesStore,
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
        
        if (context is null)
            return KeyValueStaticResponses.DoesNotExistResponse;
        
        HLCTimestamp currentTime;
        
        if (message.TransactionId == HLCTimestamp.Zero)
            currentTime = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());
        else
            currentTime = raft.HybridLogicalClock.ReceiveEvent(raft.GetLocalNodeId(), message.TransactionId);
        
        // Validate if there's an exclusive key acquired on the lock and whether it is expired
        // if we find expired write intents we can remove it to allow new transactions to proceed
        if (context.WriteIntent is not null)
        {
            if (context.WriteIntent.TransactionId != message.TransactionId)
            {
                if (context.WriteIntent.Expires - currentTime > TimeSpan.Zero)                
                    return new(KeyValueResponseType.MustRetry, 0);
                
                context.WriteIntent = null;
            }
        }
        
        // Validate if there's a prefix lock acquired on the bucket
        // if we find expired write intents we can remove it to allow new transactions to proceed
        if (context.Bucket is not null && locksByPrefix.TryGetValue(context.Bucket, out KeyValueWriteIntent? intent))
        {
            if (intent.TransactionId != message.TransactionId)
            {
                if (intent.Expires - currentTime > TimeSpan.Zero)
                    return new(KeyValueResponseType.MustRetry, 0);
            
                locksByPrefix.Remove(context.Bucket);
            }
        }
        
        // Temporarily store the value in the MVCC entry if the transaction ID is set
        if (message.TransactionId != HLCTimestamp.Zero)
        {
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
            
            if (entry.State == KeyValueState.Deleted)
                return new(KeyValueResponseType.DoesNotExist, entry.Revision);
            
            if (entry.Expires != HLCTimestamp.Zero && entry.Expires - currentTime < TimeSpan.Zero)
                return new(KeyValueResponseType.DoesNotExist, context.Revision);
            
            if (context.Revision > entry.Revision) // early conflict detection
                return KeyValueStaticResponses.AbortedResponse;
            
            entry.Expires = currentTime + message.ExpiresMs;
            entry.LastUsed = currentTime;
            entry.LastModified = currentTime;
            
            return new(KeyValueResponseType.Extended, entry.Revision, entry.LastModified);
        }
        
        if (context.State == KeyValueState.Deleted || (context.Expires != HLCTimestamp.Zero && context.Expires - currentTime < TimeSpan.Zero))
            return new(KeyValueResponseType.DoesNotExist, context.Revision);

        KeyValueProposal proposal = new(
            message.Key,
            context.Value,
            context.Revision,
            currentTime + message.ExpiresMs,
            currentTime,
            currentTime,
            context.State
        );
        
        if (message.Durability == KeyValueDurability.Persistent)
        {
            bool success = await PersistAndReplicateKeyValueMessage(message.Type, proposal, currentTime);
            if (!success)
                return KeyValueStaticResponses.ErroredResponse;
        }
        
        context.Expires = proposal.Expires;
        context.LastUsed = proposal.LastUsed;
        context.LastModified = proposal.LastModified;

        return new(KeyValueResponseType.Extended, context.Revision, context.LastModified);
    }
}