
using Kahuna.Server.Configuration;
using Nixie;

using Kommander;
using Kommander.Time;

using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Shared.KeyValue;
using Kahuna.Utils;

namespace Kahuna.Server.KeyValues.Handlers;

/// <summary>
/// Handles the execution of delete operations for key-value data. This handler is responsible
/// for processing requests to delete entries in a key-value store while ensuring proper
/// synchronization and persistence via raft consensus and background writing mechanisms.
/// </summary>
internal sealed class TryDeleteHandler : BaseHandler
{
    public TryDeleteHandler(BTree<string, KeyValueContext> keyValuesStore,
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
        
        // Make sure the current time is ahead of the transactionId
        if (message.TransactionId == HLCTimestamp.Zero)
            currentTime = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());
        else
            currentTime = raft.HybridLogicalClock.ReceiveEvent(raft.GetLocalNodeId(), message.TransactionId); // Force currentTime to be higher than the transaction ID
        
        // Validate if there's an exclusive key acquired on the lock and whether it is expired
        // if we find expired write intents we can remove to allow new transactions to proceed
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
            
            if (context.Revision > entry.Revision) // early conflict detection
                return KeyValueStaticResponses.AbortedResponse;
            
            if (context.State == KeyValueState.Deleted)
                return new(KeyValueResponseType.DoesNotExist, entry.Revision);
            
            entry.State = KeyValueState.Deleted;
            entry.LastModified = currentTime;
            
            return new(KeyValueResponseType.Deleted, entry.Revision, entry.LastModified);
        }
        
        if (context.State == KeyValueState.Deleted)
            return new(KeyValueResponseType.DoesNotExist, context.Revision);
        
        KeyValueProposal proposal = new(
            message.Key,
            null,
            context.Revision,
            context.Expires,
            currentTime,
            currentTime,
            KeyValueState.Deleted
        );

        if (message.Durability == KeyValueDurability.Persistent)
        {
            bool success = await PersistAndReplicateKeyValueMessage(message.Type, proposal, currentTime);
            if (!success)
                return KeyValueStaticResponses.ErroredResponse;
        }
        
        context.Value = proposal.Value;
        context.LastUsed = proposal.LastUsed;
        context.LastModified = proposal.LastModified;
        context.State = proposal.State;
        
        return new(KeyValueResponseType.Deleted, context.Revision, context.LastModified);
    }
}