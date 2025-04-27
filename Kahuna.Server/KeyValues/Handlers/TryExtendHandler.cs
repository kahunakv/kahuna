
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
    public TryExtendHandler(
        BTree<string, KeyValueContext> keyValuesStore,
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
        
        if (context is null)
            return KeyValueStaticResponses.DoesNotExistResponse;
        
        if (context.WriteIntent is not null && context.WriteIntent.TransactionId != message.TransactionId)
            return new(KeyValueResponseType.MustRetry, 0);
        
        HLCTimestamp currentTime;
        
        if (message.TransactionId == HLCTimestamp.Zero)
            currentTime = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());
        else
            currentTime = raft.HybridLogicalClock.ReceiveEvent(raft.GetLocalNodeId(), message.TransactionId);
        
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
            
            entry.Expires = currentTime + message.ExpiresMs;
            entry.LastUsed = currentTime;
            entry.LastModified = currentTime;
            
            return new(KeyValueResponseType.Extended, entry.Revision, entry.LastModified);
        }
        
        if (context.State == KeyValueState.Deleted)
            return new(KeyValueResponseType.DoesNotExist, context.Revision);
        
        if (context.Expires != HLCTimestamp.Zero && context.Expires - currentTime < TimeSpan.Zero)
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