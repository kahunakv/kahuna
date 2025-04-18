
using Nixie;

using Kommander;
using Kommander.Time;

using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Handlers;

internal sealed class TryDeleteHandler : BaseHandler
{
    public TryDeleteHandler(
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
        if (context is null)
            return KeyValueStaticResponses.DoesNotExistResponse;
        
        if (context.WriteIntent is not null && context.WriteIntent.TransactionId != message.TransactionId)
            return new(KeyValueResponseType.MustRetry, 0);
        
        HLCTimestamp currentTime;
        
        if (message.TransactionId == HLCTimestamp.Zero)
            currentTime = raft.HybridLogicalClock.TrySendOrLocalEvent();
        else
            currentTime = raft.HybridLogicalClock.ReceiveEvent(message.TransactionId); // Force currentTime to be higher than the transaction ID
        
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