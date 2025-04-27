
using Nixie;
using Kommander;
using Kommander.Time;

using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Shared.KeyValue;
using Kahuna.Utils;

namespace Kahuna.Server.KeyValues.Handlers;

internal sealed class TrySetHandler : BaseHandler
{        
    public TrySetHandler(
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
        bool exists = true;
        HLCTimestamp currentTime;
        
        if (message.TransactionId == HLCTimestamp.Zero)
            currentTime = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());
        else
            currentTime = raft.HybridLogicalClock.ReceiveEvent(raft.GetLocalNodeId(), message.TransactionId);

        if (!keyValuesStore.TryGetValue(message.Key, out KeyValueContext? context))
        {
            exists = false;
            KeyValueContext? newContext = null;

            /// Try to retrieve KeyValue context from persistence
            if (message.Durability == KeyValueDurability.Persistent)
            {
                newContext = await raft.ReadThreadPool.EnqueueTask(() => PersistenceBackend.GetKeyValue(message.Key));
                if (newContext is not null)
                {
                    if (newContext.State is KeyValueState.Deleted or KeyValueState.Undefined)
                    {
                        newContext.Value = null;
                        exists = false;
                    }
                    else
                    {
                        if (newContext.Expires != HLCTimestamp.Zero && newContext.Expires - currentTime < TimeSpan.Zero)
                        {
                            newContext.State = KeyValueState.Deleted;
                            newContext.Value = null;
                            exists = false;
                        }
                        else
                        {
                            exists = true;
                        }
                    }
                }
            }

            newContext ??= new() { State = KeyValueState.Undefined, Revision = -1 };
            
            context = newContext;
            
            // logger.LogDebug("{0} {1}", context.State, context.Revision);

            keyValuesStore.Insert(message.Key, newContext);
        }
        else
        {
            if (context.State is KeyValueState.Deleted or KeyValueState.Undefined)
            {
                context.Value = null;
                exists = false;
            }
        }
        
        if (context.WriteIntent is not null && context.WriteIntent.TransactionId != message.TransactionId)
            return new(KeyValueResponseType.MustRetry, 0);

        HLCTimestamp newExpires = message.ExpiresMs > 0 ? (currentTime + message.ExpiresMs) : HLCTimestamp.Zero;
        
        // Temporarily store the value in the MVCC entry if the transaction ID is set
        if (message.TransactionId != HLCTimestamp.Zero)
        {
            exists = true;
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

            if (entry.State is KeyValueState.Deleted or KeyValueState.Undefined)
            {
                entry.Value = null;
                exists = false;
            }
            else
            {
                if (entry.Expires != HLCTimestamp.Zero && entry.Expires - currentTime < TimeSpan.Zero)
                {
                    entry.State = KeyValueState.Deleted;
                    entry.Value = null;
                    exists = false;
                }
            }
            
            // logger.LogDebug("Key={0} Flags={1} Rev={2} CmpRev={3}", message.Key, message.Flags, entry.Revision, message.CompareRevision);
            
            switch (message.Flags)
            {
                case KeyValueFlags.SetIfExists when !exists:
                case KeyValueFlags.SetIfNotExists when exists:
                case KeyValueFlags.SetIfEqualToValue when !((ReadOnlySpan<byte>)entry.Value).SequenceEqual(message.CompareValue): // exists &&
                case KeyValueFlags.SetIfEqualToRevision when entry.Revision != message.CompareRevision: // exists && 
                    return new(KeyValueResponseType.NotSet, entry.Revision, entry.LastModified);

                case KeyValueFlags.None:
                case KeyValueFlags.Set:
                default:
                    break;
            }

            entry.Value = message.Value;
            entry.Expires = newExpires;
            entry.Revision++;
            entry.LastUsed = currentTime;
            entry.LastModified = currentTime;
            entry.State = KeyValueState.Set;
                       
            
            return new(KeyValueResponseType.Set, entry.Revision, currentTime);
        }
        
        /*if (message.CompareValue is not null)
            Console.WriteLine(
                "{0} {1} {2} {3} {4} {5}", 
                exists, 
                Encoding.UTF8.GetString(context.Value ?? []), 
                Encoding.UTF8.GetString(message.Value ?? []), 
                Encoding.UTF8.GetString(message.CompareValue ?? []),
                context.Revision,
                message.CompareRevision
            );*/
        
        switch (message.Flags)
        {
            case KeyValueFlags.SetIfExists when !exists:
            case KeyValueFlags.SetIfNotExists when exists:
            case KeyValueFlags.SetIfEqualToValue when !((ReadOnlySpan<byte>)context.Value).SequenceEqual(message.CompareValue): // exists && 
            case KeyValueFlags.SetIfEqualToRevision when context.Revision != message.CompareRevision: // exists && 
                return new(KeyValueResponseType.NotSet, context.Revision);

            case KeyValueFlags.None:
            case KeyValueFlags.Set:
            default:
                break;
        }
        
        KeyValueProposal proposal = new(
            message.Key,
            message.Value,
            context.Revision + 1,
            newExpires,
            currentTime,
            currentTime,
            KeyValueState.Set
        );

        if (message.Durability == KeyValueDurability.Persistent)
        {
            bool success = await PersistAndReplicateKeyValueMessage(message.Type, proposal, currentTime);
            if (!success)
                return KeyValueStaticResponses.ErroredResponse;
        }

        if (context.Revisions is not null)
            RemoveExpiredRevisions(context, proposal.Revision);        
        
        context.Revisions ??= new();                       
        context.Revisions.Add(context.Revision, context.Value);
        
        context.Value = proposal.Value;
        context.Revision = proposal.Revision;
        context.Expires = proposal.Expires;
        context.LastUsed = proposal.LastUsed;
        context.LastModified = proposal.LastModified;
        context.State = proposal.State;

        return new(KeyValueResponseType.Set, context.Revision);
    }   
}