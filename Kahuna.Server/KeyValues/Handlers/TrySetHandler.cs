
using System.Text;
using Kahuna.Server.Persistence;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Time;
using Nixie;

namespace Kahuna.Server.KeyValues.Handlers;

internal sealed class TrySetHandler : BaseHandler
{
    public TrySetHandler(
        Dictionary<string, KeyValueContext> keyValuesStore,
        IActorRef<BackgroundWriterActor, BackgroundWriteRequest> backgroundWriter,
        IPersistence persistence,
        IRaft raft,
        ILogger<IKahuna> logger
    ) : base(keyValuesStore, backgroundWriter, persistence, raft, logger)
    {
        
    }
    
    public async Task<KeyValueResponse> Execute(KeyValueRequest message)
    {
        bool exists = true;
        HLCTimestamp currentTime = raft.HybridLogicalClock.TrySendOrLocalEvent();

        if (!keyValuesStore.TryGetValue(message.Key, out KeyValueContext? context))
        {
            exists = false;
            KeyValueContext? newContext = null;

            /// Try to retrieve KeyValue context from persistence
            if (message.Durability == KeyValueDurability.Persistent)
            {
                newContext = await raft.ReadThreadPool.EnqueueTask(() => persistence.GetKeyValue(message.Key));
                if (newContext is not null)
                {
                    if (newContext.State == KeyValueState.Deleted)
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

            newContext ??= new() { Revision = -1 };
            
            context = newContext;

            keyValuesStore.Add(message.Key, newContext);
        }
        else
        {
            if (context.State == KeyValueState.Deleted)
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
                    State = context.State
                };
                
                context.MvccEntries.Add(message.TransactionId, entry);
            }

            if (entry.State == KeyValueState.Deleted)
            {
                entry.Value = null;
                exists = false;
            }

            if (entry.Expires != HLCTimestamp.Zero && entry.Expires - currentTime < TimeSpan.Zero)
            {
                entry.State = KeyValueState.Deleted;
                entry.Value = null;
                exists = false;
            }

            switch (message.Flags)
            {
                case KeyValueFlags.SetIfExists when !exists:
                case KeyValueFlags.SetIfNotExists when exists:
                case KeyValueFlags.SetIfEqualToValue when exists && !((ReadOnlySpan<byte>)entry.Value).SequenceEqual(message.CompareValue):
                case KeyValueFlags.SetIfEqualToRevision when exists && entry.Revision != message.CompareRevision:
                    return new(KeyValueResponseType.NotSet, entry.Revision);

                case KeyValueFlags.None:
                case KeyValueFlags.Set:
                default:
                    break;
            }

            entry.Value = message.Value;
            entry.Expires = newExpires;
            entry.Revision++;
            entry.LastUsed = currentTime;
            entry.State = KeyValueState.Set;
            
            return new(KeyValueResponseType.Set, context.Revision);
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
            case KeyValueFlags.SetIfEqualToValue when exists && !((ReadOnlySpan<byte>)context.Value).SequenceEqual(message.CompareValue):
            case KeyValueFlags.SetIfEqualToRevision when exists && context.Revision != message.CompareRevision:
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
            KeyValueState.Set
        );

        if (message.Durability == KeyValueDurability.Persistent)
        {
            bool success = await PersistAndReplicateKeyValueMessage(message.Type, proposal, currentTime);
            if (!success)
                return KeyValueStaticResponses.ErroredResponse;
        }
        
        context.Value = proposal.Value;
        context.Revision = proposal.Revision;
        context.Expires = proposal.Expires;
        context.LastUsed = proposal.LastUsed;
        context.State = proposal.State;

        return new(KeyValueResponseType.Set, context.Revision);
    }
}