
using Nixie;
using Kommander;
using Kommander.Time;

using Kahuna.Utils;
using Kahuna.Server.Configuration;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Handlers;

/// <summary>
/// Executes the TrySet operation in the key-value store.
/// </summary>
internal sealed class TrySetHandler : BaseHandler
{        
    public TrySetHandler(KeyValueContext context) : base(context)
    {
        
    }
    
    public async Task<KeyValueResponse> Execute(KeyValueRequest message)
    {
        bool exists = true;
        HLCTimestamp currentTime;
        
        if (message.TransactionId == HLCTimestamp.Zero)
            currentTime = context.Raft.HybridLogicalClock.TrySendOrLocalEvent(context.Raft.GetLocalNodeId());
        else
            currentTime = context.Raft.HybridLogicalClock.ReceiveEvent(context.Raft.GetLocalNodeId(), message.TransactionId);

        if (!context.Store.TryGetValue(message.Key, out KeyValueEntry? entry))
        {
            exists = false;
            KeyValueEntry? newEntry = null;

            /// Try to retrieve KeyValue context from persistence
            if (message.Durability == KeyValueDurability.Persistent)
            {
                newEntry = await context.Raft.ReadThreadPool.EnqueueTask(() => context.PersistenceBackend.GetKeyValue(message.Key));
                if (newEntry is not null)
                {
                    if (newEntry.State is KeyValueState.Deleted or KeyValueState.Undefined)
                    {
                        newEntry.Value = null;
                        exists = false;
                    }
                    else
                    {
                        if (newEntry.Expires != HLCTimestamp.Zero && newEntry.Expires - currentTime < TimeSpan.Zero)
                        {
                            newEntry.State = KeyValueState.Deleted;
                            newEntry.Value = null;
                            exists = false;
                        }
                        else
                        {
                            exists = true;
                        }
                    }
                }
            }

            newEntry ??= new() { Bucket = GetBucket(message.Key), State = KeyValueState.Undefined, Revision = -1 };
            
            entry = newEntry;
            
            // logger.LogDebug("{0} {1}", context.State, context.Revision);

            context.Store.Insert(message.Key, newEntry);
        }
        else
        {
            if (entry.Expires != HLCTimestamp.Zero && entry.Expires - currentTime < TimeSpan.Zero)
                entry.State = KeyValueState.Deleted;
            
            if (entry.State is KeyValueState.Deleted or KeyValueState.Undefined)
            {
                entry.Value = null;
                exists = false;
            }
        }
        
        // Validate if there's an exclusive key acquired on the lock and whether it is expired
        // if we find expired write intents we can remove it to allow new transactions to proceed
        if (entry.WriteIntent != null)
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
        if (entry.Bucket is not null && context.LocksByPrefix.TryGetValue(entry.Bucket, out KeyValueWriteIntent? intent))
        {
            if (intent.TransactionId != message.TransactionId)
            {
                if (intent.Expires - currentTime > TimeSpan.Zero)
                    return new(KeyValueResponseType.MustRetry, 0);
            
                context.LocksByPrefix.Remove(entry.Bucket);
            }
        }

        HLCTimestamp newExpires = message.ExpiresMs > 0 ? (currentTime + message.ExpiresMs) : HLCTimestamp.Zero;
        
        // Temporarily store the value in the MVCC entry if the transaction ID is set
        if (message.TransactionId != HLCTimestamp.Zero)
        {
            exists = true;
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

            if (mvccEntry.State is KeyValueState.Deleted or KeyValueState.Undefined)
            {
                mvccEntry.Value = null;
                exists = false;
            }
            else
            {
                if (mvccEntry.Expires != HLCTimestamp.Zero && mvccEntry.Expires - currentTime < TimeSpan.Zero)
                {
                    mvccEntry.State = KeyValueState.Deleted;
                    mvccEntry.Value = null;
                    exists = false;
                }
            }
            
            // logger.LogDebug("Key={0} Flags={1} Rev={2} CmpRev={3}", message.Key, message.Flags, entry.Revision, message.CompareRevision);
            
            /*switch (message.Flags)
            {
                case KeyValueFlags.SetIfExists when !exists:
                case KeyValueFlags.SetIfNotExists when exists:
                case KeyValueFlags.SetIfEqualToValue when !((ReadOnlySpan<byte>)entry.Value).SequenceEqual(message.CompareValue): // exists &&
                case KeyValueFlags.SetIfEqualToRevision when entry.Revision != message.CompareRevision: // exists && 
                    return new(KeyValueResponseType.NotSet, entry.Revision, entry.LastModified);

                case KeyValueFlags.None:
                case KeyValueFlags.Set:
                case KeyValueFlags.SetNoRevision:
                default:
                    break;
            }*/ 
            
            // Check if the value must not be changed according to flags
            if (
                (message.Flags & KeyValueFlags.SetIfExists) != 0 && !exists || 
                (message.Flags & KeyValueFlags.SetIfNotExists) != 0 && exists || 
                (message.Flags & KeyValueFlags.SetIfEqualToValue) != 0 && !((ReadOnlySpan<byte>)mvccEntry.Value).SequenceEqual(message.CompareValue) || 
                (message.Flags & KeyValueFlags.SetIfEqualToRevision) != 0 && mvccEntry.Revision != message.CompareRevision
            )
                return new(KeyValueResponseType.NotSet, mvccEntry.Revision, mvccEntry.LastModified);

            mvccEntry.Value = message.Value;
            mvccEntry.Expires = newExpires;
            mvccEntry.Revision++;
            mvccEntry.LastUsed = currentTime;
            mvccEntry.LastModified = currentTime;
            mvccEntry.State = KeyValueState.Set;                       
            
            return new(KeyValueResponseType.Set, mvccEntry.Revision, currentTime);
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
        
        /*switch (message.Flags)
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
        }*/
        
        // Check if the value must not be changed according to flags
        if (
            (message.Flags & KeyValueFlags.SetIfExists) != 0 && !exists || 
            (message.Flags & KeyValueFlags.SetIfNotExists) != 0 && exists || 
            (message.Flags & KeyValueFlags.SetIfEqualToValue) != 0 && !((ReadOnlySpan<byte>)entry.Value).SequenceEqual(message.CompareValue) || 
            (message.Flags & KeyValueFlags.SetIfEqualToRevision) != 0 && entry.Revision != message.CompareRevision
        )
            return new(KeyValueResponseType.NotSet, entry.Revision, entry.LastModified);
        
        KeyValueProposal proposal = new(
            message.Key,
            message.Value,
            entry.Revision + 1,
            (message.Flags & KeyValueFlags.SetNoRevision) != 0,
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

        if (entry.Revisions is not null)
            RemoveExpiredRevisions(entry, proposal.Revision);        
        
        entry.Revisions ??= new();                       
        entry.Revisions.Add(entry.Revision, entry.Value);
        
        entry.Value = proposal.Value;
        entry.Revision = proposal.Revision;
        entry.Expires = proposal.Expires;
        entry.LastUsed = proposal.LastUsed;
        entry.LastModified = proposal.LastModified;
        entry.State = proposal.State;

        return new(KeyValueResponseType.Set, entry.Revision);
    }   
}

