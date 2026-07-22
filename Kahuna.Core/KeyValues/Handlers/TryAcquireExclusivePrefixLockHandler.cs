
using Kahuna.Server.Configuration;
using Nixie;
using Kommander;
using Kahuna.Server.KeyValues.Logging;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Shared.KeyValue;
using Kahuna.Utils;
using Kommander.Time;

namespace Kahuna.Server.KeyValues.Handlers;

/// <summary>
/// Handles the process of attempting to acquire an exclusive lock on a group of key-value resources
/// prefixed by a given prefix-key.
/// </summary>
/// <see cref="BaseHandler"/>
internal sealed class TryAcquireExclusivePrefixLockHandler : BaseHandler
{        
    public TryAcquireExclusivePrefixLockHandler(KeyValueContext context) : base(context)
    {
        
    }
    
    /// <summary>
    /// Executes the get by bucket request
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    public KeyValueResponse Execute(KeyValueRequest message)
    {
        if (message.TransactionId == HLCTimestamp.Zero || message.ExpiresMs < 0)
            return KeyValueStaticResponses.ErroredResponse;

        HLCTimestamp currentTime = context.Raft.HybridLogicalClock.TrySendOrLocalEvent(context.Raft.GetLocalNodeId());
        
        // Check if the prefix is already locked by the current transaction
        if (context.LocksByPrefix.TryGetValue(message.Key, out KeyValueWriteIntent? writeIntent))
        {
            if (writeIntent.TransactionId == message.TransactionId)
            {
                HLCTimestamp renewedExpiry = KeyValueWriteIntentLease.FromRequest(currentTime, message.ExpiresMs);
                writeIntent.Expires = renewedExpiry;
                foreach ((string _, KeyValueEntry entry) in context.Store.GetByBucket(message.Key))
                {
                    if (entry.WriteIntent?.TransactionId == message.TransactionId)
                        entry.WriteIntent.Expires = renewedExpiry;
                }
                return KeyValueStaticResponses.LockedResponse;
            }

            // Locked by another transaction but check if the lease is still active
            if (KeyValueWriteIntentLease.IsLive(writeIntent, currentTime))
                return KeyValueResponse.Denied(KeyValueResponseType.AlreadyLocked, writeIntent.TransactionId);
            
            // The lock is expired, remove it
            context.LocksByPrefix.Remove(message.Key);
        }
                
        return LockExistingKeysByPrefix(currentTime, message);               
    }
    
    /// <summary>
    /// Locks entries matching the specified prefix in an ephemeral durability.
    /// Even keys that do not exist or are deleted will be locked because the prefix lock
    /// works as predicate locking for the group of keys.
    /// </summary>
    /// <param name="currentTime"></param> 
    /// <param name="message"></param>
    /// <returns></returns>
    private KeyValueResponse LockExistingKeysByPrefix(HLCTimestamp currentTime, KeyValueRequest message)
    {
        // Stamp per-key write intents atomically: if any key in the bucket is mid-replication we abort
        // and roll back every intent already written this call, so a mid-loop failure never strands a
        // partial set of intents on the bucket's keys. The single LocksByPrefix.Add below is the O(1)
        // record that actually blocks the whole bucket (including phantom keys) on the write path.
        List<(KeyValueEntry Entry, KeyValueWriteIntent? Prior)>? stamped = null;

        foreach ((string key, KeyValueEntry entry) in context.Store.GetByBucket(message.Key))
        {
            // clients must retry operations until an in-flight replication on the key completes,
            // before its intent can be safely overwritten.
            if (entry.ReplicationIntent is not null)
            {
                if (entry.ReplicationIntent.Expires - currentTime > TimeSpan.Zero)
                {
                    if (stamped is not null)
                        foreach ((KeyValueEntry rollback, KeyValueWriteIntent? prior) in stamped)
                            rollback.WriteIntent = prior;

                    return KeyValueStaticResponses.WaitingForReplicationResponse;
                }

                entry.ReplicationIntent = null;
            }

            if (entry.WriteIntent is not null)
            {
                // if the transactionId is the same owner no need to re-stamp the intent
                if (entry.WriteIntent.TransactionId == message.TransactionId)
                    continue;

                // Another transaction holds a live write intent on this key. Leave it in place —
                // the LocksByPrefix entry is enough to block that transaction's commit as a phantom.
                if (KeyValueWriteIntentLease.IsLive(entry.WriteIntent, currentTime))
                    continue;
            }

            stamped ??= [];
            stamped.Add((entry, entry.WriteIntent));

            entry.WriteIntent = new()
            {
                TransactionId = message.TransactionId,
                Expires = KeyValueWriteIntentLease.FromRequest(currentTime, message.ExpiresMs),
            };

            context.Logger.LogAssignedWriteIntent(key, message.TransactionId);
        }

        context.LocksByPrefix.Add(message.Key, new()
        {
            TransactionId = message.TransactionId,
            Expires = KeyValueWriteIntentLease.FromRequest(currentTime, message.ExpiresMs)
        });

        return KeyValueStaticResponses.LockedResponse;
    }
}
