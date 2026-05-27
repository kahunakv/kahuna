
using Nixie;
using Kommander;
using Kommander.Time;

using Kahuna.Server.Configuration;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Utils;

namespace Kahuna.Server.KeyValues.Handlers;

internal sealed class TryReleaseExclusivePrefixLockHandler : BaseHandler
{        
    public TryReleaseExclusivePrefixLockHandler(KeyValueContext context) : base(context)
    {
        
    }
    
    /// <summary>
    /// Executes the release of the exclusive prefix lock.
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    public KeyValueResponse Execute(KeyValueRequest message)
    {
        if (message.TransactionId == HLCTimestamp.Zero)
            return KeyValueStaticResponses.ErroredResponse;
        
        HLCTimestamp currentTime = context.Raft.HybridLogicalClock.TrySendOrLocalEvent(context.Raft.GetLocalNodeId());
        
        // Check if the prefix is already locked by the current transaction
        if (context.LocksByPrefix.TryGetValue(message.Key, out KeyValueWriteIntent? writeIntent))
        {
            if (writeIntent.TransactionId == message.TransactionId)
            {
                ReleaseExistingLocksByPrefix(currentTime, message);
                
                context.LocksByPrefix.Remove(message.Key);
            }
        }

        return KeyValueStaticResponses.UnlockedResponse;
    }
    
    /// <summary>
    /// Unlocks entries matching the specified prefix in an ephemeral durability.
    /// Even keys that do not exist or are deleted will be locked because the prefix lock
    /// works as predicate locking for the group of keys.
    /// </summary>
    /// <param name="currentTime"></param> 
    /// <param name="message"></param>
    /// <returns></returns>
    private void ReleaseExistingLocksByPrefix(HLCTimestamp currentTime, KeyValueRequest message)
    {
        foreach ((string key, KeyValueEntry entry) in context.Store.GetByBucket(message.Key))
            TryReleaseLock( message.TransactionId, entry);                                                    
    }           
    
    private static void TryReleaseLock(HLCTimestamp transactionId, KeyValueEntry entry)
    {
        //if (entry.ReplicationIntent is not null)
        //    return KeyValueStaticResponses.WaitingForReplicationResponse;
        
        if (entry.WriteIntent is not null)
        {
            // if the transactionId is the same owner no need to acquire the lock
            if (entry.WriteIntent.TransactionId == transactionId)
                entry.WriteIntent = null;
        }
    }     
}