
using Kahuna.Server.Configuration;
using Nixie;
using Kommander;
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
        HLCTimestamp currentTime = context.Raft.HybridLogicalClock.TrySendOrLocalEvent(context.Raft.GetLocalNodeId());
        
        // Check if the prefix is already locked by the current transaction
        if (context.LocksByPrefix.TryGetValue(message.Key, out KeyValueWriteIntent? writeIntent))
        {
            if (writeIntent.TransactionId == message.TransactionId) 
                return KeyValueStaticResponses.LockedResponse;

            // Locked by another transaction but check if the lease is still active
            if (writeIntent.Expires != HLCTimestamp.Zero && writeIntent.Expires - currentTime > TimeSpan.Zero)            
                return KeyValueStaticResponses.AlreadyLockedResponse;            
            
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
        if (message.TransactionId == HLCTimestamp.Zero)
            return KeyValueStaticResponses.ErroredResponse;               
        
        foreach ((string key, KeyValueEntry entry) in context.Store.GetByBucket(message.Key))
        {
            KeyValueResponse response = TryLock(currentTime, message.TransactionId, key, message.ExpiresMs, entry);                                                    

            if (response.Type != KeyValueResponseType.Locked)
                return response;
        }

        context.LocksByPrefix.Add(message.Key, new()
        {
            TransactionId = message.TransactionId,
            Expires = message.TransactionId + message.ExpiresMs
        });
                
        return KeyValueStaticResponses.LockedResponse;
    }           
    
    private KeyValueResponse TryLock(HLCTimestamp currentTime, HLCTimestamp transactionId, string key, int expiresMs, KeyValueEntry entry)
    {                               
        if (entry.WriteIntent is not null)
        {
            // if the transactionId is the same owner no need to acquire the lock
            if (entry.WriteIntent.TransactionId == transactionId) 
                return KeyValueStaticResponses.LockedResponse;

            // Check if the lease is still active
            if (entry.WriteIntent.Expires != HLCTimestamp.Zero && entry.WriteIntent.Expires - currentTime > TimeSpan.Zero)            
                return KeyValueStaticResponses.AlreadyLockedResponse;
        }

        entry.WriteIntent = new()
        {
            TransactionId = transactionId,
            Expires = transactionId + expiresMs,
        };        
        
        context.Logger.LogDebug("Assigned {Key} write intent to TxId={TransactionId}", key, transactionId);
        
        return KeyValueStaticResponses.LockedResponse;
    }     
}