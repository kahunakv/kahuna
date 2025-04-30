
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
    public TryAcquireExclusivePrefixLockHandler(BTree<string, KeyValueContext> keyValuesStore,
        Dictionary<string, KeyValueWriteIntent> locksByPrefix,
        IActorRef<BackgroundWriterActor, BackgroundWriteRequest> backgroundWriter,
        IPersistenceBackend persistenceBackend,
        IRaft raft,
        KahunaConfiguration configuration,
        ILogger<IKahuna> logger) : base(keyValuesStore, locksByPrefix, backgroundWriter, persistenceBackend, raft, configuration, logger)
    {
        
    }
    
    /// <summary>
    /// Executes the get by prefix request
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    public KeyValueResponse Execute(KeyValueRequest message)
    {
        HLCTimestamp currentTime = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());
        
        // Check if the prefix is already locked by the current transaction
        if (locksByPrefix.TryGetValue(message.Key, out KeyValueWriteIntent? writeIntent))
        {
            if (writeIntent.TransactionId == message.TransactionId) 
                return KeyValueStaticResponses.LockedResponse;

            // Locked by another transaction but check if the lease is still active
            if (writeIntent.Expires != HLCTimestamp.Zero && writeIntent.Expires - currentTime > TimeSpan.Zero)            
                return KeyValueStaticResponses.AlreadyLockedResponse;            
            
            // The lock is expired, remove it
            locksByPrefix.Remove(message.Key);
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
        
        foreach ((string key, KeyValueContext context) in keyValuesStore.GetByPrefix(message.Key))
        {
            KeyValueResponse response = TryLock(currentTime, message.TransactionId, key, message.ExpiresMs, context);                                                    

            if (response.Type != KeyValueResponseType.Locked)
                return response;
        }

        locksByPrefix.Add(message.Key, new()
        {
            TransactionId = message.TransactionId,
            Expires = message.TransactionId + message.ExpiresMs
        });
                
        return KeyValueStaticResponses.LockedResponse;
    }           
    
    private KeyValueResponse TryLock(HLCTimestamp currentTime, HLCTimestamp transactionId, string key, int expiresMs, KeyValueContext context)
    {                               
        if (context.WriteIntent is not null)
        {
            // if the transactionId is the same owner no need to acquire the lock
            if (context.WriteIntent.TransactionId == transactionId) 
                return KeyValueStaticResponses.LockedResponse;

            // Check if the lease is still active
            if (context.WriteIntent.Expires != HLCTimestamp.Zero && context.WriteIntent.Expires - currentTime > TimeSpan.Zero)            
                return KeyValueStaticResponses.AlreadyLockedResponse;
        }

        context.WriteIntent = new()
        {
            TransactionId = transactionId,
            Expires = transactionId + expiresMs,
        };        
        
        logger.LogDebug("Assigned {Key} write intent to TxId={TransactionId}", key, transactionId);
        
        return KeyValueStaticResponses.LockedResponse;
    }     
}