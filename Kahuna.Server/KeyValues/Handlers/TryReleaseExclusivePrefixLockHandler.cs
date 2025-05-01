
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
    public TryReleaseExclusivePrefixLockHandler(BTree<string, KeyValueContext> keyValuesStore,
        Dictionary<string, KeyValueWriteIntent> locksByPrefix,
        IActorRef<BackgroundWriterActor, BackgroundWriteRequest> backgroundWriter,
        IPersistenceBackend persistenceBackend,
        IRaft raft,
        KahunaConfiguration configuration,
        ILogger<IKahuna> logger) : base(keyValuesStore, locksByPrefix, backgroundWriter, persistenceBackend, raft, configuration, logger)
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
        
        HLCTimestamp currentTime = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());
        
        // Check if the prefix is already locked by the current transaction
        if (locksByPrefix.TryGetValue(message.Key, out KeyValueWriteIntent? writeIntent))
        {
            if (writeIntent.TransactionId == message.TransactionId)
            {
                ReleaseExistingLocksByPrefix(currentTime, message);
                
                locksByPrefix.Remove(message.Key);
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
        foreach ((string key, KeyValueContext context) in keyValuesStore.GetByBucket(message.Key))
            TryReleaseLock( message.TransactionId, context);                                                    
    }           
    
    private static void TryReleaseLock(HLCTimestamp transactionId, KeyValueContext context)
    {                               
        if (context.WriteIntent is not null)
        {
            // if the transactionId is the same owner no need to acquire the lock
            if (context.WriteIntent.TransactionId == transactionId)
                context.WriteIntent = null;
        }
    }     
}