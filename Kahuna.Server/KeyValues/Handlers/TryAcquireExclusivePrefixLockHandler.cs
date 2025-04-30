
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
    public TryAcquireExclusivePrefixLockHandler(
        BTree<string, KeyValueContext> keyValuesStore,
        IActorRef<BackgroundWriterActor, BackgroundWriteRequest> backgroundWriter,
        IPersistenceBackend persistenceBackend,
        IRaft raft,
        KahunaConfiguration configuration,
        ILogger<IKahuna> logger
    ) : base(keyValuesStore, backgroundWriter, persistenceBackend, raft, configuration, logger)
    {
        
    }
    
    /// <summary>
    /// Executes the get by prefix request
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    public async Task<KeyValueResponse> Execute(KeyValueRequest message)
    {
        if (message.Durability == KeyValueDurability.Ephemeral)
            return await LockByPrefixEphemeral(message);
        
        return await LockByPrefixPersistent(message);
    }
    
    /// <summary>
    /// Queries the key-value store for entries matching the specified prefix in an ephemeral context.
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    private async Task<KeyValueResponse> LockByPrefixEphemeral(KeyValueRequest message)
    {        
        HLCTimestamp currentTime = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());
        
        foreach ((string key, KeyValueContext? _) in keyValuesStore.GetByPrefix(message.Key))
        {
            KeyValueResponse response = await Get(currentTime, message.TransactionId, key, message.Durability);     
            
            if (response.Type == KeyValueResponseType.DoesNotExist)
                continue;
            
            if 

            if (response.Type != KeyValueResponseType.Get)
                return new(response.Type);
        }        
                
        return new(KeyValueResponseType.Locked);
    }
    
    private async Task<KeyValueResponse> Get(HLCTimestamp currentTime, HLCTimestamp transactionId, string key, KeyValueDurability durability, ReadOnlyKeyValueContext? keyValueContext = null)
    {
        KeyValueContext? context = await GetKeyValueContext(key, durability, keyValueContext);

        ReadOnlyKeyValueContext readOnlyKeyValueContext;
        
        if (context?.WriteIntent != null && context.WriteIntent.TransactionId != transactionId)
            return new(KeyValueResponseType.MustRetry, 0);

        // TransactionId is provided so we keep a MVCC entry for it
        if (transactionId != HLCTimestamp.Zero)
        {
            if (context is null)
            {
                context = new() { State = KeyValueState.Undefined, Revision = -1 };
                keyValuesStore.Insert(key, context);
            }
            
            context.MvccEntries ??= new();

            if (!context.MvccEntries.TryGetValue(transactionId, out KeyValueMvccEntry? entry))
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

                context.MvccEntries.Add(transactionId, entry);
            }
            
            if (context.Revision > entry.Revision) // early conflict detection
                return KeyValueStaticResponses.AbortedResponse;
            
            if (entry.State is KeyValueState.Undefined or KeyValueState.Deleted || entry.Expires != HLCTimestamp.Zero && entry.Expires - currentTime < TimeSpan.Zero)
                return KeyValueStaticResponses.DoesNotExistContextResponse;

            readOnlyKeyValueContext = new(
                entry.Value, 
                entry.Revision, 
                entry.Expires, 
                entry.LastUsed, 
                entry.LastModified, 
                context.State
            );

            return new(KeyValueResponseType.Get, readOnlyKeyValueContext);
        }

        if (context is null || context.State == KeyValueState.Deleted || context.Expires != HLCTimestamp.Zero && context.Expires - currentTime < TimeSpan.Zero)
            return KeyValueStaticResponses.DoesNotExistContextResponse;

        context.LastUsed = currentTime;

        readOnlyKeyValueContext = new(
            context.Value, 
            context.Revision, 
            context.Expires, 
            context.LastUsed, 
            context.LastModified, 
            context.State
        );

        return new(KeyValueResponseType.Get, readOnlyKeyValueContext);
    } 

    public async Task<KeyValueResponse> Execute(KeyValueRequest message)
    {
        if (message.TransactionId == HLCTimestamp.Zero)
            return KeyValueStaticResponses.ErroredResponse;

        await Task.CompletedTask;
        
        /*HLCTimestamp currentTime = raft.HybridLogicalClock.ReceiveEvent(raft.GetLocalNodeId(), message.TransactionId);

        if (!keyValuesStore.TryGetValue(message.Key, out KeyValueContext? context))
        {
            KeyValueContext? newContext = null;

            /// Try to retrieve KeyValue context from persistence
            if (message.Durability == KeyValueDurability.Persistent)
                newContext = await raft.ReadThreadPool.EnqueueTask(() => PersistenceBackend.GetKeyValue(message.Key));

            newContext ??= new() { State = KeyValueState.Undefined, Revision = -1 };
            
            context = newContext;

            keyValuesStore.Insert(message.Key, newContext);
        }

        if (context.WriteIntent is not null)
        {
            // if the transactionId is the same owner no need to acquire the lock
            if (context.WriteIntent.TransactionId == message.TransactionId) 
                return KeyValueStaticResponses.LockedResponse;

            // Check if the lease is still active
            if (context.WriteIntent.Expires != HLCTimestamp.Zero && context.WriteIntent.Expires - currentTime > TimeSpan.Zero)
                return KeyValueStaticResponses.AlreadyLockedResponse;
        }

        context.WriteIntent = new()
        {
            TransactionId = message.TransactionId,
            Expires = message.TransactionId + message.ExpiresMs,
        };
        
        logger.LogDebug("Assigned {Key} write intent to TxId={TransactionId}", message.Key, message.TransactionId);*/
        
        return KeyValueStaticResponses.LockedResponse;
    }
}