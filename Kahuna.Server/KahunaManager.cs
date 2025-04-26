
using Nixie;

using Kommander;
using Kommander.Data;
using Kommander.Time;

using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Locks;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Server.Replication;
using Kahuna.Server.ScriptParser;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Locks;
using Kahuna.Server.Communication.Internode;

namespace Kahuna;

/// <summary>
/// Façade to the internal systems of Kahuna.
/// </summary>
public sealed class KahunaManager : IKahuna
{
    private readonly ActorSystem actorSystem;
    
    private readonly LockManager locks;

    private readonly KeyValuesManager keyValues;

    private readonly IActorRef<ScriptParserEvicterActor, ScriptParserEvicterRequest> scriptParserEvicter;
    
    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="actorSystem"></param>
    /// <param name="raft"></param>
    /// <param name="configuration"></param>
    /// <param name="logger"></param>
    public KahunaManager(ActorSystem actorSystem, IRaft raft, KahunaConfiguration configuration, IInterNodeCommunication interNodeCommunication, ILogger<IKahuna> logger)
    {
        this.actorSystem = actorSystem;
        
        IPersistenceBackend persistenceBackend = GetPersistence(configuration);
        
        IActorRef<BackgroundWriterActor, BackgroundWriteRequest> backgroundWriter = actorSystem.Spawn<BackgroundWriterActor, BackgroundWriteRequest>(
            "background-writer", 
            raft, 
            persistenceBackend, 
            logger
        );

        scriptParserEvicter = actorSystem.Spawn<ScriptParserEvicterActor, ScriptParserEvicterRequest>("script-parser-evicter", logger);
        
        this.locks = new(actorSystem, raft, interNodeCommunication, persistenceBackend, backgroundWriter, configuration, logger);
        this.keyValues = new(actorSystem, raft, interNodeCommunication, persistenceBackend, backgroundWriter, configuration, logger);
    }
    
    /// <summary>
    /// Creates the persistence instance
    /// </summary>
    /// <param name="configuration"></param>
    /// <returns></returns>
    /// <exception cref="KahunaServerException"></exception>
    private static IPersistenceBackend GetPersistence(KahunaConfiguration configuration)
    {
        return configuration.Storage switch
        {
            "rocksdb" => new RocksDbPersistenceBackend(configuration.StoragePath, configuration.StorageRevision),
            "sqlite" => new SqlitePersistenceBackend(configuration.StoragePath, configuration.StorageRevision),
            "memory" => new MemoryPersistenceBackend(),
            _ => throw new KahunaServerException("Invalid storage type: " + configuration.Storage)
        };
    }
    
    /// <summary>
    /// Locates the leader node for the given key and passes a TryLock request to the locker actor for the given lock name.
    /// </summary>
    /// <param name="resource"></param>
    /// <param name="owner"></param>
    /// <param name="expiresMs"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public Task<(LockResponseType, long)> LocateAndTryLock(string resource, byte[] owner, int expiresMs, LockDurability durability, CancellationToken cancellationToken)
    {
        return locks.LocateAndTryLock(resource, owner, expiresMs, durability, cancellationToken);
    }

    /// <summary>
    /// Locates the leader node for the given key and passes a TryExtendLock request to the locker actor for the given lock name.
    /// </summary>
    /// <param name="resource"></param>
    /// <param name="owner"></param>
    /// <param name="expiresMs"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public Task<(LockResponseType, long)> LocateAndTryExtendLock(string resource, byte[] owner, int expiresMs, LockDurability durability, CancellationToken cancellationToken)
    {
        return locks.LocateAndTryExtendLock(resource, owner, expiresMs, durability, cancellationToken);
    }

    /// <summary>
    /// Locates the leader node for the given key and passes a TryUnlock request to the locker actor for the given lock name.
    /// </summary>
    /// <param name="resource"></param>
    /// <param name="owner"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public Task<LockResponseType> LocateAndTryUnlock(string resource, byte[] owner, LockDurability durability,CancellationToken cancellationToken)
    {
        return locks.LocateAndTryUnlock(resource, owner, durability, cancellationToken);
    }

    /// <summary>
    /// Locates the leader node for the given key and passes a TryGetLock request to the locker actor for the given lock name.
    /// </summary>
    /// <param name="resource"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public Task<(LockResponseType, ReadOnlyLockContext?)> LocateAndGetLock(string resource, LockDurability durability, CancellationToken cancellationToken)
    {
        return locks.LocateAndGetLock(resource, durability, cancellationToken);
    }
    
    /// <summary>
    /// Passes a TryLock request to the locker actor for the given lock name. 
    /// </summary>
    /// <param name="resource"></param>
    /// <param name="owner"></param>
    /// <param name="expiresMs"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public Task<(LockResponseType, long)> TryLock(string resource, byte[] owner, int expiresMs, LockDurability durability)
    {
        return locks.TryLock(resource, owner, expiresMs, durability);
    }

    /// <summary>
    /// Passes a TryExtendLock request to the locker actor for the given lock name. 
    /// </summary>
    /// <param name="resource"></param>
    /// <param name="owner"></param>
    /// <param name="expiresMs"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public Task<(LockResponseType, long)> TryExtendLock(string resource, byte[] owner, int expiresMs, LockDurability durability)
    {
        return locks.TryExtendLock(resource, owner, expiresMs, durability);
    }

    /// <summary>
    /// Passes a TryUnlock request to the locker actor for the given lock name.
    /// </summary>
    /// <param name="resource"></param>
    /// <param name="owner"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public Task<LockResponseType> TryUnlock(string resource, byte[] owner, LockDurability durability)
    {
        return locks.TryUnlock(resource, owner, durability);
    }

    /// <summary>
    /// Returns information about the lock for the given resource.
    /// </summary>
    /// <param name="resource"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public Task<(LockResponseType, ReadOnlyLockContext?)> GetLock(string resource, LockDurability durability)
    {
        return locks.GetLock(resource, durability);
    }

    /// <summary>
    /// Locates the leader node for the given key and executes the TrySet request.
    /// </summary>
    /// <param name="key"></param>
    /// <param name="value"></param>
    /// <param name="compareValue"></param>
    /// <param name="compareRevision"></param>
    /// <param name="flags"></param>
    /// <param name="expiresMs"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, long, HLCTimestamp)> LocateAndTrySetKeyValue(
        HLCTimestamp transactionId,
        string key,
        byte[]? value,
        byte[]? compareValue,
        long compareRevision,
        KeyValueFlags flags,
        int expiresMs,
        KeyValueDurability durability,
        CancellationToken cancellationToken
    )
    {
        return await keyValues.LocateAndTrySetKeyValue(
            transactionId, 
            key, 
            value, 
            compareValue, 
            compareRevision, 
            flags, 
            expiresMs, 
            durability, 
            cancellationToken
        );
    }

    /// <summary>
    /// Locates the leader node for the given key and executes the TryGetValue request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="revision"></param>
    /// <param name="durability"></param>
    /// <param name="cancelationToken"></param>
    /// <returns></returns>
    public Task<(KeyValueResponseType, ReadOnlyKeyValueContext?)> LocateAndTryGetValue(
        HLCTimestamp transactionId, 
        string key, 
        long revision, 
        KeyValueDurability durability, 
        CancellationToken cancelationToken
    )
    {
        return keyValues.LocateAndTryGetValue(transactionId, key, revision, durability, cancelationToken);
    }
    
    /// <summary>
    /// Locates the leader node for the given key and executes the TryExistsValue request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="revision"></param>
    /// <param name="durability"></param>
    /// <param name="cancelationToken"></param>
    /// <returns></returns>
    public Task<(KeyValueResponseType, ReadOnlyKeyValueContext?)> LocateAndTryExistsValue(
        HLCTimestamp transactionId, 
        string key, 
        long revision, 
        KeyValueDurability durability, 
        CancellationToken cancelationToken
    )
    {
        return keyValues.LocateAndTryExistsValue(transactionId, key, revision, durability, cancelationToken);
    }

    /// <summary>
    /// Locates the leader node for the given key and executes the TryDeleteValue request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public Task<(KeyValueResponseType, long, HLCTimestamp)> LocateAndTryDeleteKeyValue(
        HLCTimestamp transactionId, 
        string key, 
        KeyValueDurability durability, 
        CancellationToken cancellationToken
    )
    {
        return keyValues.LocateAndTryDeleteKeyValue(transactionId, key, durability, cancellationToken);
    }
    
    /// <summary>
    /// Locates the leader node for the given key and executes the TryExtendValue request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="expiresMs"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public Task<(KeyValueResponseType, long, HLCTimestamp)> LocateAndTryExtendKeyValue(
        HLCTimestamp transactionId, 
        string key, 
        int expiresMs, 
        KeyValueDurability durability, 
        CancellationToken cancellationToken
    )
    {
        return keyValues.LocateAndTryExtendKeyValue(transactionId, key, expiresMs, durability, cancellationToken);
    }
    
    /// <summary>
    /// Locates the leader node for the given key and executes the TryAcquireExclusiveLock request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="expiresMs"></param>
    /// <param name="durability"></param>
    /// <param name="cancelationToken"></param>
    /// <returns></returns>
    public Task<(KeyValueResponseType, string, KeyValueDurability)> LocateAndTryAcquireExclusiveLock(
        HLCTimestamp transactionId, 
        string key, 
        int expiresMs, 
        KeyValueDurability durability, 
        CancellationToken cancelationToken
    )
    {
        return keyValues.LocateAndTryAcquireExclusiveLock(transactionId, key, expiresMs, durability, cancelationToken);
    }

    /// <summary>
    /// Locates the leader node for the given key and executes the TryAcquireExclusiveLock request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="keys"></param>
    /// <param name="cancelationToken"></param>
    /// <returns></returns>
    public Task<List<(KeyValueResponseType, string, KeyValueDurability)>> LocateAndTryAcquireManyExclusiveLocks(
        HLCTimestamp transactionId,
        List<(string key, int expiresMs, KeyValueDurability durability)> keys, 
        CancellationToken cancelationToken
    )
    {
        return keyValues.LocateAndTryAcquireManyExclusiveLocks(transactionId, keys, cancelationToken);
    }

    /// <summary>
    /// Locates the leader node for the given key and executes the TryReleaseExclusiveLock request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="durability"></param>
    /// <param name="cancelationToken"></param>
    /// <returns></returns>
    public Task<(KeyValueResponseType, string)> LocateAndTryReleaseExclusiveLock(
        HLCTimestamp transactionId, 
        string key, 
        KeyValueDurability durability, 
        CancellationToken cancelationToken
    )
    {
        return keyValues.LocateAndTryReleaseExclusiveLock(transactionId, key, durability, cancelationToken);
    }
    
    /// <summary>
    /// Locates the leader node for the given keysx and executes the TryReleaseExclusiveLock request. 
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="keys"></param>
    /// <param name="cancelationToken"></param>
    /// <returns></returns>
    public Task<List<(KeyValueResponseType, string, KeyValueDurability)>> LocateAndTryReleaseManyExclusiveLocks(
        HLCTimestamp transactionId, 
        List<(string key, KeyValueDurability durability)> keys, 
        CancellationToken cancelationToken
    )
    {
        return keyValues.LocateAndTryReleaseManyExclusiveLocks(transactionId, keys, cancelationToken);
    }

    /// <summary>
    /// Locates the leader node for the given key and executes the TryPrepareMutations request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="commitId"></param> 
    /// <param name="key"></param>
    /// <param name="durability"></param>
    /// <param name="cancelationToken"></param>
    /// <returns></returns>
    public Task<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)> LocateAndTryPrepareMutations(
        HLCTimestamp transactionId,
        HLCTimestamp commitId,
        string key, 
        KeyValueDurability durability, 
        CancellationToken cancelationToken
    )
    {
        return keyValues.LocateAndTryPrepareMutations(transactionId, commitId, key, durability, cancelationToken);
    }

    /// <summary>
    /// Locates the leader node for the given key and executes the TryPrepareManyMutations request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="commitId"></param> 
    /// <param name="keys"></param>
    /// <param name="cancelationToken"></param>
    /// <returns></returns>
    public Task<List<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)>> LocateAndTryPrepareManyMutations(
        HLCTimestamp transactionId,
        HLCTimestamp commitId,
        List<(string key, KeyValueDurability durability)> keys, 
        CancellationToken cancelationToken
    )
    {
        return keyValues.LocateAndTryPrepareManyMutations(transactionId, commitId, keys, cancelationToken);
    }

    /// <summary>
    /// Locates the leader node for the given key and executes the TryCommitMutations request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="durability"></param>
    /// <param name="cancelationToken"></param>
    /// <returns></returns>
    public Task<(KeyValueResponseType, long)> LocateAndTryCommitMutations(
        HLCTimestamp transactionId, 
        string key, 
        HLCTimestamp ticketId, 
        KeyValueDurability durability, 
        CancellationToken cancelationToken
    )
    {
        return keyValues.LocateAndTryCommitMutations(transactionId, key, ticketId, durability, cancelationToken);
    }

    /// <summary>
    /// Locates the leader node for the given keys and executes the TryCommitMutations request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="keys"></param>
    /// <param name="cancelationToken"></param>
    /// <returns></returns>
    public Task<List<(KeyValueResponseType, string, long, KeyValueDurability)>> LocateAndTryCommitManyMutations(
        HLCTimestamp transactionId, 
        List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)> keys, 
        CancellationToken cancelationToken
    )
    {
        return keyValues.LocateAndTryCommitManyMutations(transactionId, keys, cancelationToken);
    }
    
    /// <summary>
    /// Locates the leader node for the given key and executes the TryRollbackMutations request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="durability"></param>
    /// <param name="cancelationToken"></param>
    /// <returns></returns>
    public Task<(KeyValueResponseType, long)> LocateAndTryRollbackMutations(
        HLCTimestamp transactionId, 
        string key, 
        HLCTimestamp ticketId, 
        KeyValueDurability durability, 
        CancellationToken cancelationToken
    )
    {
        return keyValues.LocateAndTryRollbackMutations(transactionId, key, ticketId, durability, cancelationToken);
    }
    
    /// <summary>
    /// Locates the leader node for the given keys and executes the TryRollbackMutations request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="keys"></param>
    /// <param name="cancelationToken"></param>
    /// <returns></returns>
    public Task<List<(KeyValueResponseType, string, long, KeyValueDurability)>> LocateAndTryRollbackManyMutations(
        HLCTimestamp transactionId, 
        List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)> keys, 
        CancellationToken cancelationToken
    )
    {
        return keyValues.LocateAndTryRollbackManyMutations(transactionId, keys, cancelationToken);
    }

    /// <summary>
    /// Locates the leader node for the given prefix and executes the GetByPrefix request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="prefixedKey"></param>
    /// <param name="durability"></param>
    /// <param name="cancelationToken"></param>
    /// <returns></returns>
    public Task<KeyValueGetByPrefixResult> LocateAndGetByPrefix(
        HLCTimestamp transactionId, 
        string prefixedKey, 
        KeyValueDurability durability, 
        CancellationToken cancelationToken
    )
    {
        return keyValues.LocateAndGetByPrefix(transactionId, prefixedKey, durability, cancelationToken);
    }

    /// <summary>
    /// Starts a transaction for a key-value operation by locating the appropriate node and setting up the transaction.
    /// </summary>
    /// <param name="options">The options specifying the parameters of the transaction.</param>
    /// <param name="cancelationToken">The token to monitor for cancellation requests.</param>
    /// <returns>A task that represents the asynchronous operation. The task result contains the response type and the timestamp of the initiated transaction.</returns>
    public Task<(KeyValueResponseType, HLCTimestamp)> LocateAndStartTransaction(KeyValueTransactionOptions options, CancellationToken cancelationToken)
    {
        return keyValues.LocateAndStartTransaction(options, cancelationToken);        
    }

    /// <summary>
    /// Commits a transaction by locating required resources and applying all necessary modifications.
    /// </summary>
    /// <param name="uniqueId">The unique identifier for the transaction to be committed.</param>
    /// <param name="timestamp">The timestamp associated with the transaction.</param>
    /// <param name="acquiredLocks">A list of locks acquired as part of the transaction.</param>
    /// <param name="modifiedKeys">A list of keys modified as part of the transaction.</param>
    /// <param name="cancelationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task representing the asynchronous operation, with a result of <see cref="KeyValueResponseType"/> indicating the outcome of the transaction commit.</returns>
    public Task<KeyValueResponseType> LocateAndCommitTransaction(
        string uniqueId, 
        HLCTimestamp timestamp, 
        List<KeyValueTransactionModifiedKey> acquiredLocks, 
        List<KeyValueTransactionModifiedKey> modifiedKeys,
        CancellationToken cancelationToken
    )
    {
        return keyValues.LocateAndCommitTransaction(uniqueId, timestamp, acquiredLocks, modifiedKeys, cancelationToken);
    }

    /// <summary>
    /// Rolls back a transaction and restores the state of the system to the point
    /// before the transaction was executed.
    /// </summary>
    /// <param name="uniqueId">The unique identifier for the transaction to be rolled back.</param>
    /// <param name="timestamp">The timestamp associated with the transaction.</param>
    /// <param name="acquiredLocks">A list of locks acquired during the transaction.</param>
    /// <param name="modifiedKeys">A list of keys modified during the transaction.</param>
    /// <param name="cancelationToken">A token to observe for cancellation requests.</param>
    /// <returns>A <see cref="KeyValueResponseType"/> indicating the result of the rollback operation.</returns>
    public Task<KeyValueResponseType> LocateAndRollbackTransaction(
        string uniqueId, 
        HLCTimestamp timestamp, 
        List<KeyValueTransactionModifiedKey> acquiredLocks, 
        List<KeyValueTransactionModifiedKey> modifiedKeys,
        CancellationToken cancelationToken
    )
    {
        return keyValues.LocateAndRollbackTransaction(uniqueId, timestamp, acquiredLocks, modifiedKeys, cancelationToken);
    }

    /// <summary>
    /// Set key to hold the string value. If key already holds a value, it is overwritten
    /// </summary>
    /// <param name="key"></param>
    /// <param name="value"></param>
    /// <param name="compareValue"></param>
    /// <param name="compareRevision"></param>
    /// <param name="flags"></param>
    /// <param name="expiresMs"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public Task<(KeyValueResponseType, long, HLCTimestamp)> TrySetKeyValue(
        HLCTimestamp transactionId,
        string key, 
        byte[]? value,
        byte[]? compareValue,
        long compareRevision,
        KeyValueFlags flags,
        int expiresMs, 
        KeyValueDurability durability
    )
    {
        return keyValues.TrySetKeyValue(
            transactionId, 
            key, 
            value, 
            compareValue, 
            compareRevision, 
            flags, 
            expiresMs, 
            durability
        );
    }

    /// <summary>
    /// Set a timeout on key. After the timeout has expired, the key will automatically be deleted
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="expiresMs"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public Task<(KeyValueResponseType, long, HLCTimestamp)> TryExtendKeyValue(
        HLCTimestamp transactionId, 
        string key, 
        int expiresMs, 
        KeyValueDurability durability
    )
    {
        return keyValues.TryExtendKeyValue(transactionId, key, expiresMs, durability);
    }

    /// <summary>
    /// Removes the specified key. A key is ignored if it does not exist.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public Task<(KeyValueResponseType, long, HLCTimestamp)> TryDeleteKeyValue(
        HLCTimestamp transactionId, 
        string key, 
        KeyValueDurability durability
    )
    {
        return keyValues.TryDeleteKeyValue(transactionId, key, durability);
    }

    /// <summary>
    /// Returns a value and its context by the specified key
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="revision"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public Task<(KeyValueResponseType, ReadOnlyKeyValueContext?)> TryGetValue(
        HLCTimestamp transactionId, 
        string key, 
        long revision, 
        KeyValueDurability durability
    )
    {
        return keyValues.TryGetValue(transactionId, key, revision, durability);
    }
    
    /// <summary>
    /// Checks if a key exists and its context (without the value)
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="revision"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public Task<(KeyValueResponseType, ReadOnlyKeyValueContext?)> TryExistsValue(
        HLCTimestamp transactionId, 
        string key, 
        long revision, 
        KeyValueDurability durability
    )
    {
        return keyValues.TryExistsValue(transactionId, key, revision, durability);
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="expiresMs"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public Task<(KeyValueResponseType, string, KeyValueDurability)> TryAcquireExclusiveLock(
        HLCTimestamp transactionId, 
        string key, 
        int expiresMs, 
        KeyValueDurability durability
    )
    {
        return keyValues.TryAcquireExclusiveLock(transactionId, key, expiresMs, durability);
    }
    
    /// <summary>
    /// 
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public Task<(KeyValueResponseType, string)> TryReleaseExclusiveLock(
        HLCTimestamp transactionId, 
        string key, 
        KeyValueDurability durability
    )
    {
        return keyValues.TryReleaseExclusiveLock(transactionId, key, durability);
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="commitId"></param>
    /// <param name="key"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public Task<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)> TryPrepareMutations(
        HLCTimestamp transactionId,
        HLCTimestamp commitId,
        string key, 
        KeyValueDurability durability
    )
    {
        return keyValues.TryPrepareMutations(transactionId, commitId, key, durability);
    }

    /// <summary>
    /// Attempts to commit mutations for a specified transaction and key with the given durability.
    /// </summary>
    /// <param name="transactionId">The timestamp representing the transaction ID.</param>
    /// <param name="key">The key for which the mutations are being committed.</param>
    /// <param name="proposalTicketId">The timestamp representing the ID of the proposal ticket.</param>
    /// <param name="durability">The durability level of the transaction, indicating whether it is ephemeral or persistent.</param>
    /// <returns>A task representing the asynchronous operation, containing a tuple where the first element is the result of the operation as a <see cref="KeyValueResponseType"/>, and the second element is a long value associated with the operation.</returns>
    public Task<(KeyValueResponseType, long)> TryCommitMutations(
        HLCTimestamp transactionId,
        string key,
        HLCTimestamp proposalTicketId,
        KeyValueDurability durability
    )
    {
        return keyValues.TryCommitMutations(transactionId, key, proposalTicketId, durability);
    }

    /// <summary>
    /// Attempts to rollback mutations for a given key, transaction, and proposal ticket.
    /// </summary>
    /// <param name="transactionId">The unique identifier of the transaction to rollback.</param>
    /// <param name="key">The key associated with the mutations.</param>
    /// <param name="proposalTicketId">The identifier of the proposal ticket related to the mutation.</param>
    /// <param name="durability">The durability level for the operation.</param>
    /// <returns>A task containing a tuple with the response type and the associated transaction version.</returns>
    public Task<(KeyValueResponseType, long)> TryRollbackMutations(
        HLCTimestamp transactionId,
        string key,
        HLCTimestamp proposalTicketId,
        KeyValueDurability durability
    )
    {
        return keyValues.TryRollbackMutations(transactionId, key, proposalTicketId, durability);
    }

    /// <summary>
    /// Attempts to execute a transaction script with the provided parameters and returns the result.
    /// </summary>
    /// <param name="script">The transaction script to execute, represented as a byte array.</param>
    /// <param name="hash">An optional hash representing the script for validation or identification purposes.</param>
    /// <param name="parameters">An optional list of parameters to be passed into the script during execution.</param>
    /// <returns>A task that represents the asynchronous operation and resolves to the result of the transaction execution.</returns>
    public Task<KeyValueTransactionResult> TryExecuteTransactionScript(byte[] script, string? hash, List<KeyValueParameter>? parameters)
    {
        return keyValues.TryExecuteTx(script, hash, parameters);
    }

    /// <summary>
    /// Scans the current node in the cluster and returns key/value pairs by prefix
    /// </summary>
    /// <param name="prefixKeyName"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public Task<KeyValueGetByPrefixResult> ScanByPrefix(string prefixKeyName, KeyValueDurability durability)
    {
        return keyValues.ScanByPrefix(prefixKeyName, durability);
    }
    
    /// <summary>
    /// Gets the key/value pairs by prefix from the current node in the cluster
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="prefixKeyName"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public Task<KeyValueGetByPrefixResult> GetByPrefix(HLCTimestamp transactionId, string prefixKeyName, KeyValueDurability durability)
    {
        return keyValues.GetByPrefix(transactionId, prefixKeyName, durability);
    }
    
    /// <summary>
    /// Scans all nodes in the cluster and returns key/value pairs by prefix
    /// </summary>
    /// <param name="prefixKeyName"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public Task<KeyValueGetByPrefixResult> ScanAllByPrefix(string prefixKeyName, KeyValueDurability durability)
    {
        return keyValues.ScanAllByPrefix(prefixKeyName, durability);
    }

    /// <summary>
    /// Starts a new interactive transaction with the specified options.
    /// </summary>
    /// <param name="options">The options to configure the transaction.</param>
    /// <returns>Returns the timestamp of the started transaction.</returns>
    public Task<(KeyValueResponseType, HLCTimestamp)> StartTransaction(KeyValueTransactionOptions options)
    {
        return keyValues.StartTransaction(options);
    }

    /// <summary>
    /// Commits a transaction with the specified timestamp and list of modified keys.
    /// </summary>
    /// <param name="timestamp">The timestamp associated with the transaction.</param>
    /// <param name="modifiedKeys">The list of keys that were modified as part of the transaction.</param>
    /// <returns>A task that represents the asynchronous operation, containing a boolean indicating whether the transaction commitment was successful.</returns>
    public Task<KeyValueResponseType> CommitTransaction(
        HLCTimestamp timestamp, 
        List<KeyValueTransactionModifiedKey> acquiredLocks, 
        List<KeyValueTransactionModifiedKey> modifiedKeys
    )
    {
        return keyValues.CommitTransaction(timestamp, acquiredLocks, modifiedKeys);
    }

    /// <summary>
    /// Rolls back a transaction, reverting changes associated with the specified keys.
    /// </summary>
    /// <param name="timestamp">The timestamp of the transaction to be rolled back.</param>
    /// <param name="modifiedKeys">The list of modified keys associated with the transaction.</param>
    /// <returns>A task that represents the asynchronous operation. The task result contains a boolean indicating whether the rollback succeeded.</returns>
    public Task<KeyValueResponseType> RollbackTransaction(
        HLCTimestamp timestamp, 
        List<KeyValueTransactionModifiedKey> acquiredLocks, 
        List<KeyValueTransactionModifiedKey> modifiedKeys
    )
    {
        return keyValues.RollbackTransaction(timestamp, acquiredLocks, modifiedKeys);
    }
    
    public async Task<bool> OnLogRestored(int partitionId, RaftLog log)
    {
        return log.LogType switch
        {
            ReplicationTypes.KeyValues => await keyValues.OnLogRestored(partitionId, log),
            ReplicationTypes.Locks => await locks.OnLogRestored(partitionId, log),
            _ => true
        };
    }

    public async Task<bool> OnReplicationReceived(int partitionId, RaftLog log)
    {
        return log.LogType switch
        {
            ReplicationTypes.KeyValues => await keyValues.OnReplicationReceived(partitionId, log),
            ReplicationTypes.Locks => await locks.OnReplicationReceived(partitionId, log),
            _ => true
        };
    }

    public void OnReplicationError(int partitionId, RaftLog log)
    {
        locks.OnReplicationError(log);
        keyValues.OnReplicationError(log);
    }
}