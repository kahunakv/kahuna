
using Kahuna.Server.Communication.Internode;
using Nixie;
using Nixie.Routers;

using Kommander;
using Kommander.Data;
using Kommander.Time;

using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Locks;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Server.ScriptParser;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Locks;

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
    /// Locates the leader node for the given key and executes the GetByPrefix request.
    /// </summary>
    /// <param name="prefixedKey"></param>
    /// <param name="durability"></param>
    /// <param name="cancelationToken"></param>
    /// <returns></returns>
    public Task<KeyValueGetByPrefixResult> LocateAndGetByPrefix(
        string prefixedKey, 
        KeyValueDurability durability, 
        CancellationToken cancelationToken
    )
    {
        return keyValues.LocateAndGetByPrefix(prefixedKey, durability, cancelationToken);
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
    /// 
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="proposalTicketId"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
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
    /// Executes a key/value transaction
    /// </summary>
    /// <param name="script"></param>
    /// <param name="hash"></param>
    /// <param name="parameters"></param>
    /// <returns></returns>
    public Task<KeyValueTransactionResult> TryExecuteTx(byte[] script, string? hash, List<KeyValueParameter>? parameters)
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
    /// <param name="prefixKeyName"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public Task<KeyValueGetByPrefixResult> GetByPrefix(string prefixKeyName, KeyValueDurability durability)
    {
        return keyValues.GetByPrefix(prefixKeyName, durability);
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
    
    public async Task<bool> OnLogRestored(int partitionId, RaftLog log)
    {
        await Task.WhenAll(locks.OnLogRestored(log), keyValues.OnLogRestored(log));
        return true;
    }

    public async Task<bool> OnReplicationReceived(int partitionId, RaftLog log)
    {
        await Task.WhenAll(locks.OnReplicationReceived(log), keyValues.OnReplicationReceived(log));
        return true;
    }

    public void OnReplicationError(int partitionId, RaftLog log)
    {
        locks.OnReplicationError(log);
        keyValues.OnReplicationError(log);
    }
}