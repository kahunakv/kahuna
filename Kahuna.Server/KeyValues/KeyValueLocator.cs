
using Kommander;
using Kommander.Time;
using Kommander.Diagnostics;

using System.Collections.Concurrent;

using Kahuna.Server.Communication.Internode;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// Locates the appropriate leader node for a given key and executes the corresponding key-value operations.
/// </summary>
internal sealed class KeyValueLocator
{
    private readonly KeyValuesManager manager;

    private readonly KahunaConfiguration configuration;
    
    private readonly IRaft raft;
    
    private readonly IInterNodeCommunication interNodeCommunication;

    private readonly ILogger<IKahuna> logger;
    
    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="manager"></param>
    /// <param name="configuration"></param>
    /// <param name="raft"></param>
    /// <param name="interNodeCommunication"></param>
    /// <param name="logger"></param>
    public KeyValueLocator(
        KeyValuesManager manager, 
        KahunaConfiguration configuration, 
        IRaft raft, 
        IInterNodeCommunication interNodeCommunication, 
        ILogger<IKahuna> logger
    )
    {
        this.manager = manager;
        this.configuration = configuration;
        this.raft = raft;
        this.interNodeCommunication = interNodeCommunication;
        this.logger = logger;
    }

    /// <summary>
    /// Locates the leader node for the given key and executes the TrySet request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="value"></param>
    /// <param name="compareValue"></param>
    /// <param name="compareRevision"></param>
    /// <param name="flags"></param>
    /// <param name="expiresMs"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
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
        if (string.IsNullOrEmpty(key) || expiresMs < 0)
            return (KeyValueResponseType.InvalidInput, 0, HLCTimestamp.Zero);

        int partitionId = raft.GetPartitionKey(key);

        if (!raft.Joined || await raft.AmILeader(partitionId, cancellationToken))
        {
            return await manager.TrySetKeyValue(
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
            
        string leader = await raft.WaitForLeader(partitionId, cancellationToken);
        if (leader == raft.GetLocalEndpoint())
            return (KeyValueResponseType.MustRetry, 0, HLCTimestamp.Zero);
        
        ValueStopwatch stopwatch = ValueStopwatch.StartNew();              
        
        (KeyValueResponseType, long, HLCTimestamp) response = await interNodeCommunication.TrySetKeyValue(
            leader,
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
        
        logger.LogDebug("SET-KEYVALUE Redirected {Key} to leader partition {Partition} at {Leader} Time={Elapsed}ms", key, partitionId, leader, stopwatch.GetElapsedMilliseconds());

        return response;
    }
    
    /// <summary>
    /// 
    /// </summary>
    /// <param name="setManyItems"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    /// <exception cref="NotImplementedException"></exception>
    public async Task<List<KahunaSetKeyValueResponseItem>> LocateAndTrySetManyKeyValue(
        List<KahunaSetKeyValueRequestItem> setManyItems, 
        CancellationToken cancellationToken
    )
    {                
        string localNode = raft.GetLocalEndpoint();
        
        Dictionary<string, List<KahunaSetKeyValueRequestItem>> acquisitionPlan = [];

        foreach (KahunaSetKeyValueRequestItem key in setManyItems)
        {
            if (string.IsNullOrEmpty(key.Key))
                return [new KahunaSetKeyValueResponseItem { Key = key.Key, Type = KeyValueResponseType.InvalidInput, Durability = key.Durability }];

            int partitionId = raft.GetPartitionKey(key.Key);
            string leader = await raft.WaitForLeader(partitionId, cancellationToken);
            
            if (acquisitionPlan.TryGetValue(leader, out List<KahunaSetKeyValueRequestItem>? list))
                list.Add(key);
            else
                acquisitionPlan[leader] = [key];
        }
        
        Lock lockSync = new();
        List<Task> tasks = new(acquisitionPlan.Count);
        List<KahunaSetKeyValueResponseItem> responses = new(setManyItems.Count);
        
        // Requests to nodes are sent in parallel
        foreach ((string leader, List<KahunaSetKeyValueRequestItem> items) in acquisitionPlan)
            tasks.Add(TrySetManyNodeKeyValue(leader, localNode, items, lockSync, responses, cancellationToken));
        
        await Task.WhenAll(tasks);

        return responses;
    }

    private async Task TrySetManyNodeKeyValue(
        string leader, 
        string localNode, 
        List<KahunaSetKeyValueRequestItem> items, 
        Lock lockSync, 
        List<KahunaSetKeyValueResponseItem> responses, 
        CancellationToken cancellationToken
    )
    {
        logger.LogDebug("SET-MANY-KEYVALUE Redirect {Number} set key/value pairs to node {Leader}", items.Count, leader);
        
        if (leader == localNode)
        {
            List<KahunaSetKeyValueResponseItem> acquireResponses = await manager.SetManyNodeKeyValue(items);

            lock (lockSync)            
                responses.AddRange(acquireResponses);            

            return;
        }
            
        await interNodeCommunication.TrySetManyNodeKeyValue(leader, items, lockSync, responses, cancellationToken);
    }

    /// <summary>
    /// Locates the leader node for the given key and executes the TryDelete request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, long, HLCTimestamp)> LocateAndTryDeleteKeyValue(
        HLCTimestamp transactionId, 
        string key, 
        KeyValueDurability durability, 
        CancellationToken cancellationToken
    )
    {
        if (string.IsNullOrEmpty(key))
            return (KeyValueResponseType.InvalidInput, 0, HLCTimestamp.Zero);
        
        int partitionId = raft.GetPartitionKey(key);

        if (!raft.Joined || await raft.AmILeader(partitionId, cancellationToken))
            return await manager.TryDeleteKeyValue(transactionId, key, durability);
            
        string leader = await raft.WaitForLeader(partitionId, cancellationToken);
        if (leader == raft.GetLocalEndpoint())
            return (KeyValueResponseType.MustRetry, 0, HLCTimestamp.Zero);               
        
        logger.LogDebug("DELETE-KEYVALUE Redirected {KeyValueName} to leader partition {Partition} at {Leader}", key, partitionId, leader);
        
        return await interNodeCommunication.TryDeleteKeyValue(leader, transactionId, key, durability, cancellationToken);
    }
    
    /// <summary>
    /// Locates the leader node for the given key and executes the TryExtend request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="expiresMs"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, long, HLCTimestamp)> LocateAndTryExtendKeyValue(HLCTimestamp transactionId, string key, int expiresMs, KeyValueDurability durability, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(key))
            return (KeyValueResponseType.InvalidInput, 0, HLCTimestamp.Zero);
        
        int partitionId = raft.GetPartitionKey(key);

        if (!raft.Joined || await raft.AmILeader(partitionId, cancellationToken))
            return await manager.TryExtendKeyValue(transactionId, key, expiresMs, durability);
            
        string leader = await raft.WaitForLeader(partitionId, cancellationToken);
        if (leader == raft.GetLocalEndpoint())
            return (KeyValueResponseType.MustRetry, 0, HLCTimestamp.Zero);               
        
        logger.LogDebug("EXTEND-KEYVALUE Redirected {KeyValueName} to leader partition {Partition} at {Leader}", key, partitionId, leader);

        return await interNodeCommunication.TryExtendKeyValue(leader, transactionId, key, expiresMs, durability, cancellationToken);
    }
    
    /// <summary>
    /// Locates the leader node for the given key and executes the TryGetValue request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, ReadOnlyKeyValueContext?)> LocateAndTryGetValue(
        HLCTimestamp transactionId, 
        string key, 
        long revision,
        KeyValueDurability durability,
        CancellationToken cancellationToken
    )
    {
        if (string.IsNullOrEmpty(key))
            return (KeyValueResponseType.InvalidInput, null);
        
        int partitionId = raft.GetPartitionKey(key);

        if (!raft.Joined || await raft.AmILeader(partitionId, cancellationToken))
            return await manager.TryGetValue(transactionId, key, revision, durability);
            
        string leader = await raft.WaitForLeader(partitionId, cancellationToken);
        if (leader == raft.GetLocalEndpoint())
            return (KeyValueResponseType.MustRetry, null);

        ValueStopwatch stopwatch = ValueStopwatch.StartNew();
        
        (KeyValueResponseType, ReadOnlyKeyValueContext?) response = await interNodeCommunication.TryGetValue(leader, transactionId, key, revision, durability, cancellationToken);
        
        logger.LogDebug("GET-KEYVALUE Redirected {KeyValueName} to leader partition {Partition} at {Leader} Time={Elapsed}ms", key, partitionId, leader, stopwatch.GetElapsedMilliseconds());

        return response;
    }
    
    /// <summary>
    /// Locates the leader node for the given key and executes the TryExistsValue request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, ReadOnlyKeyValueContext?)> LocateAndTryExistsValue(
        HLCTimestamp transactionId, 
        string key, 
        long revision,
        KeyValueDurability durability,
        CancellationToken cancellationToken
    )
    {
        if (string.IsNullOrEmpty(key))
            return (KeyValueResponseType.InvalidInput, null);
        
        int partitionId = raft.GetPartitionKey(key);

        if (!raft.Joined || await raft.AmILeader(partitionId, cancellationToken))
            return await manager.TryExistsValue(transactionId, key, revision, durability);
            
        string leader = await raft.WaitForLeader(partitionId, cancellationToken);
        if (leader == raft.GetLocalEndpoint())
            return (KeyValueResponseType.MustRetry, null);
        
        logger.LogDebug("EXISTS-KEYVALUE Redirect {KeyValueName} to leader partition {Partition} at {Leader}", key, partitionId, leader);
        
        return await interNodeCommunication.TryExistsValue(leader, transactionId, key, revision, durability, cancellationToken);
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
    public async Task<(KeyValueResponseType, string, KeyValueDurability)> LocateAndTryAcquireExclusiveLock(HLCTimestamp transactionId, string key, int expiresMs, KeyValueDurability durability, CancellationToken cancelationToken)
    {
        if (string.IsNullOrEmpty(key))
            return (KeyValueResponseType.InvalidInput, key, durability);
        
        int partitionId = raft.GetPartitionKey(key);

        if (!raft.Joined || await raft.AmILeader(partitionId, cancelationToken))
            return await manager.TryAcquireExclusiveLock(transactionId, key, expiresMs, durability);
            
        string leader = await raft.WaitForLeader(partitionId, cancelationToken);
        if (leader == raft.GetLocalEndpoint())
            return (KeyValueResponseType.MustRetry, key, durability);
        
        logger.LogDebug("ACQUIRE-LOCK-KEYVALUE Redirect {KeyValueName} to leader partition {Partition} at {Leader}", key, partitionId, leader);
        
        return await interNodeCommunication.TryAcquireExclusiveLock(leader, transactionId, key, expiresMs, durability, cancelationToken);
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="prefixKey"></param>
    /// <param name="expiresMs"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public async Task<KeyValueResponseType> LocateAndTryAcquireExclusivePrefixLock(
        HLCTimestamp transactionId,
        string prefixKey,
        int expiresMs,
        KeyValueDurability durability,
        CancellationToken cancellationToken
    )
    {
        if (string.IsNullOrEmpty(prefixKey))
            return KeyValueResponseType.InvalidInput;
        
        int partitionId = raft.GetPrefixPartitionKey(prefixKey);

        if (!raft.Joined || await raft.AmILeader(partitionId, cancellationToken))
            return await manager.TryAcquireExclusivePrefixLock(transactionId, prefixKey, expiresMs, durability);
            
        string leader = await raft.WaitForLeader(partitionId, cancellationToken);
        if (leader == raft.GetLocalEndpoint())
            return KeyValueResponseType.MustRetry;
        
        logger.LogDebug("ACQUIRE-PREFIX-LOCK-KEYVALUE Redirect {KeyValueName} to leader partition {Partition} at {Leader}", prefixKey, partitionId, leader);
        
        return await interNodeCommunication.TryAcquireExclusivePrefixLock(leader, transactionId, prefixKey, expiresMs, durability, cancellationToken);
    }
    
    /// <summary>
    /// Locates the leader node for the given keys and executes the TryAcquireManyExclusiveLocks request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="keys"></param>
    /// <param name="cancelationToken"></param>
    /// <returns></returns>
    public async Task<List<(KeyValueResponseType, string, KeyValueDurability)>> LocateAndTryAcquireManyExclusiveLocks(
        HLCTimestamp transactionId, 
        List<(string key, int expiresMs, KeyValueDurability durability)> keys, 
        CancellationToken cancelationToken
    )
    {
        string localNode = raft.GetLocalEndpoint();
        
        Dictionary<string, List<(string key, int expiresMs, KeyValueDurability durability)>> acquisitionPlan = [];

        foreach ((string key, int expiresMs, KeyValueDurability durability) key in keys)
        {
            if (string.IsNullOrEmpty(key.key))
                return [(KeyValueResponseType.InvalidInput, key.key, key.durability)];

            int partitionId = raft.GetPartitionKey(key.key);
            string leader = await raft.WaitForLeader(partitionId, cancelationToken);
            
            if (acquisitionPlan.TryGetValue(leader, out List<(string key, int expiresMs, KeyValueDurability durability)>? list))
                list.Add(key);
            else
                acquisitionPlan[leader] = [key];
        }
        
        Lock lockSync = new();
        List<Task> tasks = new(acquisitionPlan.Count);
        List<(KeyValueResponseType, string, KeyValueDurability)> responses = new(keys.Count);
        
        // Requests to nodes are sent in parallel
        foreach ((string leader, List<(string key, int expiresMs, KeyValueDurability durability)> xkeys) in acquisitionPlan)
            tasks.Add(TryAcquireNodeExclusiveLocks(transactionId, leader, localNode, xkeys, lockSync, responses, cancelationToken));
        
        await Task.WhenAll(tasks);

        return responses;
    }

    private async Task TryAcquireNodeExclusiveLocks(
        HLCTimestamp transactionId, 
        string leader, 
        string localNode, 
        List<(string key, int expiresMs, KeyValueDurability durability)> xkeys,
        Lock lockSync,
        List<(KeyValueResponseType type, string key, KeyValueDurability durability)> responses,
        CancellationToken cancellationToken
    )
    {
        logger.LogDebug("ACQUIRE-LOCK-KEYVALUE Redirect {Number} lock acquisitions to node {Leader}", xkeys.Count, leader);
        
        if (leader == localNode)
        {
            List<(KeyValueResponseType type, string key, KeyValueDurability durability)> acquireResponses = await manager.TryAcquireManyExclusiveLocks(transactionId, xkeys);

            lock (lockSync)
            {
                foreach ((KeyValueResponseType type, string key, KeyValueDurability durability) item in acquireResponses)
                    responses.Add((item.type, item.key, item.durability));
            }

            return;
        }
            
        await interNodeCommunication.TryAcquireNodeExclusiveLocks(leader, transactionId, xkeys, lockSync, responses, cancellationToken);
    }

    /// <summary>
    /// Locates the leader node for the given key and executes the TryReleaseExclusiveLock request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, string)> LocateAndTryReleaseExclusiveLock(HLCTimestamp transactionId, string key, KeyValueDurability durability, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(key))
            return (KeyValueResponseType.InvalidInput, key);
        
        int partitionId = raft.GetPartitionKey(key);

        if (!raft.Joined || await raft.AmILeader(partitionId, cancellationToken))
            return await manager.TryReleaseExclusiveLock(transactionId, key, durability);
            
        string leader = await raft.WaitForLeader(partitionId, cancellationToken);
        if (leader == raft.GetLocalEndpoint())
            return (KeyValueResponseType.MustRetry, key);
        
        logger.LogDebug("RELEASE-LOCK-KEYVALUE Redirect {KeyValueName} to leader partition {Partition} at {Leader}", key, partitionId, leader);
        
        return await interNodeCommunication.TryReleaseExclusiveLock(leader, transactionId, key, durability, cancellationToken);
    }
    
    /// <summary>
    /// 
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="prefixKey"></param>
    /// <param name="expiresMs"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public async Task<KeyValueResponseType> LocateAndTryReleaseExclusivePrefixLock(
        HLCTimestamp transactionId,
        string prefixKey,
        KeyValueDurability durability,
        CancellationToken cancellationToken
    )
    {
        if (string.IsNullOrEmpty(prefixKey))
            return KeyValueResponseType.InvalidInput;
        
        int partitionId = raft.GetPrefixPartitionKey(prefixKey);

        if (!raft.Joined || await raft.AmILeader(partitionId, cancellationToken))
            return await manager.TryReleaseExclusivePrefixLock(transactionId, prefixKey, durability);
            
        string leader = await raft.WaitForLeader(partitionId, cancellationToken);
        if (leader == raft.GetLocalEndpoint())
            return KeyValueResponseType.MustRetry;
        
        logger.LogDebug("RELEASE-PREFIX-LOCK-KEYVALUE Redirect {KeyValueName} to leader partition {Partition} at {Leader}", prefixKey, partitionId, leader);
        
        return await interNodeCommunication.TryReleaseExclusivePrefixLock(leader, transactionId, prefixKey, durability, cancellationToken);
    }
    
    /// <summary>
    /// Locates the leader node for the given keys and executes the TryReleaseManyExclusiveLocks request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="keys"></param>
    /// <param name="cancelationToken"></param>
    /// <returns></returns>
    public async Task<List<(KeyValueResponseType, string, KeyValueDurability)>> LocateAndTryReleaseManyExclusiveLocks(
        HLCTimestamp transactionId, 
        List<(string key, KeyValueDurability durability)> keys, 
        CancellationToken cancelationToken
    )
    {
        string localNode = raft.GetLocalEndpoint();
        
        Dictionary<string, List<(string key, KeyValueDurability durability)>> acquisitionPlan = [];

        foreach ((string key, KeyValueDurability durability) key in keys)
        {
            if (string.IsNullOrEmpty(key.key))
                return [(KeyValueResponseType.InvalidInput, key.key, key.durability)];

            int partitionId = raft.GetPartitionKey(key.key);
            string leader = await raft.WaitForLeader(partitionId, cancelationToken);
            
            if (acquisitionPlan.TryGetValue(leader, out List<(string key, KeyValueDurability durability)>? list))
                list.Add(key);
            else
                acquisitionPlan[leader] = [key];
        }
        
        Lock lockSync = new();
        List<Task> tasks = new(acquisitionPlan.Count);
        List<(KeyValueResponseType, string, KeyValueDurability)> responses = new(keys.Count);
        
        // Requests to nodes are sent in parallel
        foreach ((string leader, List<(string key, KeyValueDurability durability)> xkeys) in acquisitionPlan)
            tasks.Add(TryReleaseNodeExclusiveLocks(transactionId, leader, localNode, xkeys, lockSync, responses, cancelationToken));
        
        await Task.WhenAll(tasks);

        return responses;
    }
    
    private async Task TryReleaseNodeExclusiveLocks(
        HLCTimestamp transactionId, 
        string leader, 
        string localNode, 
        List<(string key, KeyValueDurability durability)> xkeys,
        Lock lockSync,
        List<(KeyValueResponseType type, string key, KeyValueDurability durability)> responses,
        CancellationToken cancelationToken
    )
    {
        logger.LogDebug("RELEASE-LOCK-KEYVALUE Redirect {Number} lock releases to node {Leader}", xkeys.Count, leader);
        
        if (leader == localNode)
        {
            List<(KeyValueResponseType type, string key, KeyValueDurability durability)> acquireResponses = await manager.TryReleaseManyExclusiveLocks(transactionId, xkeys);

            lock (lockSync)
            {
                foreach ((KeyValueResponseType type, string key, KeyValueDurability durability) item in acquireResponses)
                    responses.Add((item.type, item.key, item.durability));
            }

            return;
        }
            
        await interNodeCommunication.TryReleaseNodeExclusiveLocks(leader, transactionId, xkeys, lockSync, responses, cancelationToken);
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
    public async Task<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)> LocateAndTryPrepareMutations(
        HLCTimestamp transactionId, 
        HLCTimestamp commitId, 
        string key, 
        KeyValueDurability durability, 
        CancellationToken cancelationToken
    )
    {
        if (string.IsNullOrEmpty(key))
            return (KeyValueResponseType.InvalidInput, HLCTimestamp.Zero, key, durability);
        
        int partitionId = raft.GetPartitionKey(key);

        if (!raft.Joined || await raft.AmILeader(partitionId, cancelationToken))
            return await manager.TryPrepareMutations(transactionId, commitId, key, durability);
            
        string leader = await raft.WaitForLeader(partitionId, cancelationToken);
        if (leader == raft.GetLocalEndpoint())
            return (KeyValueResponseType.MustRetry, HLCTimestamp.Zero, key, durability);
        
        logger.LogDebug("PREPARE-KEYVALUE Redirect {KeyValueName} to leader partition {Partition} at {Leader}", key, partitionId, leader);
        
        return await interNodeCommunication.TryPrepareMutations(leader, transactionId, commitId, key, durability, cancelationToken);
    }
    
    /// <summary>
    /// Locates the leader node for the given keys and executes the TryPrepareManyMutations request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="commitId"></param> 
    /// <param name="keys"></param>
    /// <param name="cancelationToken"></param>
    /// <returns></returns>
    public async Task<List<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)>> LocateAndTryPrepareManyMutations(
        HLCTimestamp transactionId,
        HLCTimestamp commitId,
        List<(string key, KeyValueDurability durability)> keys, 
        CancellationToken cancelationToken
    )
    {
        string localNode = raft.GetLocalEndpoint();
        
        Dictionary<string, List<(string key, KeyValueDurability durability)>> acquisitionPlan = [];

        foreach ((string key, KeyValueDurability durability) key in keys)
        {
            if (string.IsNullOrEmpty(key.key))
                return [(KeyValueResponseType.InvalidInput, HLCTimestamp.Zero, key.key, key.durability)];

            int partitionId = raft.GetPartitionKey(key.key);
            string leader = await raft.WaitForLeader(partitionId, cancelationToken);
            
            if (acquisitionPlan.TryGetValue(leader, out List<(string key, KeyValueDurability durability)>? list))
                list.Add(key);
            else
                acquisitionPlan[leader] = [key];
        }
        
        Lock lockSync = new();
        List<Task> tasks = new(acquisitionPlan.Count);
        List<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)> responses = new(keys.Count);
        
        // Requests to nodes are sent in parallel
        foreach ((string leader, List<(string key, KeyValueDurability durability)> xkeys) in acquisitionPlan)
            tasks.Add(TryPrepareNodeMutations(transactionId, commitId, leader, localNode, xkeys, lockSync, responses, cancelationToken));
        
        await Task.WhenAll(tasks);

        return responses;
    }

    private async Task TryPrepareNodeMutations(
        HLCTimestamp transactionId,
        HLCTimestamp commitId,
        string leader, 
        string localNode, 
        List<(string key, KeyValueDurability durability)> xkeys,
        Lock lockSync,
        List<(KeyValueResponseType type, HLCTimestamp, string key, KeyValueDurability durability)> responses,
        CancellationToken cancellationToken
    )
    {
        logger.LogDebug("PREPARE-KEYVALUE Redirect {Number} prepare mutations to node {Leader}", xkeys.Count, leader);
        
        if (leader == localNode)
        {
            List<(KeyValueResponseType type, HLCTimestamp ticketId, string key, KeyValueDurability durability)> prepareResponses = await manager.TryPrepareManyMutations(transactionId, commitId, xkeys);

            lock (lockSync)
            {
                foreach ((KeyValueResponseType type, HLCTimestamp ticketId, string key, KeyValueDurability durability) item in prepareResponses)
                    responses.Add((item.type, item.ticketId, item.key, item.durability));
            }

            return;
        }
            
        await interNodeCommunication.TryPrepareNodeMutations(leader, transactionId, commitId, xkeys, lockSync, responses, cancellationToken);
    }
    
    /// <summary>
    /// Locates the leader node for the given key and executes the TryCommitMutations request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="ticketId"></param>
    /// <param name="durability"></param>
    /// <param name="cancelationToken"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, long)> LocateAndTryCommitMutations(HLCTimestamp transactionId, string key, HLCTimestamp ticketId, KeyValueDurability durability, CancellationToken cancelationToken)
    {
        if (string.IsNullOrEmpty(key))
            return (KeyValueResponseType.InvalidInput, 0);
        
        int partitionId = raft.GetPartitionKey(key);

        if (!raft.Joined || await raft.AmILeader(partitionId, cancelationToken))
            return await manager.TryCommitMutations(transactionId, key, ticketId, durability);
            
        string leader = await raft.WaitForLeader(partitionId, cancelationToken);
        if (leader == raft.GetLocalEndpoint())
            return (KeyValueResponseType.MustRetry, 0);
        
        logger.LogDebug("COMMIT-KEYVALUE Redirect {KeyValueName} to leader partition {Partition} at {Leader}", key, partitionId, leader);
        
        return await interNodeCommunication.TryCommitMutations(leader, transactionId, key, ticketId, durability, cancelationToken);
    }
    
    /// <summary>
    /// Locates the leader node for the given keys and executes the TryCommitManyMutations request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="keys"></param>
    /// <param name="cancelationToken"></param>
    /// <returns></returns>
    public async Task<List<(KeyValueResponseType, string, long, KeyValueDurability)>> LocateAndTryCommitManyMutations(
        HLCTimestamp transactionId, 
        List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)> keys, 
        CancellationToken cancelationToken
    )
    {
        string localNode = raft.GetLocalEndpoint();
        
        Dictionary<string, List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)>> acquisitionPlan = [];

        foreach ((string key, HLCTimestamp ticketId, KeyValueDurability durability) key in keys)
        {
            if (string.IsNullOrEmpty(key.key))
                return [(KeyValueResponseType.InvalidInput, key.key, 0, key.durability)];

            int partitionId = raft.GetPartitionKey(key.key);
            string leader = await raft.WaitForLeader(partitionId, cancelationToken);
            
            if (acquisitionPlan.TryGetValue(leader, out List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)>? list))
                list.Add(key);
            else
                acquisitionPlan[leader] = [key];
        }
        
        Lock lockSync = new();
        List<Task> tasks = new(acquisitionPlan.Count);
        List<(KeyValueResponseType, string, long, KeyValueDurability)> responses = new(keys.Count);
        
        // Requests to nodes are sent in parallel
        foreach ((string leader, List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)> xkeys) in acquisitionPlan)
            tasks.Add(TryCommitManyMutations(transactionId, leader, localNode, xkeys, lockSync, responses, cancelationToken));
        
        await Task.WhenAll(tasks);

        return responses;
    }

    private async Task TryCommitManyMutations(
        HLCTimestamp transactionId, 
        string leader, 
        string localNode, 
        List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)> xkeys,
        Lock lockSync,
        List<(KeyValueResponseType, string, long, KeyValueDurability)> responses,
        CancellationToken cancelationToken
    )
    {
        logger.LogDebug("COMMIT-KEYVALUE Redirect {Number} Commit mutations to node {Leader}", xkeys.Count, leader);
        
        if (leader == localNode)
        {
            List<(KeyValueResponseType type, string key, long proposalIndex, KeyValueDurability durability)> commitResponses = await manager.TryCommitManyMutations(transactionId, xkeys);

            lock (lockSync)
            {
                foreach ((KeyValueResponseType type, string key, long proposalIndex, KeyValueDurability durability) item in commitResponses)
                    responses.Add((item.type, item.key, item.proposalIndex, item.durability));
            }

            return;
        }
            
        await interNodeCommunication.TryCommitNodeMutations(leader, transactionId, xkeys, lockSync, responses, cancelationToken);
    }
    
    /// <summary>
    /// Locates the leader node for the given key and executes the TryRollbackMutations request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="ticketId"></param>
    /// <param name="durability"></param>
    /// <param name="cancelationToken"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, long)> LocateAndTryRollbackMutations(HLCTimestamp transactionId, string key, HLCTimestamp ticketId, KeyValueDurability durability, CancellationToken cancelationToken)
    {
        if (string.IsNullOrEmpty(key))
            return (KeyValueResponseType.InvalidInput, 0);
        
        int partitionId = raft.GetPartitionKey(key);

        if (!raft.Joined || await raft.AmILeader(partitionId, cancelationToken))
            return await manager.TryCommitMutations(transactionId, key, ticketId, durability);
            
        string leader = await raft.WaitForLeader(partitionId, cancelationToken);
        if (leader == raft.GetLocalEndpoint())
            return (KeyValueResponseType.MustRetry, 0);
        
        logger.LogDebug("ROLLBACK-KEYVALUE Redirect {KeyValueName} to leader partition {Partition} at {Leader}", key, partitionId, leader);
        
        return await interNodeCommunication.TryRollbackMutations(leader, transactionId, key, ticketId, durability, cancelationToken);
    }
    
    /// <summary>
    /// Locates the leader node for the given keys and executes the TryRollbackManyMutations request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="keys"></param>
    /// <param name="cancelationToken"></param>
    /// <returns></returns>
    public async Task<List<(KeyValueResponseType, string, long, KeyValueDurability)>> LocateAndTryRollbackManyMutations(
        HLCTimestamp transactionId, 
        List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)> keys, 
        CancellationToken cancelationToken
    )
    {
        string localNode = raft.GetLocalEndpoint();
        
        Dictionary<string, List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)>> acquisitionPlan = [];

        foreach ((string key, HLCTimestamp ticketId, KeyValueDurability durability) key in keys)
        {
            if (string.IsNullOrEmpty(key.key))
                return [(KeyValueResponseType.InvalidInput, key.key, 0, key.durability)];

            int partitionId = raft.GetPartitionKey(key.key);
            string leader = await raft.WaitForLeader(partitionId, cancelationToken);
            
            if (acquisitionPlan.TryGetValue(leader, out List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)>? list))
                list.Add(key);
            else
                acquisitionPlan[leader] = [key];
        }
        
        Lock lockSync = new();
        List<Task> tasks = new(acquisitionPlan.Count);
        List<(KeyValueResponseType, string, long, KeyValueDurability)> responses = new(keys.Count);
        
        // Requests to nodes are sent in parallel
        foreach ((string leader, List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)> xkeys) in acquisitionPlan)
            tasks.Add(TryRollbackManyMutations(transactionId, leader, localNode, xkeys, lockSync, responses, cancelationToken));
        
        await Task.WhenAll(tasks);

        return responses;
    }

    private async Task TryRollbackManyMutations(
        HLCTimestamp transactionId, 
        string leader, 
        string localNode, 
        List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)> xkeys,
        Lock lockSync,
        List<(KeyValueResponseType, string, long, KeyValueDurability)> responses,
        CancellationToken cancelationToken
    )
    {
        logger.LogDebug("ROLLBACK-KEYVALUE Redirect {Number} Commit mutations to node {Leader}", xkeys.Count, leader);
        
        if (leader == localNode)
        {
            List<(KeyValueResponseType type, string key, long proposalIndex, KeyValueDurability durability)> commitResponses = await manager.TryRollbackManyMutations(transactionId, xkeys);

            lock (lockSync)
            {
                foreach ((KeyValueResponseType type, string key, long proposalIndex, KeyValueDurability durability) item in commitResponses)
                    responses.Add((item.type, item.key, item.proposalIndex, item.durability));
            }

            return;
        }
            
        await interNodeCommunication.TryRollbackNodeMutations(leader, transactionId, xkeys, lockSync, responses, cancelationToken);
    }

    /// <summary>
    /// Locates the appropriate node for the specified key prefix and retrieves the corresponding key-value items.
    /// </summary>
    /// <param name="transactionId">The timestamp of the transaction used for locating and fetching records.</param>
    /// <param name="prefixedKey">The key prefix used to search and retrieve matching key-value pairs.</param>
    /// <param name="durability">Specifies the durability requirement for the operation, such as Ephemeral or Persistent.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests during the operation.</param>
    /// <returns>Returns a <see cref="KeyValueGetByPrefixResult"/> containing the result of the operation with key-value items and response type.</returns>
    public async Task<KeyValueGetByPrefixResult> LocateAndGetByPrefix(HLCTimestamp transactionId, string prefixedKey, KeyValueDurability durability, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(prefixedKey))
            return new(KeyValueResponseType.Errored, []);
        
        int partitionId = raft.GetPrefixPartitionKey(prefixedKey);

        if (!raft.Joined || await raft.AmILeader(partitionId, cancellationToken))
            return await manager.GetByPrefix(transactionId, prefixedKey, durability);
            
        string leader = await raft.WaitForLeader(partitionId, cancellationToken);
        if (leader == raft.GetLocalEndpoint())
            return new(KeyValueResponseType.MustRetry, []);
        
        logger.LogDebug("GETPREFIX-KEYVALUE Redirect {KeyValueName} to leader partition {Partition} at {Leader}", prefixedKey, partitionId, leader);
        
        return await interNodeCommunication.GetByPrefix(leader, transactionId, prefixedKey, durability, cancellationToken);               
    }

    /// <summary>
    /// Attempts to locate the appropriate partition leader and starts a transaction based on the provided options.
    /// </summary>
    /// <param name="options">The transaction options, including a unique identifier used to determine the partition.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A tuple containing the result of the transaction operation (<see cref="KeyValueResponseType"/>),
    /// and a timestamp (<see cref="HLCTimestamp"/>) indicating when the transaction was processed.</returns>
    public async Task<(KeyValueResponseType, HLCTimestamp)> LocateAndStartTransaction(KeyValueTransactionOptions options, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(options.UniqueId))
            return new(KeyValueResponseType.Errored, HLCTimestamp.Zero);
        
        int partitionId = raft.GetPartitionKey(options.UniqueId);

        if (!raft.Joined || await raft.AmILeader(partitionId, cancellationToken))
            return await manager.StartTransaction(options);
            
        string leader = await raft.WaitForLeader(partitionId, cancellationToken);
        if (leader == raft.GetLocalEndpoint())
            return new(KeyValueResponseType.MustRetry, HLCTimestamp.Zero);
        
        logger.LogDebug("START-TRANSACTION Redirect {KeyValueName} to leader partition {Partition} at {Leader}", options.UniqueId, partitionId, leader);
        
        return await interNodeCommunication.StartTransaction(leader, options, cancellationToken);
    }

    /// <summary>
    /// Locates the appropriate partition and commits a transaction associated with the given unique identifier.
    /// </summary>
    /// <param name="uniqueId">The unique identifier associated with the transaction.</param>
    /// <param name="timestamp">The timestamp of the transaction using hybrid logical clock.</param>
    /// <param name="acquiredLocks">The list of keys that have been locked as part of the transaction.</param>
    /// <param name="modifiedKeys">The list of keys that have been modified as part of the transaction.</param>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <returns>
    /// A <see cref="KeyValueResponseType"/> indicating the outcome of the transaction operation.
    /// </returns>
    public async Task<KeyValueResponseType> LocateAndCommitTransaction(
        string uniqueId,
        HLCTimestamp timestamp,
        List<KeyValueTransactionModifiedKey> acquiredLocks,
        List<KeyValueTransactionModifiedKey> modifiedKeys,
        CancellationToken cancellationToken
    )
    {
        if (string.IsNullOrEmpty(uniqueId))
            return KeyValueResponseType.Errored;
        
        int partitionId = raft.GetPartitionKey(uniqueId);

        if (!raft.Joined || await raft.AmILeader(partitionId, cancellationToken))
            return await manager.CommitTransaction(timestamp, acquiredLocks, modifiedKeys);
            
        string leader = await raft.WaitForLeader(partitionId, cancellationToken);
        if (leader == raft.GetLocalEndpoint())
            return KeyValueResponseType.MustRetry;
        
        logger.LogDebug("COMMIT-TRANSACTION Redirect {KeyValueName} to leader partition {Partition} at {Leader}", uniqueId, partitionId, leader);
        
        return await interNodeCommunication.CommitTransaction(leader, uniqueId, timestamp, acquiredLocks, modifiedKeys, cancellationToken);
    }

    /// <summary>
    /// Locates and rolls back a transaction based on the specified parameters.
    /// </summary>
    /// <param name="uniqueId">The unique identifier of the transaction to locate and rollback.</param>
    /// <param name="timestamp">The timestamp associated with the transaction.</param>
    /// <param name="acquiredLocks">The list of keys that were locked during the transaction.</param>
    /// <param name="modifiedKeys">The list of keys that were modified during the transaction.</param>
    /// <param name="cancellationToken">The token to monitor for cancellation requests.</param>
    /// <returns>A <see cref="KeyValueResponseType"/> indicating the result of the operation.</returns>
    public async Task<KeyValueResponseType> LocateAndRollbackTransaction(
        string uniqueId,
        HLCTimestamp timestamp,
        List<KeyValueTransactionModifiedKey> acquiredLocks,
        List<KeyValueTransactionModifiedKey> modifiedKeys, 
        CancellationToken cancellationToken
    )
    {
        if (string.IsNullOrEmpty(uniqueId))
            return KeyValueResponseType.Errored;
        
        int partitionId = raft.GetPartitionKey(uniqueId);

        if (!raft.Joined || await raft.AmILeader(partitionId, cancellationToken))
            return await manager.RollbackTransaction(timestamp, acquiredLocks, modifiedKeys);
            
        string leader = await raft.WaitForLeader(partitionId, cancellationToken);
        if (leader == raft.GetLocalEndpoint())
            return KeyValueResponseType.MustRetry;
        
        logger.LogDebug("ROLLBACK-TRANSACTION Redirect {KeyValueName} to leader partition {Partition} at {Leader}", uniqueId, partitionId, leader);
        
        return await interNodeCommunication.RollbackTransaction(leader, uniqueId, timestamp, acquiredLocks, modifiedKeys, cancellationToken);
    }

    /// <summary>
    /// Scans all nodes in the cluster and returns key/value pairs by prefix
    /// </summary>
    /// <param name="prefixKeyName"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public async Task<KeyValueGetByPrefixResult> ScanAllByPrefix(string prefixKeyName, KeyValueDurability durability, CancellationToken cancellationToken)
    {
        ConcurrentDictionary<string, ReadOnlyKeyValueContext> unionItems = [];
        
        KeyValueGetByPrefixResult items = await manager.ScanByPrefix(prefixKeyName, durability);

        if (items.Type == KeyValueResponseType.Get)
        {
            foreach ((string, ReadOnlyKeyValueContext) item in items.Items)
                unionItems.TryAdd(item.Item1, item.Item2);
        }

        IList<RaftNode> nodes = raft.GetNodes();
        
        List<Task> tasks = new(nodes.Count);

        foreach (RaftNode node in nodes)
            tasks.Add(NodeScanByPrefix(unionItems, node, prefixKeyName, durability, cancellationToken));
        
        await Task.WhenAll(tasks);               

        if (durability == KeyValueDurability.Persistent)
        {
            KeyValueGetByPrefixResult result = await manager.ScanByPrefixFromDisk(prefixKeyName);

            if (items.Type == KeyValueResponseType.Get)
            {
                foreach ((string, ReadOnlyKeyValueContext) item in result.Items)
                    unionItems.TryAdd(item.Item1, item.Item2);
            }
        }

        return new(KeyValueResponseType.Get, unionItems.Select(kv => (kv.Key, kv.Value)).ToList());
    }
    
    /// <summary>
    /// 
    /// </summary>
    /// <param name="unionItems"></param>
    /// <param name="node"></param>
    /// <param name="prefixKeyName"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    private async Task NodeScanByPrefix(
        ConcurrentDictionary<string, ReadOnlyKeyValueContext> unionItems, 
        RaftNode node, 
        string prefixKeyName, 
        KeyValueDurability durability, 
        CancellationToken cancellationToken
    )
    {
        KeyValueGetByPrefixResult response = await interNodeCommunication.ScanByPrefix(node.Endpoint, prefixKeyName, durability, cancellationToken);
        
        if (response.Type == KeyValueResponseType.Get)
        {
            foreach ((string, ReadOnlyKeyValueContext) item in response.Items)
                unionItems.TryAdd(item.Item1, item.Item2);
        }
    }    
}