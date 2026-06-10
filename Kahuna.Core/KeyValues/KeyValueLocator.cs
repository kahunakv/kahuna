
using Kommander;
using Kommander.Time;
using Kommander.Diagnostics;

using System.Collections.Concurrent;

using Kahuna.Server.Communication.Internode;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues.Ranges;
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

    private readonly KeySpaceRegistry keySpaceRegistry;

    private readonly DataPartitionRouter dataPartitionRouter;

    private readonly ILogger<IKahuna> logger;

    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="manager"></param>
    /// <param name="configuration"></param>
    /// <param name="raft"></param>
    /// <param name="interNodeCommunication"></param>
    /// <param name="keySpaceRegistry"></param>
    /// <param name="logger"></param>
    public KeyValueLocator(
        KeyValuesManager manager,
        KahunaConfiguration configuration,
        IRaft raft,
        IInterNodeCommunication interNodeCommunication,
        KeySpaceRegistry keySpaceRegistry,
        ILogger<IKahuna> logger
    )
    {
        this.manager = manager;
        this.configuration = configuration;
        this.raft = raft;
        this.interNodeCommunication = interNodeCommunication;
        this.keySpaceRegistry = keySpaceRegistry;
        this.dataPartitionRouter = new DataPartitionRouter(raft);
        this.logger = logger;
    }

    /// <summary>
    /// The key-order router: resolves <paramref name="key"/> to <c>(partitionId,
    /// generation)</c> through the range-descriptor map for key-range spaces, or falls back to the
    /// hash router (<c>GetPartitionKey</c>) for hash spaces. Added in Task 3 — <b>no caller is
    /// switched to it yet</b> (that is Task 9, which also points <c>KeyValueProposalActor</c> at the
    /// same <see cref="RangeRouting.Locate"/> so the two routing sites cannot drift).
    /// </summary>
    public (int PartitionId, long Generation) LocateRange(string key) =>
        RangeRouting.Locate(keySpaceRegistry, manager.RangeMapStore.Current, dataPartitionRouter, key);

    /// <summary>Routes a per-key operation via <see cref="RangeRouting.Locate"/>.</summary>
    private int RouteKey(string key) =>
        RangeRouting.Locate(keySpaceRegistry, manager.RangeMapStore.Current, dataPartitionRouter, key).PartitionId;

    /// <summary>
    /// Routes a prefix/bucket operation. A bare prefix (no trailing <c>/</c>) is the key space
    /// itself; appending <c>/</c> lets <see cref="KeySpaceRegistry.ExtractKeySpace"/> strip it
    /// back to the prefix, consistent with how real keys look (<c>"t:r/0001"</c> → space <c>"t:r"</c>).
    /// </summary>
    private int RoutePrefixKey(string prefix) =>
        RangeRouting.Locate(keySpaceRegistry, manager.RangeMapStore.Current, dataPartitionRouter, prefix + "/").PartitionId;

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
        CancellationToken cancellationToken,
        long routedGeneration = 0
    )
    {
        if (string.IsNullOrEmpty(key) || expiresMs < 0)
            return (KeyValueResponseType.InvalidInput, 0, HLCTimestamp.Zero);

        // Key-range spaces route + fence via the descriptor map. Hash spaces use DataPartitionRouter.
        // routedGeneration is non-zero when this call arrived via an inter-node redirect; the coordinator's
        // generation is preserved so the remote fence checks against the coordinator's view, catching the
        // case where the coordinator is fresher (split applied there but not yet here) or staler (split
        // applied here but not there — fence fails → MustRetry → coordinator re-resolves).
        int partitionId;
        if (RangeRouting.IsKeyRange(keySpaceRegistry, key))
        {
            long freshGeneration;
            (partitionId, freshGeneration) = LocateRange(key);
            if (routedGeneration == 0)
                routedGeneration = freshGeneration;
        }
        else
        {
            partitionId = dataPartitionRouter.Locate(key);
        }

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
                durability,
                routedGeneration
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
            routedGeneration,
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

            int partitionId;
            if (RangeRouting.IsKeyRange(keySpaceRegistry, key.Key))
            {
                long freshGeneration;
                (partitionId, freshGeneration) = LocateRange(key.Key);
                // Preserve a coordinator-supplied generation (non-zero = already redirected once);
                // on the first call resolve fresh and stamp it so the remote fence can check it.
                if (key.RoutedGeneration == 0)
                    key.RoutedGeneration = freshGeneration;
            }
            else
            {
                partitionId = dataPartitionRouter.Locate(key.Key);
                // Hash path: no generation fence, RoutedGeneration stays 0.
            }

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

    public async Task<List<KahunaDeleteKeyValueResponseItem>> LocateAndTryDeleteManyKeyValue(
        List<KahunaDeleteKeyValueRequestItem> deleteManyItems,
        CancellationToken cancellationToken
    )
    {
        string localNode = raft.GetLocalEndpoint();

        Dictionary<string, List<KahunaDeleteKeyValueRequestItem>> acquisitionPlan = [];
        List<KahunaDeleteKeyValueResponseItem> responses = new(deleteManyItems.Count);

        foreach (KahunaDeleteKeyValueRequestItem item in deleteManyItems)
        {
            if (string.IsNullOrEmpty(item.Key))
            {
                responses.Add(new()
                {
                    Key = item.Key,
                    Type = KeyValueResponseType.InvalidInput,
                    Durability = item.Durability
                });
                continue;
            }

            int partitionId = RouteKey(item.Key);
            string leader = await raft.WaitForLeader(partitionId, cancellationToken);

            if (acquisitionPlan.TryGetValue(leader, out List<KahunaDeleteKeyValueRequestItem>? list))
                list.Add(item);
            else
                acquisitionPlan[leader] = [item];
        }

        Lock lockSync = new();
        List<Task> tasks = new(acquisitionPlan.Count);

        foreach ((string leader, List<KahunaDeleteKeyValueRequestItem> items) in acquisitionPlan)
            tasks.Add(TryDeleteManyNodeKeyValue(leader, localNode, items, lockSync, responses, cancellationToken));

        await Task.WhenAll(tasks);

        return responses;
    }

    private async Task TryDeleteManyNodeKeyValue(
        string leader,
        string localNode,
        List<KahunaDeleteKeyValueRequestItem> items,
        Lock lockSync,
        List<KahunaDeleteKeyValueResponseItem> responses,
        CancellationToken cancellationToken
    )
    {
        logger.LogDebug("DELETE-MANY-KEYVALUE Redirect {Number} delete key/value pairs to node {Leader}", items.Count, leader);

        if (leader == localNode)
        {
            List<KahunaDeleteKeyValueResponseItem> acquireResponses = await manager.DeleteManyNodeKeyValue(items);

            lock (lockSync)
                responses.AddRange(acquireResponses);

            return;
        }

        await interNodeCommunication.TryDeleteManyNodeKeyValue(leader, items, lockSync, responses, cancellationToken);
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
        
        int partitionId = RouteKey(key);

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
        
        int partitionId = RouteKey(key);

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
    public async Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> LocateAndTryGetValue(
        HLCTimestamp transactionId, 
        string key, 
        long revision,
        KeyValueDurability durability,
        CancellationToken cancellationToken
    )
    {
        if (string.IsNullOrEmpty(key))
            return (KeyValueResponseType.InvalidInput, null);
        
        int partitionId = RouteKey(key);

        if (!raft.Joined || await raft.AmILeader(partitionId, cancellationToken))
            return await manager.TryGetValue(transactionId, key, revision, durability);
            
        string leader = await raft.WaitForLeader(partitionId, cancellationToken);
        if (leader == raft.GetLocalEndpoint())
            return (KeyValueResponseType.MustRetry, null);

        ValueStopwatch stopwatch = ValueStopwatch.StartNew();
        
        (KeyValueResponseType, ReadOnlyKeyValueEntry?) response = await interNodeCommunication.TryGetValue(leader, transactionId, key, revision, durability, cancellationToken);
        
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
    public async Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> LocateAndTryExistsValue(
        HLCTimestamp transactionId, 
        string key, 
        long revision,
        KeyValueDurability durability,
        CancellationToken cancellationToken
    )
    {
        if (string.IsNullOrEmpty(key))
            return (KeyValueResponseType.InvalidInput, null);
        
        int partitionId = RouteKey(key);

        if (!raft.Joined || await raft.AmILeader(partitionId, cancellationToken))
            return await manager.TryExistsValue(transactionId, key, revision, durability);
            
        string leader = await raft.WaitForLeader(partitionId, cancellationToken);
        if (leader == raft.GetLocalEndpoint())
            return (KeyValueResponseType.MustRetry, null);
        
        logger.LogDebug("EXISTS-KEYVALUE Redirect {KeyValueName} to leader partition {Partition} at {Leader}", key, partitionId, leader);
        
        return await interNodeCommunication.TryExistsValue(leader, transactionId, key, revision, durability, cancellationToken);
    }

    public async Task<List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)>> LocateAndTryExistsManyValues(
        HLCTimestamp transactionId,
        List<(string key, long revision, KeyValueDurability durability)> keys,
        CancellationToken cancellationToken
    )
    {
        if (keys.Count == 0)
            return [(KeyValueResponseType.InvalidInput, string.Empty, KeyValueDurability.Persistent, null)];

        string localNode = raft.GetLocalEndpoint();
        Dictionary<string, List<(string key, long revision, KeyValueDurability durability)>> acquisitionPlan = [];

        foreach ((string key, long revision, KeyValueDurability durability) item in keys)
        {
            if (string.IsNullOrEmpty(item.key))
                return [(KeyValueResponseType.InvalidInput, item.key, item.durability, null)];

            int partitionId = RouteKey(item.key);
            string leader = await raft.WaitForLeader(partitionId, cancellationToken);

            if (acquisitionPlan.TryGetValue(leader, out List<(string key, long revision, KeyValueDurability durability)>? list))
                list.Add(item);
            else
                acquisitionPlan[leader] = [item];
        }

        Lock lockSync = new();
        List<Task> tasks = new(acquisitionPlan.Count);
        List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)> responses = new(keys.Count);

        foreach ((string leader, List<(string key, long revision, KeyValueDurability durability)> xkeys) in acquisitionPlan)
            tasks.Add(TryExistsManyNodeValues(transactionId, leader, localNode, xkeys, lockSync, responses, cancellationToken));

        await Task.WhenAll(tasks);

        return responses;
    }

    public async Task<List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)>> LocateAndTryGetManyValues(
        HLCTimestamp transactionId,
        List<(string key, long revision, KeyValueDurability durability)> keys,
        CancellationToken cancellationToken
    )
    {
        if (keys.Count == 0)
            return [(KeyValueResponseType.InvalidInput, string.Empty, KeyValueDurability.Persistent, null)];

        string localNode = raft.GetLocalEndpoint();
        Dictionary<string, List<(string key, long revision, KeyValueDurability durability)>> acquisitionPlan = [];

        foreach ((string key, long revision, KeyValueDurability durability) item in keys)
        {
            if (string.IsNullOrEmpty(item.key))
                return [(KeyValueResponseType.InvalidInput, item.key, item.durability, null)];

            int partitionId = RouteKey(item.key);
            string leader = await raft.WaitForLeader(partitionId, cancellationToken);

            if (acquisitionPlan.TryGetValue(leader, out List<(string key, long revision, KeyValueDurability durability)>? list))
                list.Add(item);
            else
                acquisitionPlan[leader] = [item];
        }

        Lock lockSync = new();
        List<Task> tasks = new(acquisitionPlan.Count);
        List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)> responses = new(keys.Count);

        foreach ((string leader, List<(string key, long revision, KeyValueDurability durability)> xkeys) in acquisitionPlan)
            tasks.Add(TryGetManyNodeValues(transactionId, leader, localNode, xkeys, lockSync, responses, cancellationToken));

        await Task.WhenAll(tasks);

        return responses;
    }

    private async Task TryExistsManyNodeValues(
        HLCTimestamp transactionId,
        string leader,
        string localNode,
        List<(string key, long revision, KeyValueDurability durability)> xkeys,
        Lock lockSync,
        List<(KeyValueResponseType type, string key, KeyValueDurability durability, ReadOnlyKeyValueEntry? entry)> responses,
        CancellationToken cancellationToken
    )
    {
        logger.LogDebug("EXISTS-KEYVALUE Redirect {Number} batched exists probes to node {Leader}", xkeys.Count, leader);

        if (leader == localNode)
        {
            List<(KeyValueResponseType type, string key, KeyValueDurability durability, ReadOnlyKeyValueEntry? entry)> readResponses =
                await manager.TryExistsManyValues(transactionId, xkeys);

            lock (lockSync)
            {
                foreach ((KeyValueResponseType type, string key, KeyValueDurability durability, ReadOnlyKeyValueEntry? entry) item in readResponses)
                    responses.Add((item.type, item.key, item.durability, item.entry));
            }

            return;
        }

        await interNodeCommunication.TryExistsManyNodeValues(leader, transactionId, xkeys, lockSync, responses, cancellationToken);
    }

    private async Task TryGetManyNodeValues(
        HLCTimestamp transactionId,
        string leader,
        string localNode,
        List<(string key, long revision, KeyValueDurability durability)> xkeys,
        Lock lockSync,
        List<(KeyValueResponseType type, string key, KeyValueDurability durability, ReadOnlyKeyValueEntry? entry)> responses,
        CancellationToken cancellationToken
    )
    {
        logger.LogDebug("GET-KEYVALUE Redirect {Number} batched gets to node {Leader}", xkeys.Count, leader);

        if (leader == localNode)
        {
            List<(KeyValueResponseType type, string key, KeyValueDurability durability, ReadOnlyKeyValueEntry? entry)> readResponses =
                await manager.TryGetManyValues(transactionId, xkeys);

            lock (lockSync)
            {
                foreach ((KeyValueResponseType type, string key, KeyValueDurability durability, ReadOnlyKeyValueEntry? entry) item in readResponses)
                    responses.Add((item.type, item.key, item.durability, item.entry));
            }

            return;
        }

        await interNodeCommunication.TryGetManyNodeValues(leader, transactionId, xkeys, lockSync, responses, cancellationToken);
    }
    
    /// <summary>
    /// Locates the leader node for the given key and checks whether a live write intent from another
    /// transaction exists. Used at commit time by optimistic transactions as a write-skew guard.
    /// Returns Aborted when a conflicting write intent is found; DoesNotExist otherwise.
    /// </summary>
    public async Task<KeyValueResponseType> LocateAndTryCheckWriteIntent(
        HLCTimestamp transactionId,
        string key,
        KeyValueDurability durability,
        CancellationToken cancellationToken
    )
    {
        if (string.IsNullOrEmpty(key))
            return KeyValueResponseType.InvalidInput;

        int partitionId = RouteKey(key);

        if (!raft.Joined || await raft.AmILeader(partitionId, cancellationToken))
            return await manager.TryCheckWriteIntentValue(transactionId, key, durability);

        string leader = await raft.WaitForLeader(partitionId, cancellationToken);
        if (leader == raft.GetLocalEndpoint())
            return KeyValueResponseType.MustRetry;

        logger.LogDebug("CHECK-WRITE-INTENT Redirect {KeyValueName} to leader partition {Partition} at {Leader}", key, partitionId, leader);

        return await interNodeCommunication.TryCheckWriteIntentValue(leader, transactionId, key, durability, cancellationToken);
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
        
        int partitionId = RouteKey(key);

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

        if (!RangeRouting.IsPrefixOpSafe(keySpaceRegistry, manager.RangeMapStore.Current, prefixKey))
        {
            // Deprecated by design: a prefix lock is a single-partition bucket lock and cannot cover
            // a key space that has been key-range split across partitions. Callers must use the
            // per-range exclusive range lock instead (TryAcquireExclusiveRangeLock, design §8).
            logger.LogWarning("ACQUIRE-PREFIX-LOCK: prefix {Prefix} is on a key-range-split space — prefix locks are unsupported there; use a range lock", prefixKey);
            return KeyValueResponseType.PrefixLockUnsupportedOnRangedSpace;
        }

        int partitionId = RoutePrefixKey(prefixKey);

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

            int partitionId = RouteKey(key.key);
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
        
        int partitionId = RouteKey(key);

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

        if (!RangeRouting.IsPrefixOpSafe(keySpaceRegistry, manager.RangeMapStore.Current, prefixKey))
        {
            // Deprecated by design (see acquire): prefix locks are unsupported on key-range-split
            // spaces. A release on such a space can only be a caller error — there is no prefix lock
            // to release — so surface the typed response rather than a generic error.
            logger.LogWarning("RELEASE-PREFIX-LOCK: prefix {Prefix} is on a key-range-split space — prefix locks are unsupported there; use a range lock", prefixKey);
            return KeyValueResponseType.PrefixLockUnsupportedOnRangedSpace;
        }

        int partitionId = RoutePrefixKey(prefixKey);

        if (!raft.Joined || await raft.AmILeader(partitionId, cancellationToken))
            return await manager.TryReleaseExclusivePrefixLock(transactionId, prefixKey, durability);
            
        string leader = await raft.WaitForLeader(partitionId, cancellationToken);
        if (leader == raft.GetLocalEndpoint())
            return KeyValueResponseType.MustRetry;
        
        logger.LogDebug("RELEASE-PREFIX-LOCK-KEYVALUE Redirect {KeyValueName} to leader partition {Partition} at {Leader}", prefixKey, partitionId, leader);
        
        return await interNodeCommunication.TryReleaseExclusivePrefixLock(leader, transactionId, prefixKey, durability, cancellationToken);
    }
    
    public async Task<KeyValueResponseType> LocateAndTryAcquireExclusiveRangeLock(
        HLCTimestamp transactionId,
        string prefix,
        string? startKey, bool startInclusive,
        string? endKey,   bool endInclusive,
        int expiresMs,
        KeyValueDurability durability,
        CancellationToken cancellationToken
    )
    {
        if (string.IsNullOrEmpty(prefix))
            return KeyValueResponseType.InvalidInput;

        IReadOnlyList<RangeDescriptor> descriptors =
            manager.RangeMapStore.Current.FindIntersecting(prefix, startKey, endKey);

        if (descriptors.Count == 0)
        {
            // Hash-space or range space with no descriptors yet: single-partition path.
            // Hash spaces never split, so no generation fence is needed.
            int hashPartitionId = RoutePrefixKey(prefix);
            return await AcquireRangeLockOnPartition(transactionId, hashPartitionId, prefix,
                startKey, startInclusive, endKey, endInclusive, expiresMs, durability, cancellationToken);
        }

        if (descriptors.Count == 1)
        {
            KeyValueResponseType result = await AcquireRangeLockOnPartition(
                transactionId, descriptors[0].PartitionId, prefix,
                startKey, startInclusive, endKey, endInclusive, expiresMs, durability, cancellationToken);

            if (result != KeyValueResponseType.Locked)
                return result;

            // Generation fence: a split that committed after FindIntersecting but before the
            // sub-lock RPC would leave P' un-locked. Re-check the map; if the descriptor set
            // changed, roll back and signal the caller to re-resolve.
            if (!DescriptorSetStable(descriptors, manager.RangeMapStore.Current.FindIntersecting(prefix, startKey, endKey)))
            {
                KeyValueResponseType rel = await ReleaseRangeLockOnPartition(transactionId, descriptors[0].PartitionId, prefix,
                    startKey, startInclusive, endKey, endInclusive, durability, cancellationToken);

                if (rel != KeyValueResponseType.Unlocked)
                    logger.LogWarning("ACQUIRE-RANGE-LOCK {Prefix} P{Pid}: fence rollback release returned {Status} — sub-lock leaks until TTL",
                        prefix, descriptors[0].PartitionId, rel);

                return KeyValueResponseType.MustRetry;
            }

            return KeyValueResponseType.Locked;
        }

        // Multi-descriptor: per-range sub-locks with roll-back on partial failure.
        var acquired = new List<(int PartitionId, string? ClampStart, bool ClampStartIncl, string? ClampEnd, bool ClampEndIncl)>(descriptors.Count);

        foreach (RangeDescriptor desc in descriptors)
        {
            (string? cs, bool csI, string? ce, bool ceI) = ClipRange(
                startKey, startInclusive, endKey, endInclusive, desc);

            KeyValueResponseType result = await AcquireRangeLockOnPartition(
                transactionId, desc.PartitionId, prefix, cs, csI, ce, ceI, expiresMs, durability, cancellationToken);

            if (result == KeyValueResponseType.Locked)
            {
                acquired.Add((desc.PartitionId, cs, csI, ce, ceI));
                continue;
            }

            foreach ((int pid, string? rcs, bool rcsi, string? rce, bool rcei) in acquired)
            {
                KeyValueResponseType rel = await ReleaseRangeLockOnPartition(
                    transactionId, pid, prefix, rcs, rcsi, rce, rcei, durability, cancellationToken);

                if (rel != KeyValueResponseType.Unlocked)
                    logger.LogWarning("ACQUIRE-RANGE-LOCK {Prefix} P{Pid}: partial-acquire rollback release returned {Status} — sub-lock leaks until TTL",
                        prefix, pid, rel);
            }

            return result;
        }

        // Generation fence: re-check after all sub-locks are held. If the map changed
        // (split committed in the acquire window), roll everything back and MustRetry.
        if (!DescriptorSetStable(descriptors, manager.RangeMapStore.Current.FindIntersecting(prefix, startKey, endKey)))
        {
            logger.LogDebug("ACQUIRE-RANGE-LOCK {Prefix}: descriptor set changed during acquisition — MustRetry", prefix);

            foreach ((int pid, string? rcs, bool rcsi, string? rce, bool rcei) in acquired)
            {
                KeyValueResponseType rel = await ReleaseRangeLockOnPartition(
                    transactionId, pid, prefix, rcs, rcsi, rce, rcei, durability, cancellationToken);

                if (rel != KeyValueResponseType.Unlocked)
                    logger.LogWarning("ACQUIRE-RANGE-LOCK {Prefix} P{Pid}: fence rollback release returned {Status} — sub-lock leaks until TTL",
                        prefix, pid, rel);
            }

            return KeyValueResponseType.MustRetry;
        }

        return KeyValueResponseType.Locked;
    }

    /// <summary>
    /// Returns true when both snapshots cover the same descriptors in the same order with the
    /// same generations. Used as the acquire-time generation fence: if false, a split committed
    /// in the window between <c>FindIntersecting</c> and the sub-lock RPCs, and the caller must
    /// roll back acquired sub-locks and retry.
    /// </summary>
    private static bool DescriptorSetStable(
        IReadOnlyList<RangeDescriptor> before,
        IReadOnlyList<RangeDescriptor> after)
    {
        if (before.Count != after.Count)
            return false;

        for (int i = 0; i < before.Count; i++)
        {
            if (before[i].PartitionId != after[i].PartitionId ||
                before[i].Generation  != after[i].Generation)
                return false;
        }

        return true;
    }

    public async Task<KeyValueResponseType> LocateAndTryReleaseExclusiveRangeLock(
        HLCTimestamp transactionId,
        string prefix,
        string? startKey, bool startInclusive,
        string? endKey,   bool endInclusive,
        KeyValueDurability durability,
        CancellationToken cancellationToken
    )
    {
        if (string.IsNullOrEmpty(prefix))
            return KeyValueResponseType.InvalidInput;

        IReadOnlyList<RangeDescriptor> descriptors =
            manager.RangeMapStore.Current.FindIntersecting(prefix, startKey, endKey);

        if (descriptors.Count == 0)
        {
            int hashPartitionId = RoutePrefixKey(prefix);
            return await ReleaseRangeLockOnPartition(transactionId, hashPartitionId, prefix,
                startKey, startInclusive, endKey, endInclusive, durability, cancellationToken);
        }

        if (descriptors.Count == 1)
        {
            return await ReleaseRangeLockOnPartition(transactionId, descriptors[0].PartitionId, prefix,
                startKey, startInclusive, endKey, endInclusive, durability, cancellationToken);
        }

        // Release all sub-locks even if one fails (best-effort). Return Unlocked only when
        // every descriptor released successfully; return the first non-Unlocked result otherwise
        // so the caller knows at least one sub-lock was not cleaned up.
        KeyValueResponseType firstFailure = KeyValueResponseType.Unlocked;
        foreach (RangeDescriptor desc in descriptors)
        {
            (string? cs, bool csI, string? ce, bool ceI) = ClipRange(
                startKey, startInclusive, endKey, endInclusive, desc);

            KeyValueResponseType rel = await ReleaseRangeLockOnPartition(
                transactionId, desc.PartitionId, prefix, cs, csI, ce, ceI, durability, cancellationToken);

            if (rel != KeyValueResponseType.Unlocked)
            {
                logger.LogWarning("RELEASE-RANGE-LOCK {Prefix} P{Pid}: release returned {Status} — sub-lock leaks until TTL",
                    prefix, desc.PartitionId, rel);

                if (firstFailure == KeyValueResponseType.Unlocked)
                    firstFailure = rel;
            }
        }

        return firstFailure;
    }

    private async Task<KeyValueResponseType> AcquireRangeLockOnPartition(
        HLCTimestamp transactionId,
        int partitionId,
        string prefix,
        string? startKey, bool startInclusive,
        string? endKey,   bool endInclusive,
        int expiresMs,
        KeyValueDurability durability,
        CancellationToken cancellationToken)
    {
        if (!raft.Joined || await raft.AmILeader(partitionId, cancellationToken))
            return await manager.TryAcquireExclusiveRangeLock(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, expiresMs, durability);

        string leader = await raft.WaitForLeader(partitionId, cancellationToken);
        if (leader == raft.GetLocalEndpoint())
            return KeyValueResponseType.MustRetry;

        logger.LogDebug("ACQUIRE-RANGE-LOCK-KEYVALUE Redirect {Prefix} P{Partition} → {Leader}", prefix, partitionId, leader);

        return await interNodeCommunication.TryAcquireExclusiveRangeLock(leader, transactionId, prefix, startKey, startInclusive, endKey, endInclusive, expiresMs, durability, cancellationToken);
    }

    private async Task<KeyValueResponseType> ReleaseRangeLockOnPartition(
        HLCTimestamp transactionId,
        int partitionId,
        string prefix,
        string? startKey, bool startInclusive,
        string? endKey,   bool endInclusive,
        KeyValueDurability durability,
        CancellationToken cancellationToken)
    {
        if (!raft.Joined || await raft.AmILeader(partitionId, cancellationToken))
            return await manager.TryReleaseExclusiveRangeLock(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, durability);

        string leader = await raft.WaitForLeader(partitionId, cancellationToken);
        if (leader == raft.GetLocalEndpoint())
            return KeyValueResponseType.MustRetry;

        logger.LogDebug("RELEASE-RANGE-LOCK-KEYVALUE Redirect {Prefix} P{Partition} → {Leader}", prefix, partitionId, leader);

        return await interNodeCommunication.TryReleaseExclusiveRangeLock(leader, transactionId, prefix, startKey, startInclusive, endKey, endInclusive, durability, cancellationToken);
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

            int partitionId = RouteKey(key.key);
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
        CancellationToken cancelationToken,
        long routedGeneration = 0
    )
    {
        if (string.IsNullOrEmpty(key))
            return (KeyValueResponseType.InvalidInput, HLCTimestamp.Zero, key, durability);

        // Resolve both partition and generation; preserve the coordinator's generation when redirected.
        int partitionId;
        long freshGeneration;
        (partitionId, freshGeneration) = RangeRouting.Locate(
            keySpaceRegistry, manager.RangeMapStore.Current, dataPartitionRouter, key);
        if (routedGeneration == 0)
            routedGeneration = freshGeneration;

        if (!raft.Joined || await raft.AmILeader(partitionId, cancelationToken))
            return await manager.TryPrepareMutations(transactionId, commitId, key, durability, routedGeneration);

        string leader = await raft.WaitForLeader(partitionId, cancelationToken);
        if (leader == raft.GetLocalEndpoint())
            return (KeyValueResponseType.MustRetry, HLCTimestamp.Zero, key, durability);

        logger.LogDebug("PREPARE-KEYVALUE Redirect {KeyValueName} to leader partition {Partition} at {Leader}", key, partitionId, leader);

        return await interNodeCommunication.TryPrepareMutations(leader, transactionId, commitId, key, durability, routedGeneration, cancelationToken);
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

            int partitionId = RouteKey(key.key);
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
        
        int partitionId = RouteKey(key);

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

            int partitionId = RouteKey(key.key);
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
        
        int partitionId = RouteKey(key);

        if (!raft.Joined || await raft.AmILeader(partitionId, cancelationToken))
            return await manager.TryRollbackMutations(transactionId, key, ticketId, durability);

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

            int partitionId = RouteKey(key.key);
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
    /// For unsplit spaces routes to the single partition leader. For split key-range spaces fans out across
    /// all descriptors sequentially, pages through each with <see cref="QueryDescriptorRange"/>, and returns
    /// the concatenated result (Task 10b multi-range GetByBucket).
    ///
    /// <para>
    /// <b>Performance notes (deferred optimisations):</b>
    /// <list type="bullet">
    ///   <item>Fan-out is sequential. A future improvement may issue descriptors in parallel via a
    ///     bounded <c>Task.WhenAll</c> when the descriptor count exceeds a threshold.</item>
    ///   <item>No per-leader coalescing. When multiple ranges share a leader, separate paged RPC
    ///     streams are opened. A future improvement should group by leader and issue one stream per
    ///     leader.</item>
    ///   <item>Full materialisation. The entire bucket is buffered before returning. Callers that
    ///     need bounded memory over a large split bucket should use the paged
    ///     <see cref="LocateAndGetByRange"/> / <c>LocateAndScanRange</c> path instead.</item>
    /// </list>
    /// </para>
    /// </summary>
    public async Task<KeyValueGetByBucketResult> LocateAndGetByBucket(HLCTimestamp transactionId, string prefixedKey, KeyValueDurability durability, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(prefixedKey))
            return new(KeyValueResponseType.Errored, []);

        // Fast path: unsplit (or hash/schema-log) space — single partition, no fan-out overhead.
        if (RangeRouting.IsPrefixOpSafe(keySpaceRegistry, manager.RangeMapStore.Current, prefixedKey))
        {
            int singlePartitionId = RoutePrefixKey(prefixedKey);

            if (!raft.Joined || await raft.AmILeader(singlePartitionId, cancellationToken))
                return await manager.GetByBucket(transactionId, prefixedKey, durability);

            string singleLeader = await raft.WaitForLeader(singlePartitionId, cancellationToken);
            if (singleLeader == raft.GetLocalEndpoint())
                return new(KeyValueResponseType.MustRetry, []);

            logger.LogDebug("GETPREFIX-KEYVALUE Redirect {KeyValueName} to leader partition {Partition} at {Leader}", prefixedKey, singlePartitionId, singleLeader);

            return await interNodeCommunication.GetByBucket(singleLeader, transactionId, prefixedKey, durability, cancellationToken);
        }

        // Multi-range path (Task 10b): key-range space is split; fan out to all descriptors.
        // Snapshot the map once — safe for the same reason as LocateAndGetByRange: orphan retention
        // + MVCC means the source partition still answers snapshot reads even after a cutover.
        string keySpace = KeySpaceRegistry.ExtractKeySpace(prefixedKey + "/");
        IReadOnlyList<RangeDescriptor> descriptors =
            manager.RangeMapStore.Current.FindAll(keySpace);

        if (descriptors.Count == 0)
            return new(KeyValueResponseType.Get, []);

        // Within-call snapshot safety: a split landing between two descriptors' queries means the
        // second descriptor's source partition is queried with a now-stale routing entry. This is
        // safe for the same reason as the per-page re-resolution note in LocateAndGetByRange: Task 6
        // orphan-retains [K,E) on the source partition after a cutover, so a fixed readTimestamp
        // (MVCC) still resolves correctly from there. The caller sees a consistent snapshot of the
        // moment this call started; a stale partition returns all snapshot-committed data it holds.
        const int bucketPageSize = 512;
        var allItems = new List<(string, ReadOnlyKeyValueEntry)>();

        foreach (RangeDescriptor descriptor in descriptors)
        {
            (string? clStart, bool clStartInc, string? clEnd, bool clEndInc) =
                ClipRange(null, true, null, false, descriptor);

            string? cursorKey = clStart;
            bool    cursorInc = clStartInc;

            while (true)
            {
                KeyValueGetByRangeResult page = await QueryDescriptorRange(
                    descriptor.PartitionId, transactionId, prefixedKey,
                    cursorKey, cursorInc, clEnd, clEndInc,
                    bucketPageSize, HLCTimestamp.Zero, durability, cancellationToken);

                if (page.Type is KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication)
                    return new(page.Type, []);

                if (page.Type != KeyValueResponseType.Get)
                    break;

                allItems.AddRange(page.Items);

                if (!page.HasMore || page.NextCursor is null)
                    break;

                if (!KeyValueRangeCursor.TryDecode(page.NextCursor, out string lastKey, out _, out _, out _))
                    break;

                cursorKey = lastKey;
                cursorInc = false;
            }
        }

        // No explicit sort needed: FindAll returns descriptors in StartKey order, ranges are disjoint
        // (no-overlap invariant), and each range's pages are key-sorted — so the concatenation is
        // already globally ordered, exactly as in LocateAndGetByRange.
        return new(KeyValueResponseType.Get, allItems);
    }

    /// <summary>
    /// Locates the leader for the given prefix and executes a bounded, cursor-paged range scan.
    /// For unsplit spaces routes to the single partition leader directly. For split key-range spaces
    /// fans out across all intersecting descriptors in StartKey order, clips each sub-range, and
    /// merges results maintaining key order (Task 10 multi-range stitch).
    /// </summary>
    public async Task<KeyValueGetByRangeResult> LocateAndGetByRange(
        HLCTimestamp transactionId,
        string prefix,
        string? startKey,
        bool startInclusive,
        string? endKey,
        bool endInclusive,
        int limit,
        HLCTimestamp readTimestamp,
        KeyValueDurability durability,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(prefix))
            return new(KeyValueResponseType.Errored, [], null, false);

        // Fast path: unsplit space (or hash space) — single partition, no fan-out overhead.
        if (RangeRouting.IsPrefixOpSafe(keySpaceRegistry, manager.RangeMapStore.Current, prefix))
        {
            int singlePartitionId = RoutePrefixKey(prefix);

            if (!raft.Joined || await raft.AmILeader(singlePartitionId, cancellationToken))
                return await manager.GetByRange(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, limit, readTimestamp, durability);

            string singleLeader = await raft.WaitForLeader(singlePartitionId, cancellationToken);
            if (singleLeader == raft.GetLocalEndpoint())
                return new(KeyValueResponseType.MustRetry, [], null, false);

            logger.LogDebug("GETRANGE-KEYVALUE Redirect {Prefix} to leader partition {Partition} at {Leader}", prefix, singlePartitionId, singleLeader);

            return await interNodeCommunication.GetByRange(singleLeader, transactionId, prefix, startKey, startInclusive, endKey, endInclusive, limit, readTimestamp, durability, cancellationToken);
        }

        // Multi-range path: key-range space has been split; fan out across intersecting descriptors.
        // RangeMap is snapshotted once for this page. A split landing mid-fan-out means the loop
        // may query a now-stale source partition, but that is safe: Task 6 orphan-retains [K,E) on
        // the source, so the fixed readTimestamp (MVCC) still resolves correctly from there. The
        // next page re-resolves RangeMapStore.Current fresh and routes to the new partition.
        string keySpace = KeySpaceRegistry.ExtractKeySpace(prefix + "/");
        RangeMap rangeMap = manager.RangeMapStore.Current;
        IReadOnlyList<RangeDescriptor> descriptors = rangeMap.FindIntersecting(keySpace, startKey, endKey);

        if (descriptors.Count == 0)
            return new(KeyValueResponseType.Get, [], null, false);

        var accumulated = new List<(string, ReadOnlyKeyValueEntry)>();
        int remaining   = limit > 0 ? limit : int.MaxValue;
        bool hasMore    = false;

        foreach (RangeDescriptor descriptor in descriptors)
        {
            if (remaining <= 0) { hasMore = true; break; }

            (string? clStart, bool clStartInc, string? clEnd, bool clEndInc) =
                ClipRange(startKey, startInclusive, endKey, endInclusive, descriptor);

            int pageLimit = remaining == int.MaxValue ? 0 : remaining;

            KeyValueGetByRangeResult part = await QueryDescriptorRange(
                descriptor.PartitionId, transactionId, prefix,
                clStart, clStartInc, clEnd, clEndInc,
                pageLimit, readTimestamp, durability, cancellationToken);

            if (part.Type is KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication)
                return part;

            if (part.Type != KeyValueResponseType.Get)
                continue;

            accumulated.AddRange(part.Items);

            if (limit > 0)
                remaining -= part.Items.Count;

            if (part.HasMore) { hasMore = true; break; }
        }

        if (accumulated.Count == 0)
            return new(KeyValueResponseType.Get, [], null, false);

        string? cursor = null;
        if (hasMore)
        {
            string lastKey = accumulated[^1].Item1;
            HLCTimestamp ts = readTimestamp.IsNull() ? HLCTimestamp.Zero : readTimestamp;
            // Intentionally generation-free: each page re-resolves FindIntersecting from lastKey
            // against the live map, so a split between pages is handled unconditionally — no
            // generation miss-detection needed. Do not add rangeGeneration here.
            cursor = KeyValueRangeCursor.Encode(lastKey, durability, prefix, ts);
        }

        return new(KeyValueResponseType.Get, accumulated, cursor, hasMore);
    }

    /// <summary>Routes a GetByRange page to <paramref name="partitionId"/>'s leader.</summary>
    private async Task<KeyValueGetByRangeResult> QueryDescriptorRange(
        int partitionId,
        HLCTimestamp transactionId,
        string prefix,
        string? startKey, bool startInclusive,
        string? endKey,   bool endInclusive,
        int limit,
        HLCTimestamp readTimestamp,
        KeyValueDurability durability,
        CancellationToken cancellationToken)
    {
        if (!raft.Joined || await raft.AmILeader(partitionId, cancellationToken))
            return await manager.GetByRange(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, limit, readTimestamp, durability);

        string leader = await raft.WaitForLeader(partitionId, cancellationToken);
        if (leader == raft.GetLocalEndpoint())
            return new(KeyValueResponseType.MustRetry, [], null, false);

        return await interNodeCommunication.GetByRange(leader, transactionId, prefix, startKey, startInclusive, endKey, endInclusive, limit, readTimestamp, durability, cancellationToken);
    }

    /// <summary>
    /// Clips the caller's query range <c>[queryStart, queryEnd)</c> to the descriptor's half-open
    /// interval <c>[d.StartKey, d.EndKey)</c>, preserving the caller's inclusive/exclusive flags
    /// where they dominate; the descriptor boundary is always inclusive at start, exclusive at end.
    /// </summary>
    private static (string? start, bool startInc, string? end, bool endInc) ClipRange(
        string? queryStart, bool queryStartInc,
        string? queryEnd,   bool queryEndInc,
        RangeDescriptor d)
    {
        string? clStart;
        bool    clStartInc;

        if (d.StartKey is null)
        {
            // descriptor starts at -∞; query start is the effective lower bound
            clStart    = queryStart;
            clStartInc = queryStartInc;
        }
        else if (queryStart is null)
        {
            // query unbounded below; descriptor's start is the effective lower bound (inclusive)
            clStart    = d.StartKey;
            clStartInc = true;
        }
        else
        {
            int cmp = string.CompareOrdinal(queryStart, d.StartKey);
            if (cmp >= 0) { clStart = queryStart; clStartInc = queryStartInc; }
            else          { clStart = d.StartKey;  clStartInc = true; }
        }

        string? clEnd;
        bool    clEndInc;

        if (d.EndKey is null && queryEnd is null)
        {
            clEnd    = null;
            clEndInc = false;
        }
        else if (d.EndKey is null)
        {
            clEnd    = queryEnd;
            clEndInc = queryEndInc;
        }
        else if (queryEnd is null)
        {
            clEnd    = d.EndKey;
            clEndInc = false;  // descriptor boundary is always exclusive
        }
        else
        {
            int cmp = string.CompareOrdinal(queryEnd, d.EndKey);
            if (cmp <= 0) { clEnd = queryEnd;  clEndInc = queryEndInc; }
            else          { clEnd = d.EndKey;   clEndInc = false; }
        }

        return (clStart, clStartInc, clEnd, clEndInc);
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
        
        int partitionId = dataPartitionRouter.Locate(options.UniqueId);

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
        List<KeyValueTransactionReadKey> readKeys,
        CancellationToken cancellationToken
    )
    {
        if (string.IsNullOrEmpty(uniqueId))
            return KeyValueResponseType.Errored;
        
        int partitionId = dataPartitionRouter.Locate(uniqueId);

        if (!raft.Joined || await raft.AmILeader(partitionId, cancellationToken))
            return await manager.CommitTransaction(timestamp, acquiredLocks, modifiedKeys, readKeys);
            
        string leader = await raft.WaitForLeader(partitionId, cancellationToken);
        if (leader == raft.GetLocalEndpoint())
            return KeyValueResponseType.MustRetry;
        
        logger.LogDebug("COMMIT-TRANSACTION Redirect {KeyValueName} to leader partition {Partition} at {Leader}", uniqueId, partitionId, leader);
        
        return await interNodeCommunication.CommitTransaction(leader, uniqueId, timestamp, acquiredLocks, modifiedKeys, readKeys, cancellationToken);
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
        
        int partitionId = dataPartitionRouter.Locate(uniqueId);

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
    public async Task<KeyValueGetByBucketResult> ScanAllByPrefix(string prefixKeyName, KeyValueDurability durability, CancellationToken cancellationToken)
    {
        ConcurrentDictionary<string, ReadOnlyKeyValueEntry> unionItems = [];
        
        KeyValueGetByBucketResult items = await manager.ScanByPrefix(prefixKeyName, durability);

        if (items.Type == KeyValueResponseType.Get)
        {
            foreach ((string, ReadOnlyKeyValueEntry) item in items.Items)
                unionItems.TryAdd(item.Item1, item.Item2);
        }

        IList<RaftNode> nodes = raft.GetNodes();
        
        List<Task> tasks = new(nodes.Count);

        foreach (RaftNode node in nodes)
            tasks.Add(NodeScanByPrefix(unionItems, node, prefixKeyName, durability, cancellationToken));
        
        await Task.WhenAll(tasks);               

        if (durability == KeyValueDurability.Persistent)
        {
            KeyValueGetByBucketResult result = await manager.ScanByPrefixFromDisk(prefixKeyName);

            if (items.Type == KeyValueResponseType.Get)
            {
                foreach ((string, ReadOnlyKeyValueEntry) item in result.Items)
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
        ConcurrentDictionary<string, ReadOnlyKeyValueEntry> unionItems, 
        RaftNode node, 
        string prefixKeyName, 
        KeyValueDurability durability, 
        CancellationToken cancellationToken
    )
    {
        KeyValueGetByBucketResult response = await interNodeCommunication.ScanByPrefix(node.Endpoint, prefixKeyName, durability, cancellationToken);
        
        if (response.Type == KeyValueResponseType.Get)
        {
            foreach ((string, ReadOnlyKeyValueEntry) item in response.Items)
                unionItems.TryAdd(item.Item1, item.Item2);
        }
    }    
}
