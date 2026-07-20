using System.Collections.Concurrent;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Locks;
using Kahuna.Server.Locks.Data;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Locks;
using Kommander.Time;

namespace Kahuna.Server.Communication.Internode;

/// <summary>
/// Provides inter-node communication functionality using memory-based calls, implementing operations
/// such as locking, unlocking, key-value management, and transactional support among distributed nodes.
/// </summary>
public class MemoryInterNodeCommmunication : IInterNodeCommunication
{
    private int getByRangeCallCount;
    private int beginOperationCallCount;
    private int completeOperationCallCount;

    /// <summary>Number of <c>GetByRange</c> RPCs dispatched to a remote node. Used by multi-range fan-out tests.</summary>
    public int GetByRangeCallCount => Volatile.Read(ref getByRangeCallCount);

    /// <summary>Number of register-remote <c>BeginOperation</c> RPCs dispatched to a remote coordinator node.</summary>
    public int BeginOperationCallCount => Volatile.Read(ref beginOperationCallCount);

    /// <summary>Number of register-remote <c>CompleteOperation</c> RPCs dispatched to a remote coordinator node.</summary>
    public int CompleteOperationCallCount => Volatile.Read(ref completeOperationCallCount);

    /// <summary>
    /// Stores the mapping of node names to their respective `IKahuna` instances.
    /// </summary>
    private Dictionary<string, IKahuna>? nodes;

    /// <summary>
    /// Sets the nodes for inter-node communication.
    /// </summary>
    /// <param name="nodes">A dictionary mapping node names to `IKahuna` instances.</param>
    public void SetNodes(Dictionary<string, IKahuna> nodes)
    {
        this.nodes = nodes;
    }
    
    /// <summary>
    /// Attempts to acquire a lock on a resource in a specific node.
    /// </summary>
    /// <param name="node">The target node.</param>
    /// <param name="resource">The resource to lock.</param>
    /// <param name="owner">The owner of the lock.</param>
    /// <param name="expiresMs">The expiration time in milliseconds.</param>
    /// <param name="durability">The durability level of the lock.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A tuple containing the lock response type and the lock revision.</returns>
    /// <exception cref="KahunaServerException">Thrown if the node does not exist.</exception>
    public async Task<(LockResponseType, long)> TryLock(
        string node, 
        string resource, 
        byte[] owner, 
        int expiresMs, 
        LockDurability durability, 
        CancellationToken cancellationToken
    )
    {
        if (nodes is not null && nodes.TryGetValue(node, out IKahuna? kahunaNode))
            return await kahunaNode.TryLock(resource, owner, expiresMs, durability);
        
        throw new KahunaServerException($"The node {node} does not exist.");
    }
    
    /// <summary>
    /// Attempts to extend an existing lock on a resource in a specific node.
    /// </summary>
    /// <param name="node">The target node.</param>
    /// <param name="resource">The resource to extend the lock on.</param>
    /// <param name="owner">The owner of the lock.</param>
    /// <param name="expiresMs">The new expiration time in milliseconds.</param>
    /// <param name="durability">The durability level of the lock.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A tuple containing the lock response type and the lock revision.</returns>
    /// <exception cref="KahunaServerException">Thrown if the node does not exist.</exception>
    public async Task<(LockResponseType, long)> TryExtendLock(
        string node, 
        string resource, 
        byte[] owner, 
        int expiresMs, 
        LockDurability durability, 
        CancellationToken cancellationToken
    )
    {
        if (nodes is not null && nodes.TryGetValue(node, out IKahuna? kahunaNode))
            return await kahunaNode.TryExtendLock(resource, owner, expiresMs, durability);
        
        throw new KahunaServerException($"The node {node} does not exist.");
    }
    
    /// <summary>
    /// Attempts to release a lock on a resource in a specific node.
    /// </summary>
    /// <param name="node">The target node.</param>
    /// <param name="resource">The resource to unlock.</param>
    /// <param name="owner">The owner of the lock.</param>
    /// <param name="durability">The durability level of the lock.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>The lock response type.</returns>
    /// <exception cref="KahunaServerException">Thrown if the node does not exist.</exception>
    public async Task<LockResponseType> TryUnlock(
        string node, 
        string resource, 
        byte[] owner, 
        LockDurability durability, 
        CancellationToken cancellationToken
    )
    {
        if (nodes is not null && nodes.TryGetValue(node, out IKahuna? kahunaNode))
            return await kahunaNode.TryUnlock(resource, owner, durability);
        
        throw new KahunaServerException($"The node {node} does not exist.");
    }
    
    /// <summary>
    /// Retrieves information about the lock
    /// </summary>
    /// <param name="node">The target node.</param>
    /// <param name="resource">The resource to retrieve the lock for.</param>
    /// <param name="durability">The durability level of the lock.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A tuple containing the lock response type and the lock context.</returns>
    /// <exception cref="KahunaServerException">Thrown if the node does not exist.</exception>
    public async Task<(LockResponseType, ReadOnlyLockEntry?)> GetLock(
        string node, 
        string resource, 
        LockDurability durability, 
        CancellationToken cancellationToken
    )
    {
        if (nodes is not null && nodes.TryGetValue(node, out IKahuna? kahunaNode))
            return await kahunaNode.GetLock(resource, durability);
        
        throw new KahunaServerException($"The node {node} does not exist.");
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="node"></param>
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
    /// <exception cref="KahunaServerException"></exception>
    public async Task<(KeyValueResponseType, long, HLCTimestamp)> TrySetKeyValue(
        string node,
        HLCTimestamp transactionId,
        string key,
        byte[]? value,
        byte[]? compareValue,
        long compareRevision,
        KeyValueFlags flags,
        int expiresMs,
        KeyValueDurability durability,
        long routedGeneration,
        CancellationToken cancellationToken
    )
    {
        if (nodes is not null && nodes.TryGetValue(node, out IKahuna? kahunaNode))
            return await kahunaNode.TrySetKeyValue(transactionId, key, value, compareValue, compareRevision, flags, expiresMs, durability, routedGeneration);

        throw new KahunaServerException($"The node {node} does not exist.");
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="node"></param>
    /// <param name="items"></param>
    /// <param name="lockSync"></param>
    /// <param name="responses"></param>
    /// <param name="cancellationToken"></param>
    /// <exception cref="KahunaServerException"></exception>
    public async Task TrySetManyNodeKeyValue(
        string node, 
        List<KahunaSetKeyValueRequestItem> items, 
        Lock lockSync, 
        List<KahunaSetKeyValueResponseItem> responses, 
        CancellationToken cancellationToken
    )
    {
        if (nodes is not null && nodes.TryGetValue(node, out IKahuna? kahunaNode))
        {
            ConcurrentBag<KahunaSetKeyValueResponseItem> bag = [];

            foreach (KahunaSetKeyValueRequestItem item in items)
            {
                (KeyValueResponseType, long, HLCTimestamp) resp = await kahunaNode.TrySetKeyValue(
                    item.TransactionId,
                    item.Key ?? "",
                    item.Value,
                    item.CompareValue,
                    item.CompareRevision,
                    item.Flags,
                    item.ExpiresMs,
                    item.Durability,
                    item.RoutedGeneration
                );
                
                bag.Add(new()
                {
                    Key = item.Key,
                    Type = resp.Item1,
                    Revision = resp.Item2,
                    LastModified = resp.Item3,
                    Durability = item.Durability
                });
            }

            SetKeyValueLockResponses(bag, lockSync, responses);
            return;
        }
        
        throw new KahunaServerException($"The node {node} does not exist.");
    }
    
    /// <summary>
    /// 
    /// </summary>
    /// <param name="bag"></param>
    /// <param name="lockSync"></param>
    /// <param name="responses"></param>
    private static void SetKeyValueLockResponses(
        ConcurrentBag<KahunaSetKeyValueResponseItem> bag, 
        Lock lockSync, 
        List<KahunaSetKeyValueResponseItem> responses
    )
    {
        foreach (KahunaSetKeyValueResponseItem reponseBag in bag)
        {
            lock (lockSync)
                responses.Add(reponseBag);
        }
    }

    public async Task TryDeleteManyNodeKeyValue(
        string node,
        List<KahunaDeleteKeyValueRequestItem> items,
        Lock lockSync,
        List<KahunaDeleteKeyValueResponseItem> responses,
        CancellationToken cancellationToken
    )
    {
        if (nodes is not null && nodes.TryGetValue(node, out IKahuna? kahunaNode))
        {
            ConcurrentBag<KahunaDeleteKeyValueResponseItem> bag = [];

            foreach (KahunaDeleteKeyValueRequestItem item in items)
            {
                (KeyValueResponseType, long, HLCTimestamp) resp = await kahunaNode.TryDeleteKeyValue(
                    item.TransactionId,
                    item.Key ?? "",
                    item.Durability
                );

                bag.Add(new()
                {
                    Key = item.Key,
                    Type = resp.Item1,
                    Revision = resp.Item2,
                    LastModified = resp.Item3,
                    Durability = item.Durability
                });
            }

            foreach (KahunaDeleteKeyValueResponseItem responseBag in bag)
            {
                lock (lockSync)
                    responses.Add(responseBag);
            }

            return;
        }

        throw new KahunaServerException($"The node {node} does not exist.");
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="node"></param>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    /// <exception cref="KahunaServerException"></exception>
    public async Task<(KeyValueResponseType, long, HLCTimestamp)> TryDeleteKeyValue(
        string node, 
        HLCTimestamp transactionId, 
        string key, 
        KeyValueDurability durability, 
        CancellationToken cancellationToken
    )
    {
        if (nodes is not null && nodes.TryGetValue(node, out IKahuna? kahunaNode))
            return await kahunaNode.TryDeleteKeyValue(transactionId, key, durability);
        
        throw new KahunaServerException($"The node {node} does not exist.");
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="node"></param>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="expiresMs"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    /// <exception cref="KahunaServerException"></exception>
    public async Task<(KeyValueResponseType, long, HLCTimestamp)> TryExtendKeyValue(
        string node, 
        HLCTimestamp transactionId, 
        string key, 
        int expiresMs, 
        KeyValueDurability durability, 
        CancellationToken cancellationToken
    )
    {
        if (nodes is not null && nodes.TryGetValue(node, out IKahuna? kahunaNode))
            return await kahunaNode.TryExtendKeyValue(transactionId, key, expiresMs, durability);
        
        throw new KahunaServerException($"The node {node} does not exist.");
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="node"></param>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="revision"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    /// <exception cref="KahunaServerException"></exception>
    public async Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> TryGetValue(
        string node,
        HLCTimestamp transactionId,
        string key,
        long revision,
        HLCTimestamp readTimestamp,
        KeyValueDurability durability,
        CancellationToken cancellationToken
    )
    {
        if (nodes is not null && nodes.TryGetValue(node, out IKahuna? kahunaNode))
            return await kahunaNode.TryGetValue(transactionId, key, revision, readTimestamp, durability);

        throw new KahunaServerException($"The node {node} does not exist.");
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="node"></param>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="revision"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    /// <exception cref="KahunaServerException"></exception>
    public async Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> TryExistsValue(
        string node,
        HLCTimestamp transactionId,
        string key,
        long revision,
        HLCTimestamp readTimestamp,
        KeyValueDurability durability,
        CancellationToken cancellationToken
    )
    {
        if (nodes is not null && nodes.TryGetValue(node, out IKahuna? kahunaNode))
            return await kahunaNode.TryExistsValue(transactionId, key, revision, readTimestamp, durability);

        throw new KahunaServerException($"The node {node} does not exist.");
    }

    public async Task TryGetManyNodeValues(
        string node,
        HLCTimestamp transactionId,
        HLCTimestamp readTimestamp,
        List<(string key, long revision, KeyValueDurability durability)> keys,
        Lock lockSync,
        List<(KeyValueResponseType type, string key, KeyValueDurability durability, ReadOnlyKeyValueEntry? entry)> responses,
        CancellationToken cancellationToken
    )
    {
        if (nodes is not null && nodes.TryGetValue(node, out IKahuna? kahunaNode))
        {
            List<(KeyValueResponseType type, string key, KeyValueDurability durability, ReadOnlyKeyValueEntry? entry)> readResponses =
                await kahunaNode.TryGetManyValues(transactionId, readTimestamp, keys);

            // Test seam: replace designated per-key results with MustRetry to simulate a per-key routing
            // transient. Only fires when GetManyValuesFault is set by a test.
            if (GetManyValuesFault is not null)
            {
                for (int i = 0; i < readResponses.Count; i++)
                {
                    (_, string key, KeyValueDurability dur, _) = readResponses[i];
                    if (GetManyValuesFault(transactionId, key))
                        readResponses[i] = (KeyValueResponseType.MustRetry, key, dur, null);
                }
            }

            // Test seam: replace designated per-key results with Errored to simulate a dropped actor
            // response (a non-transient, non-confirmed type). Only fires when GetManyValuesErrorFault is set.
            if (GetManyValuesErrorFault is not null)
            {
                for (int i = 0; i < readResponses.Count; i++)
                {
                    (_, string key, KeyValueDurability dur, _) = readResponses[i];
                    if (GetManyValuesErrorFault(transactionId, key))
                        readResponses[i] = (KeyValueResponseType.Errored, key, dur, null);
                }
            }

            AddToReadManyResponses(readResponses, lockSync, responses);
            return;
        }

        throw new KahunaServerException($"The node {node} does not exist.");
    }

    /// <summary>
    /// Test seam: when set, invoked per-key after the remote batch read completes. Returns true to replace
    /// that key's result with <c>MustRetry</c>, simulating a per-key routing transient in a batch read.
    /// </summary>
    public Func<HLCTimestamp, string, bool>? GetManyValuesFault { get; set; }

    /// <summary>
    /// Test seam: when set, invoked per-key after the remote batch read completes. Returns true to replace
    /// that key's result with <c>Errored</c>, simulating a dropped actor response — a non-transient,
    /// non-confirmed type that must still be excluded from the read set rather than folded as "absent".
    /// </summary>
    public Func<HLCTimestamp, string, bool>? GetManyValuesErrorFault { get; set; }

    public async Task TryExistsManyNodeValues(
        string node,
        HLCTimestamp transactionId,
        HLCTimestamp readTimestamp,
        List<(string key, long revision, KeyValueDurability durability)> keys,
        Lock lockSync,
        List<(KeyValueResponseType type, string key, KeyValueDurability durability, ReadOnlyKeyValueEntry? entry)> responses,
        CancellationToken cancellationToken
    )
    {
        if (nodes is not null && nodes.TryGetValue(node, out IKahuna? kahunaNode))
        {
            List<(KeyValueResponseType type, string key, KeyValueDurability durability, ReadOnlyKeyValueEntry? entry)> readResponses =
                await kahunaNode.TryExistsManyValues(transactionId, readTimestamp, keys);

            AddToReadManyResponses(readResponses, lockSync, responses);
            return;
        }

        throw new KahunaServerException($"The node {node} does not exist.");
    }

    private static void AddToReadManyResponses(
        IEnumerable<(KeyValueResponseType type, string key, KeyValueDurability durability, ReadOnlyKeyValueEntry? entry)> items,
        Lock lockSync,
        List<(KeyValueResponseType type, string key, KeyValueDurability durability, ReadOnlyKeyValueEntry? entry)> responses
    )
    {
        foreach ((KeyValueResponseType type, string key, KeyValueDurability durability, ReadOnlyKeyValueEntry? entry) in items)
        {
            lock (lockSync)
                responses.Add((type, key, durability, entry));
        }
    }

    public async Task<KeyValueResponseType> TryCheckWriteIntentValue(
        string node,
        HLCTimestamp transactionId,
        string key,
        KeyValueDurability durability,
        CancellationToken cancellationToken
    )
    {
        if (nodes is not null && nodes.TryGetValue(node, out IKahuna? kahunaNode))
            return await kahunaNode.TryCheckWriteIntentValue(transactionId, key, durability);

        throw new KahunaServerException($"The node {node} does not exist.");
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="node"></param>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="expiresMs"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    /// <exception cref="KahunaServerException"></exception>
    public async Task<(KeyValueResponseType, string, KeyValueDurability, HLCTimestamp HolderTransactionId)> TryAcquireExclusiveLock(
        string node,
        HLCTimestamp transactionId,
        string key,
        int expiresMs,
        KeyValueDurability durability,
        CancellationToken cancellationToken
    )
    {
        if (nodes is not null && nodes.TryGetValue(node, out IKahuna? kahunaNode))
            return await kahunaNode.TryAcquireExclusiveLock(transactionId, key, expiresMs, durability);

        throw new KahunaServerException($"The node {node} does not exist.");
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="node"></param>
    /// <param name="transactionId"></param>
    /// <param name="prefixKey"></param>
    /// <param name="expiresMs"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public async Task<KeyValueResponseType> TryAcquireExclusivePrefixLock(
        string node, 
        HLCTimestamp transactionId,
        string prefixKey, 
        int expiresMs, 
        KeyValueDurability durability, 
        CancellationToken cancellationToken
    )
    {
        if (nodes is not null && nodes.TryGetValue(node, out IKahuna? kahunaNode))
            return await kahunaNode.TryAcquireExclusivePrefixLock(transactionId, prefixKey, expiresMs, durability);
        
        throw new KahunaServerException($"The node {node} does not exist.");
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="node"></param>
    /// <param name="transactionId"></param>
    /// <param name="xkeys"></param>
    /// <param name="lockSync"></param>
    /// <param name="responses"></param>
    /// <param name="cancellationToken"></param>
    /// <exception cref="KahunaServerException"></exception>
    public async Task TryAcquireNodeExclusiveLocks(
        string node,
        HLCTimestamp transactionId,
        List<(string key, int expiresMs, KeyValueDurability durability)> xkeys,
        Lock lockSync,
        List<(KeyValueResponseType type, string key, KeyValueDurability durability, HLCTimestamp holder)> responses,
        CancellationToken cancellationToken
    )
    {
        if (nodes is not null && nodes.TryGetValue(node, out IKahuna? kahunaNode))
        {
            ConcurrentBag<(KeyValueResponseType type, string key, KeyValueDurability durability, HLCTimestamp holder)> bag = [];

            foreach ((string key, int expiresMs, KeyValueDurability durability) in xkeys)
                bag.Add(await kahunaNode.TryAcquireExclusiveLock(transactionId, key, expiresMs, durability));

            AddToAcquireLockResponses(bag, lockSync, responses);
            return;
        }

        throw new KahunaServerException($"The node {node} does not exist.");
    }

    private static void AddToAcquireLockResponses(
        ConcurrentBag<(KeyValueResponseType type, string key, KeyValueDurability durability, HLCTimestamp holder)> bag,
        Lock lockSync,
        List<(KeyValueResponseType type, string key, KeyValueDurability durability, HLCTimestamp holder)> responses
    )
    {
        foreach ((KeyValueResponseType type, string key, KeyValueDurability durability, HLCTimestamp holder) in bag)
        {
            lock (lockSync)
                responses.Add((type, key, durability, holder));
        }
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="node"></param>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    /// <exception cref="KahunaServerException"></exception>
    public async Task<(KeyValueResponseType, string)> TryReleaseExclusiveLock(
        string node, 
        HLCTimestamp transactionId, 
        string key, 
        KeyValueDurability durability,
        CancellationToken cancellationToken
    )
    {
        if (nodes is not null && nodes.TryGetValue(node, out IKahuna? kahunaNode))
            return await kahunaNode.TryReleaseExclusiveLock(transactionId, key, durability);
        
        throw new KahunaServerException($"The node {node} does not exist.");
    }
    
    /// <summary>
    /// 
    /// </summary>
    /// <param name="node"></param>
    /// <param name="transactionId"></param>
    /// <param name="prefixKey"></param>
    /// <param name="expiresMs"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    /// <exception cref="NotImplementedException"></exception>
    public async Task<KeyValueResponseType> TryReleaseExclusivePrefixLock(
        string node, 
        HLCTimestamp transactionId, 
        string prefixKey, 
        KeyValueDurability durability, 
        CancellationToken cancellationToken
    )
    {
        if (nodes is not null && nodes.TryGetValue(node, out IKahuna? kahunaNode))
            return await kahunaNode.TryReleaseExclusivePrefixLock(transactionId, prefixKey, durability);

        throw new KahunaServerException($"The node {node} does not exist.");
    }

    /// <summary>
    /// Optional hook invoked before every <c>TryAcquireRangeLock</c> RPC. Used by tests to inject latency
    /// or cancellation for a specific transaction so the bounded-parallel sweep can be stressed without
    /// needing a real slow participant.
    /// </summary>
    public Func<HLCTimestamp, string, CancellationToken, Task>? AcquireRangeLockHook { get; set; }

    public async Task<(KeyValueResponseType, HLCTimestamp HolderTransactionId)> TryAcquireRangeLock(
        string node,
        HLCTimestamp transactionId,
        string prefix,
        string? startKey, bool startInclusive,
        string? endKey,   bool endInclusive,
        int expiresMs,
        KeyValueDurability durability,
        RangeLockMode mode,
        CancellationToken cancellationToken
    )
    {
        // Pass the caller's token so an injected delay can honor the renewal-sweep deadline (a slow
        // participant is cancelled at the budget, not merely delayed).
        if (AcquireRangeLockHook is not null)
            await AcquireRangeLockHook(transactionId, prefix, cancellationToken);

        if (nodes is not null && nodes.TryGetValue(node, out IKahuna? kahunaNode))
            return await kahunaNode.TryAcquireRangeLock(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, expiresMs, durability, mode);
        throw new KahunaServerException($"The node {node} does not exist.");
    }

    public Task<(KeyValueResponseType, HLCTimestamp HolderTransactionId)> TryAcquireExclusiveRangeLock(
        string node,
        HLCTimestamp transactionId,
        string prefix,
        string? startKey, bool startInclusive,
        string? endKey, bool endInclusive,
        int expiresMs,
        KeyValueDurability durability,
        CancellationToken cancellationToken
    ) => TryAcquireRangeLock(node, transactionId, prefix, startKey, startInclusive, endKey, endInclusive, expiresMs, durability, RangeLockMode.Exclusive, cancellationToken);

    public async Task<KeyValueResponseType> TryReleaseExclusiveRangeLock(
        string node,
        HLCTimestamp transactionId,
        string prefix,
        string? startKey, bool startInclusive,
        string? endKey, bool endInclusive,
        KeyValueDurability durability,
        CancellationToken cancellationToken
    )
    {
        if (nodes is not null && nodes.TryGetValue(node, out IKahuna? kahunaNode))
            return await kahunaNode.TryReleaseExclusiveRangeLock(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, durability);
        throw new KahunaServerException($"The node {node} does not exist.");
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="node"></param>
    /// <param name="transactionId"></param>
    /// <param name="xkeys"></param>
    /// <param name="lockSync"></param>
    /// <param name="responses"></param>
    /// <param name="cancellationToken"></param>
    /// <exception cref="KahunaServerException"></exception>
    public async Task TryReleaseNodeExclusiveLocks(
        string node, 
        HLCTimestamp transactionId, 
        List<(string key, KeyValueDurability durability)> xkeys, 
        Lock lockSync, 
        List<(KeyValueResponseType type, string key, KeyValueDurability durability)> responses, 
        CancellationToken cancellationToken
    )
    {
        if (nodes is not null && nodes.TryGetValue(node, out IKahuna? kahunaNode))
        {
            ConcurrentBag<(KeyValueResponseType type, string key, KeyValueDurability durability)> bag = [];

            foreach ((string key, KeyValueDurability durability) in xkeys)
            {
                (KeyValueResponseType type, string _) = await kahunaNode.TryReleaseExclusiveLock(transactionId, key, durability);
                bag.Add((type, key, durability));
            }

            AddToReleaseLockResponses(bag, lockSync, responses);
            return;
        }
        
        throw new KahunaServerException($"The node {node} does not exist.");
    }
    
    /// <summary>
    /// 
    /// </summary>
    /// <param name="bag"></param>
    /// <param name="lockSync"></param>
    /// <param name="responses"></param>
    private static void AddToReleaseLockResponses(ConcurrentBag<(KeyValueResponseType type, string key, KeyValueDurability durability)> bag, Lock lockSync, List<(KeyValueResponseType type, string key, KeyValueDurability durability)> responses)
    {
        foreach ((KeyValueResponseType type, string key, KeyValueDurability durability) in bag)
        {
            lock (lockSync)
                responses.Add((type, key, durability));
        }
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="node"></param>
    /// <param name="transactionId"></param>
    /// <param name="commitId"></param>
    /// <param name="key"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    /// <exception cref="KahunaServerException"></exception>
    public async Task<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)> TryPrepareMutations(
        string node,
        HLCTimestamp transactionId,
        HLCTimestamp commitId,
        string key,
        KeyValueDurability durability,
        long routedGeneration,
        CancellationToken cancellationToken,
        string? recordAnchorKey = null
    )
    {
        if (nodes is not null && nodes.TryGetValue(node, out IKahuna? kahunaNode))
            return await kahunaNode.TryPrepareMutations(transactionId, commitId, key, durability, routedGeneration, recordAnchorKey);

        throw new KahunaServerException($"The node {node} does not exist.");
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="node"></param>
    /// <param name="transactionId"></param>
    /// <param name="commitId"></param>
    /// <param name="xkeys"></param>
    /// <param name="lockSync"></param>
    /// <param name="responses"></param>
    /// <param name="cancellationToken"></param>
    /// <exception cref="KahunaServerException"></exception>
    public async Task TryPrepareNodeMutations(
        string node, 
        HLCTimestamp transactionId,
        HLCTimestamp commitId,
        List<(string key, KeyValueDurability durability)> xkeys, 
        Lock lockSync, 
        List<(KeyValueResponseType type, HLCTimestamp, string key, KeyValueDurability durability)> responses,
        CancellationToken cancellationToken,
        string? recordAnchorKey = null
    )
    {
        if (nodes is not null && nodes.TryGetValue(node, out IKahuna? kahunaNode))
        {
            ConcurrentBag<(KeyValueResponseType type, HLCTimestamp, string key, KeyValueDurability durability)> bag = [];

            foreach ((string key, KeyValueDurability durability) in xkeys)
            {
                (KeyValueResponseType type, HLCTimestamp proposalId, string _, KeyValueDurability _) = await kahunaNode.TryPrepareMutations(transactionId, commitId, key, durability, 0, recordAnchorKey);
                bag.Add((type, proposalId, key, durability));
            }

            AddToPrepareMutationsResponses(bag, lockSync, responses);
            return;
        }
        
        throw new KahunaServerException($"The node {node} does not exist.");
    }
    
    /// <summary>
    /// 
    /// </summary>
    /// <param name="bag"></param>
    /// <param name="lockSync"></param>
    /// <param name="responses"></param>
    private static void AddToPrepareMutationsResponses(
        ConcurrentBag<(KeyValueResponseType type, HLCTimestamp, string key, KeyValueDurability durability)> bag, 
        Lock lockSync, 
        List<(KeyValueResponseType type, HLCTimestamp, string key, KeyValueDurability durability)> responses
    )
    {
        foreach ((KeyValueResponseType type, HLCTimestamp ticketId, string key, KeyValueDurability durability) in bag)
        {
            lock (lockSync)
                responses.Add((type, ticketId, key, durability));
        }
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="node"></param>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="ticketId"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    /// <exception cref="KahunaServerException"></exception>
    public async Task<(KeyValueResponseType, long)> TryCommitMutations(
        string node, 
        HLCTimestamp transactionId, 
        string key, 
        HLCTimestamp ticketId, 
        KeyValueDurability durability, 
        CancellationToken cancellationToken
    )
    {
        if (nodes is not null && nodes.TryGetValue(node, out IKahuna? kahunaNode))
        {
            // Test seam: fail a specific participant's inter-node commit transiently before it reaches the
            // remote leader, so its prepare survives on that leader for recovery to drive — the partial durable
            // commit condition. Returns the retryable signal, never a definite failure.
            if (CommitMutationsFault is not null && CommitMutationsFault(transactionId, key))
                return (KeyValueResponseType.MustRetry, 0);

            // Test seam: let the commit apply on the remote leader (so the value, receipt, and — for an anchor —
            // the durable decision are all installed there) but hide its success from the caller as MustRetry.
            // This models a committed-but-lost/covered response: the coordinator cannot tell the commit landed.
            if (CommitMutationsResponseFault is not null && CommitMutationsResponseFault(transactionId, key))
            {
                await kahunaNode.TryCommitMutations(transactionId, key, ticketId, durability);
                return (KeyValueResponseType.MustRetry, 0);
            }

            return await kahunaNode.TryCommitMutations(transactionId, key, ticketId, durability);
        }

        throw new KahunaServerException($"The node {node} does not exist.");
    }

    public async Task<bool> DurableOperation(string node, int partitionId, int kind, string logType, byte[] payload, CancellationToken cancellationToken)
    {
        if (nodes is not null && nodes.TryGetValue(node, out IKahuna? kahunaNode))
            return await kahunaNode.DurableOperationLocal(partitionId, kind, logType, payload, cancellationToken);

        throw new KahunaServerException($"The node {node} does not exist.");
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="node"></param>
    /// <param name="transactionId"></param>
    /// <param name="xkeys"></param>
    /// <param name="lockSync"></param>
    /// <param name="responses"></param>
    /// <param name="cancellationToken"></param>
    /// <exception cref="KahunaServerException"></exception>
    public async Task TryCommitNodeMutations(
        string node, 
        HLCTimestamp transactionId, 
        List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)> xkeys, 
        Lock lockSync, 
        List<(KeyValueResponseType type, string key, long, KeyValueDurability durability)> responses,
        CancellationToken cancellationToken
    )
    {
        if (nodes is not null && nodes.TryGetValue(node, out IKahuna? kahunaNode))
        {
            ConcurrentBag<(KeyValueResponseType, string, long, KeyValueDurability)> bag = [];

            foreach ((string key, HLCTimestamp ticketId, KeyValueDurability durability) in xkeys)
            {
                // Same transient fault seam as the single-key inter-node commit: a faulted participant's commit
                // never reaches the remote leader, so its prepare survives for recovery to drive.
                if (CommitMutationsFault is not null && CommitMutationsFault(transactionId, key))
                {
                    bag.Add((KeyValueResponseType.MustRetry, key, 0, durability));
                    continue;
                }

                (KeyValueResponseType type, long commitIndex) = await kahunaNode.TryCommitMutations(transactionId, key, ticketId, durability);
                bag.Add((type, key, commitIndex, durability));
            }

            AddToCommitMutationsResponses(bag, lockSync, responses);
            return;
        }
        
        throw new KahunaServerException($"The node {node} does not exist.");
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="bag"></param>
    /// <param name="lockSync"></param>
    /// <param name="responses"></param>
    private static void AddToCommitMutationsResponses(
        ConcurrentBag<(KeyValueResponseType, string, long, KeyValueDurability)> bag, 
        Lock lockSync, 
        List<(KeyValueResponseType type, string key, long, KeyValueDurability durability)> responses
    )
    {
        foreach ((KeyValueResponseType type, string key, long commitIndex, KeyValueDurability durability) in bag)
        {
            lock (lockSync)
                responses.Add((type, key, commitIndex, durability));
        }
    }
    
    /// <summary>
    /// 
    /// </summary>
    /// <param name="node"></param>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="ticketId"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    /// <exception cref="KahunaServerException"></exception>
    public async Task<(KeyValueResponseType, long)> TryRollbackMutations(string node, HLCTimestamp transactionId, string key, HLCTimestamp ticketId, KeyValueDurability durability, CancellationToken cancellationToken)
    {
        if (nodes is not null && nodes.TryGetValue(node, out IKahuna? kahunaNode))
        {
            RollbackObserver?.Invoke(transactionId, key);
            return await kahunaNode.TryRollbackMutations(transactionId, key, ticketId, durability);
        }

        throw new KahunaServerException($"The node {node} does not exist.");
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="node"></param>
    /// <param name="transactionId"></param>
    /// <param name="xkeys"></param>
    /// <param name="lockSync"></param>
    /// <param name="responses"></param>
    /// <param name="cancellationToken"></param>
    /// <exception cref="KahunaServerException"></exception>
    public async Task TryRollbackNodeMutations(string node, HLCTimestamp transactionId, List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)> xkeys, Lock lockSync, List<(KeyValueResponseType type, string key, long, KeyValueDurability durability)> responses, CancellationToken cancellationToken)
    {
        if (nodes is not null && nodes.TryGetValue(node, out IKahuna? kahunaNode))
        {
            ConcurrentBag<(KeyValueResponseType, string, long, KeyValueDurability)> bag = [];

            foreach ((string key, HLCTimestamp ticketId, KeyValueDurability durability) in xkeys)
            {
                RollbackObserver?.Invoke(transactionId, key);
                (KeyValueResponseType type, long commitIndex) = await kahunaNode.TryRollbackMutations(transactionId, key, ticketId, durability);
                bag.Add((type, key, commitIndex, durability));
            }

            AddToRollbackMutationsResponses(bag, lockSync, responses);
            return;
        }
        
        throw new KahunaServerException($"The node {node} does not exist.");
    }
    
    /// <summary>
    /// 
    /// </summary>
    /// <param name="bag"></param>
    /// <param name="lockSync"></param>
    /// <param name="responses"></param>
    private static void AddToRollbackMutationsResponses(
        ConcurrentBag<(KeyValueResponseType, string, long, KeyValueDurability)> bag, 
        Lock lockSync, 
        List<(KeyValueResponseType type, string key, long, KeyValueDurability durability)> responses
    )
    {
        foreach ((KeyValueResponseType type, string key, long commitIndex, KeyValueDurability durability) in bag)
        {
            lock (lockSync)
                responses.Add((type, key, commitIndex, durability));
        }
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="node"></param>
    /// <param name="transactionId"></param>
    /// <param name="prefixedKey"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    /// <exception cref="KahunaServerException"></exception>
    public async Task<KeyValueGetByBucketResult> GetByBucket(string node, HLCTimestamp transactionId, string prefixedKey, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken)
    {
        if (nodes is not null && nodes.TryGetValue(node, out IKahuna? kahunaNode))
            return await kahunaNode.GetByBucket(transactionId, prefixedKey, readTimestamp, durability);

        throw new KahunaServerException($"The node {node} does not exist.");
    }

    public async Task<KeyValueGetByRangeResult> GetByRange(string node, HLCTimestamp transactionId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive, int limit, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref getByRangeCallCount);

        if (nodes is not null && nodes.TryGetValue(node, out IKahuna? kahunaNode))
            return await kahunaNode.LocateAndGetByRange(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, limit, readTimestamp, durability, cancellationToken);

        throw new KahunaServerException($"The node {node} does not exist.");
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="node"></param>
    /// <param name="prefixedKey"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    /// <exception cref="KahunaServerException"></exception>
    public async Task<KeyValueGetByBucketResult> ScanByPrefix(string node, string prefixedKey, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken)
    {
        if (nodes is not null && nodes.TryGetValue(node, out IKahuna? kahunaNode))
            return await kahunaNode.ScanByPrefix(prefixedKey, readTimestamp, durability);

        throw new KahunaServerException($"The node {node} does not exist.");
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="node"></param>
    /// <param name="options"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    /// <exception cref="KahunaServerException"></exception>
    public async Task<(KeyValueResponseType, TransactionHandle)> StartTransaction(string node, KeyValueTransactionOptions options, CancellationToken cancellationToken)
    {
        if (nodes is not null && nodes.TryGetValue(node, out IKahuna? kahunaNode))
            return await kahunaNode.StartTransaction(options);

        throw new KahunaServerException($"The node {node} does not exist.");
    }

    public async Task<(KeyValueResponseType, string?)> CommitTransaction(string node, TransactionHandle handle, CancellationToken cancellationToken)
    {
        if (nodes is not null && nodes.TryGetValue(node, out IKahuna? kahunaNode))
            return await kahunaNode.CommitTransaction(handle);

        throw new KahunaServerException($"The node {node} does not exist.");
    }

    public async Task<KeyValueResponseType> RollbackTransaction(string node, TransactionHandle handle, CancellationToken cancellationToken)
    {
        if (nodes is not null && nodes.TryGetValue(node, out IKahuna? kahunaNode))
            return await kahunaNode.RollbackTransaction(handle);

        throw new KahunaServerException($"The node {node} does not exist.");
    }

    public async Task<(OperationRegistrationOutcome outcome, KeyValueResponseType cachedType, long cachedRevision, HLCTimestamp cachedTimestamp, string? recordAnchorKey)> BeginOperation(string node, string coordinatorKey, HLCTimestamp transactionId, TransactionOperationId operationId, OperationKind kind, byte[]? payloadDigest, CancellationToken cancellationToken)
    {
        if (nodes is not null && nodes.TryGetValue(node, out IKahuna? kahunaNode))
        {
            Interlocked.Increment(ref beginOperationCallCount);
            return await Task.FromResult(kahunaNode.BeginOperation(transactionId, operationId, kind, payloadDigest));
        }

        throw new KahunaServerException($"The node {node} does not exist.");
    }

    public async Task<(KeyValueResponseType outcome, string? anchor)> CompleteOperation(string node, string coordinatorKey, HLCTimestamp transactionId, TransactionOperationId operationId, OperationCompletionPayload payload, CancellationToken cancellationToken)
    {
        if (nodes is not null && nodes.TryGetValue(node, out IKahuna? kahunaNode))
        {
            Interlocked.Increment(ref completeOperationCallCount);

            // Test seam: simulate a completion that never reaches the coordinator (the record stays
            // pending) so participant-side retry recovery can be exercised. The throw happens before the
            // coordinator is touched, matching a lost/failed inbound RPC.
            if (CompleteOperationFault is not null && CompleteOperationFault(transactionId, operationId))
                throw new KahunaServerException("Injected completion fault (test seam).");

            // Test seam: simulate a non-throwing "not delivered" response (e.g. a leader swap between
            // the routing decision and the RPC landing) — returns MustRetry without calling the
            // coordinator, exercising the aliasing path that previously returned a silent false-ack.
            if (CompleteOperationRedirectFault is not null && CompleteOperationRedirectFault(transactionId, operationId))
                return (KeyValueResponseType.MustRetry, null);

            return await kahunaNode.CompleteOperationInbound(coordinatorKey, transactionId, operationId, payload);
        }

        throw new KahunaServerException($"The node {node} does not exist.");
    }

    /// <summary>
    /// Test seam: when set and it returns true for a given completion, that <see cref="CompleteOperation"/>
    /// call throws before reaching the coordinator, leaving the operation record pending so a same-id
    /// retry must recover it from the participant cache instead of reapplying the operation.
    /// </summary>
    public Func<HLCTimestamp, TransactionOperationId, bool>? CompleteOperationFault { get; set; }

    /// <summary>
    /// Test seam: when set and it returns true for a given completion, that <see cref="CompleteOperation"/>
    /// call returns <c>(MustRetry, null)</c> without reaching the coordinator, simulating the non-throwing
    /// not-delivered outcome that arises when the target node loses its coordinator-partition leadership
    /// between the routing decision and the RPC landing.
    /// </summary>
    public Func<HLCTimestamp, TransactionOperationId, bool>? CompleteOperationRedirectFault { get; set; }

    /// <summary>
    /// Test seam: when set and it returns true for a given (transaction, key), that inter-node
    /// <see cref="TryCommitMutations"/> call returns <c>MustRetry</c> before reaching the remote leader, so the
    /// participant's prepare survives there. Used to force a partial durable commit — the anchor commits while a
    /// secondary stays pending — and prove recovery drives that surviving prepare to completion.
    /// </summary>
    public Func<HLCTimestamp, string, bool>? CommitMutationsFault { get; set; }

    /// <summary>
    /// When set and it returns true for a given (transactionId, key), the inter-node <see cref="TryCommitMutations"/>
    /// call lets the commit apply on the remote leader — installing the value, receipt, and (for an anchor) the
    /// durable decision record — but returns <c>MustRetry</c> to the caller, hiding the success. Models a
    /// committed-but-lost response so the coordinator must consult the durable record instead of assuming failure.
    /// </summary>
    public Func<HLCTimestamp, string, bool>? CommitMutationsResponseFault { get; set; }

    /// <summary>
    /// When set, invoked for every (transactionId, key) whose rollback is issued over the inter-node transport,
    /// just before it reaches the remote leader. Lets a test observe that a prepared participant's ticket was
    /// actually driven to rollback — as opposed to being orphaned by a rollback that threw before issuing any RPC.
    /// </summary>
    public Action<HLCTimestamp, string>? RollbackObserver { get; set; }

    public async Task<TransactionWorkingSet?> GetTransactionWorkingSet(string node, string coordinatorKey, HLCTimestamp transactionId, CancellationToken cancellationToken)
    {
        if (nodes is not null && nodes.TryGetValue(node, out IKahuna? kahunaNode))
            return await Task.FromResult(kahunaNode.GetTransactionWorkingSet(transactionId));

        throw new KahunaServerException($"The node {node} does not exist.");
    }

    public async Task<(KeyValueResponseType, TransactionWorkingSet?)> CloseTransaction(string node, string coordinatorKey, HLCTimestamp transactionId, CancellationToken cancellationToken)
    {
        if (nodes is not null && nodes.TryGetValue(node, out IKahuna? kahunaNode))
            return await kahunaNode.CloseTransaction(transactionId, cancellationToken);

        throw new KahunaServerException($"The node {node} does not exist.");
    }

    public async Task<bool> EnsureKeyRangeSeeded(string node, string keySpace, CancellationToken cancellationToken)
    {
        if (nodes is not null && nodes.TryGetValue(node, out IKahuna? kahunaNode))
            return await kahunaNode.RegisterKeyRangeAsync(keySpace, cancellationToken);

        throw new KahunaServerException($"The node {node} does not exist.");
    }

    public async Task<bool> EnsureKeyRangeRemoved(string node, string keySpace, CancellationToken cancellationToken)
    {
        if (nodes is not null && nodes.TryGetValue(node, out IKahuna? kahunaNode))
            return await kahunaNode.RemoveKeyRangeAsync(keySpace, cancellationToken);

        throw new KahunaServerException($"The node {node} does not exist.");
    }

    public async Task<List<KeyValueRangeLock>> GetRangeLocks(string node, string keySpace, CancellationToken cancellationToken)
    {
        if (nodes is not null && nodes.TryGetValue(node, out IKahuna? kahunaNode))
            return await kahunaNode.GetRangeLocks(keySpace);

        throw new KahunaServerException($"The node {node} does not exist.");
    }

    public async Task ImportRangeLocks(string node, string keySpace, List<KeyValueRangeLock> locks, CancellationToken cancellationToken)
    {
        if (nodes is not null && nodes.TryGetValue(node, out IKahuna? kahunaNode))
        {
            await kahunaNode.ImportRangeLocks(keySpace, locks);
            return;
        }

        throw new KahunaServerException($"The node {node} does not exist.");
    }

    public async Task<bool> ImportCompletionReceipts(string node, int partitionId, IReadOnlyCollection<CompletionReceiptRecord> receipts, CancellationToken cancellationToken, bool forget = false)
    {
        if (nodes is not null && nodes.TryGetValue(node, out IKahuna? kahunaNode))
            return forget
                ? await kahunaNode.ForgetCompletionReceiptsReplicated(partitionId, receipts)
                : await kahunaNode.ImportCompletionReceiptsReplicated(partitionId, receipts);

        throw new KahunaServerException($"The node {node} does not exist.");
    }

    public async Task<(KeyValueResponseType Type, string HoldId, HLCTimestamp LeaseExpiry)>
        AcquireSnapshotHold(string node, string holderId, HLCTimestamp timestamp, int leaseMs, CancellationToken cancellationToken)
    {
        if (nodes is not null && nodes.TryGetValue(node, out IKahuna? kahunaNode))
            return await kahunaNode.LocateAndAcquireSnapshotHold(holderId, timestamp, leaseMs, cancellationToken);

        throw new KahunaServerException($"The node {node} does not exist.");
    }

    public async Task<(KeyValueResponseType Type, HLCTimestamp LeaseExpiry)>
        RenewSnapshotHold(string node, string holdId, int leaseMs, CancellationToken cancellationToken)
    {
        if (nodes is not null && nodes.TryGetValue(node, out IKahuna? kahunaNode))
            return await kahunaNode.LocateAndRenewSnapshotHold(holdId, leaseMs, cancellationToken);

        throw new KahunaServerException($"The node {node} does not exist.");
    }

    public async Task<KeyValueResponseType>
        ReleaseSnapshotHold(string node, string holdId, CancellationToken cancellationToken)
    {
        if (nodes is not null && nodes.TryGetValue(node, out IKahuna? kahunaNode))
            return await kahunaNode.LocateAndReleaseSnapshotHold(holdId, cancellationToken);

        throw new KahunaServerException($"The node {node} does not exist.");
    }

    public async Task<(HLCTimestamp Floor, int LiveHolds)>
        GetSnapshotFloor(string node, CancellationToken cancellationToken)
    {
        if (nodes is not null && nodes.TryGetValue(node, out IKahuna? kahunaNode))
            return await kahunaNode.GetSnapshotFloor(cancellationToken);

        throw new KahunaServerException($"The node {node} does not exist.");
    }
}
