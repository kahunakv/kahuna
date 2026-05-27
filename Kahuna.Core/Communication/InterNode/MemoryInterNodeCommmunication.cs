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
        CancellationToken cancellationToken
    )
    {
        if (nodes is not null && nodes.TryGetValue(node, out IKahuna? kahunaNode))
            return await kahunaNode.TrySetKeyValue(transactionId, key, value, compareValue, compareRevision, flags, expiresMs, durability);
        
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
        KeyValueDurability durability, 
        CancellationToken cancellationToken
    )
    {
        if (nodes is not null && nodes.TryGetValue(node, out IKahuna? kahunaNode))
            return await kahunaNode.TryGetValue(transactionId, key, revision, durability);
        
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
        KeyValueDurability durability, 
        CancellationToken cancellationToken
    )
    {
        if (nodes is not null && nodes.TryGetValue(node, out IKahuna? kahunaNode))
            return await kahunaNode.TryExistsValue(transactionId, key, revision, durability);
        
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
    public async Task<(KeyValueResponseType, string, KeyValueDurability)> TryAcquireExclusiveLock(
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
        List<(KeyValueResponseType type, string key, KeyValueDurability durability)> responses, 
        CancellationToken cancellationToken
    )
    {
        if (nodes is not null && nodes.TryGetValue(node, out IKahuna? kahunaNode))
        {
            ConcurrentBag<(KeyValueResponseType type, string key, KeyValueDurability durability)> bag = [];
            
            foreach ((string key, int expiresMs, KeyValueDurability durability) in xkeys)
                bag.Add(await kahunaNode.TryAcquireExclusiveLock(transactionId, key, expiresMs, durability));

            AddToAcquireLockResponses(bag, lockSync, responses);
            return;
        }
        
        throw new KahunaServerException($"The node {node} does not exist.");
    }

    private static void AddToAcquireLockResponses(
        ConcurrentBag<(KeyValueResponseType type, string key, KeyValueDurability durability)> bag, 
        Lock lockSync, 
        List<(KeyValueResponseType type, string key, KeyValueDurability durability)> responses
    )
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
        CancellationToken cancellationToken
    )
    {
        if (nodes is not null && nodes.TryGetValue(node, out IKahuna? kahunaNode))
            return await kahunaNode.TryPrepareMutations(transactionId, commitId, key, durability);
        
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
        CancellationToken cancellationToken
    )
    {
        if (nodes is not null && nodes.TryGetValue(node, out IKahuna? kahunaNode))
        {
            ConcurrentBag<(KeyValueResponseType type, HLCTimestamp, string key, KeyValueDurability durability)> bag = [];

            foreach ((string key, KeyValueDurability durability) in xkeys)
            {
                (KeyValueResponseType type, HLCTimestamp proposalId, string _, KeyValueDurability _) = await kahunaNode.TryPrepareMutations(transactionId, commitId, key, durability);
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
            return await kahunaNode.TryCommitMutations(transactionId, key, ticketId, durability);
        
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
            return await kahunaNode.TryRollbackMutations(transactionId, key, ticketId, durability);
        
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
    public async Task<KeyValueGetByBucketResult> GetByBucket(string node, HLCTimestamp transactionId, string prefixedKey, KeyValueDurability durability, CancellationToken cancellationToken)
    {
        if (nodes is not null && nodes.TryGetValue(node, out IKahuna? kahunaNode))        
            return await kahunaNode.GetByBucket(transactionId, prefixedKey, durability);        
        
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
    public async Task<KeyValueGetByBucketResult> ScanByPrefix(string node, string prefixedKey, KeyValueDurability durability, CancellationToken cancellationToken)
    {
        if (nodes is not null && nodes.TryGetValue(node, out IKahuna? kahunaNode))        
            return await kahunaNode.ScanByPrefix(prefixedKey, durability);        
        
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
    public async Task<(KeyValueResponseType, HLCTimestamp)> StartTransaction(string node, KeyValueTransactionOptions options, CancellationToken cancellationToken)
    {
        if (nodes is not null && nodes.TryGetValue(node, out IKahuna? kahunaNode))        
            return await kahunaNode.StartTransaction(options);        
        
        throw new KahunaServerException($"The node {node} does not exist.");
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="node"></param>
    /// <param name="uniqueId"></param>
    /// <param name="timestamp"></param>
    /// <param name="acquiredLocks"></param>
    /// <param name="modifiedKeys"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    /// <exception cref="KahunaServerException"></exception>
    public async Task<KeyValueResponseType> CommitTransaction(string node, string uniqueId, HLCTimestamp timestamp, List<KeyValueTransactionModifiedKey> acquiredLocks, List<KeyValueTransactionModifiedKey> modifiedKeys, CancellationToken cancellationToken)
    {
        if (nodes is not null && nodes.TryGetValue(node, out IKahuna? kahunaNode))        
            return await kahunaNode.CommitTransaction(timestamp, acquiredLocks, modifiedKeys);        
        
        throw new KahunaServerException($"The node {node} does not exist.");
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="node"></param>
    /// <param name="uniqueId"></param>
    /// <param name="timestamp"></param>
    /// <param name="acquiredLocks"></param>
    /// <param name="modifiedKeys"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    /// <exception cref="KahunaServerException"></exception>
    public async Task<KeyValueResponseType> RollbackTransaction(string node, string uniqueId, HLCTimestamp timestamp, List<KeyValueTransactionModifiedKey> acquiredLocks, List<KeyValueTransactionModifiedKey> modifiedKeys, CancellationToken cancellationToken)
    {
        if (nodes is not null && nodes.TryGetValue(node, out IKahuna? kahunaNode))        
            return await kahunaNode.RollbackTransaction(timestamp, acquiredLocks, modifiedKeys);
        
        throw new KahunaServerException($"The node {node} does not exist.");
    }
}