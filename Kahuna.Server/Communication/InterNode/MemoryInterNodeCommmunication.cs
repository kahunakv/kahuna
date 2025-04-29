using System.Collections.Concurrent;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Locks;
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
    private Dictionary<string, IKahuna>? nodes;

    public void SetNodes(Dictionary<string, IKahuna> nodes)
    {
        this.nodes = nodes;
    }
    
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
    
    public async Task<(LockResponseType, ReadOnlyLockContext?)> GetLock(
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

    public Task TrySetManyNodeKeyValue(string node, List<KahunaSetKeyValueRequestItem> items, Lock lockSync, List<KahunaSetKeyValueResponseItem> responses, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public async Task<(KeyValueResponseType, long, HLCTimestamp)> TryDeleteKeyValue(
        string node, 
        HLCTimestamp transactionId, 
        string key, 
        KeyValueDurability durability, 
        CancellationToken cancelationToken
    )
    {
        if (nodes is not null && nodes.TryGetValue(node, out IKahuna? kahunaNode))
            return await kahunaNode.TryDeleteKeyValue(transactionId, key, durability);
        
        throw new KahunaServerException($"The node {node} does not exist.");
    }

    public async Task<(KeyValueResponseType, long, HLCTimestamp)> TryExtendKeyValue(
        string node, 
        HLCTimestamp transactionId, 
        string key, 
        int expiresMs, 
        KeyValueDurability durability, 
        CancellationToken cancelationToken
    )
    {
        if (nodes is not null && nodes.TryGetValue(node, out IKahuna? kahunaNode))
            return await kahunaNode.TryExtendKeyValue(transactionId, key, expiresMs, durability);
        
        throw new KahunaServerException($"The node {node} does not exist.");
    }

    public async Task<(KeyValueResponseType, ReadOnlyKeyValueContext?)> TryGetValue(
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

    public async Task<(KeyValueResponseType, ReadOnlyKeyValueContext?)> TryExistsValue(
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

    public async Task<(KeyValueResponseType, string, KeyValueDurability)> TryAcquireExclusiveLock(
        string node, 
        HLCTimestamp transactionId, 
        string key, 
        int expiresMs, 
        KeyValueDurability durability, 
        CancellationToken cancelationToken
    )
    {
        if (nodes is not null && nodes.TryGetValue(node, out IKahuna? kahunaNode))
            return await kahunaNode.TryAcquireExclusiveLock(transactionId, key, expiresMs, durability);
        
        throw new KahunaServerException($"The node {node} does not exist.");
    }

    public async Task TryAcquireNodeExclusiveLocks(
        string node, 
        HLCTimestamp transactionId, 
        List<(string key, int expiresMs, KeyValueDurability durability)> xkeys, 
        Lock lockSync, 
        List<(KeyValueResponseType type, string key, KeyValueDurability durability)> responses, 
        CancellationToken cancelationToken
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

    public async Task<(KeyValueResponseType, string)> TryReleaseExclusiveLock(
        string node, 
        HLCTimestamp transactionId, 
        string key, 
        KeyValueDurability durability,
        CancellationToken cancelationToken
    )
    {
        if (nodes is not null && nodes.TryGetValue(node, out IKahuna? kahunaNode))
            return await kahunaNode.TryReleaseExclusiveLock(transactionId, key, durability);
        
        throw new KahunaServerException($"The node {node} does not exist.");
    }

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
    
    private static void AddToReleaseLockResponses(ConcurrentBag<(KeyValueResponseType type, string key, KeyValueDurability durability)> bag, Lock lockSync, List<(KeyValueResponseType type, string key, KeyValueDurability durability)> responses)
    {
        foreach ((KeyValueResponseType type, string key, KeyValueDurability durability) in bag)
        {
            lock (lockSync)
                responses.Add((type, key, durability));
        }
    }

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

    public async Task<(KeyValueResponseType, long)> TryCommitMutations(
        string node, 
        HLCTimestamp transactionId, 
        string key, 
        HLCTimestamp ticketId, 
        KeyValueDurability durability, 
        CancellationToken cancelationToken
    )
    {
        if (nodes is not null && nodes.TryGetValue(node, out IKahuna? kahunaNode))
            return await kahunaNode.TryCommitMutations(transactionId, key, ticketId, durability);
        
        throw new KahunaServerException($"The node {node} does not exist.");
    }

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
    
    public async Task<(KeyValueResponseType, long)> TryRollbackMutations(string node, HLCTimestamp transactionId, string key, HLCTimestamp ticketId, KeyValueDurability durability, CancellationToken cancelationToken)
    {
        if (nodes is not null && nodes.TryGetValue(node, out IKahuna? kahunaNode))
            return await kahunaNode.TryRollbackMutations(transactionId, key, ticketId, durability);
        
        throw new KahunaServerException($"The node {node} does not exist.");
    }

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

    public async Task<KeyValueGetByPrefixResult> GetByPrefix(string node, HLCTimestamp transactionId, string prefixedKey, KeyValueDurability durability, CancellationToken cancelationToken)
    {
        if (nodes is not null && nodes.TryGetValue(node, out IKahuna? kahunaNode))        
            return await kahunaNode.GetByPrefix(transactionId, prefixedKey, durability);        
        
        throw new KahunaServerException($"The node {node} does not exist.");
    }

    public async Task<(KeyValueResponseType, HLCTimestamp)> StartTransaction(string node, KeyValueTransactionOptions options, CancellationToken cancellationToken)
    {
        if (nodes is not null && nodes.TryGetValue(node, out IKahuna? kahunaNode))        
            return await kahunaNode.StartTransaction(options);        
        
        throw new KahunaServerException($"The node {node} does not exist.");
    }

    public async Task<KeyValueResponseType> CommitTransaction(string node, string uniqueId, HLCTimestamp timestamp, List<KeyValueTransactionModifiedKey> acquiredLocks, List<KeyValueTransactionModifiedKey> modifiedKeys, CancellationToken cancellationToken)
    {
        if (nodes is not null && nodes.TryGetValue(node, out IKahuna? kahunaNode))        
            return await kahunaNode.CommitTransaction(timestamp, acquiredLocks, modifiedKeys);        
        
        throw new KahunaServerException($"The node {node} does not exist.");
    }

    public async Task<KeyValueResponseType> RollbackTransaction(string node, string uniqueId, HLCTimestamp timestamp, List<KeyValueTransactionModifiedKey> acquiredLocks, List<KeyValueTransactionModifiedKey> modifiedKeys, CancellationToken cancellationToken)
    {
        if (nodes is not null && nodes.TryGetValue(node, out IKahuna? kahunaNode))        
            return await kahunaNode.RollbackTransaction(timestamp, acquiredLocks, modifiedKeys);
        
        throw new KahunaServerException($"The node {node} does not exist.");
    }
}