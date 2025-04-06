using Kahuna.Server.Locks;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Locks;
using Kommander.Time;

namespace Kahuna.Server.Communication.Internode;

public class MemoryInterNodeCommmunication : IInterNodeCommunication
{
    private Dictionary<string, IKahuna>? nodes;

    public void SetNodes(Dictionary<string, IKahuna> nodes)
    {
        this.nodes = nodes;
    }
    
    public async Task<(LockResponseType, long)> TryLock(string node, string resource, byte[] owner, int expiresMs, LockDurability durability, CancellationToken cancellationToken)
    {
        if (nodes is not null && nodes.TryGetValue(node, out IKahuna? kahunaNode))
            return await kahunaNode.TryLock(resource, owner, expiresMs, durability);
        
        throw new KahunaServerException($"The node {node} does not exist.");
    }

    public async Task<(LockResponseType, long)> TryExtendLock(string node, string resource, byte[] owner, int expiresMs, LockDurability durability, CancellationToken cancellationToken)
    {
        if (nodes is not null && nodes.TryGetValue(node, out IKahuna? kahunaNode))
            return await kahunaNode.TryExtendLock(resource, owner, expiresMs, durability);
        
        throw new KahunaServerException($"The node {node} does not exist.");
    }
    
    public async Task<LockResponseType> TryUnlock(string node, string resource, byte[] owner, LockDurability durability, CancellationToken cancellationToken)
    {
        if (nodes is not null && nodes.TryGetValue(node, out IKahuna? kahunaNode))
            return await kahunaNode.TryUnlock(resource, owner, durability);
        
        throw new KahunaServerException($"The node {node} does not exist.");
    }
    
    public async Task<(LockResponseType, ReadOnlyLockContext?)> GetLock(string node, string resource, LockDurability durability, CancellationToken cancellationToken)
    {
        if (nodes is not null && nodes.TryGetValue(node, out IKahuna? kahunaNode))
            return await kahunaNode.GetLock(resource, durability);
        
        throw new KahunaServerException($"The node {node} does not exist.");
    }

    public async Task<(KeyValueResponseType, long)> TrySetKeyValue(string node, HLCTimestamp transactionId, string key, byte[]? value, byte[]? compareValue, long compareRevision, KeyValueFlags flags, int expiresMs, KeyValueDurability durability, CancellationToken cancellationToken)
    {
        if (nodes is not null && nodes.TryGetValue(node, out IKahuna? kahunaNode))
            return await kahunaNode.TrySetKeyValue(transactionId, key, value, compareValue, compareRevision, flags, expiresMs, durability);
        
        throw new KahunaServerException($"The node {node} does not exist.");
    }

    public async Task<(KeyValueResponseType, long)> TryDeleteKeyValue(string node, HLCTimestamp transactionId, string key, KeyValueDurability durability, CancellationToken cancelationToken)
    {
        if (nodes is not null && nodes.TryGetValue(node, out IKahuna? kahunaNode))
            return await kahunaNode.TryDeleteKeyValue(transactionId, key, durability);
        
        throw new KahunaServerException($"The node {node} does not exist.");
    }
}