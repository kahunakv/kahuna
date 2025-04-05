using Kahuna.Shared.Locks;

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
}