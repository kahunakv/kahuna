
using Kahuna.Shared.Locks;

namespace Kahuna.Server.Communication.Internode;

public interface IInterNodeCommunication
{
    public Task<(LockResponseType, long)> TryLock(string node, string resource, byte[] owner, int expiresMs, LockDurability durability, CancellationToken cancellationToken);
}