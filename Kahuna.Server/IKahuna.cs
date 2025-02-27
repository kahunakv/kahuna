
using Kahuna.Locks;
using Kommander.Data;

namespace Kahuna;

public interface IKahuna
{
    public Task<(LockResponseType, long)> TryLock(string lockName, string lockId, int expiresMs, LockConsistency consistency);

    public Task<LockResponseType> TryExtendLock(string lockName, string lockId, int expiresMs, LockConsistency consistency);

    public Task<LockResponseType> TryUnlock(string lockName, string lockId, LockConsistency consistency);
    
    public Task<(LockResponseType, ReadOnlyLockContext?)> GetLock(string lockName, LockConsistency consistency);

    public Task<bool> OnReplicationReceived(string type, byte[] message);

    public void OnReplicationError(RaftLog log);
}