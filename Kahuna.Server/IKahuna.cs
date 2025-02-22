
namespace Kahuna;

public interface IKahuna
{
    public Task<LockResponseType> TryLock(string lockName, string lockId, int expiresMs);

    public Task<LockResponseType> TryExtendLock(string lockName, string lockId, int expiresMs);

    public Task<LockResponseType> TryUnlock(string lockName, string lockId);
}