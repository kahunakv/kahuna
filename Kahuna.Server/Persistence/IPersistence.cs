using Kahuna.Locks;

namespace Kahuna.Persistence;

public interface IPersistence
{
    public Task UpdateLock(string resource, string owner, long expiresLogical, long expiresCounter, long fencingToken, long consistency, LockState state);

    public Task UpdateLocks(List<PersistenceItem> items);

    public Task<LockContext?> GetLock(string resource);
}