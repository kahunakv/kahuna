
using Kahuna.Locks;
using Kahuna.KeyValues;

namespace Kahuna.Persistence;

public interface IPersistence
{
    public Task StoreLock(string resource, string owner, long expiresPhysical, uint expiresCounter, long fencingToken, long consistency, LockState state);

    public Task<LockContext?> GetLock(string resource);
    
    public Task<KeyValueContext?> GetKeyValue(string keyName);
}