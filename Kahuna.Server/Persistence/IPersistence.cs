
using Kahuna.Server.Locks;
using Kahuna.Server.KeyValues;

namespace Kahuna.Server.Persistence;

public interface IPersistence
{
    public Task<bool> StoreLock(string resource, byte[]? owner, long expiresPhysical, uint expiresCounter, long fencingToken, int state);

    public Task<bool> StoreKeyValue(string key, byte[]? value, long expiresPhysical, uint expiresCounter, long revision, int state);

    public Task<LockContext?> GetLock(string resource);
    
    public Task<KeyValueContext?> GetKeyValue(string keyName);
    
    public Task<KeyValueContext?> GetKeyValueRevision(string keyName, long revision);
}