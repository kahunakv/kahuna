
using Kahuna.Server.Locks;
using Kahuna.Server.KeyValues;

namespace Kahuna.Server.Persistence;

public interface IPersistence
{
    public Task<bool> StoreLocks(List<PersistenceRequestItem> items);

    public Task<bool> StoreKeyValues(List<PersistenceRequestItem> items);

    public Task<LockContext?> GetLock(string resource);
    
    public Task<KeyValueContext?> GetKeyValue(string keyName);
    
    public Task<KeyValueContext?> GetKeyValueRevision(string keyName, long revision);
}