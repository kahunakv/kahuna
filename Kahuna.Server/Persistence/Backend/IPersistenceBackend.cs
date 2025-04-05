
using Kahuna.Server.Locks;
using Kahuna.Server.KeyValues;

namespace Kahuna.Server.Persistence.Backend;

public interface IPersistenceBackend
{
    public bool StoreLocks(List<PersistenceRequestItem> items);

    public bool StoreKeyValues(List<PersistenceRequestItem> items);

    public LockContext? GetLock(string resource);
    
    public KeyValueContext? GetKeyValue(string keyName);
    
    public KeyValueContext? GetKeyValueRevision(string keyName, long revision);

    public List<(string, ReadOnlyKeyValueContext)> GetKeyValueByPrefix(string prefixKeyName);
}