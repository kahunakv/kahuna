
using Kahuna.Server.KeyValues;
using Kahuna.Server.Locks;

namespace Kahuna.Server.Persistence.Backend;

public class MemoryPersistenceBackend : IPersistenceBackend, IDisposable
{
    public bool StoreLocks(List<PersistenceRequestItem> items)
    {
        throw new NotImplementedException();
    }

    public bool StoreKeyValues(List<PersistenceRequestItem> items)
    {
        throw new NotImplementedException();
    }

    public LockContext? GetLock(string resource)
    {
        throw new NotImplementedException();
    }

    public KeyValueContext? GetKeyValue(string keyName)
    {
        throw new NotImplementedException();
    }

    public KeyValueContext? GetKeyValueRevision(string keyName, long revision)
    {
        throw new NotImplementedException();
    }

    public List<(string, ReadOnlyKeyValueContext)> GetKeyValueByPrefix(string prefixKeyName)
    {
        throw new NotImplementedException();
    }

    public void Dispose()
    {
        throw new NotImplementedException();
    }
}