
using Kahuna.Server.Locks;
using Kahuna.Server.KeyValues;
using Kahuna.Server.Locks.Data;

namespace Kahuna.Server.Persistence.Backend;

/// <summary>
/// Defines an interface for persistence backend implementations, providing methods
/// for storing and retrieving locks and key-value pairs from a persistent storage system.
/// </summary>
internal interface IPersistenceBackend
{
    public bool StoreLocks(List<PersistenceRequestItem> items);

    public bool StoreKeyValues(List<PersistenceRequestItem> items);

    public LockContext? GetLock(string resource);
    
    public KeyValueEntry? GetKeyValue(string keyName);
    
    public KeyValueEntry? GetKeyValueRevision(string keyName, long revision);

    public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByPrefix(string prefixKeyName);
}