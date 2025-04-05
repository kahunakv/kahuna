
using Kahuna.Server.Locks;
using Kahuna.Server.KeyValues;
using System.Collections.Concurrent;
using Kommander.Time;

namespace Kahuna.Server.Persistence.Backend;

public class MemoryPersistenceBackend : IPersistenceBackend, IDisposable
{
    private readonly ConcurrentDictionary<string, LockContext> locks = new();
    
    private readonly ConcurrentDictionary<string, KeyValueContext> keyValues = new();
    
    public bool StoreLocks(List<PersistenceRequestItem> items)
    {
        foreach (PersistenceRequestItem item in items)
        {
            if (locks.TryGetValue(item.Key, out LockContext? lockContext))
            {
                lockContext.Owner = item.Value;
                lockContext.Expires = new HLCTimestamp(item.ExpiresPhysical, item.ExpiresCounter);
                lockContext.FencingToken = item.Revision;
                lockContext.State = (LockState)item.State;
            }
            else
            {
                locks.TryAdd(item.Key, new()
                {
                    Owner = item.Value,
                    FencingToken = item.Revision,
                    Expires = new(item.ExpiresPhysical, item.ExpiresCounter),
                    State = (LockState)item.State
                });
            }
        }

        return true;
    }

    public bool StoreKeyValues(List<PersistenceRequestItem> items)
    {
        foreach (PersistenceRequestItem item in items)
        {
            if (keyValues.TryGetValue(item.Key, out KeyValueContext? keyValueContext))
            {
                keyValueContext.Value = item.Value;
                keyValueContext.Expires = new(item.ExpiresPhysical, item.ExpiresCounter);
                keyValueContext.Revision = item.Revision;
                keyValueContext.State = (KeyValueState)item.State;
            }
            else
            {
                keyValues.TryAdd(item.Key, new()
                {
                    Value = item.Value,
                    Revision = item.Revision,
                    Expires = new(item.ExpiresPhysical, item.ExpiresCounter),
                    State = (KeyValueState)item.State
                });
            }
        }

        return true;
    }

    public LockContext? GetLock(string resource)
    {
        return locks.GetValueOrDefault(resource);
    }

    public KeyValueContext? GetKeyValue(string keyName)
    {
        return keyValues.GetValueOrDefault(keyName);
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
        //throw new NotImplementedException();
    }
}