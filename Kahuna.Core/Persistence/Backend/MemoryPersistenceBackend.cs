
using Kahuna.Server.Locks;
using Kahuna.Server.KeyValues;
using System.Collections.Concurrent;
using Kahuna.Server.Locks.Data;
using Kommander.Time;

namespace Kahuna.Server.Persistence.Backend;

/// <summary>
/// Provides an in-memory implementation of the <see cref="IPersistenceBackend"/> interface
/// to store locks and key-value pairs without the use of persistent storage.
/// </summary>
internal sealed class MemoryPersistenceBackend : IPersistenceBackend, IDisposable
{
    private readonly ConcurrentDictionary<string, LockEntry> locks = new();
    
    private readonly ConcurrentDictionary<string, KeyValueEntry> keyValues = new();

    /// <summary>
    /// Stores locks in the persistence backend. Updates existing locks or adds new ones
    /// based on the provided list of persistence request items.
    /// </summary>
    /// <param name="items">A list of <see cref="PersistenceRequestItem"/> containing the lock data to be stored or updated.</param>
    /// <returns>Returns <c>true</c> if the locks were successfully stored or updated.</returns>
    public bool StoreLocks(List<PersistenceRequestItem> items)
    {
        foreach (PersistenceRequestItem item in items)
        {
            if (locks.TryGetValue(item.Key, out LockEntry? lockContext))
            {
                lockContext.Owner = item.Value;
                lockContext.Expires = new(item.ExpiresNode, item.ExpiresPhysical, item.ExpiresCounter);
                lockContext.FencingToken = item.Revision;
                lockContext.LastUsed = new(item.LastUsedNode, item.LastUsedPhysical, item.LastUsedCounter);
                lockContext.LastModified = new(item.LastModifiedNode, item.LastModifiedPhysical, item.LastModifiedCounter);
                lockContext.State = (LockState)item.State;
            }
            else
            {
                locks.TryAdd(item.Key, new()
                {
                    Owner = item.Value,
                    FencingToken = item.Revision,
                    Expires = new(item.ExpiresNode, item.ExpiresPhysical, item.ExpiresCounter),
                    LastUsed = new(item.LastUsedNode, item.LastUsedPhysical, item.LastUsedCounter),
                    LastModified = new(item.LastModifiedNode, item.LastModifiedPhysical, item.LastModifiedCounter),
                    State = (LockState)item.State
                });
            }
        }

        return true;
    }

    /// <summary>
    /// Stores key-value pairs in the memory persistence backend. Updates existing entries or adds new ones
    /// based on the provided list of persistence request items.
    /// </summary>
    /// <param name="items">A list of <see cref="PersistenceRequestItem"/> containing the key-value data to be stored or updated.</param>
    /// <returns>Returns <c>true</c> if the key-value pairs were successfully stored or updated.</returns>
    public bool StoreKeyValues(List<PersistenceRequestItem> items)
    {
        foreach (PersistenceRequestItem item in items)
        {
            if (keyValues.TryGetValue(item.Key, out KeyValueEntry? keyValueContext))
            {
                keyValueContext.Value = item.Value;
                keyValueContext.Expires = new(item.ExpiresNode, item.ExpiresPhysical, item.ExpiresCounter);
                keyValueContext.Revision = item.Revision;
                keyValueContext.LastUsed = new(item.LastUsedNode, item.LastUsedPhysical, item.LastUsedCounter);
                keyValueContext.LastModified = new(item.LastModifiedNode, item.LastModifiedPhysical, item.LastModifiedCounter);
                keyValueContext.State = (KeyValueState)item.State;
            }
            else
            {
                keyValues.TryAdd(item.Key, new()
                {
                    Value = item.Value,
                    Revision = item.Revision,
                    Expires = new(item.ExpiresNode, item.ExpiresPhysical, item.ExpiresCounter),
                    LastUsed = new(item.LastUsedNode, item.LastUsedPhysical, item.LastUsedCounter),
                    LastModified = new(item.LastModifiedNode, item.LastModifiedPhysical, item.LastModifiedCounter),
                    State = (KeyValueState)item.State
                });
            }
        }

        return true;
    }

    public LockEntry? GetLock(string resource)
    {
        return locks.GetValueOrDefault(resource);
    }

    public KeyValueEntry? GetKeyValue(string keyName)
    {
        return keyValues.GetValueOrDefault(keyName);
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="keyName"></param>
    /// <param name="revision"></param>
    /// <returns></returns>
    /// <exception cref="NotImplementedException"></exception>
    public KeyValueEntry? GetKeyValueRevision(string keyName, long revision)
    {
        throw new NotImplementedException();
    }

    /// <summary>
    /// Retrieves a list of key-value pairs where the key starts with the specified prefix.
    /// The values are returned in a read-only context.
    /// </summary>
    /// <param name="prefixKeyName">The prefix used to filter keys in the key-value store.</param>
    /// <returns>Returns a list of tuples where each tuple contains a key and its associated <see cref="ReadOnlyKeyValueEntry"/>.</returns>
    public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByPrefix(string prefixKeyName)
    {
        List<(string, ReadOnlyKeyValueEntry)> items = [];
        
        foreach ((string key, KeyValueEntry value) in keyValues)
        {
            if (key.StartsWith(prefixKeyName))
            {
                items.Add((key, new(
                    value.Value,
                    value.Revision,
                    value.Expires,
                    value.LastUsed,
                    value.LastModified,
                    value.State
                )));
            }
        }

        return items;
    }

    public void Dispose()
    {
        GC.SuppressFinalize(this);
        
        //throw new NotImplementedException();
    }
}