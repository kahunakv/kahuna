
using Kahuna.Server.Configuration;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Shared.KeyValue;
using Kahuna.Utils;
using Kommander;
using Kommander.Time;
using Nixie;

namespace Kahuna.Server.KeyValues.Handlers;

internal sealed class TryGetByBucketHandler : BaseHandler
{
    public TryGetByBucketHandler(KeyValueContext context) : base(context)
    {
        
    }

    /// <summary>
    /// Executes the get by bucket request
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    public async Task<KeyValueResponse> Execute(KeyValueRequest message)
    {
        if (message.Durability == KeyValueDurability.Ephemeral)
            return await GetByBucketEphemeral(message);
        
        return await GetByBucketPersistent(message);
    }

    /// <summary>
    /// Queries the key-value store for entries matching the specified bucket in an ephemeral context.
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    private async Task<KeyValueResponse> GetByBucketEphemeral(KeyValueRequest message)
    {
        List<(string, ReadOnlyKeyValueEntry)> items = [];
        HLCTimestamp currentTime = context.Raft.HybridLogicalClock.TrySendOrLocalEvent(context.Raft.GetLocalNodeId());
        
        foreach ((string key, KeyValueEntry? _) in context.Store.GetByBucket(message.Key))
        {
            KeyValueResponse response = await Get(currentTime, message.TransactionId, key, message.Durability);     
            
            if (response.Type == KeyValueResponseType.DoesNotExist)
                continue;

            if (response.Type != KeyValueResponseType.Get)
                return new(response.Type, []);

            if (response is { Type: KeyValueResponseType.Get, Entry: not null })
                items.Add((key, response.Entry));
        }

        items.Sort(EnsureLexicographicalOrder);
                
        return new(KeyValueResponseType.Get, items);
    }

    /// <summary>
    /// Queries the key-value store for entries matching the specified prefix in a persistent context.
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    private async Task<KeyValueResponse> GetByBucketPersistent(KeyValueRequest message)
    {
        Dictionary<string, ReadOnlyKeyValueEntry> items = new();
        
        HLCTimestamp currentTime = context.Raft.HybridLogicalClock.TrySendOrLocalEvent(context.Raft.GetLocalNodeId());

        // step 1: we need to check the in-memory store to get the MVCC entry or the latest value
        foreach ((string key, KeyValueEntry? _) in context.Store.GetByBucket(message.Key))
        {
            KeyValueResponse response = await Get(currentTime, message.TransactionId, key, message.Durability);

            if (response.Type == KeyValueResponseType.DoesNotExist)
                continue;
            
            if (response.Type != KeyValueResponseType.Get || response.Entry is null)
                return new(response.Type, []);

            items.Add(key, new(
                response.Entry.Value, 
                response.Entry.Revision, 
                response.Entry.Expires, 
                response.Entry.LastUsed,
                response.Entry.LastModified,
                response.Entry.State
            ));
        }

        // step 2: we join the in-memory store with the disk store
        // @todo we probably want to cache this in an mvcc entry
        List<(string, ReadOnlyKeyValueEntry)> itemsFromDisk = await context.Raft.ReadThreadPool.EnqueueTask(() => context.PersistenceBackend.GetKeyValueByPrefix(message.Key));
        
        foreach ((string key, ReadOnlyKeyValueEntry readOnlyKeyValueEntry) in itemsFromDisk)
        {
            if (items.ContainsKey(key))
                continue;
            
            KeyValueResponse response = await Get(currentTime, message.TransactionId, key, message.Durability, readOnlyKeyValueEntry);
            
            if (response.Type == KeyValueResponseType.DoesNotExist)
                continue;

            if (response is { Type: KeyValueResponseType.Get, Entry: not null })
                items.Add(key, response.Entry);
        }

        // step 3: make sure the items are sorted in lexicographical order
        List<(string Key, ReadOnlyKeyValueEntry Value)> itemsToReturn = items.Select(kv => (kv.Key, kv.Value)).ToList();
        
        itemsToReturn.Sort(EnsureLexicographicalOrder);
                
        return new(KeyValueResponseType.Get, itemsToReturn);
    }

    private async Task<KeyValueResponse> Get(
        HLCTimestamp currentTime, 
        HLCTimestamp transactionId, 
        string key, 
        KeyValueDurability durability, 
        ReadOnlyKeyValueEntry? keyValueEntry = null
    )
    {
        KeyValueEntry? entry = await GetKeyValueEntry(key, durability, keyValueEntry);

        ReadOnlyKeyValueEntry readOnlyKeyValueEntry;
        
        // Validate if there's an active replication enty on the key/value entry
        // clients must retry operations to make sure the entry is fully replicated
        // before modifying the entry
        if (entry?.ReplicationIntent is not null)
        {
            if (entry.ReplicationIntent.Expires - currentTime > TimeSpan.Zero)                
                return KeyValueStaticResponses.WaitingForReplicationResponse;
                
            entry.ReplicationIntent = null;
        }
        
        // Validate if there's an exclusive key acquired on the lock and whether it is expired
        // if we find expired write intents we can remove it to allow new transactions to proceed
        if (entry?.WriteIntent != null)
        {
            if (entry.WriteIntent.TransactionId != transactionId)
            {
                if (entry.WriteIntent.Expires - currentTime > TimeSpan.Zero)                
                    return KeyValueStaticResponses.MustRetryResponse;
                
                entry.WriteIntent = null;
            }
        }

        // TransactionId is provided so we keep a MVCC entry for it
        if (transactionId != HLCTimestamp.Zero)
        {
            if (entry is null)
            {
                entry = new() { Bucket = GetBucket(key), State = KeyValueState.Undefined, Revision = -1 };
                context.Store.Insert(key, entry);
            }
            
            entry.MvccEntries ??= new();

            if (!entry.MvccEntries.TryGetValue(transactionId, out KeyValueMvccEntry? mvccEntry))
            {
                mvccEntry = new()
                {
                    Value = entry.Value, 
                    Revision = entry.Revision, 
                    Expires = entry.Expires, 
                    LastUsed = entry.LastUsed,
                    LastModified = entry.LastModified,
                    State = entry.State
                };

                entry.MvccEntries.Add(transactionId, mvccEntry);
            }
            
            if (entry.Revision > mvccEntry.Revision) // early conflict detection
                return KeyValueStaticResponses.AbortedResponse;
            
            if (mvccEntry.State is KeyValueState.Undefined or KeyValueState.Deleted || mvccEntry.Expires != HLCTimestamp.Zero && mvccEntry.Expires - currentTime < TimeSpan.Zero)
                return KeyValueStaticResponses.DoesNotExistContextResponse;

            readOnlyKeyValueEntry = new(
                mvccEntry.Value, 
                mvccEntry.Revision, 
                mvccEntry.Expires, 
                mvccEntry.LastUsed, 
                mvccEntry.LastModified, 
                entry.State
            );

            return new(KeyValueResponseType.Get, readOnlyKeyValueEntry);
        }

        if (entry is null || entry.State == KeyValueState.Deleted || entry.Expires != HLCTimestamp.Zero && entry.Expires - currentTime < TimeSpan.Zero)
            return KeyValueStaticResponses.DoesNotExistContextResponse;

        entry.LastUsed = currentTime;

        readOnlyKeyValueEntry = new(
            entry.Value, 
            entry.Revision, 
            entry.Expires, 
            entry.LastUsed, 
            entry.LastModified, 
            entry.State
        );

        return new(KeyValueResponseType.Get, readOnlyKeyValueEntry);
    } 
    
    private static int EnsureLexicographicalOrder((string, ReadOnlyKeyValueEntry) x, (string, ReadOnlyKeyValueEntry) y)
    {
        return string.Compare(x.Item1, y.Item1, StringComparison.Ordinal);
    }
}