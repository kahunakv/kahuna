
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

    private async Task<KeyValueResponse> GetByBucketEphemeral(KeyValueRequest message)
    {
        List<(string, ReadOnlyKeyValueEntry)> items = [];
        HLCTimestamp currentTime = context.Raft.HybridLogicalClock.TrySendOrLocalEvent(context.Raft.GetLocalNodeId());

        foreach ((string key, KeyValueEntry? _) in context.Store.GetByBucket(message.Key))
        {
            KeyValueResponse response = await Get(currentTime, message.TransactionId, key, message.Durability, message.ReadTimestamp);

            if (response.Type == KeyValueResponseType.DoesNotExist)
                continue;

            if (response.Type != KeyValueResponseType.Get)
                return new(response.Type, []);

            if (response is { Type: KeyValueResponseType.Get, Entry: not null })
                items.Add((key, response.Entry));

            if (items.Count >= KeyValueScanLimits.MaxPrefixScanResults)
                break;
        }

        items.Sort(EnsureLexicographicalOrder);

        return new(KeyValueResponseType.Get, items);
    }

    private async Task<KeyValueResponse> GetByBucketPersistent(KeyValueRequest message)
    {
        List<(string, ReadOnlyKeyValueEntry)> items = [];
        HashSet<string> seenKeys = [];

        HLCTimestamp currentTime = context.Raft.HybridLogicalClock.TrySendOrLocalEvent(context.Raft.GetLocalNodeId());

        // step 1: check the in-memory store to get the MVCC entry or the latest value
        foreach ((string key, KeyValueEntry? _) in context.Store.GetByBucket(message.Key))
        {
            KeyValueResponse response = await Get(currentTime, message.TransactionId, key, message.Durability, message.ReadTimestamp);

            if (response.Type == KeyValueResponseType.DoesNotExist)
                continue;

            if (response.Type != KeyValueResponseType.Get || response.Entry is null)
                return new(response.Type, []);

            seenKeys.Add(key);
            items.Add((key, new(
                response.Entry.Value,
                response.Entry.Revision,
                response.Entry.Expires,
                response.Entry.LastUsed,
                response.Entry.LastModified,
                response.Entry.State
            )));

            if (items.Count >= KeyValueScanLimits.MaxPrefixScanResults)
                break;
        }

        // step 2: join the in-memory store with the disk store
        List<(string, ReadOnlyKeyValueEntry)> itemsFromDisk = await context.Raft.ReadScheduler.EnqueueTask(message.PartitionId, () => context.PersistenceBackend.GetKeyValueByPrefix(message.Key));

        foreach ((string key, ReadOnlyKeyValueEntry readOnlyKeyValueEntry) in itemsFromDisk)
        {
            if (items.Count >= KeyValueScanLimits.MaxPrefixScanResults)
                break;

            if (seenKeys.Contains(key))
                continue;

            KeyValueResponse response = await Get(currentTime, message.TransactionId, key, message.Durability, message.ReadTimestamp, readOnlyKeyValueEntry);

            if (response.Type == KeyValueResponseType.DoesNotExist)
                continue;

            if (response is { Type: KeyValueResponseType.Get, Entry: not null })
            {
                seenKeys.Add(key);
                items.Add((key, response.Entry));
            }
        }

        // step 3: make sure the items are sorted in lexicographical order
        items.Sort(EnsureLexicographicalOrder);

        return new(KeyValueResponseType.Get, items);
    }

    private async Task<KeyValueResponse> Get(
        HLCTimestamp currentTime,
        HLCTimestamp transactionId,
        string key,
        KeyValueDurability durability,
        HLCTimestamp readTimestamp = default,
        ReadOnlyKeyValueEntry? keyValueEntry = null
    )
    {
        KeyValueEntry? entry = await GetKeyValueEntry(key, durability, keyValueEntry);

        ReadOnlyKeyValueEntry readOnlyKeyValueEntry;

        // Validate if there's an active replication entry on the key/value entry.
        if (entry?.ReplicationIntent is not null)
        {
            if (entry.ReplicationIntent.Expires - currentTime > TimeSpan.Zero)
                return KeyValueStaticResponses.WaitingForReplicationResponse;

            entry.ReplicationIntent = null;
        }

        // Safe-time wait: when a snapshot readTimestamp is in play, a live foreign write intent
        // whose pending commit could land at-or-before T must be awaited (mirror TryGetHandler).
        // Without a snapshot (readTimestamp.IsNull()), fall through to committed state as before.
        if (entry?.WriteIntent != null)
        {
            if (entry.WriteIntent.TransactionId != transactionId)
            {
                if (entry.WriteIntent.Expires - currentTime <= TimeSpan.Zero)
                    entry.WriteIntent = null;
                else if (!readTimestamp.IsNull())
                {
                    HLCTimestamp commitTs = entry.WriteIntent.CommitTimestamp;
                    if (commitTs.IsNull() || commitTs.CompareTo(readTimestamp) <= 0)
                        return KeyValueStaticResponses.WaitingForReplicationResponse;
                    // commitTs > readTimestamp: write won't land in our snapshot; fall through
                }
                // live write intent without snapshot: fall through to committed state
            }
        }

        // TransactionId is provided so we keep a MVCC entry for it.
        // Exception: when readTimestamp is also set, this is an AS OF snapshot read — skip MVCC
        // and fall through to the snapshot visibility path below.
        if (transactionId != HLCTimestamp.Zero && readTimestamp.IsNull())
        {
            if (entry is null)
            {
                entry = new() { Bucket = GetBucket(key), State = KeyValueState.Undefined, Revision = -1 };
                context.InsertStoreEntry(key, entry);
            }

            entry.MvccEntries ??= new();

            if (!entry.MvccEntries.TryGetValue(transactionId, out KeyValueMvccEntry? mvccEntry))
            {
                bool mvccDictJustCreated = entry.MvccEntries.Count == 0;
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
                context.AdjustEstimatedEntryBytes(entry, KeyValueStoreAccounting.MvccEntryAddedBytes(mvccDictJustCreated, mvccEntry.Value));
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

        // Snapshot visibility (non-transactional or AS OF within transaction).
        if (!readTimestamp.IsNull() && entry is not null && entry.LastModified > readTimestamp)
        {
            if (!entry.TryGetRevisionAtOrBefore(readTimestamp, out long snapRevision, out KeyValueRevisionEntry snapshot))
                return KeyValueStaticResponses.DoesNotExistContextResponse;

            if (snapshot.State is KeyValueState.Deleted or KeyValueState.Undefined ||
                (snapshot.Expires != HLCTimestamp.Zero && snapshot.Expires - currentTime < TimeSpan.Zero))
                return KeyValueStaticResponses.DoesNotExistContextResponse;

            return new(KeyValueResponseType.Get, new ReadOnlyKeyValueEntry(
                snapshot.Value,
                snapRevision,
                snapshot.Expires,
                currentTime,
                snapshot.LastModified,
                snapshot.State));
        }

        if (entry is null || entry.State is KeyValueState.Deleted or KeyValueState.Undefined || entry.Expires != HLCTimestamp.Zero && entry.Expires - currentTime < TimeSpan.Zero)
            return KeyValueStaticResponses.DoesNotExistContextResponse;

        context.TouchEntry(entry, currentTime);

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
