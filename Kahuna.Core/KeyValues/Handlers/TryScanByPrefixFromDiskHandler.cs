
using Kahuna.Server.Configuration;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Shared.KeyValue;
using Kahuna.Utils;
using Kommander;
using Kommander.Time;
using Nixie;

namespace Kahuna.Server.KeyValues.Handlers;

/// <summary>
/// Represents a handler that attempts to scan key-value entries from disk based on a specified prefix.
/// </summary>
/// <remarks>
/// This handler asks the backend persistence to scan the key-value store for entries that match the given prefix.
/// </remarks>
internal sealed class TryScanByPrefixFromDiskHandler : BaseHandler
{
    public TryScanByPrefixFromDiskHandler(KeyValueContext context) : base(context)
    {
        
    }

    /// <summary>
    /// Executes the scan by prefix from disk request
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    public async Task<KeyValueResponse> Execute(KeyValueRequest message)
    {
        Dictionary<string, ReadOnlyKeyValueEntry> items = new();

        HLCTimestamp currentTime = context.Raft.HybridLogicalClock.TrySendOrLocalEvent(context.Raft.GetLocalNodeId());
        HLCTimestamp readTimestamp = message.ReadTimestamp;

        List<(string, ReadOnlyKeyValueEntry)> itemsFromDisk = await context.Raft.ReadScheduler.EnqueueTask(
            message.PartitionId,
            () =>
            {
                List<(string, ReadOnlyKeyValueEntry)> scanned = context.PersistenceBackend.GetKeyValueByPrefix(message.Key);

                if (readTimestamp.IsNull())
                    return scanned;

                // Snapshot scan: the prefix scan returns each key's latest committed revision. When that
                // revision is newer than the snapshot, walk the persisted revision history backwards to the
                // most recent revision at-or-before the snapshot, mirroring the in-memory scan path. A key
                // with no retained revision at-or-before the snapshot is dropped (it did not exist, or was
                // written, after the snapshot).
                List<(string, ReadOnlyKeyValueEntry)> projected = new(scanned.Count);

                foreach ((string key, ReadOnlyKeyValueEntry entry) in scanned)
                {
                    if (entry.LastModified.CompareTo(readTimestamp) <= 0)
                    {
                        projected.Add((key, entry));
                        continue;
                    }

                    KeyValueEntry? snapshot = context.PersistenceBackend.GetKeyValueRevisionAtOrBefore(key, entry.Revision - 1, readTimestamp);
                    if (snapshot is not null && snapshot.State != KeyValueState.Deleted)
                        projected.Add((key, new(snapshot.Value, snapshot.Revision, snapshot.Expires, snapshot.LastUsed, snapshot.LastModified, snapshot.State)));
                }

                return projected;
            });

        foreach ((string key, ReadOnlyKeyValueEntry readOnlyKeyValueEntry) in itemsFromDisk)
        {
            if (items.ContainsKey(key))
                continue;

            if (readOnlyKeyValueEntry.State == KeyValueState.Deleted || readOnlyKeyValueEntry.Expires != HLCTimestamp.Zero && readOnlyKeyValueEntry.Expires - currentTime < TimeSpan.Zero)
                continue;

            items.Add(key, readOnlyKeyValueEntry);
        }

        return new(KeyValueResponseType.Get, items.Select(kv => (kv.Key, kv.Value)).ToList());
    }
}
