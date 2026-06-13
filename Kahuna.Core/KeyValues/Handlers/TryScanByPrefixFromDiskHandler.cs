
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

                    if (TryResolveRevisionAtOrBefore(key, entry.Revision, readTimestamp, out ReadOnlyKeyValueEntry snapshot))
                        projected.Add((key, snapshot));
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

    /// <summary>
    /// Walks the persisted revision history of <paramref name="key"/> from <paramref name="latestRevision"/>
    /// downwards, returning the most recent revision whose LastModified is at-or-before
    /// <paramref name="readTimestamp"/>. Returns false when no such revision is retained.
    /// </summary>
    private bool TryResolveRevisionAtOrBefore(string key, long latestRevision, HLCTimestamp readTimestamp, out ReadOnlyKeyValueEntry snapshot)
    {
        snapshot = default!;

        for (long revision = latestRevision - 1; revision >= 0; revision--)
        {
            KeyValueEntry? historical = context.PersistenceBackend.GetKeyValueRevision(key, revision);
            if (historical is null)
                return false;

            if (historical.LastModified.CompareTo(readTimestamp) > 0)
                continue;

            if (historical.State == KeyValueState.Deleted)
                return false;

            snapshot = new(
                historical.Value,
                historical.Revision,
                historical.Expires,
                historical.LastUsed,
                historical.LastModified,
                historical.State);

            return true;
        }

        return false;
    }
}
