
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
/// Represents a handler that attempts to scan key-value entries in a data store based on a specified prefix.
/// </summary>
/// <remarks>
/// This handler iterates over the key-value store (b+tree), filters entries that match the given prefix, discards expired entries,
/// and returns the filtered entries. 
/// </remarks>
internal sealed class TryScanByPrefixHandler : BaseHandler
{
    public TryScanByPrefixHandler(KeyValueContext context) : base(context)
    {
        
    }

    /// <summary>
    /// Executes the scan by prefix request
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    public async Task<KeyValueResponse> Execute(KeyValueRequest message)
    {
        List<(string, ReadOnlyKeyValueEntry)> items = [];
        HLCTimestamp currentTime = context.Raft.HybridLogicalClock.TrySendOrLocalEvent(context.Raft.GetLocalNodeId());

        // Bound inspected entries, not just returned ones: a run of interleaved tombstones/expired rows
        // must not make this synchronous walk scan the whole resident bucket on the mailbox thread.
        int inspectionBudget = KeyValueScanLimits.MaxPrefixScanResults + KeyValueScanLimits.MaxScanInspectionSlack;
        int inspected = 0;
        bool truncated = false;

        foreach ((string key, KeyValueEntry? _) in context.Store.GetByBucket(message.Key))
        {
            inspected++;

            KeyValueResponse response = await Get(currentTime, key, message.Durability, message.ReadTimestamp);

            if (response is { Type: KeyValueResponseType.Get, Entry: not null })
                items.Add((key, response.Entry));

            if (items.Count >= KeyValueScanLimits.MaxPrefixScanResults)
                break;

            if (inspected >= inspectionBudget)
            {
                truncated = true;
                break;
            }
        }

        if (truncated)
            context.Logger.LogWarning(
                "Prefix scan of {Prefix} stopped after inspecting {Inspected} entries with {Collected} results; " +
                "bucket is tombstone-dense — use the paginated range scan for a complete result set",
                message.Key, inspected, items.Count);

        return new(KeyValueResponseType.Get, items);
    }

    private async Task<KeyValueResponse> Get(HLCTimestamp currentTime, string key, KeyValueDurability durability, HLCTimestamp readTimestamp = default, ReadOnlyKeyValueEntry? keyValueEntry = null)
    {
        KeyValueEntry? entry = await GetKeyValueEntry(key, durability, keyValueEntry);

        // Non-transactional snapshot visibility: serve the revision at-or-before readTimestamp.
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

        if (entry is null || entry.State == KeyValueState.Deleted || entry.Expires != HLCTimestamp.Zero && entry.Expires - currentTime < TimeSpan.Zero)
            return KeyValueStaticResponses.DoesNotExistContextResponse;

        ReadOnlyKeyValueEntry readOnlyKeyValueEntry = new(
            entry.Value,
            entry.Revision,
            entry.Expires,
            entry.LastUsed,
            entry.LastModified,
            entry.State
        );

        return new(KeyValueResponseType.Get, readOnlyKeyValueEntry);
    }
}