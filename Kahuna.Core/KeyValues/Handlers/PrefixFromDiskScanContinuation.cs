
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.KeyValues.Handlers;

/// <summary>
/// Stage-3 continuation for a prefix-from-disk scan (ScanByPrefixFromDisk).
///
/// Stage 2 runs the full GetKeyValueByPrefix query (or, for snapshot reads, the single-pass
/// GetKeyValueByPrefixAtOrBefore projection) off-actor. Stage 3 (Execute) applies the
/// deleted/expired filter against the raw disk page returned in ScanDiskResult, then resolves
/// all waiters.
///
/// Non-snapshot requests for the same prefix coalesce onto one disk read. Snapshot requests
/// coalesce in their own map, keyed additionally by the read timestamp their result depends on.
/// </summary>
internal sealed class PrefixFromDiskScanContinuation : ReadContinuation
{
    private readonly HLCTimestamp readTimestamp;
    private readonly HLCTimestamp currentTime;
    private readonly (string, long, bool)? scanKey;
    private readonly (string, HLCTimestamp, bool)? snapshotScanKey;
    private readonly bool includeTombstones;

    internal PrefixFromDiskScanContinuation(
        string prefix,
        HLCTimestamp readTimestamp,
        HLCTimestamp currentTime,
        KeyValueReplyRef promise,
        (string, long, bool)? scanKey,
        (string, HLCTimestamp, bool)? snapshotScanKey = null,
        bool includeTombstones = false) : base(promise)
    {
        this.readTimestamp = readTimestamp;
        this.currentTime = currentTime;
        this.scanKey = scanKey;
        this.snapshotScanKey = snapshotScanKey;
        this.includeTombstones = includeTombstones;
    }

    internal override void RemovePendingKey(KeyValueContext context)
    {
        // Only remove the registration this continuation installed. A snapshot continuation
        // must not evict a concurrent non-snapshot scan's entry that happens to share the same
        // prefix, and vice versa.
        if (scanKey.HasValue)
            context.PendingReads.Remove(scanKey.Value);
        if (snapshotScanKey.HasValue)
            context.PendingSnapshotPrefixScans.Remove(snapshotScanKey.Value);
    }

    internal override void Execute(KeyValueContext context)
    {
        RemovePendingKey(context);

        if (Faulted)
        {
            Resolve(KeyValueStaticResponses.MustRetryResponse);
            return;
        }

        Dictionary<string, ReadOnlyKeyValueEntry> items = new();

        if (ScanDiskResult is not null)
        {
            foreach ((string key, ReadOnlyKeyValueEntry entry) in ScanDiskResult)
            {
                if (items.ContainsKey(key))
                    continue;

                if (entry.State == KeyValueState.Deleted && !includeTombstones)
                    continue;

                if (entry.State != KeyValueState.Deleted &&
                    entry.Expires != HLCTimestamp.Zero &&
                    entry.Expires - currentTime < TimeSpan.Zero)
                    continue;

                items.Add(key, entry);
            }
        }

        Resolve(new(KeyValueResponseType.Get, items.Select(kv => (kv.Key, kv.Value)).ToList()));
    }
}
