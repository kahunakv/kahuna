
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.KeyValues.Handlers;

/// <summary>
/// Stage-3 continuation for a prefix-from-disk scan (ScanByPrefixFromDisk).
///
/// Stage 2 runs the full GetKeyValueByPrefix query (plus per-key GetKeyValueRevisionAtOrBefore
/// for snapshot reads) off-actor. Stage 3 (Execute) applies the deleted/expired filter against
/// the raw disk page returned in ScanDiskResult, then resolves all waiters.
///
/// Non-snapshot requests for the same prefix coalesce onto one disk read. Snapshot requests
/// (readTimestamp set) are never coalesced because their result depends on the read timestamp.
/// </summary>
internal sealed class PrefixFromDiskScanContinuation : ReadContinuation
{
    private readonly string prefix;
    private readonly HLCTimestamp readTimestamp;
    private readonly HLCTimestamp currentTime;

    internal PrefixFromDiskScanContinuation(
        string prefix,
        HLCTimestamp readTimestamp,
        HLCTimestamp currentTime,
        TaskCompletionSource<KeyValueResponse?> promise) : base(promise)
    {
        this.prefix = prefix;
        this.readTimestamp = readTimestamp;
        this.currentTime = currentTime;
    }

    internal override void Execute(KeyValueContext context)
    {
        // Only non-snapshot requests are registered in PendingReads; Remove is a no-op for others.
        context.PendingReads.Remove((prefix, -3L, false));

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

                if (entry.State == KeyValueState.Deleted ||
                    (entry.Expires != HLCTimestamp.Zero &&
                     entry.Expires - currentTime < TimeSpan.Zero))
                    continue;

                items.Add(key, entry);
            }
        }

        Resolve(new(KeyValueResponseType.Get, items.Select(kv => (kv.Key, kv.Value)).ToList()));
    }
}
