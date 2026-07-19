using Kahuna.Server.KeyValues.Transactions.Data;
using Kommander.Time;

namespace Kahuna.Server.KeyValues.Transactions;

/// <summary>
/// Reconciles a range/prefix scan page with the durable prepared intents covering its range (the scan-merge of
/// the read-visibility contract). Applied after a scan handler has assembled a page from MVCC/disk and before it
/// builds the paged response: a committed intent overrides an existing key's value or injects an intent-only
/// committed key, a committed delete excludes a key, an aborted or below-snapshot intent is invisible, and any
/// undecided intent within the snapshot makes the whole page retry. It is a no-op when no intents cover the range
/// (durable-intent path disabled), so scans are unchanged off the durable path.
/// </summary>
internal static class PreparedIntentScanMerge
{
    /// <param name="items">The page the scan produced, ordinal-ordered by key.</param>
    /// <param name="intents">The prepared intents whose key falls in the scan's range (e.g. from
    /// <c>PreparedIntentStore.SnapshotRange</c>).</param>
    /// <param name="snapshotTs">The scan's frozen snapshot timestamp (<see cref="HLCTimestamp.Zero"/> = latest).</param>
    /// <param name="pageExhausted">True when the scan reached the end of its range on this page (did not hit the
    /// page limit). When false the page is bounded by <paramref name="items"/>'s last key, so an intent-only key
    /// beyond that key belongs to a later page and must not be injected here.</param>
    /// <returns>The reconciled page (ordinal-ordered) and whether the page must retry (an undecided intent within
    /// the snapshot). On retry the returned list is the original, unmodified.</returns>
    /// <param name="decisionLookup">Resolves the canonical decision of a still-pending intent's transaction (from
    /// the transaction record) so a committed value is visible before it settles under deferred settlement. Null
    /// leaves pending intents at retry.</param>
    public static (List<(string Key, ReadOnlyKeyValueEntry Entry)> Items, bool MustRetry) Merge(
        List<(string Key, ReadOnlyKeyValueEntry Entry)> items,
        IReadOnlyList<PreparedIntent> intents,
        HLCTimestamp snapshotTs,
        bool pageExhausted,
        Func<PreparedIntent, TransactionDecision>? decisionLookup = null)
    {
        if (intents.Count == 0)
            return (items, false);

        Dictionary<string, PreparedIntent> overrides = [];
        HashSet<string> excludes = [];

        foreach (PreparedIntent intent in intents)
        {
            TransactionDecision decision = intent.Resolution == PreparedIntentResolution.Pending && decisionLookup is not null
                ? decisionLookup(intent)
                : TransactionDecision.Undecided;

            switch (PreparedIntentVisibility.Resolve(intent, snapshotTs, decision))
            {
                case ReadVisibilityAction.Retry:
                    return (items, true); // an undecided key in range: the page cannot be linearizable.

                case ReadVisibilityAction.UseIntentValue:
                    if (intent.State == KeyValueState.Deleted)
                        excludes.Add(intent.Key);
                    else
                        overrides[intent.Key] = intent;
                    break;

                case ReadVisibilityAction.UseExisting:
                default:
                    break; // invisible: aborted, or a snapshot below the intent's commit timestamp.
            }
        }

        if (overrides.Count == 0 && excludes.Count == 0)
            return (items, false);

        // The upper key of the fetched window: an intent-only key beyond it belongs to a later page unless the
        // scan already reached the end of its range.
        string? windowMax = items.Count > 0 ? items[^1].Key : null;

        SortedDictionary<string, ReadOnlyKeyValueEntry> merged = new(StringComparer.Ordinal);

        foreach ((string key, ReadOnlyKeyValueEntry entry) in items)
        {
            if (excludes.Contains(key))
                continue; // committed delete removes the key from the result.

            merged[key] = overrides.TryGetValue(key, out PreparedIntent? ov)
                ? ToEntry(ov) // committed intent overrides the older MVCC value already in the window.
                : entry;
        }

        foreach ((string key, PreparedIntent ov) in overrides)
        {
            if (merged.ContainsKey(key))
                continue; // already overrode an existing item.

            // Inject an intent-only committed key only if it lies within this page's emitted window.
            if (pageExhausted || (windowMax is not null && string.CompareOrdinal(key, windowMax) <= 0))
                merged[key] = ToEntry(ov);
        }

        List<(string, ReadOnlyKeyValueEntry)> result = new(merged.Count);
        foreach (KeyValuePair<string, ReadOnlyKeyValueEntry> kv in merged)
            result.Add((kv.Key, kv.Value));

        return (result, false);
    }

    private static ReadOnlyKeyValueEntry ToEntry(PreparedIntent i) =>
        new(i.Value, i.Revision, i.Expires, i.CommitTimestamp, i.CommitTimestamp, i.State);
}
