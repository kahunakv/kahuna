using Kahuna.Server.KeyValues.Transactions.Data;
using Kommander.Time;

namespace Kahuna.Server.KeyValues.Transactions;

/// <summary>
/// Reconciles a range/prefix scan page with the durable prepared intents covering its window (the scan-merge of the
/// read-visibility contract). Applied after a scan handler has assembled a page from MVCC/disk and before it builds
/// the paged response: a committed intent overrides an existing key's value or injects an intent-only committed key,
/// a committed delete (or a committed value whose TTL has elapsed) excludes a key, an aborted or below-snapshot
/// intent is invisible, and any undecided intent within the snapshot makes the whole page retry. It is only invoked
/// when at least one intent covers the window (the caller keeps its own pagination off the durable path).
///
/// <para>Pagination is owned here, not by the caller's sentinel logic: the visible intents are merge-inserted into
/// the KV rows in ordinal order, the union is capped at exactly <c>limit</c>, and the next-page cursor is derived
/// from the merged sequence — so an injected key counts toward the page and can itself become the cursor, and no
/// injection can push the page past <c>limit</c> or re-emit a boundary key on the next page. The intent set must be
/// enumerated with the same window boundaries the KV rows were drawn from (<c>SnapshotScanWindow</c>).</para>
/// </summary>
internal static class PreparedIntentScanMerge
{
    /// <summary>The reconciled page plus its pagination outcome: the ordinal-ordered items, whether the whole page
    /// must retry (an undecided intent within the snapshot), whether a further page exists, and the resume cursor
    /// key (the last key accounted for on this page; the next page resumes strictly after it).</summary>
    public readonly record struct ScanMergeResult(
        List<(string Key, ReadOnlyKeyValueEntry Entry)> Items,
        bool MustRetry,
        bool HasMore,
        string? NextCursorKey);

    /// <param name="items">The page the scan produced from MVCC/disk, ordinal-ordered by key, up to
    /// <paramref name="limit"/>+1 rows (the pagination sentinel included when present).</param>
    /// <param name="intents">The prepared intents whose key falls in this page's window, enumerated with the page's
    /// own start-exclusivity/end-inclusivity via <c>PreparedIntentStore.SnapshotScanWindow</c>.</param>
    /// <param name="snapshotTs">The scan's frozen snapshot timestamp (<see cref="HLCTimestamp.Zero"/> = latest),
    /// used for commit-timestamp visibility ordering.</param>
    /// <param name="currentTime">Wall-clock HLC "now", used for the ordinary-read expiry filter on committed
    /// intents (a committed-but-expired intent is dropped, matching an expired MVCC head entry).</param>
    /// <param name="limit">The page item cap.</param>
    /// <param name="kvHasMore">True when the KV side has rows beyond this page's window (a sentinel row was fetched,
    /// or the ephemeral walk truncated on its inspection budget) — an independent "more pages" signal so a committed
    /// delete that removes the sentinel cannot masquerade as end-of-scan.</param>
    /// <param name="kvCeilingKey">The largest KV key accounted for on this page (the sentinel/last-inspected key);
    /// used as the resume cursor when the merged page is empty or shorter than the KV window yet more rows exist.</param>
    /// <param name="decisionLookup">Resolves the canonical decision of a still-pending intent's transaction (from the
    /// transaction record) so a committed value is visible before it settles under deferred settlement. Null leaves
    /// pending intents at retry.</param>
    public static ScanMergeResult Merge(
        List<(string Key, ReadOnlyKeyValueEntry Entry)> items,
        IReadOnlyList<PreparedIntent> intents,
        HLCTimestamp snapshotTs,
        HLCTimestamp currentTime,
        int limit,
        bool kvHasMore,
        string? kvCeilingKey,
        Func<PreparedIntent, TransactionDecision>? decisionLookup = null)
    {
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
                    return new(items, MustRetry: true, HasMore: false, NextCursorKey: null);

                case ReadVisibilityAction.UseIntentValue:
                    // A committed delete, or a committed value whose TTL has elapsed, removes the key from the page —
                    // the same result an expired MVCC head entry produces on the ordinary scan.
                    if (intent.State == KeyValueState.Deleted || PreparedIntentVisibility.IsExpired(intent, currentTime))
                        excludes.Add(intent.Key);
                    else
                        overrides[intent.Key] = intent;
                    break;

                case ReadVisibilityAction.UseExisting:
                default:
                    break; // invisible: aborted, or a snapshot below the intent's commit timestamp.
            }
        }

        // Ordinal union of the KV rows (committed deletes removed, committed values overridden) and any intent-only
        // committed keys injected at their ordinal position. The window fetch already bounds the injected keys to
        // this page, so injecting every surviving override here and capping the union at limit+1 below is exact.
        SortedDictionary<string, ReadOnlyKeyValueEntry> merged = new(StringComparer.Ordinal);

        foreach ((string key, ReadOnlyKeyValueEntry entry) in items)
        {
            if (excludes.Contains(key))
                continue;

            merged[key] = overrides.TryGetValue(key, out PreparedIntent? ov) ? ToEntry(ov) : entry;
        }

        foreach ((string key, PreparedIntent ov) in overrides)
        {
            if (!merged.ContainsKey(key))
                merged[key] = ToEntry(ov); // intent-only committed key injected at its ordinal position.
        }

        List<(string Key, ReadOnlyKeyValueEntry Entry)> result = new(merged.Count);
        foreach (KeyValuePair<string, ReadOnlyKeyValueEntry> kv in merged)
            result.Add((kv.Key, kv.Value));

        // Cap at exactly limit and derive the cursor from the merged sequence. A full merged page (> limit) keeps
        // its first limit items and resumes after the limit-th key; a merged page that fits but whose KV side had
        // more (e.g. a committed delete removed the sentinel, or the ephemeral walk truncated) resumes after the KV
        // ceiling so the scan still advances.
        if (result.Count > limit)
        {
            result.RemoveRange(limit, result.Count - limit);
            return new(result, MustRetry: false, HasMore: true, NextCursorKey: result[^1].Key);
        }

        if (kvHasMore)
        {
            string? cursor = kvCeilingKey ?? (result.Count > 0 ? result[^1].Key : null);
            return new(result, MustRetry: false, HasMore: cursor is not null, NextCursorKey: cursor);
        }

        return new(result, MustRetry: false, HasMore: false, NextCursorKey: null);
    }

    private static ReadOnlyKeyValueEntry ToEntry(PreparedIntent i) =>
        new(i.Value, i.Revision, i.Expires, i.CommitTimestamp, i.CommitTimestamp, i.State);
}
