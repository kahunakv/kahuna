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
/// <para>A committed intent is only the newest committed state of its key until a later committed write moves the
/// head past it: a non-transactional write proceeds over a committed-but-unsettled intent (it materializes it and
/// writes the next revision), and the intent lingers until its settlement removes it. Such an intent is ignored —
/// a page row strictly newer than the intent stands, and so does a head the caller knows of for a key the page
/// excluded (a newer delete) — so the page never goes back to the older committed value.</para>
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
    /// <param name="readerHasOwnVersion">True for a key the scanning transaction holds its own MVCC version of; the
    /// page already reflects that version and no foreign intent may change it.</param>
    /// <param name="supersededByHead">True when the caller knows a committed head of the intent's key strictly newer
    /// than the intent — for a key the page holds no row for (a newer delete, or a row outside what the page drew).
    /// A row the page does hold is compared by the merge itself.</param>
    public static ScanMergeResult Merge(
        List<(string Key, ReadOnlyKeyValueEntry Entry)> items,
        IReadOnlyList<PreparedIntent> intents,
        HLCTimestamp snapshotTs,
        HLCTimestamp currentTime,
        int limit,
        bool kvHasMore,
        string? kvCeilingKey,
        Func<PreparedIntent, TransactionDecision>? decisionLookup = null,
        Func<string, bool>? readerHasOwnVersion = null,
        Func<PreparedIntent, bool>? supersededByHead = null)
    {
        // Small (the window is clamped to the page) and usually empty, so allocated on first use only. Dead marks a
        // committed delete or an expired committed value: it removes the key rather than overriding it.
        List<(PreparedIntent Intent, bool Dead)>? overrides = null;

        foreach (PreparedIntent intent in intents)
        {
            // A key the scanning transaction already holds its own version of is served from that version
            // (read-your-own-write / snapshot consistency): the KV page already reflects it via the per-key MVCC
            // path, so a foreign committed intent must not override it, exclude it, or (for a delete) resurrect it.
            // Without this, an UPDATE/DELETE over a committed-but-unsettled row is masked by that row's lingering
            // foreign intent, so the transaction fails to see its own mutation.
            if (readerHasOwnVersion is not null && readerHasOwnVersion(intent.Key))
                continue;

            TransactionDecision decision = intent.Resolution == PreparedIntentResolution.Pending && decisionLookup is not null
                ? decisionLookup(intent)
                : TransactionDecision.Undecided;

            switch (PreparedIntentVisibility.Resolve(intent, snapshotTs, decision))
            {
                case ReadVisibilityAction.Retry:
                    return new(items, MustRetry: true, HasMore: false, NextCursorKey: null);

                case ReadVisibilityAction.UseIntentValue:
                    // A later committed write already moved the head past this intent: the page's own view of the
                    // key (its row, or its absence) is newer and stands.
                    if (supersededByHead is not null && supersededByHead(intent))
                        break;

                    // A committed delete, or a committed value whose TTL has elapsed, removes the key from the page —
                    // the same result an expired MVCC head entry produces on the ordinary scan.
                    (overrides ??= []).Add((intent,
                        intent.State == KeyValueState.Deleted || PreparedIntentVisibility.IsExpired(intent, currentTime)));
                    break;

                case ReadVisibilityAction.UseExisting:
                default:
                    break; // invisible: aborted, or a snapshot below the intent's commit timestamp.
            }
        }

        // Ordinal union of the KV rows (committed deletes removed, committed values overridden) and any intent-only
        // committed keys injected at their ordinal position. The window fetch already bounds the injected keys to
        // this page, so injecting every surviving override here and capping the union at limit+1 below is exact.
        //
        // The KV rows arrive ordinal-ordered (the in-memory tree walk, the K-way disk merge, and the bucket path's
        // explicit sort all produce that order), so the union is a two-way merge of the rows with the sorted
        // overrides: O(n + k log k) with only the result list allocated, instead of a rebuilt O((n + k) log (n + k))
        // tree. The order precondition is checked in one linear pass; a misordered page is restored and counted so
        // the contract violation is visible to operators, and never merged into a wrong result silently.
        if (!IsOrdinalAscending(items))
        {
            KeyValueScanMetrics.MergePagesReordered.Add(1);
            items.Sort(static (a, b) => string.CompareOrdinal(a.Key, b.Key));
        }

        overrides?.Sort(static (a, b) => string.CompareOrdinal(a.Intent.Key, b.Intent.Key));

        int rowCount = items.Count;
        int overrideCount = overrides?.Count ?? 0;

        // Emit at most limit + 1 items: the extra one is enough to decide the page is full and to derive the cursor.
        long emitCap = limit == int.MaxValue ? (long)rowCount + overrideCount : (long)limit + 1;
        List<(string Key, ReadOnlyKeyValueEntry Entry)> result = new((int)Math.Min((long)rowCount + overrideCount, emitCap));

        int i = 0, j = 0;
        while (result.Count < emitCap && (i < rowCount || j < overrideCount))
        {
            if (j >= overrideCount)
            {
                result.Add(items[i++]);
                continue;
            }

            (PreparedIntent ov, bool dead) = overrides![j];

            int cmp = i >= rowCount ? 1 : string.CompareOrdinal(items[i].Key, ov.Key);
            if (cmp < 0)
            {
                result.Add(items[i++]);
            }
            else if (cmp > 0)
            {
                // Intent-only committed key injected at its ordinal position; a dead one has nothing to inject.
                if (!dead)
                    result.Add((ov.Key, ToEntry(ov)));
                j++;
            }
            else
            {
                // A row strictly newer than the intent was committed after it and stands. Otherwise the committed
                // intent overrides the row, or removes it when dead.
                if (items[i].Entry.Revision > ov.Revision)
                    result.Add(items[i]);
                else if (!dead)
                    result.Add((ov.Key, ToEntry(ov)));
                i++;
                j++;
            }
        }

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

    private static bool IsOrdinalAscending(List<(string Key, ReadOnlyKeyValueEntry Entry)> items)
    {
        for (int i = 1; i < items.Count; i++)
        {
            if (string.CompareOrdinal(items[i - 1].Key, items[i].Key) > 0)
                return false;
        }

        return true;
    }
}
