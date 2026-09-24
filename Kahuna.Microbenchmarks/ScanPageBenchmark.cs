using System.Collections.Concurrent;
using BenchmarkDotNet.Attributes;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Microbenchmarks;

/// <summary>
/// Before/after for the scan-page work that scaled with the node-wide backlog instead of the page:
///
/// <list type="bullet">
/// <item><b>Overlay range read.</b> <c>Old</c> = the previous union: collect every matching unflushed head,
/// build a dictionary, materialise one entry per head, sort every key, then cut to the page. <c>New</c> =
/// the production <see cref="UnflushedOverlayPersistenceBackend.GetKeyValueByRange"/>: bounded selection of
/// the page's candidates during the same enumeration, then a two-way merge. Both run over identical data:
/// the production overlay and a mirror dictionary populated with the same writes, with an empty inner
/// backend so every row comes from the overlay (the worst case for the union).</item>
/// <item><b>Intent window capture.</b> <c>Old</c> = the previous <c>SnapshotScanWindow</c> shape over a mirror
/// dictionary of the same intents: <c>Values</c> copies the whole concurrent dictionary under every bucket lock,
/// then filters. <c>New</c> = the production <c>SnapshotScanWindow</c>, which enumerates the map lock-free.</item>
/// </list>
///
/// Sizes follow the report and the production evidence: the unflushed backlog held 4K–19K items per node
/// under load and up to 700K on followers before its 1M-item budget, and the overlay holds one entry per
/// distinct unflushed key.
/// </summary>
[MemoryDiagnoser]
[ShortRunJob]
public class ScanPageBenchmark
{
    [Params(1_000, 10_000, 100_000)]
    public int N;

    private const int PageSize = 100;

    private readonly UnflushedKeyValueWritesIndex overlay = new();
    private readonly ConcurrentDictionary<string, UnflushedKeyValueWrite> mirror = new(StringComparer.Ordinal);
    private UnflushedOverlayPersistenceBackend backend = null!;

    private readonly PreparedIntentStore store = new();
    private readonly ConcurrentDictionary<string, PreparedIntent> intentMirror = new(StringComparer.Ordinal);

    [GlobalSetup]
    public void Setup()
    {
        byte[] value = [1];
        HLCTimestamp ts = new(0, 1, 0);

        // Shuffled insertion so dictionary order is unrelated to key order, as in production.
        Random random = new(42);
        foreach (int i in Enumerable.Range(0, N).OrderBy(_ => random.Next()))
        {
            string key = $"k/{i:D8}";
            overlay.Record(key, value, 1, default, default, ts, KeyValueState.Set, false);
            // A first write for a key is also the oldest one still queued, which is what Record stores.
            mirror[key] = new UnflushedKeyValueWrite(value, 1, default, default, ts, KeyValueState.Set, false, OldestRevision: 1);

            PreparedIntent intent = new(new HLCTimestamp(0, 500, 0), 1, key, 0, key, new HLCTimestamp(0, 1000, 0),
                KeyValueState.Set, [9], null, 5, default, false, 0, KeyValueState.Set, new HLCTimestamp(0, 9000, 0),
                PreparedIntentResolution.Committed);
            intentMirror[key] = intent;
        }

        store.ImportIntents(intentMirror.Values);
        backend = new UnflushedOverlayPersistenceBackend(new MemoryPersistenceBackend(), overlay, new UnflushedLockWritesIndex());
    }

    // ── overlay range read, limit 100, every row in the overlay ────────────────

    [Benchmark(Baseline = true)]
    public int OverlayRangeRead_Old()
    {
        List<KeyValuePair<string, UnflushedKeyValueWrite>> matches = [];
        foreach (KeyValuePair<string, UnflushedKeyValueWrite> kv in mirror)
        {
            if (kv.Key.StartsWith("k/", StringComparison.Ordinal))
                matches.Add(kv);
        }

        return OldMergeScan([], matches, PageSize).Count;
    }

    [Benchmark]
    public int OverlayRangeRead_New() => backend.GetKeyValueByRange("k/", null, PageSize).Count;

    [Benchmark]
    public int OverlayAbsentPrefix_New() => overlay.Collect("absent/", null, PageSize, null).Count;

    // ── prepared-intent window capture, one row ────────────────────────────────

    [Benchmark]
    public int IntentWindow_Old()
    {
        List<PreparedIntent> result = [];
        foreach (PreparedIntent intent in intentMirror.Values)
        {
            int cmpStart = string.CompareOrdinal(intent.Key, "k/00000000");
            if (cmpStart < 0)
                continue;
            int cmpEnd = string.CompareOrdinal(intent.Key, "k/00000000");
            if (cmpEnd > 0)
                continue;
            result.Add(intent);
        }

        return result.Count;
    }

    [Benchmark]
    public int IntentWindow_New() => store.SnapshotScanWindow("k/00000000", true, "k/00000000", true).Count;

    private static List<(string, ReadOnlyKeyValueEntry)> OldMergeScan(
        List<(string, ReadOnlyKeyValueEntry)> diskItems,
        List<KeyValuePair<string, UnflushedKeyValueWrite>> queuedItems,
        int limit)
    {
        if (queuedItems.Count == 0)
            return diskItems;

        Dictionary<string, ReadOnlyKeyValueEntry> merged = new(diskItems.Count + queuedItems.Count, StringComparer.Ordinal);

        foreach ((string key, ReadOnlyKeyValueEntry entry) in diskItems)
            merged[key] = entry;

        foreach ((string key, UnflushedKeyValueWrite queued) in queuedItems)
        {
            if (merged.TryGetValue(key, out ReadOnlyKeyValueEntry? existing)
                && (existing.Revision > queued.Revision
                    || (existing.Revision == queued.Revision && existing.LastModified > queued.LastModified)))
                continue;

            merged[key] = new(queued.Value, queued.Revision, queued.Expires, queued.LastUsed, queued.LastModified, queued.State);
        }

        List<(string, ReadOnlyKeyValueEntry)> result = new(Math.Min(merged.Count, limit));
        foreach (string key in merged.Keys.OrderBy(static k => k, StringComparer.Ordinal))
        {
            if (result.Count >= limit)
                break;
            result.Add((key, merged[key]));
        }

        return result;
    }
}

/// <summary>
/// Before/after for <see cref="PreparedIntentScanMerge.Merge"/> on a page of <c>N</c> sorted rows with one
/// committed override: <c>Old</c> rebuilds a <c>SortedDictionary</c> of the whole union, <c>New</c> is the
/// production two-way merge over the sorted rows and the sorted overrides.
/// </summary>
[MemoryDiagnoser]
[ShortRunJob]
public class IntentScanMergeBenchmark
{
    [Params(100, 1_000, 10_000)]
    public int N;

    private List<(string Key, ReadOnlyKeyValueEntry Entry)> rows = null!;
    private PreparedIntent[] intents = null!;

    [GlobalSetup]
    public void Setup()
    {
        rows = Enumerable.Range(0, N)
            .Select(static i => ($"k/{i:D8}", new ReadOnlyKeyValueEntry([1], 1, default, default, default, KeyValueState.Set)))
            .ToList();

        intents =
        [
            new PreparedIntent(new HLCTimestamp(0, 500, 0), 1, "k/00000000", 0, "k/00000000", new HLCTimestamp(0, 1000, 0),
                KeyValueState.Set, [9], null, 5, default, false, 0, KeyValueState.Set, new HLCTimestamp(0, 9000, 0),
                PreparedIntentResolution.Committed)
        ];
    }

    [Benchmark(Baseline = true)]
    public int Merge_Old() => OldMerge(rows, intents, default, default, N, false, null).Items.Count;

    [Benchmark]
    public int Merge_New() => PreparedIntentScanMerge.Merge(rows, intents, default, default, N, false, null).Items.Count;

    private static PreparedIntentScanMerge.ScanMergeResult OldMerge(
        List<(string Key, ReadOnlyKeyValueEntry Entry)> items,
        IReadOnlyList<PreparedIntent> intents,
        HLCTimestamp snapshotTs,
        HLCTimestamp currentTime,
        int limit,
        bool kvHasMore,
        string? kvCeilingKey)
    {
        Dictionary<string, PreparedIntent> overrides = [];
        HashSet<string> excludes = [];

        foreach (PreparedIntent intent in intents)
        {
            switch (PreparedIntentVisibility.Resolve(intent, snapshotTs, TransactionDecision.Undecided))
            {
                case ReadVisibilityAction.Retry:
                    return new(items, MustRetry: true, HasMore: false, NextCursorKey: null);

                case ReadVisibilityAction.UseIntentValue:
                    if (intent.State == KeyValueState.Deleted || PreparedIntentVisibility.IsExpired(intent, currentTime))
                        excludes.Add(intent.Key);
                    else
                        overrides[intent.Key] = intent;
                    break;
            }
        }

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
                merged[key] = ToEntry(ov);
        }

        List<(string Key, ReadOnlyKeyValueEntry Entry)> result = new(merged.Count);
        foreach (KeyValuePair<string, ReadOnlyKeyValueEntry> kv in merged)
            result.Add((kv.Key, kv.Value));

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
