using Kahuna.Server.KeyValues;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Server.Persistence.Pitr;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// Differential tests for the overlay scan merge in <see cref="UnflushedOverlayPersistenceBackend"/>: the
/// bounded candidate selection plus the two-way merge must produce exactly the page the previous
/// unbounded union produced — same keys, same versions, same order, same cap — on every mix of disk
/// rows, queued heads, seeks, prefixes and page sizes, including the historical prefix path. A reference
/// implementation of the unbounded union is the oracle; the overlay under test and a mirror of its
/// contents are fed identical writes.
/// </summary>
public sealed class TestUnflushedOverlayScanBound
{
    private static HLCTimestamp Ts(long l) => new(0, l, 0);

    private sealed class Fixture : IDisposable
    {
        public readonly IPersistenceBackend Inner = new MemoryPersistenceBackend();
        public readonly UnflushedKeyValueWritesIndex Overlay = new();
        public readonly Dictionary<string, UnflushedKeyValueWrite> Mirror = new(StringComparer.Ordinal);
        public readonly UnflushedOverlayPersistenceBackend Backend;

        public Fixture()
        {
            Backend = new(Inner, Overlay, new UnflushedLockWritesIndex());
        }

        public void Dispose() => Backend.Dispose();

        public void StoreInner(string key, byte[]? value, long revision, HLCTimestamp lastModified, KeyValueState state)
        {
            Assert.True(Inner.StoreKeyValues([
                new PersistenceRequestItem(key, value, revision, 0, 0, 0, 0, 0, 0,
                    lastModified.N, lastModified.L, lastModified.C, (int)state)
            ]));
        }

        public void Record(string key, byte[]? value, long revision, HLCTimestamp lastModified, KeyValueState state, bool noRevision = false)
        {
            Overlay.Record(key, value, revision, Ts(revision * 10), Ts(revision * 20), lastModified, state, noRevision);

            UnflushedKeyValueWrite incoming = new(value, revision, Ts(revision * 10), Ts(revision * 20), lastModified, state, noRevision);
            if (!Mirror.TryGetValue(key, out UnflushedKeyValueWrite existing)
                || !UnflushedKeyValueWritesIndex.IsNewer(existing, revision, lastModified))
                Mirror[key] = incoming;
        }
    }

    // ── reference implementations (the previous unbounded union) ─────────────────

    private static List<(string, ReadOnlyKeyValueEntry)> ReferenceMerge(
        List<(string, ReadOnlyKeyValueEntry)> disk,
        Dictionary<string, UnflushedKeyValueWrite> mirror,
        string prefix,
        string? startKey,
        int limit)
    {
        Dictionary<string, ReadOnlyKeyValueEntry> merged = new(StringComparer.Ordinal);
        foreach ((string key, ReadOnlyKeyValueEntry entry) in disk)
            merged[key] = entry;

        foreach ((string key, UnflushedKeyValueWrite queued) in mirror)
        {
            if (!key.StartsWith(prefix, StringComparison.Ordinal))
                continue;
            if (startKey is not null && string.CompareOrdinal(key, startKey) < 0)
                continue;
            if (merged.TryGetValue(key, out ReadOnlyKeyValueEntry? existing)
                && (existing.Revision > queued.Revision
                    || (existing.Revision == queued.Revision && existing.LastModified > queued.LastModified)))
                continue;
            merged[key] = new(queued.Value, queued.Revision, queued.Expires, queued.LastUsed, queued.LastModified, queued.State);
        }

        if (limit < 0)
            limit = int.MaxValue;

        List<(string, ReadOnlyKeyValueEntry)> result = [];
        foreach (string key in merged.Keys.OrderBy(static k => k, StringComparer.Ordinal))
        {
            if (result.Count >= limit)
                break;
            result.Add((key, merged[key]));
        }

        return result;
    }

    private static List<(string, ReadOnlyKeyValueEntry, ReadOnlyKeyValueEntry?)> ReferenceHistorical(
        List<(string Key, ReadOnlyKeyValueEntry Current, ReadOnlyKeyValueEntry? Snapshot)> items,
        Dictionary<string, UnflushedKeyValueWrite> mirror,
        string prefix,
        HLCTimestamp readTimestamp)
    {
        Dictionary<string, (ReadOnlyKeyValueEntry Current, ReadOnlyKeyValueEntry? Snapshot)> merged = new(StringComparer.Ordinal);
        foreach ((string key, ReadOnlyKeyValueEntry current, ReadOnlyKeyValueEntry? snapshot) in items)
            merged[key] = (current, snapshot);

        foreach ((string key, UnflushedKeyValueWrite queued) in mirror)
        {
            if (!key.StartsWith(prefix, StringComparison.Ordinal))
                continue;

            ReadOnlyKeyValueEntry queuedEntry = new(queued.Value, queued.Revision, queued.Expires, queued.LastUsed, queued.LastModified, queued.State);

            if (!merged.TryGetValue(key, out (ReadOnlyKeyValueEntry Current, ReadOnlyKeyValueEntry? Snapshot) existing))
            {
                merged[key] = (queuedEntry, queued.LastModified.CompareTo(readTimestamp) <= 0 ? queuedEntry : null);
                continue;
            }

            ReadOnlyKeyValueEntry current = existing.Current.Revision > queued.Revision
                || (existing.Current.Revision == queued.Revision && existing.Current.LastModified > queued.LastModified)
                ? existing.Current
                : queuedEntry;

            ReadOnlyKeyValueEntry? snap;
            if (current.LastModified.CompareTo(readTimestamp) <= 0)
                snap = current;
            else
            {
                snap = existing.Snapshot;
                if (!queued.NoRevision
                    && queued.LastModified.CompareTo(readTimestamp) <= 0
                    && queued.Revision < current.Revision
                    && (snap is null || queued.Revision > snap.Revision))
                    snap = queuedEntry;
            }

            merged[key] = (current, snap);
        }

        List<(string, ReadOnlyKeyValueEntry, ReadOnlyKeyValueEntry?)> result = [];
        foreach (string key in merged.Keys.OrderBy(static k => k, StringComparer.Ordinal))
        {
            if (result.Count >= KeyValueScanLimits.MaxPrefixScanResults)
                break;
            (ReadOnlyKeyValueEntry current, ReadOnlyKeyValueEntry? snapshot) = merged[key];
            result.Add((key, current, snapshot));
        }

        return result;
    }

    private static void AssertSameEntry(ReadOnlyKeyValueEntry? expected, ReadOnlyKeyValueEntry? actual, string key)
    {
        if (expected is null)
        {
            Assert.Null(actual);
            return;
        }

        Assert.NotNull(actual);
        Assert.True(expected.Value is null ? actual.Value is null : actual.Value is not null && expected.Value.AsSpan().SequenceEqual(actual.Value), $"value differs at {key}");
        Assert.Equal(expected.Revision, actual.Revision);
        Assert.Equal(expected.Expires, actual.Expires);
        Assert.Equal(expected.LastUsed, actual.LastUsed);
        Assert.Equal(expected.LastModified, actual.LastModified);
        Assert.Equal(expected.State, actual.State);
    }

    private static void AssertSamePage(List<(string, ReadOnlyKeyValueEntry)> expected, List<(string, ReadOnlyKeyValueEntry)> actual)
    {
        Assert.Equal(expected.Select(static e => e.Item1).ToList(), actual.Select(static a => a.Item1).ToList());
        for (int i = 0; i < expected.Count; i++)
            AssertSameEntry(expected[i].Item2, actual[i].Item2, expected[i].Item1);
    }

    // ── randomized population ──────────────────────────────────────────────────

    private static readonly string[] Universe = BuildUniverse();

    private static string[] BuildUniverse()
    {
        List<string> keys = [];
        for (int i = 0; i < 40; i++)
            keys.Add($"k/{i:D2}");
        keys.Add("k/1");   // shorter than the two-digit keys: sorts before k/10 and is a prefix of them
        keys.Add("k/");    // the prefix itself as a key: the inclusive seek must keep it
        keys.Add("kz/00"); // matches prefix "k" but not "k/"
        keys.Add("j/99");  // below every k/ key
        keys.Add("l/00");  // above every k/ key
        return keys.ToArray();
    }

    private static readonly string[] Prefixes = ["k/", "k/1", "k", "absent/", ""];

    private static readonly int[] Limits = [0, 1, 2, 3, 5, 8, 13, 40, 100, int.MaxValue, -1];

    private static byte[]? RandomValue(Random random, KeyValueState state)
    {
        if (state == KeyValueState.Deleted && random.Next(2) == 0)
            return null;
        int length = random.Next(4); // includes the empty value
        byte[] value = new byte[length];
        random.NextBytes(value);
        return value;
    }

    private static Fixture Populate(Random random)
    {
        using Fixture f = new();

        foreach (string key in Universe)
        {
            if (random.Next(2) == 0)
            {
                KeyValueState state = random.Next(8) == 0 ? KeyValueState.Deleted : KeyValueState.Set;
                f.StoreInner(key, RandomValue(random, state), random.Next(1, 5), Ts(random.Next(1, 100)), state);
            }

            if (random.Next(2) == 0)
            {
                // One to three queued heads per key: exercises Record's newest-wins including same-revision HLC ties.
                int writes = random.Next(1, 4);
                for (int w = 0; w < writes; w++)
                {
                    KeyValueState state = random.Next(8) == 0 ? KeyValueState.Deleted : KeyValueState.Set;
                    f.Record(key, RandomValue(random, state), random.Next(1, 5), Ts(random.Next(1, 100)), state, noRevision: random.Next(10) == 0);
                }
            }
        }

        return f;
    }

    private static string? RandomStart(Random random)
    {
        return random.Next(5) switch
        {
            0 or 1 => null,
            2 => Universe[random.Next(Universe.Length)],
            3 => $"k/{random.Next(40):D2} ", // just past a key, the shape a continuation cursor takes
            _ => "k/2", // between k/1 and k/20: not itself a key
        };
    }

    [Fact]
    public void RangeRead_MatchesUnboundedUnion_OnRandomizedPopulations()
    {
        for (int seed = 0; seed < 400; seed++)
        {
            Random random = new(seed);
            using Fixture f = Populate(random);

            foreach (string prefix in Prefixes)
            {
                foreach (int limit in Limits)
                {
                    string? startKey = RandomStart(random);

                    List<(string, ReadOnlyKeyValueEntry)> disk = f.Inner.GetKeyValueByRange(prefix, startKey, limit);
                    string? effectiveStart = startKey is not null && string.CompareOrdinal(startKey, prefix) > 0 ? startKey : null;
                    List<(string, ReadOnlyKeyValueEntry)> expected = ReferenceMerge(disk, f.Mirror, prefix, effectiveStart, limit);

                    List<(string, ReadOnlyKeyValueEntry)> actual = f.Backend.GetKeyValueByRange(prefix, startKey, limit);

                    AssertSamePage(expected, actual);
                }
            }
        }
    }

    [Fact]
    public void PrefixRead_MatchesUnboundedUnion_OnRandomizedPopulations()
    {
        for (int seed = 0; seed < 200; seed++)
        {
            Random random = new(seed);
            using Fixture f = Populate(random);

            foreach (string prefix in Prefixes)
            {
                List<(string, ReadOnlyKeyValueEntry)> disk = f.Inner.GetKeyValueByPrefix(prefix);
                List<(string, ReadOnlyKeyValueEntry)> expected = ReferenceMerge(disk, f.Mirror, prefix, null, KeyValueScanLimits.MaxPrefixScanResults);

                AssertSamePage(expected, f.Backend.GetKeyValueByPrefix(prefix));
            }
        }
    }

    [Fact]
    public void HistoricalPrefixRead_MatchesUnboundedUnion_OnRandomizedPopulations()
    {
        for (int seed = 0; seed < 200; seed++)
        {
            Random random = new(seed);
            using Fixture f = Populate(random);

            foreach (string prefix in Prefixes)
            {
                // Read timestamps around the population's HLC range, so heads land on both sides of the snapshot.
                foreach (long readAt in new long[] { 0, 25, 50, 75, 1000 })
                {
                    HLCTimestamp readTs = Ts(readAt);
                    List<(string Key, ReadOnlyKeyValueEntry Current, ReadOnlyKeyValueEntry? Snapshot)> inner =
                        f.Inner.GetKeyValueByPrefixAtOrBefore(prefix, readTs);
                    List<(string, ReadOnlyKeyValueEntry, ReadOnlyKeyValueEntry?)> expected = ReferenceHistorical(inner, f.Mirror, prefix, readTs);

                    List<(string Key, ReadOnlyKeyValueEntry Current, ReadOnlyKeyValueEntry? Snapshot)> actual =
                        f.Backend.GetKeyValueByPrefixAtOrBefore(prefix, readTs);

                    Assert.Equal(expected.Select(static e => e.Item1).ToList(), actual.Select(static a => a.Key).ToList());
                    for (int i = 0; i < expected.Count; i++)
                    {
                        AssertSameEntry(expected[i].Item2, actual[i].Current, expected[i].Item1);
                        AssertSameEntry(expected[i].Item3, actual[i].Snapshot, expected[i].Item1);
                    }
                }
            }
        }
    }

    [Fact]
    public void Pagination_EmitsEveryKeyExactlyOnce_AcrossBoundedPages()
    {
        for (int seed = 0; seed < 100; seed++)
        {
            Random random = new(seed);
            using Fixture f = Populate(random);

            foreach (int limit in new[] { 1, 2, 3, 7 })
            {
                List<(string, ReadOnlyKeyValueEntry)> expected = ReferenceMerge(f.Inner.GetKeyValueByRange("k/", null, int.MaxValue), f.Mirror, "k/", null, int.MaxValue);

                List<(string, ReadOnlyKeyValueEntry)> collected = [];
                string? cursor = null;
                while (true)
                {
                    List<(string, ReadOnlyKeyValueEntry)> page = f.Backend.GetKeyValueByRange("k/", cursor, limit);
                    if (page.Count == 0)
                        break;
                    Assert.True(page.Count <= limit);
                    collected.AddRange(page);
                    if (page.Count < limit)
                        break;
                    cursor = page[^1].Item1 + " "; // inclusive seek strictly past the last emitted key
                }

                AssertSamePage(expected, collected);
            }
        }
    }

    [Fact]
    public void DiskCeiling_DoesNotDropAnOverlayKeyBelowIt_WhenInnerPageIsFull()
    {
        // Inner page of exactly `limit` rows: an overlay key below its last row displaces a disk row; an overlay
        // key above it must not appear; an overlay key equal to its last row replaces it when newer.
        using Fixture f = new();
        f.StoreInner("k/10", [1], 1, Ts(10), KeyValueState.Set);
        f.StoreInner("k/20", [1], 1, Ts(10), KeyValueState.Set);
        f.StoreInner("k/30", [1], 1, Ts(10), KeyValueState.Set);
        f.Record("k/15", [2], 1, Ts(20), KeyValueState.Set);
        f.Record("k/30", [3], 2, Ts(20), KeyValueState.Set);
        f.Record("k/40", [4], 1, Ts(20), KeyValueState.Set);

        List<(string, ReadOnlyKeyValueEntry)> page = f.Backend.GetKeyValueByRange("k/", null, 3);

        Assert.Equal(["k/10", "k/15", "k/20"], page.Select(static p => p.Item1).ToList());
        AssertSamePage(ReferenceMerge(f.Inner.GetKeyValueByRange("k/", null, 3), f.Mirror, "k/", null, 3), page);

        List<(string, ReadOnlyKeyValueEntry)> next = f.Backend.GetKeyValueByRange("k/", "k/20 ", 3);
        Assert.Equal(["k/30", "k/40"], next.Select(static p => p.Item1).ToList());
        Assert.Equal(2, next[0].Item2.Revision); // the newer queued head replaced the disk row
    }

    [Fact]
    public void ShortInnerPage_KeepsEveryLargerOverlayKey()
    {
        // The inner side is exhausted after one row, so overlay keys above it still fill the page.
        using Fixture f = new();
        f.StoreInner("k/10", [1], 1, Ts(10), KeyValueState.Set);
        f.Record("k/20", [2], 1, Ts(20), KeyValueState.Set);
        f.Record("k/30", [2], 1, Ts(20), KeyValueState.Set);
        f.Record("k/40", [2], 1, Ts(20), KeyValueState.Set);

        List<(string, ReadOnlyKeyValueEntry)> page = f.Backend.GetKeyValueByRange("k/", null, 3);

        Assert.Equal(["k/10", "k/20", "k/30"], page.Select(static p => p.Item1).ToList());
    }

    [Fact]
    public void Collect_SelectsTheSmallestKeysInOrder_NotTheFirstDictionaryEntries()
    {
        UnflushedKeyValueWritesIndex overlay = new();
        Random random = new(7);
        List<string> keys = Enumerable.Range(0, 5_000).Select(static i => $"k/{i:D5}").ToList();
        // Insert in shuffled order so the dictionary's enumeration order is unrelated to the key order.
        foreach (string key in keys.OrderBy(_ => random.Next()))
            overlay.Record(key, [1], 1, default, default, Ts(1), KeyValueState.Set, false);

        List<KeyValuePair<string, UnflushedKeyValueWrite>> selected = overlay.Collect("k/", "k/00100", 10, null);
        Assert.Equal(keys.Skip(100).Take(10).ToList(), selected.Select(static kv => kv.Key).ToList());

        List<KeyValuePair<string, UnflushedKeyValueWrite>> ceiled = overlay.Collect("k/", "k/00100", 10, "k/00104");
        Assert.Equal(keys.Skip(100).Take(5).ToList(), ceiled.Select(static kv => kv.Key).ToList());

        Assert.Empty(overlay.Collect("absent/", null, 10, null));
        Assert.Empty(overlay.Collect("k/", null, 0, null));

        List<KeyValuePair<string, UnflushedKeyValueWrite>> all = overlay.Collect("k/", null, int.MaxValue, null);
        Assert.Equal(keys, all.Select(static kv => kv.Key).ToList());
    }

    [Fact]
    public async Task Collect_UnderConcurrentRecordAndRemove_StaysOrderedUniqueAndComplete()
    {
        UnflushedKeyValueWritesIndex overlay = new();

        // A stable population that is never touched: every read must contain all of it that fits.
        List<string> stable = Enumerable.Range(0, 500).Select(static i => $"s/{i:D3}").ToList();
        foreach (string key in stable)
            overlay.Record(key, [1], 1, default, default, Ts(1), KeyValueState.Set, false);

        using CancellationTokenSource cts = new();

        Task writer = Task.Run(() =>
        {
            Random random = new(11);
            long revision = 1;
            while (!cts.IsCancellationRequested)
            {
                string key = $"v/{random.Next(300):D3}";
                revision++;
                overlay.Record(key, [2], revision, default, default, Ts(revision), KeyValueState.Set, false);
                if (random.Next(2) == 0)
                    overlay.RemoveFlushed(key, revision, Ts(revision));
            }
        }, TestContext.Current.CancellationToken);

        for (int i = 0; i < 2_000; i++)
        {
            List<KeyValuePair<string, UnflushedKeyValueWrite>> unbounded = overlay.Collect("s/", null, int.MaxValue, null);
            Assert.Equal(stable, unbounded.Select(static kv => kv.Key).ToList());

            List<KeyValuePair<string, UnflushedKeyValueWrite>> bounded = overlay.Collect("", null, 100, null);
            Assert.True(bounded.Count <= 100);
            for (int k = 1; k < bounded.Count; k++)
                Assert.True(string.CompareOrdinal(bounded[k - 1].Key, bounded[k].Key) < 0, "bounded selection must be strictly ascending");

            // Every stable key that sorts at or below the page's last key was present for the whole read, so the
            // bounded selection must contain it — a bounded read never loses an entry it visited.
            if (bounded.Count > 0)
            {
                string last = bounded[^1].Key;
                HashSet<string> returned = bounded.Select(static kv => kv.Key).ToHashSet(StringComparer.Ordinal);
                foreach (string key in stable)
                {
                    if (string.CompareOrdinal(key, last) <= 0)
                        Assert.Contains(key, returned);
                }
            }
        }

        cts.Cancel();
        await writer;
    }

    [Fact]
    public void FailedFlush_KeepsQueuedHeadsVisibleToTheScan()
    {
        using Fixture f = new();
        f.Record("k/10", [2], 2, Ts(20), KeyValueState.Set);

        // A flush the inner backend refuses must not prune the overlay: the scan still sees the queued head.
        using RefusingBackend refusing = new();
        using UnflushedOverlayPersistenceBackend backend = new(refusing, f.Overlay, new UnflushedLockWritesIndex());
        Assert.False(backend.StoreKeyValues([new PersistenceRequestItem("k/10", [2], 2, 0, 0, 0, 0, 0, 0, 0, 20, 0, (int)KeyValueState.Set)]));

        List<(string, ReadOnlyKeyValueEntry)> page = backend.GetKeyValueByRange("k/", null, 10);
        Assert.Single(page);
        Assert.Equal("k/10", page[0].Item1);
        Assert.Equal(2, page[0].Item2.Revision);
    }

    /// <summary>A memory backend whose key-value flush always fails, so the overlay must keep covering the rows.</summary>
    private sealed class RefusingBackend : IPersistenceBackend, IDisposable
    {
        private readonly IPersistenceBackend inner = new MemoryPersistenceBackend();

        public void Dispose() => (inner as IDisposable)?.Dispose();

        public bool StoreKeyValues(List<PersistenceRequestItem> items) => false;

        public bool StoreLocks(List<PersistenceRequestItem> items) => inner.StoreLocks(items);

        public Kahuna.Server.Locks.Data.LockEntry? GetLock(string resource) => inner.GetLock(resource);

        public KeyValueEntry? GetKeyValue(string keyName) => inner.GetKeyValue(keyName);

        public KeyValueEntry?[] GetKeyValues(string[] keyNames) => inner.GetKeyValues(keyNames);

        public KeyValueEntry? GetKeyValueRevision(string keyName, long revision) => inner.GetKeyValueRevision(keyName, revision);

        public KeyValueHydration GetKeyValueWithRecentRevisions(string keyName, int recentRevisions) => inner.GetKeyValueWithRecentRevisions(keyName, recentRevisions);

        public KeyValueEntry? GetKeyValueRevisionAtOrBefore(string keyName, long maxRevision, HLCTimestamp readTimestamp) => inner.GetKeyValueRevisionAtOrBefore(keyName, maxRevision, readTimestamp);

        public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByPrefix(string prefixKeyName) => inner.GetKeyValueByPrefix(prefixKeyName);

        public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByRange(string prefix, string? startKey, int limit) => inner.GetKeyValueByRange(prefix, startKey, limit);

        public KeyValueScanPage ScanKeyValues(string? cursor, int limit) => inner.ScanKeyValues(cursor, limit);

        public LockScanPage ScanLocks(string? cursor, int limit) => inner.ScanLocks(cursor, limit);

        public bool DeleteKeyValues(IReadOnlyList<string> keys) => inner.DeleteKeyValues(keys);

        public bool DeleteLocks(IReadOnlyList<string> resources) => inner.DeleteLocks(resources);

        public bool PruneKeyValueRevisions(IReadOnlyCollection<string>? keys, int retentionCount, TimeSpan retentionAge, int batchSize, HLCTimestamp floorTimestamp, out RevisionPruneResult result) =>
            inner.PruneKeyValueRevisions(keys, retentionCount, retentionAge, batchSize, floorTimestamp, out result);

        public CheckpointResult CreateCheckpoint(string destinationPath, long appliedIndex, HLCTimestamp appliedTime) => inner.CreateCheckpoint(destinationPath, appliedIndex, appliedTime);

        public CheckpointResult CreateCheckpointAsOf(string destinationPath, long appliedIndex, HLCTimestamp cut, CancellationToken ct = default) => inner.CreateCheckpointAsOf(destinationPath, appliedIndex, cut, ct);

        public bool SupportsExactAsOfCheckpoint => inner.SupportsExactAsOfCheckpoint;

        public HLCTimestamp GetPrunedHistoryFloor() => inner.GetPrunedHistoryFloor();
    }
}
