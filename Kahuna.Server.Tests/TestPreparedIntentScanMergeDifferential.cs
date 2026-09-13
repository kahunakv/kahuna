using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// Differential tests for <see cref="PreparedIntentScanMerge.Merge"/>: the ordered two-way merge must produce
/// exactly what the previous tree-rebuilding union produced — items, versions, retry verdict, has-more flag and
/// cursor — over randomized pages and intent sets that cover every visibility outcome, pagination shape and
/// caller-supplied predicate. The reference implementation below is that previous union, kept verbatim as the
/// oracle.
/// </summary>
public sealed class TestPreparedIntentScanMergeDifferential
{
    private static HLCTimestamp Ts(long l) => new(0, l, 0);

    private static readonly HLCTimestamp Tc = Ts(1000);

    private static PreparedIntent Intent(string key, PreparedIntentResolution resolution, KeyValueState state, byte[]? value, HLCTimestamp expires, long revision) =>
        new(Ts(500), 1, key, 0, key, CommitTimestamp: Tc,
            State: state, Value: value, Bucket: null, Revision: revision, Expires: expires,
            NoRevision: false, BaseRevision: 0, BaseState: KeyValueState.Set, RecoveryDeadline: Ts(9000),
            Resolution: resolution);

    // ── reference implementation (the previous tree-rebuilding union) ──────────────

    private static PreparedIntentScanMerge.ScanMergeResult ReferenceMerge(
        List<(string Key, ReadOnlyKeyValueEntry Entry)> items,
        IReadOnlyList<PreparedIntent> intents,
        HLCTimestamp snapshotTs,
        HLCTimestamp currentTime,
        int limit,
        bool kvHasMore,
        string? kvCeilingKey,
        Func<PreparedIntent, TransactionDecision>? decisionLookup,
        Func<string, bool>? readerHasOwnVersion)
    {
        Dictionary<string, PreparedIntent> overrides = [];
        HashSet<string> excludes = [];

        foreach (PreparedIntent intent in intents)
        {
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
                    if (intent.State == KeyValueState.Deleted || PreparedIntentVisibility.IsExpired(intent, currentTime))
                        excludes.Add(intent.Key);
                    else
                        overrides[intent.Key] = intent;
                    break;

                default:
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

    // ── randomized inputs ─────────────────────────────────────────────────────

    private static readonly string[] Universe = Enumerable.Range(0, 30).Select(static i => $"k/{i:D2}").ToArray();

    private static readonly HLCTimestamp[] SnapshotChoices = [HLCTimestamp.Zero, Ts(999), Ts(1000), Ts(2000)];

    private static readonly HLCTimestamp[] NowChoices = [default, Ts(1200), Ts(2000)];

    private static readonly int[] LimitChoices = [1, 2, 3, 5, 10, 100, int.MaxValue];

    private static (List<(string Key, ReadOnlyKeyValueEntry Entry)> Items, List<PreparedIntent> Intents) Generate(Random random)
    {
        List<(string Key, ReadOnlyKeyValueEntry Entry)> items = [];
        foreach (string key in Universe)
        {
            if (random.Next(3) == 0)
                continue;
            byte[] value = new byte[random.Next(3)];
            random.NextBytes(value);
            items.Add((key, new ReadOnlyKeyValueEntry(value, random.Next(1, 4), default, default, Ts(random.Next(1, 3000)), KeyValueState.Set)));
        }

        List<PreparedIntent> intents = [];
        HashSet<string> used = [];
        int count = random.Next(0, 9);
        for (int i = 0; i < count; i++)
        {
            string key = Universe[random.Next(Universe.Length)];
            if (!used.Add(key))
                continue;

            PreparedIntentResolution resolution = random.Next(4) switch
            {
                0 => PreparedIntentResolution.Pending,
                1 => PreparedIntentResolution.Aborted,
                _ => PreparedIntentResolution.Committed,
            };
            KeyValueState state = random.Next(4) == 0 ? KeyValueState.Deleted : KeyValueState.Set;
            byte[]? value = state == KeyValueState.Deleted && random.Next(2) == 0 ? null : [(byte)random.Next(256)];
            HLCTimestamp expires = random.Next(3) == 0 ? Ts(1500) : default;

            intents.Add(Intent(key, resolution, state, value, expires, random.Next(1, 9)));
        }

        // Intents arrive in dictionary order from the store, which is unrelated to key order.
        intents = intents.OrderBy(_ => random.Next()).ToList();

        return (items, intents);
    }

    private static void AssertSameResult(PreparedIntentScanMerge.ScanMergeResult expected, PreparedIntentScanMerge.ScanMergeResult actual)
    {
        Assert.Equal(expected.MustRetry, actual.MustRetry);
        Assert.Equal(expected.HasMore, actual.HasMore);
        Assert.Equal(expected.NextCursorKey, actual.NextCursorKey);
        Assert.Equal(expected.Items.Select(static i => i.Key).ToList(), actual.Items.Select(static i => i.Key).ToList());

        for (int i = 0; i < expected.Items.Count; i++)
        {
            ReadOnlyKeyValueEntry e = expected.Items[i].Entry;
            ReadOnlyKeyValueEntry a = actual.Items[i].Entry;
            Assert.True(e.Value is null ? a.Value is null : a.Value is not null && e.Value.AsSpan().SequenceEqual(a.Value));
            Assert.Equal(e.Revision, a.Revision);
            Assert.Equal(e.Expires, a.Expires);
            Assert.Equal(e.LastUsed, a.LastUsed);
            Assert.Equal(e.LastModified, a.LastModified);
            Assert.Equal(e.State, a.State);
        }
    }

    [Fact]
    public void OrderedMerge_MatchesTreeUnion_OnRandomizedPagesAndIntents()
    {
        for (int seed = 0; seed < 3_000; seed++)
        {
            Random random = new(seed);
            (List<(string Key, ReadOnlyKeyValueEntry Entry)> items, List<PreparedIntent> intents) = Generate(random);

            HLCTimestamp snapshotTs = SnapshotChoices[random.Next(SnapshotChoices.Length)];
            HLCTimestamp now = NowChoices[random.Next(NowChoices.Length)];
            int limit = LimitChoices[random.Next(LimitChoices.Length)];
            bool kvHasMore = random.Next(2) == 0;
            string? ceiling = random.Next(3) switch
            {
                0 => null,
                1 => items.Count > 0 ? items[^1].Key : null,
                _ => "k/99",
            };

            Func<PreparedIntent, TransactionDecision>? decisionLookup = random.Next(2) == 0
                ? null
                : i => (TransactionDecision)(i.Key[^1] % 3);
            Func<string, bool>? readerHasOwnVersion = random.Next(2) == 0
                ? null
                : k => k[^1] % 2 == 0;

            PreparedIntentScanMerge.ScanMergeResult expected = ReferenceMerge(
                items, intents, snapshotTs, now, limit, kvHasMore, ceiling, decisionLookup, readerHasOwnVersion);
            PreparedIntentScanMerge.ScanMergeResult actual = PreparedIntentScanMerge.Merge(
                items, intents, snapshotTs, now, limit, kvHasMore, ceiling, decisionLookup, readerHasOwnVersion);

            AssertSameResult(expected, actual);
        }
    }

    [Fact]
    public void EmptyPage_WithIntentsOnly_InjectsInOrder_AndCapsAtLimit()
    {
        List<(string Key, ReadOnlyKeyValueEntry Entry)> items = [];
        PreparedIntent[] intents =
        [
            Intent("k/03", PreparedIntentResolution.Committed, KeyValueState.Set, [3], default, 1),
            Intent("k/01", PreparedIntentResolution.Committed, KeyValueState.Set, [1], default, 1),
            Intent("k/02", PreparedIntentResolution.Committed, KeyValueState.Set, [2], default, 1),
        ];

        PreparedIntentScanMerge.ScanMergeResult result = PreparedIntentScanMerge.Merge(
            items, intents, HLCTimestamp.Zero, default, limit: 2, kvHasMore: false, kvCeilingKey: null);

        Assert.Equal(["k/01", "k/02"], result.Items.Select(static i => i.Key).ToList());
        Assert.True(result.HasMore);
        Assert.Equal("k/02", result.NextCursorKey);
    }

    [Fact]
    public void EmptyOutput_WithMoreKvRows_ResumesAfterTheKvCeiling()
    {
        // Every row on the page is a committed delete: the merged page is empty, but the KV side has more, so the
        // scan must advance past the ceiling rather than report end-of-scan.
        List<(string Key, ReadOnlyKeyValueEntry Entry)> items =
        [
            ("k/01", new ReadOnlyKeyValueEntry([1], 1, default, default, default, KeyValueState.Set)),
            ("k/02", new ReadOnlyKeyValueEntry([1], 1, default, default, default, KeyValueState.Set)),
        ];
        PreparedIntent[] intents =
        [
            Intent("k/01", PreparedIntentResolution.Committed, KeyValueState.Deleted, null, default, 2),
            Intent("k/02", PreparedIntentResolution.Committed, KeyValueState.Deleted, null, default, 2),
        ];

        PreparedIntentScanMerge.ScanMergeResult result = PreparedIntentScanMerge.Merge(
            items, intents, HLCTimestamp.Zero, default, limit: 5, kvHasMore: true, kvCeilingKey: "k/02");

        Assert.Empty(result.Items);
        Assert.True(result.HasMore);
        Assert.Equal("k/02", result.NextCursorKey);
    }

    [Fact]
    public void MisorderedPage_IsRestoredBeforeTheMerge()
    {
        // The producers sort their pages; if one ever does not, the merge must still be exact rather than emit a
        // page whose order or membership depends on the input order.
        List<(string Key, ReadOnlyKeyValueEntry Entry)> misordered =
        [
            ("k/03", new ReadOnlyKeyValueEntry([3], 1, default, default, default, KeyValueState.Set)),
            ("k/01", new ReadOnlyKeyValueEntry([1], 1, default, default, default, KeyValueState.Set)),
            ("k/02", new ReadOnlyKeyValueEntry([2], 1, default, default, default, KeyValueState.Set)),
        ];
        PreparedIntent[] intents = [Intent("k/02", PreparedIntentResolution.Committed, KeyValueState.Set, [9], default, 2)];

        List<(string Key, ReadOnlyKeyValueEntry Entry)> sorted = misordered.OrderBy(static i => i.Key, StringComparer.Ordinal).ToList();
        PreparedIntentScanMerge.ScanMergeResult expected = ReferenceMerge(sorted, intents, HLCTimestamp.Zero, default, 10, false, null, null, null);
        PreparedIntentScanMerge.ScanMergeResult actual = PreparedIntentScanMerge.Merge(misordered, intents, HLCTimestamp.Zero, default, 10, false, null);

        AssertSameResult(expected, actual);
    }
}
