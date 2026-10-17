using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// The scan-window and bucket captures of <see cref="PreparedIntentStore"/> enumerate the live intent map without a
/// locked copy. These tests pin the property the scan merge relies on: an intent present for the whole capture is
/// returned exactly once and within the requested bounds, while concurrent prepares and purges of other keys —
/// including the table growth they trigger — neither hide it, duplicate it, nor leak an out-of-window key in.
/// </summary>
public sealed class TestPreparedIntentWindowCapture
{
    private static HLCTimestamp Ts(long l) => new(0, l, 0);

    private static PreparedIntent Intent(string key, string bucket, long txn, PreparedIntentResolution resolution = PreparedIntentResolution.Committed) =>
        new(
            TransactionId: Ts(txn),
            Epoch: 1,
            Key: key,
            ManifestHash: 1,
            RecordAnchorKey: key,
            CommitTimestamp: Ts(txn + 100),
            State: KeyValueState.Set,
            Value: [1],
            Bucket: bucket,
            Revision: 1,
            Expires: HLCTimestamp.Zero,
            NoRevision: false,
            BaseRevision: 0,
            BaseState: KeyValueState.Set,
            RecoveryDeadline: Ts(txn + 5000),
            Resolution: resolution);

    private static void AssertExactly(IEnumerable<string> expected, IReadOnlyList<PreparedIntent> captured)
    {
        List<string> keys = captured.Select(static i => i.Key).ToList();
        Assert.Equal(keys.Count, keys.Distinct(StringComparer.Ordinal).Count());
        Assert.Equal(expected.OrderBy(static k => k, StringComparer.Ordinal).ToList(), keys.OrderBy(static k => k, StringComparer.Ordinal).ToList());
    }

    [Fact]
    public async Task ScanWindowAndBucket_UnderConcurrentPreparesAndPurges_ReturnStableIntentsExactlyOnce()
    {
        PreparedIntentStore store = new();

        List<string> stable = Enumerable.Range(0, 2_000).Select(static i => $"s/{i:D4}").ToList();
        foreach (string key in stable)
            Assert.Equal(TransactionApplyOutcome.Applied, store.Apply(new PrepareIntentCommand(Intent(key, "s", 1000))).Outcome);

        using CancellationTokenSource cts = new();

        // Churn on unrelated keys: prepares grow the map (table resizes), purges shrink it, re-prepares replace nodes.
        // A dedicated thread, never the pool: the loop runs unyielding for the whole test and would
        // otherwise hold a pool thread hostage (see TestBackendStorageRecovery for the failure mode).
        Task writer = Task.Factory.StartNew(() =>
        {
            Random random = new(5);
            long txn = 10_000;
            while (!cts.IsCancellationRequested)
            {
                for (int i = 0; i < 64; i++)
                {
                    txn++;
                    store.Apply(new PrepareIntentCommand(Intent($"v/{random.Next(3_000):D4}", "v", txn)));
                }

                int drop = random.Next(3_000);
                store.PurgeWhere(k => k.StartsWith("v/", StringComparison.Ordinal) && k.GetHashCode() % 3 == drop % 3);
            }
        }, TestContext.Current.CancellationToken, TaskCreationOptions.LongRunning, TaskScheduler.Default);

        List<string> window = stable.Skip(100).Take(100).ToList(); // s/0100 .. s/0199 inclusive

        for (int i = 0; i < 1_000; i++)
        {
            AssertExactly(window, store.SnapshotScanWindow("s/0100", startInclusive: true, "s/0199", endInclusive: true));

            AssertExactly(window.Skip(1), store.SnapshotScanWindow("s/0100", startInclusive: false, "s/0199", endInclusive: true));

            IReadOnlyList<PreparedIntent> all = store.SnapshotScanWindow("s/", startInclusive: true, "s/\uffff", endInclusive: false);
            AssertExactly(stable, all);

            IReadOnlyList<PreparedIntent> bucket = store.SnapshotPrefix("s/");
            AssertExactly(stable, bucket);

            // The open window sees the churn too: every stable key exactly once, and nothing that is not a live or
            // just-purged volatile key — never a key from outside the map.
            IReadOnlyList<PreparedIntent> open = store.SnapshotScanWindow(null, true, null, true);
            List<string> openKeys = open.Select(static i => i.Key).ToList();
            Assert.Equal(openKeys.Count, openKeys.Distinct(StringComparer.Ordinal).Count());
            Assert.Equal(stable.Count, openKeys.Count(static k => k.StartsWith("s/", StringComparison.Ordinal)));
            Assert.All(openKeys, static k => Assert.True(k.StartsWith("s/", StringComparison.Ordinal) || k.StartsWith("v/", StringComparison.Ordinal)));
        }

        cts.Cancel();
        await writer;
    }

    [Fact]
    public void ScanWindow_OnAnEmptyStore_IsEmpty()
    {
        PreparedIntentStore store = new();
        Assert.Empty(store.SnapshotScanWindow("a", true, "z", true));
        Assert.Empty(store.SnapshotPrefix("a"));
    }

    /// <summary>
    /// The captures come from an ordered key index kept beside the map. Under random prepares, commits, settlements,
    /// imports and purges, every window and prefix capture must equal the brute-force filter of the full map with
    /// the window's own boundary semantics, in ordinal key order: the index can never name a key the map lacks nor
    /// miss a key the map holds.
    /// </summary>
    [Fact]
    public void ScanWindowAndPrefix_MatchTheFullMap_UnderRandomMutations()
    {
        PreparedIntentStore store = new();
        Random random = new(77);
        long txn = 1;

        // Keys that share prefixes at several depths, so prefix and bound edges fall between related keys.
        List<string> keys = [];
        for (int i = 0; i < 300; i++)
        {
            keys.Add($"k/{i:D4}");
            if (i % 7 == 0)
                keys.Add($"k/{i:D4}/x");
        }

        string RandomBound() => random.Next(6) switch
        {
            0 => "a",
            1 => "z",
            2 => $"k/{random.Next(300):D4}/",
            3 => $"k/{random.Next(30):D2}",
            _ => keys[random.Next(keys.Count)]
        };

        for (int round = 0; round < 200; round++)
        {
            for (int m = 0; m < 25; m++)
            {
                string key = keys[random.Next(keys.Count)];
                PreparedIntent? current = store.Get(key);

                switch (random.Next(5))
                {
                    case 0:
                        store.Apply(new PrepareIntentCommand(Intent(key, "k", ++txn, PreparedIntentResolution.Pending)));
                        break;
                    case 1:
                        if (current is not null)
                            store.Apply(new ResolveIntentCommand(current.TransactionId, current.Epoch, key, random.Next(4) != 0));
                        break;
                    case 2:
                        if (current is not null)
                            store.Apply(new RemoveIntentCommand(current.TransactionId, current.Epoch, key));
                        break;
                    case 3:
                        if (current is null)
                            store.ImportIntents([Intent(key, "k", ++txn)]);
                        break;
                    default:
                        if (current is not null)
                            store.Apply(new PrepareIntentCommand(Intent(key, "k", ++txn, PreparedIntentResolution.Pending)));
                        break;
                }
            }

            if (round % 40 == 39)
            {
                int bucket = random.Next(4);
                store.PurgeWhere(k => (k.Length + k[^1]) % 4 == bucket);
            }

            List<PreparedIntent> all = store.Snapshot().OrderBy(static i => i.Key, StringComparer.Ordinal).ToList();
            Assert.Equal(all.Count, store.LiveIntentCount);

            AssertSameSequence(all, store.SnapshotScanWindow(null, true, null, true));

            for (int q = 0; q < 8; q++)
            {
                string? start = random.Next(5) == 0 ? null : RandomBound();
                string? end = random.Next(5) == 0 ? null : RandomBound();
                bool startInclusive = random.Next(2) == 0;
                bool endInclusive = random.Next(2) == 0;

                List<PreparedIntent> expected = all.Where(i =>
                    (start is null || string.CompareOrdinal(i.Key, start) > 0 || (startInclusive && string.CompareOrdinal(i.Key, start) == 0))
                    && (end is null || string.CompareOrdinal(i.Key, end) < 0 || (endInclusive && string.CompareOrdinal(i.Key, end) == 0))).ToList();

                AssertSameSequence(expected, store.SnapshotScanWindow(start, startInclusive, end, endInclusive));
            }

            string prefix = random.Next(4) switch { 0 => "k/", 1 => "", 2 => $"k/{random.Next(30):D2}", _ => keys[random.Next(keys.Count)] };
            AssertSameSequence(all.Where(i => i.Key.StartsWith(prefix, StringComparison.Ordinal)).ToList(), store.SnapshotPrefix(prefix));
        }

        Assert.True(store.LiveIntentCount > 0, "the random walk should leave some intents live");
    }

    private static void AssertSameSequence(List<PreparedIntent> expected, IReadOnlyList<PreparedIntent> actual)
    {
        Assert.Equal(expected.Select(static i => i.Key).ToList(), actual.Select(static i => i.Key).ToList());
        for (int i = 0; i < expected.Count; i++)
            Assert.Same(expected[i], actual[i]);
    }

    /// <summary>Bounds outside the keys, reversed bounds and exclusive bounds on present keys answer the window
    /// without an exception; the prefix capture takes the empty prefix, a prefix past every key and a full key.</summary>
    [Fact]
    public void ScanWindowAndPrefix_EdgeBounds_AnswerWithoutThrowing()
    {
        PreparedIntentStore store = new();
        foreach (string key in new[] { "m/1", "m/2", "m/3" })
            Assert.Equal(TransactionApplyOutcome.Applied, store.Apply(new PrepareIntentCommand(Intent(key, "m", 10))).Outcome);

        static List<string> Keys(IReadOnlyList<PreparedIntent> intents) => intents.Select(static i => i.Key).ToList();

        Assert.Empty(store.SnapshotScanWindow("z", true, null, true));
        Assert.Empty(store.SnapshotScanWindow(null, true, "a", true));
        Assert.Empty(store.SnapshotScanWindow("m/2", true, "m/1", true));
        Assert.Empty(store.SnapshotScanWindow("m/3", false, "m/3", true));
        Assert.Empty(store.SnapshotScanWindow("m/2", false, "m/3", false));
        Assert.Equal(["m/3"], Keys(store.SnapshotScanWindow("m/3", true, "m/3", true)));
        Assert.Equal(["m/1"], Keys(store.SnapshotScanWindow(null, true, "m/2", false)));
        Assert.Equal(["m/2", "m/3"], Keys(store.SnapshotScanWindow("m/1", false, null, true)));
        Assert.Equal(["m/1", "m/2", "m/3"], Keys(store.SnapshotScanWindow("a", false, "z", false)));

        Assert.Equal(["m/1", "m/2", "m/3"], Keys(store.SnapshotPrefix("")));
        Assert.Equal(["m/1", "m/2", "m/3"], Keys(store.SnapshotPrefix("m/")));
        Assert.Equal(["m/2"], Keys(store.SnapshotPrefix("m/2")));
        Assert.Empty(store.SnapshotPrefix("m/4"));
        Assert.Empty(store.SnapshotPrefix("n"));
    }

    /// <summary>
    /// A store that held a bulk load's intents and settled every one of them: the map is empty, and the captures
    /// answer empty from the index count. The map's bucket table never shrinks, so the old whole-map walk kept
    /// visiting every empty bucket per page; the index does not visit the map at all.
    /// </summary>
    [Fact]
    public void ScanWindow_AfterThousandsOfIntentsSettled_IsEmpty()
    {
        PreparedIntentStore store = new();
        const int count = 20_000;

        for (int i = 0; i < count; i++)
            Assert.Equal(TransactionApplyOutcome.Applied, store.Apply(new PrepareIntentCommand(Intent($"k/{i:D6}", "k", 100 + i, PreparedIntentResolution.Pending))).Outcome);

        Assert.Equal(count, store.LiveIntentCount);
        Assert.Equal(count, store.SnapshotScanWindow(null, true, null, true).Count);

        for (int i = 0; i < count; i++)
        {
            Assert.Equal(TransactionApplyOutcome.Applied, store.Apply(new ResolveIntentCommand(Ts(100 + i), 1, $"k/{i:D6}", true)).Outcome);
            Assert.Equal(TransactionApplyOutcome.Applied, store.Apply(new RemoveIntentCommand(Ts(100 + i), 1, $"k/{i:D6}")).Outcome);
        }

        Assert.Equal(0, store.LiveIntentCount);
        Assert.Empty(store.Snapshot());
        Assert.Empty(store.SnapshotScanWindow(null, true, null, true));
        Assert.Empty(store.SnapshotScanWindow("k/000000", true, "k/999999", true));
        Assert.Empty(store.SnapshotPrefix("k/"));
    }
}
