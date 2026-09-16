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

    private static PreparedIntent Intent(string key, string bucket, long txn) =>
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
            Resolution: PreparedIntentResolution.Committed);

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
        Task writer = Task.Run(() =>
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
        }, TestContext.Current.CancellationToken);

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
}
