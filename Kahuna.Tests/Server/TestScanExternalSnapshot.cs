
using Kommander;
using Kommander.Time;
using System.Text;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Microsoft.Extensions.Logging;

namespace Kahuna.Tests.Server;

/// <summary>
/// Tests for LocateAndScanRange pinned to an external readTimestamp.
/// </summary>
public class TestScanExternalSnapshot : BaseCluster
{
    private readonly ILogger<IRaft> raftLogger;
    private readonly ILogger<IKahuna> kahunaLogger;

    public TestScanExternalSnapshot(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = TestLogFactory.Create(outputHelper);
        raftLogger   = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    private static async Task<HLCTimestamp> SeedAndGetLastModified(
        IKahuna node, string prefix, int count, KeyValueDurability durability,
        CancellationToken ct)
    {
        HLCTimestamp lastModified = HLCTimestamp.Zero;

        for (int i = 0; i < count; i++)
        {
            string key = $"{prefix}/{i:D4}";
            (KeyValueResponseType t, _, _) = await node.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, key,
                Encoding.UTF8.GetBytes("v" + i),
                null, -1, KeyValueFlags.Set, 0, durability, ct);
            Assert.Equal(KeyValueResponseType.Set, t);

            // Read back to get this key's LastModified from the leader actor.
            (_, ReadOnlyKeyValueEntry? entry) = await node.LocateAndTryGetValue(
                HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, durability, ct);
            if (entry is not null)
                lastModified = entry.LastModified;
        }

        return lastModified;
    }

    private static async Task<List<string>> DrainKeys(
        IAsyncEnumerable<(string Key, ReadOnlyKeyValueEntry Entry)> source)
    {
        List<string> keys = [];
        await foreach ((string k, ReadOnlyKeyValueEntry _) in source)
            keys.Add(k);
        return keys;
    }

    /// <summary>
    /// Two scans of the same range at the same T, with a committed write in between,
    /// return identical rows — the snapshot is truly pinned to T.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task TwoScansAtSameSnapshot_ReturnIdenticalRows(
        [CombinatorialValues("memory")] string storage,
        [CombinatorialValues(8)] int partitions
    )
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            const string prefix = "k2snap/twoScans";

            // Seed 5 keys and capture snapshotT as the LastModified of the last written key.
            // Since the same actor stamps all writes, any subsequent write has a strictly
            // larger LastModified (HLC monotonicity), so snapshotT is a reliable boundary.
            HLCTimestamp snapshotT = await SeedAndGetLastModified(
                kahuna1, prefix, 5, KeyValueDurability.Persistent,
                TestContext.Current.CancellationToken);

            // First scan at snapshotT — baseline.
            List<string> scan1 = await DrainKeys(kahuna2.LocateAndScanRange(
                HLCTimestamp.Zero, prefix,
                null, true, null, false,
                pageSize: 3, snapshotT, KeyValueDurability.Persistent,
                TestContext.Current.CancellationToken));

            // Write one more key AFTER the snapshot. This key must not appear in snapshot scans.
            (KeyValueResponseType setType, _, _) = await kahuna1.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, $"{prefix}/extra",
                Encoding.UTF8.GetBytes("extra"),
                null, -1, KeyValueFlags.Set, 0,
                KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.Set, setType);

            // Second scan at the same snapshotT — must return identical rows.
            List<string> scan2 = await DrainKeys(kahuna2.LocateAndScanRange(
                HLCTimestamp.Zero, prefix,
                null, true, null, false,
                pageSize: 3, snapshotT, KeyValueDurability.Persistent,
                TestContext.Current.CancellationToken));

            // Both scans see exactly the same keys — the post-snapshot write is invisible.
            Assert.Equal(scan1, scan2);
            Assert.DoesNotContain($"{prefix}/extra", scan2);

            // Zero scan (latest) must see the extra key.
            List<string> scanLatest = await DrainKeys(kahuna2.LocateAndScanRange(
                HLCTimestamp.Zero, prefix,
                null, true, null, false,
                pageSize: 3, HLCTimestamp.Zero, KeyValueDurability.Persistent,
                TestContext.Current.CancellationToken));

            Assert.Contains($"{prefix}/extra", scanLatest);
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    /// <summary>
    /// A multi-page scan seeded at T stays on T across all pages.
    /// Keys written after T are absent on every page, not just the first.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task MultiPageScan_StaysOnSeededSnapshot(
        [CombinatorialValues("memory")] string storage,
        [CombinatorialValues(8)] int partitions
    )
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            const string prefix = "k2snap/multiPage";
            const int count     = 10;

            // Seed 10 keys; snapshotT = LastModified of the last key.
            HLCTimestamp snapshotT = await SeedAndGetLastModified(
                kahuna1, prefix, count, KeyValueDurability.Persistent,
                TestContext.Current.CancellationToken);

            // Write 3 extra keys after the snapshot.
            for (int i = count; i < count + 3; i++)
            {
                (KeyValueResponseType t, _, _) = await kahuna1.LocateAndTrySetKeyValue(
                    HLCTimestamp.Zero, $"{prefix}/{i:D4}",
                    Encoding.UTF8.GetBytes("late"),
                    null, -1, KeyValueFlags.Set, 0,
                    KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
                Assert.Equal(KeyValueResponseType.Set, t);
            }

            // Small page size (2) forces multiple pages. All pages must stay on snapshotT.
            List<string> snapped = await DrainKeys(kahuna2.LocateAndScanRange(
                HLCTimestamp.Zero, prefix,
                null, true, null, false,
                pageSize: 2, snapshotT, KeyValueDurability.Persistent,
                TestContext.Current.CancellationToken));

            // Exactly the original 10 keys — no late-written key leaks through.
            Assert.Equal(count, snapped.Count);
            for (int i = count; i < count + 3; i++)
                Assert.DoesNotContain($"{prefix}/{i:D4}", snapped);

            // Zero scan sees all 13 keys.
            List<string> latest = await DrainKeys(kahuna2.LocateAndScanRange(
                HLCTimestamp.Zero, prefix,
                null, true, null, false,
                pageSize: 2, HLCTimestamp.Zero, KeyValueDurability.Persistent,
                TestContext.Current.CancellationToken));

            Assert.Equal(count + 3, latest.Count);
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }
}
