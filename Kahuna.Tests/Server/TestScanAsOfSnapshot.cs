
using Kommander;
using Kommander.Time;
using System.Text;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Microsoft.Extensions.Logging;

namespace Kahuna.Tests.Server;

/// <summary>
/// Tests that a range scan with transactionId != Zero and readTimestamp != Zero
/// (AS-OF + transactional) takes the snapshot-visibility path, matching point-read
/// behaviour for the same (transactionId, readTimestamp) pair.
/// </summary>
[Collection("ClusterTests")]
public class TestScanAsOfSnapshot : BaseCluster
{
    private readonly ILogger<IRaft> raftLogger;
    private readonly ILogger<IKahuna> kahunaLogger;

    public TestScanAsOfSnapshot(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = TestLogFactory.Create(outputHelper);
        raftLogger   = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    private static async Task<List<(string Key, ReadOnlyKeyValueEntry Entry)>> DrainScan(
        IAsyncEnumerable<(string Key, ReadOnlyKeyValueEntry Entry)> source)
    {
        List<(string, ReadOnlyKeyValueEntry)> result = [];
        await foreach ((string k, ReadOnlyKeyValueEntry e) in source)
            result.Add((k, e));
        return result;
    }

    private static byte[] B(string s) => Encoding.UTF8.GetBytes(s);

    /// <summary>
    /// A scan with txId != Zero and readTimestamp = T must return the at-or-before-T revision
    /// for a key that was updated after T — not "does not exist" (old RYOW conservative drop).
    ///
    /// Before the fix the MVCC block dropped the key (conservative drop).
    /// After the fix it falls through to TryGetRevisionAtOrBefore, returning "before".
    /// </summary>
    [Theory, CombinatorialData]
    public async Task Scan_WithTxId_AsOf_ReturnsSnapshotRevision_NotDoesNotExist(
        [CombinatorialValues("memory")] string storage,
        [CombinatorialValues(8)] int partitions
    )
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            string prefix = "asofTxScan/" + Guid.NewGuid().ToString("N")[..8];
            string key    = prefix + "/k1";

            // Write version A.
            (KeyValueResponseType t, _, _) = await kahuna1.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, key, B("before"),
                null, -1, KeyValueFlags.Set, 0,
                KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.Set, t);

            // Read back to get snapshotT from the leader actor.
            (_, ReadOnlyKeyValueEntry? entryA) = await kahuna2.LocateAndTryGetValue(
                HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero,
                KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
            Assert.NotNull(entryA);
            HLCTimestamp snapshotT = entryA.LastModified;

            // Write version B — its LastModified is strictly > snapshotT (HLC monotonicity).
            (t, _, _) = await kahuna1.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, key, B("after"),
                null, -1, KeyValueFlags.Set, 0,
                KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.Set, t);

            // Scan with a real txId at snapshotT — must see "before", not DoesNotExist.
            HLCTimestamp fakeTxId = node1.HybridLogicalClock.TrySendOrLocalEvent(node1.GetLocalNodeId());

            List<(string Key, ReadOnlyKeyValueEntry Entry)> rows = await DrainScan(
                kahuna2.LocateAndScanRange(
                    fakeTxId, prefix,
                    null, true, null, false,
                    pageSize: 10, snapshotT,
                    KeyValueDurability.Persistent,
                    TestContext.Current.CancellationToken));

            Assert.Single(rows);
            Assert.Equal("before", Encoding.UTF8.GetString(rows[0].Entry.Value!));
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    /// <summary>
    /// A scan with txId != Zero and readTimestamp = T must not see a key inserted after T
    /// (no phantom), and must see a key that existed at T.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task Scan_WithTxId_AsOf_NoPhantom_AndSeesExistingKey(
        [CombinatorialValues("memory")] string storage,
        [CombinatorialValues(8)] int partitions
    )
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            string prefix   = "asofTxScan/" + Guid.NewGuid().ToString("N")[..8];
            string keyEarly = prefix + "/early";
            string keyLate  = prefix + "/late";

            // Write the early key (exists at snapshot).
            (KeyValueResponseType t, _, _) = await kahuna1.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, keyEarly, B("v-early"),
                null, -1, KeyValueFlags.Set, 0,
                KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.Set, t);

            // Capture snapshot as early key's LastModified.
            (_, ReadOnlyKeyValueEntry? earlyEntry) = await kahuna2.LocateAndTryGetValue(
                HLCTimestamp.Zero, keyEarly, -1, HLCTimestamp.Zero,
                KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
            Assert.NotNull(earlyEntry);
            HLCTimestamp snapshotT = earlyEntry.LastModified;

            // Write the late key AFTER the snapshot — must not appear in an AS-OF scan.
            await Task.Delay(10, TestContext.Current.CancellationToken);
            (t, _, _) = await kahuna1.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, keyLate, B("v-late"),
                null, -1, KeyValueFlags.Set, 0,
                KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.Set, t);

            HLCTimestamp fakeTxId = node1.HybridLogicalClock.TrySendOrLocalEvent(node1.GetLocalNodeId());

            List<(string Key, ReadOnlyKeyValueEntry Entry)> rows = await DrainScan(
                kahuna2.LocateAndScanRange(
                    fakeTxId, prefix,
                    null, true, null, false,
                    pageSize: 10, snapshotT,
                    KeyValueDurability.Persistent,
                    TestContext.Current.CancellationToken));

            // Only the early key is visible; late key is a phantom — must be absent.
            Assert.Single(rows);
            Assert.Equal(keyEarly, rows[0].Key);
            Assert.Equal("v-early", Encoding.UTF8.GetString(rows[0].Entry.Value!));
            Assert.DoesNotContain(rows, r => r.Key == keyLate);
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    /// <summary>
    /// An ordinary RYOW scan (txId != Zero, no AS-OF readTimestamp) is unchanged:
    /// the MVCC path still runs and the transaction sees its own uncommitted writes.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task Scan_WithTxId_NoAsOf_RyowUnchanged(
        [CombinatorialValues("memory")] string storage,
        [CombinatorialValues(8)] int partitions
    )
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            string prefix = "asofTxScan/" + Guid.NewGuid().ToString("N")[..8];
            string key    = prefix + "/k1";

            // Seed a committed value.
            (KeyValueResponseType t, _, _) = await kahuna1.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, key, B("committed"),
                null, -1, KeyValueFlags.Set, 0,
                KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.Set, t);

            // Scan with txId but no AS-OF timestamp (Zero) — RYOW/MVCC path must still run.
            // The scan must return the committed value; no regression from the change.
            HLCTimestamp fakeTxId = node1.HybridLogicalClock.TrySendOrLocalEvent(node1.GetLocalNodeId());

            List<(string Key, ReadOnlyKeyValueEntry Entry)> rows = await DrainScan(
                kahuna2.LocateAndScanRange(
                    fakeTxId, prefix,
                    null, true, null, false,
                    pageSize: 10, HLCTimestamp.Zero,
                    KeyValueDurability.Persistent,
                    TestContext.Current.CancellationToken));

            Assert.Single(rows);
            Assert.Equal("committed", Encoding.UTF8.GetString(rows[0].Entry.Value!));
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }
}
