
using Kommander;
using Kommander.Time;
using System.Text;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Microsoft.Extensions.Logging;

namespace Kahuna.Tests.Server;

public class TestSnapshotPointRead : BaseCluster
{
    private readonly ILogger<IRaft> raftLogger;
    private readonly ILogger<IKahuna> kahunaLogger;

    public TestSnapshotPointRead(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = TestLogFactory.Create(outputHelper);
        raftLogger   = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    /// <summary>
    /// Point read at T (before update) sees the pre-update value;
    /// point read with Zero sees the latest committed value.
    ///
    /// Uses valA's own LastModified as the snapshot: any subsequent write on the same
    /// actor is strictly greater (HLC monotonicity), so this boundary is reliable without
    /// wall-clock sleeps.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task PointRead_AtSnapshot_SeesPreUpdateValue(
        [CombinatorialValues("memory")] string storage,
        [CombinatorialValues(8)] int partitions
    )
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            string key  = Guid.NewGuid().ToString("N")[..10];
            byte[] valA = Encoding.UTF8.GetBytes("before");
            byte[] valB = Encoding.UTF8.GetBytes("after");

            // Write version A.
            (KeyValueResponseType setType, _, _) = await kahuna1.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, key, valA, null, -1, KeyValueFlags.Set, 0,
                KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.Set, setType);

            // Read valA back to get its exact LastModified from the leader's actor.
            // Using entry.LastModified as snapshotT is reliable: valB (written next on
            // the same actor) will always get a strictly larger LastModified (HLC monotonicity).
            (KeyValueResponseType getType, ReadOnlyKeyValueEntry? entryA) =
                await kahuna2.LocateAndTryGetValue(
                    HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero,
                    KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.Get, getType);
            Assert.NotNull(entryA);

            HLCTimestamp snapshotT = entryA.LastModified;

            // Write version B — its LastModified will be > snapshotT.
            (setType, _, _) = await kahuna1.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, key, valB, null, -1, KeyValueFlags.Set, 0,
                KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.Set, setType);

            // Read at snapshotT → must return valA.
            (getType, ReadOnlyKeyValueEntry? snap) =
                await kahuna2.LocateAndTryGetValue(
                    HLCTimestamp.Zero, key, -1, snapshotT,
                    KeyValueDurability.Persistent, TestContext.Current.CancellationToken);

            Assert.Equal(KeyValueResponseType.Get, getType);
            Assert.NotNull(snap);
            Assert.Equal("before", Encoding.UTF8.GetString(snap.Value!));

            // Read with Zero → latest committed → must return valB.
            (getType, ReadOnlyKeyValueEntry? latest) =
                await kahuna2.LocateAndTryGetValue(
                    HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero,
                    KeyValueDurability.Persistent, TestContext.Current.CancellationToken);

            Assert.Equal(KeyValueResponseType.Get, getType);
            Assert.NotNull(latest);
            Assert.Equal("after", Encoding.UTF8.GetString(latest.Value!));
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    /// <summary>
    /// Point read at T of a key that had no committed revision at-or-before T returns DoesNotExist.
    ///
    /// A 10 ms delay between capturing snapshotT and writing guarantees the wall-clock (and thus
    /// all node HLCs) advances past snapshotT before the write's LastModified is stamped.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task PointRead_AtSnapshot_KeyNotYetWritten_DoesNotExist(
        [CombinatorialValues("memory")] string storage,
        [CombinatorialValues(8)] int partitions
    )
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            // Capture snapshotT. The subsequent Task.Delay ensures every node's HLC is
            // >= snapshotT + at least 1 ms before the write, so the write's LastModified
            // is strictly > snapshotT regardless of which node is the partition leader.
            HLCTimestamp snapshotT = node1.HybridLogicalClock.TrySendOrLocalEvent(node1.GetLocalNodeId());

            await Task.Delay(10, TestContext.Current.CancellationToken);

            string key = Guid.NewGuid().ToString("N")[..10];
            byte[] val = Encoding.UTF8.GetBytes("value");

            (KeyValueResponseType setType, _, _) = await kahuna1.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, key, val, null, -1, KeyValueFlags.Set, 0,
                KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.Set, setType);

            // Read at snapshotT → key didn't exist at T → DoesNotExist.
            (KeyValueResponseType getType, _) = await kahuna2.LocateAndTryGetValue(
                HLCTimestamp.Zero, key, -1, snapshotT,
                KeyValueDurability.Persistent, TestContext.Current.CancellationToken);

            Assert.Equal(KeyValueResponseType.DoesNotExist, getType);
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    /// <summary>
    /// txId != Zero + readTimestamp != Zero: AS-OF snapshot path applies — serves the revision
    /// at-or-before readTimestamp (valA), not the current committed state (valB).
    /// When readTimestamp is non-zero the handler treats the read as an AS-OF snapshot regardless
    /// of whether a transaction ID is present; OCC conflict tracking is skipped for snapshot reads.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task PointRead_WithReadTimestamp_ServesSnapshot(
        [CombinatorialValues("memory")] string storage,
        [CombinatorialValues(8)] int partitions
    )
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            string key  = Guid.NewGuid().ToString("N")[..10];
            byte[] valA = Encoding.UTF8.GetBytes("initial");
            byte[] valB = Encoding.UTF8.GetBytes("updated");

            // Write valA.
            (KeyValueResponseType setType, _, _) = await kahuna1.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, key, valA, null, -1, KeyValueFlags.Set, 0,
                KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.Set, setType);

            // Read valA to get snapshotT from the leader's actor.
            (_, ReadOnlyKeyValueEntry? entryA) = await kahuna2.LocateAndTryGetValue(
                HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero,
                KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
            Assert.NotNull(entryA);
            HLCTimestamp snapshotT = entryA.LastModified;

            // Write valB.
            (setType, _, _) = await kahuna1.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, key, valB, null, -1, KeyValueFlags.Set, 0,
                KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.Set, setType);

            // txId != Zero + readTimestamp != Zero: AS-OF snapshot path applies.
            // snapshotT points at valA's commit, so the handler returns valA even though
            // valB is the current committed state.
            HLCTimestamp fakeTxId = node1.HybridLogicalClock.TrySendOrLocalEvent(node1.GetLocalNodeId());

            (KeyValueResponseType getType, ReadOnlyKeyValueEntry? entry) =
                await kahuna2.LocateAndTryGetValue(
                    fakeTxId, key, -1, snapshotT,
                    KeyValueDurability.Persistent, TestContext.Current.CancellationToken);

            Assert.Equal(KeyValueResponseType.Get, getType);
            Assert.NotNull(entry);
            Assert.Equal("initial", Encoding.UTF8.GetString(entry.Value!));
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }
}
