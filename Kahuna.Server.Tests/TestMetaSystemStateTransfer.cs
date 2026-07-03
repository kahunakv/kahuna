using Google.Protobuf;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.Replication;
using Kahuna.Server.Replication.Protos;
using Kommander;
using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// Tests for the meta-partition (id 0) whole-state transfer that repairs a node below the meta WAL
/// compaction floor. Because the hold registry replicates deltas, a compacted-past follower cannot
/// rebuild it from surviving log entries; the transfer ships the range map and the hold set together
/// so both state machines are restored atomically. These exercise the public export/import surface
/// directly — no cluster or leader election needed.
/// </summary>
[Collection("ClusterTests")]
public sealed class TestMetaSystemStateTransfer : BaseCluster, IDisposable
{
    private readonly ILogger<IRaft> raftLogger;
    private readonly ILogger<IKahuna> kahunaLogger;
    private readonly string tempDir;

    public TestMetaSystemStateTransfer(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = LoggerFactory.Create(builder =>
            builder.AddXUnit(outputHelper).SetMinimumLevel(LogLevel.Warning));

        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();

        tempDir = Path.Combine(Path.GetTempPath(), "kahuna-metaxfer-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(tempDir);
    }

    public void Dispose()
    {
        try { Directory.Delete(tempDir, recursive: true); } catch { /* best effort */ }
    }

    // An unjoined RaftManager never elects; the transfer + apply seams never touch Raft, so no
    // cluster is needed to exercise serialization and disk fidelity.
    private RaftManager UnjoinedRaft(int port) => new(
        new RaftConfiguration { NodeName = "t", NodeId = 1, Host = "localhost", Port = port, InitialPartitions = 2, EnableQuiescence = false },
        new StaticDiscovery([]),
        new InMemoryWAL(raftLogger),
        new InMemoryCommunication(),
        new HybridLogicalClock(),
        raftLogger);

    private static RaftLog RangeMapLog(params (string keySpace, int partitionId)[] descriptors)
    {
        RangeMapMessage message = new();
        foreach ((string keySpace, int partitionId) in descriptors)
            message.Descriptors.Add(new RangeDescriptorMessage { KeySpace = keySpace, PartitionId = partitionId, Generation = 1 });

        return new RaftLog { LogType = ReplicationTypes.RangeMap, LogData = ReplicationSerializer.Serialize(message) };
    }

    private static RaftLog UpsertHoldLog(params SnapshotHold[] holds)
    {
        SnapshotFloorDeltaMessage delta = new();
        foreach (SnapshotHold hold in holds)
        {
            delta.Entries.Add(new SnapshotFloorDeltaEntry
            {
                Remove = false,
                Hold = new SnapshotHoldMessage
                {
                    HoldId = hold.HoldId,
                    HolderId = hold.HolderId,
                    TimestampNode     = hold.Timestamp.N,
                    TimestampPhysical = hold.Timestamp.L,
                    TimestampCounter  = hold.Timestamp.C,
                    LeaseExpiryNode     = hold.LeaseExpiry.N,
                    LeaseExpiryPhysical = hold.LeaseExpiry.L,
                    LeaseExpiryCounter  = hold.LeaseExpiry.C,
                },
            });
        }
        return new RaftLog { LogType = ReplicationTypes.SnapshotFloor, LogData = ReplicationSerializer.Serialize(delta) };
    }

    [Fact]
    public async Task ExportThenImport_RestoresBothRangeMapAndHolds()
    {
        RaftManager srcRaft = UnjoinedRaft(8301);
        string srcDir = Path.Combine(tempDir, "src");
        Directory.CreateDirectory(srcDir);

        RangeMapStore srcMap = new(srcRaft, srcDir, "src", kahunaLogger);
        SnapshotFloorStore srcFloor = new(srcRaft, srcDir, "src", kahunaLogger);

        srcMap.Replicate(RangeMapStore.MetaPartitionId, RangeMapLog(("t:r", 2), ("t:s", 3)));

        // Lease expiries must be live relative to wall-clock HLC time (the cache is built with the
        // real clock at commit), so derive them from the source node's current HLC.
        HLCTimestamp srcNow = srcRaft.HybridLogicalClock.TrySendOrLocalEvent(srcRaft.GetLocalNodeId());
        HLCTimestamp farExpiry = new(1, srcNow.L + 600_000, 0);
        SnapshotHold h1 = new("hold-1", "client-a", new HLCTimestamp(1, 1000, 0), farExpiry);
        SnapshotHold h2 = new("hold-2", "client-b", new HLCTimestamp(1, 3000, 0), farExpiry);
        srcFloor.Restore(RangeMapStore.MetaPartitionId, UpsertHoldLog(h1, h2));

        MetaSystemStateTransfer srcTransfer = new(srcMap, srcFloor);
        Stream blob = await srcTransfer.ExportPartitionState(RangeMapStore.MetaPartitionId, upToIndex: 5, CancellationToken.None);

        // A fresh node below the floor: both stores start empty on a different disk revision.
        RaftManager dstRaft = UnjoinedRaft(8302);
        string dstDir = Path.Combine(tempDir, "dst");
        Directory.CreateDirectory(dstDir);

        RangeMapStore dstMap = new(dstRaft, dstDir, "dst", kahunaLogger);
        SnapshotFloorStore dstFloor = new(dstRaft, dstDir, "dst", kahunaLogger);
        Assert.Empty(dstFloor.Holds);

        MetaSystemStateTransfer dstTransfer = new(dstMap, dstFloor);
        await dstTransfer.ImportPartitionState(RangeMapStore.MetaPartitionId, blob, CancellationToken.None);

        // Range map restored.
        Assert.Equal(2, dstMap.Current.Find("t:r", "any")?.PartitionId);
        Assert.Equal(3, dstMap.Current.Find("t:s", "any")?.PartitionId);

        // Holds restored with fidelity.
        Assert.Equal(2, dstFloor.Holds.Count);
        Assert.Equal(h1.Timestamp, dstFloor.Holds["hold-1"].Timestamp);
        Assert.Equal("client-a", dstFloor.Holds["hold-1"].HolderId);
        Assert.Equal(h2.LeaseExpiry, dstFloor.Holds["hold-2"].LeaseExpiry);

        // The imported set drives the effective floor to the lowest held timestamp.
        HLCTimestamp dstNow = dstRaft.HybridLogicalClock.TrySendOrLocalEvent(dstRaft.GetLocalNodeId());
        Assert.Equal(h1.Timestamp, dstFloor.GetEffectiveFloor(dstNow));

        srcRaft.Dispose();
        dstRaft.Dispose();
    }

    [Fact]
    public async Task Import_CorruptSubState_LeavesPriorStateIntact()
    {
        RaftManager raft = UnjoinedRaft(8303);
        string dir = Path.Combine(tempDir, "atomic");
        Directory.CreateDirectory(dir);

        RangeMapStore map = new(raft, dir, "a", kahunaLogger);
        SnapshotFloorStore floor = new(raft, dir, "a", kahunaLogger);

        // Prior committed state on the node being (mis-)repaired.
        map.Replicate(RangeMapStore.MetaPartitionId, RangeMapLog(("keep:r", 7)));
        SnapshotHold kept = new("keep-hold", "client-k", new HLCTimestamp(1, 2000, 0), new HLCTimestamp(1, 999_000, 0));
        floor.Restore(RangeMapStore.MetaPartitionId, UpsertHoldLog(kept));

        // A blob with a well-formed range map but a corrupt hold sub-state. The transfer parses and
        // validates both before installing either, so the import must throw and change nothing.
        MetaSystemStateMessage message = new()
        {
            RangeMap = UnsafeByteOperations.UnsafeWrap(map.SerializeState()),
            SnapshotFloor = ByteString.CopyFrom([0xFF, 0xFF, 0xFF, 0xFF]),
        };
        Stream blob = new MemoryStream(ReplicationSerializer.Serialize(message), writable: false);

        MetaSystemStateTransfer transfer = new(map, floor);
        await Assert.ThrowsAnyAsync<Exception>(() =>
            transfer.ImportPartitionState(RangeMapStore.MetaPartitionId, blob, CancellationToken.None));

        // Both prior state machines are intact — no half-applied import.
        Assert.Equal(7, map.Current.Find("keep:r", "any")?.PartitionId);
        Assert.Single(floor.Holds);
        Assert.True(floor.Holds.ContainsKey("keep-hold"));

        raft.Dispose();
    }

    [Fact]
    public void DeltaHolds_SurviveColdRestartFromDisk()
    {
        RaftManager raft = UnjoinedRaft(8304);
        string dir = Path.Combine(tempDir, "coldrestart");
        Directory.CreateDirectory(dir);

        // Deltas applied to a store with a disk path persist the full set to disk on every mutation.
        SnapshotFloorStore store = new(raft, dir, "cr", kahunaLogger);
        SnapshotHold h1 = new("h1", "c1", new HLCTimestamp(1, 1000, 0), new HLCTimestamp(1, 999_000, 0));
        SnapshotHold h2 = new("h2", "c2", new HLCTimestamp(1, 5000, 0), new HLCTimestamp(1, 999_000, 0));
        store.Restore(RangeMapStore.MetaPartitionId, UpsertHoldLog(h1, h2));
        store.Restore(RangeMapStore.MetaPartitionId, UpsertHoldLog(h1 with { LeaseExpiry = new HLCTimestamp(1, 1_500_000, 0) }));

        // A fresh store on the same disk revision reconstructs the complete set without WAL replay.
        SnapshotFloorStore reopened = new(raft, dir, "cr", kahunaLogger);
        Assert.Equal(2, reopened.Holds.Count);
        Assert.Equal(new HLCTimestamp(1, 1_500_000, 0), reopened.Holds["h1"].LeaseExpiry);
        Assert.Equal(h2.Timestamp, reopened.Holds["h2"].Timestamp);

        raft.Dispose();
    }
}
