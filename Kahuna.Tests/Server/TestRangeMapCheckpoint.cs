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

namespace Kahuna.Tests.Server;

/// <summary>
/// Durability tests for the range-descriptor map (Task 2c). The map's only durable home is the meta
/// partition, so before Kommander compacts that WAL we persist a full snapshot to disk; on restart
/// the map is reconstructed from disk and refined by replaying the WAL tail. These tests exercise
/// the disk round-trip directly through the <c>Replicate</c> apply seam — no cluster or leader
/// election needed, so they run in milliseconds. The leader-only checkpoint path is covered by the
/// 3-node harness in <see cref="TestRangeMapReplication"/>.
/// </summary>
[Collection("ClusterTests")]
public sealed class TestRangeMapCheckpoint : BaseCluster, IDisposable
{
    private readonly ILogger<IRaft> raftLogger;
    private readonly ILogger<IKahuna> kahunaLogger;
    private readonly string tempDir;

    public TestRangeMapCheckpoint(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = LoggerFactory.Create(builder =>
            builder.AddXUnit(outputHelper).SetMinimumLevel(LogLevel.Warning));

        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();

        tempDir = Path.Combine(Path.GetTempPath(), "kahuna-rangemap-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(tempDir);
    }

    public void Dispose()
    {
        try { Directory.Delete(tempDir, recursive: true); } catch { /* best effort */ }
    }

    /// <summary>
    /// An unjoined RaftManager — cheap to construct and never elects. The durability paths
    /// (<c>Replicate</c> → <c>PersistToDisk</c>, and <c>LoadFromDisk</c> on construction) never touch
    /// Raft, so no cluster is needed to test snapshot persistence.
    /// </summary>
    private RaftManager UnjoinedRaft() => new(
        new RaftConfiguration { NodeName = "t", NodeId = 1, Host = "localhost", Port = 8201, InitialPartitions = 2, EnableQuiescence = false },
        new StaticDiscovery([]),
        new InMemoryWAL(raftLogger),
        new InMemoryCommunication(),
        new HybridLogicalClock(),
        raftLogger);

    /// <summary>Builds a committed meta-partition log entry carrying a full snapshot of the given descriptors.</summary>
    private static RaftLog RangeMapLog(params (string keySpace, int partitionId)[] descriptors)
    {
        RangeMapMessage message = new();
        foreach ((string keySpace, int partitionId) in descriptors)
            message.Descriptors.Add(new RangeDescriptorMessage { KeySpace = keySpace, PartitionId = partitionId, Generation = 1 });

        return new RaftLog { LogType = ReplicationTypes.RangeMap, LogData = ReplicationSerializer.Serialize(message) };
    }

    // ── Snapshot_DurableAcrossFreshStore_WithoutWalReplay ────────────────────────

    [Fact]
    public void Snapshot_DurableAcrossFreshStore_WithoutWalReplay()
    {
        const string revision = "rev-durable";
        RaftManager raft = UnjoinedRaft();

        // Applying a committed meta entry (the follower/restore path) persists the snapshot to disk.
        RangeMapStore store = new(raft, tempDir, revision, kahunaLogger);
        Assert.True(store.Replicate(RangeMapStore.MetaPartitionId, RangeMapLog(("t:r", 2))));

        // A fresh store at the same path/revision rebuilds purely from disk — it never sees a WAL
        // entry. This is exactly what keeps the map alive after the meta WAL is checkpointed +
        // compacted (Task 2c).
        RangeMapStore fresh = new(raft, tempDir, revision, kahunaLogger);

        RangeDescriptor? descriptor = fresh.Current.Find("t:r", "anything");
        Assert.NotNull(descriptor);
        Assert.Equal(2, descriptor!.PartitionId);
    }

    // ── EmptySnapshot_RoundTripsThroughDisk ──────────────────────────────────────

    [Fact]
    public void EmptySnapshot_RoundTripsThroughDisk()
    {
        const string revision = "rev-empty";
        RaftManager raft = UnjoinedRaft();

        RangeMapStore store = new(raft, tempDir, revision, kahunaLogger);
        Assert.True(store.Replicate(RangeMapStore.MetaPartitionId, RangeMapLog(("t:r", 2))));

        // Clear back to empty — the end state of a drop-table / full merge (0-byte snapshot).
        Assert.True(store.Replicate(RangeMapStore.MetaPartitionId, RangeMapLog()));
        Assert.Empty(store.Current.Descriptors);

        RangeMapStore fresh = new(raft, tempDir, revision, kahunaLogger);
        Assert.Empty(fresh.Current.Descriptors);
        Assert.Null(fresh.Current.Find("t:r", "anything"));
    }

    // ── Checkpoint_OnNonLeaderIsNoop ─────────────────────────────────────────────

    [Fact]
    public async Task Checkpoint_OnNonLeaderIsNoop()
    {
        // A store over a raft that never joined a cluster is not the meta leader: checkpoint is a
        // no-op (returns false) and never throws.
        RangeMapStore store = new(UnjoinedRaft(), tempDir, "rev-noleader", kahunaLogger);

        bool checkpointed = await store.CheckpointNowAsync(TestContext.Current.CancellationToken);
        Assert.False(checkpointed);
    }

    // ── Checkpoint_OnLeaderSucceeds ──────────────────────────────────────────────

    [Fact]
    public async Task Checkpoint_OnLeaderSucceeds()
    {
        // The one genuinely cluster-dependent check: a real meta-partition leader can checkpoint.
        // Uses the canonical BaseCluster helper rather than a bespoke harness.
        (IRaft raft1, IRaft raft2, IRaft raft3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster("memory", 2, raftLogger, kahunaLogger);

        try
        {
            CancellationToken ct = TestContext.Current.CancellationToken;
            (IRaft, IKahuna)[] nodes = [(raft1, kahuna1), (raft2, kahuna2), (raft3, kahuna3)];

            KahunaManager leader = (KahunaManager)await MetaLeaderKahuna(nodes, ct);

            Assert.True(await leader.RangeMapStore.MutateAsync(
                _ => [new RangeDescriptor { KeySpace = "t:r", PartitionId = 2, Generation = 1 }], ct));

            Assert.True(await leader.RangeMapStore.CheckpointNowAsync(ct));
        }
        finally
        {
            await LeaveCluster(raft1, raft2, raft3);
        }
    }

    private static async Task<IKahuna> MetaLeaderKahuna((IRaft Raft, IKahuna Kahuna)[] nodes, CancellationToken ct)
    {
        while (true)
        {
            foreach ((IRaft raft, IKahuna kahuna) in nodes)
                if (await raft.AmILeader(RangeMapStore.MetaPartitionId, ct))
                    return kahuna;

            await Task.Delay(50, ct);
        }
    }
}
