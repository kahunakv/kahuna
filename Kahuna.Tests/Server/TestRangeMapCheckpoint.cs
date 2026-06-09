using Kahuna.Server.Communication.Internode;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues.Ranges;
using Kommander;
using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;
using Nixie;

namespace Kahuna.Tests.Server;

/// <summary>
/// Durability + checkpoint tests for the range-descriptor map (Task 2c). The map's only durable
/// home is the meta partition, so before Kommander compacts that WAL we persist a full snapshot to
/// disk. These tests prove the snapshot alone reconstructs the map (the property that makes
/// compaction safe) and that periodic checkpointing is leader-only. A single-node cluster keeps the
/// node a stable leader of the meta partition.
/// </summary>
public sealed class TestRangeMapCheckpoint : IDisposable
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

    private static RangeDescriptor RowRange(int partitionId, long generation = 1) => new()
    {
        KeySpace = "t:r", StartKey = null, EndKey = null, PartitionId = partitionId, Generation = generation
    };

    /// <summary>Brings up a single-node cluster whose KahunaManager persists snapshots under <paramref name="revision"/>.</summary>
    private async Task<(RaftManager Raft, KahunaManager Kahuna)> BuildSingleNode(string revision)
    {
        MemoryInterNodeCommmunication interNode = new();
        InMemoryCommunication comm = new();

        ActorSystem actorSystem = new(logger: raftLogger);

        RaftConfiguration config = new()
        {
            NodeName = "kahuna1",
            NodeId = 1,
            Host = "localhost",
            Port = 8101,
            InitialPartitions = 2, // partition 1 = meta map, partition 2 = data
            StartElectionTimeout = 50,
            EndElectionTimeout = 150
        };

        RaftManager raft = new(
            config,
            new StaticDiscovery([]),
            new InMemoryWAL(raftLogger),
            comm,
            new HybridLogicalClock(),
            raftLogger);

        KahunaConfiguration kahunaConfiguration = new()
        {
            HttpsCertificate = "",
            HttpsCertificatePassword = "",
            LocksWorkers = 4,
            KeyValueWorkers = 4,
            BackgroundWriterWorkers = 1,
            Storage = "memory",
            StoragePath = tempDir,
            StorageRevision = revision,
            DefaultTransactionTimeout = 5000,
            ScriptCacheExpiration = TimeSpan.FromMinutes(1),
        };

        KahunaManager kahuna = new(actorSystem, raft, kahunaConfiguration, interNode, kahunaLogger);

        interNode.SetNodes(new() { { "localhost:8101", kahuna } });
        comm.SetNodes(new() { { "localhost:8101", raft } });

        raft.OnLogRestored += kahuna.OnLogRestored;
        raft.OnReplicationReceived += kahuna.OnReplicationReceived;
        raft.OnReplicationError += kahuna.OnReplicationError;

        await raft.JoinCluster();

        CancellationToken ct = TestContext.Current.CancellationToken;
        for (int partition = 1; partition <= 2; partition++)
            while (!await raft.AmILeader(partition, ct))
                await Task.Delay(50, ct);

        return (raft, kahuna);
    }

    // ── Snapshot_DurableAcrossFreshStore_WithoutWalReplay ────────────────────────

    [Fact]
    public async Task Snapshot_DurableAcrossFreshStore_WithoutWalReplay()
    {
        const string revision = "rev-durable";
        (RaftManager raft, KahunaManager kahuna) = await BuildSingleNode(revision);
        try
        {
            bool committed = await kahuna.RangeMapStore.MutateAsync(
                _ => [RowRange(2)], TestContext.Current.CancellationToken);
            Assert.True(committed);

            // A brand-new store pointed at the same path/revision must reconstruct the map purely
            // from the on-disk snapshot — it never sees a WAL entry. This is exactly what keeps the
            // map alive after the meta WAL is checkpointed + compacted.
            RangeMapStore fresh = new(raft, tempDir, revision, kahunaLogger);

            RangeDescriptor? descriptor = fresh.Current.Find("t:r", "anything");
            Assert.NotNull(descriptor);
            Assert.Equal(2, descriptor!.PartitionId);
        }
        finally
        {
            await raft.LeaveCluster(dispose: true);
        }
    }

    // ── Checkpoint_OnLeaderSucceeds ──────────────────────────────────────────────

    [Fact]
    public async Task Checkpoint_OnLeaderSucceeds()
    {
        (RaftManager raft, KahunaManager kahuna) = await BuildSingleNode("rev-leader");
        try
        {
            await kahuna.RangeMapStore.MutateAsync(_ => [RowRange(2)], TestContext.Current.CancellationToken);

            bool checkpointed = await kahuna.RangeMapStore.CheckpointNowAsync(TestContext.Current.CancellationToken);
            Assert.True(checkpointed);
        }
        finally
        {
            await raft.LeaveCluster(dispose: true);
        }
    }

    // ── Checkpoint_OnNonLeaderIsNoop ─────────────────────────────────────────────

    [Fact]
    public async Task Checkpoint_OnNonLeaderIsNoop()
    {
        // A store over a raft that never joined a cluster is not the meta leader.
        ActorSystem actorSystem = new(logger: raftLogger);
        RaftConfiguration config = new()
        {
            NodeName = "kahuna9", NodeId = 9, Host = "localhost", Port = 8109, InitialPartitions = 2
        };
        RaftManager raft = new(
            config, new StaticDiscovery([]), new InMemoryWAL(raftLogger),
            new InMemoryCommunication(), new HybridLogicalClock(), raftLogger);

        RangeMapStore store = new(raft, tempDir, "rev-noleader", kahunaLogger);

        bool checkpointed = await store.CheckpointNowAsync(TestContext.Current.CancellationToken);
        Assert.False(checkpointed);
    }

    // ── EmptySnapshot_RoundTripsThroughDisk ──────────────────────────────────────

    [Fact]
    public async Task EmptySnapshot_RoundTripsThroughDisk()
    {
        const string revision = "rev-empty";
        (RaftManager raft, KahunaManager kahuna) = await BuildSingleNode(revision);
        try
        {
            // Populate, then clear back to empty — the end state of a drop-table / full merge.
            Assert.True(await kahuna.RangeMapStore.MutateAsync(_ => [RowRange(2)], TestContext.Current.CancellationToken));
            Assert.True(await kahuna.RangeMapStore.MutateAsync(_ => [], TestContext.Current.CancellationToken));

            Assert.Empty(kahuna.RangeMapStore.Current.Descriptors);

            // The on-disk snapshot is the empty map, and a fresh store loads it as empty (not stale).
            RangeMapStore fresh = new(raft, tempDir, revision, kahunaLogger);
            Assert.Empty(fresh.Current.Descriptors);
            Assert.Null(fresh.Current.Find("t:r", "anything"));
        }
        finally
        {
            await raft.LeaveCluster(dispose: true);
        }
    }
}
