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
/// Replication tests for the range-descriptor map. The map is the replicated source of
/// truth, hosted on the system/meta partition (<see cref="RangeMapStore.MetaPartitionId"/> = 0) —
/// shared with the Kommander partition-map coordinator by log type (0.11.0+). These tests run a
/// real 3-node cluster and exercise the three commit paths: WAL replay on restart
/// (<c>OnLogRestored</c>), follower replication + failover (<c>OnReplicationReceived</c>), and the
/// single-writer serialization of <see cref="RangeMapStore.MutateAsync"/>.
/// </summary>
[Collection("ClusterTests")]
public sealed class TestRangeMapReplication
{
    private readonly ILogger<IRaft> raftLogger;
    private readonly ILogger<IKahuna> kahunaLogger;

    public TestRangeMapReplication(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = LoggerFactory.Create(builder =>
            builder.AddXUnit(outputHelper).SetMinimumLevel(LogLevel.Warning));

        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    // ── helpers ────────────────────────────────────────────────────────────────

    private sealed record Node(RaftManager Raft, KahunaManager Kahuna);

    // Distinct per-node seeds prevent all three nodes from drawing the same election-timeout
    // sequence and splitting votes indefinitely (Raft election livelock).
    private const int ElectionTimeoutSeedBase = 91000;

    /// <summary>Builds a single node bound to the shared in-memory transports and the given WAL revision.</summary>
    private (RaftManager, KahunaManager) BuildNode(
        int nodeId, int port, string[] peers, string walStorage, string raftRevision,
        MemoryInterNodeCommmunication interNode, InMemoryCommunication comm)
    {
        IWAL wal = walStorage switch
        {
            "memory" => new InMemoryWAL(raftLogger),
            "sqlite" => new SqliteWAL("/tmp", raftRevision, raftLogger, syncWrites: false),
            _ => throw new ArgumentException($"Unknown wal: {walStorage}")
        };

        ActorSystem actorSystem = new(logger: raftLogger);

        RaftConfiguration config = new()
        {
            NodeName = "kahuna" + nodeId,
            NodeId = nodeId,
            Host = "localhost",
            Port = port,
            InitialPartitions = 2, // meta map on P0 (system); partitions 1, 2 = data
            StartElectionTimeout = (int)(50 * TimingScale),
            EndElectionTimeout = (int)(150 * TimingScale),
            ElectionTimeoutSeed = ElectionTimeoutSeedBase + nodeId, // distinct per node, prevents election livelock
            CompactEveryOperations = 1000,
            CompactNumberEntries = 50
        };

        RaftManager raft = new(
            config,
            new StaticDiscovery([new(peers[0]), new(peers[1])]),
            wal,
            comm,
            new HybridLogicalClock(),
            raftLogger);

        KahunaConfiguration kahunaConfiguration = new()
        {
            HttpsCertificate = "",
            HttpsCertificatePassword = "",
            LocksWorkers = 8,
            KeyValueWorkers = 8,
            BackgroundWriterWorkers = 1,
            Storage = "memory",
            StoragePath = "/tmp",
            StorageRevision = Guid.NewGuid().ToString(),
            DefaultTransactionTimeout = 5000,
            ScriptCacheExpiration = TimeSpan.FromMinutes(1),
        };

        KahunaManager kahuna = new(actorSystem, raft, kahunaConfiguration, interNode, kahunaLogger);

        raft.OnLogRestored += kahuna.OnLogRestored;
        raft.OnReplicationReceived += kahuna.OnReplicationReceived;
        raft.OnReplicationError += kahuna.OnReplicationError;

        return (raft, kahuna);
    }

    /// <summary>Assembles a 3-node cluster on fixed WAL revisions (so it can be restarted onto the same WAL).</summary>
    private async Task<Node[]> Assemble(string walStorage, string[] revisions)
    {
        MemoryInterNodeCommmunication interNode = new();
        InMemoryCommunication comm = new();

        string[] p1 = ["localhost:8002", "localhost:8003"];
        string[] p2 = ["localhost:8001", "localhost:8003"];
        string[] p3 = ["localhost:8001", "localhost:8002"];

        (RaftManager r1, KahunaManager k1) = BuildNode(1, 8001, p1, walStorage, revisions[0], interNode, comm);
        (RaftManager r2, KahunaManager k2) = BuildNode(2, 8002, p2, walStorage, revisions[1], interNode, comm);
        (RaftManager r3, KahunaManager k3) = BuildNode(3, 8003, p3, walStorage, revisions[2], interNode, comm);

        interNode.SetNodes(new() { { "localhost:8001", k1 }, { "localhost:8002", k2 }, { "localhost:8003", k3 } });
        comm.SetNodes(new() { { "localhost:8001", r1 }, { "localhost:8002", r2 }, { "localhost:8003", r3 } });

        await Task.WhenAll(r1.JoinCluster(), r2.JoinCluster(), r3.JoinCluster());

        for (int partition = 1; partition <= 2; partition++)
            await WaitForAnyLeader(partition, r1, r2, r3);

        return [new(r1, k1), new(r2, k2), new(r3, k3)];
    }

    private static async Task WaitForAnyLeader(int partition, params RaftManager[] rafts)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        while (true)
        {
            foreach (RaftManager raft in rafts)
                if (await raft.AmILeader(partition, ct))
                    return;

            await Task.Delay(50, ct);
        }
    }

    private static async Task<Node> LeaderOf(int partition, Node[] nodes)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        while (true)
        {
            foreach (Node node in nodes)
                if (await node.Raft.AmILeader(partition, ct))
                    return node;

            await Task.Delay(50, ct);
        }
    }

    private static async Task LeaveAll(Node[] nodes)
    {
        foreach (Node node in nodes)
        {
            try { await node.Raft.LeaveCluster(dispose: true); }
            catch (ObjectDisposedException) { /* already torn down */ }
        }
    }

    private static readonly double TimingScale = GetTimingScale();
    private static double GetTimingScale()
    {
        string? val = Environment.GetEnvironmentVariable("KAHUNA_TEST_TIMING_SCALE");
        return val is not null && double.TryParse(val, out double s) && s >= 1.0 ? s : 1.0;
    }

    /// <summary>Polls until <paramref name="predicate"/> holds over <paramref name="node"/>'s current map, or times out.</summary>
    private static async Task WaitUntil(Node node, Func<RangeMap, bool> predicate, int timeoutMs = 5000)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        long deadline = Environment.TickCount64 + (long)(timeoutMs * TimingScale);
        while (Environment.TickCount64 < deadline)
        {
            if (predicate(node.Kahuna.RangeMapStore.Current))
                return;

            await Task.Delay(25, ct);
        }

        Assert.Fail("Timed out waiting for the range map to converge.");
    }

    private static RangeDescriptor RowRange(int partitionId, long generation = 1) => new()
    {
        KeySpace = "t:r",
        StartKey = null,
        EndKey = null,
        PartitionId = partitionId,
        Generation = generation
    };

    // ── Descriptor_SurvivesRestart ───────────────────────────────────────────────

    [Fact]
    public async Task Descriptor_SurvivesRestart()
    {
        string[] revisions = [Guid.NewGuid().ToString(), Guid.NewGuid().ToString(), Guid.NewGuid().ToString()];

        Node[] nodes = await Assemble("sqlite", revisions);
        try
        {
            Node leader = await LeaderOf(RangeMapStore.MetaPartitionId, nodes);

            bool committed = await leader.Kahuna.RangeMapStore.MutateAsync(
                _ => [RowRange(2)], TestContext.Current.CancellationToken);
            Assert.True(committed);

            // Ensure the entry has been committed + persisted to every node's WAL before we restart.
            foreach (Node node in nodes)
                await WaitUntil(node, map => map.Find("t:r", "k") is { PartitionId: 2 });
        }
        finally
        {
            await LeaveAll(nodes);
        }

        // Restart onto the SAME WAL files — the map must be rebuilt purely from the meta log via OnLogRestored.
        Node[] restarted = await Assemble("sqlite", revisions);
        try
        {
            Node leader = await LeaderOf(RangeMapStore.MetaPartitionId, restarted);
            await WaitUntil(leader, map => map.Find("t:r", "k") is { PartitionId: 2 });

            RangeDescriptor? descriptor = leader.Kahuna.RangeMapStore.Current.Find("t:r", "anything");
            Assert.NotNull(descriptor);
            Assert.Equal(2, descriptor!.PartitionId);
        }
        finally
        {
            await LeaveAll(restarted);
        }
    }

    // ── Descriptor_VisibleAfterFailover ──────────────────────────────────────────

    [Fact]
    public async Task Descriptor_VisibleAfterFailover()
    {
        Node[] nodes = await Assemble("memory",
            [Guid.NewGuid().ToString(), Guid.NewGuid().ToString(), Guid.NewGuid().ToString()]);
        try
        {
            Node leader = await LeaderOf(RangeMapStore.MetaPartitionId, nodes);

            bool committed = await leader.Kahuna.RangeMapStore.MutateAsync(
                _ => [RowRange(2)], TestContext.Current.CancellationToken);
            Assert.True(committed);

            // Replicated to followers via OnReplicationReceived.
            foreach (Node node in nodes)
                await WaitUntil(node, map => map.Find("t:r", "k") is { PartitionId: 2 });

            // Force a leadership change on the meta partition.
            await leader.Raft.StepDownAsync(RangeMapStore.MetaPartitionId, TestContext.Current.CancellationToken);

            Node newLeader = await LeaderOf(RangeMapStore.MetaPartitionId, nodes);
            Assert.NotSame(leader, newLeader);

            // The promoted node still serves the descriptor — it was carried in its replicated map.
            RangeDescriptor? descriptor = newLeader.Kahuna.RangeMapStore.Current.Find("t:r", "anything");
            Assert.NotNull(descriptor);
            Assert.Equal(2, descriptor!.PartitionId);
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }

    // ── Mutate_ConcurrentAttemptsSerialize ───────────────────────────────────────

    [Fact]
    public async Task Mutate_ConcurrentAttemptsSerialize()
    {
        Node[] nodes = await Assemble("memory",
            [Guid.NewGuid().ToString(), Guid.NewGuid().ToString(), Guid.NewGuid().ToString()]);
        try
        {
            Node leader = await LeaderOf(RangeMapStore.MetaPartitionId, nodes);

            const int writers = 8;

            // Each writer appends a distinct key space as one full range. If the read-modify-write
            // were not serialized, racing transforms would clobber earlier appends (last snapshot
            // wins) and the final map would have fewer than `writers` spaces.
            IEnumerable<Task<bool>> mutations = Enumerable.Range(0, writers).Select(i =>
                leader.Kahuna.RangeMapStore.MutateAsync(current =>
                {
                    List<RangeDescriptor> next = [.. current];
                    next.Add(new RangeDescriptor
                    {
                        KeySpace = "ks" + i,
                        StartKey = null,
                        EndKey = null,
                        PartitionId = i + 2, // data partitions are >= 2
                        Generation = 1
                    });

                    // The map must satisfy the no-gap/no-overlap invariant after every commit.
                    Assert.True(new RangeMap(next).Validate(out string? error), error);
                    return next;
                }, TestContext.Current.CancellationToken));

            bool[] results = await Task.WhenAll(mutations);
            Assert.All(results, Assert.True);

            RangeMap finalMap = leader.Kahuna.RangeMapStore.Current;
            Assert.True(finalMap.Validate(out string? finalError), finalError);

            // Single linear history: every one of the N appends survived.
            Assert.Equal(writers, finalMap.Descriptors.Count);
            for (int i = 0; i < writers; i++)
                Assert.Equal(i + 2, finalMap.Find("ks" + i, "x")!.PartitionId);
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }

    // ── EmptySnapshot_ClearsMapAcrossCluster ─────────────────────────────────────

    [Fact]
    public async Task EmptySnapshot_ClearsMapAcrossCluster()
    {
        Node[] nodes = await Assemble("memory",
            [Guid.NewGuid().ToString(), Guid.NewGuid().ToString(), Guid.NewGuid().ToString()]);
        try
        {
            Node leader = await LeaderOf(RangeMapStore.MetaPartitionId, nodes);

            // Populate, and confirm every node sees the descriptor.
            Assert.True(await leader.Kahuna.RangeMapStore.MutateAsync(
                _ => [RowRange(2)], TestContext.Current.CancellationToken));

            foreach (Node node in nodes)
                await WaitUntil(node, map => map.Find("t:r", "k") is { PartitionId: 2 });

            // Commit an EMPTY map (drop-table / full-merge end state). This serializes to a 0-byte
            // proto payload; followers must clear, not skip it (regression: empty-snapshot divergence).
            Assert.True(await leader.Kahuna.RangeMapStore.MutateAsync(
                _ => [], TestContext.Current.CancellationToken));

            foreach (Node node in nodes)
                await WaitUntil(node, map => map.Descriptors.Count == 0 && map.Find("t:r", "k") is null);
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }

    // ── MetaPartition_IsZero_DataPartitionsAreOnePlus ────────────────────────────

    [Fact]
    public async Task MetaPartition_IsZero_DataPartitionsAreOnePlus()
    {
        Assert.Equal(0, RangeMapStore.MetaPartitionId);
        Assert.Equal(1, RangeMapStore.FirstDataPartitionId);

        Node[] nodes = await Assemble("memory",
            [Guid.NewGuid().ToString(), Guid.NewGuid().ToString(), Guid.NewGuid().ToString()]);
        try
        {
            Node leader = await LeaderOf(RangeMapStore.MetaPartitionId, nodes);

            // A descriptor naming the reserved system/meta partition (0) is rejected — ranged data lives on >= 1.
            bool rejectedMeta = await leader.Kahuna.RangeMapStore.MutateAsync(
                _ => [RowRange(0)], TestContext.Current.CancellationToken);
            Assert.False(rejectedMeta);
            Assert.Empty(leader.Kahuna.RangeMapStore.Current.Descriptors);

            // A descriptor on a data partition (>= 1) commits.
            bool acceptedData = await leader.Kahuna.RangeMapStore.MutateAsync(
                _ => [RowRange(1)], TestContext.Current.CancellationToken);
            Assert.True(acceptedData);
            Assert.All(leader.Kahuna.RangeMapStore.Current.Descriptors, d => Assert.True(d.PartitionId >= 1));
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }
}
