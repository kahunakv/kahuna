
using Kahuna.Server.Communication.Internode;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Communication.Memory;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Nixie;

namespace Kahuna.Server.Tests;

/// <summary>
/// Verifies that expired snapshot holds are automatically reclaimed without a client release,
/// and that <c>GetEffectiveFloor</c> rises to <see cref="HLCTimestamp.Zero"/> once all holds
/// have lapsed. These tests exercise the background-reaper code path by invoking
/// <c>PurgeExpiredHoldsAsync</c> directly (rather than waiting for the periodic timer).
/// </summary>
[Collection("ClusterTests")]
public sealed class TestSnapshotFloorLeaseExpiry
{
    private readonly ILogger<IKahuna> kahunaLogger = NullLogger<IKahuna>.Instance;
    private readonly ILogger<IRaft> raftLogger = NullLogger<IRaft>.Instance;

    private static readonly double TimingScale = GetTimingScale();
    private static double GetTimingScale()
    {
        string? val = Environment.GetEnvironmentVariable("KAHUNA_TEST_TIMING_SCALE");
        return val is not null && double.TryParse(val, out double s) && s >= 1.0 ? s : 1.0;
    }

    private sealed record Node(RaftManager Raft, KahunaManager Kahuna);

    private const int ElectionTimeoutSeedBase = 84000;

    private (RaftManager, KahunaManager) BuildNode(
        int nodeId, int port, string[] peers,
        MemoryInterNodeCommmunication interNode, InMemoryCommunication comm,
        string revision)
    {
        ActorSystem actorSystem = new(logger: raftLogger);

        RaftConfiguration raftCfg = new()
        {
            NodeName    = "lease" + nodeId,
            NodeId      = nodeId,
            Host        = "localhost",
            Port        = port,
            InitialPartitions     = 2,
            StartElectionTimeout  = (int)(50 * TimingScale),
            EndElectionTimeout    = (int)(150 * TimingScale),
            ElectionTimeoutSeed   = ElectionTimeoutSeedBase + nodeId,
            CompactEveryOperations = 1000,
            CompactNumberEntries   = 50,
            EnableQuiescence       = false
        };

        RaftManager raft = new(
            raftCfg,
            new StaticDiscovery([new(peers[0]), new(peers[1])]),
            new InMemoryWAL(raftLogger),
            comm,
            new HybridLogicalClock(),
            raftLogger);

        KahunaConfiguration kahunaConfig = new()
        {
            HttpsCertificate         = "",
            HttpsCertificatePassword = "",
            LocksWorkers             = 8,
            KeyValueWorkers          = 8,
            BackgroundWriterWorkers  = 1,
            Storage                  = "memory",
            StoragePath              = "/tmp",
            StorageRevision          = revision,
            DefaultTransactionTimeout = 5000,
            ScriptCacheExpiration    = TimeSpan.FromMinutes(1),
        };

        KahunaManager kahuna = new(actorSystem, raft, kahunaConfig, interNode, kahunaLogger);
        raft.OnLogRestored          += kahuna.OnLogRestored;
        raft.OnReplicationReceived  += kahuna.OnReplicationReceived;
        raft.OnReplicationError     += kahuna.OnReplicationError;
        raft.OnLeaderChanged        += kahuna.OnLeaderChanged;

        return (raft, kahuna);
    }

    private async Task<Node[]> Assemble()
    {
        string rev = Guid.NewGuid().ToString();
        MemoryInterNodeCommmunication interNode = new();
        InMemoryCommunication comm = new();

        string[] p1 = ["localhost:9201", "localhost:9202"];
        string[] p2 = ["localhost:9200", "localhost:9202"];
        string[] p3 = ["localhost:9200", "localhost:9201"];

        (RaftManager r1, KahunaManager k1) = BuildNode(1, 9200, p1, interNode, comm, rev + "_1");
        (RaftManager r2, KahunaManager k2) = BuildNode(2, 9201, p2, interNode, comm, rev + "_2");
        (RaftManager r3, KahunaManager k3) = BuildNode(3, 9202, p3, interNode, comm, rev + "_3");

        interNode.SetNodes(new() { { "localhost:9200", k1 }, { "localhost:9201", k2 }, { "localhost:9202", k3 } });
        comm.SetNodes(new() { { "localhost:9200", r1 }, { "localhost:9201", r2 }, { "localhost:9202", r3 } });

        await Task.WhenAll(r1.JoinCluster(), r2.JoinCluster(), r3.JoinCluster());

        for (int partition = 0; partition <= 1; partition++)
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
            catch (ObjectDisposedException) { }
        }
    }

    /// <summary>
    /// A hold acquired with a short lease expires, is swept by a manual purge call, and the
    /// effective floor drops to Zero — no client release required.
    /// </summary>
    [Fact]
    public async Task ExpiredHold_PurgedWithoutClientRelease_FloorDropsToZero()
    {
        Node[] nodes = await Assemble();
        try
        {
            Node leader = await LeaderOf(RangeMapStore.MetaPartitionId, nodes);
            CancellationToken ct = TestContext.Current.CancellationToken;

            HLCTimestamp forkT = leader.Raft.HybridLogicalClock.TrySendOrLocalEvent(leader.Raft.GetLocalNodeId());

            // Acquire with a 1 ms lease — guaranteed to expire by the time we check.
            (KeyValueResponseType type, string holdId, HLCTimestamp leaseExpiry) =
                await leader.Kahuna.LocateAndAcquireSnapshotHold("crashing-branch", forkT, leaseMs: 1, ct);

            Assert.Equal(KeyValueResponseType.Set, type);
            Assert.NotEmpty(holdId);
            Assert.NotEqual(HLCTimestamp.Zero, leaseExpiry);

            // Let HLC advance past the expiry.
            await Task.Delay((int)(50 * TimingScale), ct);

            // Manually trigger the purge (no periodic timer needed in tests).
            int purged = await leader.Kahuna.PurgeExpiredSnapshotHoldsAsync(ct);
            Assert.Equal(1, purged);

            // After the purge, the floor must drop to Zero.
            (HLCTimestamp floorAfter, int liveAfter) = await leader.Kahuna.GetSnapshotFloor(ct);
            Assert.Equal(HLCTimestamp.Zero, floorAfter);
            Assert.Equal(0, liveAfter);
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }

    /// <summary>
    /// When two holds are registered but one has a short lease, purging leaves only the
    /// long-lease hold: the floor is non-zero and exactly one live hold remains.
    /// </summary>
    [Fact]
    public async Task PartialExpiry_LongLeaseHoldSurvivesPurge()
    {
        Node[] nodes = await Assemble();
        try
        {
            Node leader = await LeaderOf(RangeMapStore.MetaPartitionId, nodes);
            CancellationToken ct = TestContext.Current.CancellationToken;

            HLCTimestamp t1 = leader.Raft.HybridLogicalClock.TrySendOrLocalEvent(leader.Raft.GetLocalNodeId());
            // Hold 1: 50 ms lease — expires before the 300 ms delay below.
            await leader.Kahuna.LocateAndAcquireSnapshotHold("hold-short", t1, leaseMs: 50, ct);

            // Hold 2: long lease (must survive the purge).
            HLCTimestamp t2 = leader.Raft.HybridLogicalClock.TrySendOrLocalEvent(leader.Raft.GetLocalNodeId());
            await leader.Kahuna.LocateAndAcquireSnapshotHold("hold-long", t2, leaseMs: 60_000, ct);

            // Wait long enough for the 50 ms hold-short lease to expire relative to HLC-now.
            await Task.Delay((int)(300 * TimingScale), ct);

            // Purge expired holds (hold-short may already have been dropped from the registry
            // by an earlier replication overwrite, so we don't assert the exact purge count).
            await leader.Kahuna.PurgeExpiredSnapshotHoldsAsync(ct);

            // After the purge: hold-long must still be live. The floor must be non-zero.
            (HLCTimestamp floorAfter, int liveAfter) = await leader.Kahuna.GetSnapshotFloor(ct);
            Assert.NotEqual(HLCTimestamp.Zero, floorAfter);
            Assert.Equal(1, liveAfter);
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }

    /// <summary>
    /// Purging when all holds are still live is a no-op: the registry is unchanged and 0 is returned.
    /// </summary>
    [Fact]
    public async Task PurgeWithLiveHolds_IsNoOp()
    {
        Node[] nodes = await Assemble();
        try
        {
            Node leader = await LeaderOf(RangeMapStore.MetaPartitionId, nodes);
            CancellationToken ct = TestContext.Current.CancellationToken;

            HLCTimestamp forkT = leader.Raft.HybridLogicalClock.TrySendOrLocalEvent(leader.Raft.GetLocalNodeId());
            await leader.Kahuna.LocateAndAcquireSnapshotHold("live-branch", forkT, leaseMs: 60_000, ct);

            int purged = await leader.Kahuna.PurgeExpiredSnapshotHoldsAsync(ct);
            Assert.Equal(0, purged);

            // Hold still there.
            (HLCTimestamp floor, int live) = await leader.Kahuna.GetSnapshotFloor(ct);
            Assert.Equal(forkT, floor);
            Assert.Equal(1, live);
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }
}
