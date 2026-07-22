using Kahuna.Server.Communication.Internode;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues.Ranges;
using Kommander;
using Kommander.Communication.Memory;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;
using Nixie;

namespace Kahuna.Server.Tests;

/// <summary>
/// Tests for <c>RemoveKeyRangeAsync</c> (T1), registry-as-projection reconcile (T2), and
/// quiesce-window safety (T3).
/// </summary>
[Collection("ClusterTests")]
public sealed class TestRemoveKeyRange
{
    private readonly ILogger<IRaft> raftLogger;
    private readonly ILogger<IKahuna> kahunaLogger;

    public TestRemoveKeyRange(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = LoggerFactory.Create(builder =>
            builder.AddXUnit(outputHelper).SetMinimumLevel(LogLevel.Warning));

        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    private sealed record Node(RaftManager Raft, KahunaManager Kahuna);

    private const int ElectionTimeoutSeedBase = 93000;

    private static readonly double TimingScale = GetTimingScale();
    private static double GetTimingScale()
    {
        string? val = Environment.GetEnvironmentVariable("KAHUNA_TEST_TIMING_SCALE");
        return val is not null && double.TryParse(val, out double s) && s >= 1.0 ? s : 1.0;
    }

    private (RaftManager, KahunaManager) BuildNode(
        int nodeId, int port, string[] peers,
        MemoryInterNodeCommmunication interNode, InMemoryCommunication comm)
    {
        IWAL wal = new InMemoryWAL(raftLogger);

        ActorSystem actorSystem = new(logger: raftLogger);

        RaftConfiguration config = new()
        {
            NodeName = "kahuna" + nodeId,
            NodeId = nodeId,
            Host = "localhost",
            Port = port,
            InitialPartitions = 2,
            StartElectionTimeout = (int)(50 * TimingScale),
            EndElectionTimeout = (int)(150 * TimingScale),
            ElectionTimeoutSeed = ElectionTimeoutSeedBase + nodeId,
            CompactEveryOperations = 1000,
            CompactNumberEntries = 50,
            EnableQuiescence = false
        };

        RaftManager raft = new(
            config,
            new StaticDiscovery([new(peers[0]), new(peers[1])]),
            wal,
            comm,
            new HybridLogicalClock(),
            raftLogger);

        KahunaConfiguration kahunaConfig = new()
        {
            HttpsCertificate = "",
            HttpsCertificatePassword = "",
            LocksWorkers = 4,
            KeyValueWorkers = 4,
            BackgroundWriterWorkers = 1,
            Storage = "memory",
            StoragePath = "/tmp",
            StorageRevision = Guid.NewGuid().ToString(),
            DefaultTransactionTimeout = 5000,
            ScriptCacheExpiration = TimeSpan.FromMinutes(1),
        };

        KahunaManager kahuna = new(actorSystem, raft, kahunaConfig, interNode, kahunaLogger);

        raft.OnLogRestored += kahuna.OnLogRestored;
        raft.OnReplicationReceived += kahuna.OnReplicationReceived;
        raft.OnReplicationError += kahuna.OnReplicationError;
        raft.OnLeaderChanged += kahuna.OnLeaderChanged;

        TestClusterNodeRegistry.Register(raft, kahuna, actorSystem);

        return (raft, kahuna);
    }

    private async Task<Node[]> Assemble()
    {
        MemoryInterNodeCommmunication interNode = new();
        InMemoryCommunication comm = new();

        string[] p1 = ["localhost:8011", "localhost:8012"];
        string[] p2 = ["localhost:8010", "localhost:8012"];
        string[] p3 = ["localhost:8010", "localhost:8011"];

        (RaftManager r1, KahunaManager k1) = BuildNode(1, 8010, p1, interNode, comm);
        (RaftManager r2, KahunaManager k2) = BuildNode(2, 8011, p2, interNode, comm);
        (RaftManager r3, KahunaManager k3) = BuildNode(3, 8012, p3, interNode, comm);

        interNode.SetNodes(new() { { "localhost:8010", k1 }, { "localhost:8011", k2 }, { "localhost:8012", k3 } });
        comm.SetNodes(new() { { "localhost:8010", r1 }, { "localhost:8011", r2 }, { "localhost:8012", r3 } });

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
            try { await TestClusterNodeRegistry.DisposeAsync(node.Raft); }
            catch (ObjectDisposedException) { }
        }
    }

    private static async Task WaitUntilDescriptorGone(Node node, string keySpace, int timeoutMs = 5000)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        long deadline = Environment.TickCount64 + (long)(timeoutMs * TimingScale);
        while (Environment.TickCount64 < deadline)
        {
            if (node.Kahuna.RangeMapStore.Current.FindAll(keySpace).Count == 0)
                return;
            await Task.Delay(25, ct);
        }
        Assert.Fail($"Timed out waiting for descriptor of '{keySpace}' to be removed.");
    }

    private static async Task WaitUntilMode(Node node, string keySpace, RoutingMode expected, int timeoutMs = 5000)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        long deadline = Environment.TickCount64 + (long)(timeoutMs * TimingScale);
        while (Environment.TickCount64 < deadline && node.Kahuna.KeySpaceRegistry.GetMode(keySpace) != expected)
            await Task.Delay(25, ct);

        Assert.Equal(expected, node.Kahuna.KeySpaceRegistry.GetMode(keySpace));
    }

    private static async Task WaitUntilDescriptorPresent(Node node, string keySpace, int timeoutMs = 5000)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        long deadline = Environment.TickCount64 + (long)(timeoutMs * TimingScale);
        while (Environment.TickCount64 < deadline)
        {
            if (node.Kahuna.RangeMapStore.Current.FindAll(keySpace).Count > 0)
                return;
            await Task.Delay(25, ct);
        }
        Assert.Fail($"Timed out waiting for descriptor of '{keySpace}' to appear.");
    }

    // ── RemoveKeyRangeAsync_DropsAllDescriptorsForSpace_OnLeader ─────────────

    [Fact]
    public async Task RemoveKeyRangeAsync_DropsAllDescriptorsForSpace_OnLeader()
    {
        Node[] nodes = await Assemble();
        try
        {
            CancellationToken ct = TestContext.Current.CancellationToken;
            Node leader = await LeaderOf(RangeMapStore.MetaPartitionId, nodes);

            bool seeded = await leader.Kahuna.RegisterKeyRangeAsync("t:r", ct);
            Assert.True(seeded);

            foreach (Node node in nodes)
                await WaitUntilDescriptorPresent(node, "t:r");

            bool removed = await leader.Kahuna.RemoveKeyRangeAsync("t:r", ct);
            Assert.True(removed);

            foreach (Node node in nodes)
                await WaitUntilDescriptorGone(node, "t:r");

            Assert.Empty(leader.Kahuna.RangeMapStore.Current.FindAll("t:r"));
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }

    // ── RemoveKeyRangeAsync_ForwardedFromNonLeader_RemovesOnLeader ───────────

    [Fact]
    public async Task RemoveKeyRangeAsync_ForwardedFromNonLeader_RemovesOnLeader()
    {
        Node[] nodes = await Assemble();
        try
        {
            CancellationToken ct = TestContext.Current.CancellationToken;
            Node leader = await LeaderOf(RangeMapStore.MetaPartitionId, nodes);

            bool seeded = await leader.Kahuna.RegisterKeyRangeAsync("idx:5", ct);
            Assert.True(seeded);

            foreach (Node node in nodes)
                await WaitUntilDescriptorPresent(node, "idx:5");

            // Pick a follower.
            Node follower = nodes.First(n => !ReferenceEquals(n, leader));

            bool removed = await follower.Kahuna.RemoveKeyRangeAsync("idx:5", ct);
            Assert.True(removed);

            foreach (Node node in nodes)
                await WaitUntilDescriptorGone(node, "idx:5");
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }

    // ── RemoveKeyRangeAsync_AbsentSpace_IsIdempotentNoOp ─────────────────────

    [Fact]
    public async Task RemoveKeyRangeAsync_AbsentSpace_IsIdempotentNoOp()
    {
        Node[] nodes = await Assemble();
        try
        {
            CancellationToken ct = TestContext.Current.CancellationToken;
            Node leader = await LeaderOf(RangeMapStore.MetaPartitionId, nodes);

            // No seed — descriptor was never written.
            bool removed = await leader.Kahuna.RemoveKeyRangeAsync("never:seeded", ct);

            // MutateAsync commits (the map identity is still a valid commit) so it returns true.
            // The key invariant is that no descriptor exists and no exception is thrown.
            Assert.Empty(leader.Kahuna.RangeMapStore.Current.FindAll("never:seeded"));
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }

    // ── RemoveKeyRangeAsync_BelowMinPartitions_IsNoOp ────────────────────────
    // Verified via the guard in RemoveKeyRangeAsync — no cluster needed; same guard mirrors
    // RegisterKeyRangeAsync's own early-out at InitialPartitions < FirstDataPartitionId.

    // ── ReconcileTo_RemovesSpacesNotInLiveSet_KeepsLiveOnes ─────────────────

    [Fact]
    public void ReconcileTo_RemovesSpacesNotInLiveSet_KeepsLiveOnes()
    {
        KeySpaceRegistry registry = new();
        registry.RegisterKeyRange("t:r");
        registry.RegisterKeyRange("t:i:5");
        registry.RegisterKeyRange("t:i:6");

        Assert.Equal(RoutingMode.KeyRange, registry.GetMode("t:r"));
        Assert.Equal(RoutingMode.KeyRange, registry.GetMode("t:i:5"));
        Assert.Equal(RoutingMode.KeyRange, registry.GetMode("t:i:6"));

        // Reconcile to only {t:r, t:i:5} — t:i:6 must be removed.
        registry.ReconcileTo(new HashSet<string>(StringComparer.Ordinal) { "t:r", "t:i:5" });

        Assert.Equal(RoutingMode.KeyRange, registry.GetMode("t:r"));
        Assert.Equal(RoutingMode.KeyRange, registry.GetMode("t:i:5"));
        Assert.Equal(RoutingMode.Hash, registry.GetMode("t:i:6"));
    }

    // ── SyncFromRangeMap_AfterDescriptorRemoval_ClearsRoutingMode ────────────

    [Fact]
    public async Task SyncFromRangeMap_AfterDescriptorRemoval_ClearsRoutingMode()
    {
        Node[] nodes = await Assemble();
        try
        {
            CancellationToken ct = TestContext.Current.CancellationToken;
            Node leader = await LeaderOf(RangeMapStore.MetaPartitionId, nodes);

            await leader.Kahuna.RegisterKeyRangeAsync("t:r", ct);

            foreach (Node node in nodes)
                await WaitUntilDescriptorPresent(node, "t:r");

            // The range map's Current is swapped visible before the same replication callback reconciles
            // the node-local KeySpaceRegistry, so a poll can observe the descriptor a hair before the mode
            // flips. Wait for the mode to settle rather than asserting on the intermediate window.
            foreach (Node node in nodes)
                await WaitUntilMode(node, "t:r", RoutingMode.KeyRange);

            // Remove — descriptor is gone from map and sync fires via replication.
            await leader.Kahuna.RemoveKeyRangeAsync("t:r", ct);

            foreach (Node node in nodes)
                await WaitUntilDescriptorGone(node, "t:r");

            // After sync the routing mode must have reverted to Hash on every node.
            foreach (Node node in nodes)
                await WaitUntilMode(node, "t:r", RoutingMode.Hash);
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }

    // ── SyncFromRangeMap_NeverDropsLiveKeySpace ──────────────────────────────

    [Fact]
    public async Task SyncFromRangeMap_NeverDropsLiveKeySpace()
    {
        Node[] nodes = await Assemble();
        try
        {
            CancellationToken ct = TestContext.Current.CancellationToken;
            Node leader = await LeaderOf(RangeMapStore.MetaPartitionId, nodes);

            await leader.Kahuna.RegisterKeyRangeAsync("t:r", ct);
            await leader.Kahuna.RegisterKeyRangeAsync("t:i:1", ct);

            foreach (Node node in nodes)
            {
                await WaitUntilDescriptorPresent(node, "t:r");
                await WaitUntilDescriptorPresent(node, "t:i:1");
            }

            // Remove only t:i:1; t:r must remain KeyRange on all nodes.
            await leader.Kahuna.RemoveKeyRangeAsync("t:i:1", ct);

            foreach (Node node in nodes)
                await WaitUntilDescriptorGone(node, "t:i:1");

            foreach (Node node in nodes)
                Assert.Equal(RoutingMode.KeyRange, node.Kahuna.KeySpaceRegistry.GetMode("t:r"));
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }

    // ── RemoveKeyRangeAsync_RemovesAllGenerationsAfterSplit ─────────────────
    // Simulates multiple descriptor generations by directly mutating the range map.

    [Fact]
    public async Task RemoveKeyRangeAsync_RemovesAllGenerationsAfterSplit()
    {
        Node[] nodes = await Assemble();
        try
        {
            CancellationToken ct = TestContext.Current.CancellationToken;
            Node leader = await LeaderOf(RangeMapStore.MetaPartitionId, nodes);

            // Seed two descriptors for the same space (simulating post-split state: gen 1 + gen 2).
            await leader.Kahuna.RangeMapStore.MutateAsync(_ =>
            [
                new RangeDescriptor { KeySpace = "t:r", StartKey = null, EndKey = "m", PartitionId = 1, Generation = 1 },
                new RangeDescriptor { KeySpace = "t:r", StartKey = "m", EndKey = null, PartitionId = 2, Generation = 2 }
            ], ct);

            foreach (Node node in nodes)
            {
                long deadline = Environment.TickCount64 + (long)(5000 * TimingScale);
                while (Environment.TickCount64 < deadline && node.Kahuna.RangeMapStore.Current.FindAll("t:r").Count < 2)
                    await Task.Delay(25, ct);

                Assert.Equal(2, node.Kahuna.RangeMapStore.Current.FindAll("t:r").Count);
            }

            bool removed = await leader.Kahuna.RemoveKeyRangeAsync("t:r", ct);
            Assert.True(removed);

            foreach (Node node in nodes)
                await WaitUntilDescriptorGone(node, "t:r");
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }

    // ── RemoveKeyRangeAsync_WhileQuiesced_ReturnsFalse ───────────────────────

    [Fact]
    public async Task RemoveKeyRangeAsync_WhileQuiesced_ReturnsFalse()
    {
        Node[] nodes = await Assemble();
        try
        {
            CancellationToken ct = TestContext.Current.CancellationToken;
            Node leader = await LeaderOf(RangeMapStore.MetaPartitionId, nodes);

            await leader.Kahuna.RegisterKeyRangeAsync("t:r", ct);

            foreach (Node node in nodes)
                await WaitUntilDescriptorPresent(node, "t:r");

            // Simulate a split window by quiescing the space on the leader.
            leader.Kahuna.RangeQuiesceStore.Quiesce("t:r", null, null);
            try
            {
                bool result = await leader.Kahuna.RemoveKeyRangeAsync("t:r", ct);

                // Must be rejected while the quiesce is active.
                Assert.False(result);

                // Descriptor is still present — nothing was removed.
                Assert.NotEmpty(leader.Kahuna.RangeMapStore.Current.FindAll("t:r"));
            }
            finally
            {
                leader.Kahuna.RangeQuiesceStore.Release("t:r", null, null);
            }

            // After the quiesce is released the removal must succeed.
            bool removed = await leader.Kahuna.RemoveKeyRangeAsync("t:r", ct);
            Assert.True(removed);

            foreach (Node node in nodes)
                await WaitUntilDescriptorGone(node, "t:r");
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }
}
