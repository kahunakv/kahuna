
using Kahuna.Server.Communication.Internode;
using Kahuna.Server.Configuration;
using Kahuna.Shared.Locks;
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
/// Verifies that a held persistent lock survives a partition step-down observably: the un-warmed
/// promoted leader has no entry in its actor table (the lock replicator materialises nothing on
/// followers) and its backend may not yet contain the flushed mutation — the lock lives only in
/// the promoted node's background-writer queue. A second client's acquire must answer Busy, never
/// be granted; the lock's owner and fencing token must read back intact. Mutual exclusion across
/// failover is the whole point of a distributed lock — a false grant here is a split-brain.
/// </summary>
public sealed class TestLockFailoverCoherence : RaftTrackingTest
{
    private readonly ILogger<IRaft> raftLogger = NullLogger<IRaft>.Instance;
    private readonly ILogger<IKahuna> kahunaLogger = NullLogger<IKahuna>.Instance;

    private static readonly double TimingScale = GetTimingScale();
    private static double GetTimingScale()
    {
        string? val = Environment.GetEnvironmentVariable("KAHUNA_TEST_TIMING_SCALE");
        return val is not null && double.TryParse(val, out double s) && s >= 1.0 ? s : 1.0;
    }

    private sealed record Node(RaftManager Raft, KahunaManager Kahuna);

    private const int ElectionTimeoutSeedBase = 99300;

    private (RaftManager, KahunaManager) BuildNode(
        int nodeId, int port, string[] peers,
        MemoryInterNodeCommmunication interNode, InMemoryCommunication comm,
        string storage)
    {
        ActorSystem actorSystem = new(logger: raftLogger);

        RaftConfiguration raftCfg = new()
        {
            NodeName              = "lockcoherence" + nodeId,
            NodeId                = nodeId,
            Host                  = "localhost",
            Port                  = port,
            InitialPartitions     = 2,
            HeartbeatInterval     = TimeSpan.FromMilliseconds((int)(10 * TimingScale)),
            CheckLeaderInterval   = TimeSpan.FromMilliseconds((int)(25 * TimingScale)),
            StartElectionTimeout  = (int)(50  * TimingScale),
            EndElectionTimeout    = (int)(150 * TimingScale),
            ElectionTimeoutSeed   = ElectionTimeoutSeedBase + nodeId,
            CompactEveryOperations = 1000,
            CompactNumberEntries   = 50,
            EnableQuiescence = false, PartitionExecutorPoolSize = 1
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
            HttpsCertificate          = "",
            HttpsCertificatePassword  = "",
            LocksWorkers              = 8,
            KeyValueWorkers           = 8,
            BackgroundWriterWorkers   = 1,
            Storage                   = storage,
            StoragePath               = storage == "memory"
                ? "/tmp"
                : Directory.CreateDirectory(
                    Path.Combine(Path.GetTempPath(), $"kahuna-lockfailover-{storage}-{Guid.NewGuid():N}")).FullName,
            StorageRevision           = Guid.NewGuid().ToString(),
            DefaultTransactionTimeout = 5000,
            ScriptCacheExpiration     = TimeSpan.FromMinutes(1),
        };

        KahunaManager kahuna = new(actorSystem, Track(raft), kahunaConfig, interNode, kahunaLogger);
        raft.OnLogRestored         += kahuna.OnLogRestored;
        raft.OnReplicationReceived += kahuna.OnReplicationReceived;
        raft.OnReplicationError    += kahuna.OnReplicationError;
        raft.OnLeaderChanged       += kahuna.OnLeaderChanged;

        TestClusterNodeRegistry.Register(raft, kahuna, actorSystem);

        return (raft, kahuna);
    }

    private async Task<Node[]> Assemble(string storage)
    {
        MemoryInterNodeCommmunication interNode = new();
        InMemoryCommunication comm = new();

        string[] p1 = ["localhost:9321", "localhost:9322"];
        string[] p2 = ["localhost:9320", "localhost:9322"];
        string[] p3 = ["localhost:9320", "localhost:9321"];

        (RaftManager r1, KahunaManager k1) = BuildNode(1, 9320, p1, interNode, comm, storage);
        (RaftManager r2, KahunaManager k2) = BuildNode(2, 9321, p2, interNode, comm, storage);
        (RaftManager r3, KahunaManager k3) = BuildNode(3, 9322, p3, interNode, comm, storage);

        interNode.SetNodes(new() { { "localhost:9320", k1 }, { "localhost:9321", k2 }, { "localhost:9322", k3 } });
        comm.SetNodes(new() { { "localhost:9320", r1 }, { "localhost:9321", r2 }, { "localhost:9322", r3 } });

        await Task.WhenAll(r1.JoinCluster(), r2.JoinCluster(), r3.JoinCluster());

        for (int partition = 0; partition <= 2; partition++)
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

    /// <summary>
    /// A persistent lock is acquired and acknowledged but never read anywhere — no cache is warmed.
    /// Every data partition leader then steps down. A different owner's acquire must answer Busy on
    /// the new topology (a grant would be a mutual-exclusion violation), and the lock must read back
    /// with its original owner and fencing token. Runs against all three persistence backends.
    /// </summary>
    [Theory]
    [InlineData("memory")]
    [InlineData("sqlite")]
    [InlineData("rocksdb")]
    public async Task PromotedLeader_UnwarmedLock_IsNeverGrantedToSecondOwner(string storage)
    {
        Node[] nodes = await Assemble(storage);
        try
        {
            CancellationToken ct = TestContext.Current.CancellationToken;

            byte[] holder = "holder-a"u8.ToArray();
            byte[] usurper = "holder-b"u8.ToArray();

            // ── phase 1: acquire with a generous lease, no warming, no reads ────────────────
            (LockResponseType lockType, long fencingToken) = await nodes[0].Kahuna.LocateAndTryLock(
                "unwarmed-lock", holder, 60_000, LockDurability.Persistent, ct);
            Assert.Equal(LockResponseType.Locked, lockType);

            // ── phase 2: force promotion on every data partition ────────────────────────────
            for (int p = 1; p <= 2; p++)
            {
                Node oldLeader = await LeaderOf(p, nodes);
                await oldLeader.Raft.StepDownAsync(p, ct);
            }

            for (int p = 1; p <= 2; p++)
                await WaitForAnyLeader(p, nodes[0].Raft, nodes[1].Raft, nodes[2].Raft);

            // ── phase 3: a second owner's acquire must never be granted ─────────────────────
            long deadline = Environment.TickCount64 + (long)(10_000 * TimingScale);
            while (true)
            {
                (LockResponseType usurperType, _) = await nodes[0].Kahuna.LocateAndTryLock(
                    "unwarmed-lock", usurper, 60_000, LockDurability.Persistent, ct);

                Assert.NotEqual(LockResponseType.Locked, usurperType);

                if (usurperType == LockResponseType.Busy)
                    break;

                Assert.Equal(LockResponseType.WaitingForReplication, usurperType);
                if (Environment.TickCount64 >= deadline)
                    Assert.Fail("no terminal Busy answer within deadline");
                await Task.Delay(20, ct);
            }

            // ── phase 4: the lock reads back intact — original owner and fencing token ──────
            (LockResponseType getType, Server.Locks.Data.ReadOnlyLockEntry? entry) =
                await nodes[0].Kahuna.LocateAndGetLock("unwarmed-lock", LockDurability.Persistent, ct);

            Assert.Equal(LockResponseType.Got, getType);
            Assert.NotNull(entry);
            Assert.Equal(holder, entry!.Owner);
            Assert.Equal(fencingToken, entry.FencingToken);
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }
}
