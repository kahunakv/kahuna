
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
/// Verifies fencing-token safety for lock actors across leadership changes. A lock actor's
/// in-memory entry belongs to the node, not the leadership term: when another leader grants
/// tokens, every node that applies those committed entries must advance (or drop) its resident
/// entry. If a former leader's cache stays frozen at its last tenure, a re-promotion mints
/// fencing tokens from that stale state — replaying token values that were already granted and
/// acknowledged, which inverts the fencing order downstream resources rely on.
/// </summary>
public sealed class TestLockFailoverCoherence : RaftTrackingTest
{
    private readonly ILogger<IRaft>   raftLogger   = NullLogger<IRaft>.Instance;
    private readonly ILogger<IKahuna> kahunaLogger = NullLogger<IKahuna>.Instance;

    private static readonly double TimingScale = GetTimingScale();
    private static double GetTimingScale()
    {
        string? val = Environment.GetEnvironmentVariable("KAHUNA_TEST_TIMING_SCALE");
        return val is not null && double.TryParse(val, out double s) && s >= 1.0 ? s : 1.0;
    }

    private sealed record Node(RaftManager Raft, KahunaManager Kahuna);

    private const int ElectionTimeoutSeedBase = 98000;

    private (RaftManager, KahunaManager) BuildNode(
        int nodeId, int port, string[] peers,
        MemoryInterNodeCommmunication interNode, InMemoryCommunication comm)
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
            Storage                   = "memory",
            StoragePath               = "/tmp",
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

    private async Task<Node[]> Assemble()
    {
        MemoryInterNodeCommmunication interNode = new();
        InMemoryCommunication comm = new();

        string[] p1 = ["localhost:9311", "localhost:9312"];
        string[] p2 = ["localhost:9310", "localhost:9312"];
        string[] p3 = ["localhost:9310", "localhost:9311"];

        (RaftManager r1, KahunaManager k1) = BuildNode(1, 9310, p1, interNode, comm);
        (RaftManager r2, KahunaManager k2) = BuildNode(2, 9311, p2, interNode, comm);
        (RaftManager r3, KahunaManager k3) = BuildNode(3, 9312, p3, interNode, comm);

        interNode.SetNodes(new() { { "localhost:9310", k1 }, { "localhost:9311", k2 }, { "localhost:9312", k3 } });
        comm.SetNodes(new() { { "localhost:9310", r1 }, { "localhost:9311", r2 }, { "localhost:9312", r3 } });

        await Task.WhenAll(r1.JoinCluster(), r2.JoinCluster(), r3.JoinCluster());

        // InitialPartitions = 2 → partitions 0 (meta), 1 and 2 (data): wait for all.
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

    /// <summary>Retries a lock operation while the cluster answers MustRetry (leadership settling).</summary>
    private static async Task<(LockResponseType, long)> RetryLockOp(
        Func<Task<(LockResponseType, long)>> op, int timeoutMs = 10_000)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        long deadline = Environment.TickCount64 + (long)(timeoutMs * TimingScale);
        while (true)
        {
            (LockResponseType type, long token) = await op();
            if (type != LockResponseType.MustRetry || Environment.TickCount64 >= deadline)
                return (type, token);
            await Task.Delay(50, ct);
        }
    }

    private static async Task<LockResponseType> RetryUnlock(
        Func<Task<LockResponseType>> op, int timeoutMs = 10_000)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        long deadline = Environment.TickCount64 + (long)(timeoutMs * TimingScale);
        while (true)
        {
            LockResponseType type = await op();
            if (type != LockResponseType.MustRetry || Environment.TickCount64 >= deadline)
                return type;
            await Task.Delay(50, ct);
        }
    }

    /// <summary>
    /// Fencing tokens must be globally monotonic per resource across leadership changes. The test
    /// grants and releases the same persistent lock repeatedly, forcing a leadership change on
    /// every data partition between rounds. Rotating leadership over three nodes for four rounds
    /// guarantees some node leads the resource's partition twice, with grants by other leaders in
    /// between — exactly the shape where a stale resident entry on the re-promoted leader would
    /// mint an already-granted token again. Every grant must therefore return a strictly larger
    /// token than every earlier grant, no matter which node is serving.
    /// </summary>
    [Fact]
    public async Task PromotedLeader_MintsMonotonicFencingTokens()
    {
        Node[] nodes = await Assemble();
        try
        {
            CancellationToken ct = TestContext.Current.CancellationToken;
            const string resource = "fencing-coherence-lock";
            long lastToken = -1;

            for (int round = 0; round < 4; round++)
            {
                for (int grant = 0; grant < 3; grant++)
                {
                    byte[] owner = Guid.NewGuid().ToByteArray();

                    (LockResponseType lockType, long token) = await RetryLockOp(() =>
                        nodes[0].Kahuna.LocateAndTryLock(resource, owner, 30_000, LockDurability.Persistent, ct));

                    Assert.Equal(LockResponseType.Locked, lockType);
                    Assert.True(token > lastToken,
                        $"round {round} grant {grant}: token {token} did not advance past {lastToken} — " +
                        "a re-promoted leader minted from a stale cached entry");
                    lastToken = token;

                    LockResponseType unlockType = await RetryUnlock(() =>
                        nodes[0].Kahuna.LocateAndTryUnlock(resource, owner, LockDurability.Persistent, ct));
                    Assert.Equal(LockResponseType.Unlocked, unlockType);
                }

                // Force a leadership change on both data partitions (partition 0 is the meta
                // partition and not involved in lock routing) before the next round of grants.
                for (int p = 1; p <= 2; p++)
                {
                    Node oldLeader = await LeaderOf(p, nodes);
                    await oldLeader.Raft.StepDownAsync(p, ct);
                }

                for (int p = 1; p <= 2; p++)
                    await WaitForAnyLeader(p, nodes[0].Raft, nodes[1].Raft, nodes[2].Raft);
            }
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }

    /// <summary>
    /// The lock read path must show the same coherence: after another leader grants the lock, a
    /// promoted leader answering a Get from its resident entry must report the latest committed
    /// holder and token, never the state of its own last tenure.
    /// </summary>
    [Fact]
    public async Task PromotedLeader_GetLock_SeesLatestGrant()
    {
        Node[] nodes = await Assemble();
        try
        {
            CancellationToken ct = TestContext.Current.CancellationToken;
            const string resource = "get-coherence-lock";
            long lastToken = -1;

            // Rotate leadership with a grant per round so several nodes hold a resident entry,
            // then verify the cluster read reflects the newest grant every time.
            for (int round = 0; round < 4; round++)
            {
                byte[] owner = Guid.NewGuid().ToByteArray();

                (LockResponseType lockType, long token) = await RetryLockOp(() =>
                    nodes[0].Kahuna.LocateAndTryLock(resource, owner, 60_000, LockDurability.Persistent, ct));
                Assert.Equal(LockResponseType.Locked, lockType);
                Assert.True(token > lastToken,
                    $"round {round}: token {token} did not advance past {lastToken}");
                lastToken = token;

                (LockResponseType getType, Kahuna.Server.Locks.Data.ReadOnlyLockEntry? entry) =
                    await RetryGetLock(() => nodes[0].Kahuna.LocateAndGetLock(resource, LockDurability.Persistent, ct));
                Assert.Equal(LockResponseType.Got, getType);
                Assert.NotNull(entry);
                Assert.Equal(token, entry!.FencingToken);
                Assert.Equal(owner, entry.Owner);

                LockResponseType unlockType = await RetryUnlock(() =>
                    nodes[0].Kahuna.LocateAndTryUnlock(resource, owner, LockDurability.Persistent, ct));
                Assert.Equal(LockResponseType.Unlocked, unlockType);

                for (int p = 1; p <= 2; p++)
                {
                    Node oldLeader = await LeaderOf(p, nodes);
                    await oldLeader.Raft.StepDownAsync(p, ct);
                }

                for (int p = 1; p <= 2; p++)
                    await WaitForAnyLeader(p, nodes[0].Raft, nodes[1].Raft, nodes[2].Raft);
            }
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }

    private static async Task<(LockResponseType, Kahuna.Server.Locks.Data.ReadOnlyLockEntry?)> RetryGetLock(
        Func<Task<(LockResponseType, Kahuna.Server.Locks.Data.ReadOnlyLockEntry?)>> op, int timeoutMs = 10_000)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        long deadline = Environment.TickCount64 + (long)(timeoutMs * TimingScale);
        while (true)
        {
            (LockResponseType type, Kahuna.Server.Locks.Data.ReadOnlyLockEntry? entry) = await op();
            if (type != LockResponseType.MustRetry || Environment.TickCount64 >= deadline)
                return (type, entry);
            await Task.Delay(50, ct);
        }
    }
}
