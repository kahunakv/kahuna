
using Kahuna.Server.Communication.Internode;
using Kahuna.Server.Configuration;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Communication.Memory;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;
using Nixie;

namespace Kahuna.Server.Tests;

public abstract class BaseCluster
{
    /// <summary>
    /// Base for the per-node deterministic election seeds (Kommander 0.10.16 <c>ElectionTimeoutSeed</c>).
    /// Each node adds its NodeId so the three nodes get distinct, reproducible election timers — a
    /// shared seed would make all nodes time out together (split-vote deadlock).
    /// </summary>
    private const int ElectionTimeoutSeedBase = 91000;

    private static (IRaft, IKahuna) GetNode1(MemoryInterNodeCommmunication interNodeCommmunication, InMemoryCommunication communication, string walStorage, int partitions, ILogger<IRaft> raftLogger, ILogger<IKahuna> kahunaLogger, Action<KahunaConfiguration>? configure = null)
    {
        IWAL wal = GetWAL(walStorage, raftLogger);

        ActorSystem actorSystem = new(logger: raftLogger);

        RaftConfiguration config = new()
        {
            NodeName = "kahuna1",
            NodeId = 1,
            Host = "localhost",
            Port = 8001,
            InitialPartitions = partitions,
            StartElectionTimeout = (int)(50 * TimingScale),
            EndElectionTimeout = (int)(150 * TimingScale),
            // Deterministic election timers (Kommander 0.10.16): each node uses a DISTINCT seed so
            // their per-partition timeouts differ (a shared seed yields identical timers → split-vote
            // deadlock). Fixed seeds make elections reproducible — no run-to-run split-vote retries,
            // which is the main source of cluster-test flakiness/variance.
            ElectionTimeoutSeed = ElectionTimeoutSeedBase + 1,
            CompactEveryOperations = 1000,
            CompactNumberEntries = 50,
            // Fast election timers (50–150 ms) sit below the default SWIM PingInterval (1 s), which
            // quiescence requires to be < StartElectionTimeout. Keep the classic per-partition
            // heartbeat model in tests so RaftConfiguration.Validate() does not reject the fast timers.
            EnableQuiescence = false
        };

        RaftManager raft = new(
            config,
            new StaticDiscovery([new("localhost:8002"), new("localhost:8003")]),
            wal,
            communication,
            new HybridLogicalClock(),
            raftLogger
        );

        KahunaConfiguration configuration = new()
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
        configure?.Invoke(configuration);

        // ActorSystem actorSystem, IRaft raft, KahunaConfiguration configuration, ILogger<IKahuna> logger
        KahunaManager kahuna = new(actorSystem, raft, configuration, interNodeCommmunication, kahunaLogger);

        raft.OnLogRestored += kahuna.OnLogRestored;
        raft.OnReplicationReceived += kahuna.OnReplicationReceived;
        raft.OnReplicationError += kahuna.OnReplicationError;

        return (raft, kahuna);
    }
    
    private static (IRaft, IKahuna) GetNode2(MemoryInterNodeCommmunication interNodeCommmunication, InMemoryCommunication communication, string walStorage, int partitions, ILogger<IRaft> raftLogger, ILogger<IKahuna> kahunaLogger, Action<KahunaConfiguration>? configure = null)
    {
        IWAL wal = GetWAL(walStorage, raftLogger);

        ActorSystem actorSystem = new(logger: raftLogger);

        RaftConfiguration config = new()
        {
            NodeName = "kahuna2",
            NodeId = 2,
            Host = "localhost",
            Port = 8002,
            InitialPartitions = partitions,
            StartElectionTimeout = (int)(50 * TimingScale),
            EndElectionTimeout = (int)(150 * TimingScale),
            ElectionTimeoutSeed = ElectionTimeoutSeedBase + 2, // distinct per node (see GetNode1)
            CompactEveryOperations = 1000,
            CompactNumberEntries = 50,
            // Fast election timers (50–150 ms) sit below the default SWIM PingInterval (1 s), which
            // quiescence requires to be < StartElectionTimeout. Keep the classic per-partition
            // heartbeat model in tests so RaftConfiguration.Validate() does not reject the fast timers.
            EnableQuiescence = false
        };

        RaftManager raft = new(
            config,
            new StaticDiscovery([new("localhost:8001"), new("localhost:8003")]),
            wal,
            communication,
            new HybridLogicalClock(),
            raftLogger
        );

        KahunaConfiguration configuration = new()
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
        configure?.Invoke(configuration);

        // ActorSystem actorSystem, IRaft raft, KahunaConfiguration configuration, ILogger<IKahuna> logger
        KahunaManager kahuna = new(actorSystem, raft, configuration, interNodeCommmunication, kahunaLogger);

        raft.OnLogRestored += kahuna.OnLogRestored;
        raft.OnReplicationReceived += kahuna.OnReplicationReceived;
        raft.OnReplicationError += kahuna.OnReplicationError;

        return (raft, kahuna);
    }
    
    private static (IRaft, IKahuna) GetNode3(MemoryInterNodeCommmunication interNodeCommmunication, InMemoryCommunication communication, string walStorage, int partitions, ILogger<IRaft> raftLogger, ILogger<IKahuna> kahunaLogger, Action<KahunaConfiguration>? configure = null)
    {
        IWAL wal = GetWAL(walStorage, raftLogger);

        ActorSystem actorSystem = new(logger: raftLogger);

        RaftConfiguration config = new()
        {
            NodeName = "kahuna3",
            NodeId = 3,
            Host = "localhost",
            Port = 8003,
            InitialPartitions = partitions,
            StartElectionTimeout = (int)(50 * TimingScale),
            EndElectionTimeout = (int)(150 * TimingScale),
            ElectionTimeoutSeed = ElectionTimeoutSeedBase + 3, // distinct per node (see GetNode1)
            CompactEveryOperations = 1000,
            CompactNumberEntries = 50,
            // Fast election timers (50–150 ms) sit below the default SWIM PingInterval (1 s), which
            // quiescence requires to be < StartElectionTimeout. Keep the classic per-partition
            // heartbeat model in tests so RaftConfiguration.Validate() does not reject the fast timers.
            EnableQuiescence = false
        };

        RaftManager raft = new(
            config,
            new StaticDiscovery([new("localhost:8001"), new("localhost:8002")]),
            wal,
            communication,
            new HybridLogicalClock(),
            raftLogger
        );

        KahunaConfiguration configuration = new()
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
        configure?.Invoke(configuration);

        // ActorSystem actorSystem, IRaft raft, KahunaConfiguration configuration, ILogger<IKahuna> logger
        KahunaManager kahuna = new(actorSystem, raft, configuration, interNodeCommmunication, kahunaLogger);

        raft.OnLogRestored += kahuna.OnLogRestored;
        raft.OnReplicationReceived += kahuna.OnReplicationReceived;
        raft.OnReplicationError += kahuna.OnReplicationError;

        return (raft, kahuna);
    }
    
    protected static async Task<(IRaft, IRaft, IRaft, IKahuna, IKahuna, IKahuna)> AssembleThreNodeCluster(string walStorage, int partitions, ILogger<IRaft> raftLogger, ILogger<IKahuna> kahunaLogger, Action<KahunaConfiguration>? configure = null)
    {
        InMemoryCommunication raftCommunication = new();
        MemoryInterNodeCommmunication interNodeCommmunication = new();

        (IRaft raft1, IKahuna kahuna1) = GetNode1(interNodeCommmunication, raftCommunication, walStorage, partitions, raftLogger, kahunaLogger, configure);
        (IRaft raft2, IKahuna kahuna2) = GetNode2(interNodeCommmunication, raftCommunication, walStorage, partitions, raftLogger, kahunaLogger, configure);
        (IRaft raft3, IKahuna kahuna3) = GetNode3(interNodeCommmunication, raftCommunication, walStorage, partitions, raftLogger, kahunaLogger, configure);

        await WaitForClusterToAssemble(interNodeCommmunication, raftCommunication, partitions, raft1, raft2, raft3, kahuna1, kahuna2, kahuna3);

        return (raft1, raft2, raft3, kahuna1, kahuna2, kahuna3);
    }

    /// <summary>
    /// Variant that also returns the shared <see cref="MemoryInterNodeCommmunication"/> instance so
    /// tests can inspect transport-level counters (e.g. <see cref="MemoryInterNodeCommmunication.GetByRangeCallCount"/>).
    /// </summary>
    protected static async Task<(IRaft, IRaft, IRaft, IKahuna, IKahuna, IKahuna, MemoryInterNodeCommmunication)> AssembleThreNodeClusterWithTransport(string walStorage, int partitions, ILogger<IRaft> raftLogger, ILogger<IKahuna> kahunaLogger)
    {
        InMemoryCommunication raftCommunication = new();
        MemoryInterNodeCommmunication interNodeCommmunication = new();

        (IRaft raft1, IKahuna kahuna1) = GetNode1(interNodeCommmunication, raftCommunication, walStorage, partitions, raftLogger, kahunaLogger);
        (IRaft raft2, IKahuna kahuna2) = GetNode2(interNodeCommmunication, raftCommunication, walStorage, partitions, raftLogger, kahunaLogger);
        (IRaft raft3, IKahuna kahuna3) = GetNode3(interNodeCommmunication, raftCommunication, walStorage, partitions, raftLogger, kahunaLogger);

        await WaitForClusterToAssemble(interNodeCommmunication, raftCommunication, partitions, raft1, raft2, raft3, kahuna1, kahuna2, kahuna3);

        return (raft1, raft2, raft3, kahuna1, kahuna2, kahuna3, interNodeCommmunication);
    }

    /// <summary>
    /// Like <see cref="AssembleThreNodeCluster"/> but exposes both the Raft and inter-node transports so
    /// tests can add or remove nodes after the initial cluster forms.
    /// </summary>
    protected static async Task<(IRaft, IRaft, IRaft, IKahuna, IKahuna, IKahuna, InMemoryCommunication, MemoryInterNodeCommmunication)> AssembleThreeNodeClusterFull(
        string walStorage, int partitions,
        ILogger<IRaft> raftLogger, ILogger<IKahuna> kahunaLogger)
    {
        InMemoryCommunication raftCommunication = new();
        MemoryInterNodeCommmunication interNodeCommmunication = new();

        (IRaft raft1, IKahuna kahuna1) = GetNode1(interNodeCommmunication, raftCommunication, walStorage, partitions, raftLogger, kahunaLogger);
        (IRaft raft2, IKahuna kahuna2) = GetNode2(interNodeCommmunication, raftCommunication, walStorage, partitions, raftLogger, kahunaLogger);
        (IRaft raft3, IKahuna kahuna3) = GetNode3(interNodeCommmunication, raftCommunication, walStorage, partitions, raftLogger, kahunaLogger);

        await WaitForClusterToAssemble(interNodeCommmunication, raftCommunication, partitions, raft1, raft2, raft3, kahuna1, kahuna2, kahuna3);

        return (raft1, raft2, raft3, kahuna1, kahuna2, kahuna3, raftCommunication, interNodeCommmunication);
    }

    /// <summary>
    /// Creates a single Raft+Kahuna node with the given identity and wires it into the shared transports.
    /// Does NOT call <c>JoinCluster</c> — callers are responsible for that step.
    /// </summary>
    protected static (IRaft, IKahuna) BuildNode(
        MemoryInterNodeCommmunication interNodeComm,
        InMemoryCommunication raftComm,
        string walStorage,
        int nodeId,
        int port,
        IEnumerable<string> peers,
        ILogger<IRaft> raftLogger,
        ILogger<IKahuna> kahunaLogger,
        int initialPartitions = 3)
    {
        IWAL wal = GetWAL(walStorage, raftLogger);
        ActorSystem actorSystem = new(logger: raftLogger);

        RaftConfiguration config = new()
        {
            NodeName = $"kahuna{nodeId}",
            NodeId = nodeId,
            Host = "localhost",
            Port = port,
            InitialPartitions = initialPartitions,
            StartElectionTimeout = (int)(50 * TimingScale),
            EndElectionTimeout = (int)(150 * TimingScale),
            ElectionTimeoutSeed = ElectionTimeoutSeedBase + nodeId,
            CompactEveryOperations = 1000,
            CompactNumberEntries = 50,
            EnableQuiescence = false
        };

        RaftManager raft = new(
            config,
            new StaticDiscovery(peers.Select(p => new RaftNode(p)).ToList()),
            wal,
            raftComm,
            new HybridLogicalClock(),
            raftLogger
        );

        KahunaConfiguration configuration = new()
        {
            LocksWorkers = 8,
            KeyValueWorkers = 8,
            BackgroundWriterWorkers = 1,
            Storage = "memory",
            StoragePath = "/tmp",
            StorageRevision = Guid.NewGuid().ToString(),
            DefaultTransactionTimeout = 5000,
            ScriptCacheExpiration = TimeSpan.FromMinutes(1)
        };

        KahunaManager kahuna = new(actorSystem, raft, configuration, interNodeComm, kahunaLogger);

        raft.OnLogRestored += kahuna.OnLogRestored;
        raft.OnReplicationReceived += kahuna.OnReplicationReceived;
        raft.OnReplicationError += kahuna.OnReplicationError;

        return (raft, kahuna);
    }

    /// <summary>
    /// Like <see cref="BuildNode"/> but accepts a pre-seeded <paramref name="preSeededWal"/> and
    /// <paramref name="preSeededBackend"/> instead of creating fresh instances. Used by PITR
    /// bootstrap tests where the WAL and persistence backend are populated by
    /// <c>BootstrapHelper.BootstrapNode</c> before the node joins the cluster.
    /// </summary>
    internal static (IRaft, IKahuna) BuildNodeWithExternalWal(
        MemoryInterNodeCommmunication interNodeComm,
        InMemoryCommunication raftComm,
        IWAL preSeededWal,
        IPersistenceBackend preSeededBackend,
        int nodeId,
        int port,
        IEnumerable<string> peers,
        ILogger<IRaft> raftLogger,
        ILogger<IKahuna> kahunaLogger,
        int initialPartitions = 3)
    {
        ActorSystem actorSystem = new(logger: raftLogger);

        RaftConfiguration config = new()
        {
            NodeName = $"kahuna{nodeId}",
            NodeId = nodeId,
            Host = "localhost",
            Port = port,
            InitialPartitions = initialPartitions,
            StartElectionTimeout = (int)(50 * TimingScale),
            EndElectionTimeout = (int)(150 * TimingScale),
            ElectionTimeoutSeed = ElectionTimeoutSeedBase + nodeId,
            CompactEveryOperations = 1000,
            CompactNumberEntries = 50,
            EnableQuiescence = false
        };

        RaftManager raft = new(
            config,
            new StaticDiscovery(peers.Select(p => new RaftNode(p)).ToList()),
            preSeededWal,
            raftComm,
            new HybridLogicalClock(),
            raftLogger
        );

        KahunaConfiguration configuration = new()
        {
            LocksWorkers = 8,
            KeyValueWorkers = 8,
            BackgroundWriterWorkers = 1,
            Storage = "memory",
            StoragePath = "/tmp",
            StorageRevision = Guid.NewGuid().ToString(),
            DefaultTransactionTimeout = 5000,
            ScriptCacheExpiration = TimeSpan.FromMinutes(1)
        };

        KahunaManager kahuna = new(actorSystem, raft, configuration, interNodeComm, preSeededBackend, kahunaLogger);

        raft.OnLogRestored += kahuna.OnLogRestored;
        raft.OnReplicationReceived += kahuna.OnReplicationReceived;
        raft.OnReplicationError += kahuna.OnReplicationError;

        return (raft, kahuna);
    }

    /// <summary>
    /// Like <see cref="AssembleThreeNodeClusterFull"/> but with SWIM enabled for failure-detector tests.
    /// Uses scaled-down timeouts that sit above the fast election timers so the detector fires without
    /// causing false evictions of healthy nodes.
    /// </summary>
    protected static async Task<(IRaft, IRaft, IRaft, IKahuna, IKahuna, IKahuna, InMemoryCommunication, MemoryInterNodeCommmunication)> AssembleSwimCluster(
        string walStorage, int partitions,
        ILogger<IRaft> raftLogger, ILogger<IKahuna> kahunaLogger,
        int pingIntervalMs = 200,
        int pingTimeoutMs = 100,
        int suspicionTimeoutMs = 400,
        int deadMemberEvictionGraceMs = 300,
        int updateNodesIntervalMs = 300)
    {
        InMemoryCommunication raftCommunication = new();
        MemoryInterNodeCommmunication interNodeCommmunication = new();

        (IRaft raft1, IKahuna kahuna1) = BuildSwimNode(interNodeCommmunication, raftCommunication, walStorage, partitions, 1, 8001, ["localhost:8002", "localhost:8003"], raftLogger, kahunaLogger, pingIntervalMs, pingTimeoutMs, suspicionTimeoutMs, deadMemberEvictionGraceMs, updateNodesIntervalMs);
        (IRaft raft2, IKahuna kahuna2) = BuildSwimNode(interNodeCommmunication, raftCommunication, walStorage, partitions, 2, 8002, ["localhost:8001", "localhost:8003"], raftLogger, kahunaLogger, pingIntervalMs, pingTimeoutMs, suspicionTimeoutMs, deadMemberEvictionGraceMs, updateNodesIntervalMs);
        (IRaft raft3, IKahuna kahuna3) = BuildSwimNode(interNodeCommmunication, raftCommunication, walStorage, partitions, 3, 8003, ["localhost:8001", "localhost:8002"], raftLogger, kahunaLogger, pingIntervalMs, pingTimeoutMs, suspicionTimeoutMs, deadMemberEvictionGraceMs, updateNodesIntervalMs);

        await WaitForClusterToAssemble(interNodeCommmunication, raftCommunication, partitions, raft1, raft2, raft3, kahuna1, kahuna2, kahuna3);

        return (raft1, raft2, raft3, kahuna1, kahuna2, kahuna3, raftCommunication, interNodeCommmunication);
    }

    private static (IRaft, IKahuna) BuildSwimNode(
        MemoryInterNodeCommmunication interNodeComm,
        InMemoryCommunication raftComm,
        string walStorage,
        int partitions,
        int nodeId,
        int port,
        IEnumerable<string> peers,
        ILogger<IRaft> raftLogger,
        ILogger<IKahuna> kahunaLogger,
        int pingIntervalMs,
        int pingTimeoutMs,
        int suspicionTimeoutMs,
        int deadMemberEvictionGraceMs,
        int updateNodesIntervalMs)
    {
        IWAL wal = GetWAL(walStorage, raftLogger);
        ActorSystem actorSystem = new(logger: raftLogger);

        RaftConfiguration config = new()
        {
            NodeName = $"kahuna{nodeId}",
            NodeId = nodeId,
            Host = "localhost",
            Port = port,
            InitialPartitions = partitions,
            StartElectionTimeout = (int)(50 * TimingScale),
            EndElectionTimeout = (int)(150 * TimingScale),
            ElectionTimeoutSeed = ElectionTimeoutSeedBase + nodeId,
            CompactEveryOperations = 1000,
            CompactNumberEntries = 50,
            EnableQuiescence = false,
            PingInterval = TimeSpan.FromMilliseconds(pingIntervalMs * TimingScale),
            PingTimeout = TimeSpan.FromMilliseconds(pingTimeoutMs * TimingScale),
            SuspicionTimeout = TimeSpan.FromMilliseconds(suspicionTimeoutMs * TimingScale),
            DeadMemberEvictionGrace = TimeSpan.FromMilliseconds(deadMemberEvictionGraceMs * TimingScale),
            UpdateNodesInterval = TimeSpan.FromMilliseconds(updateNodesIntervalMs * TimingScale)
        };

        RaftManager raft = new(
            config,
            new StaticDiscovery(peers.Select(p => new RaftNode(p)).ToList()),
            wal,
            raftComm,
            new HybridLogicalClock(),
            raftLogger
        );

        KahunaConfiguration configuration = new()
        {
            LocksWorkers = 8,
            KeyValueWorkers = 8,
            BackgroundWriterWorkers = 1,
            Storage = "memory",
            StoragePath = "/tmp",
            StorageRevision = Guid.NewGuid().ToString(),
            DefaultTransactionTimeout = 5000,
            ScriptCacheExpiration = TimeSpan.FromMinutes(1)
        };

        KahunaManager kahuna = new(actorSystem, raft, configuration, interNodeComm, kahunaLogger);

        raft.OnLogRestored += kahuna.OnLogRestored;
        raft.OnReplicationReceived += kahuna.OnReplicationReceived;
        raft.OnReplicationError += kahuna.OnReplicationError;

        return (raft, kahuna);
    }

    /// <summary>
    /// Like <see cref="AssembleSwimCluster"/> but with the advisory leader balancer enabled on every
    /// node. SWIM must be on because load reports ride the gossip path; the balancer intervals are
    /// scaled far below their production defaults so several planning passes elapse within a test.
    /// SWIM suspicion/eviction windows are kept generous so healthy nodes are never evicted while the
    /// balancer runs.
    /// </summary>
    protected static async Task<(IRaft, IRaft, IRaft, IKahuna, IKahuna, IKahuna, InMemoryCommunication, MemoryInterNodeCommmunication)> AssembleLeaderBalancerCluster(
        string walStorage, int partitions,
        ILogger<IRaft> raftLogger, ILogger<IKahuna> kahunaLogger)
    {
        InMemoryCommunication raftCommunication = new();
        MemoryInterNodeCommmunication interNodeCommmunication = new();

        (IRaft raft1, IKahuna kahuna1) = BuildLeaderBalancerNode(interNodeCommmunication, raftCommunication, walStorage, partitions, 1, 8001, ["localhost:8002", "localhost:8003"], raftLogger, kahunaLogger);
        (IRaft raft2, IKahuna kahuna2) = BuildLeaderBalancerNode(interNodeCommmunication, raftCommunication, walStorage, partitions, 2, 8002, ["localhost:8001", "localhost:8003"], raftLogger, kahunaLogger);
        (IRaft raft3, IKahuna kahuna3) = BuildLeaderBalancerNode(interNodeCommmunication, raftCommunication, walStorage, partitions, 3, 8003, ["localhost:8001", "localhost:8002"], raftLogger, kahunaLogger);

        await WaitForClusterToAssemble(interNodeCommmunication, raftCommunication, partitions, raft1, raft2, raft3, kahuna1, kahuna2, kahuna3);

        return (raft1, raft2, raft3, kahuna1, kahuna2, kahuna3, raftCommunication, interNodeCommmunication);
    }

    private static (IRaft, IKahuna) BuildLeaderBalancerNode(
        MemoryInterNodeCommmunication interNodeComm,
        InMemoryCommunication raftComm,
        string walStorage,
        int partitions,
        int nodeId,
        int port,
        IEnumerable<string> peers,
        ILogger<IRaft> raftLogger,
        ILogger<IKahuna> kahunaLogger)
    {
        IWAL wal = GetWAL(walStorage, raftLogger);
        ActorSystem actorSystem = new(logger: raftLogger);

        RaftConfiguration config = new()
        {
            NodeName = $"kahuna{nodeId}",
            NodeId = nodeId,
            Host = "localhost",
            Port = port,
            InitialPartitions = partitions,
            StartElectionTimeout = (int)(50 * TimingScale),
            EndElectionTimeout = (int)(150 * TimingScale),
            ElectionTimeoutSeed = ElectionTimeoutSeedBase + nodeId,
            CompactEveryOperations = 1000,
            CompactNumberEntries = 50,
            EnableQuiescence = false,
            // SWIM on (load reports ride gossip); generous suspicion/eviction so healthy nodes stay.
            PingInterval = TimeSpan.FromMilliseconds(200 * TimingScale),
            PingTimeout = TimeSpan.FromMilliseconds(100 * TimingScale),
            SuspicionTimeout = TimeSpan.FromMilliseconds(5000 * TimingScale),
            DeadMemberEvictionGrace = TimeSpan.FromMilliseconds(5000 * TimingScale),
            UpdateNodesInterval = TimeSpan.FromMilliseconds(300 * TimingScale),
            // Advisory leader balancer, intervals scaled down so passes run within the test window.
            EnableLeaderBalancer = true,
            LeaderBalancerReportInterval = TimeSpan.FromMilliseconds(200 * TimingScale),
            LeaderBalancerReportTtl = TimeSpan.FromMilliseconds(2000 * TimingScale),
            LeaderBalancerInterval = TimeSpan.FromMilliseconds(300 * TimingScale),
            MinLeaderStabilityMs = (long)(200 * TimingScale),
            MoveCooldown = TimeSpan.FromMilliseconds(500 * TimingScale),
            SuggestionTimeout = TimeSpan.FromMilliseconds(1500 * TimingScale)
        };

        RaftManager raft = new(
            config,
            new StaticDiscovery(peers.Select(p => new RaftNode(p)).ToList()),
            wal,
            raftComm,
            new HybridLogicalClock(),
            raftLogger
        );

        KahunaConfiguration configuration = new()
        {
            LocksWorkers = 8,
            KeyValueWorkers = 8,
            BackgroundWriterWorkers = 1,
            Storage = "memory",
            StoragePath = "/tmp",
            StorageRevision = Guid.NewGuid().ToString(),
            DefaultTransactionTimeout = 5000,
            ScriptCacheExpiration = TimeSpan.FromMinutes(1)
        };

        KahunaManager kahuna = new(actorSystem, raft, configuration, interNodeComm, kahunaLogger);

        raft.OnLogRestored += kahuna.OnLogRestored;
        raft.OnReplicationReceived += kahuna.OnReplicationReceived;
        raft.OnReplicationError += kahuna.OnReplicationError;

        return (raft, kahuna);
    }

    /// <summary>
    /// Tears down a single node cleanly, for use in leave / eviction tests.
    /// </summary>
    protected static async Task LeaveClusterSingle(IRaft raft)
    {
        try { await raft.LeaveCluster(dispose: true); }
        catch (ObjectDisposedException) { }
    }
    
    private static async Task WaitForClusterToAssemble(
        MemoryInterNodeCommmunication interNodeCommmunication, 
        InMemoryCommunication communication, 
        int partitions, 
        IRaft raft1, 
        IRaft raft2, 
        IRaft raft3,
        IKahuna kahuna1,
        IKahuna kahuna2,
        IKahuna kahuna3
    )
    {
        interNodeCommmunication.SetNodes(new()
        {
            { "localhost:8001", kahuna1 }, 
            { "localhost:8002", kahuna2 },
            { "localhost:8003", kahuna3 }
        });
        
        communication.SetNodes(new()
        {
            { "localhost:8001", raft1 }, 
            { "localhost:8002", raft2 },
            { "localhost:8003", raft3 }
        });
        
        await Task.WhenAll(raft1.JoinCluster(), raft2.JoinCluster(), raft3.JoinCluster());

        using CancellationTokenSource assemblyCts = CancellationTokenSource.CreateLinkedTokenSource(
            TestContext.Current.CancellationToken);
        assemblyCts.CancelAfter(TimeSpan.FromSeconds(90 * TimingScale));

        for (int i = 0; i <= partitions; i++)
        {
            while (true)
            {
                assemblyCts.Token.ThrowIfCancellationRequested();

                if (await raft1.AmILeader(i, cancellationToken: assemblyCts.Token) ||
                    await raft2.AmILeader(i, cancellationToken: assemblyCts.Token) ||
                    await raft3.AmILeader(i, cancellationToken: assemblyCts.Token))
                    break;

                await Task.Delay(50, cancellationToken: assemblyCts.Token);
            }
        }
    }

    /// <summary>
    /// Retries <paramref name="body"/> up to <paramref name="maxAttempts"/> times, swallowing
    /// all exceptions on non-final attempts. Use for cluster tests that can fail transiently due
    /// to Raft leader-election timing on loaded CI runners.
    /// </summary>
    protected static async Task RetryAsync(Func<Task> body, int maxAttempts = 3)
    {
        for (int attempt = 1; attempt < maxAttempts; attempt++)
        {
            try { await body(); return; }
            catch (OperationCanceledException) { throw; }  // test cancellation must propagate
            catch (Xunit.Sdk.XunitException) { throw; }   // assertion failures are not transient
            catch { /* swallow transient cluster errors — next attempt */ }
        }
        await body(); // final attempt: let all exceptions propagate
    }

    /// <summary>
    /// Runs a key/value operation and re-runs it while it resolves to
    /// <see cref="KeyValueResponseType.MustRetry"/>, up to a deadline (scaled by
    /// <c>KAHUNA_TEST_TIMING_SCALE</c>). MustRetry is the retryable signal the write path returns
    /// when a proposal races a leadership change (NodeIsNotLeader) or another transient replication
    /// condition; retrying lets the operation land once the partition settles instead of failing the
    /// test on a transient blip. The final result is returned so the caller asserts on it — a genuine
    /// non-retryable outcome (or a persistent MustRetry past the deadline) still surfaces normally.
    /// </summary>
    protected static async Task<T> RetryOnMustRetryAsync<T>(
        Func<Task<T>> operation, Func<T, KeyValueResponseType> statusSelector, int timeoutMs = 10_000)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        long deadline = Environment.TickCount64 + (long)(timeoutMs * TimingScale);

        T result = await operation();
        while (statusSelector(result) == KeyValueResponseType.MustRetry && Environment.TickCount64 < deadline)
        {
            await Task.Delay(50, ct);
            result = await operation();
        }

        return result;
    }

    private static readonly double TimingScale = GetTimingScale();
    private static double GetTimingScale()
    {
        string? val = Environment.GetEnvironmentVariable("KAHUNA_TEST_TIMING_SCALE");
        return val is not null && double.TryParse(val, out double s) && s >= 1.0 ? s : 1.0;
    }

    /// <summary>
    /// Polls <paramref name="predicate"/> every 50 ms until it returns true or the deadline
    /// (scaled by <c>KAHUNA_TEST_TIMING_SCALE</c>) is reached; throws <see cref="TimeoutException"/> on expiry.
    /// </summary>
    protected static async Task WaitUntilAsync(Func<bool> predicate, int timeoutMs = 10_000)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        long deadline = Environment.TickCount64 + (long)(timeoutMs * TimingScale);
        while (Environment.TickCount64 < deadline)
        {
            if (predicate()) return;
            await Task.Delay(50, ct);
        }
        throw new TimeoutException($"Timed out after {timeoutMs * TimingScale} ms waiting for condition.");
    }

    /// <summary>
    /// Async-predicate overload of <see cref="WaitUntilAsync(Func{bool}, int)"/>.
    /// </summary>
    protected static async Task WaitUntilAsync(Func<Task<bool>> predicate, int timeoutMs = 10_000)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        long deadline = Environment.TickCount64 + (long)(timeoutMs * TimingScale);
        while (Environment.TickCount64 < deadline)
        {
            if (await predicate()) return;
            await Task.Delay(50, ct);
        }
        throw new TimeoutException($"Timed out after {timeoutMs * TimingScale} ms waiting for condition.");
    }

    protected static async Task LeaveCluster(IRaft raft1, IRaft raft2, IRaft raft3)
    {
        await Task.WhenAll(
            LeaveCluster(raft1), 
            LeaveCluster(raft2),
            LeaveCluster(raft3)
        );
    }

    private static async Task LeaveCluster(IRaft raft)
    {
        try
        {
            await raft.LeaveCluster(dispose: true);
        }
        catch (ObjectDisposedException)
        {
            // Kommander may already have torn down its internal cancellation source during shutdown.
        }
    }

    private static IWAL GetWAL(string walStorage, ILogger<IRaft> logger)
    {
        return walStorage switch
        {
            "memory" => new InMemoryWAL(logger),
            "sqlite" => new SqliteWAL("/tmp", Guid.NewGuid().ToString(), logger, syncWrites: false),
            "rocksdb" => new RocksDbWAL("/tmp", Guid.NewGuid().ToString(), logger, syncWrites: false),
            _ => throw new ArgumentException($"Unknown wal: {walStorage}")
        };
    }
    
    private static async Task<IRaft?> GetLeader(int partitionId, IRaft[] nodes)
    {
        foreach (IRaft node in nodes)
        {
            if (await node.AmILeader(partitionId, CancellationToken.None))
                return node;
        }

        return null;
    }
    
    private static async Task<List<IRaft>> GetFollowers(IRaft[] nodes)
    {
        List<IRaft> followers = [];
        
        foreach (IRaft node in nodes)
        {
            if (!await node.AmILeader(1, CancellationToken.None))
                followers.Add(node);
        }

        return followers;
    }
}
