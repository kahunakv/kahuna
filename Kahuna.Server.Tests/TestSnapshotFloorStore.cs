
using System.Diagnostics.Metrics;
using Kahuna.Server.Communication.Internode;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.Replication;
using Kahuna.Server.Replication.Protos;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Nixie;

namespace Kahuna.Server.Tests;

/// <summary>
/// Tests for <see cref="SnapshotFloorStore"/>: the replicated, refcounted, leased MVCC snapshot-floor
/// registry.
///
/// <para><b>Unit tests (no cluster).</b> The store's state machine — hold registry, effective-floor
/// computation, live/expiry logic, and proto round-trips — is exercised by injecting Raft log entries
/// directly via <see cref="SnapshotFloorStore.Restore"/> without starting a cluster.</para>
///
/// <para><b>Multi-node integration tests.</b> Acquire/renew/release operations that require real
/// <c>ReplicateLogs</c> are exercised by a 3-node in-memory cluster, mirroring the pattern in
/// <see cref="TestRangeMapReplication"/>.</para>
/// </summary>
[Collection("ClusterTests")]
public sealed class TestSnapshotFloorStore
{
    private readonly ILogger<IKahuna> kahunaLogger = NullLogger<IKahuna>.Instance;
    private readonly ILogger<IRaft> raftLogger = NullLogger<IRaft>.Instance;

    // ── helpers ─────────────────────────────────────────────────────────────────────────────

    private static readonly double TimingScale = GetTimingScale();
    private static double GetTimingScale()
    {
        string? val = Environment.GetEnvironmentVariable("KAHUNA_TEST_TIMING_SCALE");
        return val is not null && double.TryParse(val, out double s) && s >= 1.0 ? s : 1.0;
    }

    private sealed record Node(RaftManager Raft, KahunaManager Kahuna);

    private const int ElectionTimeoutSeedBase = 72000;

    private (RaftManager, SnapshotFloorStore) CreateSingleNodeStore()
    {
        RaftManager raft = new(
            new RaftConfiguration
            {
                NodeName = "floor-test",
                NodeId = 1,
                Host = "localhost",
                Port = 0,
                InitialPartitions = 1,
                EnableQuiescence = false
            },
            new StaticDiscovery([]),
            new InMemoryWAL(raftLogger),
            new InMemoryCommunication(),
            new HybridLogicalClock(),
            raftLogger
        );
        // storagePath = null disables the durable snapshot (fine for unit tests).
        SnapshotFloorStore store = new(raft, null, null, kahunaLogger);
        return (raft, store);
    }

    /// <summary>Builds a delta log entry (one upsert per hold) and injects it as a restore.</summary>
    private static bool InjectHolds(SnapshotFloorStore store, IEnumerable<SnapshotHold> h)
    {
        SnapshotFloorDeltaMessage delta = new();
        foreach (SnapshotHold hold in h)
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
        byte[] data = ReplicationSerializer.Serialize(delta);
        RaftLog log = new() { LogType = ReplicationTypes.SnapshotFloor, LogData = data };
        return store.Restore(RangeMapStore.MetaPartitionId, log);
    }

    /// <summary>Builds a delta log entry that removes the given holdIds and injects it as a restore.</summary>
    private static bool InjectRemove(SnapshotFloorStore store, params string[] holdIds)
    {
        SnapshotFloorDeltaMessage delta = new();
        foreach (string id in holdIds)
            delta.Entries.Add(new SnapshotFloorDeltaEntry { Remove = true, Hold = new SnapshotHoldMessage { HoldId = id } });
        byte[] data = ReplicationSerializer.Serialize(delta);
        RaftLog log = new() { LogType = ReplicationTypes.SnapshotFloor, LogData = data };
        return store.Restore(RangeMapStore.MetaPartitionId, log);
    }

    // ── State-machine unit tests ─────────────────────────────────────────────────────────────

    [Fact]
    public void EmptyRegistry_EffectiveFloor_IsZero()
    {
        (RaftManager raft, SnapshotFloorStore store) = CreateSingleNodeStore();
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        Assert.Equal(HLCTimestamp.Zero, store.GetEffectiveFloor(now));
        Assert.Empty(store.Holds);
    }

    [Fact]
    public void SingleLiveHold_EffectiveFloor_EqualsItsTimestamp()
    {
        (RaftManager raft, SnapshotFloorStore store) = CreateSingleNodeStore();
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        HLCTimestamp ts = new(1, 1000, 0);
        HLCTimestamp expiry = new(1, now.L + 60_000, 0); // 60 s from now

        bool ok = InjectHolds(store, [new SnapshotHold("h1", "client-a", ts, expiry)]);

        Assert.True(ok);
        Assert.Equal(ts, store.GetEffectiveFloor(now));
    }

    [Fact]
    public void MultipleHolds_EffectiveFloor_IsMinTimestamp()
    {
        (RaftManager raft, SnapshotFloorStore store) = CreateSingleNodeStore();
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        HLCTimestamp t1 = new(1, 1000, 0);
        HLCTimestamp t2 = new(1, 2000, 0);
        HLCTimestamp t3 = new(1, 3000, 0);
        HLCTimestamp expiry = new(1, now.L + 60_000, 0);

        InjectHolds(store,
        [
            new SnapshotHold("h1", "c1", t1, expiry),
            new SnapshotHold("h2", "c2", t2, expiry),
            new SnapshotHold("h3", "c3", t3, expiry),
        ]);

        Assert.Equal(t1, store.GetEffectiveFloor(now));
    }

    [Fact]
    public void ExpiredHold_ExcludedFromFloor()
    {
        (RaftManager raft, SnapshotFloorStore store) = CreateSingleNodeStore();
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        // Expired hold: leaseExpiry is in the past.
        HLCTimestamp expiredExpiry = new(1, now.L - 1, 0);
        HLCTimestamp liveExpiry = new(1, now.L + 60_000, 0);

        InjectHolds(store,
        [
            new SnapshotHold("h-expired", "c1", new HLCTimestamp(1, 500, 0), expiredExpiry),
            new SnapshotHold("h-live",    "c2", new HLCTimestamp(1, 2000, 0), liveExpiry),
        ]);

        // Only the live hold contributes; the expired one is invisible.
        Assert.Equal(new HLCTimestamp(1, 2000, 0), store.GetEffectiveFloor(now));
    }

    [Fact]
    public void AllHoldsExpired_EffectiveFloor_IsZero()
    {
        (RaftManager raft, SnapshotFloorStore store) = CreateSingleNodeStore();
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        HLCTimestamp expiredExpiry = new(1, now.L - 1, 0);
        InjectHolds(store,
        [
            new SnapshotHold("h1", "c1", new HLCTimestamp(1, 100, 0), expiredExpiry),
            new SnapshotHold("h2", "c2", new HLCTimestamp(1, 200, 0), expiredExpiry),
        ]);

        Assert.Equal(HLCTimestamp.Zero, store.GetEffectiveFloor(now));
    }

    [Fact]
    public void WrongLogType_Ignored_HoldsUnchanged()
    {
        (RaftManager raft, SnapshotFloorStore store) = CreateSingleNodeStore();
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        // First inject a known hold.
        HLCTimestamp ts = new(1, 1000, 0);
        HLCTimestamp expiry = new(1, now.L + 60_000, 0);
        InjectHolds(store, [new SnapshotHold("h1", "c1", ts, expiry)]);

        // A log with a different type (e.g. RangeMap) on the same partition must be ignored.
        RaftLog unrelated = new() { LogType = ReplicationTypes.RangeMap, LogData = [] };
        bool ok = store.Restore(RangeMapStore.MetaPartitionId, unrelated);

        Assert.True(ok);
        Assert.Single(store.Holds);
    }

    [Fact]
    public void WrongPartition_Ignored_HoldsUnchanged()
    {
        (RaftManager raft, SnapshotFloorStore store) = CreateSingleNodeStore();
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        HLCTimestamp ts = new(1, 1000, 0);
        HLCTimestamp expiry = new(1, now.L + 60_000, 0);
        InjectHolds(store, [new SnapshotHold("h1", "c1", ts, expiry)]);

        // A snapshot-floor log on a data partition (id 1+) must be a no-op.
        SnapshotFloorMessage msg = new();
        byte[] data = ReplicationSerializer.Serialize(msg);
        RaftLog wrong = new() { LogType = ReplicationTypes.SnapshotFloor, LogData = data };
        store.Restore(1 /* data partition */, wrong);

        Assert.Single(store.Holds); // still the original hold
    }

    [Fact]
    public void RemoveDelta_ClearsHold()
    {
        (RaftManager raft, SnapshotFloorStore store) = CreateSingleNodeStore();
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        HLCTimestamp ts = new(1, 1000, 0);
        HLCTimestamp expiry = new(1, now.L + 60_000, 0);
        InjectHolds(store, [new SnapshotHold("h1", "c1", ts, expiry)]);

        // A remove delta for the only hold clears the registry and lifts the floor.
        bool ok = InjectRemove(store, "h1");

        Assert.True(ok);
        Assert.Empty(store.Holds);
        Assert.Equal(HLCTimestamp.Zero, store.GetEffectiveFloor(now));
    }

    [Fact]
    public void EmptyDelta_IsNoOp()
    {
        (RaftManager raft, SnapshotFloorStore store) = CreateSingleNodeStore();
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        HLCTimestamp ts = new(1, 1000, 0);
        HLCTimestamp expiry = new(1, now.L + 60_000, 0);
        InjectHolds(store, [new SnapshotHold("h1", "c1", ts, expiry)]);

        // Unlike the old full-snapshot format, an empty delta carries no ops and changes nothing.
        bool ok = InjectHolds(store, []);

        Assert.True(ok);
        Assert.Single(store.Holds);
    }

    [Fact]
    public void UpsertDelta_IsIdempotentUnderReapply()
    {
        (RaftManager raft, SnapshotFloorStore store) = CreateSingleNodeStore();
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        HLCTimestamp ts = new(1, 1000, 0);
        HLCTimestamp expiry = new(1, now.L + 60_000, 0);
        SnapshotHold hold = new("h1", "c1", ts, expiry);

        // Re-delivering the same upsert (Raft re-delivery / tail replay above an installed snapshot)
        // converges to the same single hold rather than duplicating or corrupting it.
        InjectHolds(store, [hold]);
        InjectHolds(store, [hold]);

        Assert.Single(store.Holds);
        Assert.Equal(ts, store.GetEffectiveFloor(now));
    }

    [Fact]
    public void RemoveDelta_IsIdempotentAndDoesNotResurrect()
    {
        (RaftManager raft, SnapshotFloorStore store) = CreateSingleNodeStore();
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        HLCTimestamp ts = new(1, 1000, 0);
        HLCTimestamp expiry = new(1, now.L + 60_000, 0);
        SnapshotHold hold = new("h1", "c1", ts, expiry);

        InjectHolds(store, [hold]);
        InjectRemove(store, "h1");
        // A duplicate remove is a harmless no-op; a stale upsert re-delivered out of the leader's
        // commit order cannot happen (Raft applies in order), so the hold stays gone.
        InjectRemove(store, "h1");

        Assert.Empty(store.Holds);
        Assert.Equal(HLCTimestamp.Zero, store.GetEffectiveFloor(now));
    }

    [Fact]
    public void ReleasingLowestHold_FloorRisesToNext()
    {
        (RaftManager raft, SnapshotFloorStore store) = CreateSingleNodeStore();
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        HLCTimestamp t1 = new(1, 1000, 0);
        HLCTimestamp t2 = new(1, 3000, 0);
        HLCTimestamp expiry = new(1, now.L + 60_000, 0);

        InjectHolds(store,
        [
            new SnapshotHold("h1", "c1", t1, expiry),
            new SnapshotHold("h2", "c2", t2, expiry),
        ]);
        Assert.Equal(t1, store.GetEffectiveFloor(now));

        // Release the lower hold via a remove delta; the floor rises to the next-lowest.
        InjectRemove(store, "h1");
        Assert.Equal(t2, store.GetEffectiveFloor(now));
    }

    [Fact]
    public void ProtoRoundTrip_PreservesAllFields()
    {
        (RaftManager raft, SnapshotFloorStore store) = CreateSingleNodeStore();

        HLCTimestamp ts = new(42, 9_999_999L, 7);
        HLCTimestamp expiry = new(43, 10_100_000L, 3);
        SnapshotHold original = new("hold-abc", "holder-xyz", ts, expiry);

        InjectHolds(store, [original]);

        Assert.True(store.Holds.TryGetValue("hold-abc", out SnapshotHold? loaded));
        Assert.Equal("hold-abc",    loaded!.HoldId);
        Assert.Equal("holder-xyz",  loaded.HolderId);
        Assert.Equal(ts,            loaded.Timestamp);
        Assert.Equal(expiry,        loaded.LeaseExpiry);
    }

    // ── Multi-node integration tests ─────────────────────────────────────────────────────────

    private (RaftManager, KahunaManager) BuildNode(
        int nodeId, int port, string[] peers, string walStorage, string raftRevision,
        MemoryInterNodeCommmunication interNode, InMemoryCommunication comm)
    {
        IWAL wal = walStorage == "sqlite"
            ? new SqliteWAL("/tmp", raftRevision, raftLogger, syncWrites: false)
            : new InMemoryWAL(raftLogger);

        ActorSystem actorSystem = new(logger: raftLogger);

        RaftConfiguration config = new()
        {
            NodeName = "floor" + nodeId,
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
            StorageRevision = raftRevision,
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

    private async Task<Node[]> Assemble(string walStorage, string[] revisions)
    {
        MemoryInterNodeCommmunication interNode = new();
        InMemoryCommunication comm = new();

        string[] p1 = ["localhost:9101", "localhost:9102"];
        string[] p2 = ["localhost:9100", "localhost:9102"];
        string[] p3 = ["localhost:9100", "localhost:9101"];

        (RaftManager r1, KahunaManager k1) = BuildNode(1, 9100, p1, walStorage, revisions[0], interNode, comm);
        (RaftManager r2, KahunaManager k2) = BuildNode(2, 9101, p2, walStorage, revisions[1], interNode, comm);
        (RaftManager r3, KahunaManager k3) = BuildNode(3, 9102, p3, walStorage, revisions[2], interNode, comm);

        interNode.SetNodes(new() { { "localhost:9100", k1 }, { "localhost:9101", k2 }, { "localhost:9102", k3 } });
        comm.SetNodes(new() { { "localhost:9100", r1 }, { "localhost:9101", r2 }, { "localhost:9102", r3 } });

        await Task.WhenAll(r1.JoinCluster(), r2.JoinCluster(), r3.JoinCluster());

        // Wait for a leader on the meta partition (0) before any acquire can succeed.
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
            try { await TestClusterNodeRegistry.DisposeAsync(node.Raft); }
            catch (ObjectDisposedException) { }
        }
    }

    /// <summary>Polls until <paramref name="predicate"/> holds for <paramref name="node"/>.</summary>
    private static async Task WaitUntil(Node node, Func<(HLCTimestamp Floor, int Live), bool> predicate, int timeoutMs = 5000)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        long deadline = Environment.TickCount64 + (long)(timeoutMs * TimingScale);
        while (Environment.TickCount64 < deadline)
        {
            HLCTimestamp now = node.Raft.HybridLogicalClock.TrySendOrLocalEvent(node.Raft.GetLocalNodeId());
            HLCTimestamp floor = node.Kahuna.SnapshotFloorStore.GetEffectiveFloor(now);
            int live = 0;
            foreach (SnapshotHold h in node.Kahuna.SnapshotFloorStore.Holds.Values)
                if (h.IsLive(now))
                    live++;
            if (predicate((floor, live)))
                return;
            await Task.Delay(25, ct);
        }
        Assert.Fail("Timed out waiting for snapshot-floor state to converge.");
    }

    // ─────────────────────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task Acquire_ReturnsHoldId_FloorEqualsTimestamp()
    {
        Node[] nodes = await Assemble("memory",
            [Guid.NewGuid().ToString(), Guid.NewGuid().ToString(), Guid.NewGuid().ToString()]);
        try
        {
            Node leader = await LeaderOf(RangeMapStore.MetaPartitionId, nodes);
            CancellationToken ct = TestContext.Current.CancellationToken;

            HLCTimestamp forkT = leader.Raft.HybridLogicalClock.TrySendOrLocalEvent(leader.Raft.GetLocalNodeId());

            (KeyValueResponseType type, string holdId, HLCTimestamp leaseExpiry) =
                await leader.Kahuna.LocateAndAcquireSnapshotHold("branch-1", forkT, 60_000, ct);

            Assert.Equal(KeyValueResponseType.Set, type);
            Assert.NotEmpty(holdId);
            Assert.NotEqual(HLCTimestamp.Zero, leaseExpiry);

            // The floor must equal the only held timestamp.
            (HLCTimestamp floor, int live) = await leader.Kahuna.GetSnapshotFloor(ct);
            Assert.Equal(forkT, floor);
            Assert.Equal(1, live);
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }

    [Fact]
    public async Task TwoHolds_FloorIsMin_ReleasingLowest_FloorRises()
    {
        Node[] nodes = await Assemble("memory",
            [Guid.NewGuid().ToString(), Guid.NewGuid().ToString(), Guid.NewGuid().ToString()]);
        try
        {
            Node leader = await LeaderOf(RangeMapStore.MetaPartitionId, nodes);
            CancellationToken ct = TestContext.Current.CancellationToken;

            HLCTimestamp t1 = leader.Raft.HybridLogicalClock.TrySendOrLocalEvent(leader.Raft.GetLocalNodeId());
            (_, string holdId1, _) = await leader.Kahuna.LocateAndAcquireSnapshotHold("branch-1", t1, 60_000, ct);

            HLCTimestamp t2 = leader.Raft.HybridLogicalClock.TrySendOrLocalEvent(leader.Raft.GetLocalNodeId());
            (_, string holdId2, _) = await leader.Kahuna.LocateAndAcquireSnapshotHold("branch-2", t2, 60_000, ct);

            // Floor = min(t1, t2) = t1.
            (HLCTimestamp floor, int live) = await leader.Kahuna.GetSnapshotFloor(ct);
            Assert.Equal(t1, floor);
            Assert.Equal(2, live);

            // Release the lower hold.
            KeyValueResponseType rel = await leader.Kahuna.LocateAndReleaseSnapshotHold(holdId1, ct);
            Assert.Equal(KeyValueResponseType.Deleted, rel);

            // Floor now rises to t2.
            (HLCTimestamp floorAfter, int liveAfter) = await leader.Kahuna.GetSnapshotFloor(ct);
            Assert.Equal(t2, floorAfter);
            Assert.Equal(1, liveAfter);
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }

    [Fact]
    public async Task Acquire_IdempotentBySameHolderTimestamp_ReturnsSameHoldId()
    {
        Node[] nodes = await Assemble("memory",
            [Guid.NewGuid().ToString(), Guid.NewGuid().ToString(), Guid.NewGuid().ToString()]);
        try
        {
            Node leader = await LeaderOf(RangeMapStore.MetaPartitionId, nodes);
            CancellationToken ct = TestContext.Current.CancellationToken;

            HLCTimestamp forkT = leader.Raft.HybridLogicalClock.TrySendOrLocalEvent(leader.Raft.GetLocalNodeId());

            (_, string id1, _) = await leader.Kahuna.LocateAndAcquireSnapshotHold("same-client", forkT, 60_000, ct);
            (_, string id2, _) = await leader.Kahuna.LocateAndAcquireSnapshotHold("same-client", forkT, 60_000, ct);

            Assert.Equal(id1, id2);

            // Still only one hold in the registry.
            (_, int live) = await leader.Kahuna.GetSnapshotFloor(ct);
            Assert.Equal(1, live);
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }

    [Fact]
    public async Task Hold_ReplicatedToFollowers_FloorConverges()
    {
        Node[] nodes = await Assemble("memory",
            [Guid.NewGuid().ToString(), Guid.NewGuid().ToString(), Guid.NewGuid().ToString()]);
        try
        {
            Node leader = await LeaderOf(RangeMapStore.MetaPartitionId, nodes);
            CancellationToken ct = TestContext.Current.CancellationToken;

            HLCTimestamp forkT = leader.Raft.HybridLogicalClock.TrySendOrLocalEvent(leader.Raft.GetLocalNodeId());
            await leader.Kahuna.LocateAndAcquireSnapshotHold("branch-1", forkT, 60_000, ct);

            // Every node (leader + followers) must see the hold via OnReplicationReceived.
            foreach (Node node in nodes)
                await WaitUntil(node, s => s.Floor == forkT && s.Live == 1);
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }

    [Fact]
    public async Task Hold_SurvivesLeaderChange_NewLeaderSeesFloor()
    {
        Node[] nodes = await Assemble("memory",
            [Guid.NewGuid().ToString(), Guid.NewGuid().ToString(), Guid.NewGuid().ToString()]);
        try
        {
            Node leader = await LeaderOf(RangeMapStore.MetaPartitionId, nodes);
            CancellationToken ct = TestContext.Current.CancellationToken;

            HLCTimestamp forkT = leader.Raft.HybridLogicalClock.TrySendOrLocalEvent(leader.Raft.GetLocalNodeId());
            await leader.Kahuna.LocateAndAcquireSnapshotHold("branch-1", forkT, 60_000, ct);

            // Wait for all nodes to converge.
            foreach (Node node in nodes)
                await WaitUntil(node, s => s.Floor == forkT);

            // Force a leadership change.
            await leader.Raft.StepDownAsync(RangeMapStore.MetaPartitionId, ct);

            Node newLeader = await LeaderOf(RangeMapStore.MetaPartitionId, nodes);
            Assert.NotSame(leader, newLeader);

            // New leader still reports the same floor.
            HLCTimestamp now = newLeader.Raft.HybridLogicalClock.TrySendOrLocalEvent(newLeader.Raft.GetLocalNodeId());
            Assert.Equal(forkT, newLeader.Kahuna.SnapshotFloorStore.GetEffectiveFloor(now));
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }

    [Fact]
    public async Task Hold_SurvivesRestart()
    {
        string[] revisions = [Guid.NewGuid().ToString(), Guid.NewGuid().ToString(), Guid.NewGuid().ToString()];

        Node[] nodes = await Assemble("sqlite", revisions);
        HLCTimestamp forkT;
        try
        {
            Node leader = await LeaderOf(RangeMapStore.MetaPartitionId, nodes);
            CancellationToken ct = TestContext.Current.CancellationToken;

            forkT = leader.Raft.HybridLogicalClock.TrySendOrLocalEvent(leader.Raft.GetLocalNodeId());
            await leader.Kahuna.LocateAndAcquireSnapshotHold("branch-1", forkT, 60_000, ct);

            foreach (Node node in nodes)
                await WaitUntil(node, s => s.Floor == forkT);
        }
        finally
        {
            await LeaveAll(nodes);
        }

        // Restart the cluster on the same WAL files.
        Node[] restarted = await Assemble("sqlite", revisions);
        try
        {
            Node leader = await LeaderOf(RangeMapStore.MetaPartitionId, restarted);

            // Floor must survive the restart — recovered via WAL replay, disk snapshot, or both.
            await WaitUntil(leader, s => s.Floor == forkT && s.Live == 1);
        }
        finally
        {
            await LeaveAll(restarted);
        }
    }

    [Fact]
    public async Task Renew_ExtendsLease_ReleasedHold_ReturnsDoesNotExist()
    {
        Node[] nodes = await Assemble("memory",
            [Guid.NewGuid().ToString(), Guid.NewGuid().ToString(), Guid.NewGuid().ToString()]);
        try
        {
            Node leader = await LeaderOf(RangeMapStore.MetaPartitionId, nodes);
            CancellationToken ct = TestContext.Current.CancellationToken;

            HLCTimestamp forkT = leader.Raft.HybridLogicalClock.TrySendOrLocalEvent(leader.Raft.GetLocalNodeId());
            (_, string holdId, HLCTimestamp origExpiry) = await leader.Kahuna.LocateAndAcquireSnapshotHold("c1", forkT, 60_000, ct);

            // Renew extends the expiry.
            (KeyValueResponseType renewType, HLCTimestamp newExpiry) = await leader.Kahuna.LocateAndRenewSnapshotHold(holdId, 120_000, ct);
            Assert.Equal(KeyValueResponseType.Set, renewType);
            Assert.True(newExpiry.CompareTo(origExpiry) > 0);

            // Release.
            KeyValueResponseType relType = await leader.Kahuna.LocateAndReleaseSnapshotHold(holdId, ct);
            Assert.Equal(KeyValueResponseType.Deleted, relType);

            // Renew on released hold returns DoesNotExist.
            (KeyValueResponseType missType, _) = await leader.Kahuna.LocateAndRenewSnapshotHold(holdId, 60_000, ct);
            Assert.Equal(KeyValueResponseType.DoesNotExist, missType);
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }

    [Fact]
    public async Task ExpiredHold_DropsFromFloor_AfterLeaseElapsed()
    {
        Node[] nodes = await Assemble("memory",
            [Guid.NewGuid().ToString(), Guid.NewGuid().ToString(), Guid.NewGuid().ToString()]);
        try
        {
            Node leader = await LeaderOf(RangeMapStore.MetaPartitionId, nodes);
            CancellationToken ct = TestContext.Current.CancellationToken;

            HLCTimestamp forkT = leader.Raft.HybridLogicalClock.TrySendOrLocalEvent(leader.Raft.GetLocalNodeId());

            // Acquire with a 1 ms lease — effectively expired immediately.
            await leader.Kahuna.LocateAndAcquireSnapshotHold("c-shortlived", forkT, 1, ct);

            // Advance HLC by at least 2 ms so the lease is definitely expired.
            await Task.Delay(10, ct);
            HLCTimestamp later = leader.Raft.HybridLogicalClock.TrySendOrLocalEvent(leader.Raft.GetLocalNodeId());

            // The expired hold must not count toward the floor.
            HLCTimestamp floor = leader.Kahuna.SnapshotFloorStore.GetEffectiveFloor(later);
            Assert.Equal(HLCTimestamp.Zero, floor);

            (HLCTimestamp floorApi, int live) = await leader.Kahuna.GetSnapshotFloor(ct);
            Assert.Equal(HLCTimestamp.Zero, floorApi);
            Assert.Equal(0, live);
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }

    [Fact]
    public async Task ReleaseNonExistentHold_ReturnsDoesNotExist()
    {
        Node[] nodes = await Assemble("memory",
            [Guid.NewGuid().ToString(), Guid.NewGuid().ToString(), Guid.NewGuid().ToString()]);
        try
        {
            Node leader = await LeaderOf(RangeMapStore.MetaPartitionId, nodes);
            CancellationToken ct = TestContext.Current.CancellationToken;

            KeyValueResponseType type = await leader.Kahuna.LocateAndReleaseSnapshotHold("no-such-hold", ct);
            Assert.Equal(KeyValueResponseType.DoesNotExist, type);
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }

    /// <summary>
    /// Verifies that snapshot-hold mutations routed through a follower node still reach the
    /// meta-partition leader via <see cref="IInterNodeCommunication"/>. A full
    /// acquire → get-floor → renew → release cycle is performed through a node that is
    /// explicitly confirmed NOT to be the meta leader, exercising the routing path.
    /// </summary>
    [Fact]
    public async Task AcquireRenewRelease_ViaFollowerNode_RoutesToLeaderSuccessfully()
    {
        Node[] nodes = await Assemble("memory",
            [Guid.NewGuid().ToString(), Guid.NewGuid().ToString(), Guid.NewGuid().ToString()]);
        try
        {
            CancellationToken ct = TestContext.Current.CancellationToken;

            Node leader = await LeaderOf(RangeMapStore.MetaPartitionId, nodes);

            // Pick a follower — a node that is definitely not the meta leader.
            Node follower = nodes.First(n => n.Raft.GetLocalEndpoint() != leader.Raft.GetLocalEndpoint());

            HLCTimestamp forkT = leader.Raft.HybridLogicalClock.TrySendOrLocalEvent(leader.Raft.GetLocalNodeId());

            // Acquire through the follower — must be routed to the leader and succeed.
            (KeyValueResponseType acquireType, string holdId, HLCTimestamp origExpiry) =
                await follower.Kahuna.LocateAndAcquireSnapshotHold("r4-test", forkT, leaseMs: 60_000, ct);
            Assert.Equal(KeyValueResponseType.Set, acquireType);
            Assert.NotEmpty(holdId);
            Assert.NotEqual(HLCTimestamp.Zero, origExpiry);

            // The floor should now be visible on the leader after replication.
            await WaitUntil(leader, state => state.Live >= 1);
            (HLCTimestamp floor, int live) = await leader.Kahuna.GetSnapshotFloor(ct);
            Assert.Equal(forkT, floor);
            Assert.Equal(1, live);

            // Renew through the follower.
            (KeyValueResponseType renewType, HLCTimestamp newExpiry) =
                await follower.Kahuna.LocateAndRenewSnapshotHold(holdId, leaseMs: 60_000, ct);
            Assert.Equal(KeyValueResponseType.Set, renewType);
            Assert.NotEqual(HLCTimestamp.Zero, newExpiry);

            // Release through the follower.
            KeyValueResponseType releaseType =
                await follower.Kahuna.LocateAndReleaseSnapshotHold(holdId, ct);
            Assert.Equal(KeyValueResponseType.Deleted, releaseType);

            // Floor must drop back to zero once the only hold is released.
            await WaitUntil(leader, state => state.Live == 0);
            (HLCTimestamp floorAfter, int liveAfter) = await leader.Kahuna.GetSnapshotFloor(ct);
            Assert.Equal(HLCTimestamp.Zero, floorAfter);
            Assert.Equal(0, liveAfter);
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }

    // ── Floor-cache correctness (R8) ─────────────────────────────────────────────────────────

    /// <summary>
    /// Floor cache is populated after InjectHolds (Restore path). O(1) fast path returns the
    /// correct floor and live count without rescanning the hold dict.
    /// </summary>
    [Fact]
    public void FloorCache_AfterRestore_ReturnsCorrectFloorAndCount()
    {
        (RaftManager raft, SnapshotFloorStore store) = CreateSingleNodeStore();
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        HLCTimestamp t1 = new(1, 1000, 0);
        HLCTimestamp t2 = new(1, 2000, 0);
        HLCTimestamp farExpiry = new(1, now.L + 600_000, 0); // 10 min away

        bool ok = InjectHolds(store, [
            new SnapshotHold("h1", "c1", t1, farExpiry),
            new SnapshotHold("h2", "c2", t2, farExpiry)
        ]);
        Assert.True(ok);

        (HLCTimestamp floor, int live) = store.GetEffectiveFloorAndCount(now);
        Assert.Equal(t1, floor);       // min(t1, t2)
        Assert.Equal(2, live);
    }

    /// <summary>
    /// Replacing the hold set via a second Restore call invalidates the cache: the updated cache
    /// must reflect the new hold (restore/failover parity for the O(1) path).
    /// </summary>
    [Fact]
    public void FloorCache_AfterSecondRestore_ReflectsNewHoldSet()
    {
        (RaftManager raft, SnapshotFloorStore store) = CreateSingleNodeStore();
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        HLCTimestamp t1 = new(1, 1000, 0);
        HLCTimestamp t3 = new(1, 500, 0);  // earlier than t1 — will become the new floor
        HLCTimestamp farExpiry = new(1, now.L + 600_000, 0);

        InjectHolds(store, [new SnapshotHold("h1", "c1", t1, farExpiry)]);
        Assert.Equal(t1, store.GetEffectiveFloor(now));

        // A later delta adds h3 (simulates leader-change WAL replay catching up on the tail).
        InjectHolds(store, [
            new SnapshotHold("h3", "c3", t3, farExpiry)
        ]);

        (HLCTimestamp floor, int live) = store.GetEffectiveFloorAndCount(now);
        Assert.Equal(t3, floor);
        Assert.Equal(2, live);
    }

    /// <summary>
    /// When <c>NextExpiry</c> passes, the fast path must fall through to the slow scan.
    /// Injects a hold that expires shortly, advances time past the expiry, and verifies
    /// the floor drops to the surviving hold only.
    /// </summary>
    [Fact]
    public void FloorCache_ExpiredHold_SlowPathRecomputesCorrectly()
    {
        (RaftManager raft, SnapshotFloorStore store) = CreateSingleNodeStore();
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        HLCTimestamp t1 = new(1, 1000, 0);  // floor (smallest)
        HLCTimestamp t2 = new(1, 2000, 0);

        // h1 expires in 100 ms, h2 survives.
        HLCTimestamp shortExpiry = new(1, now.L + 100, 0);
        HLCTimestamp farExpiry   = new(1, now.L + 600_000, 0);

        InjectHolds(store, [
            new SnapshotHold("h1", "c1", t1, shortExpiry),
            new SnapshotHold("h2", "c2", t2, farExpiry)
        ]);

        // Fast path: cache valid (now < shortExpiry).
        Assert.Equal(t1, store.GetEffectiveFloor(now));

        // Advance time past h1's expiry — cache is now stale, slow path must kick in.
        HLCTimestamp later = new(1, now.L + 200, 0); // 200 ms after now → past shortExpiry
        (HLCTimestamp floor, int live) = store.GetEffectiveFloorAndCount(later);

        Assert.Equal(t2, floor);   // h1 expired; h2 is the only live hold
        Assert.Equal(1, live);
    }

    /// <summary>
    /// Many holds: verifies the cache fast path returns the same result as a direct scan,
    /// demonstrating both correctness and that GetEffectiveFloor does not regress under load.
    /// </summary>
    [Fact]
    public void FloorCache_ManyHolds_FastPathMatchesDirectScan()
    {
        (RaftManager raft, SnapshotFloorStore store) = CreateSingleNodeStore();
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());
        HLCTimestamp farExpiry = new(1, now.L + 600_000, 0);

        const int holdCount = 500;
        SnapshotHold[] holdSet = new SnapshotHold[holdCount];
        HLCTimestamp expectedFloor = new(1, holdCount * 1000L, 0); // will be set to the smallest ts
        for (int i = holdCount; i >= 1; i--)
        {
            HLCTimestamp ts = new(1, i * 1000L, 0);
            if (i == 1) expectedFloor = ts; // smallest
            holdSet[holdCount - i] = new SnapshotHold($"h{i}", $"c{i}", ts, farExpiry);
        }

        InjectHolds(store, holdSet);

        (HLCTimestamp floor, int live) = store.GetEffectiveFloorAndCount(now);

        Assert.Equal(expectedFloor, floor);
        Assert.Equal(holdCount, live);
    }

    /// <summary>
    /// AcquireAsync with leaseMs &lt;= 0 must return InvalidInput at the store level.
    /// </summary>
    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(-1000)]
    public async Task AcquireAsync_ZeroOrNegativeLeaseMs_ReturnsInvalidInput(int leaseMs)
    {
        Node[] nodes = await Assemble("memory",
            [Guid.NewGuid().ToString(), Guid.NewGuid().ToString(), Guid.NewGuid().ToString()]);
        try
        {
            CancellationToken ct = TestContext.Current.CancellationToken;
            Node leader = await LeaderOf(RangeMapStore.MetaPartitionId, nodes);
            HLCTimestamp forkT = leader.Raft.HybridLogicalClock.TrySendOrLocalEvent(leader.Raft.GetLocalNodeId());

            (KeyValueResponseType type, string holdId, HLCTimestamp leaseExpiry) =
                await leader.Kahuna.LocateAndAcquireSnapshotHold("r6-acquire", forkT, leaseMs, ct);

            Assert.Equal(KeyValueResponseType.InvalidInput, type);
            Assert.True(string.IsNullOrEmpty(holdId));
            Assert.Equal(HLCTimestamp.Zero, leaseExpiry);

            // No hold registered — floor stays at Zero.
            (HLCTimestamp floor, int live) = await leader.Kahuna.GetSnapshotFloor(ct);
            Assert.Equal(HLCTimestamp.Zero, floor);
            Assert.Equal(0, live);
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }

    /// <summary>
    /// RenewAsync with leaseMs &lt;= 0 must return InvalidInput — even when the hold ID is valid.
    /// </summary>
    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(-1000)]
    public async Task RenewAsync_ZeroOrNegativeLeaseMs_ReturnsInvalidInput(int leaseMs)
    {
        Node[] nodes = await Assemble("memory",
            [Guid.NewGuid().ToString(), Guid.NewGuid().ToString(), Guid.NewGuid().ToString()]);
        try
        {
            CancellationToken ct = TestContext.Current.CancellationToken;
            Node leader = await LeaderOf(RangeMapStore.MetaPartitionId, nodes);
            HLCTimestamp forkT = leader.Raft.HybridLogicalClock.TrySendOrLocalEvent(leader.Raft.GetLocalNodeId());

            (KeyValueResponseType acquireType, string holdId, _) =
                await leader.Kahuna.LocateAndAcquireSnapshotHold("r6-renew", forkT, 60_000, ct);
            Assert.Equal(KeyValueResponseType.Set, acquireType);

            (KeyValueResponseType renewType, HLCTimestamp renewExpiry) =
                await leader.Kahuna.LocateAndRenewSnapshotHold(holdId, leaseMs, ct);

            Assert.Equal(KeyValueResponseType.InvalidInput, renewType);
            Assert.Equal(HLCTimestamp.Zero, renewExpiry);

            // The existing hold was not removed — floor is still set.
            (HLCTimestamp floor, int live) = await leader.Kahuna.GetSnapshotFloor(ct);
            Assert.Equal(forkT, floor);
            Assert.Equal(1, live);
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }

    // ── Gauge dispose-loop test (R9) ─────────────────────────────────────────────────────────

    /// <summary>
    /// Verifies that disposing a store removes its observable gauge callbacks. A store with a
    /// sentinel hold count (997) is created, confirmed to fire its callback, then disposed.
    /// After dispose, the sentinel value must no longer appear — the per-instance Meter was
    /// disposed, removing the instrument.
    /// </summary>
    [Fact]
    public void DisposeLoop_OldStoreGaugesStop_SentinelDisappearsAfterDispose()
    {
        const int sentinel = 997; // unlikely to collide with any other test's hold count

        List<int> observations = [];
        using MeterListener listener = new();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Name == "kahuna.snapshot_floor.live_holds")
                l.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<int>((_, value, _, _) =>
        {
            lock (observations) observations.Add(value);
        });
        listener.Start();

        // Build a store and inject `sentinel` holds with long leases.
        (RaftManager raft, SnapshotFloorStore store) = CreateSingleNodeStore();
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());
        HLCTimestamp farExpiry = new(1, now.L + 600_000, 0);

        SnapshotHold[] manyHolds = Enumerable.Range(0, sentinel)
            .Select(i => new SnapshotHold($"h{i}", $"c{i}", new(1, 1000L * (i + 1), 0), farExpiry))
            .ToArray();
        InjectHolds(store, manyHolds);

        // Confirm the gauge fires with the sentinel value before dispose.
        listener.RecordObservableInstruments();
        lock (observations)
        {
            Assert.Contains(sentinel, observations);
        }

        // Dispose the store — its per-instance Meter is disposed, removing the gauge callback.
        store.Dispose();

        // Collect again: the sentinel must not appear in any new observation.
        int countBefore;
        lock (observations) countBefore = observations.Count;

        listener.RecordObservableInstruments();

        lock (observations)
        {
            IEnumerable<int> newObservations = observations.Skip(countBefore);
            Assert.DoesNotContain(sentinel, newObservations);
        }
    }
}
