
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
/// Verifies the snapshot-hold recovery contract: a hold's protection ends only when its
/// replicated removal (release or purge) commits, never at bare lease expiry.
///
/// <para><b>Unit tests (no cluster).</b> The reclamation floor (<c>BeginPrune</c>) must honor a
/// registered hold whose lease has lapsed — including one loaded from the durable snapshot after
/// a restart, and one installed by the P0 whole-partition state transfer — so revision churn
/// immediately after a restart, before the holder's first renew, cannot reclaim the pinned
/// history.</para>
///
/// <para><b>Multi-node integration tests.</b> A renew of a lapsed-but-registered hold revives it
/// (success proves the protection never lapsed); a renew after the purge fails closed with
/// DoesNotExist. Holds loaded from disk stay exempt from the purge for the configured startup
/// grace window, so a holder that was down together with the cluster longer than its lease can
/// still recover.</para>
/// </summary>
public sealed class TestSnapshotHoldRestartRecovery : RaftTrackingTest
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

    private const int ElectionTimeoutSeedBase = 87000;

    // ── unit-level helpers ──────────────────────────────────────────────────────────────────

    private (RaftManager, SnapshotFloorStore) CreateSingleNodeStore()
    {
        RaftManager raft = new(
            new RaftConfiguration
            {
                NodeName = "hold-recovery-test",
                NodeId = 1,
                Host = "localhost",
                Port = 0,
                InitialPartitions = 1,
                EnableQuiescence = false, PartitionExecutorPoolSize = 1
            },
            new StaticDiscovery([]),
            new InMemoryWAL(raftLogger),
            new InMemoryCommunication(),
            new HybridLogicalClock(),
            raftLogger
        );
        SnapshotFloorStore store = new(raft, null, null, kahunaLogger);
        return (raft, store);
    }

    private static bool InjectHold(SnapshotFloorStore store, SnapshotHold hold)
    {
        SnapshotFloorDeltaMessage delta = new();
        delta.Entries.Add(new SnapshotFloorDeltaEntry { Remove = false, Hold = ToHoldMessage(hold) });
        byte[] data = ReplicationSerializer.Serialize(delta);
        RaftLog log = new() { LogType = ReplicationTypes.SnapshotFloor, LogData = data };
        return store.Restore(RangeMapStore.MetaPartitionId, log);
    }

    private static bool InjectRemove(SnapshotFloorStore store, string holdId)
    {
        SnapshotFloorDeltaMessage delta = new();
        delta.Entries.Add(new SnapshotFloorDeltaEntry { Remove = true, Hold = new SnapshotHoldMessage { HoldId = holdId } });
        byte[] data = ReplicationSerializer.Serialize(delta);
        RaftLog log = new() { LogType = ReplicationTypes.SnapshotFloor, LogData = data };
        return store.Restore(RangeMapStore.MetaPartitionId, log);
    }

    private static SnapshotHoldMessage ToHoldMessage(SnapshotHold hold) =>
        new()
        {
            HoldId = hold.HoldId,
            HolderId = hold.HolderId,
            TimestampNode     = hold.Timestamp.N,
            TimestampPhysical = hold.Timestamp.L,
            TimestampCounter  = hold.Timestamp.C,
            LeaseExpiryNode     = hold.LeaseExpiry.N,
            LeaseExpiryPhysical = hold.LeaseExpiry.L,
            LeaseExpiryCounter  = hold.LeaseExpiry.C,
        };

    /// <summary>Writes a durable snapshot-floor file containing the given holds, as a node
    /// shutdown would have left it.</summary>
    private static void SeedSnapshotFile(string storagePath, string revision, params SnapshotHold[] holdSet)
    {
        SnapshotFloorMessage message = new();
        foreach (SnapshotHold hold in holdSet)
            message.Holds.Add(ToHoldMessage(hold));
        Directory.CreateDirectory(storagePath);
        File.WriteAllBytes(
            Path.Combine(storagePath, $"snapshotfloor_{revision}.snapshot"),
            ReplicationSerializer.Serialize(message));
    }

    /// <summary>A hold whose lease lapsed while every node was down: acquired two minutes ago,
    /// expired one minute ago, both relative to the wall clock the HLC physical component uses.</summary>
    private static SnapshotHold LapsedHold(string holdId, string holderId)
    {
        long nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        return new SnapshotHold(
            holdId,
            holderId,
            new HLCTimestamp(1, nowMs - 120_000, 0),
            new HLCTimestamp(1, nowMs - 60_000, 0));
    }

    // ── unit tests: reclamation floor honors lapsed-but-registered holds ────────────────────

    /// <summary>
    /// A registered hold whose lease has lapsed still bounds the prune floor. Only its removal
    /// from the registry lifts the protection.
    /// </summary>
    [Fact]
    public void BeginPrune_LapsedRegisteredHold_FloorHonorsIt()
    {
        (RaftManager _, SnapshotFloorStore store) = CreateSingleNodeStore();

        SnapshotHold hold = LapsedHold("h-lapsed", "c1");
        Assert.True(InjectHold(store, hold));

        (HLCTimestamp floor, long token) = store.BeginPrune();
        store.EndPrune(token);
        Assert.Equal(hold.Timestamp, floor);

        // The replicated removal — a release or the reaper's purge — is what ends protection.
        Assert.True(InjectRemove(store, hold.HoldId));

        (floor, token) = store.BeginPrune();
        store.EndPrune(token);
        Assert.Equal(HLCTimestamp.Zero, floor);
    }

    /// <summary>
    /// Full-downtime scenario, prune side: a store restarted after downtime longer than the
    /// lease loads its holds already expired, yet the prune floor honors them immediately —
    /// so write churn right after restart, before the holder's first renew, cannot reclaim
    /// past the held timestamp.
    /// </summary>
    [Fact]
    public void BeginPrune_AfterRestartWithLapsedHold_FloorHonorsLoadedHold()
    {
        string dir = Path.Combine(Path.GetTempPath(), "kahuna-hold-recovery-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            (RaftManager raft, _) = CreateSingleNodeStore();
            SnapshotHold hold = LapsedHold("h-downtime", "camus-branch");
            SeedSnapshotFile(dir, "vtest", hold);

            SnapshotFloorStore restarted = new(raft, dir, "vtest", kahunaLogger, TimeSpan.FromMinutes(5));
            try
            {
                (HLCTimestamp pruneFloor, long token) = restarted.BeginPrune();
                restarted.EndPrune(token);
                Assert.Equal(hold.Timestamp, pruneFloor);
            }
            finally
            {
                restarted.Dispose();
            }
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    /// <summary>
    /// P0 state-transfer repair path (a follower restored below the WAL compaction floor): a
    /// transferred registry containing a lapsed hold must constrain pruning on the repaired
    /// node exactly like a loaded one.
    /// </summary>
    [Fact]
    public void CommitState_WithLapsedHold_FloorHonorsIt()
    {
        (RaftManager _, SnapshotFloorStore source) = CreateSingleNodeStore();
        SnapshotHold hold = LapsedHold("h-transfer", "c1");
        Assert.True(InjectHold(source, hold));

        (RaftManager _, SnapshotFloorStore repaired) = CreateSingleNodeStore();
        Dictionary<string, SnapshotHold> parsed = repaired.ParseState(source.SerializeState());
        repaired.CommitState(parsed);

        (HLCTimestamp floor, long token) = repaired.BeginPrune();
        repaired.EndPrune(token);
        Assert.Equal(hold.Timestamp, floor);
    }

    // ── cluster helpers ─────────────────────────────────────────────────────────────────────

    private (RaftManager, KahunaManager) BuildNode(
        int nodeId, int port, string[] peers,
        MemoryInterNodeCommmunication interNode, InMemoryCommunication comm,
        string storagePath, string revision, TimeSpan startupGrace)
    {
        ActorSystem actorSystem = new(logger: raftLogger);

        RaftConfiguration raftCfg = new()
        {
            NodeName    = "holdrec" + nodeId,
            NodeId      = nodeId,
            Host        = "localhost",
            Port        = port,
            InitialPartitions     = 2,
            HeartbeatInterval = TimeSpan.FromMilliseconds((int)(10 * TimingScale)),
            // Kommander rejects a de-dup window at or above the heartbeat cadence: the window would
            // swallow every timer-driven round. Its 100 ms default sits far above these fast timers.
            RecentHeartbeat = TimeSpan.FromMilliseconds(10 * TimingScale / 4),
            CheckLeaderInterval = TimeSpan.FromMilliseconds((int)(25 * TimingScale)),
            StartElectionTimeout  = (int)(50 * TimingScale),
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
            HttpsCertificate         = "",
            HttpsCertificatePassword = "",
            LocksWorkers             = 8,
            KeyValueWorkers          = 8,
            BackgroundWriterWorkers  = 1,
            Storage                  = "memory",
            StoragePath              = storagePath,
            StorageRevision          = revision,
            DefaultTransactionTimeout = 5000,
            ScriptCacheExpiration    = TimeSpan.FromMinutes(1),
            SnapshotHoldStartupGraceWindow = startupGrace,
        };

        KahunaManager kahuna = new(actorSystem, Track(raft), kahunaConfig, interNode, kahunaLogger);
        raft.OnLogRestored          += kahuna.OnLogRestored;
        raft.OnReplicationReceived  += kahuna.OnReplicationReceived;
        raft.OnReplicationError     += kahuna.OnReplicationError;
        raft.OnLeaderChanged        += kahuna.OnLeaderChanged;

        TestClusterNodeRegistry.Register(raft, kahuna, actorSystem);

        return (raft, kahuna);
    }

    /// <summary>
    /// Assembles a 3-node cluster. When <paramref name="seededHold"/> is set, every node's
    /// durable snapshot-floor file is pre-written with that hold, simulating a restart of the
    /// whole cluster from persisted state.
    /// </summary>
    private async Task<Node[]> Assemble(string storageRoot, TimeSpan startupGrace, SnapshotHold? seededHold)
    {
        string rev = Guid.NewGuid().ToString();
        MemoryInterNodeCommmunication interNode = new();
        InMemoryCommunication comm = new();

        string[] p1 = ["localhost:9931", "localhost:9932"];
        string[] p2 = ["localhost:9930", "localhost:9932"];
        string[] p3 = ["localhost:9930", "localhost:9931"];

        string[] dirs = [Path.Combine(storageRoot, "n1"), Path.Combine(storageRoot, "n2"), Path.Combine(storageRoot, "n3")];
        string[] revs = [rev + "_1", rev + "_2", rev + "_3"];

        for (int i = 0; i < 3; i++)
        {
            Directory.CreateDirectory(dirs[i]);
            if (seededHold is not null)
                SeedSnapshotFile(dirs[i], revs[i], seededHold);
        }

        (RaftManager r1, KahunaManager k1) = BuildNode(1, 9930, p1, interNode, comm, dirs[0], revs[0], startupGrace);
        (RaftManager r2, KahunaManager k2) = BuildNode(2, 9931, p2, interNode, comm, dirs[1], revs[1], startupGrace);
        (RaftManager r3, KahunaManager k3) = BuildNode(3, 9932, p3, interNode, comm, dirs[2], revs[2], startupGrace);

        interNode.SetNodes(new() { { "localhost:9930", k1 }, { "localhost:9931", k2 }, { "localhost:9932", k3 } });
        comm.SetNodes(new() { { "localhost:9930", r1 }, { "localhost:9931", r2 }, { "localhost:9932", r3 } });

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
            try { await TestClusterNodeRegistry.DisposeAsync(node.Raft); }
            catch (ObjectDisposedException) { }
        }
    }

    // ── cluster tests: renew revival and the startup grace window ───────────────────────────

    /// <summary>
    /// A hold whose lease lapsed but that was not yet purged is revived by a renew: the renew
    /// answers Set with a live expiry, and the hold counts toward the live floor again.
    /// </summary>
    [Fact]
    public async Task LapsedHold_RenewBeforePurge_Revives()
    {
        string root = Path.Combine(Path.GetTempPath(), "kahuna-hold-recovery-" + Guid.NewGuid().ToString("N"));
        Node[] nodes = await Assemble(root, TimeSpan.Zero, seededHold: null);
        try
        {
            Node leader = await LeaderOf(RangeMapStore.MetaPartitionId, nodes);
            CancellationToken ct = TestContext.Current.CancellationToken;

            HLCTimestamp forkT = leader.Raft.HybridLogicalClock.TrySendOrLocalEvent(leader.Raft.GetLocalNodeId());

            (KeyValueResponseType acquireType, string holdId, _) =
                await leader.Kahuna.LocateAndAcquireSnapshotHold("reviving-branch", forkT, leaseMs: 1, ct);
            Assert.Equal(KeyValueResponseType.Set, acquireType);

            // Let the HLC advance well past the 1 ms lease. The reaper has not purged.
            await Task.Delay((int)(50 * TimingScale), ct);

            (KeyValueResponseType renewType, HLCTimestamp newExpiry) =
                await leader.Kahuna.LocateAndRenewSnapshotHold(holdId, 60_000, ct);
            Assert.Equal(KeyValueResponseType.Set, renewType);

            HLCTimestamp now = leader.Raft.HybridLogicalClock.TrySendOrLocalEvent(leader.Raft.GetLocalNodeId());
            Assert.True(newExpiry.CompareTo(now) > 0, "revived lease must be live");

            (_, HLCTimestamp floor, int live) = await leader.Kahuna.GetSnapshotFloor(ct);
            Assert.Equal(forkT, floor);
            Assert.Equal(1, live);
        }
        finally
        {
            await LeaveAll(nodes);
            Directory.Delete(root, recursive: true);
        }
    }

    /// <summary>
    /// Once the purge has removed a lapsed hold, a renew fails closed with DoesNotExist: the
    /// purge is the single action that irrevocably ends a hold's protection.
    /// </summary>
    [Fact]
    public async Task LapsedHold_AfterPurge_RenewFailsClosed()
    {
        string root = Path.Combine(Path.GetTempPath(), "kahuna-hold-recovery-" + Guid.NewGuid().ToString("N"));
        Node[] nodes = await Assemble(root, TimeSpan.Zero, seededHold: null);
        try
        {
            Node leader = await LeaderOf(RangeMapStore.MetaPartitionId, nodes);
            CancellationToken ct = TestContext.Current.CancellationToken;

            HLCTimestamp forkT = leader.Raft.HybridLogicalClock.TrySendOrLocalEvent(leader.Raft.GetLocalNodeId());

            (KeyValueResponseType acquireType, string holdId, _) =
                await leader.Kahuna.LocateAndAcquireSnapshotHold("purged-branch", forkT, leaseMs: 1, ct);
            Assert.Equal(KeyValueResponseType.Set, acquireType);

            await Task.Delay((int)(50 * TimingScale), ct);

            int purged = await leader.Kahuna.PurgeExpiredSnapshotHoldsAsync(ct);
            Assert.Equal(1, purged);

            (KeyValueResponseType renewType, _) =
                await leader.Kahuna.LocateAndRenewSnapshotHold(holdId, 60_000, ct);
            Assert.Equal(KeyValueResponseType.DoesNotExist, renewType);
        }
        finally
        {
            await LeaveAll(nodes);
            Directory.Delete(root, recursive: true);
        }
    }

    /// <summary>
    /// Full-downtime recovery, end to end: every node restarts with a durable registry whose
    /// hold lapsed during the downtime. The prune floor honors the loaded hold before any renew
    /// runs; the purge defers to the startup grace window; and the holder's renew revives the
    /// hold with a live lease.
    /// </summary>
    [Fact]
    public async Task RestartLongerThanLease_GraceDefersPurge_RenewRevives()
    {
        string root = Path.Combine(Path.GetTempPath(), "kahuna-hold-recovery-" + Guid.NewGuid().ToString("N"));
        SnapshotHold hold = LapsedHold("hold-restart", "camus-branch");
        Node[] nodes = await Assemble(root, TimeSpan.FromMinutes(5), hold);
        try
        {
            Node leader = await LeaderOf(RangeMapStore.MetaPartitionId, nodes);
            CancellationToken ct = TestContext.Current.CancellationToken;

            // Reclamation timing adversary: before the holder's first renew, every node's prune
            // floor must already honor the lapsed hold loaded from disk.
            foreach (Node node in nodes)
            {
                (HLCTimestamp pruneFloor, long token) = node.Kahuna.KeyValues.SnapshotFloorStore.BeginPrune();
                node.Kahuna.KeyValues.SnapshotFloorStore.EndPrune(token);
                Assert.Equal(hold.Timestamp, pruneFloor);
            }

            // The reaper must not purge the loaded hold while the grace window is open.
            int purged = await leader.Kahuna.PurgeExpiredSnapshotHoldsAsync(ct);
            Assert.Equal(0, purged);

            // The holder retries renewal at startup and revives the hold.
            (KeyValueResponseType renewType, HLCTimestamp newExpiry) =
                await leader.Kahuna.LocateAndRenewSnapshotHold(hold.HoldId, 60_000, ct);
            Assert.Equal(KeyValueResponseType.Set, renewType);

            HLCTimestamp now = leader.Raft.HybridLogicalClock.TrySendOrLocalEvent(leader.Raft.GetLocalNodeId());
            Assert.True(newExpiry.CompareTo(now) > 0, "revived lease must be live");

            (_, HLCTimestamp floor, int live) = await leader.Kahuna.GetSnapshotFloor(ct);
            Assert.Equal(hold.Timestamp, floor);
            Assert.Equal(1, live);

            // A revived hold is live again — a later purge cycle must leave it alone.
            purged = await leader.Kahuna.PurgeExpiredSnapshotHoldsAsync(ct);
            Assert.Equal(0, purged);
        }
        finally
        {
            await LeaveAll(nodes);
            Directory.Delete(root, recursive: true);
        }
    }

    /// <summary>
    /// A holder that does not return within the startup grace lapses normally: once the window
    /// has closed the purge removes the loaded hold and a late renew fails closed.
    /// </summary>
    [Fact]
    public async Task RestartLongerThanLease_GraceElapsed_PurgeRemovesAndRenewFailsClosed()
    {
        string root = Path.Combine(Path.GetTempPath(), "kahuna-hold-recovery-" + Guid.NewGuid().ToString("N"));
        SnapshotHold hold = LapsedHold("hold-late", "camus-branch");
        // A 1 ms grace has always closed by the time the cluster settles.
        Node[] nodes = await Assemble(root, TimeSpan.FromMilliseconds(1), hold);
        try
        {
            Node leader = await LeaderOf(RangeMapStore.MetaPartitionId, nodes);
            CancellationToken ct = TestContext.Current.CancellationToken;

            int purged = await leader.Kahuna.PurgeExpiredSnapshotHoldsAsync(ct);
            Assert.Equal(1, purged);

            (KeyValueResponseType renewType, _) =
                await leader.Kahuna.LocateAndRenewSnapshotHold(hold.HoldId, 60_000, ct);
            Assert.Equal(KeyValueResponseType.DoesNotExist, renewType);

            (_, HLCTimestamp floor, int live) = await leader.Kahuna.GetSnapshotFloor(ct);
            Assert.Equal(HLCTimestamp.Zero, floor);
            Assert.Equal(0, live);
        }
        finally
        {
            await LeaveAll(nodes);
            Directory.Delete(root, recursive: true);
        }
    }

    /// <summary>
    /// An idempotent re-acquire that lands on a lapsed-but-registered hold is a revival: it
    /// answers with the same holdId, which asserts the hold's protection was continuous.
    /// </summary>
    [Fact]
    public async Task LapsedHold_IdempotentReacquire_ReturnsSameHoldId()
    {
        string root = Path.Combine(Path.GetTempPath(), "kahuna-hold-recovery-" + Guid.NewGuid().ToString("N"));
        Node[] nodes = await Assemble(root, TimeSpan.Zero, seededHold: null);
        try
        {
            Node leader = await LeaderOf(RangeMapStore.MetaPartitionId, nodes);
            CancellationToken ct = TestContext.Current.CancellationToken;

            HLCTimestamp forkT = leader.Raft.HybridLogicalClock.TrySendOrLocalEvent(leader.Raft.GetLocalNodeId());

            (KeyValueResponseType acquireType, string holdId, _) =
                await leader.Kahuna.LocateAndAcquireSnapshotHold("reacquiring-branch", forkT, leaseMs: 1, ct);
            Assert.Equal(KeyValueResponseType.Set, acquireType);

            await Task.Delay((int)(50 * TimingScale), ct);

            (KeyValueResponseType reacquireType, string holdId2, HLCTimestamp newExpiry) =
                await leader.Kahuna.LocateAndAcquireSnapshotHold("reacquiring-branch", forkT, leaseMs: 60_000, ct);
            Assert.Equal(KeyValueResponseType.Set, reacquireType);
            Assert.Equal(holdId, holdId2);

            HLCTimestamp now = leader.Raft.HybridLogicalClock.TrySendOrLocalEvent(leader.Raft.GetLocalNodeId());
            Assert.True(newExpiry.CompareTo(now) > 0, "revived lease must be live");
        }
        finally
        {
            await LeaveAll(nodes);
            Directory.Delete(root, recursive: true);
        }
    }
}
