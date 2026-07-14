
using Kahuna.Server.Communication.Internode;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
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
/// Acceptance tests for the per-partition-leader recovery driver (Phase 6.4): it re-drives outstanding durable
/// coordinator decisions to completion on the anchor partition's leader — re-committing not-yet-acked
/// participants, marking a decision <c>Completed</c> once all are acknowledged, and purging completed records
/// after the retention window — while a follower drives nothing.
/// </summary>
[Collection("ClusterTests")]
public sealed class TestCoordinatorDecisionRecovery
{
    private static readonly TimeSpan NoPurge = TimeSpan.FromHours(1);

    private static EmbeddedKahunaOptions EmbeddedOptions() => new()
    {
        Storage = "memory",
        WalStorage = "memory",
        InitialPartitions = 1,
    };

    private static CoordinatorDecisionStore Store(EmbeddedKahunaNode node) =>
        ((KahunaManager)node.Kahuna).CoordinatorDecisionStore;

    private static Task Recover(EmbeddedKahunaNode node, TimeSpan retentionTtl) =>
        ((KahunaManager)node.Kahuna).RecoverOutstandingDecisions(retentionTtl, TestContext.Current.CancellationToken);

    private static CoordinatorParticipant Participant(string key, bool acked, bool released) =>
        new(key, KeyValueDurability.Persistent, HLCTimestamp.Zero, acked, released);

    // ── A fully-acknowledged CommitDecided record is marked Completed and its receipts released ────────────

    [Fact]
    public async Task Recovery_MarksFullyAckedCommitDecidedRecord_Completed()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = new(EmbeddedOptions());
        await node.StartAsync(ct);

        HLCTimestamp txId = node.Raft.HybridLogicalClock.TrySendOrLocalEvent(node.Raft.GetLocalNodeId());
        const string anchor = "recover:aaa";
        CoordinatorDecisionRecord seed = new(
            txId, "coord", anchor, txId, CoordinatorDecisionStatus.CommitDecided,
            [Participant(anchor, acked: true, released: false)], [], txId, HLCTimestamp.Zero);
        await node.Kahuna.ImportCoordinatorDecisions([seed]);

        await Recover(node, NoPurge);

        Assert.True(Store(node).TryGet(txId, out CoordinatorDecisionRecord rec));
        Assert.Equal(CoordinatorDecisionStatus.Completed, rec.Status);
        Assert.True(rec.Participants[0].ReceiptReleased);
        Assert.NotEqual(HLCTimestamp.Zero, rec.CompletedAt);
    }

    // ── A committed-but-unacked participant is re-driven to acknowledged, completing the decision ──────────

    [Fact]
    public async Task Recovery_ReDrivesCommittedButUnackedParticipant_ToCompleted()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = new(EmbeddedOptions());
        await node.StartAsync(ct);

        const string anchor = "recover-drive:anchor";
        const string other  = "recover-drive:other";

        // Commit `other` through 2PC under a transaction id, so its completion receipt exists — the proof a
        // recovery re-drive uses to resolve the participant as Committed.
        HLCTimestamp txId = node.Raft.HybridLogicalClock.TrySendOrLocalEvent(node.Raft.GetLocalNodeId());
        HLCTimestamp commitId = node.Raft.HybridLogicalClock.TrySendOrLocalEvent(node.Raft.GetLocalNodeId());

        (KeyValueResponseType setType, _, _) = await node.Kahuna.TrySetKeyValue(
            txId, other, "v"u8.ToArray(), null, -1, KeyValueFlags.None, 0, KeyValueDurability.Persistent);
        Assert.Equal(KeyValueResponseType.Set, setType);
        (KeyValueResponseType prepType, HLCTimestamp ticket, _, _) = await node.Kahuna.TryPrepareMutations(
            txId, commitId, other, KeyValueDurability.Persistent, recordAnchorKey: anchor);
        Assert.Equal(KeyValueResponseType.Prepared, prepType);
        (KeyValueResponseType commitType, _) = await node.Kahuna.TryCommitMutations(
            txId, other, ticket, KeyValueDurability.Persistent);
        Assert.Equal(KeyValueResponseType.Committed, commitType);

        // Seed a decision that reflects the anchor acknowledged but `other` still pending.
        CoordinatorDecisionRecord seed = new(
            txId, "coord", anchor, commitId, CoordinatorDecisionStatus.CommitDecided,
            [Participant(anchor, acked: true, released: false), Participant(other, acked: false, released: false)],
            [], txId, HLCTimestamp.Zero);
        await node.Kahuna.ImportCoordinatorDecisions([seed]);

        await Recover(node, NoPurge);

        Assert.True(Store(node).TryGet(txId, out CoordinatorDecisionRecord rec));
        Assert.Equal(CoordinatorDecisionStatus.Completed, rec.Status);
        Assert.True(rec.AllParticipantsAcked);
        Assert.All(rec.Participants, p => Assert.True(p.ReceiptReleased));
    }

    // ── A completed record is purged once past the retention window and every receipt is released ──────────

    [Fact]
    public async Task Recovery_PurgesCompletedRecord_AfterRetentionWindow()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = new(EmbeddedOptions());
        await node.StartAsync(ct);

        HLCTimestamp txId = node.Raft.HybridLogicalClock.TrySendOrLocalEvent(node.Raft.GetLocalNodeId());
        const string anchor = "recover-purge:aaa";

        // A Completed record whose completion timestamp is far in the past, all receipts released.
        HLCTimestamp longAgo = new(1, 1, 1);
        CoordinatorDecisionRecord seed = new(
            txId, "coord", anchor, txId, CoordinatorDecisionStatus.Completed,
            [Participant(anchor, acked: true, released: true)], [], txId, longAgo);
        await node.Kahuna.ImportCoordinatorDecisions([seed]);
        Assert.True(Store(node).TryGet(txId, out _));

        // A 1 ms retention window is long exceeded, so the record is purged.
        await Recover(node, TimeSpan.FromMilliseconds(1));

        Assert.False(Store(node).TryGet(txId, out _));
    }

    // ── A completed record within its retention window is retained ─────────────────────────────────────────

    [Fact]
    public async Task Recovery_RetainsCompletedRecord_WithinRetentionWindow()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = new(EmbeddedOptions());
        await node.StartAsync(ct);

        HLCTimestamp txId = node.Raft.HybridLogicalClock.TrySendOrLocalEvent(node.Raft.GetLocalNodeId());
        const string anchor = "recover-retain:aaa";

        HLCTimestamp completedNow = node.Raft.HybridLogicalClock.TrySendOrLocalEvent(node.Raft.GetLocalNodeId());
        CoordinatorDecisionRecord seed = new(
            txId, "coord", anchor, txId, CoordinatorDecisionStatus.Completed,
            [Participant(anchor, acked: true, released: true)], [], txId, completedNow);
        await node.Kahuna.ImportCoordinatorDecisions([seed]);

        await Recover(node, NoPurge);

        Assert.True(Store(node).TryGet(txId, out CoordinatorDecisionRecord rec));
        Assert.Equal(CoordinatorDecisionStatus.Completed, rec.Status);
    }

    // ── A follower (not the anchor partition's leader) drives nothing ──────────────────────────────────────

    private sealed record ClusterNode(RaftManager Raft, KahunaManager Kahuna);

    private (RaftManager, KahunaManager) BuildClusterNode(
        int nodeId, int port, string[] peers, MemoryInterNodeCommmunication interNode, InMemoryCommunication comm)
    {
        ILogger<IRaft>   raftLogger   = NullLogger<IRaft>.Instance;
        ILogger<IKahuna> kahunaLogger = NullLogger<IKahuna>.Instance;

        ActorSystem actorSystem = new(logger: raftLogger);

        RaftConfiguration raftCfg = new()
        {
            NodeName             = "recoverdecision" + nodeId,
            NodeId               = nodeId,
            Host                 = "localhost",
            Port                 = port,
            InitialPartitions    = 2,
            StartElectionTimeout = 50,
            EndElectionTimeout   = 150,
            ElectionTimeoutSeed  = 74000 + nodeId,
            EnableQuiescence     = false
        };

        RaftManager raft = new(
            raftCfg,
            new StaticDiscovery([new(peers[0]), new(peers[1])]),
            new InMemoryWAL(raftLogger),
            comm,
            new HybridLogicalClock(),
            raftLogger);

        Kahuna.Server.Configuration.KahunaConfiguration kahunaConfig = new()
        {
            HttpsCertificate         = "",
            HttpsCertificatePassword = "",
            LocksWorkers             = 8,
            KeyValueWorkers          = 8,
            BackgroundWriterWorkers  = 1,
            Storage                  = "memory",
            StoragePath              = "/tmp",
            StorageRevision          = Guid.NewGuid().ToString(),
            DefaultTransactionTimeout = 5000,
            Phase2CommitTimeout       = 5000,
            ScriptCacheExpiration     = TimeSpan.FromMinutes(1),
        };

        KahunaManager kahuna = new(actorSystem, raft, kahunaConfig, interNode, kahunaLogger);
        raft.OnLogRestored         += kahuna.OnLogRestored;
        raft.OnReplicationReceived += kahuna.OnReplicationReceived;
        raft.OnReplicationError    += kahuna.OnReplicationError;
        raft.OnLeaderChanged       += kahuna.OnLeaderChanged;

        return (raft, kahuna);
    }

    [Fact]
    public async Task Recovery_OnFollower_DoesNotDriveRecord()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        MemoryInterNodeCommmunication interNode = new();
        InMemoryCommunication comm = new();

        string[] p1 = ["localhost:9441", "localhost:9442"];
        string[] p2 = ["localhost:9440", "localhost:9442"];
        string[] p3 = ["localhost:9440", "localhost:9441"];

        (RaftManager r1, KahunaManager k1) = BuildClusterNode(1, 9440, p1, interNode, comm);
        (RaftManager r2, KahunaManager k2) = BuildClusterNode(2, 9441, p2, interNode, comm);
        (RaftManager r3, KahunaManager k3) = BuildClusterNode(3, 9442, p3, interNode, comm);

        interNode.SetNodes(new() { { "localhost:9440", k1 }, { "localhost:9441", k2 }, { "localhost:9442", k3 } });
        comm.SetNodes(new() { { "localhost:9440", r1 }, { "localhost:9441", r2 }, { "localhost:9442", r3 } });

        ClusterNode[] nodes = [new(r1, k1), new(r2, k2), new(r3, k3)];
        try
        {
            await Task.WhenAll(r1.JoinCluster(), r2.JoinCluster(), r3.JoinCluster());
            for (int partition = 0; partition <= 1; partition++)
                while (!await r1.AmILeader(partition, ct) && !await r2.AmILeader(partition, ct) && !await r3.AmILeader(partition, ct))
                    await Task.Delay(50, ct);

            // Pick a node that does NOT lead the data partition (partition 1).
            ClusterNode follower = nodes[0];
            bool found = false;
            foreach (ClusterNode n in nodes)
                if (!await n.Raft.AmILeader(1, ct)) { follower = n; found = true; break; }
            Assert.True(found, "expected at least one follower of partition 1");

            // Seed a fully-acked CommitDecided record on the follower only (local inject, no replication). If the
            // follower drove it, it would become Completed.
            HLCTimestamp txId = follower.Raft.HybridLogicalClock.TrySendOrLocalEvent(follower.Raft.GetLocalNodeId());
            const string anchor = "recover-follower:aaa";
            CoordinatorDecisionRecord seed = new(
                txId, "coord", anchor, txId, CoordinatorDecisionStatus.CommitDecided,
                [Participant(anchor, acked: true, released: false)], [], txId, HLCTimestamp.Zero);
            await follower.Kahuna.ImportCoordinatorDecisions([seed]);

            await follower.Kahuna.RecoverOutstandingDecisions(NoPurge, ct);

            // The follower does not lead the anchor partition, so it left the record untouched.
            Assert.True(follower.Kahuna.CoordinatorDecisionStore.TryGet(txId, out CoordinatorDecisionRecord rec));
            Assert.Equal(CoordinatorDecisionStatus.CommitDecided, rec.Status);
        }
        finally
        {
            foreach (ClusterNode n in nodes)
            {
                try { await n.Raft.LeaveCluster(dispose: true); }
                catch (ObjectDisposedException) { }
            }
        }
    }
}
