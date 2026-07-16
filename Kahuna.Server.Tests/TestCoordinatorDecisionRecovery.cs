
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
/// Acceptance tests for the per-partition-leader recovery driver: it re-drives outstanding durable
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

    private static EmbeddedKahunaOptions TwoPartitionOptions() => new()
    {
        Storage = "memory",
        WalStorage = "memory",
        InitialPartitions = 2,
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

    // ── A receipt forget that cannot be made durable leaves the receipt and the unreleased flag ───────────

    /// <summary>
    /// The decision record persists <c>ReceiptReleased</c> only after the participant's receipt forget is durably
    /// replicated. When the forget cannot be made durable, the receipt stays held and the record keeps the
    /// participant unreleased, so a later sweep re-drives the forget rather than reporting a release that never
    /// reached the participant replicas.
    /// </summary>
    [Fact]
    public async Task Recovery_ReceiptForgetNotDurable_KeepsReceiptAndLeavesReceiptReleasedFalse()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = new(EmbeddedOptions());
        await node.StartAsync(ct);

        KahunaManager manager = (KahunaManager)node.Kahuna;

        HLCTimestamp txId = node.Raft.HybridLogicalClock.TrySendOrLocalEvent(node.Raft.GetLocalNodeId());
        const string anchor = "recover-forget:aaa";

        // A real receipt exists for the acked participant.
        manager.CompletionReceiptStore.Record(txId, anchor, anchor, KeyValueDurability.Persistent);

        CoordinatorDecisionRecord seed = new(
            txId, "coord", anchor, txId, CoordinatorDecisionStatus.CommitDecided,
            [Participant(anchor, acked: true, released: false)], [], txId, HLCTimestamp.Zero);
        await node.Kahuna.ImportCoordinatorDecisions([seed]);

        // The receipt forget cannot be made durable.
        manager.KeyValues.ReplicateReceiptForgetFault = _ => true;

        await Recover(node, NoPurge);

        // The proof is still held and the record still reports the participant unreleased.
        Assert.True(manager.CompletionReceiptStore.Contains(txId, anchor, KeyValueDurability.Persistent));
        Assert.True(Store(node).TryGet(txId, out CoordinatorDecisionRecord rec));
        Assert.False(rec.Participants[0].ReceiptReleased);
    }

    // ── Records anchored to distinct partitions are re-driven concurrently, not head-of-line ──────────────

    /// <summary>
    /// The recovery sweep groups outstanding records by anchor partition and drives distinct partitions
    /// concurrently. A participant that consumes the whole phase-two commit timeout stalls only its own
    /// partition's records — a record anchored to a different partition makes progress in parallel. Proven by
    /// parking every drive on a latch and asserting both partitions' drives are in flight at once; a serial sweep
    /// would enter only the first record's drive and never reach the second.
    /// </summary>
    [Fact]
    public async Task Recovery_DrivesDistinctAnchorPartitionsConcurrently()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = new(TwoPartitionOptions());
        await node.StartAsync(ct);

        KahunaManager manager = (KahunaManager)node.Kahuna;

        string? anchorA = null, anchorB = null;
        int partA = -1, partB = -1;
        for (int i = 0; i < 64 && (anchorA is null || anchorB is null); i++)
        {
            string anchor = $"crec{i}/key";
            int partition = manager.GetDataPartitionForKey(anchor);
            if (anchorA is null) { anchorA = anchor; partA = partition; }
            else if (partition != partA) { anchorB = anchor; partB = partition; }
        }
        Assert.NotNull(anchorA);
        Assert.NotNull(anchorB);
        Assert.NotEqual(partA, partB);

        await node.WaitForLeaderForKeyAsync(anchorA!, ct);
        await node.WaitForLeaderForKeyAsync(anchorB!, ct);

        HLCTimestamp txA = node.Raft.HybridLogicalClock.TrySendOrLocalEvent(node.Raft.GetLocalNodeId());
        HLCTimestamp txB = node.Raft.HybridLogicalClock.TrySendOrLocalEvent(node.Raft.GetLocalNodeId());

        CoordinatorDecisionRecord Seed(HLCTimestamp txId, string anchor) => new(
            txId, "coord", anchor, txId, CoordinatorDecisionStatus.CommitDecided,
            [Participant(anchor, acked: false, released: false)], [], txId, HLCTimestamp.Zero);

        await node.Kahuna.ImportCoordinatorDecisions([Seed(txA, anchorA!), Seed(txB, anchorB!)]);

        TaskCompletionSource enteredA = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource enteredB = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        manager.KeyValues.RecoveryDriveHook = record =>
        {
            int partition = manager.GetDataPartitionForKey(record.RecordAnchorKey);
            if (partition == partA) enteredA.TrySetResult();
            else if (partition == partB) enteredB.TrySetResult();
            return release.Task;
        };

        Task recoverTask = Recover(node, NoPurge);

        // Both partition drives must be in flight at once — a serial sweep would park in the first and never
        // enter the second, so this await would time out.
        await Task.WhenAll(enteredA.Task, enteredB.Task).WaitAsync(TimeSpan.FromSeconds(10), ct);

        release.SetResult();
        await recoverTask.WaitAsync(TimeSpan.FromSeconds(10), ct);
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

    // ── Concurrent drives on one record converge idempotently: single Completed, no double-apply ───────────

    /// <summary>
    /// The request path and the recovery actor may drive the same decision at once, and the underlying commit,
    /// receipt forget, and decision upsert are all designed to be idempotent so correctness does not depend on a
    /// single winner. This forces the worst case — several drives held on a barrier so they all run through the
    /// same record concurrently — and asserts they converge to one <c>Completed</c> record with the frozen
    /// participant set intact (no duplicated participant, no regressed ack/release), never throwing or double
    /// applying.
    /// </summary>
    [Fact]
    public async Task ConcurrentDrivesOnSameRecord_ConvergeToSingleCompleted_NoDoubleApply()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = new(EmbeddedOptions());
        await node.StartAsync(ct);

        KahunaManager manager = (KahunaManager)node.Kahuna;

        const string anchor = "recover-race:anchor";
        const string other  = "recover-race:other";

        // Commit `other` through 2PC so its completion receipt exists — the proof every concurrent re-drive uses
        // to resolve the participant as Committed.
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

        CoordinatorDecisionRecord seed = new(
            txId, "coord", anchor, commitId, CoordinatorDecisionStatus.CommitDecided,
            [Participant(anchor, acked: true, released: false), Participant(other, acked: false, released: false)],
            [], txId, HLCTimestamp.Zero);
        await node.Kahuna.ImportCoordinatorDecisions([seed]);

        // Hold every drive on a barrier so all of them are inside the record's drive at once, maximizing the race.
        const int drives = 5;
        int entered = 0;
        TaskCompletionSource allEntered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        manager.KeyValues.RecoveryDriveHook = _ =>
        {
            if (Interlocked.Increment(ref entered) >= drives)
                allEntered.TrySetResult();
            return allEntered.Task;
        };

        Task[] sweeps = Enumerable.Range(0, drives).Select(_ => Recover(node, NoPurge)).ToArray();
        await Task.WhenAll(sweeps).WaitAsync(TimeSpan.FromSeconds(20), ct);

        // Exactly one record, Completed once, the frozen two-participant set intact, every participant acked and
        // released — a double-apply or a regressed merge would break one of these.
        Assert.Equal(1, Store(node).Count);
        Assert.True(Store(node).TryGet(txId, out CoordinatorDecisionRecord rec));
        Assert.Equal(CoordinatorDecisionStatus.Completed, rec.Status);
        Assert.Equal(2, rec.Participants.Count);
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
            await Task.WhenAll(r1.JoinCluster(ct), r2.JoinCluster(ct), r3.JoinCluster(ct));
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
                try { await n.Raft.LeaveCluster(dispose: true, cancellationToken: ct); }
                catch (ObjectDisposedException) { }
            }
        }
    }
}
