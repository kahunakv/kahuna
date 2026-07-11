
using Kahuna.Server.Communication.Internode;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
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
/// Verifies 2PC commit/rollback recovery invariants: participant state is preserved on transient
/// Raft failures (so the coordinator can retry), and commits/rollbacks are idempotent after ack loss.
/// </summary>
[Collection("ClusterTests")]
public sealed class TestTwoPhaseCommitRecovery
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

    private const int ElectionTimeoutSeedBase = 93000;

    private (RaftManager, KahunaManager) BuildClusterNode(
        int nodeId, int port, string[] peers,
        MemoryInterNodeCommmunication interNode, InMemoryCommunication comm,
        int phase2CommitTimeout = 5000)
    {
        ActorSystem actorSystem = new(logger: raftLogger);

        RaftConfiguration raftCfg = new()
        {
            NodeName               = "recovery" + nodeId,
            NodeId                 = nodeId,
            Host                   = "localhost",
            Port                   = port,
            InitialPartitions      = 2,
            StartElectionTimeout   = (int)(50  * TimingScale),
            EndElectionTimeout     = (int)(150 * TimingScale),
            ElectionTimeoutSeed    = ElectionTimeoutSeedBase + nodeId,
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
            HttpsCertificate          = "",
            HttpsCertificatePassword  = "",
            LocksWorkers              = 8,
            KeyValueWorkers           = 8,
            BackgroundWriterWorkers   = 1,
            Storage                   = "memory",
            StoragePath               = "/tmp",
            StorageRevision           = Guid.NewGuid().ToString(),
            DefaultTransactionTimeout = 5000,
            Phase2CommitTimeout       = phase2CommitTimeout,
            ScriptCacheExpiration     = TimeSpan.FromMinutes(1),
        };

        KahunaManager kahuna = new(actorSystem, raft, kahunaConfig, interNode, kahunaLogger);
        raft.OnLogRestored         += kahuna.OnLogRestored;
        raft.OnReplicationReceived += kahuna.OnReplicationReceived;
        raft.OnReplicationError    += kahuna.OnReplicationError;
        raft.OnLeaderChanged       += kahuna.OnLeaderChanged;

        return (raft, kahuna);
    }

    private async Task<Node[]> AssembleCluster(int phase2CommitTimeout = 5000)
    {
        MemoryInterNodeCommmunication interNode = new();
        InMemoryCommunication comm = new();

        string[] p1 = ["localhost:9401", "localhost:9402"];
        string[] p2 = ["localhost:9400", "localhost:9402"];
        string[] p3 = ["localhost:9400", "localhost:9401"];

        (RaftManager r1, KahunaManager k1) = BuildClusterNode(1, 9400, p1, interNode, comm, phase2CommitTimeout);
        (RaftManager r2, KahunaManager k2) = BuildClusterNode(2, 9401, p2, interNode, comm, phase2CommitTimeout);
        (RaftManager r3, KahunaManager k3) = BuildClusterNode(3, 9402, p3, interNode, comm, phase2CommitTimeout);

        interNode.SetNodes(new() { { "localhost:9400", k1 }, { "localhost:9401", k2 }, { "localhost:9402", k3 } });
        comm.SetNodes(new() { { "localhost:9400", r1 }, { "localhost:9401", r2 }, { "localhost:9402", r3 } });

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

    /// <summary>Waits for any leader for <paramref name="partition"/> that is NOT <paramref name="excluded"/>.</summary>
    private static async Task<Node> WaitForNewLeader(int partition, Node excluded, Node[] nodes)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        while (true)
        {
            foreach (Node node in nodes)
                if (node != excluded && await node.Raft.AmILeader(partition, ct))
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

    /// <summary>Retries the operation until the response is no longer MustRetry, up to a deadline.</summary>
    private static async Task<(KeyValueResponseType, long, HLCTimestamp)> RetrySet(
        Func<Task<(KeyValueResponseType, long, HLCTimestamp)>> op, int timeoutMs = 10_000)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        long deadline = Environment.TickCount64 + (long)(timeoutMs * TimingScale);
        while (true)
        {
            (KeyValueResponseType type, long rev, HLCTimestamp ts) = await op();
            if (type != KeyValueResponseType.MustRetry || Environment.TickCount64 >= deadline)
                return (type, rev, ts);
            await Task.Delay(50, ct);
        }
    }

    // Prepare a 2PC transaction for a key on a specific cluster node (must be the leader for that key's partition).
    private static async Task<HLCTimestamp> PrepareTransaction(
        Node leaderNode,
        HLCTimestamp txId,
        HLCTimestamp commitId,
        string key,
        byte[] value)
    {
        (KeyValueResponseType setType, _, _) = await leaderNode.Kahuna.TrySetKeyValue(
            txId, key, value, null, -1, KeyValueFlags.None, 0, KeyValueDurability.Persistent);
        Assert.Equal(KeyValueResponseType.Set, setType);

        (KeyValueResponseType prepType, HLCTimestamp ticketId, _, _) = await leaderNode.Kahuna.TryPrepareMutations(
            txId, commitId, key, KeyValueDurability.Persistent);
        Assert.Equal(KeyValueResponseType.Prepared, prepType);

        return ticketId;
    }

    // ─── tests ───────────────────────────────────────────────────────────────────

    /// <summary>
    /// A commit replayed after the first ack was lost must return Committed rather than Errored.
    /// On the leader node, the first commit succeeds and clears write intent + MVCC. The second
    /// call finds both cleared and returns Committed via the early-exit idempotency path.
    /// </summary>
    [Fact]
    public async Task CommitIsIdempotentAfterAckLoss()
    {
        Node[] nodes = await AssembleCluster();
        try
        {
            CancellationToken ct = TestContext.Current.CancellationToken;

            const string key   = "idem-commit-key";
            byte[]       value = "hello"u8.ToArray();

            (KeyValueResponseType s, _, _) = await RetrySet(() =>
                nodes[0].Kahuna.LocateAndTrySetKeyValue(
                    HLCTimestamp.Zero, key, value, null, -1, KeyValueFlags.None, 0,
                    KeyValueDurability.Persistent, ct));
            Assert.Equal(KeyValueResponseType.Set, s);

            int  keyPartition = 1 + HashUtils.ConsistentHash(key, nodes[0].Raft.Configuration.InitialPartitions);
            Node leaderNode   = await LeaderOf(keyPartition, nodes);

            HybridLogicalClock clock    = leaderNode.Raft.HybridLogicalClock;
            HLCTimestamp       txId     = clock.TrySendOrLocalEvent(leaderNode.Raft.GetLocalNodeId());
            HLCTimestamp       commitId = clock.TrySendOrLocalEvent(leaderNode.Raft.GetLocalNodeId());

            HLCTimestamp ticketId = await PrepareTransaction(leaderNode, txId, commitId, key, "v2"u8.ToArray());

            // First commit: succeeds, clears write intent + MVCC, advances the entry.
            (KeyValueResponseType r1, long _) = await leaderNode.Kahuna.TryCommitMutations(txId, key, ticketId, KeyValueDurability.Persistent);
            Assert.Equal(KeyValueResponseType.Committed, r1);

            // Second commit (simulates ack-loss retry): must return Committed, not Errored.
            (KeyValueResponseType r2, long _) = await leaderNode.Kahuna.TryCommitMutations(txId, key, ticketId, KeyValueDurability.Persistent);
            Assert.Equal(KeyValueResponseType.Committed, r2);
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }

    /// <summary>
    /// A rollback replayed after the first ack was lost must return RolledBack rather than Errored.
    /// The first rollback clears write intent + MVCC; the second call returns RolledBack via the
    /// early-exit idempotency path.
    /// </summary>
    [Fact]
    public async Task RollbackIsIdempotentAfterAckLoss()
    {
        Node[] nodes = await AssembleCluster();
        try
        {
            CancellationToken ct = TestContext.Current.CancellationToken;

            const string key   = "idem-rollback-key";
            byte[]       value = "world"u8.ToArray();

            (KeyValueResponseType s, _, _) = await RetrySet(() =>
                nodes[0].Kahuna.LocateAndTrySetKeyValue(
                    HLCTimestamp.Zero, key, value, null, -1, KeyValueFlags.None, 0,
                    KeyValueDurability.Persistent, ct));
            Assert.Equal(KeyValueResponseType.Set, s);

            int  keyPartition = 1 + HashUtils.ConsistentHash(key, nodes[0].Raft.Configuration.InitialPartitions);
            Node leaderNode   = await LeaderOf(keyPartition, nodes);

            HybridLogicalClock clock    = leaderNode.Raft.HybridLogicalClock;
            HLCTimestamp       txId     = clock.TrySendOrLocalEvent(leaderNode.Raft.GetLocalNodeId());
            HLCTimestamp       commitId = clock.TrySendOrLocalEvent(leaderNode.Raft.GetLocalNodeId());

            HLCTimestamp ticketId = await PrepareTransaction(leaderNode, txId, commitId, key, "v2"u8.ToArray());

            // First rollback: succeeds, clears write intent + MVCC.
            (KeyValueResponseType r1, long _) = await leaderNode.Kahuna.TryRollbackMutations(txId, key, ticketId, KeyValueDurability.Persistent);
            Assert.Equal(KeyValueResponseType.RolledBack, r1);

            // Second rollback (simulates ack-loss retry): must return RolledBack, not Errored.
            (KeyValueResponseType r2, long _) = await leaderNode.Kahuna.TryRollbackMutations(txId, key, ticketId, KeyValueDurability.Persistent);
            Assert.Equal(KeyValueResponseType.RolledBack, r2);
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }

    /// <summary>
    /// When CommitLogs returns NodeIsNotLeader (because the leader stepped down between prepare and
    /// commit), the handler must return MustRetry and leave MVCC + write intent intact.
    ///
    /// State preservation is verified by calling TryRollbackMutations on the same non-leader node
    /// afterward: it finds the write intent still present (also MustRetry) rather than Errored
    /// ("write intent missing"), which would be the result if commit had erroneously cleared state.
    /// </summary>
    [Fact]
    public async Task CommitParticipant_TransientNotLeader_RetainsStateAndReturnsMustRetry()
    {
        Node[] nodes = await AssembleCluster();
        try
        {
            CancellationToken ct = TestContext.Current.CancellationToken;

            const string key   = "2pc-transient-commit-key";
            byte[]       value = "v1"u8.ToArray();

            (KeyValueResponseType setType, _, _) = await RetrySet(() =>
                nodes[0].Kahuna.LocateAndTrySetKeyValue(
                    HLCTimestamp.Zero, key, value, null, -1,
                    KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct));
            Assert.Equal(KeyValueResponseType.Set, setType);

            int  keyPartition = 1 + HashUtils.ConsistentHash(key, nodes[0].Raft.Configuration.InitialPartitions);
            Node leaderNode   = await LeaderOf(keyPartition, nodes);

            HybridLogicalClock clock    = leaderNode.Raft.HybridLogicalClock;
            HLCTimestamp       txId     = clock.TrySendOrLocalEvent(leaderNode.Raft.GetLocalNodeId());
            HLCTimestamp       commitId = clock.TrySendOrLocalEvent(leaderNode.Raft.GetLocalNodeId());

            HLCTimestamp ticketId = await PrepareTransaction(leaderNode, txId, commitId, key, "v2"u8.ToArray());

            // Step down the leader so CommitLogs returns NodeIsNotLeader.
            await leaderNode.Raft.StepDownAsync(keyPartition, ct);
            await Task.Delay((int)(100 * TimingScale), ct);

            // Commit directly on the former leader: NodeIsNotLeader → MustRetry, state preserved.
            (KeyValueResponseType commitType, long _) =
                await leaderNode.Kahuna.TryCommitMutations(txId, key, ticketId, KeyValueDurability.Persistent);
            Assert.Equal(KeyValueResponseType.MustRetry, commitType);

            // State preserved: rollback on same non-leader also returns MustRetry (write intent found).
            // Old bug: commit would have cleared intent → rollback returns Errored.
            (KeyValueResponseType rollbackType, long _) =
                await leaderNode.Kahuna.TryRollbackMutations(txId, key, ticketId, KeyValueDurability.Persistent);
            Assert.Equal(KeyValueResponseType.MustRetry, rollbackType);
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }

    /// <summary>
    /// When RollbackLogs returns NodeIsNotLeader (transient), the rollback handler must return
    /// MustRetry and leave MVCC + write intent intact.
    ///
    /// State preservation is verified by calling TryCommitMutations afterward: it also returns
    /// MustRetry (not Errored) because the write intent is still present on the non-leader node.
    /// </summary>
    [Fact]
    public async Task RollbackParticipant_TransientFailure_StaysScheduled()
    {
        Node[] nodes = await AssembleCluster();
        try
        {
            CancellationToken ct = TestContext.Current.CancellationToken;

            const string key   = "2pc-transient-rollback-key";
            byte[]       value = "initial"u8.ToArray();

            (KeyValueResponseType setType, _, _) = await RetrySet(() =>
                nodes[0].Kahuna.LocateAndTrySetKeyValue(
                    HLCTimestamp.Zero, key, value, null, -1,
                    KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct));
            Assert.Equal(KeyValueResponseType.Set, setType);

            int  keyPartition = 1 + HashUtils.ConsistentHash(key, nodes[0].Raft.Configuration.InitialPartitions);
            Node leaderNode   = await LeaderOf(keyPartition, nodes);

            HybridLogicalClock clock    = leaderNode.Raft.HybridLogicalClock;
            HLCTimestamp       txId     = clock.TrySendOrLocalEvent(leaderNode.Raft.GetLocalNodeId());
            HLCTimestamp       commitId = clock.TrySendOrLocalEvent(leaderNode.Raft.GetLocalNodeId());

            HLCTimestamp ticketId = await PrepareTransaction(leaderNode, txId, commitId, key, "updated"u8.ToArray());

            await leaderNode.Raft.StepDownAsync(keyPartition, ct);
            await Task.Delay((int)(100 * TimingScale), ct);

            // Rollback on former leader: NodeIsNotLeader → MustRetry, state preserved.
            (KeyValueResponseType rollbackType, long _) =
                await leaderNode.Kahuna.TryRollbackMutations(txId, key, ticketId, KeyValueDurability.Persistent);
            Assert.Equal(KeyValueResponseType.MustRetry, rollbackType);

            // State preserved: commit on same non-leader also returns MustRetry.
            // Old bug: rollback would have cleared intent → commit returns Errored.
            (KeyValueResponseType commitType, long _) =
                await leaderNode.Kahuna.TryCommitMutations(txId, key, ticketId, KeyValueDurability.Persistent);
            Assert.Equal(KeyValueResponseType.MustRetry, commitType);
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }

    /// <summary>
    /// Both keys in a 2-key transaction commit idempotently. A second commit for each key returns
    /// Committed via the write-intent-null + MVCC-gone early exit, simulating ack-loss retry in a
    /// multi-key 2PC scenario.
    /// </summary>
    [Fact]
    public async Task MultiKeyCommit_BothParticipantsIdempotentAfterAckLoss()
    {
        Node[] nodes = await AssembleCluster();
        try
        {
            CancellationToken ct = TestContext.Current.CancellationToken;

            const string key1 = "multi-idem-key-1";
            const string key2 = "multi-idem-key-2";
            byte[]       v1   = "alpha"u8.ToArray();
            byte[]       v2   = "beta"u8.ToArray();

            // Set initial values so actors have entries for both keys.
            (KeyValueResponseType s1, _, _) = await RetrySet(() =>
                nodes[0].Kahuna.LocateAndTrySetKeyValue(HLCTimestamp.Zero, key1, v1, null, -1, KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct));
            Assert.Equal(KeyValueResponseType.Set, s1);
            (KeyValueResponseType s2, _, _) = await RetrySet(() =>
                nodes[0].Kahuna.LocateAndTrySetKeyValue(HLCTimestamp.Zero, key2, v2, null, -1, KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct));
            Assert.Equal(KeyValueResponseType.Set, s2);

            // Find the leader for key1's partition (both keys may or may not be on the same partition).
            int  kp1     = 1 + HashUtils.ConsistentHash(key1, nodes[0].Raft.Configuration.InitialPartitions);
            int  kp2     = 1 + HashUtils.ConsistentHash(key2, nodes[0].Raft.Configuration.InitialPartitions);
            Node leader1 = await LeaderOf(kp1, nodes);
            Node leader2 = await LeaderOf(kp2, nodes);

            HybridLogicalClock clock = leader1.Raft.HybridLogicalClock;
            HLCTimestamp txId        = clock.TrySendOrLocalEvent(leader1.Raft.GetLocalNodeId());
            HLCTimestamp commitId    = clock.TrySendOrLocalEvent(leader1.Raft.GetLocalNodeId());

            HLCTimestamp t1 = await PrepareTransaction(leader1, txId, commitId, key1, "alpha2"u8.ToArray());
            HLCTimestamp t2 = await PrepareTransaction(leader2, txId, commitId, key2, "beta2"u8.ToArray());

            // First commit for both keys.
            (KeyValueResponseType c1a, long _) = await leader1.Kahuna.TryCommitMutations(txId, key1, t1, KeyValueDurability.Persistent);
            Assert.Equal(KeyValueResponseType.Committed, c1a);
            (KeyValueResponseType c2a, long _) = await leader2.Kahuna.TryCommitMutations(txId, key2, t2, KeyValueDurability.Persistent);
            Assert.Equal(KeyValueResponseType.Committed, c2a);

            // Second commit (ack-loss retry): both must return Committed, not Errored.
            (KeyValueResponseType c1b, long _) = await leader1.Kahuna.TryCommitMutations(txId, key1, t1, KeyValueDurability.Persistent);
            Assert.Equal(KeyValueResponseType.Committed, c1b);
            (KeyValueResponseType c2b, long _) = await leader2.Kahuna.TryCommitMutations(txId, key2, t2, KeyValueDurability.Persistent);
            Assert.Equal(KeyValueResponseType.Committed, c2b);
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }

    /// <summary>
    /// Two keys are written inside a coordinator-managed transaction and committed through the full
    /// 2PC coordinator path: LocateAndStartTransaction → LocateAndTrySetKeyValue × 2 →
    /// LocateAndCommitTransaction → TwoPhaseCommit → PrepareMutations → CommitMutations.
    /// Both keys must be readable with their new values after commit.
    ///
    /// This is the first direct test of the coordinator's CommitMutations retry loop: previously
    /// the coordinator path had no test coverage and was exercised only by scripted-transaction
    /// happy-path tests that never touched the retry branch.
    ///
    /// Note: the retry branch for leader-change mid-phase-two is not exercised here because prepare
    /// state is held only in the partition leader's actor memory (not Raft-replicated), so after a
    /// leader change the new leader has no write intent. With the bounded committed-txn guard
    /// (WasCommittedHere), the new leader now returns MustRetry (not a false-positive Committed),
    /// visible as an abort to the coordinator. Full failover recovery with a durable coordinator
    /// decision still requires Task B2.
    /// </summary>
    [Fact]
    public async Task MultiKeyCommit_CoordinatorDrivesFullTwoPhaseCommit()
    {
        Node[] nodes = await AssembleCluster();
        try
        {
            CancellationToken ct = TestContext.Current.CancellationToken;

            const string uniqueId = "coord-txn-session-1";
            const string key1     = "coord-txn-alpha";
            const string key2     = "coord-txn-beta";
            byte[]       init1    = "initial-alpha"u8.ToArray();
            byte[]       init2    = "initial-beta"u8.ToArray();
            byte[]       new1     = "new-alpha"u8.ToArray();
            byte[]       new2     = "new-beta"u8.ToArray();

            // Establish initial values so the key actors have entries for both keys.
            (KeyValueResponseType si1, _, _) = await RetrySet(() =>
                nodes[0].Kahuna.LocateAndTrySetKeyValue(HLCTimestamp.Zero, key1, init1, null, -1,
                    KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct));
            Assert.Equal(KeyValueResponseType.Set, si1);
            (KeyValueResponseType si2, _, _) = await RetrySet(() =>
                nodes[0].Kahuna.LocateAndTrySetKeyValue(HLCTimestamp.Zero, key2, init2, null, -1,
                    KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct));
            Assert.Equal(KeyValueResponseType.Set, si2);

            // Start a coordinator transaction. UniqueId routes the session to a coordinator node.
            (KeyValueResponseType startType, HLCTimestamp txId) =
                await nodes[0].Kahuna.LocateAndStartTransaction(
                    new KeyValueTransactionOptions
                    {
                        UniqueId     = uniqueId,
                        Locking      = KeyValueTransactionLocking.Pessimistic,
                        AsyncRelease = true,
                        Timeout      = 10_000,
                    },
                    ct);
            Assert.Equal(KeyValueResponseType.Set, startType);
            Assert.NotEqual(HLCTimestamp.Zero, txId);

            // Write both keys inside the transaction (routes to each key's partition leader).
            (KeyValueResponseType set1, _, _) = await RetrySet(() =>
                nodes[0].Kahuna.LocateAndTrySetKeyValue(txId, key1, new1, null, -1,
                    KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct));
            Assert.Equal(KeyValueResponseType.Set, set1);
            (KeyValueResponseType set2, _, _) = await RetrySet(() =>
                nodes[0].Kahuna.LocateAndTrySetKeyValue(txId, key2, new2, null, -1,
                    KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct));
            Assert.Equal(KeyValueResponseType.Set, set2);

            // Commit via coordinator: drives TwoPhaseCommit → PrepareMutations → CommitMutations.
            // Pass both keys in acquiredLocks and modifiedKeys (pessimistic write-only transaction).
            List<KeyValueTransactionModifiedKey> modKeys =
            [
                new() { Key = key1, Durability = KeyValueDurability.Persistent },
                new() { Key = key2, Durability = KeyValueDurability.Persistent },
            ];
            KeyValueResponseType commitResult = await nodes[0].Kahuna.LocateAndCommitTransaction(
                uniqueId, txId, acquiredLocks: modKeys, modifiedKeys: modKeys, readKeys: [], ct);
            Assert.Equal(KeyValueResponseType.Committed, commitResult);

            // Read both keys back and verify the new values are durably committed.
            (KeyValueResponseType r1, ReadOnlyKeyValueEntry? e1) = await nodes[0].Kahuna.LocateAndTryGetValue(
                HLCTimestamp.Zero, key1, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Get, r1);
            Assert.Equal(new1, e1!.Value);

            (KeyValueResponseType r2, ReadOnlyKeyValueEntry? e2) = await nodes[0].Kahuna.LocateAndTryGetValue(
                HLCTimestamp.Zero, key2, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Get, r2);
            Assert.Equal(new2, e2!.Value);
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }

    /// <summary>
    /// Deadline-bounded phase two: with an aggressively small Phase2CommitTimeout the initial
    /// CommitLogs wait trips before the cross-node commit confirms, returning the retryable
    /// OperationCancelled (mapped to MustRetry). The commit is nevertheless enqueued at the Raft
    /// layer and applies shortly after, so the coordinator's next retry re-issues CommitLogs for the
    /// same ticket and gets an idempotent Success — the transaction commits with no data loss and no
    /// permanent abort. This exercises the deadline → transient → retry → idempotent-recommit path
    /// end to end (composing the deadline bound with Task B's retained-state retry and Kommander's
    /// idempotent re-commit).
    /// </summary>
    [Fact]
    public async Task DeadlineBoundedCommit_TripsThenCommitsViaIdempotentRetry()
    {
        // 1 ms per-attempt commit deadline: the initial cross-node CommitLogs cannot confirm that
        // fast, so it is cancelled and retried; the retry re-drives the same ticket idempotently.
        Node[] nodes = await AssembleCluster(phase2CommitTimeout: 1);
        try
        {
            CancellationToken ct = TestContext.Current.CancellationToken;

            const string uniqueId = "deadline-txn-session-1";
            const string key1     = "deadline-txn-alpha";
            const string key2     = "deadline-txn-beta";
            byte[]       init1    = "initial-alpha"u8.ToArray();
            byte[]       init2    = "initial-beta"u8.ToArray();
            byte[]       new1     = "new-alpha"u8.ToArray();
            byte[]       new2     = "new-beta"u8.ToArray();

            (KeyValueResponseType si1, _, _) = await RetrySet(() =>
                nodes[0].Kahuna.LocateAndTrySetKeyValue(HLCTimestamp.Zero, key1, init1, null, -1,
                    KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct));
            Assert.Equal(KeyValueResponseType.Set, si1);
            (KeyValueResponseType si2, _, _) = await RetrySet(() =>
                nodes[0].Kahuna.LocateAndTrySetKeyValue(HLCTimestamp.Zero, key2, init2, null, -1,
                    KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct));
            Assert.Equal(KeyValueResponseType.Set, si2);

            (KeyValueResponseType startType, HLCTimestamp txId) =
                await nodes[0].Kahuna.LocateAndStartTransaction(
                    new KeyValueTransactionOptions
                    {
                        UniqueId     = uniqueId,
                        Locking      = KeyValueTransactionLocking.Pessimistic,
                        AsyncRelease = true,
                        Timeout      = 10_000,
                    },
                    ct);
            Assert.Equal(KeyValueResponseType.Set, startType);
            Assert.NotEqual(HLCTimestamp.Zero, txId);

            (KeyValueResponseType set1, _, _) = await RetrySet(() =>
                nodes[0].Kahuna.LocateAndTrySetKeyValue(txId, key1, new1, null, -1,
                    KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct));
            Assert.Equal(KeyValueResponseType.Set, set1);
            (KeyValueResponseType set2, _, _) = await RetrySet(() =>
                nodes[0].Kahuna.LocateAndTrySetKeyValue(txId, key2, new2, null, -1,
                    KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct));
            Assert.Equal(KeyValueResponseType.Set, set2);

            List<KeyValueTransactionModifiedKey> modKeys =
            [
                new() { Key = key1, Durability = KeyValueDurability.Persistent },
                new() { Key = key2, Durability = KeyValueDurability.Persistent },
            ];

            // Despite the sub-millisecond per-attempt deadline tripping the initial commit, the
            // coordinator's idempotent retry drives the transaction to a durable commit.
            KeyValueResponseType commitResult = await nodes[0].Kahuna.LocateAndCommitTransaction(
                uniqueId, txId, acquiredLocks: modKeys, modifiedKeys: modKeys, readKeys: [], ct);
            Assert.Equal(KeyValueResponseType.Committed, commitResult);

            (KeyValueResponseType r1, ReadOnlyKeyValueEntry? e1) = await nodes[0].Kahuna.LocateAndTryGetValue(
                HLCTimestamp.Zero, key1, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Get, r1);
            Assert.Equal(new1, e1!.Value);

            (KeyValueResponseType r2, ReadOnlyKeyValueEntry? e2) = await nodes[0].Kahuna.LocateAndTryGetValue(
                HLCTimestamp.Zero, key2, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Get, r2);
            Assert.Equal(new2, e2!.Value);
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }

    /// <summary>
    /// Regression test for the silent data-loss guard introduced in Task B.
    ///
    /// Prepare state (WriteIntent + MVCC) is set only on the preparing leader's actor and is NOT
    /// Raft-replicated. InvalidateOrApply (which propagates committed writes to follower actors) is
    /// a no-op when the key is not already in the follower's BTree, so only a node that was
    /// previously the leader for the partition (and wrote the key) retains the entry in its BTree.
    ///
    /// The test drives this scenario by:
    ///   1. Setting the key on the initial leader (populates that node's BTree)
    ///   2. Stepping down the initial leader
    ///   3. Preparing the transaction on the new leader (only the new leader gets WriteIntent+MVCC)
    ///   4. Routing TryCommitMutations to the old leader (has the key entry but no write intent)
    ///
    /// Before the fix: WriteIntent==null &amp;&amp; MVCC gone → Committed (silent data loss). After the
    /// fix: WasCommittedHere is false on the old leader, so the guard returns MustRetry instead.
    /// </summary>
    [Fact]
    public async Task CommitOnUnpreparedNode_ReturnsMustRetryNotCommitted()
    {
        Node[] nodes = await AssembleCluster();
        try
        {
            CancellationToken ct = TestContext.Current.CancellationToken;

            const string key = "unprepared-commit-node-key";
            byte[]       v1  = "original"u8.ToArray();

            int  keyPartition  = 1 + HashUtils.ConsistentHash(key, nodes[0].Raft.Configuration.InitialPartitions);
            Node initialLeader = await LeaderOf(keyPartition, nodes);

            // Set the key on the initial leader: this node's BTree will have the committed entry.
            (KeyValueResponseType s, _, _) = await RetrySet(() =>
                initialLeader.Kahuna.TrySetKeyValue(HLCTimestamp.Zero, key, v1, null, -1,
                    KeyValueFlags.None, 0, KeyValueDurability.Persistent));
            Assert.Equal(KeyValueResponseType.Set, s);

            // Step down the initial leader so a different node wins the next election.
            await initialLeader.Raft.StepDownAsync(keyPartition, ct);
            await Task.Delay((int)(100 * TimingScale), ct);

            // Wait for a different node to become leader — it will prepare the transaction.
            Node preparingLeader = await WaitForNewLeader(keyPartition, initialLeader, nodes);

            HybridLogicalClock clock    = initialLeader.Raft.HybridLogicalClock;
            HLCTimestamp       txId     = clock.TrySendOrLocalEvent(initialLeader.Raft.GetLocalNodeId());
            HLCTimestamp       commitId = clock.TrySendOrLocalEvent(initialLeader.Raft.GetLocalNodeId());

            // Prepare on the new leader only — WriteIntent + MVCC live only in preparingLeader's actor.
            HLCTimestamp ticketId = await PrepareTransaction(preparingLeader, txId, commitId, key, "v2"u8.ToArray());

            // Route commit to the old leader: its actor has the base-value entry (from step 1)
            // but no WriteIntent and no MVCC for txId. This is the leader-change failover scenario.
            (KeyValueResponseType commitType, long _) =
                await initialLeader.Kahuna.TryCommitMutations(txId, key, ticketId, KeyValueDurability.Persistent);
            Assert.Equal(KeyValueResponseType.MustRetry, commitType);
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }

    /// <summary>
    /// Regression test: same leader-change scenario as CommitOnUnpreparedNode_ReturnsMustRetryNotCommitted
    /// but for the rollback path. Before the fix a rollback arriving at a node that never prepared
    /// the transaction returned RolledBack (false positive). After the fix it returns MustRetry.
    /// </summary>
    [Fact]
    public async Task RollbackOnUnpreparedNode_ReturnsMustRetryNotRolledBack()
    {
        Node[] nodes = await AssembleCluster();
        try
        {
            CancellationToken ct = TestContext.Current.CancellationToken;

            const string key = "unprepared-rollback-node-key";
            byte[]       v1  = "original"u8.ToArray();

            int  keyPartition  = 1 + HashUtils.ConsistentHash(key, nodes[0].Raft.Configuration.InitialPartitions);
            Node initialLeader = await LeaderOf(keyPartition, nodes);

            (KeyValueResponseType s, _, _) = await RetrySet(() =>
                initialLeader.Kahuna.TrySetKeyValue(HLCTimestamp.Zero, key, v1, null, -1,
                    KeyValueFlags.None, 0, KeyValueDurability.Persistent));
            Assert.Equal(KeyValueResponseType.Set, s);

            await initialLeader.Raft.StepDownAsync(keyPartition, ct);
            await Task.Delay((int)(100 * TimingScale), ct);

            Node preparingLeader = await WaitForNewLeader(keyPartition, initialLeader, nodes);

            HybridLogicalClock clock    = initialLeader.Raft.HybridLogicalClock;
            HLCTimestamp       txId     = clock.TrySendOrLocalEvent(initialLeader.Raft.GetLocalNodeId());
            HLCTimestamp       commitId = clock.TrySendOrLocalEvent(initialLeader.Raft.GetLocalNodeId());

            HLCTimestamp ticketId = await PrepareTransaction(preparingLeader, txId, commitId, key, "v2"u8.ToArray());

            (KeyValueResponseType rollbackType, long _) =
                await initialLeader.Kahuna.TryRollbackMutations(txId, key, ticketId, KeyValueDurability.Persistent);
            Assert.Equal(KeyValueResponseType.MustRetry, rollbackType);
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }
}
