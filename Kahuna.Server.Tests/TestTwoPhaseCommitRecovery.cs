
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
/// Verifies coordinator-driven two-phase commit across a multi-node cluster: a full commit through the
/// durable-intent path (both partitions durable, no manual ticket path), a coordinator that leads none of the
/// participant partitions (all routing forwarded), and a deadline-bounded commit that trips then completes via
/// idempotent retry. The participant-side recovery invariants (ack-loss idempotency, MustRetry-on-unprepared,
/// receipt/leader-change resolution) are covered for the durable path in TestDurableOutcomeConsultation,
/// TestDurableStateTransfer, and TestDurableReadRoutedResolution.
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
            // Keep per-node IO/executor threads at 1 so parallel test clusters don't inflate the process
            // thread count (production embedding keeps its own defaults).
            ReadIOThreads          = 1,
            WriteIOThreads         = 1,
            PartitionExecutorPoolSize = 1,
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

        TestClusterNodeRegistry.Register(raft, kahuna, actorSystem);

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
            try { await TestClusterNodeRegistry.DisposeAsync(node.Raft); }
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

            // Start a coordinator transaction (coordinator key routes the session to a stable node).
            (KeyValueResponseType startType, TransactionHandle txHandle) =
                await nodes[0].Kahuna.LocateAndStartTransaction(
                    new KeyValueTransactionOptions
                    {
                        CoordinatorKey = uniqueId,
                        Locking        = KeyValueTransactionLocking.Pessimistic,
                        AsyncRelease   = true,
                        Timeout        = 10_000,
                    },
                    ct);
            Assert.Equal(KeyValueResponseType.Set, startType);
            HLCTimestamp txId = txHandle.TransactionId;
            Assert.NotEqual(HLCTimestamp.Zero, txId);

            // Write both keys inside the transaction via the register-remote path, so each confirmed effect
            // folds into the coordinator's working set (routes to each key's partition leader).
            (KeyValueResponseType set1, _, _) = await RetrySet(() =>
                nodes[0].Kahuna.LocateAndTrySetKeyValue(txId, key1, new1, null, -1,
                    KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct,
                    coordinatorKey: txHandle.CoordinatorKey, operationId: TransactionOperationId.NewRandom()));
            Assert.Equal(KeyValueResponseType.Set, set1);
            (KeyValueResponseType set2, _, _) = await RetrySet(() =>
                nodes[0].Kahuna.LocateAndTrySetKeyValue(txId, key2, new2, null, -1,
                    KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct,
                    coordinatorKey: txHandle.CoordinatorKey, operationId: TransactionOperationId.NewRandom()));
            Assert.Equal(KeyValueResponseType.Set, set2);

            // Commit via coordinator: drives TwoPhaseCommit → PrepareMutations → CommitMutations from the
            // server-owned working set (the confirmed effects folded above).
            (KeyValueResponseType commitResult, _) = await nodes[0].Kahuna.LocateAndCommitTransaction(txHandle, ct);
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

            // Multi-node durable guard: a real coordinator-driven persistent transaction settles through the
            // durable-intent path, never the manual ticket path. The manual persistent-settlement count must be
            // zero on every node, and a canonical durable record must exist. (This is the multi-node counterpart
            // to the single-node TestManualTicketPathInvariant guard.)
            long manual = nodes.Sum(n => n.Kahuna.ManualTicketPersistentSettlementCount);
            long records = nodes.Sum(n => n.Kahuna.DurableTransactionRecordStore.Count);
            Assert.Equal(0, manual);
            Assert.True(records > 0, $"expected a durable record; manual={manual} records={records}");
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }

    /// <summary>
    /// The durable-intent 2PC path across nodes: an all-persistent script transaction is finalized on a
    /// coordinator node that leads <b>none</b> of the transaction's participant partitions, so every
    /// canonical-record initialization, intent prepare, decision, and committed-value materialization must be
    /// forwarded to a remote partition leader over the inter-node transport (the durable-operation routing). The
    /// commit must reach <c>Set</c> with no spurious <c>MustRetry</c>, and every committed value must be readable
    /// from all three nodes — proving the routed durable path both decides and materializes correctly on remote
    /// leaders, not only when the coordinator happens to lead the participants.
    /// </summary>
    [Fact]
    public async Task DurableScriptTransaction_CoordinatorLeadsNoParticipant_CommitsAndReadsBackClusterWide()
    {
        Node[] nodes = await AssembleCluster();
        try
        {
            CancellationToken ct = TestContext.Current.CancellationToken;

            const int count = 8;
            string[] keys = new string[count];
            byte[][] values = new byte[count][];
            for (int i = 0; i < count; i++)
            {
                keys[i] = $"durroute/key-{i}";
                values[i] = System.Text.Encoding.UTF8.GetBytes($"value-{i}");
            }

            // Resolve the participant partitions and their current leaders, then pick a coordinator node that
            // leads none of them: that guarantees every durable operation (record init, prepare, decision,
            // materialization) is forwarded to a remote leader instead of applied locally.
            HashSet<int> participantPartitions = [];
            foreach (string key in keys)
                participantPartitions.Add(nodes[0].Kahuna.GetDataPartitionForKey(key));

            HashSet<Node> participantLeaders = [];
            foreach (int partition in participantPartitions)
                participantLeaders.Add(await LeaderOf(partition, nodes));

            Node coordinator = nodes.First(node => !participantLeaders.Contains(node));
            Assert.DoesNotContain(coordinator, participantLeaders);

            string script = "BEGIN " + string.Concat(keys.Select((k, i) => $"SET `{k}` 'value-{i}' ")) + "COMMIT END";
            byte[] scriptBytes = System.Text.Encoding.UTF8.GetBytes(script);

            // A retry re-runs the whole script as a fresh transaction; an idempotent SET makes that safe. The
            // point of the test is that no retry should be needed for a correctness reason — only transient
            // young-cluster settling.
            KeyValueResponseType commitType = KeyValueResponseType.MustRetry;
            for (int attempt = 0; attempt < 40 && commitType == KeyValueResponseType.MustRetry; attempt++)
            {
                KeyValueTransactionResult result = await coordinator.Kahuna.TryExecuteTransactionScript(scriptBytes, null, null);
                commitType = result.Type;
                if (commitType == KeyValueResponseType.MustRetry)
                    await Task.Delay(100, ct);
            }
            Assert.Equal(KeyValueResponseType.Set, commitType);

            // Every committed value is readable from every node (reads route to the partition leader), proving the
            // routed durable materialization landed on the remote participant leaders.
            for (int n = 0; n < nodes.Length; n++)
                for (int i = 0; i < count; i++)
                    await AssertKeyReadable(nodes[n], keys[i], values[i], ct);
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }

    private static async Task AssertKeyReadable(Node node, string key, byte[] expected, CancellationToken ct)
    {
        for (int attempt = 0; attempt < 50; attempt++)
        {
            (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await node.Kahuna.LocateAndTryGetValue(
                HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);

            if (type == KeyValueResponseType.Get && entry is not null)
            {
                Assert.Equal(expected, entry.Value);
                return;
            }

            await Task.Delay(100, ct);
        }

        Assert.Fail($"Key {key} was not readable with the committed value from the node");
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

            (KeyValueResponseType startType, TransactionHandle txHandle2) =
                await nodes[0].Kahuna.LocateAndStartTransaction(
                    new KeyValueTransactionOptions
                    {
                        CoordinatorKey = uniqueId,
                        Locking        = KeyValueTransactionLocking.Pessimistic,
                        AsyncRelease   = true,
                        Timeout        = 10_000,
                    },
                    ct);
            Assert.Equal(KeyValueResponseType.Set, startType);
            HLCTimestamp txId = txHandle2.TransactionId;
            Assert.NotEqual(HLCTimestamp.Zero, txId);

            (KeyValueResponseType set1, _, _) = await RetrySet(() =>
                nodes[0].Kahuna.LocateAndTrySetKeyValue(txId, key1, new1, null, -1,
                    KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct,
                    coordinatorKey: txHandle2.CoordinatorKey, operationId: TransactionOperationId.NewRandom()));
            Assert.Equal(KeyValueResponseType.Set, set1);
            (KeyValueResponseType set2, _, _) = await RetrySet(() =>
                nodes[0].Kahuna.LocateAndTrySetKeyValue(txId, key2, new2, null, -1,
                    KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct,
                    coordinatorKey: txHandle2.CoordinatorKey, operationId: TransactionOperationId.NewRandom()));
            Assert.Equal(KeyValueResponseType.Set, set2);

            // Despite the sub-millisecond per-attempt deadline tripping the initial commit, the
            // coordinator's idempotent retry drives the transaction to a durable commit.
            (KeyValueResponseType commitResult, _) = await nodes[0].Kahuna.LocateAndCommitTransaction(txHandle2, ct);
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
}
