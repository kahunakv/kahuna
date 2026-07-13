
using Kahuna.Server.Communication.Internode;
using Kahuna.Server.KeyValues;
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
/// Acceptance tests for the durable coordinator decision integrated into two-phase commit: the anchored record
/// installed atomically with the anchor commit, ack ordering, completion, the durable-policy prepare guards,
/// and capacity rejection.
///
/// <para>The full request-path lifecycle (progress acks → <c>Completed</c>) only advances when the coordinator
/// session runs on the node that leads the anchor's data partition — a follower cannot replicate the progress
/// delta, and driving that to a remote anchor leader is the per-partition-leader recovery driver's job. The
/// coordinator session is routed by its coordinator key, which need not share a partition with the anchor, so
/// the lifecycle tests run on a single embedded node (one leader for every partition) to make colocation
/// deterministic. A separate multi-node test asserts the initial record replicates to every node from the one
/// committed anchor proposal.</para>
/// </summary>
[Collection("ClusterTests")]
public sealed class TestDurableCoordinatorDecision
{
    private static EmbeddedKahunaOptions EmbeddedOptions(int outcomeRetentionMax = 10_000) => new()
    {
        Storage                        = "memory",
        WalStorage                     = "memory",
        InitialPartitions              = 1,
        TransactionOutcomeRetentionMax = outcomeRetentionMax,
    };

    private static CoordinatorDecisionStore Store(EmbeddedKahunaNode node) =>
        ((KahunaManager)node.Kahuna).CoordinatorDecisionStore;

    private static async Task<(KeyValueResponseType, long, HLCTimestamp)> RetrySet(
        Func<Task<(KeyValueResponseType, long, HLCTimestamp)>> op, int timeoutMs = 10_000)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        long deadline = Environment.TickCount64 + timeoutMs;
        while (true)
        {
            (KeyValueResponseType type, long rev, HLCTimestamp ts) = await op();
            if (type != KeyValueResponseType.MustRetry || Environment.TickCount64 >= deadline)
                return (type, rev, ts);
            await Task.Delay(50, ct);
        }
    }

    private static async Task<CoordinatorDecisionRecord?> WaitForDecision(
        EmbeddedKahunaNode node, HLCTimestamp txId, CoordinatorDecisionStatus? wantStatus, int timeoutMs = 10_000)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        long deadline = Environment.TickCount64 + timeoutMs;
        while (true)
        {
            if (Store(node).TryGet(txId, out CoordinatorDecisionRecord rec) &&
                (wantStatus is null || rec.Status == wantStatus))
                return rec;

            if (Environment.TickCount64 >= deadline)
                return null;
            await Task.Delay(50, ct);
        }
    }

    private static async Task<(KeyValueResponseType, TransactionHandle)> StartDurable(EmbeddedKahunaNode node, string coordinatorKey)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        return await node.Kahuna.LocateAndStartTransaction(
            new KeyValueTransactionOptions
            {
                CoordinatorKey     = coordinatorKey,
                Locking            = KeyValueTransactionLocking.Pessimistic,
                DecisionDurability = DecisionDurability.Durable,
                AsyncRelease       = false,
                Timeout            = 10_000,
            },
            ct);
    }

    private static async Task WriteInTxn(EmbeddedKahunaNode node, TransactionHandle handle, string key, byte[] value, KeyValueDurability durability)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (KeyValueResponseType type, _, _) = await RetrySet(() =>
            node.Kahuna.LocateAndTrySetKeyValue(handle.TransactionId, key, value, null, -1,
                KeyValueFlags.None, 0, durability, ct,
                coordinatorKey: handle.CoordinatorKey, operationId: TransactionOperationId.NewRandom()));
        Assert.Equal(KeyValueResponseType.Set, type);
    }

    private static async Task AssertValueCommitted(EmbeddedKahunaNode node, string key, byte[] expected)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (KeyValueResponseType r, ReadOnlyKeyValueEntry? e) = await node.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Get, r);
        Assert.Equal(expected, e!.Value);
    }

    // ── A durable multi-key commit installs a Completed record anchored on the data partition ──────────────

    [Fact]
    public async Task DurableCommit_MultiKey_InstallsCompletedDecisionAndCommitsValues()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = new(EmbeddedOptions());
        await node.StartAsync(ct);

        const string anchorKey = "durable:aaa";
        const string key2      = "durable:bbb";
        const string key3      = "durable:ccc";
        byte[] v1 = "one"u8.ToArray();
        byte[] v2 = "two"u8.ToArray();
        byte[] v3 = "three"u8.ToArray();

        (KeyValueResponseType startType, TransactionHandle handle) = await StartDurable(node, "durable-coord-1");
        Assert.Equal(KeyValueResponseType.Set, startType);
        HLCTimestamp txId = handle.TransactionId;

        // The anchor is the first confirmed persistent modified key: write it first.
        await WriteInTxn(node, handle, anchorKey, v1, KeyValueDurability.Persistent);
        await WriteInTxn(node, handle, key2, v2, KeyValueDurability.Persistent);
        await WriteInTxn(node, handle, key3, v3, KeyValueDurability.Persistent);

        (KeyValueResponseType commitResult, string? committedAnchor) =
            await node.Kahuna.LocateAndCommitTransaction(handle, ct);
        Assert.Equal(KeyValueResponseType.Committed, commitResult);
        Assert.Equal(anchorKey, committedAnchor);

        await AssertValueCommitted(node, anchorKey, v1);
        await AssertValueCommitted(node, key2, v2);
        await AssertValueCommitted(node, key3, v3);

        // The decision record reached Completed with every participant acknowledged, anchored on the first
        // modified key.
        CoordinatorDecisionRecord? record = await WaitForDecision(node, txId, CoordinatorDecisionStatus.Completed);
        Assert.NotNull(record);
        Assert.Equal(anchorKey, record!.RecordAnchorKey);
        Assert.Equal(3, record.Participants.Count);
        Assert.True(record.AllParticipantsAcked);
        Assert.Contains(record.Participants, p => p.Key == anchorKey && p.Acked);
        Assert.Contains(record.Participants, p => p.Key == key2 && p.Acked);
        Assert.Contains(record.Participants, p => p.Key == key3 && p.Acked);
    }

    // ── A single-key durable commit records only the anchor participant, already acknowledged ──────────────

    [Fact]
    public async Task DurableCommit_SingleKey_RecordHasAnchorParticipantAcked()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = new(EmbeddedOptions());
        await node.StartAsync(ct);

        const string anchorKey = "durable-single:key";
        byte[] value = "solo"u8.ToArray();

        (KeyValueResponseType startType, TransactionHandle handle) = await StartDurable(node, "durable-coord-single");
        Assert.Equal(KeyValueResponseType.Set, startType);
        HLCTimestamp txId = handle.TransactionId;

        await WriteInTxn(node, handle, anchorKey, value, KeyValueDurability.Persistent);

        (KeyValueResponseType commitResult, string? committedAnchor) =
            await node.Kahuna.LocateAndCommitTransaction(handle, ct);
        Assert.Equal(KeyValueResponseType.Committed, commitResult);
        Assert.Equal(anchorKey, committedAnchor);

        await AssertValueCommitted(node, anchorKey, value);

        CoordinatorDecisionRecord? record = await WaitForDecision(node, txId, CoordinatorDecisionStatus.Completed);
        Assert.NotNull(record);
        Assert.Equal(anchorKey, record!.RecordAnchorKey);
        Assert.Single(record.Participants);
        Assert.Equal(anchorKey, record.Participants[0].Key);
        Assert.True(record.Participants[0].Acked);
    }

    // ── A durable transaction may not modify an ephemeral key ──────────────────────────────────────────────

    [Fact]
    public async Task DurableCommit_RejectsEphemeralModifiedKey()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = new(EmbeddedOptions());
        await node.StartAsync(ct);

        const string persistentKey = "durable-eph:persistent";
        const string ephemeralKey  = "durable-eph:ephemeral";
        byte[] value = "x"u8.ToArray();

        (KeyValueResponseType startType, TransactionHandle handle) = await StartDurable(node, "durable-coord-eph");
        Assert.Equal(KeyValueResponseType.Set, startType);
        HLCTimestamp txId = handle.TransactionId;

        await WriteInTxn(node, handle, persistentKey, value, KeyValueDurability.Persistent);
        await WriteInTxn(node, handle, ephemeralKey, value, KeyValueDurability.Ephemeral);

        (KeyValueResponseType commitResult, _) = await node.Kahuna.LocateAndCommitTransaction(handle, ct);
        Assert.Equal(KeyValueResponseType.Aborted, commitResult);

        // No decision record was written for a rejected durable transaction.
        CoordinatorDecisionRecord? record = await WaitForDecision(node, txId, wantStatus: null, timeoutMs: 1_000);
        Assert.Null(record);
    }

    // ── A new durable transaction is rejected under decision-store capacity pressure ───────────────────────

    [Fact]
    public async Task DurableCommit_UnderCapacity_RejectsNewDurableTransaction()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        // Cap the decision store at one record and pre-fill it, so a new durable transaction is rejected
        // rather than cap-evicting the outstanding record.
        await using EmbeddedKahunaNode node = new(EmbeddedOptions(outcomeRetentionMax: 1));
        await node.StartAsync(ct);

        HLCTimestamp seedTx = new(1, 100, 1);
        CoordinatorDecisionRecord seed = new(
            seedTx, "seed-coord", "durable-cap:seed", seedTx, CoordinatorDecisionStatus.CommitDecided,
            [new CoordinatorParticipant("durable-cap:seed", KeyValueDurability.Persistent, HLCTimestamp.Zero, true, false)],
            [], seedTx, HLCTimestamp.Zero);
        await node.Kahuna.ImportCoordinatorDecisions([seed]);

        const string anchorKey = "durable-cap:key";
        byte[] value = "y"u8.ToArray();

        (KeyValueResponseType startType, TransactionHandle handle) = await StartDurable(node, "durable-coord-cap");
        Assert.Equal(KeyValueResponseType.Set, startType);

        await WriteInTxn(node, handle, anchorKey, value, KeyValueDurability.Persistent);

        (KeyValueResponseType commitResult, _) = await node.Kahuna.LocateAndCommitTransaction(handle, ct);
        Assert.Equal(KeyValueResponseType.Aborted, commitResult);
    }

    // ── The initial record replicates to every node from the one committed anchor proposal ─────────────────

    private sealed record ClusterNode(RaftManager Raft, KahunaManager Kahuna);

    private (RaftManager, KahunaManager) BuildClusterNode(
        int nodeId, int port, string[] peers, MemoryInterNodeCommmunication interNode, InMemoryCommunication comm)
    {
        ILogger<IRaft>   raftLogger   = NullLogger<IRaft>.Instance;
        ILogger<IKahuna> kahunaLogger = NullLogger<IKahuna>.Instance;

        ActorSystem actorSystem = new(logger: raftLogger);

        RaftConfiguration raftCfg = new()
        {
            NodeName             = "durabledecision" + nodeId,
            NodeId               = nodeId,
            Host                 = "localhost",
            Port                 = port,
            InitialPartitions    = 2,
            StartElectionTimeout = 50,
            EndElectionTimeout   = 150,
            ElectionTimeoutSeed  = 71000 + nodeId,
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
    public async Task DurableCommit_MultiNode_ReplicatesInstalledDecisionToEveryNode()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        MemoryInterNodeCommmunication interNode = new();
        InMemoryCommunication comm = new();

        string[] p1 = ["localhost:9431", "localhost:9432"];
        string[] p2 = ["localhost:9430", "localhost:9432"];
        string[] p3 = ["localhost:9430", "localhost:9431"];

        (RaftManager r1, KahunaManager k1) = BuildClusterNode(1, 9430, p1, interNode, comm);
        (RaftManager r2, KahunaManager k2) = BuildClusterNode(2, 9431, p2, interNode, comm);
        (RaftManager r3, KahunaManager k3) = BuildClusterNode(3, 9432, p3, interNode, comm);

        interNode.SetNodes(new() { { "localhost:9430", k1 }, { "localhost:9431", k2 }, { "localhost:9432", k3 } });
        comm.SetNodes(new() { { "localhost:9430", r1 }, { "localhost:9431", r2 }, { "localhost:9432", r3 } });

        ClusterNode[] nodes = [new(r1, k1), new(r2, k2), new(r3, k3)];
        try
        {
            await Task.WhenAll(r1.JoinCluster(), r2.JoinCluster(), r3.JoinCluster());

            for (int partition = 0; partition <= 1; partition++)
                while (!await r1.AmILeader(partition, ct) && !await r2.AmILeader(partition, ct) && !await r3.AmILeader(partition, ct))
                    await Task.Delay(50, ct);

            // Drive from partition 1's leader so the anchor commit applies locally and replicates to the peers.
            ClusterNode coord = nodes[0];
            foreach (ClusterNode n in nodes)
                if (await n.Raft.AmILeader(1, ct)) { coord = n; break; }

            const string anchorKey = "durable-repl:aaa";
            const string key2      = "durable-repl:bbb";
            byte[] v1 = "alpha"u8.ToArray();
            byte[] v2 = "beta"u8.ToArray();

            (KeyValueResponseType startType, TransactionHandle handle) = await coord.Kahuna.LocateAndStartTransaction(
                new KeyValueTransactionOptions
                {
                    CoordinatorKey     = "durable-repl-coord",
                    Locking            = KeyValueTransactionLocking.Pessimistic,
                    DecisionDurability = DecisionDurability.Durable,
                    AsyncRelease       = false,
                    Timeout            = 10_000,
                }, ct);
            Assert.Equal(KeyValueResponseType.Set, startType);
            HLCTimestamp txId = handle.TransactionId;

            foreach ((string key, byte[] value) in new[] { (anchorKey, v1), (key2, v2) })
            {
                (KeyValueResponseType s, _, _) = await RetrySet(() =>
                    coord.Kahuna.LocateAndTrySetKeyValue(txId, key, value, null, -1, KeyValueFlags.None, 0,
                        KeyValueDurability.Persistent, ct, coordinatorKey: handle.CoordinatorKey,
                        operationId: TransactionOperationId.NewRandom()));
                Assert.Equal(KeyValueResponseType.Set, s);
            }

            (KeyValueResponseType commitResult, string? committedAnchor) =
                await coord.Kahuna.LocateAndCommitTransaction(handle, ct);
            Assert.Equal(KeyValueResponseType.Committed, commitResult);
            Assert.Equal(anchorKey, committedAnchor);

            // The initial CommitDecided record — the frozen participant set with the anchor already acknowledged
            // — rides the one committed anchor proposal and replicates to every node.
            long deadline = Environment.TickCount64 + 10_000;
            while (Environment.TickCount64 < deadline &&
                   nodes.Any(n => !n.Kahuna.CoordinatorDecisionStore.TryGet(txId, out _)))
                await Task.Delay(50, ct);

            foreach (ClusterNode n in nodes)
            {
                Assert.True(n.Kahuna.CoordinatorDecisionStore.TryGet(txId, out CoordinatorDecisionRecord rec),
                    "a node is missing the replicated decision record");
                Assert.Equal(anchorKey, rec.RecordAnchorKey);
                Assert.Equal(2, rec.Participants.Count);
                Assert.Contains(rec.Participants, p => p.Key == anchorKey && p.Acked);
                Assert.Contains(rec.Participants, p => p.Key == key2);
            }
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
