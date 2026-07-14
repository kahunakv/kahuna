
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
/// A Durable commit must not decide the anchor's fate from the coordinator's local view of the commit RPC alone.
/// If the anchor proposal actually committed and installed its decision but the coordinator only saw a lost or
/// leadership-changed response, treating that as failure would tell the client <c>Aborted</c> and roll the
/// participants back while recovery separately completes the durably-committed anchor — a torn outcome. So an
/// uncertain anchor commit consults the anchored decision record before assuming failure: a present record means
/// the anchor is committed (defer to recovery, never abort). Only when no record exists does the anchor genuinely
/// fail, and that rollback must settle the prepared tickets cleanly from the <c>Committing</c> state rather than
/// throwing on a stale state CAS and orphaning them.
/// </summary>
[Collection("ClusterTests")]
public sealed class TestDurableAnchorUncertainty
{
    private sealed record ClusterNode(RaftManager Raft, KahunaManager Kahuna);

    private static (RaftManager, KahunaManager) BuildNode(
        int nodeId, int port, string[] peers, MemoryInterNodeCommmunication interNode, InMemoryCommunication comm)
    {
        ILogger<IRaft> raftLogger = NullLogger<IRaft>.Instance;
        ILogger<IKahuna> kahunaLogger = NullLogger<IKahuna>.Instance;

        ActorSystem actorSystem = new(logger: raftLogger);

        RaftConfiguration raftCfg = new()
        {
            NodeName             = "anchoruncertain" + nodeId,
            NodeId               = nodeId,
            Host                 = "localhost",
            Port                 = port,
            InitialPartitions    = 4,
            // Long election timeouts so forced leadership holds through the whole phase-two retry window.
            StartElectionTimeout = 2000,
            EndElectionTimeout   = 4000,
            ElectionTimeoutSeed  = 91000 + nodeId,
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

    // ── (A) A committed-but-lost anchor response defers to recovery — no Aborted, no participant rollback ──────

    [Fact]
    public async Task UncertainAnchorCommit_WithRecordPresent_DefersToRecovery_NeverAborts()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await RunCluster(async (nodes, interNode, coord, anchorOwner, coordKey, anchorKey, secondaryKey) =>
        {
            byte[] anchorValue = "anchor-v"u8.ToArray();
            byte[] secondaryValue = "secondary-v"u8.ToArray();
            HLCTimestamp txId = await StartAndStage(coord, coordKey, anchorKey, anchorValue, secondaryKey, secondaryValue, ct);

            // The anchor commits and installs its decision on the remote leader, but its success is hidden from
            // the coordinator as MustRetry — the committed-but-lost response.
            interNode.CommitMutationsResponseFault = (_, key) => key == anchorKey;

            (KeyValueResponseType commitResult, _) = await coord.Kahuna.LocateAndCommitTransaction(
                HandleFor(txId, coordKey), ct);

            // The coordinator could not confirm the anchor, but the record proves it committed: MustRetry, never
            // Aborted, and the still-prepared secondary is left intact for recovery.
            Assert.Equal(KeyValueResponseType.MustRetry, commitResult);
            Assert.True(anchorOwner.Kahuna.CoordinatorDecisionStore.TryGet(txId, out _));
            Assert.Equal(anchorValue, await ReadValue(coord, anchorKey, ct));
            Assert.Null(await ReadValue(coord, secondaryKey, ct));

            // Recovery on the anchor leader drives the surviving prepare and completes the durably-committed tx.
            interNode.CommitMutationsResponseFault = null;
            await WaitUntil(async () =>
            {
                await anchorOwner.Kahuna.RecoverOutstandingDecisions(TimeSpan.FromHours(1), ct);
                return anchorOwner.Kahuna.CoordinatorDecisionStore.TryGet(txId, out CoordinatorDecisionRecord r) &&
                       r.Status == CoordinatorDecisionStatus.Completed;
            }, ct);

            Assert.Equal(secondaryValue, await ReadValue(coord, secondaryKey, ct));
        }, ct);
    }

    // ── (B) A genuinely-failed anchor rolls the prepared secondary back cleanly, without throwing ──────────────

    [Fact]
    public async Task FailedAnchorCommit_RollsBackPreparedSecondary_Cleanly()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await RunCluster(async (nodes, interNode, coord, anchorOwner, coordKey, anchorKey, secondaryKey) =>
        {
            HLCTimestamp txId = await StartAndStage(coord, coordKey, anchorKey, "anchor-v"u8.ToArray(),
                secondaryKey, "secondary-v"u8.ToArray(), ct);

            // The anchor commit never reaches the remote leader: it never commits and installs no record.
            interNode.CommitMutationsFault = (_, key) => key == anchorKey;

            // Observe which prepared tickets are actually driven to rollback over the transport.
            System.Collections.Concurrent.ConcurrentBag<string> rolledBack = [];
            interNode.RollbackObserver = (_, key) => rolledBack.Add(key);

            (KeyValueResponseType commitResult, _) = await coord.Kahuna.LocateAndCommitTransaction(
                HandleFor(txId, coordKey), ct);

            // No record exists, so the anchor genuinely failed: the transaction aborts and nothing committed.
            Assert.Equal(KeyValueResponseType.Aborted, commitResult);
            Assert.False(anchorOwner.Kahuna.CoordinatorDecisionStore.TryGet(txId, out _));
            Assert.Null(await ReadValue(coord, anchorKey, ct));
            Assert.Null(await ReadValue(coord, secondaryKey, ct));

            // The prepared secondary's ticket was settled via a real rollback — not left orphaned by a rollback
            // that threw on a stale state CAS before ever issuing the participant's RPC.
            Assert.Contains(secondaryKey, rolledBack);
            interNode.RollbackObserver = null;

            // The prepared secondary's ticket was rolled back cleanly (not orphaned by a state-CAS throw). Prove
            // it with an OPTIMISTIC writer, which holds no exclusive lock to clobber a leftover intent with: it
            // stages its own write intent directly, so a lingering intent from the failed transaction would make
            // this write conflict and never commit. A clean rollback lets it write and commit the same key.
            interNode.CommitMutationsFault = null;
            byte[] rewrite = "rewrite-v"u8.ToArray();
            HLCTimestamp txId2 = await StartTx(coord, coordKey, KeyValueTransactionLocking.Optimistic, DecisionDurability.BestEffort, ct);
            await SetInTxn(coord, coordKey, txId2, secondaryKey, rewrite, ct);
            (KeyValueResponseType commit2, _) = await coord.Kahuna.LocateAndCommitTransaction(
                HandleFor(txId2, coordKey), ct);
            Assert.Equal(KeyValueResponseType.Committed, commit2);
            Assert.Equal(rewrite, await ReadValue(coord, secondaryKey, ct));
        }, ct);
    }

    // ── harness ───────────────────────────────────────────────────────────────────────────────────────────────

    private delegate Task ClusterBody(
        ClusterNode[] nodes, MemoryInterNodeCommmunication interNode, ClusterNode coord, ClusterNode anchorOwner,
        string coordKey, string anchorKey, string secondaryKey);

    private static async Task RunCluster(ClusterBody body, CancellationToken ct)
    {
        MemoryInterNodeCommmunication interNode = new();
        InMemoryCommunication comm = new();

        string[] p1 = ["localhost:9461", "localhost:9462"];
        string[] p2 = ["localhost:9460", "localhost:9462"];
        string[] p3 = ["localhost:9460", "localhost:9461"];

        (RaftManager r1, KahunaManager k1) = BuildNode(1, 9460, p1, interNode, comm);
        (RaftManager r2, KahunaManager k2) = BuildNode(2, 9461, p2, interNode, comm);
        (RaftManager r3, KahunaManager k3) = BuildNode(3, 9462, p3, interNode, comm);

        interNode.SetNodes(new() { { "localhost:9460", k1 }, { "localhost:9461", k2 }, { "localhost:9462", k3 } });
        comm.SetNodes(new() { { "localhost:9460", r1 }, { "localhost:9461", r2 }, { "localhost:9462", r3 } });

        ClusterNode[] nodes = [new(r1, k1), new(r2, k2), new(r3, k3)];
        try
        {
            await Task.WhenAll(r1.JoinCluster(), r2.JoinCluster(), r3.JoinCluster());
            for (int partition = 0; partition <= 4; partition++)
                while (!await r1.AmILeader(partition, ct) && !await r2.AmILeader(partition, ct) && !await r3.AmILeader(partition, ct))
                    await Task.Delay(50, ct);

            // The coordinator key routes the session to node 1; the anchor and secondary keys share a data
            // partition placed on node 2, so both the anchor commit and its rollback are inter-node calls the
            // fault seams can intercept while the coordinator drives from node 1.
            const string anchorKey = "anchoruncertain:anchor:aaa";
            int anchorPartition = k1.GetDataPartitionForKey(anchorKey);
            string secondaryKey = FindKeyInPartition(k1, anchorPartition, anchorKey);
            string coordKey = FindKeyInDifferentPartition(k1, anchorPartition);
            int coordPartition = k1.GetDataPartitionForKey(coordKey);

            ClusterNode coord = nodes[0];
            ClusterNode anchorOwner = nodes[1];
            await ForceLeader(coord, coordPartition, ct);
            await ForceLeader(anchorOwner, anchorPartition, ct);
            Assert.NotEqual(coord.Raft.GetLocalNodeId(), anchorOwner.Raft.GetLocalNodeId());

            await body(nodes, interNode, coord, anchorOwner, coordKey, anchorKey, secondaryKey);
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

    private static async Task<HLCTimestamp> StartAndStage(
        ClusterNode coord, string coordKey, string anchorKey, byte[] anchorValue,
        string secondaryKey, byte[] secondaryValue, CancellationToken ct)
    {
        HLCTimestamp txId = await StartDurable(coord, coordKey, ct);
        await SetInTxn(coord, coordKey, txId, anchorKey, anchorValue, ct);
        await SetInTxn(coord, coordKey, txId, secondaryKey, secondaryValue, ct);
        return txId;
    }

    private static async Task<HLCTimestamp> StartDurable(ClusterNode coord, string coordKey, CancellationToken ct) =>
        await StartTx(coord, coordKey, KeyValueTransactionLocking.Pessimistic, DecisionDurability.Durable, ct);

    private static async Task<HLCTimestamp> StartTx(
        ClusterNode coord, string coordKey, KeyValueTransactionLocking locking, DecisionDurability durability, CancellationToken ct)
    {
        (KeyValueResponseType startType, TransactionHandle handle) = await coord.Kahuna.LocateAndStartTransaction(
            new KeyValueTransactionOptions
            {
                CoordinatorKey     = coordKey,
                Locking            = locking,
                DecisionDurability = durability,
                AsyncRelease       = false,
                Timeout            = 10_000,
            }, ct);
        Assert.Equal(KeyValueResponseType.Set, startType);
        return handle.TransactionId;
    }

    private static TransactionHandle HandleFor(HLCTimestamp txId, string coordKey) => new(txId, coordKey);

    private static string FindKeyInDifferentPartition(KahunaManager router, int avoidPartition)
    {
        for (int i = 0; i < 8192; i++)
        {
            string candidate = "anchoruncertain:coord:c" + i;
            if (router.GetDataPartitionForKey(candidate) != avoidPartition)
                return candidate;
        }
        throw new InvalidOperationException("Could not find a key in a different data partition.");
    }

    private static string FindKeyInPartition(KahunaManager router, int targetPartition, string avoidKey)
    {
        for (int i = 0; i < 8192; i++)
        {
            string candidate = "anchoruncertain:secondary:s" + i;
            if (candidate != avoidKey && router.GetDataPartitionForKey(candidate) == targetPartition)
                return candidate;
        }
        throw new InvalidOperationException("Could not find a key in the target data partition.");
    }

    private static async Task ForceLeader(ClusterNode node, int partition, CancellationToken ct)
    {
        await node.Raft.ForceLeaderForTestingAsync(partition, ct);

        long deadline = Environment.TickCount64 + 10_000;
        while (Environment.TickCount64 < deadline)
        {
            if (await node.Raft.AmILeader(partition, ct))
                return;
            await Task.Delay(25, ct);
        }
        throw new TimeoutException($"Node {node.Raft.GetLocalNodeId()} did not become leader of partition {partition}.");
    }

    private static async Task SetInTxn(ClusterNode node, string coordKey, HLCTimestamp txId, string key, byte[] value, CancellationToken ct)
    {
        long deadline = Environment.TickCount64 + 10_000;
        while (true)
        {
            (KeyValueResponseType type, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
                txId, key, value, null, -1, KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct,
                coordinatorKey: coordKey, operationId: TransactionOperationId.NewRandom());
            if (type == KeyValueResponseType.Set)
                return;
            if (type != KeyValueResponseType.MustRetry || Environment.TickCount64 >= deadline)
                Assert.Fail($"Set {key} returned {type}");
            await Task.Delay(50, ct);
        }
    }

    private static async Task<byte[]?> ReadValue(ClusterNode node, string key, CancellationToken ct)
    {
        (KeyValueResponseType r, ReadOnlyKeyValueEntry? e) = await node.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        return r == KeyValueResponseType.Get ? e!.Value : null;
    }

    private static async Task WaitUntil(Func<Task<bool>> predicate, CancellationToken ct, int timeoutMs = 20_000)
    {
        long deadline = Environment.TickCount64 + timeoutMs;
        while (Environment.TickCount64 < deadline)
        {
            if (await predicate())
                return;
            await Task.Delay(100, ct);
        }
        throw new TimeoutException("Timed out waiting for condition.");
    }
}
