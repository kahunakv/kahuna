
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
/// A durable commit that installs its anchor decision but leaves a secondary participant pending must not release
/// that participant's still-live prepare during finalize: the prepare is exactly what the per-partition-leader
/// recovery needs to drive the commit to completion. Before the fix, finalize's unconditional working-set release
/// cleared the prepared secondary's write intent, so recovery could only ever return <c>MustRetry</c> and the
/// secondary's write was lost even though the transaction was durably commit-decided. This exercises the real
/// path: a transient inter-node commit failure on the secondary forces the partial commit, then — with the fault
/// cleared — recovery finds the prepare intact, commits the secondary, and the decision reaches <c>Completed</c>.
/// </summary>
[Collection("ClusterTests")]
public sealed class TestDurablePartialCommitRecovery
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
            NodeName             = "partialcommit" + nodeId,
            NodeId               = nodeId,
            Host                 = "localhost",
            Port                 = port,
            InitialPartitions    = 4,
            StartElectionTimeout = 50,
            EndElectionTimeout   = 150,
            ElectionTimeoutSeed  = 76000 + nodeId,
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
    public async Task PartialDurableCommit_PreservesSecondaryPrepare_UntilRecoveryCommits()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        MemoryInterNodeCommmunication interNode = new();
        InMemoryCommunication comm = new();

        string[] p1 = ["localhost:9451", "localhost:9452"];
        string[] p2 = ["localhost:9450", "localhost:9452"];
        string[] p3 = ["localhost:9450", "localhost:9451"];

        (RaftManager r1, KahunaManager k1) = BuildNode(1, 9450, p1, interNode, comm);
        (RaftManager r2, KahunaManager k2) = BuildNode(2, 9451, p2, interNode, comm);
        (RaftManager r3, KahunaManager k3) = BuildNode(3, 9452, p3, interNode, comm);

        interNode.SetNodes(new() { { "localhost:9450", k1 }, { "localhost:9451", k2 }, { "localhost:9452", k3 } });
        comm.SetNodes(new() { { "localhost:9450", r1 }, { "localhost:9451", r2 }, { "localhost:9452", r3 } });

        ClusterNode[] nodes = [new(r1, k1), new(r2, k2), new(r3, k3)];
        try
        {
            await Task.WhenAll(r1.JoinCluster(), r2.JoinCluster(), r3.JoinCluster());
            for (int partition = 0; partition <= 4; partition++)
                while (!await r1.AmILeader(partition, ct) && !await r2.AmILeader(partition, ct) && !await r3.AmILeader(partition, ct))
                    await Task.Delay(50, ct);

            // The anchor is the first written key; drive the whole transaction from the anchor partition's leader
            // so the coordinator session is colocated with the anchor (its progress replicates). The secondary is
            // a key in a DIFFERENT data partition; we move that partition's leadership onto another node so its
            // commit is an inter-node call the fault can intercept while its prepare stays intact on the remote
            // leader — the partial durable commit. In-memory elections tend to hand one node every partition, so
            // relocate the secondary's leadership deterministically rather than hoping for a natural spread.
            const string anchorKey = "partial:anchor:aaa";
            int anchorPartition = k1.GetDataPartitionForKey(anchorKey);
            ClusterNode anchorLeaderNode = await LeaderOf(nodes, anchorPartition, ct);
            ClusterNode coord = anchorLeaderNode;

            string secondaryKey = await FindKeyLedByDifferentNode(nodes, k1, anchorLeaderNode, ct);
            int secondaryPartition = k1.GetDataPartitionForKey(secondaryKey);
            ClusterNode secondaryLeader = await LeaderOf(nodes, secondaryPartition, ct);
            Assert.NotEqual(anchorLeaderNode.Raft.GetLocalNodeId(), secondaryLeader.Raft.GetLocalNodeId());

            // Fault the secondary's inter-node commit while active; the anchor commit and everything else proceed.
            bool faultActive = true;
            interNode.CommitMutationsFault = (_, key) => faultActive && key == secondaryKey;

            (KeyValueResponseType startType, TransactionHandle handle) = await coord.Kahuna.LocateAndStartTransaction(
                new KeyValueTransactionOptions
                {
                    CoordinatorKey     = anchorKey,
                    Locking            = KeyValueTransactionLocking.Pessimistic,
                    DecisionDurability = DecisionDurability.Durable,
                    AsyncRelease       = false,
                    Timeout            = 10_000,
                }, ct);
            Assert.Equal(KeyValueResponseType.Set, startType);
            HLCTimestamp txId = handle.TransactionId;

            byte[] anchorValue = "anchor-v"u8.ToArray();
            byte[] secondaryValue = "secondary-v"u8.ToArray();

            await SetInTxn(coord, handle, anchorKey, anchorValue, ct);
            await SetInTxn(coord, handle, secondaryKey, secondaryValue, ct);

            // Commit: the anchor commits and installs the decision, but the faulted secondary stays pending, so the
            // durable commit returns MustRetry with the decision outstanding.
            (KeyValueResponseType commitResult, _) = await coord.Kahuna.LocateAndCommitTransaction(handle, ct);
            Assert.Equal(KeyValueResponseType.MustRetry, commitResult);

            // The anchor is committed; the secondary is not yet — but its prepare must have survived finalize.
            Assert.Equal(anchorValue, await ReadValue(coord, anchorKey, ct));
            Assert.Null(await ReadValue(coord, secondaryKey, ct));

            // Clear the fault and let recovery on the anchor's partition leader drive the surviving prepare.
            faultActive = false;

            await WaitUntil(async () =>
            {
                await anchorLeaderNode.Kahuna.RecoverOutstandingDecisions(TimeSpan.FromHours(1), ct);
                return anchorLeaderNode.Kahuna.CoordinatorDecisionStore.TryGet(txId, out CoordinatorDecisionRecord r) &&
                       r.Status == CoordinatorDecisionStatus.Completed;
            }, ct);

            // The secondary's write was preserved and committed by recovery — not lost to finalize cleanup.
            Assert.Equal(secondaryValue, await ReadValue(coord, secondaryKey, ct));

            Assert.True(anchorLeaderNode.Kahuna.CoordinatorDecisionStore.TryGet(txId, out CoordinatorDecisionRecord rec));
            Assert.Equal(CoordinatorDecisionStatus.Completed, rec.Status);
            Assert.True(rec.AllParticipantsAcked);
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

    // Per-partition election timeouts are seeded deterministically (seed ^ partitionId), so each partition has a
    // stable natural leader that always re-wins — no leadership transfer is needed to obtain a secondary whose
    // leader differs from the anchor's, and that difference holds through the whole phase-two retry window.
    private static async Task<string> FindKeyLedByDifferentNode(
        ClusterNode[] nodes, KahunaManager router, ClusterNode avoidLeader, CancellationToken ct)
    {
        int avoidPartition = router.GetDataPartitionForKey("partial:anchor:aaa");
        for (int i = 0; i < 8192; i++)
        {
            string candidate = "partial:secondary:b" + i;
            int partition = router.GetDataPartitionForKey(candidate);
            if (partition == avoidPartition)
                continue;

            ClusterNode leader = await LeaderOf(nodes, partition, ct);
            if (leader.Raft.GetLocalNodeId() != avoidLeader.Raft.GetLocalNodeId())
                return candidate;
        }
        throw new InvalidOperationException("Could not find a key whose data partition is led by a different node.");
    }

    private static async Task<ClusterNode> LeaderOf(ClusterNode[] nodes, int partition, CancellationToken ct)
    {
        long deadline = Environment.TickCount64 + 10_000;
        while (Environment.TickCount64 < deadline)
        {
            foreach (ClusterNode n in nodes)
                if (await n.Raft.AmILeader(partition, ct))
                    return n;
            await Task.Delay(50, ct);
        }
        throw new TimeoutException($"No leader for partition {partition}.");
    }

    private static async Task SetInTxn(ClusterNode node, TransactionHandle handle, string key, byte[] value, CancellationToken ct)
    {
        long deadline = Environment.TickCount64 + 10_000;
        while (true)
        {
            (KeyValueResponseType type, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
                handle.TransactionId, key, value, null, -1, KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct,
                coordinatorKey: handle.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
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
