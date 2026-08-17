using Kahuna.Server.Communication.Internode;
using Kahuna.Server.Configuration;
using Kahuna.Shared.Locks;
using Kommander;
using Kommander.Communication;
using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.Gossip;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Nixie;

namespace Kahuna.Server.Tests;

/// <summary>
/// A follower can hold a quorum-committed entry as <c>Proposed</c> — it acked the append but never
/// received the leader's commit broadcast. Promotion applies only what the node already knows is
/// committed, so such an inherited entry reaches the lock state only when the new leader commits its
/// own first entry. Until then the promoted leader must not answer an acquire from its (incomplete)
/// state, or it grants a lock somebody else already holds.
/// </summary>
public sealed class TestLockPromotionInheritedApply : RaftTrackingTest
{
    private readonly ILogger<IRaft> raftLogger = NullLogger<IRaft>.Instance;
    private readonly ILogger<IKahuna> kahunaLogger = NullLogger<IKahuna>.Instance;

    private sealed record Node(RaftManager Raft, KahunaManager Kahuna);

    /// <summary>
    /// Wraps the in-memory transport and, while withholding is armed, drops every AppendLogs batch
    /// carrying committed entries on the data partitions. Proposals and their acks still flow, so a
    /// write reaches quorum durability and the client sees success, while both followers keep the
    /// entry as <c>Proposed</c> — exactly the state a slow machine produces when the leader steps
    /// down before its commit broadcast lands.
    /// </summary>
    private sealed class CommitWithholdingCommunication : ICommunication
    {
        private readonly InMemoryCommunication inner = new();

        public volatile bool WithholdCommits;

        public void SetNodes(Dictionary<string, IRaft> nodes) => inner.SetNodes(nodes);

        public Task<AppendLogsResponse> AppendLogs(RaftManager manager, RaftNode node, AppendLogsRequest request)
        {
            if (WithholdCommits && request.Partition > 0 &&
                request.Logs is not null && request.Logs.Exists(log => log.Type == RaftLogType.Committed))
                return Task.FromResult(new AppendLogsResponse());

            return inner.AppendLogs(manager, node, request);
        }

        public Task<HandshakeResponse> Handshake(RaftManager m, RaftNode n, HandshakeRequest r) => inner.Handshake(m, n, r);
        public Task<RequestVotesResponse> RequestVotes(RaftManager m, RaftNode n, RequestVotesRequest r) => inner.RequestVotes(m, n, r);
        public Task<VoteResponse> Vote(RaftManager m, RaftNode n, VoteRequest r) => inner.Vote(m, n, r);
        public Task<CompleteAppendLogsResponse> CompleteAppendLogs(RaftManager m, RaftNode n, CompleteAppendLogsRequest r) => inner.CompleteAppendLogs(m, n, r);
        public Task<BatchRequestsResponse> BatchRequests(RaftManager m, RaftNode n, BatchRequestsRequest r) => inner.BatchRequests(m, n, r);
        public Task<JoinResponse> SendJoin(RaftManager m, RaftNode n, JoinRequest r) => inner.SendJoin(m, n, r);
        public Task<LeaveResponse> SendLeave(RaftManager m, RaftNode n, LeaveRequest r, CancellationToken ct = default) => inner.SendLeave(m, n, r, ct);
        public Task<GossipAck> SendGossip(RaftManager m, RaftNode n, GossipMessage d, CancellationToken ct = default) => inner.SendGossip(m, n, d, ct);
        public Task<SetMemberRoleResponse> SendSetMemberRole(RaftManager m, RaftNode n, SetMemberRoleRequest r, CancellationToken ct = default) => inner.SendSetMemberRole(m, n, r, ct);
        public Task<Kommander.Gossip.PingResponse> SendPing(RaftManager m, RaftNode n, Kommander.Gossip.PingRequest r, CancellationToken ct = default) => inner.SendPing(m, n, r, ct);
        public Task<Kommander.Gossip.PingReqResponse> SendPingReq(RaftManager m, RaftNode n, Kommander.Gossip.PingReqRequest r, CancellationToken ct = default) => inner.SendPingReq(m, n, r, ct);
        public Task<long?> GetRemoteFollowerLag(RaftManager m, RaftNode n, int partitionId, string follower) => inner.GetRemoteFollowerLag(m, n, partitionId, follower);
        public Task<SnapshotResponse> SendInstallSnapshot(RaftManager m, RaftNode n, SnapshotRequest r, CancellationToken ct = default) => inner.SendInstallSnapshot(m, n, r, ct);
    }

    private (RaftManager, KahunaManager) BuildNode(
        int nodeId, int port, string[] peers, MemoryInterNodeCommmunication interNode, ICommunication comm)
    {
        ActorSystem actorSystem = new(logger: raftLogger);

        RaftConfiguration raftCfg = new()
        {
            NodeName = "lockpromotion" + nodeId,
            NodeId = nodeId,
            Host = "localhost",
            Port = port,
            InitialPartitions = 2,
            HeartbeatInterval = TimeSpan.FromMilliseconds(10),
            CheckLeaderInterval = TimeSpan.FromMilliseconds(25),
            StartElectionTimeout = 50,
            EndElectionTimeout = 150,
            ElectionTimeoutSeed = 98100 + nodeId,
            CompactEveryOperations = 1000,
            CompactNumberEntries = 50,
            EnableQuiescence = false,
            PartitionExecutorPoolSize = 1
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

        KahunaManager kahuna = new(actorSystem, Track(raft), kahunaConfig, interNode, kahunaLogger);
        raft.OnLogRestored += kahuna.OnLogRestored;
        raft.OnReplicationReceived += kahuna.OnReplicationReceived;
        raft.OnReplicationError += kahuna.OnReplicationError;
        raft.OnLeaderChanged += kahuna.OnLeaderChanged;

        TestClusterNodeRegistry.Register(raft, kahuna, actorSystem);

        return (raft, kahuna);
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

    [Fact]
    public async Task PromotedLeader_InheritedUnappliedLock_IsNeverGrantedToSecondOwner()
    {
        MemoryInterNodeCommmunication interNode = new();
        CommitWithholdingCommunication comm = new();

        (RaftManager r1, KahunaManager k1) = BuildNode(1, 9420, ["localhost:9421", "localhost:9422"], interNode, comm);
        (RaftManager r2, KahunaManager k2) = BuildNode(2, 9421, ["localhost:9420", "localhost:9422"], interNode, comm);
        (RaftManager r3, KahunaManager k3) = BuildNode(3, 9422, ["localhost:9420", "localhost:9421"], interNode, comm);

        interNode.SetNodes(new() { { "localhost:9420", k1 }, { "localhost:9421", k2 }, { "localhost:9422", k3 } });
        comm.SetNodes(new() { { "localhost:9420", r1 }, { "localhost:9421", r2 }, { "localhost:9422", r3 } });

        Node[] nodes = [new(r1, k1), new(r2, k2), new(r3, k3)];

        CancellationToken ct = TestContext.Current.CancellationToken;

        try
        {
            await Task.WhenAll(r1.JoinCluster(ct), r2.JoinCluster(ct), r3.JoinCluster(ct));

            for (int partition = 0; partition <= 2; partition++)
                await WaitForAnyLeader(partition, r1, r2, r3);

            byte[] holder = "holder-a"u8.ToArray();
            byte[] usurper = "holder-b"u8.ToArray();

            // The acquire commits by quorum durability while both followers stay one commit behind.
            comm.WithholdCommits = true;

            (LockResponseType lockType, long fencingToken) = await nodes[0].Kahuna.LocateAndTryLock(
                "inherited-lock", holder, 60_000, LockDurability.Persistent, ct);

            Assert.Equal(LockResponseType.Locked, lockType);

            for (int p = 1; p <= 2; p++)
            {
                Node oldLeader = await LeaderOf(p, nodes);
                await oldLeader.Raft.StepDownAsync(p, ct);
            }

            comm.WithholdCommits = false;

            for (int p = 1; p <= 2; p++)
                await WaitForAnyLeader(p, r1, r2, r3);

            long deadline = Environment.TickCount64 + 10_000;
            while (true)
            {
                (LockResponseType usurperType, _) = await nodes[0].Kahuna.LocateAndTryLock(
                    "inherited-lock", usurper, 60_000, LockDurability.Persistent, ct);

                Assert.NotEqual(LockResponseType.Locked, usurperType);

                if (usurperType == LockResponseType.Busy)
                    break;

                if (Environment.TickCount64 >= deadline)
                    Assert.Fail($"no terminal Busy answer within deadline (last: {usurperType})");

                await Task.Delay(20, ct);
            }

            (LockResponseType getType, Server.Locks.Data.ReadOnlyLockEntry? entry) =
                await nodes[0].Kahuna.LocateAndGetLock("inherited-lock", LockDurability.Persistent, ct);

            Assert.Equal(LockResponseType.Got, getType);
            Assert.NotNull(entry);
            Assert.Equal(holder, entry!.Owner);
            Assert.Equal(fencingToken, entry.FencingToken);
        }
        finally
        {
            foreach (Node node in nodes)
            {
                try { await TestClusterNodeRegistry.DisposeAsync(node.Raft, ct); }
                catch (ObjectDisposedException) { }
            }
        }
    }
}
