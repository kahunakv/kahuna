using Kahuna.Server.Communication.Internode;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.Sequencer;
using Kahuna.Shared.Sequences;
using Kommander;
using Kommander.Communication.Memory;
using Kommander.System;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// Under per-partition replica placement, a node that does not host a sequence's partition forwards
/// the operation on a guess: the gossiped leader hint when it has one, else the first voter of the
/// committed replica set. Gossip runs in rounds of seconds, so a node can lack a hint long after it
/// boots, and the first voter is a follower two times in three. The receiver must then take the one
/// hop that is strictly better than the guess, to the leader it resolves from its own Raft state,
/// instead of answering <c>MustRetry</c> to a sender that would guess the same target again.
///
/// <para>Five nodes, two data partitions, replication factor three, no gossip: every non-hosting
/// node forwards on the committed replica set alone, deterministically.</para>
/// </summary>
public sealed class TestSequencerPlacementForwarding : BaseCluster
{
    private const int Nodes = 5;

    private const int Partitions = 2;

    private const int ReplicationFactor = 3;

    private readonly ILogger<IRaft> raftLogger;

    private readonly ILogger<IKahuna> kahunaLogger;

    public TestSequencerPlacementForwarding(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = LoggerFactory.Create(b => b.AddXUnit(outputHelper).SetMinimumLevel(LogLevel.Warning));
        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    private Task<(IRaft[] Rafts, IKahuna[] Kahunas, InMemoryCommunication RaftComm, MemoryInterNodeCommmunication InterComm)> AssembleAsync() =>
        AssembleClusterWithTransports(
            Nodes, "memory", Partitions, raftLogger, kahunaLogger,
            ReplicationFactor,
            enablePlacementRebalancer: false,
            // No gossip rounds, so no leader hint ever forms on a non-hosting node and every forward
            // from one is a guess on the committed replica set.
            configureRaft: c => c.GossipFanout = 0);

    /// <summary>
    /// A non-hosting sender whose guess lands on a follower allocates a value in exactly two hops:
    /// the guess, then the follower's redirect to the leader it resolves locally.
    /// </summary>
    [Fact]
    public async Task AllocatesFromANonHostingNodeWhoseGuessLandsOnAFollower()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, IKahuna[] kahunas, _, MemoryInterNodeCommmunication transport) = await AssembleAsync();

        try
        {
            string name = "placed/" + Guid.NewGuid().ToString("N");
            int partitionId = new DataPartitionRouter(rafts[0]).Locate(SequenceActor.GetStorageKey(name));

            int outside = NonHostingIndex(rafts, partitionId);

            Assert.Null(rafts[outside].GetPartitionLeaderHint(partitionId));

            int guess = IndexOf(rafts, FirstRemoteVoter(rafts[outside], partitionId));

            await EnsureFollower(rafts, partitionId, guess);

            int leader = await LeaderIndex(rafts, partitionId);
            Assert.NotEqual(guess, leader);

            Assert.Equal(SequenceResponseType.Success, await CreateWithRetry(kahunas[leader], name));

            int forwardsBefore = transport.SequenceForwardCallCount;

            (SequenceResponseType response, SequenceAllocation allocation) = await kahunas[outside].LocateAndNextSequenceValue(
                name, null, SequenceDurability.Persistent, ct);

            Assert.Equal(SequenceResponseType.Success, response);
            Assert.Equal(1, allocation.Count);
            Assert.Equal(2, transport.SequenceForwardCallCount - forwardsBefore);
        }
        finally
        {
            await TearDownAsync(rafts);
        }
    }

    /// <summary>
    /// A forwarded request that lands on a node which does not host the partition is refused: that
    /// node holds nothing but the same committed map the sender already read, so its target would be
    /// another guess, and chaining guesses is how two nodes with stale placement views bounce one
    /// operation between each other.
    /// </summary>
    [Fact]
    public async Task AChainedGuessOnANonHostingNodeIsRefused()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, IKahuna[] kahunas, _, MemoryInterNodeCommmunication transport) = await AssembleAsync();

        try
        {
            string name = "chained/" + Guid.NewGuid().ToString("N");
            int partitionId = new DataPartitionRouter(rafts[0]).Locate(SequenceActor.GetStorageKey(name));

            int outside = NonHostingIndex(rafts, partitionId);
            int leader = await LeaderIndex(rafts, partitionId);

            Assert.Equal(SequenceResponseType.Success, await CreateWithRetry(kahunas[leader], name));

            int forwardsBefore = transport.SequenceForwardCallCount;

            // The transport forward is what a request arriving from another node looks like here.
            (SequenceResponseType response, _) = await transport.NextSequenceValue(
                rafts[outside].GetLocalEndpoint(), name, null, SequenceDurability.Persistent, ct);

            Assert.Equal(SequenceResponseType.MustRetry, response);
            Assert.Equal(1, transport.SequenceForwardCallCount - forwardsBefore);
        }
        finally
        {
            await TearDownAsync(rafts);
        }
    }

    private static int NonHostingIndex(IRaft[] rafts, int partitionId)
    {
        for (int i = 0; i < rafts.Length; i++)
            if (!rafts[i].HostsPartition(partitionId))
                return i;

        throw new InvalidOperationException("every node hosts the sequence's partition; the fixture cannot exercise a non-hosting sender");
    }

    /// <summary>The target a non-hosting node without a leader hint forwards to.</summary>
    private static string FirstRemoteVoter(IRaft raft, int partitionId)
    {
        string local = raft.GetLocalEndpoint();

        foreach (RaftReplica replica in raft.GetPartitionReplicas(partitionId))
            if (replica.Role == RaftReplicaRole.Voter && !string.Equals(replica.Endpoint, local, StringComparison.Ordinal))
                return replica.Endpoint;

        throw new InvalidOperationException($"partition {partitionId} has no remote voter in the committed map");
    }

    private static int IndexOf(IRaft[] rafts, string endpoint)
    {
        for (int i = 0; i < rafts.Length; i++)
            if (string.Equals(rafts[i].GetLocalEndpoint(), endpoint, StringComparison.Ordinal))
                return i;

        throw new InvalidOperationException($"no node is listening on {endpoint}");
    }

    private static async Task<int> LeaderIndex(IRaft[] rafts, int partitionId)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        int leader = -1;

        await WaitUntilAsync(async () =>
        {
            for (int i = 0; i < rafts.Length; i++)
                if (await rafts[i].AmILeaderIfHosted(partitionId, ct))
                {
                    leader = i;
                    return true;
                }

            return false;
        }, timeoutMs: 30_000);

        return leader;
    }

    /// <summary>
    /// Hands leadership away from <paramref name="index"/> when it leads the partition, so the
    /// sender's guess is a follower by construction rather than by luck.
    /// </summary>
    private static async Task EnsureFollower(IRaft[] rafts, int partitionId, int index)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        for (int attempt = 0; attempt < 10; attempt++)
        {
            int leader = await LeaderIndex(rafts, partitionId);

            if (leader != index)
                return;

            string local = rafts[index].GetLocalEndpoint();
            string? target = null;

            foreach (RaftReplica replica in rafts[index].GetPartitionReplicas(partitionId))
                if (replica.Role == RaftReplicaRole.Voter && !string.Equals(replica.Endpoint, local, StringComparison.Ordinal))
                {
                    target = replica.Endpoint;
                    break;
                }

            Assert.NotNull(target);

            await rafts[index].TransferLeadershipAsync(partitionId, target, ct);

            await WaitUntilAsync(async () => !await rafts[index].AmILeaderIfHosted(partitionId, ct), timeoutMs: 30_000);
        }

        throw new TimeoutException($"node {rafts[index].GetLocalEndpoint()} kept leading partition {partitionId}");
    }

    /// <summary>
    /// Creates on the given node, absorbing the retryable outcome a freshly assembled cluster
    /// produces before its partition leaders settle.
    /// </summary>
    private static async Task<SequenceResponseType> CreateWithRetry(IKahuna kahuna, string name)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        for (int attempt = 0; attempt < 60; attempt++)
        {
            (SequenceResponseType response, _) = await kahuna.LocateAndCreateSequence(
                name, 0, 1, null, null, SequenceDurability.Persistent, ct);

            if (response != SequenceResponseType.MustRetry)
                return response;

            await Task.Delay(Math.Min(10 * (attempt + 1), 100), ct);
        }

        return SequenceResponseType.MustRetry;
    }

    private static async Task TearDownAsync(IRaft[] rafts)
    {
        foreach (IRaft raft in rafts)
            await LeaveCluster(raft);
    }
}
