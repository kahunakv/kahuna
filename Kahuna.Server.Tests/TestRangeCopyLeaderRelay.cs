using System.Text;

using Kahuna.Server.Communication.Internode;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Ranges;

using Kommander;
using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.System;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// The write leg of the split/merge range copy under per-partition replica placement. A split's
/// destination partition is created moments before the copy starts, so its first leader election
/// is still in flight, and the driving node usually does not host it — its target is a guess from
/// the committed replica set. These tests pin the two behaviors that make the copy land anyway:
/// a replica that receives a page it does not lead relays it to its group's leader instead of
/// failing a local proposal, and a send that throws against an unreachable target costs the copy
/// one attempt rather than the whole split.
/// </summary>
public sealed class TestRangeCopyLeaderRelay : BaseCluster
{
    private readonly Microsoft.Extensions.Logging.ILogger<IRaft> raftLogger;

    private readonly Microsoft.Extensions.Logging.ILogger<IKahuna> kahunaLogger;

    public TestRangeCopyLeaderRelay(ITestOutputHelper outputHelper)
    {
        Microsoft.Extensions.Logging.ILoggerFactory loggerFactory = TestLogFactory.Create(outputHelper);
        raftLogger = Microsoft.Extensions.Logging.LoggerFactoryExtensions.CreateLogger<IRaft>(loggerFactory);
        kahunaLogger = Microsoft.Extensions.Logging.LoggerFactoryExtensions.CreateLogger<IKahuna>(loggerFactory);
    }

    private const int Partitions = 3;

    private async Task<(IRaft[] Rafts, KahunaManager[] Kahunas, MemoryInterNodeCommmunication InterComm, CancellationTokenSource Cts)> AssembleRf2Cluster()
    {
        InMemoryCommunication raftComm = new();
        MemoryInterNodeCommmunication interComm = new();

        // Replication factor 2 on three voters: every partition has exactly two replicas, so a
        // fresh partition always leaves one node hosting nothing and one hosting replica that is
        // not the leader — the two vantage points these tests need.
        (IRaft raft1, IKahuna kahuna1) = BuildNode(interComm, raftComm, "memory", 1, 8001, ["localhost:8002", "localhost:8003"], raftLogger, kahunaLogger, Partitions, replicationFactor: 2);
        (IRaft raft2, IKahuna kahuna2) = BuildNode(interComm, raftComm, "memory", 2, 8002, ["localhost:8001", "localhost:8003"], raftLogger, kahunaLogger, Partitions, replicationFactor: 2);
        (IRaft raft3, IKahuna kahuna3) = BuildNode(interComm, raftComm, "memory", 3, 8003, ["localhost:8001", "localhost:8002"], raftLogger, kahunaLogger, Partitions, replicationFactor: 2);

        interComm.SetNodes(new()
        {
            { "localhost:8001", kahuna1 },
            { "localhost:8002", kahuna2 },
            { "localhost:8003", kahuna3 }
        });

        raftComm.SetNodes(new()
        {
            { "localhost:8001", raft1 },
            { "localhost:8002", raft2 },
            { "localhost:8003", raft3 }
        });

        CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(180));
        CancellationToken ct = cts.Token;

        IRaft[] rafts = [raft1, raft2, raft3];
        KahunaManager[] kahunas = [(KahunaManager)kahuna1, (KahunaManager)kahuna2, (KahunaManager)kahuna3];

        await Task.WhenAll(raft1.JoinCluster(ct), raft2.JoinCluster(ct), raft3.JoinCluster(ct));

        for (int partitionId = 0; partitionId <= Partitions; partitionId++)
        {
            while (true)
            {
                ct.ThrowIfCancellationRequested();

                bool anyLeader = false;
                foreach (IRaft raft in rafts)
                {
                    if (await raft.AmILeaderIfHosted(partitionId, ct))
                    {
                        anyLeader = true;
                        break;
                    }
                }

                if (anyLeader)
                    break;

                await Task.Delay(50, ct);
            }
        }

        return (rafts, kahunas, interComm, cts);
    }

    /// <summary>
    /// Creates a fresh partition through the system-partition leader and resolves the three
    /// vantage points on it: its leader node, a hosting replica that is not the leader, and the
    /// node that does not host it at all.
    /// </summary>
    private static async Task<(int PartitionId, int LeaderIndex, int FollowerIndex, int OutsiderIndex)> CreateFreshPartition(
        IRaft[] rafts, CancellationToken ct)
    {
        IRaft p0Leader;
        while (true)
        {
            IRaft? candidate = null;
            foreach (IRaft raft in rafts)
                if (await raft.AmILeaderIfHosted(RaftSystemConfig.SystemPartition, ct))
                {
                    candidate = raft;
                    break;
                }

            if (candidate is not null)
            {
                p0Leader = candidate;
                break;
            }

            await Task.Delay(50, ct);
        }

        int partitionId = p0Leader.GetNextAvailablePartitionId();
        RaftPartitionLifecycleResult created = await p0Leader.CreatePartitionAsync(partitionId, RaftRoutingMode.Unrouted, null, ct);
        Assert.True(created.Success);

        // Wait for the committed replica set to reach every node's map.
        foreach (IRaft raft in rafts)
            while (raft.GetPartitionReplicas(partitionId).Count == 0)
            {
                ct.ThrowIfCancellationRequested();
                await Task.Delay(50, ct);
            }

        // Wait for the fresh group's first election.
        int leaderIndex;
        while (true)
        {
            int candidate = -1;
            for (int i = 0; i < rafts.Length; i++)
                if (await rafts[i].AmILeaderIfHosted(partitionId, ct))
                {
                    candidate = i;
                    break;
                }

            if (candidate >= 0)
            {
                leaderIndex = candidate;
                break;
            }

            await Task.Delay(50, ct);
        }

        int followerIndex = -1, outsiderIndex = -1;
        for (int i = 0; i < rafts.Length; i++)
        {
            if (i == leaderIndex)
                continue;
            if (rafts[i].HostsPartition(partitionId))
                followerIndex = i;
            else
                outsiderIndex = i;
        }

        Assert.True(followerIndex >= 0);
        Assert.True(outsiderIndex >= 0);

        return (partitionId, leaderIndex, followerIndex, outsiderIndex);
    }

    private static byte[] BuildPage(string key)
    {
        HLCTimestamp ts = new(0, 1, 0);
        List<(string, ReadOnlyKeyValueEntry)> items =
        [
            (key, new ReadOnlyKeyValueEntry(Encoding.UTF8.GetBytes(key), 1, HLCTimestamp.Zero, ts, ts, KeyValueState.Set))
        ];

        using MemoryStream frame = new();
        KvStateMachineTransfer.WritePage(frame, items, hasMore: false);
        return frame.ToArray();
    }

    [Fact]
    public async Task PageSentToFollowerReplica_RelaysToTheGroupLeader_AndCommits()
    {
        (IRaft[] rafts, KahunaManager[] _, MemoryInterNodeCommmunication interComm, CancellationTokenSource cts) = await AssembleRf2Cluster();
        using CancellationTokenSource ctsGuard = cts;
        CancellationToken ct = cts.Token;

        (int partitionId, _, int followerIndex, _) = await CreateFreshPartition(rafts, ct);

        string followerEndpoint = rafts[followerIndex].GetLocalEndpoint();

        // The sender guessed a hosting replica that is not the leader — exactly what a split
        // driver's committed-map pick does before any leader hint has gossiped. The receiving
        // replica must relay the page to its group's leader, not fail a local proposal.
        bool committed = await interComm.ReplicateKeyValueRangePage(followerEndpoint, partitionId, BuildPage("relay/k1"), ct);

        Assert.True(committed);
    }

    [Fact]
    public async Task PageSendThatThrows_CostsOneAttempt_AndTheCopyStillLands()
    {
        (IRaft[] rafts, KahunaManager[] kahunas, MemoryInterNodeCommmunication _, CancellationTokenSource cts) = await AssembleRf2Cluster();
        using CancellationTokenSource ctsGuard = cts;
        CancellationToken ct = cts.Token;

        (int partitionId, _, _, int outsiderIndex) = await CreateFreshPartition(rafts, ct);

        KahunaManager driver = kahunas[outsiderIndex];

        // The first send fails as an unreachable node's transport would; before the retry-and-
        // rotate discipline this exception aborted the whole copy, and the driver's deterministic
        // replica guess re-targeted the same dead node on every split attempt.
        int sends = 0;
        driver.KeyValues.RangePageSendFault = _ => Interlocked.Increment(ref sends) == 1;

        try
        {
            bool committed = await driver.KeyValues.ReplicateKeyValueRangePageToPartitionLeaderAsync(
                partitionId, BuildPage("relay/k2"), ct);

            Assert.True(committed);
            Assert.True(sends >= 2);
        }
        finally
        {
            driver.KeyValues.RangePageSendFault = null;
        }
    }
}
