using Kahuna.Server.KeyValues.Ranges;
using Kommander;
using Microsoft.Extensions.Logging;

namespace Kahuna.Tests.Server;

public class TestRegisterKeyRangeForwarding : BaseCluster
{
    private readonly ILogger<IRaft> raftLogger;
    private readonly ILogger<IKahuna> kahunaLogger;

    public TestRegisterKeyRangeForwarding(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = TestLogFactory.Create(outputHelper, quietKommander: true);
        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    [Theory, CombinatorialData]
    public async Task RegisterKeyRangeAsync_FromNonLeader_SeedsViaLeader(
        [CombinatorialValues("memory")] string storage,
        [CombinatorialValues(4)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            const string keySpace = "forward-test";

            // Find the node that is NOT the meta-partition leader.
            KahunaManager km1 = (KahunaManager)kahuna1;
            KahunaManager km2 = (KahunaManager)kahuna2;
            KahunaManager km3 = (KahunaManager)kahuna3;

            (IRaft leaderRaft, KahunaManager leaderKahuna) = await FindMetaLeader(node1, km1, node2, km2, node3, km3);
            (IRaft followerRaft, KahunaManager followerKahuna) = FindMetaFollower(
                (node1, km1), (node2, km2), (node3, km3), leaderRaft);

            // Register from the follower — F8 must forward the seed to the leader.
            bool seeded = await followerKahuna.RegisterKeyRangeAsync(keySpace, TestContext.Current.CancellationToken);

            // The descriptor must appear on every node within a short window (replication).
            await WaitForDescriptor(km1, keySpace);
            await WaitForDescriptor(km2, keySpace);
            await WaitForDescriptor(km3, keySpace);

            Assert.True(km1.RangeMapStore.Current.FindAll(keySpace).Count > 0, "node1 has descriptor");
            Assert.True(km2.RangeMapStore.Current.FindAll(keySpace).Count > 0, "node2 has descriptor");
            Assert.True(km3.RangeMapStore.Current.FindAll(keySpace).Count > 0, "node3 has descriptor");
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    private static async Task<(IRaft, KahunaManager)> FindMetaLeader(
        IRaft r1, KahunaManager k1, IRaft r2, KahunaManager k2, IRaft r3, KahunaManager k3)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        while (true)
        {
            if (await r1.AmILeader(RangeMapStore.MetaPartitionId, ct)) return (r1, k1);
            if (await r2.AmILeader(RangeMapStore.MetaPartitionId, ct)) return (r2, k2);
            if (await r3.AmILeader(RangeMapStore.MetaPartitionId, ct)) return (r3, k3);
            await Task.Delay(25, ct);
        }
    }

    private static (IRaft, KahunaManager) FindMetaFollower(
        (IRaft, KahunaManager) n1, (IRaft, KahunaManager) n2, (IRaft, KahunaManager) n3,
        IRaft leader)
    {
        if (!ReferenceEquals(n1.Item1, leader)) return n1;
        if (!ReferenceEquals(n2.Item1, leader)) return n2;
        return n3;
    }

    private static async Task WaitForDescriptor(KahunaManager km, string keySpace)
    {
        long deadline = Environment.TickCount64 + 10_000;
        while (Environment.TickCount64 < deadline && km.RangeMapStore.Current.FindAll(keySpace).Count == 0)
            await Task.Delay(25, TestContext.Current.CancellationToken);
    }
}
