using System.Text;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.System;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// Exercises Kahuna with Kommander's advisory leader balancer turned on. The balancer is a
/// cluster-mode Raft feature: it only ever relocates partition leadership (never data) through the
/// normal Raft handoff path. These tests assert that enabling it does not break the Kahuna data
/// path — the cluster still assembles, key/value operations remain correct, committed data survives
/// any balancer-driven leadership transfers, and the roster stays healthy (no spurious evictions,
/// no leaderless partitions). They deliberately do NOT assert a particular rebalanced distribution:
/// that is Kommander's own concern and is non-deterministic in an in-process multi-node harness.
/// </summary>
[Collection("ClusterTests")]
public sealed class TestLeaderBalancerCluster : BaseCluster
{
    private readonly ILogger<IRaft> raftLogger;
    private readonly ILogger<IKahuna> kahunaLogger;

    public TestLeaderBalancerCluster(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = TestLogFactory.Create(outputHelper);
        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    /// <summary>
    /// With the balancer enabled the cluster assembles and serves a basic set/get/delete cycle
    /// correctly, routed through different nodes.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task TestClusterServesKeyValuesWithLeaderBalancerEnabled(
        [CombinatorialValues("memory")] string storage,
        [CombinatorialValues(1, 3)] int partitions,
        [CombinatorialValues(KeyValueDurability.Ephemeral, KeyValueDurability.Persistent)] KeyValueDurability durability)
    {
        (IRaft n1, IRaft n2, IRaft n3, IKahuna k1, IKahuna k2, IKahuna k3, _, _) =
            await AssembleLeaderBalancerCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            const string prefix = "balancer/basic";

            // Write via k1, read back via k2 (routing forwards to the current leader).
            for (int i = 0; i < 20; i++)
            {
                (KeyValueResponseType setType, _, _) = await k1.LocateAndTrySetKeyValue(
                    HLCTimestamp.Zero, $"{prefix}/{i:D4}", Encoding.UTF8.GetBytes("v" + i),
                    null, -1, KeyValueFlags.Set, 0, durability, TestContext.Current.CancellationToken);
                Assert.Equal(KeyValueResponseType.Set, setType);
            }

            for (int i = 0; i < 20; i++)
                await AssertKeyEquals(k2, $"{prefix}/{i:D4}", "v" + i, durability);

            // Delete a few via k3 and confirm they are gone.
            for (int i = 0; i < 5; i++)
            {
                (KeyValueResponseType delType, _, _) = await k3.LocateAndTryDeleteKeyValue(
                    HLCTimestamp.Zero, $"{prefix}/{i:D4}", durability, TestContext.Current.CancellationToken);
                Assert.Equal(KeyValueResponseType.Deleted, delType);
            }

            for (int i = 0; i < 5; i++)
            {
                (KeyValueResponseType getType, _) = await k1.LocateAndTryGetValue(
                    HLCTimestamp.Zero, $"{prefix}/{i:D4}", -1, HLCTimestamp.Zero, durability,
                    TestContext.Current.CancellationToken);
                Assert.Equal(KeyValueResponseType.DoesNotExist, getType);
            }

            await AssertClusterHealthy(partitions, n1, n2, n3);
        }
        finally
        {
            await LeaveCluster(n1, n2, n3);
        }
    }

    /// <summary>
    /// Drives a sustained write workload across several partitions for long enough that the balancer
    /// runs multiple planning passes (and may transfer leaderships underneath the workload), then
    /// asserts every committed key is still readable with its original value and the cluster is
    /// healthy. This is the core "works well with the balancer on" guarantee: leadership moves never
    /// lose committed data or take the keyspace offline.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task TestCommittedDataSurvivesBalancerPasses(
        [CombinatorialValues("memory")] string storage,
        [CombinatorialValues(4)] int partitions)
    {
        const KeyValueDurability durability = KeyValueDurability.Persistent;

        (IRaft n1, IRaft n2, IRaft n3, IKahuna k1, IKahuna k2, IKahuna k3, _, _) =
            await AssembleLeaderBalancerCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            const string prefix = "balancer/workload";
            IKahuna[] nodes = [k1, k2, k3];
            Dictionary<string, string> confirmed = [];

            // ~200 rounds with a short pause between each spans several balancer pass intervals.
            // A write may transiently fail while a leadership handoff is in flight; only keys whose
            // Set was acknowledged are tracked, and those must all survive whatever moves occur.
            for (int i = 0; i < 200; i++)
            {
                string key = $"{prefix}/{i:D4}";
                string value = "v" + i;
                try
                {
                    (KeyValueResponseType setType, _, _) = await nodes[i % nodes.Length].LocateAndTrySetKeyValue(
                        HLCTimestamp.Zero, key, Encoding.UTF8.GetBytes(value),
                        null, -1, KeyValueFlags.Set, 0, durability, TestContext.Current.CancellationToken);

                    if (setType == KeyValueResponseType.Set)
                        confirmed[key] = value;
                }
                catch
                {
                    // Transient unavailability during a handoff — skip; not all writes must succeed.
                }

                await Task.Delay(15, TestContext.Current.CancellationToken);
            }

            // We expect the vast majority of writes to have committed even with balancing active.
            Assert.True(confirmed.Count >= 180, $"Only {confirmed.Count}/200 writes committed with the balancer enabled.");

            // Every acknowledged key must read back with its original value through the current
            // leader, regardless of which node now holds that partition's leadership.
            foreach ((string key, string value) in confirmed)
                await AssertKeyEquals(k2, key, value, durability);

            await AssertClusterHealthy(partitions, n1, n2, n3);
        }
        finally
        {
            await LeaveCluster(n1, n2, n3);
        }
    }

    // ── helpers ─────────────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Reads a key back, tolerating brief unavailability while a leadership handoff is in flight,
    /// and asserts it carries the expected value.
    /// </summary>
    private async Task AssertKeyEquals(IKahuna node, string key, string expected, KeyValueDurability durability)
    {
        await WaitUntilAsync(async () =>
        {
            try
            {
                (KeyValueResponseType type, var entry) = await node.LocateAndTryGetValue(
                    HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, durability,
                    TestContext.Current.CancellationToken);

                byte[]? bytes = entry?.Value;
                return type == KeyValueResponseType.Get
                       && bytes is not null
                       && Encoding.UTF8.GetString(bytes) == expected;
            }
            catch
            {
                return false;
            }
        });
    }

    /// <summary>
    /// Asserts the roster is still the three original voters (the balancer must never evict a node)
    /// and that no partition is left leaderless after the balancer's activity.
    /// </summary>
    private async Task AssertClusterHealthy(int partitions, IRaft n1, IRaft n2, IRaft n3)
    {
        ClusterMembership membership = n1.GetMembership();
        List<ClusterMember> voters = membership.Members
            .Where(member => member.Role == ClusterMemberRole.Voter)
            .ToList();
        Assert.Equal(3, voters.Count);

        for (int partition = 0; partition <= partitions; partition++)
        {
            int p = partition;
            await WaitUntilAsync(async () =>
                await n1.AmILeader(p, TestContext.Current.CancellationToken)
                || await n2.AmILeader(p, TestContext.Current.CancellationToken)
                || await n3.AmILeader(p, TestContext.Current.CancellationToken));
        }
    }
}
