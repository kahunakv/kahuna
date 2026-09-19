using System.Text;
using Kahuna.Server.Communication.Internode;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Routing;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// The in-memory inter-node transport can drop the calls between two given nodes, so an in-process
/// cluster can show a network partition. Each node calls through its own view of the shared transport,
/// which knows the sender; a blocked link fails the calls between its two nodes in both directions,
/// before they run, and leaves every other link working.
/// </summary>
public sealed class TestMemoryInterNodeLinkFilter
{
    private static readonly TimeSpan RunDeadline = TimeSpan.FromMinutes(3);

    private readonly ILoggerFactory loggerFactory;

    public TestMemoryInterNodeLinkFilter(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper, quietKommander: true);
    }

    [Fact]
    public async Task BlockedLink_FailsCallsBetweenThePair_AndLeavesOtherLinksWorking()
    {
        await SingleThreadedContext.RunAsync(async () =>
        {
            using CancellationTokenSource cts = new(RunDeadline);
            CancellationToken ct = cts.Token;

            await using EmbeddedKahunaCluster cluster = await EmbeddedKahunaCluster.CreateInMemoryAsync(
                3, TestEmbeddedKahunaCluster.ClusterOptions(), loggerFactory, ct);

            const string key = "link/key";
            // The partition Kahuna routes the key to: data partitions start at 1, after the meta partition.
            int partitionId = 1 + HashPlacement.BucketOfKey(key, TestEmbeddedKahunaCluster.ClusterOptions().InitialPartitions);
            int leader = await cluster.GetLeaderIndexAsync(partitionId, ct);
            int blocked = (leader + 1) % cluster.NodeCount;
            int open = (leader + 2) % cluster.NodeCount;

            string leaderEndpoint = cluster.GetEndpoint(leader);
            string blockedEndpoint = cluster.GetEndpoint(blocked);
            MemoryInterNodeCommmunication transport = cluster.InterNode;

            // One direction is enough: a call is a request and a reply, so it needs both directions.
            transport.BlockLink(blockedEndpoint, leaderEndpoint);
            Assert.True(transport.IsLinkBlocked(blockedEndpoint, leaderEndpoint));
            Assert.False(transport.IsLinkBlocked(leaderEndpoint, blockedEndpoint));

            // The blocked node cannot forward a write to the leader of the key's partition.
            (KeyValueResponseType refused, bool threw) = await TrySetAsync(cluster.GetNode(blocked), key, "through-blocked", ct);
            Assert.True(threw || refused != KeyValueResponseType.Set, $"a write forwarded over a blocked link succeeded ({refused})");

            // The other follower still reaches the leader.
            (KeyValueResponseType set, _) = await TrySetAsync(cluster.GetNode(open), key, "through-open", ct);
            Assert.Equal(KeyValueResponseType.Set, set);

            // Direct calls: both directions of the blocked pair fail before they run, with the reason;
            // the shared unbound instance knows no sender and never filters.
            KahunaServerException fromBlocked = await Assert.ThrowsAsync<KahunaServerException>(() =>
                transport.ForNode(blockedEndpoint).TryGetValue(leaderEndpoint, HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct));
            Assert.Contains("not reachable", fromBlocked.Message);

            await Assert.ThrowsAsync<KahunaServerException>(() =>
                transport.ForNode(leaderEndpoint).TryGetValue(blockedEndpoint, HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct));

            (KeyValueResponseType unbound, _) = await transport.TryGetValue(
                leaderEndpoint, HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Get, unbound);

            // An unknown node keeps its own message.
            KahunaServerException unknown = await Assert.ThrowsAsync<KahunaServerException>(() =>
                transport.ForNode(blockedEndpoint).TryGetValue("nowhere:1", HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct));
            Assert.Contains("does not exist", unknown.Message);

            // Unblocked, the node forwards again.
            transport.UnblockLink(blockedEndpoint, leaderEndpoint);
            Assert.False(transport.IsLinkBlocked(blockedEndpoint, leaderEndpoint));
            (set, _) = await TrySetAsync(cluster.GetNode(blocked), key, "through-unblocked", ct);
            Assert.Equal(KeyValueResponseType.Set, set);

            // UnblockAllLinks clears every blocked link at once.
            transport.BlockLink(blockedEndpoint, leaderEndpoint);
            transport.BlockLink(cluster.GetEndpoint(open), leaderEndpoint);
            transport.UnblockAllLinks();
            Assert.False(transport.IsLinkBlocked(blockedEndpoint, leaderEndpoint));
            Assert.False(transport.IsLinkBlocked(cluster.GetEndpoint(open), leaderEndpoint));
        }, RunDeadline + TimeSpan.FromSeconds(30));
    }

    /// <summary>
    /// A non-transactional write through <paramref name="node"/>, retried only on MustRetry so a leader
    /// change does not fail the test. A blocked link surfaces as a thrown exception or a refused status.
    /// </summary>
    private static async Task<(KeyValueResponseType Type, bool Threw)> TrySetAsync(EmbeddedKahunaNode node, string key, string value, CancellationToken ct)
    {
        for (int attempt = 0; attempt < 20; attempt++)
        {
            try
            {
                (KeyValueResponseType type, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
                    HLCTimestamp.Zero, key, Encoding.UTF8.GetBytes(value), null, -1, KeyValueFlags.Set, 0,
                    KeyValueDurability.Persistent, ct);

                if (type != KeyValueResponseType.MustRetry)
                    return (type, false);
            }
            catch (KahunaServerException)
            {
                return (KeyValueResponseType.Errored, true);
            }

            await Task.Delay(50, ct);
        }

        return (KeyValueResponseType.MustRetry, false);
    }
}
