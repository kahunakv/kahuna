using System.Text;
using Kahuna.Communication.External.Rest;
using Kahuna.Server.Communication.Internode;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Shared.Communication.Rest;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Communication.Memory;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// The acceptance run for the range-administration surface, driven the way an operator drives it:
/// register a key space on every node, write keys, read the map, split, read the map again.
/// <para>
/// Run under a replication factor, where each data partition lives on exactly one node, so the node
/// driving the split provably hosts neither the source range nor the destination. That is the
/// configuration in which a split moves data across nodes rather than within one, and where a
/// surface that merely returned success without the data following would show up.
/// </para>
/// <para>
/// The property asserted after the split is not "two descriptors exist" but that the space is still
/// covered exactly once and every key written before the cutover is still readable exactly once —
/// coverage and data, checked separately, because a map that tiles perfectly can still have lost the
/// rows it points at.
/// </para>
/// </summary>
public sealed class TestRangeAdminAcceptance : BaseCluster
{
    private const int Partitions = 6;
    private const string Space = "acceptance:rows";
    private const string SplitKey = Space + "/k50";

    private readonly ILogger<IRaft> raftLogger;
    private readonly ILogger<IKahuna> kahunaLogger;

    public TestRangeAdminAcceptance(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = TestLogFactory.Create(outputHelper, quietKommander: true);
        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    private static byte[] V(string s) => Encoding.UTF8.GetBytes(s);

    private async Task<(IRaft[] Rafts, KahunaManager[] Nodes)> AssembleReplicatedCluster()
    {
        InMemoryCommunication raftComm = new();
        MemoryInterNodeCommmunication interComm = new();

        (IRaft raft1, IKahuna kahuna1) = BuildNode(interComm, raftComm, "memory", 1, 8001,
            ["localhost:8002", "localhost:8003"], raftLogger, kahunaLogger, Partitions, replicationFactor: 1);
        (IRaft raft2, IKahuna kahuna2) = BuildNode(interComm, raftComm, "memory", 2, 8002,
            ["localhost:8001", "localhost:8003"], raftLogger, kahunaLogger, Partitions, replicationFactor: 1);
        (IRaft raft3, IKahuna kahuna3) = BuildNode(interComm, raftComm, "memory", 3, 8003,
            ["localhost:8001", "localhost:8002"], raftLogger, kahunaLogger, Partitions, replicationFactor: 1);

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

        CancellationToken ct = TestContext.Current.CancellationToken;
        await Task.WhenAll(raft1.JoinCluster(ct), raft2.JoinCluster(ct), raft3.JoinCluster(ct));

        IRaft[] rafts = [raft1, raft2, raft3];

        // Placement is committed with the bootstrap map; wait until every node's applied map shows
        // the replica assignments so the split does not race the placement itself.
        foreach (IRaft raft in rafts)
            for (int partitionId = 1; partitionId <= Partitions; partitionId++)
                await WaitUntilAsync(() => raft.GetPartitionReplicas(partitionId).Count > 0);

        return (rafts, [(KahunaManager)kahuna1, (KahunaManager)kahuna2, (KahunaManager)kahuna3]);
    }

    /// <summary>
    /// Splits through the public surface the way the CLI does: try each node, moving on only when the
    /// refusal is about leadership. Any other status is this cluster's answer and would be the same
    /// from every node.
    /// </summary>
    private static async Task<KahunaSplitRangeResponse> SplitViaAnyLeader(
        KahunaManager[] nodes, string keySpace, string splitKey, CancellationToken ct)
    {
        KahunaSplitRangeResponse? last = null;

        for (int attempt = 0; attempt < 5; attempt++)
        {
            foreach (KahunaManager node in nodes)
            {
                last = await node.SplitRangeAtKeyWithOutcomeAsync(keySpace, splitKey, ct);

                // Leadership is the one refusal worth re-asking elsewhere.
                if (last.Status == "NotLeader")
                    continue;

                // Anything determinate is this cluster's answer, success or not.
                if (last.Determinate)
                    return last;

                // Indeterminate: a leadership change between the gate and the cutover commit. Back
                // off and start over rather than treating a coincidence as a verdict.
                break;
            }

            await Task.Delay(200, ct);
        }

        return last!;
    }

    [Fact]
    public async Task RangeAdminLifecycle_UnderReplication_KeepsTheSpaceCoveredAndEveryKeyReadable()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (IRaft[] rafts, KahunaManager[] nodes) = await AssembleReplicatedCluster();

        try
        {
            // ── register on every node ───────────────────────────────────────────────────────
            // The seed is replicated; the routing mode is not, which is why this runs everywhere.
            int seededBy = 0;
            foreach (KahunaManager node in nodes)
            {
                KahunaRegisterKeyRangeResponse registered = await node.RegisterKeyRangeWithOutcomeAsync(Space, ct);
                Assert.True(registered.Success, $"register refused: {registered.Status} — {registered.Reason}");
                Assert.Equal(nameof(RoutingMode.KeyRange), registered.RoutingMode);

                if (registered.Seeded)
                    seededBy++;
            }

            // Exactly one call commits the seed, however many nodes are told.
            Assert.Equal(1, seededBy);

            foreach (KahunaManager node in nodes)
                await WaitUntilAsync(() => node.GetRangeMap(Space).KeySpaces[0].Descriptors.Count == 1);

            // ── write keys spanning the split point ──────────────────────────────────────────
            List<string> keys = [];
            for (int i = 0; i < 100; i += 10)
                keys.Add($"{Space}/k{i:D2}");

            foreach (string key in keys)
            {
                (KeyValueResponseType type, _, _) = await nodes[0].LocateAndTrySetKeyValue(
                    HLCTimestamp.Zero, key, V(key), null, -1, KeyValueFlags.Set, 0,
                    KeyValueDurability.Persistent, ct);
                Assert.Equal(KeyValueResponseType.Set, type);
            }

            // ── one whole-space descriptor before the split ──────────────────────────────────
            KahunaKeySpaceRangesResponse before = nodes[0].GetRangeMap(Space).KeySpaces[0];
            KahunaRangeDescriptorResponse seed = Assert.Single(before.Descriptors);
            Assert.Null(seed.StartKey);
            Assert.Null(seed.EndKey);

            // ── split ────────────────────────────────────────────────────────────────────────
            KahunaSplitRangeResponse split = await SplitViaAnyLeader(nodes, Space, SplitKey, ct);
            Assert.True(split.Success, $"split refused: {split.Status} — {split.Reason}");
            Assert.True(split.Determinate);

            foreach (KahunaManager node in nodes)
                await WaitUntilAsync(() => node.GetRangeMap(Space).KeySpaces[0].Descriptors.Count == 2);

            // ── the space is still covered exactly once, on every node ───────────────────────
            foreach (KahunaManager node in nodes)
            {
                List<KahunaRangeDescriptorResponse> descriptors = node.GetRangeMap(Space).KeySpaces[0].Descriptors;

                Assert.Equal(2, descriptors.Count);
                Assert.Null(descriptors[0].StartKey);
                Assert.Equal(SplitKey, descriptors[0].EndKey);
                Assert.Equal(SplitKey, descriptors[1].StartKey);
                Assert.Null(descriptors[1].EndKey);
                Assert.NotEqual(descriptors[0].PartitionId, descriptors[1].PartitionId);
                Assert.Equal(split.NewPartitionId, descriptors[1].PartitionId);
            }

            // ── every key survived the cutover, exactly once ─────────────────────────────────
            // Read through the router from a node that hosts neither half, so the answer comes from
            // the partition leaders rather than from local state that happens to still hold the rows.
            foreach (string key in keys)
            {
                string k = key;
                await WaitUntilAsync(async () =>
                {
                    (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await nodes[2].LocateAndTryGetValue(
                        HLCTimestamp.Zero, k, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);

                    return type == KeyValueResponseType.Get && entry?.Value is not null;
                });
            }

            // A bucket scan crosses both halves and must return the whole set once — the check a
            // per-key lookup cannot make, because a duplicated row answers a point read just fine.
            KeyValueGetByBucketResult scan = await nodes[2].LocateAndGetByBucket(
                HLCTimestamp.Zero, Space, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);

            Assert.Equal(KeyValueResponseType.Get, scan.Type);
            Assert.Equal(keys.Count, scan.Items.Count);
            Assert.Equal(
                keys.OrderBy(static k => k, StringComparer.Ordinal),
                scan.Items.Select(static i => i.Item1).OrderBy(static k => k, StringComparer.Ordinal));

            // ── the destination partition's placement is visible ─────────────────────────────
            // A split creates a partition, and the rebalancer places it. An operator who can see the
            // new range but not who serves it cannot act on either.
            KahunaClusterPlacementResponse placement = ClusterHandlers.BuildPlacementResponse(rafts[0]);
            KahunaPartitionPlacementResponse destination = Assert.Single(
                placement.Partitions, p => p.PartitionId == split.NewPartitionId);

            Assert.NotEmpty(destination.Replicas);
            Assert.All(destination.Replicas, r => Assert.False(string.IsNullOrEmpty(r.Endpoint)));
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }
}
