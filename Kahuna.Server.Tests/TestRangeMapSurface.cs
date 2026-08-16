using System.Text;
using System.Text.Json;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Shared.Communication.Rest;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// Tests for the range-map read surface: the projection a caller reads to see which contiguous
/// ranges exist, which partition serves each, and how the answering node routes each key space.
/// <para>
/// The projection is what makes a split observable from outside the process. A run that cannot read
/// the descriptor set can only report that a split was requested, never that the key space is still
/// covered afterwards — so these tests assert the tiling itself (no gap, no overlap, ordinal bounds),
/// not merely that a call returned success.
/// </para>
/// </summary>
public sealed class TestRangeMapSurface : BaseCluster
{
    /// <summary>Kommander's partition 0 leads both the system partition and the meta range map.</summary>
    private const int MetaPartition = 0;

    private readonly ILogger<IRaft> raftLogger;
    private readonly ILogger<IKahuna> kahunaLogger;

    public TestRangeMapSurface(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = TestLogFactory.Create(outputHelper, quietKommander: true);
        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    private static byte[] V(string s) => Encoding.UTF8.GetBytes(s);

    private static async Task<(IRaft Raft, KahunaManager Kahuna)> LeaderOf(
        int partition, (IRaft, KahunaManager)[] nodes)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        while (true)
        {
            foreach ((IRaft raft, KahunaManager kahuna) in nodes)
                if (await raft.AmILeader(partition, ct))
                    return (raft, kahuna);

            await Task.Delay(25, ct);
        }
    }

    private async Task<(IRaft[] Rafts, (IRaft, KahunaManager)[] Nodes)> AssembleCluster()
    {
        (IRaft r1, IRaft r2, IRaft r3, IKahuna k1, IKahuna k2, IKahuna k3) =
            await AssembleThreNodeCluster("memory", 3, raftLogger, kahunaLogger);

        return ([r1, r2, r3], [(r1, (KahunaManager)k1), (r2, (KahunaManager)k2), (r3, (KahunaManager)k3)]);
    }

    /// <summary>
    /// Reads one key space out of a projection, failing with the whole document when it is absent —
    /// a missing key space is the failure mode worth seeing in full.
    /// </summary>
    private static KahunaKeySpaceRangesResponse SpaceIn(KahunaRangeMapResponse map, string keySpace)
    {
        foreach (KahunaKeySpaceRangesResponse entry in map.KeySpaces)
            if (entry.KeySpace == keySpace)
                return entry;

        Assert.Fail($"key space '{keySpace}' missing from: " +
                    JsonSerializer.Serialize(map, KahunaJsonContext.Default.KahunaRangeMapResponse));
        return null!;
    }

    /// <summary>
    /// Asserts the descriptors tile the key space: ascending ordinal bounds, the first opening at
    /// −infinity, the last closing at +infinity, and each range starting exactly where the previous
    /// one ended. A gap loses keys and an overlap serves them from two partitions, so both are
    /// checked here rather than trusting a count.
    /// </summary>
    private static void AssertTilesSpace(List<KahunaRangeDescriptorResponse> descriptors)
    {
        Assert.NotEmpty(descriptors);
        Assert.Null(descriptors[0].StartKey);
        Assert.Null(descriptors[^1].EndKey);

        for (int i = 1; i < descriptors.Count; i++)
        {
            KahunaRangeDescriptorResponse previous = descriptors[i - 1];
            KahunaRangeDescriptorResponse current = descriptors[i];

            Assert.NotNull(previous.EndKey);
            Assert.NotNull(current.StartKey);
            Assert.Equal(0, string.CompareOrdinal(previous.EndKey, current.StartKey));
            Assert.True(
                current.EndKey is null || string.CompareOrdinal(current.StartKey, current.EndKey) < 0,
                $"range [{current.StartKey}, {current.EndKey}) is empty or inverted");
        }
    }

    /// <summary>
    /// Registration seeds one whole-space descriptor, and every node reports it — the descriptor set
    /// is replicated even though the routing mode that accompanies it is not.
    /// </summary>
    [Fact]
    public async Task RangeMap_AfterRegistration_ReportsOneWholeSpaceDescriptorOnEveryNode()
    {
        const string space = "surface:seed";
        (IRaft[] rafts, (IRaft, KahunaManager)[] nodes) = await AssembleCluster();

        try
        {
            CancellationToken ct = TestContext.Current.CancellationToken;

            foreach ((IRaft _, KahunaManager kahuna) in nodes)
                await kahuna.RegisterKeyRangeAsync(space, ct);

            foreach ((IRaft _, KahunaManager kahuna) in nodes)
                await WaitUntilAsync(() => kahuna.GetRangeMap(space).KeySpaces[0].Descriptors.Count == 1);

            foreach ((IRaft raft, KahunaManager kahuna) in nodes)
            {
                KahunaRangeMapResponse map = kahuna.GetRangeMap();

                Assert.True(map.Initialized);
                Assert.Equal(raft.GetLocalEndpoint(), map.LocalEndpoint);

                KahunaKeySpaceRangesResponse entry = SpaceIn(map, space);
                Assert.Equal(nameof(RoutingMode.KeyRange), entry.RoutingMode);

                KahunaRangeDescriptorResponse descriptor = Assert.Single(entry.Descriptors);
                Assert.Null(descriptor.StartKey);
                Assert.Null(descriptor.EndKey);
                Assert.True(descriptor.PartitionId >= RangeMapStore.FirstDataPartitionId);
            }
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    /// <summary>
    /// After a split the space is served by two adjacent descriptors on two partitions, and the
    /// projection shows the tiling. This is the property a chaos run checks: the split is only safe
    /// if the post-split map still covers every key exactly once.
    /// </summary>
    [Fact]
    public async Task RangeMap_AfterSplit_ShowsTwoAdjacentDescriptorsOnTwoPartitions()
    {
        const string space = "surface:split";
        const string splitKey = space + "/m";

        (IRaft[] rafts, (IRaft, KahunaManager)[] nodes) = await AssembleCluster();

        try
        {
            CancellationToken ct = TestContext.Current.CancellationToken;

            foreach ((IRaft _, KahunaManager kahuna) in nodes)
                await kahuna.RegisterKeyRangeAsync(space, ct);

            foreach ((IRaft _, KahunaManager kahuna) in nodes)
                await WaitUntilAsync(() => kahuna.GetRangeMap(space).KeySpaces[0].Descriptors.Count == 1);

            // The splitter refuses a split that would leave either half empty, so both sides of the
            // split key must hold keys before it is asked to split there.
            (IRaft _, KahunaManager writer) = await LeaderOf(MetaPartition, nodes);
            foreach (string key in (string[])[space + "/a", space + "/b", space + "/p", space + "/q"])
            {
                (KeyValueResponseType type, _, _) = await writer.LocateAndTrySetKeyValue(
                    HLCTimestamp.Zero, key, V("v"), null, -1, KeyValueFlags.Set, 0,
                    KeyValueDurability.Persistent, ct);
                Assert.Equal(KeyValueResponseType.Set, type);
            }

            SplitOutcome outcome = default;
            for (int attempt = 0; attempt < 5; attempt++)
            {
                (IRaft _, KahunaManager leader) = await LeaderOf(MetaPartition, nodes);
                outcome = await leader.ForceSplitAtKeyAsync(space, splitKey, null, ct);

                // A leader change between resolving the leader and committing the cutover is a
                // retryable coincidence, not a failure of the surface under test.
                if (outcome.Status is not (SplitStatus.CutoverFailed or SplitStatus.ConcurrentSplit))
                    break;

                await Task.Delay(100, ct);
            }

            Assert.True(outcome.IsSuccess, $"split failed: {outcome.Status}");

            foreach ((IRaft _, KahunaManager kahuna) in nodes)
                await WaitUntilAsync(() => kahuna.GetRangeMap(space).KeySpaces[0].Descriptors.Count == 2);

            foreach ((IRaft _, KahunaManager kahuna) in nodes)
            {
                List<KahunaRangeDescriptorResponse> descriptors = SpaceIn(kahuna.GetRangeMap(), space).Descriptors;

                Assert.Equal(2, descriptors.Count);
                AssertTilesSpace(descriptors);

                // The cut lands exactly on the requested key, and the halves are served by different
                // Raft groups — a "split" that left both halves on one partition would still tile.
                Assert.Equal(splitKey, descriptors[0].EndKey);
                Assert.Equal(splitKey, descriptors[1].StartKey);
                Assert.NotEqual(descriptors[0].PartitionId, descriptors[1].PartitionId);
                Assert.Equal(outcome.NewPartitionId, descriptors[1].PartitionId);
            }
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    /// <summary>
    /// Descriptors are emitted in ordinal order, which is the order routing binary-searches. A
    /// culture-aware sort would place "a" before "A" and hand a reader a descriptor list whose
    /// adjacency checks fail on a map that is perfectly valid.
    /// </summary>
    [Fact]
    public async Task RangeMap_DescriptorOrder_IsOrdinalNotCultureAware()
    {
        const string space = "surface:order";
        (IRaft[] rafts, (IRaft, KahunaManager)[] nodes) = await AssembleCluster();

        try
        {
            CancellationToken ct = TestContext.Current.CancellationToken;

            foreach ((IRaft _, KahunaManager kahuna) in nodes)
                kahuna.RegisterKeyRange(space);

            (IRaft _, KahunaManager metaLeader) = await LeaderOf(RangeMapStore.MetaPartitionId, nodes);

            // Committed deliberately out of order: the map sorts, and the projection must not resort.
            bool committed = await metaLeader.RangeMapStore.MutateAsync(_ =>
            [
                new RangeDescriptor { KeySpace = space, StartKey = "a",  EndKey = null, PartitionId = 1, Generation = 1 },
                new RangeDescriptor { KeySpace = space, StartKey = null, EndKey = "A",  PartitionId = 1, Generation = 1 },
                new RangeDescriptor { KeySpace = space, StartKey = "A",  EndKey = "a",  PartitionId = 2, Generation = 1 }
            ], ct);
            Assert.True(committed);

            await WaitUntilAsync(() => metaLeader.GetRangeMap(space).KeySpaces[0].Descriptors.Count == 3);

            List<KahunaRangeDescriptorResponse> descriptors = SpaceIn(metaLeader.GetRangeMap(), space).Descriptors;

            // Ordinal: 'A' (65) precedes 'a' (97). Culture-aware ordering inverts exactly this pair.
            Assert.Null(descriptors[0].StartKey);
            Assert.Equal("A", descriptors[1].StartKey);
            Assert.Equal("a", descriptors[2].StartKey);
            AssertTilesSpace(descriptors);
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    /// <summary>
    /// A key space registered on this node but not yet seeded is reported with its routing mode and
    /// an empty descriptor list. Dropping such spaces would hide the one state in which the space
    /// routes by key range and has nothing to route to — every write to it throws.
    /// </summary>
    [Fact]
    public async Task RangeMap_RegisteredButUnseededSpace_IsReportedWithNoDescriptors()
    {
        const string space = "surface:unseeded";
        (IRaft[] rafts, (IRaft, KahunaManager)[] nodes) = await AssembleCluster();

        try
        {
            (IRaft _, KahunaManager node) = nodes[0];

            // The mode flip alone, with no seed: read it back before any range-map entry can be
            // applied (an applied entry re-derives the registry from the map).
            node.RegisterKeyRange(space);
            KahunaKeySpaceRangesResponse entry = SpaceIn(node.GetRangeMap(), space);

            Assert.Equal(nameof(RoutingMode.KeyRange), entry.RoutingMode);
            Assert.Empty(entry.Descriptors);
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    /// <summary>
    /// A filtered read answers for a key space nothing knows about, rather than returning an empty
    /// document the caller has to interpret: a poller waiting for a registration to land reads
    /// "Hash, no descriptors" until it does.
    /// </summary>
    [Fact]
    public async Task RangeMap_FilteredByUnknownKeySpace_ReportsHashWithNoDescriptors()
    {
        const string known = "surface:filter";
        const string unknown = "surface:absent";

        (IRaft[] rafts, (IRaft, KahunaManager)[] nodes) = await AssembleCluster();

        try
        {
            CancellationToken ct = TestContext.Current.CancellationToken;
            (IRaft _, KahunaManager node) = nodes[0];

            await node.RegisterKeyRangeAsync(known, ct);
            await WaitUntilAsync(() => node.GetRangeMap(known).KeySpaces[0].Descriptors.Count == 1);

            KahunaRangeMapResponse filtered = node.GetRangeMap(unknown);
            KahunaKeySpaceRangesResponse entry = Assert.Single(filtered.KeySpaces);

            Assert.Equal(unknown, entry.KeySpace);
            Assert.Equal(nameof(RoutingMode.Hash), entry.RoutingMode);
            Assert.Empty(entry.Descriptors);

            // The filter narrows the document rather than changing the envelope.
            Assert.True(filtered.Initialized);
            Assert.Equal(node.GetRangeMap().LocalEndpoint, filtered.LocalEndpoint);
            Assert.Single(node.GetRangeMap(known).KeySpaces);
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    /// <summary>
    /// The wire names a cross-repo consumer reads. Null bounds must survive as JSON null (they mean
    /// ±infinity, and a bound dropped from the document reads as a missing key, not an open range).
    /// </summary>
    [Fact]
    public void RangeMapResponse_SurvivesJsonRoundTripWithNullBounds()
    {
        KahunaRangeMapResponse map = new()
        {
            Initialized = true,
            LocalEndpoint = "localhost:8001",
            KeySpaces =
            [
                new KahunaKeySpaceRangesResponse
                {
                    KeySpace = "t:r",
                    RoutingMode = "KeyRange",
                    Descriptors =
                    [
                        new KahunaRangeDescriptorResponse { StartKey = null, EndKey = "t:r/m", PartitionId = 1, Generation = 2 },
                        new KahunaRangeDescriptorResponse { StartKey = "t:r/m", EndKey = null, PartitionId = 4, Generation = 2 }
                    ]
                }
            ]
        };

        string json = JsonSerializer.Serialize(map, KahunaJsonContext.Default.KahunaRangeMapResponse);

        using (JsonDocument document = JsonDocument.Parse(json))
        {
            JsonElement space = document.RootElement.GetProperty("keySpaces")[0];
            Assert.Equal("t:r", space.GetProperty("keySpace").GetString());
            Assert.Equal("KeyRange", space.GetProperty("routingMode").GetString());

            JsonElement first = space.GetProperty("descriptors")[0];
            Assert.Equal(JsonValueKind.Null, first.GetProperty("startKey").ValueKind);
            Assert.Equal("t:r/m", first.GetProperty("endKey").GetString());
            Assert.Equal(1, first.GetProperty("partitionId").GetInt32());
            Assert.Equal(2, first.GetProperty("generation").GetInt64());
        }

        KahunaRangeMapResponse? back = JsonSerializer.Deserialize(json, KahunaJsonContext.Default.KahunaRangeMapResponse);

        Assert.NotNull(back);
        Assert.True(back!.Initialized);
        KahunaKeySpaceRangesResponse entry = Assert.Single(back.KeySpaces);
        Assert.Equal(2, entry.Descriptors.Count);
        Assert.Null(entry.Descriptors[0].StartKey);
        Assert.Null(entry.Descriptors[1].EndKey);
        Assert.Equal(4, entry.Descriptors[1].PartitionId);
    }
}
