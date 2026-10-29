using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Shared.Communication.Rest;
using Kommander;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// Where a key-range space's first descriptor starts (<see cref="KeyRangeSeedPlacement"/>). A new placement
/// group seeds on the partition hash routing gives it, so a table's initial partition is the same in both routing
/// modes; a space whose group is already seeded follows the group, so a table's late index space stays with its
/// rows even when the rows were seeded elsewhere by an earlier rule or an older meta leader.
/// </summary>
public sealed class TestKeyRangeSeedPlacement : BaseCluster
{
    private readonly ILogger<IRaft> raftLogger;
    private readonly ILogger<IKahuna> kahunaLogger;

    public TestKeyRangeSeedPlacement(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = TestLogFactory.Create(outputHelper, quietKommander: true);
        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    /// <summary>Spaces with and without a group, with a key-space separator inside the name, and the CamusDB shapes.</summary>
    private static readonly string[] KeySpaces =
    [
        "t:r",
        "t:r|i:pk",
        "t:r|i:by_name",
        "orders",
        "orders|rows",
        "a/b",
        "a/b|c",
        "1:1:r",
        "1:2:r",
        "1:3:r",
        "68b9a4c2e1f0a3d4b5c6d7e8:68b9a4c2e1f0a3d4b5c6d7e9:r",
        "68b9a4c2e1f0a3d4b5c6d7e8:68b9a4c2e1f0a3d4b5c6d7e9:r|i:~pk",
        "ünïcödé",
        "|leading"
    ];

    private static RangeDescriptor Head(string keySpace, int partitionId, string? endKey = null) =>
        new() { KeySpace = keySpace, StartKey = null, EndKey = endKey, PartitionId = partitionId, Generation = 1 };

    /// <summary>
    /// With no sibling in the map, the seed is exactly the partition hash routing gives a key of the space, at every
    /// pool size a cluster is likely to run with, through every hash site.
    /// </summary>
    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    [InlineData(5)]
    [InlineData(8)]
    public void ANewGroup_SeedsOnItsHashPartition(int poolSize)
    {
        DataPartitionRouter router = new(poolSize);

        foreach (string keySpace in KeySpaces)
        {
            int seed = KeyRangeSeedPlacement.SeedPartitionFor(keySpace, [], poolSize);

            Assert.InRange(seed, RangeMapStore.FirstDataPartitionId, poolSize);
            Assert.Equal(router.Locate($"{keySpace}/0001"), seed);
            Assert.Equal(router.LocateKeySpace(keySpace), seed);
            Assert.Equal(PartitionDataEnumerator.HashPartitionOfKeySpace(keySpace, poolSize), seed);
        }

        // Spaces of one group seed together even when neither is in the map yet: the bucket is a function of the group.
        Assert.Equal(
            KeyRangeSeedPlacement.SeedPartitionFor("t:r", [], poolSize),
            KeyRangeSeedPlacement.SeedPartitionFor("t:r|i:pk", [], poolSize));
    }

    /// <summary>The data-partition floor of the range map is the first partition of the hash pool.</summary>
    [Fact]
    public void TheRangeMapFloor_IsTheFirstHashPartition() =>
        Assert.Equal(DataPartitionRouter.FirstUserPartitionId, RangeMapStore.FirstDataPartitionId);

    /// <summary>
    /// The sibling named exactly like the group (the row space of a rows-plus-indexes layout) anchors the group,
    /// whatever partition the hash bucket or another sibling would give.
    /// </summary>
    [Fact]
    public void ANewSpace_FollowsTheSiblingNamedLikeItsGroup()
    {
        RangeDescriptor[] map = [Head("t:r|i:a", 2), Head("t:r", 7), Head("u:r", 9)];

        Assert.Equal(7, KeyRangeSeedPlacement.SeedPartitionFor("t:r|i:b", map, poolSize: 3));
        Assert.Equal(7, KeyRangeSeedPlacement.SeedPartitionFor("t:r|i:b", [.. Enumerable.Reverse(map)], poolSize: 3));
    }

    /// <summary>Without the group-named sibling, the lowest sibling by ordinal name anchors, in any map order.</summary>
    [Fact]
    public void ANewSpace_FollowsTheLowestSibling_WhenNoSiblingIsNamedLikeTheGroup()
    {
        RangeDescriptor[] map = [Head("t:r|i:b", 5), Head("t:r|i:a", 9), Head("t:r|i:c", 6)];

        Assert.Equal(9, KeyRangeSeedPlacement.SeedPartitionFor("t:r|i:d", map, poolSize: 3));
        Assert.Equal(9, KeyRangeSeedPlacement.SeedPartitionFor("t:r|i:d", [.. Enumerable.Reverse(map)], poolSize: 3));
    }

    /// <summary>A split sibling is represented by its head descriptor, the range whose start is -infinity.</summary>
    [Fact]
    public void ANewSpace_FollowsTheHeadOfASplitSibling()
    {
        RangeDescriptor[] map =
        [
            new() { KeySpace = "t:r", StartKey = "m", EndKey = null, PartitionId = 9, Generation = 2 },
            Head("t:r", 3, endKey: "m")
        ];

        Assert.Equal(3, KeyRangeSeedPlacement.SeedPartitionFor("t:r|i:pk", map, poolSize: 3));
    }

    /// <summary>The group-named space itself follows an index space that was seeded first.</summary>
    [Fact]
    public void AGroupNamedSpace_FollowsItsAlreadySeededIndexSpace()
    {
        RangeDescriptor[] map = [Head("t:r|i:pk", 4)];

        Assert.Equal(4, KeyRangeSeedPlacement.SeedPartitionFor("t:r", map, poolSize: 3));
    }

    /// <summary>A space of another group, a space that merely shares a prefix, and the space itself never anchor.</summary>
    [Fact]
    public void OtherGroups_AndTheSpaceItself_DoNotAnchorTheSeed()
    {
        RangeDescriptor[] map = [Head("u:r", 9), Head("t:rx", 8), Head("t:r|i:pk", 7)];

        Assert.False(KeyRangeSeedPlacement.TryFindGroupAnchor("t:rx|i:pk", [Head("t:r", 9), Head("u:r", 8)], out _));
        Assert.False(KeyRangeSeedPlacement.TryFindGroupAnchor("t:r", [Head("t:r", 9)], out _));

        int seed = KeyRangeSeedPlacement.SeedPartitionFor("v:r", map, poolSize: 3);
        Assert.Equal(DataPartitionRouter.HashPartitionOfKeySpace("v:r", 3), seed);
        Assert.InRange(seed, 1, 3);
    }

    /// <summary>
    /// On a live cluster with the pool of the CamusDB evidence (three partitions): every registered group's
    /// descriptor sits on the partition hash routing gives a never-registered sibling of that group, on every node.
    /// </summary>
    [Fact]
    public async Task OnACluster_ANewGroup_SeedsWhereHashRoutingPlacesIt()
    {
        const int partitions = 3;
        (IRaft r1, IRaft r2, IRaft r3, IKahuna k1, IKahuna k2, IKahuna k3) =
            await AssembleThreNodeCluster("memory", partitions, raftLogger, kahunaLogger);

        try
        {
            CancellationToken ct = TestContext.Current.CancellationToken;
            KahunaManager[] nodes = [(KahunaManager)k1, (KahunaManager)k2, (KahunaManager)k3];
            string run = Guid.NewGuid().ToString("N")[..8];

            string[] groups = [$"{run}:1:r", $"{run}:2:r", $"{run}:3:r", $"{run}:4:r", $"{run}:5:r", $"{run}:6:r"];

            for (int i = 0; i < groups.Length; i++)
            {
                string space = $"{groups[i]}|r";
                Assert.True((await nodes[i % nodes.Length].RegisterKeyRangeWithOutcomeAsync(space, ct)).Success);
            }

            foreach (KahunaManager node in nodes)
            {
                foreach (string group in groups)
                {
                    string space = $"{group}|r";
                    await WaitUntilAsync(() => HasOneDescriptor(node, space));

                    int seed = node.GetRangeMap(space).KeySpaces[0].Descriptors[0].PartitionId;
                    int hashed = node.KeyValues.LocateDurablePartition($"{group}|never-registered/x").PartitionId;

                    Assert.Equal(hashed, seed);
                    Assert.Equal(DataPartitionRouter.HashPartitionOfKeySpace(space, partitions), seed);
                    Assert.Equal(seed, node.KeyValues.LocateDurablePartition($"{space}/x").PartitionId);
                }
            }
        }
        finally
        {
            await LeaveCluster(r1, r2, r3);
        }
    }

    /// <summary>
    /// A row space seeded away from its hash bucket, as an earlier rule or an older binary would have left it: the
    /// index space registered afterwards, through a forwarding follower, seeds with the rows and not on the bucket.
    /// After the meta leader moves to another node, a second index space still seeds with the rows.
    /// </summary>
    [Fact]
    public async Task OnACluster_ANewIndexSpace_FollowsItsRowSpace_AcrossAMetaLeaderChange()
    {
        const int partitions = 4;
        (IRaft r1, IRaft r2, IRaft r3, IKahuna k1, IKahuna k2, IKahuna k3) =
            await AssembleThreNodeCluster("memory", partitions, raftLogger, kahunaLogger);

        try
        {
            CancellationToken ct = TestContext.Current.CancellationToken;
            (IRaft Raft, KahunaManager Kahuna)[] nodes =
                [(r1, (KahunaManager)k1), (r2, (KahunaManager)k2), (r3, (KahunaManager)k3)];

            string rows = $"seed-{Guid.NewGuid().ToString("N")[..8]}:r";
            string firstIndex = $"{rows}|i:pk";
            string secondIndex = $"{rows}|i:name";

            int bucket = DataPartitionRouter.HashPartitionOfKeySpace(rows, partitions);
            int elsewhere = bucket == RangeMapStore.FirstDataPartitionId ? bucket + 1 : RangeMapStore.FirstDataPartitionId;

            int leader = await IndexOfMetaLeaderAsync(nodes, ct);

            // Seed the rows directly, where the bucket would not have put them.
            Assert.True(await nodes[leader].Kahuna.RangeMapStore.MutateAsync(current =>
            {
                RangeDescriptor[] next = new RangeDescriptor[current.Count + 1];
                for (int i = 0; i < current.Count; i++)
                    next[i] = current[i];
                next[current.Count] = Head(rows, elsewhere);
                return next;
            }, ct));

            foreach ((IRaft _, KahunaManager node) in nodes)
                await WaitUntilAsync(() => HasOneDescriptor(node, rows));

            // The first index space is registered on a follower, so the seed is forwarded to the meta leader.
            int follower = (leader + 1) % nodes.Length;
            Assert.True((await nodes[follower].Kahuna.RegisterKeyRangeWithOutcomeAsync(firstIndex, ct)).Success);

            foreach ((IRaft _, KahunaManager node) in nodes)
            {
                await WaitUntilAsync(() => HasOneDescriptor(node, firstIndex));
                Assert.Equal(elsewhere, node.GetRangeMap(firstIndex).KeySpaces[0].Descriptors[0].PartitionId);
                Assert.NotEqual(bucket, node.GetRangeMap(firstIndex).KeySpaces[0].Descriptors[0].PartitionId);
            }

            // Move meta leadership to a node that has not led so far, then register from the third node.
            int newLeader = (leader + 2) % nodes.Length;
            await nodes[newLeader].Raft.ForceLeaderForTestingAsync(RangeMapStore.MetaPartitionId, ct);
            await WaitUntilAsync(async () => await nodes[newLeader].Raft.AmILeader(RangeMapStore.MetaPartitionId, ct));

            Assert.True((await nodes[follower].Kahuna.RegisterKeyRangeWithOutcomeAsync(secondIndex, ct)).Success);

            foreach ((IRaft _, KahunaManager node) in nodes)
            {
                await WaitUntilAsync(() => HasOneDescriptor(node, secondIndex));
                Assert.Equal(elsewhere, node.GetRangeMap(secondIndex).KeySpaces[0].Descriptors[0].PartitionId);

                // Every space of the group routes to one partition, so a rows-plus-index write is single-partition.
                Assert.Equal(elsewhere, node.KeyValues.LocateDurablePartition($"{rows}/1").PartitionId);
                Assert.Equal(elsewhere, node.KeyValues.LocateDurablePartition($"{firstIndex}/1").PartitionId);
                Assert.Equal(elsewhere, node.KeyValues.LocateDurablePartition($"{secondIndex}/1").PartitionId);
            }
        }
        finally
        {
            await LeaveCluster(r1, r2, r3);
        }
    }

    private static async Task<int> IndexOfMetaLeaderAsync((IRaft Raft, KahunaManager Kahuna)[] nodes, CancellationToken ct)
    {
        while (true)
        {
            for (int i = 0; i < nodes.Length; i++)
                if (await nodes[i].Raft.AmILeader(RangeMapStore.MetaPartitionId, ct))
                    return i;

            await Task.Delay(25, ct);
        }
    }

    private static bool HasOneDescriptor(KahunaManager node, string keySpace)
    {
        KahunaRangeMapResponse map = node.GetRangeMap(keySpace);
        return map.KeySpaces.Count == 1 && map.KeySpaces[0].Descriptors.Count == 1;
    }
}
