using Kahuna.Client.Routing;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Shared.Communication.Rest;
using Kahuna.Shared.Routing;
using Kommander;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// The hash-placement rule (<see cref="HashPlacement"/>): a key's key space is the prefix before its last
/// <c>'/'</c>; the key space's placement group is the prefix before its first <c>'|'</c>; hash routing places the
/// group. Key spaces that name one group land on one partition — which is what lets a consumer keep a table's
/// rows and its index entries together, so a primary-key update reads and writes on a single partition and can
/// take the one-phase commit. A key space with no group separator must place exactly as it did before the rule
/// existed: existing data must not move.
/// </summary>
public sealed class TestHashPlacement : BaseCluster
{
    private readonly ILogger<IRaft> raftLogger;
    private readonly ILogger<IKahuna> kahunaLogger;

    public TestHashPlacement(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = TestLogFactory.Create(outputHelper, quietKommander: true);
        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    /// <summary>Keys without a group separator, chosen to exercise the key-space rule's edges.</summary>
    private static readonly string[] UngroupedKeys =
    [
        "no-separator",
        "/leading",
        "a/b/c/d",
        "orders/1",
        "orders/000000000000000000000000000000000000001",
        "mydb/meta/orders",
        "ünïcödé/ключ",
        "emoji/\U0001F600",
        "spaces in key/value",
        "UPPER/lower",
        "68b9a4c2e1f0a3d4b5c6d7e8:68b9a4c2e1f0a3d4b5c6d7e9:r/68b9a4c2e1f0a3d4b5c6d7ea",
        "68b9a4c2e1f0a3d4b5c6d7e8:68b9a4c2e1f0a3d4b5c6d7e9:i:~pk/abc"
    ];

    [Theory]
    [InlineData("t:r/0001", "t:r")]
    [InlineData("t:r|i:pk/abc", "t:r|i:pk")]
    [InlineData("a/b/c/d", "a/b/c")]
    [InlineData("no-separator", "no-separator")]
    [InlineData("/leading", "")]
    [InlineData("users/a|b", "users")]
    public void KeySpace_IsThePrefixBeforeTheLastSlash(string key, string expected) =>
        Assert.Equal(expected, HashPlacement.KeySpaceOf(key).ToString());

    [Theory]
    [InlineData("t:r", "t:r")]
    [InlineData("t:r|i:pk", "t:r")]
    [InlineData("orders|rows", "orders")]
    [InlineData("a|b|c", "a")]
    [InlineData("|leading", "")]
    [InlineData("", "")]
    public void PlacementGroup_IsThePrefixBeforeTheFirstBar(string keySpace, string expected)
    {
        Assert.Equal(expected, HashPlacement.GroupOf(keySpace.AsSpan()).ToString());
        Assert.Equal(expected, HashPlacement.GroupOf(keySpace));
    }

    /// <summary>A key space that names no group is returned as the same instance: no copy on the common path.</summary>
    [Fact]
    public void PlacementGroup_OfAnUngroupedKeySpace_IsTheSameString()
    {
        const string keySpace = "orders";
        Assert.Same(keySpace, HashPlacement.GroupOf(keySpace));
    }

    /// <summary>
    /// The placement of a key without a group separator is unchanged from the pre-rule function
    /// (Kommander's <c>InversePrefixedHash</c> over the key space), for every pool size a cluster is likely to
    /// run with. This is the guarantee that existing data does not move on upgrade.
    /// </summary>
    [Fact]
    public void UngroupedKeys_PlaceExactlyAsBefore()
    {
        for (int pool = 1; pool <= 16; pool++)
        {
            foreach (string key in UngroupedKeys)
            {
                Assert.Equal(HashUtils.InversePrefixedHash(key, '/', pool), HashPlacement.BucketOfKey(key, pool));

                string keySpace = KeySpaceRegistry.ExtractKeySpace(key);
                Assert.Equal(HashUtils.ConsistentHash(keySpace, pool), HashPlacement.BucketOfKeySpace(keySpace, pool));
            }
        }
    }

    /// <summary>Key spaces that name one group share a bucket at every pool size, and the bucket is the group's own.</summary>
    [Fact]
    public void KeySpaces_ThatShareAGroup_ShareABucket()
    {
        for (int pool = 1; pool <= 16; pool++)
        {
            int rows = HashPlacement.BucketOfKey("t:r/0001", pool);

            Assert.Equal(rows, HashPlacement.BucketOfKey("t:r|i:pk/abc", pool));
            Assert.Equal(rows, HashPlacement.BucketOfKey("t:r|i:by_name/\U0001F600", pool));
            Assert.Equal(rows, HashPlacement.BucketOfKey("t:r|/no-space-suffix", pool));
            Assert.Equal(rows, HashPlacement.BucketOfKeySpace("t:r|i:pk", pool));
            Assert.Equal(HashUtils.ConsistentHash("t:r", pool), rows);

            // The separator counts only inside the key space; after the last '/' it is part of the key.
            Assert.Equal(HashPlacement.BucketOfKey("users/1", pool), HashPlacement.BucketOfKey("users/a|b", pool));
        }
    }

    /// <summary>
    /// The shape that made the one-phase commit a coin flip: a table's row space and index space, both named
    /// from ids minted per run, hash apart for most ids. Naming the index space in the row space's group makes
    /// them hash together for every id.
    /// </summary>
    [Fact]
    public void AnIndexSpaceInItsTableGroup_AlwaysPlacesWithTheRows()
    {
        const int pool = 3;
        int apartWithoutGroup = 0;

        for (int i = 0; i < 200; i++)
        {
            string dbId = Guid.NewGuid().ToString("N")[..24];
            string tableId = Guid.NewGuid().ToString("N")[..24];
            string rowSpace = $"{dbId}:{tableId}:r";

            if (HashPlacement.BucketOfKeySpace(rowSpace, pool) != HashPlacement.BucketOfKeySpace($"{dbId}:{tableId}:i:~pk", pool))
                apartWithoutGroup++;

            Assert.Equal(
                HashPlacement.BucketOfKeySpace(rowSpace, pool),
                HashPlacement.BucketOfKeySpace($"{rowSpace}|i:~pk", pool));
        }

        // Two out of three independent draws land apart on a three-partition pool; the point is only that
        // the ungrouped naming is a draw at all, so a loose bound keeps the test free of hash-value luck.
        Assert.InRange(apartWithoutGroup, 60, 180);
    }

    /// <summary>Every server-side hash site and the client's local resolver answer through the same rule.</summary>
    [Fact]
    public void EveryHashSite_AgreesOnAGroupedKey()
    {
        const int pool = 4;
        const string key = "t:r|i:pk/abc";

        int enumerator = PartitionDataEnumerator.HashPartitionOfKeySpace("t:r|i:pk", pool);
        Assert.Equal(1 + HashPlacement.BucketOfKey(key, pool), enumerator);
        Assert.Equal(PartitionDataEnumerator.HashPartitionOfKeySpace("t:r", pool), enumerator);

        KahunaRoutingMetadataResponse metadata = new()
        {
            Initialized = true,
            Coherent = true,
            SchemaVersion = 1,
            HashAlgorithm = HashPlacement.AlgorithmIdentifier,
            PrefixSeparator = "/",
            GroupSeparator = "|",
            HashPoolSize = pool,
            HashPartitionOffset = 1,
            SequenceStorageKeyFormat = "__kahuna:sequences:{0}",
            ReservedKeyPrefix = "__kahuna:",
            LocalEndpoint = "http://localhost:8001"
        };

        RoutingMetadataSnapshot? snapshot = RoutingMetadataSnapshot.TryCreate(metadata, TimeSpan.FromMinutes(1), out string rejection);
        Assert.NotNull(snapshot);
        Assert.Equal("", rejection);
        Assert.True(snapshot!.TryResolvePartition(KahunaRoutingDomain.KeyValue, key, out int client));
        Assert.Equal(enumerator, client);
    }

    /// <summary>
    /// On a live cluster: the request router places a grouped key with its group (hash mode), and registering
    /// two key-range spaces of one group seeds both descriptors on one partition, so ranged rows and ranged
    /// index entries also start together.
    /// </summary>
    [Fact]
    public async Task OnACluster_GroupedSpaces_RouteAndSeedTogether()
    {
        (IRaft r1, IRaft r2, IRaft r3, IKahuna k1, IKahuna k2, IKahuna k3) =
            await AssembleThreNodeCluster("memory", 3, raftLogger, kahunaLogger);

        try
        {
            CancellationToken ct = TestContext.Current.CancellationToken;
            KahunaManager[] nodes = [(KahunaManager)k1, (KahunaManager)k2, (KahunaManager)k3];
            string table = "placement-" + Guid.NewGuid().ToString("N")[..8];

            foreach (KahunaManager node in nodes)
            {
                int rows = node.KeyValues.LocateDurablePartition($"{table}:r/1").PartitionId;
                Assert.Equal(rows, node.KeyValues.LocateDurablePartition($"{table}:r|i:pk/abc").PartitionId);
                Assert.Equal(rows, node.KeyValues.LocateDurablePartition($"{table}:r|i:name/zzz").PartitionId);
                Assert.Equal(rows, node.KeyValues.LocateDurablePartition($"{table}:r").PartitionId);
            }

            string rangedRows = $"{table}:ranged|r";
            string rangedIndex = $"{table}:ranged|i:pk";

            Assert.True((await nodes[0].RegisterKeyRangeWithOutcomeAsync(rangedRows, ct)).Success);
            Assert.True((await nodes[1].RegisterKeyRangeWithOutcomeAsync(rangedIndex, ct)).Success);

            foreach (KahunaManager node in nodes)
            {
                // The mode flip is node-local; the descriptor arrives by replication on the nodes that did not seed.
                await WaitUntilAsync(() => HasOneDescriptor(node, rangedRows) && HasOneDescriptor(node, rangedIndex));

                int rowsPartition = node.GetRangeMap(rangedRows).KeySpaces[0].Descriptors[0].PartitionId;
                Assert.Equal(rowsPartition, node.GetRangeMap(rangedIndex).KeySpaces[0].Descriptors[0].PartitionId);
            }
        }
        finally
        {
            await LeaveCluster(r1, r2, r3);
        }
    }

    private static bool HasOneDescriptor(KahunaManager node, string keySpace)
    {
        KahunaRangeMapResponse map = node.GetRangeMap(keySpace);
        return map.KeySpaces.Count == 1 && map.KeySpaces[0].Descriptors.Count == 1;
    }
}
