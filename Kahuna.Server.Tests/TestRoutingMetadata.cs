using Google.Protobuf;
using Grpc.Core;
using Kahuna.Client.Routing;
using Kahuna.Communication.External.Grpc;
using Kahuna.Server.Configuration;
using Kahuna.Shared.Communication.Rest;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Routing;
using Kommander;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kahuna.Server.Tests;

/// <summary>
/// Routing metadata: what a node publishes about how it routes resources, and whether a client
/// reading it resolves the same resources to the same partitions the server does.
///
/// <para>
/// The client's answers are checked against the server's own — the partition each response reports
/// for the very same resource — rather than against a re-derivation of the rule. A test that
/// re-implemented the hash would agree with itself while both drifted from the cluster.
/// </para>
/// </summary>
public sealed class TestRoutingMetadata : BaseCluster
{
    private const int Partitions = 4;

    private static void AdvertiseOverHttp(KahunaConfiguration configuration) =>
        configuration.AdvertisedClientScheme = "http://";

    private static ServerCallContext Context() => new StubServerCallContext();

    /// <summary>
    /// Keys chosen to exercise the parts of the rule that are easy to get wrong: no separator at
    /// all, an empty prefix, several separators, non-ASCII text (the hash runs over UTF-8 bytes),
    /// and a long key.
    /// </summary>
    private static readonly string[] GoldenKeys =
    [
        "no-separator",
        "/leading",
        "a/b/c/d",
        "orders/1",
        "orders/2",
        "orders/000000000000000000000000000000000000001",
        "ünïcödé/ключ",
        "emoji/\U0001F600",
        "spaces in key/value",
        "UPPER/lower"
    ];

    [Fact]
    public async Task ThePublishedMap_DescribesHowTheNodeRoutes()
    {
        (IRaft raft1, IRaft raft2, IRaft raft3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster("memory", Partitions, NullLogger<IRaft>.Instance, NullLogger<IKahuna>.Instance, AdvertiseOverHttp);

        try
        {
            KahunaRoutingMetadataResponse metadata = await kahuna1.GetRoutingMetadata();

            Assert.True(metadata.Initialized);
            Assert.True(metadata.Coherent);
            Assert.Equal(1, metadata.SchemaVersion);
            Assert.Equal(Partitions, metadata.HashPoolSize);
            Assert.Equal(1, metadata.HashPartitionOffset);
            Assert.Equal("/", metadata.PrefixSeparator);
            Assert.Equal("__kahuna:sequences:{0}", metadata.SequenceStorageKeyFormat);
            Assert.Equal("__kahuna:", metadata.ReservedKeyPrefix);
            Assert.Equal("http://" + raft1.GetLocalEndpoint(), metadata.LocalEndpoint);

            // Every partition a client could be routed to is listed, and a known leader names a URL
            // a client can dial rather than a Raft address.
            Assert.Equal(Partitions, metadata.Leaders.Count);

            foreach (KahunaPartitionLeaderResponse leader in metadata.Leaders)
            {
                Assert.InRange(leader.PartitionId, 1, Partitions);

                if (leader.Endpoint.Length > 0)
                    Assert.StartsWith("http://localhost:800", leader.Endpoint, StringComparison.Ordinal);
            }

            _ = kahuna2;
            _ = kahuna3;
        }
        finally
        {
            await LeaveCluster(raft1, raft2, raft3);
        }
    }

    /// <summary>
    /// A client resolving from published metadata must land on the same partition the server routes
    /// the resource to. The server's answer comes from the hint on a real
    /// operation, so the comparison is against the route that actually admitted the request.
    /// </summary>
    [Fact]
    public async Task AClientResolvesTheSamePartitionsTheServerDoes()
    {
        (IRaft raft1, IRaft raft2, IRaft raft3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster("memory", Partitions, NullLogger<IRaft>.Instance, NullLogger<IKahuna>.Instance, AdvertiseOverHttp);

        try
        {
            KahunaRoutingMetadataResponse metadata = await kahuna1.GetRoutingMetadata();

            RoutingMetadataSnapshot? snapshot = RoutingMetadataSnapshot.TryCreate(metadata, TimeSpan.FromMinutes(1), out string rejection);

            Assert.NotNull(snapshot);
            Assert.Equal("", rejection);

            KeyValuesService keyValues = new(kahuna1, NullLogger<IKahuna>.Instance);
            LocksService locks = new(kahuna1, new KahunaConfiguration(), raft1, NullLogger<IKahuna>.Instance);

            foreach (string key in GoldenKeys)
            {
                GrpcTrySetKeyValueResponse set = await keyValues.TrySetKeyValue(
                    new GrpcTrySetKeyValueRequest
                    {
                        Key = key,
                        Value = ByteString.CopyFromUtf8("v"),
                        ExpiresMs = 0,
                        Durability = GrpcKeyValueDurability.Persistent
                    },
                    Context());

                Assert.Equal(GrpcKeyValueResponseType.TypeSet, set.Type);
                Assert.NotNull(set.Route);

                Assert.True(snapshot!.TryResolvePartition(KahunaRoutingDomain.KeyValue, key, out int clientPartition));
                Assert.Equal(set.Route.PartitionId, clientPartition);

                GrpcTryLockResponse locked = await locks.TryLock(
                    new GrpcTryLockRequest
                    {
                        Resource = key,
                        Owner = ByteString.CopyFromUtf8("owner"),
                        ExpiresMs = 3000,
                        Durability = GrpcLockDurability.LockDurabilityEphemeral
                    },
                    Context());

                Assert.Equal(GrpcLockResponseType.LockResponseTypeLocked, locked.Type);
                Assert.NotNull(locked.Route);

                Assert.True(snapshot.TryResolvePartition(KahunaRoutingDomain.Lock, key, out int clientLockPartition));
                Assert.Equal(locked.Route.PartitionId, clientLockPartition);
            }

            _ = kahuna2;
            _ = kahuna3;
        }
        finally
        {
            await LeaveCluster(raft1, raft2, raft3);
        }
    }

    /// <summary>
    /// A sequence routes by the partition of its storage key, not by a hash of its bare name. A
    /// client that hashed the name would resolve almost every sequence to the wrong partition.
    /// </summary>
    [Fact]
    public async Task AClientResolvesASequenceThroughItsStorageKey()
    {
        (IRaft raft1, IRaft raft2, IRaft raft3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster("memory", Partitions, NullLogger<IRaft>.Instance, NullLogger<IKahuna>.Instance, AdvertiseOverHttp);

        try
        {
            KahunaRoutingMetadataResponse metadata = await kahuna1.GetRoutingMetadata();

            RoutingMetadataSnapshot? snapshot = RoutingMetadataSnapshot.TryCreate(metadata, TimeSpan.FromMinutes(1), out _);
            Assert.NotNull(snapshot);

            SequencesService sequences = new(kahuna1, NullLogger<IKahuna>.Instance);

            for (int i = 0; i < 6; i++)
            {
                string name = "seq-" + Guid.NewGuid().ToString("N")[..8];

                GrpcSequenceResponse created = await sequences.CreateSequence(
                    new GrpcCreateSequenceRequest { Name = name, InitialValue = 1, Increment = 1, Durability = GrpcSequenceDurability.SequencePersistent },
                    Context());

                Assert.Equal(GrpcSequenceResponseType.SequenceSuccess, created.Type);
                Assert.NotNull(created.Route);

                Assert.True(snapshot!.TryResolvePartition(KahunaRoutingDomain.Sequence, name, out int clientPartition));
                Assert.Equal(created.Route.PartitionId, clientPartition);
            }

            _ = kahuna2;
            _ = kahuna3;
        }
        finally
        {
            await LeaveCluster(raft1, raft2, raft3);
        }
    }

    /// <summary>
    /// A key space that routes by range resolves through its descriptor intervals, and a split moves
    /// only the half above the split key. A client that fell back to the hash rule for a ranged
    /// space would answer a different partition for every key in it.
    /// </summary>
    [Fact]
    public async Task AClientResolvesRangeBoundariesAcrossASplit()
    {
        (IRaft raft1, IRaft raft2, IRaft raft3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster("memory", Partitions, NullLogger<IRaft>.Instance, NullLogger<IKahuna>.Instance, AdvertiseOverHttp);

        try
        {
            const string keySpace = "ranged:r";

            Assert.True(await kahuna1.RegisterKeyRangeAsync(keySpace, TestContext.Current.CancellationToken));

            // The map mutation is leader-only on the meta partition, so the split is asked of the
            // node that leads it. Asking an arbitrary node would refuse and leave the boundary half
            // of this test unexercised without saying so.
            IKahuna metaLeader = kahuna1;

            foreach ((IRaft raft, IKahuna kahuna) in new[] { (raft1, kahuna1), (raft2, kahuna2), (raft3, kahuna3) })
            {
                if (await raft.AmILeader(0, TestContext.Current.CancellationToken))
                {
                    metaLeader = kahuna;
                    break;
                }
            }

            KeyValuesService keyValues = new(kahuna1, NullLogger<IKahuna>.Instance);

            string lower = keySpace + "/aaaa";
            string upper = keySpace + "/zzzz";

            foreach (string key in new[] { lower, upper })
            {
                GrpcTrySetKeyValueResponse set = await keyValues.TrySetKeyValue(
                    new GrpcTrySetKeyValueRequest { Key = key, Value = ByteString.CopyFromUtf8("v"), ExpiresMs = 0, Durability = GrpcKeyValueDurability.Persistent },
                    Context());

                Assert.Equal(GrpcKeyValueResponseType.TypeSet, set.Type);
            }

            // Before the split both keys share the whole-space descriptor.
            await AssertClientAgreesWithServer(kahuna1, keyValues, lower, upper, expectSame: true);

            KahunaSplitRangeResponse split = await metaLeader.SplitRangeAtKeyWithOutcomeAsync(
                keySpace, keySpace + "/m", TestContext.Current.CancellationToken);

            Assert.True(split.Success, split.Status + ": " + split.Reason);

            // The split commits on the meta-partition leader; every other node applies the new map
            // through replication. The agreement check below reads metadata and route hints from
            // this node, so both answers come from its applied map. Wait until this node has
            // applied the split, because a comparison over a half-replicated map is meaningless.
            await WaitUntilAsync(async () =>
            {
                KahunaRoutingMetadataResponse published = await kahuna1.GetRoutingMetadata(keySpace);

                return published.Coherent
                    && published.KeySpaces.Count == 1
                    && published.KeySpaces[0].Ranges.Count == 2;
            });

            await AssertClientAgreesWithServer(kahuna1, keyValues, lower, upper, expectSame: false);
        }
        finally
        {
            await LeaveCluster(raft1, raft2, raft3);
        }
    }

    /// <summary>
    /// Reads the published map, then checks that the client resolves both keys exactly where the
    /// server's own hints put them.
    /// </summary>
    private static async Task AssertClientAgreesWithServer(
        IKahuna kahuna, KeyValuesService keyValues, string lower, string upper, bool expectSame)
    {
        KahunaRoutingMetadataResponse metadata = await kahuna.GetRoutingMetadata();

        RoutingMetadataSnapshot? snapshot = RoutingMetadataSnapshot.TryCreate(metadata, TimeSpan.FromMinutes(1), out _);
        Assert.NotNull(snapshot);

        int serverLower = await ServerPartitionOf(keyValues, lower);
        int serverUpper = await ServerPartitionOf(keyValues, upper);

        Assert.True(snapshot!.TryResolvePartition(KahunaRoutingDomain.KeyValue, lower, out int clientLower));
        Assert.True(snapshot.TryResolvePartition(KahunaRoutingDomain.KeyValue, upper, out int clientUpper));

        Assert.Equal(serverLower, clientLower);
        Assert.Equal(serverUpper, clientUpper);

        if (expectSame)
            Assert.Equal(serverLower, serverUpper);
        else
            Assert.NotEqual(serverLower, serverUpper);
    }

    private static async Task<int> ServerPartitionOf(KeyValuesService keyValues, string key)
    {
        GrpcTryGetKeyValueResponse read = await keyValues.TryGetKeyValue(
            new GrpcTryGetKeyValueRequest { Key = key, Revision = -1, Durability = GrpcKeyValueDurability.Persistent },
            Context());

        Assert.NotNull(read.Route);

        return read.Route.PartitionId;
    }

    /// <summary>
    /// Everything a client cannot implement exactly must be refused rather than approximated: a
    /// near-match resolves most keys correctly and a few silently to the wrong partition, which
    /// reads as unexplained forwarding instead of as the version mismatch it is.
    /// </summary>
    [Theory]
    [InlineData("schema")]
    [InlineData("hash")]
    [InlineData("separator")]
    [InlineData("incoherent")]
    [InlineData("uninitialized")]
    [InlineData("sequence-key")]
    public void AnUnusableMap_IsRefusedRatherThanApproximated(string defect)
    {
        KahunaRoutingMetadataResponse metadata = new()
        {
            Initialized = true,
            Coherent = true,
            SchemaVersion = 1,
            HashAlgorithm = "kommander.inverse-prefixed-jump-xxh32-v1",
            PrefixSeparator = "/",
            HashPoolSize = 4,
            HashPartitionOffset = 1,
            SequenceStorageKeyFormat = "__kahuna:sequences:{0}",
            ReservedKeyPrefix = "__kahuna:",
            LocalEndpoint = "http://localhost:8001"
        };

        switch (defect)
        {
            case "schema": metadata.SchemaVersion = 2; break;
            case "hash": metadata.HashAlgorithm = "kommander.inverse-prefixed-jump-xxh32-v2"; break;
            case "separator": metadata.PrefixSeparator = ":"; break;
            case "incoherent": metadata.Coherent = false; break;
            case "uninitialized": metadata.Initialized = false; break;
            case "sequence-key": metadata.SequenceStorageKeyFormat = "seq:{0}:record"; break;
        }

        RoutingMetadataSnapshot? snapshot = RoutingMetadataSnapshot.TryCreate(metadata, TimeSpan.FromMinutes(1), out string rejection);

        Assert.Null(snapshot);
        Assert.NotEqual("", rejection);
    }

    /// <summary>
    /// The point of metadata mode: a resource the client has never touched goes straight to its
    /// owner, without a discovery round trip on the operation's own path.
    /// </summary>
    [Fact]
    public async Task AfterBootstrap_APreviouslyUnseenKeyRoutesDirectly()
    {
        (IRaft raft1, IRaft raft2, IRaft raft3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster("memory", Partitions, NullLogger<IRaft>.Instance, NullLogger<IKahuna>.Instance, AdvertiseOverHttp);

        try
        {
            string[] cluster =
            [
                "http://" + raft1.GetLocalEndpoint(),
                "http://" + raft2.GetLocalEndpoint(),
                "http://" + raft3.GetLocalEndpoint()
            ];

            ClientRouteResolver resolver = new(
                KahunaRoutingMode.Metadata,
                new RouteCache(1024, TimeSpan.FromMinutes(1)),
                new RoutingEndpointPolicy(cluster, null, allowUnlisted: false),
                new DirectMetadataTransport(kahuna1),
                () => cluster[0],
                TimeSpan.FromMinutes(1),
                TimeSpan.FromSeconds(5),
                NullLogger.Instance);

            // Nothing learned and no map yet: the operation goes out on rotation.
            Assert.Null(resolver.Select(KahunaRoutingDomain.KeyValue, "unseen/1"));

            Assert.True(await resolver.RefreshMetadataAsync(TestContext.Current.CancellationToken));

            KeyValuesService keyValues = new(kahuna1, NullLogger<IKahuna>.Instance);

            int direct = 0;
            const int probes = 20;

            for (int i = 0; i < probes; i++)
            {
                string key = "unseen/" + Guid.NewGuid().ToString("N")[..8];

                string? chosen = resolver.Select(KahunaRoutingDomain.KeyValue, key);

                if (chosen is null)
                    continue;

                // The server's own hint says where the key really lives; the client picked it
                // without ever having touched the key.
                GrpcTryGetKeyValueResponse read = await keyValues.TryGetKeyValue(
                    new GrpcTryGetKeyValueRequest { Key = key, Revision = -1, Durability = GrpcKeyValueDurability.Persistent },
                    Context());

                Assert.NotNull(read.Route);
                Assert.Equal(read.Route.Endpoint, chosen);

                direct++;
            }

            // Every partition has a known leader in a settled three-node cluster, so every probe
            // should resolve. The bound leaves room for a partition whose election is still running.
            Assert.True(direct >= probes - 2, $"direct={direct}");
        }
        finally
        {
            await LeaveCluster(raft1, raft2, raft3);
        }
    }

    /// <summary>Reads metadata straight off a node, standing in for the transport in these tests.</summary>
    private sealed class DirectMetadataTransport(IKahuna kahuna) : IKahunaRoutingTransport
    {
        public Task<KahunaRoutingMetadataResponse> GetRoutingMetadata(string url, string? keySpace, CancellationToken cancellationToken) =>
            kahuna.GetRoutingMetadata(keySpace);
    }

    /// <summary>Minimal context: the services read only the cancellation token.</summary>
    private sealed class StubServerCallContext : ServerCallContext
    {
        protected override CancellationToken CancellationTokenCore => CancellationToken.None;
        protected override string MethodCore => "test";
        protected override string HostCore => "test";
        protected override string PeerCore => "test";
        protected override DateTime DeadlineCore => DateTime.MaxValue;
        protected override Metadata RequestHeadersCore => new();
        protected override Metadata ResponseTrailersCore => new();
        protected override Status StatusCore { get; set; }
        protected override WriteOptions? WriteOptionsCore { get; set; }
        protected override AuthContext AuthContextCore => throw new NotSupportedException();
        protected override ContextPropagationToken CreatePropagationTokenCore(ContextPropagationOptions? options) => throw new NotSupportedException();
        protected override Task WriteResponseHeadersAsyncCore(Metadata responseHeaders) => throw new NotSupportedException();
    }
}
