using System.Net.Http.Json;
using Google.Protobuf;
using Grpc.Core;
using Grpc.Net.Client;
using Kahuna.Communication.External.Grpc;
using Kahuna.Server.Communication;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.Communication.Grpc;
using Kahuna.Shared.Communication.Rest;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kahuna.Server.Tests;

/// <summary>
/// Bounds on what one client request can make a node hold: the page a range read materializes, and the snapshot
/// holds that pin MVCC reclamation. Each drives the entry point a client reaches, so a bound applied below it,
/// or only on one path, does not pass for one applied at the door.
/// </summary>
public sealed class TestClientInputBounds
{
    private const string Space = "t:bounds";
    private const string Prefix = Space + "/";

    private readonly ILoggerFactory loggerFactory;

    public TestClientInputBounds(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    private async Task<EmbeddedKahunaNode> StartNode(CancellationToken ct, Action<EmbeddedKahunaOptions>? configure = null)
    {
        EmbeddedKahunaOptions options = new()
        {
            TimerInitialDelay = TimeSpan.FromMilliseconds(50),
            ReadIOThreads = 1,
            WriteIOThreads = 1,
            PartitionExecutorPoolSize = 1,
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 4
        };

        configure?.Invoke(options);

        EmbeddedKahunaNode node = new(options, loggerFactory);

        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync(Prefix + "0", ct);

        return node;
    }

    private static GrpcGetByRangeRequest RangeRequest(int limit) => new()
    {
        Prefix = Prefix,
        Limit = limit,
        Durability = GrpcKeyValueDurability.Ephemeral
    };

    /// <summary>
    /// A client that asks the paginated range read for an int.MaxValue page gets one server-sized page and a
    /// cursor, never the whole range; zero asks for the server's page size; a negative limit names no page and
    /// is refused. The gRPC unary call, its stream and the REST handler share the same bound.
    /// </summary>
    [Fact]
    public async Task RangeRead_CutsTheClientPageToTheServerMaximum()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(ct);

        int keys = KeyValueScanLimits.MaxRangePageSize + 5;
        for (int i = 0; i < keys; i++)
        {
            (KeyValueResponseType type, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, Prefix + i.ToString("D6"), [1], null, -1, KeyValueFlags.Set, 0,
                KeyValueDurability.Ephemeral, ct);

            Assert.Equal(KeyValueResponseType.Set, type);
        }

        KeyValuesService service = new(node.Kahuna, NodeTransportGate.Disabled, NullLogger<IKahuna>.Instance);

        GrpcGetByRangeResponse unbounded = await service.GetByRange(RangeRequest(int.MaxValue), new StubServerCallContext(ct));
        Assert.Equal(GrpcKeyValueResponseType.TypeGot, unbounded.Type);
        Assert.Equal(KeyValueScanLimits.MaxRangePageSize, unbounded.Items.Count);
        Assert.True(unbounded.HasMore);
        Assert.False(string.IsNullOrEmpty(unbounded.NextCursor));

        GrpcGetByRangeResponse serverSized = await service.GetByRange(RangeRequest(0), new StubServerCallContext(ct));
        Assert.Equal(GrpcKeyValueResponseType.TypeGot, serverSized.Type);
        Assert.Equal(KeyValueScanLimits.MaxRangePageSize, serverSized.Items.Count);
        Assert.True(serverSized.HasMore);

        GrpcGetByRangeResponse small = await service.GetByRange(RangeRequest(10), new StubServerCallContext(ct));
        Assert.Equal(10, small.Items.Count);

        GrpcGetByRangeResponse negative = await service.GetByRange(RangeRequest(-1), new StubServerCallContext(ct));
        Assert.Equal(GrpcKeyValueResponseType.TypeInvalidInput, negative.Type);
        Assert.Empty(negative.Items);

        Assert.True(KeyValueScanLimits.TryBoundRangePageLimit(int.MaxValue, out int bounded));
        Assert.Equal(KeyValueScanLimits.MaxRangePageSize, bounded);
        Assert.False(KeyValueScanLimits.TryBoundRangePageLimit(-1, out _));
    }

    /// <summary>
    /// Snapshot holds carry the bounds history holds already have: a lease above the maximum is refused on both
    /// acquire and renew, and a fresh hold past the per-holder or the registry bound is refused with nothing
    /// registered. A repeat of a registered hold adds nothing, so a bound never refuses it.
    /// </summary>
    [Fact]
    public async Task SnapshotHolds_RefuseALongLeaseAndAHoldPastTheirBounds()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(ct, o =>
        {
            o.SnapshotHoldMaxLeaseMs = 10_000;
            o.SnapshotHoldMaxPerHolder = 2;
            o.SnapshotHoldMaxTotal = 3;
        });

        IKahuna kahuna = node.Kahuna;

        Assert.Equal(KeyValueResponseType.InvalidInput, (await Acquire(kahuna, "a", 1, int.MaxValue, ct)).Type);

        (KeyValueResponseType firstType, string firstId) = await Acquire(kahuna, "a", 1, 5_000, ct);
        Assert.Equal(KeyValueResponseType.Set, firstType);
        Assert.Equal(KeyValueResponseType.Set, (await Acquire(kahuna, "a", 2, 5_000, ct)).Type);

        // The holder is at its bound: a third timestamp is refused, the repeat of a registered one is not.
        Assert.Equal(KeyValueResponseType.SnapshotHoldLimitReached, (await Acquire(kahuna, "a", 3, 5_000, ct)).Type);
        (KeyValueResponseType repeatType, string repeatId) = await Acquire(kahuna, "a", 1, 5_000, ct);
        Assert.Equal(KeyValueResponseType.Set, repeatType);
        Assert.Equal(firstId, repeatId);

        // The registry is at its bound after one more holder's hold.
        Assert.Equal(KeyValueResponseType.Set, (await Acquire(kahuna, "b", 1, 5_000, ct)).Type);
        Assert.Equal(KeyValueResponseType.SnapshotHoldLimitReached, (await Acquire(kahuna, "c", 1, 5_000, ct)).Type);

        (KeyValueResponseType renewTooLong, _) = await kahuna.LocateAndRenewSnapshotHold(firstId, int.MaxValue, ct);
        Assert.Equal(KeyValueResponseType.InvalidInput, renewTooLong);

        (KeyValueResponseType renewed, _) = await kahuna.LocateAndRenewSnapshotHold(firstId, 10_000, ct);
        Assert.Equal(KeyValueResponseType.Set, renewed);

        // A release frees a slot of the registry.
        Assert.Equal(KeyValueResponseType.Deleted, await kahuna.LocateAndReleaseSnapshotHold(firstId, ct));
        Assert.Equal(KeyValueResponseType.Set, (await Acquire(kahuna, "c", 1, 5_000, ct)).Type);
    }

    /// <summary>
    /// A client write or lock acquire past the key or value bound is refused as invalid input before it reaches the
    /// store, and a refused batch writes none of its items. A multi-byte key is measured in UTF-8 bytes, not chars.
    /// The inter-node path is not bound: a forwarded write that a client entry point admitted on another node, under
    /// that node's bound, must still apply here.
    /// </summary>
    [Fact]
    public async Task Writes_RefuseAKeyOrValuePastTheirBounds_AtTheClientEntryOnly()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(ct);

        KahunaConfiguration configuration = new() { MaxKeyBytes = 24, MaxValueBytes = 8 };
        KeyValuesService service = new(node.Kahuna, NodeTransportGate.Disabled, NullLogger<IKahuna>.Instance, configuration);

        GrpcTrySetKeyValueRequest Set(string key, int valueBytes, int compareBytes = -1)
        {
            GrpcTrySetKeyValueRequest request = new()
            {
                Key = key,
                Value = ByteString.CopyFrom(new byte[valueBytes]),
                Flags = GrpcKeyValueFlags.Set,
                Durability = GrpcKeyValueDurability.Ephemeral
            };

            if (compareBytes >= 0)
                request.CompareValue = ByteString.CopyFrom(new byte[compareBytes]);

            return request;
        }

        // Fits exactly: a 24-byte key (the 9-byte prefix and 15 more) and an 8-byte value.
        Assert.Equal(GrpcKeyValueResponseType.TypeSet, (await service.TrySetKeyValue(Set(Prefix + "k00000000000000", 8), new StubServerCallContext(ct))).Type);

        Assert.Equal(GrpcKeyValueResponseType.TypeInvalidInput, (await service.TrySetKeyValue(Set(Prefix + "k000000000000000", 1), new StubServerCallContext(ct))).Type);
        Assert.Equal(GrpcKeyValueResponseType.TypeInvalidInput, (await service.TrySetKeyValue(Set(Prefix + "v", 9), new StubServerCallContext(ct))).Type);
        Assert.Equal(GrpcKeyValueResponseType.TypeInvalidInput, (await service.TrySetKeyValue(Set(Prefix + "c", 1, compareBytes: 9), new StubServerCallContext(ct))).Type);

        // 'é' is two UTF-8 bytes, so both keys are under 24 chars: 9 + 12 + 3 = 24 bytes fits, 9 + 16 = 25 bytes does not.
        Assert.Equal(GrpcKeyValueResponseType.TypeSet, (await service.TrySetKeyValue(Set(Prefix + "ééééééaaa", 1), new StubServerCallContext(ct))).Type);
        Assert.Equal(GrpcKeyValueResponseType.TypeInvalidInput, (await service.TrySetKeyValue(Set(Prefix + "éééééééé", 1), new StubServerCallContext(ct))).Type);

        GrpcTrySetManyKeyValueRequest many = new();
        many.Items.Add(new GrpcTrySetManyKeyValueRequestItem { Key = Prefix + "m1", Value = ByteString.CopyFrom(new byte[1]), Flags = GrpcKeyValueFlags.Set, Durability = GrpcKeyValueDurability.Ephemeral });
        many.Items.Add(new GrpcTrySetManyKeyValueRequestItem { Key = Prefix + "m2", Value = ByteString.CopyFrom(new byte[9]), Flags = GrpcKeyValueFlags.Set, Durability = GrpcKeyValueDurability.Ephemeral });

        GrpcTrySetManyKeyValueResponse manyResponse = await service.TrySetManyKeyValue(many, new StubServerCallContext(ct));
        Assert.Equal(2, manyResponse.Items.Count);
        Assert.All(manyResponse.Items, i => Assert.Equal(GrpcKeyValueResponseType.TypeInvalidInput, i.Type));

        (KeyValueResponseType unwritten, _) = await node.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, Prefix + "m1", -1, HLCTimestamp.Zero, KeyValueDurability.Ephemeral, ct);
        Assert.Equal(KeyValueResponseType.DoesNotExist, unwritten);

        HLCTimestamp owner = node.Raft.HybridLogicalClock.TrySendOrLocalEvent(node.Raft.GetLocalNodeId());

        GrpcTryAcquireExclusiveLockResponse lockResponse = await service.TryAcquireExclusiveLock(new GrpcTryAcquireExclusiveLockRequest
        {
            TransactionIdNode = owner.N, TransactionIdPhysical = owner.L, TransactionIdCounter = owner.C,
            Key = Prefix + "lock-key-too-long", ExpiresMs = 1_000, Durability = GrpcKeyValueDurability.Ephemeral
        }, new StubServerCallContext(ct));
        Assert.Equal(GrpcKeyValueResponseType.TypeInvalidInput, lockResponse.Type);

        GrpcTryAcquireExclusiveRangeLockResponse rangeResponse = await service.TryAcquireExclusiveRangeLock(new GrpcTryAcquireExclusiveRangeLockRequest
        {
            TransactionIdNode = owner.N, TransactionIdPhysical = owner.L, TransactionIdCounter = owner.C,
            Prefix = Space, StartKey = Prefix + "a", StartInclusive = true, EndKey = Prefix + "end-key-too-long",
            ExpiresMs = 1_000, Durability = GrpcKeyValueDurability.Ephemeral
        }, new StubServerCallContext(ct));
        Assert.Equal(GrpcKeyValueResponseType.TypeInvalidInput, rangeResponse.Type);

        // The path the inter-node batcher takes is not bound.
        GrpcTrySetKeyValueResponse forwarded = await service.TrySetKeyValueInternal(Set(Prefix + "forwarded-key", 64), new StubServerCallContext(ct));
        Assert.Equal(GrpcKeyValueResponseType.TypeSet, forwarded.Type);
    }

    /// <summary>
    /// A client batch past <c>MaxBatchItems</c> is refused whole before any item is routed: set-many, delete-many,
    /// get-many, exists-many and the many-locks acquire, over gRPC and REST. A batch at the bound runs. The path the
    /// inter-node batcher takes is not bound.
    /// </summary>
    [Fact]
    public async Task Batches_PastTheItemBound_AreRefusedWhole_AtTheClientEntryOnly()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        KahunaConfiguration configuration = new() { MaxBatchItems = 3 };
        await using KahunaTestHost host = await KahunaTestHost.StartAsync(loggerFactory, ct, configuration: configuration);
        using GrpcChannel channel = GrpcChannel.ForAddress(host.GrpcUrl);
        KeyValuer.KeyValuerClient grpc = new(channel);

        GrpcTrySetManyKeyValueRequest SetMany(int count, string tag)
        {
            GrpcTrySetManyKeyValueRequest request = new();
            for (int i = 0; i < count; i++)
                request.Items.Add(new GrpcTrySetManyKeyValueRequestItem
                {
                    Key = $"{Prefix}{tag}{i}", Value = ByteString.CopyFrom([1]), Flags = GrpcKeyValueFlags.Set, Durability = GrpcKeyValueDurability.Persistent
                });
            return request;
        }

        GrpcTrySetManyKeyValueResponse refusedSet = await grpc.TrySetManyKeyValueAsync(SetMany(4, "over"), cancellationToken: ct);
        Assert.Equal(4, refusedSet.Items.Count);
        Assert.All(refusedSet.Items, i => Assert.Equal(GrpcKeyValueResponseType.TypeInvalidInput, i.Type));
        (KeyValueResponseType unwritten, _) = await host.Kahuna.LocateAndTryGetValue(HLCTimestamp.Zero, Prefix + "over0", -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.DoesNotExist, unwritten);

        GrpcTrySetManyKeyValueResponse acceptedSet = await grpc.TrySetManyKeyValueAsync(SetMany(3, "at"), cancellationToken: ct);
        Assert.All(acceptedSet.Items, i => Assert.Equal(GrpcKeyValueResponseType.TypeSet, i.Type));

        GrpcTryGetManyValuesRequest getMany = new();
        GrpcTryExistsManyValuesRequest existsMany = new();
        GrpcTryDeleteManyKeyValueRequest deleteMany = new();
        GrpcTryAcquireManyExclusiveLocksRequest locksMany = new() { TransactionIdPhysical = 1 };
        for (int i = 0; i < 4; i++)
        {
            string key = $"{Prefix}at{i}";
            getMany.Items.Add(new GrpcTryManyValuesRequestItem { Key = key, Revision = -1, Durability = GrpcKeyValueDurability.Persistent });
            existsMany.Items.Add(new GrpcTryManyValuesRequestItem { Key = key, Revision = -1, Durability = GrpcKeyValueDurability.Persistent });
            deleteMany.Items.Add(new GrpcTryDeleteManyKeyValueRequestItem { Key = key, Durability = GrpcKeyValueDurability.Persistent });
            locksMany.Items.Add(new GrpcTryAcquireManyExclusiveLocksRequestItem { Key = key, ExpiresMs = 1_000, Durability = GrpcKeyValueDurability.Persistent });
        }

        Assert.All((await grpc.TryGetManyValuesAsync(getMany, cancellationToken: ct)).Items, i => Assert.Equal(GrpcKeyValueResponseType.TypeInvalidInput, i.Type));
        Assert.All((await grpc.TryExistsManyValuesAsync(existsMany, cancellationToken: ct)).Items, i => Assert.Equal(GrpcKeyValueResponseType.TypeInvalidInput, i.Type));
        Assert.All((await grpc.TryDeleteManyKeyValueAsync(deleteMany, cancellationToken: ct)).Items, i => Assert.Equal(GrpcKeyValueResponseType.TypeInvalidInput, i.Type));
        Assert.All((await grpc.TryAcquireManyExclusiveLocksAsync(locksMany, cancellationToken: ct)).Items, i => Assert.Equal(GrpcKeyValueResponseType.TypeInvalidInput, i.Type));

        // The refused delete-many removed nothing.
        (KeyValueResponseType stillThere, _) = await host.Kahuna.LocateAndTryGetValue(HLCTimestamp.Zero, Prefix + "at0", -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Get, stillThere);

        getMany.Items.RemoveAt(3);
        Assert.All((await grpc.TryGetManyValuesAsync(getMany, cancellationToken: ct)).Items, i => Assert.Equal(GrpcKeyValueResponseType.TypeGot, i.Type));

        // REST: an oversized batch gets the envelope refusal, without an item per key.
        using HttpClient http = new();
        List<KahunaSetKeyValueRequestItem> restItems = [.. Enumerable.Range(0, 4).Select(i => new KahunaSetKeyValueRequestItem
        {
            Key = $"{Prefix}rest{i}", Value = [1], Flags = KeyValueFlags.Set, Durability = KeyValueDurability.Persistent
        })];

        HttpResponseMessage restSet = await http.PostAsJsonAsync(host.RestUrl + "/v1/kv/try-set-many",
            new KahunaSetManyKeyValueRequest { Items = restItems }, KahunaJsonContext.Default.KahunaSetManyKeyValueRequest, ct);
        KahunaSetManyKeyValueResponse? restSetResponse = await restSet.Content.ReadFromJsonAsync(KahunaJsonContext.Default.KahunaSetManyKeyValueResponse, ct);
        Assert.Equal(KeyValueResponseType.InvalidInput, restSetResponse!.Type);
        Assert.True(restSetResponse.Items!.Count <= 1);
        (KeyValueResponseType restUnwritten, _) = await host.Kahuna.LocateAndTryGetValue(HLCTimestamp.Zero, Prefix + "rest0", -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.DoesNotExist, restUnwritten);

        KahunaManyKeyValuesRequest restGet = new()
        {
            Items = [.. Enumerable.Range(0, 4).Select(i => new KahunaGetManyKeyValuesRequestItem { Key = $"{Prefix}at{i % 3}", Durability = KeyValueDurability.Persistent })]
        };
        foreach (string route in new[] { "/v1/kv/try-get-many", "/v1/kv/try-exists-many" })
        {
            HttpResponseMessage response = await http.PostAsJsonAsync(host.RestUrl + route, restGet, KahunaJsonContext.Default.KahunaManyKeyValuesRequest, ct);
            KahunaManyKeyValuesResponse? body = await response.Content.ReadFromJsonAsync(KahunaJsonContext.Default.KahunaManyKeyValuesResponse, ct);
            Assert.Equal(KeyValueResponseType.InvalidInput, body!.Type);
        }

        HttpResponseMessage restDelete = await http.PostAsJsonAsync(host.RestUrl + "/v1/kv/try-delete-many",
            new KahunaDeleteManyKeyValueRequest { Items = [.. Enumerable.Range(0, 4).Select(i => new KahunaDeleteKeyValueRequestItem { Key = $"{Prefix}at{i % 3}", Durability = KeyValueDurability.Persistent })] },
            KahunaJsonContext.Default.KahunaDeleteManyKeyValueRequest, ct);
        Assert.Equal(KeyValueResponseType.InvalidInput, (await restDelete.Content.ReadFromJsonAsync(KahunaJsonContext.Default.KahunaDeleteManyKeyValueResponse, ct))!.Type);

        // The path the inter-node batcher takes is not bound.
        KeyValuesService service = new(host.Kahuna, NodeTransportGate.Disabled, NullLogger<IKahuna>.Instance, configuration);
        GrpcTrySetManyKeyValueResponse forwarded = await service.TrySetManyKeyValueInternal(SetMany(4, "fwd"), new StubServerCallContext(ct));
        Assert.All(forwarded.Items, i => Assert.Equal(GrpcKeyValueResponseType.TypeSet, i.Type));
    }

    /// <summary>A value bound that one Raft message cannot carry is refused when the node starts.</summary>
    [Fact]
    public void ValueBound_MustFitInOneRaftMessage()
    {
        const int raftMaxMessageBytes = 16 * 1024 * 1024;

        ConfigurationValidator.ValidateValueBound(new KahunaConfiguration(), raftMaxMessageBytes);
        ConfigurationValidator.ValidateValueBound(new KahunaConfiguration { MaxValueBytes = 0 }, raftMaxMessageBytes);

        Assert.Throws<KahunaServerException>(() =>
            ConfigurationValidator.ValidateValueBound(new KahunaConfiguration { MaxValueBytes = raftMaxMessageBytes }, raftMaxMessageBytes));

        KeyValueInputLimits limits = new(MaxKeyBytes: 4, MaxValueBytes: 0);
        Assert.True(limits.KeyFits(null));
        Assert.True(limits.KeyFits("abcd"));
        Assert.False(limits.KeyFits("abcde"));
        Assert.False(limits.KeyFits("ééé"));
        Assert.True(limits.ValueFits(int.MaxValue));
    }

    /// <summary>Acquires a hold at a fixed timestamp, retrying the transient answer a fresh meta leader can give.</summary>
    private static async Task<(KeyValueResponseType Type, string HoldId)> Acquire(
        IKahuna kahuna, string holderId, long physical, int leaseMs, CancellationToken ct)
    {
        for (int attempt = 0; ; attempt++)
        {
            (KeyValueResponseType type, string holdId, _) = await kahuna.LocateAndAcquireSnapshotHold(
                holderId, new HLCTimestamp(0, physical, 0), leaseMs, ct);

            if (type != KeyValueResponseType.MustRetry || attempt >= 20)
                return (type, holdId);

            await Task.Delay(50, ct);
        }
    }

    private sealed class StubServerCallContext(CancellationToken ct) : ServerCallContext
    {
        protected override CancellationToken CancellationTokenCore => ct;
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
