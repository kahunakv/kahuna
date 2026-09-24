
using System.Net;
using System.Net.Sockets;
using System.Text;

using Google.Protobuf;
using Grpc.Core;

using Kahuna.Communication.External.Grpc;
using Kahuna.Server.Communication;
using Kahuna.Server.Communication.Internode;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;

using Kommander;
using Kommander.Time;

using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kahuna.Server.Tests;

/// <summary>
/// A batched set that runs into another transaction's live write intent, prefix lock, range lock, or
/// undecided durable intent still answers <see cref="KeyValueResponseType.MustRetry"/>, and now also names
/// that transaction in <see cref="KahunaSetKeyValueResponseItem.HolderTransactionId"/>. An optimistic
/// writer takes no lock before its batched set, so this answer is its first contact with a competing
/// writer; without the holder it can only retry until its deadline, while with it the younger side can
/// abort at once (wait-die). The answer type is unchanged on purpose: a consumer that predates the field
/// keeps its retry loop and ignores the holder. These tests drive the real entry points on one node, on a
/// three-node in-memory cluster, through the client-facing gRPC service, and over a live gRPC hop of the
/// inter-node transport.
/// </summary>
public sealed class TestBatchedSetConflictHolder : BaseCluster
{
    private readonly ILoggerFactory loggerFactory;
    private readonly ILogger<IRaft> raftLogger;
    private readonly ILogger<IKahuna> kahunaLogger;

    public TestBatchedSetConflictHolder(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    private static readonly HLCTimestamp HolderTransaction = new(0, 1_000, 0);
    private static readonly HLCTimestamp WriterTransaction = new(0, 2_000, 0);

    private static KahunaSetKeyValueRequestItem SetItem(string key, HLCTimestamp transactionId) => new()
    {
        TransactionId = transactionId,
        Key = key,
        Value = Encoding.UTF8.GetBytes("v"),
        ExpiresMs = 0,
        Flags = KeyValueFlags.Set,
        Durability = KeyValueDurability.Persistent
    };

    private static async Task<EmbeddedKahunaNode> StartNode(ILoggerFactory loggerFactory, string seedKey, CancellationToken ct)
    {
        EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            ReadIOThreads = 1,
            WriteIOThreads = 1,
            PartitionExecutorPoolSize = 1,
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 4
        }, loggerFactory);

        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync(seedKey, ct);

        return node;
    }

    /// <summary>
    /// Issues the batched set until every item has a definite answer. A freshly started node can answer an
    /// infrastructure MustRetry (leadership not yet confirmed) that names no holder; that answer had no
    /// effect, so re-issuing is what a real caller does. A MustRetry that names a holder is the conflict
    /// under test and is returned as is.
    /// </summary>
    private static async Task<List<KahunaSetKeyValueResponseItem>> SetManyUntilSettled(
        IKahuna kahuna, List<KahunaSetKeyValueRequestItem> items, CancellationToken ct)
    {
        List<KahunaSetKeyValueResponseItem> responses = await kahuna.LocateAndTrySetManyKeyValue(items, ct);

        long deadline = Environment.TickCount64 + (long)(10_000 * TimingScale);
        while (Environment.TickCount64 < deadline
               && responses.Exists(static r => r.Type == KeyValueResponseType.MustRetry && r.HolderTransactionId == HLCTimestamp.Zero))
        {
            await Task.Delay(50, ct);
            responses = await kahuna.LocateAndTrySetManyKeyValue(items, ct);
        }

        return responses;
    }

    private static KahunaSetKeyValueResponseItem ItemFor(List<KahunaSetKeyValueResponseItem> responses, string key) =>
        Assert.Single(responses, r => string.Equals(r.Key, key, StringComparison.Ordinal));

    // ── one node ─────────────────────────────────────────────────────────────

    /// <summary>
    /// The write intent an exclusive lock leaves on a key is the conflict an optimistic writer meets first.
    /// The blocked item names the lock's transaction; an item that met no conflict names nobody, whether it
    /// is transactional or not.
    /// </summary>
    [Fact]
    public async Task BatchedSet_OverLiveForeignWriteIntent_NamesTheHolder()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, "holder/intent/seed", ct);

        (KeyValueResponseType locked, _, _, _) = await RetryOnMustRetryAsync(
            () => node.Kahuna.LocateAndTryAcquireExclusiveLock(HolderTransaction, "holder/intent/blocked", 60_000, KeyValueDurability.Persistent, ct),
            static r => r.Item1);
        Assert.Equal(KeyValueResponseType.Locked, locked);

        List<KahunaSetKeyValueResponseItem> responses = await SetManyUntilSettled(node.Kahuna,
        [
            SetItem("holder/intent/blocked", WriterTransaction),
            SetItem("holder/intent/clean", WriterTransaction),
            SetItem("holder/intent/plain", HLCTimestamp.Zero)
        ], ct);

        Assert.Equal(3, responses.Count);

        KahunaSetKeyValueResponseItem blocked = ItemFor(responses, "holder/intent/blocked");
        Assert.Equal(KeyValueResponseType.MustRetry, blocked.Type);
        Assert.Equal(HolderTransaction, blocked.HolderTransactionId);

        KahunaSetKeyValueResponseItem clean = ItemFor(responses, "holder/intent/clean");
        Assert.Equal(KeyValueResponseType.Set, clean.Type);
        Assert.Equal(HLCTimestamp.Zero, clean.HolderTransactionId);

        KahunaSetKeyValueResponseItem plain = ItemFor(responses, "holder/intent/plain");
        Assert.Equal(KeyValueResponseType.Set, plain.Type);
        Assert.Equal(HLCTimestamp.Zero, plain.HolderTransactionId);
    }

    /// <summary>A non-transactional batched set is blocked by the same intent and learns the same holder.</summary>
    [Fact]
    public async Task BatchedSet_NonTransactional_OverLiveForeignWriteIntent_NamesTheHolder()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, "holder/plain/seed", ct);

        (KeyValueResponseType locked, _, _, _) = await RetryOnMustRetryAsync(
            () => node.Kahuna.LocateAndTryAcquireExclusiveLock(HolderTransaction, "holder/plain/blocked", 60_000, KeyValueDurability.Persistent, ct),
            static r => r.Item1);
        Assert.Equal(KeyValueResponseType.Locked, locked);

        List<KahunaSetKeyValueResponseItem> responses = await SetManyUntilSettled(node.Kahuna,
            [SetItem("holder/plain/blocked", HLCTimestamp.Zero)], ct);

        KahunaSetKeyValueResponseItem blocked = Assert.Single(responses);
        Assert.Equal(KeyValueResponseType.MustRetry, blocked.Type);
        Assert.Equal(HolderTransaction, blocked.HolderTransactionId);
    }

    /// <summary>
    /// Under deferred settlement a committed-but-undecided durable intent blocks a write before the in-memory
    /// intent check runs. The blocked item names the intent's transaction, so a caller waiting behind a
    /// prepare that has not yet decided can apply the same policy it applies to a live lock.
    /// </summary>
    [Fact]
    public async Task BatchedSet_OverUndecidedDurableIntent_NamesTheIntentOwner()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, "holder/durable/seed", ct);

        const string key = "holder/durable/blocked";

        PreparedIntentStore store = ((KahunaManager)node.Kahuna).DurablePreparedIntentStore;
        store.Apply(new PrepareIntentCommand(new PreparedIntent(
            TransactionId: HolderTransaction, Epoch: 1, Key: key, ManifestHash: 0, RecordAnchorKey: key,
            CommitTimestamp: new HLCTimestamp(0, 1_500, 0),
            State: KeyValueState.Set, Value: Encoding.UTF8.GetBytes("P"), Bucket: null, Revision: 2,
            Expires: HLCTimestamp.Zero, NoRevision: false, BaseRevision: 1, BaseState: KeyValueState.Set,
            // Far-future recovery deadline so the periodic sweep never resolves it mid-test.
            RecoveryDeadline: new HLCTimestamp(0, long.MaxValue, 0), Resolution: PreparedIntentResolution.Pending)));

        List<KahunaSetKeyValueResponseItem> responses = await SetManyUntilSettled(node.Kahuna,
            [SetItem(key, WriterTransaction)], ct);

        KahunaSetKeyValueResponseItem blocked = Assert.Single(responses);
        Assert.Equal(KeyValueResponseType.MustRetry, blocked.Type);
        Assert.Equal(HolderTransaction, blocked.HolderTransactionId);
    }

    /// <summary>A live foreign prefix lock over the key's bucket is reported with the lock's transaction.</summary>
    [Fact]
    public async Task BatchedSet_UnderForeignPrefixLock_NamesTheHolder()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, "holder/prefix/seed", ct);

        KeyValueResponseType locked = await RetryOnMustRetryAsync(
            () => node.Kahuna.LocateAndTryAcquireExclusivePrefixLock(HolderTransaction, "holder/prefix", 60_000, KeyValueDurability.Persistent, ct),
            static r => r);
        Assert.Equal(KeyValueResponseType.Locked, locked);

        List<KahunaSetKeyValueResponseItem> responses = await SetManyUntilSettled(node.Kahuna,
            [SetItem("holder/prefix/blocked", WriterTransaction)], ct);

        KahunaSetKeyValueResponseItem blocked = Assert.Single(responses);
        Assert.Equal(KeyValueResponseType.MustRetry, blocked.Type);
        Assert.Equal(HolderTransaction, blocked.HolderTransactionId);
    }

    /// <summary>A live foreign range lock that covers the key is reported with the lock's transaction.</summary>
    [Fact]
    public async Task BatchedSet_UnderForeignRangeLock_NamesTheHolder()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, "holder/range/seed", ct);

        (KeyValueResponseType locked, _) = await RetryOnMustRetryAsync(
            () => node.Kahuna.LocateAndTryAcquireExclusiveRangeLock(
                HolderTransaction, "holder/range", null, true, null, false, 60_000, KeyValueDurability.Persistent, ct),
            static r => r.Item1);
        Assert.Equal(KeyValueResponseType.Locked, locked);

        List<KahunaSetKeyValueResponseItem> responses = await SetManyUntilSettled(node.Kahuna,
            [SetItem("holder/range/blocked", WriterTransaction)], ct);

        KahunaSetKeyValueResponseItem blocked = Assert.Single(responses);
        Assert.Equal(KeyValueResponseType.MustRetry, blocked.Type);
        Assert.Equal(HolderTransaction, blocked.HolderTransactionId);
    }

    // ── three nodes, in-memory transport ─────────────────────────────────────

    /// <summary>
    /// The holder survives the forwarding hop. Whichever node leads the key's partition, at least two of the
    /// three callers reach it through the inter-node transport, and every caller learns the same holder.
    /// </summary>
    [Fact]
    public async Task BatchedSet_AcrossCluster_EveryNodeLearnsTheHolder()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft raft1, IRaft raft2, IRaft raft3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster("memory", 8, raftLogger, kahunaLogger);

        try
        {
            string key = "holder/cluster/" + Guid.NewGuid().ToString("N")[..8] + "/blocked";

            (KeyValueResponseType locked, _, _, _) = await RetryOnMustRetryAsync(
                () => kahuna1.LocateAndTryAcquireExclusiveLock(HolderTransaction, key, 60_000, KeyValueDurability.Persistent, ct),
                static r => r.Item1);
            Assert.Equal(KeyValueResponseType.Locked, locked);

            foreach (IKahuna caller in (IKahuna[])[kahuna1, kahuna2, kahuna3])
            {
                List<KahunaSetKeyValueResponseItem> responses = await SetManyUntilSettled(caller, [SetItem(key, WriterTransaction)], ct);

                KahunaSetKeyValueResponseItem blocked = Assert.Single(responses);
                Assert.Equal(KeyValueResponseType.MustRetry, blocked.Type);
                Assert.Equal(HolderTransaction, blocked.HolderTransactionId);
            }
        }
        finally
        {
            await LeaveCluster(raft1, raft2, raft3);
        }
    }

    // ── gRPC wire ────────────────────────────────────────────────────────────

    /// <summary>
    /// The client-facing gRPC service encodes the holder on the item, and protobuf keeps it: after a byte
    /// round trip the item still names the transaction. A clean item encodes an all-zero holder.
    /// </summary>
    [Fact]
    public async Task GrpcService_BatchedSetItem_CarriesTheHolderOverTheWire()
    {
        KeyValuesService service = new(new ConflictingSetManyKahuna(), NodeTransportGate.Disabled, NullLogger<IKahuna>.Instance);

        GrpcTrySetManyKeyValueRequest request = new();
        request.Items.Add(new GrpcTrySetManyKeyValueRequestItem { Key = "blocked", Durability = GrpcKeyValueDurability.Persistent });
        request.Items.Add(new GrpcTrySetManyKeyValueRequestItem { Key = "clean", Durability = GrpcKeyValueDurability.Persistent });

        GrpcTrySetManyKeyValueResponse response = await service.TrySetManyKeyValue(request, new StubServerCallContext());
        GrpcTrySetManyKeyValueResponse onWire = GrpcTrySetManyKeyValueResponse.Parser.ParseFrom(response.ToByteArray());

        Assert.Equal(2, onWire.Items.Count);

        GrpcTrySetManyKeyValueResponseItem blocked = Assert.Single(onWire.Items, static i => i.Key == "blocked");
        Assert.Equal(GrpcKeyValueResponseType.TypeMustRetry, blocked.Type);
        Assert.Equal(HolderTransaction, new HLCTimestamp(blocked.HolderTransactionIdNode, blocked.HolderTransactionIdPhysical, blocked.HolderTransactionIdCounter));

        GrpcTrySetManyKeyValueResponseItem clean = Assert.Single(onWire.Items, static i => i.Key == "clean");
        Assert.Equal(GrpcKeyValueResponseType.TypeSet, clean.Type);
        Assert.Equal(HLCTimestamp.Zero, new HLCTimestamp(clean.HolderTransactionIdNode, clean.HolderTransactionIdPhysical, clean.HolderTransactionIdCounter));
    }

    /// <summary>
    /// The production inter-node hop: a forwarding node's <see cref="GrpcInterNodeCommunication"/> sends the
    /// batch over the shared duplex stream to a real Kestrel listener that hosts the Kahuna gRPC services,
    /// and decodes the holder from the answer. The leader here is a fake that answers one item with a
    /// conflict, so the test isolates the wire, not the actor.
    /// </summary>
    [Fact]
    public async Task InterNodeGrpc_BatchedSetItem_CarriesTheHolderBackToTheForwardingNode()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        int port = FreePort();

        WebApplicationBuilder builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.Services.AddSingleton<IKahuna>(new ConflictingSetManyKahuna());
        // The shared inter-node batcher opens the lock stream beside the key-value stream, and the lock
        // service is activated with a Raft handle it never reaches in this test.
        builder.Services.AddSingleton<IRaft>(new LeadershipStubRaft());
        builder.Services.AddSingleton(new KahunaConfiguration());
        builder.Services.AddSingleton(NodeTransportGate.Disabled);
        builder.Services.AddSingleton<ILogger<IKahuna>>(NullLogger<IKahuna>.Instance);
        builder.Services.AddGrpc(static options => options.EnableDetailedErrors = true);
        builder.WebHost.ConfigureKestrel(kestrel =>
            kestrel.Listen(IPAddress.Loopback, port, static listenOptions => listenOptions.Protocols = HttpProtocols.Http2));

        WebApplication app = builder.Build();
        app.MapGrpcKahunaRoutes();
        await app.StartAsync(ct);

        try
        {
            GrpcInterNodeCommunication transport = new(
                new KahunaConfiguration { InterNodeGrpcScheme = "http://" },
                new RaftTransportSecurityOptions(),
                NullLogger<GrpcInterNodeCommunication>.Instance);

            Lock lockSync = new();
            List<KahunaSetKeyValueResponseItem> responses = [];

            await transport.TrySetManyNodeKeyValue(
                $"localhost:{port}",
                [SetItem("blocked", HLCTimestamp.Zero), SetItem("clean", HLCTimestamp.Zero)],
                lockSync, responses, ct);

            Assert.Equal(2, responses.Count);

            KahunaSetKeyValueResponseItem blocked = ItemFor(responses, "blocked");
            Assert.Equal(KeyValueResponseType.MustRetry, blocked.Type);
            Assert.Equal(HolderTransaction, blocked.HolderTransactionId);

            KahunaSetKeyValueResponseItem clean = ItemFor(responses, "clean");
            Assert.Equal(KeyValueResponseType.Set, clean.Type);
            Assert.Equal(HLCTimestamp.Zero, clean.HolderTransactionId);
        }
        finally
        {
            using CancellationTokenSource stop = new(TimeSpan.FromSeconds(2));
            try { await app.StopAsync(stop.Token); } catch { /* best-effort */ }
            await app.DisposeAsync();
        }
    }

    private static int FreePort()
    {
        using TcpListener listener = new(IPAddress.Loopback, 0);
        listener.Start();
        return ((IPEndPoint)listener.LocalEndpoint).Port;
    }

    /// <summary>
    /// Stands in for the leader's batched-set path: the item keyed "blocked" ran into
    /// <see cref="HolderTransaction"/>; every other item was written.
    /// </summary>
    private sealed class ConflictingSetManyKahuna : FakeKahunaBase
    {
        public override Task<List<KahunaSetKeyValueResponseItem>> LocateAndTrySetManyKeyValue(
            List<KahunaSetKeyValueRequestItem> setManyItems, CancellationToken cancellationToken,
            string coordinatorKey = "", TransactionOperationId operationId = default)
        {
            List<KahunaSetKeyValueResponseItem> responses = new(setManyItems.Count);

            foreach (KahunaSetKeyValueRequestItem item in setManyItems)
            {
                bool blocked = string.Equals(item.Key, "blocked", StringComparison.Ordinal);
                responses.Add(new()
                {
                    Key = item.Key,
                    Type = blocked ? KeyValueResponseType.MustRetry : KeyValueResponseType.Set,
                    Revision = blocked ? 0 : 1,
                    Durability = item.Durability,
                    HolderTransactionId = blocked ? HolderTransaction : HLCTimestamp.Zero
                });
            }

            return Task.FromResult(responses);
        }
    }

    /// <summary>Minimal context: the service reads only the cancellation token.</summary>
    private sealed class StubServerCallContext : ServerCallContext
    {
        protected override CancellationToken CancellationTokenCore => CancellationToken.None;
        protected override string MethodCore => "test";
        protected override string HostCore => "test";
        protected override string PeerCore => "test";
        protected override System.DateTime DeadlineCore => System.DateTime.MaxValue;
        protected override Metadata RequestHeadersCore => new();
        protected override Metadata ResponseTrailersCore => new();
        protected override Status StatusCore { get; set; }
        protected override WriteOptions? WriteOptionsCore { get; set; }
        protected override AuthContext AuthContextCore => throw new NotSupportedException();
        protected override ContextPropagationToken CreatePropagationTokenCore(ContextPropagationOptions? options) => throw new NotSupportedException();
        protected override Task WriteResponseHeadersAsyncCore(Metadata responseHeaders) => throw new NotSupportedException();
    }
}
