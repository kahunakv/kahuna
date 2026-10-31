using System.Net.Http.Json;
using System.Text;
using Google.Protobuf;
using Grpc.Core;
using Grpc.Net.Client;
using Kahuna.Communication.External.Grpc;
using Kahuna.Server.Communication;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.Communication.Grpc;
using Kahuna.Shared.Communication.Rest;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kahuna.Server.Tests;

/// <summary>
/// The per-transaction limits a client is held to: <c>MaxTransactionMutations</c> written keys and, separately, held
/// locks per session (and per script), and the rule that a key-value lock from a client must name the session that
/// owns it. Driven through the real client entry points of a Kestrel host. Embedded callers and forwarded starts keep
/// the limit the entry node chose, or none.
/// </summary>
public sealed class TestTransactionMutationLimit
{
    private const int Limit = 3;

    private const string Prefix = "t:mut/";

    private readonly ILoggerFactory loggerFactory;

    public TestTransactionMutationLimit(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    private Task<KahunaTestHost> StartHost(CancellationToken ct) =>
        KahunaTestHost.StartAsync(loggerFactory, ct, configuration: new KahunaConfiguration { MaxTransactionMutations = Limit });

    private sealed record Session(HLCTimestamp TransactionId, string CoordinatorKey);

    private static async Task<Session> Start(KeyValuer.KeyValuerClient grpc, string coordinatorKey, int requestedLimit, CancellationToken ct)
    {
        GrpcStartTransactionResponse started = await grpc.StartTransactionAsync(new GrpcStartTransactionRequest
        {
            CoordinatorKey = coordinatorKey,
            LockingType = GrpcLockingType.LockingTypePessimistic,
            Timeout = 30_000,
            // A client cannot choose its own limit: the entry point overwrites this.
            MutationLimit = requestedLimit
        }, cancellationToken: ct);

        Assert.Equal(GrpcKeyValueResponseType.TypeSet, started.Type);
        return new(new(started.TransactionIdNode, started.TransactionIdPhysical, started.TransactionIdCounter), started.CoordinatorKey);
    }

    private static async Task<GrpcKeyValueResponseType> Set(KeyValuer.KeyValuerClient grpc, Session session, string key, CancellationToken ct)
    {
        TransactionOperationId operation = TransactionOperationId.NewRandom();
        GrpcTrySetKeyValueResponse response = await grpc.TrySetKeyValueAsync(new GrpcTrySetKeyValueRequest
        {
            TransactionIdNode = session.TransactionId.N, TransactionIdPhysical = session.TransactionId.L, TransactionIdCounter = session.TransactionId.C,
            Key = key, Value = ByteString.CopyFrom([1]), Flags = GrpcKeyValueFlags.Set, Durability = GrpcKeyValueDurability.Persistent,
            CoordinatorKey = session.CoordinatorKey, OperationIdHigh = operation.High, OperationIdLow = operation.Low
        }, cancellationToken: ct);
        return response.Type;
    }

    private static GrpcTryAcquireExclusiveLockRequest Lock(Session session, string key, bool registered)
    {
        TransactionOperationId operation = TransactionOperationId.NewRandom();
        GrpcTryAcquireExclusiveLockRequest request = new()
        {
            TransactionIdNode = session.TransactionId.N, TransactionIdPhysical = session.TransactionId.L, TransactionIdCounter = session.TransactionId.C,
            Key = key, ExpiresMs = 30_000, Durability = GrpcKeyValueDurability.Persistent
        };

        if (registered)
        {
            request.CoordinatorKey = session.CoordinatorKey;
            request.OperationIdHigh = operation.High;
            request.OperationIdLow = operation.Low;
        }

        return request;
    }

    private static async Task<GrpcKeyValueResponseType> Commit(KeyValuer.KeyValuerClient grpc, Session session, CancellationToken ct) =>
        (await grpc.CommitTransactionAsync(new GrpcCommitTransactionRequest
        {
            CoordinatorKey = session.CoordinatorKey,
            TransactionIdNode = session.TransactionId.N, TransactionIdPhysical = session.TransactionId.L, TransactionIdCounter = session.TransactionId.C
        }, cancellationToken: ct)).Type;

    private static async Task<KeyValueResponseType> Read(KahunaTestHost host, string key, CancellationToken ct) =>
        (await host.Kahuna.LocateAndTryGetValue(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct)).Item1;

    /// <summary>
    /// A client session that locks N keys and then writes the same N keys — the shape of a CamusDB statement — fits a
    /// limit of N. One more written key, or one more lock, is refused with nothing applied, and the session can still
    /// commit what it holds. The limit is the entry node's, not the one the client put in its start request.
    /// </summary>
    [Fact]
    public async Task ClientSession_IsHeldToItsWriteAndLockLimits_AndStillCommits()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using KahunaTestHost host = await StartHost(ct);
        using GrpcChannel channel = GrpcChannel.ForAddress(host.GrpcUrl);
        KeyValuer.KeyValuerClient grpc = new(channel);

        Session session = await Start(grpc, Prefix + "c1", requestedLimit: 1_000_000, ct);

        TransactionOperationId lockBatch = TransactionOperationId.NewRandom();
        GrpcTryAcquireManyExclusiveLocksRequest locks = new()
        {
            TransactionIdNode = session.TransactionId.N, TransactionIdPhysical = session.TransactionId.L, TransactionIdCounter = session.TransactionId.C,
            CoordinatorKey = session.CoordinatorKey, OperationIdHigh = lockBatch.High, OperationIdLow = lockBatch.Low
        };
        for (int i = 0; i < Limit; i++)
            locks.Items.Add(new GrpcTryAcquireManyExclusiveLocksRequestItem { Key = $"{Prefix}k{i}", ExpiresMs = 30_000, Durability = GrpcKeyValueDurability.Persistent });

        GrpcTryAcquireManyExclusiveLocksResponse locked = await grpc.TryAcquireManyExclusiveLocksAsync(locks, cancellationToken: ct);
        Assert.All(locked.Items, i => Assert.Equal(GrpcKeyValueResponseType.TypeLocked, i.Type));

        for (int i = 0; i < Limit; i++)
            Assert.Equal(GrpcKeyValueResponseType.TypeSet, await Set(grpc, session, $"{Prefix}k{i}", ct));

        Assert.Equal(GrpcKeyValueResponseType.TypeTransactionMutationLimitReached, await Set(grpc, session, Prefix + "k-over", ct));
        Assert.Equal(GrpcKeyValueResponseType.TypeTransactionMutationLimitReached,
            (await grpc.TryAcquireExclusiveLockAsync(Lock(session, Prefix + "lock-over", registered: true), cancellationToken: ct)).Type);

        Assert.Equal(GrpcKeyValueResponseType.TypeCommitted, await Commit(grpc, session, ct));

        Assert.Equal(KeyValueResponseType.Get, await Read(host, Prefix + "k0", ct));
        Assert.Equal(KeyValueResponseType.DoesNotExist, await Read(host, Prefix + "k-over", ct));
    }

    /// <summary>
    /// A start forwarded from another node arrives on the inter-node path with the entry node's limit, and keeps it:
    /// only a client start is stamped with the local limit. An embedded session has no limit at all.
    /// </summary>
    [Fact]
    public async Task ForwardedStart_KeepsItsLimit_AndEmbeddedSessionsAreUnlimited()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using KahunaTestHost host = await StartHost(ct);
        using GrpcChannel channel = GrpcChannel.ForAddress(host.GrpcUrl);
        KeyValuer.KeyValuerClient grpc = new(channel);

        KeyValuesService service = new(host.Kahuna, NodeTransportGate.Disabled, NullLogger<IKahuna>.Instance, new KahunaConfiguration { MaxTransactionMutations = Limit });
        GrpcStartTransactionResponse forwarded = await service.StartTransactionInternal(new GrpcStartTransactionRequest
        {
            CoordinatorKey = Prefix + "fwd", LockingType = GrpcLockingType.LockingTypePessimistic, Timeout = 30_000, MutationLimit = 1
        }, new StubServerCallContext(ct));
        Session forwardedSession = new(new(forwarded.TransactionIdNode, forwarded.TransactionIdPhysical, forwarded.TransactionIdCounter), forwarded.CoordinatorKey);

        Assert.Equal(GrpcKeyValueResponseType.TypeSet, await Set(grpc, forwardedSession, Prefix + "fwd0", ct));
        Assert.Equal(GrpcKeyValueResponseType.TypeTransactionMutationLimitReached, await Set(grpc, forwardedSession, Prefix + "fwd1", ct));

        (KeyValueResponseType startType, TransactionHandle handle) = await host.Kahuna.LocateAndStartTransaction(
            new KeyValueTransactionOptions { CoordinatorKey = Prefix + "embedded", Timeout = 30_000 }, ct);
        Assert.Equal(KeyValueResponseType.Set, startType);

        for (int i = 0; i < Limit + 2; i++)
        {
            (KeyValueResponseType type, _, _) = await host.Kahuna.LocateAndTrySetKeyValue(
                handle.TransactionId, $"{Prefix}emb{i}", [1], null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct,
                coordinatorKey: handle.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
            Assert.Equal(KeyValueResponseType.Set, type);
        }
    }

    /// <summary>A session started over REST takes the entry node's limit as well.</summary>
    [Fact]
    public async Task RestSession_IsHeldToTheLimit()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using KahunaTestHost host = await StartHost(ct);
        using HttpClient http = new();

        HttpResponseMessage startResponse = await http.PostAsJsonAsync(host.RestUrl + "/v1/kv/start-tx-session",
            new KahunaStartTransactionRequest { CoordinatorKey = Prefix + "rest", Timeout = 30_000 }, KahunaJsonContext.Default.KahunaStartTransactionRequest, ct);
        KahunaStartTransactionResponse started = (await startResponse.Content.ReadFromJsonAsync(KahunaJsonContext.Default.KahunaStartTransactionResponse, ct))!;
        Assert.Equal(KeyValueResponseType.Set, started.Type);

        async Task<KeyValueResponseType> RestSet(string key)
        {
            TransactionOperationId operation = TransactionOperationId.NewRandom();
            HttpResponseMessage response = await http.PostAsJsonAsync(host.RestUrl + "/v1/kv/try-set", new KahunaSetKeyValueRequest
            {
                TransactionId = started.TransactionId, Key = key, Value = [1], Flags = KeyValueFlags.Set, Durability = KeyValueDurability.Persistent,
                CoordinatorKey = started.CoordinatorKey, OperationIdHigh = operation.High, OperationIdLow = operation.Low
            }, KahunaJsonContext.Default.KahunaSetKeyValueRequest, ct);
            return (await response.Content.ReadFromJsonAsync(KahunaJsonContext.Default.KahunaSetKeyValueResponse, ct))!.Type;
        }

        for (int i = 0; i < Limit; i++)
            Assert.Equal(KeyValueResponseType.Set, await RestSet($"{Prefix}rest{i}"));

        Assert.Equal(KeyValueResponseType.TransactionMutationLimitReached, await RestSet(Prefix + "rest-over"));
    }

    /// <summary>
    /// A script a client sends is held to the limit too: one that writes more keys than the limit allows does not
    /// commit, and one at the limit does. An embedded caller's script is not limited.
    /// </summary>
    [Fact]
    public async Task ClientScript_PastTheLimit_DoesNotCommit()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using KahunaTestHost host = await StartHost(ct);
        using GrpcChannel channel = GrpcChannel.ForAddress(host.GrpcUrl);
        KeyValuer.KeyValuerClient grpc = new(channel);

        static string Script(string tag, int keys)
        {
            StringBuilder script = new("BEGIN ");
            for (int i = 0; i < keys; i++)
                script.Append($"SET `{Prefix}{tag}{i}` 'v' ");
            return script.Append("COMMIT END").ToString();
        }

        GrpcTryExecuteTransactionScriptResponse over = await grpc.TryExecuteTransactionScriptAsync(
            new GrpcTryExecuteTransactionScriptRequest { Script = ByteString.CopyFromUtf8(Script("sover", Limit + 1)) }, cancellationToken: ct);
        Assert.Equal(GrpcKeyValueResponseType.TypeTransactionMutationLimitReached, over.Type);
        Assert.Equal(KeyValueResponseType.DoesNotExist, await Read(host, Prefix + "sover0", ct));

        GrpcTryExecuteTransactionScriptResponse at = await grpc.TryExecuteTransactionScriptAsync(
            new GrpcTryExecuteTransactionScriptRequest { Script = ByteString.CopyFromUtf8(Script("sat", Limit)) }, cancellationToken: ct);
        Assert.NotEqual(GrpcKeyValueResponseType.TypeTransactionMutationLimitReached, at.Type);
        Assert.Equal(KeyValueResponseType.Get, await Read(host, Prefix + "sat0", ct));

        KeyValueTransactionResult embedded = await host.Kahuna.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(Script("semb", Limit + 1)), null, null);
        Assert.NotEqual(KeyValueResponseType.TransactionMutationLimitReached, embedded.Type);
        Assert.Equal(KeyValueResponseType.Get, await Read(host, Prefix + "semb0", ct));
    }

    /// <summary>
    /// A key-value lock from a client must name the session that owns it. Without the coordinator key and an operation
    /// id the lock is refused on every client entry point, single, prefix, range and many, over gRPC and REST; with
    /// them it is granted; and a made-up session is not a session.
    /// </summary>
    [Fact]
    public async Task ClientLocks_WithoutTheOwningSession_AreRefused()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using KahunaTestHost host = await StartHost(ct);
        using GrpcChannel channel = GrpcChannel.ForAddress(host.GrpcUrl);
        KeyValuer.KeyValuerClient grpc = new(channel);
        using HttpClient http = new();

        Session session = await Start(grpc, Prefix + "locks", requestedLimit: 0, ct);
        HLCTimestamp tx = session.TransactionId;

        Assert.Equal(GrpcKeyValueResponseType.TypeInvalidInput,
            (await grpc.TryAcquireExclusiveLockAsync(Lock(session, Prefix + "l0", registered: false), cancellationToken: ct)).Type);
        Assert.Equal(GrpcKeyValueResponseType.TypeInvalidInput,
            (await grpc.TryAcquireExclusivePrefixLockAsync(new GrpcTryAcquireExclusivePrefixLockRequest
            {
                TransactionIdNode = tx.N, TransactionIdPhysical = tx.L, TransactionIdCounter = tx.C,
                PrefixKey = Prefix + "p", ExpiresMs = 30_000, Durability = GrpcKeyValueDurability.Persistent
            }, cancellationToken: ct)).Type);
        Assert.Equal(GrpcKeyValueResponseType.TypeInvalidInput,
            (await grpc.TryAcquireExclusiveRangeLockAsync(new GrpcTryAcquireExclusiveRangeLockRequest
            {
                TransactionIdNode = tx.N, TransactionIdPhysical = tx.L, TransactionIdCounter = tx.C,
                Prefix = "t:mut", StartKey = Prefix + "a", StartInclusive = true, EndKey = Prefix + "z",
                ExpiresMs = 30_000, Durability = GrpcKeyValueDurability.Persistent
            }, cancellationToken: ct)).Type);

        GrpcTryAcquireManyExclusiveLocksRequest unregisteredMany = new() { TransactionIdNode = tx.N, TransactionIdPhysical = tx.L, TransactionIdCounter = tx.C };
        unregisteredMany.Items.Add(new GrpcTryAcquireManyExclusiveLocksRequestItem { Key = Prefix + "m0", ExpiresMs = 30_000, Durability = GrpcKeyValueDurability.Persistent });
        Assert.All((await grpc.TryAcquireManyExclusiveLocksAsync(unregisteredMany, cancellationToken: ct)).Items,
            i => Assert.Equal(GrpcKeyValueResponseType.TypeInvalidInput, i.Type));

        HttpResponseMessage restLock = await http.PostAsJsonAsync(host.RestUrl + "/v1/kv/try-acquire-exclusive-lock",
            new KahunaAcquireKeyValueLockRequest { TransactionId = tx, Key = Prefix + "r0", ExpiresMs = 30_000, Durability = KeyValueDurability.Persistent },
            KahunaJsonContext.Default.KahunaAcquireKeyValueLockRequest, ct);
        Assert.Equal(KeyValueResponseType.InvalidInput, (await restLock.Content.ReadFromJsonAsync(KahunaJsonContext.Default.KahunaKeyValueLockResponse, ct))!.Type);

        // The owning session's identity gets the lock.
        Assert.Equal(GrpcKeyValueResponseType.TypeLocked,
            (await grpc.TryAcquireExclusiveLockAsync(Lock(session, Prefix + "l0", registered: true), cancellationToken: ct)).Type);

        // A made-up session: a timestamp and a coordinator key no coordinator knows.
        Session forged = new(new HLCTimestamp(0, 42, 0), Prefix + "no-such-session");
        GrpcKeyValueResponseType forgedType = (await grpc.TryAcquireExclusiveLockAsync(Lock(forged, Prefix + "forged", registered: true), cancellationToken: ct)).Type;
        Assert.NotEqual(GrpcKeyValueResponseType.TypeLocked, forgedType);
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
