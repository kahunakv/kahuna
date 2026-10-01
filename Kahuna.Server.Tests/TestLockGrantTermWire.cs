using Google.Protobuf;
using Grpc.Core;
using Kahuna.Communication.External.Grpc;
using Kahuna.Server.Communication;
using Kahuna.Server.Communication.Internode;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Time;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kahuna.Server.Tests;

/// <summary>
/// A lock grant reports the leadership term it was issued under, and the report has to survive the way back to
/// the transaction's coordinator: out of the locator on the granting node, through the gRPC response when the
/// request was forwarded, and into the capture the forwarding node opened. These tests drive the four lock
/// handlers of the gRPC service against a real cluster, on the leader and on a node that forwards, and the
/// commit-time probe that asks whether a partition is still led under a term.
/// </summary>
public sealed class TestLockGrantTermWire : BaseCluster
{
    private const int Nodes = 3;

    private const int Partitions = 4;

    private const int LockExpiresMs = 30_000;

    private readonly ILogger<IRaft> raftLogger;

    private readonly ILogger<IKahuna> kahunaLogger;

    public TestLockGrantTermWire(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = TestLogFactory.Create(outputHelper);
        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    private static string BucketOf(string key) => key[..key.LastIndexOf('/')];

    private static async Task<int> LeaderIndexOf(int partition, IRaft[] rafts, CancellationToken ct)
    {
        int index = -1;
        await WaitUntilAsync(async () =>
        {
            for (int i = 0; i < rafts.Length; i++)
            {
                if (!await rafts[i].AmILeaderIfHosted(partition, ct))
                    continue;

                index = i;
                return true;
            }

            return false;
        }, timeoutMs: 30_000);

        return index;
    }

    private static KeyValuesService ServiceOf(IKahuna kahuna) =>
        new(kahuna, NodeTransportGate.Disabled, NullLogger<IKahuna>.Instance);

    private static void AssertSingleGrant(
        Google.Protobuf.Collections.RepeatedField<GrpcLockGrantTerm> grantTerms, int partition, long term, string routingKey)
    {
        GrpcLockGrantTerm grant = Assert.Single(grantTerms);
        Assert.Equal(partition, grant.PartitionId);
        Assert.Equal(term, grant.Term);
        Assert.Equal(routingKey, grant.RoutingKey);
    }

    /// <param name="throughForwardingNode">The request is served by a node that does not lead the partition, so
    /// it forwards to the leader and must still answer the leader's term.</param>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task EveryLockHandler_AnswersTheTermOfTheGrantingLeader(bool throughForwardingNode)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleCluster(Nodes, "memory", Partitions, raftLogger, kahunaLogger);

        try
        {
            KahunaManager probe = (KahunaManager)kahunas[0];
            string random = Guid.NewGuid().ToString("N")[..8];

            string pointKey = $"lgw-point/{random}";
            string manyKeyA = $"lgw-many-a/{random}";
            string manyKeyB = $"lgw-many-b/{random}";
            string rangeKey = $"lgw-range/{random}";
            string prefixKey = $"lgw-prefix/{random}";

            int pointPartition = probe.KeyValues.LocateDurablePartition(pointKey).PartitionId;
            int leader = await LeaderIndexOf(pointPartition, rafts, ct);
            int served = throughForwardingNode ? (leader + 1) % Nodes : leader;

            KeyValuesService service = ServiceOf(kahunas[served]);
            ServerCallContext context = new StubServerCallContext();

            HLCTimestamp transactionId = rafts[served].HybridLogicalClock.TrySendOrLocalEvent(rafts[served].GetLocalNodeId());

            // Every partition the test locks on, with the term its leader holds.
            Dictionary<int, long> terms = [];
            foreach (string key in (string[])[pointKey, manyKeyA, manyKeyB, rangeKey, prefixKey])
            {
                int partition = probe.KeyValues.LocateDurablePartition(key).PartitionId;
                terms[partition] = rafts[await LeaderIndexOf(partition, rafts, ct)].GetPartitionTerm(partition);
            }

            long TermOf(int partition) => terms[partition];

            // ── point lock ──
            GrpcTryAcquireExclusiveLockResponse point = GrpcTryAcquireExclusiveLockResponse.Parser.ParseFrom(
                (await service.TryAcquireExclusiveLockInternal(new GrpcTryAcquireExclusiveLockRequest
                {
                    TransactionIdNode = transactionId.N, TransactionIdPhysical = transactionId.L, TransactionIdCounter = transactionId.C,
                    Key = pointKey, ExpiresMs = LockExpiresMs, Durability = GrpcKeyValueDurability.Persistent
                }, context)).ToByteArray());

            Assert.Equal(GrpcKeyValueResponseType.TypeLocked, point.Type);
            AssertSingleGrant(point.GrantTerms, pointPartition, TermOf(pointPartition), pointKey);

            // ── many point locks ──
            GrpcTryAcquireManyExclusiveLocksRequest manyRequest = new()
            {
                TransactionIdNode = transactionId.N, TransactionIdPhysical = transactionId.L, TransactionIdCounter = transactionId.C
            };
            manyRequest.Items.Add(new GrpcTryAcquireManyExclusiveLocksRequestItem { Key = manyKeyA, ExpiresMs = LockExpiresMs, Durability = GrpcKeyValueDurability.Persistent });
            manyRequest.Items.Add(new GrpcTryAcquireManyExclusiveLocksRequestItem { Key = manyKeyB, ExpiresMs = LockExpiresMs, Durability = GrpcKeyValueDurability.Persistent });

            GrpcTryAcquireManyExclusiveLocksResponse many = GrpcTryAcquireManyExclusiveLocksResponse.Parser.ParseFrom(
                (await service.TryAcquireManyExclusiveLocksInternal(manyRequest, context)).ToByteArray());

            Assert.All(many.Items, static item => Assert.Equal(GrpcKeyValueResponseType.TypeLocked, item.Type));

            HashSet<int> manyPartitions =
            [
                probe.KeyValues.LocateDurablePartition(manyKeyA).PartitionId,
                probe.KeyValues.LocateDurablePartition(manyKeyB).PartitionId
            ];
            Assert.Equal(manyPartitions, many.GrantTerms.Select(static g => g.PartitionId).ToHashSet());
            Assert.All(many.GrantTerms, grant => Assert.Equal(TermOf(grant.PartitionId), grant.Term));
            Assert.All(many.GrantTerms, grant =>
                Assert.Equal(grant.PartitionId, probe.KeyValues.LocateDurablePartition(grant.RoutingKey).PartitionId));

            // ── range lock ──
            int rangePartition = probe.KeyValues.LocateDurablePartition(rangeKey).PartitionId;
            GrpcTryAcquireExclusiveRangeLockResponse range = GrpcTryAcquireExclusiveRangeLockResponse.Parser.ParseFrom(
                (await service.TryAcquireExclusiveRangeLockInternal(new GrpcTryAcquireExclusiveRangeLockRequest
                {
                    TransactionIdNode = transactionId.N, TransactionIdPhysical = transactionId.L, TransactionIdCounter = transactionId.C,
                    Prefix = BucketOf(rangeKey), StartKey = rangeKey, StartInclusive = true, EndKey = rangeKey, EndInclusive = true,
                    ExpiresMs = LockExpiresMs, Durability = GrpcKeyValueDurability.Persistent, Mode = (GrpcRangeLockMode)RangeLockMode.Shared
                }, context)).ToByteArray());

            Assert.Equal(GrpcKeyValueResponseType.TypeLocked, range.Type);
            AssertSingleGrant(range.GrantTerms, rangePartition, TermOf(rangePartition), rangeKey);

            // ── prefix lock ──
            int prefixPartition = probe.KeyValues.LocateDurablePartition(prefixKey).PartitionId;
            GrpcTryAcquireExclusivePrefixLockResponse prefix = GrpcTryAcquireExclusivePrefixLockResponse.Parser.ParseFrom(
                (await service.TryAcquireExclusivePrefixLockInternal(new GrpcTryAcquireExclusivePrefixLockRequest
                {
                    TransactionIdNode = transactionId.N, TransactionIdPhysical = transactionId.L, TransactionIdCounter = transactionId.C,
                    PrefixKey = BucketOf(prefixKey), ExpiresMs = LockExpiresMs, Durability = GrpcKeyValueDurability.Persistent
                }, context)).ToByteArray());

            Assert.Equal(GrpcKeyValueResponseType.TypeLocked, prefix.Type);
            GrpcLockGrantTerm prefixGrant = Assert.Single(prefix.GrantTerms);
            Assert.Equal(prefixPartition, prefixGrant.PartitionId);
            Assert.Equal(TermOf(prefixPartition), prefixGrant.Term);
            Assert.Equal(prefixPartition, probe.KeyValues.LocateDurablePartition(prefixGrant.RoutingKey).PartitionId);

            // ── a refused acquire reports no grant ──
            HLCTimestamp other = rafts[served].HybridLogicalClock.TrySendOrLocalEvent(rafts[served].GetLocalNodeId());
            GrpcTryAcquireExclusiveLockResponse refused = await service.TryAcquireExclusiveLockInternal(new GrpcTryAcquireExclusiveLockRequest
            {
                TransactionIdNode = other.N, TransactionIdPhysical = other.L, TransactionIdCounter = other.C,
                Key = pointKey, ExpiresMs = LockExpiresMs, Durability = GrpcKeyValueDurability.Persistent
            }, context);

            Assert.Equal(GrpcKeyValueResponseType.TypeAlreadyLocked, refused.Type);
            Assert.Empty(refused.GrantTerms);
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    [Fact]
    public void RemoteGrantTerms_AreReplayedIntoTheCallersCapture()
    {
        GrpcTryAcquireExclusiveLockResponse response = new();
        response.GrantTerms.Add(new GrpcLockGrantTerm { PartitionId = 3, Term = 12, RoutingKey = "k/1" });
        response.GrantTerms.Add(new GrpcLockGrantTerm { PartitionId = 4, Term = 7, RoutingKey = "k/2" });

        GrpcTryAcquireExclusiveLockResponse onWire = GrpcTryAcquireExclusiveLockResponse.Parser.ParseFrom(response.ToByteArray());

        // With no capture open the replay is a no-op.
        GrpcInterNodeCommunication.RecordLockGrantTerms(onWire.GrantTerms);

        LockGrantCapture capture;
        using (LockGrantScope.Begin(out capture))
            GrpcInterNodeCommunication.RecordLockGrantTerms(onWire.GrantTerms);

        Assert.Equal([new LockGrantTerm(3, 12, "k/1"), new LockGrantTerm(4, 7, "k/2")], capture.Take());
    }

    /// <param name="throughForwardingNode">The probe is issued on a node that does not lead the partition.</param>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task LeaderTermProbe_PassesOnlyForTheCurrentTerm(bool throughForwardingNode)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleCluster(Nodes, "memory", Partitions, raftLogger, kahunaLogger);

        try
        {
            KahunaManager probe = (KahunaManager)kahunas[0];
            string key = $"lgw-probe/{Guid.NewGuid().ToString("N")[..8]}";

            int partition = probe.KeyValues.LocateDurablePartition(key).PartitionId;
            int leader = await LeaderIndexOf(partition, rafts, ct);
            long term = rafts[leader].GetPartitionTerm(partition);

            IKahuna asker = kahunas[throughForwardingNode ? (leader + 1) % Nodes : leader];

            async Task<KeyValueResponseType> Probe(long expectedTerm)
            {
                List<(KeyValueResponseType type, string key, KeyValueDurability durability)> results = await RetryOnMustRetryAsync(
                    () => asker.LocateAndTryCheckManyWriteIntents(
                        HLCTimestamp.Zero,
                        [new KeyValueConflictProbe(key, KeyValueDurability.Persistent, KeyValueConflictChecks.LeaderTerm, expectedTerm)],
                        ct),
                    r => r[0].type);

                return Assert.Single(results).type;
            }

            Assert.Equal(KeyValueResponseType.DoesNotExist, await Probe(term));
            Assert.Equal(KeyValueResponseType.Aborted, await Probe(term - 1));
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    /// <summary>
    /// A probe that arrives over the wire is served by the node it was sent to, on the sender's belief of who
    /// leads. A follower shares the leader's term, so the term alone would let it vouch for a leadership it
    /// does not hold: only the confirmed leader may answer that the term stands.
    /// </summary>
    [Fact]
    public async Task LeaderTermProbeServedOverTheWire_IsAnsweredOnlyByTheConfirmedLeader()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleCluster(Nodes, "memory", Partitions, raftLogger, kahunaLogger);

        try
        {
            KahunaManager probe = (KahunaManager)kahunas[0];
            string key = $"lgw-served/{Guid.NewGuid().ToString("N")[..8]}";

            int partition = probe.KeyValues.LocateDurablePartition(key).PartitionId;
            int leader = await LeaderIndexOf(partition, rafts, ct);
            int follower = (leader + 1) % Nodes;
            long term = rafts[leader].GetPartitionTerm(partition);

            await WaitUntilAsync(() => Task.FromResult(rafts[follower].GetPartitionTerm(partition) == term), timeoutMs: 30_000);

            async Task<GrpcKeyValueResponseType> Serve(int node, long expectedTerm)
            {
                GrpcTryCheckManyWriteIntentsRequest request = new();
                request.Items.Add(new GrpcTryCheckManyWriteIntentsRequestItem
                {
                    Key = key,
                    Durability = GrpcKeyValueDurability.Persistent,
                    Checks = (uint)KeyValueConflictChecks.LeaderTerm,
                    BaseRevision = expectedTerm
                });

                GrpcTryCheckManyWriteIntentsResponse response = await ServiceOf(kahunas[node]).TryCheckManyWriteIntentsInternal(
                    GrpcTryCheckManyWriteIntentsRequest.Parser.ParseFrom(request.ToByteArray()), new StubServerCallContext());

                return Assert.Single(response.Items).Type;
            }

            Assert.Equal(GrpcKeyValueResponseType.TypeDoesNotExist, await Serve(leader, term));
            Assert.Equal(GrpcKeyValueResponseType.TypeAborted, await Serve(leader, term - 1));

            // The follower is in the same term and does not lead: it cannot say.
            Assert.Equal(GrpcKeyValueResponseType.TypeMustRetry, await Serve(follower, term));

            // A term the partition is already past is proof on any node.
            Assert.Equal(GrpcKeyValueResponseType.TypeAborted, await Serve(follower, term - 1));

            // A term no node has reached is not proof of anything.
            Assert.Equal(GrpcKeyValueResponseType.TypeMustRetry, await Serve(leader, term + 1));
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
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
