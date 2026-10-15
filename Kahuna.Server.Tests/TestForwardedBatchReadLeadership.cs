using Google.Protobuf;
using Grpc.Core;
using Kahuna.Communication.External.Grpc;
using Kahuna.Server.Communication;
using Kahuna.Server.Communication.Internode;
using Kahuna.Server.KeyValues;
using Kahuna.Server.Replication;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Time;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kahuna.Server.Tests;

/// <summary>
/// A batched point read forwarded between nodes lands on the node the sender believes leads the keys. The
/// belief is a hint or a replica guess, so the group can arrive at a follower, or at a node that stopped leading.
/// The receiving handler must route the keys again: serve a partition it leads after a confirmed leadership
/// check, forward one it does not lead to the leader, and never answer from its own actors on the sender's
/// word. These tests drive the inter-node get-many and exists-many handlers of the gRPC service on a follower
/// whose apply stream was held back, so its own state is provably behind the leader's.
/// </summary>
public sealed class TestForwardedBatchReadLeadership : BaseCluster
{
    private const int Nodes = 3;

    private const int Partitions = 4;

    private readonly ILogger<IRaft> raftLogger;

    private readonly ILogger<IKahuna> kahunaLogger;

    public TestForwardedBatchReadLeadership(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = TestLogFactory.Create(outputHelper);
        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    private static KeyValuesService ServiceOf(IKahuna kahuna) =>
        new(kahuna, NodeTransportGate.Disabled, NullLogger<IKahuna>.Instance);

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

    private static async Task<long> SetAsync(IKahuna kahuna, string key, string value, CancellationToken ct)
    {
        (KeyValueResponseType type, long revision, _) = await kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, key, System.Text.Encoding.UTF8.GetBytes(value), null, -1,
            KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);

        Assert.Equal(KeyValueResponseType.Set, type);
        return revision;
    }

    /// <summary>Reads the key from the node's own actors, bypassing routing, to show what that node holds.</summary>
    private static async Task<(KeyValueResponseType type, long revision, string? value)> LocalStateOf(IKahuna kahuna, string key)
    {
        (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) =
            await kahuna.TryGetValue(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent);

        return (type, entry?.Revision ?? 0, entry?.Value is null ? null : System.Text.Encoding.UTF8.GetString(entry.Value));
    }

    private static GrpcTryGetManyValuesRequest GetManyRequestFor(string key)
    {
        GrpcTryGetManyValuesRequest request = new();
        request.Items.Add(new GrpcTryManyValuesRequestItem { Key = key, Revision = -1, Durability = GrpcKeyValueDurability.Persistent });
        return GrpcTryGetManyValuesRequest.Parser.ParseFrom(request.ToByteArray());
    }

    private static GrpcTryExistsManyValuesRequest ExistsManyRequestFor(string key)
    {
        GrpcTryExistsManyValuesRequest request = new();
        request.Items.Add(new GrpcTryManyValuesRequestItem { Key = key, Revision = -1, Durability = GrpcKeyValueDurability.Persistent });
        return GrpcTryExistsManyValuesRequest.Parser.ParseFrom(request.ToByteArray());
    }

    /// <summary>
    /// Holds back every key-value apply of <paramref name="partition"/> on <paramref name="follower"/>, writes
    /// the key twice through the leader, and waits until the follower provably still holds the first value while
    /// the leader holds the second. The hold stays in place for the duration of the returned scope.
    /// </summary>
    private static async Task<(string key, long staleRevision, long headRevision)> StageStaleFollower(
        KahunaManager leader, KahunaManager follower, int partition, string key, CancellationToken ct)
    {
        long staleRevision = await SetAsync(leader, key, "v1", ct);

        // The first write of a fresh key is revision 0, the same number an absent key reports, so only the
        // value proves the follower applied it. Installing the hold before that apply would drop it too.
        await WaitUntilAsync(async () =>
        {
            (KeyValueResponseType type, long revision, string? value) = await LocalStateOf(follower, key);
            return type == KeyValueResponseType.Get && revision == staleRevision && value == "v1";
        }, timeoutMs: 30_000);

        follower.KeyValues.ReplicationDispatcher.ApplySkipForTesting =
            (p, log) => p == partition && log.LogType == ReplicationTypes.KeyValues;

        long headRevision = await SetAsync(leader, key, "v2", ct);
        Assert.True(headRevision > staleRevision);

        (KeyValueResponseType type, long revision, string? value) = await LocalStateOf(follower, key);
        Assert.Equal(KeyValueResponseType.Get, type);
        Assert.Equal(staleRevision, revision);
        Assert.Equal("v1", value);

        return (key, staleRevision, headRevision);
    }

    /// <summary>
    /// A get-many that a gateway forwarded to a follower must answer the leader's value, never the follower's
    /// own. Before the handler routed on arrival it served the local actors, so the stale value came back as a
    /// committed read, and a read-modify-write computed over it erased the writes the follower had not applied.
    /// </summary>
    [Fact]
    public async Task GetManyForwardedToAFollower_AnswersTheLeadersValueNotTheFollowersOwn()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleCluster(Nodes, "memory", Partitions, raftLogger, kahunaLogger);
        KahunaManager[] managers = [.. kahunas.Cast<KahunaManager>()];
        KahunaManager? held = null;

        try
        {
            string key = $"fwd-get/{Guid.NewGuid().ToString("N")[..8]}";
            int partition = managers[0].KeyValues.LocateDurablePartition(key).PartitionId;
            int leader = await LeaderIndexOf(partition, rafts, ct);
            int follower = (leader + 1) % Nodes;
            held = managers[follower];

            (_, long staleRevision, long headRevision) = await StageStaleFollower(managers[leader], managers[follower], partition, key, ct);

            // The sender already forwarded once: the receiving node has exactly one hop left to reach the leader.
            GrpcTryGetManyValuesResponse onFollower;
            using (ForwardedRequestScope.EnterAt(1))
                onFollower = await ServiceOf(kahunas[follower]).TryGetManyValuesInternal(GetManyRequestFor(key), new StubServerCallContext());

            GrpcTryGetManyValuesResponseItem item = Assert.Single(onFollower.Items);
            Assert.Equal(GrpcKeyValueResponseType.TypeGot, item.Type);
            Assert.Equal(headRevision, item.Revision);
            Assert.Equal("v2", item.Value.ToStringUtf8());
            Assert.NotEqual(staleRevision, item.Revision);

            // Served on the leader, the same request is answered from local state after the leadership check.
            GrpcTryGetManyValuesResponse onLeader;
            using (ForwardedRequestScope.EnterAt(1))
                onLeader = await ServiceOf(kahunas[leader]).TryGetManyValuesInternal(GetManyRequestFor(key), new StubServerCallContext());

            GrpcTryGetManyValuesResponseItem leaderItem = Assert.Single(onLeader.Items);
            Assert.Equal(GrpcKeyValueResponseType.TypeGot, leaderItem.Type);
            Assert.Equal(headRevision, leaderItem.Revision);
            Assert.Equal("v2", leaderItem.Value.ToStringUtf8());
        }
        finally
        {
            if (held is not null)
                held.KeyValues.ReplicationDispatcher.ApplySkipForTesting = null;

            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    /// <summary>
    /// The exists-many handler is the commit-time base check's wire shape: a follower that confirms a base the
    /// leader already moved past would let a stale write commit. It routes on arrival like the get-many.
    /// </summary>
    [Fact]
    public async Task ExistsManyForwardedToAFollower_AnswersTheLeadersRevisionNotTheFollowersOwn()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleCluster(Nodes, "memory", Partitions, raftLogger, kahunaLogger);
        KahunaManager[] managers = [.. kahunas.Cast<KahunaManager>()];
        KahunaManager? held = null;

        try
        {
            string key = $"fwd-exists/{Guid.NewGuid().ToString("N")[..8]}";
            int partition = managers[0].KeyValues.LocateDurablePartition(key).PartitionId;
            int leader = await LeaderIndexOf(partition, rafts, ct);
            int follower = (leader + 1) % Nodes;
            held = managers[follower];

            (_, long staleRevision, long headRevision) = await StageStaleFollower(managers[leader], managers[follower], partition, key, ct);

            GrpcTryExistsManyValuesResponse onFollower;
            using (ForwardedRequestScope.EnterAt(1))
                onFollower = await ServiceOf(kahunas[follower]).TryExistsManyValuesInternal(ExistsManyRequestFor(key), new StubServerCallContext());

            GrpcTryExistsManyValuesResponseItem item = Assert.Single(onFollower.Items);
            Assert.Equal(GrpcKeyValueResponseType.TypeExists, item.Type);
            Assert.Equal(headRevision, item.Revision);
            Assert.NotEqual(staleRevision, item.Revision);
        }
        finally
        {
            if (held is not null)
                held.KeyValues.ReplicationDispatcher.ApplySkipForTesting = null;

            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    /// <summary>
    /// A follower that has spent the whole hop budget may not forward onward; its only honest answer for a
    /// partition it does not lead is a retry, never its own state.
    /// </summary>
    [Fact]
    public async Task GetManyOnAFollowerPastTheHopBudget_AnswersMustRetryNotItsOwnState()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleCluster(Nodes, "memory", Partitions, raftLogger, kahunaLogger);
        KahunaManager[] managers = [.. kahunas.Cast<KahunaManager>()];
        KahunaManager? held = null;

        try
        {
            string key = $"fwd-budget/{Guid.NewGuid().ToString("N")[..8]}";
            int partition = managers[0].KeyValues.LocateDurablePartition(key).PartitionId;
            int leader = await LeaderIndexOf(partition, rafts, ct);
            int follower = (leader + 1) % Nodes;
            held = managers[follower];

            await StageStaleFollower(managers[leader], managers[follower], partition, key, ct);

            GrpcTryGetManyValuesResponse onFollower;
            using (ForwardedRequestScope.EnterAt(ForwardedRequestScope.MaxForwardHops))
                onFollower = await ServiceOf(kahunas[follower]).TryGetManyValuesInternal(GetManyRequestFor(key), new StubServerCallContext());

            GrpcTryGetManyValuesResponseItem item = Assert.Single(onFollower.Items);
            Assert.Equal(GrpcKeyValueResponseType.TypeMustRetry, item.Type);
        }
        finally
        {
            if (held is not null)
                held.KeyValues.ReplicationDispatcher.ApplySkipForTesting = null;

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
