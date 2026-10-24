using System.Text;
using Google.Protobuf;
using Grpc.Core;
using Kahuna.Communication.External.Grpc;
using Kahuna.Server.Communication;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Data;
using Kommander.Time;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kahuna.Server.Tests;

/// <summary>
/// A caller whose data is not placed by the hash rule (a key-range table lives on its range descriptor's
/// partition) chooses the partition whose leader hosts its transaction's coordinator session. The start pins
/// the coordinator key to that partition, and every later call that carries the key (commit, rollback,
/// operation registration) routes to it without hashing the key again or reading the range map.
/// </summary>
public sealed class TestCoordinatorPartitionPinning : BaseCluster
{
    private const int Nodes = 3;

    private const int Partitions = 6;

    private const string Space = "pin:s";

    private const string SplitKey = Space + "/m";

    private readonly ILogger<IRaft> raftLogger;

    private readonly ILogger<IKahuna> kahunaLogger;

    public TestCoordinatorPartitionPinning(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = TestLogFactory.Create(outputHelper);
        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    private static byte[] V(string s) => Encoding.UTF8.GetBytes(s);

    private static string EndpointOf(int index) => $"localhost:{8001 + index}";

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

    private static async Task MoveLeadership(IRaft[] rafts, int partition, int from, int to, CancellationToken ct)
    {
        RaftOperationStatus status = await rafts[from].TransferLeadershipAsync(partition, EndpointOf(to), ct);
        Assert.Equal(RaftOperationStatus.Success, status);

        await WaitUntilAsync(async () => await rafts[to].AmILeaderIfHosted(partition, ct), timeoutMs: 30_000);
        await WaitUntilAsync(async () => !await rafts[from].AmILeaderIfHosted(partition, ct), timeoutMs: 30_000);
    }

    private static KeyValueTransactionOptions Options(string coordinatorKey, int? coordinatorPartitionId) => new()
    {
        CoordinatorKey = coordinatorKey,
        CoordinatorPartitionId = coordinatorPartitionId,
        Locking = KeyValueTransactionLocking.Optimistic,
        ReadValidation = ReadValidation.TrackAndValidate,
        AsyncRelease = true,
        Timeout = 60_000
    };

    private static async Task<TransactionHandle> Start(KahunaManager node, string coordinatorKey, int? coordinatorPartitionId, CancellationToken ct)
    {
        (KeyValueResponseType startType, TransactionHandle handle) = await RetryOnMustRetryAsync(
            () => node.LocateAndStartTransaction(Options(coordinatorKey, coordinatorPartitionId), ct), r => r.Item1);

        Assert.Equal(KeyValueResponseType.Set, startType);
        Assert.False(handle.IsEmpty);
        return handle;
    }

    private static async Task Write(KahunaManager node, TransactionHandle handle, string key, string value, CancellationToken ct)
    {
        (KeyValueResponseType writeType, _, _) = await RetryOnMustRetryAsync(
            () => node.LocateAndTrySetKeyValue(
                handle.TransactionId, key, V(value), null, -1, KeyValueFlags.None, 0,
                KeyValueDurability.Persistent, ct,
                coordinatorKey: handle.CoordinatorKey, operationId: TransactionOperationId.NewRandom()),
            r => r.Item1, timeoutMs: 30_000);

        Assert.Equal(KeyValueResponseType.Set, writeType);
    }

    private static async Task AssertValueVisible(KahunaManager reader, string key, string expected, CancellationToken ct)
    {
        await WaitUntilAsync(async () =>
        {
            (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await reader.LocateAndTryGetValue(
                HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
            return type == KeyValueResponseType.Get && entry?.Value is not null
                && Encoding.UTF8.GetString(entry.Value) == expected;
        }, timeoutMs: 30_000);
    }

    /// <summary>The indexes of the nodes whose coordinator holds the session for <paramref name="transactionId"/>.</summary>
    private static List<int> SessionHolders(KahunaManager[] managers, HLCTimestamp transactionId)
    {
        List<int> holders = [];

        for (int i = 0; i < managers.Length; i++)
            if (managers[i].TransactionCoordinator.HasSession(transactionId))
                holders.Add(i);

        return holders;
    }

    /// <summary>
    /// A coordinator key and a partition that the key does not hash to, led by another node than the key's hash
    /// partition, so a session that lands on the chosen partition's leader proves the key was not hashed.
    /// </summary>
    private static async Task<(string CoordinatorKey, int Chosen, int ChosenLeader)> KeyAndPartitionLedElsewhere(
        IRaft[] rafts, KahunaManager[] managers, CancellationToken ct)
    {
        string runId = Guid.NewGuid().ToString("N")[..8];

        for (int i = 0; i < 256; i++)
        {
            string candidate = $"pin-coord{i}/{runId}";
            int hashed = managers[0].KeyValues.LocateCoordinatorPartition(candidate);
            int hashedLeader = await LeaderIndexOf(hashed, rafts, ct);

            for (int partition = 1; partition <= Partitions; partition++)
            {
                if (partition == hashed)
                    continue;

                int leader = await LeaderIndexOf(partition, rafts, ct);
                if (leader != hashedLeader)
                    return (candidate, partition, leader);
            }
        }

        throw new InvalidOperationException("no partition is led by another node than the key's hash partition");
    }

    /// <summary>
    /// A session started with a chosen partition is hosted by that partition's leader even when the start is
    /// sent to another node, and its handle names the partition. Driven from the partition's leader, the
    /// operation registrations, the commit and the rollback are all served there with no forward.
    /// </summary>
    [Fact]
    public async Task PinnedSession_IsHostedByTheChosenPartitionsLeader_AndServedThereWithoutForwards()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleCluster(
            Nodes, "memory", Partitions, raftLogger, kahunaLogger, replicationFactor: 1);

        KahunaManager[] managers = [.. kahunas.Cast<KahunaManager>()];

        try
        {
            (string coordinatorKey, int chosen, int chosenLeader) = await KeyAndPartitionLedElsewhere(rafts, managers, ct);
            int starter = (chosenLeader + 1) % Nodes;

            TransactionHandle handle = await Start(managers[starter], coordinatorKey, chosen, ct);

            Assert.Equal(chosen, handle.CoordinatorPartitionId);
            Assert.Equal(CoordinatorKeys.Pin(chosen, coordinatorKey), handle.CoordinatorKey);
            Assert.Equal(new List<int> { chosenLeader }, SessionHolders(managers, handle.TransactionId));

            foreach (KahunaManager manager in managers)
                Assert.Equal(chosen, manager.KeyValues.LocateCoordinatorPartition(handle.CoordinatorKey));

            KahunaManager session = managers[chosenLeader];
            string dataKey = $"pin-data/{Guid.NewGuid():N}";

            SessionRegistrationCounts before = session.KeyValues.SessionRegistrationCounts;

            await Write(session, handle, dataKey, "pinned", ct);

            (KeyValueResponseType commitType, _) = await session.LocateAndCommitTransaction(handle, ct);
            Assert.Equal(KeyValueResponseType.Committed, commitType);

            SessionRegistrationCounts after = session.KeyValues.SessionRegistrationCounts;
            Assert.Equal(0, after.BeginForwarded - before.BeginForwarded);
            Assert.Equal(0, after.CompleteForwarded - before.CompleteForwarded);
            Assert.Equal(0, after.Unrouted - before.Unrouted);
            Assert.True(after.BeginLocal > before.BeginLocal, "the registration must be served on the session's node");

            await AssertValueVisible(managers[starter], dataKey, "pinned", ct);

            // A rollback of a second pinned session is served on the same leader.
            TransactionHandle rolledBack = await Start(managers[starter], coordinatorKey + "-rb", chosen, ct);
            Assert.Equal(new List<int> { chosenLeader }, SessionHolders(managers, rolledBack.TransactionId));

            await Write(session, rolledBack, $"pin-data/{Guid.NewGuid():N}", "rolled-back", ct);

            KeyValueResponseType rollbackType = await session.LocateAndRollbackTransaction(rolledBack, ct);
            Assert.Equal(KeyValueResponseType.RolledBack, rollbackType);
            Assert.Empty(SessionHolders(managers, rolledBack.TransactionId));
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    /// <summary>
    /// After the chosen partition's leadership moves away from the node that holds the session, a commit sent to
    /// the new leader still reaches the session: the new leader does not hold it and offers it to its peers.
    /// </summary>
    [Fact]
    public async Task PinnedSession_CommitSentToTheNewLeader_ReachesTheSessionAfterLeadershipMoved()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleCluster(Nodes, "memory", Partitions, raftLogger, kahunaLogger);
        KahunaManager[] managers = [.. kahunas.Cast<KahunaManager>()];

        try
        {
            const int chosen = 2;

            string coordinatorKey = $"pin-move/{Guid.NewGuid():N}";
            int owner = await LeaderIndexOf(chosen, rafts, ct);

            TransactionHandle handle = await Start(managers[owner], coordinatorKey, chosen, ct);
            Assert.Equal(new List<int> { owner }, SessionHolders(managers, handle.TransactionId));

            // The staged write lives in its own partition leader's memory, so it is placed off the chosen
            // partition: only the coordinator's leadership moves.
            string dataKey = "";
            for (int i = 0; i < 256 && dataKey.Length == 0; i++)
            {
                string candidate = $"pin-move-data-{i}-{Guid.NewGuid():N}/k";
                if (managers[owner].LocateRange(candidate).PartitionId != chosen)
                    dataKey = candidate;
            }

            Assert.NotEmpty(dataKey);

            await Write(managers[owner], handle, dataKey, "moved-then-committed", ct);

            int successor = (owner + 1) % Nodes;
            await MoveLeadership(rafts, chosen, owner, successor, ct);

            (KeyValueResponseType commitType, _) = await RetryOnMustRetryAsync(
                () => managers[successor].LocateAndCommitTransaction(handle, ct), r => r.Type);

            Assert.Equal(KeyValueResponseType.Committed, commitType);

            foreach (KahunaManager reader in managers)
                await AssertValueVisible(reader, dataKey, "moved-then-committed", ct);
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    /// <summary>
    /// A split of the caller's key range between the start and the commit moves the upper half of the range to a
    /// new partition, but not the session: the pinned key still routes to the partition the session started on,
    /// and the transaction commits writes on both sides of the split.
    /// </summary>
    [Fact]
    public async Task PinnedSession_StaysOnItsPartition_WhenTheCallersRangeSplitsBeforeCommit()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleCluster(
            Nodes, "memory", Partitions, raftLogger, kahunaLogger, replicationFactor: 1);

        KahunaManager[] managers = [.. kahunas.Cast<KahunaManager>()];

        try
        {
            foreach (KahunaManager kahuna in managers)
                kahuna.RegisterKeyRange(Space);

            int metaLeader = await LeaderIndexOf(RangeMapStore.MetaPartitionId, rafts, ct);

            // The range's partition is placed away from the split driver, as a key-range table's partition
            // generally is.
            int sourcePartition = 0;
            for (int partitionId = 1; partitionId <= Partitions && sourcePartition == 0; partitionId++)
                if (!rafts[metaLeader].HostsPartition(partitionId))
                    sourcePartition = partitionId;

            Assert.NotEqual(0, sourcePartition);

            bool seeded = await managers[metaLeader].RangeMapStore.MutateAsync(
                _ => [new RangeDescriptor { KeySpace = Space, PartitionId = sourcePartition, Generation = 1 }], ct);
            Assert.True(seeded);

            foreach (KahunaManager kahuna in managers)
                await WaitUntilAsync(
                    () => kahuna.RangeMapStore.Current.Find(Space, Space + "/x")?.Generation == 1, timeoutMs: 30_000);

            // The splitter refuses a split that leaves an empty half.
            foreach (string key in new[] { Space + "/a0", Space + "/a1", Space + "/z0", Space + "/z1" })
            {
                (KeyValueResponseType type, _, _) = await RetryOnMustRetryAsync(
                    () => managers[metaLeader].LocateAndTrySetKeyValue(
                        HLCTimestamp.Zero, key, V(key), null, -1, KeyValueFlags.Set, 0,
                        KeyValueDurability.Persistent, ct),
                    r => r.Item1);

                Assert.Equal(KeyValueResponseType.Set, type);
            }

            // The caller reads the range's partition from the range map and pins its session there.
            int chosen = managers[metaLeader].RangeMapStore.Current.Find(Space, Space + "/a5")!.PartitionId;
            Assert.Equal(sourcePartition, chosen);

            int sessionNode = await LeaderIndexOf(chosen, rafts, ct);
            TransactionHandle handle = await Start(managers[metaLeader], $"{Space}|tx/{Guid.NewGuid():N}", chosen, ct);
            Assert.Equal(new List<int> { sessionNode }, SessionHolders(managers, handle.TransactionId));

            SplitOutcome outcome = SplitOutcome.PartitionCreationFailed;

            for (int attempt = 0; attempt < 5; attempt++)
            {
                int driver = await LeaderIndexOf(RangeMapStore.MetaPartitionId, rafts, ct);

                outcome = await managers[driver].ForceSplitAtKeyAsync(Space, SplitKey, duringQuiesce: null, ct);

                if (outcome.IsSuccess || outcome.Status is SplitStatus.NoRange or SplitStatus.InvalidSplitKey or SplitStatus.BelowMinRangeSize)
                    break;

                await Task.Delay(100, ct);
            }

            Assert.True(outcome.IsSuccess, $"Split failed: {outcome.Status}");

            foreach (KahunaManager kahuna in managers)
                await WaitUntilAsync(
                    () => kahuna.RangeMapStore.Current.Find(Space, Space + "/z5")?.PartitionId is { } p && p != sourcePartition,
                    timeoutMs: 30_000);

            // The range moved; the session did not.
            Assert.Equal(sourcePartition, handle.CoordinatorPartitionId);
            Assert.Equal(new List<int> { sessionNode }, SessionHolders(managers, handle.TransactionId));

            foreach (KahunaManager manager in managers)
                Assert.Equal(sourcePartition, manager.KeyValues.LocateCoordinatorPartition(handle.CoordinatorKey));

            KahunaManager session = managers[sessionNode];

            await Write(session, handle, Space + "/a5", "lower", ct);
            await Write(session, handle, Space + "/z5", "upper", ct);

            (KeyValueResponseType commitType, _) = await RetryOnMustRetryAsync(
                () => managers[metaLeader].LocateAndCommitTransaction(handle, ct), r => r.Type, timeoutMs: 30_000);

            Assert.Equal(KeyValueResponseType.Committed, commitType);

            foreach (KahunaManager reader in managers)
            {
                await AssertValueVisible(reader, Space + "/a5", "lower", ct);
                await AssertValueVisible(reader, Space + "/z5", "upper", ct);
            }
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    /// <summary>
    /// An id that names no data partition, a malformed pinned key, and a pinned key that disagrees with the
    /// requested partition are each refused as invalid input with nothing started; none of them falls back to
    /// the hash rule. A pinned key the caller composed itself is accepted as the request for its partition.
    /// </summary>
    [Fact]
    public async Task InvalidCoordinatorPartition_IsRefused_NotHashed()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleCluster(Nodes, "memory", Partitions, raftLogger, kahunaLogger);
        KahunaManager[] managers = [.. kahunas.Cast<KahunaManager>()];

        try
        {
            string key = $"pin-invalid/{Guid.NewGuid():N}";

            (string CoordinatorKey, int? Partition)[] refused =
            [
                (key, 0),
                (key, -1),
                (key, 10_000),
                ("\u0001p/x/" + key, null),
                ("\u0001p/3/", null),
                ("\u0001p/0/" + key, null),
                ("\u0001" + key, null),
                (CoordinatorKeys.Pin(2, key), 3),
            ];

            foreach (KahunaManager manager in managers)
            {
                foreach ((string coordinatorKey, int? partition) in refused)
                {
                    (KeyValueResponseType type, TransactionHandle handle) = await manager.LocateAndStartTransaction(
                        Options(coordinatorKey, partition), ct);

                    Assert.Equal(KeyValueResponseType.InvalidInput, type);
                    Assert.True(handle.IsEmpty);
                }
            }

            TransactionHandle composed = await Start(managers[0], CoordinatorKeys.Pin(3, key), null, ct);
            Assert.Equal(3, composed.CoordinatorPartitionId);
            Assert.Equal(CoordinatorKeys.Pin(3, key), composed.CoordinatorKey);

            TransactionHandle agreeing = await Start(managers[0], CoordinatorKeys.Pin(3, key + "-b"), 3, ct);
            Assert.Equal(3, agreeing.CoordinatorPartitionId);
            Assert.Equal(CoordinatorKeys.Pin(3, key + "-b"), agreeing.CoordinatorKey);

            foreach (TransactionHandle handle in new[] { composed, agreeing })
                Assert.Equal(KeyValueResponseType.RolledBack, await RetryOnMustRetryAsync(
                    () => managers[0].LocateAndRollbackTransaction(handle, ct), r => r));
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    /// <summary>
    /// A start that chooses no partition keeps the hash rule: the handle carries the caller's key unchanged and no
    /// partition, which is also the handle a node that predates pinning returns, and it commits through the hash
    /// rule from any node.
    /// </summary>
    [Fact]
    public async Task UnpinnedSession_KeepsTheHashRule()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleCluster(
            Nodes, "memory", Partitions, raftLogger, kahunaLogger, replicationFactor: 1);

        KahunaManager[] managers = [.. kahunas.Cast<KahunaManager>()];

        try
        {
            string coordinatorKey = $"pin-plain/{Guid.NewGuid():N}";
            int hashed = managers[0].KeyValues.LocateCoordinatorPartition(coordinatorKey);
            int hashedLeader = await LeaderIndexOf(hashed, rafts, ct);

            TransactionHandle handle = await Start(managers[(hashedLeader + 1) % Nodes], coordinatorKey, null, ct);

            Assert.Equal(coordinatorKey, handle.CoordinatorKey);
            Assert.Null(handle.CoordinatorPartitionId);
            Assert.Equal(new List<int> { hashedLeader }, SessionHolders(managers, handle.TransactionId));

            string dataKey = $"pin-plain-data/{Guid.NewGuid():N}";
            int driver = (hashedLeader + 2) % Nodes;

            await Write(managers[driver], handle, dataKey, "hashed", ct);

            (KeyValueResponseType commitType, _) = await RetryOnMustRetryAsync(
                () => managers[driver].LocateAndCommitTransaction(new TransactionHandle(handle.TransactionId, coordinatorKey), ct), r => r.Type);

            Assert.Equal(KeyValueResponseType.Committed, commitType);
            await AssertValueVisible(managers[driver], dataKey, "hashed", ct);
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    /// <summary>
    /// The gRPC start carries the chosen partition across a real encode and decode, and the response returns
    /// the pinned key the caller must send on every later call.
    /// </summary>
    [Fact]
    public async Task GrpcStart_CarriesThePartition_AndReturnsThePinnedKey()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleCluster(Nodes, "memory", Partitions, raftLogger, kahunaLogger);
        KahunaManager[] managers = [.. kahunas.Cast<KahunaManager>()];

        try
        {
            KeyValuesService service = new(managers[0], NodeTransportGate.Disabled, NullLogger<IKahuna>.Instance);
            string coordinatorKey = $"pin-grpc/{Guid.NewGuid():N}";

            GrpcStartTransactionRequest request = new()
            {
                CoordinatorKey = coordinatorKey,
                CoordinatorPartitionId = 4,
                LockingType = GrpcLockingType.LockingTypeOptimistic,
                Timeout = 60_000
            };

            GrpcStartTransactionRequest parsed = GrpcStartTransactionRequest.Parser.ParseFrom(request.ToByteArray());
            Assert.True(parsed.HasCoordinatorPartitionId);

            GrpcStartTransactionResponse response = null!;
            await WaitUntilAsync(async () =>
            {
                response = await service.StartTransactionInternal(parsed, new StubServerCallContext());
                return response.Type != GrpcKeyValueResponseType.TypeMustRetry;
            }, timeoutMs: 30_000);

            GrpcStartTransactionResponse decoded = GrpcStartTransactionResponse.Parser.ParseFrom(response.ToByteArray());

            Assert.Equal(GrpcKeyValueResponseType.TypeSet, decoded.Type);
            Assert.True(decoded.HasCoordinatorKey);
            Assert.Equal(CoordinatorKeys.Pin(4, coordinatorKey), decoded.CoordinatorKey);

            TransactionHandle handle = new(
                new HLCTimestamp(decoded.TransactionIdNode, decoded.TransactionIdPhysical, decoded.TransactionIdCounter),
                decoded.CoordinatorKey);

            int leader = await LeaderIndexOf(4, rafts, ct);
            Assert.Equal(new List<int> { leader }, SessionHolders(managers, handle.TransactionId));

            Assert.Equal(KeyValueResponseType.RolledBack, await RetryOnMustRetryAsync(
                () => managers[0].LocateAndRollbackTransaction(handle, ct), r => r));
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    [Fact]
    public void PinnedKey_RoundTripsItsPartition()
    {
        string pinned = CoordinatorKeys.Pin(17, "group|tx/abc");

        Assert.True(CoordinatorKeys.IsReserved(pinned));
        Assert.True(CoordinatorKeys.TryGetPinnedPartition(pinned, out int partition));
        Assert.Equal(17, partition);
        Assert.Equal(17, new TransactionHandle(new HLCTimestamp(1, 1, 1), pinned).CoordinatorPartitionId);
    }

    [Theory]
    [InlineData("group|tx/abc")]
    [InlineData("")]
    [InlineData("p/3/abc")]
    [InlineData("\u0001p/")]
    [InlineData("\u0001p/3")]
    [InlineData("\u0001p/3/")]
    [InlineData("\u0001p//abc")]
    [InlineData("\u0001p/0/abc")]
    [InlineData("\u0001p/-3/abc")]
    [InlineData("\u0001p/+3/abc")]
    [InlineData("\u0001p/ 3/abc")]
    [InlineData("\u0001p/99999999999/abc")]
    [InlineData("\u0001q/3/abc")]
    public void NonPinnedOrMalformedKey_NamesNoPartition(string coordinatorKey)
    {
        Assert.False(CoordinatorKeys.TryGetPinnedPartition(coordinatorKey, out _));
        Assert.Null(new TransactionHandle(new HLCTimestamp(1, 1, 1), coordinatorKey).CoordinatorPartitionId);
    }

    /// <summary>Minimal context: the handlers read only the cancellation token.</summary>
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
