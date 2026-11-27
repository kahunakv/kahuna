using System.Diagnostics;
using Kahuna.Server.Communication.Internode;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Data;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// The cluster shapes of a completion that the coordinator does not acknowledge. A session stays on the node that
/// began it when the coordinator partition changes leader, and a new leader finds the owner only by probing its
/// peers. A completion that the probe cannot deliver keeps the record pending on the owner; the participant sends it
/// again, detached from a caller that stopped retrying, until the owner folds it. A release that finds no session
/// on the new leader is not acknowledged, because the owner can still hold the record.
/// </summary>
public sealed class TestUnacknowledgedCompletionRetryCluster : BaseCluster
{
    private const int Nodes = 3;

    private const int Partitions = 4;

    // A session timeout well above the bound: a finalize that waits on a record nobody completes waits this long.
    private const int SessionTimeoutMs = 30_000;

    private const int FinalizeBoundMs = 5_000;

    private readonly ILogger<IRaft> raftLogger;

    private readonly ILogger<IKahuna> kahunaLogger;

    public TestUnacknowledgedCompletionRetryCluster(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = TestLogFactory.Create(outputHelper);
        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

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

    /// <summary>Moves the leadership and waits until every node names the new leader.</summary>
    private static async Task MoveLeadership(IRaft[] rafts, int partition, int from, int to, CancellationToken ct)
    {
        RaftOperationStatus status = await rafts[from].TransferLeadershipAsync(partition, EndpointOf(to), ct);
        Assert.Equal(RaftOperationStatus.Success, status);

        await WaitUntilAsync(async () => await rafts[to].AmILeaderIfHosted(partition, ct), timeoutMs: 30_000);
        await WaitUntilAsync(async () => !await rafts[from].AmILeaderIfHosted(partition, ct), timeoutMs: 30_000);

        foreach (IRaft raft in rafts)
            await WaitUntilAsync(async () => await raft.WaitForLeader(partition, ct) == EndpointOf(to), timeoutMs: 30_000);
    }

    /// <summary>Begins a pessimistic session on the leader of the coordinator partition of <paramref name="key"/>.</summary>
    private static async Task<(TransactionHandle Handle, int Partition, int Owner)> BeginOnItsCoordinator(
        IRaft[] rafts, KahunaManager[] managers, string key, CancellationToken ct)
    {
        int partition = managers[0].LocateRange(key).PartitionId;
        int leader = await LeaderIndexOf(partition, rafts, ct);

        (KeyValueResponseType startType, TransactionHandle handle) = await managers[leader].LocateAndStartTransaction(new()
        {
            CoordinatorKey = key,
            Locking = KeyValueTransactionLocking.Pessimistic,
            AsyncRelease = false,
            Timeout = SessionTimeoutMs
        }, ct);

        Assert.Equal(KeyValueResponseType.Set, startType);
        Assert.True(managers[leader].TransactionCoordinator.HasSession(handle.TransactionId));

        return (handle, partition, leader);
    }

    /// <summary>A fresh data key that routes to a partition other than <paramref name="partition"/>.</summary>
    private static string DataKeyOffPartition(KahunaManager manager, string prefix, int partition)
    {
        string runId = Guid.NewGuid().ToString("N")[..8];

        for (int i = 0; i < 256; i++)
        {
            string candidate = $"{prefix}-{runId}-{i}/k";
            if (manager.LocateRange(candidate).PartitionId != partition)
                return candidate;
        }

        throw new InvalidOperationException("no data key routes away from the coordinator partition");
    }

    private static int PendingOn(KahunaManager owner, TransactionHandle handle) =>
        owner.TransactionCoordinator.GetTransactionWorkingSet(handle.TransactionId)?.PendingOperationCount ?? -1;

    private static async Task<KeyValueResponseType> Write(KahunaManager entry, TransactionHandle handle, string key, TransactionOperationId op, CancellationToken ct)
    {
        (KeyValueResponseType type, _, _) = await entry.LocateAndTrySetKeyValue(
            handle.TransactionId, key, "never-visible"u8.ToArray(), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct,
            coordinatorKey: handle.CoordinatorKey, operationId: op);
        return type;
    }

    private static async Task AssertNotVisible(KahunaManager reader, string key, CancellationToken ct)
    {
        (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await RetryOnMustRetryAsync(
            async () => await reader.LocateAndTryGetValue(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct),
            r => r.Item1);
        Assert.True(type == KeyValueResponseType.DoesNotExist || entry?.Value is null, $"the rolled-back write is visible ({type})");
    }

    /// <summary>
    /// The coordinator partition changes leader between the participant work and its completion: the entry node
    /// (neither the owner nor the new leader) gets its first completion refused, as the move refuses it, and then
    /// the move happens. The caller stops. The rollback, sent from each of the three nodes in turn, ends in seconds,
    /// because the detached retry reaches the owner through the new leader.
    /// </summary>
    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    public async Task LeaderMovesBeforeTheCompletion_CallerStops_RollbackEndsInSeconds(int rollbackOffset)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleCluster(Nodes, "memory", Partitions, raftLogger, kahunaLogger);
        KahunaManager[] managers = [.. kahunas.Cast<KahunaManager>()];

        try
        {
            string key = "unack-move/" + Guid.NewGuid().ToString("N")[..8];
            (TransactionHandle handle, int partition, int owner) = await BeginOnItsCoordinator(rafts, managers, key, ct);

            int successor = (owner + 1) % Nodes;
            int entry = (owner + 2) % Nodes;
            string dataKey = DataKeyOffPartition(managers[0], "unack-move-data", partition);

            // Every completion of the write is refused until the move ends, so the retry that lands goes through the
            // new leader, which has no session and finds the owner among its peers.
            TransactionOperationId op = TransactionOperationId.NewRandom();
            bool moving = true;
            managers[entry].KeyValues.Registrar.TestCompletionRefusal = (_, operationId) => operationId == op && Volatile.Read(ref moving);

            KeyValueResponseType written = await Write(managers[entry], handle, dataKey, op, ct);
            Assert.Equal(KeyValueResponseType.MustRetry, written);

            await MoveLeadership(rafts, partition, owner, successor, ct);
            Assert.Equal(1, PendingOn(managers[owner], handle));
            Volatile.Write(ref moving, false);

            Stopwatch clock = Stopwatch.StartNew();
            KeyValueResponseType rolledBack = await managers[(owner + rollbackOffset) % Nodes].LocateAndRollbackTransaction(handle, ct);

            Assert.Equal(KeyValueResponseType.RolledBack, rolledBack);
            Assert.True(clock.ElapsedMilliseconds < FinalizeBoundMs, $"the rollback waited on the finalize drain: {clock.ElapsedMilliseconds} ms");
            Assert.False(managers[owner].TransactionCoordinator.HasSession(handle.TransactionId));

            await AssertNotVisible(managers[successor], dataKey, ct);
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    /// <summary>
    /// The new leader of the coordinator partition has no session, and its probe cannot reach the owner (every
    /// forwarded completion of the operation fails, as a cut link does). The new leader answers that it holds no
    /// session, and that does not end the retry: the record stays pending on the owner while the probe fails, and
    /// the retry folds the write once the owner is reachable again. The rollback then ends in seconds.
    /// </summary>
    [Fact]
    public async Task NewLeaderWithoutTheSession_CannotReachTheOwner_TheRetryGoesOn_AndFoldsOnceItCan()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleCluster(Nodes, "memory", Partitions, raftLogger, kahunaLogger);
        KahunaManager[] managers = [.. kahunas.Cast<KahunaManager>()];

        try
        {
            string key = "unack-cut/" + Guid.NewGuid().ToString("N")[..8];
            (TransactionHandle handle, int partition, int owner) = await BeginOnItsCoordinator(rafts, managers, key, ct);

            int successor = (owner + 1) % Nodes;
            await MoveLeadership(rafts, partition, owner, successor, ct);

            string dataKey = DataKeyOffPartition(managers[0], "unack-cut-data", partition);

            TransactionOperationId op = TransactionOperationId.NewRandom();
            bool cut = true;
            MemoryInterNodeCommmunication transport = Assert.IsType<MemoryInterNodeCommmunication>(managers[successor].KeyValues.InterNodeCommunication);
            transport.CompleteOperationFault = (_, operationId) => operationId == op && Volatile.Read(ref cut);

            try
            {
                // The write enters on the new leader: its registration reaches the owner through the probe, and its
                // completion cannot.
                KeyValueResponseType written = await Write(managers[successor], handle, dataKey, op, ct);
                Assert.Equal(KeyValueResponseType.MustRetry, written);
                Assert.Equal(1, PendingOn(managers[owner], handle));

                // Several attempts of the retry fail while the owner is cut off; the record stays pending.
                await Task.Delay(1_500, ct);
                Assert.Equal(1, managers[successor].KeyValues.Registrar.ActiveRedrives);
                Assert.Equal(1, PendingOn(managers[owner], handle));

                Volatile.Write(ref cut, false);

                await WaitUntilAsync(() => PendingOn(managers[owner], handle) == 0, timeoutMs: 5_000);
                Assert.Contains(managers[owner].TransactionCoordinator.GetTransactionWorkingSet(handle.TransactionId)!.ModifiedKeys!, m => m.Item1 == dataKey);
            }
            finally
            {
                transport.CompleteOperationFault = null;
            }

            Stopwatch clock = Stopwatch.StartNew();
            Assert.Equal(KeyValueResponseType.RolledBack, await managers[successor].LocateAndRollbackTransaction(handle, ct));
            Assert.True(clock.ElapsedMilliseconds < FinalizeBoundMs, $"the rollback waited on the finalize drain: {clock.ElapsedMilliseconds} ms");

            await AssertNotVisible(managers[owner], dataKey, ct);
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    /// <summary>
    /// The work of an operation throws, and the release of the registration cannot reach the owner: the owner
    /// probe of the new leader fails (only the probe; a plain forward to the new leader still lands). The new
    /// leader holds no session, so it does not acknowledge the release: an acknowledgement would leave the record
    /// pending on the owner with nothing kept to end it. The detached retry sends the release again, and once the
    /// rollback closes the session on the owner the release ends the record, so the rollback ends in seconds. The
    /// operation enters on the new leader (its own route answers) or on the third node (the new leader answers the
    /// forwarded release).
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ReleaseThatTheNewLeaderCannotDeliver_IsNotAcknowledged_AndTheRollbackEndsOnceItLands(bool throughTheThirdNode)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleCluster(Nodes, "memory", Partitions, raftLogger, kahunaLogger);
        KahunaManager[] managers = [.. kahunas.Cast<KahunaManager>()];

        try
        {
            string key = "unack-release/" + Guid.NewGuid().ToString("N")[..8];
            (TransactionHandle handle, int partition, int owner) = await BeginOnItsCoordinator(rafts, managers, key, ct);

            int successor = (owner + 1) % Nodes;
            await MoveLeadership(rafts, partition, owner, successor, ct);

            int entry = throughTheThirdNode ? (owner + 2) % Nodes : successor;
            string dataKey = DataKeyOffPartition(managers[0], "unack-release-data", partition);
            string bucket = dataKey[..dataKey.LastIndexOf('/')];

            // A probe sends with the forward budget spent, so only the probe of the new leader fails.
            TransactionOperationId op = TransactionOperationId.NewRandom();
            bool cut = true;
            MemoryInterNodeCommmunication transport = Assert.IsType<MemoryInterNodeCommmunication>(managers[successor].KeyValues.InterNodeCommunication);
            transport.CompleteOperationFault = (_, operationId) => operationId == op && Volatile.Read(ref cut) && !ForwardedRequestScope.CanForward;

            try
            {
                await Assert.ThrowsAnyAsync<OperationCanceledException>(() => managers[entry].RegisterAndAcquireRangeLockWithHook(
                    handle.TransactionId, handle.CoordinatorKey, op, bucket, null, true, null, false,
                    30_000, KeyValueDurability.Persistent, RangeLockMode.Exclusive,
                    afterSnapshot: () => Task.FromException(new OperationCanceledException()),
                    cancellationToken: ct));

                Assert.Equal(1, PendingOn(managers[owner], handle));
                Assert.Equal(1, managers[entry].KeyValues.Registrar.ActiveRedrives);

                Volatile.Write(ref cut, false);
            }
            finally
            {
                transport.CompleteOperationFault = null;
            }

            // The owner is reachable now, and the session still takes operations: the owner defers each copy of the
            // release, so the record stays pending and the retry goes on.
            await Task.Delay(1_500, ct);
            Assert.Equal(1, PendingOn(managers[owner], handle));
            Assert.Equal(1, managers[entry].KeyValues.Registrar.ActiveRedrives);

            Stopwatch clock = Stopwatch.StartNew();
            Assert.Equal(KeyValueResponseType.RolledBack, await managers[successor].LocateAndRollbackTransaction(handle, ct));
            Assert.True(clock.ElapsedMilliseconds < FinalizeBoundMs, $"the rollback waited on the finalize drain: {clock.ElapsedMilliseconds} ms");

            await WaitUntilAsync(() => managers[entry].KeyValues.Registrar.ActiveRedrives == 0, timeoutMs: 5_000);
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }
}
