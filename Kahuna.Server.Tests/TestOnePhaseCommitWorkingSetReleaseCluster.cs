using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Data;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// The working-set release of a one-phase commit on three nodes. Only an apply on the committing node can report
/// that it removed the transaction's intent: the apply that is forwarded to another leader answers a bool. So a
/// commit whose keys are led by the committing node sends no per-key release, and a commit whose keys are led by
/// another node sends every one, as before. A leader change after the commit finds nothing of the transaction on
/// the new leader.
/// </summary>
public sealed class TestOnePhaseCommitWorkingSetReleaseCluster : BaseCluster
{
    private const int Nodes = 3;

    private const int Partitions = 4;

    private const string CallMetric = "kahuna.transactions.commit_call_ms";

    private readonly ILogger<IRaft> raftLogger;

    private readonly ILogger<IKahuna> kahunaLogger;

    public TestOnePhaseCommitWorkingSetReleaseCluster(ITestOutputHelper outputHelper)
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

    /// <summary>Moves the leadership of the partition and waits until every node names the new leader.</summary>
    private static async Task MoveLeadership(IRaft[] rafts, int partition, int from, int to, CancellationToken ct)
    {
        RaftOperationStatus status = await rafts[from].TransferLeadershipAsync(partition, EndpointOf(to), ct);
        Assert.Equal(RaftOperationStatus.Success, status);

        await WaitUntilAsync(async () => await rafts[to].AmILeaderIfHosted(partition, ct), timeoutMs: 30_000);
        await WaitUntilAsync(async () => !await rafts[from].AmILeaderIfHosted(partition, ct), timeoutMs: 30_000);

        foreach (IRaft raft in rafts)
            await WaitUntilAsync(async () => await raft.WaitForLeader(partition, ct) == EndpointOf(to), timeoutMs: 30_000);
    }

    private Task<(IRaft[] Rafts, IKahuna[] Kahunas)> Assemble() =>
        AssembleCluster(Nodes, "memory", Partitions, raftLogger, kahunaLogger,
            // The bundled commit validates at its apply, as the consumer configures it, so a write keeps the
            // one-phase path on a group of several processes.
            configureKahuna: static configuration => configuration.OnePhaseApplyTimeValidation = true);

    private static async Task<TransactionHandle> Start(KahunaManager session, string coordinatorKey, CancellationToken ct)
    {
        (KeyValueResponseType type, TransactionHandle handle) = await session.LocateAndStartTransaction(
            new KeyValueTransactionOptions
            {
                CoordinatorKey = coordinatorKey,
                Locking = KeyValueTransactionLocking.Optimistic,
                ReadValidation = ReadValidation.TrackAndValidate,
                AsyncRelease = false,
                Timeout = 60_000
            }, ct);

        Assert.Equal(KeyValueResponseType.Set, type);
        return handle;
    }

    private static async Task Write(KahunaManager session, TransactionHandle tx, string key, CancellationToken ct)
    {
        (KeyValueResponseType type, _, _) = await RetryOnMustRetryAsync(
            () => session.LocateAndTrySetKeyValue(
                tx.TransactionId, key, "v"u8.ToArray(), null, -1, KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct,
                coordinatorKey: tx.CoordinatorKey, operationId: TransactionOperationId.NewRandom()),
            static r => r.Item1);

        Assert.Equal(KeyValueResponseType.Set, type);
    }

    private static int OwnerOf(KahunaManager[] managers, TransactionHandle handle)
    {
        int owner = -1;
        for (int i = 0; i < managers.Length; i++)
        {
            if (managers[i].TransactionCoordinator.HasSession(handle.TransactionId))
                owner = i;
        }

        Assert.True(owner >= 0, "some node must hold the session");
        return owner;
    }

    private static async Task<bool> HoldsNothing(KahunaManager manager, HLCTimestamp transactionId, string key) =>
        (await manager.KeyValues.TryReleaseExclusiveLockHeldHere(transactionId, key, KeyValueDurability.Persistent)).NothingHeld;

    private static (long Sent, long LeftToCommitApply) ReleaseCounts(KahunaManager manager) =>
        (manager.TransactionCoordinator.PointReleasesSent, manager.TransactionCoordinator.PointReleasesLeftToCommitApply);

    /// <summary>
    /// The committing node leads the written keys: the commit is one-phase, its apply runs on that node, and no
    /// per-key release is sent. The leadership then moves, and the new leader holds nothing of the transaction:
    /// the next transaction writes the same keys there.
    /// </summary>
    [Fact]
    public async Task CommitOnTheLeaderOfItsKeys_SendsNoPerKeyRelease_AndANewLeaderHoldsNothingOfIt()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, IKahuna[] kahunas) = await Assemble();
        KahunaManager[] managers = [.. kahunas.Cast<KahunaManager>()];
        using MetricCapture capture = new("path", CallMetric);

        try
        {
            string bucket = "rel-local-" + Guid.NewGuid().ToString("N")[..8];
            string[] keys = [$"{bucket}/a", $"{bucket}/b", $"{bucket}/c"];
            int partition = managers[0].LocateRange(keys[0]).PartitionId;
            int leader = await LeaderIndexOf(partition, rafts, ct);

            TransactionHandle tx = await Start(managers[leader], bucket + "/tx", ct);
            Assert.Equal(leader, OwnerOf(managers, tx));

            foreach (string key in keys)
                await Write(managers[leader], tx, key, ct);

            (long sentBefore, long leftBefore) = ReleaseCounts(managers[leader]);
            long onePhaseBefore = managers[leader].TransactionCoordinator.CommitCalls.Calls(CommitCallPath.OnePhase);

            Assert.Equal(KeyValueResponseType.Committed, (await managers[leader].LocateAndCommitTransaction(tx, ct)).Type);

            (long sent, long left) = ReleaseCounts(managers[leader]);
            Assert.Equal(1, managers[leader].TransactionCoordinator.CommitCalls.Calls(CommitCallPath.OnePhase) - onePhaseBefore);
            Assert.Equal(0, sent - sentBefore);
            Assert.Equal(keys.Length, left - leftBefore);

            foreach (string key in keys)
                Assert.True(await HoldsNothing(managers[leader], tx.TransactionId, key), $"the committer still holds something on {key}");

            int successor = (leader + 1) % Nodes;
            await MoveLeadership(rafts, partition, leader, successor, ct);

            foreach (string key in keys)
                Assert.True(await HoldsNothing(managers[successor], tx.TransactionId, key), $"the new leader holds something of the committer on {key}");

            TransactionHandle next = await Start(managers[successor], bucket + "/tx", ct);
            foreach (string key in keys)
                await Write(managers[successor], next, key, ct);

            (KeyValueResponseType nextCommit, string? _) = await RetryOnMustRetryAsync(
                () => managers[OwnerOf(managers, next)].LocateAndCommitTransaction(next, ct), static r => r.Type);
            Assert.Equal(KeyValueResponseType.Committed, nextCommit);
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    /// <summary>
    /// The written keys are led by another node than the one that holds the session: the bundle and its apply
    /// are forwarded to that leader, which answers a bool for the apply. The committing node cannot know that the
    /// intent was removed, so it sends the release of every key, and the leader then holds nothing of the
    /// transaction.
    /// </summary>
    [Fact]
    public async Task CommitOnANodeThatDoesNotLeadItsKeys_SendsEveryPerKeyRelease()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, IKahuna[] kahunas) = await Assemble();
        KahunaManager[] managers = [.. kahunas.Cast<KahunaManager>()];
        using MetricCapture capture = new("path", CallMetric);

        try
        {
            string bucket = "rel-remote-" + Guid.NewGuid().ToString("N")[..8];
            string[] keys = [$"{bucket}/a", $"{bucket}/b", $"{bucket}/c"];
            int partition = managers[0].LocateRange(keys[0]).PartitionId;
            int owner = await LeaderIndexOf(partition, rafts, ct);

            // The session stays on the node that began it; its keys are then staged on the new leader.
            TransactionHandle tx = await Start(managers[owner], bucket + "/tx", ct);
            Assert.Equal(owner, OwnerOf(managers, tx));

            int leader = (owner + 1) % Nodes;
            await MoveLeadership(rafts, partition, owner, leader, ct);

            foreach (string key in keys)
                await Write(managers[owner], tx, key, ct);

            (long sentBefore, long leftBefore) = ReleaseCounts(managers[owner]);
            long onePhaseBefore = managers[owner].TransactionCoordinator.CommitCalls.Calls(CommitCallPath.OnePhase);

            Assert.Equal(KeyValueResponseType.Committed, (await managers[owner].LocateAndCommitTransaction(tx, ct)).Type);

            (long sent, long left) = ReleaseCounts(managers[owner]);
            Assert.Equal(1, managers[owner].TransactionCoordinator.CommitCalls.Calls(CommitCallPath.OnePhase) - onePhaseBefore);
            Assert.Equal(keys.Length, sent - sentBefore);
            Assert.Equal(0, left - leftBefore);

            foreach (string key in keys)
                Assert.True(await HoldsNothing(managers[leader], tx.TransactionId, key), $"the leader still holds something of the committer on {key}");
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }
}
