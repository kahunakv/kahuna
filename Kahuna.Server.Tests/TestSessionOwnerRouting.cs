
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Data;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// An interactive session lives on the node that began it and does not move with the coordinator partition's
/// leadership: the session's own finalizer forwards its durable operations to whichever node leads the partition
/// now. The routed entry points (commit, rollback, operation registration) must therefore serve a session this
/// node owns locally whatever the current leader is, and a leader that receives one for a session begun on the
/// previous leader must forward it there — not answer "No transaction session". Before this, every commit in
/// flight on a leader that stepped down was routed to the successor, which had no such session, and spun until the
/// client's deadline (CamusDB slow-disk runs sd2–sd6, ~100 indeterminate commits per leader pause).
/// </summary>
public sealed class TestSessionOwnerRouting : BaseCluster
{
    private const int Nodes = 3;

    private const int Partitions = 4;

    private readonly ILogger<IRaft> raftLogger;

    private readonly ILogger<IKahuna> kahunaLogger;

    public TestSessionOwnerRouting(ITestOutputHelper outputHelper)
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

    private static async Task MoveLeadership(IRaft[] rafts, int partition, int from, int to, CancellationToken ct)
    {
        RaftOperationStatus status = await rafts[from].TransferLeadershipAsync(partition, EndpointOf(to), ct);
        Assert.Equal(RaftOperationStatus.Success, status);

        await WaitUntilAsync(async () => await rafts[to].AmILeaderIfHosted(partition, ct), timeoutMs: 30_000);
        await WaitUntilAsync(async () => !await rafts[from].AmILeaderIfHosted(partition, ct), timeoutMs: 30_000);
    }

    private static async Task<TransactionHandle> StartTransaction(KahunaManager session, string coordinatorKey, CancellationToken ct)
    {
        (KeyValueResponseType startType, TransactionHandle handle) = await session.LocateAndStartTransaction(
            new KeyValueTransactionOptions
            {
                CoordinatorKey = coordinatorKey,
                Locking = KeyValueTransactionLocking.Optimistic,
                ReadValidation = ReadValidation.TrackAndValidate,
                AsyncRelease = true,
                Timeout = 60_000
            }, ct);

        Assert.Equal(KeyValueResponseType.Set, startType);
        return handle;
    }

    private static async Task Write(KahunaManager session, TransactionHandle handle, string key, string value, CancellationToken ct)
    {
        (KeyValueResponseType writeType, _, _) = await session.LocateAndTrySetKeyValue(
            handle.TransactionId, key, System.Text.Encoding.UTF8.GetBytes(value), null, -1,
            KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct,
            coordinatorKey: handle.CoordinatorKey, operationId: TransactionOperationId.NewRandom());

        Assert.Equal(KeyValueResponseType.Set, writeType);
    }

    /// <summary>
    /// Begins a session coordinated on <paramref name="key"/> and returns it with the coordinator partition and the
    /// node that owns the session: begin routes to the leader of the hash-routed coordinator partition, so the
    /// owner is found by asking each node's coordinator rather than assumed from the starting node.
    /// </summary>
    private static async Task<(TransactionHandle Handle, int Partition, int Owner)> BeginOnItsCoordinator(
        IRaft[] rafts, KahunaManager[] managers, string key, CancellationToken ct)
    {
        int partition = rafts[0].GetPartitionKey(key);
        int leader = await LeaderIndexOf(partition, rafts, ct);

        TransactionHandle handle = await StartTransaction(managers[leader], key, ct);

        int owner = -1;
        for (int i = 0; i < managers.Length; i++)
        {
            if (managers[i].TransactionCoordinator.HasSession(handle.TransactionId))
                owner = i;
        }

        Assert.True(owner >= 0, "some node must hold the session");
        Assert.Equal(leader, owner);

        return (handle, partition, owner);
    }

    private static async Task AssertValueVisible(KahunaManager reader, string key, string expected, CancellationToken ct)
    {
        await WaitUntilAsync(async () =>
        {
            (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await reader.LocateAndTryGetValue(
                HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
            return type == KeyValueResponseType.Get && entry?.Value is not null
                && System.Text.Encoding.UTF8.GetString(entry.Value) == expected;
        }, timeoutMs: 30_000);
    }

    /// <summary>
    /// The session's owner commits it after the coordinator partition's leadership moved away from it: the commit
    /// is served on the owner (its finalizer forwards the durable work to the new leader), not routed to a node
    /// that has no session.
    /// </summary>
    [Fact]
    public async Task OwnerCommitsItsSession_AfterLeadershipMovedAway()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleCluster(Nodes, "memory", Partitions, raftLogger, kahunaLogger);
        KahunaManager[] managers = [.. kahunas.Cast<KahunaManager>()];

        try
        {
            string key = "owner-route/" + Guid.NewGuid().ToString("N")[..8];
            (TransactionHandle handle, int partition, int owner) = await BeginOnItsCoordinator(rafts, managers, key, ct);
            int successor = (owner + 1) % Nodes;

            await Write(managers[owner], handle, key, "moved-then-committed", ct);

            await MoveLeadership(rafts, partition, owner, successor, ct);

            (KeyValueResponseType commitType, _) = await RetryOnMustRetryAsync(
                () => managers[owner].LocateAndCommitTransaction(handle, ct), r => r.Item1);

            Assert.Equal(KeyValueResponseType.Committed, commitType);

            foreach (KahunaManager reader in managers)
                await AssertValueVisible(reader, key, "moved-then-committed", ct);
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    /// <summary>
    /// The commit reaches the new leader instead (a client that learned the partition's leader): the leader does
    /// not own the session and forwards the commit to the node that minted the transaction id.
    /// </summary>
    [Fact]
    public async Task NewLeaderForwardsACommit_ToTheSessionOwner()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleCluster(Nodes, "memory", Partitions, raftLogger, kahunaLogger);
        KahunaManager[] managers = [.. kahunas.Cast<KahunaManager>()];

        try
        {
            string key = "owner-fwd/" + Guid.NewGuid().ToString("N")[..8];
            (TransactionHandle handle, int partition, int owner) = await BeginOnItsCoordinator(rafts, managers, key, ct);
            int successor = (owner + 1) % Nodes;

            await Write(managers[owner], handle, key, "forwarded-to-owner", ct);

            await MoveLeadership(rafts, partition, owner, successor, ct);

            (KeyValueResponseType commitType, _) = await RetryOnMustRetryAsync(
                () => managers[successor].LocateAndCommitTransaction(handle, ct), r => r.Item1);

            Assert.Equal(KeyValueResponseType.Committed, commitType);

            foreach (KahunaManager reader in managers)
                await AssertValueVisible(reader, key, "forwarded-to-owner", ct);
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    /// <summary>The same rule for a rollback sent to the new leader: it is forwarded to the owner and lands there.</summary>
    [Fact]
    public async Task NewLeaderForwardsARollback_ToTheSessionOwner()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleCluster(Nodes, "memory", Partitions, raftLogger, kahunaLogger);
        KahunaManager[] managers = [.. kahunas.Cast<KahunaManager>()];

        try
        {
            string key = "owner-rb/" + Guid.NewGuid().ToString("N")[..8];
            (TransactionHandle handle, int partition, int owner) = await BeginOnItsCoordinator(rafts, managers, key, ct);
            int successor = (owner + 1) % Nodes;

            await Write(managers[owner], handle, key, "never-visible", ct);

            await MoveLeadership(rafts, partition, owner, successor, ct);

            KeyValueResponseType rollbackType = await RetryOnMustRetryAsync(
                () => managers[successor].LocateAndRollbackTransaction(handle, ct), r => r);

            Assert.Equal(KeyValueResponseType.RolledBack, rollbackType);
            Assert.False(managers[owner].TransactionCoordinator.HasSession(handle.TransactionId));

            (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await managers[successor].LocateAndTryGetValue(
                HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
            Assert.True(type == KeyValueResponseType.DoesNotExist || entry?.Value is null);
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }
}
