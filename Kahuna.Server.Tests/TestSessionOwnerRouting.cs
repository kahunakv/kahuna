
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
/// client's deadline (~100 indeterminate commits per leader pause under a paused disk).
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

    private static async Task<TransactionHandle> StartTransaction(KahunaManager session, string coordinatorKey, CancellationToken ct, bool pessimistic = false)
    {
        KeyValueTransactionOptions options = pessimistic
            ? new()
            {
                CoordinatorKey = coordinatorKey,
                Locking = KeyValueTransactionLocking.Pessimistic,
                AsyncRelease = false,
                Timeout = 60_000
            }
            : new()
            {
                CoordinatorKey = coordinatorKey,
                Locking = KeyValueTransactionLocking.Optimistic,
                ReadValidation = ReadValidation.TrackAndValidate,
                AsyncRelease = true,
                Timeout = 60_000
            };

        (KeyValueResponseType startType, TransactionHandle handle) = await session.LocateAndStartTransaction(options, ct);

        Assert.Equal(KeyValueResponseType.Set, startType);
        return handle;
    }

    private static string BucketOf(string key) => key[..key.LastIndexOf('/')];

    /// <summary>Acquires a Shared range lock over the single key, registered with the session's coordinator.</summary>
    private static async Task<KeyValueResponseType> TryPointRangeLock(KahunaManager session, TransactionHandle handle, string key, CancellationToken ct)
    {
        (KeyValueResponseType lockType, _) = await RetryOnMustRetryAsync(
            () => session.LocateAndTryAcquireRangeLock(
                handle.TransactionId, BucketOf(key), key, true, key, true, 60_000, KeyValueDurability.Persistent, RangeLockMode.Shared, ct,
                coordinatorKey: handle.CoordinatorKey, operationId: TransactionOperationId.NewRandom()),
            r => r.Item1);

        return lockType;
    }

    private static async Task<bool> HoldsRangeLock(KahunaManager node, string key, HLCTimestamp transactionId)
    {
        List<KeyValueRangeLock> held = await node.KeyValues.GetRangeLocksAsync(BucketOf(key));
        return held.Any(l => l.TransactionId == transactionId);
    }

    /// <summary>
    /// A pessimistic session on its coordinator with a Shared range lock on a data key off the coordinator
    /// partition: the lock lives on the data key's leader, the session on the coordinator leader. Returns the
    /// node a third party would use, which is neither the owner nor <paramref name="successor"/>.
    /// </summary>
    private static async Task<(TransactionHandle Handle, int Partition, int Owner, string DataKey, int DataLeader, int Third)> BeginLockedSession(
        IRaft[] rafts, KahunaManager[] managers, string key, string dataPrefix, int successorOffset, CancellationToken ct)
    {
        // Begin routes to the leader of the hash-routed coordinator partition (the same router a hash-space key
        // takes); the owner is the node whose coordinator holds the session.
        int partition = managers[0].LocateRange(key).PartitionId;

        TransactionHandle handle = await StartTransaction(managers[0], key, ct, pessimistic: true);

        int owner = -1;
        for (int i = 0; i < managers.Length; i++)
        {
            if (managers[i].TransactionCoordinator.HasSession(handle.TransactionId))
                owner = i;
        }

        Assert.True(owner >= 0, "some node must hold the session");
        Assert.Equal(await LeaderIndexOf(partition, rafts, ct), owner);

        // A range lock on a bucket lives on the leader of the partition the bucket routes to; keep that partition
        // off the coordinator partition, so moving the coordinator's leadership leaves the lock where it is.
        string runId = Guid.NewGuid().ToString("N")[..8];
        string? dataKey = null;
        int lockPartition = -1;

        for (int i = 0; i < 256 && dataKey is null; i++)
        {
            string candidate = $"{dataPrefix}-{runId}-{i}/k";
            int candidatePartition = managers[owner].LocateRange(BucketOf(candidate) + "/").PartitionId;

            if (candidatePartition != partition)
                (dataKey, lockPartition) = (candidate, candidatePartition);
        }

        Assert.NotNull(dataKey);

        int dataLeader = await LeaderIndexOf(lockPartition, rafts, ct);

        Assert.Equal(KeyValueResponseType.Locked, await TryPointRangeLock(managers[owner], handle, dataKey, ct));
        Assert.True(await HoldsRangeLock(managers[dataLeader], dataKey, handle.TransactionId));

        int successor = (owner + successorOffset) % Nodes;
        int third = Enumerable.Range(0, Nodes).Single(i => i != owner && i != successor);

        return (handle, partition, owner, dataKey, dataLeader, third);
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

    /// <summary>
    /// A fresh data key that routes to a partition other than <paramref name="partition"/>, so moving that
    /// partition's leadership moves the session's coordinator only, not the leader that holds the staged write. A
    /// staged write lives in its leader's memory, and a commit after its partition changed leader is refused (see
    /// <see cref="OwnerCommit_AfterTheWrittenKeysPartitionChangedLeader_IsRefused"/>). Keys route by their parent
    /// bucket, so the candidates vary the bucket.
    /// </summary>
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
            string dataKey = DataKeyOffPartition(managers[owner], "owner-route-data", partition);

            await Write(managers[owner], handle, dataKey, "moved-then-committed", ct);

            await MoveLeadership(rafts, partition, owner, successor, ct);

            (KeyValueResponseType commitType, string? _) = await RetryOnMustRetryAsync(
                () => managers[owner].LocateAndCommitTransaction(handle, ct), r => r.Type);

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
            string dataKey = DataKeyOffPartition(managers[owner], "owner-fwd-data", partition);

            await Write(managers[owner], handle, dataKey, "forwarded-to-owner", ct);

            await MoveLeadership(rafts, partition, owner, successor, ct);

            (KeyValueResponseType commitType, string? _) = await RetryOnMustRetryAsync(
                () => managers[successor].LocateAndCommitTransaction(handle, ct), r => r.Type);

            Assert.Equal(KeyValueResponseType.Committed, commitType);

            foreach (KahunaManager reader in managers)
                await AssertValueVisible(reader, dataKey, "forwarded-to-owner", ct);
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    /// <summary>
    /// A staged write lives in the memory of the leader that staged it. When the written key's partition changes
    /// leader before the commit, the new leader holds nothing that would make a snapshot read wait for the write,
    /// so the commit is refused (a retryable abort) rather than landing inside a snapshot that may already have
    /// answered without it.
    /// </summary>
    [Fact]
    public async Task OwnerCommit_AfterTheWrittenKeysPartitionChangedLeader_IsRefused()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleCluster(Nodes, "memory", Partitions, raftLogger, kahunaLogger);
        KahunaManager[] managers = [.. kahunas.Cast<KahunaManager>()];

        try
        {
            string key = "owner-moved-data/" + Guid.NewGuid().ToString("N")[..8];
            (TransactionHandle handle, _, int owner) = await BeginOnItsCoordinator(rafts, managers, key, ct);

            string dataKey = "owner-moved-data-" + Guid.NewGuid().ToString("N")[..8] + "/k";
            await Write(managers[owner], handle, dataKey, "never-visible", ct);

            int dataPartition = managers[owner].LocateRange(dataKey).PartitionId;
            int dataLeader = await LeaderIndexOf(dataPartition, rafts, ct);
            await MoveLeadership(rafts, dataPartition, dataLeader, (dataLeader + 1) % Nodes, ct);

            (KeyValueResponseType commitType, string? _) = await RetryOnMustRetryAsync(
                () => managers[owner].LocateAndCommitTransaction(handle, ct), r => r.Type);

            Assert.Equal(KeyValueResponseType.Aborted, commitType);

            (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await managers[owner].LocateAndTryGetValue(
                HLCTimestamp.Zero, dataKey, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
            Assert.True(type == KeyValueResponseType.DoesNotExist || entry?.Value is null);
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

    /// <summary>
    /// The commit reaches a node that neither owns the session nor leads the coordinator partition (the data
    /// leader a consumer loop runs on): it forwards the commit to the new leader, which has no such session and
    /// must still offer it to the owner. Before this, a forwarded commit was answered from the leader's empty
    /// table as an unknown transaction, and the session's range lock stayed on the data leader until its lease.
    /// </summary>
    [Fact]
    public async Task ACommitForwardedToTheNewLeader_ReachesTheSessionOwner_AndReleasesTheLock()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleCluster(Nodes, "memory", Partitions, raftLogger, kahunaLogger);
        KahunaManager[] managers = [.. kahunas.Cast<KahunaManager>()];

        try
        {
            string key = "owner-fwd-commit/" + Guid.NewGuid().ToString("N")[..8];
            (TransactionHandle handle, int partition, int owner, string dataKey, int dataLeader, int third) =
                await BeginLockedSession(rafts, managers, key, "owner-fwd-commit-data", successorOffset: 1, ct);

            await MoveLeadership(rafts, partition, owner, (owner + 1) % Nodes, ct);

            (KeyValueResponseType commitType, string? _) = await RetryOnMustRetryAsync(
                () => managers[third].LocateAndCommitTransaction(handle, ct), r => r.Type);

            Assert.Equal(KeyValueResponseType.Committed, commitType);
            Assert.False(managers[owner].TransactionCoordinator.HasSession(handle.TransactionId));
            Assert.False(await HoldsRangeLock(managers[dataLeader], dataKey, handle.TransactionId), "the committed session's range lock must be gone from the data leader");
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    /// <summary>The same route for a rollback: forwarded to the new leader, offered to the owner, and the lock is released.</summary>
    [Fact]
    public async Task ARollbackForwardedToTheNewLeader_ReachesTheSessionOwner_AndReleasesTheLock()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleCluster(Nodes, "memory", Partitions, raftLogger, kahunaLogger);
        KahunaManager[] managers = [.. kahunas.Cast<KahunaManager>()];

        try
        {
            string key = "owner-fwd-rb/" + Guid.NewGuid().ToString("N")[..8];
            (TransactionHandle handle, int partition, int owner, string dataKey, int dataLeader, int third) =
                await BeginLockedSession(rafts, managers, key, "owner-fwd-rb-data", successorOffset: 1, ct);

            await MoveLeadership(rafts, partition, owner, (owner + 1) % Nodes, ct);

            KeyValueResponseType rollbackType = await RetryOnMustRetryAsync(
                () => managers[third].LocateAndRollbackTransaction(handle, ct), r => r);

            Assert.Equal(KeyValueResponseType.RolledBack, rollbackType);
            Assert.False(managers[owner].TransactionCoordinator.HasSession(handle.TransactionId));
            Assert.False(await HoldsRangeLock(managers[dataLeader], dataKey, handle.TransactionId), "the rolled-back session's range lock must be gone from the data leader");
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    /// <summary>
    /// A lock acquired through a third node after the coordinator partition changed leader: the acquire registers
    /// its operation with the session's coordinator, which routes to the new leader. The leader does not hold the
    /// session and must offer the registration, and then the completion, to the owner, so the lock joins the
    /// owner's working set and the owner's commit releases it. Before this, the registration was refused as a
    /// closed session, or the completion was lost after the lock was granted.
    /// </summary>
    [Fact]
    public async Task ALockRegisteredThroughTheNewLeader_JoinsTheSessionOwnersWorkingSet()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleCluster(Nodes, "memory", Partitions, raftLogger, kahunaLogger);
        KahunaManager[] managers = [.. kahunas.Cast<KahunaManager>()];

        try
        {
            string key = "owner-fwd-lock/" + Guid.NewGuid().ToString("N")[..8];
            (TransactionHandle handle, int partition, int owner, string dataKey, int dataLeader, int third) =
                await BeginLockedSession(rafts, managers, key, "owner-fwd-lock-data", successorOffset: 1, ct);

            await MoveLeadership(rafts, partition, owner, (owner + 1) % Nodes, ct);

            // A second range on the same bucket, registered from the third node after the move.
            string secondKey = BucketOf(dataKey) + "/second";
            Assert.Equal(KeyValueResponseType.Locked, await TryPointRangeLock(managers[third], handle, secondKey, ct));
            Assert.True(await HoldsRangeLock(managers[dataLeader], secondKey, handle.TransactionId));

            // The new leader has no session of its own: a working-set query that reaches it directly is offered
            // to the owner and answers both locks.
            TransactionWorkingSet? workingSet = await managers[(owner + 1) % Nodes].LocateAndGetTransactionWorkingSet(handle.CoordinatorKey, handle.TransactionId, ct);
            Assert.NotNull(workingSet);
            Assert.Equal(2, workingSet.AcquiredRangeLocks.Count);

            (KeyValueResponseType commitType, string? _) = await RetryOnMustRetryAsync(
                () => managers[owner].LocateAndCommitTransaction(handle, ct), r => r.Type);

            Assert.Equal(KeyValueResponseType.Committed, commitType);
            Assert.False(await HoldsRangeLock(managers[dataLeader], dataKey, handle.TransactionId));
            Assert.False(await HoldsRangeLock(managers[dataLeader], secondKey, handle.TransactionId), "a lock registered through the new leader must be released by the owner's commit");
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    /// <summary>
    /// A close sent to the new leader after the move (a client that learned the partition's leader): the leader
    /// has no session and offers the close to the owner, which answers the frozen working set; the owner's own
    /// close answers the same snapshot, and a rollback through a third node still reaches the owner.
    /// </summary>
    [Fact]
    public async Task ACloseSentToTheNewLeader_ReachesTheSessionOwner()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleCluster(Nodes, "memory", Partitions, raftLogger, kahunaLogger);
        KahunaManager[] managers = [.. kahunas.Cast<KahunaManager>()];

        try
        {
            string key = "owner-fwd-close/" + Guid.NewGuid().ToString("N")[..8];
            (TransactionHandle handle, int partition, int owner, string dataKey, int dataLeader, int third) =
                await BeginLockedSession(rafts, managers, key, "owner-fwd-close-data", successorOffset: 1, ct);

            int successor = (owner + 1) % Nodes;
            await MoveLeadership(rafts, partition, owner, successor, ct);

            (KeyValueResponseType closeType, TransactionWorkingSet? snapshot) = await RetryOnMustRetryAsync(
                () => managers[successor].LocateAndCloseTransaction(handle.CoordinatorKey, handle.TransactionId, ct), r => r.Item1);

            Assert.Equal(KeyValueResponseType.Set, closeType);
            Assert.NotNull(snapshot);
            Assert.Single(snapshot.AcquiredRangeLocks);

            (KeyValueResponseType ownerCloseType, TransactionWorkingSet? ownerSnapshot) =
                await managers[owner].LocateAndCloseTransaction(handle.CoordinatorKey, handle.TransactionId, ct);

            Assert.Equal(KeyValueResponseType.Set, ownerCloseType);
            Assert.NotNull(ownerSnapshot);
            Assert.Single(ownerSnapshot.AcquiredRangeLocks);

            KeyValueResponseType rollbackType = await RetryOnMustRetryAsync(
                () => managers[third].LocateAndRollbackTransaction(handle, ct), r => r);

            Assert.Equal(KeyValueResponseType.RolledBack, rollbackType);
            Assert.False(await HoldsRangeLock(managers[dataLeader], dataKey, handle.TransactionId));
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }
}
