
using Grpc.Core;
using Kahuna.Communication.External.Grpc;
using Kahuna.Server.Communication;
using Kahuna.Server.Communication.Internode;
using Kahuna.Server.KeyValues;
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

    /// <summary>
    /// Moves the leadership and waits until <paramref name="observer"/> names the new leader. A node that still
    /// names the old leader, which owns the session, sends a request straight to the owner, and a test of the route
    /// through the new leader would then test nothing.
    /// </summary>
    private static async Task MoveLeadershipSeenBy(IRaft[] rafts, int partition, int from, int to, int observer, CancellationToken ct)
    {
        await MoveLeadership(rafts, partition, from, to, ct);
        await WaitUntilAsync(async () => await rafts[observer].WaitForLeader(partition, ct) == EndpointOf(to), timeoutMs: 30_000);
    }

    private static async Task<TransactionHandle> StartTransaction(
        KahunaManager session, string coordinatorKey, CancellationToken ct, bool pessimistic = false, int timeoutMs = 60_000)
    {
        KeyValueTransactionOptions options = pessimistic
            ? new()
            {
                CoordinatorKey = coordinatorKey,
                Locking = KeyValueTransactionLocking.Pessimistic,
                AsyncRelease = false,
                Timeout = timeoutMs
            }
            : new()
            {
                CoordinatorKey = coordinatorKey,
                Locking = KeyValueTransactionLocking.Optimistic,
                ReadValidation = ReadValidation.TrackAndValidate,
                AsyncRelease = true,
                Timeout = timeoutMs
            };

        (KeyValueResponseType startType, TransactionHandle handle) = await session.LocateAndStartTransaction(options, ct);

        Assert.Equal(KeyValueResponseType.Set, startType);
        return handle;
    }

    private static string BucketOf(string key) => key[..key.LastIndexOf('/')];

    /// <summary>
    /// The partition whose leader begins a session for <paramref name="coordinatorKey"/>: Kahuna's own router, which
    /// hashes the placement group of the key's key space. Kommander's partition of the whole key is another number.
    /// </summary>
    private static int CoordinatorPartitionOf(KahunaManager manager, string coordinatorKey) => manager.LocateRange(coordinatorKey).PartitionId;

    /// <summary>The node whose coordinator holds the session of <paramref name="handle"/>.</summary>
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
        int partition = CoordinatorPartitionOf(managers[0], key);

        TransactionHandle handle = await StartTransaction(managers[0], key, ct, pessimistic: true);

        int owner = OwnerOf(managers, handle);
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
    /// node that owns the session: begin routes to the leader of the coordinator partition, so the owner is found by
    /// asking each node's coordinator rather than assumed from the starting node.
    /// </summary>
    private static async Task<(TransactionHandle Handle, int Partition, int Owner)> BeginOnItsCoordinator(
        IRaft[] rafts, KahunaManager[] managers, string key, CancellationToken ct, bool pessimistic = false, int timeoutMs = 60_000)
    {
        int partition = CoordinatorPartitionOf(managers[0], key);
        int leader = await LeaderIndexOf(partition, rafts, ct);

        TransactionHandle handle = await StartTransaction(managers[leader], key, ct, pessimistic, timeoutMs);

        int owner = OwnerOf(managers, handle);
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

            int successor = (owner + 1) % Nodes;
            await MoveLeadership(rafts, partition, owner, successor, ct);

            using MetricCapture timed = new("path", "kahuna.transactions.commit_call_ms");

            (KeyValueResponseType commitType, string? _) = await RetryOnMustRetryAsync(
                () => managers[third].LocateAndCommitTransaction(handle, ct), r => r.Type);

            Assert.Equal(KeyValueResponseType.Committed, commitType);

            // Both nodes the commit passed through record a forwarded call: the third node sent it to the new leader,
            // and the new leader found the owner among its peers.
            Assert.True(managers[third].TransactionCoordinator.CommitCalls.Calls(CommitCallPath.Forwarded) >= 1);
            Assert.True(managers[successor].TransactionCoordinator.CommitCalls.Calls(CommitCallPath.Forwarded) >= 1);

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

    /// <summary>
    /// A working-set query from a node that neither owns the session nor leads the coordinator partition: the node
    /// forwards it to the new leader, which has no such session and must offer it to the owner. Before this, the
    /// forwarded query was answered from the leader's own table, so the third node read no working set at all for a
    /// session that the owner still held.
    /// </summary>
    [Fact]
    public async Task AWorkingSetQueryForwardedToTheNewLeader_ReachesTheSessionOwner()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, IKahuna[] kahunas, _, MemoryInterNodeCommmunication interNode) =
            await AssembleClusterWithTransports(Nodes, "memory", Partitions, raftLogger, kahunaLogger);
        KahunaManager[] managers = [.. kahunas.Cast<KahunaManager>()];

        try
        {
            string key = "owner-fwd-ws/" + Guid.NewGuid().ToString("N")[..8];
            (TransactionHandle handle, int partition, int owner, string dataKey, _, int third) =
                await BeginLockedSession(rafts, managers, key, "owner-fwd-ws-data", successorOffset: 1, ct);

            await MoveLeadershipSeenBy(rafts, partition, owner, (owner + 1) % Nodes, third, ct);

            int callsBefore = interNode.WorkingSetCallCount;

            TransactionWorkingSet? workingSet = null;
            await WaitUntilAsync(async () =>
            {
                workingSet = await managers[third].LocateAndGetTransactionWorkingSet(handle.CoordinatorKey, handle.TransactionId, ct);
                return workingSet is not null;
            });

            Assert.Equal(dataKey, Assert.Single(workingSet!.AcquiredRangeLocks).StartKey);

            // One forward to the leader and at least the probe that the owner served.
            Assert.True(interNode.WorkingSetCallCount - callsBefore >= 2);

            // A query does not close the session: the owner still takes a registration.
            (OperationRegistrationOutcome outcome, _, _, _, _, _) = await managers[owner].LocateAndBeginOperation(
                handle.CoordinatorKey, handle.TransactionId, TransactionOperationId.NewRandom(), OperationKind.Set, [1], ct);
            Assert.Equal(OperationRegistrationOutcome.New, outcome);
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    /// <summary>
    /// The same route for a close: the owner freezes the session and the third node gets the frozen set. The commit
    /// that follows, from the same node, finalizes that set. Before this, the third node got Errored, so a caller that
    /// closes to learn the written keys had to fall back to what it remembered itself.
    /// </summary>
    [Fact]
    public async Task ACloseForwardedToTheNewLeader_ReachesTheSessionOwner()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, IKahuna[] kahunas, _, MemoryInterNodeCommmunication interNode) =
            await AssembleClusterWithTransports(Nodes, "memory", Partitions, raftLogger, kahunaLogger);
        KahunaManager[] managers = [.. kahunas.Cast<KahunaManager>()];

        try
        {
            string key = "owner-fwd-close3/" + Guid.NewGuid().ToString("N")[..8];
            (TransactionHandle handle, int partition, int owner, string dataKey, int dataLeader, int third) =
                await BeginLockedSession(rafts, managers, key, "owner-fwd-close3-data", successorOffset: 1, ct);

            await MoveLeadershipSeenBy(rafts, partition, owner, (owner + 1) % Nodes, third, ct);

            int callsBefore = interNode.CloseTransactionCallCount;

            (KeyValueResponseType closeType, TransactionWorkingSet? snapshot) = await RetryOnMustRetryAsync(
                () => managers[third].LocateAndCloseTransaction(handle.CoordinatorKey, handle.TransactionId, ct), r => r.Item1);

            Assert.Equal(KeyValueResponseType.Set, closeType);
            Assert.Equal(dataKey, Assert.Single(snapshot!.AcquiredRangeLocks).StartKey);

            // One forward to the leader and at least the probe that the owner served.
            Assert.True(interNode.CloseTransactionCallCount - callsBefore >= 2);

            // The owner's session is the one that closed: it refuses a new registration and keeps the session.
            (OperationRegistrationOutcome outcome, _, _, _, _, _) = await managers[owner].LocateAndBeginOperation(
                handle.CoordinatorKey, handle.TransactionId, TransactionOperationId.NewRandom(), OperationKind.Set, [1], ct);
            Assert.Equal(OperationRegistrationOutcome.RejectedSessionClosed, outcome);

            (KeyValueResponseType commitType, string? _) = await RetryOnMustRetryAsync(
                () => managers[third].LocateAndCommitTransaction(handle, ct), r => r.Type);

            Assert.Equal(KeyValueResponseType.Committed, commitType);
            Assert.False(await HoldsRangeLock(managers[dataLeader], dataKey, handle.TransactionId), "the committed session's range lock must be gone from the data leader");
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    /// <summary>
    /// A close that the owner cannot finish yet, because an operation is still pending at its drain deadline: the
    /// owner answers MustRetry, and that is the answer the third node gets. Answered from the new leader's own table
    /// it read Errored, which tells the caller to stop, in the one case where a write of the transaction is about to
    /// join the set. The same call after the completion answers the frozen set with that write in it.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task AClosePastItsDrainDeadline_AnswersTheOwnersMustRetry_ThroughTheNewLeader(bool throughThirdNode)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleCluster(Nodes, "memory", Partitions, raftLogger, kahunaLogger);
        KahunaManager[] managers = [.. kahunas.Cast<KahunaManager>()];

        try
        {
            string key = "owner-fwd-drain/" + Guid.NewGuid().ToString("N")[..8];
            (TransactionHandle handle, int partition, int owner) =
                await BeginOnItsCoordinator(rafts, managers, key, ct, pessimistic: true, timeoutMs: 1_000);

            int successor = (owner + 1) % Nodes;
            int third = (owner + 2) % Nodes;

            TransactionOperationId pending = TransactionOperationId.NewRandom();
            (OperationRegistrationOutcome outcome, _, _, _, _, _) = await managers[owner].LocateAndBeginOperation(
                handle.CoordinatorKey, handle.TransactionId, pending, OperationKind.Set, [1, 2, 3], ct);
            Assert.Equal(OperationRegistrationOutcome.New, outcome);

            await MoveLeadershipSeenBy(rafts, partition, owner, successor, third, ct);

            KahunaManager caller = managers[throughThirdNode ? third : successor];

            (KeyValueResponseType drainingType, TransactionWorkingSet? draining) =
                await caller.LocateAndCloseTransaction(handle.CoordinatorKey, handle.TransactionId, ct);

            Assert.Equal(KeyValueResponseType.MustRetry, drainingType);
            Assert.Null(draining);

            // MustRetry came from the owner, not from a node that found no leader: the owner's session is closed.
            (OperationRegistrationOutcome late, _, _, _, _, _) = await managers[owner].LocateAndBeginOperation(
                handle.CoordinatorKey, handle.TransactionId, TransactionOperationId.NewRandom(), OperationKind.Set, [4], ct);
            Assert.Equal(OperationRegistrationOutcome.RejectedSessionClosed, late);

            (KeyValueResponseType completed, _) = await managers[owner].LocateAndCompleteOperation(
                handle.CoordinatorKey, handle.TransactionId, pending,
                new OperationCompletionPayload
                {
                    ModifiedKey = "owner-fwd-drain-data/k",
                    Durability = KeyValueDurability.Persistent,
                    CachedType = KeyValueResponseType.Set
                },
                ct);
            Assert.Equal(KeyValueResponseType.Set, completed);

            (KeyValueResponseType closeType, TransactionWorkingSet? snapshot) = await RetryOnMustRetryAsync(
                () => caller.LocateAndCloseTransaction(handle.CoordinatorKey, handle.TransactionId, ct), r => r.Item1);

            Assert.Equal(KeyValueResponseType.Set, closeType);
            Assert.Equal("owner-fwd-drain-data/k", Assert.Single(snapshot!.ModifiedKeys).Key);
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    /// <summary>
    /// A close that reaches the new leader while the owner cannot be reached: no node can say that the session is gone,
    /// so the answer is MustRetry, and the same call is served when the owner is back. Errored here would end the
    /// caller's retries for a session that is alive.
    /// </summary>
    [Fact]
    public async Task AClose_WhileTheOwnerIsNotReachable_AnswersMustRetry()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, IKahuna[] kahunas, _, MemoryInterNodeCommmunication interNode) =
            await AssembleClusterWithTransports(Nodes, "memory", Partitions, raftLogger, kahunaLogger);
        KahunaManager[] managers = [.. kahunas.Cast<KahunaManager>()];

        try
        {
            string key = "owner-fwd-away/" + Guid.NewGuid().ToString("N")[..8];
            (TransactionHandle handle, int partition, int owner) = await BeginOnItsCoordinator(rafts, managers, key, ct);

            int successor = (owner + 1) % Nodes;
            int third = (owner + 2) % Nodes;
            await MoveLeadershipSeenBy(rafts, partition, owner, successor, third, ct);

            Dictionary<string, IKahuna> reachable = Enumerable.Range(0, Nodes).ToDictionary(EndpointOf, i => kahunas[i]);

            Dictionary<string, IKahuna> withoutOwner = new(reachable);
            withoutOwner.Remove(EndpointOf(owner));
            interNode.SetNodes(withoutOwner);

            foreach (int caller in new[] { successor, third })
            {
                (KeyValueResponseType awayType, TransactionWorkingSet? away) =
                    await managers[caller].LocateAndCloseTransaction(handle.CoordinatorKey, handle.TransactionId, ct);

                Assert.Equal(KeyValueResponseType.MustRetry, awayType);
                Assert.Null(away);
            }

            interNode.SetNodes(reachable);

            (KeyValueResponseType closeType, TransactionWorkingSet? snapshot) = await RetryOnMustRetryAsync(
                () => managers[third].LocateAndCloseTransaction(handle.CoordinatorKey, handle.TransactionId, ct), r => r.Item1);

            Assert.Equal(KeyValueResponseType.Set, closeType);
            Assert.NotNull(snapshot);
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    /// <summary>
    /// A request that reaches the new leader after two forwards: a caller that does not know the leader sends it to
    /// another replica, and the replica sends it on. The leader must still offer it to the owner. The scope stands for
    /// the first forward, so the third node's own forward is the second one.
    /// </summary>
    [Fact]
    public async Task ARequestForwardedTwice_ReachesTheSessionOwner()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleCluster(Nodes, "memory", Partitions, raftLogger, kahunaLogger);
        KahunaManager[] managers = [.. kahunas.Cast<KahunaManager>()];

        try
        {
            string key = "owner-fwd-twice/" + Guid.NewGuid().ToString("N")[..8];
            (TransactionHandle handle, int partition, int owner, string dataKey, int dataLeader, int third) =
                await BeginLockedSession(rafts, managers, key, "owner-fwd-twice-data", successorOffset: 1, ct);

            await MoveLeadershipSeenBy(rafts, partition, owner, (owner + 1) % Nodes, third, ct);

            using ForwardedRequestScope.Scope arrivedForwarded = ForwardedRequestScope.Enter();

            TransactionWorkingSet? workingSet = null;
            await WaitUntilAsync(async () =>
            {
                workingSet = await managers[third].LocateAndGetTransactionWorkingSet(handle.CoordinatorKey, handle.TransactionId, ct);
                return workingSet is not null;
            });

            Assert.Equal(dataKey, Assert.Single(workingSet!.AcquiredRangeLocks).StartKey);

            (KeyValueResponseType closeType, TransactionWorkingSet? snapshot) = await RetryOnMustRetryAsync(
                () => managers[third].LocateAndCloseTransaction(handle.CoordinatorKey, handle.TransactionId, ct), r => r.Item1);

            Assert.Equal(KeyValueResponseType.Set, closeType);
            Assert.Single(snapshot!.AcquiredRangeLocks);

            (KeyValueResponseType commitType, string? _) = await RetryOnMustRetryAsync(
                () => managers[third].LocateAndCommitTransaction(handle, ct), r => r.Type);

            Assert.Equal(KeyValueResponseType.Committed, commitType);
            Assert.False(await HoldsRangeLock(managers[dataLeader], dataKey, handle.TransactionId), "the committed session's range lock must be gone from the data leader");
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    /// <summary>
    /// The inter-node landing points of the gRPC transport, driven through the batch stream a peer opens: the new
    /// leader gets the query and the close with the hop count that a third node stamps on them, and answers with the
    /// owner's working set. A probe of the same stream, which carries a hop count above the forward budget, is
    /// answered from the receiver's own table on a node that does not own the session and on the owner.
    /// </summary>
    [Fact]
    public async Task TheGrpcLandingPoints_ReachTheSessionOwner()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleCluster(Nodes, "memory", Partitions, raftLogger, kahunaLogger);
        KahunaManager[] managers = [.. kahunas.Cast<KahunaManager>()];

        try
        {
            string key = "owner-fwd-grpc/" + Guid.NewGuid().ToString("N")[..8];
            (TransactionHandle handle, int partition, int owner, string dataKey, _, int third) =
                await BeginLockedSession(rafts, managers, key, "owner-fwd-grpc-data", successorOffset: 1, ct);

            int successor = (owner + 1) % Nodes;
            await MoveLeadershipSeenBy(rafts, partition, owner, successor, third, ct);

            const int probeHops = ForwardedRequestScope.MaxForwardHops + 1;

            (GrpcGetTransactionWorkingSetResponse probedQuery, GrpcCloseTransactionResponse probedClose) =
                await SendQueryAndClose(kahunas[third], handle, probeHops);

            Assert.False(probedQuery.Found);
            Assert.Equal(KeyValueResponseType.Errored, (KeyValueResponseType)probedClose.Type);
            Assert.False(probedClose.HasWorkingSet);

            (GrpcGetTransactionWorkingSetResponse query, GrpcCloseTransactionResponse close) =
                await SendQueryAndClose(kahunas[successor], handle, forwardHops: 1);

            Assert.True(query.Found);
            Assert.Equal(dataKey, Assert.Single(query.WorkingSet.AcquiredRangeLocks).StartKey);

            Assert.Equal(KeyValueResponseType.Set, (KeyValueResponseType)close.Type);
            Assert.True(close.HasWorkingSet);
            Assert.Equal(dataKey, Assert.Single(close.WorkingSet.AcquiredRangeLocks).StartKey);

            (GrpcGetTransactionWorkingSetResponse ownerQuery, GrpcCloseTransactionResponse ownerClose) =
                await SendQueryAndClose(kahunas[owner], handle, probeHops);

            Assert.True(ownerQuery.Found);
            Assert.Equal(KeyValueResponseType.Set, (KeyValueResponseType)ownerClose.Type);
            Assert.True(ownerClose.HasWorkingSet);
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    /// <summary>
    /// A probe asks a peer for its own session table. A peer that does not hold the session answers at once, with no
    /// call of its own, whether it leads the coordinator partition or not.
    /// </summary>
    [Fact]
    public async Task AProbe_IsAnsweredFromThePeersOwnTable_WithNoFurtherCall()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, IKahuna[] kahunas, _, MemoryInterNodeCommmunication interNode) =
            await AssembleClusterWithTransports(Nodes, "memory", Partitions, raftLogger, kahunaLogger);
        KahunaManager[] managers = [.. kahunas.Cast<KahunaManager>()];

        try
        {
            string key = "owner-probe/" + Guid.NewGuid().ToString("N")[..8];
            (TransactionHandle handle, int partition, int owner) = await BeginOnItsCoordinator(rafts, managers, key, ct);

            int successor = (owner + 1) % Nodes;
            int third = (owner + 2) % Nodes;
            await MoveLeadershipSeenBy(rafts, partition, owner, successor, third, ct);

            int queriesBefore = interNode.WorkingSetCallCount;
            int closesBefore = interNode.CloseTransactionCallCount;

            using (ForwardedRequestScope.EnterAt(ForwardedRequestScope.MaxForwardHops + 1))
            {
                foreach (int node in new[] { successor, third })
                {
                    Assert.Null(await managers[node].LocateAndGetTransactionWorkingSet(handle.CoordinatorKey, handle.TransactionId, ct));

                    (KeyValueResponseType closeType, TransactionWorkingSet? snapshot) =
                        await managers[node].LocateAndCloseTransaction(handle.CoordinatorKey, handle.TransactionId, ct);
                    Assert.Equal(KeyValueResponseType.Errored, closeType);
                    Assert.Null(snapshot);
                }

                Assert.NotNull(await managers[owner].LocateAndGetTransactionWorkingSet(handle.CoordinatorKey, handle.TransactionId, ct));

                (KeyValueResponseType ownerType, TransactionWorkingSet? ownerSnapshot) =
                    await managers[owner].LocateAndCloseTransaction(handle.CoordinatorKey, handle.TransactionId, ct);
                Assert.Equal(KeyValueResponseType.Set, ownerType);
                Assert.NotNull(ownerSnapshot);
            }

            Assert.Equal(queriesBefore, interNode.WorkingSetCallCount);
            Assert.Equal(closesBefore, interNode.CloseTransactionCallCount);
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    /// <summary>
    /// Two nodes can take themselves for the leader at once, so a probe can reach a peer that leads by its own view.
    /// That peer answers a registration, a completion, a rollback and a commit from its own table and asks no other
    /// node, because two such nodes would probe each other.
    /// </summary>
    [Fact]
    public async Task AProbeThatReachesALeader_StartsNoProbesOfItsOwn()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, IKahuna[] kahunas, _, MemoryInterNodeCommmunication interNode) =
            await AssembleClusterWithTransports(Nodes, "memory", Partitions, raftLogger, kahunaLogger);
        KahunaManager[] managers = [.. kahunas.Cast<KahunaManager>()];

        try
        {
            string key = "owner-probe-leader/" + Guid.NewGuid().ToString("N")[..8];
            (TransactionHandle handle, int partition, int owner) = await BeginOnItsCoordinator(rafts, managers, key, ct);

            int successor = (owner + 1) % Nodes;
            int third = (owner + 2) % Nodes;
            await MoveLeadershipSeenBy(rafts, partition, owner, successor, third, ct);

            TransactionOperationId pending = TransactionOperationId.NewRandom();
            (OperationRegistrationOutcome registered, _, _, _, _, _) = await managers[owner].LocateAndBeginOperation(
                handle.CoordinatorKey, handle.TransactionId, pending, OperationKind.Set, null, ct);
            Assert.Equal(OperationRegistrationOutcome.New, registered);

            int beginsBefore = interNode.BeginOperationCallCount;
            int completionsBefore = interNode.CompleteOperationCallCount;

            using (ForwardedRequestScope.EnterAt(ForwardedRequestScope.MaxForwardHops + 1))
            {
                (OperationRegistrationOutcome outcome, _, _, _, _, _) = await managers[successor].LocateAndBeginOperation(
                    handle.CoordinatorKey, handle.TransactionId, TransactionOperationId.NewRandom(), OperationKind.Set, null, ct);
                Assert.Equal(OperationRegistrationOutcome.RejectedSessionClosed, outcome);

                await Assert.ThrowsAsync<KahunaServerException>(async () => await managers[successor].LocateAndCompleteOperation(
                    handle.CoordinatorKey, handle.TransactionId, pending, new OperationCompletionPayload { CachedType = KeyValueResponseType.Set }, ct));

                Assert.Equal(KeyValueResponseType.Errored, await managers[successor].LocateAndRollbackTransaction(handle, ct));
                Assert.Equal(KeyValueResponseType.Errored, (await managers[successor].LocateAndCommitTransaction(handle, ct)).Type);
            }

            Assert.Equal(beginsBefore, interNode.BeginOperationCallCount);
            Assert.Equal(completionsBefore, interNode.CompleteOperationCallCount);

            // The leader decided nothing for the session: its owner holds it, open to operations.
            (registered, _, _, _, _, _) = await managers[owner].LocateAndBeginOperation(
                handle.CoordinatorKey, handle.TransactionId, TransactionOperationId.NewRandom(), OperationKind.Set, null, ct);
            Assert.Equal(OperationRegistrationOutcome.New, registered);
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    /// <summary>
    /// A query and a close for a transaction that no node holds cost the forwards of the route and one probe of each
    /// peer, from every node. A probed peer that sent the request on, or that probed the peers itself, would show here.
    /// </summary>
    [Fact]
    public async Task ATransactionNoNodeHolds_CostsOneProbeOfEachPeer()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, IKahuna[] kahunas, _, MemoryInterNodeCommmunication interNode) =
            await AssembleClusterWithTransports(Nodes, "memory", Partitions, raftLogger, kahunaLogger);
        KahunaManager[] managers = [.. kahunas.Cast<KahunaManager>()];

        try
        {
            string key = "owner-none/" + Guid.NewGuid().ToString("N")[..8];
            HLCTimestamp unknown = new(1, 42, 0);
            int partition = CoordinatorPartitionOf(managers[0], key);

            await RunUnderStableLeadership(rafts[0], Partitions, async () =>
            {
                int leader = await LeaderIndexOf(partition, rafts, ct);

                for (int node = 0; node < Nodes; node++)
                {
                    IRaft raft = rafts[node];
                    await WaitUntilAsync(async () => await raft.WaitForLeader(partition, ct) == EndpointOf(leader));

                    int forwards = node == leader ? 0 : 1;

                    int queriesBefore = interNode.WorkingSetCallCount;
                    long unroutedBefore = managers.Sum(m => m.KeyValues.SessionRegistrationCounts.Unrouted);
                    Assert.Null(await managers[node].LocateAndGetTransactionWorkingSet(key, unknown, ct));
                    Assert.Equal(forwards + Nodes - 1, interNode.WorkingSetCallCount - queriesBefore);

                    // A probed peer did not look for a leader to send the query to.
                    Assert.Equal(unroutedBefore, managers.Sum(m => m.KeyValues.SessionRegistrationCounts.Unrouted));

                    int closesBefore = interNode.CloseTransactionCallCount;
                    (KeyValueResponseType closeType, TransactionWorkingSet? snapshot) = await managers[node].LocateAndCloseTransaction(key, unknown, ct);
                    Assert.Equal(KeyValueResponseType.Errored, closeType);
                    Assert.Null(snapshot);
                    Assert.Equal(forwards + Nodes - 1, interNode.CloseTransactionCallCount - closesBefore);
                }
            });
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    /// <summary>
    /// Sends a working-set query and then a close for <paramref name="handle"/> to <paramref name="receiver"/> through
    /// the inter-node gRPC batch stream, stamped with <paramref name="forwardHops"/>, and returns the two answers.
    /// </summary>
    private static async Task<(GrpcGetTransactionWorkingSetResponse Query, GrpcCloseTransactionResponse Close)> SendQueryAndClose(
        IKahuna receiver, TransactionHandle handle, int forwardHops)
    {
        GrpcBatchServerKeyValueResponse query = await SendOne(receiver, new GrpcBatchServerKeyValueRequest
        {
            Type = GrpcServerBatchType.ServerGetTransactionWorkingSet,
            RequestId = 1,
            ForwardHops = forwardHops,
            GetTransactionWorkingSet = new GrpcGetTransactionWorkingSetRequest
            {
                CoordinatorKey = handle.CoordinatorKey,
                TransactionIdNode = handle.TransactionId.N,
                TransactionIdPhysical = handle.TransactionId.L,
                TransactionIdCounter = handle.TransactionId.C
            }
        });

        GrpcBatchServerKeyValueResponse close = await SendOne(receiver, new GrpcBatchServerKeyValueRequest
        {
            Type = GrpcServerBatchType.ServerCloseTransaction,
            RequestId = 2,
            ForwardHops = forwardHops,
            CloseTransaction = new GrpcCloseTransactionRequest
            {
                CoordinatorKey = handle.CoordinatorKey,
                TransactionIdNode = handle.TransactionId.N,
                TransactionIdPhysical = handle.TransactionId.L,
                TransactionIdCounter = handle.TransactionId.C
            }
        });

        return (query.GetTransactionWorkingSet, close.CloseTransaction);
    }

    /// <summary>Sends one request over an inter-node batch stream of its own, so its answer is not bundled with another.</summary>
    private static async Task<GrpcBatchServerKeyValueResponse> SendOne(IKahuna receiver, GrpcBatchServerKeyValueRequest request)
    {
        KeyValuesService service = new(receiver, NodeTransportGate.Disabled, NullLogger<IKahuna>.Instance);
        CapturingStreamWriter<GrpcBatchServerKeyValueResponse> responses = new();

        await service.BatchServerKeyValueRequests(
            new ListStreamReader<GrpcBatchServerKeyValueRequest>([request]), responses, new StubServerCallContext());

        GrpcBatchServerKeyValueResponse response = Assert.Single(responses.Written);
        Assert.Equal(request.Type, response.Type);
        Assert.Equal(request.RequestId, response.RequestId);

        return response;
    }

    /// <summary>Feeds a fixed list of requests to the batch handler, then closes the stream.</summary>
    private sealed class ListStreamReader<T>(IReadOnlyList<T> items) : IAsyncStreamReader<T>
    {
        private int index = -1;

        public T Current => items[index];

        public Task<bool> MoveNext(CancellationToken cancellationToken) => Task.FromResult(++index < items.Count);
    }

    /// <summary>Captures everything the batch handler writes back onto the response stream.</summary>
    private sealed class CapturingStreamWriter<T> : IServerStreamWriter<T>
    {
        private readonly Lock mutex = new();

        public List<T> Written { get; } = [];

        public WriteOptions? WriteOptions { get; set; }

        public Task WriteAsync(T message)
        {
            lock (mutex)
                Written.Add(message);

            return Task.CompletedTask;
        }
    }

    /// <summary>Minimal context: the handlers read only the cancellation token.</summary>
    private sealed class StubServerCallContext : ServerCallContext
    {
        protected override CancellationToken CancellationTokenCore => TestContext.Current.CancellationToken;
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
        protected override Task WriteResponseHeadersAsyncCore(Metadata responseHeaders) => Task.CompletedTask;
    }
}
