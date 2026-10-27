using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Communication;
using Kommander.Data;
using Kommander.Diagnostics;
using Kommander.Discovery;
using Kommander.Scheduling;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL;
using Kommander.WAL.IO;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// A transaction's range lock lives on the leader that granted it, and its release is sent by the node that
/// coordinates the transaction. When that node believes it leads the partition but was deposed, or is going
/// away, it holds nothing of the transaction and cannot vouch that the lock is gone. These tests pin what then
/// happens to the release: it travels to the other voters and the confirmed leader drops the lock; a release
/// that no node can vouch for is not acknowledged; a commit retries the releases it could not get acknowledged
/// until the lock lease ends; and a node that signaled its shutdown takes no new session, forwarded commit or
/// lock grant, while it still passes a release on.
/// </summary>
public sealed class TestStaleCoordinatorLockRelease : BaseCluster
{
    private const int Nodes = 3;

    private const int Partitions = 4;

    private const int LockExpiresMs = 60_000;

    private readonly ILogger<IRaft> raftLogger;

    private readonly ILogger<IKahuna> kahunaLogger;

    public TestStaleCoordinatorLockRelease(ITestOutputHelper outputHelper)
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

    private static KeyValuePair<int, string>[] FreshKeyPerPartition(KahunaManager probe, string tag)
    {
        string random = Guid.NewGuid().ToString("N")[..8];
        Dictionary<int, string> byPartition = [];

        for (int i = 0; i < 4_096 && byPartition.Count < Partitions; i++)
        {
            string candidate = $"{tag}{i}/{random}";
            int partition = probe.KeyValues.LocateDurablePartition(candidate).PartitionId;
            byPartition.TryAdd(partition, candidate);
        }

        return [.. byPartition.OrderBy(kv => kv.Key)];
    }

    /// <summary>
    /// A data partition and a coordinator partition with different leaders: the data leader holds the locks,
    /// the coordinator leader begins the sessions. When one node leads every partition, the leadership of the
    /// coordinator partition is transferred to another node first.
    /// </summary>
    private static async Task<(int DataPartition, string DataKey, int DataLeader, string CoordinatorKey, int CoordinatorLeader)> PickDistinctLeaders(
        KeyValuePair<int, string>[] keys, IRaft[] rafts, CancellationToken ct)
    {
        int[] leaders = new int[keys.Length];
        for (int i = 0; i < keys.Length; i++)
            leaders[i] = await LeaderIndexOf(keys[i].Key, rafts, ct);

        for (int data = 0; data < keys.Length; data++)
        {
            for (int coordinator = 0; coordinator < keys.Length; coordinator++)
            {
                if (data != coordinator && leaders[data] != leaders[coordinator])
                    return (keys[data].Key, keys[data].Value, leaders[data], keys[coordinator].Value, leaders[coordinator]);
            }
        }

        // One node leads every partition: move the second partition to the next node.
        int target = (leaders[1] + 1) % rafts.Length;
        RaftOperationStatus transferred = await rafts[leaders[1]].TransferLeadershipAsync(keys[1].Key, rafts[target].GetLocalEndpoint(), ct);
        Assert.Equal(RaftOperationStatus.Success, transferred);

        int moved = -1;
        await WaitUntilAsync(async () =>
        {
            moved = await LeaderIndexOf(keys[1].Key, rafts, ct);
            return moved != leaders[0];
        }, timeoutMs: 30_000);

        return (keys[0].Key, keys[0].Value, leaders[0], keys[1].Value, moved);
    }

    private static async Task Seed(KahunaManager session, string key, CancellationToken ct)
    {
        (KeyValueResponseType seedType, _, _) = await RetryOnMustRetryAsync(
            () => session.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, key, "seed"u8.ToArray(), null, -1, KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct),
            r => r.Item1);

        Assert.Equal(KeyValueResponseType.Set, seedType);
    }

    /// <summary>A pessimistic session whose commit releases its working set before it answers.</summary>
    private static async Task<TransactionHandle> StartPessimistic(KahunaManager session, string coordinatorKey, CancellationToken ct)
    {
        (KeyValueResponseType startType, TransactionHandle handle) = await RetryOnMustRetryAsync(
            () => session.LocateAndStartTransaction(
                new KeyValueTransactionOptions
                {
                    CoordinatorKey = coordinatorKey,
                    Locking = KeyValueTransactionLocking.Pessimistic,
                    AsyncRelease = false,
                    Timeout = 60_000
                }, ct),
            r => r.Item1);

        Assert.Equal(KeyValueResponseType.Set, startType);
        return handle;
    }

    /// <summary>Acquires (or upgrades to) a range lock over the single key, registered with the coordinator.</summary>
    private static Task<KeyValueResponseType> TryPointRangeLock(
        KahunaManager session, TransactionHandle handle, string key, RangeLockMode mode, CancellationToken ct, int expiresMs = LockExpiresMs) =>
        RetryOnMustRetryAsync(async () =>
        {
            (KeyValueResponseType lockType, _) = await session.LocateAndTryAcquireRangeLock(
                handle.TransactionId, BucketOf(key), key, true, key, true, expiresMs, KeyValueDurability.Persistent, mode, ct,
                coordinatorKey: handle.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
            return lockType;
        }, t => t);

    private static Task<KeyValueResponseType> ReleasePointRangeLock(KahunaManager from, HLCTimestamp transactionId, string key, CancellationToken ct) =>
        from.LocateAndTryReleaseExclusiveRangeLock(transactionId, BucketOf(key), key, true, key, true, KeyValueDurability.Persistent, ct);

    private static async Task<bool> HoldsRangeLock(KahunaManager node, string key, HLCTimestamp transactionId)
    {
        List<KeyValueRangeLock> held = await node.KeyValues.GetRangeLocksAsync(BucketOf(key));
        return held.Any(l => l.TransactionId == transactionId);
    }

    private async Task<(IRaft[] Rafts, KahunaManager[] Managers, List<LeadershipOverridingRaft> Overrides)> AssembleOverridable()
    {
        List<LeadershipOverridingRaft> overrides = [];

        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleCluster(
            Nodes, "memory", Partitions, raftLogger, kahunaLogger,
            wrapRaft: raft =>
            {
                LeadershipOverridingRaft wrapped = new(raft);
                overrides.Add(wrapped);
                return wrapped;
            });

        return (rafts, [.. kahunas.Cast<KahunaManager>()], overrides);
    }

    /// <summary>
    /// The lock lives on the leader of its partition. Another node believes it leads that partition, without a
    /// quorum confirmation, and holds nothing of the transaction: the deposed form of a coordinator. A release
    /// it sends must still reach the leader and drop the lock, so a writer into the range is admitted at once.
    /// </summary>
    [Fact]
    public async Task StaleCoordinator_ReleaseReachesTheLeader_AndDropsTheLock()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, KahunaManager[] managers, List<LeadershipOverridingRaft> overrides) = await AssembleOverridable();

        try
        {
            KeyValuePair<int, string>[] keys = FreshKeyPerPartition(managers[0], "scr");
            Assert.True(keys.Length >= 2, "the fixture needs a data partition and a coordinator partition");

            (int dataPartition, string dataKey) = (keys[0].Key, keys[0].Value);
            string coordinatorKey = keys[1].Value;

            int dataLeader = await LeaderIndexOf(dataPartition, rafts, ct);
            int stale = (dataLeader + 1) % Nodes;

            KahunaManager leader = managers[dataLeader];
            await Seed(leader, dataKey, ct);

            TransactionHandle holder = await StartPessimistic(leader, coordinatorKey, ct);
            Assert.Equal(KeyValueResponseType.Locked, await TryPointRangeLock(leader, holder, dataKey, RangeLockMode.Shared, ct));
            Assert.True(await HoldsRangeLock(leader, dataKey, holder.TransactionId));

            // The stale node believes it leads the data partition; no quorum confirms it.
            overrides[stale].ClaimWithoutConfirmation(dataPartition);

            KeyValueResponseType released;
            try
            {
                released = await ReleasePointRangeLock(managers[stale], holder.TransactionId, dataKey, ct);
            }
            finally
            {
                overrides[stale].Restore();
            }

            Assert.Equal(KeyValueResponseType.Unlocked, released);
            Assert.False(await HoldsRangeLock(leader, dataKey, holder.TransactionId), "the leader must have dropped the lock the stale node could not vouch for");

            TransactionHandle writer = await StartPessimistic(leader, coordinatorKey, ct);
            Assert.Equal(KeyValueResponseType.Locked, await TryPointRangeLock(leader, writer, dataKey, RangeLockMode.Exclusive, ct));

            await leader.LocateAndRollbackTransaction(writer, ct);
            await leader.LocateAndRollbackTransaction(holder, ct);
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    /// <summary>
    /// The same release, when no node can vouch for the partition: the stale node believes it leads without a
    /// confirmation, and the real leader no longer believes it leads at all (the moment between a lost election
    /// and the drop of its state). The release must not be acknowledged, and the lock must stay where it is.
    /// </summary>
    [Fact]
    public async Task NoConfirmedLeader_ReleaseIsNotAcknowledged_AndTheLockStays()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, KahunaManager[] managers, List<LeadershipOverridingRaft> overrides) = await AssembleOverridable();

        try
        {
            KeyValuePair<int, string>[] keys = FreshKeyPerPartition(managers[0], "ncl");
            Assert.True(keys.Length >= 2, "the fixture needs a data partition and a coordinator partition");

            (int dataPartition, string dataKey) = (keys[0].Key, keys[0].Value);
            string coordinatorKey = keys[1].Value;

            int dataLeader = await LeaderIndexOf(dataPartition, rafts, ct);
            int stale = (dataLeader + 1) % Nodes;

            KahunaManager leader = managers[dataLeader];
            await Seed(leader, dataKey, ct);

            TransactionHandle holder = await StartPessimistic(leader, coordinatorKey, ct);
            Assert.Equal(KeyValueResponseType.Locked, await TryPointRangeLock(leader, holder, dataKey, RangeLockMode.Shared, ct));
            Assert.True(await HoldsRangeLock(leader, dataKey, holder.TransactionId));

            overrides[stale].ClaimWithoutConfirmation(dataPartition);
            overrides[dataLeader].Disown(dataPartition);

            KeyValueResponseType released;
            try
            {
                released = await ReleasePointRangeLock(managers[stale], holder.TransactionId, dataKey, ct);

                Assert.Equal(KeyValueResponseType.MustRetry, released);
                Assert.True(await HoldsRangeLock(leader, dataKey, holder.TransactionId), "no node could vouch for the lock, so it must stay");
            }
            finally
            {
                overrides[stale].Restore();
                overrides[dataLeader].Restore();
            }

            // With the leader back, the same release lands.
            Assert.Equal(KeyValueResponseType.Unlocked, await RetryOnMustRetryAsync(
                () => ReleasePointRangeLock(managers[stale], holder.TransactionId, dataKey, ct), t => t));
            Assert.False(await HoldsRangeLock(leader, dataKey, holder.TransactionId));

            await leader.LocateAndRollbackTransaction(holder, ct);
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    /// <summary>
    /// The commit's cleanup cannot get the range-lock release acknowledged at first: the leader of the data
    /// partition answers nothing it can vouch for. The commit answers anyway, and a detached retry resends the
    /// release until it lands. Once the leader is back, the lock is gone well before its lease ends, and the
    /// late release is counted.
    /// </summary>
    [Fact]
    public async Task CommitRetriesTheReleaseItCouldNotGetAcknowledged_UntilTheLeaderIsBack()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, KahunaManager[] managers, List<LeadershipOverridingRaft> overrides) = await AssembleOverridable();

        try
        {
            KeyValuePair<int, string>[] keys = FreshKeyPerPartition(managers[0], "crr");
            Assert.True(keys.Length >= 2, "the fixture needs a data partition and a coordinator partition");

            (int dataPartition, string dataKey, int dataLeader, string coordinatorKey, int coordinatorLeader) = await PickDistinctLeaders(keys, rafts, ct);

            KahunaManager leader = managers[dataLeader];
            KahunaManager coordinator = managers[coordinatorLeader];
            await Seed(leader, dataKey, ct);

            // The session lives on the coordinator node; the lock lives on the data leader.
            TransactionHandle holder = await StartPessimistic(coordinator, coordinatorKey, ct);
            Assert.Equal(KeyValueResponseType.Locked, await TryPointRangeLock(coordinator, holder, dataKey, RangeLockMode.Shared, ct));
            Assert.True(await HoldsRangeLock(leader, dataKey, holder.TransactionId));

            long lateBefore = DurableTransactionMetrics.LockReleasesLateCount;
            long retriesBefore = DurableTransactionMetrics.LockReleaseRetriesCount;

            // The data leader stops answering for the partition: it neither confirms nor believes it leads.
            overrides[dataLeader].Disown(dataPartition);

            TransactionCommitAnswer commit;
            try
            {
                commit = await coordinator.LocateAndCommitTransaction(holder, ct);

                // The commit answered; the release was not acknowledged, and the lock is still there.
                Assert.NotEqual(KeyValueResponseType.MustRetry, commit.Type);
                Assert.True(await HoldsRangeLock(leader, dataKey, holder.TransactionId), "the first release attempt cannot land while no node vouches");

                await WaitUntilAsync(() => DurableTransactionMetrics.LockReleaseRetriesCount > retriesBefore, timeoutMs: 5_000);
            }
            finally
            {
                overrides[dataLeader].Restore();
            }

            // The leader is back: a retry lands the release long before the 60 s lease ends.
            await WaitUntilAsync(async () => !await HoldsRangeLock(leader, dataKey, holder.TransactionId), timeoutMs: 10_000);
            await WaitUntilAsync(() => DurableTransactionMetrics.LockReleasesLateCount > lateBefore, timeoutMs: 5_000);

            TransactionHandle writer = await StartPessimistic(leader, coordinatorKey, ct);
            Assert.Equal(KeyValueResponseType.Locked, await TryPointRangeLock(leader, writer, dataKey, RangeLockMode.Exclusive, ct));
            await leader.LocateAndRollbackTransaction(writer, ct);
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    /// <summary>
    /// The same cleanup, when no node ever vouches again: the retry stops when the lock lease ends, because the
    /// lock is gone by then anyway, and the release is counted as left to the lease.
    /// </summary>
    [Fact]
    public async Task CommitRetryStopsAtTheLeaseEnd_AndCountsTheReleaseLeftToTheLease()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, KahunaManager[] managers, List<LeadershipOverridingRaft> overrides) = await AssembleOverridable();

        try
        {
            KeyValuePair<int, string>[] keys = FreshKeyPerPartition(managers[0], "crl");
            Assert.True(keys.Length >= 2, "the fixture needs a data partition and a coordinator partition");

            (int dataPartition, string dataKey, int dataLeader, string coordinatorKey, int coordinatorLeader) = await PickDistinctLeaders(keys, rafts, ct);

            KahunaManager leader = managers[dataLeader];
            KahunaManager coordinator = managers[coordinatorLeader];
            await Seed(leader, dataKey, ct);

            const int shortLeaseMs = 3_000;

            TransactionHandle holder = await StartPessimistic(coordinator, coordinatorKey, ct);
            Assert.Equal(KeyValueResponseType.Locked, await TryPointRangeLock(coordinator, holder, dataKey, RangeLockMode.Shared, ct, shortLeaseMs));
            Assert.True(await HoldsRangeLock(leader, dataKey, holder.TransactionId));

            long leftBefore = DurableTransactionMetrics.LockReleasesLeftToLeaseCount;
            long startedTicks = Environment.TickCount64;

            overrides[dataLeader].Disown(dataPartition);

            try
            {
                TransactionCommitAnswer commit = await coordinator.LocateAndCommitTransaction(holder, ct);
                Assert.NotEqual(KeyValueResponseType.MustRetry, commit.Type);

                // The retry gives up once the 3 s lease has ended, not at the 60 s a renewal would have kept.
                await WaitUntilAsync(() => DurableTransactionMetrics.LockReleasesLeftToLeaseCount > leftBefore, timeoutMs: 15_000);

                long elapsedMs = Environment.TickCount64 - startedTicks;
                Assert.True(elapsedMs < 12_000, $"the retry must stop at the lease end, not after {elapsedMs} ms");
            }
            finally
            {
                overrides[dataLeader].Restore();
            }
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    /// <summary>
    /// A node that signaled its shutdown, with its Raft still alive: a new session, a commit of a session it
    /// does not own and a lock grant are refused with MustRetry, each refusal counted; a release it is asked
    /// to serve still reaches the leader that holds the lock.
    /// </summary>
    [Fact]
    public async Task StoppingNode_RefusesNewWork_AndStillPassesAReleaseOn()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, KahunaManager[] managers, _) = await AssembleOverridable();

        try
        {
            KeyValuePair<int, string>[] keys = FreshKeyPerPartition(managers[0], "stp");
            Assert.True(keys.Length >= 2, "the fixture needs a data partition and a coordinator partition");

            (int dataPartition, string dataKey) = (keys[0].Key, keys[0].Value);
            string coordinatorKey = keys[1].Value;

            int dataLeader = await LeaderIndexOf(dataPartition, rafts, ct);
            int stopping = (dataLeader + 1) % Nodes;

            KahunaManager leader = managers[dataLeader];
            await Seed(leader, dataKey, ct);

            TransactionHandle holder = await StartPessimistic(leader, coordinatorKey, ct);
            Assert.Equal(KeyValueResponseType.Locked, await TryPointRangeLock(leader, holder, dataKey, RangeLockMode.Shared, ct));
            Assert.True(await HoldsRangeLock(leader, dataKey, holder.TransactionId));

            KahunaManager stoppingNode = managers[stopping];
            long refusedBefore = stoppingNode.KeyValues.RefusedWhileStoppingCount;

            stoppingNode.SignalShutdown();

            // A new session.
            (KeyValueResponseType beginType, _) = await stoppingNode.LocateAndStartTransaction(
                new KeyValueTransactionOptions { CoordinatorKey = coordinatorKey, Locking = KeyValueTransactionLocking.Pessimistic, Timeout = 60_000 }, ct);
            Assert.Equal(KeyValueResponseType.MustRetry, beginType);

            // A commit of a session this node does not own.
            TransactionCommitAnswer commit = await stoppingNode.LocateAndCommitTransaction(holder, ct);
            Assert.Equal(KeyValueResponseType.MustRetry, commit.Type);
            Assert.Equal(CommitRefusalClass.Unresolved, commit.Refusal);

            // A lock grant.
            (KeyValueResponseType grantType, _) = await stoppingNode.LocateAndTryAcquireRangeLock(
                holder.TransactionId, BucketOf(dataKey), dataKey, true, dataKey, true, LockExpiresMs, KeyValueDurability.Persistent, RangeLockMode.Shared, ct);
            Assert.Equal(KeyValueResponseType.MustRetry, grantType);

            Assert.True(stoppingNode.KeyValues.RefusedWhileStoppingCount >= refusedBefore + 3, "each refusal is counted");

            // A release is passed on to a node whose leadership view is live, and lands on the leader.
            Assert.Equal(KeyValueResponseType.Unlocked, await RetryOnMustRetryAsync(
                () => ReleasePointRangeLock(stoppingNode, holder.TransactionId, dataKey, ct), t => t));
            Assert.False(await HoldsRangeLock(leader, dataKey, holder.TransactionId));

            await leader.LocateAndRollbackTransaction(holder, ct);
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    /// <summary>
    /// The shutdown form end to end, as a cluster consumer meets it: sessions begun through the coordinator
    /// partition's leader take Shared range locks on another node's partition and commit, in a loop, while the
    /// coordinator node is disposed under them. No lock of a finished transaction may stay on the data leader,
    /// and a writer into the range must be admitted within seconds, not after the lock lease.
    /// </summary>
    [Fact]
    public async Task DisposedCoordinatorNode_LeavesNoLockBehind_OnTheDataLeader()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, KahunaManager[] managers, _) = await AssembleOverridable();

        int disposedNode = -1;

        try
        {
            KeyValuePair<int, string>[] keys = FreshKeyPerPartition(managers[0], "dcn");
            Assert.True(keys.Length >= 2, "the fixture needs a data partition and a coordinator partition");

            (_, string dataKey, int dataLeader, string coordinatorKey, int coordinatorLeader) = await PickDistinctLeaders(keys, rafts, ct);

            KahunaManager leader = managers[dataLeader];
            await Seed(leader, dataKey, ct);

            using CancellationTokenSource loopCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            int scans = 0;

            // The consumer's loop, on the data leader: begin (routed to the coordinator leader), lock, commit.
            async Task ScanLoop()
            {
                while (!loopCts.IsCancellationRequested)
                {
                    try
                    {
                        (KeyValueResponseType beginType, TransactionHandle handle) = await leader.LocateAndStartTransaction(
                            new KeyValueTransactionOptions
                            {
                                CoordinatorKey = coordinatorKey,
                                Locking = KeyValueTransactionLocking.Pessimistic,
                                AsyncRelease = false,
                                Timeout = 60_000
                            }, loopCts.Token);

                        if (beginType != KeyValueResponseType.Set)
                        {
                            await Task.Delay(20, loopCts.Token);
                            continue;
                        }

                        (KeyValueResponseType lockType, _) = await leader.LocateAndTryAcquireRangeLock(
                            handle.TransactionId, BucketOf(dataKey), dataKey, true, dataKey, true, LockExpiresMs, KeyValueDurability.Persistent, RangeLockMode.Shared, loopCts.Token,
                            coordinatorKey: handle.CoordinatorKey, operationId: TransactionOperationId.NewRandom());

                        if (lockType == KeyValueResponseType.Locked)
                            Interlocked.Increment(ref scans);

                        await leader.LocateAndCommitTransaction(handle, loopCts.Token);
                    }
                    catch (OperationCanceledException)
                    {
                        return;
                    }
                    catch (Exception)
                    {
                        // The coordinator node is going away under the loop; the next iteration routes again.
                        try { await Task.Delay(20, loopCts.Token); } catch (OperationCanceledException) { return; }
                    }
                }
            }

            Task loop = ScanLoop();

            await WaitUntilAsync(() => Volatile.Read(ref scans) >= 3, timeoutMs: 20_000);

            await LeaveCluster(rafts[coordinatorLeader]);
            disposedNode = coordinatorLeader;

            await Task.Delay(2_000, ct);
            loopCts.Cancel();
            await loop;

            Assert.True(Volatile.Read(ref scans) >= 3);

            // Every scan's lock is gone from the data leader within seconds, not after the 60 s lease.
            await WaitUntilAsync(async () => (await leader.KeyValues.GetRangeLocksAsync(BucketOf(dataKey))).Count == 0, timeoutMs: 10_000);

            TransactionHandle writer = await StartPessimistic(leader, "", ct);
            Assert.Equal(KeyValueResponseType.Locked, await TryPointRangeLock(leader, writer, dataKey, RangeLockMode.Exclusive, ct));
            await leader.LocateAndRollbackTransaction(writer, ct);
        }
        finally
        {
            List<Task> teardown = [];
            for (int i = 0; i < rafts.Length; i++)
            {
                if (i != disposedNode)
                    teardown.Add(LeaveCluster(rafts[i]));
            }

            await Task.WhenAll(teardown);
        }
    }

    /// <summary>
    /// An <see cref="IRaft"/> that delegates every member to the real <see cref="RaftManager"/>, except that
    /// Kahuna's view of one partition's leadership can be faulted: the confirmation refused, the leadership
    /// claimed without one (the node answers that it leads and names itself as the leader), or disowned (the
    /// node answers that it does not lead). The real instance keeps leading, heartbeating and replicating.
    /// </summary>
    private sealed class LeadershipOverridingRaft(IRaft inner) : IRaft
    {
        private int refusedPartition = -1;

        private int claimedPartition = -1;

        private int disownedPartition = -1;

        /// <summary>This node answers that it leads <paramref name="partitionId"/>, and that no quorum confirms it.</summary>
        public void ClaimWithoutConfirmation(int partitionId)
        {
            Volatile.Write(ref refusedPartition, partitionId);
            Volatile.Write(ref claimedPartition, partitionId);
        }

        /// <summary>This node answers that it does not lead <paramref name="partitionId"/>, and confirms nothing for it.</summary>
        public void Disown(int partitionId)
        {
            Volatile.Write(ref refusedPartition, partitionId);
            Volatile.Write(ref disownedPartition, partitionId);
        }

        public void Restore()
        {
            Volatile.Write(ref refusedPartition, -1);
            Volatile.Write(ref claimedPartition, -1);
            Volatile.Write(ref disownedPartition, -1);
        }

        public ValueTask<bool> ConfirmLeadershipAsync(int partitionId, CancellationToken cancellationToken = default) =>
            Volatile.Read(ref refusedPartition) == partitionId
                ? ValueTask.FromResult(false)
                : inner.ConfirmLeadershipAsync(partitionId, cancellationToken);

        public ValueTask<bool> AmILeaderQuick(int partitionId)
        {
            if (Volatile.Read(ref claimedPartition) == partitionId)
                return ValueTask.FromResult(true);

            if (Volatile.Read(ref disownedPartition) == partitionId)
                return ValueTask.FromResult(false);

            return inner.AmILeaderQuick(partitionId);
        }

        public ValueTask<bool> AmILeader(int partitionId, CancellationToken cancellationToken)
        {
            if (Volatile.Read(ref claimedPartition) == partitionId)
                return ValueTask.FromResult(true);

            if (Volatile.Read(ref disownedPartition) == partitionId)
                return ValueTask.FromResult(false);

            return inner.AmILeader(partitionId, cancellationToken);
        }

        public ValueTask<string> WaitForLeader(int partitionId, CancellationToken cancellationToken) =>
            Volatile.Read(ref claimedPartition) == partitionId
                ? ValueTask.FromResult(inner.GetLocalEndpoint())
                : inner.WaitForLeader(partitionId, cancellationToken);

        public bool Joined => inner.Joined;
        public IWAL WalAdapter => inner.WalAdapter;
        public ICommunication Communication => inner.Communication;
        public IDiscovery Discovery => inner.Discovery;
        public RaftConfiguration Configuration => inner.Configuration;
        public HybridLogicalClock HybridLogicalClock => inner.HybridLogicalClock;
        public IRaftReadScheduler ReadScheduler => inner.ReadScheduler;
        public IRaftWalScheduler WalScheduler => inner.WalScheduler;
        public bool IsInitialized => inner.IsInitialized;
        public ClusterMemberRole LocalRole => inner.LocalRole;

        public event Action<int>? OnRestoreStarted { add => inner.OnRestoreStarted += value; remove => inner.OnRestoreStarted -= value; }
        public event Action<int>? OnRestoreFinished { add => inner.OnRestoreFinished += value; remove => inner.OnRestoreFinished -= value; }
        public event Action<int, RaftLog>? OnReplicationError { add => inner.OnReplicationError += value; remove => inner.OnReplicationError -= value; }
        public event Func<int, RaftLog, Task<bool>>? OnLogRestored { add => inner.OnLogRestored += value; remove => inner.OnLogRestored -= value; }
        public event Func<int, RaftLog, Task<bool>>? OnReplicationReceived { add => inner.OnReplicationReceived += value; remove => inner.OnReplicationReceived -= value; }
        public event Func<int, string, Task<bool>>? OnLeaderChanged { add => inner.OnLeaderChanged += value; remove => inner.OnLeaderChanged -= value; }
        public event Func<int, long, Task>? OnLeadershipLost { add => inner.OnLeadershipLost += value; remove => inner.OnLeadershipLost -= value; }
        public event Action<IReadOnlyList<RaftPartitionRange>>? OnPartitionMapChanged { add => inner.OnPartitionMapChanged += value; remove => inner.OnPartitionMapChanged -= value; }
        public event Action<ClusterMembership>? OnMembershipChanged { add => inner.OnMembershipChanged += value; remove => inner.OnMembershipChanged -= value; }

        public ClusterMembership GetMembership() => inner.GetMembership();
        public Task JoinCluster(CancellationToken cancellationToken = default) => inner.JoinCluster(cancellationToken);
        public Task JoinCluster(IEnumerable<string> seeds, CancellationToken cancellationToken = default) => inner.JoinCluster(seeds, cancellationToken);
        public Task LeaveCluster(bool dispose = false, CancellationToken cancellationToken = default) => inner.LeaveCluster(dispose, cancellationToken);
        public Task<LeaveClusterResult> RequestLeaveAsync(CancellationToken cancellationToken = default) => inner.RequestLeaveAsync(cancellationToken);
        public Task UpdateNodes() => inner.UpdateNodes();
        public IList<RaftNode> GetNodes() => inner.GetNodes();
        public HLCTimestamp GetLastNodeActivity(string endpoint) => inner.GetLastNodeActivity(endpoint);
        public IReadOnlyList<string> GetActiveNodes(TimeSpan within) => inner.GetActiveNodes(within);
        public Task Handshake(HandshakeRequest request) => inner.Handshake(request);
        public void RequestVote(RequestVotesRequest request) => inner.RequestVote(request);
        public void Vote(VoteRequest request) => inner.Vote(request);
        public void AppendLogs(AppendLogsRequest request) => inner.AppendLogs(request);
        public void CompleteAppendLogs(CompleteAppendLogsRequest request) => inner.CompleteAppendLogs(request);
        public Task<RaftReplicationResult> ReplicateLogs(int partitionId, string type, byte[] data, bool autoCommit = true, long expectedGeneration = 0, long expectedTerm = 0, CancellationToken cancellationToken = default) => inner.ReplicateLogs(partitionId, type, data, autoCommit, expectedGeneration, expectedTerm, cancellationToken);
        public Task<RaftReplicationResult> ReplicateLogs(int partitionId, string type, IEnumerable<byte[]> logs, bool autoCommit = true, long expectedGeneration = 0, long expectedTerm = 0, CancellationToken cancellationToken = default) => inner.ReplicateLogs(partitionId, type, logs, autoCommit, expectedGeneration, expectedTerm, cancellationToken);
        public Task<RaftBatchReplicationResult> ReplicateEntries(int partitionId, IReadOnlyList<RaftProposalEntry> entries, CancellationToken cancellationToken = default) => inner.ReplicateEntries(partitionId, entries, cancellationToken);
        public Task<RaftReplicationResult> ReplicateCheckpoint(int partitionId, CancellationToken cancellationToken = default) => inner.ReplicateCheckpoint(partitionId, cancellationToken);
        public Task<(bool success, RaftOperationStatus status, long commitLogId)> CommitLogs(int partitionId, HLCTimestamp ticketId, CancellationToken cancellationToken = default) => inner.CommitLogs(partitionId, ticketId, cancellationToken);
        public Task<(bool success, RaftOperationStatus status, long commitLogId)> RollbackLogs(int partitionId, HLCTimestamp ticketId, CancellationToken cancellationToken = default) => inner.RollbackLogs(partitionId, ticketId, cancellationToken);
        public void SetMinRetainIndex(int partitionId, long index) => inner.SetMinRetainIndex(partitionId, index);
        public IDisposable AcquireRetentionHold(int partitionId, long index) => inner.AcquireRetentionHold(partitionId, index);
        public string GetLocalEndpoint() => inner.GetLocalEndpoint();
        public int GetLocalNodeId() => inner.GetLocalNodeId();
        public string GetLocalNodeName() => inner.GetLocalNodeName();
        public ValueTask<long?> GetFollowerLagAsync(int partitionId, string followerEndpoint) => inner.GetFollowerLagAsync(partitionId, followerEndpoint);
        public ValueTask<bool> ConfirmLocalApplicationAsync(int partitionId, CancellationToken cancellationToken = default) => inner.ConfirmLocalApplicationAsync(partitionId, cancellationToken);
        public IReadOnlyList<RaftReplica> GetPartitionReplicas(int partitionId) => inner.GetPartitionReplicas(partitionId);
        public int GetEffectiveReplicationFactor(int partitionId) => inner.GetEffectiveReplicationFactor(partitionId);
        public Task<RaftPartitionLifecycleResult> SetReplicationFactorAsync(int partitionId, int replicationFactor, CancellationToken ct = default) => inner.SetReplicationFactorAsync(partitionId, replicationFactor, ct);
        public ValueTask<string> WaitForLeaderStableAsync(int partitionId, TimeSpan minStableFor, CancellationToken cancellationToken = default) => inner.WaitForLeaderStableAsync(partitionId, minStableFor, cancellationToken);
        public ValueTask<string> WaitForLeaderStableAsync(int partitionId, TimeSpan minStableFor, TimeSpan timeout, CancellationToken cancellationToken = default) => inner.WaitForLeaderStableAsync(partitionId, minStableFor, timeout, cancellationToken);
        public Task<RaftOperationStatus> ForceLeaderForTestingAsync(int partitionId, CancellationToken cancellationToken = default) => inner.ForceLeaderForTestingAsync(partitionId, cancellationToken);
        public Task<RaftOperationStatus> StepDownAsync(int partitionId, CancellationToken cancellationToken = default) => inner.StepDownAsync(partitionId, cancellationToken);
        public Task<RaftOperationStatus> TransferLeadershipAsync(int partitionId, string targetEndpoint, CancellationToken cancellationToken = default) => inner.TransferLeadershipAsync(partitionId, targetEndpoint, cancellationToken);
        public Task<RaftOperationStatus> RequestReseedAsync(int partitionId, CancellationToken cancellationToken = default) => inner.RequestReseedAsync(partitionId, cancellationToken);
        public RaftOperationStatus SetCandidacyWithheld(int partitionId, bool withheld) => inner.SetCandidacyWithheld(partitionId, withheld);
        public bool IsCandidacyWithheld(int partitionId) => inner.IsCandidacyWithheld(partitionId);
        public Task<RaftOperationStatus> SuspendHeartbeatsAsync(int partitionId, CancellationToken cancellationToken = default) => inner.SuspendHeartbeatsAsync(partitionId, cancellationToken);
        public Task<RaftOperationStatus> ResumeHeartbeatsAsync(int partitionId, CancellationToken cancellationToken = default) => inner.ResumeHeartbeatsAsync(partitionId, cancellationToken);
        public Task<RaftPartitionLifecycleResult> CreatePartitionAsync(int partitionId, RaftRoutingMode mode = RaftRoutingMode.Unrouted, (int start, int end)? hashRange = null, CancellationToken ct = default) => inner.CreatePartitionAsync(partitionId, mode, hashRange, ct);
        public Task<RaftPartitionLifecycleResult> RemovePartitionAsync(int partitionId, CancellationToken ct = default) => inner.RemovePartitionAsync(partitionId, ct);
        public Task<RaftPartitionLifecycleResult> SplitPartitionAsync(int sourcePartitionId, int targetPartitionId = 0, RaftSplitPlan? plan = null, CancellationToken ct = default) => inner.SplitPartitionAsync(sourcePartitionId, targetPartitionId, plan, ct);
        public Task<RaftPartitionLifecycleResult> MergePartitionsAsync(int survivorPartitionId, int sourcePartitionId, RaftMergePlan? plan = null, CancellationToken ct = default) => inner.MergePartitionsAsync(survivorPartitionId, sourcePartitionId, plan, ct);
        public long GetPartitionGeneration(int partitionId) => inner.GetPartitionGeneration(partitionId);
        public long GetPartitionTerm(int partitionId) => inner.GetPartitionTerm(partitionId);
        public double GetPartitionLogOpsPerSecond(int partitionId) => inner.GetPartitionLogOpsPerSecond(partitionId);
        public int GetPartitionWalQueueDepth(int partitionId) => inner.GetPartitionWalQueueDepth(partitionId);
        public double GetPartitionCommitWaitMs(int partitionId) => inner.GetPartitionCommitWaitMs(partitionId);
        public long GetCommitIndex(int partitionId) => inner.GetCommitIndex(partitionId);
        public IReadOnlyList<RaftPartitionRange> GetPartitionMap() => inner.GetPartitionMap();
        public int GetPartitionKey(string partitionKey) => inner.GetPartitionKey(partitionKey);
        public int GetPrefixPartitionKey(string prefixPartitionKey) => inner.GetPrefixPartitionKey(prefixPartitionKey);
        public void RegisterStateMachineTransfer(IRaftStateMachineTransfer? transfer) => inner.RegisterStateMachineTransfer(transfer);
        public void RegisterSystemStateTransfer(IRaftSystemStateTransfer? transfer) => inner.RegisterSystemStateTransfer(transfer);
        public void RegisterPartitionStateTransfer(IRaftPartitionStateTransfer? transfer) => inner.RegisterPartitionStateTransfer(transfer);
        public long GetStaleProposedSkippedCount(int partitionId) => inner.GetStaleProposedSkippedCount(partitionId);
        public IReadOnlyList<RaftSnapshotStatus> GetSnapshotStatuses(int partitionId) => inner.GetSnapshotStatuses(partitionId);
        public IReadOnlyList<RaftBackfillStatus> GetBackfillStatuses(int partitionId) => inner.GetBackfillStatuses(partitionId);
        public bool HostsPartition(int partitionId) => inner.HostsPartition(partitionId);
        public string? GetPartitionLeaderHint(int partitionId) => inner.GetPartitionLeaderHint(partitionId);
        public IDisposable HoldCommittedProposalRepliesForTesting(int partitionId, Action<HeldProposalReply> onHeld) => inner.HoldCommittedProposalRepliesForTesting(partitionId, onHeld);
        public IDisposable SetSnapshotInstallGateForTesting(int partitionId, SnapshotInstallPhase phase, Func<SnapshotInstallSignal, ValueTask> gate) => inner.SetSnapshotInstallGateForTesting(partitionId, phase, gate);
        public Task<RaftOperationStatus> HoldConsumerAppliesForTesting(int partitionId, CancellationToken cancellationToken = default) => inner.HoldConsumerAppliesForTesting(partitionId, cancellationToken);
        public Task<RaftOperationStatus> ResumeConsumerAppliesForTesting(int partitionId, CancellationToken cancellationToken = default) => inner.ResumeConsumerAppliesForTesting(partitionId, cancellationToken);
    }
}
