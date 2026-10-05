using System.Text;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Handlers;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// A pessimistic transaction protects a read with a Shared range lock over the single key it reads. The lock
/// is a lease in the partition leader's memory, and the leader stays the leader for the whole test, so the
/// leadership term proves nothing here: what is at stake is the lease itself.
///
/// <para>Two things can end a lease while its holder is still running:</para>
/// <list type="bullet">
/// <item>The hybrid logical clock leaps forward, because a node's wall clock was stepped or a peer with a
/// stepped clock sent a timestamp. No time passed, so the lease must not be affected: the lock keeps excluding
/// other transactions and its holder commits.</item>
/// <item>The lease really runs out, because nothing renewed it in time. The leader then grants the key to
/// another transaction, and the holder, which was told nothing, must not commit.</item>
/// </list>
///
/// <para>The write skew is the shape that makes either failure visible: two transactions each read the key the
/// other one writes. With both read locks gone both writes are granted, and both transactions commit a value
/// computed from a state the other one changed.</para>
/// </summary>
public sealed class TestRangeLockLeaseLapse : BaseCluster
{
    private const int Nodes = 3;

    private const int Partitions = 4;

    private const int LockExpiresMs = 60_000;

    /// <summary>Further than <see cref="LockExpiresMs"/>, closer than the session timeout and the ceiling of a
    /// session-owned intent, so the range lock lease is the only bound the leap crosses.</summary>
    private const int ClockLeapMs = 90_000;

    private const int SessionTimeoutMs = 240_000;

    private readonly ILogger<IRaft> raftLogger;

    private readonly ILogger<IKahuna> kahunaLogger;

    public TestRangeLockLeaseLapse(ITestOutputHelper outputHelper)
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

    private static async Task<long> Seed(KahunaManager session, string key, CancellationToken ct)
    {
        (KeyValueResponseType seedType, long seedRevision, _) = await RetryOnMustRetryAsync(
            () => session.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, key, "seed"u8.ToArray(), null, -1, KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct),
            r => r.Item1);

        Assert.Equal(KeyValueResponseType.Set, seedType);
        return seedRevision;
    }

    private static async Task<TransactionHandle> StartPessimistic(KahunaManager session, string coordinatorKey, CancellationToken ct)
    {
        (KeyValueResponseType startType, TransactionHandle handle) = await session.LocateAndStartTransaction(
            new KeyValueTransactionOptions
            {
                CoordinatorKey = coordinatorKey,
                Locking = KeyValueTransactionLocking.Pessimistic,
                AsyncRelease = true,
                Timeout = SessionTimeoutMs
            }, ct);

        Assert.Equal(KeyValueResponseType.Set, startType);
        return handle;
    }

    /// <summary>Acquires (or upgrades to) a range lock over the single key, registered with the coordinator.</summary>
    private static Task<KeyValueResponseType> TryPointRangeLock(
        KahunaManager session, TransactionHandle handle, string key, RangeLockMode mode, int expiresMs, CancellationToken ct) =>
        RetryOnMustRetryAsync(async () =>
        {
            (KeyValueResponseType lockType, _) = await session.LocateAndTryAcquireRangeLock(
                handle.TransactionId, BucketOf(key), key, true, key, true, expiresMs, KeyValueDurability.Persistent, mode, ct,
                coordinatorKey: handle.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
            return lockType;
        }, t => t);

    /// <summary>Reads the key under a Shared range lock, without registering the read with the coordinator: the
    /// lock is the only protection the read has.</summary>
    private static async Task ReadUnderSharedLock(
        KahunaManager session, TransactionHandle handle, string key, int expiresMs, CancellationToken ct)
    {
        Assert.Equal(KeyValueResponseType.Locked, await TryPointRangeLock(session, handle, key, RangeLockMode.Shared, expiresMs, ct));

        (KeyValueResponseType readType, _) = await RetryOnMustRetryAsync(
            () => session.LocateAndTryGetValue(handle.TransactionId, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct),
            r => r.Item1);

        Assert.Equal(KeyValueResponseType.Get, readType);
    }

    /// <summary>The write of a pessimistic transaction: the Exclusive range lock over the key, the session-owned
    /// exclusive point lock, then the staged value. False as soon as one step is refused.</summary>
    private static async Task<bool> TryLockAndWrite(
        KahunaManager session, TransactionHandle handle, string key, string value, CancellationToken ct)
    {
        if (await TryPointRangeLock(session, handle, key, RangeLockMode.Exclusive, LockExpiresMs, ct) != KeyValueResponseType.Locked)
            return false;

        List<(KeyValueResponseType Type, string Key, KeyValueDurability Durability, HLCTimestamp Holder)> locks =
            await session.LocateAndTryAcquireManyExclusiveLocks(
                handle.TransactionId, [(key, 0, KeyValueDurability.Persistent)], ct,
                coordinatorKey: handle.CoordinatorKey, operationId: TransactionOperationId.NewRandom());

        if (Assert.Single(locks).Type != KeyValueResponseType.Locked)
            return false;

        (KeyValueResponseType writeType, _, _) = await session.LocateAndTrySetKeyValue(
            handle.TransactionId, key, Encoding.UTF8.GetBytes(value), null, -1,
            KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct,
            coordinatorKey: handle.CoordinatorKey, operationId: TransactionOperationId.NewRandom());

        return writeType == KeyValueResponseType.Set;
    }

    /// <summary>Commits when every write was staged, rolls back otherwise. True when the transaction committed.</summary>
    private static async Task<bool> Finish(KahunaManager session, TransactionHandle handle, bool wrote, CancellationToken ct)
    {
        if (!wrote)
        {
            await session.LocateAndRollbackTransaction(handle, ct);
            return false;
        }

        (KeyValueResponseType commitType, _) = await session.LocateAndCommitTransaction(handle, ct);
        return commitType == KeyValueResponseType.Committed;
    }

    /// <summary>
    /// Moves the hybrid logical clock of every node forward, the way a timestamp from a peer whose wall clock
    /// was stepped does: the receiving clock adopts the largest physical time it has seen and never goes back.
    /// </summary>
    private static void LeapClocks(IRaft[] rafts, int forwardMs)
    {
        foreach (IRaft raft in rafts)
        {
            HLCTimestamp now = raft.HybridLogicalClock.SendOrLocalEvent(raft.GetLocalNodeId());
            raft.HybridLogicalClock.ReceiveEvent(raft.GetLocalNodeId(), new HLCTimestamp(now.N, now.L + forwardMs, 0));
        }
    }

    private static async Task SettleEverywhere(KahunaManager[] managers, string[] keys, CancellationToken ct)
    {
        await WaitUntilAsync(async () =>
        {
            foreach (KahunaManager manager in managers)
                await manager.KeyValues.RecoverPreparedIntents(ct);

            foreach (KahunaManager manager in managers)
                foreach (string key in keys)
                    if (manager.DurablePreparedIntentStore.Get(key) is not null)
                        return false;

            return true;
        }, timeoutMs: 60_000);
    }

    private static async Task<string?> ReadValue(KahunaManager reader, string key, CancellationToken ct)
    {
        (KeyValueResponseType readType, ReadOnlyKeyValueEntry? entry) = await RetryOnMustRetryAsync(
            () => reader.LocateAndTryGetValue(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct),
            r => r.Item1);

        return readType == KeyValueResponseType.Get ? Encoding.UTF8.GetString(entry!.Value!) : null;
    }

    /// <summary>
    /// The lease of a lock an actor holds does not follow the hybrid logical clock, and leaves the actor as the
    /// time it has left: a copy carries a deadline ahead of whatever the clock says at that moment, and the
    /// actor that takes the copy in measures the same time on its own monotonic clock again.
    /// </summary>
    [Fact]
    public void LeaseOfAHeldLock_IgnoresAClockLeapAndTravelsAsTheTimeLeft()
    {
        HLCTimestamp grantedAt = new(1, 1_000_000, 0);

        KeyValueRangeLock held = new()
        {
            TransactionId = new(1, 999_000, 0),
            StartKey = "a",
            StartInclusive = true,
            EndKey = "b",
            Mode = RangeLockMode.Shared
        };

        RangeLockChecks.StartLease(held, grantedAt, LockExpiresMs);

        HLCTimestamp leapt = grantedAt + ClockLeapMs;
        Assert.True(leapt - held.Expires > TimeSpan.Zero, "the leap must pass the deadline recorded at the grant");
        Assert.True(RangeLockChecks.IsLive(held, leapt, sessionOwnedCeilingMs: 0));

        KeyValueRangeLock carried = RangeLockChecks.DetachedCopy(held, leapt);
        Assert.Equal(0, carried.LeaseEndsAtTick);
        Assert.InRange((carried.Expires - leapt).TotalMilliseconds, LockExpiresMs - 10_000, LockExpiresMs);
        Assert.Equal(held.TransactionId, carried.TransactionId);
        Assert.Equal(held.Mode, carried.Mode);

        // Nobody holds the copy, so its deadline is all there is to judge it by.
        Assert.True(RangeLockChecks.IsLive(carried, leapt, sessionOwnedCeilingMs: 0));
        Assert.False(RangeLockChecks.IsLive(carried, leapt + LockExpiresMs, sessionOwnedCeilingMs: 0));

        RangeLockChecks.AdoptLease(carried, leapt);
        Assert.NotEqual(0, carried.LeaseEndsAtTick);
        Assert.True(RangeLockChecks.IsLive(carried, leapt + (2 * LockExpiresMs), sessionOwnedCeilingMs: 0));

        // A session-owned lock has no deadline in either form.
        RangeLockChecks.StartLease(held, leapt, expiresMs: 0);
        Assert.Equal(HLCTimestamp.Zero, held.Expires);
        Assert.Equal(0, held.LeaseEndsAtTick);
    }

    /// <summary>The registry answers for exactly the transactions recorded in it.</summary>
    [Fact]
    public void LapsedRangeLockRegistry_AnswersForRecordedHoldersOnly()
    {
        LapsedRangeLockRegistry registry = new();

        HLCTimestamp lapsed = new(1, 5_000, 0);
        HLCTimestamp other = new(1, 5_000, 1);

        registry.Record(HLCTimestamp.Zero, retentionMs: 60_000);
        Assert.Equal(0, registry.Count);

        registry.Record(lapsed, retentionMs: 60_000);
        registry.Record(lapsed, retentionMs: 60_000);

        Assert.Equal(1, registry.Count);
        Assert.True(registry.Contains(lapsed));
        Assert.False(registry.Contains(other));
    }

    /// <summary>
    /// Two transactions each read one key under a Shared lock and then write the key the other one read. Both
    /// read locks stop being honored between the reads and the writes, and the two transactions must still not
    /// both commit.
    /// </summary>
    /// <param name="clockLeap">The clocks leap past the lease while no time passes; otherwise the leases are
    /// short and really run out.</param>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task TransactionsWhoseReadLocksStoppedBeingHonored_NeverBothCommitAWriteSkew(bool clockLeap)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleCluster(Nodes, "memory", Partitions, raftLogger, kahunaLogger);

        KahunaManager[] managers = [.. kahunas.Cast<KahunaManager>()];

        try
        {
            KeyValuePair<int, string>[] keys = FreshKeyPerPartition(managers[0], "lls");
            Assert.True(keys.Length >= 3, "the fixture needs two data partitions and a coordinator partition");

            string firstKey = keys[0].Value;
            string secondKey = keys[1].Value;
            string coordinatorKey = keys[2].Value;

            await LeaderIndexOf(keys[0].Key, rafts, ct);
            await LeaderIndexOf(keys[1].Key, rafts, ct);

            KahunaManager session = managers[0];

            await Seed(session, firstKey, ct);
            await Seed(session, secondKey, ct);

            int readLeaseMs = clockLeap ? LockExpiresMs : (int)(300 * TimingScale);

            TransactionHandle first = await StartPessimistic(session, coordinatorKey, ct);
            TransactionHandle second = await StartPessimistic(session, coordinatorKey, ct);

            await ReadUnderSharedLock(session, first, firstKey, readLeaseMs, ct);
            await ReadUnderSharedLock(session, second, secondKey, readLeaseMs, ct);

            if (clockLeap)
                LeapClocks(rafts, ClockLeapMs);
            else
                await Task.Delay(readLeaseMs * 2, ct);

            bool firstWrote = await TryLockAndWrite(session, first, secondKey, "first", ct);
            bool secondWrote = await TryLockAndWrite(session, second, firstKey, "second", ct);

            bool firstCommitted = await Finish(session, first, firstWrote, ct);
            bool secondCommitted = await Finish(session, second, secondWrote, ct);

            Assert.False(firstCommitted && secondCommitted,
                "each transaction wrote the key the other one read under a lock: committing both is a write skew");

            await SettleEverywhere(managers, [firstKey, secondKey], ct);

            foreach (KahunaManager reader in managers)
            {
                Assert.Equal(secondCommitted ? "second" : "seed", await ReadValue(reader, firstKey, ct));
                Assert.Equal(firstCommitted ? "first" : "seed", await ReadValue(reader, secondKey, ct));
            }
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    /// <summary>
    /// A leap of the clock is not the passage of time. The lock a transaction holds across it still refuses a
    /// conflicting acquire, still leaves the node with a deadline ahead of the clock (which is what a transfer
    /// to another partition would carry), and does not cost its holder the commit.
    /// </summary>
    [Fact]
    public async Task LockHeldAcrossAClockLeap_StaysInForceAndItsHolderCommits()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleCluster(Nodes, "memory", Partitions, raftLogger, kahunaLogger);

        KahunaManager[] managers = [.. kahunas.Cast<KahunaManager>()];

        try
        {
            KeyValuePair<int, string>[] keys = FreshKeyPerPartition(managers[0], "llh");
            Assert.True(keys.Length >= 3, "the fixture needs a data partition, a coordinator partition and a third partition");

            (int dataPartition, string dataKey) = (keys[0].Key, keys[0].Value);
            string coordinatorKey = keys[1].Value;
            string otherKey = keys[2].Value;

            int leader = await LeaderIndexOf(dataPartition, rafts, ct);
            await LeaderIndexOf(keys[2].Key, rafts, ct);

            KahunaManager session = managers[leader];

            await Seed(session, dataKey, ct);

            TransactionHandle holder = await StartPessimistic(session, coordinatorKey, ct);
            await ReadUnderSharedLock(session, holder, dataKey, LockExpiresMs, ct);

            LeapClocks(rafts, ClockLeapMs);

            TransactionHandle contender = await StartPessimistic(session, coordinatorKey, ct);
            Assert.Equal(
                KeyValueResponseType.AlreadyLocked,
                await TryPointRangeLock(session, contender, dataKey, RangeLockMode.Exclusive, LockExpiresMs, ct));
            await session.LocateAndRollbackTransaction(contender, ct);

            HLCTimestamp now = rafts[leader].HybridLogicalClock.SendOrLocalEvent(rafts[leader].GetLocalNodeId());
            List<KeyValueRangeLock> snapshot = await managers[leader].KeyValues.GetRangeLocksAsync(BucketOf(dataKey));
            KeyValueRangeLock carried = Assert.Single(snapshot, l => l.TransactionId == holder.TransactionId);
            Assert.True(carried.Expires - now > TimeSpan.Zero, "the deadline a transfer would carry must still be ahead of the clock");

            long lostLockAborts = DurableTransactionMetrics.LostLockAbortsCount;

            Assert.True(await TryLockAndWrite(session, holder, otherKey, "holder", ct));
            Assert.True(await Finish(session, holder, wrote: true, ct), "no lock was lost, so nothing may refuse the commit");
            Assert.Equal(lostLockAborts, DurableTransactionMetrics.LostLockAbortsCount);

            await SettleEverywhere(managers, [otherKey], ct);

            foreach (KahunaManager reader in managers)
                Assert.Equal("holder", await ReadValue(reader, otherKey, ct));
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    /// <summary>
    /// The lease of a read lock runs out, the leader grants the key to a second transaction, and that one
    /// writes the key and commits. The first transaction then writes another key and commits: the leader never
    /// changed, so only the lapse itself can refuse it.
    /// </summary>
    /// <param name="renew">The coordinator's renewal runs after the second transaction committed. The leader
    /// answers it as a fresh grant, so the lock looks held again; it was not held in between.</param>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task HolderWhoseLeaseLapsed_NeverCommitsAfterTheKeyWasGrantedToAnother(bool renew)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleCluster(Nodes, "memory", Partitions, raftLogger, kahunaLogger);

        KahunaManager[] managers = [.. kahunas.Cast<KahunaManager>()];

        try
        {
            KeyValuePair<int, string>[] keys = FreshKeyPerPartition(managers[0], "lll");
            Assert.True(keys.Length >= 3, "the fixture needs a data partition, a coordinator partition and a third partition");

            (int dataPartition, string dataKey) = (keys[0].Key, keys[0].Value);
            string coordinatorKey = keys[1].Value;
            string otherKey = keys[2].Value;

            int leader = await LeaderIndexOf(dataPartition, rafts, ct);
            await LeaderIndexOf(keys[2].Key, rafts, ct);

            KahunaManager session = managers[leader];

            await Seed(session, dataKey, ct);

            int shortLeaseMs = (int)(300 * TimingScale);

            TransactionHandle holder = await StartPessimistic(session, coordinatorKey, ct);
            await ReadUnderSharedLock(session, holder, dataKey, shortLeaseMs, ct);

            await Task.Delay(shortLeaseMs * 2, ct);

            TransactionHandle winner = await StartPessimistic(session, coordinatorKey, ct);
            Assert.True(await TryLockAndWrite(session, winner, dataKey, "winner", ct), "the lease ran out, so the key is free");
            Assert.True(await Finish(session, winner, wrote: true, ct));

            await SettleEverywhere(managers, [dataKey], ct);

            if (renew)
            {
                // Only the node that coordinates the session has range locks to renew; the others have none.
                foreach (KahunaManager manager in managers)
                    await manager.TransactionCoordinator.RenewSessionRangeLocks();
            }

            long lostLockAborts = DurableTransactionMetrics.LostLockAbortsCount;

            bool holderWrote = await TryLockAndWrite(session, holder, otherKey, "holder", ct);
            Assert.False(await Finish(session, holder, holderWrote, ct), "the holder read the key under a lock that lapsed");

            if (holderWrote)
                Assert.True(DurableTransactionMetrics.LostLockAbortsCount > lostLockAborts,
                    "the commit must be refused because the lock was lost, and counted as such");

            await SettleEverywhere(managers, [dataKey, otherKey], ct);

            foreach (KahunaManager reader in managers)
            {
                Assert.Equal("winner", await ReadValue(reader, dataKey, ct));
                Assert.Null(await ReadValue(reader, otherKey, ct));
            }
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }
}
