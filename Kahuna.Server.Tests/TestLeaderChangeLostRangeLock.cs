using System.Text;
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
/// A pessimistic transaction protects a read with a Shared range lock over the single key it reads, and reads
/// the key without registering the read with its coordinator. The lock lives in the partition leader's memory,
/// so a leader change drops it without telling the transaction, and the new leader grants the key to a second
/// transaction, which writes it and commits.
///
/// <para>The first transaction must not commit on what it read under the lost lock. Two shapes are covered:</para>
/// <list type="bullet">
/// <item>It writes the key from the value it read: it upgrades its lock to Exclusive (which the new leader
/// answers as a fresh grant), takes the exclusive point lock (whose base is by then the second transaction's
/// commit), and stages a value computed from the base it read. Committing it would silently replace the
/// second transaction's write.</item>
/// <item>It writes nothing and reads the key again: the second read answers the second transaction's value,
/// so the transaction saw one key at two committed states.</item>
/// </list>
///
/// <para>The refusal comes from the leadership term each lock grant reports: the coordinator keeps one term per
/// partition and proves at commit that the partition is still led under it. The remaining tests pin the edges
/// of that proof: an exclusive point lock held only to protect a read, a coordinator renewal that silently
/// re-creates the lost lock, a one-phase bundle proposed after the leader changed, and the control in which no
/// leader changes and nothing may be refused.</para>
/// </summary>
public sealed class TestLeaderChangeLostRangeLock : BaseCluster
{
    private const int Nodes = 3;

    private const int Partitions = 4;

    private const int LockExpiresMs = 60_000;

    private readonly ILogger<IRaft> raftLogger;

    private readonly ILogger<IKahuna> kahunaLogger;

    public TestLeaderChangeLostRangeLock(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = TestLogFactory.Create(outputHelper);
        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    private static string EndpointOf(int index) => $"localhost:{8001 + index}";

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

    private static Dictionary<int, string> FreshKeyPerPartition(KahunaManager probe, string tag)
    {
        string random = Guid.NewGuid().ToString("N")[..8];
        Dictionary<int, string> byPartition = [];

        for (int i = 0; i < 4_096 && byPartition.Count < Partitions; i++)
        {
            string candidate = $"{tag}{i}/{random}";
            int partition = probe.KeyValues.LocateDurablePartition(candidate).PartitionId;
            byPartition.TryAdd(partition, candidate);
        }

        return byPartition;
    }

    private static async Task<TransactionHandle> StartPessimistic(KahunaManager session, string coordinatorKey, CancellationToken ct)
    {
        (KeyValueResponseType startType, TransactionHandle handle) = await session.LocateAndStartTransaction(
            new KeyValueTransactionOptions
            {
                CoordinatorKey = coordinatorKey,
                Locking = KeyValueTransactionLocking.Pessimistic,
                AsyncRelease = true,
                Timeout = 60_000
            }, ct);

        Assert.Equal(KeyValueResponseType.Set, startType);
        return handle;
    }

    /// <summary>Acquires (or upgrades to) a range lock over the single key, registered with the coordinator.</summary>
    private static Task<KeyValueResponseType> TryPointRangeLock(
        KahunaManager session, TransactionHandle handle, string key, RangeLockMode mode, CancellationToken ct) =>
        RetryOnMustRetryAsync(async () =>
        {
            (KeyValueResponseType lockType, _) = await session.LocateAndTryAcquireRangeLock(
                handle.TransactionId, BucketOf(key), key, true, key, true, LockExpiresMs, KeyValueDurability.Persistent, mode, ct,
                coordinatorKey: handle.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
            return lockType;
        }, t => t);

    private static async Task PointRangeLock(
        KahunaManager session, TransactionHandle handle, string key, RangeLockMode mode, CancellationToken ct) =>
        Assert.Equal(KeyValueResponseType.Locked, await TryPointRangeLock(session, handle, key, mode, ct));

    /// <summary>A transactional latest read that is not registered with the coordinator: it pins the key on the
    /// partition leader and leaves no observation in the transaction's read set.</summary>
    private static Task<(KeyValueResponseType Type, ReadOnlyKeyValueEntry? Entry)> UnregisteredRead(
        KahunaManager session, TransactionHandle handle, string key, CancellationToken ct) =>
        RetryOnMustRetryAsync(
            () => session.LocateAndTryGetValue(handle.TransactionId, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct),
            r => r.Item1);

    private static async Task<KeyValueResponseType> TryExclusivePointLock(
        KahunaManager session, TransactionHandle handle, string key, CancellationToken ct)
    {
        List<(KeyValueResponseType Type, string Key, KeyValueDurability Durability, HLCTimestamp Holder)> locks =
            await session.LocateAndTryAcquireManyExclusiveLocks(
                handle.TransactionId, [(key, LockExpiresMs, KeyValueDurability.Persistent)], ct,
                coordinatorKey: handle.CoordinatorKey, operationId: TransactionOperationId.NewRandom());

        return Assert.Single(locks).Type;
    }

    private static async Task<(KeyValueResponseType Type, long Revision)> TryWrite(
        KahunaManager session, TransactionHandle handle, string key, string value, CancellationToken ct)
    {
        (KeyValueResponseType writeType, long revision, _) = await session.LocateAndTrySetKeyValue(
            handle.TransactionId, key, Encoding.UTF8.GetBytes(value), null, -1,
            KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct,
            coordinatorKey: handle.CoordinatorKey, operationId: TransactionOperationId.NewRandom());

        return (writeType, revision);
    }

    private static async Task MoveLeadership(IRaft[] rafts, int partition, int from, int to, CancellationToken ct)
    {
        RaftOperationStatus status = await rafts[from].TransferLeadershipAsync(partition, EndpointOf(to), ct);
        Assert.Equal(RaftOperationStatus.Success, status);

        await WaitUntilAsync(async () => await rafts[to].AmILeaderIfHosted(partition, ct), timeoutMs: 30_000);
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

    private static async Task<(KeyValueResponseType Type, ReadOnlyKeyValueEntry? Entry)> Read(
        KahunaManager reader, string key, CancellationToken ct) =>
        await RetryOnMustRetryAsync(
            () => reader.LocateAndTryGetValue(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct),
            r => r.Item1);

    /// <summary>The second transaction, on the new leader: read the key under a Shared lock, upgrade, take the
    /// exclusive point lock, write and commit.</summary>
    private static async Task CommitWinner(
        KahunaManager session, string coordinatorKey, string key, long baseRevision, CancellationToken ct)
    {
        TransactionHandle winner = await StartPessimistic(session, coordinatorKey, ct);

        await PointRangeLock(session, winner, key, RangeLockMode.Shared, ct);

        (KeyValueResponseType readType, ReadOnlyKeyValueEntry? read) = await UnregisteredRead(session, winner, key, ct);
        Assert.Equal(KeyValueResponseType.Get, readType);
        Assert.Equal(baseRevision, read!.Revision);

        await PointRangeLock(session, winner, key, RangeLockMode.Exclusive, ct);
        Assert.Equal(KeyValueResponseType.Locked, await TryExclusivePointLock(session, winner, key, ct));

        (KeyValueResponseType writeType, long writeRevision) = await TryWrite(session, winner, key, "winner", ct);
        Assert.Equal(KeyValueResponseType.Set, writeType);
        Assert.Equal(baseRevision + 1, writeRevision);

        (KeyValueResponseType commitType, _) = await session.LocateAndCommitTransaction(winner, ct);
        Assert.Equal(KeyValueResponseType.Committed, commitType);
    }

    /// <param name="applyTimeValidation">The group's one-phase apply-time validation setting.</param>
    /// <param name="twoPhase">The stale writer also writes a key on a third partition, which sends its commit
    /// through the two-phase prepare.</param>
    /// <param name="settleWinner">The second transaction's commit is settled on every node before the stale
    /// writer continues; otherwise the stale writer meets it as a committed but unsettled intent.</param>
    [Theory]
    [InlineData(true, false, true)]
    [InlineData(true, false, false)]
    [InlineData(false, false, true)]
    [InlineData(true, true, true)]
    [InlineData(false, true, true)]
    [InlineData(false, true, false)]
    public async Task WriterWhoseSharedLockWasLost_NeverCommitsAValueComputedFromWhatItRead(
        bool applyTimeValidation, bool twoPhase, bool settleWinner)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleCluster(
            Nodes, "memory", Partitions, raftLogger, kahunaLogger,
            configureKahuna: config => config.OnePhaseApplyTimeValidation = applyTimeValidation);

        KahunaManager[] managers = [.. kahunas.Cast<KahunaManager>()];

        try
        {
            Dictionary<int, string> keys = FreshKeyPerPartition(managers[0], "lrl");
            Assert.True(keys.Count >= 3, "the fixture needs a data partition, a coordinator partition and a third partition");

            KeyValuePair<int, string>[] ordered = [.. keys.OrderBy(kv => kv.Key)];
            (int dataPartition, string dataKey) = (ordered[0].Key, ordered[0].Value);
            string coordinatorKey = ordered[1].Value;
            string otherKey = ordered[2].Value;

            int leader = await LeaderIndexOf(dataPartition, rafts, ct);
            int successor = (leader + 1) % Nodes;

            KahunaManager session = managers[leader];

            (KeyValueResponseType seedType, long seedRevision, _) = await session.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, dataKey, "seed"u8.ToArray(), null, -1, KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Set, seedType);

            // The stale writer reads the key under a Shared lock on the leader that is about to be deposed.
            TransactionHandle stale = await StartPessimistic(session, coordinatorKey, ct);
            await PointRangeLock(session, stale, dataKey, RangeLockMode.Shared, ct);

            (KeyValueResponseType staleReadType, ReadOnlyKeyValueEntry? staleRead) = await UnregisteredRead(session, stale, dataKey, ct);
            Assert.Equal(KeyValueResponseType.Get, staleReadType);
            Assert.Equal(seedRevision, staleRead!.Revision);

            if (twoPhase)
            {
                Assert.Equal(KeyValueResponseType.Locked, await TryExclusivePointLock(session, stale, otherKey, ct));
                Assert.Equal(KeyValueResponseType.Set, (await TryWrite(session, stale, otherKey, "stale-other", ct)).Type);
            }

            await MoveLeadership(rafts, dataPartition, leader, successor, ct);

            await CommitWinner(managers[successor], coordinatorKey, dataKey, seedRevision, ct);

            if (settleWinner)
                await SettleEverywhere(managers, [dataKey], ct);

            // The stale writer continues as if it still held its lock: upgrade, exclusive point lock, then the
            // write of the value it computed from the base it read. Any step may refuse; none may let it commit.
            bool refused = false;

            KeyValueResponseType upgrade = await TryPointRangeLock(session, stale, dataKey, RangeLockMode.Exclusive, ct);
            refused |= upgrade != KeyValueResponseType.Locked;

            if (!refused)
                refused |= await TryExclusivePointLock(session, stale, dataKey, ct) != KeyValueResponseType.Locked;

            if (!refused)
                refused |= (await TryWrite(session, stale, dataKey, "stale", ct)).Type != KeyValueResponseType.Set;

            if (refused)
            {
                KeyValueResponseType staleRollback = await session.LocateAndRollbackTransaction(stale, ct);
                Assert.NotEqual(KeyValueResponseType.Committed, staleRollback);
            }
            else
            {
                long lostLockAborts = DurableTransactionMetrics.LostLockAbortsCount;

                (KeyValueResponseType staleCommit, _) = await session.LocateAndCommitTransaction(stale, ct);
                Assert.NotEqual(KeyValueResponseType.Committed, staleCommit);
                Assert.True(DurableTransactionMetrics.LostLockAbortsCount > lostLockAborts,
                    "the commit must be refused because the lock was lost, and counted as such");
            }

            await SettleEverywhere(managers, twoPhase ? [dataKey, otherKey] : [dataKey], ct);

            foreach (KahunaManager reader in managers)
            {
                (KeyValueResponseType readType, ReadOnlyKeyValueEntry? entry) = await Read(reader, dataKey, ct);
                Assert.Equal(KeyValueResponseType.Get, readType);
                Assert.Equal("winner", Encoding.UTF8.GetString(entry!.Value!));
                Assert.Equal(seedRevision + 1, entry.Revision);

                if (twoPhase)
                {
                    (KeyValueResponseType otherType, _) = await Read(reader, otherKey, ct);
                    Assert.Equal(KeyValueResponseType.DoesNotExist, otherType);
                }
            }
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    /// <param name="rereadKey">The reader reads the key a second time after the second transaction committed;
    /// otherwise it only commits, having read the key once under the lock it lost.</param>
    /// <param name="exclusivePointLock">The reader protects its read with an exclusive point lock instead of a
    /// Shared range lock. It never writes the key, so no write-side base check ever looks at it.</param>
    [Theory]
    [InlineData(true, false)]
    [InlineData(false, false)]
    [InlineData(true, true)]
    [InlineData(false, true)]
    public async Task ReaderWhoseLockWasLost_NeverCommitsAfterTheKeyMoved(bool rereadKey, bool exclusivePointLock)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleCluster(Nodes, "memory", Partitions, raftLogger, kahunaLogger);

        KahunaManager[] managers = [.. kahunas.Cast<KahunaManager>()];

        try
        {
            Dictionary<int, string> keys = FreshKeyPerPartition(managers[0], "lrr");
            Assert.True(keys.Count >= 2, "the fixture needs a data partition and a coordinator partition");

            KeyValuePair<int, string>[] ordered = [.. keys.OrderBy(kv => kv.Key)];
            (int dataPartition, string dataKey) = (ordered[0].Key, ordered[0].Value);
            string coordinatorKey = ordered[1].Value;

            int leader = await LeaderIndexOf(dataPartition, rafts, ct);
            int successor = (leader + 1) % Nodes;

            KahunaManager session = managers[leader];

            (KeyValueResponseType seedType, long seedRevision, _) = await session.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, dataKey, "seed"u8.ToArray(), null, -1, KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Set, seedType);

            TransactionHandle reader = await StartPessimistic(session, coordinatorKey, ct);
            if (exclusivePointLock)
                Assert.Equal(KeyValueResponseType.Locked, await TryExclusivePointLock(session, reader, dataKey, ct));
            else
                await PointRangeLock(session, reader, dataKey, RangeLockMode.Shared, ct);

            (KeyValueResponseType firstType, ReadOnlyKeyValueEntry? first) = await UnregisteredRead(session, reader, dataKey, ct);
            Assert.Equal(KeyValueResponseType.Get, firstType);
            Assert.Equal("seed", Encoding.UTF8.GetString(first!.Value!));

            await MoveLeadership(rafts, dataPartition, leader, successor, ct);

            await CommitWinner(managers[successor], coordinatorKey, dataKey, seedRevision, ct);
            await SettleEverywhere(managers, [dataKey], ct);

            bool sawBothStates = false;

            if (rereadKey)
            {
                // The reader believes its lock still covers the key, so it reads again without a lock request.
                (KeyValueResponseType secondType, ReadOnlyKeyValueEntry? second) = await UnregisteredRead(session, reader, dataKey, ct);
                sawBothStates = secondType == KeyValueResponseType.Get && Encoding.UTF8.GetString(second!.Value!) == "winner";
            }

            long lostLockAborts = DurableTransactionMetrics.LostLockAbortsCount;

            (KeyValueResponseType commitType, _) = await session.LocateAndCommitTransaction(reader, ct);

            Assert.True(commitType != KeyValueResponseType.Committed,
                sawBothStates
                    ? "the reader committed after it read one key at two committed states"
                    : "the reader committed although the lock that protected its read was lost and the key moved");
            Assert.True(DurableTransactionMetrics.LostLockAbortsCount > lostLockAborts,
                "the commit must be refused because the lock was lost, and counted as such");
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    /// <summary>
    /// The coordinator renews a session's range locks on a timer. After a leader change the renewal reaches a
    /// leader that never held the lock and is answered as a fresh grant, so the lock looks held again. The
    /// commit must still be refused: the lock was not held in between, and the second transaction committed in
    /// that gap.
    /// </summary>
    [Fact]
    public async Task RenewalThatRecreatesALostLock_DoesNotMakeTheCommitSafe()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleCluster(Nodes, "memory", Partitions, raftLogger, kahunaLogger);

        KahunaManager[] managers = [.. kahunas.Cast<KahunaManager>()];

        try
        {
            Dictionary<int, string> keys = FreshKeyPerPartition(managers[0], "lrn");
            Assert.True(keys.Count >= 2, "the fixture needs a data partition and a coordinator partition");

            KeyValuePair<int, string>[] ordered = [.. keys.OrderBy(kv => kv.Key)];
            (int dataPartition, string dataKey) = (ordered[0].Key, ordered[0].Value);
            string coordinatorKey = ordered[1].Value;

            int leader = await LeaderIndexOf(dataPartition, rafts, ct);
            int successor = (leader + 1) % Nodes;

            KahunaManager session = managers[leader];

            (KeyValueResponseType seedType, long seedRevision, _) = await session.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, dataKey, "seed"u8.ToArray(), null, -1, KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Set, seedType);

            TransactionHandle reader = await StartPessimistic(session, coordinatorKey, ct);
            await PointRangeLock(session, reader, dataKey, RangeLockMode.Shared, ct);

            (KeyValueResponseType firstType, _) = await UnregisteredRead(session, reader, dataKey, ct);
            Assert.Equal(KeyValueResponseType.Get, firstType);

            await MoveLeadership(rafts, dataPartition, leader, successor, ct);

            await CommitWinner(managers[successor], coordinatorKey, dataKey, seedRevision, ct);
            await SettleEverywhere(managers, [dataKey], ct);

            // Only the node that coordinates the session has range locks to renew; the others have none.
            foreach (KahunaManager manager in managers)
                await manager.TransactionCoordinator.RenewSessionRangeLocks();

            (KeyValueResponseType commitType, _) = await session.LocateAndCommitTransaction(reader, ct);
            Assert.NotEqual(KeyValueResponseType.Committed, commitType);
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    /// <summary>
    /// The one-phase bundle validates the transaction before it proposes, and decides in the same batch as its
    /// prepare. When the partition changes leader between that validation and the propose, the bundle is
    /// proposed by a leader that never held the transaction's locks. Every replica must refuse it at apply,
    /// from the term of the log entry alone.
    /// </summary>
    [Fact]
    public async Task OnePhaseBundleProposedAfterTheLeaderChanged_IsRefusedAtApply()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleCluster(
            Nodes, "memory", Partitions, raftLogger, kahunaLogger,
            configureKahuna: config => config.OnePhaseApplyTimeValidation = true);

        KahunaManager[] managers = [.. kahunas.Cast<KahunaManager>()];
        DurableTransactionFinalizer[] finalizers = [.. managers.Select(static m => m.TransactionCoordinator.DurableFinalizerForTests)];

        try
        {
            Dictionary<int, string> keys = FreshKeyPerPartition(managers[0], "lro");
            Assert.True(keys.Count >= 2, "the fixture needs a data partition and a coordinator partition");

            KeyValuePair<int, string>[] ordered = [.. keys.OrderBy(kv => kv.Key)];
            (int dataPartition, string dataKey) = (ordered[0].Key, ordered[0].Value);
            string coordinatorKey = ordered[1].Value;

            int leader = await LeaderIndexOf(dataPartition, rafts, ct);
            int successor = (leader + 1) % Nodes;

            KahunaManager session = managers[leader];

            (KeyValueResponseType seedType, long seedRevision, _) = await session.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, dataKey, "seed"u8.ToArray(), null, -1, KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Set, seedType);

            TransactionHandle writer = await StartPessimistic(session, coordinatorKey, ct);
            Assert.Equal(KeyValueResponseType.Locked, await TryExclusivePointLock(session, writer, dataKey, ct));
            Assert.Equal(KeyValueResponseType.Set, (await TryWrite(session, writer, dataKey, "written", ct)).Type);

            // One-shot: whichever node coordinates the commit moves the leadership inside its window between
            // the validation and the propose, then clears the hook everywhere so nothing replays it.
            int hookRuns = 0;
            foreach (DurableTransactionFinalizer finalizer in finalizers)
            {
                finalizer.TestAfterReadSetValidationHook = async hookCt =>
                {
                    foreach (DurableTransactionFinalizer other in finalizers)
                        other.TestAfterReadSetValidationHook = null;

                    Interlocked.Increment(ref hookRuns);
                    await MoveLeadership(rafts, dataPartition, leader, successor, hookCt);
                };
            }

            long lostLockAborts = DurableTransactionMetrics.LostLockAbortsCount;

            KeyValueResponseType commitType;
            try
            {
                (commitType, _) = await session.LocateAndCommitTransaction(writer, ct);
            }
            finally
            {
                foreach (DurableTransactionFinalizer finalizer in finalizers)
                    finalizer.TestAfterReadSetValidationHook = null;
            }

            Assert.Equal(1, hookRuns);
            Assert.NotEqual(KeyValueResponseType.Committed, commitType);
            Assert.True(DurableTransactionMetrics.LostLockAbortsCount > lostLockAborts,
                "the bundle must be refused because a leader of another term proposed it, and counted as such");

            await SettleEverywhere(managers, [dataKey], ct);

            foreach (KahunaManager reader in managers)
            {
                (KeyValueResponseType readType, ReadOnlyKeyValueEntry? entry) = await Read(reader, dataKey, ct);
                Assert.Equal(KeyValueResponseType.Get, readType);
                Assert.Equal("seed", Encoding.UTF8.GetString(entry!.Value!));
                Assert.Equal(seedRevision, entry.Revision);
            }
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    /// <summary>
    /// A reader that takes no lock at all: it reads at a fixed timestamp, as a read-only snapshot transaction
    /// does. Its protection is the timestamp, not a lock, so a leader change has nothing of its to drop: the
    /// second read, served by the new leader after another transaction committed the key, must answer the
    /// revision the first read answered.
    /// </summary>
    /// <param name="settleWinner">The second transaction's commit is settled on every node before the second
    /// read; otherwise the read meets it as a committed but unsettled intent.</param>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task SnapshotReaderAcrossALeaderChange_ReadsTheSameRevisionTwice(bool settleWinner)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleCluster(Nodes, "memory", Partitions, raftLogger, kahunaLogger);

        KahunaManager[] managers = [.. kahunas.Cast<KahunaManager>()];

        try
        {
            Dictionary<int, string> keys = FreshKeyPerPartition(managers[0], "lrs");
            Assert.True(keys.Count >= 2, "the fixture needs a data partition and a coordinator partition");

            KeyValuePair<int, string>[] ordered = [.. keys.OrderBy(kv => kv.Key)];
            (int dataPartition, string dataKey) = (ordered[0].Key, ordered[0].Value);
            string coordinatorKey = ordered[1].Value;

            int leader = await LeaderIndexOf(dataPartition, rafts, ct);
            int successor = (leader + 1) % Nodes;

            KahunaManager session = managers[leader];

            (KeyValueResponseType seedType, long seedRevision, _) = await session.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, dataKey, "seed"u8.ToArray(), null, -1, KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Set, seedType);

            TransactionHandle reader = await StartPessimistic(session, coordinatorKey, ct);

            Task<(KeyValueResponseType Type, ReadOnlyKeyValueEntry? Entry)> SnapshotRead() =>
                RetryOnMustRetryAsync(
                    () => session.LocateAndTryGetValue(reader.TransactionId, dataKey, -1, reader.TransactionId, KeyValueDurability.Persistent, ct),
                    r => r.Item1);

            (KeyValueResponseType firstType, ReadOnlyKeyValueEntry? first) = await SnapshotRead();
            Assert.Equal(KeyValueResponseType.Get, firstType);
            Assert.Equal(seedRevision, first!.Revision);

            await MoveLeadership(rafts, dataPartition, leader, successor, ct);

            await CommitWinner(managers[successor], coordinatorKey, dataKey, seedRevision, ct);

            if (settleWinner)
                await SettleEverywhere(managers, [dataKey], ct);

            (KeyValueResponseType secondType, ReadOnlyKeyValueEntry? second) = await SnapshotRead();
            Assert.Equal(KeyValueResponseType.Get, secondType);
            Assert.Equal("seed", Encoding.UTF8.GetString(second!.Value!));
            Assert.Equal(seedRevision, second.Revision);

            Assert.NotEqual(KeyValueResponseType.Committed, await session.LocateAndRollbackTransaction(reader, ct));
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    /// <summary>
    /// The control: no partition changes leader, so every lock is still held at commit and nothing may be
    /// refused. A reader commits on its Shared lock, and a writer that reads under a Shared lock, upgrades and
    /// writes commits its value, through the one-phase bundle and through the two-phase prepare.
    /// </summary>
    /// <param name="applyTimeValidation">The group's one-phase apply-time validation setting.</param>
    /// <param name="twoPhase">The writer also writes a key on a third partition.</param>
    [Theory]
    [InlineData(true, false)]
    [InlineData(false, false)]
    [InlineData(true, true)]
    [InlineData(false, true)]
    public async Task LocksHeldUnderOneLeadership_NeverRefuseTheCommit(bool applyTimeValidation, bool twoPhase)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleCluster(
            Nodes, "memory", Partitions, raftLogger, kahunaLogger,
            configureKahuna: config => config.OnePhaseApplyTimeValidation = applyTimeValidation);

        KahunaManager[] managers = [.. kahunas.Cast<KahunaManager>()];

        try
        {
            Dictionary<int, string> keys = FreshKeyPerPartition(managers[0], "lrc");
            Assert.True(keys.Count >= 3, "the fixture needs a data partition, a coordinator partition and a third partition");

            KeyValuePair<int, string>[] ordered = [.. keys.OrderBy(kv => kv.Key)];
            (int dataPartition, string dataKey) = (ordered[0].Key, ordered[0].Value);
            string coordinatorKey = ordered[1].Value;
            string otherKey = ordered[2].Value;

            int leader = await LeaderIndexOf(dataPartition, rafts, ct);

            // The session runs on a node that does not lead the data partition, so every lock request and the
            // commit-time proof cross the node boundary.
            KahunaManager session = managers[(leader + 1) % Nodes];

            (KeyValueResponseType seedType, long seedRevision, _) = await session.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, dataKey, "seed"u8.ToArray(), null, -1, KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Set, seedType);

            TransactionHandle reader = await StartPessimistic(session, coordinatorKey, ct);
            await PointRangeLock(session, reader, dataKey, RangeLockMode.Shared, ct);
            Assert.Equal(KeyValueResponseType.Get, (await UnregisteredRead(session, reader, dataKey, ct)).Type);

            foreach (KahunaManager manager in managers)
                await manager.TransactionCoordinator.RenewSessionRangeLocks();

            Assert.Equal(KeyValueResponseType.Get, (await UnregisteredRead(session, reader, dataKey, ct)).Type);

            (KeyValueResponseType readerCommit, _) = await session.LocateAndCommitTransaction(reader, ct);
            Assert.Equal(KeyValueResponseType.Committed, readerCommit);

            TransactionHandle writer = await StartPessimistic(session, coordinatorKey, ct);
            await PointRangeLock(session, writer, dataKey, RangeLockMode.Shared, ct);
            Assert.Equal(KeyValueResponseType.Get, (await UnregisteredRead(session, writer, dataKey, ct)).Type);
            await PointRangeLock(session, writer, dataKey, RangeLockMode.Exclusive, ct);
            Assert.Equal(KeyValueResponseType.Locked, await TryExclusivePointLock(session, writer, dataKey, ct));
            Assert.Equal(KeyValueResponseType.Set, (await TryWrite(session, writer, dataKey, "written", ct)).Type);

            if (twoPhase)
            {
                Assert.Equal(KeyValueResponseType.Locked, await TryExclusivePointLock(session, writer, otherKey, ct));
                Assert.Equal(KeyValueResponseType.Set, (await TryWrite(session, writer, otherKey, "written-other", ct)).Type);
            }

            (KeyValueResponseType writerCommit, _) = await session.LocateAndCommitTransaction(writer, ct);
            Assert.Equal(KeyValueResponseType.Committed, writerCommit);

            await SettleEverywhere(managers, twoPhase ? [dataKey, otherKey] : [dataKey], ct);

            foreach (KahunaManager node in managers)
            {
                (KeyValueResponseType readType, ReadOnlyKeyValueEntry? entry) = await Read(node, dataKey, ct);
                Assert.Equal(KeyValueResponseType.Get, readType);
                Assert.Equal("written", Encoding.UTF8.GetString(entry!.Value!));
                Assert.Equal(seedRevision + 1, entry.Revision);
            }
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }
}
