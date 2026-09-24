using System.Text;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Data;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// A pessimistic writer takes the exclusive lock on a key and observes its base on a partition leader, then the
/// partition changes leader before the writer stages its value. The lock lived in the deposed leader's memory, so
/// the new leader grants the same key to a second writer, which commits the next revision. The first writer then
/// stages its value on the new leader: the actor allocates the revision from the head it now holds, so the staged
/// revision is one above the second writer's commit and the same-revision rule cannot see it. The value was still
/// computed from the base the writer observed under its lost exclusion.
///
/// <para>The first writer must lose — its write or its commit must be any outcome but success — and the second
/// writer's value must be what every replica holds. The scenario runs through the one-phase bundle (with and
/// without apply-time validation) and through the two-phase prepare, for a blind write (no read of the key inside
/// the transaction) and for a read-then-write.</para>
/// </summary>
public sealed class TestLeaderChangeStaleBaseAboveHead : BaseCluster
{
    private const int Nodes = 3;

    private const int Partitions = 4;

    private readonly ILogger<IRaft> raftLogger;

    private readonly ILogger<IKahuna> kahunaLogger;

    public TestLeaderChangeStaleBaseAboveHead(ITestOutputHelper outputHelper)
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

    private static async Task Lock(KahunaManager session, TransactionHandle handle, string key, CancellationToken ct)
    {
        (KeyValueResponseType lockType, _, _, _) = await session.LocateAndTryAcquireExclusiveLock(
            handle.TransactionId, key, 60_000, KeyValueDurability.Persistent, ct,
            coordinatorKey: handle.CoordinatorKey, operationId: TransactionOperationId.NewRandom());

        Assert.Equal(KeyValueResponseType.Locked, lockType);
    }

    private static async Task<long> TransactionalRead(KahunaManager session, TransactionHandle handle, string key, CancellationToken ct)
    {
        (KeyValueResponseType readType, ReadOnlyKeyValueEntry? entry) = await session.LocateAndTryGetValue(
            handle.TransactionId, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct,
            coordinatorKey: handle.CoordinatorKey, operationId: TransactionOperationId.NewRandom());

        Assert.Equal(KeyValueResponseType.Get, readType);
        return entry!.Revision;
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

    private static async Task<long> Write(KahunaManager session, TransactionHandle handle, string key, string value, CancellationToken ct)
    {
        (KeyValueResponseType writeType, long revision) = await TryWrite(session, handle, key, value, ct);
        Assert.Equal(KeyValueResponseType.Set, writeType);
        return revision;
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

    /// <param name="applyTimeValidation">The group's one-phase apply-time validation setting.</param>
    /// <param name="twoPhase">The stale writer also writes a key on a third partition, which closes the one-phase
    /// gate and sends it through the two-phase prepare.</param>
    /// <param name="readBeforeWrite">The stale writer reads the key inside the transaction under its lock before
    /// the leader change, so its write carries a validated base; otherwise the write is blind.</param>
    [Theory]
    [InlineData(true, false, false)]
    [InlineData(false, false, false)]
    [InlineData(true, true, false)]
    [InlineData(false, true, false)]
    [InlineData(true, false, true)]
    [InlineData(false, true, true)]
    public async Task WriterWhoseExclusionWasLost_NeverCommitsAValueComputedFromAStaleBase(
        bool applyTimeValidation, bool twoPhase, bool readBeforeWrite)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleCluster(
            Nodes, "memory", Partitions, raftLogger, kahunaLogger,
            configureKahuna: config => config.OnePhaseApplyTimeValidation = applyTimeValidation);

        KahunaManager[] managers = [.. kahunas.Cast<KahunaManager>()];

        try
        {
            Dictionary<int, string> keys = FreshKeyPerPartition(managers[0], "src");
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

            // The stale writer: locked (and optionally read) on the leader that is about to be deposed. It has
            // not staged its value yet.
            TransactionHandle stale = await StartPessimistic(session, coordinatorKey, ct);
            await Lock(session, stale, dataKey, ct);
            if (readBeforeWrite)
                Assert.Equal(seedRevision, await TransactionalRead(session, stale, dataKey, ct));

            if (twoPhase)
            {
                await Lock(session, stale, otherKey, ct);
                await Write(session, stale, otherKey, "stale-other", ct);
            }

            await MoveLeadership(rafts, dataPartition, leader, successor, ct);

            // The winner: the new leader has no memory of the stale writer's lock, so it grants the key, stages
            // the next revision, and commits.
            KahunaManager winnerSession = managers[successor];
            TransactionHandle winner = await StartPessimistic(winnerSession, coordinatorKey, ct);
            KeyValueResponseType winnerLock = await RetryOnMustRetryAsync(async () =>
            {
                (KeyValueResponseType lockType, _, _, _) = await winnerSession.LocateAndTryAcquireExclusiveLock(
                    winner.TransactionId, dataKey, 60_000, KeyValueDurability.Persistent, ct,
                    coordinatorKey: winner.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
                return lockType;
            }, t => t);
            Assert.Equal(KeyValueResponseType.Locked, winnerLock);

            Assert.Equal(seedRevision, await TransactionalRead(winnerSession, winner, dataKey, ct));
            Assert.Equal(seedRevision + 1, await Write(winnerSession, winner, dataKey, "winner", ct));

            (KeyValueResponseType winnerCommit, _) = await winnerSession.LocateAndCommitTransaction(winner, ct);
            Assert.Equal(KeyValueResponseType.Committed, winnerCommit);

            await SettleEverywhere(managers, [dataKey], ct);

            // The stale writer now stages the value it computed from the base it observed under its lost lock.
            // The new leader's actor allocates the revision from the head it holds, so the staged revision sits
            // above the winner's commit. Either the write or the commit must refuse it.
            (KeyValueResponseType staleWrite, long staleRevision) = await TryWrite(session, stale, dataKey, "stale", ct);

            if (staleWrite == KeyValueResponseType.Set)
            {
                Assert.Equal(seedRevision + 2, staleRevision);

                (KeyValueResponseType staleCommit, _) = await session.LocateAndCommitTransaction(stale, ct);
                Assert.NotEqual(KeyValueResponseType.Committed, staleCommit);
            }
            else
            {
                Assert.True(staleWrite is KeyValueResponseType.MustRetry or KeyValueResponseType.Aborted,
                    $"the stale write must be refused with a retryable or conflict outcome, got {staleWrite}");

                KeyValueResponseType staleRollback = await session.LocateAndRollbackTransaction(stale, ct);
                Assert.NotEqual(KeyValueResponseType.Committed, staleRollback);
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
}
