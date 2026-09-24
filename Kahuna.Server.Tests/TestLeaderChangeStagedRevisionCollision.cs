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
/// A pessimistic writer stages its write on a partition leader, then the partition changes leader before the
/// writer commits. The exclusive lock and the write intent that excluded every other writer were in-memory state
/// on the old leader, so the new leader grants the same key to a second writer, which stages the same next
/// revision and commits first. When the first writer then commits, its mutation carries a revision the key's
/// committed history already holds: letting it commit makes two committed writes share one revision, and the
/// later one silently replaces the earlier, acknowledged one.
///
/// <para>The first writer must lose — any outcome but Committed — and the second writer's value must be what
/// every replica holds, at the revision it committed. The scenario runs through the one-phase bundle (with and
/// without apply-time validation) and through the two-phase prepare, for a blind write (no read of the key
/// before the write, so no validated base) and for a read-then-write.</para>
/// </summary>
public sealed class TestLeaderChangeStagedRevisionCollision : BaseCluster
{
    private const int Nodes = 3;

    private const int Partitions = 4;

    private readonly ILogger<IRaft> raftLogger;

    private readonly ILogger<IKahuna> kahunaLogger;

    public TestLeaderChangeStagedRevisionCollision(ITestOutputHelper outputHelper)
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

    /// <summary>A fresh key per partition. Hash routing hashes the key space (the prefix before the last '/'),
    /// so the candidates vary that prefix rather than the leaf.</summary>
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

    private static async Task<long> Write(KahunaManager session, TransactionHandle handle, string key, string value, CancellationToken ct)
    {
        (KeyValueResponseType writeType, long revision, _) = await session.LocateAndTrySetKeyValue(
            handle.TransactionId, key, Encoding.UTF8.GetBytes(value), null, -1,
            KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct,
            coordinatorKey: handle.CoordinatorKey, operationId: TransactionOperationId.NewRandom());

        Assert.Equal(KeyValueResponseType.Set, writeType);
        return revision;
    }

    private static async Task MoveLeadership(IRaft[] rafts, int partition, int from, int to, CancellationToken ct)
    {
        RaftOperationStatus status = await rafts[from].TransferLeadershipAsync(partition, EndpointOf(to), ct);
        Assert.Equal(RaftOperationStatus.Success, status);

        await WaitUntilAsync(async () => await rafts[to].AmILeaderIfHosted(partition, ct), timeoutMs: 30_000);
    }

    /// <summary>Drives every node's recovery sweep until no node still holds a prepared intent for the keys.</summary>
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
    /// <param name="readBeforeWrite">The stale writer reads the key inside the transaction before it writes it,
    /// so its write carries a validated base; otherwise the write is blind.</param>
    [Theory]
    [InlineData(true, false, false)]
    [InlineData(false, false, false)]
    [InlineData(true, true, false)]
    [InlineData(false, true, false)]
    [InlineData(true, false, true)]
    [InlineData(false, true, true)]
    public async Task WriterStagedOnADeposedLeader_NeverCommitsOverTheNewLeadersCommitAtTheSameRevision(
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

            // The data key's partition is the one that changes leader. The coordinator key lives on a partition
            // that keeps its leader, so both transactions' sessions survive the move — the shape of a coordinator
            // whose table prefix routes elsewhere than the rows it writes.
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

            // The stale writer: locked and staged on the leader that is about to be deposed.
            TransactionHandle stale = await StartPessimistic(session, coordinatorKey, ct);
            await Lock(session, stale, dataKey, ct);
            if (readBeforeWrite)
                Assert.Equal(seedRevision, await TransactionalRead(session, stale, dataKey, ct));

            long staleRevision = await Write(session, stale, dataKey, "stale", ct);
            Assert.Equal(seedRevision + 1, staleRevision);

            if (twoPhase)
            {
                await Lock(session, stale, otherKey, ct);
                await Write(session, stale, otherKey, "stale-other", ct);
            }

            await MoveLeadership(rafts, dataPartition, leader, successor, ct);

            // The winner: the new leader has no memory of the stale writer's lock or write intent, so it grants
            // the key, stages the same next revision, and commits.
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

            // The stale writer commits last, over a key whose committed history already holds its revision.
            (KeyValueResponseType staleCommit, _) = await session.LocateAndCommitTransaction(stale, ct);
            Assert.NotEqual(KeyValueResponseType.Committed, staleCommit);

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
