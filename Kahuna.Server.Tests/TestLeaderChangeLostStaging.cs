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
/// A pessimistic transaction locks a key, reads it and stages a write on the partition leader, and then the
/// partition changes leader before the transaction ends. The staging lived in the deposed leader's memory only,
/// so the new leader has no trace of it: a read of the key answers the committed value, and a second write of
/// the key stages from the committed head at the same revision the first staging had. No competitor touched
/// the key, so the committed head still equals the base the lock observed, and the base rule cannot see the
/// loss. The transaction's second value was computed without its own first write; committing it would erase
/// that write while the client believes both landed.
///
/// <para>The commit must be refused, and every replica must keep the seed. The scenario runs through the
/// one-phase bundle and through the two-phase prepare (a write on a third partition closes the bundle), and
/// with the loss surfaced by a read of the key, by a second write of it, or by both.</para>
/// </summary>
public sealed class TestLeaderChangeLostStaging : BaseCluster
{
    private const int Nodes = 3;
    private const int Partitions = 4;

    private readonly ILogger<IRaft> raftLogger;
    private readonly ILogger<IKahuna> kahunaLogger;

    public TestLeaderChangeLostStaging(ITestOutputHelper outputHelper)
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

    private static async Task<(KeyValueResponseType Type, ReadOnlyKeyValueEntry? Entry)> TransactionalRead(
        KahunaManager session, TransactionHandle handle, string key, CancellationToken ct) =>
        await RetryOnMustRetryAsync(
            () => session.LocateAndTryGetValue(
                handle.TransactionId, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct,
                coordinatorKey: handle.CoordinatorKey, operationId: TransactionOperationId.NewRandom()),
            r => r.Item1);

    private static async Task<(KeyValueResponseType Type, long Revision)> Write(
        KahunaManager session, TransactionHandle handle, string key, string value, CancellationToken ct)
    {
        (KeyValueResponseType writeType, long revision, _) = await RetryOnMustRetryAsync(
            () => session.LocateAndTrySetKeyValue(
                handle.TransactionId, key, Encoding.UTF8.GetBytes(value), null, -1,
                KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct,
                coordinatorKey: handle.CoordinatorKey, operationId: TransactionOperationId.NewRandom()),
            r => r.Item1);

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

    /// <param name="twoPhase">The transaction also writes a key on a third partition, which closes the one-phase
    /// gate and sends it through the two-phase prepare.</param>
    /// <param name="readAfterMove">After the leader change the transaction reads the key back before it writes
    /// it again; the read alone must surface the loss.</param>
    /// <param name="writeAfterMove">After the leader change the transaction writes the key a second time; the
    /// restaging alone must surface the loss.</param>
    [Theory]
    [InlineData(false, false, true)]
    [InlineData(false, true, false)]
    [InlineData(false, true, true)]
    [InlineData(true, false, true)]
    [InlineData(true, true, false)]
    public async Task TransactionWhoseStagingWasDroppedByALeaderChange_NeverCommits(bool twoPhase, bool readAfterMove, bool writeAfterMove)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleCluster(Nodes, "memory", Partitions, raftLogger, kahunaLogger);
        KahunaManager[] managers = [.. kahunas.Cast<KahunaManager>()];

        try
        {
            Dictionary<int, string> keys = FreshKeyPerPartition(managers[0], "lost");
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

            // Lock, read and stage on the leader that is about to be deposed. The staging is confirmed to the
            // client and folded into the coordinator.
            TransactionHandle tx = await StartPessimistic(session, coordinatorKey, ct);
            await Lock(session, tx, dataKey, ct);

            (KeyValueResponseType baseType, ReadOnlyKeyValueEntry? baseEntry) = await TransactionalRead(session, tx, dataKey, ct);
            Assert.Equal(KeyValueResponseType.Get, baseType);
            Assert.Equal(seedRevision, baseEntry!.Revision);

            (KeyValueResponseType firstType, long firstRevision) = await Write(session, tx, dataKey, "first", ct);
            Assert.Equal(KeyValueResponseType.Set, firstType);
            Assert.Equal(seedRevision + 1, firstRevision);

            if (twoPhase)
            {
                await Lock(session, tx, otherKey, ct);
                (KeyValueResponseType otherType, _) = await Write(session, tx, otherKey, "other", ct);
                Assert.Equal(KeyValueResponseType.Set, otherType);
            }

            await MoveLeadership(rafts, dataPartition, leader, successor, ct);

            // The new leader has no memory of the transaction on this key. Its read answers the committed seed,
            // and its write pins the committed head, so the restaging lands on the revision the first staging had.
            if (readAfterMove)
            {
                (KeyValueResponseType readType, ReadOnlyKeyValueEntry? readEntry) = await TransactionalRead(session, tx, dataKey, ct);
                Assert.Equal(KeyValueResponseType.Get, readType);
                Assert.Equal("seed", Encoding.UTF8.GetString(readEntry!.Value!));
                Assert.Equal(seedRevision, readEntry.Revision);
            }

            if (writeAfterMove)
            {
                (KeyValueResponseType secondType, long secondRevision) = await Write(session, tx, dataKey, "second", ct);
                Assert.Equal(KeyValueResponseType.Set, secondType);
                Assert.Equal(firstRevision, secondRevision);
            }

            (KeyValueResponseType commit, _) = await session.LocateAndCommitTransaction(tx, ct);
            Assert.Equal(KeyValueResponseType.Aborted, commit);

            await SettleEverywhere(managers, twoPhase ? [dataKey, otherKey] : [dataKey], ct);

            foreach (KahunaManager reader in managers)
            {
                (KeyValueResponseType readType, ReadOnlyKeyValueEntry? entry) = await Read(reader, dataKey, ct);
                Assert.Equal(KeyValueResponseType.Get, readType);
                Assert.Equal("seed", Encoding.UTF8.GetString(entry!.Value!));
                Assert.Equal(seedRevision, entry.Revision);

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
