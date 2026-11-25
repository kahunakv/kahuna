using System.Text;

using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.KeyValues.Watch;
using Kahuna.Server.KeyValues.Writes;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// Operations on key-actor state on three nodes, admitted without a wait for the writes of other keys. A write that a
/// follower showed is never staged over, a leader that lost its quorum admits nothing, and no transfer is lost while the
/// leadership moves.
/// </summary>
public sealed class TestKeyScopedApplyWaitActorOperationsCluster : BaseCluster
{
    private const int Partition = 1;

    private readonly ILogger<IRaft> raftLogger;

    private readonly ILogger<IKahuna> kahunaLogger;

    public TestKeyScopedApplyWaitActorOperationsCluster(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = TestLogFactory.Create(outputHelper, quietKommander: true);
        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    private static string EndpointOf(int index) => $"localhost:{8001 + index}";

    private static async Task<int> LeaderOf(IRaft[] rafts, CancellationToken ct, int except = -1)
    {
        int leader = -1;

        await WaitUntilAsync(async () =>
        {
            for (int i = 0; i < rafts.Length && leader < 0; i++)
            {
                if (i != except && await rafts[i].AmILeaderIfHosted(Partition, ct))
                    leader = i;
            }

            return leader >= 0;
        }, timeoutMs: 30_000);

        return leader;
    }

    private static async Task Set(IKahuna kahuna, string key, string value, CancellationToken ct)
    {
        (KeyValueResponseType type, _, _) = await RetryOnMustRetryAsync(
            () => kahuna.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, key, Encoding.UTF8.GetBytes(value), null, -1, KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct),
            static answer => answer.Item1);

        Assert.Equal(KeyValueResponseType.Set, type);
    }

    private static async Task<KeyValueWatchEvent> FirstEvent(KeyValueWatch watch, CancellationToken ct)
    {
        await foreach (KeyValueWatchFrame frame in watch.ReadAllAsync(ct))
        {
            if (frame.Event is { } watched)
                return watched;

            Assert.False(frame.IsTerminal, $"the watch ended: {frame.LagReason}");
        }

        throw new InvalidOperationException("the watch ended without an event");
    }

    private static Task<(KeyValueResponseType, long, HLCTimestamp)> StageIfEqual(
        IKahuna kahuna, HLCTimestamp tx, string key, string expected, CancellationToken ct) =>
        kahuna.LocateAndTrySetKeyValue(
            tx, key, "staged"u8.ToArray(), Encoding.UTF8.GetBytes(expected), -1, KeyValueFlags.SetIfEqualToValue, 0, KeyValueDurability.Persistent, ct);

    /// <summary>
    /// A follower applies a committed entry without waiting for the leader, so a watch there can report a write that the
    /// leader has not delivered to itself. A conditional write that a consumer builds on the event must be judged on the
    /// value the event showed, and on no older one.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AWriteThatAFollowerShowed_IsTheBaseOfALaterStagedWrite(bool viaFollower)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (IRaft[] rafts, IKahuna[] kahunas, _, _) = await AssembleClusterWithTransports(
            3, "memory", 1, raftLogger, kahunaLogger, configureRaft: static config =>
            {
                config.HeartbeatInterval = TimeSpan.FromMilliseconds(50);
                config.StartElectionTimeout = 1_500;
                config.EndElectionTimeout = 3_000;
            });

        try
        {
            int leader = await LeaderOf(rafts, ct);
            int follower = (leader + 1) % rafts.Length;
            UndeliveredKeys marks = ((KahunaManager)kahunas[leader]).KeyValues.UndeliveredKeys;

            await Set(kahunas[leader], "wait/watched", "old", ct);
            await Set(kahunas[leader], "wait/other", "other", ct);
            await WaitUntilAsync(() => !marks.MayChange(Partition, "wait/watched") && !marks.MayChange(Partition, "wait/other"));

            KeyValueWatchOpening opening = await kahunas[follower].WatchKeyValues(
                new KeyValueWatchRequest(KeyValueWatchScope.ForKey("wait/watched")), ct);
            Assert.NotNull(opening.Watch);
            await using KeyValueWatch watch = opening.Watch;

            Assert.Equal(RaftOperationStatus.Success, await rafts[leader].HoldConsumerAppliesForTesting(Partition, ct));

            // The proposer is answered at quorum on the leader, which holds the entry back from itself.
            Task write = Set(kahunas[leader], "wait/watched", "new", ct);

            KeyValueWatchEvent watched = await FirstEvent(watch, ct).WaitAsync(TimeSpan.FromSeconds(20), ct);
            Assert.Equal("new", Encoding.UTF8.GetString(watched.Value!));

            // From the follower the write is forwarded to the leader, which applies the same rule.
            IKahuna writer = kahunas[viaFollower ? follower : leader];
            HLCTimestamp tx = rafts[leader].HybridLogicalClock.TrySendOrLocalEvent(rafts[leader].GetLocalNodeId());

            Task<(KeyValueResponseType Type, long, HLCTimestamp)> staging = StageIfEqual(writer, tx, "wait/watched", "new", ct);

            // The write of another key is admitted while the entry is held. One transaction stages it, so an attempt
            // that met a lapsed lease and lands late changes nothing.
            HLCTimestamp otherTx = rafts[leader].HybridLogicalClock.TrySendOrLocalEvent(rafts[leader].GetLocalNodeId());
            KeyValueResponseType other = KeyValueResponseType.MustRetry;

            for (int attempt = 0; attempt < 40 && other != KeyValueResponseType.Set; attempt++)
            {
                Task<(KeyValueResponseType Type, long, HLCTimestamp)> task = writer.LocateAndTrySetKeyValue(
                    otherTx, "wait/other", "staged"u8.ToArray(), null, -1, KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct);

                if (await Task.WhenAny(task, Task.Delay(250, ct)) == task)
                    other = (await task).Type;
            }

            Assert.Equal(KeyValueResponseType.Set, other);

            await Task.WhenAny(staging, Task.Delay(300, ct));
            if (staging.IsCompleted)
                Assert.Equal(KeyValueResponseType.MustRetry, (await staging).Type);

            Assert.Equal(RaftOperationStatus.Success, await rafts[leader].ResumeConsumerAppliesForTesting(Partition, ct));

            KeyValueResponseType type = (await staging).Type;
            if (type == KeyValueResponseType.MustRetry)
                type = (await RetryOnMustRetryAsync(() => StageIfEqual(writer, tx, "wait/watched", "new", ct), static answer => answer.Item1)).Item1;

            Assert.Equal(KeyValueResponseType.Set, type);
            await write;
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    /// <summary>
    /// A leader cut off from its voters, with the step-down that would end it turned off. No entry of its own is in
    /// flight, so nothing but the lapse of its lease keeps it from admitting an operation.
    /// </summary>
    [Fact]
    public async Task ALeaderWithoutItsQuorum_AdmitsNothingAfterItsLeaseLapsed()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, IKahuna[] kahunas, InMemoryCommunication raftComm, _) = await AssembleClusterWithTransports(
            3, "memory", 1, raftLogger, kahunaLogger, configureRaft: static config =>
            {
                config.EnableCheckQuorum = false;
                config.LeadershipConfirmationTimeout = TimeSpan.FromMilliseconds(500);
            });

        int isolated = -1;

        try
        {
            isolated = await LeaderOf(rafts, ct);
            UndeliveredKeys marks = ((KahunaManager)kahunas[isolated]).KeyValues.UndeliveredKeys;

            await Set(kahunas[isolated], "wait/key", "old", ct);
            await WaitUntilAsync(() => !marks.MayChange(Partition, "wait/key"));
            long term = rafts[isolated].GetPartitionTerm(Partition);

            raftComm.PartitionNode(EndpointOf(isolated));
            await LeaderOf(rafts, ct, except: isolated);

            Assert.True(await rafts[isolated].AmILeaderQuick(Partition), "the cut-off leader must still believe that it leads");
            Assert.False(marks.MayChange(Partition, "wait/key"));

            KahunaManager stale = (KahunaManager)kahunas[isolated];
            HLCTimestamp tx = rafts[isolated].HybridLogicalClock.TrySendOrLocalEvent(rafts[isolated].GetLocalNodeId());

            foreach (KeyValueDurability durability in (KeyValueDurability[])[KeyValueDurability.Persistent, KeyValueDurability.Ephemeral])
            {
                KeyValueResponseType[] refused =
                [
                    (await stale.LocateAndTrySetKeyValue(tx, "wait/key", "stale"u8.ToArray(), null, -1, KeyValueFlags.None, 0, durability, ct)).Item1,
                    (await stale.LocateAndTrySetManyKeyValue([new() { TransactionId = tx, Key = "wait/key", Value = "stale"u8.ToArray(), CompareRevision = -1, Durability = durability }], ct))[0].Type,
                    (await stale.LocateAndTryDeleteKeyValue(tx, "wait/key", durability, ct)).Item1,
                    (await stale.LocateAndTryDeleteManyKeyValue([new() { TransactionId = tx, Key = "wait/key", Durability = durability }], ct))[0].Type,
                    (await stale.LocateAndTryExtendKeyValue(tx, "wait/key", 60_000, durability, ct)).Item1,
                    (await stale.LocateAndTryAcquireExclusiveLock(tx, "wait/key", 30_000, durability, ct)).Item1,
                    (await stale.LocateAndTryAcquireManyExclusiveLocks(tx, [("wait/key", 30_000, durability)], ct))[0].Item1,
                    (await stale.LocateAndTryAcquireRangeLock(tx, "wait", "wait/key", true, "wait/key", true, 30_000, durability, RangeLockMode.Shared, ct)).Item1,
                    (await stale.LocateAndTryProbeMany(tx, HLCTimestamp.Zero, ["wait/key"], KeyValueProbeLock.Shared, 30_000, durability, ct))[0].Type,
                    await stale.LocateAndTryCheckWriteIntent(tx, "wait/key", durability, ct),
                    (await stale.TryCheckManyWriteIntentValuesConfirmed(
                        tx, [new("wait/key", durability, KeyValueConflictChecks.OwnStagedIntent | KeyValueConflictChecks.ForeignRangeLock)], ct))[0].type
                ];

                Assert.All(refused, static type => Assert.Equal(KeyValueResponseType.MustRetry, type));
            }

            // The proof that a lock granted in this leadership is still in force.
            Assert.Equal(KeyValueResponseType.MustRetry, (await stale.TryCheckManyWriteIntentValuesConfirmed(
                tx, [new("wait/key", KeyValueDurability.Persistent, KeyValueConflictChecks.LeaderTerm, term)], ct))[0].type);

            // The turn of a script over one ephemeral key.
            Assert.False(await stale.KeyValues.IsLocallyLedHashKey("wait/key", KeyValueDurability.Ephemeral, ct));
        }
        finally
        {
            if (isolated >= 0)
                raftComm.HealPartition(EndpointOf(isolated));

            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    /// <summary>
    /// Transfers between a few accounts through every node while the leadership of the accounts moves, with and without
    /// locks. A transfer whose base was read or staged over an undelivered write would break the total.
    /// </summary>
    [Theory]
    [InlineData(KeyValueTransactionLocking.Optimistic)]
    [InlineData(KeyValueTransactionLocking.Pessimistic)]
    public async Task WhileTheLeadershipMoves_TheTotalOfTheAccountsHolds(KeyValueTransactionLocking locking)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (IRaft raft1, IRaft raft2, IRaft raft3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) = await AssembleThreNodeCluster(
            "memory", 1, raftLogger, kahunaLogger, configure: static configuration =>
            {
                configuration.OnePhaseApplyTimeValidation = true;
                configuration.DurableMaterializeOnResolve = true;
            });
        IRaft[] rafts = [raft1, raft2, raft3];
        IKahuna[] kahunas = [kahuna1, kahuna2, kahuna3];

        const int Accounts = 12;
        const long Initial = 1_000;
        long committed = 0;
        int moves = 0;
        using CancellationTokenSource stop = CancellationTokenSource.CreateLinkedTokenSource(ct);

        static string KeyOf(int account) => "wait/account-" + account;

        async Task<long?> Read(IKahuna kahuna, TransactionHandle tx, int account)
        {
            (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await kahuna.LocateAndTryGetValue(
                tx.TransactionId, KeyOf(account), -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct,
                coordinatorKey: tx.CoordinatorKey, operationId: TransactionOperationId.NewRandom());

            return type == KeyValueResponseType.Get ? long.Parse(Encoding.UTF8.GetString(entry!.Value!)) : null;
        }

        async Task<bool> Write(IKahuna kahuna, TransactionHandle tx, params (int Account, long Balance)[] balances)
        {
            List<KahunaSetKeyValueRequestItem> items = [.. balances.Select(balance => new KahunaSetKeyValueRequestItem
            {
                TransactionId = tx.TransactionId,
                Key = KeyOf(balance.Account),
                Value = Encoding.UTF8.GetBytes(balance.Balance.ToString()),
                CompareRevision = -1,
                Durability = KeyValueDurability.Persistent
            })];

            List<KahunaSetKeyValueResponseItem> answers = await kahuna.LocateAndTrySetManyKeyValue(items, ct, tx.CoordinatorKey, TransactionOperationId.NewRandom());
            return answers.All(static answer => answer.Type == KeyValueResponseType.Set);
        }

        async Task<bool> Transfer(IKahuna kahuna, int from, int to)
        {
            (KeyValueResponseType started, TransactionHandle tx) = await kahuna.LocateAndStartTransaction(
                new KeyValueTransactionOptions { CoordinatorKey = KeyOf(from), Locking = locking, ReadValidation = ReadValidation.TrackAndValidate, Timeout = 10_000 }, ct);
            if (started != KeyValueResponseType.Set)
                return false;

            bool ready = true;

            if (locking == KeyValueTransactionLocking.Pessimistic)
            {
                List<(string, int, KeyValueDurability)> locks = [.. new[] { Math.Min(from, to), Math.Max(from, to) }.Select(static account => (KeyOf(account), 10_000, KeyValueDurability.Persistent))];
                ready = (await kahuna.LocateAndTryAcquireManyExclusiveLocks(tx.TransactionId, locks, ct, tx.CoordinatorKey, TransactionOperationId.NewRandom()))
                    .Count(static answer => answer.Item1 == KeyValueResponseType.Locked) == locks.Count;
            }

            if (ready
                && await Read(kahuna, tx, from) is { } debit
                && await Read(kahuna, tx, to) is { } credit
                && await Write(kahuna, tx, (from, debit - 1), (to, credit + 1)))
                return (await kahuna.LocateAndCommitTransaction(tx, ct)).Type == KeyValueResponseType.Committed;

            await kahuna.LocateAndRollbackTransaction(tx, ct);
            return false;
        }

        async Task Worker(int seed)
        {
            Random random = new(seed);

            while (!stop.IsCancellationRequested)
            {
                int from = random.Next(Accounts), to = (from + 1 + random.Next(Accounts - 1)) % Accounts;

                try
                {
                    if (await Transfer(kahunas[random.Next(kahunas.Length)], from, to))
                        Interlocked.Increment(ref committed);
                }
                catch (Exception) when (!ct.IsCancellationRequested)
                {
                    // A node that lost its leadership or its session answers with an exception. The outcome is not known.
                }
            }
        }

        try
        {
            await LeaderOf(rafts, ct);

            for (int account = 0; account < Accounts; account++)
                await Set(kahuna1, KeyOf(account), Initial.ToString(), ct);

            Task[] work = [.. Enumerable.Range(0, 8).Select(seed => Task.Run(() => Worker(seed), ct))];

            DateTime until = DateTime.UtcNow + TimeSpan.FromSeconds(8);
            while (DateTime.UtcNow < until)
            {
                await Task.Delay(400, ct);

                int leader = await LeaderOf(rafts, ct);
                if (await rafts[leader].TransferLeadershipAsync(Partition, EndpointOf((leader + 1) % rafts.Length), ct) == RaftOperationStatus.Success)
                    moves++;
            }

            await stop.CancelAsync();
            await Task.WhenAll(work);

            // Every transfer is decided or fenced when a read of its keys answers.
            long total = 0;
            for (int account = 0; account < Accounts; account++)
            {
                (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await RetryOnMustRetryAsync(
                    () => kahuna1.LocateAndTryGetValue(HLCTimestamp.Zero, KeyOf(account), -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct).AsTask(),
                    static answer => answer.Item1, timeoutMs: 30_000);
                Assert.Equal(KeyValueResponseType.Get, type);
                total += long.Parse(Encoding.UTF8.GetString(entry!.Value!));
            }

            Assert.Equal(Accounts * Initial, total);
            Assert.True(moves >= 3, $"the leadership moved {moves} times");
            Assert.True(committed > 50, $"{committed} transfers committed");
        }
        finally
        {
            await stop.CancelAsync();
            await LeaveCluster(raft1, raft2, raft3);
        }
    }
}
