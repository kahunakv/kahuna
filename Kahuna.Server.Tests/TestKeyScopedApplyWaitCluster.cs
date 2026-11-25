using System.Collections.Concurrent;
using System.Text;

using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.KeyValues.Watch;
using Kahuna.Server.KeyValues.Writes;
using Kahuna.Server.Replication;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// Reads of named keys on three nodes. What a follower showed is never unread on the leader, a leader that lost its
/// quorum answers nothing, and a new leader answers with everything the one before it acknowledged.
/// </summary>
public sealed class TestKeyScopedApplyWaitCluster : BaseCluster
{
    private const int Partition = 1;

    private readonly ILogger<IRaft> raftLogger;

    private readonly ILogger<IKahuna> kahunaLogger;

    public TestKeyScopedApplyWaitCluster(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = TestLogFactory.Create(outputHelper, quietKommander: true);
        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    private static string EndpointOf(int index) => $"localhost:{8001 + index}";

    /// <summary>A cluster whose leaders stay while a test holds the delivery on one of them, also when other tests keep
    /// the machine busy.</summary>
    private async Task<(IRaft[] Rafts, IKahuna[] Kahunas)> AssembleCalmCluster(int partitions, Action<KahunaConfiguration>? configure = null)
    {
        (IRaft[] rafts, IKahuna[] kahunas, _, _) = await AssembleClusterWithTransports(
            3, "memory", partitions, raftLogger, kahunaLogger, configureRaft: static config =>
            {
                config.HeartbeatInterval = TimeSpan.FromMilliseconds(50);
                config.StartElectionTimeout = 1_500;
                config.EndElectionTimeout = 3_000;
            }, configureKahuna: configure);

        return (rafts, kahunas);
    }

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

    private static UndeliveredKeys MarksOf(IKahuna kahuna) => ((KahunaManager)kahuna).KeyValues.UndeliveredKeys;

    private static async Task Set(IKahuna kahuna, string key, string value, CancellationToken ct)
    {
        (KeyValueResponseType type, _, _) = await RetryOnMustRetryAsync(
            () => kahuna.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, key, Encoding.UTF8.GetBytes(value), null, -1, KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct),
            static answer => answer.Item1);

        Assert.Equal(KeyValueResponseType.Set, type);
    }

    private static async Task<(KeyValueResponseType Type, string? Value)> Get(IKahuna kahuna, string key, CancellationToken ct)
    {
        (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);

        return (type, type == KeyValueResponseType.Get ? Encoding.UTF8.GetString(entry!.Value!) : null);
    }

    /// <summary>
    /// Reads until one read answers without a wait. The lease lasts one heartbeat, and a read that meets a lapsed lease
    /// runs a round that waits for the delivery with it.
    /// </summary>
    private static async Task<string?> GetPromptly(IKahuna kahuna, string key, CancellationToken ct)
    {
        for (int attempt = 0; attempt < 40; attempt++)
        {
            Task<(KeyValueResponseType Type, string? Value)> read = Get(kahuna, key, ct);

            if (await Task.WhenAny(read, Task.Delay(250, ct)) == read && (await read).Type == KeyValueResponseType.Get)
                return (await read).Value;
        }

        throw new TimeoutException($"no read of {key} answered");
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

    /// <summary>
    /// A follower applies a committed entry without waiting for the leader, so a watch there can report a write that the
    /// leader has not delivered to itself. A consumer that drops its cached copy on the event and reads the key again
    /// must not get the old value back: nothing would correct it later.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AWriteThatAFollowerShowed_IsNotReadBackInItsOldValue(bool viaFollower)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleCalmCluster(partitions: 1);

        try
        {
            int leader = await LeaderOf(rafts, ct);
            int follower = (leader + 1) % rafts.Length;
            UndeliveredKeys marks = MarksOf(kahunas[leader]);

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

            // From the follower the read is forwarded to the leader, which applies the same rule.
            IKahuna reader = kahunas[viaFollower ? follower : leader];
            Task<(KeyValueResponseType Type, string? Value)> read = Get(reader, "wait/watched", ct);

            Assert.Equal("other", await GetPromptly(reader, "wait/other", ct));

            await Task.WhenAny(read, Task.Delay(300, ct));
            if (read.IsCompleted)
                Assert.NotEqual("old", (await read).Value);

            Assert.Equal(RaftOperationStatus.Success, await rafts[leader].ResumeConsumerAppliesForTesting(Partition, ct));

            Assert.NotEqual("old", (await read).Value);
            await write;
            await WaitUntilAsync(async () => await Get(reader, "wait/watched", ct) is (KeyValueResponseType.Get, "new"));
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    /// <summary>Records whether the keys of a batch were marked when the batch reached Raft, and holds it there.</summary>
    private sealed class MarkProbe(IPartitionBatchExecutor inner) : IPartitionBatchExecutor
    {
        public Func<bool>? Marked;

        public readonly ConcurrentQueue<bool> AtTheProposal = [];

        public Task<RaftBatchReplicationResult> ReplicateAsync(int partitionId, IReadOnlyList<RaftProposalEntry> entries, CancellationToken cancellationToken)
        {
            if (partitionId == Partition && Volatile.Read(ref Marked) is { } marked)
                AtTheProposal.Enqueue(marked());

            return inner.ReplicateAsync(partitionId, entries, cancellationToken);
        }
    }

    /// <summary>
    /// A key is marked before its entry reaches Raft, and stays marked until the stores of the leader hold the entry: a
    /// follower can apply it at any moment in between.
    /// </summary>
    [Fact]
    public async Task AKeyIsMarkedBeforeItsProposal_AndUntilItsApplyEnds()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        MarkProbe[] probes = new MarkProbe[3];
        (IRaft raft1, IRaft raft2, IRaft raft3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) = await AssembleThreNodeCluster(
            "memory", 1, raftLogger, kahunaLogger, decorateWriteBatchExecutor: id => inner => probes[id - 1] = new MarkProbe(inner));
        IRaft[] rafts = [raft1, raft2, raft3];
        IKahuna[] kahunas = [kahuna1, kahuna2, kahuna3];

        try
        {
            int leader = await LeaderOf(rafts, ct);
            KahunaManager manager = (KahunaManager)kahunas[leader];
            UndeliveredKeys marks = manager.KeyValues.UndeliveredKeys;

            await Set(kahunas[leader], "wait/key", "old", ct);
            await WaitUntilAsync(() => !marks.MayChange(Partition, "wait/key"));

            ConcurrentQueue<bool> atTheApply = [];
            Volatile.Write(ref probes[leader].Marked, () => marks.MayChange(Partition, "wait/key"));
            manager.KeyValues.ReplicationDispatcher.ApplySkipForTesting = (partition, log) =>
            {
                if (partition == Partition && log.LogType == ReplicationTypes.KeyValues)
                    atTheApply.Enqueue(marks.MayChange(Partition, "wait/key"));

                return false;
            };

            await Set(kahunas[leader], "wait/key", "new", ct);
            await WaitUntilAsync(() => !marks.MayChange(Partition, "wait/key"));

            Assert.Equal([true], probes[leader].AtTheProposal);
            Assert.Equal([true], atTheApply);
        }
        finally
        {
            await LeaveCluster(raft1, raft2, raft3);
        }
    }

    /// <summary>
    /// A leader cut off from its voters, with the step-down that would end it turned off. It still believes that it
    /// leads, no entry of its own is in flight, and so nothing but the lapse of its lease keeps it from answering.
    /// </summary>
    [Fact]
    public async Task ALeaderWithoutItsQuorum_AnswersNoReadAfterItsLeaseLapsed()
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
            UndeliveredKeys marks = MarksOf(kahunas[isolated]);

            await Set(kahunas[isolated], "wait/key", "old", ct);
            await WaitUntilAsync(() => !marks.MayChange(Partition, "wait/key"));
            Assert.Equal((KeyValueResponseType.Get, "old"), await Get(kahunas[isolated], "wait/key", ct));

            raftComm.PartitionNode(EndpointOf(isolated));
            int successor = await LeaderOf(rafts, ct, except: isolated);

            await Set(kahunas[successor], "wait/key", "new", ct);
            Assert.True(await rafts[isolated].AmILeaderQuick(Partition), "the cut-off leader must still believe that it leads");

            KahunaManager stale = (KahunaManager)kahunas[isolated];

            Assert.Equal(KeyValueResponseType.MustRetry, (await Get(stale, "wait/key", ct)).Type);
            Assert.Equal(KeyValueResponseType.MustRetry, (await stale.LocateAndTryExistsValue(
                HLCTimestamp.Zero, "wait/key", -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct)).Item1);
            Assert.Equal(KeyValueResponseType.MustRetry, (await stale.LocateAndTryGetManyValues(
                HLCTimestamp.Zero, HLCTimestamp.Zero, [("wait/key", -1, KeyValueDurability.Persistent)], ct))[0].Item1);
            Assert.Equal(KeyValueResponseType.MustRetry, (await stale.LocateAndTryProbeMany(
                HLCTimestamp.Zero, HLCTimestamp.Zero, ["wait/key"], KeyValueProbeLock.None, 0, KeyValueDurability.Persistent, ct))[0].Type);

            // An ephemeral read skips the wait for the apply on every path, and not the proof of leadership.
            Assert.Equal(KeyValueResponseType.MustRetry, (await stale.LocateAndTryGetValue(
                HLCTimestamp.Zero, "wait/key", -1, HLCTimestamp.Zero, KeyValueDurability.Ephemeral, ct)).Item1);
        }
        finally
        {
            if (isolated >= 0)
                raftComm.HealPartition(EndpointOf(isolated));

            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    /// <summary>
    /// A commit that another node coordinates. The bundle reaches the leader of its keys over the node transport, and the
    /// leader marks its keys as it does for its own.
    /// </summary>
    [Fact]
    public async Task ACommitCoordinatedElsewhere_MarksItsKeysOnTheLeader()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleCalmCluster(partitions: 2, static configuration =>
        {
            configuration.OnePhaseApplyTimeValidation = true;
            configuration.DurableMaterializeOnResolve = true;
        });

        try
        {
            int leader = await LeaderOf(rafts, ct);
            int other = (leader + 1) % rafts.Length;
            KahunaManager manager = (KahunaManager)kahunas[leader];

            // Keys on partition 1, and a coordinator key on partition 2, which another node leads.
            string Key(int partition, string tag)
            {
                for (int i = 0; ; i++)
                {
                    string candidate = $"wait-{tag}-{i}/k";
                    if (manager.KeyValues.LocateDurablePartition(candidate).PartitionId == partition)
                        return candidate;
                }
            }

            string written = Key(Partition, "written"), untouched = Key(Partition, "untouched"), coordinator = Key(2, "coordinator");
            await Set(kahunas[leader], written, "old", ct);
            await Set(kahunas[leader], untouched, "old", ct);

            await WaitUntilAsync(async () =>
            {
                int current = -1;
                for (int i = 0; i < rafts.Length; i++)
                {
                    if (await rafts[i].AmILeaderIfHosted(2, ct))
                        current = i;
                }

                if (current == other)
                    return true;

                if (current >= 0)
                    await rafts[current].TransferLeadershipAsync(2, EndpointOf(other), ct);

                return false;
            }, timeoutMs: 30_000);

            Assert.True(await rafts[leader].AmILeaderIfHosted(Partition, ct));

            (KeyValueResponseType started, TransactionHandle tx) = await RetryOnMustRetryAsync(
                () => kahunas[other].LocateAndStartTransaction(
                    new KeyValueTransactionOptions { CoordinatorKey = coordinator, Locking = KeyValueTransactionLocking.Optimistic, Timeout = 60_000 }, ct),
                static answer => answer.Item1);
            Assert.Equal(KeyValueResponseType.Set, started);

            (KeyValueResponseType staged, _, _) = await kahunas[other].LocateAndTrySetKeyValue(
                tx.TransactionId, written, "new"u8.ToArray(), null, -1, KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct,
                coordinatorKey: tx.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
            Assert.Equal(KeyValueResponseType.Set, staged);

            UndeliveredKeys marks = MarksOf(kahunas[leader]);
            await WaitUntilAsync(() => !marks.MayChange(Partition, written));
            Assert.Equal(RaftOperationStatus.Success, await rafts[leader].HoldConsumerAppliesForTesting(Partition, ct));

            Task<KeyValueResponseType> commit = Task.Run(async () => (await kahunas[other].LocateAndCommitTransaction(tx, ct)).Type, ct);

            await WaitUntilAsync(() => marks.MayChange(Partition, written));
            Assert.False(marks.MayChange(Partition, untouched));
            Assert.Equal("old", await GetPromptly(kahunas[leader], untouched, ct));

            Assert.Equal(RaftOperationStatus.Success, await rafts[leader].ResumeConsumerAppliesForTesting(Partition, ct));
            Assert.Equal(KeyValueResponseType.Committed, await commit);
            Assert.Equal((KeyValueResponseType.Get, "new"), await Get(kahunas[leader], written, ct));
        }
        finally
        {
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    /// <summary>
    /// One writer per key counts up, with direct writes on some keys and transactions on the others. Readers ask through
    /// every node while the leadership of the keys moves. A read that starts after the acknowledgement of a value must
    /// answer that value or a later one, and the reads of one reader never go back.
    /// </summary>
    [Fact]
    public async Task WhileTheLeadershipMoves_AReadAnswersEveryAcknowledgedWrite()
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

        const int Keys = 8;
        long[] acknowledged = new long[Keys];
        ConcurrentQueue<string> violations = [];
        long served = 0;
        int moves = 0;
        using CancellationTokenSource stop = CancellationTokenSource.CreateLinkedTokenSource(ct);

        string KeyOf(int key) => "wait/counter-" + key;

        async Task Writer(int key)
        {
            Random random = new(key);

            for (long value = 1; !stop.IsCancellationRequested; value++)
            {
                IKahuna kahuna = kahunas[random.Next(kahunas.Length)];
                KeyValueResponseType type;

                try
                {
                    type = key % 2 == 0 ? await WriteDirect(kahuna, key, value) : await WriteInTransaction(kahuna, key, value);
                }
                catch (Exception) when (!ct.IsCancellationRequested)
                {
                    type = KeyValueResponseType.Errored;
                }

                // A write whose outcome is not known may have landed or not. The same value again cannot be told from it.
                if (type is KeyValueResponseType.Set or KeyValueResponseType.Committed)
                    Volatile.Write(ref acknowledged[key], value);
            }
        }

        async Task<KeyValueResponseType> WriteDirect(IKahuna kahuna, int key, long value) => (await kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, KeyOf(key), Encoding.UTF8.GetBytes(value.ToString()), null, -1, KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct)).Item1;

        async Task<KeyValueResponseType> WriteInTransaction(IKahuna kahuna, int key, long value)
        {
            (KeyValueResponseType started, TransactionHandle tx) = await kahuna.LocateAndStartTransaction(
                new KeyValueTransactionOptions { CoordinatorKey = KeyOf(key), Locking = KeyValueTransactionLocking.Optimistic, Timeout = 10_000 }, ct);
            if (started != KeyValueResponseType.Set)
                return started;

            (KeyValueResponseType staged, _, _) = await kahuna.LocateAndTrySetKeyValue(
                tx.TransactionId, KeyOf(key), Encoding.UTF8.GetBytes(value.ToString()), null, -1, KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct,
                coordinatorKey: tx.CoordinatorKey, operationId: TransactionOperationId.NewRandom());

            if (staged != KeyValueResponseType.Set)
            {
                await kahuna.LocateAndRollbackTransaction(tx, ct);
                return staged;
            }

            return (await kahuna.LocateAndCommitTransaction(tx, ct)).Type;
        }

        async Task Reader(int seed)
        {
            Random random = new(seed);
            long[] seen = new long[Keys];

            while (!stop.IsCancellationRequested)
            {
                int key = random.Next(Keys);
                long expected = Math.Max(Volatile.Read(ref acknowledged[key]), seen[key]);

                (KeyValueResponseType Type, string? Value) answer;
                try
                {
                    answer = await Get(kahunas[random.Next(kahunas.Length)], KeyOf(key), ct);
                }
                catch (Exception) when (!ct.IsCancellationRequested)
                {
                    continue;
                }

                if (answer.Type == KeyValueResponseType.DoesNotExist && expected > 0)
                    violations.Enqueue($"{KeyOf(key)} is absent after {expected}");

                if (answer.Type != KeyValueResponseType.Get)
                    continue;

                Interlocked.Increment(ref served);
                long value = long.Parse(answer.Value!);
                if (value < expected)
                    violations.Enqueue($"{KeyOf(key)} answered {value} after {expected}");

                seen[key] = Math.Max(seen[key], value);
            }
        }

        try
        {
            await LeaderOf(rafts, ct);

            Task[] work =
            [
                .. Enumerable.Range(0, Keys).Select(key => Task.Run(() => Writer(key), ct)),
                .. Enumerable.Range(0, 6).Select(seed => Task.Run(() => Reader(100 + seed), ct))
            ];

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

            Assert.Empty(violations);
            Assert.True(moves >= 3, $"the leadership moved {moves} times");
            Assert.True(served > 100, $"{served} reads were answered");
            Assert.All(acknowledged, static value => Assert.True(value > 0));
        }
        finally
        {
            await stop.CancelAsync();
            await LeaveCluster(raft1, raft2, raft3);
        }
    }
}
