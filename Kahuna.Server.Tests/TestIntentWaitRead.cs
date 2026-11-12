using System.Diagnostics;
using System.Text;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// A transactional latest read that asks to wait (<see cref="KeyValueIntentWait"/>) parks behind a live write intent
/// of another transaction instead of pinning the committed head below it, and a flagged transactional set that can no
/// longer commit (<see cref="KeyValueFlags.AbortWhenDoomed"/>) answers <c>Aborted</c> at once.
///
/// <para>Every read here goes through <see cref="IKahuna"/>, so the locator, the published-head path and the actor
/// all take part. The holder's write is a staged transactional set, which plants the write intent the reader meets.</para>
/// </summary>
public sealed class TestIntentWaitRead
{
    private static readonly KeyValueIntentWait LongWait = new(10_000, HoldsLocks: false);

    private readonly ILoggerFactory loggerFactory;

    public TestIntentWaitRead(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper, quietKommander: true);
    }

    private static async Task<EmbeddedKahunaNode> StartNode(ILoggerFactory loggerFactory, CancellationToken ct, int stagedLeaseMs = 15_000, int maxParkedPerKey = 64)
    {
        EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            TimerInitialDelay = TimeSpan.FromMilliseconds(50),
            ReadIOThreads = 1,
            WriteIOThreads = 1,
            PartitionExecutorPoolSize = 1,
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 4,
            StagedWriteIntentLeaseMs = stagedLeaseMs,
            MaxParkedReadsPerKey = maxParkedPerKey
        }, loggerFactory);
        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync("intentwait/x", ct);
        return node;
    }

    private static string NewKey() => "intentwait/" + Guid.NewGuid().ToString("N")[..8];

    private static async Task<TransactionHandle> Start(IKahuna kahuna, string coordinatorKey, CancellationToken ct)
    {
        (KeyValueResponseType type, TransactionHandle handle) = await kahuna.LocateAndStartTransaction(
            new KeyValueTransactionOptions { CoordinatorKey = coordinatorKey, Timeout = 30_000, Locking = KeyValueTransactionLocking.Optimistic },
            ct);
        Assert.Equal(KeyValueResponseType.Set, type);
        return handle;
    }

    private static Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> TxGet(
        IKahuna kahuna, TransactionHandle tx, string key, KeyValueIntentWait wait, CancellationToken ct) =>
        kahuna.LocateAndTryGetValue(
            tx.TransactionId, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct,
            tx.CoordinatorKey, TransactionOperationId.NewRandom(), intentWait: wait).AsTask();

    private static async Task<KeyValueResponseType> TxSet(
        IKahuna kahuna, TransactionHandle tx, string key, string value, CancellationToken ct, KeyValueFlags flags = KeyValueFlags.Set)
    {
        (KeyValueResponseType type, _, _) = await kahuna.LocateAndTrySetKeyValue(
            tx.TransactionId, key, Encoding.UTF8.GetBytes(value), null, -1, flags, 0,
            KeyValueDurability.Persistent, ct, coordinatorKey: tx.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
        return type;
    }

    private static async Task AutoCommitSet(IKahuna kahuna, string key, string value, CancellationToken ct)
    {
        (KeyValueResponseType type, _, _) = await kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, key, Encoding.UTF8.GetBytes(value), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Set, type);
    }

    private static async Task Commit(IKahuna kahuna, TransactionHandle tx, CancellationToken ct)
    {
        (KeyValueResponseType type, _) = await kahuna.LocateAndCommitTransaction(tx, ct);
        Assert.Equal(KeyValueResponseType.Committed, type);
    }

    private static string ValueOf((KeyValueResponseType Type, ReadOnlyKeyValueEntry? Entry) read)
    {
        Assert.Equal(KeyValueResponseType.Get, read.Type);
        return Encoding.UTF8.GetString(read.Entry!.Value!);
    }

    /// <summary>Gives a read that should be parked time to answer if it were not, and asserts it did not.</summary>
    private static async Task AssertStillParked(Task read)
    {
        await Task.Delay(300, TestContext.Current.CancellationToken);
        Assert.False(read.IsCompleted, "the read answered while the other transaction's write intent was live");
    }

    [Fact]
    public async Task WaitingRead_ParksBehindLiveIntent_AndPinsTheHoldersCommit()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        IKahuna kahuna = node.Kahuna;
        string key = NewKey();
        await AutoCommitSet(kahuna, key, "seed", ct);

        TransactionHandle holder = await Start(kahuna, key + "/holder", ct);
        Assert.Equal(KeyValueResponseType.Set, await TxSet(kahuna, holder, key, "holder", ct));

        TransactionHandle reader = await Start(kahuna, key + "/reader", ct);
        Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> read = TxGet(kahuna, reader, key, LongWait, ct);

        await AssertStillParked(read);
        await Commit(kahuna, holder, ct);

        Assert.Equal("holder", ValueOf(await read.WaitAsync(TimeSpan.FromSeconds(10), ct)));

        // The read pinned the committed write, so the read-modify-write goes through with no abort.
        Assert.Equal(KeyValueResponseType.Set, await TxSet(kahuna, reader, key, "reader", ct));
        await Commit(kahuna, reader, ct);
    }

    [Fact]
    public async Task WaitingRead_HolderRollsBack_PinsTheOldValue()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        IKahuna kahuna = node.Kahuna;
        string key = NewKey();
        await AutoCommitSet(kahuna, key, "seed", ct);

        TransactionHandle holder = await Start(kahuna, key + "/holder", ct);
        Assert.Equal(KeyValueResponseType.Set, await TxSet(kahuna, holder, key, "holder", ct));

        TransactionHandle reader = await Start(kahuna, key + "/reader", ct);
        Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> read = TxGet(kahuna, reader, key, LongWait, ct);

        await AssertStillParked(read);
        Assert.Equal(KeyValueResponseType.RolledBack, await kahuna.LocateAndRollbackTransaction(holder, ct));

        Assert.Equal("seed", ValueOf(await read.WaitAsync(TimeSpan.FromSeconds(10), ct)));
        Assert.Equal(KeyValueResponseType.Set, await TxSet(kahuna, reader, key, "reader", ct));
        await Commit(kahuna, reader, ct);
    }

    [Fact]
    public async Task WaitingRead_AtTheLimit_IsServedTheOldHead()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        IKahuna kahuna = node.Kahuna;
        string key = NewKey();
        await AutoCommitSet(kahuna, key, "seed", ct);

        TransactionHandle holder = await Start(kahuna, key + "/holder", ct);
        Assert.Equal(KeyValueResponseType.Set, await TxSet(kahuna, holder, key, "holder", ct));

        TransactionHandle reader = await Start(kahuna, key + "/reader", ct);

        Stopwatch elapsed = Stopwatch.StartNew();
        (KeyValueResponseType, ReadOnlyKeyValueEntry?) read = await TxGet(kahuna, reader, key, new KeyValueIntentWait(400, HoldsLocks: false), ct)
            .WaitAsync(TimeSpan.FromSeconds(10), ct);
        elapsed.Stop();

        Assert.Equal("seed", ValueOf(read));
        Assert.True(elapsed.ElapsedMilliseconds >= 300, $"the read answered after {elapsed.ElapsedMilliseconds} ms, before its limit");

        await kahuna.LocateAndRollbackTransaction(holder, ct);
    }

    [Fact]
    public async Task WaitingRead_IntentLeaseLapses_WakesAndPinsTheOldValue()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct, stagedLeaseMs: 600);
        IKahuna kahuna = node.Kahuna;
        string key = NewKey();
        await AutoCommitSet(kahuna, key, "seed", ct);

        TransactionHandle holder = await Start(kahuna, key + "/holder", ct);
        Assert.Equal(KeyValueResponseType.Set, await TxSet(kahuna, holder, key, "holder", ct));

        TransactionHandle reader = await Start(kahuna, key + "/reader", ct);

        // Nothing touches the key after the read parks: only the read's own timer finds the lapsed lease, well
        // before the wait's limit.
        Stopwatch elapsed = Stopwatch.StartNew();
        (KeyValueResponseType, ReadOnlyKeyValueEntry?) read = await TxGet(kahuna, reader, key, LongWait, ct)
            .WaitAsync(TimeSpan.FromSeconds(10), ct);
        elapsed.Stop();

        Assert.Equal("seed", ValueOf(read));
        Assert.True(elapsed.ElapsedMilliseconds < 5_000, $"the read waited {elapsed.ElapsedMilliseconds} ms for a lapsed lease");
    }

    [Fact]
    public async Task YoungerRequesterThatHoldsLocks_DoesNotWait()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        IKahuna kahuna = node.Kahuna;
        string key = NewKey();
        await AutoCommitSet(kahuna, key, "seed", ct);

        TransactionHandle holder = await Start(kahuna, key + "/holder", ct);
        TransactionHandle reader = await Start(kahuna, key + "/reader", ct);
        Assert.True(reader.TransactionId.CompareTo(holder.TransactionId) > 0);
        Assert.Equal(KeyValueResponseType.Set, await TxSet(kahuna, holder, key, "holder", ct));

        (KeyValueResponseType, ReadOnlyKeyValueEntry?) read = await TxGet(kahuna, reader, key, new KeyValueIntentWait(10_000, HoldsLocks: true), ct)
            .WaitAsync(TimeSpan.FromSeconds(2), ct);

        Assert.Equal("seed", ValueOf(read));
        await kahuna.LocateAndRollbackTransaction(holder, ct);
    }

    [Fact]
    public async Task OlderRequesterThatHoldsLocks_Waits()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        IKahuna kahuna = node.Kahuna;
        string key = NewKey();
        await AutoCommitSet(kahuna, key, "seed", ct);

        TransactionHandle reader = await Start(kahuna, key + "/reader", ct);
        TransactionHandle holder = await Start(kahuna, key + "/holder", ct);
        Assert.True(reader.TransactionId.CompareTo(holder.TransactionId) < 0);
        Assert.Equal(KeyValueResponseType.Set, await TxSet(kahuna, holder, key, "holder", ct));

        Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> read = TxGet(kahuna, reader, key, new KeyValueIntentWait(10_000, HoldsLocks: true), ct);

        await AssertStillParked(read);
        await Commit(kahuna, holder, ct);

        Assert.Equal("holder", ValueOf(await read.WaitAsync(TimeSpan.FromSeconds(10), ct)));
    }

    [Fact]
    public async Task ReadWithoutTheOption_IsUnchanged()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        IKahuna kahuna = node.Kahuna;
        string key = NewKey();
        await AutoCommitSet(kahuna, key, "seed", ct);

        TransactionHandle holder = await Start(kahuna, key + "/holder", ct);
        Assert.Equal(KeyValueResponseType.Set, await TxSet(kahuna, holder, key, "holder", ct));

        TransactionHandle reader = await Start(kahuna, key + "/reader", ct);
        (KeyValueResponseType, ReadOnlyKeyValueEntry?) read = await TxGet(kahuna, reader, key, KeyValueIntentWait.None, ct)
            .WaitAsync(TimeSpan.FromSeconds(2), ct);

        Assert.Equal("seed", ValueOf(read));
        await kahuna.LocateAndRollbackTransaction(holder, ct);
    }

    [Fact]
    public async Task SecondRead_OfAPinnedKey_DoesNotWait()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        IKahuna kahuna = node.Kahuna;
        string key = NewKey();
        await AutoCommitSet(kahuna, key, "seed", ct);

        TransactionHandle reader = await Start(kahuna, key + "/reader", ct);
        Assert.Equal("seed", ValueOf(await TxGet(kahuna, reader, key, LongWait, ct)));

        TransactionHandle holder = await Start(kahuna, key + "/holder", ct);
        Assert.Equal(KeyValueResponseType.Set, await TxSet(kahuna, holder, key, "holder", ct));

        // The pin is the transaction's view of the key; a later read serves it without waiting.
        Assert.Equal("seed", ValueOf(await TxGet(kahuna, reader, key, LongWait, ct).WaitAsync(TimeSpan.FromSeconds(2), ct)));
        await kahuna.LocateAndRollbackTransaction(holder, ct);
    }

    [Fact]
    public async Task ParkedReads_OverTheKeyBound_AreServedTheOldHead()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct, maxParkedPerKey: 1);
        IKahuna kahuna = node.Kahuna;
        string key = NewKey();
        await AutoCommitSet(kahuna, key, "seed", ct);

        TransactionHandle holder = await Start(kahuna, key + "/holder", ct);
        Assert.Equal(KeyValueResponseType.Set, await TxSet(kahuna, holder, key, "holder", ct));

        TransactionHandle first = await Start(kahuna, key + "/first", ct);
        Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> parked = TxGet(kahuna, first, key, LongWait, ct);
        await AssertStillParked(parked);

        TransactionHandle second = await Start(kahuna, key + "/second", ct);
        Assert.Equal("seed", ValueOf(await TxGet(kahuna, second, key, LongWait, ct).WaitAsync(TimeSpan.FromSeconds(2), ct)));

        await Commit(kahuna, holder, ct);
        Assert.Equal("holder", ValueOf(await parked.WaitAsync(TimeSpan.FromSeconds(10), ct)));
    }

    [Fact]
    public async Task TwoReadModifyWrites_OnOneKey_CommitInSequence()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        IKahuna kahuna = node.Kahuna;
        string key = NewKey();
        await AutoCommitSet(kahuna, key, "0", ct);

        async Task Increment(string name)
        {
            TransactionHandle tx = await Start(kahuna, key + "/" + name, ct);
            int value = int.Parse(ValueOf(await TxGet(kahuna, tx, key, LongWait, ct)));
            Assert.Equal(KeyValueResponseType.Set, await TxSet(kahuna, tx, key, (value + 1).ToString(), ct, KeyValueFlags.Set | KeyValueFlags.AbortWhenDoomed));
            await Commit(kahuna, tx, ct);
        }

        TransactionHandle first = await Start(kahuna, key + "/first", ct);
        Assert.Equal("0", ValueOf(await TxGet(kahuna, first, key, LongWait, ct)));
        Assert.Equal(KeyValueResponseType.Set, await TxSet(kahuna, first, key, "1", ct));

        Task second = Increment("second");
        await AssertStillParked(second);

        await Commit(kahuna, first, ct);
        await second.WaitAsync(TimeSpan.FromSeconds(10), ct);

        TransactionHandle check = await Start(kahuna, key + "/check", ct);
        Assert.Equal("2", ValueOf(await TxGet(kahuna, check, key, KeyValueIntentWait.None, ct)));
        await kahuna.LocateAndRollbackTransaction(check, ct);
    }

    [Fact]
    public async Task WaitingProbe_ParksBehindLiveIntent_AndReadsEveryKeyAfterTheCommit()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        IKahuna kahuna = node.Kahuna;
        string keySpace = NewKey();
        string a = keySpace + "/a";
        string b = keySpace + "/b";
        await AutoCommitSet(kahuna, a, "seed-a", ct);
        await AutoCommitSet(kahuna, b, "seed-b", ct);

        TransactionHandle holder = await Start(kahuna, a + "/holder", ct);
        Assert.Equal(KeyValueResponseType.Set, await TxSet(kahuna, holder, b, "holder-b", ct));

        TransactionHandle reader = await Start(kahuna, a + "/reader", ct);
        Task<KeyValueProbeResult[]> probe = kahuna.LocateAndTryProbeMany(
            reader.TransactionId, HLCTimestamp.Zero, [a, b], KeyValueProbeLock.None, 0, KeyValueDurability.Persistent, ct,
            reader.CoordinatorKey, TransactionOperationId.NewRandom(), intentWait: LongWait);

        await AssertStillParked(probe);
        await Commit(kahuna, holder, ct);

        KeyValueProbeResult[] results = await probe.WaitAsync(TimeSpan.FromSeconds(10), ct);
        Assert.Equal(KeyValueResponseType.Get, results[0].Type);
        Assert.Equal("seed-a", Encoding.UTF8.GetString(results[0].Entry!.Value!));
        Assert.Equal(KeyValueResponseType.Get, results[1].Type);
        Assert.Equal("holder-b", Encoding.UTF8.GetString(results[1].Entry!.Value!));
    }

    [Fact]
    public async Task FlaggedSet_WithAReadPin_RefusedByALiveIntent_IsAbortedAtOnce()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        IKahuna kahuna = node.Kahuna;
        string key = NewKey();
        await AutoCommitSet(kahuna, key, "seed", ct);

        TransactionHandle pinned = await Start(kahuna, key + "/pinned", ct);
        TransactionHandle unpinned = await Start(kahuna, key + "/unpinned", ct);
        TransactionHandle unflagged = await Start(kahuna, key + "/unflagged", ct);
        Assert.Equal("seed", ValueOf(await TxGet(kahuna, pinned, key, KeyValueIntentWait.None, ct)));
        Assert.Equal("seed", ValueOf(await TxGet(kahuna, unflagged, key, KeyValueIntentWait.None, ct)));

        TransactionHandle holder = await Start(kahuna, key + "/holder", ct);
        Assert.Equal(KeyValueResponseType.Set, await TxSet(kahuna, holder, key, "holder", ct));

        const KeyValueFlags Doomed = KeyValueFlags.Set | KeyValueFlags.AbortWhenDoomed;

        Assert.Equal(KeyValueResponseType.Aborted, await TxSet(kahuna, pinned, key, "pinned", ct, Doomed));
        Assert.Equal(KeyValueResponseType.MustRetry, await RawSet(kahuna, unpinned, key, Doomed, ct));
        Assert.Equal(KeyValueResponseType.MustRetry, await RawSet(kahuna, unflagged, key, KeyValueFlags.Set, ct));

        await kahuna.LocateAndRollbackTransaction(holder, ct);
    }

    [Fact]
    public async Task FlaggedSetMany_WithAReadPin_RefusedByALiveIntent_IsAbortedAtOnce()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        IKahuna kahuna = node.Kahuna;
        string keySpace = NewKey();
        string doomed = keySpace + "/doomed";
        string free = keySpace + "/free";
        await AutoCommitSet(kahuna, doomed, "seed", ct);

        TransactionHandle pinned = await Start(kahuna, doomed + "/pinned", ct);
        Assert.Equal("seed", ValueOf(await TxGet(kahuna, pinned, doomed, KeyValueIntentWait.None, ct)));

        TransactionHandle holder = await Start(kahuna, doomed + "/holder", ct);
        Assert.Equal(KeyValueResponseType.Set, await TxSet(kahuna, holder, doomed, "holder", ct));

        using CancellationTokenSource deadline = CancellationTokenSource.CreateLinkedTokenSource(ct);
        deadline.CancelAfter(TimeSpan.FromSeconds(5));

        Stopwatch elapsed = Stopwatch.StartNew();
        List<KahunaSetKeyValueResponseItem> answers = await kahuna.LocateAndTrySetManyKeyValue(
        [
            new KahunaSetKeyValueRequestItem { TransactionId = pinned.TransactionId, Key = free, Value = "x"u8.ToArray(), CompareRevision = -1, Flags = KeyValueFlags.Set | KeyValueFlags.AbortWhenDoomed, Durability = KeyValueDurability.Persistent },
            new KahunaSetKeyValueRequestItem { TransactionId = pinned.TransactionId, Key = doomed, Value = "x"u8.ToArray(), CompareRevision = -1, Flags = KeyValueFlags.Set | KeyValueFlags.AbortWhenDoomed, Durability = KeyValueDurability.Persistent },
        ], deadline.Token, pinned.CoordinatorKey, TransactionOperationId.NewRandom());
        elapsed.Stop();

        Assert.Contains(answers, answer => answer.Key == doomed && answer.Type == KeyValueResponseType.Aborted);
        Assert.True(elapsed.ElapsedMilliseconds < 2_000, $"the doomed batch answered after {elapsed.ElapsedMilliseconds} ms");

        await kahuna.LocateAndRollbackTransaction(holder, ct);
    }

    /// <summary>
    /// One attempt of a transactional set, through the routed write path but with a deadline short enough that the
    /// path's own retry against a live intent ends in the answer it retried on.
    /// </summary>
    private static async Task<KeyValueResponseType> RawSet(IKahuna kahuna, TransactionHandle tx, string key, KeyValueFlags flags, CancellationToken ct)
    {
        using CancellationTokenSource deadline = CancellationTokenSource.CreateLinkedTokenSource(ct);
        deadline.CancelAfter(TimeSpan.FromSeconds(3));

        (KeyValueResponseType type, _, _) = await kahuna.LocateAndTrySetKeyValue(
            tx.TransactionId, key, "x"u8.ToArray(), null, -1, flags, 0,
            KeyValueDurability.Persistent, deadline.Token, coordinatorKey: tx.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
        return type;
    }
}

/// <summary>
/// The wait behind a live foreign write intent on a cluster: a read that a node forwards to the key's leader parks on
/// the leader and is answered when the holder commits, exactly as a read the leader receives itself.
/// </summary>
public sealed class TestIntentWaitReadAcrossNodes : BaseCluster
{
    private readonly ILogger<IRaft> raftLogger;
    private readonly ILogger<IKahuna> kahunaLogger;

    public TestIntentWaitReadAcrossNodes(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = TestLogFactory.Create(outputHelper, quietKommander: true);
        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    [Fact]
    public async Task ReadReceivedByAnyNode_WaitsOnTheLeader_AndPinsTheHoldersCommit()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster("memory", 4, raftLogger, kahunaLogger);

        try
        {
            // Of the three receivers at least two do not lead the key, so the read is forwarded at least twice.
            int receiverIndex = 0;
            foreach (IKahuna receiver in new[] { kahuna1, kahuna2, kahuna3 })
            {
                string key = $"intentwait-{Guid.NewGuid():N}/k{receiverIndex++}";

                await RetryOnMustRetryAsync(async () =>
                {
                    (KeyValueResponseType type, _, _) = await kahuna1.LocateAndTrySetKeyValue(
                        HLCTimestamp.Zero, key, "seed"u8.ToArray(), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
                    return type;
                }, static type => type);

                TransactionHandle holder = await StartTransaction(kahuna1, key + "/holder", ct);
                (KeyValueResponseType staged, _, _) = await kahuna1.LocateAndTrySetKeyValue(
                    holder.TransactionId, key, "holder"u8.ToArray(), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct,
                    coordinatorKey: holder.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
                Assert.Equal(KeyValueResponseType.Set, staged);

                TransactionHandle reader = await StartTransaction(receiver, key + "/reader", ct);
                Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> read = receiver.LocateAndTryGetValue(
                    reader.TransactionId, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct,
                    reader.CoordinatorKey, TransactionOperationId.NewRandom(), intentWait: new KeyValueIntentWait(10_000, HoldsLocks: false)).AsTask();

                await Task.Delay(300, ct);
                Assert.False(read.IsCompleted, "the read answered while the other transaction's write intent was live");

                (KeyValueResponseType committed, _) = await kahuna1.LocateAndCommitTransaction(holder, ct);
                Assert.Equal(KeyValueResponseType.Committed, committed);

                (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await read.WaitAsync(TimeSpan.FromSeconds(10), ct);
                Assert.Equal(KeyValueResponseType.Get, type);
                Assert.Equal("holder", Encoding.UTF8.GetString(entry!.Value!));

                await receiver.LocateAndRollbackTransaction(reader, ct);
            }
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    private static async Task<TransactionHandle> StartTransaction(IKahuna kahuna, string coordinatorKey, CancellationToken ct)
    {
        for (int attempt = 0; ; attempt++)
        {
            (KeyValueResponseType type, TransactionHandle handle) = await kahuna.LocateAndStartTransaction(
                new KeyValueTransactionOptions { CoordinatorKey = coordinatorKey, Timeout = 30_000, Locking = KeyValueTransactionLocking.Optimistic },
                ct);

            if (type == KeyValueResponseType.Set)
                return handle;

            Assert.True(attempt < 50, $"could not start a transaction: {type}");
            await Task.Delay(50, ct);
        }
    }
}
