using System.Collections.Concurrent;
using System.Text;
using Kahuna.Server.Communication.Internode;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// Batched probe through <see cref="IKahuna.LocateAndTryProbeMany"/>, the call an embedding client makes: the
/// answers against single gets, the Shared locks as part of a registered transaction (held until finalize, a
/// replay that never takes a lock, the read set folded for commit validation), and a probe forwarded between nodes.
/// </summary>
public sealed class TestProbeMany : BaseCluster
{
    private readonly ILoggerFactory loggerFactory;

    public TestProbeMany(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    private static async Task<EmbeddedKahunaNode> StartNode(ILoggerFactory loggerFactory, CancellationToken ct)
    {
        EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            TimerInitialDelay = TimeSpan.FromMilliseconds(50),
            ReadIOThreads = 1,
            WriteIOThreads = 1,
            PartitionExecutorPoolSize = 1,
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1
        }, loggerFactory);
        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync("probe/k0", ct);
        return node;
    }

    private static string Space(string name) => $"probe-{name}-{Guid.NewGuid().ToString("N")[..6]}";

    private static async Task<long> Set(IKahuna kahuna, string key, string value, CancellationToken ct)
    {
        (KeyValueResponseType type, long revision, _) = await kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, key, Encoding.UTF8.GetBytes(value), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Set, type);
        return revision;
    }

    private static async Task<TransactionHandle> Begin(IKahuna kahuna, string coordinatorKey, KeyValueTransactionLocking locking, CancellationToken ct)
    {
        (KeyValueResponseType type, TransactionHandle handle) = await kahuna.LocateAndStartTransaction(
            new KeyValueTransactionOptions { CoordinatorKey = coordinatorKey, Locking = locking, Timeout = 10_000 }, ct);
        Assert.Equal(KeyValueResponseType.Set, type);
        return handle;
    }

    private static Task<KeyValueProbeResult[]> Probe(
        IKahuna kahuna, TransactionHandle? tx, string[] keys, KeyValueProbeLock lockMode, CancellationToken ct,
        TransactionOperationId operationId = default, HLCTimestamp readTimestamp = default) =>
        kahuna.LocateAndTryProbeMany(
            tx?.TransactionId ?? HLCTimestamp.Zero, readTimestamp, keys, lockMode, 0, KeyValueDurability.Persistent, ct,
            tx?.CoordinatorKey ?? "", tx is null ? default : (operationId.IsEmpty ? TransactionOperationId.NewRandom() : operationId));

    private static async Task AssertSameAsSingleGets(IKahuna kahuna, string[] keys, KeyValueProbeResult[] probed, HLCTimestamp readTimestamp, CancellationToken ct)
    {
        Assert.Equal(keys.Length, probed.Length);

        for (int i = 0; i < keys.Length; i++)
        {
            (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await kahuna.LocateAndTryGetValue(
                HLCTimestamp.Zero, keys[i], -1, readTimestamp, KeyValueDurability.Persistent, ct);

            Assert.Equal(type, probed[i].Type);
            Assert.Equal(entry?.Revision, probed[i].Entry?.Revision);
            Assert.Equal(entry?.State, probed[i].Entry?.State);
            Assert.Equal(entry?.LastModified, probed[i].Entry?.LastModified);
            Assert.Equal(entry?.Value, probed[i].Entry?.Value);
        }
    }

    [Fact]
    public async Task LatestAndSnapshotProbes_AnswerAsSingleGets()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        IKahuna kahuna = node.Kahuna;

        string space = Space("eq");
        string[] keys = [$"{space}/a", $"{space}/b", $"{space}/c", $"{space}/absent"];

        await Set(kahuna, keys[0], "a1", ct);
        await Set(kahuna, keys[1], "b1", ct);
        await Set(kahuna, keys[2], "c1", ct);
        Assert.Equal(KeyValueResponseType.Deleted, (await kahuna.LocateAndTryDeleteKeyValue(HLCTimestamp.Zero, keys[2], KeyValueDurability.Persistent, ct)).Item1);

        (_, ReadOnlyKeyValueEntry? before) = await kahuna.LocateAndTryGetValue(HLCTimestamp.Zero, keys[1], -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        HLCTimestamp snapshot = before!.LastModified;

        await Set(kahuna, keys[0], "a2", ct);
        await Set(kahuna, keys[1], "b2", ct);

        KeyValueProbeResult[] latest = await Probe(kahuna, null, keys, KeyValueProbeLock.None, ct);
        await AssertSameAsSingleGets(kahuna, keys, latest, HLCTimestamp.Zero, ct);
        Assert.Equal("a2", Encoding.UTF8.GetString(latest[0].Entry!.Value!));
        Assert.Equal(KeyValueResponseType.DoesNotExist, latest[2].Type);

        KeyValueProbeResult[] atSnapshot = await Probe(kahuna, null, keys, KeyValueProbeLock.None, ct, readTimestamp: snapshot);
        await AssertSameAsSingleGets(kahuna, keys, atSnapshot, snapshot, ct);
        Assert.Equal("a1", Encoding.UTF8.GetString(atSnapshot[0].Entry!.Value!));
        Assert.Equal("b1", Encoding.UTF8.GetString(atSnapshot[1].Entry!.Value!));
        Assert.All(latest.Concat(atSnapshot), r => Assert.False(r.Locked));
    }

    [Fact]
    public async Task MixedKeySpaces_AndALockWithoutATransaction_AreRefusedWhole()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        KeyValueProbeResult[] mixed = await Probe(node.Kahuna, null, ["one/a", "two/b"], KeyValueProbeLock.None, ct);
        Assert.All(mixed, r => Assert.Equal(KeyValueResponseType.InvalidInput, r.Type));

        KeyValueProbeResult[] unowned = await Probe(node.Kahuna, null, ["one/a"], KeyValueProbeLock.Shared, ct);
        Assert.All(unowned, r => Assert.Equal(KeyValueResponseType.InvalidInput, r.Type));
    }

    [Fact]
    public async Task SharedProbe_HoldsItsLocksUntilCommit()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        IKahuna kahuna = node.Kahuna;

        string space = Space("hold");
        string[] keys = [$"{space}/a", $"{space}/b", $"{space}/c"];
        await Set(kahuna, keys[1], "b0", ct);

        TransactionHandle reader = await Begin(kahuna, $"{space}-reader", KeyValueTransactionLocking.Pessimistic, ct);
        KeyValueProbeResult[] probed = await Probe(kahuna, reader, keys, KeyValueProbeLock.Shared, ct);

        Assert.All(probed, r => Assert.True(r.Locked));
        Assert.Equal("b0", Encoding.UTF8.GetString(probed[1].Entry!.Value!));

        // A plain write into a locked key is refused while the reader holds the lock.
        (KeyValueResponseType refused, _, _) = await kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, keys[1], Encoding.UTF8.GetBytes("b1"), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
        Assert.NotEqual(KeyValueResponseType.Set, refused);

        (KeyValueResponseType committed, _) = await kahuna.LocateAndCommitTransaction(reader, ct);
        Assert.Equal(KeyValueResponseType.Committed, committed);

        // Finalize released every lock the probe took, through the coordinator's record of them.
        await Set(kahuna, keys[1], "b1", ct);
        await Set(kahuna, keys[0], "a1", ct);
        await Set(kahuna, keys[2], "c1", ct);
    }

    [Fact]
    public async Task SharedProbe_StopsAtADenial_AndAReplayNeverTakesANewLock()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        IKahuna kahuna = node.Kahuna;

        string space = Space("replay");
        string[] keys = [$"{space}/a", $"{space}/b", $"{space}/c"];

        TransactionHandle holder = await Begin(kahuna, $"{space}-holder", KeyValueTransactionLocking.Pessimistic, ct);
        (KeyValueResponseType held, _) = await kahuna.LocateAndTryAcquireRangeLock(
            holder.TransactionId, space, keys[1], true, keys[1], true, 0, KeyValueDurability.Persistent, RangeLockMode.Exclusive, ct,
            holder.CoordinatorKey, TransactionOperationId.NewRandom());
        Assert.Equal(KeyValueResponseType.Locked, held);

        TransactionHandle reader = await Begin(kahuna, $"{space}-reader", KeyValueTransactionLocking.Pessimistic, ct);
        TransactionOperationId operation = TransactionOperationId.NewRandom();

        KeyValueProbeResult[] first = await Probe(kahuna, reader, keys, KeyValueProbeLock.Shared, ct, operation);
        Assert.True(first[0].Locked);
        Assert.Equal(KeyValueResponseType.AlreadyLocked, first[1].Type);
        Assert.Equal(holder.TransactionId, first[1].HolderTransactionId);
        Assert.False(first[1].Locked);
        Assert.Equal(KeyValueResponseType.MustRetry, first[2].Type);
        Assert.False(first[2].Locked);

        // The holder goes away; the same operation replayed must not now lock what the first attempt did not.
        Assert.Equal(KeyValueResponseType.RolledBack, await kahuna.LocateAndRollbackTransaction(holder, ct));

        KeyValueProbeResult[] replay = await Probe(kahuna, reader, keys, KeyValueProbeLock.Shared, ct, operation);
        Assert.True(replay[0].Locked);
        Assert.False(replay[1].Locked);
        Assert.False(replay[2].Locked);

        // Nothing holds key b now: another transaction's exclusive lock on it is granted.
        TransactionHandle other = await Begin(kahuna, $"{space}-other", KeyValueTransactionLocking.Pessimistic, ct);
        (KeyValueResponseType free, _) = await kahuna.LocateAndTryAcquireRangeLock(
            other.TransactionId, space, keys[1], true, keys[1], true, 0, KeyValueDurability.Persistent, RangeLockMode.Exclusive, ct,
            other.CoordinatorKey, TransactionOperationId.NewRandom());
        Assert.Equal(KeyValueResponseType.Locked, free);

        // Key a is still the reader's: the exclusive lock on it is refused.
        (KeyValueResponseType taken, HLCTimestamp takenBy) = await kahuna.LocateAndTryAcquireRangeLock(
            other.TransactionId, space, keys[0], true, keys[0], true, 0, KeyValueDurability.Persistent, RangeLockMode.Exclusive, ct,
            other.CoordinatorKey, TransactionOperationId.NewRandom());
        Assert.Equal(KeyValueResponseType.AlreadyLocked, taken);
        Assert.Equal(reader.TransactionId, takenBy);

        await kahuna.LocateAndRollbackTransaction(other, ct);
        await kahuna.LocateAndRollbackTransaction(reader, ct);
    }

    [Fact]
    public async Task LatestProbe_FoldsItsReadsIntoTheReadSet()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        IKahuna kahuna = node.Kahuna;

        // Control: nothing the reader read changes, so it commits.
        Assert.Equal(KeyValueResponseType.Committed, await ReadThenCommit(kahuna, Space("readset-quiet"), changeUnderneath: false, ct));

        // A key the probe read as absent is created before the commit: the read set no longer holds.
        Assert.NotEqual(KeyValueResponseType.Committed, await ReadThenCommit(kahuna, Space("readset-moved"), changeUnderneath: true, ct));
    }

    private static async Task<KeyValueResponseType> ReadThenCommit(IKahuna kahuna, string space, bool changeUnderneath, CancellationToken ct)
    {
        string[] keys = [$"{space}/a", $"{space}/b"];
        await Set(kahuna, keys[0], "a0", ct);

        TransactionHandle reader = await Begin(kahuna, $"{space}-reader", KeyValueTransactionLocking.Optimistic, ct);
        KeyValueProbeResult[] probed = await Probe(kahuna, reader, keys, KeyValueProbeLock.None, ct);
        Assert.Equal(KeyValueResponseType.Get, probed[0].Type);
        Assert.Equal(KeyValueResponseType.DoesNotExist, probed[1].Type);

        // The reader writes something, so its commit validates what it read.
        (KeyValueResponseType staged, _, _) = await kahuna.LocateAndTrySetKeyValue(
            reader.TransactionId, $"{space}/out", Encoding.UTF8.GetBytes("x"), null, -1, KeyValueFlags.Set, 0,
            KeyValueDurability.Persistent, ct, coordinatorKey: reader.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
        Assert.Equal(KeyValueResponseType.Set, staged);

        if (changeUnderneath)
            await Set(kahuna, keys[1], "b-created", ct);

        (KeyValueResponseType commit, _) = await kahuna.LocateAndCommitTransaction(reader, ct);
        return commit;
    }

    [Fact]
    public async Task ProbeFromAnyNode_TravelsAsOneMessagePerRemoteLeader_AndAnswersAsSingleGets()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        ILogger<IRaft> raftLogger = loggerFactory.CreateLogger<IRaft>();
        ILogger<IKahuna> kahunaLogger = loggerFactory.CreateLogger<IKahuna>();

        (IRaft[] rafts, IKahuna[] kahunas, _, MemoryInterNodeCommmunication interComm) =
            await AssembleClusterWithTransports(3, "memory", 4, raftLogger, kahunaLogger);

        try
        {
            string space = Space("remote");
            string[] keys = [.. Enumerable.Range(0, 24).Select(i => $"{space}/k{i:D2}")];
            for (int i = 0; i < keys.Length; i += 2)
                await Set(kahunas[0], keys[i], $"v{i}", ct);

            ConcurrentBag<(string Node, int Keys)> forwarded = [];
            interComm.ProbeManyForwarded = (node, carried) => forwarded.Add((node, carried.Length));
            int forwardedInAll = 0;

            foreach (IKahuna kahuna in kahunas)
            {
                forwarded.Clear();

                KeyValueProbeResult[] probed = await Probe(kahuna, null, keys, KeyValueProbeLock.None, ct);

                // Each remote leader received one message, and every key went to exactly one place.
                Assert.Equal(forwarded.Count, forwarded.Select(f => f.Node).Distinct().Count());
                Assert.True(forwarded.Sum(f => f.Keys) <= keys.Length);

                for (int i = 0; i < keys.Length; i++)
                {
                    if (i % 2 == 0)
                        Assert.Equal($"v{i}", Encoding.UTF8.GetString(probed[i].Entry!.Value!));
                    else
                        Assert.Equal(KeyValueResponseType.DoesNotExist, probed[i].Type);
                }

                await AssertSameAsSingleGets(kahuna, keys, probed, HLCTimestamp.Zero, ct);
                forwardedInAll += forwarded.Count;
            }

            // At least two of the three nodes lead none of the keys' partitions, so their probes were forwarded.
            Assert.True(forwardedInAll >= 2, $"only {forwardedInAll} probes were forwarded");
        }
        finally
        {
            interComm.ProbeManyForwarded = null;
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }
}
