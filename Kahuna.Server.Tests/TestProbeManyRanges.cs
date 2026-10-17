using System.Collections.Concurrent;
using System.Text;
using Kahuna.Server.Communication.Internode;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Data;
using Kommander.System;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// Batched range probe through <see cref="IKahuna.LocateAndTryProbeManyRanges"/>, the call an embedding client makes:
/// the answers against single range pages, the Shared locks as part of a registered transaction (held until finalize,
/// a replay that never takes a lock, the returned rows folded for commit validation), a probe forwarded between
/// nodes, and seeks over a key space split across partitions.
/// </summary>
public sealed class TestProbeManyRanges : BaseCluster
{
    private readonly ILoggerFactory loggerFactory;

    public TestProbeManyRanges(ITestOutputHelper outputHelper)
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

    private static string Space(string name) => $"seek-{name}-{Guid.NewGuid().ToString("N")[..6]}";

    /// <summary>A seek over every row of group <paramref name="group"/> of <paramref name="space"/>.</summary>
    private static KeyValueProbeRange Group(string space, int group, int limit) =>
        new($"{space}/g{group}-", true, $"{space}/g{group}.", false, limit);

    private static string Row(string space, int group, int row) => $"{space}/g{group}-{row:D2}";

    private static async Task Set(IKahuna kahuna, string key, string value, CancellationToken ct)
    {
        (KeyValueResponseType type, _, _) = await kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, key, Encoding.UTF8.GetBytes(value), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Set, type);
    }

    private static async Task SeedGroups(IKahuna kahuna, string space, int groups, int rows, CancellationToken ct)
    {
        for (int g = 0; g < groups; g++)
            for (int r = 0; r < rows; r++)
                await Set(kahuna, Row(space, g, r), $"v{g}-{r}", ct);
    }

    private static async Task<TransactionHandle> Begin(IKahuna kahuna, string coordinatorKey, KeyValueTransactionLocking locking, CancellationToken ct)
    {
        (KeyValueResponseType type, TransactionHandle handle) = await kahuna.LocateAndStartTransaction(
            new KeyValueTransactionOptions { CoordinatorKey = coordinatorKey, Locking = locking, Timeout = 10_000 }, ct);
        Assert.Equal(KeyValueResponseType.Set, type);
        return handle;
    }

    private static Task<KeyValueProbeRangeResult[]> Probe(
        IKahuna kahuna, TransactionHandle? tx, string space, KeyValueProbeRange[] seeks, KeyValueProbeLock lockMode, CancellationToken ct,
        TransactionOperationId operationId = default, HLCTimestamp readTimestamp = default) =>
        kahuna.LocateAndTryProbeManyRanges(
            tx?.TransactionId ?? HLCTimestamp.Zero, readTimestamp, space, seeks, lockMode, 0, KeyValueDurability.Persistent, ct,
            tx?.CoordinatorKey ?? "", tx is null ? default : (operationId.IsEmpty ? TransactionOperationId.NewRandom() : operationId));

    private static async Task AssertSameAsSinglePages(
        IKahuna kahuna, string space, KeyValueProbeRange[] seeks, KeyValueProbeRangeResult[] probed, HLCTimestamp readTimestamp, CancellationToken ct)
    {
        Assert.Equal(seeks.Length, probed.Length);

        for (int i = 0; i < seeks.Length; i++)
        {
            KeyValueProbeRange seek = seeks[i];
            KeyValueGetByRangeResult single = await kahuna.LocateAndGetByRange(
                HLCTimestamp.Zero, space, seek.StartKey, seek.StartInclusive, seek.EndKey, seek.EndInclusive, seek.Limit,
                readTimestamp, KeyValueDurability.Persistent, ct);

            Assert.Equal(single.Type, probed[i].Type);
            Assert.Equal(single.HasMore, probed[i].HasMore);
            Assert.Equal(single.Items.Select(x => x.Item1), probed[i].Items.Select(x => x.Item1));

            for (int j = 0; j < single.Items.Count; j++)
            {
                Assert.Equal(single.Items[j].Item2.Revision, probed[i].Items[j].Item2.Revision);
                Assert.Equal(single.Items[j].Item2.LastModified, probed[i].Items[j].Item2.LastModified);
                Assert.Equal(single.Items[j].Item2.Value, probed[i].Items[j].Item2.Value);
            }
        }
    }

    [Fact]
    public async Task LatestAndSnapshotProbes_AnswerAsSinglePages()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        IKahuna kahuna = node.Kahuna;

        string space = Space("eq");
        await SeedGroups(kahuna, space, 3, 5, ct);
        Assert.Equal(KeyValueResponseType.Deleted,
            (await kahuna.LocateAndTryDeleteKeyValue(HLCTimestamp.Zero, Row(space, 1, 2), KeyValueDurability.Persistent, ct)).Item1);

        (_, ReadOnlyKeyValueEntry? before) = await kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, Row(space, 2, 4), -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        HLCTimestamp snapshot = before!.LastModified;

        await Set(kahuna, Row(space, 0, 0), "changed", ct);
        await Set(kahuna, Row(space, 2, 9), "added", ct);

        KeyValueProbeRange[] seeks = [Group(space, 0, 3), Group(space, 1, 10), Group(space, 2, 10), Group(space, 7, 10)];

        KeyValueProbeRangeResult[] latest = await Probe(kahuna, null, space, seeks, KeyValueProbeLock.None, ct);
        await AssertSameAsSinglePages(kahuna, space, seeks, latest, HLCTimestamp.Zero, ct);
        Assert.Equal("changed", Encoding.UTF8.GetString(latest[0].Items[0].Item2.Value!));
        Assert.True(latest[0].HasMore);
        Assert.Equal(Row(space, 0, 2), latest[0].ResumeKey);
        Assert.Equal(4, latest[1].Items.Count);
        Assert.Equal(6, latest[2].Items.Count);
        Assert.Empty(latest[3].Items);

        KeyValueProbeRangeResult[] atSnapshot = await Probe(kahuna, null, space, seeks, KeyValueProbeLock.None, ct, readTimestamp: snapshot);
        await AssertSameAsSinglePages(kahuna, space, seeks, atSnapshot, snapshot, ct);
        Assert.Equal("v0-0", Encoding.UTF8.GetString(atSnapshot[0].Items[0].Item2.Value!));
        Assert.Equal(5, atSnapshot[1].Items.Count);
        Assert.Equal(5, atSnapshot[2].Items.Count);
        Assert.All(latest.Concat(atSnapshot), r => Assert.False(r.Locked));
    }

    [Fact]
    public async Task MalformedProbes_AreRefusedWhole()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        string space = Space("bad");

        KeyValueProbeRangeResult[] emptyLimit = await Probe(node.Kahuna, null, space, [Group(space, 0, 0)], KeyValueProbeLock.None, ct);
        Assert.All(emptyLimit, r => Assert.Equal(KeyValueResponseType.InvalidInput, r.Type));

        KeyValueProbeRangeResult[] unowned = await Probe(node.Kahuna, null, space, [Group(space, 0, 1)], KeyValueProbeLock.Shared, ct);
        Assert.All(unowned, r => Assert.Equal(KeyValueResponseType.InvalidInput, r.Type));

        KeyValueProbeRangeResult[] noPrefix = await Probe(node.Kahuna, null, "", [Group(space, 0, 1)], KeyValueProbeLock.None, ct);
        Assert.All(noPrefix, r => Assert.Equal(KeyValueResponseType.InvalidInput, r.Type));
    }

    [Fact]
    public async Task SharedProbe_HoldsItsLocksUntilCommit()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        IKahuna kahuna = node.Kahuna;

        string space = Space("hold");
        await SeedGroups(kahuna, space, 2, 3, ct);

        TransactionHandle reader = await Begin(kahuna, $"{space}-reader", KeyValueTransactionLocking.Pessimistic, ct);
        KeyValueProbeRangeResult[] probed = await Probe(kahuna, reader, space, [Group(space, 0, 10), Group(space, 1, 10)], KeyValueProbeLock.Shared, ct);

        Assert.All(probed, r =>
        {
            Assert.True(r.Locked);
            Assert.Equal(3, r.Items.Count);
        });

        // A plain write into a locked seek is refused while the reader holds the lock: an existing row and a new one.
        (KeyValueResponseType refused, _, _) = await kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, Row(space, 0, 1), Encoding.UTF8.GetBytes("x"), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
        Assert.NotEqual(KeyValueResponseType.Set, refused);

        (KeyValueResponseType phantom, _, _) = await kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, Row(space, 1, 50), Encoding.UTF8.GetBytes("x"), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
        Assert.NotEqual(KeyValueResponseType.Set, phantom);

        (KeyValueResponseType committed, _) = await kahuna.LocateAndCommitTransaction(reader, ct);
        Assert.Equal(KeyValueResponseType.Committed, committed);

        // Finalize released every lock the probe took, through the coordinator's record of them.
        await Set(kahuna, Row(space, 0, 1), "after", ct);
        await Set(kahuna, Row(space, 1, 50), "after", ct);
    }

    [Fact]
    public async Task SharedProbe_StopsAtADenial_AndAReplayNeverTakesANewLock()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        IKahuna kahuna = node.Kahuna;

        string space = Space("replay");
        KeyValueProbeRange[] seeks = [Group(space, 0, 5), Group(space, 1, 5), Group(space, 2, 5)];

        TransactionHandle holder = await Begin(kahuna, $"{space}-holder", KeyValueTransactionLocking.Pessimistic, ct);
        (KeyValueResponseType held, _) = await kahuna.LocateAndTryAcquireRangeLock(
            holder.TransactionId, space, Row(space, 1, 3), true, Row(space, 1, 3), true, 0, KeyValueDurability.Persistent, RangeLockMode.Exclusive, ct,
            holder.CoordinatorKey, TransactionOperationId.NewRandom());
        Assert.Equal(KeyValueResponseType.Locked, held);

        TransactionHandle reader = await Begin(kahuna, $"{space}-reader", KeyValueTransactionLocking.Pessimistic, ct);
        TransactionOperationId operation = TransactionOperationId.NewRandom();

        KeyValueProbeRangeResult[] first = await Probe(kahuna, reader, space, seeks, KeyValueProbeLock.Shared, ct, operation);
        Assert.True(first[0].Locked);
        Assert.Equal(KeyValueResponseType.AlreadyLocked, first[1].Type);
        Assert.Equal(holder.TransactionId, first[1].HolderTransactionId);
        Assert.False(first[1].Locked);
        Assert.Equal(KeyValueResponseType.MustRetry, first[2].Type);
        Assert.False(first[2].Locked);

        // The holder goes away; the same operation replayed must not now lock what the first attempt did not.
        Assert.Equal(KeyValueResponseType.RolledBack, await kahuna.LocateAndRollbackTransaction(holder, ct));

        KeyValueProbeRangeResult[] replay = await Probe(kahuna, reader, space, seeks, KeyValueProbeLock.Shared, ct, operation);
        Assert.True(replay[0].Locked);
        Assert.False(replay[1].Locked);
        Assert.False(replay[2].Locked);

        // Nothing holds seek 1 now: another transaction's exclusive lock over it is granted.
        TransactionHandle other = await Begin(kahuna, $"{space}-other", KeyValueTransactionLocking.Pessimistic, ct);
        (KeyValueResponseType free, _) = await kahuna.LocateAndTryAcquireRangeLock(
            other.TransactionId, space, seeks[1].StartKey, true, seeks[1].EndKey, false, 0, KeyValueDurability.Persistent, RangeLockMode.Exclusive, ct,
            other.CoordinatorKey, TransactionOperationId.NewRandom());
        Assert.Equal(KeyValueResponseType.Locked, free);

        // Seek 0 is still the reader's: the exclusive lock over it is refused.
        (KeyValueResponseType taken, HLCTimestamp takenBy) = await kahuna.LocateAndTryAcquireRangeLock(
            other.TransactionId, space, seeks[0].StartKey, true, seeks[0].EndKey, false, 0, KeyValueDurability.Persistent, RangeLockMode.Exclusive, ct,
            other.CoordinatorKey, TransactionOperationId.NewRandom());
        Assert.Equal(KeyValueResponseType.AlreadyLocked, taken);
        Assert.Equal(reader.TransactionId, takenBy);

        await kahuna.LocateAndRollbackTransaction(other, ct);
        await kahuna.LocateAndRollbackTransaction(reader, ct);
    }

    [Fact]
    public async Task LatestProbe_FoldsItsRowsIntoTheReadSet()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        IKahuna kahuna = node.Kahuna;

        // Control: nothing the reader read changes, so it commits.
        Assert.Equal(KeyValueResponseType.Committed, await ReadThenCommit(kahuna, Space("readset-quiet"), changeUnderneath: false, ct));

        // A row the probe returned changes before the commit: the read set no longer holds.
        Assert.NotEqual(KeyValueResponseType.Committed, await ReadThenCommit(kahuna, Space("readset-moved"), changeUnderneath: true, ct));
    }

    private static async Task<KeyValueResponseType> ReadThenCommit(IKahuna kahuna, string space, bool changeUnderneath, CancellationToken ct)
    {
        await SeedGroups(kahuna, space, 2, 2, ct);

        TransactionHandle reader = await Begin(kahuna, $"{space}-reader", KeyValueTransactionLocking.Optimistic, ct);
        KeyValueProbeRangeResult[] probed = await Probe(kahuna, reader, space, [Group(space, 0, 5), Group(space, 1, 5)], KeyValueProbeLock.None, ct);
        Assert.All(probed, r => Assert.Equal(2, r.Items.Count));

        // The reader writes something, so its commit validates what it read.
        (KeyValueResponseType staged, _, _) = await kahuna.LocateAndTrySetKeyValue(
            reader.TransactionId, $"{space}/out", Encoding.UTF8.GetBytes("x"), null, -1, KeyValueFlags.Set, 0,
            KeyValueDurability.Persistent, ct, coordinatorKey: reader.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
        Assert.Equal(KeyValueResponseType.Set, staged);

        if (changeUnderneath)
            await Set(kahuna, Row(space, 1, 1), "changed", ct);

        (KeyValueResponseType commit, _) = await kahuna.LocateAndCommitTransaction(reader, ct);
        return commit;
    }

    [Fact]
    public async Task ProbeFromAnyNode_TravelsAsOneMessagePerRemoteLeader_AndAnswersAsSinglePages()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        ILogger<IRaft> raftLogger = loggerFactory.CreateLogger<IRaft>();
        ILogger<IKahuna> kahunaLogger = loggerFactory.CreateLogger<IKahuna>();

        (IRaft[] rafts, IKahuna[] kahunas, _, MemoryInterNodeCommmunication interComm) =
            await AssembleClusterWithTransports(3, "memory", 4, raftLogger, kahunaLogger);

        try
        {
            string space = Space("remote");
            await SeedGroups(kahunas[0], space, 6, 4, ct);

            KeyValueProbeRange[] seeks = [.. Enumerable.Range(0, 8).Select(g => Group(space, g, 3))];

            ConcurrentBag<(string Node, int Seeks)> forwarded = [];
            interComm.ProbeManyRangesForwarded = (node, carried) => forwarded.Add((node, carried.Length));
            int forwardedInAll = 0;

            foreach (IKahuna kahuna in kahunas)
            {
                forwarded.Clear();

                KeyValueProbeRangeResult[] probed = await Probe(kahuna, null, space, seeks, KeyValueProbeLock.None, ct);

                // The prefix hashes to one partition, so the whole probe went to one place, in one message.
                Assert.True(forwarded.Count <= 1);
                Assert.All(forwarded, f => Assert.Equal(seeks.Length, f.Seeks));

                for (int g = 0; g < 6; g++)
                {
                    Assert.Equal(3, probed[g].Items.Count);
                    Assert.True(probed[g].HasMore);
                }
                Assert.Empty(probed[6].Items);
                Assert.Empty(probed[7].Items);

                await AssertSameAsSinglePages(kahuna, space, seeks, probed, HLCTimestamp.Zero, ct);
                forwardedInAll += forwarded.Count;
            }

            // Two of the three nodes do not lead the prefix's partition, so their probes were forwarded.
            Assert.Equal(2, forwardedInAll);
        }
        finally
        {
            interComm.ProbeManyRangesForwarded = null;
            await LeaveCluster(rafts[0], rafts[1], rafts[2]);
        }
    }

    [Fact]
    public async Task SplitKeySpace_GroupsSeeksByPartition_AndASpanningSeekAnswersAsTheSinglePaths()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        ILogger<IRaft> raftLogger = loggerFactory.CreateLogger<IRaft>();
        ILogger<IKahuna> kahunaLogger = loggerFactory.CreateLogger<IKahuna>();

        (IRaft r1, IRaft r2, IRaft r3, IKahuna k1, IKahuna k2, IKahuna k3) =
            await AssembleThreNodeCluster("memory", 3, raftLogger, kahunaLogger);

        (IRaft, KahunaManager)[] nodes = [(r1, (KahunaManager)k1), (r2, (KahunaManager)k2), (r3, (KahunaManager)k3)];

        try
        {
            const string space = "prb:r";

            foreach ((IRaft _, KahunaManager kahuna) in nodes)
                kahuna.RegisterKeyRange(space);

            (IRaft _, KahunaManager metaLeader) = await LeaderOf(RangeMapStore.MetaPartitionId, nodes);
            Assert.True(await metaLeader.RangeMapStore.MutateAsync(
                _ => [new RangeDescriptor
                {
                    KeySpace = space, StartKey = null, EndKey = null,
                    PartitionId = RangeMapStore.FirstDataPartitionId, Generation = 1
                }], ct));

            foreach ((IRaft _, KahunaManager kahuna) in nodes)
                await WaitUntilAsync(() => kahuna.RangeMapStore.Current.Find(space, space + "/x") is not null);

            for (int i = 0; i < 20; i++)
                await Set(k1, $"{space}/{i:D4}", $"v{i}", ct);

            // Split at /0010: rows 0-9 stay, rows 10-19 move to a new partition.
            (IRaft systemLeader, KahunaManager _) = await LeaderOf(0, nodes);
            int newId = RangeSplitter.ComputeNextPartitionId(systemLeader, metaLeader.RangeMapStore.Current);
            Assert.True((await systemLeader.CreatePartitionAsync(newId, RaftRoutingMode.Unrouted, null, ct)).Success);
            SplitOutcome outcome = await metaLeader.RangeSplitter.SplitAsync(space, $"{space}/0010", newId, ct);
            Assert.True(outcome.IsSuccess, $"Split failed: {outcome.Status}");

            foreach ((IRaft _, KahunaManager kahuna) in nodes)
            {
                KahunaManager each = kahuna;
                await WaitUntilAsync(() =>
                {
                    RangeMap map = each.RangeMapStore.Current;
                    return map.Find(space, space + "/0005")?.PartitionId != map.Find(space, space + "/0015")?.PartitionId;
                });
                await WaitUntilAsync(async () =>
                    (await each.LocateAndGetByRange(HLCTimestamp.Zero, space, null, true, null, false, 50, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct)).Type
                        == KeyValueResponseType.Get, timeoutMs: 30_000);
            }

            KeyValueProbeRange left = new($"{space}/0002", true, $"{space}/0005", true, 10);
            KeyValueProbeRange right = new($"{space}/0013", false, $"{space}/0017", false, 10);
            KeyValueProbeRange spanning = new($"{space}/0008", true, $"{space}/0012", true, 10);
            KeyValueProbeRange[] seeks = [left, right, spanning];

            foreach (IKahuna kahuna in new IKahuna[] { k1, k2, k3 })
            {
                KeyValueProbeRangeResult[] probed = await Probe(kahuna, null, space, seeks, KeyValueProbeLock.None, ct);
                await AssertSameAsSinglePages(kahuna, space, seeks, probed, HLCTimestamp.Zero, ct);

                Assert.Equal(4, probed[0].Items.Count);
                Assert.Equal(3, probed[1].Items.Count);
                Assert.Equal([$"{space}/0008", $"{space}/0009", $"{space}/0010", $"{space}/0011", $"{space}/0012"], probed[2].Items.Select(x => x.Item1));
            }

            // Locked: every seek holds its lock, the spanning one over both partitions, and a foreign writer into any of
            // them is refused until the reader finishes.
            TransactionHandle reader = await Begin(k2, $"{space}-reader", KeyValueTransactionLocking.Pessimistic, ct);
            KeyValueProbeRangeResult[] locked = await Probe(k2, reader, space, seeks, KeyValueProbeLock.Shared, ct);
            Assert.All(locked, r => Assert.True(r.Locked));
            Assert.Equal(5, locked[2].Items.Count);

            foreach (string row in new[] { $"{space}/0003", $"{space}/0014", $"{space}/0009", $"{space}/0011" })
            {
                (KeyValueResponseType refused, _, _) = await k3.LocateAndTrySetKeyValue(
                    HLCTimestamp.Zero, row, Encoding.UTF8.GetBytes("x"), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
                Assert.NotEqual(KeyValueResponseType.Set, refused);
            }

            (KeyValueResponseType committed, _) = await k2.LocateAndCommitTransaction(reader, ct);
            Assert.Equal(KeyValueResponseType.Committed, committed);

            foreach (string row in new[] { $"{space}/0003", $"{space}/0014", $"{space}/0009", $"{space}/0011" })
                await Set(k3, row, "after", ct);
        }
        finally
        {
            await LeaveCluster(r1, r2, r3);
        }
    }

    private static async Task<(IRaft, KahunaManager)> LeaderOf(int partition, (IRaft Raft, KahunaManager Kahuna)[] nodes)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        while (true)
        {
            foreach ((IRaft raft, KahunaManager kahuna) in nodes)
                if (await raft.AmILeader(partition, ct))
                    return (raft, kahuna);
            await Task.Delay(50, ct);
        }
    }
}
