using System.Text;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Replication;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Data;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// The interleave of <see cref="TestRangeMoveCatchUpInterleave"/> on a live three-node cluster. One node is behind
/// on the log of the source partition while it applies the log of the destination that a split created: it holds
/// the prepare of a source transaction of a moved key and not yet its settle. The other two nodes applied the
/// settle before the cutover, so the key's next transaction, a one-phase bundle on the destination, found the key
/// free there.
///
/// <para>The node that is behind must admit that bundle too. A node that refused it kept the row of every key of
/// the bundle one commit behind, and served that row when it led the destination next (CamusDB
/// bank-split-kill-window, 2026-11-22).</para>
/// </summary>
public sealed class TestRangeMoveCatchUpInterleaveCluster : BaseCluster
{
    private const string Space = "t:il";

    private const string SplitKey = Space + "/m";

    private static readonly string[] KeysBelow = [Space + "/a", Space + "/b"];

    private static readonly string[] KeysAbove = [Space + "/p", Space + "/q"];

    private const string MovedKey = Space + "/p";

    private const int SourcePartition = RangeMapStore.FirstDataPartitionId;

    private readonly ILogger<IRaft> raftLogger;

    private readonly ILogger<IKahuna> kahunaLogger;

    public TestRangeMoveCatchUpInterleaveCluster(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = TestLogFactory.Create(outputHelper);
        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    /// <summary>
    /// Keeps entries of a node's partition logs aside and applies them in their order on release. For each
    /// partition the node keeps every entry from the first one that the partition's condition accepts. The node
    /// acknowledges a kept entry, so its replication goes on: it is a node whose apply of that log is behind.
    /// </summary>
    private sealed class HeldLogs
    {
        private sealed class Held(Func<RaftLog, bool> firstHeld)
        {
            public readonly Func<RaftLog, bool> FirstHeld = firstHeld;

            public readonly Queue<RaftLog> Entries = new();

            public bool Holding;
        }

        private readonly KahunaManager node;

        private readonly object gate = new();

        private readonly Dictionary<int, Held> partitions = [];

        private RaftLog? replaying;

        public HeldLogs(KahunaManager node)
        {
            this.node = node;

            node.KeyValues.ReplicationDispatcher.ApplySkipForTesting = (partitionId, log) =>
            {
                lock (gate)
                {
                    if (ReferenceEquals(log, replaying) || !partitions.TryGetValue(partitionId, out Held? held))
                        return false;

                    if (!held.Holding && !held.FirstHeld(log))
                        return false;

                    held.Holding = true;
                    held.Entries.Enqueue(log);
                    return true;
                }
            };
        }

        public void Hold(int partition, Func<RaftLog, bool> firstHeld)
        {
            lock (gate)
                partitions[partition] = new Held(firstHeld);
        }

        public int Count(int partition)
        {
            lock (gate)
                return partitions.TryGetValue(partition, out Held? held) ? held.Entries.Count : 0;
        }

        /// <summary>Applies the kept entries of one partition in their order. An entry that arrives during the
        /// release is kept behind them, so the order of the log holds.</summary>
        public async Task ReleaseAsync(int partition)
        {
            while (true)
            {
                RaftLog log;

                lock (gate)
                {
                    if (!partitions.TryGetValue(partition, out Held? held) || !held.Entries.TryDequeue(out log!))
                    {
                        partitions.Remove(partition);
                        replaying = null;
                        return;
                    }

                    replaying = log;
                }

                Assert.True(await node.KeyValues.ReplicationDispatcher.OnReplicationReceived(partition, log));
            }
        }
    }

    private static async Task<int> LeaderOf(int partition, (IRaft Raft, KahunaManager Kahuna)[] nodes, CancellationToken ct)
    {
        while (true)
        {
            for (int i = 0; i < nodes.Length; i++)
            {
                if (await nodes[i].Raft.AmILeaderIfHosted(partition, ct))
                    return i;
            }

            await Task.Delay(50, ct);
        }
    }

    private static async Task CommitThroughScript(IKahuna kahuna, string key, string value)
    {
        KeyValueTransactionResult result = await RetryOnMustRetry(
            kahuna, Encoding.UTF8.GetBytes($"BEGIN SET `{key}` '{value}' COMMIT END"), null, null);
        Assert.Equal(KeyValueResponseType.Set, result.Type);
    }

    private static async Task<string?> ReadThrough(KahunaManager kahuna, string key, CancellationToken ct)
    {
        (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await RetryOnMustRetryAsync(
            () => kahuna.LocateAndTryGetValue(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct).AsTask(),
            r => r.Item1, timeoutMs: 30_000);

        Assert.Equal(KeyValueResponseType.Get, type);
        return entry?.Value is null ? null : Encoding.UTF8.GetString(entry.Value);
    }

    private static long HeadOf(KahunaManager kahuna, int partition, string key) =>
        kahuna.DurablePreparedIntentStore.TryGetLedgerHead(partition, key, out long revision, out _, out _) ? revision : -1;

    /// <summary>Whether a prepared-intent entry carries the settle of an intent of <paramref name="key"/>.</summary>
    private static bool SettlesKey(RaftLog log, string key)
    {
        if (log.LogType != ReplicationTypes.PreparedIntent || log.LogData is null)
            return false;

        foreach (PreparedIntentCommand command in PreparedIntentStore.DecodeDelta(log.LogData))
        {
            if (command is ResolveIntentCommand resolve && resolve.Key == key)
                return true;
        }

        return false;
    }

    private static async Task<int> SplitAsync((IRaft Raft, KahunaManager Kahuna)[] nodes, CancellationToken ct)
    {
        for (int attempt = 0; attempt < 5; attempt++)
        {
            int meta = await LeaderOf(RangeMapStore.MetaPartitionId, nodes, ct);
            int newPartition = RangeSplitter.ComputeNextPartitionId(nodes[meta].Raft, nodes[meta].Kahuna.RangeMapStore.Current);

            SplitOutcome outcome = await nodes[meta].Kahuna.ForceSplitAtKeyAsync(Space, SplitKey, duringQuiesce: null, ct);

            if (outcome.IsSuccess)
            {
                foreach ((IRaft _, KahunaManager kahuna) in nodes)
                    await WaitUntilAsync(() => kahuna.RangeMapStore.Current.Find(Space, MovedKey)?.PartitionId == newPartition, timeoutMs: 30_000);

                return newPartition;
            }

            if (outcome.Status is not (SplitStatus.CutoverFailed or SplitStatus.ProbeIndeterminate or SplitStatus.UnsettledMovingIntents))
                Assert.Fail($"Split failed: {outcome.Status}");

            await Task.Delay(100, ct);
        }

        Assert.Fail("Split did not succeed within five attempts");
        return 0;
    }

    private const string StayedKey = Space + "/b";

    private static async Task CommitBothThroughScript(IKahuna kahuna, string first, string second, string value)
    {
        KeyValueTransactionResult result = await RetryOnMustRetry(
            kahuna, Encoding.UTF8.GetBytes($"BEGIN SET `{first}` '{value}' SET `{second}` '{value}' COMMIT END"), null, null);
        Assert.Equal(KeyValueResponseType.Set, result.Type);
    }

    private static async Task MoveLeadershipAwayFrom(
        (IRaft Raft, KahunaManager Kahuna)[] nodes, int partition, int from, int to, CancellationToken ct)
    {
        if (await LeaderOf(partition, nodes, ct) != from)
            return;

        Assert.Equal(RaftOperationStatus.Success, await nodes[from].Raft.TransferLeadershipAsync(partition, nodes[to].Raft.GetLocalEndpoint(), ct));
        await WaitUntilAsync(async () => await nodes[to].Raft.AmILeaderIfHosted(partition, ct), timeoutMs: 30_000);
        await WaitUntilAsync(async () => !await nodes[from].Raft.AmILeaderIfHosted(partition, ct), timeoutMs: 30_000);
    }

    /// <summary>
    /// The opposite order. The node kept the whole of a source transaction aside: a bundle that wrote a key that
    /// moves and a key that stays. The destination's log is ahead on it, and an intent of the destination holds
    /// the moved key when the source bundle applies at last. Every other node applied that bundle before the
    /// cutover. The node must admit it too. A node that refused the source prepare of the moved key refused the
    /// bundle in the one-phase form, and its source slice never recorded the head of the transaction for that key.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task NodeAheadOnTheDestinationLog_AdmitsTheSourceBundleOfAMovedKey(bool onePhase)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleCluster(
            3, "memory", 4, raftLogger, kahunaLogger, configureKahuna: config =>
            {
                config.OnePhaseApplyTimeValidation = onePhase;
                config.DurableMaterializeOnResolve = onePhase;
            });

        (IRaft Raft, KahunaManager Kahuna)[] nodes = [.. rafts.Zip(kahunas, (raft, kahuna) => (raft, (KahunaManager)kahuna))];

        try
        {
            foreach ((IRaft _, KahunaManager kahuna) in nodes)
                kahuna.RegisterKeyRange(Space);

            int metaLeader = await LeaderOf(RangeMapStore.MetaPartitionId, nodes, ct);

            Assert.True(await nodes[metaLeader].Kahuna.RangeMapStore.MutateAsync(
                _ => [new RangeDescriptor { KeySpace = Space, StartKey = null, EndKey = null, PartitionId = SourcePartition, Generation = 1 }],
                ct));

            foreach ((IRaft _, KahunaManager kahuna) in nodes)
                await WaitUntilAsync(() => kahuna.RangeMapStore.Current.Find(Space, Space + "/x")?.Generation == 1, timeoutMs: 30_000);

            foreach (string key in KeysBelow.Concat(KeysAbove))
            {
                await CommitThroughScript(nodes[0].Kahuna, key, "one");

                foreach ((IRaft _, KahunaManager kahuna) in nodes)
                    await WaitUntilAsync(() => HeadOf(kahuna, SourcePartition, key) >= 0, timeoutMs: 30_000);
            }

            long movedHead = HeadOf(nodes[0].Kahuna, SourcePartition, MovedKey);
            long stayedHead = HeadOf(nodes[0].Kahuna, SourcePartition, StayedKey);

            int sourceLeader = await LeaderOf(SourcePartition, nodes, ct);
            metaLeader = await LeaderOf(RangeMapStore.MetaPartitionId, nodes, ct);
            int behind = Enumerable.Range(0, 3).First(i => i != sourceLeader && i != metaLeader);
            int[] current = [.. Enumerable.Range(0, 3).Where(i => i != behind)];

            // The node keeps the source log aside from here on: the whole of the next source transaction.
            HeldLogs heldLogs = new(nodes[behind].Kahuna);
            heldLogs.Hold(SourcePartition, static _ => true);

            await CommitBothThroughScript(nodes[current[0]].Kahuna, MovedKey, StayedKey, "two");

            foreach (int i in current)
            {
                await WaitUntilAsync(() => HeadOf(nodes[i].Kahuna, SourcePartition, MovedKey) == movedHead + 1, timeoutMs: 30_000);
                await WaitUntilAsync(() => HeadOf(nodes[i].Kahuna, SourcePartition, StayedKey) == stayedHead + 1, timeoutMs: 30_000);
            }

            Assert.Null(nodes[behind].Kahuna.DurablePreparedIntentStore.Get(MovedKey));
            Assert.Equal(stayedHead, HeadOf(nodes[behind].Kahuna, SourcePartition, StayedKey));

            int destination = await SplitAsync(nodes, ct);

            foreach ((IRaft _, KahunaManager kahuna) in nodes)
                await WaitUntilAsync(() => HeadOf(kahuna, destination, MovedKey) == movedHead + 1, timeoutMs: 30_000);

            await MoveLeadershipAwayFrom(nodes, destination, behind, current[0], ct);

            // The destination commits the next transaction of the moved key, and the node keeps its settle aside:
            // an intent of the destination holds the key on the node when the source bundle applies.
            heldLogs.Hold(destination, log => SettlesKey(log, MovedKey));

            await CommitThroughScript(nodes[current[0]].Kahuna, MovedKey, "three");

            foreach (int i in current)
                await WaitUntilAsync(() => HeadOf(nodes[i].Kahuna, destination, MovedKey) == movedHead + 2, timeoutMs: 30_000);

            await WaitUntilAsync(() => heldLogs.Count(destination) > 0, timeoutMs: 30_000);

            PreparedIntent? destinationLeg = nodes[behind].Kahuna.DurablePreparedIntentStore.Get(MovedKey);
            Assert.NotNull(destinationLeg);
            Assert.Equal(movedHead + 2, destinationLeg.Revision);

            // The source log catches up while the destination's intent holds the moved key.
            await heldLogs.ReleaseAsync(SourcePartition);

            // The source bundle applied on the node as on its peers: both heads of the source slice moved, the
            // record of no transaction of either key holds a refused bundle, and the node still shows the
            // destination's intent.
            await WaitUntilAsync(() => HeadOf(nodes[behind].Kahuna, SourcePartition, StayedKey) == stayedHead + 1, timeoutMs: 30_000);
            await WaitUntilAsync(() => HeadOf(nodes[behind].Kahuna, SourcePartition, MovedKey) == movedHead + 1, timeoutMs: 30_000);

            foreach (TransactionRecord record in nodes[behind].Kahuna.DurableTransactionRecordStore.EnumerateRecords())
            {
                if (record.RecordAnchorKey == MovedKey || record.RecordAnchorKey == StayedKey)
                {
                    Assert.True(record.RejectedBundledCommitOpIds is not { Count: > 0 }, $"the node refused the bundle of transaction {record.TransactionId}");
                    Assert.Equal(TransactionDecision.Commit, record.Decision);
                }
            }

            Assert.Equal(destinationLeg.TransactionId, nodes[behind].Kahuna.DurablePreparedIntentStore.Get(MovedKey)!.TransactionId);
            Assert.Equal(0, nodes[behind].Kahuna.DurablePreparedIntentStore.CoveredIntentCount);
            Assert.False(nodes[behind].Kahuna.KeyValues.DivergenceContainment.AnyGated);

            // The destination's settle arrives, and the node holds the state of its peers for both keys.
            await heldLogs.ReleaseAsync(destination);

            await WaitUntilAsync(() => nodes[behind].Kahuna.DurablePreparedIntentStore.Get(MovedKey) is null, timeoutMs: 30_000);
            Assert.Equal(movedHead + 2, HeadOf(nodes[behind].Kahuna, destination, MovedKey));

            // The node leads the source next and serves the key that stayed from its own state.
            int sourceLeaderNow = await LeaderOf(SourcePartition, nodes, ct);
            if (sourceLeaderNow != behind)
            {
                Assert.Equal(RaftOperationStatus.Success, await nodes[sourceLeaderNow].Raft.TransferLeadershipAsync(SourcePartition, nodes[behind].Raft.GetLocalEndpoint(), ct));
                await WaitUntilAsync(async () => await nodes[behind].Raft.AmILeaderIfHosted(SourcePartition, ct), timeoutMs: 30_000);
            }

            foreach ((IRaft _, KahunaManager kahuna) in nodes)
            {
                Assert.Equal("two", await ReadThrough(kahuna, StayedKey, ct));
                Assert.Equal("three", await ReadThrough(kahuna, MovedKey, ct));
            }

            await CommitThroughScript(nodes[behind].Kahuna, StayedKey, "four");
            Assert.Equal("four", await ReadThrough(nodes[current[0]].Kahuna, StayedKey, ct));
        }
        finally
        {
            await LeaveCluster(nodes[0].Raft, nodes[1].Raft, nodes[2].Raft);
        }
    }

    /// <param name="onePhase">True commits through one-phase bundles that are judged at apply and settle with a
    /// materializing resolve. False commits in two phases with a materialization record that names the intent.
    /// A node that refuses the destination's prepare holds no source for the commit in either form: the gate
    /// refuses the bundle and the resolve of the settle finds no intent, or the materialization record finds no
    /// intent. In both forms the node then gates the partition and asks for a new copy of it.</param>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task NodeBehindOnTheSourceLog_AdmitsTheDestinationCommitOfAMovedKey_AndServesItWhenItLeads(bool onePhase)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleCluster(
            3, "memory", 4, raftLogger, kahunaLogger, configureKahuna: config =>
            {
                config.OnePhaseApplyTimeValidation = onePhase;
                config.DurableMaterializeOnResolve = onePhase;
            });

        (IRaft Raft, KahunaManager Kahuna)[] nodes = [.. rafts.Zip(kahunas, (raft, kahuna) => (raft, (KahunaManager)kahuna))];

        try
        {
            foreach ((IRaft _, KahunaManager kahuna) in nodes)
                kahuna.RegisterKeyRange(Space);

            int metaLeader = await LeaderOf(RangeMapStore.MetaPartitionId, nodes, ct);

            Assert.True(await nodes[metaLeader].Kahuna.RangeMapStore.MutateAsync(
                _ => [new RangeDescriptor { KeySpace = Space, StartKey = null, EndKey = null, PartitionId = SourcePartition, Generation = 1 }],
                ct));

            foreach ((IRaft _, KahunaManager kahuna) in nodes)
                await WaitUntilAsync(() => kahuna.RangeMapStore.Current.Find(Space, Space + "/x")?.Generation == 1, timeoutMs: 30_000);

            // Every key has one transactional commit, so each has a head in the source slice of every node.
            foreach (string key in KeysBelow.Concat(KeysAbove))
            {
                await CommitThroughScript(nodes[0].Kahuna, key, "one");

                foreach ((IRaft _, KahunaManager kahuna) in nodes)
                    await WaitUntilAsync(() => HeadOf(kahuna, SourcePartition, key) >= 0, timeoutMs: 30_000);
            }

            long firstHead = HeadOf(nodes[0].Kahuna, SourcePartition, MovedKey);

            // The node that falls behind on the source log leads neither the source nor the meta partition, so
            // it settles nothing and drives no move.
            int sourceLeader = await LeaderOf(SourcePartition, nodes, ct);
            metaLeader = await LeaderOf(RangeMapStore.MetaPartitionId, nodes, ct);
            int behind = Enumerable.Range(0, 3).First(i => i != sourceLeader && i != metaLeader);
            int[] current = [.. Enumerable.Range(0, 3).Where(i => i != behind)];

            // From the settle of the next source transaction of the moved key on, the node keeps the source
            // log aside: it holds the prepare and the commit record of that transaction and not its settle.
            HeldLogs heldLogs = new(nodes[behind].Kahuna);
            heldLogs.Hold(SourcePartition, log => SettlesKey(log, MovedKey));

            await CommitThroughScript(nodes[current[0]].Kahuna, MovedKey, "two");

            foreach (int i in current)
                await WaitUntilAsync(() => HeadOf(nodes[i].Kahuna, SourcePartition, MovedKey) == firstHead + 1, timeoutMs: 30_000);

            await WaitUntilAsync(() => heldLogs.Count(SourcePartition) > 0, timeoutMs: 30_000);

            PreparedIntent? sourceLeg = nodes[behind].Kahuna.DurablePreparedIntentStore.Get(MovedKey);
            Assert.NotNull(sourceLeg);
            Assert.Equal(firstHead + 1, sourceLeg.Revision);
            Assert.Equal(firstHead, HeadOf(nodes[behind].Kahuna, SourcePartition, MovedKey));

            // The split moves the key. The two current nodes settled the source transaction, so the barrier of
            // the move passes, and the hand-off seeds the head that transaction committed.
            int destination = await SplitAsync(nodes, ct);

            foreach ((IRaft _, KahunaManager kahuna) in nodes)
                await WaitUntilAsync(() => HeadOf(kahuna, destination, MovedKey) == firstHead + 1, timeoutMs: 30_000);

            // The node is a follower of the destination when the key's next transaction commits there: a leader
            // would carry the settle of the source transaction in its own bundle.
            int destinationLeader = await LeaderOf(destination, nodes, ct);
            if (destinationLeader == behind)
            {
                string target = nodes[current[0]].Raft.GetLocalEndpoint();
                Assert.Equal(RaftOperationStatus.Success, await nodes[behind].Raft.TransferLeadershipAsync(destination, target, ct));
                await WaitUntilAsync(async () => await nodes[current[0]].Raft.AmILeaderIfHosted(destination, ct), timeoutMs: 30_000);
                await WaitUntilAsync(async () => !await nodes[behind].Raft.AmILeaderIfHosted(destination, ct), timeoutMs: 30_000);
            }

            Assert.NotNull(nodes[behind].Kahuna.DurablePreparedIntentStore.GetByIdentity(sourceLeg.TransactionId, sourceLeg.Epoch, MovedKey));

            // A node that finds a commit of the destination missing gates the partition until a new copy of it
            // installs. The gate is watched from here on.
            using CancellationTokenSource watch = CancellationTokenSource.CreateLinkedTokenSource(ct);
            bool gated = false;

            Task watcher = Task.Run(async () =>
            {
                while (!watch.IsCancellationRequested)
                {
                    if (nodes[behind].Kahuna.KeyValues.DivergenceContainment.IsGated(destination))
                        gated = true;

                    await Task.Delay(1);
                }
            }, CancellationToken.None);

            await CommitThroughScript(nodes[current[0]].Kahuna, MovedKey, "three");

            // Every node applies the destination's commit, the node that is behind on the source log too: the
            // settle of that commit records its head in the destination slice only where its prepare installed.
            foreach ((IRaft _, KahunaManager kahuna) in nodes)
                await WaitUntilAsync(() => HeadOf(kahuna, destination, MovedKey) == firstHead + 2, timeoutMs: 30_000);

            // The source leg is still live for the source log on that node, and it refused nothing.
            Assert.NotNull(nodes[behind].Kahuna.DurablePreparedIntentStore.GetByIdentity(sourceLeg.TransactionId, sourceLeg.Epoch, MovedKey));

            foreach (TransactionRecord record in nodes[behind].Kahuna.DurableTransactionRecordStore.EnumerateRecords())
            {
                if (record.RecordAnchorKey == MovedKey)
                    Assert.True(record.RejectedBundledCommitOpIds is not { Count: > 0 }, $"the node refused the bundle of transaction {record.TransactionId}");
            }

            await Task.Delay(200, ct);
            await watch.CancelAsync();
            await watcher;
            Assert.False(gated, "the node gated the destination partition: it found a commit of the destination missing");

            // The source log catches up: the settle finds its leg and records the head on the source slice.
            await heldLogs.ReleaseAsync(SourcePartition);

            await WaitUntilAsync(() => nodes[behind].Kahuna.DurablePreparedIntentStore.Get(MovedKey) is null, timeoutMs: 30_000);
            Assert.Equal(firstHead + 1, HeadOf(nodes[behind].Kahuna, SourcePartition, MovedKey));
            Assert.Equal(0, nodes[behind].Kahuna.DurablePreparedIntentStore.CoveredIntentCount);

            // The node leads the destination next and serves the key from its own state.
            destinationLeader = await LeaderOf(destination, nodes, ct);
            if (destinationLeader != behind)
            {
                string target = nodes[behind].Raft.GetLocalEndpoint();
                Assert.Equal(RaftOperationStatus.Success, await nodes[destinationLeader].Raft.TransferLeadershipAsync(destination, target, ct));
                await WaitUntilAsync(async () => await nodes[behind].Raft.AmILeaderIfHosted(destination, ct), timeoutMs: 30_000);
            }

            foreach ((IRaft _, KahunaManager kahuna) in nodes)
                Assert.Equal("three", await ReadThrough(kahuna, MovedKey, ct));

            // A transaction that reads the row through the new leader commits over the current revision.
            await CommitThroughScript(nodes[behind].Kahuna, MovedKey, "four");

            foreach ((IRaft _, KahunaManager kahuna) in nodes)
                await WaitUntilAsync(() => HeadOf(kahuna, destination, MovedKey) == firstHead + 3, timeoutMs: 30_000);

            Assert.Equal("four", await ReadThrough(nodes[current[0]].Kahuna, MovedKey, ct));
        }
        finally
        {
            await LeaveCluster(nodes[0].Raft, nodes[1].Raft, nodes[2].Raft);
        }
    }
}
