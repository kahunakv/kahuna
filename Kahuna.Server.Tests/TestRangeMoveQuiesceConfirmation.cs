using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Replication;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Data;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// A range move (a split or a merge) publishes its quiesce on the range map, which the node that runs the move
/// commits on the meta partition and every other node applies a replication round later. The write scheduler's
/// dispatch fence judges a proposal against the applied map of the node that dispatches it: the leader of the
/// partition the range moves out of. Until that leader's map shows the quiesce, its fence refuses nothing.
///
/// <para>The write that gets through is a one-phase commit. Its validation runs before it proposes, so one
/// that validated before the move's range lock existed has nothing left to stop it: appended on the source
/// partition after the move gathered the range's state, it is acknowledged, and the cutover then routes its
/// key to a partition that never saw it. These tests hold the source leader's map behind the quiesce and
/// require that the move does not go on without that leader, that it goes on once the leader shows the
/// quiesce, and that the leader answers only when it leads, shows the quiesce and applied what it had
/// dispatched.</para>
///
/// <para>Most of the fixture places each data partition on exactly one node (replication factor 1), so the
/// source leader is the partition's only replica and the questions it is asked have one possible answerer.
/// The test that freezes the whole sequence runs under legacy full replication instead, and says why.</para>
/// </summary>
public sealed class TestRangeMoveQuiesceConfirmation : BaseCluster
{
    private const int Partitions = 6;

    private const string Space = "qc:s";

    private const string SplitKey = Space + "/m";

    /// <summary>Lives in the moving half <c>[SplitKey, +inf)</c>.</summary>
    private const string MovingKey = Space + "/z-held";

    private readonly Microsoft.Extensions.Logging.ILogger<IRaft> raftLogger;

    private readonly Microsoft.Extensions.Logging.ILogger<IKahuna> kahunaLogger;

    public TestRangeMoveQuiesceConfirmation(ITestOutputHelper outputHelper)
    {
        Microsoft.Extensions.Logging.ILoggerFactory loggerFactory = TestLogFactory.Create(outputHelper);
        raftLogger = Microsoft.Extensions.Logging.LoggerFactoryExtensions.CreateLogger<IRaft>(loggerFactory);
        kahunaLogger = Microsoft.Extensions.Logging.LoggerFactoryExtensions.CreateLogger<IKahuna>(loggerFactory);
    }

    private static byte[] V(string s) => Encoding.UTF8.GetBytes(s);

    private static async Task<(IRaft Raft, KahunaManager Kahuna)> LeaderOf(
        int partition, IRaft[] rafts, KahunaManager[] kahunas, CancellationToken ct)
    {
        while (true)
        {
            for (int i = 0; i < rafts.Length; i++)
                if (await rafts[i].AmILeaderIfHosted(partition, ct))
                    return (rafts[i], kahunas[i]);

            await Task.Delay(50, ct);
        }
    }

    /// <summary>
    /// Holds one node's range map behind the cluster's: every range-map entry the node would apply is kept
    /// aside instead, as an apply of the meta partition that lags does, without blocking any thread of the
    /// node. <see cref="Release"/> applies the kept entries in order and lets the node follow again.
    /// </summary>
    private sealed class HeldRangeMaps : IDisposable
    {
        private readonly KahunaManager node;

        private readonly ConcurrentQueue<RaftLog> held = new();

        private int holding = 1;

        public HeldRangeMaps(KahunaManager node)
        {
            this.node = node;

            node.KeyValues.ReplicationDispatcher.ApplySkipForTesting = (partitionId, log) =>
            {
                if (Volatile.Read(ref holding) == 0 || partitionId != RangeMapStore.MetaPartitionId || log.LogType != ReplicationTypes.RangeMap)
                    return false;

                held.Enqueue(log);
                return true;
            };
        }

        /// <summary>How many range-map entries reached the node and were kept aside.</summary>
        public int Count => held.Count;

        public void Release()
        {
            if (Interlocked.Exchange(ref holding, 0) == 0)
                return;

            // An entry that arrives from here on applies by itself; the store ignores a kept entry that is
            // older than what it already installed.
            while (held.TryDequeue(out RaftLog? log))
                node.RangeMapStore.Replicate(RangeMapStore.MetaPartitionId, log);

            node.KeyValues.ReplicationDispatcher.ApplySkipForTesting = null;
        }

        public void Dispose() => Release();
    }

    /// <summary>
    /// Assembles the cluster, registers the ranged space everywhere, and seeds one whole-space descriptor on a
    /// data partition the meta leader does not lead, with keys on both sides of <see cref="SplitKey"/>. The
    /// source leader is therefore never the node that runs the move. With replication factor 1 it is also the
    /// partition's only replica; with 0 (legacy full replication) every node hosts every partition.
    /// </summary>
    private async Task<(IRaft[] Rafts, KahunaManager[] Kahunas, KahunaManager Driver, int SourcePartition, KahunaManager SourceLeader)> Setup(
        CancellationToken ct, int replicationFactor = 1)
    {
        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleCluster(
            3, "memory", Partitions, raftLogger, kahunaLogger, replicationFactor: replicationFactor);

        KahunaManager[] managers = [.. kahunas.Cast<KahunaManager>()];

        foreach (KahunaManager kahuna in managers)
            kahuna.RegisterKeyRange(Space);

        (IRaft driverRaft, KahunaManager driver) = await LeaderOf(RangeMapStore.MetaPartitionId, rafts, managers, ct);

        int sourcePartition = 0;
        for (int partitionId = 1; partitionId <= Partitions; partitionId++)
        {
            if (!await driverRaft.AmILeaderIfHosted(partitionId, ct))
            {
                sourcePartition = partitionId;
                break;
            }
        }

        Assert.NotEqual(0, sourcePartition);

        bool seeded = await driver.RangeMapStore.MutateAsync(
            _ => [new RangeDescriptor { KeySpace = Space, PartitionId = sourcePartition, Generation = 1 }], ct);
        Assert.True(seeded);

        foreach (KahunaManager kahuna in managers)
            await WaitUntilAsync(
                () => kahuna.RangeMapStore.Current.Find(Space, Space + "/x")?.Generation == 1, timeoutMs: 30_000);

        // The moving half starts with two keys no test writes in a transaction: the splitter's probe of that half
        // reads its first key and looks at the next one, and a commit parked on either makes the probe
        // indeterminate.
        foreach (string key in new[] { Space + "/a0", Space + "/a1", Space + "/n0", Space + "/n1", Space + "/z0", Space + "/z1", MovingKey })
        {
            (KeyValueResponseType type, _, _) = await RetryOnMustRetryAsync(
                () => driver.LocateAndTrySetKeyValue(
                    HLCTimestamp.Zero, key, V("base"), null, -1, KeyValueFlags.Set, 0,
                    KeyValueDurability.Persistent, ct),
                r => r.Item1);

            Assert.Equal(KeyValueResponseType.Set, type);
        }

        (_, KahunaManager sourceLeader) = await LeaderOf(sourcePartition, rafts, managers, ct);
        Assert.NotSame(driver, sourceLeader);

        return (rafts, managers, driver, sourcePartition, sourceLeader);
    }

    /// <summary>Starts a durable session whose coordinator lives on the source partition's leader and stages one
    /// write to <see cref="MovingKey"/> in it, before any move exists.</summary>
    private static async Task<TransactionHandle> StageWriteOnTheSourceLeader(
        KahunaManager session, int sourcePartition, KeyValueTransactionLocking locking, CancellationToken ct)
    {
        (KeyValueResponseType started, TransactionHandle handle) = await session.LocateAndStartTransaction(
            new KeyValueTransactionOptions
            {
                Locking = locking,
                DecisionDurability = DecisionDurability.Durable,
                Timeout = 60_000,
                CoordinatorPartitionId = sourcePartition
            }, ct);
        Assert.Equal(KeyValueResponseType.Set, started);

        (KeyValueResponseType set, _, _) = await RetryOnMustRetryAsync(
            () => session.LocateAndTrySetKeyValue(
                handle.TransactionId, MovingKey, V("attempt"), null, -1, KeyValueFlags.Set, 0,
                KeyValueDurability.Persistent, ct, 0, handle.CoordinatorKey, TransactionOperationId.NewRandom()),
            r => r.Item1);
        Assert.Equal(KeyValueResponseType.Set, set);

        return handle;
    }

    private static async Task<string?> ReadLatest(KahunaManager kahuna, string key, CancellationToken ct)
    {
        (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await RetryOnMustRetryAsync(
            () => kahuna.LocateAndTryGetValue(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct).AsTask(),
            r => r.Item1, timeoutMs: 30_000);

        return type == KeyValueResponseType.Get && entry?.Value is not null ? Encoding.UTF8.GetString(entry.Value) : null;
    }

    /// <summary>Waits until every node's map agrees with the meta leader's about <see cref="MovingKey"/> and shows
    /// no quiesce over it.</summary>
    private static async Task WaitForMapsToAgree(KahunaManager[] kahunas, KahunaManager driver)
    {
        foreach (KahunaManager kahuna in kahunas)
        {
            KahunaManager observer = kahuna;

            await WaitUntilAsync(() =>
            {
                RangeDescriptor? seen = observer.RangeMapStore.Current.Find(Space, MovingKey);
                RangeDescriptor? committed = driver.RangeMapStore.Current.Find(Space, MovingKey);

                return seen is not null && committed is not null
                    && seen.QuiescedUntil == HLCTimestamp.Zero
                    && seen.Generation == committed.Generation
                    && seen.PartitionId == committed.PartitionId;
            }, timeoutMs: 30_000);
        }
    }

    /// <summary>
    /// The whole sequence, frozen. A session stages a write in the moving half. Its commit validates, and
    /// before it proposes, a split of the range runs from its first step to its last while the source leader's
    /// map is held behind. The commit then proposes on the source leader, whose map shows neither a quiesce nor
    /// a new generation, so its dispatch fence lets the proposal through.
    ///
    /// <para>The move must not have cut over: the source leader could not confirm that it enforces the quiesce,
    /// so the attempt is refused and the range stays where it is, which is where the commit then lands.
    /// Whatever the commit answers, every node must agree with it. Before the confirmation existed the split
    /// succeeded here, and the commit was acknowledged on a partition its key no longer routed to.</para>
    ///
    /// <para>Runs under legacy full replication. With a committed replica set the move pages the range through
    /// scans, and a scan waits behind the write intent of a commit that is parked this way, so the move never
    /// gets as far as its quiesce and the interleaving cannot be frozen. Under full replication the copy reads
    /// the local backend and does not wait. Every node also holds every partition's rows there, so the write
    /// left on the source stays readable; what this test pins is that the move did not go on.</para>
    /// </summary>
    [Theory]
    [InlineData(KeyValueTransactionLocking.Optimistic)]
    [InlineData(KeyValueTransactionLocking.Pessimistic)]
    public async Task CommitValidatedBeforeTheSplit_ProposedWhileTheSourceLeadersMapIsBehind_IsNotLeftOnTheSource(KeyValueTransactionLocking locking)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, KahunaManager[] kahunas, KahunaManager driver, int sourcePartition, KahunaManager sourceLeader) = await Setup(ct, replicationFactor: 0);

        TransactionHandle handle = await StageWriteOnTheSourceLeader(sourceLeader, sourcePartition, locking, ct);

        using HeldRangeMaps held = new(sourceLeader);

        SplitOutcome splitOutcome = default;
        bool splitRan = false;
        int ran = 0;

        foreach (KahunaManager kahuna in kahunas)
        {
            kahuna.KeyValues.Coordinator.DurableFinalizerForTests.TestAfterReadSetValidationHook = async hookCt =>
            {
                if (Interlocked.Exchange(ref ran, 1) != 0)
                    return;

                (_, KahunaManager splitDriver) = await LeaderOf(RangeMapStore.MetaPartitionId, rafts, kahunas, hookCt);
                splitOutcome = await splitDriver.ForceSplitAtKeyAsync(Space, SplitKey, null, hookCt);
                splitRan = true;
            };
        }

        TransactionCommitAnswer answer;

        try
        {
            answer = await sourceLeader.LocateAndCommitTransaction(handle, ct);
        }
        finally
        {
            foreach (KahunaManager kahuna in kahunas)
                kahuna.KeyValues.Coordinator.DurableFinalizerForTests.TestAfterReadSetValidationHook = null;
        }

        Assert.True(splitRan, "the commit did not reach the gap between its validation and its proposal");

        held.Release();
        await WaitForMapsToAgree(kahunas, driver);

        // The source leader never showed the quiesce, so the move had no confirmation and changed nothing.
        Assert.True(splitOutcome.Status == SplitStatus.QuiesceFailed,
            $"the split answered {splitOutcome.Status} while the source leader's map was behind the quiesce");

        foreach (KahunaManager kahuna in kahunas)
        {
            RangeDescriptor still = kahuna.RangeMapStore.Current.Find(Space, MovingKey)!;
            Assert.Equal(sourcePartition, still.PartitionId);
            Assert.Equal(1, still.Generation);
        }

        // Whatever the commit answers in the end, every node agrees with it.
        if (answer.Type == KeyValueResponseType.MustRetry)
            answer = await RetryOnMustRetryAsync(() => sourceLeader.LocateAndCommitTransaction(handle, ct), a => a.Type, timeoutMs: 30_000);

        Assert.True(answer.Type is KeyValueResponseType.Committed or KeyValueResponseType.Aborted,
            $"the commit answered {answer.Type} ({answer.Refusal}: {answer.Reason})");

        string expected = answer.Type == KeyValueResponseType.Committed ? "attempt" : "base";

        for (int i = 0; i < kahunas.Length; i++)
        {
            string? value = await ReadLatest(kahunas[i], MovingKey, ct);
            Assert.True(value == expected,
                $"the commit answered {answer.Type}, and node {i} reads {value ?? "no value"} for {MovingKey} instead of {expected}");
        }
    }

    /// <summary>
    /// The lag that ends inside the move's budget. The move must wait for the source leader, and once that
    /// leader shows the quiesce it must enforce it for the rest of the window: a commit that reaches it inside
    /// the window, with every coordinator's own gate switched off, is released by its dispatch fence. The test
    /// seam does not wait for anything; the move did.
    /// </summary>
    [Fact]
    public async Task SourceLeaderBehindTheQuiesce_TheMoveWaitsForIt_ThenTheLeaderEnforcesTheQuiesce()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        using MetricCapture releases = new("reason", "kahuna.kv.write.rejections");

        (_, KahunaManager[] kahunas, KahunaManager driver, int sourcePartition, KahunaManager sourceLeader) = await Setup(ct);

        TransactionHandle handle = await StageWriteOnTheSourceLeader(sourceLeader, sourcePartition, KeyValueTransactionLocking.Optimistic, ct);

        foreach (KahunaManager kahuna in kahunas)
            kahuna.KeyValues.Coordinator.TestSkipRangeQuiesceAdmissionGate = true;

        try
        {
            using HeldRangeMaps held = new(sourceLeader);

            bool enforcedAtTheSeam = false;
            TransactionCommitAnswer inWindow = default;

            Task<SplitOutcome> split = driver.ForceSplitAtKeyAsync(Space, SplitKey, async () =>
            {
                enforcedAtTheSeam = sourceLeader.KeyValues.IsKeyQuiesced(MovingKey);
                inWindow = await sourceLeader.LocateAndCommitTransaction(handle, ct);
            }, ct);

            // The quiesce reached the source leader and is kept aside: its map does not show it.
            await WaitUntilAsync(() => held.Count >= 1);
            await Task.Delay(300, ct);

            Assert.False(sourceLeader.KeyValues.IsKeyQuiesced(MovingKey));
            Assert.False(split.IsCompleted, "the move went on while the source leader's map did not show the quiesce");

            held.Release();

            SplitOutcome outcome = await split;

            Assert.True(outcome.IsSuccess, $"Split failed: {outcome.Status}");
            Assert.True(enforcedAtTheSeam, "the move reached its in-window steps before the source leader enforced the quiesce");
            Assert.True(inWindow.Type == KeyValueResponseType.MustRetry,
                $"the commit inside the window answered {inWindow.Type} ({inWindow.Refusal}: {inWindow.Reason})");
            Assert.True(releases.Total("kahuna.kv.write.rejections", "range_quiesced") >= 1,
                "the source leader's dispatch fence did not release the proposal");
        }
        finally
        {
            foreach (KahunaManager kahuna in kahunas)
                kahuna.KeyValues.Coordinator.TestSkipRangeQuiesceAdmissionGate = false;
        }

        await WaitForMapsToAgree(kahunas, driver);

        // The client gives the refused attempt up. Nothing of it is left, on the source or on the destination.
        Assert.Equal(KeyValueResponseType.RolledBack, await RetryOnMustRetryAsync(
            () => sourceLeader.LocateAndRollbackTransaction(handle, ct), r => r, timeoutMs: 30_000));

        foreach (KahunaManager kahuna in kahunas)
            Assert.Equal("base", await ReadLatest(kahuna, MovingKey, ct));
    }

    /// <summary>
    /// What the source leader answers, and to whom. Only the confirmed leader of the partition confirms, only
    /// for the move that owns the quiesce, only over an interval the quiesce covers, and only while the window
    /// is open. The leader brings its own map up to date to answer, so the first question after the quiesce
    /// was published is answered without the caller having to wait for the map.
    /// </summary>
    [Fact]
    public async Task OnlyTheConfirmedLeader_ConfirmsOnlyTheOwnersQuiesce_OverTheIntervalItCovers()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, KahunaManager[] kahunas, KahunaManager driver, int sourcePartition, KahunaManager sourceLeader) = await Setup(ct);

        HLCTimestamp owner = rafts[0].HybridLogicalClock.TrySendOrLocalEvent(rafts[0].GetLocalNodeId());
        HLCTimestamp stranger = rafts[0].HybridLogicalClock.TrySendOrLocalEvent(rafts[0].GetLocalNodeId());
        HLCTimestamp until = rafts[0].HybridLogicalClock.TrySendOrLocalEvent(rafts[0].GetLocalNodeId()) + 60_000;

        // No quiesce at all: nobody confirms one.
        foreach (KahunaManager kahuna in kahunas)
            Assert.Equal(KeyValueResponseType.MustRetry, await kahuna.ConfirmRangeQuiesce(sourcePartition, Space, SplitKey, null, owner, ct));

        Assert.True(await driver.RangeMapStore.QuiesceRangeAsync(Space, SplitKey, null, owner, until, ct));

        // Asked at once, with no wait for the source leader's map.
        Assert.Equal(KeyValueResponseType.Locked, await RetryOnMustRetryAsync(
            () => sourceLeader.ConfirmRangeQuiesce(sourcePartition, Space, SplitKey, null, owner, ct), t => t));

        // A node that does not lead the partition cannot confirm for it, whatever its map shows.
        foreach (KahunaManager kahuna in kahunas)
        {
            if (ReferenceEquals(kahuna, sourceLeader))
                continue;

            KahunaManager observer = kahuna;
            await WaitUntilAsync(() => observer.KeyValues.IsKeyQuiesced(MovingKey));

            Assert.Equal(KeyValueResponseType.MustRetry, await kahuna.ConfirmRangeQuiesce(sourcePartition, Space, SplitKey, null, owner, ct));
        }

        // The mover's side reaches the leader from every node.
        foreach (KahunaManager kahuna in kahunas)
            Assert.True(await kahuna.KeyValues.ConfirmRangeQuiesceOnPartitionLeaderAsync(sourcePartition, Space, SplitKey, null, owner, ct));

        // Another move's id, and an interval the quiesce does not cover, are not confirmed.
        Assert.Equal(KeyValueResponseType.MustRetry, await sourceLeader.ConfirmRangeQuiesce(sourcePartition, Space, SplitKey, null, stranger, ct));
        Assert.Equal(KeyValueResponseType.MustRetry, await sourceLeader.ConfirmRangeQuiesce(sourcePartition, Space, null, null, owner, ct));
        Assert.Equal(KeyValueResponseType.Locked, await sourceLeader.ConfirmRangeQuiesce(sourcePartition, Space, Space + "/q", Space + "/t", owner, ct));

        // The window closed: nothing is confirmed any more.
        Assert.True(await driver.RangeMapStore.ReleaseQuiesceAsync(owner, ct));
        await WaitUntilAsync(async () =>
            await sourceLeader.ConfirmRangeQuiesce(sourcePartition, Space, SplitKey, null, owner, ct) == KeyValueResponseType.MustRetry);
    }

    /// <summary>
    /// The leader's answer covers the writes it had already dispatched: it proposes one entry behind them and
    /// answers only when its own apply of the partition passed that entry. Here that apply is delayed, and the
    /// answer must not come before it.
    /// </summary>
    [Fact]
    public async Task TheLeaderConfirms_OnlyAfterItAppliedTheEntryItProposedBehindItsDispatchedWrites()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, KahunaManager[] _, KahunaManager driver, int sourcePartition, KahunaManager sourceLeader) = await Setup(ct);

        HLCTimestamp owner = rafts[0].HybridLogicalClock.TrySendOrLocalEvent(rafts[0].GetLocalNodeId());
        HLCTimestamp until = rafts[0].HybridLogicalClock.TrySendOrLocalEvent(rafts[0].GetLocalNodeId()) + 60_000;

        Assert.True(await driver.RangeMapStore.QuiesceRangeAsync(Space, SplitKey, null, owner, until, ct));
        await WaitUntilAsync(() => sourceLeader.KeyValues.IsKeyQuiesced(MovingKey));

        long appliedBefore = sourceLeader.KeyValues.ReplicationDispatcher.AppliedIndex(sourcePartition);
        long markerIndex = 0;
        long applyResumedAt = 0;

        sourceLeader.KeyValues.ReplicationDispatcher.ApplySkipForTesting = (partitionId, log) =>
        {
            if (partitionId == sourcePartition && log.LogType == ReplicationTypes.ProgressTick
                && Interlocked.CompareExchange(ref markerIndex, log.Id, 0) == 0)
            {
                Thread.Sleep(250);
                Volatile.Write(ref applyResumedAt, Stopwatch.GetTimestamp());
            }

            return false;
        };

        KeyValueResponseType answer;
        long answeredAt;

        try
        {
            answer = await sourceLeader.ConfirmRangeQuiesce(sourcePartition, Space, SplitKey, null, owner, ct);
            answeredAt = Stopwatch.GetTimestamp();
        }
        finally
        {
            sourceLeader.KeyValues.ReplicationDispatcher.ApplySkipForTesting = null;
        }

        Assert.Equal(KeyValueResponseType.Locked, answer);

        long marker = Volatile.Read(ref markerIndex);
        long resumed = Volatile.Read(ref applyResumedAt);

        Assert.True(marker > appliedBefore, $"no entry was proposed behind the dispatched writes (applied before {appliedBefore}, entry {marker})");
        Assert.True(resumed != 0 && answeredAt >= resumed, "the leader answered before its apply of the entry had finished");
        Assert.True(sourceLeader.KeyValues.ReplicationDispatcher.AppliedIndex(sourcePartition) >= marker);
    }

    /// <summary>
    /// A merge takes the same step for the partition it retires. A retiring leader that cannot show the quiesce
    /// refuses the attempt; both ranges stay where they are, the window is closed again, and the range takes
    /// writes.
    /// </summary>
    [Fact]
    public async Task Merge_WhoseRetiringLeaderCannotShowTheQuiesce_IsRefused_AndTheRangeReopens()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, IKahuna[] nodes) = await AssembleCluster(
            3, "memory", Partitions, raftLogger, kahunaLogger, replicationFactor: 1);

        KahunaManager[] kahunas = [.. nodes.Cast<KahunaManager>()];

        foreach (KahunaManager kahuna in kahunas)
            kahuna.RegisterKeyRange(Space);

        (IRaft driverRaft, KahunaManager driver) = await LeaderOf(RangeMapStore.MetaPartitionId, rafts, kahunas, ct);

        // The retiring partition off the node that runs the merge, and the survivor on a third node.
        int rightPartition = Enumerable.Range(1, Partitions).First(p => !driverRaft.HostsPartition(p));
        IRaft rightHost = rafts.Single(r => r.HostsPartition(rightPartition));
        int leftPartition = Enumerable.Range(1, Partitions).First(p => p != rightPartition && !rightHost.HostsPartition(p));

        Assert.True(await driver.RangeMapStore.MutateAsync(_ =>
        [
            new RangeDescriptor { KeySpace = Space, StartKey = null, EndKey = SplitKey, PartitionId = leftPartition, Generation = 1 },
            new RangeDescriptor { KeySpace = Space, StartKey = SplitKey, EndKey = null, PartitionId = rightPartition, Generation = 1 }
        ], ct));

        foreach (KahunaManager kahuna in kahunas)
        {
            KahunaManager observer = kahuna;
            await WaitUntilAsync(() => observer.RangeMapStore.Current.FindAll(Space).Count == 2, timeoutMs: 30_000);
        }

        foreach (string key in new[] { Space + "/a0", Space + "/z0" })
        {
            (KeyValueResponseType type, _, _) = await RetryOnMustRetryAsync(
                () => driver.LocateAndTrySetKeyValue(
                    HLCTimestamp.Zero, key, V("base"), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct),
                r => r.Item1, timeoutMs: 30_000);

            Assert.Equal(KeyValueResponseType.Set, type);
        }

        (_, KahunaManager retiringLeader) = await LeaderOf(rightPartition, rafts, kahunas, ct);

        MergeOutcome outcome;

        using (HeldRangeMaps held = new(retiringLeader))
        {
            IReadOnlyList<RangeDescriptor> pair = driver.RangeMapStore.Current.FindAll(Space);
            outcome = await driver.RangeMerger.MergeAsync(Space, pair[0], pair[1], ct);
        }

        Assert.True(outcome.Status == MergeStatus.QuiesceFailed,
            $"the merge answered {outcome.Status} while the retiring leader's map was behind the quiesce");

        foreach (KahunaManager kahuna in kahunas)
        {
            KahunaManager observer = kahuna;

            await WaitUntilAsync(() =>
            {
                IReadOnlyList<RangeDescriptor> seen = observer.RangeMapStore.Current.FindAll(Space);
                return seen.Count == 2 && seen.All(d => d.QuiescedUntil == HLCTimestamp.Zero && d.Generation == 1);
            }, timeoutMs: 30_000);
        }

        (KeyValueResponseType reopened, _, _) = await RetryOnMustRetryAsync(
            () => driver.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, Space + "/z-after", V("open"), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct),
            r => r.Item1, timeoutMs: 30_000);

        Assert.Equal(KeyValueResponseType.Set, reopened);
    }
}
