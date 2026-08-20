using System.Text;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.System;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// A range merge moves <c>[B,C)</c> off the retiring partition onto the survivor. Between the copy
/// and the meta cutover the moving range must accept no further write: one that commits on the
/// retiring partition in that window is absent from the copy and unreachable once the range routes
/// to the survivor — acknowledged to the client and then gone.
///
/// <para>
/// The merge runs on one node; a write can arrive on any of them. These tests drive writes from
/// <b>every</b> node while a merge is in flight and compare what the clients saw acknowledged
/// against what the range actually holds afterwards. Asserting on acknowledgements alone is what
/// misses this class of bug — the losing write is acknowledged, so every status code looks right.
/// </para>
///
/// <para>
/// The fixture places each data partition on exactly one node (replication factor 1) and puts the
/// two merging ranges on partitions that different nodes host. That is what makes a stranded write
/// observable: the persistence backend is node-global, so when both partitions share a node the
/// survivor's store already holds the retiring partition's rows and a read routed to the survivor
/// still finds the value, masking the loss.
/// </para>
/// </summary>
public sealed class TestMergeCrossNodeWriteLoss : BaseCluster
{
    private const int Partitions = 6;

    private const string Space = "xm:s";

    /// <summary>Boundary between the left (surviving) range and the right (moving) one.</summary>
    private const string BoundaryKey = Space + "/m";

    private readonly Microsoft.Extensions.Logging.ILogger<IRaft> raftLogger;

    private readonly Microsoft.Extensions.Logging.ILogger<IKahuna> kahunaLogger;

    public TestMergeCrossNodeWriteLoss(ITestOutputHelper outputHelper)
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
    /// Assembles the RF=1 cluster, registers the ranged space everywhere, and seeds two adjacent
    /// descriptors on data partitions that <b>different</b> nodes host, plus a couple of keys on each
    /// side of <see cref="BoundaryKey"/> so the copy has something to carry.
    /// </summary>
    private async Task<(IRaft[] Rafts, KahunaManager[] Kahunas, KahunaManager Driver)> Setup(
        CancellationToken ct)
    {
        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleCluster(
            3, "memory", Partitions, raftLogger, kahunaLogger, replicationFactor: 1);

        KahunaManager[] managers = [.. kahunas.Cast<KahunaManager>()];

        foreach (KahunaManager kahuna in managers)
            kahuna.RegisterKeyRange(Space);

        (IRaft driverRaft, KahunaManager driver) = await LeaderOf(RangeMapStore.MetaPartitionId, rafts, managers, ct);

        // The survivor and the retiring partition must sit on different nodes, or the shared
        // node-global backend answers a read that routing can no longer reach and the loss is
        // invisible. Keeping the retiring one off the driver mirrors how a merge runs in production,
        // where the executor leads the meta partition and rarely the data partitions it moves.
        int leftPartition = 0;
        int rightPartition = 0;

        for (int partitionId = 1; partitionId <= Partitions && rightPartition == 0; partitionId++)
            if (!driverRaft.HostsPartition(partitionId))
                rightPartition = partitionId;

        Assert.NotEqual(0, rightPartition);

        IRaft rightHost = rafts.Single(r => r.HostsPartition(rightPartition));

        for (int partitionId = 1; partitionId <= Partitions && leftPartition == 0; partitionId++)
            if (partitionId != rightPartition && !rightHost.HostsPartition(partitionId))
                leftPartition = partitionId;

        Assert.NotEqual(0, leftPartition);

        bool seeded = await driver.RangeMapStore.MutateAsync(_ =>
        [
            new RangeDescriptor { KeySpace = Space, StartKey = null, EndKey = BoundaryKey, PartitionId = leftPartition, Generation = 1 },
            new RangeDescriptor { KeySpace = Space, StartKey = BoundaryKey, EndKey = null, PartitionId = rightPartition, Generation = 1 }
        ], ct);

        Assert.True(seeded);

        foreach (KahunaManager kahuna in managers)
            await WaitUntilAsync(
                () => kahuna.RangeMapStore.Current.FindAll(Space).Count == 2, timeoutMs: 30_000);

        foreach (string key in new[] { Space + "/a0", Space + "/a1", Space + "/z0", Space + "/z1" })
        {
            (KeyValueResponseType type, _, _) = await RetryOnMustRetryAsync(
                () => driver.LocateAndTrySetKeyValue(
                    HLCTimestamp.Zero, key, V(key), null, -1, KeyValueFlags.Set, 0,
                    KeyValueDurability.Persistent, ct),
                r => r.Item1, timeoutMs: 30_000);

            Assert.Equal(KeyValueResponseType.Set, type);
        }

        return (rafts, managers, driver);
    }

    /// <summary>The two adjacent descriptors of <see cref="Space"/> as the driver currently sees them.</summary>
    private static (RangeDescriptor Left, RangeDescriptor Right) CurrentPair(KahunaManager driver)
    {
        IReadOnlyList<RangeDescriptor> all = driver.RangeMapStore.Current.FindAll(Space);

        Assert.Equal(2, all.Count);

        return (all[0], all[1]);
    }

    /// <summary>
    /// Runs the merge on the meta-partition leader with bounded retries, so a transient refusal (an
    /// intent still undecided, a leadership blip) does not fail the test. Retires the vacated
    /// partition on success, which is what the merge trigger does and what makes a later read prove
    /// it is served by the survivor rather than by a partition nothing routes to any more.
    /// </summary>
    private static async Task<MergeOutcome> MergeWithRetriesAsync(
        IRaft[] rafts, KahunaManager[] kahunas, Func<Task>? duringQuiesce, CancellationToken ct)
    {
        MergeOutcome outcome = MergeOutcome.TransferFailed;

        for (int attempt = 0; attempt < 5; attempt++)
        {
            (IRaft driverRaft, KahunaManager driver) = await LeaderOf(RangeMapStore.MetaPartitionId, rafts, kahunas, ct);

            (RangeDescriptor left, RangeDescriptor right) = CurrentPair(driver);

            outcome = await driver.RangeMerger.MergeAsync(Space, left, right, duringQuiesce, ct);

            if (outcome.IsSuccess)
            {
                await driverRaft.RemovePartitionAsync(outcome.RetiredPartitionId, ct);
                return outcome;
            }

            if (outcome.Status == MergeStatus.NotAdjacent)
                return outcome;

            await Task.Delay(100, ct);
        }

        return outcome;
    }

    /// <summary>
    /// Reads <paramref name="key"/> back through every node's locator, absorbing the
    /// <c>MustRetry</c> a partition answers while its Raft group settles. Returns the first node that
    /// cannot produce the value, so a write that survived on one node but not through the routed path
    /// is still reported.
    /// </summary>
    private static async Task<string?> FindReadbackFailureAsync(
        KahunaManager[] kahunas, string key, CancellationToken ct)
    {
        foreach (KahunaManager kahuna in kahunas)
        {
            (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await RetryOnMustRetryAsync(
                () => kahuna.LocateAndTryGetValue(
                    HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct),
                r => r.Item1, timeoutMs: 30_000);

            string? value = entry?.Value is null ? null : Encoding.UTF8.GetString(entry.Value);

            if (type != KeyValueResponseType.Get || value != key)
                return $"{key} → {type}/{value ?? "null"}";
        }

        return null;
    }

    // ── in-window writes from every node ─────────────────────────────────────────

    /// <summary>
    /// The deterministic half of the reproduction: inside the quiesce window (after the copy, before
    /// the cutover) one direct write is issued from <b>every</b> node into the moving range. Being
    /// refused as <c>MustRetry</c> is correct — the client retries onto the survivor after cutover.
    /// Being acknowledged as <c>Set</c> is correct only if the value is still readable afterwards;
    /// anything else is an acknowledged write the merge silently dropped.
    /// </summary>
    [Fact]
    public async Task Merge_WriteFromEveryNodeInsideQuiesce_IsRefusedOrSurvives()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, KahunaManager[] kahunas, _) = await Setup(ct);

        List<(string Key, KeyValueResponseType Type, string Node)> attempts = [];

        int round = 0;

        MergeOutcome outcome = await MergeWithRetriesAsync(rafts, kahunas, async () =>
        {
            attempts.Clear();
            int thisRound = round++;

            for (int i = 0; i < kahunas.Length; i++)
            {
                string key = $"{Space}/z-p{thisRound}-n{i}";

                (KeyValueResponseType type, _, _) = await kahunas[i].LocateAndTrySetKeyValue(
                    HLCTimestamp.Zero, key, V(key), null, -1, KeyValueFlags.Set, 0,
                    KeyValueDurability.Persistent, ct);

                attempts.Add((key, type, rafts[i].GetLocalEndpoint()));
            }
        }, ct);

        Assert.True(outcome.IsSuccess, $"Merge failed: {outcome.Status}");

        // Guards against a vacuous pass: with no attempt recorded the hook never ran and the loop
        // below has nothing to check.
        Assert.Equal(kahunas.Length, attempts.Count);

        List<string> lost = [];

        foreach ((string key, KeyValueResponseType type, string node) in attempts)
        {
            if (type != KeyValueResponseType.Set)
                continue; // refused in-window — the client retries onto the survivor.

            string? failure = await FindReadbackFailureAsync(kahunas, key, ct);

            if (failure is not null)
                lost.Add($"{failure} (acknowledged on {node})");
        }

        Assert.True(lost.Count == 0,
            "Writes acknowledged inside the merge quiesce are unreadable after cutover: " + string.Join(", ", lost));
    }

    // ── the quiesce is replicated, not local to the merge executor ───────────────

    /// <summary>
    /// Inside the quiesce window every node's range map — not just the executor's — must show the
    /// moving range as refusing writes, and once the merge completes no node may still show it. This
    /// is what distinguishes the replicated quiesce from the in-memory range lock beside it: the lock
    /// lives only on the retiring partition's current leader, so it cannot refuse a write routed by a
    /// node whose map is stale, nor survive a promotion.
    /// </summary>
    [Fact]
    public async Task Merge_QuiesceWindow_IsVisibleOnEveryNodeAndClearsAfterCutover()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, KahunaManager[] kahunas, _) = await Setup(ct);

        MergeOutcome outcome = await MergeWithRetriesAsync(rafts, kahunas, async () =>
        {
            foreach (KahunaManager kahuna in kahunas)
            {
                KahunaManager observer = kahuna;

                // The map reaches followers a replication round after the executor commits it, so
                // poll rather than sample — the window is held open by this hook.
                await WaitUntilAsync(() =>
                {
                    RangeDescriptor? moving = observer.RangeMapStore.Current.Find(Space, Space + "/z0");
                    return moving is not null && moving.QuiescedUntil != HLCTimestamp.Zero;
                }, timeoutMs: 15_000);
            }
        }, ct);

        Assert.True(outcome.IsSuccess, $"Merge failed: {outcome.Status}");

        foreach (KahunaManager kahuna in kahunas)
        {
            KahunaManager observer = kahuna;

            await WaitUntilAsync(
                () => observer.RangeMapStore.Current.FindAll(Space)
                    .All(d => d.QuiescedUntil == HLCTimestamp.Zero),
                timeoutMs: 15_000);
        }
    }

    // ── continuous writes from every node across the whole merge ─────────────────

    /// <summary>
    /// The realistic half: every node writes continuously into the moving range for the whole
    /// duration of the merge — through the copy, the quiesce window and the cutover — and every
    /// acknowledged key is read back afterwards.
    ///
    /// <para>
    /// This is a soak, not the detector. With no guard at all it still passes: an unquiesced window
    /// is short enough that free-running writers rarely land inside it, which is exactly why the
    /// defect survived review. The seam test above is what catches the loss deterministically. What
    /// this covers instead is everything contention breaks: that the range lock does not deadlock
    /// against live writers, that the merge still reaches cutover under load, and that no
    /// acknowledged write is lost on the paths a hook cannot pause — a write admitted just before
    /// the quiesce arrives whose replication completes after the copy.
    /// </para>
    /// </summary>
    [Fact]
    public async Task Merge_ContinuousWritesFromEveryNode_LoseNoAcknowledgedWrite()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, KahunaManager[] kahunas, _) = await Setup(ct);

        using CancellationTokenSource writerStop = CancellationTokenSource.CreateLinkedTokenSource(ct);

        List<string> acknowledged = [];
        Lock sync = new();

        Task[] writers = new Task[kahunas.Length];

        for (int i = 0; i < kahunas.Length; i++)
        {
            int nodeIndex = i;

            writers[i] = Task.Run(async () =>
            {
                KahunaManager kahuna = kahunas[nodeIndex];

                for (int seq = 0; !writerStop.IsCancellationRequested; seq++)
                {
                    string key = $"{Space}/z-p{nodeIndex}-{seq:D4}";

                    (KeyValueResponseType type, _, _) = await kahuna.LocateAndTrySetKeyValue(
                        HLCTimestamp.Zero, key, V(key), null, -1, KeyValueFlags.Set, 0,
                        KeyValueDurability.Persistent, writerStop.Token);

                    if (type == KeyValueResponseType.Set)
                    {
                        lock (sync)
                            acknowledged.Add(key);
                    }

                    await Task.Delay(5, writerStop.Token);
                }
            }, writerStop.Token);
        }

        // Let the writers get going so the copy and the quiesce both land mid-stream.
        await Task.Delay(300, ct);

        MergeOutcome outcome = await MergeWithRetriesAsync(rafts, kahunas, null, ct);

        await writerStop.CancelAsync();

        foreach (Task writer in writers)
        {
            try { await writer; }
            catch (OperationCanceledException) { /* expected on stop */ }
        }

        Assert.True(outcome.IsSuccess, $"Merge failed: {outcome.Status}");

        List<string> snapshot;
        lock (sync)
            snapshot = [.. acknowledged];

        Assert.True(snapshot.Count > 0, "No write was acknowledged — the reproduction proves nothing.");

        List<string> lost = [];

        foreach (string key in snapshot)
        {
            string? failure = await FindReadbackFailureAsync(kahunas, key, ct);

            if (failure is not null)
                lost.Add(failure);
        }

        Assert.True(lost.Count == 0,
            $"{lost.Count} of {snapshot.Count} acknowledged writes are unreadable after the merge: "
            + string.Join(", ", lost.Take(20)));
    }

    // ── one move at a time ───────────────────────────────────────────────────────

    /// <summary>
    /// A merge whose span is already quiesced by another move is refused outright, and the other
    /// move's window is left exactly as it was. Proceeding would overwrite that move's owner, and the
    /// merge's owner-scoped release would then reopen a range the first move is still copying —
    /// a corruption the cutover's generation guard cannot catch, because a quiesce does not bump the
    /// generation.
    /// </summary>
    [Fact]
    public async Task Merge_OverARangeAnotherMoveQuiesced_IsRefusedAndLeavesTheWindowIntact()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, KahunaManager[] kahunas, KahunaManager driver) = await Setup(ct);

        HLCTimestamp stranger = rafts[0].HybridLogicalClock.TrySendOrLocalEvent(rafts[0].GetLocalNodeId());
        HLCTimestamp until = rafts[0].HybridLogicalClock.TrySendOrLocalEvent(rafts[0].GetLocalNodeId()) + 60_000;

        (RangeDescriptor left, RangeDescriptor right) = CurrentPair(driver);

        Assert.True(await driver.RangeMapStore.QuiesceRangeAsync(
            Space, right.StartKey, right.EndKey, stranger, until, ct));

        MergeOutcome outcome = await driver.RangeMerger.MergeAsync(Space, left, right, ct);

        Assert.Equal(MergeStatus.ConcurrentMove, outcome.Status);

        RangeDescriptor? moving = driver.RangeMapStore.Current.Find(Space, Space + "/z0");

        Assert.NotNull(moving);
        Assert.Equal(stranger, moving.QuiesceOwner);
        Assert.Equal(until, moving.QuiescedUntil);

        // Both ranges are still there: a refused merge cuts nothing over.
        Assert.Equal(2, driver.RangeMapStore.Current.FindAll(Space).Count);

        // The same guard holds one layer down, where it cannot be bypassed: publishing over another
        // owner's live window is refused by the store itself.
        HLCTimestamp intruder = rafts[0].HybridLogicalClock.TrySendOrLocalEvent(rafts[0].GetLocalNodeId());

        Assert.False(await driver.RangeMapStore.QuiesceRangeAsync(
            Space, right.StartKey, right.EndKey, intruder, until, ct));

        // Its own owner may still extend the window — that is a renewal, not a conflict.
        HLCTimestamp extended = rafts[0].HybridLogicalClock.TrySendOrLocalEvent(rafts[0].GetLocalNodeId()) + 90_000;

        Assert.True(await driver.RangeMapStore.QuiesceRangeAsync(
            Space, right.StartKey, right.EndKey, stranger, extended, ct));

        Assert.True(await driver.RangeMapStore.ReleaseQuiesceAsync(stranger, ct));
    }
}
