using System.Text;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.System;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// A range split moves <c>[K,E)</c> to a fresh partition. Between the catch-up export and the meta
/// cutover the source range must accept no further writes: one that commits on the source in that
/// window is absent from the exported snapshot and unreachable once the range routes to the child —
/// acknowledged to the client and then gone.
///
/// <para>
/// The split runs on one node; a write can arrive on any of them. These tests drive writes from
/// <b>every</b> node while a split is in flight and compare what the clients saw acknowledged
/// against what the range actually holds afterwards. Asserting on acknowledgements alone is what
/// misses this class of bug — the losing write is acknowledged, so every status code looks right.
/// </para>
///
/// <para>
/// The fixture places each data partition on exactly one node (replication factor 1). That is what
/// makes a stranded write observable: the persistence backend is node-global, so under legacy full
/// replication every node holds the source partition's rows too and a read routed to the child
/// still finds the value in the shared backend, masking the loss.
/// </para>
/// </summary>
public sealed class TestSplitCrossNodeWriteLoss : BaseCluster
{
    private const int Partitions = 6;

    private const string Space = "xn:s";

    private const string SplitKey = Space + "/m";

    private readonly Microsoft.Extensions.Logging.ILogger<IRaft> raftLogger;

    private readonly Microsoft.Extensions.Logging.ILogger<IKahuna> kahunaLogger;

    public TestSplitCrossNodeWriteLoss(ITestOutputHelper outputHelper)
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
    /// Assembles the RF=1 cluster, registers the ranged space everywhere, and seeds one whole-space
    /// descriptor on a data partition the split driver (the meta-partition leader) does not host,
    /// plus a couple of keys on each side of <see cref="SplitKey"/> so the splitter's
    /// non-empty-halves probe passes.
    /// </summary>
    private async Task<(IRaft[] Rafts, KahunaManager[] Kahunas, KahunaManager Driver, int SourcePartition)> Setup(
        CancellationToken ct)
    {
        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleCluster(
            3, "memory", Partitions, raftLogger, kahunaLogger, replicationFactor: 1);

        KahunaManager[] managers = [.. kahunas.Cast<KahunaManager>()];

        foreach (KahunaManager kahuna in managers)
            kahuna.RegisterKeyRange(Space);

        (IRaft driverRaft, KahunaManager driver) = await LeaderOf(RangeMapStore.MetaPartitionId, rafts, managers, ct);

        int sourcePartition = 0;
        for (int partitionId = 1; partitionId <= Partitions; partitionId++)
        {
            if (!driverRaft.HostsPartition(partitionId))
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

        foreach (string key in new[] { Space + "/a0", Space + "/a1", Space + "/z0", Space + "/z1" })
        {
            (KeyValueResponseType type, _, _) = await RetryOnMustRetryAsync(
                () => driver.LocateAndTrySetKeyValue(
                    HLCTimestamp.Zero, key, V(key), null, -1, KeyValueFlags.Set, 0,
                    KeyValueDurability.Persistent, ct),
                r => r.Item1);

            Assert.Equal(KeyValueResponseType.Set, type);
        }

        return (rafts, managers, driver, sourcePartition);
    }

    /// <summary>
    /// Reads <paramref name="key"/> back through every node's locator, absorbing the
    /// <c>MustRetry</c> a freshly-created destination partition answers while its Raft group
    /// elects. Returns the first node that cannot produce the value, so a write that survived on
    /// one node but not through the routed path is still reported.
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
    /// The deterministic half of the reproduction: inside the quiesce window (after the catch-up
    /// export, before the cutover) one direct write is issued from <b>every</b> node into the
    /// moving half. Being refused as <c>MustRetry</c> is correct — the client retries onto the
    /// child after cutover. Being acknowledged as <c>Set</c> is correct only if the value is still
    /// readable afterwards; anything else is an acknowledged write the split silently dropped.
    /// </summary>
    [Fact]
    public async Task Split_WriteFromEveryNodeInsideQuiesce_IsRefusedOrSurvives()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, KahunaManager[] kahunas, KahunaManager driver, _) = await Setup(ct);

        List<(string Key, KeyValueResponseType Type, string Node)> attempts = [];

        SplitOutcome outcome = SplitOutcome.PartitionCreationFailed;

        for (int attempt = 0; attempt < 5; attempt++)
        {
            attempts.Clear();

            (_, driver) = await LeaderOf(RangeMapStore.MetaPartitionId, rafts, kahunas, ct);

            int round = attempt;

            outcome = await driver.ForceSplitAtKeyAsync(
                Space, SplitKey,
                duringQuiesce: async () =>
                {
                    for (int i = 0; i < kahunas.Length; i++)
                    {
                        string key = $"{Space}/p{round}-n{i}";

                        (KeyValueResponseType type, _, _) = await kahunas[i].LocateAndTrySetKeyValue(
                            HLCTimestamp.Zero, key, V(key), null, -1, KeyValueFlags.Set, 0,
                            KeyValueDurability.Persistent, ct);

                        attempts.Add((key, type, rafts[i].GetLocalEndpoint()));
                    }
                },
                ct);

            if (outcome.IsSuccess || outcome.Status is SplitStatus.NoRange or SplitStatus.InvalidSplitKey
                or SplitStatus.BelowMinRangeSize)
                break;

            await Task.Delay(100, ct);
        }

        Assert.True(outcome.IsSuccess, $"Split failed: {outcome.Status}");

        List<string> lost = [];

        foreach ((string key, KeyValueResponseType type, string node) in attempts)
        {
            if (type != KeyValueResponseType.Set)
                continue; // refused in-window — the client retries onto the child.

            string? failure = await FindReadbackFailureAsync(kahunas, key, ct);

            if (failure is not null)
                lost.Add($"{failure} (acknowledged on {node})");
        }

        Assert.True(lost.Count == 0,
            "Writes acknowledged inside the split quiesce are unreadable after cutover: " + string.Join(", ", lost));
    }

    // ── the quiesce is replicated, not local to the split executor ───────────────

    /// <summary>
    /// Inside the quiesce window every node's range map — not just the executor's — must show the
    /// moving range as refusing writes, and once the split completes no node may still show it.
    /// This is what distinguishes the replicated quiesce from the executor-local guard it replaced:
    /// the reproduction tests above pass either way, because the source partition leader's in-memory
    /// range lock already refuses the writes they issue.
    /// </summary>
    [Fact]
    public async Task Split_QuiesceWindow_IsVisibleOnEveryNodeAndClearsAfterCutover()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, KahunaManager[] kahunas, KahunaManager driver, _) = await Setup(ct);

        SplitOutcome outcome = SplitOutcome.PartitionCreationFailed;

        for (int attempt = 0; attempt < 5; attempt++)
        {
            (_, driver) = await LeaderOf(RangeMapStore.MetaPartitionId, rafts, kahunas, ct);

            outcome = await driver.ForceSplitAtKeyAsync(
                Space, SplitKey,
                duringQuiesce: async () =>
                {
                    foreach (KahunaManager kahuna in kahunas)
                    {
                        KahunaManager observer = kahuna;

                        // The map reaches followers a replication round after the executor commits it,
                        // so poll rather than sample — the window is held open by this hook.
                        await WaitUntilAsync(() =>
                        {
                            RangeDescriptor? moving = observer.RangeMapStore.Current.Find(Space, Space + "/z0");
                            return moving is not null && moving.QuiescedUntil != HLCTimestamp.Zero;
                        }, timeoutMs: 15_000);
                    }
                },
                ct);

            if (outcome.IsSuccess || outcome.Status is SplitStatus.NoRange or SplitStatus.InvalidSplitKey
                or SplitStatus.BelowMinRangeSize)
                break;

            await Task.Delay(100, ct);
        }

        Assert.True(outcome.IsSuccess, $"Split failed: {outcome.Status}");

        foreach (KahunaManager kahuna in kahunas)
        {
            KahunaManager observer = kahuna;

            await WaitUntilAsync(
                () => observer.RangeMapStore.Current.FindAll(Space)
                    .All(d => d.QuiescedUntil == HLCTimestamp.Zero),
                timeoutMs: 15_000);
        }
    }

    // ── enforcement without any in-memory range lock ─────────────────────────────

    /// <summary>
    /// The state a source partition is left in when its leader changes mid-move: the range is
    /// quiesced, but no node holds the in-memory range lock any more, because range locks are
    /// leader-local and are not reconstructed on promotion. Reproduced directly by publishing a
    /// quiesce with no split running at all.
    ///
    /// <para>
    /// Delete and extend are asserted alongside set deliberately. Only <c>TrySet</c> is bounced
    /// before routing, so a refusal of the other two can have come from nowhere but the gate on the
    /// partition leader at admission — the layer that has to hold when the lock is gone.
    /// </para>
    /// </summary>
    [Fact]
    public async Task QuiescedRange_WithNoRangeLockHeld_RefusesWritesFromEveryNode()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, KahunaManager[] kahunas, KahunaManager driver, _) = await Setup(ct);

        HLCTimestamp owner = rafts[0].HybridLogicalClock.TrySendOrLocalEvent(rafts[0].GetLocalNodeId());
        HLCTimestamp until = rafts[0].HybridLogicalClock.TrySendOrLocalEvent(rafts[0].GetLocalNodeId()) + 60_000;

        Assert.True(await driver.RangeMapStore.QuiesceRangeAsync(Space, null, null, owner, until, ct));

        foreach (KahunaManager kahuna in kahunas)
        {
            KahunaManager observer = kahuna;
            await WaitUntilAsync(
                () => observer.RangeMapStore.Current.Find(Space, Space + "/z0")?.QuiescedUntil != HLCTimestamp.Zero,
                timeoutMs: 15_000);
        }

        for (int i = 0; i < kahunas.Length; i++)
        {
            (KeyValueResponseType setType, _, _) = await kahunas[i].LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, $"{Space}/q{i}", V("blocked"), null, -1, KeyValueFlags.Set, 0,
                KeyValueDurability.Persistent, ct);

            (KeyValueResponseType deleteType, _, _) = await kahunas[i].LocateAndTryDeleteKeyValue(
                HLCTimestamp.Zero, Space + "/z0", KeyValueDurability.Persistent, ct);

            (KeyValueResponseType extendType, _, _) = await kahunas[i].LocateAndTryExtendKeyValue(
                HLCTimestamp.Zero, Space + "/z0", 60_000, KeyValueDurability.Persistent, ct);

            Assert.Equal(KeyValueResponseType.MustRetry, setType);
            Assert.Equal(KeyValueResponseType.MustRetry, deleteType);
            Assert.Equal(KeyValueResponseType.MustRetry, extendType);
        }

        // A release stamped by someone else leaves the window open — otherwise a straggling release
        // from an abandoned move could reopen the window a live one is relying on.
        HLCTimestamp stranger = rafts[0].HybridLogicalClock.TrySendOrLocalEvent(rafts[0].GetLocalNodeId());
        await driver.RangeMapStore.ReleaseQuiesceAsync(stranger, ct);

        (KeyValueResponseType stillRefused, _, _) = await kahunas[1].LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, Space + "/q-still", V("blocked"), null, -1, KeyValueFlags.Set, 0,
            KeyValueDurability.Persistent, ct);

        Assert.Equal(KeyValueResponseType.MustRetry, stillRefused);

        // The owner's release reopens it.
        Assert.True(await driver.RangeMapStore.ReleaseQuiesceAsync(owner, ct));

        (KeyValueResponseType afterRelease, _, _) = await RetryOnMustRetryAsync(
            () => kahunas[1].LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, Space + "/q-after", V("open"), null, -1, KeyValueFlags.Set, 0,
                KeyValueDurability.Persistent, ct),
            r => r.Item1, timeoutMs: 30_000);

        Assert.Equal(KeyValueResponseType.Set, afterRelease);
    }

    /// <summary>
    /// A quiesce whose owner never comes back must not wedge the range: the deadline ends the window
    /// with nobody releasing it. Without that, a split executor that dies between publishing the
    /// quiesce and cutting over leaves its range permanently refusing writes.
    /// </summary>
    [Fact]
    public async Task QuiescedRange_WithNoOneToReleaseIt_ReopensWhenTheDeadlineLapses()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, KahunaManager[] kahunas, KahunaManager driver, _) = await Setup(ct);

        HLCTimestamp owner = rafts[0].HybridLogicalClock.TrySendOrLocalEvent(rafts[0].GetLocalNodeId());
        HLCTimestamp until = rafts[0].HybridLogicalClock.TrySendOrLocalEvent(rafts[0].GetLocalNodeId()) + 2_000;

        Assert.True(await driver.RangeMapStore.QuiesceRangeAsync(Space, null, null, owner, until, ct));

        foreach (KahunaManager kahuna in kahunas)
        {
            KahunaManager observer = kahuna;
            await WaitUntilAsync(
                () => observer.RangeMapStore.Current.Find(Space, Space + "/z0")?.QuiescedUntil != HLCTimestamp.Zero,
                timeoutMs: 15_000);
        }

        (KeyValueResponseType refused, _, _) = await kahunas[2].LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, Space + "/q-deadline", V("blocked"), null, -1, KeyValueFlags.Set, 0,
            KeyValueDurability.Persistent, ct);

        Assert.Equal(KeyValueResponseType.MustRetry, refused);

        // Nothing releases it — the deadline does. The descriptor still carries the stale quiesce.
        (KeyValueResponseType afterDeadline, _, _) = await RetryOnMustRetryAsync(
            () => kahunas[2].LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, Space + "/q-deadline", V("open"), null, -1, KeyValueFlags.Set, 0,
                KeyValueDurability.Persistent, ct),
            r => r.Item1, timeoutMs: 30_000);

        Assert.Equal(KeyValueResponseType.Set, afterDeadline);

        Assert.NotEqual(
            HLCTimestamp.Zero,
            driver.RangeMapStore.Current.Find(Space, Space + "/z0")!.QuiescedUntil);
    }

    // ── continuous writes from every node across the whole split ─────────────────

    /// <summary>
    /// The realistic half: every node writes continuously into the moving half for the whole
    /// duration of the split — through the bulk copy, the quiesce window and the cutover — and
    /// every acknowledged key is read back afterwards. This reaches windows the in-window seam
    /// cannot, such as a write admitted just before the quiesce arrives whose replication completes
    /// after the catch-up export.
    /// </summary>
    [Fact]
    public async Task Split_ContinuousWritesFromEveryNode_LoseNoAcknowledgedWrite()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, KahunaManager[] kahunas, KahunaManager driver, _) = await Setup(ct);

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
                    string key = $"{Space}/p{nodeIndex}-{seq:D4}";

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

        // Let the writers get going so the bulk copy and the quiesce both land mid-stream.
        await Task.Delay(300, ct);

        SplitOutcome outcome = SplitOutcome.PartitionCreationFailed;

        for (int attempt = 0; attempt < 5; attempt++)
        {
            (_, driver) = await LeaderOf(RangeMapStore.MetaPartitionId, rafts, kahunas, ct);

            outcome = await driver.ForceSplitAtKeyAsync(Space, SplitKey, null, ct);

            if (outcome.IsSuccess || outcome.Status is SplitStatus.NoRange or SplitStatus.InvalidSplitKey
                or SplitStatus.BelowMinRangeSize)
                break;

            await Task.Delay(100, ct);
        }

        await writerStop.CancelAsync();

        foreach (Task writer in writers)
        {
            try { await writer; }
            catch (OperationCanceledException) { /* expected on stop */ }
        }

        Assert.True(outcome.IsSuccess, $"Split failed: {outcome.Status}");

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
            $"{lost.Count} of {snapshot.Count} acknowledged writes are unreadable after the split: "
            + string.Join(", ", lost.Take(20)));
    }
}
