using System.Text;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.KeyValues.Transactions.Data;
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

    // ── transactional commits into a quiesced range, with no range lock anywhere ──

    /// <summary>Starts a durable pessimistic session through <paramref name="kahuna"/> and stages one write per
    /// key in it. The writes are staged <b>before</b> any quiesce exists: the locator bounces a staged write
    /// into a quiesced range on its own, and these tests are about the commit, not the staging.</summary>
    private static async Task<TransactionHandle> StageSession(KahunaManager kahuna, string[] keys, string value, CancellationToken ct)
    {
        (KeyValueResponseType started, TransactionHandle handle) = await kahuna.LocateAndStartTransaction(
            new KeyValueTransactionOptions
            {
                Locking = KeyValueTransactionLocking.Pessimistic,
                DecisionDurability = DecisionDurability.Durable,
                Timeout = 30_000
            }, ct);
        Assert.Equal(KeyValueResponseType.Set, started);

        foreach (string key in keys)
        {
            (KeyValueResponseType set, _, _) = await RetryOnMustRetryAsync(
                () => kahuna.LocateAndTrySetKeyValue(
                    handle.TransactionId, key, V(value), null, -1, KeyValueFlags.Set, 0,
                    KeyValueDurability.Persistent, ct, 0, handle.CoordinatorKey, TransactionOperationId.NewRandom()),
                r => r.Item1);
            Assert.Equal(KeyValueResponseType.Set, set);
        }

        return handle;
    }

    /// <summary>Publishes a quiesce over <c>[start, end)</c> of the space with no split running and no range
    /// lock held anywhere — the state a source leader promoted mid-move is left in — and waits until every
    /// node's map shows it.</summary>
    private static async Task<HLCTimestamp> QuiesceWithoutLock(
        IRaft[] rafts, KahunaManager[] kahunas, KahunaManager driver, string? start, string? end, string probeKey, CancellationToken ct)
    {
        HLCTimestamp owner = rafts[0].HybridLogicalClock.TrySendOrLocalEvent(rafts[0].GetLocalNodeId());
        HLCTimestamp until = rafts[0].HybridLogicalClock.TrySendOrLocalEvent(rafts[0].GetLocalNodeId()) + 60_000;

        Assert.True(await driver.RangeMapStore.QuiesceRangeAsync(Space, start, end, owner, until, ct));

        foreach (KahunaManager kahuna in kahunas)
        {
            KahunaManager observer = kahuna;
            await WaitUntilAsync(
                () => observer.RangeMapStore.Current.Find(Space, probeKey)?.QuiescedUntil != HLCTimestamp.Zero,
                timeoutMs: 15_000);
        }

        return owner;
    }

    private static async Task AssertValueEverywhere(KahunaManager[] kahunas, string key, string expected, CancellationToken ct)
    {
        foreach (KahunaManager kahuna in kahunas)
        {
            (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await RetryOnMustRetryAsync(
                () => kahuna.LocateAndTryGetValue(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct),
                r => r.Item1, timeoutMs: 30_000);

            Assert.Equal(KeyValueResponseType.Get, type);
            Assert.Equal(expected, Encoding.UTF8.GetString(entry!.Value!));
        }
    }

    private static async Task AssertAbsentEverywhere(KahunaManager[] kahunas, string key, CancellationToken ct)
    {
        foreach (KahunaManager kahuna in kahunas)
        {
            (KeyValueResponseType type, _) = await RetryOnMustRetryAsync(
                () => kahuna.LocateAndTryGetValue(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct),
                r => r.Item1, timeoutMs: 30_000);

            Assert.Equal(KeyValueResponseType.DoesNotExist, type);
        }
    }

    /// <summary>
    /// The transactional counterpart of the direct-write leader-independence test: a durable commit whose write
    /// lands in a quiesced range is refused as retryable from every node while <b>no node holds a range lock</b>.
    /// The in-memory lock is the only thing the commit probe knows about, and a leadership change on the source
    /// partition drops it; the replicated descriptor quiesce is what has to hold then. Each write is staged before
    /// the quiesce is published, so the staging's own pre-route bounce plays no part — only the commit is judged.
    /// The refusal must name the quiesce, nothing may have landed, and the same handles must commit once the owner
    /// reopens the range.
    /// </summary>
    [Fact]
    public async Task QuiescedRange_WithNoRangeLockHeld_RefusesTransactionalCommitsFromEveryNode()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        using MetricCapture refusals = new("reason", "kahuna.durable_tx.range_quiesce_refusals");

        (IRaft[] rafts, KahunaManager[] kahunas, KahunaManager driver, _) = await Setup(ct);

        TransactionHandle[] handles = new TransactionHandle[kahunas.Length];
        for (int i = 0; i < kahunas.Length; i++)
            handles[i] = await StageSession(kahunas[i], [$"{Space}/z-tx{i}"], "blocked", ct);

        HLCTimestamp owner = await QuiesceWithoutLock(rafts, kahunas, driver, null, null, Space + "/z0", ct);

        for (int i = 0; i < kahunas.Length; i++)
        {
            TransactionCommitAnswer answer = await kahunas[i].LocateAndCommitTransaction(handles[i], ct);

            Assert.Equal(KeyValueResponseType.MustRetry, answer.Type);
            Assert.Equal(CommitRefusalClass.RangeQuiesced, answer.Refusal);
        }

        Assert.True(refusals.Total("kahuna.durable_tx.range_quiesce_refusals") >= kahunas.Length);

        // Refused means nothing landed, on any node, through any route.
        for (int i = 0; i < kahunas.Length; i++)
            await AssertAbsentEverywhere(kahunas, $"{Space}/z-tx{i}", ct);

        // The owner's release reopens the range, and the very same handles commit.
        Assert.True(await driver.RangeMapStore.ReleaseQuiesceAsync(owner, ct));

        for (int i = 0; i < kahunas.Length; i++)
        {
            TransactionCommitAnswer committed = await RetryOnMustRetryAsync(
                () => kahunas[i].LocateAndCommitTransaction(handles[i], ct), a => a.Type, timeoutMs: 30_000);

            Assert.Equal(KeyValueResponseType.Committed, committed.Type);
        }

        for (int i = 0; i < kahunas.Length; i++)
            await AssertValueEverywhere(kahunas, $"{Space}/z-tx{i}", "blocked", ct);
    }

    /// <summary>
    /// A split quiesces only the half it moves, and the commit gate judges a transaction by the keys it writes,
    /// not by the descriptor they live in: a commit confined to the staying half goes through during the window,
    /// while a commit that touches both halves is refused whole — its staying-half write must not land either,
    /// since the refusal happens before anything is prepared.
    /// </summary>
    [Fact]
    public async Task QuiescedRange_PartialOverlap_RefusesOnlyCommitsThatTouchTheMovingHalf()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, KahunaManager[] kahunas, KahunaManager driver, _) = await Setup(ct);

        TransactionHandle staying = await StageSession(kahunas[1], [Space + "/a-stay"], "stayed", ct);
        TransactionHandle straddling = await StageSession(kahunas[2], [Space + "/a-both", Space + "/z-both"], "straddled", ct);

        // Quiesce the moving half only: [SplitKey, +inf).
        HLCTimestamp owner = await QuiesceWithoutLock(rafts, kahunas, driver, SplitKey, null, Space + "/z0", ct);

        TransactionCommitAnswer stayed = await RetryOnMustRetryAsync(
            () => kahunas[1].LocateAndCommitTransaction(staying, ct), a => a.Type, timeoutMs: 30_000);
        Assert.Equal(KeyValueResponseType.Committed, stayed.Type);

        TransactionCommitAnswer refused = await kahunas[2].LocateAndCommitTransaction(straddling, ct);
        Assert.Equal(KeyValueResponseType.MustRetry, refused.Type);
        Assert.Equal(CommitRefusalClass.RangeQuiesced, refused.Refusal);

        // Still inside the window: the staying-half write committed, the straddling transaction left nothing.
        Assert.NotEqual(HLCTimestamp.Zero, driver.RangeMapStore.Current.Find(Space, Space + "/z0")!.QuiescedUntil);
        await AssertAbsentEverywhere(kahunas, Space + "/a-both", ct);
        await AssertAbsentEverywhere(kahunas, Space + "/z-both", ct);

        Assert.True(await driver.RangeMapStore.ReleaseQuiesceAsync(owner, ct));

        TransactionCommitAnswer afterRelease = await RetryOnMustRetryAsync(
            () => kahunas[2].LocateAndCommitTransaction(straddling, ct), a => a.Type, timeoutMs: 30_000);
        Assert.Equal(KeyValueResponseType.Committed, afterRelease.Type);

        await AssertValueEverywhere(kahunas, Space + "/a-stay", "stayed", ct);
        await AssertValueEverywhere(kahunas, Space + "/a-both", "straddled", ct);
        await AssertValueEverywhere(kahunas, Space + "/z-both", "straddled", ct);
    }

    /// <summary>
    /// The binding layer on its own: with the coordinator's admission gate switched off on every node, the
    /// prepare reaches the partition leader, and the write scheduler's dispatch fence must release it against
    /// the leader's own map — the check that holds when the coordinator routed on a map that had not applied
    /// the quiesce yet. The release is counted under its own reason, the commit answers retryable, nothing
    /// lands, and the same handle commits once the range reopens.
    /// </summary>
    [Fact]
    public async Task QuiescedRange_CoordinatorGateBypassed_LeaderDispatchFenceRefusesThePrepare()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        using MetricCapture releases = new("reason", "kahuna.kv.write.rejections");

        (IRaft[] rafts, KahunaManager[] kahunas, KahunaManager driver, _) = await Setup(ct);

        foreach (KahunaManager kahuna in kahunas)
            kahuna.KeyValues.Coordinator.TestSkipRangeQuiesceAdmissionGate = true;

        try
        {
            TransactionHandle handle = await StageSession(kahunas[1], [Space + "/z-leader"], "fenced", ct);

            HLCTimestamp owner = await QuiesceWithoutLock(rafts, kahunas, driver, null, null, Space + "/z0", ct);

            TransactionCommitAnswer refused = await kahunas[1].LocateAndCommitTransaction(handle, ct);

            Assert.Equal(KeyValueResponseType.MustRetry, refused.Type);
            Assert.True(releases.Total("kahuna.kv.write.rejections", "range_quiesced") >= 1,
                "the partition leader's dispatch fence did not release the prepare");

            await AssertAbsentEverywhere(kahunas, Space + "/z-leader", ct);

            Assert.True(await driver.RangeMapStore.ReleaseQuiesceAsync(owner, ct));

            TransactionCommitAnswer committed = await RetryOnMustRetryAsync(
                () => kahunas[1].LocateAndCommitTransaction(handle, ct), a => a.Type, timeoutMs: 30_000);
            Assert.Equal(KeyValueResponseType.Committed, committed.Type);

            await AssertValueEverywhere(kahunas, Space + "/z-leader", "fenced", ct);
        }
        finally
        {
            foreach (KahunaManager kahuna in kahunas)
                kahuna.KeyValues.Coordinator.TestSkipRangeQuiesceAdmissionGate = false;
        }
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
