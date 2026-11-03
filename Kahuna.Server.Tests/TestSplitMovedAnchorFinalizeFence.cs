using System.Text;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// A durable commit refused at the partition leader's dispatch fence (the coordinator routed on a map that had
/// not applied the quiesce yet) keeps its frozen finalize input: the attempt may have left a proposal in flight,
/// so every later conclusion (a retry under the same handle, a rollback, the reaper) fences through the record
/// before it answers. The fence replicates the abort with the anchor key's frozen range generation. Once the
/// split cuts over, the anchor key routes to the child partition at a new generation, so a fence stamped with the
/// old one is released at dispatch every time it is sent. Nothing can conclude the transaction then: its claimed
/// write intents stay on every key it touched, on the half that moved and on the half that did not, until the
/// session liveness ceiling, and a snapshot read of a staying key waits behind them for that long.
///
/// <para>These tests freeze that interleaving: stage a write in the moving half first (it becomes the record
/// anchor) and one in the staying half, meet the dispatch fence inside the quiesce window, abandon the attempt,
/// let the split cut over, and require that the transaction can still be concluded and that the staying key is
/// readable at a snapshot right after.</para>
/// </summary>
public sealed class TestSplitMovedAnchorFinalizeFence : BaseCluster
{
    private const int Partitions = 6;

    /// <summary>The ranged space that is split; its moving half carries the record anchor.</summary>
    private const string Space = "xf:m";

    private const string SplitKey = Space + "/m";

    /// <summary>Written first, so it is the transaction's record anchor; lives in the moving half [SplitKey, +inf).</summary>
    private const string AnchorKey = Space + "/z-anchor";

    /// <summary>Written second; lives in the staying half, on the partition the split leaves alone.</summary>
    private const string StayingKey = Space + "/a-stay";

    private readonly Microsoft.Extensions.Logging.ILogger<IRaft> raftLogger;

    private readonly Microsoft.Extensions.Logging.ILogger<IKahuna> kahunaLogger;

    public TestSplitMovedAnchorFinalizeFence(ITestOutputHelper outputHelper)
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

    private async Task<(IRaft[] Rafts, KahunaManager[] Kahunas, KahunaManager Driver)> Setup(CancellationToken ct)
    {
        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleCluster(
            3, "memory", Partitions, raftLogger, kahunaLogger, replicationFactor: 0);

        KahunaManager[] managers = [.. kahunas.Cast<KahunaManager>()];

        foreach (KahunaManager kahuna in managers)
            kahuna.RegisterKeyRange(Space);

        (IRaft driverRaft, KahunaManager driver) = await LeaderOf(RangeMapStore.MetaPartitionId, rafts, managers, ct);

        // Seed the ranged space on a data partition the driver does not lead, so the commit's prepares and the
        // split's hand-off cross nodes the way they do in a cluster.
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

        // Both written keys plus keys on both sides of the split point, so the splitter's halves probe passes.
        foreach (string key in new[] { StayingKey, Space + "/a0", Space + "/n0", Space + "/n1", AnchorKey, Space + "/z1" })
        {
            (KeyValueResponseType type, _, _) = await RetryOnMustRetryAsync(
                () => driver.LocateAndTrySetKeyValue(
                    HLCTimestamp.Zero, key, V("base"), null, -1, KeyValueFlags.Set, 0,
                    KeyValueDurability.Persistent, ct),
                r => r.Item1);

            Assert.Equal(KeyValueResponseType.Set, type);
        }

        return (rafts, managers, driver);
    }

    /// <summary>
    /// Stages the anchor write and the staying write in one durable session, then drives its commit into the
    /// quiesce window of a split of the space with the coordinator's own quiesce gate switched off, so the
    /// prepare reaches the partition leader and its dispatch fence refuses it. The attempt is abandoned as
    /// refused, the quiesce is released and the split cuts over. Returns the abandoned handle.
    /// </summary>
    private static async Task<TransactionHandle> AbandonACommitAtTheDispatchFence_ThenCutOver(
        IRaft[] rafts, KahunaManager[] kahunas, KahunaManager session, KeyValueTransactionLocking locking, int timeoutMs, CancellationToken ct)
    {
        (KeyValueResponseType started, TransactionHandle handle) = await session.LocateAndStartTransaction(
            new KeyValueTransactionOptions
            {
                Locking = locking,
                DecisionDurability = DecisionDurability.Durable,
                Timeout = timeoutMs
            }, ct);
        Assert.Equal(KeyValueResponseType.Set, started);

        foreach (string key in new[] { AnchorKey, StayingKey })
        {
            (KeyValueResponseType set, _, _) = await RetryOnMustRetryAsync(
                () => session.LocateAndTrySetKeyValue(
                    handle.TransactionId, key, V("attempt"), null, -1, KeyValueFlags.Set, 0,
                    KeyValueDurability.Persistent, ct, 0, handle.CoordinatorKey, TransactionOperationId.NewRandom()),
                r => r.Item1);
            Assert.Equal(KeyValueResponseType.Set, set);
        }

        foreach (KahunaManager kahuna in kahunas)
            kahuna.KeyValues.Coordinator.TestSkipRangeQuiesceAdmissionGate = true;

        try
        {
            TransactionCommitAnswer refused = default;
            bool commitRan = false;

            (_, KahunaManager splitDriver) = await LeaderOf(RangeMapStore.MetaPartitionId, rafts, kahunas, ct);
            SplitOutcome outcome = await splitDriver.ForceSplitAtKeyAsync(Space, SplitKey, async () =>
            {
                // Inside the quiesce window: the anchor's prepare meets the leader's dispatch fence.
                refused = await session.LocateAndCommitTransaction(handle, ct);
                commitRan = true;
            }, ct);

            Assert.True(outcome.IsSuccess, $"Split failed: {outcome.Status}");
            Assert.True(commitRan, "the quiesce seam did not run");
            Assert.True(refused.Type == KeyValueResponseType.MustRetry,
                $"the commit inside the quiesce window answered {refused.Type} ({refused.Refusal}: {refused.Reason}) instead of a retryable refusal");
        }
        finally
        {
            foreach (KahunaManager kahuna in kahunas)
                kahuna.KeyValues.Coordinator.TestSkipRangeQuiesceAdmissionGate = false;
        }

        // The anchor routes to the child partition at a new generation; the staying key kept its partition.
        RangeDescriptor anchorNow = kahunas[0].RangeMapStore.Current.Find(Space, AnchorKey)!;
        RangeDescriptor stayNow = kahunas[0].RangeMapStore.Current.Find(Space, StayingKey)!;
        Assert.NotEqual(stayNow.PartitionId, anchorNow.PartitionId);
        Assert.True(anchorNow.Generation > 1, $"anchor descriptor still at generation {anchorNow.Generation}");

        return handle;
    }

    /// <summary>A latest read and a statement-snapshot read at the node's own clock, each retried on MustRetry
    /// inside the usual budget; returns the values, null where the read did not answer a value.</summary>
    private static async Task<(string? Latest, string? Snapshot)> ReadBothWays(IRaft raft, KahunaManager kahuna, string key, CancellationToken ct)
    {
        (KeyValueResponseType latestType, ReadOnlyKeyValueEntry? latest) = await RetryOnMustRetryAsync(
            () => kahuna.LocateAndTryGetValue(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct),
            r => r.Item1);

        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        (KeyValueResponseType snapshotType, ReadOnlyKeyValueEntry? snapshot) = await RetryOnMustRetryAsync(
            () => kahuna.LocateAndTryGetValue(HLCTimestamp.Zero, key, -1, now, KeyValueDurability.Persistent, ct, readMode: KeyValueReadMode.StatementSnapshot),
            r => r.Item1);

        return (
            latestType == KeyValueResponseType.Get && latest?.Value is not null ? Encoding.UTF8.GetString(latest.Value) : null,
            snapshotType == KeyValueResponseType.Get && snapshot?.Value is not null ? Encoding.UTF8.GetString(snapshot.Value) : null);
    }

    private static async Task AssertReadableEverywhere(IRaft[] rafts, KahunaManager[] kahunas, string key, string expected, CancellationToken ct)
    {
        for (int i = 0; i < kahunas.Length; i++)
        {
            (string? latest, string? snapshot) = await ReadBothWays(rafts[i], kahunas[i], key, ct);
            Assert.True(latest == expected, $"{key} on node {i}: latest read answered {latest ?? "no value"}, expected {expected}");
            Assert.True(snapshot == expected, $"{key} on node {i}: snapshot read at now answered {snapshot ?? "no value"}, expected {expected}");
        }
    }

    /// <summary>
    /// The client gives up on the refused attempt and rolls back after the split cut over. The rollback must
    /// conclude (RolledBack), and right after it both keys must answer their prior value to a latest read and to
    /// a snapshot read at the node's own clock: the staying key's claimed intent is gone with the rollback.
    /// </summary>
    [Theory]
    [InlineData(KeyValueTransactionLocking.Optimistic)]
    [InlineData(KeyValueTransactionLocking.Pessimistic)]
    public async Task AbandonedCommit_RefusedAtTheDispatchFence_RollsBackAfterTheAnchorRangeMoved(KeyValueTransactionLocking locking)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, KahunaManager[] kahunas, _) = await Setup(ct);

        TransactionHandle handle = await AbandonACommitAtTheDispatchFence_ThenCutOver(rafts, kahunas, kahunas[1], locking, 30_000, ct);

        KeyValueResponseType rolledBack = await RetryOnMustRetryAsync(
            () => kahunas[1].LocateAndRollbackTransaction(handle, ct), r => r);
        Assert.Equal(KeyValueResponseType.RolledBack, rolledBack);

        await AssertReadableEverywhere(rafts, kahunas, StayingKey, "base", ct);
        await AssertReadableEverywhere(rafts, kahunas, AnchorKey, "base", ct);
    }

    /// <summary>
    /// The client retries the refused commit under the same handle after the split cut over, as the retryable
    /// answer instructs. The frozen input can never prepare on the partition its anchor left, so the retry must
    /// end terminally instead of answering MustRetry until the session deadline; whichever way it ends, both keys
    /// must agree with it and answer a snapshot read right after.
    /// </summary>
    [Theory]
    [InlineData(KeyValueTransactionLocking.Optimistic)]
    [InlineData(KeyValueTransactionLocking.Pessimistic)]
    public async Task AbandonedCommit_RefusedAtTheDispatchFence_RetriedAfterTheAnchorRangeMoved_EndsTerminally(KeyValueTransactionLocking locking)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, KahunaManager[] kahunas, _) = await Setup(ct);

        TransactionHandle handle = await AbandonACommitAtTheDispatchFence_ThenCutOver(rafts, kahunas, kahunas[1], locking, 30_000, ct);

        TransactionCommitAnswer retried = await RetryOnMustRetryAsync(
            () => kahunas[1].LocateAndCommitTransaction(handle, ct), a => a.Type);
        Assert.True(retried.Type is KeyValueResponseType.Committed or KeyValueResponseType.Aborted,
            $"the retry under the same handle answered {retried.Type} ({retried.Refusal}: {retried.Reason})");

        string expected = retried.Type == KeyValueResponseType.Committed ? "attempt" : "base";
        await AssertReadableEverywhere(rafts, kahunas, StayingKey, expected, ct);
        await AssertReadableEverywhere(rafts, kahunas, AnchorKey, expected, ct);
    }

    /// <summary>
    /// The client never returns. The reaper claims the session past its deadline and must conclude it: after the
    /// sweep that runs past the deadline, both keys answer their prior value to a snapshot read on every node,
    /// which proves the claimed intents were released rather than left to the session liveness ceiling.
    /// </summary>
    [Fact]
    public async Task AbandonedCommit_RefusedAtTheDispatchFence_IsReapedAfterTheAnchorRangeMoved()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, KahunaManager[] kahunas, _) = await Setup(ct);

        const int timeoutMs = 1_000;
        await AbandonACommitAtTheDispatchFence_ThenCutOver(rafts, kahunas, kahunas[1], KeyValueTransactionLocking.Optimistic, timeoutMs, ct);

        // Past the session deadline and the reaper's grace, a sweep on every node reaps the abandoned session
        // wherever its coordinator lives.
        await Task.Delay(timeoutMs + TransactionCoordinator.ReapGraceMs + 500, ct);

        foreach (KahunaManager kahuna in kahunas)
            await kahuna.KeyValues.ReapAbandonedSessions();

        await AssertReadableEverywhere(rafts, kahunas, StayingKey, "base", ct);
        await AssertReadableEverywhere(rafts, kahunas, AnchorKey, "base", ct);
    }
}
