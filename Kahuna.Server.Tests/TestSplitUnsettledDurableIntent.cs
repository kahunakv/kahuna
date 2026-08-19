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
/// Under deferred settlement a committed transaction's value can linger as a prepared intent — decided
/// (its canonical record says Commit) but not yet materialized into any base row. Every read path serves
/// such a value through the intent overlay, so the commit is visible cluster-wide even though the backend
/// still holds the prior revision.
///
/// <para>
/// A range split copies base rows and hands the moving range's intents to the destination partition. If
/// either half of that contract drops the committed-but-unsettled value, the child range serves the prior
/// revision after cutover: a write that was acknowledged and observed becomes invisible — the read-skew /
/// lost-update shape Jepsen caught with no faults injected. These tests freeze exactly that state (a
/// pending intent plus its terminal Commit record, replicated through the real channels) and require the
/// value to survive a split of its range.
/// </para>
/// </summary>
public sealed class TestSplitUnsettledDurableIntent : BaseCluster
{
    private const int Partitions = 6;

    private const string Space = "xn:u";

    private const string SplitKey = Space + "/m";

    /// <summary>Lives in the moving half [SplitKey, +inf) of the seeded whole-space range.</summary>
    private const string MovedKey = Space + "/z0";

    private readonly Microsoft.Extensions.Logging.ILogger<IRaft> raftLogger;

    private readonly Microsoft.Extensions.Logging.ILogger<IKahuna> kahunaLogger;

    public TestSplitUnsettledDurableIntent(ITestOutputHelper outputHelper)
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
    /// Assembles the cluster (legacy full replication by default — the configuration the Jepsen control
    /// job runs), registers the ranged space, seeds one whole-space descriptor on a data partition whose
    /// leader is not the split driver, and seeds keys on both sides of <see cref="SplitKey"/> so the
    /// splitter's non-empty-halves probe passes.
    /// </summary>
    private async Task<(IRaft[] Rafts, KahunaManager[] Kahunas, KahunaManager Driver, int SourcePartition)> Setup(
        int replicationFactor, CancellationToken ct)
    {
        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleCluster(
            3, "memory", Partitions, raftLogger, kahunaLogger, replicationFactor);

        KahunaManager[] managers = [.. kahunas.Cast<KahunaManager>()];

        foreach (KahunaManager kahuna in managers)
            kahuna.RegisterKeyRange(Space);

        (IRaft driverRaft, KahunaManager driver) = await LeaderOf(RangeMapStore.MetaPartitionId, rafts, managers, ct);

        // The source partition's leader must be another node, mirroring the incident: the split
        // executor exports the moving range from its own (non-leader) view of the partition.
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

        foreach (string key in new[] { Space + "/a0", Space + "/a1", MovedKey, Space + "/z1" })
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
    /// Freezes the committed-but-unsettled state for <see cref="MovedKey"/>: a pending prepared intent
    /// (revision base+1, value <paramref name="value"/>) replicated through the source partition, and its
    /// transaction's terminal Commit record replicated through the anchor key's partition — exactly the
    /// durable state a deferred-settlement commit leaves behind until resolution materializes it.
    /// </summary>
    private static async Task<HLCTimestamp> FreezeCommittedUnsettledIntent(
        KahunaManager driver, KahunaManager[] managers, IRaft[] rafts,
        int sourcePartition, byte[] value, CancellationToken ct)
    {
        HLCTimestamp txId = rafts[0].HybridLogicalClock.TrySendOrLocalEvent(rafts[0].GetLocalNodeId());
        HLCTimestamp commitTs = rafts[0].HybridLogicalClock.TrySendOrLocalEvent(rafts[0].GetLocalNodeId());

        // Base revision of the seeded row (a single direct set ⇒ revision 0).
        (KeyValueResponseType baseType, ReadOnlyKeyValueEntry? baseEntry) = await RetryOnMustRetryAsync(
            () => driver.LocateAndTryGetValue(
                HLCTimestamp.Zero, MovedKey, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct),
            r => r.Item1);
        Assert.Equal(KeyValueResponseType.Get, baseType);
        long baseRevision = baseEntry!.Revision;

        const string anchorKey = "xn-tx/anchor-1";   // hash-routed: intentionally outside the ranged space

        PreparedIntent intent = new(
            TransactionId: txId, Epoch: 1, Key: MovedKey, ManifestHash: 42, RecordAnchorKey: anchorKey,
            CommitTimestamp: commitTs, State: KeyValueState.Set, Value: value, Bucket: Space,
            Revision: baseRevision + 1, Expires: HLCTimestamp.Zero, NoRevision: false,
            BaseRevision: baseRevision, BaseState: KeyValueState.Set,
            // Far-future recovery deadline so the periodic sweep cannot settle it mid-test: the split
            // itself must preserve (or settle) the value.
            RecoveryDeadline: new HLCTimestamp(0, long.MaxValue, 0),
            Resolution: PreparedIntentResolution.Pending);

        Assert.True(await driver.KeyValues.ImportDurableTransactionStateToPartitionLeaderAsync(
            sourcePartition, Array.Empty<TransactionRecord>(), new List<PreparedIntent> { intent }, ct));

        // Build the terminal Commit record through the real state machine, then replicate it onto the
        // anchor key's partition so every node resolves the intent's decision locally.
        TransactionRecordStore scratch = new();
        scratch.Apply(new InitializeTransactionCommand(
            txId, 1, "xn-coord", anchorKey, commitTs,
            new HLCTimestamp(0, long.MaxValue, 0), 42,
            [new TransactionParticipantRef(MovedKey, KeyValueDurability.Persistent)],
            HLCTimestamp.Zero, commitTs));
        scratch.Apply(new CommitTransactionCommand(txId, 1, 42, commitTs, commitTs));

        int anchorPartition = driver.KeyValues.LocateDurablePartition(anchorKey).PartitionId;
        Assert.True(await driver.KeyValues.ImportDurableTransactionStateToPartitionLeaderAsync(
            anchorPartition, new List<TransactionRecord>(scratch.Snapshot()), Array.Empty<PreparedIntent>(), ct));

        // Wait until the frozen state is visible where the read/copy paths consult it. Under legacy
        // full replication every node's stores converge; under placement only the partition's
        // replicas do — so wait on any node that reports the state rather than on all of them.
        await WaitUntilAsync(
            () => managers.Any(m => m.DurablePreparedIntentStore.Get(MovedKey) is { } i && i.TransactionId == txId)
                  && managers.Any(m => m.DurableTransactionRecordStore.Get(txId, 1) is { Decision: TransactionDecision.Commit }),
            timeoutMs: 30_000);

        return txId;
    }

    /// <summary>Reads <see cref="MovedKey"/> through every node's locator and returns the first mismatch.</summary>
    private static async Task<string?> FindVisibilityFailureAsync(
        KahunaManager[] kahunas, IRaft[] rafts, byte[] expected, CancellationToken ct)
    {
        for (int i = 0; i < kahunas.Length; i++)
        {
            (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await RetryOnMustRetryAsync(
                () => kahunas[i].LocateAndTryGetValue(
                    HLCTimestamp.Zero, MovedKey, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct),
                r => r.Item1, timeoutMs: 30_000);

            string? value = entry?.Value is null ? null : Encoding.UTF8.GetString(entry.Value);

            if (type != KeyValueResponseType.Get || !V(Encoding.UTF8.GetString(expected)).AsSpan().SequenceEqual(entry?.Value))
                return $"node {rafts[i].GetLocalEndpoint()} → {type}/{value ?? "null"}";
        }

        return null;
    }

    private async Task RunScenario(int replicationFactor, CancellationToken ct)
    {
        (IRaft[] rafts, KahunaManager[] kahunas, KahunaManager driver, int sourcePartition) =
            await Setup(replicationFactor, ct);

        byte[] committedValue = V("committed-but-unsettled");

        await FreezeCommittedUnsettledIntent(driver, kahunas, rafts, sourcePartition, committedValue, ct);

        // Sanity: with the intent + record in place, every node already serves the committed value.
        string? preSplit = await FindVisibilityFailureAsync(kahunas, rafts, committedValue, ct);
        Assert.True(preSplit is null, $"Committed-but-unsettled value invisible before the split: {preSplit}");

        SplitOutcome outcome = SplitOutcome.PartitionCreationFailed;

        for (int attempt = 0; attempt < 10; attempt++)
        {
            (_, driver) = await LeaderOf(RangeMapStore.MetaPartitionId, rafts, kahunas, ct);

            outcome = await driver.ForceSplitAtKeyAsync(Space, SplitKey, null, ct);

            if (outcome.IsSuccess || outcome.Status is SplitStatus.NoRange or SplitStatus.InvalidSplitKey
                or SplitStatus.BelowMinRangeSize)
                break;

            await Task.Delay(200, ct);
        }

        Assert.True(outcome.IsSuccess, $"Split failed: {outcome.Status}");

        // The acknowledged, previously-visible committed value must still be served after the range moved.
        string? postSplit = await FindVisibilityFailureAsync(kahunas, rafts, committedValue, ct);
        Assert.True(postSplit is null,
            $"Committed-but-unsettled value lost by the split (read skew / lost update): {postSplit}");

        // The split's pre-cutover barrier settles decided intents rather than moving them: the value must be
        // a real materialized row now, with no lingering intent anywhere for the recovery sweep to chase.
        foreach (KahunaManager manager in kahunas)
        {
            KahunaManager observer = manager;
            await WaitUntilAsync(() => observer.DurablePreparedIntentStore.Get(MovedKey) is null, timeoutMs: 15_000);
        }
    }

    /// <summary>The Jepsen control-job configuration: legacy full replication, split copy reads the
    /// executor's local view of the source partition.</summary>
    [Fact]
    public Task Split_CommittedButUnsettledIntent_SurvivesRangeMove_LegacyReplication() =>
        RunScenario(replicationFactor: 0, TestContext.Current.CancellationToken);

    /// <summary>The placed configuration: replica sets of one, split copy pages through the source
    /// partition's leader.</summary>
    [Fact]
    public Task Split_CommittedButUnsettledIntent_SurvivesRangeMove_ReplicationFactor1() =>
        RunScenario(replicationFactor: 1, TestContext.Current.CancellationToken);

    /// <summary>
    /// An intent that is still undecided — its coordinator inside its decision window — can be neither
    /// settled nor safely abandoned by a data move. The split must refuse the attempt retryably (the map
    /// untouched) instead of cutting over around it; once the transaction decides, a retried split settles
    /// the intent and proceeds, and the committed value is served from the child range.
    /// </summary>
    [Fact]
    public async Task Split_UndecidedIntentInMovingRange_RefusesUntilDecided()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, KahunaManager[] kahunas, KahunaManager driver, int sourcePartition) =
            await Setup(replicationFactor: 0, ct);

        HLCTimestamp txId = rafts[0].HybridLogicalClock.TrySendOrLocalEvent(rafts[0].GetLocalNodeId());
        HLCTimestamp commitTs = rafts[0].HybridLogicalClock.TrySendOrLocalEvent(rafts[0].GetLocalNodeId());

        const string anchorKey = "xn-tx/anchor-undecided";
        byte[] value = V("undecided-value");

        PreparedIntent intent = new(
            TransactionId: txId, Epoch: 1, Key: MovedKey, ManifestHash: 42, RecordAnchorKey: anchorKey,
            CommitTimestamp: commitTs, State: KeyValueState.Set, Value: value, Bucket: Space,
            Revision: 1, Expires: HLCTimestamp.Zero, NoRevision: false,
            BaseRevision: 0, BaseState: KeyValueState.Set,
            // Far-future recovery deadline: the transaction is inside its decision window, so neither the
            // sweep nor the split's settle barrier may presume-abort it.
            RecoveryDeadline: new HLCTimestamp(0, long.MaxValue, 0),
            Resolution: PreparedIntentResolution.Pending);

        Assert.True(await driver.KeyValues.ImportDurableTransactionStateToPartitionLeaderAsync(
            sourcePartition, Array.Empty<TransactionRecord>(), new List<PreparedIntent> { intent }, ct));

        await WaitUntilAsync(
            () => kahunas.Any(m => m.DurablePreparedIntentStore.Get(MovedKey) is { } i && i.TransactionId == txId),
            timeoutMs: 30_000);

        // No decision exists: the attempt must refuse retryably with the map untouched — either the
        // halves probe (whose scan an undecided intent makes retryable) or the settle barrier says so.
        (_, driver) = await LeaderOf(RangeMapStore.MetaPartitionId, rafts, kahunas, ct);
        SplitOutcome refused = await driver.ForceSplitAtKeyAsync(Space, SplitKey, null, ct);
        Assert.True(
            refused.Status is SplitStatus.UnsettledMovingIntents or SplitStatus.ProbeIndeterminate,
            $"An undecided intent in the moving half must refuse the split retryably; got {refused.Status}");

        // Decide the transaction (Commit) — the retried split now settles the intent and proceeds.
        TransactionRecordStore scratch = new();
        scratch.Apply(new InitializeTransactionCommand(
            txId, 1, "xn-coord", anchorKey, commitTs,
            new HLCTimestamp(0, long.MaxValue, 0), 42,
            [new TransactionParticipantRef(MovedKey, KeyValueDurability.Persistent)],
            HLCTimestamp.Zero, commitTs));
        scratch.Apply(new CommitTransactionCommand(txId, 1, 42, commitTs, commitTs));

        int anchorPartition = driver.KeyValues.LocateDurablePartition(anchorKey).PartitionId;
        Assert.True(await driver.KeyValues.ImportDurableTransactionStateToPartitionLeaderAsync(
            anchorPartition, new List<TransactionRecord>(scratch.Snapshot()), Array.Empty<PreparedIntent>(), ct));

        SplitOutcome outcome = SplitOutcome.PartitionCreationFailed;

        for (int attempt = 0; attempt < 10; attempt++)
        {
            (_, driver) = await LeaderOf(RangeMapStore.MetaPartitionId, rafts, kahunas, ct);

            outcome = await driver.ForceSplitAtKeyAsync(Space, SplitKey, null, ct);

            if (outcome.IsSuccess || outcome.Status is SplitStatus.NoRange or SplitStatus.InvalidSplitKey
                or SplitStatus.BelowMinRangeSize)
                break;

            await Task.Delay(200, ct);
        }

        Assert.True(outcome.IsSuccess, $"Split failed after the transaction decided: {outcome.Status}");

        string? postSplit = await FindVisibilityFailureAsync(kahunas, rafts, value, ct);
        Assert.True(postSplit is null, $"Committed value lost by the split after deciding: {postSplit}");
    }
}
