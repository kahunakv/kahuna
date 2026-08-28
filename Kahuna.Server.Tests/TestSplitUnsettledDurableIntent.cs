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

    /// <summary>Freezes an undecided intent on <see cref="MovedKey"/>: a pending prepared intent with no
    /// canonical record, inside its decision window, so nothing may presume its outcome.
    /// <paramref name="commitTimestamp"/> overrides the intent's timestamp — an aged value simulates a
    /// coordinator that has been undecided for that long already.</summary>
    private async Task<HLCTimestamp> FreezeUndecidedIntent(
        KahunaManager driver, KahunaManager[] kahunas, IRaft[] rafts,
        int sourcePartition, string anchorKey, byte[] value, CancellationToken ct,
        HLCTimestamp? commitTimestamp = null)
    {
        HLCTimestamp txId = rafts[0].HybridLogicalClock.TrySendOrLocalEvent(rafts[0].GetLocalNodeId());
        HLCTimestamp commitTs = commitTimestamp ?? rafts[0].HybridLogicalClock.TrySendOrLocalEvent(rafts[0].GetLocalNodeId());

        PreparedIntent intent = new(
            TransactionId: txId, Epoch: 1, Key: MovedKey, ManifestHash: 42, RecordAnchorKey: anchorKey,
            CommitTimestamp: commitTs, State: KeyValueState.Set, Value: value, Bucket: Space,
            Revision: 1, Expires: HLCTimestamp.Zero, NoRevision: false,
            BaseRevision: 0, BaseState: KeyValueState.Set,
            RecoveryDeadline: new HLCTimestamp(0, long.MaxValue, 0),
            Resolution: PreparedIntentResolution.Pending);

        Assert.True(await driver.KeyValues.ImportDurableTransactionStateToPartitionLeaderAsync(
            sourcePartition, Array.Empty<TransactionRecord>(), new List<PreparedIntent> { intent }, ct));

        await WaitUntilAsync(
            () => kahunas.Any(m => m.DurablePreparedIntentStore.Get(MovedKey) is { } i && i.TransactionId == txId),
            timeoutMs: 30_000);

        return txId;
    }

    /// <summary>Writes the terminal Commit record for <paramref name="txId"/> through the real state
    /// machine onto the anchor key's partition, so the intent's decision resolves cluster-wide.</summary>
    private static async Task DecideCommit(
        KahunaManager driver, HLCTimestamp txId, HLCTimestamp commitTs, string anchorKey, CancellationToken ct)
    {
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
    }

    /// <summary>
    /// A tail range's descriptor carries a null end key, which means "to the end of its key
    /// space" — not "+infinity". The split's transaction-state reads (the settle barrier and the
    /// state handoff) run over node-global stores ordered by raw key, so an unbounded end there
    /// sweeps every key space that sorts above the split key: a live intent in a completely
    /// unrelated key space then refuses the attempt, and because a busy cluster always carries a
    /// few live intents somewhere, the first split of every key space starves until the load
    /// stops. The split must ignore foreign key spaces entirely: it completes despite the foreign
    /// undecided intent, and leaves that intent exactly where it was — not settled, not moved.
    /// </summary>
    [Fact]
    public async Task Split_UndecidedIntentInForeignKeySpaceAbove_DoesNotBlockOrMove()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, KahunaManager[] kahunas, KahunaManager driver, int sourcePartition) =
            await Setup(replicationFactor: 0, ct);

        // A key space that sorts ordinally above every key of the split's space ("xn:v..." > "xn:u/...").
        const string foreignSpace = "xn:v";
        const string foreignKey = foreignSpace + "/z0";

        HLCTimestamp foreignTx = rafts[0].HybridLogicalClock.TrySendOrLocalEvent(rafts[0].GetLocalNodeId());
        HLCTimestamp foreignCommitTs = rafts[0].HybridLogicalClock.TrySendOrLocalEvent(rafts[0].GetLocalNodeId());

        // Undecided and inside its decision window: nothing may settle it or presume its outcome,
        // so a barrier that (incorrectly) gathers it can only refuse the split.
        PreparedIntent foreignIntent = new(
            TransactionId: foreignTx, Epoch: 1, Key: foreignKey, ManifestHash: 42,
            RecordAnchorKey: "xn-tx/anchor-foreign",
            CommitTimestamp: foreignCommitTs, State: KeyValueState.Set, Value: V("foreign-undecided"),
            Bucket: foreignSpace,
            Revision: 1, Expires: HLCTimestamp.Zero, NoRevision: false,
            BaseRevision: 0, BaseState: KeyValueState.Set,
            RecoveryDeadline: new HLCTimestamp(0, long.MaxValue, 0),
            Resolution: PreparedIntentResolution.Pending);

        Assert.True(await driver.KeyValues.ImportDurableTransactionStateToPartitionLeaderAsync(
            sourcePartition, Array.Empty<TransactionRecord>(), new List<PreparedIntent> { foreignIntent }, ct));

        await WaitUntilAsync(
            () => kahunas.Any(m => m.DurablePreparedIntentStore.Get(foreignKey) is { } i && i.TransactionId == foreignTx),
            timeoutMs: 30_000);

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

        Assert.True(outcome.IsSuccess,
            $"A foreign key space's undecided intent must not block a split of another key space; got {outcome.Status}");

        // The foreign intent is not part of the moved range: it stays in place, still pending,
        // still owned by its transaction — the split neither settled it nor handed it to the child.
        foreach (KahunaManager manager in kahunas)
        {
            PreparedIntent? survivor = manager.DurablePreparedIntentStore.Get(foreignKey);
            if (survivor is not null)
            {
                Assert.Equal(foreignTx, survivor.TransactionId);
                Assert.Equal(PreparedIntentResolution.Pending, survivor.Resolution);
            }
        }

        Assert.True(
            kahunas.Any(m => m.DurablePreparedIntentStore.Get(foreignKey) is not null),
            "The foreign undecided intent must survive the split untouched");
    }

    /// <summary>
    /// A bounded scan page must not refuse for an undecided intent that lies ordinally past every
    /// row the page returns: the intent cannot affect those rows, and it belongs to the page that
    /// reaches it. Without the clamp, a limit-1 probe of a busy range refuses whenever any
    /// undecided intent exists anywhere in the range — which starves the split's non-empty probe
    /// on exactly the hot ranges a split exists to relieve. A page that does reach the intent must
    /// still refuse.
    /// </summary>
    [Fact]
    public async Task Scan_UndecidedIntentBeyondPage_DoesNotRefuseTheBoundedPage()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, KahunaManager[] kahunas, KahunaManager driver, int sourcePartition) =
            await Setup(replicationFactor: 0, ct);

        await FreezeUndecidedIntent(
            driver, kahunas, rafts, sourcePartition, "xn-tx/anchor-beyond", V("undecided"), ct);

        // Page of one from the space start: rows a0 (returned) and a1 (sentinel). The undecided
        // intent on z0 lies past both and must not refuse the page.
        KeyValueGetByRangeResult bounded = await driver.LocateAndGetByRange(
            HLCTimestamp.Zero, Space, null, true, null, false, 1,
            HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);

        Assert.Equal(KeyValueResponseType.Get, bounded.Type);
        Assert.Single(bounded.Items);
        Assert.Equal(Space + "/a0", bounded.Items[0].Item1);

        // A page that reaches the intent's key still refuses: undecided is undecided.
        KeyValueGetByRangeResult reaching = await driver.LocateAndGetByRange(
            HLCTimestamp.Zero, Space, null, true, null, false, 10,
            HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);

        Assert.Equal(KeyValueResponseType.MustRetry, reaching.Type);
    }

    /// <summary>
    /// An undecided intent whose coordinator has already out-waited a full drain budget predicts a
    /// quiesced drain that stalls to its deadline. The attempt must refuse at the zero-impact
    /// admission gate — before the bulk copy and before the quiesce — not after paying for both:
    /// under sustained load, refused attempts that copy the range and hold the quiesce for the full
    /// drain window are what halve client throughput.
    /// </summary>
    [Fact]
    public async Task Split_StaleUndecidedIntent_RefusesBeforeCopyAndQuiesce()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, KahunaManager[] kahunas, KahunaManager driver, int sourcePartition) =
            await Setup(replicationFactor: 0, ct);

        // Clean keys at the head of the moving half so the probe passes and the refusal below can
        // only come from the admission gate.
        foreach (string probeKey in new[] { Space + "/n0", Space + "/n1" })
        {
            (KeyValueResponseType seedType, _, _) = await RetryOnMustRetryAsync(
                () => driver.LocateAndTrySetKeyValue(
                    HLCTimestamp.Zero, probeKey, V(probeKey), null, -1, KeyValueFlags.Set, 0,
                    KeyValueDurability.Persistent, ct),
                r => r.Item1);
            Assert.Equal(KeyValueResponseType.Set, seedType);
        }

        // An undecided intent aged one minute — far past the 10-second drain budget.
        HLCTimestamp now = rafts[0].HybridLogicalClock.TrySendOrLocalEvent(rafts[0].GetLocalNodeId());
        HLCTimestamp staleCommitTs = new(now.N, now.L - 60_000, now.C);

        await FreezeUndecidedIntent(
            driver, kahunas, rafts, sourcePartition, "xn-tx/anchor-stale", V("stale-undecided"), ct,
            commitTimestamp: staleCommitTs);

        (_, driver) = await LeaderOf(RangeMapStore.MetaPartitionId, rafts, kahunas, ct);

        System.Diagnostics.Stopwatch attemptClock = System.Diagnostics.Stopwatch.StartNew();
        SplitOutcome outcome = await driver.ForceSplitAtKeyAsync(Space, SplitKey, null, ct);
        attemptClock.Stop();

        // The gate refuses with the barrier's retryable status. Before the gate existed this
        // interleaving paid the bulk copy first and failed it instead (TransferFailed).
        Assert.Equal(SplitStatus.UnsettledMovingIntents, outcome.Status);

        // And it refuses without holding anything: no copy, no quiesce, no drain wait.
        Assert.True(attemptClock.Elapsed < TimeSpan.FromSeconds(5),
            $"The gate must refuse before the copy and quiesce; the attempt took {attemptClock.Elapsed}");

        // The range was never quiesced, so a write into the moving half lands immediately.
        (KeyValueResponseType writeType, _, _) = await RetryOnMustRetryAsync(
            () => driver.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, Space + "/z1", V("still-writable"), null, -1, KeyValueFlags.Set, 0,
                KeyValueDurability.Persistent, ct),
            r => r.Item1);
        Assert.Equal(KeyValueResponseType.Set, writeType);
    }

    /// <summary>
    /// The full starvation shape, end to end, in the placed configuration the incident ran under:
    /// an undecided intent sits in the moving half while a single split attempt runs. The probe
    /// passes (clean keys head the half, and the intent lies past the probed page), the copy's
    /// paged reads retry across the undecided window, the settle barrier drains, the transaction
    /// decides while the attempt is in flight, and that one attempt lands — no refusal, no retry
    /// cadence.
    /// </summary>
    [Fact]
    public async Task Split_IntentDecidedMidAttempt_SingleAttemptSucceeds()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, KahunaManager[] kahunas, KahunaManager driver, int sourcePartition) =
            await Setup(replicationFactor: 1, ct);

        // Clean keys at the head of the moving half so the non-empty probe's bounded page (row plus
        // sentinel) never reaches the undecided intent parked on z0.
        foreach (string probeKey in new[] { Space + "/n0", Space + "/n1" })
        {
            (KeyValueResponseType seedType, _, _) = await RetryOnMustRetryAsync(
                () => driver.LocateAndTrySetKeyValue(
                    HLCTimestamp.Zero, probeKey, V(probeKey), null, -1, KeyValueFlags.Set, 0,
                    KeyValueDurability.Persistent, ct),
                r => r.Item1);
            Assert.Equal(KeyValueResponseType.Set, seedType);
        }

        const string anchorKey = "xn-tx/anchor-midattempt";
        byte[] value = V("decided-mid-attempt");

        HLCTimestamp txId = await FreezeUndecidedIntent(
            driver, kahunas, rafts, sourcePartition, anchorKey, value, ct);

        HLCTimestamp intentCommitTs = kahunas
            .Select(m => m.DurablePreparedIntentStore.Get(MovedKey))
            .First(i => i is not null)!.CommitTimestamp;

        (_, driver) = await LeaderOf(RangeMapStore.MetaPartitionId, rafts, kahunas, ct);
        KahunaManager decider = driver;

        // One attempt, started with the intent undecided; the decision lands while it runs.
        Task<SplitOutcome> splitTask = driver.ForceSplitAtKeyAsync(Space, SplitKey, null, ct);

        await Task.Delay(1_200, ct);

        await DecideCommit(decider, txId, intentCommitTs, anchorKey, ct);

        SplitOutcome outcome = await splitTask;
        Assert.True(outcome.IsSuccess,
            $"A single attempt must ride out a decision that lands mid-drain; got {outcome.Status}");

        // The committed value is served from the child range after the cutover.
        string? postSplit = await FindVisibilityFailureAsync(kahunas, rafts, value, ct);
        Assert.True(postSplit is null, $"Committed value lost by the split: {postSplit}");
    }

    /// <summary>
    /// A range under sustained writes always carries a few just-prepared intents whose coordinators
    /// are still deciding. The settle barrier must wait for those decisions and then proceed, not
    /// refuse on first contact — a barrier that never waits refuses every attempt on a hot range
    /// and the split lands only after the load stops. This freezes an undecided intent in the
    /// moving half, starts the barrier, decides the transaction while the barrier is waiting, and
    /// requires that same barrier call to settle the intent and answer clean — after which a split
    /// of the range succeeds.
    /// </summary>
    [Fact]
    public async Task SettleBarrier_IntentDecidedDuringWait_DrainsAndSplitSucceeds()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, KahunaManager[] kahunas, KahunaManager driver, int sourcePartition) =
            await Setup(replicationFactor: 0, ct);

        HLCTimestamp txId = rafts[0].HybridLogicalClock.TrySendOrLocalEvent(rafts[0].GetLocalNodeId());
        HLCTimestamp commitTs = rafts[0].HybridLogicalClock.TrySendOrLocalEvent(rafts[0].GetLocalNodeId());

        const string anchorKey = "xn-tx/anchor-deciding";
        byte[] value = V("decided-while-draining");

        PreparedIntent intent = new(
            TransactionId: txId, Epoch: 1, Key: MovedKey, ManifestHash: 42, RecordAnchorKey: anchorKey,
            CommitTimestamp: commitTs, State: KeyValueState.Set, Value: value, Bucket: Space,
            Revision: 1, Expires: HLCTimestamp.Zero, NoRevision: false,
            BaseRevision: 0, BaseState: KeyValueState.Set,
            // Far-future recovery deadline: the barrier may only proceed through the real decision,
            // never by presuming an abort.
            RecoveryDeadline: new HLCTimestamp(0, long.MaxValue, 0),
            Resolution: PreparedIntentResolution.Pending);

        Assert.True(await driver.KeyValues.ImportDurableTransactionStateToPartitionLeaderAsync(
            sourcePartition, Array.Empty<TransactionRecord>(), new List<PreparedIntent> { intent }, ct));

        await WaitUntilAsync(
            () => kahunas.Any(m => m.DurablePreparedIntentStore.Get(MovedKey) is { } i && i.TransactionId == txId),
            timeoutMs: 30_000);

        // Start the barrier exactly as the splitter does, with the intent still undecided.
        Task<bool> barrier = driver.KeyValues.SettleMovingRangeIntentsAsync(sourcePartition, SplitKey, null, ct);

        await Task.Delay(1_500, ct);
        Assert.False(barrier.IsCompleted,
            "The barrier must wait for an undecided intent's coordinator, not answer on first contact");

        // Decide the transaction (Commit) while the barrier is waiting.
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

        Assert.True(await barrier,
            "The barrier must settle the decided intent during its wait and report the range clean");

        // The barrier settles by materializing: the committed value is a real row and no intent lingers.
        foreach (KahunaManager manager in kahunas)
        {
            KahunaManager observer = manager;
            await WaitUntilAsync(() => observer.DurablePreparedIntentStore.Get(MovedKey) is null, timeoutMs: 15_000);
        }

        string? preSplit = await FindVisibilityFailureAsync(kahunas, rafts, value, ct);
        Assert.True(preSplit is null, $"Committed value not served after the barrier settled it: {preSplit}");

        // With the range drained, the split itself lands.
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

        Assert.True(outcome.IsSuccess, $"Split failed after the barrier drained the range: {outcome.Status}");

        string? postSplit = await FindVisibilityFailureAsync(kahunas, rafts, value, ct);
        Assert.True(postSplit is null, $"Committed value lost by the split: {postSplit}");
    }
}
