using System.Text;
using Kahuna;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Replication;
using Kahuna.Server.Replication.Protos;
using Kahuna.Shared.Communication.Grpc;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Data;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// Covers the staged-base fences: the guards against the lost-update window between the pre-propose
/// staged-base validation and a transaction's prepares landing. A competitor that commits the same base
/// inside that window — reachable when the writer's in-memory write intent is lost to a lease lapse, cache
/// eviction, or leader change — was invisible to every earlier guard: the pre-propose probe ran too early,
/// the prepare's single-live-intent check saw no live foreign intent (the competitor already settled), and
/// read-set validation skips a read-then-written key. A bank soak lost 3 units of SUM(balance) over ~900k
/// transfers through exactly this interleaving.
///
/// Two mechanisms close the window: the one-phase bundle re-runs the pre-propose staged-base validation
/// immediately before its propose (its decision shares the prepare's atomic batch, so nothing later can
/// withhold it), and the 2PC path is fenced at the prepare's own apply position by the intent store's
/// committed-head compare, which refuses the prepare acknowledgement. The probe-semantics tests pin the
/// <see cref="KeyValueConflictChecks.StagedBase"/> answer, still served to mixed-version remote peers; the
/// two interleaving tests reproduce the full window end to end — one per commit path — using the
/// finalizer's test hook, because no external caller can time the competitor into the gap deterministically.
/// </summary>
public sealed class TestStagedBasePostPrepareFence
{
    private readonly ILoggerFactory loggerFactory;

    public TestStagedBasePostPrepareFence(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    private static async Task<EmbeddedKahunaNode> StartNode(ILoggerFactory loggerFactory, CancellationToken ct)
    {
        EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            ReadIOThreads = 1,
            WriteIOThreads = 1,
            PartitionExecutorPoolSize = 1,
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 4,
            // Short staged-write intent lease so the interleaving test can lapse it with a small delay
            // instead of the 15 s production default — the same lapse a paused coordinator suffers.
            StagedWriteIntentLeaseMs = 200
        }, loggerFactory);

        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync("sbf/seed", ct);

        return node;
    }

    /// <summary>Seeds one committed value non-transactionally and returns its committed revision.</summary>
    private static async Task<long> Seed(IKahuna kahuna, string key, string value, CancellationToken ct)
    {
        (KeyValueResponseType type, long revision, _) = await kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, key, Encoding.UTF8.GetBytes(value), null, -1,
            KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct);

        Assert.Equal(KeyValueResponseType.Set, type);
        return revision;
    }

    private static async Task<Dictionary<string, KeyValueResponseType>> ProbeByKey(
        IKahuna kahuna,
        HLCTimestamp transactionId,
        List<KeyValueConflictProbe> keys,
        CancellationToken ct)
    {
        List<(KeyValueResponseType type, string key, KeyValueDurability durability)> results =
            await kahuna.LocateAndTryCheckManyWriteIntents(transactionId, keys, ct);

        Dictionary<string, KeyValueResponseType> byKey = new(results.Count, StringComparer.Ordinal);

        foreach ((KeyValueResponseType type, string key, KeyValueDurability _) in results)
            byKey[key] = type;

        return byKey;
    }

    // -----------------------------------------------------------------------
    // Probe semantics
    // -----------------------------------------------------------------------

    /// <summary>
    /// A base that still matches the committed head is clean, and a base the head moved past is flagged.
    /// Both directions in one probe so a fence that flags everything (or nothing) cannot pass.
    /// </summary>
    [Fact]
    public async Task StagedBaseProbe_FlagsMovedHead_AndOnlyMovedHead()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        string prefix = "sbf/probe/" + Guid.NewGuid().ToString("N")[..8];

        long freshRevision = await Seed(node.Kahuna, $"{prefix}/fresh", "a", ct);

        await Seed(node.Kahuna, $"{prefix}/moved", "a", ct);
        long movedHead = await Seed(node.Kahuna, $"{prefix}/moved", "b", ct);
        long staleBase = movedHead - 1;

        List<KeyValueConflictProbe> keys =
        [
            new($"{prefix}/fresh", KeyValueDurability.Persistent, KeyValueConflictChecks.StagedBase, freshRevision),
            new($"{prefix}/moved", KeyValueDurability.Persistent, KeyValueConflictChecks.StagedBase, staleBase)
        ];

        Dictionary<string, KeyValueResponseType> byKey = await ProbeByKey(node.Kahuna, new HLCTimestamp(0, 500, 0), keys, ct);

        Assert.Equal(KeyValueResponseType.DoesNotExist, byKey[$"{prefix}/fresh"]);
        Assert.Equal(KeyValueResponseType.NotSet, byKey[$"{prefix}/moved"]);
    }

    /// <summary>
    /// A base validated against key absence (-1) conflicts when the key now exists, and stays clean while
    /// the key is still absent. This is the insert race half of the fence.
    /// </summary>
    [Fact]
    public async Task StagedBaseProbe_ValidatedAbsent_ConflictsOnlyWhenTheKeyAppeared()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        string prefix = "sbf/absent/" + Guid.NewGuid().ToString("N")[..8];

        await Seed(node.Kahuna, $"{prefix}/appeared", "x", ct);

        List<KeyValueConflictProbe> keys =
        [
            new($"{prefix}/appeared", KeyValueDurability.Persistent, KeyValueConflictChecks.StagedBase, -1),
            new($"{prefix}/still-absent", KeyValueDurability.Persistent, KeyValueConflictChecks.StagedBase, -1)
        ];

        Dictionary<string, KeyValueResponseType> byKey = await ProbeByKey(node.Kahuna, new HLCTimestamp(0, 500, 0), keys, ct);

        Assert.Equal(KeyValueResponseType.NotSet, byKey[$"{prefix}/appeared"]);
        Assert.Equal(KeyValueResponseType.DoesNotExist, byKey[$"{prefix}/still-absent"]);
    }

    /// <summary>
    /// The staged-base check composes with the range-lock check on one probe without changing the clean
    /// answer — the fence is added to the write set's existing probe, not issued as a second round.
    /// </summary>
    [Fact]
    public async Task StagedBaseProbe_CombinedWithRangeLockCheck_CleanStaysClean()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        string key = "sbf/combined/" + Guid.NewGuid().ToString("N")[..8];
        long revision = await Seed(node.Kahuna, key, "v", ct);

        Dictionary<string, KeyValueResponseType> byKey = await ProbeByKey(
            node.Kahuna,
            new HLCTimestamp(0, 500, 0),
            [new(key, KeyValueDurability.Persistent, KeyValueConflictChecks.ForeignRangeLock | KeyValueConflictChecks.StagedBase, revision)],
            ct);

        Assert.Equal(KeyValueResponseType.DoesNotExist, byKey[key]);
    }

    // -----------------------------------------------------------------------
    // The lost-update window, end to end
    // -----------------------------------------------------------------------

    /// <summary>
    /// Reproduces the bank-soak run-K interleaving deterministically. T2 reads a key at its committed base
    /// and stages a write computed from that read. Inside T2's finalize — after the pre-propose staged-base
    /// validation passed, before anything durable is proposed — a competitor's commit of the same base
    /// materializes (applied exactly as a replicated committed value is, emulating the competitor that
    /// slipped past T2's lost in-memory write intent). T2's commit must abort: committing would silently
    /// overwrite the competitor's write. Before the post-prepare staged-base fence existed, this committed.
    /// </summary>
    [Fact]
    public async Task LostUpdateWindow_CompetitorCommitsInsideProbePrepareWindow_Aborts()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        string key = "sbf/window/" + Guid.NewGuid().ToString("N")[..8];
        long baseRevision = await Seed(node.Kahuna, key, "100", ct);

        // T2: optimistic read-modify-write. The registered read folds the base observation; the staged
        // write moves it into the written-base set, which read-set validation deliberately skips.
        (KeyValueResponseType startType, TransactionHandle t2) = await node.Kahuna.LocateAndStartTransaction(
            new KeyValueTransactionOptions
            {
                CoordinatorKey = key + "/t2",
                Locking = KeyValueTransactionLocking.Optimistic,
                AsyncRelease = true,
                Timeout = 60_000
            }, ct);
        Assert.Equal(KeyValueResponseType.Set, startType);

        (KeyValueResponseType readType, ReadOnlyKeyValueEntry? readEntry) = await node.Kahuna.LocateAndTryGetValue(
            t2.TransactionId, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct,
            coordinatorKey: t2.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
        Assert.Equal(KeyValueResponseType.Get, readType);
        Assert.Equal(baseRevision, readEntry!.Revision);

        (KeyValueResponseType writeType, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
            t2.TransactionId, key, "99"u8.ToArray(), null, -1, KeyValueFlags.None, 0,
            KeyValueDurability.Persistent, ct,
            coordinatorKey: t2.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
        Assert.Equal(KeyValueResponseType.Set, writeType);

        // The competitor's committed value, materialized the way any committed prepared intent is: as an
        // ordinary replicated key/value record advancing the committed head to baseRevision + 1.
        PreparedIntent competitor = new(
            TransactionId: new HLCTimestamp(0, t2.TransactionId.L + 1, 0), Epoch: 1, Key: key,
            ManifestHash: 0, RecordAnchorKey: key,
            CommitTimestamp: new HLCTimestamp(0, t2.TransactionId.L + 2, 0),
            State: KeyValueState.Set, Value: "101"u8.ToArray(), Bucket: null,
            Revision: baseRevision + 1, Expires: HLCTimestamp.Zero, NoRevision: false,
            BaseRevision: baseRevision, BaseState: KeyValueState.Set,
            RecoveryDeadline: HLCTimestamp.Zero, Resolution: PreparedIntentResolution.Committed);

        byte[] competitorRecord = PreparedIntentMaterializer.ToKeyValueRecord(competitor, new KeyValueMessage());
        int partitionId = node.Raft.GetPartitionKey(key);

        KahunaManager manager = (KahunaManager)node.Kahuna;

        // Install the interleaving: the competitor's commit lands after T2's pre-propose validation and
        // before T2's prepares. One-shot — the hook clears itself so no other finalize replays it.
        DurableTransactionFinalizer finalizer = manager.TransactionCoordinator.DurableFinalizerForTests;
        finalizer.TestAfterPreValidationHook = async hookCt =>
        {
            finalizer.TestAfterPreValidationHook = null;

            // Let T2's staged in-memory write intent lapse — the pause a nemesis inflicts — so the
            // competitor's committed value is allowed to advance the head past it.
            await Task.Delay(400, hookCt);

            bool applied = await manager.OnReplicationReceived(
                partitionId,
                new RaftLog { LogType = ReplicationTypes.KeyValues, LogData = competitorRecord });

            Assert.True(applied, "the competitor's committed value must apply");
        };

        try
        {
            (KeyValueResponseType commitType, _) = await node.Kahuna.LocateAndCommitTransaction(t2, ct);

            Assert.True(KeyValueResponseType.Aborted == commitType, $"expected Aborted, got {commitType}");
        }
        finally
        {
            finalizer.TestAfterPreValidationHook = null;
        }

        // The competitor's write survived: T2's stale write must not have overwritten it.
        (KeyValueResponseType finalType, ReadOnlyKeyValueEntry? finalEntry) = await node.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);

        Assert.Equal(KeyValueResponseType.Get, finalType);
        Assert.Equal(baseRevision + 1, finalEntry!.Revision);
        Assert.Equal("101", Encoding.UTF8.GetString(finalEntry.Value!));
    }

    /// <summary>
    /// The same lost-update interleaving on the standard 2PC path (a two-partition write set never takes the
    /// one-phase bundle): the competitor's full durable lifecycle — prepare, materialization, settlement — lands
    /// inside the probe→prepare window, so its intent is gone and only its committed head remains when the
    /// victim's prepare applies. The intent store's staged-base fence must refuse that prepare's
    /// acknowledgement, and the commit must abort. Before the fence, the pre-propose validation was the last
    /// look and this committed.
    /// </summary>
    [Fact]
    public async Task LostUpdateWindow_TwoPhasePath_CompetitorSettlesInsideWindow_Aborts()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        string runId = Guid.NewGuid().ToString("N")[..8];
        string contested = $"sbf-twopc-{runId}-a/k";

        // A second written key on a different partition keeps the transaction off the one-phase bundle, so the
        // finalize runs the standard prepare barrier this test targets. Keys route by their parent bucket, so
        // the candidates vary the bucket, not the leaf.
        int contestedPartition = node.Raft.GetPartitionKey(contested);
        string? companion = null;
        for (int i = 0; i < 256; i++)
        {
            string candidate = $"sbf-twopc-{runId}-b{i}/k";
            if (node.Raft.GetPartitionKey(candidate) != contestedPartition)
            {
                companion = candidate;
                break;
            }
        }

        Assert.NotNull(companion);

        long contestedBase = await Seed(node.Kahuna, contested, "100", ct);
        long companionBase = await Seed(node.Kahuna, companion!, "500", ct);

        (KeyValueResponseType startType, TransactionHandle victim) = await node.Kahuna.LocateAndStartTransaction(
            new KeyValueTransactionOptions
            {
                CoordinatorKey = contested + "/victim",
                Locking = KeyValueTransactionLocking.Optimistic,
                AsyncRelease = true,
                Timeout = 60_000
            }, ct);
        Assert.Equal(KeyValueResponseType.Set, startType);

        foreach ((string key, long expectedRevision, string newValue) in
                 new[] { (contested, contestedBase, "99"), (companion!, companionBase, "501") })
        {
            (KeyValueResponseType readType, ReadOnlyKeyValueEntry? readEntry) = await node.Kahuna.LocateAndTryGetValue(
                victim.TransactionId, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct,
                coordinatorKey: victim.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
            Assert.Equal(KeyValueResponseType.Get, readType);
            Assert.Equal(expectedRevision, readEntry!.Revision);

            (KeyValueResponseType writeType, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
                victim.TransactionId, key, Encoding.UTF8.GetBytes(newValue), null, -1, KeyValueFlags.None, 0,
                KeyValueDurability.Persistent, ct,
                coordinatorKey: victim.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
            Assert.Equal(KeyValueResponseType.Set, writeType);
        }

        PreparedIntent competitor = new(
            TransactionId: new HLCTimestamp(0, victim.TransactionId.L + 1, 0), Epoch: 1, Key: contested,
            ManifestHash: 0, RecordAnchorKey: contested,
            CommitTimestamp: new HLCTimestamp(0, victim.TransactionId.L + 2, 0),
            State: KeyValueState.Set, Value: "101"u8.ToArray(), Bucket: null,
            Revision: contestedBase + 1, Expires: HLCTimestamp.Zero, NoRevision: false,
            BaseRevision: contestedBase, BaseState: KeyValueState.Set,
            RecoveryDeadline: HLCTimestamp.Zero, Resolution: PreparedIntentResolution.Pending);

        byte[] competitorRecord = PreparedIntentMaterializer.ToKeyValueRecord(
            competitor with { Resolution = PreparedIntentResolution.Committed }, new KeyValueMessage());

        KahunaManager manager = (KahunaManager)node.Kahuna;
        PreparedIntentStore intentStore = manager.DurablePreparedIntentStore;

        DurableTransactionFinalizer finalizer = manager.TransactionCoordinator.DurableFinalizerForTests;
        finalizer.TestAfterPreValidationHook = async hookCt =>
        {
            finalizer.TestAfterPreValidationHook = null;

            // Let the victim's staged in-memory write intent lapse, as a paused coordinator would.
            await Task.Delay(400, hookCt);

            // The competitor's whole durable lifecycle, in the key's apply order: prepare installs its intent,
            // the committed value materializes, and settlement resolves and removes the intent — leaving only
            // the committed head behind, exactly the state the victim's prepare later applies against.
            PreparedIntentApplyResult prepared = intentStore.Apply(new PrepareIntentCommand(competitor));
            Assert.Equal(TransactionApplyOutcome.Applied, prepared.Outcome);

            bool applied = await manager.OnReplicationReceived(
                contestedPartition,
                new RaftLog { LogType = ReplicationTypes.KeyValues, LogData = competitorRecord });
            Assert.True(applied, "the competitor's committed value must apply");

            intentStore.Apply(new ResolveIntentCommand(competitor.TransactionId, competitor.Epoch, contested, Commit: true));
            intentStore.Apply(new RemoveIntentCommand(competitor.TransactionId, competitor.Epoch, contested));
        };

        try
        {
            (KeyValueResponseType commitType, _) = await node.Kahuna.LocateAndCommitTransaction(victim, ct);

            Assert.True(KeyValueResponseType.Aborted == commitType, $"expected Aborted, got {commitType}");
        }
        finally
        {
            finalizer.TestAfterPreValidationHook = null;
        }

        // The competitor's write survived on the contested key, and the victim's companion write rolled back.
        (KeyValueResponseType contestedType, ReadOnlyKeyValueEntry? contestedEntry) = await node.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, contested, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Get, contestedType);
        Assert.Equal(contestedBase + 1, contestedEntry!.Revision);
        Assert.Equal("101", Encoding.UTF8.GetString(contestedEntry.Value!));

        (KeyValueResponseType companionType, ReadOnlyKeyValueEntry? companionEntry) = await node.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, companion!, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Get, companionType);
        Assert.Equal(companionBase, companionEntry!.Revision);
        Assert.Equal("500", Encoding.UTF8.GetString(companionEntry.Value!));
    }
}
