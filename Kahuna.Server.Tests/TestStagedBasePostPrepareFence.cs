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
/// Covers the post-prepare staged-base fence: the guard against the lost-update window between the
/// pre-propose staged-base validation and a transaction's prepares landing. A competitor that commits the
/// same base inside that window — reachable when the writer's in-memory write intent is lost to a lease
/// lapse, cache eviction, or leader change — was invisible to every earlier guard: the pre-propose probe
/// ran too early, the prepare's single-live-intent check saw no live foreign intent (the competitor already
/// settled), and read-set validation skips a read-then-written key. Bank soak run K lost 3 units of
/// SUM(balance) over ~900k transfers through exactly this interleaving.
///
/// The probe-semantics tests pin the new <see cref="KeyValueConflictChecks.StagedBase"/> answer directly;
/// the interleaving test reproduces the full window end to end using the finalizer's test hook, because no
/// external caller can time the competitor into the probe→prepare gap deterministically.
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
}
