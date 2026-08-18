using System.Text;
using Kahuna;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// Write-skew (G2-item) guard for interactive transactions whose keys live in a key-range-routed
/// key space. Registering a key space for key-order routing collapses every key of a multi-key
/// transaction onto one descriptor partition — the anchor bundle and single-participant fast paths
/// become reachable for every transaction, and reads route through the descriptor router instead of
/// the hash locator. The commit-time guards (concurrent-writer probe over the read set, then
/// revision revalidation) must behave exactly as they do under hash routing: two transactions that
/// each read the key the other writes must never both commit.
/// </summary>
public sealed class TestWriteSkewUnderKeyRangeRouting
{
    private const string KeySpace = "skewrange";

    private readonly ILoggerFactory loggerFactory;

    public TestWriteSkewUnderKeyRangeRouting(ITestOutputHelper outputHelper)
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
            DurableDeferredSettlement = true
        }, loggerFactory);
        await node.StartAsync(ct);

        // The whole key space rides one descriptor: every key below routes by key order to the
        // same partition, which is the configuration the anomaly under investigation ran with.
        Assert.True(await node.Kahuna.RegisterKeyRangeAsync(KeySpace, ct));
        await node.WaitForLeaderForKeyAsync($"{KeySpace}/probe", ct);
        return node;
    }

    private static async Task<TransactionHandle> StartSession(EmbeddedKahunaNode node, string coordinatorKey, CancellationToken ct)
    {
        // Pessimistic + TrackAndValidate — the combination documented as serializable, and the one
        // the failing workload ran with.
        (KeyValueResponseType startType, TransactionHandle handle) = await node.Kahuna.LocateAndStartTransaction(
            new KeyValueTransactionOptions
            {
                CoordinatorKey = coordinatorKey,
                Locking = KeyValueTransactionLocking.Pessimistic,
                ReadValidation = ReadValidation.TrackAndValidate,
                DecisionDurability = DecisionDurability.Durable,
                Timeout = 10_000
            }, ct);
        Assert.Equal(KeyValueResponseType.Set, startType);
        return handle;
    }

    /// <summary>Transactional point read; the keys in play are unwritten, so absent is the expected observation.</summary>
    private static async Task ReadAbsentInSession(EmbeddedKahunaNode node, TransactionHandle handle, string key, CancellationToken ct)
    {
        (KeyValueResponseType type, _) = await node.Kahuna.LocateAndTryGetValue(
            handle.TransactionId, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct,
            coordinatorKey: handle.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
        Assert.Equal(KeyValueResponseType.DoesNotExist, type);
    }

    private static async Task WriteInSession(EmbeddedKahunaNode node, TransactionHandle handle, string key, string value, CancellationToken ct)
    {
        (KeyValueResponseType type, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
            handle.TransactionId, key, Encoding.UTF8.GetBytes(value), null, -1,
            KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct,
            coordinatorKey: handle.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
        Assert.Equal(KeyValueResponseType.Set, type);
    }

    /// <summary>
    /// The deterministic G2-item interleaving from the anomaly under investigation: T1 appends to
    /// one key, reads the other as absent, and commits; T2 — which read T1's key as absent before
    /// T1 committed — then appends to the key T1 read and asks to commit. T2's read of T1's key was
    /// overtaken by a committed write, so its commit must abort — both committing would close an
    /// anti-dependency cycle no serial order explains.
    /// </summary>
    [Fact]
    public async Task ReadOvertakenByCommittedCrossingWriter_RangeRouted_SecondCommitAborts()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        string keyA = $"{KeySpace}/det-a";
        string keyB = $"{KeySpace}/det-b";

        TransactionHandle t1 = await StartSession(node, $"{KeySpace}-tx/det-1", ct);
        TransactionHandle t2 = await StartSession(node, $"{KeySpace}-tx/det-2", ct);

        // T2's reads happen first (reads place no intent, so they block nothing) — this is the cut
        // T2 will try to commit against. T1 then stages its write, reads b, and commits: at that
        // point T2 has staged nothing, so T1's probe of b finds no concurrent writer and T1
        // legitimately commits.
        await ReadAbsentInSession(node, t2, keyA, ct);
        await ReadAbsentInSession(node, t2, keyB, ct);

        await ReadAbsentInSession(node, t1, keyA, ct);
        await WriteInSession(node, t1, keyA, "t1", ct);
        await ReadAbsentInSession(node, t1, keyB, ct);

        (KeyValueResponseType firstCommit, _) = await node.Kahuna.LocateAndCommitTransaction(t1, ct);
        Assert.Equal(KeyValueResponseType.Committed, firstCommit);

        // T2 now stages its own write and commits — its read of a is stale against T1's commit.
        await WriteInSession(node, t2, keyB, "t2", ct);

        (KeyValueResponseType secondCommit, _) = await node.Kahuna.LocateAndCommitTransaction(t2, ct);
        Assert.Equal(KeyValueResponseType.Aborted, secondCommit);
    }

    /// <summary>
    /// The mirror guard: a commit whose read set carries a live concurrent writer's staged key must
    /// abort on the probe even though nothing committed yet. With both writes staged before either
    /// commit, the first committer's probe finds the rival's staged intent and aborts; the rival's
    /// own commit then validates against a database the abort left untouched — its cut is
    /// consistent, so it commits. Loser-aborts/survivor-commits is the serializable outcome; both
    /// committing would be the G2-item cycle.
    /// </summary>
    [Fact]
    public async Task BothStagedBeforeEitherCommit_RangeRouted_FirstCommitterAborts_SurvivorCommits()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        string keyA = $"{KeySpace}/mut-a";
        string keyB = $"{KeySpace}/mut-b";

        TransactionHandle t1 = await StartSession(node, $"{KeySpace}-tx/mut-1", ct);
        TransactionHandle t2 = await StartSession(node, $"{KeySpace}-tx/mut-2", ct);

        await ReadAbsentInSession(node, t1, keyA, ct);
        await WriteInSession(node, t1, keyA, "t1", ct);
        await ReadAbsentInSession(node, t1, keyB, ct);

        await ReadAbsentInSession(node, t2, keyA, ct);
        await ReadAbsentInSession(node, t2, keyB, ct);
        await WriteInSession(node, t2, keyB, "t2", ct);

        (KeyValueResponseType commit1, _) = await node.Kahuna.LocateAndCommitTransaction(t1, ct);
        Assert.Equal(KeyValueResponseType.Aborted, commit1);

        (KeyValueResponseType commit2, _) = await node.Kahuna.LocateAndCommitTransaction(t2, ct);
        Assert.Equal(KeyValueResponseType.Committed, commit2);
    }

    /// <summary>
    /// The write-skew guard's primitive: a staged transactional write must be visible to the
    /// commit-time concurrent-writer probe from any other transaction's perspective — for a
    /// key-range-routed key exactly as for a hash-routed one.
    /// </summary>
    [Theory]
    [InlineData(true, false)]
    [InlineData(false, false)]
    [InlineData(true, true)]
    [InlineData(false, true)]
    public async Task StagedWrite_IsVisibleToWriteIntentProbe(bool rangeRouted, bool readBeforeWrite)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        string key = rangeRouted ? $"{KeySpace}/probe-target" : "plainhash/probe-target";

        TransactionHandle writer = await StartSession(node, $"{KeySpace}-tx/probe-{rangeRouted}-{readBeforeWrite}", ct);

        // The read-modify-write shape: the write's base is the session's own prior read. The staged
        // intent must be exactly as probe-visible as a blind write's.
        if (readBeforeWrite)
            await ReadAbsentInSession(node, writer, key, ct);

        await WriteInSession(node, writer, key, "staged", ct);

        // A different transaction's probe must see the staged intent as a live concurrent writer —
        // through the single-key probe and through the batched read-set probe the commit path uses.
        KeyValueResponseType probe = await node.Kahuna.LocateAndTryCheckWriteIntent(
            HLCTimestamp.Zero, key, KeyValueDurability.Persistent, ct);

        Assert.Equal(KeyValueResponseType.Aborted, probe);

        TransactionHandle other = await StartSession(node, $"{KeySpace}-tx/prober-{rangeRouted}-{readBeforeWrite}", ct);
        List<(KeyValueResponseType type, string key, KeyValueDurability durability)> manyProbe =
            await node.Kahuna.LocateAndTryCheckManyWriteIntents(
                other.TransactionId, [new(key, KeyValueDurability.Persistent, KeyValueConflictChecks.WriteIntent)], ct);

        Assert.Single(manyProbe);
        Assert.Equal(KeyValueResponseType.Aborted, manyProbe[0].type);
    }

    /// <summary>
    /// The crossing pair under a genuine race, many rounds: each side's reads happen up front, then
    /// its stage-and-commit tail races the other's. Interleavings range from fully staggered (one
    /// commits before the other stages — the loser must abort on the overtaken read) to fully
    /// crossed (both staged first — the probes fire and both abort). No interleaving may ever let
    /// both commit: that is exactly the G2-item cycle under investigation.
    /// </summary>
    [Fact]
    public async Task CrossingReadWritePair_RangeRouted_RacedTails_NeverBothCommit()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        const int rounds = 25;

        for (int round = 0; round < rounds; round++)
        {
            string keyA = $"{KeySpace}/conc-{round}-a";
            string keyB = $"{KeySpace}/conc-{round}-b";

            TransactionHandle t1 = await StartSession(node, $"{KeySpace}-tx/conc-{round}-1", ct);
            TransactionHandle t2 = await StartSession(node, $"{KeySpace}-tx/conc-{round}-2", ct);

            // Both cuts observed before either write exists.
            await ReadAbsentInSession(node, t1, keyA, ct);
            await ReadAbsentInSession(node, t1, keyB, ct);
            await ReadAbsentInSession(node, t2, keyA, ct);
            await ReadAbsentInSession(node, t2, keyB, ct);

            async Task<KeyValueResponseType> StageAndCommit(TransactionHandle handle, string key, string value)
            {
                await WriteInSession(node, handle, key, value, ct);
                (KeyValueResponseType outcome, _) = await node.Kahuna.LocateAndCommitTransaction(handle, ct);
                return outcome;
            }

            Task<KeyValueResponseType> tail1 = Task.Run(() => StageAndCommit(t1, keyA, "t1"), ct);
            Task<KeyValueResponseType> tail2 = Task.Run(() => StageAndCommit(t2, keyB, "t2"), ct);

            KeyValueResponseType outcome1 = await tail1;
            KeyValueResponseType outcome2 = await tail2;

            Assert.False(outcome1 == KeyValueResponseType.Committed && outcome2 == KeyValueResponseType.Committed,
                $"round {round}: both transactions committed ({outcome1}/{outcome2}) — write skew under key-range routing");
        }
    }
}
