using System.Text;

using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.KeyValues.Watch;
using Kahuna.Server.KeyValues.Writes;
using Kahuna.Server.Replication;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// A transaction that writes the keys of a transaction that committed just before it, on one node. With deferred
/// settlement the first transaction answers while its intents still hold the keys. The second one commits in one
/// bundle all the same: the bundle carries the settlement of the first ahead of its own prepare. A holder that is
/// not committed keeps the two-phase flow.
///
/// <para>Each test that needs the first transaction unsettled holds its deferred resolution before it starts, so
/// nothing else settles the keys.</para>
/// </summary>
public sealed class TestOnePhaseOverCommittedPredecessor : BaseCluster, IDisposable
{
    private readonly ILoggerFactory loggerFactory;

    private readonly string tempRoot = Path.Combine(Path.GetTempPath(), "kahuna_onephase_pred_" + Guid.NewGuid().ToString("N"));

    public TestOnePhaseOverCommittedPredecessor(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper, quietKommander: true);
    }

    public void Dispose()
    {
        if (Directory.Exists(tempRoot))
            Directory.Delete(tempRoot, recursive: true);
    }

    private sealed class Rig : IAsyncDisposable
    {
        public required EmbeddedKahunaNode Node { get; init; }

        public required BundleEntryTap Tap { get; init; }

        public KahunaManager Manager => (KahunaManager)Node.Kahuna;

        public PreparedIntentStore Intents => Manager.DurablePreparedIntentStore;

        public DurableTransactionFinalizer Finalizer => Manager.TransactionCoordinator.DurableFinalizerForTests;

        // The node counts its commit calls by path only while something listens to their histogram.
        private readonly MetricCapture commitCalls = new("path", "kahuna.transactions.commit_call_ms");

        public long OnePhaseCalls => Manager.TransactionCoordinator.CommitCalls.Calls(CommitCallPath.OnePhase);

        public long TwoPhaseCalls => Manager.TransactionCoordinator.CommitCalls.Calls(CommitCallPath.TwoPhase);

        // Resolutions the test still holds; released when the node ends so its shutdown does not wait for them.
        private readonly List<TaskCompletionSource> held = [];

        /// <summary>Holds the deferred resolution of each transaction that commits while <paramref name="body"/> runs,
        /// and returns the action that lets them run.</summary>
        public async Task<Action> HoldResolutions(Func<Task> body)
        {
            TaskCompletionSource open = new(TaskCreationOptions.RunContinuationsAsynchronously);
            held.Add(open);

            Finalizer.TestBeforeDeferredResolutionHook = token => open.Task.WaitAsync(token);
            try
            {
                await body();
            }
            finally
            {
                Finalizer.TestBeforeDeferredResolutionHook = null;
            }

            return () => open.TrySetResult();
        }

        public async ValueTask DisposeAsync()
        {
            foreach (TaskCompletionSource open in held)
                open.TrySetResult();

            await Node.DisposeAsync();
            commitCalls.Dispose();
        }
    }

    private async Task<Rig> Start(CancellationToken ct, int partitions = 1, bool applyTimeValidation = true, bool materializeOnResolve = true, string? storagePath = null)
    {
        BundleEntryTap? tap = null;
        EmbeddedKahunaOptions options = new()
        {
            TimerInitialDelay = TimeSpan.FromMilliseconds(50),
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = partitions,
            OnePhaseApplyTimeValidation = applyTimeValidation,
            DurableMaterializeOnResolve = materializeOnResolve,
            WriteBatchExecutorDecorator = inner => tap = new BundleEntryTap(inner)
        };

        if (storagePath is not null)
        {
            options.Storage = "sqlite";
            options.StoragePath = Directory.CreateDirectory(Path.Combine(storagePath, "store")).FullName;
            options.StorageRevision = "onephase";
            options.WalStorage = "sqlite";
            options.WalPath = Directory.CreateDirectory(Path.Combine(storagePath, "wal")).FullName;
            options.WalRevision = "onephase-wal";
            options.WalSyncWrites = true;
        }

        EmbeddedKahunaNode node = new(options, loggerFactory);
        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync("seed/key", ct);

        return new Rig { Node = node, Tap = tap! };
    }

    private static string Space(string tag) => $"pred-{Guid.NewGuid().ToString("N")[..8]}-{tag}";

    private static async Task<TransactionHandle> Begin(IKahuna kahuna, string coordinatorKey, CancellationToken ct)
    {
        (KeyValueResponseType type, TransactionHandle handle) = await kahuna.LocateAndStartTransaction(
            new KeyValueTransactionOptions
            {
                CoordinatorKey = coordinatorKey,
                Locking = KeyValueTransactionLocking.Optimistic,
                ReadValidation = ReadValidation.TrackAndValidate,
                Timeout = 60_000
            }, ct);

        Assert.Equal(KeyValueResponseType.Set, type);
        return handle;
    }

    private static async Task<(string? Value, long Revision)> Read(IKahuna kahuna, TransactionHandle tx, string key, CancellationToken ct)
    {
        (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await kahuna.LocateAndTryGetValue(
            tx.TransactionId, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct,
            coordinatorKey: tx.CoordinatorKey, operationId: TransactionOperationId.NewRandom());

        return type == KeyValueResponseType.Get ? (Encoding.UTF8.GetString(entry!.Value!), entry.Revision) : (null, -1);
    }

    private static async Task Write(IKahuna kahuna, TransactionHandle tx, string key, string value, CancellationToken ct)
    {
        (KeyValueResponseType type, _, _) = await kahuna.LocateAndTrySetKeyValue(
            tx.TransactionId, key, Encoding.UTF8.GetBytes(value), null, -1, KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct,
            coordinatorKey: tx.CoordinatorKey, operationId: TransactionOperationId.NewRandom());

        Assert.Equal(KeyValueResponseType.Set, type);
    }

    private static async Task Delete(IKahuna kahuna, TransactionHandle tx, string key, CancellationToken ct)
    {
        (KeyValueResponseType type, _, _) = await kahuna.LocateAndTryDeleteKeyValue(
            tx.TransactionId, key, KeyValueDurability.Persistent, ct,
            coordinatorKey: tx.CoordinatorKey, operationId: TransactionOperationId.NewRandom());

        Assert.Equal(KeyValueResponseType.Deleted, type);
    }

    private static async Task Commit(IKahuna kahuna, TransactionHandle tx, CancellationToken ct)
    {
        TransactionCommitAnswer answer = await kahuna.LocateAndCommitTransaction(tx, ct);
        Assert.True(answer.Type == KeyValueResponseType.Committed, $"{answer.Type} {answer.Refusal} {answer.Reason}");
    }

    /// <summary>Reads each key, writes <paramref name="value"/> to it and commits.</summary>
    private static async Task<TransactionHandle> ReadModifyWrite(IKahuna kahuna, string value, CancellationToken ct, params string[] keys)
    {
        TransactionHandle tx = await Begin(kahuna, keys[0], ct);

        foreach (string key in keys)
            await Read(kahuna, tx, key, ct);

        foreach (string key in keys)
            await Write(kahuna, tx, key, value, ct);

        await Commit(kahuna, tx, ct);
        return tx;
    }

    private static async Task<(string? Value, long Revision)> ReadLatest(IKahuna kahuna, string key, CancellationToken ct, long revision = -1, HLCTimestamp readTimestamp = default)
    {
        (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, key, revision, readTimestamp, KeyValueDurability.Persistent, ct);

        return type == KeyValueResponseType.Get ? (Encoding.UTF8.GetString(entry!.Value!), entry.Revision) : (null, -1);
    }

    private static void AssertHeldBy(Rig rig, HLCTimestamp transactionId, params string[] keys)
    {
        foreach (string key in keys)
            Assert.Equal(transactionId, rig.Intents.Get(key)?.TransactionId);
    }

    private static Task WaitUntilSettled(Rig rig) => WaitUntilAsync(() => rig.Intents.Count == 0);

    /// <summary>
    /// An interactive transaction reads and writes the two keys that the transaction before it committed and did
    /// not settle. It commits on the one-phase path: one batch holds the settlement of the first transaction, then
    /// the record init, the prepare and the commit. The settlement of the first transaction that runs afterwards
    /// changes nothing.
    /// </summary>
    [Fact]
    public async Task Interactive_OverAnUnsettledCommit_CommitsInOneBundle()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using Rig rig = await Start(ct);
        string space = Space("interactive");
        string[] keys = [$"{space}/a", $"{space}/b"];

        TransactionHandle first = default;
        Action release = await rig.HoldResolutions(async () => first = await ReadModifyWrite(rig.Manager, "1", ct, keys));
        AssertHeldBy(rig, first.TransactionId, keys);

        long onePhase = rig.OnePhaseCalls;
        long twoPhase = rig.TwoPhaseCalls;

        TransactionHandle second = await ReadModifyWrite(rig.Manager, "2", ct, keys);

        Assert.Equal(1, rig.OnePhaseCalls - onePhase);
        Assert.Equal(0, rig.TwoPhaseCalls - twoPhase);
        Assert.Equal("settle a,b | init | prepare a,b | commit", rig.Tap.BundleOf(second.TransactionId));
        Assert.Equal(1, rig.Tap.SettlesOf(first.TransactionId));

        foreach (string key in keys)
            Assert.Equal(("2", 1L), await ReadLatest(rig.Manager, key, ct));

        await WaitUntilSettled(rig);
        release();
        await WaitUntilAsync(() => rig.Tap.SettlesOf(first.TransactionId) == 2);

        Assert.Equal(0, rig.Intents.Count);
        foreach (string key in keys)
        {
            Assert.Equal(("2", 1L), await ReadLatest(rig.Manager, key, ct));
            Assert.Equal(("1", 0L), await ReadLatest(rig.Manager, key, ct, revision: 0));
        }
    }

    /// <summary>A script takes the same path as an interactive transaction.</summary>
    [Fact]
    public async Task Script_OverAnUnsettledCommit_CommitsInOneBundle()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using Rig rig = await Start(ct);
        string space = Space("script");
        string[] keys = [$"{space}/a", $"{space}/b"];

        async Task Run(int value)
        {
            KeyValueTransactionResult result = await rig.Manager.TryExecuteTransactionScript(
                Encoding.UTF8.GetBytes($"BEGIN SET `{keys[0]}` '{value}' SET `{keys[1]}` '{value}' COMMIT END"), null, null);
            Assert.True(result.Type == KeyValueResponseType.Set, $"{result.Type}: {result.Reason}");
        }

        Action release = await rig.HoldResolutions(() => Run(1));
        HLCTimestamp first = rig.Intents.Get(keys[0])!.TransactionId;
        AssertHeldBy(rig, first, keys);

        Action releaseSecond = await rig.HoldResolutions(() => Run(2));
        HLCTimestamp second = rig.Intents.Get(keys[0])!.TransactionId;

        Assert.NotEqual(first, second);
        Assert.Equal("settle a,b | init | prepare a,b | commit", rig.Tap.BundleOf(second));
        Assert.Equal(1, rig.Tap.SettlesOf(first));

        release();
        releaseSecond();
        await WaitUntilSettled(rig);

        foreach (string key in keys)
            Assert.Equal(("2", 1L), await ReadLatest(rig.Manager, key, ct));
    }

    /// <summary>
    /// Every transaction of a chain commits over the unsettled transaction before it. Each one settles its
    /// predecessor in its own bundle, so no more than one transaction holds the key at a time, and the whole
    /// history of the key is there at the end.
    /// </summary>
    [Fact]
    public async Task Chain_OfUnsettledCommits_KeepsTheOnePhasePath()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using Rig rig = await Start(ct);
        string key = Space("chain") + "/a";
        const int length = 8;

        long twoPhase = rig.TwoPhaseCalls;
        List<HLCTimestamp> committed = [];

        Action release = await rig.HoldResolutions(async () =>
        {
            for (int i = 0; i < length; i++)
                committed.Add((await ReadModifyWrite(rig.Manager, i.ToString(), ct, key)).TransactionId);
        });

        Assert.Equal(0, rig.TwoPhaseCalls - twoPhase);
        Assert.Equal(1, rig.Intents.Count);
        AssertHeldBy(rig, committed[^1], key);

        for (int i = 0; i < length - 1; i++)
            Assert.Equal(1, rig.Tap.SettlesOf(committed[i]));

        release();
        await WaitUntilSettled(rig);
        await WaitUntilAsync(() => committed.Take(length - 1).All(tx => rig.Tap.SettlesOf(tx) == 2));

        Assert.Equal(((length - 1).ToString(), length - 1L), await ReadLatest(rig.Manager, key, ct));
        for (int i = 0; i < length; i++)
            Assert.Equal((i.ToString(), (long)i), await ReadLatest(rig.Manager, key, ct, revision: i));
    }

    /// <summary>
    /// The predecessor's own settlement lands between the pre-flight of the successor and its propose. The bundle
    /// then carries nothing, and the key was settled once.
    /// </summary>
    [Fact]
    public async Task PredecessorThatSettlesDuringTheValidation_IsNotCarried()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using Rig rig = await Start(ct);
        string key = Space("raced") + "/a";

        TransactionHandle first = default;
        Action release = await rig.HoldResolutions(async () => first = await ReadModifyWrite(rig.Manager, "1", ct, key));

        rig.Finalizer.TestAfterReadSetValidationHook = async _ =>
        {
            release();
            await WaitUntilAsync(() => rig.Intents.Get(key) is null);
        };

        TransactionHandle second;
        try
        {
            second = await ReadModifyWrite(rig.Manager, "2", ct, key);
        }
        finally
        {
            rig.Finalizer.TestAfterReadSetValidationHook = null;
        }

        Assert.Equal("init | prepare a | commit", rig.Tap.BundleOf(second.TransactionId));
        Assert.Equal(1, rig.Tap.SettlesOf(first.TransactionId));
        Assert.Equal(("2", 1L), await ReadLatest(rig.Manager, key, ct));
    }

    /// <summary>
    /// The settlement of the predecessor runs while the successor's own intent still holds the key. It must leave
    /// that intent and its value alone.
    /// </summary>
    [Fact]
    public async Task PredecessorSettlement_UnderTheSuccessorsIntent_LeavesItAlone()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using Rig rig = await Start(ct);
        string key = Space("late") + "/a";

        TransactionHandle first = default, second = default;
        Action releaseFirst = await rig.HoldResolutions(async () => first = await ReadModifyWrite(rig.Manager, "1", ct, key));
        Action releaseSecond = await rig.HoldResolutions(async () => second = await ReadModifyWrite(rig.Manager, "2", ct, key));

        AssertHeldBy(rig, second.TransactionId, key);

        releaseFirst();
        await WaitUntilAsync(() => rig.Tap.SettlesOf(first.TransactionId) == 2);

        // The settle entry of the first transaction is proposed; a write behind it in the same queue shows that it
        // also applied.
        await ReadModifyWrite(rig.Manager, "x", ct, Space("late-fence") + "/f");

        AssertHeldBy(rig, second.TransactionId, key);
        Assert.Equal(("2", 1L), await ReadLatest(rig.Manager, key, ct));

        releaseSecond();
        await WaitUntilSettled(rig);
        Assert.Equal(("2", 1L), await ReadLatest(rig.Manager, key, ct));
    }

    /// <summary>
    /// A write with no read before it, a delete and an insert over the delete, each over an unsettled commit of the
    /// same key. None has a base at an existing revision, and each commits in one bundle.
    /// </summary>
    [Fact]
    public async Task BlindWriteDeleteAndInsert_OverUnsettledCommits_CommitInOneBundleEach()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using Rig rig = await Start(ct);
        string key = Space("blind") + "/a";

        long twoPhase = rig.TwoPhaseCalls;
        List<TransactionHandle> transactions = [];

        Action release = await rig.HoldResolutions(async () =>
        {
            transactions.Add(await ReadModifyWrite(rig.Manager, "1", ct, key));

            TransactionHandle blind = await Begin(rig.Manager, key, ct);
            await Write(rig.Manager, blind, key, "2", ct);
            await Commit(rig.Manager, blind, ct);
            transactions.Add(blind);

            TransactionHandle delete = await Begin(rig.Manager, key, ct);
            Assert.Equal(("2", 1L), await Read(rig.Manager, delete, key, ct));
            await Delete(rig.Manager, delete, key, ct);
            await Commit(rig.Manager, delete, ct);
            transactions.Add(delete);
            Assert.Equal((null, -1L), await ReadLatest(rig.Manager, key, ct));

            TransactionHandle insert = await Begin(rig.Manager, key, ct);
            Assert.Equal((null, -1L), await Read(rig.Manager, insert, key, ct));
            await Write(rig.Manager, insert, key, "3", ct);
            await Commit(rig.Manager, insert, ct);
            transactions.Add(insert);
        });

        Assert.Equal(0, rig.TwoPhaseCalls - twoPhase);
        foreach (TransactionHandle transaction in transactions.Skip(1))
            Assert.Equal("settle a | init | prepare a | commit", rig.Tap.BundleOf(transaction.TransactionId));

        release();
        await WaitUntilSettled(rig);
        Assert.Equal("3", (await ReadLatest(rig.Manager, key, ct)).Value);
    }

    /// <summary>
    /// The predecessor wrote keys on two partitions, so its record is on one of them and the successor writes a key
    /// of the other. This node leads both, so the record it reads is the canonical one and the bundle settles the
    /// predecessor's intent on its own partition only.
    /// </summary>
    [Fact]
    public async Task PredecessorWithItsRecordOnAnotherPartition_LedHere_IsCarried()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using Rig rig = await Start(ct, partitions: 2);

        string anchorKey = KeyOn(rig.Manager, 1, "anchor");
        string key = KeyOn(rig.Manager, 2, "other");
        await rig.Node.WaitForLeaderForKeyAsync(anchorKey, ct);
        await rig.Node.WaitForLeaderForKeyAsync(key, ct);

        TransactionHandle first = default;
        Action release = await rig.HoldResolutions(async () => first = await ReadModifyWrite(rig.Manager, "1", ct, anchorKey, key));

        PreparedIntent held = rig.Intents.Get(key)!;
        Assert.Equal(first.TransactionId, held.TransactionId);
        Assert.Equal(1, rig.Manager.KeyValues.LocateDurablePartition(held.RecordAnchorKey).PartitionId);

        TransactionHandle second = await ReadModifyWrite(rig.Manager, "2", ct, key);

        Assert.StartsWith("settle ", rig.Tap.BundleOf(second.TransactionId));
        AssertHeldBy(rig, first.TransactionId, anchorKey);

        release();
        await WaitUntilSettled(rig);
        Assert.Equal(("1", 0L), await ReadLatest(rig.Manager, anchorKey, ct));
        Assert.Equal(("2", 1L), await ReadLatest(rig.Manager, key, ct));
    }

    /// <summary>
    /// Without apply-time validation, or without a settle that installs the value, the successor keeps the
    /// two-phase flow: its helping pass settles the predecessor and the commit succeeds.
    /// </summary>
    [Theory]
    [InlineData(false, true)]
    [InlineData(true, false)]
    public async Task WithoutARequirement_TheSuccessorTakesTheTwoPhaseFlow(bool applyTimeValidation, bool materializeOnResolve)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using Rig rig = await Start(ct, applyTimeValidation: applyTimeValidation, materializeOnResolve: materializeOnResolve);
        string key = Space("flags") + "/a";

        Action release = await rig.HoldResolutions(() => ReadModifyWrite(rig.Manager, "1", ct, key));
        long twoPhase = rig.TwoPhaseCalls;

        await ReadModifyWrite(rig.Manager, "2", ct, key);

        Assert.Equal(1, rig.TwoPhaseCalls - twoPhase);
        release();
        await WaitUntilSettled(rig);
        Assert.Equal(("2", 1L), await ReadLatest(rig.Manager, key, ct));
    }

    /// <summary>
    /// An undecided intent of another transaction holds the key. It is a live conflict: the bundle is not proposed,
    /// the two-phase flow gives up after its rounds, and the holder keeps its intent.
    /// </summary>
    [Fact]
    public async Task UndecidedHolder_TakesTheTwoPhaseFlow_AndKeepsItsIntent()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using Rig rig = await Start(ct);
        string key = Space("undecided") + "/a";

        TransactionHandle tx = await Begin(rig.Manager, key, ct);
        await Write(rig.Manager, tx, key, "2", ct);

        PreparedIntent holder = await PlantIntent(rig, key, decision: null, ct);
        long twoPhase = rig.TwoPhaseCalls;

        TransactionCommitAnswer answer = await rig.Manager.LocateAndCommitTransaction(tx, ct);

        Assert.Equal(KeyValueResponseType.Aborted, answer.Type);
        Assert.Equal(CommitRefusalClass.PrepareExhausted, answer.Refusal);
        Assert.Equal(1, rig.TwoPhaseCalls - twoPhase);
        Assert.Equal(holder.TransactionId, rig.Intents.Get(key)!.TransactionId);
        Assert.Equal(0, rig.Tap.SettlesOf(holder.TransactionId));
    }

    /// <summary>
    /// An aborted transaction left its intent on the key. The two-phase flow clears it and the commit succeeds; the
    /// aborted value never shows.
    /// </summary>
    [Fact]
    public async Task AbortedHolder_TakesTheTwoPhaseFlow_AndNeverInstallsItsValue()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using Rig rig = await Start(ct);
        string key = Space("aborted") + "/a";

        TransactionHandle tx = await Begin(rig.Manager, key, ct);
        await Write(rig.Manager, tx, key, "2", ct);

        await PlantIntent(rig, key, TransactionDecision.Abort, ct);
        long twoPhase = rig.TwoPhaseCalls;

        await Commit(rig.Manager, tx, ct);

        Assert.Equal(1, rig.TwoPhaseCalls - twoPhase);
        await WaitUntilSettled(rig);
        Assert.Equal(("2", 0L), await ReadLatest(rig.Manager, key, ct));
        Assert.Equal((null, -1L), await ReadLatest(rig.Manager, key, ct, revision: 40));
    }

    /// <summary>
    /// The node ends with the resolution of both transactions never run. The log holds the settlement of the first
    /// one only as the first entry of the second one's bundle, and no settlement of the second. After the restart
    /// the recovery settles the second, and both revisions are there.
    /// </summary>
    [Fact]
    public async Task Restart_WithThePredecessorSettledOnlyInTheBundle_KeepsBothValues()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string space = Space("restart");
        string[] keys = [$"{space}/a", $"{space}/b"];
        HLCTimestamp first = default, second = default;

        await using (Rig rig = await Start(ct, storagePath: tempRoot))
        {
            // Held past the end of the node: its shutdown cancels what it did not finish.
            TaskCompletionSource never = new();
            rig.Finalizer.TestBeforeDeferredResolutionHook = token => never.Task.WaitAsync(token);

            first = (await ReadModifyWrite(rig.Manager, "1", ct, keys)).TransactionId;
            second = (await ReadModifyWrite(rig.Manager, "2", ct, keys)).TransactionId;

            Assert.Equal("settle a,b | init | prepare a,b | commit", rig.Tap.BundleOf(second));
            Assert.Equal(1, rig.Tap.SettlesOf(first));
            Assert.Equal(0, rig.Tap.SettlesOf(second));
        }

        await using (Rig rig = await Start(ct, storagePath: tempRoot))
        {
            await rig.Node.WaitForLeaderForKeyAsync(keys[0], ct);

            foreach (string key in keys)
                Assert.NotEqual(first, rig.Intents.Get(key)?.TransactionId);

            await WaitUntilAsync(async () =>
            {
                await rig.Manager.KeyValues.RecoverPreparedIntents(ct);
                return rig.Intents.Count == 0;
            }, timeoutMs: 30_000);

            foreach (string key in keys)
            {
                Assert.Equal(("2", 1L), await ReadLatest(rig.Manager, key, ct));
                Assert.Equal(("1", 0L), await ReadLatest(rig.Manager, key, ct, revision: 0));
            }
        }
    }

    /// <summary>
    /// A watch reports each write of a chain once and in revision order, although the settlement of each
    /// transaction is proposed twice: in the bundle of the next one and by its own resolution.
    /// </summary>
    [Fact]
    public async Task Watch_ReportsEachWriteOnce_InRevisionOrder()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using Rig rig = await Start(ct);
        string key = Space("watch") + "/a";
        const int length = 4;

        (KeyValueResponseType type, KeyValueWatch? watch) = await rig.Manager.WatchKeyValues(new(KeyValueWatchScope.ForKey(key)), ct);
        Assert.Equal(KeyValueResponseType.Get, type);

        List<KeyValueWatchEvent> events = [];
        using CancellationTokenSource stop = CancellationTokenSource.CreateLinkedTokenSource(ct);
        Task pump = Task.Run(async () =>
        {
            try
            {
                await foreach (KeyValueWatchFrame frame in watch!.ReadAllAsync(stop.Token))
                {
                    if (frame.Type == KeyValueWatchFrameType.Event)
                        lock (events)
                            events.Add(frame.Event!);
                }
            }
            catch (OperationCanceledException)
            {
            }
        }, ct);

        List<HLCTimestamp> committed = [];
        Action release = await rig.HoldResolutions(async () =>
        {
            for (int i = 0; i < length; i++)
                committed.Add((await ReadModifyWrite(rig.Manager, i.ToString(), ct, key)).TransactionId);
        });

        release();
        await WaitUntilSettled(rig);
        await WaitUntilAsync(() => committed.Take(length - 1).All(tx => rig.Tap.SettlesOf(tx) == 2));
        await WaitUntilAsync(() => { lock (events) return events.Count >= length; });
        await Task.Delay(300, ct);

        await stop.CancelAsync();
        await pump;
        await watch!.DisposeAsync();

        Assert.Equal(committed, events.Select(e => e.TransactionId));
        Assert.Equal([0L, 1L, 2L, 3L], events.Select(e => e.Revision));
    }

    /// <summary>
    /// Workers move one unit between four hot accounts with no pause, so almost every commit meets an unsettled
    /// commit on one of its keys. The total is the same at the end, every account holds as many revisions as it
    /// had committed transfers, and the one-phase path carried settlements.
    /// </summary>
    [Fact]
    public async Task HotTransfers_ConserveTheTotal()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using Rig rig = await Start(ct);
        string space = Space("bank");
        string[] accounts = [.. Enumerable.Range(0, 4).Select(i => $"{space}/acct-{i}")];
        const int initial = 1_000;

        TransactionHandle seed = await Begin(rig.Manager, accounts[0], ct);
        foreach (string account in accounts)
            await Write(rig.Manager, seed, account, initial.ToString(), ct);
        await Commit(rig.Manager, seed, ct);

        using MetricCapture capture = new("unused", "kahuna.durable_tx.one_phase_carried_settles");

        int[] committedPerAccount = new int[accounts.Length];
        using CancellationTokenSource stop = CancellationTokenSource.CreateLinkedTokenSource(ct);
        stop.CancelAfter(TimeSpan.FromSeconds(3));

        async Task Worker(int seedValue)
        {
            Random random = new(seedValue);

            while (!stop.IsCancellationRequested)
            {
                int from = random.Next(accounts.Length);
                int to = (from + 1 + random.Next(accounts.Length - 1)) % accounts.Length;

                TransactionHandle tx = await Begin(rig.Manager, accounts[from], ct);

                (string? fromValue, _) = await Read(rig.Manager, tx, accounts[from], ct);
                (string? toValue, _) = await Read(rig.Manager, tx, accounts[to], ct);

                bool staged = fromValue is not null && toValue is not null
                    && await TryWrite(tx, accounts[from], int.Parse(fromValue) - 1)
                    && await TryWrite(tx, accounts[to], int.Parse(toValue) + 1);

                if (!staged)
                {
                    await rig.Manager.LocateAndRollbackTransaction(tx, ct);
                    continue;
                }

                KeyValueResponseType answer;
                do
                    answer = (await rig.Manager.LocateAndCommitTransaction(tx, ct)).Type;
                while (answer == KeyValueResponseType.MustRetry);

                if (answer == KeyValueResponseType.Committed)
                {
                    Interlocked.Increment(ref committedPerAccount[from]);
                    Interlocked.Increment(ref committedPerAccount[to]);
                }
            }
        }

        async Task<bool> TryWrite(TransactionHandle tx, string key, int value)
        {
            (KeyValueResponseType type, _, _) = await rig.Manager.LocateAndTrySetKeyValue(
                tx.TransactionId, key, Encoding.UTF8.GetBytes(value.ToString()), null, -1, KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct,
                coordinatorKey: tx.CoordinatorKey, operationId: TransactionOperationId.NewRandom());

            return type == KeyValueResponseType.Set;
        }

        await Task.WhenAll(Enumerable.Range(1, 6).Select(Worker));
        await WaitUntilSettled(rig);

        int total = 0;
        for (int i = 0; i < accounts.Length; i++)
        {
            (string? value, long revision) = await ReadLatest(rig.Manager, accounts[i], ct);
            total += int.Parse(value!);
            Assert.Equal(committedPerAccount[i], revision);
        }

        Assert.Equal(accounts.Length * initial, total);
        Assert.True(committedPerAccount.Sum() > 0);
        Assert.True(capture.Total("kahuna.durable_tx.one_phase_carried_settles") > 0);
    }

    /// <summary>
    /// Puts an intent of a fabricated transaction on the key through the log, with no in-memory write intent behind
    /// it: what a holder looks like on a leader that did not stage it. With a decision the record is written too.
    /// </summary>
    private static async Task<PreparedIntent> PlantIntent(Rig rig, string key, TransactionDecision? decision, CancellationToken ct)
    {
        KeyValuesManager keyValues = rig.Manager.KeyValues;
        HLCTimestamp now = rig.Node.Raft.HybridLogicalClock.TrySendOrLocalEvent(rig.Node.Raft.GetLocalNodeId());
        HLCTimestamp deadline = new(now.N, now.L + 60_000, now.C);
        int partition = keyValues.LocateDurablePartition(key).PartitionId;

        PreparedIntent intent = new(
            now, 1, key, ManifestHash: 7, RecordAnchorKey: key, CommitTimestamp: now,
            State: KeyValueState.Set, Value: "planted"u8.ToArray(), Bucket: key[..key.LastIndexOf('/')], Revision: 40, Expires: HLCTimestamp.Zero,
            NoRevision: false, BaseRevision: PreparedIntent.UnknownBaseRevision, BaseState: KeyValueState.Undefined,
            RecoveryDeadline: deadline, Resolution: PreparedIntentResolution.Pending);

        Assert.True(await keyValues.ReplicateDurableThroughScheduler(
            partition, ReplicationTypes.PreparedIntent, PreparedIntentStore.SerializeDelta([new PrepareIntentCommand(intent)]),
            WriteAdmissionClass.Ordinary, WriteSubmissionStage.Prepare, ct));

        if (decision == TransactionDecision.Abort)
            Assert.True(await keyValues.ReplicateDurableThroughScheduler(
                partition, ReplicationTypes.TransactionRecord,
                TransactionRecordStore.SerializeDelta([new AbortTransactionCommand(
                    now, 1, 7, TransactionAbortClass.Conflict, now, now, key, now, deadline, now)]),
                WriteAdmissionClass.Terminal, WriteSubmissionStage.Decision, ct));

        Assert.Equal(now, rig.Intents.Get(key)!.TransactionId);
        return intent;
    }

    private static string KeyOn(KahunaManager probe, int partition, string tag)
    {
        string random = Guid.NewGuid().ToString("N")[..8];

        for (int i = 0; i < 4_096; i++)
        {
            string candidate = $"{tag}{i}-{random}/k";
            if (probe.KeyValues.LocateDurablePartition(candidate).PartitionId == partition)
                return candidate;
        }

        throw new InvalidOperationException($"no key found for partition {partition}");
    }
}
