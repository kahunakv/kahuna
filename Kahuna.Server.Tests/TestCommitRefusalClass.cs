using System.Text;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// The commit answer names the arm that refused it, and settlement lag is answered as a wait rather than as a
/// lock conflict. Both are what a client needs to act on a refusal: a refused commit prints its class and
/// reason, and a write or lock that only waits on a predecessor's settlement can be given a longer budget than
/// a deadlock wait.
/// </summary>
public sealed class TestCommitRefusalClass
{
    private const int LockExpiresMs = 30_000;

    private readonly ILoggerFactory loggerFactory;

    public TestCommitRefusalClass(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper, quietKommander: true);
    }

    private static async Task<EmbeddedKahunaNode> StartNode(ILoggerFactory loggerFactory, CancellationToken ct)
    {
        EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            TimerInitialDelay = TimeSpan.FromMilliseconds(50),
            ReadIOThreads = 1,
            WriteIOThreads = 1,
            PartitionExecutorPoolSize = 1,
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 4,
            DurableDeferredSettlement = true
        }, loggerFactory);

        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync("refusal/seed", ct);

        return node;
    }

    private static string Bucket(string tag) => $"refusal-{Guid.NewGuid().ToString("N")[..8]}-{tag}";

    /// <summary>
    /// An optimistic transaction whose read was overtaken by a later commit is refused at its read-set
    /// validation. The answer carries the conflict class and the coordinator's reason, deconstructs to the old
    /// (type, anchor) pair, and the same class is replayed to a duplicate commit of the finalized session.
    /// </summary>
    [Fact]
    public async Task ReadSetConflict_NamesTheConflictClassOnTheCommitAnswer()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        IKahuna kahuna = node.Kahuna;

        string bucket = Bucket("conflict");
        string read = bucket + "/read";
        string written = bucket + "/written";
        await Seed(kahuna, read, "v0", ct);

        TransactionHandle tx = await StartTransaction(kahuna, bucket + "/tx", KeyValueTransactionLocking.Optimistic, ct);

        (KeyValueResponseType readType, _) = await kahuna.LocateAndTryGetValue(
            tx.TransactionId, read, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct,
            coordinatorKey: tx.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
        Assert.Equal(KeyValueResponseType.Get, readType);

        // A later commit moves the read key past what the transaction observed.
        await Seed(kahuna, read, "v1", ct);

        (KeyValueResponseType setType, _, _) = await kahuna.LocateAndTrySetKeyValue(
            tx.TransactionId, written, "w"u8.ToArray(), null, -1,
            KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct,
            coordinatorKey: tx.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
        Assert.Equal(KeyValueResponseType.Set, setType);

        TransactionCommitAnswer answer = await kahuna.LocateAndCommitTransaction(tx, ct);

        TestContext.Current.TestOutputHelper?.WriteLine($"commit: {answer.Type} ({answer.Refusal}: {answer.Reason})");

        Assert.Equal(KeyValueResponseType.Aborted, answer.Type);
        Assert.Equal(CommitRefusalClass.Conflict, answer.Refusal);
        Assert.NotNull(answer.Reason);
        Assert.Contains(read, answer.Reason);

        // The old shape still deconstructs from the typed answer.
        (KeyValueResponseType legacyType, string? legacyAnchor) = answer;
        Assert.Equal(KeyValueResponseType.Aborted, legacyType);
        Assert.Equal(answer.RecordAnchorKey, legacyAnchor);

        // A duplicate commit inside the idempotency window replays the retained outcome, class included.
        TransactionCommitAnswer replay = await kahuna.LocateAndCommitTransaction(tx, ct);
        Assert.Equal(KeyValueResponseType.Aborted, replay.Type);
        Assert.Equal(CommitRefusalClass.Conflict, replay.Refusal);
    }

    /// <summary>A committed transaction answers no refusal.</summary>
    [Fact]
    public async Task Commit_AnswersNoRefusal()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        IKahuna kahuna = node.Kahuna;

        string bucket = Bucket("commit");
        TransactionHandle tx = await StartTransaction(kahuna, bucket + "/tx", KeyValueTransactionLocking.Pessimistic, ct);
        await LockAndSet(kahuna, tx, bucket + "/k", "v1", ct);

        TransactionCommitAnswer answer = await kahuna.LocateAndCommitTransaction(tx, ct);

        Assert.Equal(KeyValueResponseType.Committed, answer.Type);
        Assert.Equal(CommitRefusalClass.None, answer.Refusal);
        Assert.Null(answer.Reason);
        Assert.NotNull(answer.RecordAnchorKey);
    }

    /// <summary>
    /// A committed predecessor whose settlement has not run never blocks a write: the write materializes the
    /// committed value and proceeds, so the settlement window is not a wait at all on the set path.
    /// </summary>
    [Fact]
    public async Task Set_OverACommittedButUnsettledPredecessor_Proceeds()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        IKahuna kahuna = node.Kahuna;
        KahunaManager manager = (KahunaManager)kahuna;

        string bucket = Bucket("settled");
        string key = bucket + "/k";
        await Seed(kahuna, key, "v0", ct);

        using ResolutionGate gate = new(manager);

        TransactionHandle t1 = await StartTransaction(kahuna, bucket + "/t1", KeyValueTransactionLocking.Pessimistic, ct);
        await LockAndSet(kahuna, t1, key, "v1", ct);
        Assert.Equal(KeyValueResponseType.Committed, (await kahuna.LocateAndCommitTransaction(t1, ct)).Type);
        Assert.NotNull(manager.DurablePreparedIntentStore.Get(key));

        (KeyValueResponseType setType, _, _) = await kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, key, "v2"u8.ToArray(), null, -1,
            KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Set, setType);

        gate.Release();
        await WaitUntil(() => manager.DurablePreparedIntentStore.Get(key) is null);
        Assert.Equal("v2", await ReadCommitted(kahuna, key, ct));
    }

    /// <summary>
    /// A write that meets a durable prepared intent whose decision is still pending is waiting on lag, not on a
    /// lock: the holder is inside its decision window and waits on no one, so a deadlock through it is
    /// impossible. The write answers WaitingForReplication naming the holder, and proceeds once the decision
    /// lands. Before this contract the same wait answered MustRetry, the code a live lock conflict answers, so a
    /// client bounded both with one short deadlock budget.
    /// </summary>
    [Fact]
    public async Task Set_OverAnUndecidedDurableIntent_AnswersWaitingForReplication_ThenProceedsAfterTheDecision()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        IKahuna kahuna = node.Kahuna;
        KahunaManager manager = (KahunaManager)kahuna;

        string bucket = Bucket("undecided");
        string key = bucket + "/k";
        await Seed(kahuna, key, "v0", ct);

        TransactionHandle writer = await StartTransaction(kahuna, bucket + "/writer", KeyValueTransactionLocking.Pessimistic, ct);
        InstallUndecidedIntent(manager, writer, key);

        (KeyValueResponseType heldType, _, _) = await kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, key, "v2"u8.ToArray(), null, -1,
            KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);

        TestContext.Current.TestOutputHelper?.WriteLine($"set over an undecided durable intent: {heldType}");
        Assert.Equal(KeyValueResponseType.WaitingForReplication, heldType);

        // The holder's decision lands as an abort tombstone on its record: the intent no longer stands in the way.
        DecideAbort(manager, writer, bucket + "/writer");

        (KeyValueResponseType afterType, _, _) = await kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, key, "v2"u8.ToArray(), null, -1,
            KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Set, afterType);
        Assert.Equal("v2", await ReadCommitted(kahuna, key, ct));
    }

    /// <summary>
    /// A point lock acquire whose whole wait budget is spent on an undecided durable intent answers the wait it
    /// was, not MustRetry: the holder is deciding, so the acquire can never be part of a deadlock.
    /// </summary>
    [Fact]
    public async Task Lock_OverAnUndecidedDurableIntent_WaitsThenAnswersWaitingForReplication()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        IKahuna kahuna = node.Kahuna;
        KahunaManager manager = (KahunaManager)kahuna;

        string bucket = Bucket("lock-lag");
        string key = bucket + "/k";
        await Seed(kahuna, key, "v0", ct);

        TransactionHandle writer = await StartTransaction(kahuna, bucket + "/writer", KeyValueTransactionLocking.Pessimistic, ct);
        InstallUndecidedIntent(manager, writer, key);

        TransactionHandle t2 = await StartTransaction(kahuna, bucket + "/t2", KeyValueTransactionLocking.Pessimistic, ct);
        (KeyValueResponseType heldType, _, _, _) = await kahuna.LocateAndTryAcquireExclusiveLock(
            t2.TransactionId, key, LockExpiresMs, KeyValueDurability.Persistent, ct,
            coordinatorKey: t2.CoordinatorKey, operationId: TransactionOperationId.NewRandom());

        TestContext.Current.TestOutputHelper?.WriteLine($"lock over an undecided durable intent: {heldType}");
        Assert.Equal(KeyValueResponseType.WaitingForReplication, heldType);

        DecideAbort(manager, writer, bucket + "/writer");
        await kahuna.LocateAndRollbackTransaction(t2, ct);
    }

    /// <summary>A durable prepared intent of <paramref name="writer"/> on <paramref name="key"/> with no decision
    /// yet: the shape of a commit whose decision is still in flight.</summary>
    private static void InstallUndecidedIntent(KahunaManager manager, TransactionHandle writer, string key)
    {
        manager.DurablePreparedIntentStore.ImportIntents([new PreparedIntent(
            writer.TransactionId, Epoch: 0, key, ManifestHash: 0, RecordAnchorKey: writer.CoordinatorKey,
            CommitTimestamp: writer.TransactionId, State: KeyValueState.Set, Value: "v1"u8.ToArray(), Bucket: null,
            Revision: 1, Expires: HLCTimestamp.Zero, NoRevision: false, BaseRevision: -1,
            BaseState: KeyValueState.Undefined, RecoveryDeadline: HLCTimestamp.Zero,
            Resolution: PreparedIntentResolution.Pending)]);
    }

    /// <summary>Decides the writer's record as an abort tombstone on this single node, which is its canonical
    /// record store.</summary>
    private static void DecideAbort(KahunaManager manager, TransactionHandle writer, string anchor)
    {
        HLCTimestamp now = new(writer.TransactionId.N, writer.TransactionId.L + 1, 0);
        manager.DurableTransactionRecordStore.Apply(new AbortTransactionCommand(
            writer.TransactionId, Epoch: 0, ManifestHash: 0, TransactionAbortClass.ExplicitRollback,
            OpId: now, AttemptHlc: now, anchor, CommitTimestamp: writer.TransactionId,
            DecisionDeadline: now, CreatedAt: writer.TransactionId));
    }

    private static async Task LockAndSet(IKahuna kahuna, TransactionHandle tx, string key, string value, CancellationToken ct)
    {
        (KeyValueResponseType lockType, _, _, _) = await kahuna.LocateAndTryAcquireExclusiveLock(
            tx.TransactionId, key, LockExpiresMs, KeyValueDurability.Persistent, ct,
            coordinatorKey: tx.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
        Assert.Equal(KeyValueResponseType.Locked, lockType);

        (KeyValueResponseType setType, _, _) = await kahuna.LocateAndTrySetKeyValue(
            tx.TransactionId, key, Encoding.UTF8.GetBytes(value), null, -1,
            KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct,
            coordinatorKey: tx.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
        Assert.Equal(KeyValueResponseType.Set, setType);
    }

    private static async Task<string> ReadCommitted(IKahuna kahuna, string key, CancellationToken ct)
    {
        (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);

        Assert.Equal(KeyValueResponseType.Get, type);
        return entry?.Value is null ? "" : Encoding.UTF8.GetString(entry.Value);
    }

    private static async Task<TransactionHandle> StartTransaction(IKahuna kahuna, string coordinatorKey, KeyValueTransactionLocking locking, CancellationToken ct)
    {
        (KeyValueResponseType startType, TransactionHandle handle) = await kahuna.LocateAndStartTransaction(
            new KeyValueTransactionOptions
            {
                CoordinatorKey = coordinatorKey,
                Locking = locking,
                Timeout = 60_000
            }, ct);
        Assert.Equal(KeyValueResponseType.Set, startType);

        return handle;
    }

    private static async Task Seed(IKahuna kahuna, string key, string value, CancellationToken ct)
    {
        (KeyValueResponseType type, _, _) = await kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, key, Encoding.UTF8.GetBytes(value), null, -1,
            KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct);

        Assert.Equal(KeyValueResponseType.Set, type);
    }

    private static async Task WaitUntil(Func<bool> predicate, int timeoutMs = 10_000)
    {
        long deadline = Environment.TickCount64 + timeoutMs;
        while (Environment.TickCount64 < deadline)
        {
            if (predicate()) return;
            await Task.Delay(10);
        }

        Assert.True(predicate(), "condition not met in time");
    }

    /// <summary>Holds every deferred resolution scheduled while installed, so a commit's decision→settlement
    /// window stays open until <see cref="Release"/>. Disposal releases and uninstalls the hook.</summary>
    private sealed class ResolutionGate : IDisposable
    {
        private readonly DurableTransactionFinalizer finalizer;

        private readonly TaskCompletionSource open = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public ResolutionGate(KahunaManager manager)
        {
            finalizer = manager.TransactionCoordinator.DurableFinalizerForTests;
            finalizer.TestBeforeDeferredResolutionHook = ct => open.Task.WaitAsync(ct);
        }

        public void Release()
        {
            finalizer.TestBeforeDeferredResolutionHook = null;
            open.TrySetResult();
        }

        public void Dispose() => Release();
    }
}
